#!/usr/bin/env python3
"""Unit and contract tests for M2 SQL UDFs and two-stage history replay.

Runs against DuckDB CLI (`duckdb -unsigned -json :memory:`):
1. ens_dispersion and ens_match_scenarios UDF types and core equivalence
2. NULL handling, dimension mismatch, non-finite values, and error recovery on same connection
3. Large chunks (> duckdb_vector_size() = 2048) and cross-boundary error isolation
4. Stage and publish 4-batch / 8-batch synthetic histories
5. Missing batches, single-batch whitelisted algorithm failures (break chain, status='failed')
6. Idempotent publish and RUN_VERSION_CONFLICT rejection
7. Four invariant guards (G1-G4) and atomic rollback on mid-transaction failure
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXTENSION_PATH = Path(os.environ.get("ENSABC_M2_EXTENSION", "/tmp/test-m2-ext/ensabc_m0_extension.duckdb_extension"))
SCHEMA_SQL = REPO_ROOT / "validation" / "m2" / "sql" / "schema.sql"
STAGE_PUBLISH_SQL = REPO_ROOT / "validation" / "m2" / "sql" / "stage_and_publish.sql"


from validation.m2.common import parse_duckdb_json, run_isolated_duckdb, real_replay_binding


class DuckDBRunner:
    def __init__(self, extension_path: Path):
        self.extension_path = extension_path.resolve()

    def run_sql(self, sql: str, timeout: float = 60.0, db_file: str = ":memory:") -> tuple[int, str, str]:
        dedented_sql = textwrap.dedent(sql)
        script = f"LOAD '{self.extension_path}';\n{dedented_sql}"
        proc = run_isolated_duckdb(script, database=db_file, timeout=timeout, bail=False)
        return proc.returncode, proc.stdout, proc.stderr


class TestSqlHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not EXTENSION_PATH.is_file():
            build_script = REPO_ROOT / "validation" / "m2" / "build_extension.sh"
            res = subprocess.run(
                [str(build_script), "--output-dir", "/tmp/test-m2-ext"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(f"Failed to build extension: {res.stderr}")
        cls.runner = DuckDBRunner(EXTENSION_PATH)

    def test_matching_reports_local_ambiguity_and_optimum_membership(self):
        sql = """
        SELECT ens_match_scenarios(
          [{'cluster_id':0,'mean_features':[0.0]}, {'cluster_id':1,'mean_features':[0.0]}, {'cluster_id':2,'mean_features':[100.0]}],
          [{'cluster_id':0,'mean_features':[0.0]}, {'cluster_id':1,'mean_features':[0.0]}, {'cluster_id':2,'mean_features':[100.0]}],
          [1.0], struct_pack(threshold_coefficient := 0.0, prev_dispersion := 1.0, curr_dispersion := 1.0)) AS result;
        SELECT ens_match_scenarios(
          [{'cluster_id':0,'mean_features':[0.0]}, {'cluster_id':1,'mean_features':[1.0]}],
          [{'cluster_id':0,'mean_features':[0.0]}], [1.0],
          struct_pack(threshold_coefficient := 2.0, prev_dispersion := 2.0, curr_dispersion := 2.0)) AS result;
        """
        code, stdout, stderr = self.runner.run_sql(sql)
        self.assertEqual(code, 0, stderr)
        first, second = parse_duckdb_json(stdout)
        links = first[0]["result"]["links"]
        self.assertEqual([l["ambiguous"] for l in links], [True, True, False])
        second_res = second[0]["result"]
        unmatched = second_res["unmatched_prev"]
        self.assertEqual(unmatched[0]["cluster_id"], 1)
        self.assertEqual(unmatched[0]["reason"], "not_selected_in_global_optimum")
        self.assertFalse(unmatched[0]["matched_in_some_optimum"])
        # Verify alternative_distance_increase_ratio == gap / pair_scale (S = 2.0, gap = 1.0 -> 0.5)
        link0 = second_res["links"][0]
        self.assertEqual(link0["alternative_status"], "same_count")
        self.assertAlmostEqual(link0["alternative_distance_increase"], 1.0, places=12)
        self.assertAlmostEqual(link0["alternative_distance_increase_ratio"], 0.5, places=12)

    def test_dispersion_udf_basic(self):
        sql = """
        SELECT ens_dispersion(
            [{'member_id': 'm0', 'features': [1.0, 2.0]}, {'member_id': 'm1', 'features': [3.0, 4.0]}],
            [1.0, 1.0]
        ) AS disp;
        """
        code, stdout, stderr = self.runner.run_sql(sql)
        self.assertEqual(code, 0, f"SQL error: {stderr}")
        rows = json.loads(stdout)
        self.assertAlmostEqual(rows[0]["disp"], 1.0, places=9)

    def test_match_scenarios_udf_counterexample_2x2(self):
        p1_0 = (-1.0 + 63.0**0.5) / 2.0
        p1_1 = -1.0 - p1_0
        sql = f"""
        SELECT ens_match_scenarios(
            [{{'cluster_id': 0, 'mean_features': [0.0, 0.0]}}, {{'cluster_id': 1, 'mean_features': [{p1_0}, {p1_1}]}}],
            [{{'cluster_id': 0, 'mean_features': [0.0, 0.0]}}, {{'cluster_id': 1, 'mean_features': [4.0, 4.0]}}],
            [1.0, 1.0],
            {{'threshold_coefficient': 5.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}}
        ) AS res;
        """
        code, stdout, stderr = self.runner.run_sql(sql)
        self.assertEqual(code, 0, f"SQL error: {stderr}")
        res = json.loads(stdout)[0]["res"]
        self.assertEqual(res["edge_count"], 2)
        self.assertAlmostEqual(res["total_distance"], 8.0, places=9)
        self.assertEqual(len(res["links"]), 2)
        self.assertEqual(res["links"][0]["prev_cluster_id"], 0)
        self.assertEqual(res["links"][0]["curr_cluster_id"], 1)
        self.assertEqual(res["links"][1]["prev_cluster_id"], 1)
        self.assertEqual(res["links"][1]["curr_cluster_id"], 0)

    def test_null_and_error_recovery(self):
        # 1. Valid -> NULL failure -> valid recovery all within the SAME DuckDB connection
        script = """
        SELECT ens_dispersion(
            [{'member_id': 'm0', 'features': [1.0, 2.0]}, {'member_id': 'm1', 'features': [3.0, 4.0]}],
            [1.0, 1.0]
        ) AS before_err;
        SELECT ens_dispersion(NULL, [1.0, 1.0]);
        SELECT ens_dispersion(
            [{'member_id': 'm0', 'features': [1.0, 2.0]}, {'member_id': 'm1', 'features': [3.0, 4.0]}],
            [1.0, 1.0]
        ) AS after_err;
        """
        code, stdout, stderr = self.runner.run_sql(script)
        self.assertIn("NULL_INPUT", stderr)
        results = parse_duckdb_json(stdout)
        self.assertEqual(len(results), 2)
        self.assertAlmostEqual(results[0][0]["before_err"], 1.0, places=9)
        self.assertAlmostEqual(results[1][0]["after_err"], 1.0, places=9)

        # 2. Dimension mismatch
        sql = """
        SELECT ens_match_scenarios(
            [{'cluster_id': 0, 'mean_features': [1.0, 2.0]}],
            [{'cluster_id': 0, 'mean_features': [1.0, 2.0]}],
            [1.0],
            {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
        );
        """
        code, _, stderr = self.runner.run_sql(sql)
        self.assertNotEqual(code, 0)
        self.assertIn("DIMENSION_MISMATCH", stderr)

    def test_exceeded_scenario_limit_13(self):
        sc13_items = ", ".join(f"{{'cluster_id': {i}, 'mean_features': [{float(i)}]}}" for i in range(13))
        sql = f"""
        SELECT ens_match_scenarios(
            [{sc13_items}],
            [{{'cluster_id': 0, 'mean_features': [0.0]}}],
            [1.0],
            {{'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}}
        );
        """
        code, _, stderr = self.runner.run_sql(sql)
        self.assertNotEqual(code, 0)
        self.assertIn("TOO_MANY_SCENARIOS", stderr)

    def test_synthetic_4batch_replay_and_idempotency(self):
        script = f"""
        .read '{SCHEMA_SQL}'

        SET VARIABLE threshold_coefficient = 1.0;
        SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
        SET VARIABLE planned_batch_count = 4;
        SET VARIABLE inject_failure = false;
        SET VARIABLE run_key = 'test_run_key_synth_4b_001';
        SET VARIABLE execution_binding = {{
            'run_id': 'run_synth_001',
            'input_manifest_sha256': 'sha_manifest_test',
            'scenario_config_sha256': 'sha_sc_test',
            'match_config_sha256': 'sha_match_test',
            'weights_version': 'equal.v0',
            'threshold_coefficient': 1.0,
            'cluster_count': 2,
            'software': {{
                'ensabc_extension_sha256': 'sha_ext',
                'duckomo_version': 'v0.3.5-rc.1',
                'duckomo_sha256': 'sha_duckomo',
                'duckdb_version': 'v1.5.5',
                'cluster_algorithm_version': 'm1.native.v1',
                'match_algorithm_version': 'm2.match.v1'
            }}
        }};

        CREATE OR REPLACE TEMP TABLE stage_batches AS
        SELECT * FROM (VALUES
            (0, TIMESTAMP '2020-01-01 00:00:00', 96, 'ok', NULL, 'in0', 'om0', 1.0, 'res0'),
            (1, TIMESTAMP '2020-01-01 12:00:00', 84, 'ok', NULL, 'in1', 'om1', 1.0, 'res1'),
            (2, TIMESTAMP '2020-01-02 00:00:00', 72, 'ok', NULL, 'in2', 'om2', 1.0, 'res2'),
            (3, TIMESTAMP '2020-01-02 12:00:00', 60, 'ok', NULL, 'in3', 'om3', 1.0, 'res3')
        ) t(ordinal, init_time, lead_hours, status, status_reason, input_sha256, om_sha256, dispersion, cluster_result_sha256);

        CREATE OR REPLACE TEMP TABLE stage_cluster AS
        SELECT ordinal, {{'algorithm_version': 'm1.native.v1', 'scenarios': [
            {{'cluster_id': 0, 'member_count': 25, 'member_fraction': 0.5, 'mean_features': [280.0 + ordinal * 0.1, 281.0 + ordinal * 0.1], 'representative_member_id': 'm01', 'within_cluster_sse': 0.1}},
            {{'cluster_id': 1, 'member_count': 25, 'member_fraction': 0.5, 'mean_features': [290.0 + ordinal * 0.1, 291.0 + ordinal * 0.1], 'representative_member_id': 'm02', 'within_cluster_sse': 0.1}}
        ]}} AS result
        FROM range(4) t(ordinal);

        .read '{STAGE_PUBLISH_SQL}'

        -- Republish the exact same run_key and content to verify idempotency
        .read '{STAGE_PUBLISH_SQL}'

        SELECT
            (SELECT count(*) FROM tracking_runs WHERE run_key = 'test_run_key_synth_4b_001') AS runs,
            (SELECT count(*) FROM replay_batches WHERE run_key = 'test_run_key_synth_4b_001') AS batches,
            (SELECT count(*) FROM scenario_nodes WHERE run_key = 'test_run_key_synth_4b_001') AS nodes,
            (SELECT count(*) FROM pair_comparisons WHERE run_key = 'test_run_key_synth_4b_001') AS pairs,
            (SELECT count(*) FROM scenario_links WHERE run_key = 'test_run_key_synth_4b_001') AS links;
        """
        code, stdout, stderr = self.runner.run_sql(script)
        self.assertEqual(code, 0, f"Stage and publish failed: {stderr}")
        all_results = parse_duckdb_json(stdout)
        res = all_results[-1][0]
        self.assertEqual(res["runs"], 1)
        self.assertEqual(res["batches"], 4)
        self.assertEqual(res["nodes"], 8)
        self.assertEqual(res["pairs"], 3)
        self.assertEqual(res["links"], 6)

    def test_conflict_rejection(self):
        script = f"""
        .read '{SCHEMA_SQL}'

        SET VARIABLE threshold_coefficient = 1.0;
        SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
        SET VARIABLE planned_batch_count = 4;
        SET VARIABLE inject_failure = false;
        SET VARIABLE run_key = 'test_run_key_conflict_001';
        SET VARIABLE execution_binding = {{
            'run_id': 'run_conflict_001',
            'input_manifest_sha256': 'sha_manifest_test',
            'scenario_config_sha256': 'sha_sc_test',
            'match_config_sha256': 'sha_match_test',
            'weights_version': 'equal.v0',
            'threshold_coefficient': 1.0,
            'cluster_count': 2,
            'software': {{
                'ensabc_extension_sha256': 'sha_ext',
                'duckomo_version': 'v0.3.5-rc.1',
                'duckomo_sha256': 'sha_duckomo',
                'duckdb_version': 'v1.5.5',
                'cluster_algorithm_version': 'm1.native.v1',
                'match_algorithm_version': 'm2.match.v1'
            }}
        }};

        CREATE OR REPLACE TEMP TABLE stage_batches AS
        SELECT * FROM (VALUES
            (0, TIMESTAMP '2020-01-01 00:00:00', 96, 'ok', NULL, 'in0', 'om0', 1.0, 'res0'),
            (1, TIMESTAMP '2020-01-01 12:00:00', 84, 'ok', NULL, 'in1', 'om1', 1.0, 'res1'),
            (2, TIMESTAMP '2020-01-02 00:00:00', 72, 'ok', NULL, 'in2', 'om2', 1.0, 'res2'),
            (3, TIMESTAMP '2020-01-02 12:00:00', 60, 'ok', NULL, 'in3', 'om3', 1.0, 'res3')
        ) t(ordinal, init_time, lead_hours, status, status_reason, input_sha256, om_sha256, dispersion, cluster_result_sha256);

        CREATE OR REPLACE TEMP TABLE stage_cluster AS
        SELECT ordinal, {{'algorithm_version': 'm1.native.v1', 'scenarios': [
            {{'cluster_id': 0, 'member_count': 25, 'member_fraction': 0.5, 'mean_features': [280.0, 281.0], 'representative_member_id': 'm01', 'within_cluster_sse': 0.1}},
            {{'cluster_id': 1, 'member_count': 25, 'member_fraction': 0.5, 'mean_features': [290.0, 291.0], 'representative_member_id': 'm02', 'within_cluster_sse': 0.1}}
        ]}} AS result
        FROM range(4) t(ordinal);

        .read '{STAGE_PUBLISH_SQL}'

        -- Now modify dispersion to change content_sha256 while keeping run_key identical
        UPDATE stage_batches SET dispersion = 3.5 WHERE ordinal = 0;

        .read '{STAGE_PUBLISH_SQL}'
        """
        code, stdout, stderr = self.runner.run_sql(script)
        self.assertNotEqual(code, 0)
        self.assertIn("RUN_VERSION_CONFLICT", stderr)

    def test_atomic_rollback_on_fault(self):
        script = f"""
        .read '{SCHEMA_SQL}'

        SET VARIABLE threshold_coefficient = 1.0;
        SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
        SET VARIABLE planned_batch_count = 4;
        SET VARIABLE inject_failure = true;
        SET VARIABLE run_key = 'test_run_key_rollback_001';
        SET VARIABLE execution_binding = {{
            'run_id': 'run_rollback_001',
            'input_manifest_sha256': 'sha_manifest_test',
            'scenario_config_sha256': 'sha_sc_test',
            'match_config_sha256': 'sha_match_test',
            'weights_version': 'equal.v0',
            'threshold_coefficient': 1.0,
            'cluster_count': 2,
            'software': {{
                'ensabc_extension_sha256': 'sha_ext',
                'duckomo_version': 'v0.3.5-rc.1',
                'duckomo_sha256': 'sha_duckomo',
                'duckdb_version': 'v1.5.5',
                'cluster_algorithm_version': 'm1.native.v1',
                'match_algorithm_version': 'm2.match.v1'
            }}
        }};

        CREATE OR REPLACE TEMP TABLE stage_batches AS
        SELECT * FROM (VALUES
            (0, TIMESTAMP '2020-01-01 00:00:00', 96, 'ok', NULL, 'in0', 'om0', 1.0, 'res0'),
            (1, TIMESTAMP '2020-01-01 12:00:00', 84, 'ok', NULL, 'in1', 'om1', 1.0, 'res1'),
            (2, TIMESTAMP '2020-01-02 00:00:00', 72, 'ok', NULL, 'in2', 'om2', 1.0, 'res2'),
            (3, TIMESTAMP '2020-01-02 12:00:00', 60, 'ok', NULL, 'in3', 'om3', 1.0, 'res3')
        ) t(ordinal, init_time, lead_hours, status, status_reason, input_sha256, om_sha256, dispersion, cluster_result_sha256);

        CREATE OR REPLACE TEMP TABLE stage_cluster AS
        SELECT ordinal, {{'algorithm_version': 'm1.native.v1', 'scenarios': [
            {{'cluster_id': 0, 'member_count': 25, 'member_fraction': 0.5, 'mean_features': [280.0, 281.0], 'representative_member_id': 'm01', 'within_cluster_sse': 0.1}},
            {{'cluster_id': 1, 'member_count': 25, 'member_fraction': 0.5, 'mean_features': [290.0, 291.0], 'representative_member_id': 'm02', 'within_cluster_sse': 0.1}}
        ]}} AS result
        FROM range(4) t(ordinal);

        .read '{STAGE_PUBLISH_SQL}'
        ROLLBACK;

        SELECT
            (SELECT count(*) FROM tracking_runs WHERE run_key = 'test_run_key_rollback_001') AS runs,
            (SELECT count(*) FROM replay_batches WHERE run_key = 'test_run_key_rollback_001') AS batches,
            (SELECT count(*) FROM scenario_nodes WHERE run_key = 'test_run_key_rollback_001') AS nodes,
            (SELECT count(*) FROM pair_comparisons WHERE run_key = 'test_run_key_rollback_001') AS pairs,
            (SELECT count(*) FROM scenario_links WHERE run_key = 'test_run_key_rollback_001') AS links;
        """
        code, stdout, stderr = self.runner.run_sql(script)
        self.assertIn("INJECTED_FAILURE", stderr)
        all_results = parse_duckdb_json(stdout)
        counts = all_results[-1][0]
        self.assertEqual(counts, {"runs": 0, "batches": 0, "nodes": 0, "pairs": 0, "links": 0})


if __name__ == "__main__":
    unittest.main()
