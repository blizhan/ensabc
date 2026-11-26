#!/usr/bin/env python3
"""Unit and contract tests for M3 DuckDB UDFs and 11-table atomic SQL history (T024).

Covers:
1. `ens_fsum`: Shewchuk exact expansion summation (`0`-ULP parity with `math.fsum`),
   empty list `0.0`, `-0.0 -> +0.0` normalization, top-level & nested `NULL` rejection,
   `NaN`/`Inf` rejection, and `DBL_MAX` intermediate overflow rejection.
2. M0/M1/M2 UDF preservation (`ens_m0_echo`, `ens_cluster`, `ens_dispersion`,
   `ens_match_scenarios` signatures and outputs 100% unchanged).
3. `ens_evolve_scenarios`: all output `STRUCT` fields and non-`NULL` `has_*` sentinels
   across `continuation`, `split`, `merge`, `emergence`, `disappearance`, `ambiguous_tie`,
   and `high_uncertainty`; top-level & nested `NULL` rejection across all 5 parameters;
   fraction/member/config validation errors; and tampered `backbone_match` rejection
   (`[BACKBONE_INCONSISTENCY]` and `[INVALID_PRIMARY_LINK]`).
4. Cross-chunk execution (`2100 > duckdb_vector_size() = 2048` rows) for both `ens_fsum`
   and `ens_evolve_scenarios`, plus whole-query failure when a bad row appears in the
   second vector chunk (`row >= 2048`).
5. Same-session `valid -> invalid -> valid` recovery in a single DuckDB CLI connection.
6. Two-stage SQL (`validation/m3/sql/stage_and_publish.sql`):
   - Per-batch fault isolation converting ONLY the 8 whitelisted single-batch failure
     codes into `status = 'failed'` while aborting non-whitelisted metadata/input/config
     failures before Stage B with 0 published rows.
   - Strict disjointness of `gap_interrupted` / `gap_restarted` vs `disappearance` /
     `emergence` (including `both_unavailable` double gaps).
   - 100% field-by-field parity between the 6 published M3 tables and `m3_runner --mode chain`.
   - Same-version idempotency, `EVOLUTION_RUN_VERSION_CONFLICT` rejection, multi-version
     coexistence, and `inject_m3_failure` rollback leaving 0 residual rows across all 6 M3
     tables at every insertion hook followed by clean recovery publish.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    EVOLUTION_ALGORITHM_VERSION,
    FIXTURES_DIR,
    NON_PHYSICAL_DISCLAIMER,
    canonical_json_sha256,
    default_evolution_config,
    load_json,
    parse_duckdb_json,
    run_isolated_duckdb,
    sql_value,
)

DEFAULT_EXT = Path("/tmp/ensabc-m3-extension-target/ensabc_m0_extension.duckdb_extension")
EXTENSION_PATH = Path(os.environ.get("ENSABC_M3_EXTENSION", str(DEFAULT_EXT)))
SCHEMA_SQL = REPO_ROOT / "validation" / "m3" / "sql" / "schema.sql"
STAGE_PUBLISH_SQL = REPO_ROOT / "validation" / "m3" / "sql" / "stage_and_publish.sql"


def _resolve_extension_and_runner() -> tuple[Path, Path]:
    explicit_ext = "ENSABC_M3_EXTENSION" in os.environ
    rs_sources = [
        REPO_ROOT / "crates" / "ensabc-core" / "src" / "evolution.rs",
        REPO_ROOT / "crates" / "ensabc-duckdb" / "src" / "evolve_ext.rs",
        REPO_ROOT / "crates" / "ensabc-core" / "examples" / "m3_runner.rs",
    ]
    min_mtime = max((p.stat().st_mtime for p in rs_sources if p.is_file()), default=0.0)

    def _is_fresh(p: Path) -> bool:
        return p.is_file() and (explicit_ext or p.stat().st_mtime >= min_mtime)

    ext = EXTENSION_PATH
    if not _is_fresh(ext):
        if explicit_ext:
            raise RuntimeError(f"Configured ENSABC_M3_EXTENSION does not exist: {ext}")
        fallback_dir = Path("/tmp/ensabc-m3-test-ext")
        fallback_ext = fallback_dir / "ensabc_m0_extension.duckdb_extension"
        if not _is_fresh(fallback_ext):
            res = subprocess.run(
                ["bash", str(REPO_ROOT / "validation" / "m3" / "build_extension.sh"), "--output-dir", str(fallback_dir)],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            if res.returncode != 0:
                raise RuntimeError(f"Failed to build M3 extension: {res.stderr}")
        ext = fallback_ext

    ext = ext.resolve()
    candidates = [
        ext.parent / "m3_runner",
        Path("/tmp/ensabc-m3-extension-target/ensabc_m3_runner"),
        Path("/tmp/ensabc-m3-extension-target/m3_runner"),
        Path("/tmp/ensabc-m3-extension-target/release/examples/m3_runner"),
        REPO_ROOT / "target" / "release" / "examples" / "m3_runner",
    ]
    for cand in candidates:
        if _is_fresh(cand) and os.access(cand, os.X_OK):
            return ext, cand.resolve()

    target_dir = os.environ.get("ENSABC_M3_TARGET_DIR", "/tmp/ensabc-m3-extension-target")
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = target_dir
    subprocess.run(
        ["cargo", "build", "--locked", "--release", "-p", "ensabc-core", "--example", "m3_runner"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
    )
    built = Path(target_dir) / "release" / "examples" / "m3_runner"
    return ext, built.resolve()


def _synthetic_bindings(
    run_key: str,
    evolution_run_id: str,
    *,
    threshold_coefficient: float = 1.0,
    cluster_count: int = 2,
    evolve_cfg: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    ev = evolve_cfg or default_evolution_config()
    exec_binding = {
        "run_id": run_key,
        "input_manifest_sha256": canonical_json_sha256({"run_key": run_key}),
        "scenario_config_sha256": canonical_json_sha256({"cluster_count": cluster_count}),
        "match_config_sha256": canonical_json_sha256(
            {"threshold_coefficient": threshold_coefficient, "algorithm": "m2.match.v1"}
        ),
        "weights_version": "synthetic.weights.v1",
        "threshold_coefficient": float(threshold_coefficient),
        "cluster_count": int(cluster_count),
        "software": {
            "ensabc_extension_sha256": "synthetic_test_ext",
            "duckomo_version": "v0.3.5-rc.1",
            "duckomo_sha256": "synthetic_test_duckomo",
            "duckdb_version": "v1.5.5",
            "cluster_algorithm_version": "m1.native.v1",
            "match_algorithm_version": "m2.match.v1",
        },
    }
    evol_binding = {
        "tracking_run_key": run_key,
        "input_manifest_sha256": exec_binding["input_manifest_sha256"],
        "scenario_config_sha256": exec_binding["scenario_config_sha256"],
        "match_config_sha256": exec_binding["match_config_sha256"],
        "evolve_config_sha256": canonical_json_sha256(ev),
        "weights_version": exec_binding["weights_version"],
        "secondary_rule": ev["secondary_rule"],
        "flow_rule": ev["flow_rule"],
        "uncertainty_rule": ev.get("uncertainty_rule", "m3.uncertainty.v1"),
        "fraction_sum_tolerance": float(ev["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev["high_uncertainty_secondary_ratio"]),
        "software": {
            **exec_binding["software"],
            "evolution_algorithm_version": EVOLUTION_ALGORITHM_VERSION,
        },
    }
    return exec_binding, evol_binding


def _build_stage_sql_from_chain_case(
    case: dict[str, Any],
    *,
    run_key: str,
    evolution_run_id: str,
    inject_m3_failure: str = "false",
) -> str:
    planned = len(case["batches"])
    tc = float(case["threshold_coefficient"])
    weights = [float(w) for w in case["weights"]]
    ev_cfg = case.get("config") or default_evolution_config()
    exec_binding, evol_binding = _synthetic_bindings(
        run_key,
        evolution_run_id,
        threshold_coefficient=tc,
        cluster_count=3,
        evolve_cfg=ev_cfg,
    )

    batch_rows: list[str] = []
    cluster_rows: list[str] = []
    for b in case["batches"]:
        ord_i = int(b["ordinal"])
        init_utc = b.get("init_time_utc", f"2020-02-01T00:00:00Z").replace("T", " ").replace("Z", "")
        lead_hr = int(b.get("lead_hours", max(12, 96 - ord_i * 12)))
        st = b["status"]
        if st == "ok":
            disp = float(b["dispersion"])
            batch_rows.append(
                f"({ord_i}, TIMESTAMP '{init_utc}', {lead_hr}, 'ok', NULL::VARCHAR, "
                f"'in_{ord_i}', 'om_{ord_i}', {disp!r}::DOUBLE, 'cls_{ord_i}')"
            )
            sc_structs = []
            for sc in b["scenarios"]:
                sc_structs.append(
                    "{"
                    f"'cluster_id': {int(sc['cluster_id'])}::BIGINT, "
                    f"'member_count': {int(sc['member_count'])}::BIGINT, "
                    f"'member_fraction': {float(sc['member_fraction'])!r}::DOUBLE, "
                    f"'mean_features': {sql_value([float(x) for x in sc['mean_features']])}::DOUBLE[], "
                    f"'representative_member_id': 'm_{int(sc['cluster_id'])}', "
                    "'within_cluster_sse': 0.1::DOUBLE"
                    "}"
                )
            cluster_rows.append(
                f"({ord_i}, {{'algorithm_version': 'm1.native.v1', 'scenarios': [{', '.join(sc_structs)}]}})"
            )
        else:
            reason = b.get("status_reason") or b.get("error_code") or "SOURCE_BATCH_MISSING"
            in_sha = "NULL::VARCHAR" if st == "missing" else f"'in_{ord_i}'"
            om_sha = "NULL::VARCHAR" if st == "missing" else f"'om_{ord_i}'"
            batch_rows.append(
                f"({ord_i}, TIMESTAMP '{init_utc}', {lead_hr}, {sql_value(st)}, {sql_value(reason)}, "
                f"{in_sha}, {om_sha}, NULL::DOUBLE, NULL::VARCHAR)"
            )

    cluster_stmt = (
        f"CREATE OR REPLACE TEMP TABLE stage_cluster AS SELECT * FROM (VALUES {', '.join(cluster_rows)}) t(ordinal, result);"
        if cluster_rows
        else """
        CREATE OR REPLACE TEMP TABLE stage_cluster AS
        SELECT 0::INTEGER AS ordinal, ens_cluster(
            [{'member_id': 'm0', 'features': [0.0, 1.0]},
             {'member_id': 'm1', 'features': [1.0, 0.0]},
             {'member_id': 'm2', 'features': [2.0, 3.0]}],
            {'projection_dim': 2, 'cluster_count': 2, 'initialization': 'kmeans++_chacha8_v1',
             'seed': 0, 'restarts': 1, 'initial_centers': []::DOUBLE[][][],
             'stopping_rule': 'sklearn_variance_scaled_v1', 'tolerance': 1e-4,
             'max_iterations': 10, 'rank_threshold': 1e-10}
        ) AS result WHERE false;
        """
    )

    evolve_cfg_sql = {
        "secondary_rule": ev_cfg["secondary_rule"],
        "flow_rule": ev_cfg["flow_rule"],
        "fraction_sum_tolerance": float(ev_cfg["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev_cfg["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev_cfg["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev_cfg["high_uncertainty_secondary_ratio"]),
    }

    return f"""
    SET VARIABLE run_key = {sql_value(run_key)};
    SET VARIABLE evolution_run_id = {sql_value(evolution_run_id)};
    SET VARIABLE planned_batch_count = {planned};
    SET VARIABLE threshold_coefficient = {tc!r}::DOUBLE;
    SET VARIABLE weights = {sql_value(weights)}::DOUBLE[];
    SET VARIABLE evolve_config = {sql_value(evolve_cfg_sql)};
    SET VARIABLE execution_binding = {sql_value(exec_binding)};
    SET VARIABLE evolution_binding = {sql_value(evol_binding)};
    SET VARIABLE inject_failure = false;
    SET VARIABLE inject_m3_failure = {sql_value(inject_m3_failure)};
    DROP TABLE IF EXISTS stage_raw_batches;

    CREATE OR REPLACE TEMP TABLE stage_batches AS
    SELECT * FROM (VALUES {', '.join(batch_rows)})
    t(ordinal, init_time, lead_hours, status, status_reason, input_sha256, om_sha256, dispersion, cluster_result_sha256);

    {cluster_stmt}

    .read validation/m3/sql/stage_and_publish.sql
    """


class DuckDBSessionRunner:
    def __init__(self, extension_path: Path) -> None:
        self.extension_path = extension_path.resolve()

    def run_sql(
        self,
        sql: str,
        *,
        db_file: str | Path = ":memory:",
        bail: bool = True,
        timeout: float = 60.0,
    ) -> tuple[int, str, str]:
        dedented = textwrap.dedent(sql)
        script = f"LOAD '{self.extension_path}';\n{dedented}"
        old_cwd = Path.cwd()
        try:
            os.chdir(REPO_ROOT)
            proc = run_isolated_duckdb(script, database=str(db_file), timeout=timeout, bail=bail)
        finally:
            os.chdir(old_cwd)
        return proc.returncode, proc.stdout, proc.stderr


class TestM3SqlHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ext_path, cls.runner_bin = _resolve_extension_and_runner()
        cls.db = DuckDBSessionRunner(cls.ext_path)
        cls.fixtures = load_json(FIXTURES_DIR / "synthetic_evolution_cases.json")

    def _assert_no_nulls_or_nonfinite(self, obj: Any, path: str = "root") -> None:
        self.assertIsNotNone(obj, f"{path} unexpectedly NULL")
        if isinstance(obj, bool) or isinstance(obj, (int, str)):
            return
        if isinstance(obj, float):
            self.assertTrue(math.isfinite(obj), f"{path} non-finite: {obj!r}")
            return
        if isinstance(obj, list):
            for idx, item in enumerate(obj):
                self._assert_no_nulls_or_nonfinite(item, f"{path}[{idx}]")
            return
        if isinstance(obj, dict):
            for k, val in obj.items():
                self._assert_no_nulls_or_nonfinite(val, f"{path}.{k}")
            return
        self.fail(f"Unexpected type at {path}: {type(obj).__name__}")

    def test_ens_fsum_exactness_and_errors(self) -> None:
        alt_terms = [1e16, 1.0, -1e16, 1e-16, 2.5, -1e-16, -0.5]
        expected_alt = math.fsum(alt_terms)
        sql = f"""
        SELECT
            ens_fsum([]::DOUBLE[]) AS empty_sum,
            ens_fsum([-0.0, -0.0]::DOUBLE[]) AS neg_zero_sum,
            1.0 / ens_fsum([-0.0, -0.0]::DOUBLE[]) > 0 AS is_pos_zero,
            ens_fsum([1e16, 1.0, -1e16]::DOUBLE[]) AS cancel_one,
            ens_fsum([1e16, 1.0, 1.0, -1e16]::DOUBLE[]) AS cancel_two,
            ens_fsum({sql_value(alt_terms)}::DOUBLE[]) AS alt_sum;
        """
        rc, out, err = self.db.run_sql(sql)
        self.assertEqual(rc, 0, err)
        row = parse_duckdb_json(out)[-1][0]
        self.assertEqual(row["empty_sum"], 0.0)
        self.assertEqual(row["neg_zero_sum"], 0.0)
        self.assertTrue(row["is_pos_zero"], "ens_fsum([-0.0, -0.0]) must normalize -0.0 to +0.0")
        self.assertEqual(row["cancel_one"], 1.0)
        self.assertEqual(row["cancel_two"], 2.0)
        self.assertEqual(row["alt_sum"], expected_alt)

        bad_cases = [
            ("SELECT ens_fsum(NULL::DOUBLE[]);", "NULL_INPUT"),
            ("SELECT ens_fsum([1.0, NULL, 2.0]::DOUBLE[]);", "NULL_INPUT"),
            ("SELECT ens_fsum([1.0, 'nan'::DOUBLE]::DOUBLE[]);", "NON_FINITE_INPUT"),
            ("SELECT ens_fsum(['inf'::DOUBLE]::DOUBLE[]);", "NON_FINITE_INPUT"),
            ("SELECT ens_fsum(['-inf'::DOUBLE]::DOUBLE[]);", "NON_FINITE_INPUT"),
            (
                "SELECT ens_fsum([1.7976931348623157e308, 1.7976931348623157e308]::DOUBLE[]);",
                "NUMERICAL_FAILURE",
            ),
        ]
        for bad_sql, expected_code in bad_cases:
            with self.subTest(bad_sql=bad_sql):
                b_rc, _, b_err = self.db.run_sql(bad_sql)
                self.assertNotEqual(b_rc, 0)
                self.assertIn("ens_fsum", b_err)
                self.assertIn(f"[{expected_code}]", b_err)

    def test_m1_m2_udfs_preserved_unchanged(self) -> None:
        sql = """
        SELECT
            ens_m0_echo([{'member_id': 'm0', 'features': [1.0, 2.0]}]) AS echo_out,
            ens_dispersion(
                [{'member_id': 'm0', 'features': [1.0, 2.0]},
                 {'member_id': 'm1', 'features': [3.0, 4.0]}],
                [1.0, 1.0]
            ) AS disp_out,
            ens_cluster(
                [{'member_id': 'm0', 'features': [0.0, 0.0]},
                 {'member_id': 'm1', 'features': [0.2, 0.0]},
                 {'member_id': 'm2', 'features': [10.0, 10.0]},
                 {'member_id': 'm3', 'features': [10.0, 10.3]}],
                {'projection_dim': 2, 'cluster_count': 2, 'initialization': 'kmeans++_chacha8_v1',
                 'seed': 3, 'restarts': 4, 'initial_centers': []::DOUBLE[][][],
                 'stopping_rule': 'sklearn_variance_scaled_v1', 'tolerance': 1e-4,
                 'max_iterations': 100, 'rank_threshold': 1e-10}
            ) AS cluster_out,
            ens_match_scenarios(
                [{'cluster_id': 0, 'mean_features': [0.0, 0.0]},
                 {'cluster_id': 1, 'mean_features': [10.0, 10.0]}],
                [{'cluster_id': 0, 'mean_features': [0.2, 0.2]},
                 {'cluster_id': 1, 'mean_features': [10.2, 10.2]}],
                [1.0, 1.0],
                {'threshold_coefficient': 1.0, 'prev_dispersion': 2.0, 'curr_dispersion': 2.0}
            ) AS match_out;
        """
        rc, out, err = self.db.run_sql(sql)
        self.assertEqual(rc, 0, err)
        row = parse_duckdb_json(out)[-1][0]
        self.assertTrue(len(str(row["echo_out"])) > 0)
        self.assertAlmostEqual(row["disp_out"], 1.0, places=12)
        self.assertEqual(row["cluster_out"]["algorithm_version"], "m1.native.v1")
        self.assertEqual(len(row["cluster_out"]["scenarios"]), 2)
        self.assertEqual(row["match_out"]["algorithm_version"], "m2.match.v1")
        self.assertEqual(row["match_out"]["edge_count"], 2)

    def test_ens_evolve_scenarios_topologies_and_non_null_sentinels(self) -> None:
        # Transition exercising continuation, split, merge, disappearance, emergence,
        # plus a second query exercising symmetric ambiguous tie & high uncertainty.
        sql = """
        WITH inp AS (
            SELECT
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.40::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                    {'cluster_id': 1::BIGINT, 'member_count': 10::BIGINT, 'member_fraction': 0.20::DOUBLE, 'mean_features': [0.6, 0.6]::DOUBLE[]},
                    {'cluster_id': 2::BIGINT, 'member_count': 12::BIGINT, 'member_fraction': 0.24::DOUBLE, 'mean_features': [6.0, 6.0]::DOUBLE[]},
                    {'cluster_id': 3::BIGINT, 'member_count': 8::BIGINT,  'member_fraction': 0.16::DOUBLE, 'mean_features': [-20.0, -20.0]::DOUBLE[]}
                ] AS prev_s,
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [0.1, 0.1]::DOUBLE[]},
                    {'cluster_id': 1::BIGINT, 'member_count': 9::BIGINT,  'member_fraction': 0.18::DOUBLE, 'mean_features': [6.2, 6.2]::DOUBLE[]},
                    {'cluster_id': 2::BIGINT, 'member_count': 8::BIGINT,  'member_fraction': 0.16::DOUBLE, 'mean_features': [6.9, 6.9]::DOUBLE[]},
                    {'cluster_id': 3::BIGINT, 'member_count': 8::BIGINT,  'member_fraction': 0.16::DOUBLE, 'mean_features': [25.0, 25.0]::DOUBLE[]}
                ] AS curr_s,
                [1.0, 1.0]::DOUBLE[] AS w,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
        ),
        matched AS (
            SELECT
                prev_s, curr_s, w, cfg,
                ens_match_scenarios(
                    list_transform(prev_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                    list_transform(curr_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                    w,
                    {'threshold_coefficient': 1.0::DOUBLE, 'prev_dispersion': 2.0::DOUBLE, 'curr_dispersion': 2.0::DOUBLE}
                ) AS bm
            FROM inp
        )
        SELECT ens_evolve_scenarios(prev_s, curr_s, w, bm, cfg) AS evo FROM matched;

        WITH tie_inp AS (
            SELECT
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                    {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.50::DOUBLE, 'mean_features': [2.0, 2.0]::DOUBLE[]}
                ] AS prev_s,
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.40::DOUBLE, 'mean_features': [0.0, 2.0]::DOUBLE[]},
                    {'cluster_id': 1::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.40::DOUBLE, 'mean_features': [2.0, 0.0]::DOUBLE[]},
                    {'cluster_id': 2::BIGINT, 'member_count': 10::BIGINT, 'member_fraction': 0.20::DOUBLE, 'mean_features': [1.0, 1.0]::DOUBLE[]}
                ] AS curr_s,
                [1.0, 1.0]::DOUBLE[] AS w,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
        )
        SELECT ens_evolve_scenarios(
            prev_s,
            curr_s,
            w,
            ens_match_scenarios(
                list_transform(prev_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                list_transform(curr_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                w,
                {'threshold_coefficient': 1.0::DOUBLE, 'prev_dispersion': 3.0::DOUBLE, 'curr_dispersion': 3.0::DOUBLE}
            ),
            cfg
        ) AS evo_tie
        FROM tie_inp;
        """
        rc, out, err = self.db.run_sql(sql)
        self.assertEqual(rc, 0, err)
        res_all, res_tie = parse_duckdb_json(out)
        evo = res_all[0]["evo"]
        evo_tie = res_tie[0]["evo_tie"]

        self._assert_no_nulls_or_nonfinite(evo, "evo")
        self._assert_no_nulls_or_nonfinite(evo_tie, "evo_tie")

        self.assertEqual(evo["algorithm_version"], EVOLUTION_ALGORITHM_VERSION)
        self.assertEqual(evo["non_physical_disclaimer"], NON_PHYSICAL_DISCLAIMER)
        self.assertTrue(evo["has_reference_scale"])
        self.assertAlmostEqual(evo["reference_scale"], 2.0, places=12)

        ev_types = {ev["event_type"] for ev in evo["events"]}
        self.assertEqual(ev_types, {"merge", "split", "disappearance", "emergence"})

        # Check sentinel invariants on disappeared/emerged nodes (no primary counterpart -> -1 & 0.0)
        disappeared_nodes = [n for n in evo["prev_nodes"] if n["node_role"] == "disappeared"]
        self.assertEqual(len(disappeared_nodes), 1)
        self.assertFalse(disappeared_nodes[0]["has_primary_counterpart"])
        self.assertEqual(disappeared_nodes[0]["primary_counterpart_cluster_id"], -1)
        self.assertEqual(disappeared_nodes[0]["primary_delta_fraction"], 0.0)
        self.assertTrue(disappeared_nodes[0]["has_unmatched_reason"])
        self.assertEqual(disappeared_nodes[0]["unmatched_reason"], "no_candidate_within_threshold")

        # Check ambiguous tie & canonical tiebreak
        self.assertTrue(evo_tie["summary"]["ambiguous_tie"])
        self.assertTrue(evo_tie["summary"]["high_uncertainty"])
        sec_edges = [e for e in evo_tie["edges"] if e["edge_role"] == "secondary_split"]
        self.assertEqual(len(sec_edges), 1)
        self.assertEqual(sec_edges[0]["rule_basis"], "nearest_feasible_split_canonical_tiebreak")
        self.assertTrue(sec_edges[0]["ambiguous_tie"])
        self.assertIn("ambiguous_tie", sec_edges[0]["uncertainty_reasons"])

    def test_ens_evolve_scenarios_null_and_input_validation_errors(self) -> None:
        base_cte = """
        WITH base AS (
            SELECT
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.0, 4.0]::DOUBLE[]}] AS p,
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.2, 4.2]::DOUBLE[]}] AS c,
                [1.0, 1.0]::DOUBLE[] AS w,
                ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [4.0, 4.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}, {'cluster_id': 1, 'mean_features': [4.2, 4.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS bm,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
        )
        """
        cases = [
            ("SELECT ens_evolve_scenarios(NULL, c, w, bm, cfg) FROM base;", "NULL_INPUT"),
            ("SELECT ens_evolve_scenarios(p, NULL, w, bm, cfg) FROM base;", "NULL_INPUT"),
            ("SELECT ens_evolve_scenarios(p, c, NULL, bm, cfg) FROM base;", "NULL_INPUT"),
            ("SELECT ens_evolve_scenarios(p, c, w, NULL, cfg) FROM base;", "NULL_INPUT"),
            ("SELECT ens_evolve_scenarios(p, c, w, bm, NULL) FROM base;", "NULL_INPUT"),
            (
                "SELECT ens_evolve_scenarios([NULL, p[2]], c, w, bm, cfg) FROM base;",
                "NULL_INPUT",
            ),
            (
                "SELECT ens_evolve_scenarios([{'cluster_id': NULL::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}, p[2]], c, w, bm, cfg) FROM base;",
                "NULL_INPUT",
            ),
            (
                "SELECT ens_evolve_scenarios([{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, NULL]::DOUBLE[]}, p[2]], c, w, bm, cfg) FROM base;",
                "NULL_INPUT",
            ),
            (
                "SELECT ens_evolve_scenarios(p, c, [1.0, NULL]::DOUBLE[], bm, cfg) FROM base;",
                "NULL_INPUT",
            ),
            (
                "SELECT ens_evolve_scenarios(p, c, w, struct_pack(algorithm_version := bm.algorithm_version, prev_cluster_ids := bm.prev_cluster_ids, curr_cluster_ids := bm.curr_cluster_ids, prev_dispersion := bm.prev_dispersion, curr_dispersion := bm.curr_dispersion, pair_scale := bm.pair_scale, threshold_coefficient := bm.threshold_coefficient, threshold := NULL::DOUBLE, distance_matrix := bm.distance_matrix, distance_ratio_matrix := bm.distance_ratio_matrix, eligible_matrix := bm.eligible_matrix, edge_count := bm.edge_count, total_distance := bm.total_distance, optimal_solution_count := bm.optimal_solution_count, ambiguous := bm.ambiguous, threshold_margin := bm.threshold_margin, optimality_margin := bm.optimality_margin, has_optimality_margin := bm.has_optimality_margin, links := bm.links, unmatched_prev := bm.unmatched_prev, unmatched_curr := bm.unmatched_curr), cfg) FROM base;",
                "NULL_INPUT",
            ),
            (
                "SELECT ens_evolve_scenarios(p, c, w, bm, {'secondary_rule': NULL::VARCHAR, 'flow_rule': 'star_min_mass_proportional_v1', 'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1, 'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}) FROM base;",
                "NULL_INPUT",
            ),
            (
                "SELECT ens_evolve_scenarios([{'cluster_id': -1::BIGINT, 'member_count': 50::BIGINT, 'member_fraction': 1.0::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}], c, w, bm, cfg) FROM base;",
                "INVALID_SCENARIO_ID",
            ),
            (
                "SELECT ens_evolve_scenarios([{'cluster_id': 0::BIGINT, 'member_count': 0::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}, p[2]], c, w, bm, cfg) FROM base;",
                "ZERO_MEMBER_SCENARIO",
            ),
            (
                "SELECT ens_evolve_scenarios([{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.0::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}, p[2]], c, w, bm, cfg) FROM base;",
                "INVALID_MEMBER_FRACTION",
            ),
            (
                "SELECT ens_evolve_scenarios([{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.55::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}, p[2]], c, w, bm, cfg) FROM base;",
                "INVALID_FRACTION_SUM",
            ),
            (
                "SELECT ens_evolve_scenarios(p, c, w, bm, {'secondary_rule': 'bad_rule', 'flow_rule': 'star_min_mass_proportional_v1', 'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1, 'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}) FROM base;",
                "INVALID_EVOLUTION_CONFIG",
            ),
        ]
        for query_tail, expected_code in cases:
            with self.subTest(expected_code=expected_code, query_tail=query_tail[:70]):
                rc, _, err = self.db.run_sql(base_cte + query_tail)
                self.assertNotEqual(rc, 0)
                self.assertIn("ens_evolve_scenarios", err)
                self.assertIn(f"[{expected_code}]", err)

    def test_ens_evolve_scenarios_tampered_backbone_rejection(self) -> None:
        base_cte = """
        WITH base AS (
            SELECT
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.0, 4.0]::DOUBLE[]}] AS p,
                [{'cluster_id': 0::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 25::BIGINT, 'member_fraction': 0.5::DOUBLE, 'mean_features': [4.2, 4.2]::DOUBLE[]}] AS c,
                [1.0, 1.0]::DOUBLE[] AS w,
                ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [4.0, 4.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}, {'cluster_id': 1, 'mean_features': [4.2, 4.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS bm,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
        )
        """
        tampers = [
            # 1. Tampered threshold
            ("threshold := 9.99::DOUBLE", "BACKBONE_INCONSISTENCY"),
            # 2. Tampered distance_matrix
            ("distance_matrix := [[0.5, 4.2], [4.0, 0.2]]::DOUBLE[][]", "BACKBONE_INCONSISTENCY"),
            # 3. Tampered eligible_matrix
            ("eligible_matrix := [[false, false], [false, true]]::BOOLEAN[][]", "BACKBONE_INCONSISTENCY"),
            # 4. Dropped links (suboptimal cardinality 0 < 2)
            ("links := bm.links[1:0]", "BACKBONE_INCONSISTENCY"),
            # 5. Tampered optimal_solution_count
            ("optimal_solution_count := 99::BIGINT", "BACKBONE_INCONSISTENCY"),
            # 6. Unknown cluster_id in primary link
            (
                "links := [{'prev_cluster_id': 0::BIGINT, 'curr_cluster_id': 99::BIGINT, 'distance': 0.2::DOUBLE, 'distance_ratio': 0.2::DOUBLE, 'alternative_status': 'count_drop'::VARCHAR, 'alternative_edge_count': 1::BIGINT, 'alternative_total_distance': 0.2::DOUBLE, 'alternative_distance_increase': 0.0::DOUBLE, 'alternative_distance_increase_ratio': 0.0::DOUBLE, 'has_same_count_alternative': false, 'ambiguous': false}]",
                "INVALID_PRIMARY_LINK",
            ),
            # 7. Over-threshold primary link (0 -> 1 has distance 4.2 > threshold 1.0)
            (
                "links := [{'prev_cluster_id': 0::BIGINT, 'curr_cluster_id': 1::BIGINT, 'distance': 4.2::DOUBLE, 'distance_ratio': 4.2::DOUBLE, 'alternative_status': 'count_drop'::VARCHAR, 'alternative_edge_count': 1::BIGINT, 'alternative_total_distance': 0.2::DOUBLE, 'alternative_distance_increase': 0.0::DOUBLE, 'alternative_distance_increase_ratio': 0.0::DOUBLE, 'has_same_count_alternative': false, 'ambiguous': false}]",
                "INVALID_PRIMARY_LINK",
            ),
            # 8. Tampered distance_ratio_matrix ([H1])
            (
                "distance_ratio_matrix := [[99.0, 4.2], [4.0, 0.2]]::DOUBLE[][]",
                "BACKBONE_INCONSISTENCY",
            ),
            # 9. Tampered top-level backbone_match.ambiguous ([H1])
            (
                "ambiguous := NOT bm.ambiguous",
                "BACKBONE_INCONSISTENCY",
            ),
            # 10. Tampered links[0].ambiguous ([H1])
            (
                "links := [struct_pack(prev_cluster_id := bm.links[1].prev_cluster_id, curr_cluster_id := bm.links[1].curr_cluster_id, distance := bm.links[1].distance, distance_ratio := bm.links[1].distance_ratio, alternative_status := bm.links[1].alternative_status, alternative_edge_count := bm.links[1].alternative_edge_count, alternative_total_distance := bm.links[1].alternative_total_distance, alternative_distance_increase := bm.links[1].alternative_distance_increase, alternative_distance_increase_ratio := bm.links[1].alternative_distance_increase_ratio, has_same_count_alternative := bm.links[1].has_same_count_alternative, ambiguous := NOT bm.links[1].ambiguous), bm.links[2]]",
                "BACKBONE_INCONSISTENCY",
            ),
            # 11. Reversed links order (non-sorted prev_cluster_id order) ([H1])
            (
                "links := [bm.links[2], bm.links[1]]",
                "BACKBONE_INCONSISTENCY",
            ),
        ]

        all_fields = [
            "algorithm_version := bm.algorithm_version",
            "prev_cluster_ids := bm.prev_cluster_ids",
            "curr_cluster_ids := bm.curr_cluster_ids",
            "prev_dispersion := bm.prev_dispersion",
            "curr_dispersion := bm.curr_dispersion",
            "pair_scale := bm.pair_scale",
            "threshold_coefficient := bm.threshold_coefficient",
            "threshold := bm.threshold",
            "distance_matrix := bm.distance_matrix",
            "distance_ratio_matrix := bm.distance_ratio_matrix",
            "eligible_matrix := bm.eligible_matrix",
            "edge_count := bm.edge_count",
            "total_distance := bm.total_distance",
            "optimal_solution_count := bm.optimal_solution_count",
            "ambiguous := bm.ambiguous",
            "threshold_margin := bm.threshold_margin",
            "optimality_margin := bm.optimality_margin",
            "has_optimality_margin := bm.has_optimality_margin",
            "links := bm.links",
            "unmatched_prev := bm.unmatched_prev",
            "unmatched_curr := bm.unmatched_curr",
        ]
        for override_expr, expected_code in tampers:
            field_name = override_expr.split(":=")[0].strip()
            packed = ", ".join(
                override_expr if f.startswith(f"{field_name} :=") else f for f in all_fields
            )
            sql = f"{base_cte} SELECT ens_evolve_scenarios(p, c, w, struct_pack({packed}), cfg) FROM base;"
            with self.subTest(field_name=field_name, expected_code=expected_code):
                rc, _, err = self.db.run_sql(sql)
                self.assertNotEqual(rc, 0)
                self.assertIn(f"[{expected_code}]", err)

        # 12. Tampered unmatched_prev[0].min_candidate_distance ([H1])
        unm_sql = """
        WITH base_unm AS (
            SELECT
                [{'cluster_id': 0::BIGINT, 'member_count': 30::BIGINT, 'member_fraction': 0.6::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]},
                 {'cluster_id': 1::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE, 'mean_features': [10.0, 10.0]::DOUBLE[]}] AS p,
                [{'cluster_id': 0::BIGINT, 'member_count': 50::BIGINT, 'member_fraction': 1.0::DOUBLE, 'mean_features': [0.2, 0.2]::DOUBLE[]}] AS c,
                [1.0, 1.0]::DOUBLE[] AS w,
                ens_match_scenarios(
                    [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}, {'cluster_id': 1, 'mean_features': [10.0, 10.0]}],
                    [{'cluster_id': 0, 'mean_features': [0.2, 0.2]}],
                    [1.0, 1.0],
                    {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
                ) AS bm,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
        )
        SELECT ens_evolve_scenarios(
            p, c, w,
            struct_pack(
                algorithm_version := bm.algorithm_version,
                prev_cluster_ids := bm.prev_cluster_ids,
                curr_cluster_ids := bm.curr_cluster_ids,
                prev_dispersion := bm.prev_dispersion,
                curr_dispersion := bm.curr_dispersion,
                pair_scale := bm.pair_scale,
                threshold_coefficient := bm.threshold_coefficient,
                threshold := bm.threshold,
                distance_matrix := bm.distance_matrix,
                distance_ratio_matrix := bm.distance_ratio_matrix,
                eligible_matrix := bm.eligible_matrix,
                edge_count := bm.edge_count,
                total_distance := bm.total_distance,
                optimal_solution_count := bm.optimal_solution_count,
                ambiguous := bm.ambiguous,
                threshold_margin := bm.threshold_margin,
                optimality_margin := bm.optimality_margin,
                has_optimality_margin := bm.has_optimality_margin,
                links := bm.links,
                unmatched_prev := [struct_pack(
                    cluster_id := bm.unmatched_prev[1].cluster_id,
                    reason := bm.unmatched_prev[1].reason,
                    min_candidate_distance := bm.unmatched_prev[1].min_candidate_distance + 1.0,
                    matched_in_some_optimum := bm.unmatched_prev[1].matched_in_some_optimum
                )],
                unmatched_curr := bm.unmatched_curr
            ),
            cfg
        ) FROM base_unm;
        """
        rc_unm, _, err_unm = self.db.run_sql(unm_sql)
        self.assertNotEqual(rc_unm, 0)
        self.assertIn("[BACKBONE_INCONSISTENCY]", err_unm)

    def test_large_chunk_boundary_2100_rows_and_late_bad_row_abort(self) -> None:
        # 1. Valid 2100-row chunk (> 2048 vector size) for both ens_fsum and ens_evolve_scenarios
        sql_ok = """
        WITH rows_2100 AS (
            SELECT
                r,
                [1e16::DOUBLE, (r + 1)::DOUBLE, -1e16::DOUBLE] AS terms,
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 30::BIGINT, 'member_fraction': 0.6::DOUBLE,
                     'mean_features': [0.0::DOUBLE, (r * 0.001)::DOUBLE]},
                    {'cluster_id': 1::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                     'mean_features': [5.0::DOUBLE, (5.0 + r * 0.001)::DOUBLE]}
                ] AS prev_s,
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                     'mean_features': [0.2::DOUBLE, (0.2 + r * 0.001)::DOUBLE]},
                    {'cluster_id': 1::BIGINT, 'member_count': 10::BIGINT, 'member_fraction': 0.2::DOUBLE,
                     'mean_features': [0.9::DOUBLE, (0.9 + r * 0.001)::DOUBLE]},
                    {'cluster_id': 2::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                     'mean_features': [5.2::DOUBLE, (5.2 + r * 0.001)::DOUBLE]}
                ] AS curr_s,
                [1.0, 1.0]::DOUBLE[] AS w,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
            FROM range(2100) AS t(r)
        ),
        evaluated AS (
            SELECT
                r,
                ens_fsum(terms) AS exact_sum,
                ens_evolve_scenarios(
                    prev_s,
                    curr_s,
                    w,
                    ens_match_scenarios(
                        list_transform(prev_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                        list_transform(curr_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                        w,
                        {'threshold_coefficient': 1.0::DOUBLE, 'prev_dispersion': 2.0::DOUBLE, 'curr_dispersion': 2.0::DOUBLE}
                    ),
                    cfg
                ) AS evo
            FROM rows_2100
        )
        SELECT
            count(*) AS total_rows,
            bool_and(exact_sum = (r + 1)::DOUBLE) AS all_fsum_exact,
            bool_and(evo.summary.primary_edge_count = 2 AND evo.summary.secondary_split_edge_count = 1) AS all_evo_ok,
            min(evo.summary.transition_tv_redistribution) AS min_tv,
            max(evo.summary.transition_tv_redistribution) AS max_tv
        FROM evaluated;
        """
        rc, out, err = self.db.run_sql(sql_ok, timeout=120.0)
        self.assertEqual(rc, 0, err)
        row = parse_duckdb_json(out)[-1][0]
        self.assertEqual(row["total_rows"], 2100)
        self.assertTrue(row["all_fsum_exact"])
        self.assertTrue(row["all_evo_ok"])
        self.assertAlmostEqual(row["min_tv"], 0.2, places=12)
        self.assertAlmostEqual(row["max_tv"], 0.2, places=12)

        # 2. Bad row at index 2075 (in the second chunk > 2048) must fail the entire query
        sql_bad_late = """
        CREATE TABLE should_not_commit AS
        WITH rows_2100 AS (
            SELECT
                r,
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 30::BIGINT,
                     'member_fraction': CASE WHEN r = 2075 THEN 0.95::DOUBLE ELSE 0.6::DOUBLE END,
                     'mean_features': [0.0::DOUBLE, (r * 0.001)::DOUBLE]},
                    {'cluster_id': 1::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                     'mean_features': [5.0::DOUBLE, (5.0 + r * 0.001)::DOUBLE]}
                ] AS prev_s,
                [
                    {'cluster_id': 0::BIGINT, 'member_count': 30::BIGINT, 'member_fraction': 0.6::DOUBLE,
                     'mean_features': [0.2::DOUBLE, (0.2 + r * 0.001)::DOUBLE]},
                    {'cluster_id': 1::BIGINT, 'member_count': 20::BIGINT, 'member_fraction': 0.4::DOUBLE,
                     'mean_features': [5.2::DOUBLE, (5.2 + r * 0.001)::DOUBLE]}
                ] AS curr_s,
                [1.0, 1.0]::DOUBLE[] AS w,
                {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
                 'fraction_sum_tolerance': 1e-9::DOUBLE, 'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
                 'high_uncertainty_optimality_ratio': 0.10::DOUBLE, 'high_uncertainty_secondary_ratio': 0.10::DOUBLE} AS cfg
            FROM range(2100) AS t(r)
        )
        SELECT ens_evolve_scenarios(
            prev_s,
            curr_s,
            w,
            ens_match_scenarios(
                list_transform(prev_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                list_transform(curr_s, lambda s: {'cluster_id': s.cluster_id, 'mean_features': s.mean_features}),
                w,
                {'threshold_coefficient': 1.0::DOUBLE, 'prev_dispersion': 2.0::DOUBLE, 'curr_dispersion': 2.0::DOUBLE}
            ),
            cfg
        ) AS evo
        FROM rows_2100;
        """
        b_rc, _, b_err = self.db.run_sql(sql_bad_late, timeout=120.0)
        self.assertNotEqual(b_rc, 0)
        self.assertIn("[INVALID_FRACTION_SUM]", b_err)
        self.assertIn("chunk_row=", b_err)

    def test_same_session_valid_invalid_valid_recovery(self) -> None:
        script = """
        SELECT ens_fsum([1e16, 3.0, -1e16]::DOUBLE[]) AS f_before;
        SELECT ens_evolve_scenarios(
            [{'cluster_id': 0::BIGINT, 'member_count': 50::BIGINT, 'member_fraction': 1.0::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}],
            [{'cluster_id': 0::BIGINT, 'member_count': 50::BIGINT, 'member_fraction': 1.0::DOUBLE, 'mean_features': [0.5, 0.5]::DOUBLE[]}],
            [1.0, 1.0]::DOUBLE[],
            ens_match_scenarios(
                [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}],
                [{'cluster_id': 0, 'mean_features': [0.5, 0.5]}],
                [1.0, 1.0],
                {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
            ),
            {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
             'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1,
             'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}
        ).summary.continuation_count AS c_before;

        SELECT ens_fsum([1.0, NULL]::DOUBLE[]);
        SELECT ens_evolve_scenarios(NULL, NULL, NULL, NULL, NULL);

        SELECT ens_fsum([1e16, 7.0, -1e16]::DOUBLE[]) AS f_after;
        SELECT ens_evolve_scenarios(
            [{'cluster_id': 0::BIGINT, 'member_count': 50::BIGINT, 'member_fraction': 1.0::DOUBLE, 'mean_features': [0.0, 0.0]::DOUBLE[]}],
            [{'cluster_id': 0::BIGINT, 'member_count': 50::BIGINT, 'member_fraction': 1.0::DOUBLE, 'mean_features': [0.5, 0.5]::DOUBLE[]}],
            [1.0, 1.0]::DOUBLE[],
            ens_match_scenarios(
                [{'cluster_id': 0, 'mean_features': [0.0, 0.0]}],
                [{'cluster_id': 0, 'mean_features': [0.5, 0.5]}],
                [1.0, 1.0],
                {'threshold_coefficient': 1.0, 'prev_dispersion': 1.0, 'curr_dispersion': 1.0}
            ),
            {'secondary_rule': 'nearest_feasible_v1', 'flow_rule': 'star_min_mass_proportional_v1',
             'fraction_sum_tolerance': 1e-9, 'high_uncertainty_threshold_ratio': 0.1,
             'high_uncertainty_optimality_ratio': 0.1, 'high_uncertainty_secondary_ratio': 0.1}
        ).summary.continuation_count AS c_after;
        """
        _, out, err = self.db.run_sql(script, bail=False)
        self.assertIn("ens_fsum [NULL_INPUT]", err)
        self.assertIn("ens_evolve_scenarios [NULL_INPUT]", err)
        results = parse_duckdb_json(out)
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0][0]["f_before"], 3.0)
        self.assertEqual(results[1][0]["c_before"], 1)
        self.assertEqual(results[2][0]["f_after"], 7.0)
        self.assertEqual(results[3][0]["c_after"], 1)

    def test_stage_and_publish_whitelisted_vs_non_whitelisted_batch_failures(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ensabc-m3-t024-raw-") as tmp:
            db_path = Path(tmp) / "raw_fault.duckdb"
            rk = "rk_raw_whitelist_001"
            erid = "erid_raw_whitelist_001"
            exec_b, evol_b = _synthetic_bindings(rk, erid, cluster_count=2)

            # 5-batch raw-member chain where:
            # - b0, b1, b4 are valid 2-cluster batches
            # - b2 has constant identical features -> ZERO_DISPERSION (whitelisted -> status='failed')
            # - b3 is 'missing' -> status='missing'
            ok_members = (
                "[{'member_id': 'm0', 'features': [0.0, 0.0]}, "
                "{'member_id': 'm1', 'features': [0.2, 0.1]}, "
                "{'member_id': 'm2', 'features': [6.0, 6.0]}, "
                "{'member_id': 'm3', 'features': [6.1, 6.2]}]"
            )
            zero_disp_members = (
                "[{'member_id': 'm0', 'features': [3.0, 3.0]}, "
                "{'member_id': 'm1', 'features': [3.0, 3.0]}, "
                "{'member_id': 'm2', 'features': [3.0, 3.0]}, "
                "{'member_id': 'm3', 'features': [3.0, 3.0]}]"
            )
            sql_ok = f"""
            SET VARIABLE run_key = {sql_value(rk)};
            SET VARIABLE evolution_run_id = {sql_value(erid)};
            SET VARIABLE planned_batch_count = 5;
            SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
            SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
            SET VARIABLE execution_binding = {sql_value(exec_b)};
            SET VARIABLE evolution_binding = {sql_value(evol_b)};
            SET VARIABLE cluster_config = {{
                'projection_dim': 2::BIGINT, 'cluster_count': 2::BIGINT,
                'initialization': 'kmeans++_chacha8_v1'::VARCHAR, 'seed': 3::BIGINT,
                'restarts': 4::BIGINT, 'initial_centers': []::DOUBLE[][][],
                'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
                'tolerance': 1e-4::DOUBLE, 'max_iterations': 100::BIGINT,
                'rank_threshold': 1e-10::DOUBLE
            }};

            CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
            SELECT * FROM (VALUES
                (0, TIMESTAMP '2020-03-01 00:00:00', 60, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_members}),
                (1, TIMESTAMP '2020-03-01 12:00:00', 48, 'present', NULL::VARCHAR, 'i1', 'o1', {ok_members}),
                (2, TIMESTAMP '2020-03-02 00:00:00', 36, 'present', NULL::VARCHAR, 'i2', 'o2', {zero_disp_members}),
                (3, TIMESTAMP '2020-03-02 12:00:00', 24, 'missing', 'SOURCE_BATCH_MISSING', NULL::VARCHAR, NULL::VARCHAR, NULL),
                (4, TIMESTAMP '2020-03-03 00:00:00', 12, 'present', NULL::VARCHAR, 'i4', 'o4', {ok_members})
            ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);

            .read validation/m3/sql/stage_and_publish.sql

            SELECT ordinal, status, status_reason FROM replay_batches WHERE run_key = {sql_value(rk)} ORDER BY ordinal;
            SELECT prev_ordinal, transition_status, gap_reason FROM evolution_transitions WHERE evolution_run_id = {sql_value(erid)} ORDER BY prev_ordinal;
            SELECT prev_ordinal, event_type FROM evolution_events WHERE evolution_run_id = {sql_value(erid)} ORDER BY prev_ordinal, event_index;
            """
            rc, out, err = self.db.run_sql(sql_ok, db_file=db_path)
            self.assertEqual(rc, 0, err)
            parsed = parse_duckdb_json(out)
            batches, transitions, events = parsed[-3], parsed[-2], parsed[-1]

            self.assertEqual([b["status"] for b in batches], ["ok", "ok", "failed", "missing", "ok"])
            self.assertEqual(batches[2]["status_reason"], "ZERO_DISPERSION")
            self.assertEqual(
                [(t["prev_ordinal"], t["transition_status"], t["gap_reason"]) for t in transitions],
                [
                    (0, "ok", None),
                    (1, "gap", "curr_unavailable"),
                    (2, "gap", "both_unavailable"),
                    (3, "gap", "prev_unavailable"),
                ],
            )
            # b2->b3 is both_unavailable -> 0 events; b1->b2 has only gap_interrupted; b3->b4 has only gap_restarted
            ev_by_ord: dict[int, list[str]] = {}
            for ev in events:
                ev_by_ord.setdefault(ev["prev_ordinal"], []).append(ev["event_type"])
            self.assertEqual(ev_by_ord.get(1), ["gap_interrupted", "gap_interrupted"])
            self.assertIsNone(ev_by_ord.get(2))
            self.assertEqual(ev_by_ord.get(3), ["gap_restarted", "gap_restarted"])

            # Non-whitelisted errors (bad dimension, non-finite feature, non-whitelisted failed code)
            # must abort BEFORE Stage B and leave 0 rows for their keys in the persistent DB!
            bad_rk = "rk_raw_non_whitelisted_001"
            bad_erid = "erid_raw_non_whitelisted_001"
            bad_exec, bad_evol = _synthetic_bindings(bad_rk, bad_erid, cluster_count=2)
            bad_dim_members = "[{'member_id': 'm0', 'features': [1.0, 2.0, 3.0]}, {'member_id': 'm1', 'features': [4.0, 5.0, 6.0]}]"
            sql_bad_dim = f"""
            SET VARIABLE run_key = {sql_value(bad_rk)};
            SET VARIABLE evolution_run_id = {sql_value(bad_erid)};
            SET VARIABLE planned_batch_count = 4;
            SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
            SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
            SET VARIABLE execution_binding = {sql_value(bad_exec)};
            SET VARIABLE evolution_binding = {sql_value(bad_evol)};
            SET VARIABLE cluster_config = {{
                'projection_dim': 2::BIGINT, 'cluster_count': 2::BIGINT,
                'initialization': 'kmeans++_chacha8_v1'::VARCHAR, 'seed': 3::BIGINT,
                'restarts': 2::BIGINT, 'initial_centers': []::DOUBLE[][][],
                'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
                'tolerance': 1e-4::DOUBLE, 'max_iterations': 50::BIGINT,
                'rank_threshold': 1e-10::DOUBLE
            }};
            CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
            SELECT * FROM (VALUES
                (0, TIMESTAMP '2020-03-01 00:00:00', 48, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_members}),
                (1, TIMESTAMP '2020-03-01 12:00:00', 36, 'present', NULL::VARCHAR, 'i1', 'o1', {bad_dim_members}),
                (2, TIMESTAMP '2020-03-02 00:00:00', 24, 'present', NULL::VARCHAR, 'i2', 'o2', {ok_members}),
                (3, TIMESTAMP '2020-03-02 12:00:00', 12, 'present', NULL::VARCHAR, 'i3', 'o3', {ok_members})
            ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);
            .read validation/m3/sql/stage_and_publish.sql
            """
            rc_bad, _, err_bad = self.db.run_sql(sql_bad_dim, db_file=db_path)
            self.assertNotEqual(rc_bad, 0)
            self.assertIn("DIMENSION_MISMATCH", err_bad)

            rc_chk, out_chk, err_chk = self.db.run_sql(
                f"""
                SELECT
                    (SELECT count(*) FROM tracking_runs WHERE run_key = {sql_value(bad_rk)}) AS m2_cnt,
                    (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {sql_value(bad_erid)}) AS m3_cnt;
                """,
                db_file=db_path,
            )
            self.assertEqual(rc_chk, 0, err_chk)
            chk = parse_duckdb_json(out_chk)[-1][0]
            self.assertEqual(chk, {"m2_cnt": 0, "m3_cnt": 0})

            # [H3] Exact CoreErrorCode recording via ens_diagnose_batch:
            # 1) Rank-deficient collinear members with rank_threshold=1e-10 & max_iterations=300 -> exact 'INSUFFICIENT_RANK'
            #    (previously misclassified as 'EMPTY_CLUSTER' under SQL CASE heuristic!)
            rk_ir = "rk_raw_insuff_rank_exact"
            erid_ir = "erid_raw_insuff_rank_exact"
            exec_ir, evol_ir = _synthetic_bindings(rk_ir, erid_ir, cluster_count=2)
            collinear_members = (
                "[{'member_id': 'm0', 'features': [0.0, 0.0]}, "
                "{'member_id': 'm1', 'features': [1.0, 1.0]}, "
                "{'member_id': 'm2', 'features': [2.0, 2.0]}, "
                "{'member_id': 'm3', 'features': [3.0, 3.0]}]"
            )
            sql_ir = f"""
            SET VARIABLE run_key = {sql_value(rk_ir)};
            SET VARIABLE evolution_run_id = {sql_value(erid_ir)};
            SET VARIABLE planned_batch_count = 4;
            SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
            SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
            SET VARIABLE execution_binding = {sql_value(exec_ir)};
            SET VARIABLE evolution_binding = {sql_value(evol_ir)};
            SET VARIABLE cluster_config = {{
                'projection_dim': 2::BIGINT, 'cluster_count': 2::BIGINT,
                'initialization': 'kmeans++_chacha8_v1'::VARCHAR, 'seed': 3::BIGINT,
                'restarts': 10::BIGINT, 'initial_centers': []::DOUBLE[][][],
                'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
                'tolerance': 1e-4::DOUBLE, 'max_iterations': 300::BIGINT,
                'rank_threshold': 1e-10::DOUBLE
            }};
            CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
            SELECT * FROM (VALUES
                (0, TIMESTAMP '2020-03-01 00:00:00', 48, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_members}),
                (1, TIMESTAMP '2020-03-01 12:00:00', 36, 'present', NULL::VARCHAR, 'i1', 'o1', {collinear_members}),
                (2, TIMESTAMP '2020-03-02 00:00:00', 24, 'present', NULL::VARCHAR, 'i2', 'o2', {ok_members}),
                (3, TIMESTAMP '2020-03-02 12:00:00', 12, 'present', NULL::VARCHAR, 'i3', 'o3', {ok_members})
            ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);
            .read validation/m3/sql/stage_and_publish.sql
            SELECT ordinal, status, status_reason FROM replay_batches WHERE run_key = {sql_value(rk_ir)} ORDER BY ordinal;
            """
            rc_ir, out_ir, err_ir = self.db.run_sql(sql_ir, db_file=db_path)
            self.assertEqual(rc_ir, 0, err_ir)
            ir_batches = parse_duckdb_json(out_ir)[-1]
            self.assertEqual([b["status"] for b in ir_batches], ["ok", "failed", "ok", "ok"])
            self.assertEqual(ir_batches[1]["status_reason"], "INSUFFICIENT_RANK")

            # 2) Direct ens_diagnose_batch + stage_and_publish exact 'AMBIGUOUS_SUBSPACE' and 'NOT_CONVERGED'
            rk_as = "rk_raw_ambig_subspace_exact"
            erid_as = "erid_raw_ambig_subspace_exact"
            exec_as, evol_as = _synthetic_bindings(rk_as, erid_as, cluster_count=2)
            ok_1d_proj_members = (
                "[{'member_id': 'm0', 'features': [0.0, 0.0]}, "
                "{'member_id': 'm1', 'features': [0.2, 0.0]}, "
                "{'member_id': 'm2', 'features': [6.0, 0.0]}, "
                "{'member_id': 'm3', 'features': [6.2, 0.0]}]"
            )
            ambig_cross_members = (
                "[{'member_id': 'm0', 'features': [1.0, 0.0]}, "
                "{'member_id': 'm1', 'features': [-1.0, 0.0]}, "
                "{'member_id': 'm2', 'features': [0.0, 1.0]}, "
                "{'member_id': 'm3', 'features': [0.0, -1.0]}]"
            )
            sql_as = f"""
            SET VARIABLE run_key = {sql_value(rk_as)};
            SET VARIABLE evolution_run_id = {sql_value(erid_as)};
            SET VARIABLE planned_batch_count = 4;
            SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
            SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
            SET VARIABLE execution_binding = {sql_value(exec_as)};
            SET VARIABLE evolution_binding = {sql_value(evol_as)};
            SET VARIABLE cluster_config = {{
                'projection_dim': 1::BIGINT, 'cluster_count': 2::BIGINT,
                'initialization': 'kmeans++_chacha8_v1'::VARCHAR, 'seed': 3::BIGINT,
                'restarts': 10::BIGINT, 'initial_centers': []::DOUBLE[][][],
                'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
                'tolerance': 1e-4::DOUBLE, 'max_iterations': 300::BIGINT,
                'rank_threshold': 1e-10::DOUBLE
            }};
            CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
            SELECT * FROM (VALUES
                (0, TIMESTAMP '2020-03-01 00:00:00', 48, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_1d_proj_members}),
                (1, TIMESTAMP '2020-03-01 12:00:00', 36, 'present', NULL::VARCHAR, 'i1', 'o1', {ambig_cross_members}),
                (2, TIMESTAMP '2020-03-02 00:00:00', 24, 'present', NULL::VARCHAR, 'i2', 'o2', {ok_1d_proj_members}),
                (3, TIMESTAMP '2020-03-02 12:00:00', 12, 'present', NULL::VARCHAR, 'i3', 'o3', {ok_1d_proj_members})
            ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);
            .read validation/m3/sql/stage_and_publish.sql
            SELECT ordinal, status, status_reason FROM replay_batches WHERE run_key = {sql_value(rk_as)} ORDER BY ordinal;
            """
            rc_as, out_as, err_as = self.db.run_sql(sql_as, db_file=db_path)
            self.assertEqual(rc_as, 0, err_as)
            as_batches = parse_duckdb_json(out_as)[-1]
            self.assertEqual(as_batches[1]["status"], "failed")
            self.assertEqual(as_batches[1]["status_reason"], "AMBIGUOUS_SUBSPACE")

            rk_nc = "rk_raw_not_converged_exact"
            erid_nc = "erid_raw_not_converged_exact"
            exec_nc, evol_nc = _synthetic_bindings(rk_nc, erid_nc, cluster_count=2)
            not_conv_members = (
                "[{'member_id': 'm0', 'features': [0.0, 0.0]}, "
                "{'member_id': 'm1', 'features': [1.0, 0.0]}, "
                "{'member_id': 'm2', 'features': [2.0, 0.0]}, "
                "{'member_id': 'm3', 'features': [4.0, 0.0]}, "
                "{'member_id': 'm4', 'features': [8.0, 0.1]}, "
                "{'member_id': 'm5', 'features': [16.0, 0.2]}]"
            )
            ok_conv_members = (
                "[{'member_id': 'm0', 'features': [0.0, 0.0]}, "
                "{'member_id': 'm1', 'features': [0.0, 0.1]}, "
                "{'member_id': 'm2', 'features': [10.0, 0.0]}, "
                "{'member_id': 'm3', 'features': [10.0, 0.1]}]"
            )
            sql_nc = f"""
            SET VARIABLE run_key = {sql_value(rk_nc)};
            SET VARIABLE evolution_run_id = {sql_value(erid_nc)};
            SET VARIABLE planned_batch_count = 4;
            SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
            SET VARIABLE weights = [1.0, 1.0]::DOUBLE[];
            SET VARIABLE execution_binding = {sql_value(exec_nc)};
            SET VARIABLE evolution_binding = {sql_value(evol_nc)};
            SET VARIABLE cluster_config = {{
                'projection_dim': 2::BIGINT, 'cluster_count': 2::BIGINT,
                'initialization': 'explicit_centers_v1'::VARCHAR, 'seed': 0::BIGINT,
                'restarts': 1::BIGINT, 'initial_centers': [[[0.0, 0.0], [1.0, 0.0]]]::DOUBLE[][][],
                'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
                'tolerance': 0.0::DOUBLE, 'max_iterations': 1::BIGINT,
                'rank_threshold': 1e-10::DOUBLE
            }};
            CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
            SELECT * FROM (VALUES
                (0, TIMESTAMP '2020-03-01 00:00:00', 48, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_conv_members}),
                (1, TIMESTAMP '2020-03-01 12:00:00', 36, 'present', NULL::VARCHAR, 'i1', 'o1', {not_conv_members}),
                (2, TIMESTAMP '2020-03-02 00:00:00', 24, 'present', NULL::VARCHAR, 'i2', 'o2', {ok_conv_members}),
                (3, TIMESTAMP '2020-03-02 12:00:00', 12, 'present', NULL::VARCHAR, 'i3', 'o3', {ok_conv_members})
            ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);
            .read validation/m3/sql/stage_and_publish.sql
            SELECT ordinal, status, status_reason FROM replay_batches WHERE run_key = {sql_value(rk_nc)} ORDER BY ordinal;
            """
            rc_nc, out_nc, err_nc = self.db.run_sql(sql_nc, db_file=db_path)
            self.assertEqual(rc_nc, 0, err_nc)
            nc_batches = parse_duckdb_json(out_nc)[-1]
            self.assertEqual(nc_batches[1]["status"], "failed")
            self.assertEqual(nc_batches[1]["status_reason"], "NOT_CONVERGED")

            # 3) [H3] NUMERICAL_FAILURE in stage_raw_batches (both [1e155, -1e155] and [8e153, -8e153] with w=[10, 10])
            #    must abort stage_and_publish.sql with non-zero exit code and 0 rows in all 11 tables!
            for idx_nf, (nf_feat, nf_weights) in enumerate(
                [
                    ("[1e155, -1e155]", "[1.0, 1.0]"),
                    ("[8e153, -8e153]", "[10.0, 10.0]"),
                ]
            ):
                nf_db = Path(tmp) / f"num_fail_{idx_nf}.duckdb"
                nf_rk = f"rk_raw_numerical_failure_{idx_nf}"
                nf_erid = f"erid_raw_numerical_failure_{idx_nf}"
                nf_exec, nf_evol = _synthetic_bindings(nf_rk, nf_erid, cluster_count=2)
                nf_members = (
                    f"[{{'member_id': 'm0', 'features': {nf_feat}}}, "
                    "{'member_id': 'm1', 'features': [0.0, 0.0]}, "
                    "{'member_id': 'm2', 'features': [1.0, 1.0]}, "
                    "{'member_id': 'm3', 'features': [2.0, 2.0]}]"
                )
                sql_nf = f"""
                SET VARIABLE run_key = {sql_value(nf_rk)};
                SET VARIABLE evolution_run_id = {sql_value(nf_erid)};
                SET VARIABLE planned_batch_count = 4;
                SET VARIABLE threshold_coefficient = 1.0::DOUBLE;
                SET VARIABLE weights = {nf_weights}::DOUBLE[];
                SET VARIABLE execution_binding = {sql_value(nf_exec)};
                SET VARIABLE evolution_binding = {sql_value(nf_evol)};
                SET VARIABLE cluster_config = {{
                    'projection_dim': 2::BIGINT, 'cluster_count': 2::BIGINT,
                    'initialization': 'kmeans++_chacha8_v1'::VARCHAR, 'seed': 3::BIGINT,
                    'restarts': 4::BIGINT, 'initial_centers': []::DOUBLE[][][],
                    'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
                    'tolerance': 1e-4::DOUBLE, 'max_iterations': 100::BIGINT,
                    'rank_threshold': 1e-10::DOUBLE
                }};
                CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
                SELECT * FROM (VALUES
                    (0, TIMESTAMP '2020-03-01 00:00:00', 48, 'present', NULL::VARCHAR, 'i0', 'o0', {ok_members}),
                    (1, TIMESTAMP '2020-03-01 12:00:00', 36, 'present', NULL::VARCHAR, 'i1', 'o1', {nf_members}),
                    (2, TIMESTAMP '2020-03-02 00:00:00', 24, 'present', NULL::VARCHAR, 'i2', 'o2', {ok_members}),
                    (3, TIMESTAMP '2020-03-02 12:00:00', 12, 'present', NULL::VARCHAR, 'i3', 'o3', {ok_members})
                ) t(ordinal, init_time, lead_hours, raw_status, status_reason, input_sha256, om_sha256, members);
                .read validation/m3/sql/stage_and_publish.sql
                """
                rc_nf, _, err_nf = self.db.run_sql(sql_nf, db_file=nf_db)
                self.assertNotEqual(rc_nf, 0)
                self.assertIn("NUMERICAL_FAILURE", err_nf)

                rc_nf_chk, out_nf_chk, err_nf_chk = self.db.run_sql(
                    """
                    .read validation/m3/sql/schema.sql
                    SELECT
                        (SELECT count(*) FROM tracking_runs) AS t1,
                        (SELECT count(*) FROM replay_batches) AS t2,
                        (SELECT count(*) FROM scenario_nodes) AS t3,
                        (SELECT count(*) FROM pair_comparisons) AS t4,
                        (SELECT count(*) FROM scenario_links) AS t5,
                        (SELECT count(*) FROM evolution_runs) AS t6,
                        (SELECT count(*) FROM evolution_nodes) AS t7,
                        (SELECT count(*) FROM evolution_transitions) AS t8,
                        (SELECT count(*) FROM evolution_edges) AS t9,
                        (SELECT count(*) FROM evolution_events) AS t10,
                        (SELECT count(*) FROM evolution_branch_summaries) AS t11;
                    """,
                    db_file=nf_db,
                )
                self.assertEqual(rc_nf_chk, 0, err_nf_chk)
                nf_counts = parse_duckdb_json(out_nf_chk)[-1][0]
                self.assertEqual(nf_counts, {f"t{i}": 0 for i in range(1, 12)})

    def test_synthetic_8batch_gap_chain_exact_parity_with_m3_runner(self) -> None:
        gap_case = next(
            c for c in self.fixtures["chain_cases"] if c["case_id"] == "chain_8batch_with_missing_and_failed_gaps"
        )
        rk = "synth8gap001_tracking_key"
        erid = "synth8gap001_evolution_run"

        # 1. Run m3_runner --mode chain
        runner_in = {
            "mode": "chain",
            "tracking_run_key": rk,
            "threshold_coefficient": gap_case["threshold_coefficient"],
            "weights": gap_case["weights"],
            "config": {
                k: v
                for k, v in gap_case["config"].items()
                if k != "uncertainty_rule"
            },
            "batches": [
                {
                    "ordinal": b["ordinal"],
                    "status": b["status"],
                    **(
                        {
                            "dispersion": b["dispersion"],
                            "scenarios": b["scenarios"],
                        }
                        if b["status"] == "ok"
                        else {
                            "error_code": b.get("error_code"),
                            "status_reason": b.get("status_reason"),
                        }
                    ),
                }
                for b in gap_case["batches"]
            ],
        }
        with tempfile.TemporaryDirectory(prefix="ensabc-m3-t024-parity-") as tmp_str:
            tmp = Path(tmp_str)
            in_p = tmp / "in.json"
            out_p = tmp / "out.json"
            in_p.write_text(json.dumps(runner_in), encoding="utf-8")
            proc = subprocess.run(
                [str(self.runner_bin), "--mode", "chain", "--input", str(in_p), "--output", str(out_p)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            runner_out = json.loads(out_p.read_text(encoding="utf-8"))

            # 2. Run SQL stage_and_publish.sql
            stage_sql = _build_stage_sql_from_chain_case(gap_case, run_key=rk, evolution_run_id=erid)
            query_sql = stage_sql + f"""
            SELECT chain_summary FROM evolution_runs WHERE evolution_run_id = {sql_value(erid)};
            SELECT * EXCLUDE (evolution_run_id) FROM evolution_nodes WHERE evolution_run_id = {sql_value(erid)} ORDER BY ordinal, cluster_id;
            SELECT * EXCLUDE (evolution_run_id, evolve_result) FROM evolution_transitions WHERE evolution_run_id = {sql_value(erid)} ORDER BY prev_ordinal;
            SELECT * EXCLUDE (evolution_run_id) FROM evolution_edges WHERE evolution_run_id = {sql_value(erid)} ORDER BY prev_ordinal, prev_cluster_id, curr_cluster_id;
            SELECT * EXCLUDE (evolution_run_id) FROM evolution_events WHERE evolution_run_id = {sql_value(erid)} ORDER BY prev_ordinal, event_index;
            SELECT * EXCLUDE (evolution_run_id) FROM evolution_branch_summaries WHERE evolution_run_id = {sql_value(erid)} ORDER BY branch_id;
            """
            rc, out, err = self.db.run_sql(query_sql)
            self.assertEqual(rc, 0, err)
            parsed = parse_duckdb_json(out)
            sql_cs = parsed[-6][0]["chain_summary"]
            sql_nodes = parsed[-5]
            sql_transitions = parsed[-4]
            sql_edges = parsed[-3]
            sql_events = parsed[-2]
            sql_branches = parsed[-1]

            # Exact chain_summary parity
            for k, v in runner_out["chain_summary"].items():
                if isinstance(v, float):
                    self.assertAlmostEqual(sql_cs[k], v, places=14, msg=f"chain_summary.{k}")
                else:
                    self.assertEqual(sql_cs[k], v, msg=f"chain_summary.{k}")

            # Exact nodes parity
            self.assertEqual(len(sql_nodes), len(runner_out["nodes"]))
            for sn, rn in zip(sql_nodes, runner_out["nodes"]):
                expected_pdelta = rn["primary_delta_fraction"] if rn["has_primary_delta_fraction"] else 0.0
                self.assertEqual(sn["ordinal"], rn["ordinal"])
                self.assertEqual(sn["cluster_id"], rn["cluster_id"])
                self.assertEqual(sn["canonical_rank"], rn["canonical_rank"])
                self.assertEqual(sn["branch_id"], rn["branch_id"])
                self.assertEqual(sn["assignment_reason"], rn["assignment_reason"])
                self.assertEqual(sn["incoming_role"], rn["incoming_role"])
                self.assertEqual(sn["outgoing_role"], rn["outgoing_role"])
                self.assertEqual(sn["has_primary_delta_fraction"], rn["has_primary_delta_fraction"])
                self.assertAlmostEqual(sn["primary_delta_fraction"], expected_pdelta, places=14)
                self.assertAlmostEqual(sn["unattributed_inflow_fraction"], rn["unattributed_inflow_fraction"], places=14)
                self.assertAlmostEqual(sn["unattributed_outflow_fraction"], rn["unattributed_outflow_fraction"], places=14)
                self.assertEqual(sn["high_uncertainty"], rn["high_uncertainty"])

            # Strict disjointness of gap events vs emergence/disappearance
            for st in sql_transitions:
                t_ev_types = {ev["event_type"] for ev in sql_events if ev["prev_ordinal"] == st["prev_ordinal"]}
                if st["transition_status"] == "gap":
                    self.assertTrue(t_ev_types.issubset({"gap_interrupted", "gap_restarted"}))
                else:
                    self.assertTrue(t_ev_types.isdisjoint({"gap_interrupted", "gap_restarted"}))

            # Exact edges parity
            self.assertEqual(len(sql_edges), len(runner_out["edges"]))
            for se, re_ in zip(sql_edges, runner_out["edges"]):
                self.assertEqual(se["prev_ordinal"], re_["prev_ordinal"])
                self.assertEqual(se["prev_cluster_id"], re_["prev_cluster_id"])
                self.assertEqual(se["curr_cluster_id"], re_["curr_cluster_id"])
                self.assertEqual(se["prev_branch_id"], re_["prev_branch_id"])
                self.assertEqual(se["curr_branch_id"], re_["curr_branch_id"])
                self.assertEqual(se["edge_role"], re_["edge_role"])
                self.assertEqual(se["event_type"], re_["event_type"])
                self.assertEqual(se["rule_basis"], re_["rule_basis"])
                self.assertAlmostEqual(se["distance"], re_["distance"], places=14)
                self.assertAlmostEqual(se["attributed_flow"], re_["attributed_flow"], places=14)

            # Exact branch summaries parity (0-ULP fsum verification)
            self.assertEqual(len(sql_branches), len(runner_out["branches"]))
            for sb, rb in zip(sql_branches, runner_out["branches"]):
                for k, rv in rb.items():
                    if isinstance(rv, float):
                        self.assertAlmostEqual(sb[k], rv, places=14, msg=f"branch[{sb['branch_id']}].{k}")
                    else:
                        self.assertEqual(sb[k], rv, msg=f"branch[{sb['branch_id']}].{k}")

    def test_idempotency_conflict_and_inject_m3_failure_zero_residual(self) -> None:
        case6 = next(c for c in self.fixtures["chain_cases"] if c["case_id"] == "chain_6batch_all_events")
        with tempfile.TemporaryDirectory(prefix="ensabc-m3-t024-tx-") as tmp_str:
            db_path = Path(tmp_str) / "atomicity.duckdb"
            rk1 = "txtest_run_key_v1"
            erid1 = "txtest_evol_run_v1"

            # 1. Test ALL 6 inject_m3_failure hooks BEFORE valid publish: must leave 0 residual rows!
            m3_hooks = (
                "after_runs",
                "after_nodes",
                "after_transitions",
                "after_edges",
                "after_events",
                "after_branches",
            )
            for hook in m3_hooks:
                with self.subTest(hook=hook):
                    fault_sql = _build_stage_sql_from_chain_case(
                        case6,
                        run_key=rk1,
                        evolution_run_id=erid1,
                        inject_m3_failure=hook,
                    )
                    rc_f, _, err_f = self.db.run_sql(fault_sql, db_file=db_path)
                    self.assertNotEqual(rc_f, 0)
                    self.assertIn(f"INJECTED_M3_FAILURE: {hook}", err_f)

                    rc_c, out_c, err_c = self.db.run_sql(
                        f"""
                        .read validation/m3/sql/schema.sql
                        SELECT
                            (SELECT count(*) FROM tracking_runs WHERE run_key = {sql_value(rk1)}) AS tr_cnt,
                            (SELECT count(*) FROM evolution_runs WHERE evolution_run_id = {sql_value(erid1)}) AS r_cnt,
                            (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = {sql_value(erid1)}) AS n_cnt,
                            (SELECT count(*) FROM evolution_transitions WHERE evolution_run_id = {sql_value(erid1)}) AS t_cnt,
                            (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = {sql_value(erid1)}) AS e_cnt,
                            (SELECT count(*) FROM evolution_events WHERE evolution_run_id = {sql_value(erid1)}) AS ev_cnt,
                            (SELECT count(*) FROM evolution_branch_summaries WHERE evolution_run_id = {sql_value(erid1)}) AS b_cnt;
                        """,
                        db_file=db_path,
                    )
                    self.assertEqual(rc_c, 0, err_c)
                    counts = parse_duckdb_json(out_c)[-1][0]
                    self.assertEqual(
                        counts,
                        {
                            "tr_cnt": 0,
                            "r_cnt": 0,
                            "n_cnt": 0,
                            "t_cnt": 0,
                            "e_cnt": 0,
                            "ev_cnt": 0,
                            "b_cnt": 0,
                        },
                        msg=f"Non-zero residual after {hook}: {counts}",
                    )

            # 2. Valid publish + idempotent republish in same DB
            valid_sql = _build_stage_sql_from_chain_case(case6, run_key=rk1, evolution_run_id=erid1)
            rc_v1, _, err_v1 = self.db.run_sql(valid_sql, db_file=db_path)
            self.assertEqual(rc_v1, 0, err_v1)
            rc_v2, _, err_v2 = self.db.run_sql(valid_sql, db_file=db_path)
            self.assertEqual(rc_v2, 0, err_v2)

            # 3. Same evolution_run_id with modified evolution parameter -> EVOLUTION_RUN_VERSION_CONFLICT
            tampered_case = json.loads(json.dumps(case6))
            tampered_case["config"]["high_uncertainty_threshold_ratio"] = 0.45
            conflict_sql = _build_stage_sql_from_chain_case(
                tampered_case,
                run_key=rk1,
                evolution_run_id=erid1,
            )
            rc_conf, _, err_conf = self.db.run_sql(conflict_sql, db_file=db_path)
            self.assertNotEqual(rc_conf, 0)
            self.assertIn("EVOLUTION_RUN_VERSION_CONFLICT", err_conf)

            # 4. New parameter version (distinct evolution_run_id) coexists cleanly over same M2 tracking_run_key!
            erid2 = "txtest_evol_run_v2_alt_uncertainty"
            coexist_sql = _build_stage_sql_from_chain_case(
                tampered_case,
                run_key=rk1,
                evolution_run_id=erid2,
            )
            rc_co, _, err_co = self.db.run_sql(coexist_sql, db_file=db_path)
            self.assertEqual(rc_co, 0, err_co)

            rc_final, out_final, err_final = self.db.run_sql(
                """
                SELECT count(*) AS m2_runs FROM tracking_runs;
                SELECT list(evolution_run_id ORDER BY evolution_run_id) AS m3_runs FROM evolution_runs;
                """,
                db_file=db_path,
            )
            self.assertEqual(rc_final, 0, err_final)
            p_final = parse_duckdb_json(out_final)
            self.assertEqual(p_final[-2][0]["m2_runs"], 1)
            self.assertEqual(p_final[-1][0]["m3_runs"], [erid1, erid2])

            # 5. [M4] Re-publishing with the SAME run_key and SAME stage_batches/content_sha256
            #    but scaled weights (2 * weights) OR altered execution_binding must fail with RUN_VERSION_CONFLICT!
            scaled_weights_case = json.loads(json.dumps(case6))
            scaled_weights_case["weights"] = [2.0 * float(w) for w in case6["weights"]]
            erid3_scaled_w = "txtest_evol_run_v3_scaled_weights"
            scaled_w_sql = _build_stage_sql_from_chain_case(
                scaled_weights_case,
                run_key=rk1,
                evolution_run_id=erid3_scaled_w,
            )
            rc_sw, _, err_sw = self.db.run_sql(scaled_w_sql, db_file=db_path)
            self.assertNotEqual(rc_sw, 0)
            self.assertIn("RUN_VERSION_CONFLICT", err_sw)

            altered_bind_sql = valid_sql.replace(
                f"SET VARIABLE evolution_run_id = {sql_value(erid1)};",
                "SET VARIABLE evolution_run_id = 'txtest_evol_run_v4_alt_exec_bind';",
            ).replace(
                "'weights_version': 'synthetic.weights.v1'",
                "'weights_version': 'tampered.weights.v99'",
                1,
            )
            rc_ab, _, err_ab = self.db.run_sql(altered_bind_sql, db_file=db_path)
            self.assertNotEqual(rc_ab, 0)
            self.assertIn("RUN_VERSION_CONFLICT", err_ab)

    def test_multistep_secondary_absorption_order_parity(self) -> None:
        # [M1] Verify Rust m3_runner, Python solve_chain_oracle, and SQL evolution_branch_summaries
        # agree 100% on canonical (prev_ordinal, branch_id) ordering when prev_ordinal=0 absorbs
        # `:b00:k02` and prev_ordinal=1 absorbs `:b00:k01` (lexicographically smaller).
        from validation.m3.oracle import solve_chain_oracle

        rk = "absorb0123456789_key"
        erid = "absorb0123456789_erid"
        absorb_case = {
            "case_id": "chain_4batch_multistep_secondary_absorption",
            "tracking_run_key": rk,
            "threshold_coefficient": 1.0,
            "weights": [1.0, 1.0],
            "config": default_evolution_config(),
            "batches": [
                {
                    "ordinal": 0,
                    "init_time_utc": "2020-02-01T00:00:00Z",
                    "lead_hours": 48,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 0.0]},
                        {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [0.0, 1.5]},
                        {"cluster_id": 2, "member_count": 15, "member_fraction": 0.30, "mean_features": [0.5, 0.0]},
                    ],
                },
                {
                    "ordinal": 1,
                    "init_time_utc": "2020-02-01T12:00:00Z",
                    "lead_hours": 36,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [0.0, 0.0]},
                        {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 1.5]},
                    ],
                },
                {
                    "ordinal": 2,
                    "init_time_utc": "2020-02-02T00:00:00Z",
                    "lead_hours": 24,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 50, "member_fraction": 1.00, "mean_features": [0.0, 0.0]},
                    ],
                },
                {
                    "ordinal": 3,
                    "init_time_utc": "2020-02-02T12:00:00Z",
                    "lead_hours": 12,
                    "status": "ok",
                    "dispersion": 2.0,
                    "scenarios": [
                        {"cluster_id": 0, "member_count": 50, "member_fraction": 1.00, "mean_features": [0.0, 0.0]},
                    ],
                },
            ],
        }

        ora_res = solve_chain_oracle(absorb_case)
        ora_main = next(b for b in ora_res["branch_summaries"] if b["branch_id"] == f"{rk[:12]}:b00:k00")
        expected_order = [f"{rk[:12]}:b00:k02", f"{rk[:12]}:b00:k01"]
        self.assertEqual(ora_main["absorbed_secondary_branch_ids"], expected_order)

        with tempfile.TemporaryDirectory(prefix="ensabc-m3-m1-order-") as tmp_str:
            tmp = Path(tmp_str)
            in_p = tmp / "in.json"
            out_p = tmp / "out.json"
            runner_in = {
                "mode": "chain",
                "tracking_run_key": rk,
                "threshold_coefficient": 1.0,
                "weights": [1.0, 1.0],
                "batches": [
                    {"ordinal": b["ordinal"], "status": "ok", "dispersion": b["dispersion"], "scenarios": b["scenarios"]}
                    for b in absorb_case["batches"]
                ],
            }
            in_p.write_text(json.dumps(runner_in), encoding="utf-8")
            proc = subprocess.run(
                [str(self.runner_bin), "--mode", "chain", "--input", str(in_p), "--output", str(out_p)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            rust_res = json.loads(out_p.read_text(encoding="utf-8"))
            rust_main = next(b for b in rust_res["branches"] if b["branch_id"] == f"{rk[:12]}:b00:k00")
            self.assertEqual(rust_main["absorbed_secondary_branch_ids"], expected_order)

            sql = _build_stage_sql_from_chain_case(absorb_case, run_key=rk, evolution_run_id=erid) + f"""
            SELECT branch_id, absorbed_secondary_branch_ids
            FROM evolution_branch_summaries
            WHERE evolution_run_id = {sql_value(erid)} AND branch_id = {sql_value(f"{rk[:12]}:b00:k00")};
            """
            rc, out, err = self.db.run_sql(sql)
            self.assertEqual(rc, 0, err)
            sql_main = parse_duckdb_json(out)[-1][0]
            self.assertEqual(sql_main["absorbed_secondary_branch_ids"], expected_order)


if __name__ == "__main__":
    unittest.main()
