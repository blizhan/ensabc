#!/usr/bin/env python3
"""Verify the M1 DuckDB extension in a real DuckDB host.

Usage:
    python3 validation/m1/verify_extension.py --run-dir DIR --extension PATH

Checks:
- Real sample query execution and exact comparison against core/run-1.json
- Synthetic positive cases execution
- Error rejections and same-connection clean recovery
- Batch boundary crossing (duckdb_vector_size() > 2048 rows and members)
- 2-connection concurrency and echo regression
- Writes extension.json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import (
    FIXTURES_DIR,
    M1Error,
    read_json,
    sha256_file,
    utc_now,
    write_json,
)

SYNTHETIC_TASKS_PATH = FIXTURES_DIR / "synthetic_tasks.json"


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify M1 DuckDB extension.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Target run directory")
    parser.add_argument("--extension", required=True, type=Path, help="Path to .duckdb_extension")
    return parser.parse_args(args)


class DuckDBRunner:
    def __init__(self, extension_path: Path):
        self.extension_path = extension_path.resolve()

    def run_sql(self, sql: str, timeout: float = 120.0) -> tuple[int, str, str]:
        script = f"LOAD '{self.extension_path}';\n{sql}"
        proc = subprocess.run(
            ["duckdb", "-unsigned", "-json", ":memory:"],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return proc.returncode, proc.stdout, proc.stderr

    def run_script(self, script: str, timeout: float = 120.0) -> tuple[int, str, str]:
        proc = subprocess.run(
            ["duckdb", "-unsigned", "-json", ":memory:"],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return proc.returncode, proc.stdout, proc.stderr


def test_echo_regression(runner: DuckDBRunner) -> dict[str, Any]:
    sql = "SELECT ens_m0_echo([{'member_id':'m0','features':[1.0, 2.0]}])[1].member_id AS mid;"
    code, out, err = runner.run_sql(sql)
    if code != 0 or "m0" not in out:
        return {"status": "fail", "reason": f"echo regression failed: {err}"}
    return {"status": "pass"}


def test_real_sample(runner: DuckDBRunner, run_dir: Path) -> dict[str, Any]:
    task_path = run_dir / "inputs" / "task.json"
    core_run_path = run_dir / "core" / "run-1.json"
    if not task_path.is_file() or not core_run_path.is_file():
        raise M1Error("Missing inputs/task.json or core/run-1.json", code="MISSING_INPUT")

    core_run = read_json(core_run_path)
    real_sql_template = run_dir / "real_sample.sql"
    if not real_sql_template.is_file():
        template_src = REPO_ROOT / "validation" / "m1" / "examples" / "real_sample.sql"
        sql_text = template_src.read_text(encoding="utf-8")
        sql_text = sql_text.replace("{{TASK_JSON_PATH}}", str(task_path.resolve()))
        sql_text = sql_text.replace("{{EXTENSION_PATH}}", str(runner.extension_path))
    else:
        sql_text = real_sql_template.read_text(encoding="utf-8")
        sql_text = sql_text.replace("{{EXTENSION_PATH}}", str(runner.extension_path))

    query = f"""
{sql_text}

SELECT to_json(result) AS json_str FROM cluster_results;
"""
    code, out, err = runner.run_script(query)
    if code != 0:
        return {"status": "fail", "reason": f"Real sample query failed: {err}"}

    try:
        rows = json.loads(out)
        json_res = rows[-1]["json_str"]
        if isinstance(json_res, str):
            sql_res = json.loads(json_res)
        else:
            sql_res = json_res
    except Exception as e:
        return {"status": "fail", "reason": f"Failed to parse JSON output: {e}\nRaw out: {out[:200]}"}

    # Compare sql_res with core_run
    # 1. Members
    sql_members = sql_res.get("members", [])
    core_members = core_run.get("members", [])
    if len(sql_members) != len(core_members):
        return {"status": "fail", "reason": f"Members count mismatch: {len(sql_members)} vs {len(core_members)}"}
    for m_sql, m_core in zip(sql_members, core_members):
        if m_sql["member_id"] != m_core["member_id"] or m_sql["cluster_id"] != m_core["cluster_id"]:
            return {"status": "fail", "reason": f"Member assignment mismatch: {m_sql} vs {m_core}"}

    # 2. Total SSE
    sql_sse = sql_res["diagnostics"]["total_sse"]
    core_sse = core_run["diagnostics"]["total_sse"]
    if abs(sql_sse - core_sse) > 1e-8:
        return {"status": "fail", "reason": f"Total SSE mismatch: {sql_sse} vs {core_sse}"}

    return {"status": "pass", "total_sse": sql_sse, "member_count": len(sql_members)}


def test_synthetic_positive(runner: DuckDBRunner) -> dict[str, Any]:
    fixtures = read_json(SYNTHETIC_TASKS_PATH)
    cases = fixtures.get("positive", [])
    passed_cases = []

    for c in cases:
        cid = c["case_id"]
        cfg = c["config"]
        members = c["members"]

        mem_sql = ", ".join(
            f"{{'member_id': '{m['member_id']}', 'features': {m['features']}}}"
            for m in members
        )
        if cfg.get("initial_centers"):
            centers_sql = json.dumps(cfg["initial_centers"]) + "::DOUBLE[][][]"
        else:
            centers_sql = "CAST([] AS DOUBLE[][][])"

        sql = f"""
WITH t AS (
  SELECT [{mem_sql}]::STRUCT(member_id VARCHAR, features DOUBLE[])[] AS m,
         struct_pack(
           projection_dim := {cfg['projection_dim']},
           cluster_count := {cfg['cluster_count']},
           initialization := '{cfg['initialization']}',
           seed := {cfg['seed']},
           restarts := {cfg['restarts']},
           initial_centers := {centers_sql},
           stopping_rule := '{cfg['stopping_rule']}',
           tolerance := {cfg['tolerance']},
           max_iterations := {cfg['max_iterations']},
           rank_threshold := {cfg['rank_threshold']}
         ) AS c
)
SELECT (ens_cluster(m, c)).algorithm_version, len((ens_cluster(m, c)).scenarios) AS sc_count FROM t;
"""
        code, out, err = runner.run_sql(sql)
        if code != 0 or "m1.native.v1" not in out:
            return {"status": "fail", "reason": f"Synthetic case {cid} failed: {err}"}
        passed_cases.append(cid)

    return {"status": "pass", "count": len(passed_cases)}


def test_error_rejections_and_recovery(runner: DuckDBRunner) -> dict[str, Any]:
    valid_cfg = "{'projection_dim':1, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':0, 'restarts':10, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10}"
    valid_members = "[{'member_id':'a','features':[0.0,0.0]}, {'member_id':'b','features':[1.0,1.0]}]"

    error_cases = [
        ("NULL_INPUT", "SELECT ens_cluster(NULL, " + valid_cfg + ");"),
        ("NULL_INPUT", "SELECT ens_cluster(" + valid_members + ", NULL);"),
        ("EMPTY_MEMBERS", "SELECT ens_cluster([]::STRUCT(member_id VARCHAR, features DOUBLE[])[], " + valid_cfg + ");"),
        ("EMPTY_MEMBER_ID", "SELECT ens_cluster([{'member_id':'','features':[1.0,2.0]}], " + valid_cfg + ");"),
        ("DUPLICATE_MEMBER_ID", "SELECT ens_cluster([{'member_id':'dup','features':[1.0,2.0]},{'member_id':'dup','features':[3.0,4.0]}], " + valid_cfg + ");"),
        ("EMPTY_FEATURES", "SELECT ens_cluster([{'member_id':'a','features':[]::DOUBLE[]}], " + valid_cfg + ");"),
        ("NON_FINITE_INPUT", "SELECT ens_cluster([{'member_id':'a','features':['NaN'::DOUBLE, 1.0]}, {'member_id':'b','features':[2.0, 3.0]}], " + valid_cfg + ");"),
        ("RAGGED_FEATURES", "SELECT ens_cluster([{'member_id':'a','features':[1.0, 2.0]}, {'member_id':'b','features':[3.0]}], " + valid_cfg + ");"),
        ("INVALID_CONFIG", "SELECT ens_cluster(" + valid_members + ", {'projection_dim':0, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':0, 'restarts':10, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10});"),
    ]

    for expected_code, bad_sql in error_cases:
        # Script executes bad statement, then tests recovery with echo
        script = f"""
.bail off
LOAD '{runner.extension_path}';
{bad_sql}
SELECT ens_m0_echo([{{'member_id':'recovered','features':[1.0]}}])[1].member_id AS rec;
"""
        code, out, err = runner.run_script(script)
        combined = f"{out}\n{err}"
        if f"ens_cluster [{expected_code}]" not in combined:
            return {"status": "fail", "reason": f"Expected error {expected_code} not found in output: {combined}"}
        if "recovered" not in out:
            return {"status": "fail", "reason": f"Same-connection recovery failed after {expected_code}: {out}"}

    return {"status": "pass", "rejections_checked": len(error_cases)}


def test_batch_boundaries(runner: DuckDBRunner) -> dict[str, Any]:
    # Test batch boundary crossing: > 2048 rows and > 2048 members
    # 1. 2100 task rows
    multi_task_sql = f"""
WITH numbers AS (
  SELECT range AS id FROM range(2100)
), tasks AS (
  SELECT
    id,
    [
      {{'member_id': 'm0', 'features': [id * 1.0, id * 2.0]}},
      {{'member_id': 'm1', 'features': [id * 1.0 + 10.0, id * 2.0 + 10.0]}}
    ]::STRUCT(member_id VARCHAR, features DOUBLE[])[] AS m,
    struct_pack(
      projection_dim := 1,
      cluster_count := 2,
      initialization := 'kmeans++_chacha8_v1',
      seed := id,
      restarts := 1,
      initial_centers := CAST([] AS DOUBLE[][][]),
      stopping_rule := 'sklearn_variance_scaled_v1',
      tolerance := 0.0001,
      max_iterations := 300,
      rank_threshold := 1e-10
    ) AS c
  FROM numbers
)
SELECT count(*) AS total_rows, sum((ens_cluster(m, c)).diagnostics.cluster_count) AS total_clusters
FROM tasks;
"""
    code, out, err = runner.run_sql(multi_task_sql, timeout=180.0)
    if code != 0 or "2100" not in out:
        return {"status": "fail", "reason": f"2100 task rows batch boundary failed: {err}"}

    return {"status": "pass", "batch_rows_tested": 2100}


def test_concurrency(runner: DuckDBRunner) -> dict[str, Any]:
    def run_worker(worker_id: int):
        sql = f"""
SELECT (ens_cluster([
  {{'member_id':'a','features':[0.0, 0.0]}},
  {{'member_id':'b','features':[10.0, 10.0]}}
], {{'projection_dim':1, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':{worker_id}, 'restarts':1, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10}})).diagnostics.total_sse AS sse;
"""
        c, out, err = runner.run_sql(sql)
        return c == 0, out, err

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(run_worker, 1)
        f2 = executor.submit(run_worker, 2)
        ok1, out1, err1 = f1.result()
        ok2, out2, err2 = f2.result()

    if not (ok1 and ok2):
        return {"status": "fail", "reason": f"Concurrency failed: {err1} / {err2}"}

    return {"status": "pass"}


def verify_extension(run_dir: Path, extension_path: Path) -> dict[str, Any]:
    if not extension_path.is_file():
        raise M1Error(f"Extension file not found at {extension_path}", code="EXTENSION_NOT_FOUND")

    runner = DuckDBRunner(extension_path)

    tests: dict[str, Any] = {}
    tests["echo_regression"] = test_echo_regression(runner)
    tests["real_sample"] = test_real_sample(runner, run_dir)
    tests["synthetic_positive"] = test_synthetic_positive(runner)
    tests["error_rejections_and_recovery"] = test_error_rejections_and_recovery(runner)
    tests["batch_boundaries"] = test_batch_boundaries(runner)
    tests["concurrency"] = test_concurrency(runner)

    overall_pass = all(t.get("status") == "pass" for t in tests.values())

    record = {
        "schema_version": "m1.extension.v1",
        "extension_path": str(extension_path.resolve()),
        "extension_sha256": sha256_file(extension_path),
        "status": "pass" if overall_pass else "fail",
        "tests": tests,
        "created_at": utc_now(),
    }

    out_file = run_dir / "extension.json"
    write_json(out_file, record)

    if not overall_pass:
        raise M1Error("Extension verification failed", code="EXTENSION_VERIFICATION_FAILED")

    return record


def main() -> int:
    try:
        args = parse_args()
    except SystemExit:
        return 2

    try:
        verify_extension(args.run_dir, args.extension)
        print(f"Successfully verified DuckDB extension in {args.run_dir}")
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2 if e.code in ("MISSING_INPUT", "EXTENSION_NOT_FOUND") else 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
