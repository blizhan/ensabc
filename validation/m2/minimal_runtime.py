#!/usr/bin/env python3
"""Prepare and check a full offline replay; Python runs only outside the container."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from validation.m2.common import load_json, parse_duckdb_json, atomic_write_json


def prepare(run_dir: Path, staging: Path) -> None:
    sql = (run_dir / "sql-history/full-replay.sql").read_text()
    import re
    sql = re.sub(r"LOAD '[^']*duckomo.duckdb_extension';", "LOAD '/runtime/opt/duckomo.duckdb_extension';", sql)
    sql = re.sub(r"LOAD '[^']*ensabc_m0_extension.duckdb_extension';", "LOAD '/runtime/opt/ensabc_m0_extension.duckdb_extension';", sql)
    sql = re.sub(r"SET VARIABLE om_dir = '[^']*';", "SET VARIABLE om_dir = '/runtime/om';", sql)
    prefix = ("SET extension_directory = '/results/extensions';\n"
              "SET autoinstall_known_extensions = false;\nSET autoload_known_extensions = false;\n")
    release = load_json(REPO_ROOT / "validation/m2/fixtures/duckomo_releases.json")["target"]
    guard = ("\nCREATE TEMP TABLE checked_identity AS SELECT CASE WHEN count(*)=1 THEN true "
             "ELSE error('DuckOMO identity mismatch') END FROM duckdb_extensions() "
             "WHERE extension_name='duckomo' AND loaded AND install_mode='NOT_INSTALLED' "
             f"AND extension_version='{release['extension_version']}';\n")
    sql = sql.replace("LOAD '/runtime/opt/duckomo.duckdb_extension';", "LOAD '/runtime/opt/duckomo.duckdb_extension';" + guard)
    (staging / "replay.sql").write_text(prefix + sql)
    failed = sql.replace("SET VARIABLE inject_failure = false", "SET VARIABLE inject_failure = true")
    key = load_json(run_dir / "sql-history/tracking-history.json")["history_summary"]["run_key"]
    failed = failed.replace(key, key + "-fault")
    recovery = prefix + failed + "\nROLLBACK;\n"
    recovery += "SELECT ens_dispersion([{'member_id':'','features':[1.0]}, {'member_id':'m1','features':[2.0]}], [1.0]);\n"
    recovery += "SELECT ens_dispersion([{'member_id':'m0','features':[1.0]}, {'member_id':'m1','features':[3.0]}], [1.0]);\n"
    recovery += sql.replace("CREATE TEMP TABLE checked_identity", "CREATE OR REPLACE TEMP TABLE checked_identity")
    recovery += f"\nSELECT CASE WHEN (SELECT count(*) FROM tracking_runs WHERE run_key='{key}-fault')=0 "
    recovery += f"AND (SELECT count(*) FROM scenario_nodes WHERE run_key='{key}')=24 "
    recovery += "THEN 'RECOVERED' ELSE error('Recovery or atomicity failed') END AS recovery;\n"
    (staging / "recovery.sql").write_text(recovery)


def verify(run_dir: Path, results: Path) -> None:
    import math
    runner_out = load_json(results / "runner_output.json")
    expected_dist = math.sqrt(((280.0 - 280.2) ** 2 + (281.0 - 281.1) ** 2) / 2.0)
    if (
        runner_out.get("objective", {}).get("matched_count") != 1
        or len(runner_out.get("links", [])) != 1
        or abs(runner_out["links"][0]["distance_k"] - expected_dist) > 1e-12
    ):
        raise AssertionError(f"Offline m2_runner output unexpected: {runner_out}")
    baseline = load_json(run_dir / "sql-history/tracking-history.json")["history_summary"]["results"]
    output = parse_duckdb_json((results / "sql_output.json").read_text())
    actual = dict(zip(("batches", "nodes", "pairs", "links"), output[-4:]))
    if actual != baseline:
        raise AssertionError("Offline full history differs from normal replay")
    recovered = parse_duckdb_json((results / "recovery_output.json").read_text())
    if recovered[-1] != [{"recovery": "RECOVERED"}]:
        raise AssertionError("Same-session recovery did not complete")
    errors = (results / "recovery_errors.txt").read_text()
    if "INJECTED_FAILURE" not in errors or "EMPTY_MEMBER_ID" not in errors:
        raise AssertionError("Recovery did not exercise expected failures")
    atomic_write_json(run_dir / "minimal-replay-check.json", {
        "status": "pass", "runner_output_verified": True,
        "full_history_identical": True, "same_session_recovery": True,
        "failed_publish_residual_rows": 0, "batches": 8, "nodes": 24, "pairs": 7,
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["prepare", "verify"])
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    (prepare if args.mode == "prepare" else verify)(args.run_dir, args.directory)
