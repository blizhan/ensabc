#!/usr/bin/env python3
"""Master gate orchestrator for Milestone 2 (M2) cross-batch tracking (T052).

Usage:
    python3 -B validation/m2/run_m2_gate.py --run-id RUN_ID [--cache-dir DIR]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import atomic_write_json, load_json, sha256_file

VENV_PY = REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"
SCRIPTS_DIR = REPO_ROOT / "validation" / "m2"


def run_step(step_name: str, cmd: list[str], cwd: Path = REPO_ROOT, timeout: float = 300.0) -> None:
    print(f"\n[{step_name}] Executing: {' '.join(str(c) for c in cmd)}")
    t0 = time.perf_counter()
    run_dir = Path(os.environ["ENSABC_M2_RUN_DIR"])
    records_path = run_dir / "execution-steps.json"
    records = load_json(records_path) if records_path.is_file() else {"steps": []}
    record = {"step": step_name.split()[-1], "command": cmd, "status": "running"}
    records["steps"].append(record)
    atomic_write_json(records_path, records)
    try:
        proc = subprocess.run(cmd, cwd=cwd, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        record.update(status="fail", error=str(exc), elapsed_seconds=time.perf_counter() - t0)
        atomic_write_json(records_path, records)
        raise
    elapsed = time.perf_counter() - t0
    record.update(status="pass" if proc.returncode == 0 else ("not_run" if proc.returncode == 2 else "fail"),
                  exit_code=proc.returncode, elapsed_seconds=elapsed)
    atomic_write_json(records_path, records)
    if proc.returncode != 0:
        print(f"[{step_name}] FAILED (code {proc.returncode}) after {elapsed:.2f}s", file=sys.stderr)
        sys.exit(proc.returncode if proc.returncode in (1, 2) else 1)
    print(f"[{step_name}] PASSED in {elapsed:.2f}s")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run complete M2 verification gate")
    parser.add_argument("--run-id", required=True, help="Unique run identifier")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "validation" / "m2" / ".cache", help="Cache directory")
    args = parser.parse_args()

    run_id = args.run_id
    run_dir = REPO_ROOT / "artifacts" / "m2" / run_id
    if run_dir.exists():
        print(f"Error: Run directory already exists: {run_dir}", file=sys.stderr)
        return 2

    run_dir.mkdir(parents=True, exist_ok=False)
    os.environ["ENSABC_M2_RUN_DIR"] = str(run_dir)
    cache_dir = args.cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)

    build_dir = run_dir / "build"
    build_dir.mkdir(parents=True, exist_ok=True)

    print("================================================================================")
    print(f"Starting M2 Verification Gate: {run_id}")
    print(f"Artifacts: {run_dir}")
    print("================================================================================")

    # 1. Setup environment check
    run_step("1/16 setup_env", ["bash", str(SCRIPTS_DIR / "setup_env.sh"), "--check"])

    # 2. Build extensions and runners into build_dir
    run_step("2/16 build_extension", ["bash", str(SCRIPTS_DIR / "build_extension.sh"), "--output-dir", str(build_dir)])
    ensabc_ext = build_dir / "ensabc_m0_extension.duckdb_extension"
    runner_bin = build_dir / "m2_runner"
    assert ensabc_ext.is_file()
    assert runner_bin.is_file()
    os.environ["ENSABC_M2_EXTENSION"] = str(ensabc_ext)

    # 3. Prepare real OM sample
    sample_out = run_dir / "sample"
    sample_out.mkdir(parents=True, exist_ok=True)
    om_verif_dir = run_dir / "om-verification"
    om_verif_dir.mkdir(parents=True, exist_ok=True)
    run_step(
        "3/16 prepare_om_sample",
        [str(VENV_PY), str(SCRIPTS_DIR / "prepare_om_sample.py"), "--cache-dir", str(cache_dir), "--out-dir", str(sample_out)],
    )

    # 4. Test OM sample & negative fixtures
    run_step(
        "4/16 test_om_sample",
        [str(VENV_PY), str(SCRIPTS_DIR / "test_om_sample.py"), "--sample-dir", str(sample_out), "--cache-dir", str(cache_dir), "--out-report", str(om_verif_dir / "test-om-sample-report.json")],
    )

    # 5. Verify pure Rust matching core against Fraction oracle
    run_step(
        "5/16 verify_matching_core",
        [str(VENV_PY), str(SCRIPTS_DIR / "verify_matching_core.py"), "--run-dir", str(run_dir), "--runner", str(runner_bin)],
    )

    # 6. Verify SQL history synthetic replay
    run_step(
        "6/16 verify_sql_and_history_synthetic",
        [sys.executable, str(SCRIPTS_DIR / "verify_sql_and_history.py"), "--run-dir", str(run_dir), "--scope", "synthetic", "--extension", str(ensabc_ext)],
    )

    run_step("sql_contract_tests", [str(VENV_PY), "-m", "unittest", "validation.m2.test_sql_history", "validation.m2.test_evidence", "validation.m2.test_review_regressions"], cwd=REPO_ROOT)

    # 7. Verify OM paths stage cells
    run_step(
        "7/16 verify_om_paths_cells",
        [str(VENV_PY), str(SCRIPTS_DIR / "verify_om_paths.py"), "--run-dir", str(run_dir), "--stage", "cells", "--sample-dir", str(sample_out), "--cache-dir", str(cache_dir)],
    )

    # 8. Verify SQL history real replay
    run_step(
        "8/16 verify_sql_and_history_real",
        [sys.executable, str(SCRIPTS_DIR / "verify_sql_and_history.py"), "--run-dir", str(run_dir), "--scope", "real", "--extension", str(ensabc_ext), "--sample-dir", str(sample_out), "--cache-dir", str(cache_dir)],
    )

    # 9. Verify OM paths stage replay
    run_step(
        "9/16 verify_om_paths_replay",
        [str(VENV_PY), str(SCRIPTS_DIR / "verify_om_paths.py"), "--run-dir", str(run_dir), "--stage", "replay", "--sample-dir", str(sample_out), "--cache-dir", str(cache_dir)],
    )

    # 10. Run case study, stability certificate tests, and sensitivity
    run_step(
        "10a/16 test_stability",
        [str(VENV_PY), str(SCRIPTS_DIR / "test_stability.py"), "--out-report", str(run_dir / "case-study" / "test-stability-report.json")],
    )
    run_step(
        "10/16 run_case_and_sensitivity",
        [str(VENV_PY), str(SCRIPTS_DIR / "run_case_and_sensitivity.py"), "--run-dir", str(run_dir), "--sample-dir", str(sample_out), "--cache-dir", str(cache_dir)],
    )

    # 11. Verify minimal isolated runtime container
    run_step(
        "11/16 verify_minimal_runtime",
        ["bash", str(SCRIPTS_DIR / "verify_minimal_runtime.sh"), "--run-dir", str(run_dir), "--sample-dir", str(sample_out), "--extension", str(ensabc_ext), "--runner", str(runner_bin)],
    )

    # 12. Measure 5-stage pipeline latency & RSS
    run_step(
        "12/16 measure_pipeline",
        [str(VENV_PY), str(SCRIPTS_DIR / "measure_pipeline.py"), "--run-dir", str(run_dir), "--sample-dir", str(sample_out), "--extension", str(ensabc_ext)],
    )

    # 13. Run M1 regression gate
    run_step(
        "13/16 run_m1_regression",
        [sys.executable, str(SCRIPTS_DIR / "run_m1_regression.py"), "--run-dir", str(run_dir)],
    )

    # 14. Independent reproduction
    run_step(
        "14/16 reproduce_independently",
        [sys.executable, str(SCRIPTS_DIR / "reproduce_independently.py"), "--run-dir", str(run_dir)],
    )

    # 15. Traceability matrix
    run_step(
        "15/16 traceability",
        [sys.executable, str(SCRIPTS_DIR / "traceability.py"), "--run-dir", str(run_dir)],
    )

    # 16. Verify report and independent audit
    run_step(
        "16/16 verify_report",
        [sys.executable, str(SCRIPTS_DIR / "verify_report.py"), "--run-dir", str(run_dir)],
    )

    print("\n================================================================================")
    print(f"M2 VERIFICATION GATE PASSED: Run ID {run_id}")
    print(f"All 21 Functional Requirements and 8 Success Criteria verified.")
    print(f"Artifacts saved in: {run_dir}")
    print("================================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
