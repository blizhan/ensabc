#!/usr/bin/env python3
"""Run M1 scenario computation regression gate on current M2 codebase (T048).

Usage:
    python3 -B validation/m2/run_m1_regression.py --run-dir DIR
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import atomic_write_json, load_json, sha256_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Run M1 regression gate against current codebase")
    parser.add_argument("--run-dir", type=Path, required=True, help="Target run directory")
    args = parser.parse_args()

    run_dir = args.run_dir
    regress_dir = run_dir / "m1-regression"
    regress_dir.mkdir(parents=True, exist_ok=True)

    m1_run_id = f"m1-regress-{uuid.uuid4().hex[:8]}"
    print(f"Running M1 gate with run ID {m1_run_id}...")

    gate_script = REPO_ROOT / "validation" / "m1" / "run_m1_gate.py"
    cmd = [sys.executable, str(gate_script), "--run-id", m1_run_id]

    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"M1 gate failed: {proc.stderr}", file=sys.stderr)
        summary = {
            "schema_version": "m2.m1-regression.v1",
            "status": "failed",
            "m1_run_id": m1_run_id,
            "error": proc.stderr.strip(),
        }
        atomic_write_json(regress_dir / "summary.json", summary)
        return 1

    m1_artifacts = REPO_ROOT / "artifacts" / "m1" / m1_run_id
    if not m1_artifacts.is_dir():
        raise FileNotFoundError(f"M1 artifacts directory not found: {m1_artifacts}")

    # Copy report.json, traceability.json, report-audit.json to regress_dir
    for fname in ("report.json", "traceability.json", "report-audit.json", "environment.json"):
        src_file = m1_artifacts / fname
        if src_file.is_file():
            shutil.copy2(src_file, regress_dir / fname)

    m1_report = load_json(m1_artifacts / "report.json")
    m1_overall = m1_report.get("overall") or m1_report.get("overall_status")
    summary = {
        "schema_version": "m2.m1-regression.v1",
        "status": "pass" if m1_overall == "passed" else "failed",
        "m1_run_id": m1_run_id,
        "m1_overall": m1_overall,
        "report_sha256": sha256_file(m1_artifacts / "report.json"),
    }
    atomic_write_json(regress_dir / "summary.json", summary)
    print(f"M1 regression passed. Summary written to {regress_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
