#!/usr/bin/env python3
"""Sequential M2 cross-batch tracking gate regression wrapper for M3 (T041).

Executes full `validation/m2/.venv/bin/python validation/m2/run_m2_gate.py --run-id m2-regress-<uuid8> --cache-dir <dir>`
(and `validation/m2/verify_report.py --run-dir artifacts/m2/m2-regress-<uuid8>` if not
already generated inside `run_m2_gate.py`), verifies `overall_status == "passed"`,
`traceability.status == "pass"`, and `report-audit.audit_status == "passed"`,
copies those 3 files into `<run_dir>/m2-regression/`, computes SHA-256 digests,
and writes `<run_dir>/m2-regression/summary.json`.

MUST be executed sequentially after (never concurrently with) `run_m1_regression.py`
because `run_m2_gate.py` also invokes M1 regression using `/tmp/ensabc-m1-extension-target`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    M2_CACHE_DIR,
    M2_VALIDATION_DIR,
    atomic_write_json,
    load_json,
    sha256_file,
)

SCHEMA_VERSION = "m3.m2_regression.v1"
EVIDENCE_FILENAMES: tuple[str, ...] = (
    "report.json",
    "traceability.json",
    "report-audit.json",
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_and_summarize_m2_artifacts(
    m2_artifacts_dir: Path,
    regress_dir: Path,
    child_run_id: str,
) -> dict[str, Any]:
    """Validate M2 gate evidence files, copy them to `regress_dir`, and write `summary.json`."""
    regress_dir.mkdir(parents=True, exist_ok=True)
    findings: list[str] = []

    for fname in EVIDENCE_FILENAMES:
        if not (m2_artifacts_dir / fname).is_file():
            findings.append(f"Missing required M2 gate artifact: {m2_artifacts_dir / fname}")

    if findings:
        summary = {
            "schema_version": SCHEMA_VERSION,
            "status": "FAIL",
            "overall_status": "failed",
            "traceability_status": "fail",
            "audit_status": "failed",
            "child_run_id": child_run_id,
            "child_artifacts_dir": str(m2_artifacts_dir),
            "checked_at_utc": _utc_now(),
            "evidence_sha256s": {},
            "findings": findings,
        }
        atomic_write_json(regress_dir / "summary.json", summary)
        return summary

    report = load_json(m2_artifacts_dir / "report.json")
    trace = load_json(m2_artifacts_dir / "traceability.json")
    audit = load_json(m2_artifacts_dir / "report-audit.json")

    raw_overall = report.get("overall_status")
    reqs = report.get("requirements", {})
    scs = report.get("success_criteria", {})
    reqs_ok = (
        isinstance(reqs, dict)
        and len(reqs) == 21
        and all(isinstance(v, dict) and v.get("status") == "pass" for v in reqs.values())
    )
    scs_ok = (
        isinstance(scs, dict)
        and len(scs) == 8
        and all(isinstance(v, dict) and v.get("status") == "pass" for v in scs.values())
    )
    overall_ok = (raw_overall == "passed") and reqs_ok and scs_ok
    if not overall_ok:
        findings.append(
            f"M2 report.json not fully passed (overall_status={raw_overall!r}, reqs_ok={reqs_ok}, scs_ok={scs_ok})"
        )

    raw_trace = trace.get("status")
    trace_ok = raw_trace == "pass"
    if not trace_ok:
        findings.append(f"M2 traceability.json status={raw_trace!r} != 'pass'")

    raw_audit = audit.get("audit_status")
    audit_findings = audit.get("findings")
    audit_ok = (raw_audit == "passed") and (audit_findings == [])
    if not audit_ok:
        findings.append(
            f"M2 report-audit.json did not pass cleanly: audit_status={raw_audit!r}, findings={audit_findings!r}"
        )

    src_report_sha = sha256_file(m2_artifacts_dir / "report.json")
    if audit.get("report_sha256") != src_report_sha:
        findings.append(
            f"M2 report-audit.json report_sha256 ({audit.get('report_sha256')}) != actual ({src_report_sha})"
        )

    evidence_sha256s: dict[str, str] = {}
    for fname in EVIDENCE_FILENAMES:
        src = m2_artifacts_dir / fname
        dst = regress_dir / fname
        src_sha = sha256_file(src)
        shutil.copy2(src, dst)
        dst_sha = sha256_file(dst)
        if dst_sha != src_sha:
            findings.append(f"Copied SHA-256 mismatch for {fname}: {dst_sha} != {src_sha}")
        evidence_sha256s[fname] = dst_sha

    passed = not findings and overall_ok and trace_ok and audit_ok
    summary = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if passed else "FAIL",
        "overall_status": "passed" if overall_ok else str(raw_overall or "failed"),
        "traceability_status": "pass" if trace_ok else str(raw_trace or "fail"),
        "audit_status": "passed" if audit_ok else str(raw_audit or "failed"),
        "child_run_id": child_run_id,
        "child_artifacts_dir": str(m2_artifacts_dir),
        "checked_at_utc": _utc_now(),
        "evidence_sha256s": evidence_sha256s,
        "report_sha256": evidence_sha256s["report.json"],
        "traceability_sha256": evidence_sha256s["traceability.json"],
        "report_audit_sha256": evidence_sha256s["report-audit.json"],
        "findings": findings,
    }
    atomic_write_json(regress_dir / "summary.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run full M2 cross-batch tracking regression gate sequentially for M3."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Target M3 validation run directory")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=M2_CACHE_DIR,
        help="Verified DuckOMO & WB2 cache directory (default: validation/m2/.cache)",
    )
    args = parser.parse_args(argv)

    venv_py = M2_VALIDATION_DIR / ".venv" / "bin" / "python"
    gate_script = M2_VALIDATION_DIR / "run_m2_gate.py"
    verify_script = M2_VALIDATION_DIR / "verify_report.py"
    cache_dir = args.cache_dir.resolve()

    if not venv_py.is_file():
        print(f"Error: locked M2 Python environment missing at {venv_py}", file=sys.stderr)
        return 2
    if not gate_script.is_file() or not verify_script.is_file():
        print(f"Error: M2 gate or verify_report script missing in {M2_VALIDATION_DIR}", file=sys.stderr)
        return 2

    run_dir = args.run_dir.resolve()
    regress_dir = run_dir / "m2-regression"
    regress_dir.mkdir(parents=True, exist_ok=True)

    child_run_id = f"m2-regress-{uuid.uuid4().hex[:8]}"
    print(f"Running full M2 regression gate ({child_run_id})...")

    cmd = [
        str(venv_py),
        "-B",
        str(gate_script),
        "--run-id",
        child_run_id,
        "--cache-dir",
        str(cache_dir),
    ]
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        err_tail = (proc.stderr or proc.stdout or "").strip()[-2000:]
        print(f"M2 gate failed (exit {proc.returncode}): {err_tail}", file=sys.stderr)
        fail_summary = {
            "schema_version": SCHEMA_VERSION,
            "status": "FAIL",
            "overall_status": "failed",
            "traceability_status": "fail",
            "audit_status": "failed",
            "child_run_id": child_run_id,
            "checked_at_utc": _utc_now(),
            "exit_code": proc.returncode,
            "evidence_sha256s": {},
            "error": err_tail,
        }
        atomic_write_json(regress_dir / "summary.json", fail_summary)
        return 1

    m2_artifacts_dir = REPO_ROOT / "artifacts" / "m2" / child_run_id
    if not (m2_artifacts_dir / "report-audit.json").is_file():
        audit_proc = subprocess.run(
            [str(venv_py), "-B", str(verify_script), "--run-dir", str(m2_artifacts_dir)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if audit_proc.returncode != 0:
            err_tail = (audit_proc.stderr or audit_proc.stdout or "").strip()[-2000:]
            print(f"M2 verify_report failed (exit {audit_proc.returncode}): {err_tail}", file=sys.stderr)
            fail_summary = {
                "schema_version": SCHEMA_VERSION,
                "status": "FAIL",
                "overall_status": "failed",
                "traceability_status": "fail",
                "audit_status": "failed",
                "child_run_id": child_run_id,
                "checked_at_utc": _utc_now(),
                "exit_code": audit_proc.returncode,
                "evidence_sha256s": {},
                "error": err_tail,
            }
            atomic_write_json(regress_dir / "summary.json", fail_summary)
            return 1

    summary = validate_and_summarize_m2_artifacts(m2_artifacts_dir, regress_dir, child_run_id)
    if summary["status"] != "PASS":
        print(f"M2 regression verification failed: {summary['findings']}", file=sys.stderr)
        return 1

    print(f"M2 regression gate: PASS ({child_run_id}) -> {regress_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
