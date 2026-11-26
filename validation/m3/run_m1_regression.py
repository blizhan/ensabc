#!/usr/bin/env python3
"""Sequential M1 scenario computation gate regression wrapper for M3 (T041).

Executes full `validation/m1/run_m1_gate.py --run-id m1-regress-<uuid8>`,
verifies `report.json` (`overall == "passed"` / `overall_status == "passed"`),
`traceability.json` (`status == "pass"`), and `report-audit.json` (`passed`),
copies those 3 files into `<run_dir>/m1-regression/`, computes SHA-256 digests,
and writes `<run_dir>/m1-regression/summary.json`.

MUST be executed sequentially before (never concurrently with) `run_m2_regression.py`
because both gates build into `/tmp/ensabc-m1-extension-target`.
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
    atomic_write_json,
    load_json,
    sha256_file,
)

SCHEMA_VERSION = "m3.m1_regression.v1"
EVIDENCE_FILENAMES: tuple[str, ...] = (
    "report.json",
    "traceability.json",
    "report-audit.json",
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_and_summarize_m1_artifacts(
    m1_artifacts_dir: Path,
    regress_dir: Path,
    child_run_id: str,
) -> dict[str, Any]:
    """Validate M1 gate evidence files, copy them to `regress_dir`, and write `summary.json`."""
    regress_dir.mkdir(parents=True, exist_ok=True)
    findings: list[str] = []

    for fname in EVIDENCE_FILENAMES:
        if not (m1_artifacts_dir / fname).is_file():
            findings.append(f"Missing required M1 gate artifact: {m1_artifacts_dir / fname}")

    if findings:
        summary = {
            "schema_version": SCHEMA_VERSION,
            "status": "FAIL",
            "overall_status": "failed",
            "traceability_status": "fail",
            "audit_status": "failed",
            "child_run_id": child_run_id,
            "child_artifacts_dir": str(m1_artifacts_dir),
            "checked_at_utc": _utc_now(),
            "evidence_sha256s": {},
            "findings": findings,
        }
        atomic_write_json(regress_dir / "summary.json", summary)
        return summary

    report = load_json(m1_artifacts_dir / "report.json")
    trace = load_json(m1_artifacts_dir / "traceability.json")
    audit = load_json(m1_artifacts_dir / "report-audit.json")

    # M1 `m1.report.v1` uses `"overall": "passed"` (accept `"overall_status"` as well)
    raw_overall = report.get("overall_status") if "overall_status" in report else report.get("overall")
    reqs = report.get("requirements", [])
    scs = report.get("success_criteria", [])
    reqs_ok = isinstance(reqs, list) and len(reqs) == 17 and all(r.get("status") == "pass" for r in reqs)
    scs_ok = isinstance(scs, list) and len(scs) == 8 and all(s.get("status") == "pass" for s in scs)
    overall_ok = (raw_overall == "passed") and reqs_ok and scs_ok
    if not overall_ok:
        findings.append(
            f"M1 report.json not fully passed (overall={raw_overall!r}, reqs_ok={reqs_ok}, scs_ok={scs_ok})"
        )

    # M1 `m1.traceability.v1` uses `"status": "pass"`
    raw_trace = trace.get("status")
    trace_ok = raw_trace == "pass"
    if not trace_ok:
        findings.append(f"M1 traceability.json status={raw_trace!r} != 'pass'")

    # M1 `m1.report-audit.v1` uses `"status": "pass"` + `"derived_overall": "passed"` + `"findings": []`
    audit_findings = audit.get("findings")
    if "audit_status" in audit:
        audit_ok = audit.get("audit_status") == "passed" and audit_findings == []
        raw_audit_norm = "passed" if audit_ok else str(audit.get("audit_status") or "failed")
    else:
        audit_ok = (
            audit.get("status") == "pass"
            and audit.get("derived_overall") == "passed"
            and audit_findings == []
        )
        raw_audit_norm = "passed" if audit_ok else "failed"
    if not audit_ok:
        findings.append(
            f"M1 report-audit.json did not pass cleanly: status={audit.get('status')!r}, "
            f"derived_overall={audit.get('derived_overall')!r}, findings={audit_findings!r}"
        )

    src_report_sha = sha256_file(m1_artifacts_dir / "report.json")
    if audit.get("report_sha256") != src_report_sha:
        findings.append(
            f"M1 report-audit.json report_sha256 ({audit.get('report_sha256')}) != actual ({src_report_sha})"
        )

    evidence_sha256s: dict[str, str] = {}
    for fname in EVIDENCE_FILENAMES:
        src = m1_artifacts_dir / fname
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
        "audit_status": raw_audit_norm,
        "child_run_id": child_run_id,
        "child_artifacts_dir": str(m1_artifacts_dir),
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
        description="Run full M1 scenario computation regression gate sequentially for M3."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Target M3 validation run directory")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=M2_CACHE_DIR,
        help="Shared cache directory (accepted for CLI uniformity with run_m2_regression.py)",
    )
    args = parser.parse_args(argv)

    gate_script = REPO_ROOT / "validation" / "m1" / "run_m1_gate.py"
    m0_payload = REPO_ROOT / "validation" / "m0" / "fixtures" / "real_ifs_ens_20200101_t2m_data.json"
    if not gate_script.is_file():
        print(f"Error: M1 gate script not found: {gate_script}", file=sys.stderr)
        return 2
    if not m0_payload.is_file():
        print(
            f"Error: M0 frozen sample payload missing at {m0_payload} (run bash validation/m3/setup_env.sh first)",
            file=sys.stderr,
        )
        return 2

    run_dir = args.run_dir.resolve()
    regress_dir = run_dir / "m1-regression"
    regress_dir.mkdir(parents=True, exist_ok=True)

    child_run_id = f"m1-regress-{uuid.uuid4().hex[:8]}"
    venv_py = REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"
    python_bin = str(venv_py) if venv_py.is_file() else sys.executable

    print(f"Running full M1 regression gate ({child_run_id})...")
    proc = subprocess.run(
        [python_bin, "-B", str(gate_script), "--run-id", child_run_id],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        err_tail = (proc.stderr or proc.stdout or "").strip()[-2000:]
        print(f"M1 gate failed (exit {proc.returncode}): {err_tail}", file=sys.stderr)
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

    m1_artifacts_dir = REPO_ROOT / "artifacts" / "m1" / child_run_id
    summary = validate_and_summarize_m1_artifacts(m1_artifacts_dir, regress_dir, child_run_id)
    if summary["status"] != "PASS":
        print(f"M1 regression verification failed: {summary['findings']}", file=sys.stderr)
        return 1

    print(f"M1 regression gate: PASS ({child_run_id}) -> {regress_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
