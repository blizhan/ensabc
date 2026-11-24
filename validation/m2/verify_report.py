#!/usr/bin/env python3
"""Report generator and independent read-only auditor for M2 gate (T051).

Usage:
    python3 -B validation/m2/verify_report.py --run-dir DIR
    python3 -B validation/m2/verify_report.py --self-check
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import atomic_write_json, load_json, sha256_file
from validation.m2.schema import (
    AUDIT_SCHEMA_VERSION,
    REPORT_SCHEMA_VERSION,
    REQUIREMENT_IDS,
    SUCCESS_CRITERION_IDS,
    ValidationError,
    validate_report,
)


REQUIRED_STEPS = {
    "setup_env", "build_extension", "prepare_om_sample", "test_om_sample",
    "verify_matching_core", "verify_sql_and_history_synthetic", "sql_contract_tests",
    "verify_om_paths_cells", "verify_sql_and_history_real", "verify_om_paths_replay",
    "test_stability", "run_case_and_sensitivity", "verify_minimal_runtime", "measure_pipeline",
    "run_m1_regression", "reproduce_independently", "traceability",
}


def verified_execution_steps(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "execution-steps.json"
    if not path.is_file():
        return []
    return [s for s in load_json(path).get("steps", []) if s.get("step") != "verify_report"]


def execution_complete(steps: list[dict[str, Any]]) -> bool:
    return all(any(s.get("step") == name and s.get("status") == "pass"
                   and s.get("exit_code") == 0 for s in steps) for name in REQUIRED_STEPS)


def generate_report(run_dir: Path) -> dict[str, Any]:
    trace_path = run_dir / "traceability.json"
    if not trace_path.is_file():
        raise FileNotFoundError(f"Traceability matrix missing: {trace_path}")

    trace_doc = load_json(trace_path)
    run_id = trace_doc.get("run_id", run_dir.name)

    reqs = trace_doc.get("requirements", {})
    scs = trace_doc.get("success_criteria", {})

    all_req_pass = all(v.get("status") == "pass" for v in reqs.values()) and len(reqs) == len(REQUIREMENT_IDS)
    all_sc_pass = all(v.get("status") == "pass" for v in scs.values()) and len(scs) == len(SUCCESS_CRITERION_IDS)
    steps = verified_execution_steps(run_dir)
    overall = "passed" if (all_req_pass and all_sc_pass and execution_complete(steps)) else "failed"

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": run_id,
        "created_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_status": overall,
        "requirements": reqs,
        "success_criteria": scs,
        "steps": steps,
    }

    report_path = run_dir / "report.json"
    atomic_write_json(report_path, report)
    return report


def audit_report(run_dir: Path) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    report_path = run_dir / "report.json"
    if not report_path.is_file():
        raise FileNotFoundError(f"Report missing: {report_path}")

    report = load_json(report_path)
    validate_report(report)

    audit_findings = []
    execution = verified_execution_steps(run_dir)
    if not execution_complete(execution):
        audit_findings.append("Required execution records missing, incomplete or failed")
    if report["steps"] != execution:
        audit_findings.append("Report steps differ from actual execution records")
    from validation.m2.traceability import TraceabilityEvaluator
    evaluator = TraceabilityEvaluator(run_dir)
    for section, ids, evaluate in (("requirements", REQUIREMENT_IDS, evaluator.eval_fr),
                                  ("success_criteria", SUCCESS_CRITERION_IDS, evaluator.eval_sc)):
        for item_id in ids:
            status, _, _, _ = evaluate(item_id)
            if report[section][item_id]["status"] != status:
                audit_findings.append(f"{item_id} status disagrees with evidence assertions")
    audited_files_count = 0

    # 1. Verify all evidence paths stay inside run_dir and match SHA-256 hashes
    for section_name, section in (("requirements", report["requirements"]), ("success_criteria", report["success_criteria"])):
        for item_id, item in section.items():
            ev_rel = item.get("evidence_path")
            ev_sha = item.get("evidence_sha256")
            if not ev_rel:
                audit_findings.append(f"{item_id} has no evidence_path")
                continue
            ev_full = (run_dir / ev_rel).resolve()
            if not ev_full.is_relative_to(run_dir):
                audit_findings.append(f"{item_id} evidence_path escapes run_dir: {ev_rel}")
                continue
            if not ev_full.is_file():
                audit_findings.append(f"{item_id} evidence file not found: {ev_rel}")
                continue
            actual_sha = sha256_file(ev_full)
            if actual_sha != ev_sha:
                audit_findings.append(f"{item_id} evidence SHA-256 mismatch for {ev_rel}: expected {ev_sha}, found {actual_sha}")
            audited_files_count += 1

    # 2. Verify all requirements and success criteria pass
    for rid in REQUIREMENT_IDS:
        if report["requirements"].get(rid, {}).get("status") != "pass":
            audit_findings.append(f"Requirement {rid} status is not pass")

    for sid in SUCCESS_CRITERION_IDS:
        if report["success_criteria"].get(sid, {}).get("status") != "pass":
            audit_findings.append(f"Success criterion {sid} status is not pass")

    expected_overall = "passed" if not audit_findings else "failed"
    if report["overall_status"] != expected_overall:
        audit_findings.append(f"Report overall_status '{report['overall_status']}' contradicts expected '{expected_overall}'")

    audit_status = "passed" if not audit_findings else "failed"

    audit_record = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "run_id": report["run_id"],
        "audited_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "audit_status": audit_status,
        "report_sha256": sha256_file(report_path),
        "audited_evidence_count": audited_files_count,
        "findings": audit_findings,
    }

    audit_path = run_dir / "report-audit.json"
    atomic_write_json(audit_path, audit_record)
    return audit_record


def self_check() -> int:
    import unittest
    suite = unittest.defaultTestLoader.loadTestsFromName("validation.m2.test_review_regressions")
    return 0 if unittest.TextTestRunner().run(suite).wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="M2 report generator and independent auditor")
    parser.add_argument("--run-dir", type=Path, default=None, help="Target run directory")
    parser.add_argument("--self-check", action="store_true", help="Run auditor self-checks")
    args = parser.parse_args()

    if args.self_check:
        return self_check()

    if not args.run_dir:
        print("Error: --run-dir required", file=sys.stderr)
        return 2

    run_dir = args.run_dir
    report_file = run_dir / "report.json"
    if not report_file.is_file():
        generate_report(run_dir)

    audit_res = audit_report(run_dir)
    print(f"Report audit: {audit_res['audit_status'].upper()} ({len(audit_res['findings'])} findings)")
    return 0 if audit_res["audit_status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
