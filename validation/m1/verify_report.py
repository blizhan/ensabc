#!/usr/bin/env python3
"""Audit an m1.report.v1 gate report independently of the gate itself.

Usage:
    python3 validation/m1/verify_report.py --run-dir DIR
    python3 validation/m1/verify_report.py --self-check

Checks:
- Schema conformance of report.json
- Exact 17 FR and 8 SC coverage and agreement with traceability.json
- Strict overall status aggregation
- Content hashes of all referenced artifacts
- Writes <run-dir>/report-audit.json adhering to m1.report-audit.v1 schema
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import M1Error, read_json, sha256_file, utc_now, write_json
from validation.m1.schema import (
    AUDIT_SCHEMA_VERSION,
    REQUIREMENT_IDS,
    SUCCESS_CRITERION_IDS,
    validate_report,
)

REQUIRED_RUN_ARTIFACTS = [
    "environment.json",
    "source-identity.json",
    "inputs/task.json",
    "inputs/identity.json",
    "config/native.json",
    "reference/original/run-1.json",
    "reference/original/run-2.json",
    "reference/original/run-3.json",
    "reference/shared/initialization.json",
    "reference/shared/trace.json",
    "reference/shared/observer-check.json",
    "core/run-1.json",
    "core/run-2.json",
    "core/run-3.json",
    "core/synthetic.json",
    "core/shared.json",
    "core/shared-trace.json",
    "comparison/projection-alignment.json",
    "comparison/original.json",
    "comparison/shared.json",
    "comparison/self-check.json",
    "extension.json",
    "minimal-runtime.json",
    "measurements.json",
    "independent-reproduction.json",
    "reproduction/child/reproduction-child.json",
    "steps.json",
    "traceability.json",
    "report.json",
]


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit M1 gate report.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-dir", type=Path, help="Target run directory to audit")
    group.add_argument("--self-check", action="store_true", help="Run auditor self-checks")
    return parser.parse_args(args)


def audit_report(run_dir: Path) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    report_path = run_dir / "report.json"
    if not report_path.is_file():
        raise M1Error(f"Missing gate report: {report_path}", code="MISSING_REPORT")

    report = read_json(report_path)
    validate_report(report, run_dir)

    from validation.m1.traceability import run_traceability

    findings: list[str] = []
    recomputed = run_traceability(run_dir, write=False)

    # 1. Verify required artifacts exist
    missing = [name for name in REQUIRED_RUN_ARTIFACTS if not (run_dir / name).is_file()]
    if missing:
        findings.append(f"Required artifact(s) missing: {missing}")

    # 2. Check traceability agreement
    trace_path = run_dir / "traceability.json"
    if trace_path.is_file():
        trace = read_json(trace_path)
        for field in ("status", "coverage", "assertions", "requirement_results", "success_criteria_results"):
            if trace.get(field) != recomputed[field]:
                findings.append(f"Traceability {field} differs from reevaluated evidence")
        if trace.get("status") != "pass" and report.get("overall") == "passed":
            findings.append("Traceability status is not pass, but report is passed")

        for r in report["requirements"]:
            rid = r["id"]
            trace_r = recomputed["requirement_results"].get(rid, {})
            if trace_r.get("status") != r["status"]:
                findings.append(f"Requirement {rid} status mismatch: report={r['status']} vs trace={trace_r.get('status')}")

        for sc in report["success_criteria"]:
            scid = sc["id"]
            trace_sc = recomputed["success_criteria_results"].get(scid, {})
            if trace_sc.get("status") != sc["status"]:
                findings.append(f"Success criterion {scid} status mismatch: report={sc['status']} vs trace={trace_sc.get('status')}")

    # 3. Check artifact references sha256
    all_refs = [
        report["source_identity_ref"],
        report["environment_ref"],
        report["steps_ref"],
        report["traceability_ref"],
    ] + report.get("input_refs", []) + report.get("config_refs", [])

    for r in report["requirements"]:
        all_refs.extend(r.get("artifacts", []))
    for sc in report["success_criteria"]:
        all_refs.extend(sc.get("artifacts", []))

    for ref in all_refs:
        rel = ref["path"]
        target = run_dir / rel
        if not target.is_file():
            findings.append(f"Referenced artifact not found: {rel}")
            continue
        actual_hash = sha256_file(target)
        if actual_hash != ref["sha256"]:
            findings.append(f"Hash mismatch for {rel}: recorded {ref['sha256']} != actual {actual_hash}")

    # 4. Check overall status aggregation
    any_fail = any(r["status"] == "fail" for r in report["requirements"]) or any(s["status"] == "fail" for s in report["success_criteria"])
    any_incomplete = any(r["status"] == "not_run" for r in report["requirements"]) or any(s["status"] == "not_run" for s in report["success_criteria"])

    if any_fail:
        derived_overall = "failed"
    elif any_incomplete:
        derived_overall = "incomplete"
    else:
        derived_overall = "passed"

    if derived_overall != report["overall"]:
        findings.append(f"Overall status mismatch: report={report['overall']} != derived={derived_overall}")

    audit_status = "pass" if not findings else "fail"
    report_sha = sha256_file(report_path)

    audit_record = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "status": audit_status,
        "run_dir": str(run_dir),
        "report_sha256": report_sha,
        "derived_overall": derived_overall,
        "findings": findings,
        "created_at": utc_now(),
    }

    out_file = run_dir / "report-audit.json"
    write_json(out_file, audit_record)
    return audit_record


def run_self_check() -> int:
    """Verify that bad evidence and false passes are detected."""
    import unittest
    from validation.m1.test_evidence import EvidenceNegativeTests

    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(EvidenceNegativeTests))
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    try:
        args = parse_args()
    except SystemExit:
        return 2

    if args.self_check:
        return run_self_check()

    try:
        res = audit_report(args.run_dir)
        print(f"Report audit status: {res['status']}")
        if res["findings"]:
            for f in res["findings"]:
                print(f"  finding: {f}")
        return 0 if res["status"] == "pass" else 1
    except M1Error as e:
        print(f"Audit error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
