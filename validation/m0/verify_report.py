#!/usr/bin/env python3
"""Audit an `m0.report.v1` gate report independently of the gate itself.

The auditor re-derives the verdict from the recorded evidence instead of
trusting the gate's own conclusion. It checks that:

* the report satisfies the `m0.report.v1` schema;
* FR-001 to FR-014 each appear exactly once with a valid status;
* the overall status follows the aggregation rule: a failure makes the report
  `failed`, an unexecuted requirement makes it `incomplete`, and only an
  all-pass set yields `passed`;
* every artifact the report references exists and its content hash matches the
  recorded hash;
* the evidence bundle carries the files the evidence contract requires.

The auditor also runs a self-test on synthetic reports, so that a reviewer can
confirm that it really does refuse a `passed` verdict when a requirement
failed, when a requirement was not run, or when only the synthetic sample was
used. A synthetic-only pass must never be reported as an M0 pass.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import M0Error, read_json, sha256_file, write_json  # noqa: E402
from schema import (  # noqa: E402
    REQUIREMENT_IDS,
    ValidationError,
    aggregate_overall,
    validate_gate_report,
)

REQUIRED_ARTIFACTS = (
    "environment.json",
    "source-hashes.json",
    "sample-validation.json",
    "sample-recovery.json",
    "reference/run-1.json",
    "reference/run-2.json",
    "reference/run-3.json",
    "reference/comparison.json",
    "reference/comparator-self-check.json",
    "exchange.json",
    "measurements.json",
    "minimal-runtime.json",
    "independent-reproduction.json",
    "steps.json",
    "traceability.json",
    "report.json",
)


def _synthetic_only_findings(report: dict[str, Any]) -> list[str]:
    """Return the findings for a report that only covers synthetic material.

    A gate that only exercised the synthetic samples has not demonstrated the
    real-sample requirements, so the report must not be accepted as an M0
    pass. The check looks at the recorded scope, because that is where the
    distinction is recorded.
    """
    findings: list[str] = []
    scopes = " ".join(
        str(record.get("scope", "")).lower() for record in report["requirements"].values()
    )
    if "synthetic" in scopes:
        findings.append(
            "the report only covers synthetic material, so it does not demonstrate the "
            "real-sample requirements and must not be reported as an M0 pass"
        )
    return findings


def audit_report(run_dir: Path) -> dict[str, Any]:
    """Audit the report in `run_dir` and return the audit result."""
    report_path = run_dir / "report.json"
    if not report_path.is_file():
        raise M0Error(f"the gate report is missing: {report_path}")

    report = read_json(report_path)
    summary = validate_gate_report(report)

    findings: list[str] = []

    # The overall status must follow the aggregation rule.
    derived = aggregate_overall(report["requirements"])
    if derived != report["overall"]:
        findings.append(
            f"the recorded overall status {report['overall']!r} does not follow from the "
            f"requirement statuses, which aggregate to {derived!r}"
        )

    # A synthetic-only pass must never be reported as an M0 pass.
    sample_validation = run_dir / "sample-validation.json"
    if sample_validation.is_file():
        try:
            record = read_json(sample_validation)
        except (M0Error, ValueError) as error:
            record = None
            findings.append(f"the sample validation record is not readable JSON: {error}")
        if record is not None:
            if record.get("status") == "pass" and record.get("kind") != "real_ensemble":
                findings.append(
                    "the sample validation did not use a real ensemble sample, so the M0 gate "
                    "must not be reported as passed"
                )
    else:
        findings.append("the sample validation record is missing")

    findings.extend(_synthetic_only_findings(report))

    # The traceability matrix re-derives every requirement verdict from the
    # evidence. When it is present, its verdict must agree with the report.
    traceability_path = run_dir / "traceability.json"
    if traceability_path.is_file():
        try:
            traceability = read_json(traceability_path)
        except (M0Error, ValueError) as error:
            traceability = None
            findings.append(f"the traceability record is not readable JSON: {error}")
        if traceability is not None:
            if traceability.get("status") != "pass":
                findings.append(
                    "the traceability matrix does not support the report: "
                    f"{traceability.get('failed_assertions')}"
                )
            for requirement_id, record in report["requirements"].items():
                derived = traceability.get("matrix", {}).get(requirement_id, {}).get("status")
                if derived is None:
                    continue
                if record["status"] == "pass" and derived != "pass":
                    findings.append(
                        f"{requirement_id}: the report says 'pass' but the traceability matrix "
                        "does not support it"
                    )
                elif record["status"] == "fail" and derived == "pass":
                    findings.append(
                        f"{requirement_id}: the report says 'fail' but the traceability matrix "
                        "supports it"
                    )

    # The required artifacts must exist.
    missing = [name for name in REQUIRED_ARTIFACTS if not (run_dir / name).is_file()]
    if missing:
        findings.append(f"the required evidence file(s) are missing: {missing}")

    # Every artifact the report references must resolve, and its recorded
    # content hash must match the file on disk.
    unresolved: list[str] = []
    mismatched: list[str] = []
    for requirement_id, record in report["requirements"].items():
        for artifact in record["artifacts"]:
            relative = artifact["path"]
            if relative == "report.json":
                continue
            target = run_dir / relative
            if not target.is_file():
                unresolved.append(f"{requirement_id} -> {relative}")
                continue
            actual = sha256_file(target)
            if actual != artifact["sha256"]:
                mismatched.append(
                    f"{requirement_id} -> {relative}: recorded {artifact['sha256']}, found {actual}"
                )
    if unresolved:
        findings.append(f"the referenced artifact(s) do not resolve: {unresolved}")
    if mismatched:
        findings.append(f"the referenced artifact hash(es) do not match: {mismatched}")

    return {
        "schema_version": "m0.report-audit.v1",
        "run_dir": str(run_dir),
        "report_path": str(report_path),
        "report_sha256": sha256_file(report_path),
        "summary": summary,
        "derived_overall": derived,
        "findings": findings,
        "status": "pass" if not findings else "fail",
    }


def build_synthetic_report(overall: str, requirements: dict[str, Any]) -> dict[str, Any]:
    """Build a report with the given overall status and requirement records."""
    records = {}
    for requirement_id in REQUIREMENT_IDS:
        status = requirements.get(requirement_id, "pass")
        records[requirement_id] = {
            "requirement_id": requirement_id,
            "status": status,
            "scope": "the self-test scope",
            "input_hashes": {"synthetic": "synthetic"},
            "configuration_hash": "synthetic",
            "environment": {"synthetic": "synthetic"},
            "steps": ["synthetic: pass"],
            "expected": "the self-test expectation",
            "actual": "the self-test result",
            "artifacts": [],
            "limitations": [],
        }
    return {
        "schema_version": "m0.report.v1",
        "run_id": "synthetic-self-test",
        "created_at": "2026-01-01T00:00:00Z",
        "overall": overall,
        "requirements": records,
        "limitations": [],
        "m1_handoff": {
            "sample_contract": "the self-test contract",
            "reference_config": "the self-test reference configuration",
            "comparison_rules": "the self-test rules",
            "version_combination": "the self-test version combination",
            "verified_environment": "the self-test environment",
            "dependency_decisions": "the self-test decisions",
        },
    }


def run_self_checks() -> dict[str, Any]:
    """Check that the auditor refuses the verdicts it must refuse."""
    checks: list[dict[str, Any]] = []

    def expect(case_id: str, description: str, report: dict[str, Any], should_pass: bool) -> None:
        findings: list[str] = []
        try:
            validate_gate_report(report)
        except ValidationError as error:
            findings.append(str(error))
        derived = aggregate_overall(report["requirements"])
        if derived != report["overall"]:
            findings.append(
                f"the recorded overall status {report['overall']!r} does not follow from the "
                f"requirement statuses, which aggregate to {derived!r}"
            )
        findings.extend(_synthetic_only_findings(report))
        accepted = not findings
        checks.append(
            {
                "case_id": case_id,
                "description": description,
                "expected": "accepted" if should_pass else "rejected",
                "actual": "accepted" if accepted else "rejected",
                "correct": accepted == should_pass,
                "findings": findings,
            }
        )

    expect(
        "all_pass",
        "a report whose requirements all passed is accepted",
        build_synthetic_report("passed", {}),
        True,
    )
    expect(
        "one_failure",
        "a report that claims 'passed' while a requirement failed is rejected",
        build_synthetic_report("passed", {"FR-001": "fail"}),
        False,
    )
    expect(
        "one_not_run",
        "a report that claims 'passed' while a requirement was not run is rejected",
        build_synthetic_report("passed", {"FR-013": "not_run"}),
        False,
    )
    expect(
        "failed_overall",
        "a report that correctly says 'failed' is accepted",
        build_synthetic_report("failed", {"FR-001": "fail"}),
        True,
    )
    expect(
        "incomplete_overall",
        "a report that correctly says 'incomplete' is accepted",
        build_synthetic_report("incomplete", {"FR-013": "not_run"}),
        True,
    )

    # The synthetic-only pass must not be accepted as an M0 pass.
    synthetic_only = copy.deepcopy(build_synthetic_report("passed", {}))
    for record in synthetic_only["requirements"].values():
        record["scope"] = "synthetic only; the real ensemble sample was not exercised"
    expect(
        "synthetic_only_pass",
        "a report whose scope is synthetic only is rejected as an M0 pass",
        synthetic_only,
        False,
    )
    return {"all_correct": all(check["correct"] for check in checks), "checks": checks}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-dir",
        type=Path,
        help="the run directory that holds report.json",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run the auditor's own gate logic self-checks",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the audit result to this JSON file",
    )
    arguments = parser.parse_args()

    if arguments.self_test:
        result = run_self_checks()
        if arguments.output:
            write_json(arguments.output, result)
        for check in result["checks"]:
            marker = "ok" if check["correct"] else "BROKEN"
            print(f"[{marker}] {check['case_id']}: expected {check['expected']}, got {check['actual']}")
        if not result["all_correct"]:
            print("FAIL: the auditor self-checks did not behave as required")
            return 1
        print("PASS: auditor self-checks")
        return 0

    if not arguments.run_dir:
        parser.error("--run-dir is required unless --self-test is given")

    try:
        result = audit_report(arguments.run_dir)
    except (M0Error, ValidationError) as error:
        result = {
            "schema_version": "m0.report-audit.v1",
            "run_dir": str(arguments.run_dir),
            "status": "fail",
            "findings": [str(error)],
        }
        if arguments.output:
            write_json(arguments.output, result)
        print(f"FAIL: {error}")
        return 1
    except (ValueError, OSError) as error:
        # A report or an evidence file that cannot be parsed is a finding in
        # its own right, not a crash of the auditor.
        result = {
            "schema_version": "m0.report-audit.v1",
            "run_dir": str(arguments.run_dir),
            "status": "fail",
            "findings": [f"the evidence bundle could not be read: {error}"],
        }
        if arguments.output:
            write_json(arguments.output, result)
        print(f"FAIL: {result['findings'][0]}")
        return 1

    if arguments.output:
        write_json(arguments.output, result)

    print(f"report audit: {result['status']}")
    if "summary" in result:
        print(f"  run id:      {result['summary']['run_id']}")
        print(f"  overall:     {result['summary']['overall']} (derived {result['derived_overall']})")
        counts = result["summary"]["status_counts"]
        print(f"  requirements: pass {counts['pass']}, fail {counts['fail']}, not_run {counts['not_run']}")
    for finding in result["findings"]:
        print(f"  finding: {finding}")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
