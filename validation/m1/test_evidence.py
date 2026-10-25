#!/usr/bin/env python3
"""Strict negative test suite for M1 schemas, evidence records, traceability, and auditor.

Usage:
    python3 validation/m1/test_evidence.py
    or: python3 -m unittest discover -s validation/m1 -p 'test_*.py'
"""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
import sys
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import sha256_file, write_json
from validation.m1.schema import (
    REPORT_SCHEMA_VERSION,
    REQUIREMENT_IDS,
    SUCCESS_CRITERION_IDS,
    ValidationError,
    validate_report,
    validate_traceability_matrix,
)
from validation.m1.verify_report import audit_report


def make_valid_report_dict(run_dir: Path) -> dict[str, Any]:
    # Create required files in run_dir
    files = [
        "source-identity.json",
        "environment.json",
        "inputs/task.json",
        "inputs/identity.json",
        "config/native.json",
        "steps.json",
        "traceability.json",
    ]
    for rel in files:
        p = run_dir / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        write_json(p, {"dummy": "data"})

    def ref(p: str) -> dict[str, str]:
        return {"path": p, "sha256": sha256_file(run_dir / p)}

    reqs = []
    for rid in REQUIREMENT_IDS:
        reqs.append({
            "id": rid,
            "status": "pass",
            "assertion_ids": ["A001"],
            "expected": "ok",
            "observed": "ok",
            "artifacts": [ref("inputs/task.json")],
            "reason": None,
        })

    scs = []
    for scid in SUCCESS_CRITERION_IDS:
        scs.append({
            "id": scid,
            "status": "pass",
            "assertion_ids": ["A001"],
            "expected": "ok",
            "observed": "ok",
            "artifacts": [ref("inputs/task.json")],
            "reason": None,
        })

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": "test-run",
        "created_at": "2026-10-24T00:00:00Z",
        "overall": "passed",
        "algorithm_version": "m1.native.v1",
        "source_identity_ref": ref("source-identity.json"),
        "environment_ref": ref("environment.json"),
        "input_refs": [ref("inputs/task.json"), ref("inputs/identity.json")],
        "config_refs": [ref("config/native.json")],
        "steps_ref": ref("steps.json"),
        "traceability_ref": ref("traceability.json"),
        "requirements": reqs,
        "success_criteria": scs,
        "limitations": ["Test limitation."],
    }


class EvidenceNegativeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_auditor_rejects_consistent_false_pass(self):
        from validation.m1.traceability import run_traceability
        rep = make_valid_report_dict(self.run_dir)
        trace = run_traceability(self.run_dir)
        trace["status"] = "pass"
        for section in ("requirement_results", "success_criteria_results"):
            for record in trace[section].values():
                record["status"] = "pass"
        write_json(self.run_dir / "traceability.json", trace)
        rep["traceability_ref"]["sha256"] = sha256_file(self.run_dir / "traceability.json")
        write_json(self.run_dir / "report.json", rep)
        result = audit_report(self.run_dir)
        self.assertEqual(result["status"], "fail")
        self.assertTrue(any("reevaluated evidence" in f for f in result["findings"]))
        self.assertEqual(json.loads((self.run_dir / "traceability.json").read_text()), trace)

    def test_missing_fr_rejected(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["requirements"].pop()  # 16 instead of 17
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("must contain exactly 17 entries", str(ctx.exception))

    def test_missing_sc_rejected(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["success_criteria"].pop()  # 7 instead of 8
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("must contain exactly 8 entries", str(ctx.exception))

    def test_overall_passed_with_failed_requirement_rejected(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["requirements"][0]["status"] = "fail"
        rep["requirements"][0]["reason"] = "something broke"
        rep["overall"] = "passed"
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("items failed but overall is passed", str(ctx.exception))

    def test_overall_passed_with_not_run_requirement_rejected(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["requirements"][0]["status"] = "not_run"
        rep["requirements"][0]["reason"] = "skipped"
        rep["overall"] = "passed"
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("items incomplete but overall is passed", str(ctx.exception))

    def test_path_traversal_ref_rejected(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["input_refs"][0]["path"] = "../secret.json"
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("cannot contain '..'", str(ctx.exception))

    def test_absolute_path_ref_rejected(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["input_refs"][0]["path"] = "/etc/passwd"
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("cannot be absolute", str(ctx.exception))

    def test_hash_mismatch_detected_by_auditor(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["input_refs"][0]["sha256"] = "0" * 64
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep, self.run_dir)
        self.assertIn("sha256 mismatch", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
