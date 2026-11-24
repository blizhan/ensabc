#!/usr/bin/env python3
"""Strict negative and positive test suite for M2 evidence, schemas, and traceability gates (T045).

Usage:
    python3 -B validation/m2/test_evidence.py
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

from validation.m2.common import atomic_write_json, sha256_file
from validation.m2.schema import (
    REPORT_SCHEMA_VERSION,
    REQUIREMENT_IDS,
    SUCCESS_CRITERION_IDS,
    ValidationError,
    validate_match_result,
    validate_match_task,
    validate_report,
)


def make_valid_report_dict(run_dir: Path) -> dict[str, Any]:
    # Write a dummy artifact file
    dummy_art = run_dir / "evidence.json"
    dummy_art.write_text(json.dumps({"test": "value"}))
    art_sha = sha256_file(dummy_art)

    reqs = {}
    for rid in REQUIREMENT_IDS:
        reqs[rid] = {
            "status": "pass",
            "assertion_id": f"A_{rid}",
            "observed": "verified",
            "evidence_path": "evidence.json",
            "evidence_sha256": art_sha,
        }

    scs = {}
    for sid in SUCCESS_CRITERION_IDS:
        scs[sid] = {
            "status": "pass",
            "assertion_id": f"A_{sid}",
            "observed": "verified",
            "evidence_path": "evidence.json",
            "evidence_sha256": art_sha,
        }

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": "test-run-001",
        "created_at_utc": "2026-10-29T18:00:00Z",
        "overall_status": "passed",
        "requirements": reqs,
        "success_criteria": scs,
        "steps": [
            {"step": "setup", "status": "pass"},
            {"step": "cells", "status": "pass"},
            {"step": "real", "status": "pass"},
            {"step": "replay", "status": "pass"},
        ],
    }


class TestEvidenceAndSchema(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.tmp_dir.name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_valid_report_passes(self):
        rep = make_valid_report_dict(self.run_dir)
        # Should validate without error
        validate_report(rep)

    def test_missing_fr_fails(self):
        rep = make_valid_report_dict(self.run_dir)
        del rep["requirements"]["FR-005"]
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep)
        self.assertIn("FR-005", str(ctx.exception))

    def test_missing_sc_fails(self):
        rep = make_valid_report_dict(self.run_dir)
        del rep["success_criteria"]["SC-002"]
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep)
        self.assertIn("SC-002", str(ctx.exception))

    def test_invalid_overall_status_fails(self):
        rep = make_valid_report_dict(self.run_dir)
        rep["overall_status"] = "almost_passed"
        with self.assertRaises(ValidationError) as ctx:
            validate_report(rep)
        self.assertIn("Invalid overall status", str(ctx.exception))

    def test_nan_in_match_task_rejected(self):
        task = {
            "schema_version": "m2.match-task.v1",
            "mode": "absolute",
            "task_id": "t1",
            "prev_scenarios": [{"cluster_id": 0, "mean_features": [280.0, float("nan")]}],
            "curr_scenarios": [{"cluster_id": 0, "mean_features": [280.0, 281.0]}],
            "weights": [1.0, 1.0],
            "threshold": 1.0,
        }
        with self.assertRaises(ValidationError) as ctx:
            validate_match_task(task)
        self.assertIn("finite", str(ctx.exception).lower())

    def test_ragged_features_in_match_task_rejected(self):
        task = {
            "schema_version": "m2.match-task.v1",
            "mode": "absolute",
            "task_id": "t1",
            "prev_scenarios": [{"cluster_id": 0, "mean_features": [280.0, 281.0, 282.0]}],
            "curr_scenarios": [{"cluster_id": 0, "mean_features": [280.0, 281.0]}],
            "weights": [1.0, 1.0],
            "threshold": 1.0,
        }
        with self.assertRaises(ValidationError) as ctx:
            validate_match_task(task)
        self.assertIn("length", str(ctx.exception).lower())

    def test_duplicate_cluster_id_rejected(self):
        task = {
            "schema_version": "m2.match-task.v1",
            "mode": "absolute",
            "task_id": "t1",
            "prev_scenarios": [
                {"cluster_id": 0, "mean_features": [280.0, 281.0]},
                {"cluster_id": 0, "mean_features": [282.0, 283.0]},
            ],
            "curr_scenarios": [{"cluster_id": 0, "mean_features": [280.0, 281.0]}],
            "weights": [1.0, 1.0],
            "threshold": 1.0,
        }
        with self.assertRaises(ValidationError) as ctx:
            validate_match_task(task)
        self.assertIn("duplicate", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
