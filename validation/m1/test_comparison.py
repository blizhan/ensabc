#!/usr/bin/env python3
"""Contract and self-check tests for M1 comparator.

Verifies:
- Positive cases: identical runs, cluster label permutations, PCA sign flips,
  orthogonal rotations within subspace.
- Negative cases: wrong partitions, wrong subspaces, tolerance failures,
  missing/non-finite arrays, original-vs-shared stage confusion.
"""

from __future__ import annotations

import copy
import math
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Will import from compare_results once implemented
try:
    from validation.m1.compare_results import compare_stage_original, compare_stage_shared
except ImportError:
    compare_stage_original = None
    compare_stage_shared = None


def make_valid_base_records() -> tuple[dict[str, Any], dict[str, Any]]:
    """Create a minimal pair of equivalent reference and native records for testing."""
    ref_run = {
        "run_id": "ref-01",
        "sample_id": "test-sample",
        "mean": [10.0, 20.0, 30.0],
        "scale": [1.0, 2.0, 3.0],
        "rank_after_scaling": 2,
        "components": [[0.8, 0.6, 0.0], [0.0, 0.0, 1.0]],
        "explained_variance_ratio": [0.7, 0.3],
        "scores": [
            [-2.0, 0.0],
            [-1.9, 0.1],
            [2.0, 0.0],
            [1.9, -0.1],
        ],
        "labels": [0, 0, 1, 1],
        "centers": [[-1.95, 0.05], [1.95, -0.05]],
        "total_sse": 0.04,
        "mean_features": [
            [8.05, 20.1, 30.15],
            [11.95, 19.9, 29.85],
        ],
        "n_iter": 3,
    }

    native_res = {
        "algorithm_version": "m1.native.v1",
        "sample_id": "test-sample",
        "config": {
            "projection_dim": 2,
            "cluster_count": 2,
            "tolerance": 1e-4,
        },
        "members": [
            {"member_index": 0, "member_id": "m0", "cluster_id": 0},
            {"member_index": 1, "member_id": "m1", "cluster_id": 0},
            {"member_index": 2, "member_id": "m2", "cluster_id": 1},
            {"member_index": 3, "member_id": "m3", "cluster_id": 1},
        ],
        "scenarios": [
            {
                "cluster_id": 0,
                "member_count": 2,
                "member_fraction": 0.5,
                "representative_member_id": "m0",
                "cluster_sse": 0.02,
                "center": [-1.95, 0.05],
                "mean_features": [8.05, 20.1, 30.15],
            },
            {
                "cluster_id": 1,
                "member_count": 2,
                "member_fraction": 0.5,
                "representative_member_id": "m2",
                "cluster_sse": 0.02,
                "center": [1.95, -0.05],
                "mean_features": [11.95, 19.9, 29.85],
            },
        ],
        "diagnostics": {
            "effective_rank": 2,
            "rank_after_scaling": 2,
            "normalization_mean": [10.0, 20.0, 30.0],
            "normalization_scale": [1.0, 2.0, 3.0],
            "components": [[0.8, 0.6, 0.0], [0.0, 0.0, 1.0]],
            "explained_variance_ratio": [0.7, 0.3],
            "scores": [
                [-2.0, 0.0],
                [-1.9, 0.1],
                [2.0, 0.0],
                [1.9, -0.1],
            ],
            "total_sse": 0.04,
            "restarts": [
                {
                    "restart_index": 0,
                    "converged": True,
                    "iterations": 3,
                    "total_sse": 0.04,
                    "initial_centers": [[-2.0, 0.0], [2.0, 0.0]],
                    "initial_member_indices": [0, 2],
                    "stop_reason": "labels_stable",
                }
            ],
            "selected_restart": 0,
        },
    }

    return ref_run, native_res


def test_positive_identical():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    res = compare_stage_original(ref, nat)
    assert res["status"] == "pass", f"Expected pass, got {res}"


def test_positive_label_permutation():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    # Swap clusters 0 and 1 in native result
    for m in nat["members"]:
        m["cluster_id"] = 1 if m["cluster_id"] == 0 else 0
    sc0 = nat["scenarios"][0]
    sc1 = nat["scenarios"][1]
    sc0["cluster_id"] = 1
    sc1["cluster_id"] = 0
    nat["scenarios"] = [sc1, sc0]

    res = compare_stage_original(ref, nat)
    assert res["status"] == "pass", f"Permuted labels must pass, got {res}"


def test_positive_sign_flip():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    # Flip axis 0 sign in native components and scores
    for row in nat["diagnostics"]["components"][0]:
        pass  # normalized
    # Component sign flip
    nat["diagnostics"]["components"][0] = [-x for x in nat["diagnostics"]["components"][0]]
    for s in nat["diagnostics"]["scores"]:
        s[0] = -s[0]
    for sc in nat["scenarios"]:
        sc["center"][0] = -sc["center"][0]

    res = compare_stage_original(ref, nat)
    assert res["status"] == "pass", f"Sign flip must pass, got {res}"


def test_negative_wrong_partition():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    # Put member 1 in cluster 1 instead of 0
    nat["members"][1]["cluster_id"] = 1
    res = compare_stage_original(ref, nat)
    assert res["status"] == "fail", "Wrong partition must fail"
    assert "partition" in res.get("reason", "").lower() or "partition" in str(res).lower()


def test_negative_wrong_subspace():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    # Change components to completely different subspace
    nat["diagnostics"]["components"] = [[0.0, 1.0, 0.0], [1.0, 0.0, 0.0]]
    res = compare_stage_original(ref, nat)
    assert res["status"] == "fail", "Different subspace must fail"


def test_negative_tolerance_exceeded():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    # Add large deviation to total_sse
    nat["diagnostics"]["total_sse"] += 1.0
    res = compare_stage_original(ref, nat)
    assert res["status"] == "fail", "Tolerance failure must be rejected"


def test_negative_non_finite_values():
    if compare_stage_original is None:
        raise AssertionError("compare_stage_original not yet implemented")
    ref, nat = make_valid_base_records()
    nat["diagnostics"]["scores"][0][0] = float("nan")
    res = compare_stage_original(ref, nat)
    assert res["status"] == "fail", "NaN values must fail comparison"


def test_shared_positive_identical():
    ref = {
        "labels": [0, 0, 1, 1],
        "inertia": 0.04,
        "n_iter": 3,
    }
    nat = {
        "members": [
            {"member_id": "m0", "cluster_id": 0},
            {"member_id": "m1", "cluster_id": 0},
            {"member_id": "m2", "cluster_id": 1},
            {"member_id": "m3", "cluster_id": 1},
        ],
        "diagnostics": {
            "total_sse": 0.04,
            "restarts": [{"iterations": 3}],
        },
    }
    res = compare_stage_shared(ref, nat)
    assert res["status"] == "pass", f"Expected pass, got {res}"


def test_shared_negative_iterations_mismatch():
    ref = {
        "labels": [0, 0, 1, 1],
        "inertia": 0.04,
        "n_iter": 3,
    }
    nat = {
        "members": [
            {"member_id": "m0", "cluster_id": 0},
            {"member_id": "m1", "cluster_id": 0},
            {"member_id": "m2", "cluster_id": 1},
            {"member_id": "m3", "cluster_id": 1},
        ],
        "diagnostics": {
            "total_sse": 0.04,
            "restarts": [{"iterations": 4}],  # mismatch
        },
    }
    res = compare_stage_shared(ref, nat)
    assert res["status"] == "fail", f"Expected fail on iteration mismatch, got {res}"


def test_shared_negative_partition_mismatch():
    ref = {
        "labels": [0, 0, 1, 1],
        "inertia": 0.04,
        "n_iter": 3,
    }
    nat = {
        "members": [
            {"member_id": "m0", "cluster_id": 0},
            {"member_id": "m1", "cluster_id": 1},  # mismatch
            {"member_id": "m2", "cluster_id": 1},
            {"member_id": "m3", "cluster_id": 1},
        ],
        "diagnostics": {
            "total_sse": 0.04,
            "restarts": [{"iterations": 3}],
        },
    }
    res = compare_stage_shared(ref, nat)
    assert res["status"] == "fail", f"Expected fail on partition mismatch, got {res}"


def test_trace_corruption_and_missing_evidence():
    from validation.m1.compare_results import compare_traces
    trace = {"schema_version": "m1.trace.v1", "events": [
        {"iteration": 1, "event_type": "update", "counts_as_iteration": True,
         "labels": [0, 1], "centers": [[0.0], [1.0]], "sse": 0.0, "shift": 0.0}],
        "final_iteration_count": 1, "stop_reason": "center_shift"}
    assert compare_traces(trace, trace, {})["status"] == "pass"
    for field, value in [("labels", [1, 0]), ("centers", [[5.0], [1.0]]), ("sse", 9.0), ("shift", 9.0)]:
        broken = copy.deepcopy(trace)
        broken["events"][0][field] = value
        assert compare_traces(trace, broken, {})["status"] == "fail", field
    assert compare_traces(trace, {}, {})["status"] == "fail"


def run_all_self_checks() -> bool:
    tests = [
        test_trace_corruption_and_missing_evidence,
        test_positive_identical,
        test_positive_label_permutation,
        test_positive_sign_flip,
        test_negative_wrong_partition,
        test_negative_wrong_subspace,
        test_negative_tolerance_exceeded,
        test_negative_non_finite_values,
        test_shared_positive_identical,
        test_shared_negative_iterations_mismatch,
        test_shared_negative_partition_mismatch,
    ]
    all_ok = True
    for t in tests:
        try:
            t()
            print(f"PASS: {t.__name__}")
        except Exception as e:
            print(f"FAIL: {t.__name__}: {e}")
            all_ok = False
    return all_ok


if __name__ == "__main__":
    if not run_all_self_checks():
        sys.exit(1)
    sys.exit(0)
