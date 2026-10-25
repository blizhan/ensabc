#!/usr/bin/env python3
"""Compare reference and native ensabc-core results under locked M1 comparison rules.

Usage:
    python3 validation/m1/compare_results.py --run-dir DIR --stage original|shared
    python3 validation/m1/compare_results.py --self-check

The comparator enforces:
- Exact partition matching via member co-membership bijection
- Subspace equivalence via B^T B check
- Pairwise member squared-distance consistency
- Orthogonal Procrustes alignment Q such that scores_nat * Q ≈ scores_ref
- Per-element tolerances: |actual - ref| <= atol + rtol * |ref|
- In original stage: writes comparison/original.json and comparison/projection-alignment.json
- In shared stage: writes comparison/shared.json without overwriting original artifacts
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import (
    FIXTURES_DIR,
    M1Error,
    read_json,
    utc_now,
    write_json,
)
from validation.m1.schema import (
    validate_comparison_report,
    validate_projection_alignment,
)

COMPARISON_RULES_PATH = FIXTURES_DIR / "comparison_rules.json"


def _check_close(
    actual: Any,
    reference: Any,
    atol: float = 1e-8,
    rtol: float = 1e-6,
) -> tuple[bool, float]:
    """Check element-wise |a - r| <= atol + rtol * |r|."""
    a = np.asarray(actual, dtype=np.float64)
    r = np.asarray(reference, dtype=np.float64)
    if a.shape != r.shape:
        return False, float("inf")
    if not np.isfinite(a).all() or not np.isfinite(r).all():
        return False, float("nan")
    diff = np.abs(a - r)
    allowed = atol + rtol * np.abs(r)
    max_diff = float(np.max(diff))
    passed = bool(np.all(diff <= allowed))
    return passed, max_diff


def compute_procrustes(b_nat: np.ndarray, b_ref: np.ndarray) -> tuple[np.ndarray, float]:
    """Compute orthogonal matrix Q (d x d) such that B_nat.T @ Q ≈ B_ref.T.

    b_nat: (d, p)
    b_ref: (d, p)
    Returns: Q (d, d), det(Q)
    """
    m = b_nat @ b_ref.T  # (d, d)
    u, _, vt = np.linalg.svd(m)
    q = u @ vt
    det_q = float(np.linalg.det(q))
    return q, det_q


def check_partitions(
    ref_labels: list[int],
    nat_members: list[dict[str, Any]],
) -> tuple[bool, dict[int, int], list[str]]:
    """Verify co-membership and construct mapping from nat_cluster_id to ref_cluster_id."""
    n = len(ref_labels)
    if len(nat_members) != n:
        return False, {}, ["member_count_mismatch"]

    # Check if a consistent 1-to-1 mapping exists
    nat_to_ref: dict[int, int] = {}
    ref_to_nat: dict[int, int] = {}
    mismatched = []

    for idx, (r_lab, m_nat) in enumerate(zip(ref_labels, nat_members)):
        n_lab = m_nat["cluster_id"]
        if n_lab in nat_to_ref:
            if nat_to_ref[n_lab] != r_lab:
                mismatched.append(m_nat["member_id"])
        else:
            nat_to_ref[n_lab] = r_lab

        if r_lab in ref_to_nat:
            if ref_to_nat[r_lab] != n_lab:
                if m_nat["member_id"] not in mismatched:
                    mismatched.append(m_nat["member_id"])
        else:
            ref_to_nat[r_lab] = n_lab

    if mismatched or len(nat_to_ref) != len(ref_to_nat):
        return False, {}, mismatched

    return True, nat_to_ref, []


def compare_stage_original(
    ref_run: dict[str, Any],
    native_res: dict[str, Any],
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare original reference run against native core result."""
    if rules is None:
        rules = read_json(COMPARISON_RULES_PATH) if COMPARISON_RULES_PATH.is_file() else {}

    tols = rules.get("tolerances", {})
    atol_default = tols.get("aligned_scores", {}).get("atol", 1e-8)
    rtol_default = tols.get("aligned_scores", {}).get("rtol", 1e-6)

    checks: dict[str, Any] = {}
    overall_pass = True

    # 1. Standardization: mean and scale
    nat_diag = native_res.get("diagnostics", {})
    nat_mean = nat_diag.get("normalization_mean")
    nat_scale = nat_diag.get("normalization_scale")
    ref_mean = ref_run.get("mean")
    ref_scale = ref_run.get("scale")

    if nat_mean is not None and ref_mean is not None:
        p, md = _check_close(nat_mean, ref_mean, atol_default, rtol_default)
        checks["mean"] = {"status": "pass" if p else "fail", "max_diff": md}
        if not p:
            overall_pass = False

    if nat_scale is not None and ref_scale is not None:
        p, md = _check_close(nat_scale, ref_scale, atol_default, rtol_default)
        checks["scale"] = {"status": "pass" if p else "fail", "max_diff": md}
        if not p:
            overall_pass = False

    # 2. Components and Subspace B^T B check
    nat_b = np.asarray(nat_diag.get("components", []), dtype=np.float64)
    ref_b = np.asarray(ref_run.get("components", []), dtype=np.float64)

    if nat_b.size == 0 or ref_b.size == 0 or nat_b.shape != ref_b.shape or not np.isfinite(nat_b).all() or not np.isfinite(ref_b).all():
        checks["components"] = {"status": "fail", "reason": "invalid_or_missing_components"}
        return {"status": "fail", "reason": "invalid_or_missing_components", "checks": checks}

    # Subspace projector B^T B
    proj_nat = nat_b.T @ nat_b
    proj_ref = ref_b.T @ ref_b
    p, md = _check_close(proj_nat, proj_ref, atol_default, rtol_default)
    checks["subspace_btb"] = {"status": "pass" if p else "fail", "max_diff": md}
    if not p:
        overall_pass = False

    # 3. Pairwise squared distances in projected space
    nat_scores = np.asarray(nat_diag.get("scores", []), dtype=np.float64)
    ref_scores = np.asarray(ref_run.get("scores", []), dtype=np.float64)

    if nat_scores.shape != ref_scores.shape or not np.isfinite(nat_scores).all() or not np.isfinite(ref_scores).all():
        checks["scores_shape"] = {"status": "fail", "reason": "invalid_scores_shape_or_values"}
        return {"status": "fail", "reason": "invalid_scores_shape_or_values", "checks": checks}

    # Pairwise distances: ||s_i - s_j||^2
    d_nat = np.sum((nat_scores[:, None, :] - nat_scores[None, :, :]) ** 2, axis=-1)
    d_ref = np.sum((ref_scores[:, None, :] - ref_scores[None, :, :]) ** 2, axis=-1)
    p, md = _check_close(d_nat, d_ref, atol_default, rtol_default)
    checks["pairwise_distance"] = {"status": "pass" if p else "fail", "max_diff": md}
    if not p:
        overall_pass = False

    # 4. Procrustes alignment
    q, det_q = compute_procrustes(nat_b, ref_b)
    aligned_nat_scores = nat_scores @ q
    p, md = _check_close(aligned_nat_scores, ref_scores, atol_default, rtol_default)
    checks["aligned_scores"] = {"status": "pass" if p else "fail", "max_diff": md}
    if not p:
        overall_pass = False

    # 5. Partition check
    ref_labels = ref_run.get("labels", [])
    nat_members = native_res.get("members", [])
    part_ok, nat_to_ref, mismatched = check_partitions(ref_labels, nat_members)
    checks["partition"] = {"status": "pass" if part_ok else "fail", "mismatched": mismatched}
    if not part_ok:
        overall_pass = False

    # 6. Aligned cluster centers & physical mean features
    k = len(nat_to_ref) if part_ok else len(native_res.get("scenarios", []))
    nat_scenarios = native_res.get("scenarios", [])
    ref_centers = np.asarray(ref_run.get("centers", []), dtype=np.float64)
    ref_means = np.asarray(ref_run.get("mean_features", []), dtype=np.float64)

    if part_ok and len(nat_scenarios) == k:
        # Reorder native centers and means according to nat_to_ref
        reordered_nat_centers = [None] * k
        reordered_nat_means = [None] * k
        for sc in nat_scenarios:
            ref_cid = nat_to_ref[sc["cluster_id"]]
            reordered_nat_centers[ref_cid] = sc["center"]
            reordered_nat_means[ref_cid] = sc["mean_features"]

        aligned_nat_centers = np.asarray(reordered_nat_centers, dtype=np.float64) @ q
        p, md = _check_close(aligned_nat_centers, ref_centers, atol_default, rtol_default)
        checks["aligned_centers"] = {"status": "pass" if p else "fail", "max_diff": md}
        if not p:
            overall_pass = False

        p, md = _check_close(reordered_nat_means, ref_means, atol_default, rtol_default)
        checks["physical_mean_features"] = {"status": "pass" if p else "fail", "max_diff": md}
        if not p:
            overall_pass = False

    # 7. Total SSE
    nat_sse = nat_diag.get("total_sse")
    ref_sse = ref_run.get("inertia", ref_run.get("total_sse"))
    if nat_sse is not None and ref_sse is not None:
        p, md = _check_close(nat_sse, ref_sse, atol_default, rtol_default)
        checks["total_sse"] = {"status": "pass" if p else "fail", "max_diff": md}
        if not p:
            overall_pass = False

    # 8. Member fraction sum
    frac_sum = sum(sc.get("member_fraction", 0.0) for sc in nat_scenarios)
    frac_p = abs(frac_sum - 1.0) <= 1e-12
    checks["member_fraction_sum"] = {"status": "pass" if frac_p else "fail", "diff": abs(frac_sum - 1.0)}
    if not frac_p:
        overall_pass = False

    return {
        "status": "pass" if overall_pass else "fail",
        "stage": "original",
        "checks": checks,
        "procrustes_q": q.tolist(),
        "det_q": det_q,
    }


def compare_traces(reference: dict[str, Any], native: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any]:
    """Compare observed Lloyd states in the same coordinate system and center order."""
    from validation.m1.schema import validate_trace_record
    try:
        validate_trace_record(reference)
        validate_trace_record(native)
        left, right = reference["events"], native["events"]
        if not left or len(left) != len(right):
            return {"status": "fail", "reason": "missing or unequal trace events"}
        if any(reference[key] != native[key] for key in ("final_iteration_count", "stop_reason")):
            return {"status": "fail", "reason": "stopping decision differs"}
        for ref_event, nat_event in zip(left, right):
            for key in ("iteration", "event_type", "counts_as_iteration", "labels"):
                if ref_event[key] != nat_event[key]:
                    return {"status": "fail", "reason": f"trace {key} differs"}
            for key in ("centers", "sse", "shift"):
                a, b = ref_event[key], nat_event[key]
                if a is None or b is None:
                    if a is not b:
                        return {"status": "fail", "reason": f"trace {key} missing"}
                elif not _check_close(a, b,
                    rules.get("tolerances", {}).get("absolute_tolerance", 1e-8),
                    rules.get("tolerances", {}).get("relative_tolerance", 1e-6))[0]:
                    return {"status": "fail", "reason": f"trace {key} differs"}
        return {"status": "pass"}
    except (M1Error, KeyError, ValueError, TypeError) as exc:
        return {"status": "fail", "reason": str(exc)}


def compare_stage_shared(
    ref_run: dict[str, Any],
    native_res: dict[str, Any],
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare shared reference run against native core shared result."""
    if rules is None:
        rules = read_json(COMPARISON_RULES_PATH) if COMPARISON_RULES_PATH.is_file() else {}

    tols = rules.get("tolerances", {})
    atol_default = tols.get("aligned_scores", {}).get("atol", 1e-8)
    rtol_default = tols.get("aligned_scores", {}).get("rtol", 1e-6)

    checks: dict[str, Any] = {}
    overall_pass = True

    # Partition check
    ref_labels = ref_run.get("labels", [])
    nat_members = native_res.get("members", [])
    part_ok, nat_to_ref, mismatched = check_partitions(ref_labels, nat_members)
    checks["partition"] = {"status": "pass" if part_ok else "fail", "mismatched": mismatched}
    if not part_ok:
        overall_pass = False

    # Total SSE / inertia check
    nat_sse = native_res.get("diagnostics", {}).get("total_sse")
    ref_sse = ref_run.get("inertia", ref_run.get("total_sse"))
    if nat_sse is not None and ref_sse is not None:
        p, md = _check_close(nat_sse, ref_sse, atol_default, rtol_default)
        checks["total_sse"] = {"status": "pass" if p else "fail", "max_diff": md}
        if not p:
            overall_pass = False
    else:
        checks["total_sse"] = {"status": "fail", "reason": "missing SSE"}
        overall_pass = False

    # Iteration count check
    ref_n_iter = ref_run.get("n_iter")
    restarts = native_res.get("diagnostics", {}).get("restarts", [])
    if restarts and ref_n_iter is not None:
        nat_iter = restarts[0].get("iterations")
        checks["iterations"] = {"status": "pass" if nat_iter == ref_n_iter else "fail", "native": nat_iter, "reference": ref_n_iter}
        if nat_iter != ref_n_iter:
            overall_pass = False
    else:
        checks["iterations"] = {"status": "fail", "reason": "missing iteration count"}
        overall_pass = False

    return {
        "status": "pass" if overall_pass else "fail",
        "stage": "shared",
        "checks": checks,
    }


def run_stage_original_cmd(run_dir: Path) -> dict[str, Any]:
    ref_run_path = run_dir / "reference" / "original" / "run-1.json"
    nat_run_path = run_dir / "core" / "run-1.json"
    if not ref_run_path.is_file() or not nat_run_path.is_file():
        raise M1Error(f"Missing original runs at {ref_run_path} or {nat_run_path}", code="MISSING_INPUT")

    ref_run = read_json(ref_run_path)
    nat_run = read_json(nat_run_path)
    rules = read_json(COMPARISON_RULES_PATH)

    result = compare_stage_original(ref_run, nat_run, rules)

    comp_dir = run_dir / "comparison"
    comp_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write comparison/projection-alignment.json
    alignment_record = {
        "schema_version": "m1.projection-alignment.v1",
        "status": result["status"],
        "procrustes_q": result["procrustes_q"],
        "det_q": result["det_q"],
        "btb_max_diff": result["checks"].get("subspace_btb", {}).get("max_diff", 0.0),
        "pairwise_dist_max_diff": result["checks"].get("pairwise_distance", {}).get("max_diff", 0.0),
        "aligned_scores_max_diff": result["checks"].get("aligned_scores", {}).get("max_diff", 0.0),
        "components_native": nat_run["diagnostics"]["components"],
        "components_reference": ref_run["components"],
        "created_at": utc_now(),
    }
    write_json(comp_dir / "projection-alignment.json", alignment_record)
    validate_projection_alignment(alignment_record)

    # 2. Write comparison/original.json
    report_record = {
        "schema_version": "m1.comparison-report.v1",
        "stage": "original",
        "status": result["status"],
        "rule_version": rules.get("schema_version", "m1.comparison.v1"),
        "checks": result["checks"],
        "created_at": utc_now(),
    }
    write_json(comp_dir / "original.json", report_record)
    validate_comparison_report(report_record)

    if result["status"] != "pass":
        raise M1Error("Stage original comparison failed", code="COMPARISON_FAILED")

    return result


def run_stage_shared_cmd(run_dir: Path) -> dict[str, Any]:
    ref_run_path = run_dir / "reference" / "shared" / "reference.json"
    nat_run_path = run_dir / "core" / "shared.json"
    obs_path = run_dir / "reference" / "shared" / "observer-check.json"

    if not ref_run_path.is_file() or not nat_run_path.is_file() or not obs_path.is_file():
        raise M1Error(f"Missing shared inputs at {ref_run_path}, {nat_run_path}, or {obs_path}", code="MISSING_INPUT")

    ref_run = read_json(ref_run_path)
    nat_run = read_json(nat_run_path)
    obs = read_json(obs_path)
    rules = read_json(COMPARISON_RULES_PATH)

    if obs.get("status") != "pass":
        raise M1Error("Observer check must pass before shared comparison", code="OBSERVER_CHECK_FAILED")

    result = compare_stage_shared(ref_run, nat_run, rules)
    ref_trace = read_json(run_dir / "reference" / "shared" / "trace.json")
    native_trace = read_json(run_dir / "core" / "shared-trace.json")
    result["checks"]["trace"] = compare_traces(ref_trace, native_trace, rules)
    if result["checks"]["trace"]["status"] != "pass":
        result["status"] = "fail"

    comp_dir = run_dir / "comparison"
    comp_dir.mkdir(parents=True, exist_ok=True)

    report_record = {
        "schema_version": "m1.comparison-report.v1",
        "stage": "shared",
        "status": result["status"],
        "rule_version": rules.get("schema_version", "m1.comparison.v1"),
        "checks": result["checks"],
        "observer_check": obs,
        "created_at": utc_now(),
    }
    write_json(comp_dir / "shared.json", report_record)
    validate_comparison_report(report_record)

    if result["status"] != "pass":
        raise M1Error("Stage shared comparison failed", code="COMPARISON_FAILED")

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare M1 reference and native results.")
    parser.add_argument("--self-check", action="store_true", help="Run comparator self-checks")
    parser.add_argument("--run-dir", type=Path, help="Target run directory")
    parser.add_argument("--stage", choices=["original", "shared"], help="Stage to compare")

    try:
        args = parser.parse_args()
    except SystemExit:
        return 2

    if args.self_check:
        from validation.m1.test_comparison import run_all_self_checks
        ok = run_all_self_checks()
        if args.run_dir:
            out_file = args.run_dir / "comparison" / "self-check.json"
            out_file.parent.mkdir(parents=True, exist_ok=True)
            write_json(out_file, {
                "schema_version": "m1.comparison.v1",
                "status": "pass" if ok else "fail",
                "created_at": utc_now(),
            })
        return 0 if ok else 1

    if not args.stage:
        print("Error: --stage is required when --run-dir is specified", file=sys.stderr)
        return 2

    try:
        if args.stage == "original":
            run_stage_original_cmd(args.run_dir)
            print(f"Successfully completed comparison stage original in {args.run_dir}")
        elif args.stage == "shared":
            run_stage_shared_cmd(args.run_dir)
            print(f"Successfully completed comparison stage shared in {args.run_dir}")
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2 if e.code in ("MISSING_PREREQUISITE", "MISSING_INPUT") else 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
