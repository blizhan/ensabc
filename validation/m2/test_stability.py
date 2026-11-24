#!/usr/bin/env python3
"""Test harness for stability certificate and perturbation margin bounds (T039).

Usage:
    python3 validation/m2/test_stability.py [--out-report PATH]

Validates:
1. Stability certificate granting when margins strictly satisfy triangle-inequality bounds
2. Stability certificate denial when threshold margin or optimality margin is insufficient
3. Handling of q=0 (empty candidate graph)
4. Handling of no same-count alternative (q* edges saturated or single solution)
5. Exact tied alternative detection (zero optimality margin)
"""

from __future__ import annotations

import argparse
from fractions import Fraction
import json
import math
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import atomic_write_json


def _rational_sqrt_bounds(val: Fraction, bits: int = 128) -> tuple[Fraction, Fraction]:
    """Return conservative rational bounds [lo, hi] such that lo <= sqrt(val) <= hi."""
    if val < 0:
        raise ValueError("Cannot take sqrt of negative Fraction")
    if val == 0:
        return Fraction(0), Fraction(0)
    scale = 1 << (2 * bits)
    scaled_num = (val.numerator * scale) // val.denominator
    lo_int = math.isqrt(scaled_num)
    lo = Fraction(lo_int, 1 << bits)
    hi_int = lo_int if lo_int * lo_int * val.denominator == val.numerator * scale else lo_int + 1
    hi = Fraction(hi_int, 1 << bits)
    return lo, hi


def _float_ulp_pad(val: float, multiplier: int = 8) -> Fraction:
    """Conservative binary64 rounding envelope covering intermediate floating-point error."""
    if not math.isfinite(val):
        raise ValueError(f"Non-finite value in stability certificate: {val}")
    return Fraction(math.ulp(abs(val)) if val != 0.0 else math.ulp(1.0)) * multiplier


def evaluate_certificate(
    threshold_margin: float,
    optimality_margin: float,
    q_star: int,
    epsilon_abs: float,
    delta_tau: float = 0.0,
    has_same_count_alt: bool = True,
    optimal_solution_count: int = 1,
    *,
    margin_scale: float = 1.0,
) -> dict[str, Any]:
    """Evaluate conservative stability certificate using exact outward-rounded Fraction intervals.

    Checks all three conditions from contracts/numerics.md §4:
      1. lower(min_{i,j} |d_{i,j} - tau|) > upper(2 * eps_abs + delta_tau)
      2. lower(min_e delta_Sigma_e) > upper(4 * q* * eps_abs)  (when q* > 0 and has_same_count_alt)
      3. optimal_solution_count == 1
    Any non-finite input conservatively denies the certificate.
    """
    if not all(math.isfinite(v) for v in (threshold_margin, optimality_margin, epsilon_abs, delta_tau, margin_scale)):
        return {
            "certified": False,
            "reason": "non_finite_bound",
            "cond1_satisfied": False,
            "cond2_satisfied": False,
            "cond3_satisfied": False,
            "q_star": q_star,
            "has_same_count_alt": has_same_count_alt,
            "optimal_solution_count": optimal_solution_count,
        }

    # Exact Fraction representation of binary64 operands with outward rounding pads
    # covering binary64 distance/margin/threshold rounding error.
    pad_scale = max(abs(margin_scale), abs(threshold_margin), abs(optimality_margin), abs(epsilon_abs), 1.0)
    cond1_pad = _float_ulp_pad(pad_scale, multiplier=16)
    lhs1_lo = Fraction(threshold_margin) - cond1_pad
    rhs1_hi = Fraction(2) * Fraction(epsilon_abs) + Fraction(delta_tau) + cond1_pad
    cond1 = lhs1_lo > rhs1_hi

    if q_star == 0:
        cond2 = True
        cond2_status = "empty_matching_q_zero"
        cond2_bound_f = 0.0
    elif not has_same_count_alt:
        cond2 = True
        cond2_status = "no_same_count_alternative"
        cond2_bound_f = 0.0
    else:
        cond2_pad = _float_ulp_pad(pad_scale, multiplier=max(16, 8 * q_star))
        lhs2_lo = Fraction(optimality_margin) - cond2_pad
        rhs2_hi = Fraction(4 * q_star) * Fraction(epsilon_abs) + cond2_pad
        cond2 = lhs2_lo > rhs2_hi
        cond2_status = "evaluated"
        cond2_bound_f = float(rhs2_hi)

    cond3 = optimal_solution_count == 1
    certified = cond1 and cond2 and cond3
    return {
        "certified": certified,
        "cond1_satisfied": cond1,
        "cond1_margin": threshold_margin,
        "cond1_bound": float(rhs1_hi),
        "cond2_satisfied": cond2,
        "cond2_status": cond2_status,
        "cond2_margin": optimality_margin,
        "cond2_bound": cond2_bound_f,
        "cond3_satisfied": cond3,
        "optimal_solution_count": optimal_solution_count,
        "q_star": q_star,
        "has_same_count_alt": has_same_count_alt,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Test stability certificate formulation")
    parser.add_argument("--out-report", type=Path, default=None, help="Output test report JSON")
    args = parser.parse_args()

    results = []

    # Case 1: Large margins & unique optimum -> Certified
    c1 = evaluate_certificate(threshold_margin=0.15, optimality_margin=0.25, q_star=3, epsilon_abs=0.01, optimal_solution_count=1)
    assert c1["certified"] is True
    results.append({"case": "large_margins_certified", "status": "PASS", "certified": True})

    # Case 2: Insufficient threshold margin (cond1 fails) -> Denied
    c2 = evaluate_certificate(threshold_margin=0.015, optimality_margin=0.25, q_star=3, epsilon_abs=0.01, optimal_solution_count=1)
    assert c2["certified"] is False
    assert c2["cond1_satisfied"] is False
    results.append({"case": "insufficient_threshold_margin_denied", "status": "PASS", "certified": False})

    # Case 3: Insufficient optimality margin (cond2 fails) -> Denied
    c3 = evaluate_certificate(threshold_margin=0.15, optimality_margin=0.05, q_star=3, epsilon_abs=0.01, optimal_solution_count=1)
    assert c3["certified"] is False
    assert c3["cond2_satisfied"] is False
    results.append({"case": "insufficient_optimality_margin_denied", "status": "PASS", "certified": False})

    # Case 4: q_star = 0 (empty candidate graph) -> Certified
    c4 = evaluate_certificate(threshold_margin=0.10, optimality_margin=0.0, q_star=0, epsilon_abs=0.01, optimal_solution_count=1)
    assert c4["certified"] is True
    assert c4["cond2_status"] == "empty_matching_q_zero"
    results.append({"case": "q_zero_empty_graph_certified", "status": "PASS", "certified": True})

    # Case 5: No same-count alternative -> Certified
    c5 = evaluate_certificate(threshold_margin=0.10, optimality_margin=0.0, q_star=2, epsilon_abs=0.01, has_same_count_alt=False, optimal_solution_count=1)
    assert c5["certified"] is True
    assert c5["cond2_status"] == "no_same_count_alternative"
    results.append({"case": "no_same_count_alternative_certified", "status": "PASS", "certified": True})

    # Case 6: Exact tied alternative (optimality_margin = 0, optimal_solution_count = 2) -> Denied
    c6 = evaluate_certificate(threshold_margin=0.10, optimality_margin=0.0, q_star=2, epsilon_abs=0.01, has_same_count_alt=True, optimal_solution_count=2)
    assert c6["certified"] is False
    assert c6["cond3_satisfied"] is False
    results.append({"case": "exact_tied_alternative_denied", "status": "PASS", "certified": False})

    # Case 7: Multiple optima (cond3 fails even if has_same_count_alt=False) -> Denied
    c7 = evaluate_certificate(threshold_margin=0.50, optimality_margin=0.50, q_star=2, epsilon_abs=0.01, has_same_count_alt=False, optimal_solution_count=2)
    assert c7["certified"] is False
    assert c7["cond3_satisfied"] is False
    results.append({"case": "non_unique_optimum_denied", "status": "PASS", "certified": False})

    # Case 8: 1-ULP strictly greater in f64 must be DENIED by conservative outward rounding
    eps = 0.01
    barely_above = math.nextafter(2.0 * eps, math.inf)
    assert barely_above > 2.0 * eps
    c8 = evaluate_certificate(threshold_margin=barely_above, optimality_margin=0.50, q_star=2, epsilon_abs=eps, optimal_solution_count=1)
    assert c8["certified"] is False
    assert c8["cond1_satisfied"] is False
    results.append({"case": "one_ulp_above_denied_conservatively", "status": "PASS", "certified": False})

    # Case 9: Rational sqrt bounds self-check
    lo, hi = _rational_sqrt_bounds(Fraction(2))
    assert lo * lo <= Fraction(2) <= hi * hi
    assert hi - lo <= Fraction(1, 1 << 120)
    results.append({"case": "rational_sqrt_bounds", "status": "PASS", "certified": True})

    summary = {
        "schema_version": "m2.test-stability-report.v1",
        "cases_tested": len(results),
        "status": "PASS",
        "results": results,
    }

    if args.out_report:
        atomic_write_json(args.out_report, summary)
        print(f"Report written to {args.out_report}")

    print("All stability certificate tests passed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
