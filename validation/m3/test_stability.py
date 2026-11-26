#!/usr/bin/env python3
"""Comprehensive unit tests for M3 Fraction Evolution Oracle and 4-Condition Stability Certificate (T031).

Covers:
1. Core oracle scenarios:
   - Exact weighted RMSE parenthesization & flat-fsum dispersion counterexample
   - Pure continuation (absolute & scaled modes)
   - Pure split (`1 -> 2`) across both `exact_P >= exact_Q` and `exact_P < exact_Q` proportional flow regimes
   - Pure merge (`2 -> 1`) with multi-candidate `secondary_margin` and Theorem 3 `promotion_gap`
   - Composite split + merge crossed diamond (Berge Theorems 1-4 & active vs raw threshold margin)
   - Emergence, disappearance, `all_exceed_threshold` (`q* = 0`), and `tau == 0.0` boundary
   - Exact secondary tie with `canonical_scenario_order` (`-0.0 -> 0.0` + `cluster_id`) tie-breaking & permutation invariance
   - Strict metric orthogonality: pure morphology shift (`0.0` fraction metrics) vs pure fraction shift (`0.0` morphology metrics) + `m3.uncertainty.v1` `0.10 * R` boundary clamping
   - Multi-batch chains: 6-batch full-event chain and 8-batch chain with `missing`/`failed` gaps (`gap_interrupted` / `gap_restarted` disjoint from `disappearance` / `emergence`)
   - Input validation & tampered `BackboneAssertion` rejection
2. 4-Condition Outward-Rounded Fraction Stability Certificate (`evaluate_evolution_stability_certificate`):
   - Conservative `isqrt` rational square-root bounds
   - Provably stable certified multi-event pair achieving 0 flips across 50 perturbed trials
   - Near-threshold denial (active edge & out-of-threshold pair) vs Theorem 4 inactive cross-edge immunity
   - 1-ULP strictly-greater boundary conservatively denied by outward rational rounding
   - Near-primary-switch (`SameCount` <= `4 * q* * eps_abs`) and exact primary tie (`Tied`, `optima_count > 1`)
   - Near-secondary-switch (`|F(u)| >= 2`, gap <= `4 * eps_abs`) with explicit flip witness and exact secondary tie
   - `q* = 0` empty matching and `CountDrop`-only primary matching special cases
   - `evaluation_mapping_ambiguous` denial (and `overlap_cluster_mapping` tie detection)
   - Non-empty certified AND non-empty uncertified synthetic benchmark groups
"""

from __future__ import annotations

import argparse
import io
import itertools
import json
import math
import os
import random
import sys
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.oracle import (
    OracleValidationError,
    RationalInterval,
    canonical_scenario_order,
    compute_ensemble_dispersion_exact,
    compute_weighted_rmse_exact,
    compute_weighted_rmse_rational_bounds,
    evaluate_evolution_stability_certificate,
    fsum_exact,
    overlap_cluster_mapping,
    rational_sqrt_bounds,
    solve_chain_oracle,
    solve_evolution_oracle,
    verify_berge_and_star_invariants,
    verify_chain_invariants,
)


def _perturb_features_exact_rms(
    base_features: Sequence[float],
    weights: Sequence[float],
    eps_abs: float,
    rng: random.Random,
) -> list[float]:
    """Generate a perturbed feature vector at weighted RMS distance <= eps_abs (stdlib only)."""
    n_pts = len(weights)
    w_sum = math.fsum(weights)
    raw_dir = [rng.gauss(0.0, 1.0) for _ in range(n_pts)]
    norm_sq = math.fsum(w * (d * d) for w, d in zip(weights, raw_dir)) / w_sum
    if norm_sq == 0.0:
        raw_dir = [1.0 for _ in range(n_pts)]
        norm_sq = 1.0
    rms_norm = math.sqrt(norm_sq)
    # Scale slightly inside (0.999999999) * eps_abs so binary64 rounding of base + delta
    # strictly respects d_w(mu', mu) <= eps_abs in exact arithmetic.
    step = 0.999999999 * eps_abs / rms_norm
    pert = [x + step * d for x, d in zip(base_features, raw_dir)]
    actual_rms = compute_weighted_rmse_exact(base_features, pert, weights)
    assert actual_rms <= eps_abs, f"Perturbation RMS {actual_rms!r} exceeded {eps_abs!r}"
    return pert


def _extract_topology_signature(evol_res: dict[str, Any]) -> dict[str, Any]:
    """Extract discrete topology & event signature for zero-flip verification."""
    return {
        "edges": [
            (e["prev_cluster_id"], e["curr_cluster_id"], e["edge_role"], e["event_type"])
            for e in evol_res["edges"]
        ],
        "prev_roles": [(n["cluster_id"], n["node_role"]) for n in evol_res["prev_nodes"]],
        "curr_roles": [(n["cluster_id"], n["node_role"]) for n in evol_res["curr_nodes"]],
        "events": [
            (
                ev["event_type"],
                tuple(ev["prev_cluster_ids"]),
                tuple(ev["curr_cluster_ids"]),
                ev["primary_link"],
            )
            for ev in evol_res["events"]
        ],
    }


class TestM3EvolutionOracle(unittest.TestCase):
    """Unit tests for M3 pure-Python Fraction & math.fsum evolution oracle."""

    def test_weighted_rmse_and_flat_fsum_dispersion(self) -> None:
        weights = [0.25, 0.5, 0.25]
        a = [280.0, 282.0, -0.0]
        b = [280.0, 280.0, 0.0]
        d = compute_weighted_rmse_exact(a, b, weights)
        self.assertEqual(d, math.sqrt(2.0))
        self.assertEqual(math.copysign(1.0, compute_weighted_rmse_exact([0.0], [-0.0], [1.0])), 1.0)

        # Flat vs nested fsum counterexample (matching M2 numerical contract)
        d_small = math.sqrt(0.49 * math.ulp(2.0))
        members = [
            [1.0 if i % 2 == 0 else -1.0, d_small if i % 2 == 0 else -d_small]
            for i in range(6)
        ]
        w_disp = [2.0, 1.0]
        flat_disp = compute_ensemble_dispersion_exact(members, w_disp)
        nested_q = math.fsum(
            math.fsum([w_disp[0] * (m[0] ** 2), w_disp[1] * (m[1] ** 2)]) for m in members
        )
        nested_disp = math.sqrt((nested_q / 6.0) / 3.0)
        self.assertNotEqual(flat_disp, nested_disp)

    def test_pure_continuation_scaled_and_absolute(self) -> None:
        task = {
            "mode": "scaled",
            "weights": [0.5, 0.5],
            "threshold_coefficient": 1.0,
            "prev_dispersion": 2.0,
            "curr_dispersion": 2.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.0, 280.0]},
                {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.0, 290.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.4, 280.4]},
                {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [289.6, 289.6]},
            ],
        }
        res = solve_evolution_oracle(task)
        self.assertEqual(res["algorithm_version"], "m3.evolve.v1")
        self.assertEqual(res["pair_scale"], 2.0)
        self.assertEqual(res["threshold"], 2.0)
        self.assertEqual(res["reference_scale"], 2.0)
        self.assertEqual(res["summary"]["continuation_count"], 2)
        self.assertEqual(res["summary"]["primary_edge_count"], 2)
        self.assertEqual(res["summary"]["secondary_split_edge_count"], 0)
        self.assertEqual(res["summary"]["secondary_merge_edge_count"], 0)
        self.assertAlmostEqual(res["summary"]["transition_tv_redistribution"], 0.10, places=15)

        e0, e1 = res["edges"]
        self.assertEqual(e0["edge_role"], "primary")
        self.assertEqual(e0["event_type"], "continuation")
        self.assertEqual(e0["rule_basis"], "global_optimum_primary")
        self.assertEqual(e0["attributed_flow"], 0.50)
        self.assertEqual(e1["attributed_flow"], 0.40)
        self.assertAlmostEqual(res["prev_nodes"][0]["unattributed_fraction"], 0.10, places=15)
        self.assertAlmostEqual(res["curr_nodes"][1]["unattributed_fraction"], 0.10, places=15)
        self.assertTrue(verify_berge_and_star_invariants(res)["all_passed"])

    def test_pure_split_both_mass_regimes(self) -> None:
        # Regime A: 1 -> 2 where exact_P (1.0) >= exact_Q (1.0), and check 2-component case where P_C > Q_C
        task_surplus = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 2.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 35, "member_fraction": 0.70, "mean_features": [280.0]},
                {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [300.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [280.2]},
                {"cluster_id": 1, "member_count": 12, "member_fraction": 0.24, "mean_features": [281.0]},
                {"cluster_id": 2, "member_count": 18, "member_fraction": 0.36, "mean_features": [300.1]},
            ],
        }
        res_a = solve_evolution_oracle(task_surplus)
        self.assertEqual(res_a["summary"]["split_count"], 1)
        self.assertEqual(res_a["summary"]["continuation_count"], 1)
        split_edges_a = [e for e in res_a["edges"] if e["prev_cluster_id"] == 0]
        self.assertEqual(len(split_edges_a), 2)
        # Since 0.70 >= 0.40 + 0.24 = 0.64, each child gets full q_j
        self.assertEqual(split_edges_a[0]["attributed_flow"], 0.40)
        self.assertEqual(split_edges_a[1]["attributed_flow"], 0.24)
        self.assertEqual(split_edges_a[1]["edge_role"], "secondary_split")
        self.assertEqual(split_edges_a[1]["rule_basis"], "nearest_feasible_split_unique")
        self.assertEqual(split_edges_a[1]["primary_alternative_status"], "promotion_gap")
        self.assertAlmostEqual(split_edges_a[1]["optimality_margin"], 0.8, places=12)
        self.assertAlmostEqual(res_a["prev_nodes"][0]["unattributed_fraction"], 0.06, places=15)

        # Regime B: exact_P (0.32) < exact_Q (0.42 + 0.16 = 0.58), mirroring WB2 b3->b4
        task_deficit = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 2.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 16, "member_fraction": 0.32, "mean_features": [280.0]},
                {"cluster_id": 1, "member_count": 34, "member_fraction": 0.68, "mean_features": [300.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 21, "member_fraction": 0.42, "mean_features": [280.2]},
                {"cluster_id": 1, "member_count": 8, "member_fraction": 0.16, "mean_features": [280.9]},
                {"cluster_id": 2, "member_count": 21, "member_fraction": 0.42, "mean_features": [300.1]},
            ],
        }
        res_b = solve_evolution_oracle(task_deficit)
        q_sum = fsum_exact([0.42, 0.16])
        exp_f0 = min(min(0.32, 0.42), (0.32 * 0.42) / q_sum)
        exp_f1 = min(min(0.32, 0.16), (0.32 * 0.16) / q_sum)
        split_edges_b = [e for e in res_b["edges"] if e["prev_cluster_id"] == 0]
        self.assertEqual(split_edges_b[0]["attributed_flow"], exp_f0)
        self.assertEqual(split_edges_b[1]["attributed_flow"], exp_f1)
        self.assertEqual(res_b["prev_nodes"][0]["node_role"], "split_parent")
        self.assertEqual(res_b["curr_nodes"][0]["node_role"], "split_primary_child")
        self.assertEqual(res_b["curr_nodes"][1]["node_role"], "split_secondary_child")
        self.assertEqual(res_b["prev_nodes"][0]["unattributed_fraction"], 0.0)
        self.assertGreater(res_b["curr_nodes"][0]["unattributed_fraction"], 0.0)

    def test_pure_merge_multi_candidate_secondary_margin(self) -> None:
        # Mirrors WB2 b0->b1 merge: prev c2 feasible to both curr c0 (dist 0.45) and curr c1 (dist 0.55)
        task = {
            "mode": "scaled",
            "weights": [1.0],
            "threshold_coefficient": 1.0,
            "prev_dispersion": 0.70,
            "curr_dispersion": 0.70,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 21, "member_fraction": 0.42, "mean_features": [280.00]},
                {"cluster_id": 1, "member_count": 17, "member_fraction": 0.34, "mean_features": [281.00]},
                {"cluster_id": 2, "member_count": 12, "member_fraction": 0.24, "mean_features": [280.45]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 31, "member_fraction": 0.62, "mean_features": [280.00]},
                {"cluster_id": 1, "member_count": 19, "member_fraction": 0.38, "mean_features": [281.00]},
            ],
        }
        res = solve_evolution_oracle(task)
        self.assertEqual(res["summary"]["merge_count"], 1)
        self.assertEqual(res["summary"]["continuation_count"], 1)
        sec_edge = next(e for e in res["edges"] if e["edge_role"] == "secondary_merge")
        self.assertEqual((sec_edge["prev_cluster_id"], sec_edge["curr_cluster_id"]), (2, 0))
        self.assertEqual(sec_edge["rule_basis"], "nearest_feasible_merge_unique")
        self.assertAlmostEqual(sec_edge["secondary_margin"], 0.10, places=12)
        self.assertAlmostEqual(sec_edge["optimality_margin"], 0.45, places=12)

        # Since q_0 = 0.62 < p_0 + p_2 = 0.66, proportional merge formula applies
        p_sum = fsum_exact([0.42, 0.24])
        self.assertEqual(sec_edge["attributed_flow"], min(min(0.24, 0.62), (0.62 * 0.24) / p_sum))
        self.assertEqual(res["prev_nodes"][0]["node_role"], "merge_primary_parent")
        self.assertEqual(res["prev_nodes"][2]["node_role"], "merge_secondary_parent")
        self.assertEqual(res["curr_nodes"][0]["node_role"], "merge_target")

    def test_composite_split_and_merge_crossed_diamond(self) -> None:
        # 3 -> 3 transition with q* = 2:
        # Prev: P0=0.0, P1=0.6 (merges into C0), P2=10.0 (splits into C1=10.0 and C2=10.7)
        # Inactive feasible cross edge: P0=0.0 to C2 if tau covers, or P1=0.6 to C1?
        # Let's include an inactive feasible cross edge between two primary branches:
        # P0=0.0 -> C0=0.0 (primary), P1=0.5 -> C0=0.0 (secondary merge into C0),
        # P2=1.95 -> C1=1.95 (primary), P2=1.95 -> C2=2.55 (secondary split from P2),
        # with tau = 2.00! Notice dist(P0=0.0, C1=1.95) = 1.95 <= 2.00 is an INACTIVE feasible cross-edge
        # very close to tau (margin 0.05), whereas active edges & d > tau pairs have margin >= 0.45!
        task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 2.00,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 15, "member_fraction": 0.30, "mean_features": [0.00]},
                {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [0.50]},
                {"cluster_id": 2, "member_count": 25, "member_fraction": 0.50, "mean_features": [3.50]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.00]},
                {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [1.52]},
                {"cluster_id": 2, "member_count": 15, "member_fraction": 0.30, "mean_features": [4.10]},
            ],
        }
        # Let's verify distances with tau=2.00:
        # d(P0=0, C0=0)=0.0, d(P0=0, C1=1.52)=1.52 <= 2.0, d(P0=0, C2=4.10)=4.10 > 2.0
        # d(P1=0.5, C0=0)=0.5 <= 2.0, d(P1=0.5, C1=1.52)=1.02 <= 2.0, d(P1=0.5, C2=4.10)=3.60 > 2.0
        # d(P2=3.5, C0=0)=3.5 > 2.0, d(P2=3.5, C1=1.52)=1.98 <= 2.0, d(P2=3.5, C2=4.10)=0.60 <= 2.0
        # Notice q*=3 if P0-C0, P1-C1, P2-C2! To get simultaneous split + merge in 3x3 we want q*=2:
        # Move C1 to 4.10 and C2 to 4.70 around P2=4.10, while P0=0.0, P1=0.50 around C0=0.0,
        # AND add a 2D coordinate so an inactive feasible cross edge between P0->C_other has d=1.98!
        # Wait: in 2D if P0->C1 <= tau and P1->C0 <= tau and P2->C2 <= tau, P1-C0 + P0-C1 + P2-C2 would give q*=3!
        # Instead, an inactive feasible cross-edge with q*=2 occurs between TWO primary continuations/splits/merges
        # e.g. 3 -> 4 where M* has q*=3: (P0->C0 merge target with P_extra? Or 4->4 with q*=3: {P0,P1}->C0 merge, P2->{C1,C2} split, P3->C3 continuation, where P0--C3 has d=1.98 <= 2.00!).
        task_4x4 = {
            "mode": "absolute",
            "weights": [0.5, 0.5],
            "threshold": 2.00,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 15, "member_fraction": 0.30, "mean_features": [0.0, 0.0]},
                {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [-0.6, -0.6]},
                {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [1.98, 1.98]},
                {"cluster_id": 3, "member_count": 15, "member_fraction": 0.30, "mean_features": [10.0, 10.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0, 0.0]},
                {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [1.98, 1.98]},
                {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [10.0, 10.0]},
                {"cluster_id": 3, "member_count": 10, "member_fraction": 0.20, "mean_features": [10.7, 10.7]},
            ],
        }
        res = solve_evolution_oracle(task_4x4)
        inv = verify_berge_and_star_invariants(res)
        self.assertTrue(inv["all_passed"])
        self.assertEqual(res["summary"]["merge_count"], 1)
        self.assertEqual(res["summary"]["split_count"], 1)
        self.assertEqual(res["summary"]["continuation_count"], 1)
        # Inactive feasible cross edge (P0, C1) has d = 1.98 <= 2.00 -> raw_matrix_threshold_margin == 0.02
        # whereas active edges & {d > tau} have min_threshold_margin >= 0.58!
        self.assertAlmostEqual(res["summary"]["raw_matrix_threshold_margin"], 0.02, places=12)
        self.assertGreater(res["summary"]["min_threshold_margin"], 0.50)

    def test_emergence_disappearance_q_zero_and_tau_zero(self) -> None:
        # Mixed continuation + disappearance + emergence
        mixed_task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 1.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.0]},
                {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [260.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 35, "member_fraction": 0.70, "mean_features": [280.3]},
                {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [310.0]},
            ],
        }
        mixed_res = solve_evolution_oracle(mixed_task)
        self.assertEqual(mixed_res["summary"]["continuation_count"], 1)
        self.assertEqual(mixed_res["summary"]["disappearance_count"], 1)
        self.assertEqual(mixed_res["summary"]["emergence_count"], 1)
        self.assertEqual(mixed_res["summary"]["disappeared_fraction_sum"], 0.40)
        self.assertEqual(mixed_res["summary"]["emerged_fraction_sum"], 0.30)
        self.assertEqual(mixed_res["prev_nodes"][1]["node_role"], "disappeared")
        self.assertEqual(mixed_res["curr_nodes"][1]["node_role"], "emerged")
        self.assertAlmostEqual(mixed_res["prev_nodes"][1]["threshold_margin"], 19.3, places=12)

        # All exceed threshold (q* = 0)
        zero_rel_task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 0.5,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [275.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]},
            ],
        }
        zero_res = solve_evolution_oracle(zero_rel_task)
        self.assertEqual(len(zero_res["edges"]), 0)
        self.assertEqual(zero_res["summary"]["disappearance_count"], 1)
        self.assertEqual(zero_res["summary"]["emergence_count"], 1)
        self.assertEqual(zero_res["summary"]["min_threshold_margin"], 4.5)

        # tau == 0.0 exact match boundary
        tau_zero_task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 0.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [-0.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]},
            ],
        }
        tz_res = solve_evolution_oracle(tau_zero_task)
        self.assertIsNone(tz_res["reference_scale"])
        self.assertEqual(tz_res["edges"][0]["edge_strength"], 1.0)
        self.assertTrue(tz_res["edges"][0]["high_uncertainty"])
        self.assertIn("near_threshold", tz_res["edges"][0]["uncertainty_reasons"])

    def test_exact_secondary_tie_and_permutation_invariance(self) -> None:
        # Secondary curr node C2 at [1.0] equidistant (dist = 1.0) to P0=[0.0] and P1=[2.0]
        tie_task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 1.5,
            "prev_scenarios": [
                {"cluster_id": 10, "member_count": 25, "member_fraction": 0.50, "mean_features": [2.0]},
                {"cluster_id": 20, "member_count": 25, "member_fraction": 0.50, "mean_features": [-0.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0]},
                {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [2.0]},
                {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [1.0]},
            ],
        }
        tie_res = solve_evolution_oracle(tie_task)
        sec_e = next(e for e in tie_res["edges"] if e["edge_role"] == "secondary_split")
        # Canonical order of prev puts [-0.0] (cluster_id=20) at rank 0 ahead of [2.0] (cluster_id=10)
        self.assertEqual(sec_e["prev_cluster_id"], 20)
        self.assertEqual(sec_e["secondary_margin"], 0.0)
        self.assertTrue(sec_e["ambiguous_tie"])
        self.assertEqual(sec_e["rule_basis"], "nearest_feasible_split_canonical_tiebreak")
        self.assertIn("ambiguous_tie", sec_e["uncertainty_reasons"])
        self.assertIn("near_secondary_switch", sec_e["uncertainty_reasons"])

        # Full 3x4 = 6 * 24 = 144 permutations invariance check on an asymmetric 3->4 case
        base_prev = [
            {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [275.0, 276.0]},
            {"cluster_id": 1, "member_count": 18, "member_fraction": 0.36, "mean_features": [282.0, 281.0]},
            {"cluster_id": 2, "member_count": 12, "member_fraction": 0.24, "mean_features": [295.0, 294.0]},
        ]
        base_curr = [
            {"cluster_id": 0, "member_count": 15, "member_fraction": 0.30, "mean_features": [275.2, 276.1]},
            {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [275.9, 276.8]},
            {"cluster_id": 2, "member_count": 15, "member_fraction": 0.30, "mean_features": [282.3, 280.8]},
            {"cluster_id": 3, "member_count": 10, "member_fraction": 0.20, "mean_features": [305.0, 305.0]},
        ]
        base_res = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": [0.6, 0.4],
                "threshold_coefficient": 1.0,
                "prev_dispersion": 2.0,
                "curr_dispersion": 2.0,
                "prev_scenarios": base_prev,
                "curr_scenarios": base_curr,
            }
        )
        base_phys_edges = sorted(
            (
                tuple(base_prev[e["prev_cluster_id"]]["mean_features"]),
                tuple(base_curr[e["curr_cluster_id"]]["mean_features"]),
                e["edge_role"],
                e["attributed_flow"],
                e["distance"],
            )
            for e in base_res["edges"]
        )

        for p_perm in itertools.permutations(range(3)):
            for c_perm in itertools.permutations(range(4)):
                perm_prev = [
                    {
                        **base_prev[idx],
                        "cluster_id": 100 + pos * 7,
                    }
                    for pos, idx in enumerate(p_perm)
                ]
                perm_curr = [
                    {
                        **base_curr[idx],
                        "cluster_id": 200 + pos * 11,
                    }
                    for pos, idx in enumerate(c_perm)
                ]
                p_cid_to_feats = {s["cluster_id"]: tuple(s["mean_features"]) for s in perm_prev}
                c_cid_to_feats = {s["cluster_id"]: tuple(s["mean_features"]) for s in perm_curr}
                perm_res = solve_evolution_oracle(
                    {
                        "mode": "scaled",
                        "weights": [0.6, 0.4],
                        "threshold_coefficient": 1.0,
                        "prev_dispersion": 2.0,
                        "curr_dispersion": 2.0,
                        "prev_scenarios": perm_prev,
                        "curr_scenarios": perm_curr,
                    }
                )
                perm_phys_edges = sorted(
                    (
                        p_cid_to_feats[e["prev_cluster_id"]],
                        c_cid_to_feats[e["curr_cluster_id"]],
                        e["edge_role"],
                        e["attributed_flow"],
                        e["distance"],
                    )
                    for e in perm_res["edges"]
                )
                self.assertEqual(perm_phys_edges, base_phys_edges)
                self.assertEqual(perm_res["summary"], base_res["summary"])

    def test_orthogonality_and_uncertainty_ratio_boundary(self) -> None:
        # 1. Pure morphology drift (fractions strictly constant -> exact 0.0 fraction change)
        prev_morph_totals: list[float] = []
        for shift in (0.1, 0.25, 0.5, 0.8):
            res = solve_evolution_oracle(
                {
                    "mode": "scaled",
                    "weights": [0.5, 0.5],
                    "threshold_coefficient": 1.0,
                    "prev_dispersion": 2.0,
                    "curr_dispersion": 2.0,
                    "prev_scenarios": [
                        {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.0, 280.0]},
                        {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.0, 290.0]},
                    ],
                    "curr_scenarios": [
                        {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.0 + shift, 280.0 + shift]},
                        {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.0 + shift, 290.0 + shift]},
                    ],
                }
            )
            self.assertEqual(res["summary"]["transition_tv_redistribution"], 0.0)
            self.assertEqual(res["summary"]["component_tv_residual"], 0.0)
            for e in res["edges"]:
                self.assertEqual(e["edge_delta_fraction"], 0.0)
            prev_morph_totals.append(res["summary"]["primary_total_distance"])
        self.assertEqual(prev_morph_totals, sorted(prev_morph_totals))
        self.assertEqual(len(set(prev_morph_totals)), 4)

        # 2. Pure member fraction shift (mean_features strictly identical -> exact 0.0 morphology distance)
        for delta_f, mc0, mc1 in ((0.06, 33, 17), (0.14, 37, 13), (0.24, 42, 8)):
            res_f = solve_evolution_oracle(
                {
                    "mode": "scaled",
                    "weights": [0.5, 0.5],
                    "threshold_coefficient": 1.0,
                    "prev_dispersion": 2.0,
                    "curr_dispersion": 2.0,
                    "prev_scenarios": [
                        {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.0, 281.0]},
                        {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.0, 291.0]},
                    ],
                    "curr_scenarios": [
                        {"cluster_id": 0, "member_count": mc0, "member_fraction": 0.60 + delta_f, "mean_features": [280.0, 281.0]},
                        {"cluster_id": 1, "member_count": mc1, "member_fraction": 0.40 - delta_f, "mean_features": [290.0, 291.0]},
                    ],
                }
            )
            self.assertEqual(res_f["summary"]["primary_total_distance"], 0.0)
            self.assertEqual(res_f["summary"]["all_edges_max_distance"], 0.0)
            self.assertAlmostEqual(res_f["summary"]["transition_tv_redistribution"], delta_f, places=15)

        # 3. Uncertainty `0.10 * R` clamping (R = S = 1.0, tau = 1.0: d = 0.95 -> margin 0.05R vs d = 0.85 -> margin 0.15R)
        near_thr = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "prev_dispersion": 1.0,
                "curr_dispersion": 1.0,
                "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.95]}],
            }
        )
        far_thr = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "prev_dispersion": 1.0,
                "curr_dispersion": 1.0,
                "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.85]}],
            }
        )
        self.assertTrue(near_thr["summary"]["high_uncertainty"])
        self.assertIn("near_threshold", near_thr["edges"][0]["uncertainty_reasons"])
        self.assertFalse(far_thr["summary"]["high_uncertainty"])
        self.assertEqual(far_thr["edges"][0]["uncertainty_reasons"], [])

    def test_multi_batch_6_and_8_chains_with_gaps(self) -> None:
        # 6-batch all-ok chain exercising continuation, split, merge, disappearance, emergence
        chain_6 = solve_chain_oracle(
            {
                "tracking_run_key": "abcdef1234567890fedcba",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "batches": [
                    {
                        "ordinal": 0,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [
                            {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.0]},
                            {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.0]},
                        ],
                    },
                    {
                        "ordinal": 1,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [
                            {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [280.1]},
                            {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [280.6]},
                            {"cluster_id": 2, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.1]},
                        ],
                    },
                    {
                        "ordinal": 2,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [
                            {"cluster_id": 0, "member_count": 30, "member_fraction": 0.60, "mean_features": [280.2]},
                            {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [290.2]},
                        ],
                    },
                    {
                        "ordinal": 3,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [
                            {"cluster_id": 0, "member_count": 35, "member_fraction": 0.70, "mean_features": [280.3]},
                            {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [305.0]},
                        ],
                    },
                    {
                        "ordinal": 4,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [
                            {"cluster_id": 0, "member_count": 35, "member_fraction": 0.70, "mean_features": [280.4]},
                            {"cluster_id": 1, "member_count": 15, "member_fraction": 0.30, "mean_features": [305.1]},
                        ],
                    },
                    {
                        "ordinal": 5,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [
                            {"cluster_id": 0, "member_count": 50, "member_fraction": 1.00, "mean_features": [280.5]},
                        ],
                    },
                ],
            }
        )
        self.assertTrue(verify_chain_invariants(chain_6)["all_passed"])
        cs6 = chain_6["chain_summary"]
        self.assertEqual(cs6["ok_batch_count"], 6)
        self.assertEqual(cs6["ok_transition_count"], 5)
        self.assertEqual(cs6["gap_transition_count"], 0)
        self.assertGreaterEqual(cs6["split_count"], 1)
        self.assertGreaterEqual(cs6["merge_count"], 1)
        self.assertGreaterEqual(cs6["emergence_count"], 1)
        self.assertGreaterEqual(cs6["disappearance_count"], 1)

        # Verify M2-identical branch_id prefix and split/merge cross-branch lineage links
        b_main = next(b for b in chain_6["branch_summaries"] if b["branch_id"] == "abcdef123456:b00:k00")
        self.assertEqual(b_main["birth_ordinal"], 0)
        self.assertEqual(b_main["end_ordinal"], 5)
        self.assertEqual(b_main["node_count"], 6)
        self.assertEqual(b_main["split_out_event_count"], 1)
        self.assertEqual(b_main["merge_in_event_count"], 1)
        self.assertEqual(b_main["spawned_secondary_branch_ids"], ["abcdef123456:b01:k01"])
        self.assertEqual(b_main["absorbed_secondary_branch_ids"], ["abcdef123456:b01:k01"])

        b_sec = next(b for b in chain_6["branch_summaries"] if b["branch_id"] == "abcdef123456:b01:k01")
        self.assertEqual(b_sec["split_from_parent_branch_id"], "abcdef123456:b00:k00")
        self.assertEqual(b_sec["merged_into_branch_id"], "abcdef123456:b00:k00")

        # 8-batch chain with `missing` at b2 and consecutive `failed` at b5, b6
        chain_8 = solve_chain_oracle(
            {
                "tracking_run_key": "99887766554433221100",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "batches": [
                    {
                        "ordinal": 0,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
                    },
                    {
                        "ordinal": 1,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.2]}],
                    },
                    {"ordinal": 2, "status": "missing", "status_reason": "missing_om_file"},
                    {
                        "ordinal": 3,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.3]}],
                    },
                    {
                        "ordinal": 4,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.4]}],
                    },
                    {"ordinal": 5, "status": "failed", "status_reason": "INSUFFICIENT_RANK"},
                    {"ordinal": 6, "status": "failed", "status_reason": "EMPTY_CLUSTER"},
                    {
                        "ordinal": 7,
                        "status": "ok",
                        "dispersion": 1.0,
                        "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.5]}],
                    },
                ],
            }
        )
        self.assertTrue(verify_chain_invariants(chain_8)["all_passed"])
        cs8 = chain_8["chain_summary"]
        self.assertEqual(cs8["ok_batch_count"], 5)
        self.assertEqual(cs8["missing_batch_count"], 1)
        self.assertEqual(cs8["failed_batch_count"], 2)
        self.assertEqual(cs8["ok_transition_count"], 2)
        self.assertEqual(cs8["gap_transition_count"], 5)
        self.assertEqual(cs8["emergence_count"], 0)
        self.assertEqual(cs8["disappearance_count"], 0)
        self.assertEqual(cs8["gap_interrupted_count"], 2)
        self.assertEqual(cs8["gap_restarted_count"], 2)

        gap_reasons = {tr["prev_ordinal"]: tr["gap_reason"] for tr in chain_8["transitions"] if tr["chain_broken"]}
        self.assertEqual(
            gap_reasons,
            {
                1: "curr_unavailable",
                2: "prev_unavailable",
                4: "curr_unavailable",
                5: "both_unavailable",
                6: "prev_unavailable",
            },
        )
        post_gap_nodes = [n for n in chain_8["nodes"] if n["ordinal"] in (3, 7)]
        for pn in post_gap_nodes:
            self.assertEqual(pn["assignment_reason"], "previous_planned_batch_unavailable")
            self.assertEqual(pn["incoming_role"], "gap_restarted")

    def test_input_validation_and_backbone_tamper_rejection(self) -> None:
        valid_base = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 1.0,
            "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
            "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.2]}],
        }

        # ZeroMemberScenario
        bad_zero_mc = json.loads(json.dumps(valid_base))
        bad_zero_mc["prev_scenarios"][0]["member_count"] = 0
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(bad_zero_mc)
        self.assertEqual(ctx.exception.code, "ZERO_MEMBER_SCENARIO")

        # InvalidMemberFraction
        bad_frac = json.loads(json.dumps(valid_base))
        bad_frac["curr_scenarios"][0]["member_fraction"] = 0.0
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(bad_frac)
        self.assertEqual(ctx.exception.code, "INVALID_MEMBER_FRACTION")

        # InvalidFractionSum
        bad_sum = json.loads(json.dumps(valid_base))
        bad_sum["prev_scenarios"][0]["member_fraction"] = 0.99
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(bad_sum)
        self.assertEqual(ctx.exception.code, "INVALID_FRACTION_SUM")

        # InvalidEvolutionConfig
        bad_cfg = json.loads(json.dumps(valid_base))
        bad_cfg["config"] = {"fraction_sum_tolerance": 0.01}
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(bad_cfg)
        self.assertEqual(ctx.exception.code, "INVALID_EVOLUTION_CONFIG")

        # InvalidPrimaryLink (exceeds threshold)
        bad_link = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 0.5,
            "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
            "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [285.0]}],
            "backbone": {
                "prev_cluster_ids": [0],
                "curr_cluster_ids": [0],
                "threshold": 0.5,
                "primary_links": [(0, 0)],
            },
        }
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(bad_link)
        self.assertEqual(ctx.exception.code, "INVALID_PRIMARY_LINK")

        # BackboneInconsistency (suboptimal empty primary_links when edge is feasible)
        bad_inconsist = json.loads(json.dumps(valid_base))
        bad_inconsist["backbone"] = {
            "prev_cluster_ids": [0],
            "curr_cluster_ids": [0],
            "threshold": 1.0,
            "primary_links": [],
        }
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(bad_inconsist)
        self.assertEqual(ctx.exception.code, "BACKBONE_INCONSISTENCY")

        # Subnormal pair_scale ratio overflow -> NUMERICAL_FAILURE ([H2])
        subnormal_overflow = {
            "mode": "scaled",
            "weights": [1.0],
            "threshold_coefficient": 1e308,
            "prev_dispersion": 1e-308,
            "curr_dispersion": 1e-308,
            "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
            "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [10.0]}],
        }
        with self.assertRaises(OracleValidationError) as ctx:
            solve_evolution_oracle(subnormal_overflow)
        self.assertEqual(ctx.exception.code, "NUMERICAL_FAILURE")

        # Non-whitelisted failed batch error code in solve_chain_oracle -> INVALID_EVOLUTION_CONFIG ([M3])
        bad_chain = {
            "tracking_run_key": "badchain000000",
            "threshold_coefficient": 1.0,
            "weights": [1.0],
            "batches": [
                {"ordinal": 0, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}]},
                {"ordinal": 1, "status": "failed", "error_code": "NUMERICAL_FAILURE"},
                {"ordinal": 2, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.1]}]},
                {"ordinal": 3, "status": "ok", "dispersion": 1.0, "scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.2]}]},
            ],
        }
        with self.assertRaises(OracleValidationError) as ctx:
            solve_chain_oracle(bad_chain)
        self.assertEqual(ctx.exception.code, "INVALID_EVOLUTION_CONFIG")
        self.assertEqual(ctx.exception.prev_index, 1)


class TestM3EvolutionStabilityCertificate(unittest.TestCase):
    """Unit tests for M3 4-Condition Outward-Rounded Fraction Stability Certificate."""

    def test_rational_sqrt_bounds_and_intervals(self) -> None:
        self.assertEqual(rational_sqrt_bounds(Fraction(0)), (Fraction(0), Fraction(0)))
        self.assertEqual(rational_sqrt_bounds(Fraction(9, 16)), (Fraction(3, 4), Fraction(3, 4)))

        for q in (Fraction(2), Fraction(3, 7), Fraction.from_float(1e-12), Fraction.from_float(1e6)):
            lo, hi = rational_sqrt_bounds(q, bits=128)
            self.assertLessEqual(lo * lo, q)
            self.assertGreaterEqual(hi * hi, q)
            self.assertLessEqual(hi - lo, Fraction(1, 1 << 128))

        iv_a = RationalInterval.from_float_ulp(1.25, ulp_multiplier=4)
        iv_b = RationalInterval.from_float_ulp(0.75, ulp_multiplier=4)
        prod = iv_a * iv_b
        self.assertLessEqual(prod.lo, Fraction(15, 16))
        self.assertGreaterEqual(prod.hi, Fraction(15, 16))

        rmse_lo, rmse_hi = compute_weighted_rmse_rational_bounds([0.0, 0.0], [1.0, 2.0], [1.0, 1.0])
        self.assertLessEqual(rmse_lo * rmse_lo, Fraction(5, 2))
        self.assertGreaterEqual(rmse_hi * rmse_hi, Fraction(5, 2))

    def test_certified_pair_zero_flips_under_50_perturbed_trials(self) -> None:
        # Rich 4 -> 5 multi-event evolution pair (merge + continuation + split + emergence)
        # with SameCount primary edges (gaps 0.45, 0.75, 0.50 > 4 * 3 * 0.03 = 0.36)
        # and multi-candidate secondary merge (secondary_margin = 0.30 > 4 * 0.03 = 0.12).
        weights = [0.4, 0.35, 0.25]
        prev_scens = [
            {"cluster_id": 0, "member_count": 15, "member_fraction": 0.30, "mean_features": [280.00, 280.00, 280.00]},
            {"cluster_id": 1, "member_count": 8, "member_fraction": 0.16, "mean_features": [280.45, 280.45, 280.45]},
            {"cluster_id": 2, "member_count": 12, "member_fraction": 0.24, "mean_features": [281.20, 281.20, 281.20]},
            {"cluster_id": 3, "member_count": 15, "member_fraction": 0.30, "mean_features": [285.00, 285.00, 285.00]},
        ]
        curr_scens = [
            {"cluster_id": 0, "member_count": 17, "member_fraction": 0.34, "mean_features": [280.00, 280.00, 280.00]},
            {"cluster_id": 1, "member_count": 10, "member_fraction": 0.20, "mean_features": [281.20, 281.20, 281.20]},
            {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [285.00, 285.00, 285.00]},
            {"cluster_id": 3, "member_count": 8, "member_fraction": 0.16, "mean_features": [285.50, 285.50, 285.50]},
            {"cluster_id": 4, "member_count": 5, "member_fraction": 0.10, "mean_features": [290.00, 290.00, 290.00]},
        ]
        base_task = {
            "mode": "scaled",
            "weights": weights,
            "threshold_coefficient": 1.0,
            "prev_dispersion": 1.50,
            "curr_dispersion": 1.50,
            "prev_scenarios": prev_scens,
            "curr_scenarios": curr_scens,
        }
        base_res = solve_evolution_oracle(base_task)
        # Verify rich baseline topology: 1 merge ({0,1}->0), 1 continuation (2->1), 1 split (3->{2,3}), 1 emergence (4)
        self.assertEqual(base_res["summary"]["merge_count"], 1)
        self.assertEqual(base_res["summary"]["continuation_count"], 1)
        self.assertEqual(base_res["summary"]["split_count"], 1)
        self.assertEqual(base_res["summary"]["emergence_count"], 1)

        eps_abs = 0.03  # 2% of S = 1.50
        cert = evaluate_evolution_stability_certificate(
            base_res,
            epsilon_abs=eps_abs,
            threshold_coefficient=1.0,
            delta_s=0.0,
            prev_scenarios=prev_scens,
            curr_scenarios=curr_scens,
            weights=weights,
        )
        self.assertTrue(cert["certified"], f"Expected certified=True, got: {cert}")
        self.assertTrue(cert["cond1_satisfied"])
        self.assertTrue(cert["cond2_satisfied"])
        self.assertTrue(cert["cond3_satisfied"])
        self.assertTrue(cert["cond4_satisfied"])
        self.assertEqual(cert["cond2_status"], "evaluated")
        self.assertEqual(cert["cond4_status"], "evaluated")

        # Run 50 perturbed trials within weighted RMS <= eps_abs and assert strictly 0 flips
        base_sig = _extract_topology_signature(base_res)
        rng = random.Random(20261125)
        flips = 0
        for _ in range(50):
            pert_prev = [
                {
                    **sc,
                    "mean_features": _perturb_features_exact_rms(
                        sc["mean_features"], weights, eps_abs, rng
                    ),
                }
                for sc in prev_scens
            ]
            pert_curr = [
                {
                    **sc,
                    "mean_features": _perturb_features_exact_rms(
                        sc["mean_features"], weights, eps_abs, rng
                    ),
                }
                for sc in curr_scens
            ]
            pert_res = solve_evolution_oracle(
                {
                    **base_task,
                    "prev_scenarios": pert_prev,
                    "curr_scenarios": pert_curr,
                }
            )
            if _extract_topology_signature(pert_res) != base_sig:
                flips += 1
        self.assertEqual(flips, 0)

    def test_near_threshold_denied_vs_inactive_cross_edge_immunity(self) -> None:
        # 1. Active edge within 2 * eps_abs + c * delta_s of tau -> Denied
        near_thr_res = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "prev_dispersion": 1.0,
                "curr_dispersion": 1.0,
                "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.97]}],
            }
        )
        cert_denied = evaluate_evolution_stability_certificate(
            near_thr_res,
            epsilon_abs=0.015,
            threshold_coefficient=1.0,
            delta_s=0.005,
        )
        self.assertFalse(cert_denied["certified"])
        self.assertFalse(cert_denied["cond1_satisfied"])

        # 2. Out-of-threshold emergence node with d - tau = 0.02 <= 2 * 0.015 -> Denied
        near_emerg_res = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": [1.0],
                "threshold_coefficient": 1.0,
                "prev_dispersion": 1.0,
                "curr_dispersion": 1.0,
                "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]}],
                "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [1.02]}],
            }
        )
        cert_emerg_denied = evaluate_evolution_stability_certificate(
            near_emerg_res,
            epsilon_abs=0.015,
        )
        self.assertFalse(cert_emerg_denied["certified"])
        self.assertFalse(cert_emerg_denied["cond1_satisfied"])

        # 3. Theorem 4: Inactive feasible cross-edge (0, 1) with d = 0.995 <= tau = 1.00
        # has raw_matrix_threshold_margin = 0.005, yet active min_threshold_margin = 1.00 -> Certified!
        inactive_cross_res = solve_evolution_oracle(
            {
                "mode": "absolute",
                "weights": [1.0],
                "threshold": 1.00,
                "prev_scenarios": [
                    {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.0]},
                    {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.995]},
                ],
                "curr_scenarios": [
                    {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.0]},
                    {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.995]},
                ],
            }
        )
        self.assertAlmostEqual(inactive_cross_res["summary"]["raw_matrix_threshold_margin"], 0.005, places=12)
        self.assertEqual(inactive_cross_res["summary"]["min_threshold_margin"], 1.0)
        cert_inactive_ok = evaluate_evolution_stability_certificate(
            inactive_cross_res,
            epsilon_abs=0.02,
        )
        self.assertTrue(cert_inactive_ok["certified"])

    def test_one_ulp_above_bound_conservatively_denied(self) -> None:
        eps = 0.01
        barely_thr = math.nextafter(2.0 * eps, math.inf)
        self.assertGreater(barely_thr, 2.0 * eps)
        cert_thr = evaluate_evolution_stability_certificate(
            epsilon_abs=eps,
            active_threshold_margin=barely_thr,
            optimality_margin=0.50,
            q_star=2,
            has_same_count_alt=True,
            optimal_solution_count=1,
            secondary_edge_count=0,
        )
        self.assertFalse(cert_thr["certified"])
        self.assertFalse(cert_thr["cond1_satisfied"])

        barely_sec = math.nextafter(4.0 * eps, math.inf)
        self.assertGreater(barely_sec, 4.0 * eps)
        cert_sec = evaluate_evolution_stability_certificate(
            epsilon_abs=eps,
            active_threshold_margin=0.50,
            optimality_margin=0.50,
            q_star=2,
            has_same_count_alt=True,
            optimal_solution_count=1,
            secondary_margin=barely_sec,
            has_multi_candidate_secondary=True,
            secondary_edge_count=1,
        )
        self.assertFalse(cert_sec["certified"])
        self.assertFalse(cert_sec["cond4_satisfied"])

    def test_near_primary_switch_and_exact_primary_tie_denied(self) -> None:
        # Near-primary-switch: 1 -> 2 where C0 at 0.20, C1 at 0.23 -> q* = 1, SameCount gap = 0.03 <= 4 * 1 * 0.01 = 0.04
        near_prim_res = solve_evolution_oracle(
            {
                "mode": "absolute",
                "weights": [1.0],
                "threshold": 1.0,
                "prev_scenarios": [
                    {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [0.0]},
                ],
                "curr_scenarios": [
                    {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.20]},
                    {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.23]},
                ],
            }
        )
        cert_near_prim = evaluate_evolution_stability_certificate(near_prim_res, epsilon_abs=0.01)
        self.assertFalse(cert_near_prim["certified"])
        self.assertTrue(cert_near_prim["cond1_satisfied"])
        self.assertFalse(cert_near_prim["cond2_satisfied"])
        self.assertTrue(cert_near_prim["cond3_satisfied"])

        # Exact primary tie: 2x2 symmetric square where optima_count == 2, Tied gap == 0.0
        tied_prim_res = solve_evolution_oracle(
            {
                "mode": "absolute",
                "weights": [0.5, 0.5],
                "threshold": 3.0,
                "prev_scenarios": [
                    {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.0, 0.0]},
                    {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [2.0, 2.0]},
                ],
                "curr_scenarios": [
                    {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.0, 2.0]},
                    {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [2.0, 0.0]},
                ],
            }
        )
        self.assertEqual(tied_prim_res["backbone"]["optima_count"], 2)
        cert_tied_prim = evaluate_evolution_stability_certificate(tied_prim_res, epsilon_abs=0.01)
        self.assertFalse(cert_tied_prim["certified"])
        self.assertFalse(cert_tied_prim["cond2_satisfied"])
        self.assertFalse(cert_tied_prim["cond3_satisfied"])

    def test_near_secondary_switch_with_flip_witness_and_exact_secondary_tie(self) -> None:
        # 2 -> 3 where P0=0.0->C0=0.0 and P1=1.0->C1=1.0 are rock-solid primary links,
        # while secondary child C2=0.485 has d(P0, C2)=0.485 vs d(P1, C2)=0.515 -> secondary_margin = 0.03 <= 4 * 0.01 = 0.04!
        near_sec_task = {
            "mode": "absolute",
            "weights": [1.0],
            "threshold": 1.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [0.0]},
                {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [1.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [0.0]},
                {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [1.0]},
                {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [0.485]},
            ],
        }
        near_sec_res = solve_evolution_oracle(near_sec_task)
        cert_near_sec = evaluate_evolution_stability_certificate(near_sec_res, epsilon_abs=0.01)
        self.assertFalse(cert_near_sec["certified"])
        self.assertTrue(cert_near_sec["cond1_satisfied"])
        self.assertTrue(cert_near_sec["cond2_satisfied"])
        self.assertTrue(cert_near_sec["cond3_satisfied"])
        self.assertFalse(cert_near_sec["cond4_satisfied"])

        # Explicit perturbation of <= 0.01 on each node flips C2's split parent from P0 to P1!
        flipped_task = json.loads(json.dumps(near_sec_task))
        flipped_task["prev_scenarios"][0]["mean_features"] = [-0.01]
        flipped_task["prev_scenarios"][1]["mean_features"] = [0.99]
        flipped_task["curr_scenarios"][2]["mean_features"] = [0.495]
        flipped_res = solve_evolution_oracle(flipped_task)
        orig_sec = next(e for e in near_sec_res["edges"] if e["edge_role"] == "secondary_split")
        new_sec = next(e for e in flipped_res["edges"] if e["edge_role"] == "secondary_split")
        self.assertEqual(orig_sec["prev_cluster_id"], 0)
        self.assertEqual(new_sec["prev_cluster_id"], 1)

        # Exact secondary tie: C2 at 0.50 -> secondary_margin == 0.0 -> secondary_tie_present
        exact_sec_task = json.loads(json.dumps(near_sec_task))
        exact_sec_task["curr_scenarios"][2]["mean_features"] = [0.50]
        exact_sec_res = solve_evolution_oracle(exact_sec_task)
        cert_exact_sec = evaluate_evolution_stability_certificate(exact_sec_res, epsilon_abs=0.01)
        self.assertFalse(cert_exact_sec["certified"])
        self.assertFalse(cert_exact_sec["cond4_satisfied"])
        self.assertEqual(cert_exact_sec["cond4_status"], "secondary_tie_present")

    def test_q_zero_and_count_drop_only_special_cases(self) -> None:
        # 1. q* = 0 (empty matching, all distances well above tau) -> Certified
        q_zero_res = solve_evolution_oracle(
            {
                "mode": "absolute",
                "weights": [1.0],
                "threshold": 1.0,
                "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [270.0]}],
                "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
            }
        )
        cert_q0 = evaluate_evolution_stability_certificate(q_zero_res, epsilon_abs=0.05)
        self.assertTrue(cert_q0["certified"])
        self.assertEqual(cert_q0["cond2_status"], "empty_matching_q_zero")
        self.assertEqual(cert_q0["cond4_status"], "no_secondary_edges")

        # 2. CountDrop-only (q* = 2 well-separated continuations, zero same-count alternatives) -> Certified
        count_drop_task = {
            "mode": "absolute",
            "weights": [0.5, 0.5],
            "threshold": 1.0,
            "prev_scenarios": [
                {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [275.0, 275.0]},
                {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [290.0, 290.0]},
            ],
            "curr_scenarios": [
                {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [275.2, 275.2]},
                {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [290.2, 290.2]},
            ],
        }
        cd_res = solve_evolution_oracle(count_drop_task)
        self.assertTrue(all(e["primary_alternative_status"] == "count_drop" for e in cd_res["edges"]))
        cert_cd = evaluate_evolution_stability_certificate(cd_res, epsilon_abs=0.05)
        self.assertTrue(cert_cd["certified"])
        self.assertEqual(cert_cd["cond2_status"], "count_drop_only")

    def test_evaluation_mapping_ambiguous_denied(self) -> None:
        # Detect exact overlap tie via overlap_cluster_mapping
        base_parts = {0: {"m0", "m1"}, 1: {"m2", "m3"}}
        pert_parts = {0: {"m0", "m2"}, 1: {"m1", "m3"}}
        _, is_ambig = overlap_cluster_mapping(base_parts, pert_parts)
        self.assertTrue(is_ambig)

        robust_res = solve_evolution_oracle(
            {
                "mode": "absolute",
                "weights": [1.0],
                "threshold": 2.0,
                "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
                "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.1]}],
            }
        )
        # Without mapping ambiguity -> True; with mapping ambiguity -> False
        self.assertTrue(evaluate_evolution_stability_certificate(robust_res, epsilon_abs=0.01)["certified"])
        cert_ambig = evaluate_evolution_stability_certificate(
            robust_res,
            epsilon_abs=0.01,
            evaluation_mapping_ambiguous=True,
        )
        self.assertFalse(cert_ambig["certified"])
        self.assertEqual(cert_ambig["status"], "evaluation_mapping_ambiguous")
        self.assertTrue(cert_ambig["evaluation_mapping_ambiguous"])

    def test_synthetic_cohort_both_certified_and_uncertified_groups_non_empty(self) -> None:
        cohort = [
            # Certified cases (far from all 4 boundaries)
            (
                "certified_continuation_count_drop",
                {
                    "mode": "absolute",
                    "weights": [1.0],
                    "threshold": 2.0,
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.2]}],
                },
                0.02,
                False,
            ),
            (
                "certified_split_and_merge",
                {
                    "mode": "absolute",
                    "weights": [1.0],
                    "threshold": 1.5,
                    "prev_scenarios": [
                        {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.0]},
                        {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.45]},
                    ],
                    "curr_scenarios": [
                        {"cluster_id": 0, "member_count": 50, "member_fraction": 1.00, "mean_features": [280.0]},
                    ],
                },
                0.02,
                False,
            ),
            (
                "certified_q_zero_disappearance_emergence",
                {
                    "mode": "absolute",
                    "weights": [1.0],
                    "threshold": 1.0,
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [275.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [285.0]}],
                },
                0.02,
                False,
            ),
            # Uncertified cases (near-threshold, near-primary-switch, near-secondary-switch, mapping-ambiguous)
            (
                "uncertified_near_threshold",
                {
                    "mode": "absolute",
                    "weights": [1.0],
                    "threshold": 1.0,
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.99]}],
                },
                0.02,
                False,
            ),
            (
                "uncertified_near_primary_switch",
                {
                    "mode": "absolute",
                    "weights": [1.0],
                    "threshold": 1.5,
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
                    "curr_scenarios": [
                        {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.30]},
                        {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.32]},
                    ],
                },
                0.02,
                False,
            ),
            (
                "uncertified_mapping_ambiguous",
                {
                    "mode": "absolute",
                    "weights": [1.0],
                    "threshold": 2.0,
                    "prev_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.0]}],
                    "curr_scenarios": [{"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [280.1]}],
                },
                0.02,
                True,
            ),
        ]

        certified_group: list[str] = []
        uncertified_group: list[str] = []
        rng = random.Random(4242)

        for cid, task, eps, map_ambig in cohort:
            ev_res = solve_evolution_oracle(task)
            cert = evaluate_evolution_stability_certificate(
                ev_res,
                epsilon_abs=eps,
                evaluation_mapping_ambiguous=map_ambig,
            )
            if cert["certified"]:
                certified_group.append(cid)
                base_sig = _extract_topology_signature(ev_res)
                for _ in range(25):
                    pert_p = [
                        {
                            **s,
                            "mean_features": _perturb_features_exact_rms(
                                s["mean_features"], task["weights"], eps, rng
                            ),
                        }
                        for s in task["prev_scenarios"]
                    ]
                    pert_c = [
                        {
                            **s,
                            "mean_features": _perturb_features_exact_rms(
                                s["mean_features"], task["weights"], eps, rng
                            ),
                        }
                        for s in task["curr_scenarios"]
                    ]
                    pert_ev = solve_evolution_oracle(
                        {**task, "prev_scenarios": pert_p, "curr_scenarios": pert_c}
                    )
                    self.assertEqual(_extract_topology_signature(pert_ev), base_sig)
            else:
                uncertified_group.append(cid)

        self.assertGreater(len(certified_group), 0)
        self.assertGreater(len(uncertified_group), 0)
        self.assertEqual(len(certified_group), 3)
        self.assertEqual(len(uncertified_group), 3)

    def test_m3_runner_shaped_backbone_without_eligible_matrix(self) -> None:
        # m3_runner omits `eligible_matrix` from `backbone`; verify both Cond 1 and Cond 4 work cleanly
        prev_sc = [
            {"cluster_id": 0, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.0]},
            {"cluster_id": 1, "member_count": 25, "member_fraction": 0.50, "mean_features": [281.0]},
        ]
        curr_sc = [
            {"cluster_id": 0, "member_count": 20, "member_fraction": 0.40, "mean_features": [280.0]},
            {"cluster_id": 1, "member_count": 20, "member_fraction": 0.40, "mean_features": [281.0]},
            {"cluster_id": 2, "member_count": 10, "member_fraction": 0.20, "mean_features": [280.3]},
        ]
        weights = [1.0]
        full_res = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": weights,
                "threshold_coefficient": 1.0,
                "prev_dispersion": 1.2,
                "curr_dispersion": 1.2,
                "prev_scenarios": prev_sc,
                "curr_scenarios": curr_sc,
            }
        )
        runner_shaped_res = json.loads(json.dumps(full_res))
        runner_shaped_res["backbone"].pop("eligible_matrix", None)
        self.assertNotIn("eligible_matrix", runner_shaped_res["backbone"])

        cert_full = evaluate_evolution_stability_certificate(
            full_res,
            epsilon_abs=0.02,
            threshold_coefficient=1.0,
            prev_scenarios=prev_sc,
            curr_scenarios=curr_sc,
            weights=weights,
        )
        cert_runner_shaped = evaluate_evolution_stability_certificate(
            runner_shaped_res,
            epsilon_abs=0.02,
            threshold_coefficient=1.0,
            prev_scenarios=prev_sc,
            curr_scenarios=curr_sc,
            weights=weights,
        )
        self.assertTrue(cert_runner_shaped["certified"])
        self.assertEqual(cert_runner_shaped, cert_full)

    def test_scrambled_scenario_list_order_cluster_id_mapping(self) -> None:
        # Verify prev_scenarios / curr_scenarios mapped by cluster_id rather than list position
        prev_sorted = [
            {"cluster_id": 10, "member_count": 25, "member_fraction": 0.50, "mean_features": [280.0]},
            {"cluster_id": 20, "member_count": 25, "member_fraction": 0.50, "mean_features": [281.0]},
        ]
        curr_sorted = [
            {"cluster_id": 100, "member_count": 20, "member_fraction": 0.40, "mean_features": [280.0]},
            {"cluster_id": 200, "member_count": 20, "member_fraction": 0.40, "mean_features": [281.0]},
            {"cluster_id": 300, "member_count": 10, "member_fraction": 0.20, "mean_features": [280.3]},
        ]
        weights = [1.0]
        res = solve_evolution_oracle(
            {
                "mode": "scaled",
                "weights": weights,
                "threshold_coefficient": 1.0,
                "prev_dispersion": 1.2,
                "curr_dispersion": 1.2,
                "prev_scenarios": prev_sorted,
                "curr_scenarios": curr_sorted,
            }
        )
        self.assertEqual(res["prev_cluster_ids"], [10, 20])
        self.assertEqual(res["curr_cluster_ids"], [100, 200, 300])

        prev_reversed = list(reversed(prev_sorted))  # [20, 10] vs prev_cluster_ids [10, 20]
        curr_reversed = list(reversed(curr_sorted))  # [300, 200, 100] vs curr_cluster_ids [100, 200, 300]

        cert_sorted = evaluate_evolution_stability_certificate(
            res,
            epsilon_abs=0.02,
            threshold_coefficient=1.0,
            prev_scenarios=prev_sorted,
            curr_scenarios=curr_sorted,
            weights=weights,
        )
        cert_scrambled = evaluate_evolution_stability_certificate(
            res,
            epsilon_abs=0.02,
            threshold_coefficient=1.0,
            prev_scenarios=prev_reversed,
            curr_scenarios=curr_reversed,
            weights=weights,
        )
        self.assertTrue(cert_sorted["certified"])
        self.assertEqual(cert_scrambled, cert_sorted)

    def test_large_magnitude_catastrophic_cancellation_rejected(self) -> None:
        # At feature magnitude 5.36870912e8 (2^29), binary64 ULP is 2^-23 (~1.192e-7) — 2^29 times
        # larger than ulp(1.0), and squared magnitude ~2^58 has ULP 64.0.
        # A tiny apparent float margin |d_ij - tau| ~ 5e-7 > 2 * eps_abs (2e-7) lies inside the
        # coordinate-scale binary64 rounding envelope and must NOT be certified.
        base_coord = 5.36870912e8  # 2.0 ** 29
        prev_sc = [
            {"cluster_id": 0, "member_count": 50, "member_fraction": 1.0, "mean_features": [base_coord]},
        ]
        curr_sc = [
            {
                "cluster_id": 0,
                "member_count": 50,
                "member_fraction": 1.0,
                "mean_features": [base_coord + 1.0 + 4.0 * math.ulp(base_coord)],
            },
        ]
        weights = [1.0]
        tau = 1.0
        ev_res = solve_evolution_oracle(
            {
                "mode": "absolute",
                "weights": weights,
                "threshold": tau,
                "prev_scenarios": prev_sc,
                "curr_scenarios": curr_sc,
            }
        )
        thr_margin = ev_res["summary"]["min_threshold_margin"]
        eps_abs = 1e-7
        self.assertGreater(thr_margin, 2.0 * eps_abs)

        # 1. With evolution_result + scenarios supplied -> rejected via scen_feat_scale
        cert_with_scens = evaluate_evolution_stability_certificate(
            ev_res,
            epsilon_abs=eps_abs,
            prev_scenarios=prev_sc,
            curr_scenarios=curr_sc,
            weights=weights,
        )
        self.assertFalse(cert_with_scens["certified"])
        self.assertFalse(cert_with_scens["cond1_satisfied"])

        # 2. Scalar-override mode with feature_scale supplied -> rejected
        cert_scalar_fscale = evaluate_evolution_stability_certificate(
            epsilon_abs=eps_abs,
            feature_scale=base_coord,
            threshold=tau,
            active_threshold_margin=thr_margin,
            q_star=0,
            optimal_solution_count=1,
            secondary_edge_count=0,
        )
        self.assertFalse(cert_scalar_fscale["certified"])
        self.assertFalse(cert_scalar_fscale["cond1_satisfied"])

        # 3. Scalar-override mode with scenarios + threshold supplied -> rejected via rational bounds & scen_feat_scale
        cert_scalar_scens = evaluate_evolution_stability_certificate(
            epsilon_abs=eps_abs,
            threshold=tau,
            prev_scenarios=prev_sc,
            curr_scenarios=curr_sc,
            weights=weights,
            active_threshold_margin=thr_margin,
            q_star=0,
            optimal_solution_count=1,
            secondary_edge_count=0,
        )
        self.assertFalse(cert_scalar_scens["certified"])
        self.assertFalse(cert_scalar_scens["cond1_satisfied"])


def _atomic_write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp_m3_stab_", suffix=".json", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, allow_nan=False)
            handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Run M3 oracle & 4-condition stability certificate tests")
    parser.add_argument("--out-report", type=Path, default=None, help="Optional JSON report path")
    args, remaining = parser.parse_known_args()

    if args.out_report is None:
        unittest.main(argv=[sys.argv[0], *remaining], verbosity=2)
        return 0

    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    buf = io.StringIO()
    runner = unittest.TextTestRunner(stream=buf, verbosity=2)
    result = runner.run(suite)
    sys.stdout.write(buf.getvalue())

    report = {
        "schema_version": "m3.test-stability-report.v1",
        "tests_run": result.testsRun,
        "failures": len(result.failures),
        "errors": len(result.errors),
        "status": "PASS" if result.wasSuccessful() else "FAIL",
    }
    _atomic_write_report(args.out_report, report)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
