#!/usr/bin/env python3
"""Independent Fraction-based matching oracle for M2 scenario matching.

Implements exact lexicographic optimization:
1. Maximize matched link count
2. Minimize exact sum of weighted distances (evaluated using fractions.Fraction)
3. Canonical tie-breaking based on field contents (-0 normalized, total_cmp equivalent)
4. Exact substitutability (tied, same_count, count_drop, no_same_count_alternative)
5. Unmatched classification (no_candidate_within_threshold vs unselected_in_global_competition)
6. Independent 4,000 random cases generator with fixed seed
"""

from __future__ import annotations

import json
import math
import random
import sys
from fractions import Fraction
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import is_finite_number, normalize_floats


def compute_weighted_rmse_exact(a: list[float], b: list[float], weights: list[float]) -> float:
    """Compute weighted RMSE using standard binary64 formula w_g * ((a_g - b_g) * (a_g - b_g))."""
    w_sum = math.fsum(weights)
    diff_sq_sum = math.fsum(w * ((x - y) * (x - y)) for w, x, y in zip(weights, a, b))
    return math.sqrt(diff_sq_sum / w_sum)


def compute_ensemble_dispersion_exact(members: list[list[float]], weights: list[float]) -> float:
    """Compute population ensemble dispersion using one flat fsum over all (member, point) terms."""
    n_members = len(members)
    n_points = len(weights)
    w_sum = math.fsum(weights)
    mean_field = [math.fsum(m[g] for m in members) / n_members for g in range(n_points)]
    q_sum = math.fsum(
        weights[g] * ((members[n][g] - mean_field[g]) * (members[n][g] - mean_field[g]))
        for n in range(n_members)
        for g in range(n_points)
    )
    return math.sqrt((q_sum / n_members) / w_sum)


def enumerate_partial_matchings(m: int, n: int, feasible_fn) -> list[list[tuple[int, int]]]:
    """Exhaustively enumerate all valid one-to-one partial matchings."""
    results: list[list[tuple[int, int]]] = []

    def backtrack(i: int, used_curr: set[int], current: list[tuple[int, int]]) -> None:
        if i == m:
            results.append(list(current))
            return
        # Option 1: leave prev scenario i unmatched
        backtrack(i + 1, used_curr, current)
        # Option 2: match prev scenario i to an unused curr scenario j (if feasible)
        for j in range(n):
            if j not in used_curr and feasible_fn(i, j):
                used_curr.add(j)
                current.append((i, j))
                backtrack(i + 1, used_curr, current)
                current.pop()
                used_curr.remove(j)

    backtrack(0, set(), [])
    return results


def matching_value(matching: list[tuple[int, int]], dist_matrix: list[list[float]]) -> tuple[int, Fraction]:
    """Compute (link_count, exact_distance_sum) for a matching."""
    count = len(matching)
    # Using fractions.Fraction for exact representation of float values
    dist_sum = sum((Fraction(dist_matrix[i][j]) for i, j in matching), Fraction(0))
    return (count, dist_sum)


def is_better(val_a: tuple[int, Fraction], val_b: tuple[int, Fraction]) -> bool:
    """Return True if val_a is strictly better than val_b under lexicographic order."""
    if val_a[0] > val_b[0]:
        return True
    if val_a[0] < val_b[0]:
        return False
    return val_a[1] < val_b[1]


def canonical_scenario_order(scenarios: list[dict[str, Any]]) -> list[int]:
    """Return indices of scenarios sorted by canonical key (-0 normalized, float total_cmp)."""
    def key_fn(item: tuple[int, dict[str, Any]]):
        idx, sc = item
        norm_feats = [0.0 if (isinstance(x, float) and x == 0.0) else x for x in sc.get("mean_features", [])]
        cid = sc.get("cluster_id", idx)
        return (norm_feats, cid)

    sorted_pairs = sorted(enumerate(scenarios), key=key_fn)
    return [idx for idx, _ in sorted_pairs]


def solve_oracle(match_task: dict[str, Any]) -> dict[str, Any]:
    """Solve scenario matching problem using exact exhaustive search."""
    weights = match_task["weights"]
    prev_scens = match_task["prev_scenarios"]
    curr_scens = match_task["curr_scenarios"]
    tau = match_task["threshold"]

    m = len(prev_scens)
    n = len(curr_scens)

    # Compute pairwise distance matrix
    dist_matrix: list[list[float]] = []
    for p in prev_scens:
        row = []
        for c in curr_scens:
            d = compute_weighted_rmse_exact(p["mean_features"], c["mean_features"], weights)
            row.append(d)
        dist_matrix.append(row)

    feasible = lambda i, j: dist_matrix[i][j] <= tau

    # 1. Enumerate all partial matchings
    all_matchings = enumerate_partial_matchings(m, n, feasible)

    # 2. Find optimal value
    best_val: tuple[int, Fraction] | None = None
    for mt in all_matchings:
        v = matching_value(mt, dist_matrix)
        if best_val is None or is_better(v, best_val):
            best_val = v

    assert best_val is not None
    opt_count, opt_sum = best_val

    # 3. Collect all optima
    optima = [mt for mt in all_matchings if matching_value(mt, dist_matrix) == best_val]

    # 4. Canonical tie breaking
    prev_order = canonical_scenario_order(prev_scens)
    curr_order = canonical_scenario_order(curr_scens)
    curr_rank = {c_idx: r for r, c_idx in enumerate(curr_order)}

    def tie_break_vector(mt: list[tuple[int, int]]) -> list[int]:
        mt_dict = {i: j for i, j in mt}
        return [curr_rank[mt_dict[p_idx]] if p_idx in mt_dict else n for p_idx in prev_order]

    chosen = min(optima, key=tie_break_vector)
    chosen_set = set(chosen)

    # 5. Compute substitutability for each chosen link
    links: list[dict[str, Any]] = []
    for p_idx, c_idx in sorted(chosen):
        # Best matching without this edge (p_idx, c_idx)
        alt_matchings = [mt for mt in all_matchings if (p_idx, c_idx) not in mt]
        best_alt: tuple[int, Fraction] | None = None
        for mt in alt_matchings:
            v = matching_value(mt, dist_matrix)
            if best_alt is None or is_better(v, best_alt):
                best_alt = v

        if best_alt is None or best_alt[0] == 0 and opt_count > 0:
            sub = "count_drop"
        elif best_alt[0] < opt_count:
            sub = "count_drop"
        else:
            diff = best_alt[1] - opt_sum
            if diff == 0:
                sub = "tied"
            else:
                sub = "same_count"

        is_ambiguous = len(optima) > 1 and any((p_idx, c_idx) not in mt for mt in optima)
        p_cid = prev_scens[p_idx]["cluster_id"]
        c_cid = curr_scens[c_idx]["cluster_id"]

        links.append({
            "prev_cluster_id": p_cid,
            "curr_cluster_id": c_cid,
            "distance_k": dist_matrix[p_idx][c_idx],
            "substitutability": sub,
            "ambiguous": is_ambiguous,
        })
    links.sort(key=lambda l: l["prev_cluster_id"])

    # 6. Unmatched diagnosis
    matched_prev = {p for p, _ in chosen}
    matched_curr = {c for _, c in chosen}

    unmatched_prev = []
    for i in range(m):
        if i not in matched_prev:
            has_cand = any(feasible(i, j) for j in range(n))
            reason = "not_selected_in_global_optimum" if has_cand else "no_candidate_within_threshold"
            matched_in_opt = any(any(p == i for p, _ in mt) for mt in optima)
            unmatched_prev.append({
                "cluster_id": prev_scens[i]["cluster_id"],
                "reason": reason,
                "matched_in_some_optimum": matched_in_opt,
            })
    unmatched_prev.sort(key=lambda u: u["cluster_id"])

    unmatched_curr = []
    for j in range(n):
        if j not in matched_curr:
            has_cand = any(feasible(i, j) for i in range(m))
            reason = "not_selected_in_global_optimum" if has_cand else "no_candidate_within_threshold"
            matched_in_opt = any(any(c == j for _, c in mt) for mt in optima)
            unmatched_curr.append({
                "cluster_id": curr_scens[j]["cluster_id"],
                "reason": reason,
                "matched_in_some_optimum": matched_in_opt,
            })
    unmatched_curr.sort(key=lambda u: u["cluster_id"])

    return {
        "schema_version": "m2.match-result.v1",
        "objective": {
            "matched_count": opt_count,
            "weighted_distance_sum": float(opt_sum),
        },
        "links": links,
        "unmatched_prev": unmatched_prev,
        "unmatched_curr": unmatched_curr,
        "distance_matrix": dist_matrix,
        "optima_count": len(optima),
    }


def generate_random_oracle_cases(count: int = 4000, seed: int = 20261029) -> list[dict[str, Any]]:
    """Generate deterministic random match cases (m, n in 1..5)."""
    rnd = random.Random(seed)
    cases = []
    for c_idx in range(count):
        m = rnd.randint(1, 5)
        n = rnd.randint(1, 5)
        n_features = rnd.choice([1, 2, 3, 9])
        weights = [round(rnd.uniform(0.1, 2.0), 4) for _ in range(n_features)]
        w_sum = sum(weights)
        weights = [w / w_sum for w in weights]

        prev_scens = []
        for i in range(m):
            feats = [round(rnd.uniform(250.0, 310.0), 3) for _ in range(n_features)]
            prev_scens.append({"cluster_id": i, "mean_features": feats})

        curr_scens = []
        for j in range(n):
            feats = [round(rnd.uniform(250.0, 310.0), 3) for _ in range(n_features)]
            curr_scens.append({"cluster_id": j, "mean_features": feats})

        tau = round(rnd.uniform(0.5, 15.0), 2)
        cases.append({
            "case_id": f"random_{c_idx:04d}",
            "weights": weights,
            "threshold": tau,
            "prev_scenarios": prev_scens,
            "curr_scenarios": curr_scens,
        })
    return cases


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--gen":
        out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("validation/m2/fixtures/random_oracle_cases.json")
        cases = generate_random_oracle_cases(4000)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({"cases_count": len(cases), "cases": cases}, indent=2), encoding="utf-8")
        print(f"Generated {len(cases)} oracle cases to {out_path}")
    else:
        print("Usage: python3 validation/m2/oracle.py --gen [out_file]")
