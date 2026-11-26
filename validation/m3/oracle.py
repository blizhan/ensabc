#!/usr/bin/env python3
"""Independent pure-Python Fraction & math.fsum oracle for M3 scenario evolution.

Implements (using Python 3.13 stdlib ONLY — zero code/algorithm sharing with
ensabc-core's bitmask DP):
1. Exact binary64 spatial-weighted RMSE with parenthesization
   `w * ((a - b) * (a - b))` and flat-`math.fsum` ensemble dispersion.
2. Exhaustive enumeration of all valid 1-to-1 partial matchings over `d <= tau`
   evaluated with exact `fractions.Fraction` distance sums, canonical scenario
   tie-breaking (`-0.0 -> 0.0` + `cluster_id`), per-link forbidden-edge re-solve,
   and Theorem 3 forced-inclusion secondary promotion gaps.
3. `nearest_feasible_v1` secondary split/merge edge selection, Berge Theorems 1-4
   invariant verification, and 5-class star connected-component decomposition.
4. `star_min_mass_proportional_v1` algorithmic attributed flow, `edge_strength`,
   and explicit node unattributed inflow/outflow residuals.
5. Three orthogonal metric families (Morphology, Member Fraction, Uncertainty)
   and frozen `m3.uncertainty.v1` high-uncertainty classification.
6. Multi-batch (`4..=8`) chain oracle (`solve_chain_oracle`) with M2-identical
   `branch_id` inheritance, `ok` vs `gap` transitions (`gap_interrupted` /
   `gap_restarted` disjoint from `disappearance` / `emergence`), branch summaries,
   chain summary, and `E1`-`E8` global invariant checks.
7. Outward-rounded `Fraction` 4-Condition Evolution Stability Certificate
   (`evaluate_evolution_stability_certificate`) with conservative `math.isqrt`
   rational square-root bounds.
"""

from __future__ import annotations

import itertools
import json
import math
import os
import random
import re
import sys
import tempfile
from fractions import Fraction
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

EVOLUTION_ALGORITHM_VERSION = "m3.evolve.v1"
MATCH_ALGORITHM_VERSION = "m2.match.v1"
SECONDARY_RULE_V1 = "nearest_feasible_v1"
FLOW_RULE_V1 = "star_min_mass_proportional_v1"
UNCERTAINTY_RULE_V1 = "m3.uncertainty.v1"
CERTIFICATE_RULE_V1 = "m3.certificate.v1"
NON_PHYSICAL_DISCLAIMER = "algorithmic_attribution_only_not_physical_member_migration"
MAX_SCENARIOS_PER_SIDE = 12

WHITELISTED_SINGLE_BATCH_FAILURE_CODES: frozenset[str] = frozenset(
    {
        "INSUFFICIENT_RANK",
        "AMBIGUOUS_SUBSPACE",
        "EMPTY_CLUSTER",
        "NOT_CONVERGED",
        "INSUFFICIENT_DISTINCT_VECTORS",
        "INSUFFICIENT_PROJECTED_VECTORS",
        "SVD_NOT_CONVERGED",
        "ZERO_DISPERSION",
    }
)

DEFAULT_EVOLUTION_CONFIG: dict[str, Any] = {
    "secondary_rule": SECONDARY_RULE_V1,
    "flow_rule": FLOW_RULE_V1,
    "fraction_sum_tolerance": 1e-9,
    "high_uncertainty_threshold_ratio": 0.10,
    "high_uncertainty_optimality_ratio": 0.10,
    "high_uncertainty_secondary_ratio": 0.10,
}

__all__ = [
    "CERTIFICATE_RULE_V1",
    "DEFAULT_EVOLUTION_CONFIG",
    "EVOLUTION_ALGORITHM_VERSION",
    "FLOW_RULE_V1",
    "MATCH_ALGORITHM_VERSION",
    "MAX_SCENARIOS_PER_SIDE",
    "NON_PHYSICAL_DISCLAIMER",
    "OracleValidationError",
    "RationalInterval",
    "SECONDARY_RULE_V1",
    "UNCERTAINTY_RULE_V1",
    "canonical_scenario_order",
    "compute_ensemble_dispersion_exact",
    "compute_weighted_rmse_exact",
    "compute_weighted_rmse_rational_bounds",
    "enumerate_partial_matchings",
    "evaluate_evolution_stability_certificate",
    "evolve_pair_oracle",
    "fsum_exact",
    "generate_random_evolution_cases",
    "normalize_floats",
    "normalize_neg_zero",
    "overlap_cluster_mapping",
    "rational_sqrt_bounds",
    "solve_backbone_oracle",
    "solve_chain_oracle",
    "solve_evolution_oracle",
    "solve_oracle",
    "validate_evolution_config",
    "validate_evolution_input",
    "verify_berge_and_star_invariants",
    "verify_chain_invariants",
]


class OracleValidationError(ValueError):
    """Structured validation error mirroring ensabc-core CoreError codes and indices."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        prev_index: int | None = None,
        curr_index: int | None = None,
        feature_index: int | None = None,
    ) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.prev_index = prev_index
        self.curr_index = curr_index
        self.feature_index = feature_index


def is_finite_float_like(val: Any) -> bool:
    """Return True if val is an int/float (not bool) and finite in binary64."""
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        return False
    try:
        fval = float(val)
    except (OverflowError, TypeError, ValueError):
        return False
    return math.isfinite(fval)


def normalize_neg_zero(val: float) -> float:
    """Normalize -0.0 to +0.0 while preserving all other IEEE-754 floats."""
    fval = float(val)
    return 0.0 if fval == 0.0 else fval


_norm_f64 = normalize_neg_zero


def _checked_ratio(num: float, den: float, label: str) -> float:
    if den == 0.0:
        raise OracleValidationError(
            "NUMERICAL_FAILURE",
            f"{label} overflowed to non-finite value ({num} / {den})",
        )
    q = _norm_f64(num / den)
    if not math.isfinite(q):
        raise OracleValidationError(
            "NUMERICAL_FAILURE",
            f"{label} overflowed to non-finite value ({num} / {den})",
        )
    return q


def _checked_opt_ratio(num: float, den: float | None, label: str) -> float | None:
    if den is None:
        return None
    return _checked_ratio(num, den, label)


def normalize_floats(obj: Any) -> Any:
    """Recursively normalize all floats (-0.0 -> +0.0) in nested containers."""
    if isinstance(obj, float):
        return 0.0 if obj == 0.0 else obj
    if isinstance(obj, list):
        return [normalize_floats(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(normalize_floats(v) for v in obj)
    if isinstance(obj, dict):
        return {k: normalize_floats(v) for k, v in obj.items()}
    return obj


def fsum_exact(values: Iterable[float]) -> float:
    """Single-rounding exact binary64 sum via math.fsum with -0.0 normalized to +0.0."""
    res = math.fsum(values)
    if not math.isfinite(res):
        raise OracleValidationError("NUMERICAL_FAILURE", f"Non-finite fsum result: {res}")
    return 0.0 if res == 0.0 else res


def compute_weighted_rmse_exact(
    a: Sequence[float],
    b: Sequence[float],
    weights: Sequence[float],
) -> float:
    """Compute exact binary64 weighted RMSE matching `w * ((a - b) * (a - b))`."""
    w_sum = math.fsum(weights)
    diff_sq_sum = math.fsum(w * ((x - y) * (x - y)) for w, x, y in zip(weights, a, b))
    mean_sq = diff_sq_sum / w_sum
    if not math.isfinite(mean_sq):
        raise OracleValidationError("NUMERICAL_FAILURE", f"Weighted RMSE variance overflow: {mean_sq}")
    if mean_sq <= 0.0:
        return 0.0
    dist = math.sqrt(mean_sq)
    if not math.isfinite(dist):
        raise OracleValidationError("NUMERICAL_FAILURE", f"Weighted RMSE root non-finite: {dist}")
    return 0.0 if dist == 0.0 else dist


def compute_ensemble_dispersion_exact(
    members: Sequence[Sequence[float]],
    weights: Sequence[float],
) -> float:
    """Compute single-batch population dispersion using one flat math.fsum."""
    n_members = len(members)
    n_points = len(weights)
    if n_members < 2 or n_points < 1:
        raise OracleValidationError("EMPTY_MEMBERS", "Ensemble dispersion requires >= 2 members and >= 1 point")
    w_sum = math.fsum(weights)
    if w_sum <= 0.0 or not math.isfinite(w_sum):
        raise OracleValidationError("INVALID_WEIGHTS", "Spatial weights sum must be positive and finite")
    mean_field = [math.fsum(m[g] for m in members) / n_members for g in range(n_points)]
    q_sum = math.fsum(
        weights[g] * ((members[n][g] - mean_field[g]) * (members[n][g] - mean_field[g]))
        for n in range(n_members)
        for g in range(n_points)
    )
    disp = math.sqrt((q_sum / n_members) / w_sum)
    if not math.isfinite(disp):
        raise OracleValidationError("NUMERICAL_FAILURE", f"Non-finite dispersion: {disp}")
    if disp <= 0.0:
        raise OracleValidationError("ZERO_DISPERSION", "Ensemble dispersion is 0.0")
    return disp


def rational_sqrt_bounds(val: Fraction, bits: int = 128) -> tuple[Fraction, Fraction]:
    """Return conservative outward rational bounds [lo, hi] with lo <= sqrt(val) <= hi."""
    if val < 0:
        raise ValueError(f"Cannot compute square root of negative Fraction: {val}")
    if val == 0:
        return Fraction(0), Fraction(0)
    if bits < 1:
        raise ValueError(f"Precision bits must be >= 1, got {bits}")
    scale = 1 << (2 * bits)
    scaled_num = (val.numerator * scale) // val.denominator
    lo_int = math.isqrt(scaled_num)
    denom = 1 << bits
    lo = Fraction(lo_int, denom)
    is_exact = lo_int * lo_int * val.denominator == val.numerator * scale
    hi_int = lo_int if is_exact else lo_int + 1
    hi = Fraction(hi_int, denom)
    return lo, hi


def compute_weighted_rmse_rational_bounds(
    a: Sequence[float],
    b: Sequence[float],
    weights: Sequence[float],
    bits: int = 128,
) -> tuple[Fraction, Fraction]:
    """Compute conservative outward rational bounds [lo, hi] for true real weighted RMSE."""
    w_sum = sum((Fraction.from_float(float(w)) for w in weights), Fraction(0))
    if w_sum <= 0:
        raise ValueError("Weights sum must be strictly positive")
    num_sum = Fraction(0)
    for w, x, y in zip(weights, a, b):
        diff = Fraction.from_float(float(x)) - Fraction.from_float(float(y))
        num_sum += Fraction.from_float(float(w)) * (diff * diff)
    return rational_sqrt_bounds(num_sum / w_sum, bits=bits)


class RationalInterval:
    """Closed rational interval [lo, hi] with exact outward interval arithmetic."""

    __slots__ = ("lo", "hi")

    def __init__(self, lo: Fraction, hi: Fraction) -> None:
        if lo > hi:
            raise ValueError(f"Invalid RationalInterval: lo={lo} > hi={hi}")
        self.lo = lo
        self.hi = hi

    @classmethod
    def exact_float(cls, val: float) -> RationalInterval:
        q = Fraction.from_float(float(val))
        return cls(q, q)

    @classmethod
    def from_float_ulp(cls, val: float, ulp_multiplier: int = 8) -> RationalInterval:
        fval = float(val)
        q = Fraction.from_float(fval)
        ulp = math.ulp(abs(fval)) if fval != 0.0 else math.ulp(1.0)
        pad = Fraction.from_float(ulp) * ulp_multiplier
        return cls(q - pad, q + pad)

    def __add__(self, other: RationalInterval) -> RationalInterval:
        return RationalInterval(self.lo + other.lo, self.hi + other.hi)

    def __sub__(self, other: RationalInterval) -> RationalInterval:
        return RationalInterval(self.lo - other.hi, self.hi - other.lo)

    def __mul__(self, other: RationalInterval) -> RationalInterval:
        products = (
            self.lo * other.lo,
            self.lo * other.hi,
            self.hi * other.lo,
            self.hi * other.hi,
        )
        return RationalInterval(min(products), max(products))

    def abs_interval(self) -> RationalInterval:
        if self.lo >= 0:
            return RationalInterval(self.lo, self.hi)
        if self.hi <= 0:
            return RationalInterval(-self.hi, -self.lo)
        return RationalInterval(Fraction(0), max(-self.lo, self.hi))

    def sqrt_interval(self, bits: int = 128) -> RationalInterval:
        if self.lo < 0:
            raise ValueError("Cannot take sqrt of interval containing negative values")
        lo_sqrt, _ = rational_sqrt_bounds(self.lo, bits=bits)
        _, hi_sqrt = rational_sqrt_bounds(self.hi, bits=bits)
        return RationalInterval(lo_sqrt, hi_sqrt)


def canonical_scenario_order(scenarios: Sequence[dict[str, Any]]) -> list[int]:
    """Return scenario list indices sorted by (-0.0-normalized mean_features, cluster_id)."""

    def key_fn(item: tuple[int, dict[str, Any]]) -> tuple[tuple[float, ...], int]:
        idx, sc = item
        feats = tuple(0.0 if float(x) == 0.0 else float(x) for x in sc.get("mean_features", ()))
        cid = int(sc.get("cluster_id", idx))
        return (feats, cid)

    return [idx for idx, _ in sorted(enumerate(scenarios), key=key_fn)]


def enumerate_partial_matchings(
    m: int,
    n: int,
    feasible_fn: Callable[[int, int], bool],
) -> list[list[tuple[int, int]]]:
    """Exhaustively enumerate all valid 1-to-1 partial matchings over feasible pairs."""
    adj = [[j for j in range(n) if feasible_fn(i, j)] for i in range(m)]
    results: list[list[tuple[int, int]]] = []

    def backtrack(i: int, used_curr: set[int], current: list[tuple[int, int]]) -> None:
        if i == m:
            results.append(list(current))
            return
        backtrack(i + 1, used_curr, current)
        for j in adj[i]:
            if j not in used_curr:
                used_curr.add(j)
                current.append((i, j))
                backtrack(i + 1, used_curr, current)
                current.pop()
                used_curr.remove(j)

    backtrack(0, set(), [])
    return results


def _matching_value(
    matching: Sequence[tuple[int, int]],
    dist_frac_matrix: Sequence[Sequence[Fraction]],
) -> tuple[int, Fraction]:
    """Compute (cardinality, exact_Fraction_sum) for a 1-to-1 partial matching."""
    count = len(matching)
    dist_sum = sum((dist_frac_matrix[i][j] for i, j in matching), Fraction(0))
    return (count, dist_sum)


def _is_better(val_a: tuple[int, Fraction], val_b: tuple[int, Fraction]) -> bool:
    """Lexicographic comparison: higher count first, then strictly smaller Fraction sum."""
    if val_a[0] != val_b[0]:
        return val_a[0] > val_b[0]
    return val_a[1] < val_b[1]


def validate_evolution_config(config: dict[str, Any] | None) -> dict[str, Any]:
    """Validate and fill defaults for EvolutionConfig."""
    merged = dict(DEFAULT_EVOLUTION_CONFIG)
    if config is not None:
        if not isinstance(config, dict):
            raise OracleValidationError("INVALID_EVOLUTION_CONFIG", "config must be a mapping")
        merged.update(config)

    if merged["secondary_rule"] != SECONDARY_RULE_V1:
        raise OracleValidationError(
            "INVALID_EVOLUTION_CONFIG",
            f"Unsupported secondary_rule: {merged['secondary_rule']!r}",
        )
    if merged["flow_rule"] != FLOW_RULE_V1:
        raise OracleValidationError(
            "INVALID_EVOLUTION_CONFIG",
            f"Unsupported flow_rule: {merged['flow_rule']!r}",
        )

    tol = merged["fraction_sum_tolerance"]
    if not is_finite_float_like(tol) or float(tol) < 0.0 or float(tol) > 1e-3:
        raise OracleValidationError(
            "INVALID_EVOLUTION_CONFIG",
            f"Invalid fraction_sum_tolerance: {tol!r}",
        )
    merged["fraction_sum_tolerance"] = float(tol)

    for ratio_key in (
        "high_uncertainty_threshold_ratio",
        "high_uncertainty_optimality_ratio",
        "high_uncertainty_secondary_ratio",
    ):
        rval = merged[ratio_key]
        if not is_finite_float_like(rval) or float(rval) < 0.0:
            raise OracleValidationError(
                "INVALID_EVOLUTION_CONFIG",
                f"Invalid {ratio_key}: {rval!r}",
            )
        merged[ratio_key] = float(rval)

    return merged


def _validate_scenarios_and_weights(
    prev_scens: Sequence[dict[str, Any]],
    curr_scens: Sequence[dict[str, Any]],
    weights: Sequence[float],
    *,
    check_fractions: bool,
    fraction_sum_tolerance: float = 1e-9,
) -> None:
    """Validate M2 scenario/weight invariants plus M3 member_count & member_fraction rules."""
    if not prev_scens or not curr_scens:
        raise OracleValidationError(
            "EMPTY_SCENARIOS",
            "prev_scenarios and curr_scenarios must both be non-empty",
        )
    if len(prev_scens) > MAX_SCENARIOS_PER_SIDE or len(curr_scens) > MAX_SCENARIOS_PER_SIDE:
        raise OracleValidationError(
            "TOO_MANY_SCENARIOS",
            f"Scenario count ({len(prev_scens)}, {len(curr_scens)}) exceeds {MAX_SCENARIOS_PER_SIDE}",
        )

    seen_prev: set[int] = set()
    for i, sc in enumerate(prev_scens):
        cid = int(sc["cluster_id"])
        if cid in seen_prev:
            raise OracleValidationError(
                "DUPLICATE_SCENARIO_ID",
                f"Duplicate cluster_id {cid} in prev_scenarios",
                prev_index=i,
            )
        seen_prev.add(cid)

    seen_curr: set[int] = set()
    for j, sc in enumerate(curr_scens):
        cid = int(sc["cluster_id"])
        if cid in seen_curr:
            raise OracleValidationError(
                "DUPLICATE_SCENARIO_ID",
                f"Duplicate cluster_id {cid} in curr_scenarios",
                curr_index=j,
            )
        seen_curr.add(cid)

    if not weights:
        raise OracleValidationError("EMPTY_FEATURES", "Spatial weights must not be empty")
    for g, w in enumerate(weights):
        if not is_finite_float_like(w) or float(w) < 0.0:
            raise OracleValidationError(
                "INVALID_WEIGHTS",
                f"Invalid weight at index {g}: {w!r}",
                feature_index=g,
            )
    w_sum = fsum_exact(float(w) for w in weights)
    if w_sum <= 0.0:
        raise OracleValidationError("INVALID_WEIGHTS", "Sum of spatial weights must be > 0.0")

    n_points = len(weights)
    first_prev_len = len(prev_scens[0].get("mean_features", ()))
    for i, sc in enumerate(prev_scens):
        feats = sc.get("mean_features", ())
        if len(feats) == 0:
            raise OracleValidationError("EMPTY_FEATURES", "Empty prev mean_features", prev_index=i)
        if len(feats) != first_prev_len:
            raise OracleValidationError("RAGGED_FEATURES", "Ragged prev mean_features", prev_index=i)
        for g, val in enumerate(feats):
            if not is_finite_float_like(val):
                raise OracleValidationError(
                    "NON_FINITE_INPUT",
                    f"Non-finite prev feature at ({i}, {g})",
                    prev_index=i,
                    feature_index=g,
                )

    first_curr_len = len(curr_scens[0].get("mean_features", ()))
    for j, sc in enumerate(curr_scens):
        feats = sc.get("mean_features", ())
        if len(feats) == 0:
            raise OracleValidationError("EMPTY_FEATURES", "Empty curr mean_features", curr_index=j)
        if len(feats) != first_curr_len:
            raise OracleValidationError("RAGGED_FEATURES", "Ragged curr mean_features", curr_index=j)
        for g, val in enumerate(feats):
            if not is_finite_float_like(val):
                raise OracleValidationError(
                    "NON_FINITE_INPUT",
                    f"Non-finite curr feature at ({j}, {g})",
                    curr_index=j,
                    feature_index=g,
                )

    if first_prev_len != n_points or first_curr_len != n_points:
        raise OracleValidationError(
            "DIMENSION_MISMATCH",
            f"Feature lengths ({first_prev_len}, {first_curr_len}) != weights length {n_points}",
        )

    if check_fractions:
        for i, sc in enumerate(prev_scens):
            mc = sc.get("member_count")
            if isinstance(mc, bool) or not isinstance(mc, int) or mc < 1:
                raise OracleValidationError(
                    "ZERO_MEMBER_SCENARIO",
                    f"prev_scenarios[{i}] member_count must be >= 1, got {mc!r}",
                    prev_index=i,
                )
            mf = sc.get("member_fraction")
            if not is_finite_float_like(mf) or float(mf) <= 0.0 or float(mf) > 1.0:
                raise OracleValidationError(
                    "INVALID_MEMBER_FRACTION",
                    f"prev_scenarios[{i}] invalid member_fraction: {mf!r}",
                    prev_index=i,
                )

        prev_frac_sum = fsum_exact(float(sc["member_fraction"]) for sc in prev_scens)
        if abs(prev_frac_sum - 1.0) > fraction_sum_tolerance:
            raise OracleValidationError(
                "INVALID_FRACTION_SUM",
                f"prev_scenarios fraction sum {prev_frac_sum!r} deviates from 1.0 by > {fraction_sum_tolerance}",
                prev_index=0,
            )

        for j, sc in enumerate(curr_scens):
            mc = sc.get("member_count")
            if isinstance(mc, bool) or not isinstance(mc, int) or mc < 1:
                raise OracleValidationError(
                    "ZERO_MEMBER_SCENARIO",
                    f"curr_scenarios[{j}] member_count must be >= 1, got {mc!r}",
                    curr_index=j,
                )
            mf = sc.get("member_fraction")
            if not is_finite_float_like(mf) or float(mf) <= 0.0 or float(mf) > 1.0:
                raise OracleValidationError(
                    "INVALID_MEMBER_FRACTION",
                    f"curr_scenarios[{j}] invalid member_fraction: {mf!r}",
                    curr_index=j,
                )

        curr_frac_sum = fsum_exact(float(sc["member_fraction"]) for sc in curr_scens)
        if abs(curr_frac_sum - 1.0) > fraction_sum_tolerance:
            raise OracleValidationError(
                "INVALID_FRACTION_SUM",
                f"curr_scenarios fraction sum {curr_frac_sum!r} deviates from 1.0 by > {fraction_sum_tolerance}",
                curr_index=0,
            )


def _resolve_threshold_and_scale(task: dict[str, Any]) -> tuple[
    bool,
    float,
    float | None,
    float | None,
    float | None,
    float | None,
    float | None,
]:
    """Resolve absolute vs scaled mode parameters: (is_scaled, tau, S, c, s_prev, s_curr, R)."""
    mode = task.get("mode")
    has_scaled_fields = (
        task.get("threshold_coefficient") is not None
        or task.get("prev_dispersion") is not None
        or task.get("curr_dispersion") is not None
        or mode == "scaled"
    )
    if has_scaled_fields and mode != "absolute":
        c_raw = task.get("threshold_coefficient")
        sp_raw = task.get("prev_dispersion")
        sc_raw = task.get("curr_dispersion")
        if not is_finite_float_like(c_raw) or float(c_raw) < 0.0:
            raise OracleValidationError(
                "INVALID_THRESHOLD",
                f"Invalid threshold_coefficient: {c_raw!r}",
            )
        if not is_finite_float_like(sp_raw):
            raise OracleValidationError(
                "INVALID_THRESHOLD",
                f"Non-finite prev_dispersion: {sp_raw!r}",
            )
        if float(sp_raw) <= 0.0:
            raise OracleValidationError(
                "ZERO_DISPERSION",
                f"prev_dispersion must be > 0.0, got {sp_raw!r}",
            )
        if not is_finite_float_like(sc_raw):
            raise OracleValidationError(
                "INVALID_THRESHOLD",
                f"Non-finite curr_dispersion: {sc_raw!r}",
            )
        if float(sc_raw) <= 0.0:
            raise OracleValidationError(
                "ZERO_DISPERSION",
                f"curr_dispersion must be > 0.0, got {sc_raw!r}",
            )
        c = normalize_neg_zero(float(c_raw))
        sp = normalize_neg_zero(float(sp_raw))
        sc = normalize_neg_zero(float(sc_raw))
        pair_scale = (sp + sc) / 2.0
        if not math.isfinite(pair_scale) or pair_scale <= 0.0:
            raise OracleValidationError("INVALID_THRESHOLD", f"Invalid pair_scale: {pair_scale!r}")
        tau = normalize_neg_zero(c * pair_scale)
        if not math.isfinite(tau) or tau < 0.0:
            raise OracleValidationError("INVALID_THRESHOLD", f"Invalid scaled threshold: {tau!r}")
        if task.get("threshold") is not None and float(task["threshold"]) != tau:
            raise OracleValidationError(
                "INVALID_THRESHOLD",
                f"Explicit threshold {task['threshold']!r} conflicts with scaled tau {tau!r}",
            )
        return True, tau, pair_scale, c, sp, sc, pair_scale

    tau_raw = task.get("threshold")
    if not is_finite_float_like(tau_raw) or float(tau_raw) < 0.0:
        raise OracleValidationError("INVALID_THRESHOLD", f"Invalid threshold: {tau_raw!r}")
    tau = normalize_neg_zero(float(tau_raw))
    ref_scale = tau if tau > 0.0 else None
    return False, tau, None, None, None, None, ref_scale


def validate_evolution_input(task: dict[str, Any]) -> dict[str, Any]:
    """Validate all M3 evolution input rules, returning normalized EvolutionConfig."""
    cfg = validate_evolution_config(task.get("config"))
    _validate_scenarios_and_weights(
        task.get("prev_scenarios", ()),
        task.get("curr_scenarios", ()),
        task.get("weights", ()),
        check_fractions=True,
        fraction_sum_tolerance=cfg["fraction_sum_tolerance"],
    )
    _resolve_threshold_and_scale(task)
    return cfg


def _solve_exhaustive_matching_core(
    prev_scens: Sequence[dict[str, Any]],
    curr_scens: Sequence[dict[str, Any]],
    weights: Sequence[float],
    tau: float,
    pair_scale: float | None = None,
) -> dict[str, Any]:
    """Internal exhaustive Fraction solver returning chosen primary links, optima, and gaps."""
    m = len(prev_scens)
    n = len(curr_scens)
    w_vec = [float(w) for w in weights]

    dist_matrix: list[list[float]] = []
    dist_frac_matrix: list[list[Fraction]] = []
    eligible_matrix: list[list[bool]] = []
    for p in prev_scens:
        p_feats = [float(x) for x in p["mean_features"]]
        d_row: list[float] = []
        f_row: list[Fraction] = []
        e_row: list[bool] = []
        for c in curr_scens:
            c_feats = [float(x) for x in c["mean_features"]]
            d = compute_weighted_rmse_exact(p_feats, c_feats, w_vec)
            d_row.append(d)
            f_row.append(Fraction.from_float(d))
            e_row.append(d <= tau)
        dist_matrix.append(d_row)
        dist_frac_matrix.append(f_row)
        eligible_matrix.append(e_row)

    all_matchings = enumerate_partial_matchings(m, n, lambda i, j: eligible_matrix[i][j])
    matching_vals = [_matching_value(mt, dist_frac_matrix) for mt in all_matchings]

    best_val: tuple[int, Fraction] | None = None
    for v in matching_vals:
        if best_val is None or _is_better(v, best_val):
            best_val = v
    assert best_val is not None
    opt_count, opt_sum_frac = best_val

    optima = [mt for mt, v in zip(all_matchings, matching_vals) if v == best_val]

    prev_order = canonical_scenario_order(prev_scens)
    curr_order = canonical_scenario_order(curr_scens)
    prev_rank = {p_idx: r for r, p_idx in enumerate(prev_order)}
    curr_rank = {c_idx: r for r, c_idx in enumerate(curr_order)}

    def tie_break_vector(mt: list[tuple[int, int]]) -> list[int]:
        mt_dict = dict(mt)
        return [curr_rank[mt_dict[p_idx]] if p_idx in mt_dict else n for p_idx in prev_order]

    chosen = min(optima, key=tie_break_vector)

    # Forbidden-edge re-solve for each chosen primary edge
    link_details: list[dict[str, Any]] = []
    for p_idx, c_idx in sorted(chosen, key=lambda pair: int(prev_scens[pair[0]]["cluster_id"])):
        best_alt: tuple[int, Fraction] | None = None
        for mt, v in zip(all_matchings, matching_vals):
            if (p_idx, c_idx) not in mt:
                if best_alt is None or _is_better(v, best_alt):
                    best_alt = v
        assert best_alt is not None
        alt_count, alt_sum_frac = best_alt
        alt_obj = normalize_neg_zero(float(alt_sum_frac))

        if alt_count < opt_count:
            sub_status = "count_drop"
            gap: float | None = None
            is_ambiguous = False
        else:
            diff_frac = alt_sum_frac - opt_sum_frac
            gap_val = normalize_neg_zero(float(diff_frac))
            if diff_frac == 0 or gap_val == 0.0:
                sub_status = "tied"
                gap = 0.0
                is_ambiguous = True
            else:
                sub_status = "same_count"
                gap = gap_val
                is_ambiguous = False

        d_val = dist_matrix[p_idx][c_idx]
        link_details.append(
            {
                "prev_idx": p_idx,
                "curr_idx": c_idx,
                "prev_cluster_id": int(prev_scens[p_idx]["cluster_id"]),
                "curr_cluster_id": int(curr_scens[c_idx]["cluster_id"]),
                "distance_k": d_val,
                "distance_ratio": _checked_opt_ratio(
                    d_val, pair_scale, "backbone.links.distance_ratio"
                ),
                "substitutability": sub_status,
                "substitutability_gap": gap,
                "alt_count": alt_count,
                "alt_objective": alt_obj,
                "ambiguous": is_ambiguous,
            }
        )

    matched_prev = {p for p, _ in chosen}
    matched_curr = {c for _, c in chosen}

    # Unmatched diagnosis + Theorem 3 forced-inclusion promotion gap
    unmatched_prev_details: dict[int, dict[str, Any]] = {}
    for i in range(m):
        if i not in matched_prev:
            feas_cols = [j for j in range(n) if eligible_matrix[i][j]]
            has_cand = len(feas_cols) > 0
            reason = (
                "not_selected_in_global_optimum"
                if has_cand
                else "no_candidate_within_threshold"
            )
            matched_in_opt = any(any(p == i for p, _ in mt) for mt in optima)
            prom_gap: float | None = None
            prom_gap_frac: Fraction | None = None
            if has_cand:
                best_force: tuple[int, Fraction] | None = None
                for mt, v in zip(all_matchings, matching_vals):
                    if any(p == i for p, _ in mt):
                        if best_force is None or _is_better(v, best_force):
                            best_force = v
                assert best_force is not None, "Theorem 3: forced inclusion of U_prev^comp must be feasible"
                assert best_force[0] == opt_count, (
                    f"Theorem 3 violation: forced prev count {best_force[0]} != q*={opt_count}"
                )
                prom_gap_frac = best_force[1] - opt_sum_frac
                assert prom_gap_frac >= 0
                prom_gap = normalize_neg_zero(float(prom_gap_frac))

            unmatched_prev_details[i] = {
                "idx": i,
                "cluster_id": int(prev_scens[i]["cluster_id"]),
                "reason": reason,
                "matched_in_some_optimum": matched_in_opt,
                "feasible_partners": feas_cols,
                "promotion_gap": prom_gap,
                "promotion_gap_fraction": prom_gap_frac,
            }

    unmatched_curr_details: dict[int, dict[str, Any]] = {}
    for j in range(n):
        if j not in matched_curr:
            feas_rows = [i for i in range(m) if eligible_matrix[i][j]]
            has_cand = len(feas_rows) > 0
            reason = (
                "not_selected_in_global_optimum"
                if has_cand
                else "no_candidate_within_threshold"
            )
            matched_in_opt = any(any(c == j for _, c in mt) for mt in optima)
            prom_gap_c: float | None = None
            prom_gap_c_frac: Fraction | None = None
            if has_cand:
                best_force_c: tuple[int, Fraction] | None = None
                for mt, v in zip(all_matchings, matching_vals):
                    if any(c == j for _, c in mt):
                        if best_force_c is None or _is_better(v, best_force_c):
                            best_force_c = v
                assert best_force_c is not None, "Theorem 3: forced inclusion of U_curr^comp must be feasible"
                assert best_force_c[0] == opt_count, (
                    f"Theorem 3 violation: forced curr count {best_force_c[0]} != q*={opt_count}"
                )
                prom_gap_c_frac = best_force_c[1] - opt_sum_frac
                assert prom_gap_c_frac >= 0
                prom_gap_c = normalize_neg_zero(float(prom_gap_c_frac))

            unmatched_curr_details[j] = {
                "idx": j,
                "cluster_id": int(curr_scens[j]["cluster_id"]),
                "reason": reason,
                "matched_in_some_optimum": matched_in_opt,
                "feasible_partners": feas_rows,
                "promotion_gap": prom_gap_c,
                "promotion_gap_fraction": prom_gap_c_frac,
            }

    return {
        "dist_matrix": dist_matrix,
        "dist_frac_matrix": dist_frac_matrix,
        "eligible_matrix": eligible_matrix,
        "opt_count": opt_count,
        "opt_sum_frac": opt_sum_frac,
        "opt_sum_rounded": normalize_neg_zero(float(opt_sum_frac)),
        "optima_count": len(optima),
        "optima": optima,
        "prev_order": prev_order,
        "curr_order": curr_order,
        "prev_rank": prev_rank,
        "curr_rank": curr_rank,
        "chosen": chosen,
        "link_details": link_details,
        "unmatched_prev_details": unmatched_prev_details,
        "unmatched_curr_details": unmatched_curr_details,
    }


def solve_backbone_oracle(match_task: dict[str, Any]) -> dict[str, Any]:
    """Solve M2-compatible 1-to-1 global optimal matching using exhaustive Fraction search."""
    prev_scens = match_task["prev_scenarios"]
    curr_scens = match_task["curr_scenarios"]
    weights = match_task["weights"]
    _validate_scenarios_and_weights(prev_scens, curr_scens, weights, check_fractions=False)
    is_scaled, tau, pair_scale, c_coeff, s_prev, s_curr, _ = _resolve_threshold_and_scale(match_task)

    core = _solve_exhaustive_matching_core(prev_scens, curr_scens, weights, tau, pair_scale)

    links = [
        {
            "prev_cluster_id": ld["prev_cluster_id"],
            "curr_cluster_id": ld["curr_cluster_id"],
            "distance_k": ld["distance_k"],
            **(
                {"distance_ratio": ld["distance_ratio"]}
                if is_scaled and ld["distance_ratio"] is not None
                else {}
            ),
            "substitutability": ld["substitutability"],
            "substitutability_gap": ld["substitutability_gap"],
            "alt_count": ld["alt_count"],
            "alt_objective": ld["alt_objective"],
            "ambiguous": ld["ambiguous"],
        }
        for ld in core["link_details"]
    ]
    links.sort(key=lambda x: x["prev_cluster_id"])

    unmatched_prev = sorted(
        (
            {
                "cluster_id": u["cluster_id"],
                "reason": u["reason"],
                "matched_in_some_optimum": u["matched_in_some_optimum"],
                "promotion_gap": u["promotion_gap"],
            }
            for u in core["unmatched_prev_details"].values()
        ),
        key=lambda x: x["cluster_id"],
    )
    unmatched_curr = sorted(
        (
            {
                "cluster_id": u["cluster_id"],
                "reason": u["reason"],
                "matched_in_some_optimum": u["matched_in_some_optimum"],
                "promotion_gap": u["promotion_gap"],
            }
            for u in core["unmatched_curr_details"].values()
        ),
        key=lambda x: x["cluster_id"],
    )

    out: dict[str, Any] = {
        "schema_version": "m2.match-result.v1",
        "algorithm_version": MATCH_ALGORITHM_VERSION,
        "threshold": tau,
        "objective": {
            "matched_count": core["opt_count"],
            "weighted_distance_sum": core["opt_sum_rounded"],
        },
        "links": links,
        "unmatched_prev": unmatched_prev,
        "unmatched_curr": unmatched_curr,
        "distance_matrix": core["dist_matrix"],
        "eligible_matrix": core["eligible_matrix"],
        "optima_count": core["optima_count"],
    }
    if is_scaled:
        out["pair_scale"] = pair_scale
        out["threshold_coefficient"] = c_coeff
        out["prev_dispersion"] = s_prev
        out["curr_dispersion"] = s_curr
    return normalize_floats(out)


def _verify_backbone_assertion(
    assertion: dict[str, Any],
    prev_scens: Sequence[dict[str, Any]],
    curr_scens: Sequence[dict[str, Any]],
    tau: float,
    core: dict[str, Any],
) -> None:
    """Strictly verify explicit BackboneAssertion against recomputed distances & global optimum."""
    exp_prev_ids = [int(s["cluster_id"]) for s in prev_scens]
    exp_curr_ids = [int(s["cluster_id"]) for s in curr_scens]

    if list(assertion.get("prev_cluster_ids", ())) != exp_prev_ids:
        raise OracleValidationError(
            "BACKBONE_INCONSISTENCY",
            "BackboneAssertion.prev_cluster_ids does not match prev_scenarios order",
        )
    if list(assertion.get("curr_cluster_ids", ())) != exp_curr_ids:
        raise OracleValidationError(
            "BACKBONE_INCONSISTENCY",
            "BackboneAssertion.curr_cluster_ids does not match curr_scenarios order",
        )
    b_tau = assertion.get("threshold")
    if not is_finite_float_like(b_tau) or normalize_neg_zero(float(b_tau)) != tau:
        raise OracleValidationError(
            "BACKBONE_INCONSISTENCY",
            f"BackboneAssertion.threshold {b_tau!r} != expected {tau!r}",
        )

    dist_matrix = core["dist_matrix"]
    eligible_matrix = core["eligible_matrix"]
    m = len(prev_scens)
    n = len(curr_scens)

    b_dist = assertion.get("distance_matrix")
    if b_dist is not None:
        if len(b_dist) != m or any(len(row) != n for row in b_dist):
            raise OracleValidationError(
                "BACKBONE_INCONSISTENCY",
                "BackboneAssertion.distance_matrix shape mismatch",
            )
        for i in range(m):
            for j in range(n):
                if not is_finite_float_like(b_dist[i][j]) or normalize_neg_zero(float(b_dist[i][j])) != dist_matrix[i][j]:
                    raise OracleValidationError(
                        "BACKBONE_INCONSISTENCY",
                        f"BackboneAssertion.distance_matrix[{i}][{j}] mismatch",
                        prev_index=i,
                        curr_index=j,
                    )

    b_elig = assertion.get("eligible_matrix")
    if b_elig is not None:
        if len(b_elig) != m or any(len(row) != n for row in b_elig):
            raise OracleValidationError(
                "BACKBONE_INCONSISTENCY",
                "BackboneAssertion.eligible_matrix shape mismatch",
            )
        for i in range(m):
            for j in range(n):
                if bool(b_elig[i][j]) != eligible_matrix[i][j]:
                    raise OracleValidationError(
                        "BACKBONE_INCONSISTENCY",
                        f"BackboneAssertion.eligible_matrix[{i}][{j}] mismatch",
                        prev_index=i,
                        curr_index=j,
                    )

    prev_id_to_idx = {cid: i for i, cid in enumerate(exp_prev_ids)}
    curr_id_to_idx = {cid: j for j, cid in enumerate(exp_curr_ids)}

    raw_links = assertion.get("primary_links", ())
    used_p: set[int] = set()
    used_c: set[int] = set()
    idx_pairs: list[tuple[int, int]] = []
    for raw_pair in raw_links:
        if isinstance(raw_pair, dict):
            p_cid = int(raw_pair["prev_cluster_id"])
            c_cid = int(raw_pair["curr_cluster_id"])
        else:
            p_cid, c_cid = int(raw_pair[0]), int(raw_pair[1])

        if p_cid not in prev_id_to_idx:
            raise OracleValidationError(
                "INVALID_PRIMARY_LINK",
                f"Unknown prev_cluster_id {p_cid} in primary_links",
            )
        if c_cid not in curr_id_to_idx:
            raise OracleValidationError(
                "INVALID_PRIMARY_LINK",
                f"Unknown curr_cluster_id {c_cid} in primary_links",
            )
        p_idx = prev_id_to_idx[p_cid]
        c_idx = curr_id_to_idx[c_cid]
        if p_idx in used_p or c_idx in used_c:
            raise OracleValidationError(
                "INVALID_PRIMARY_LINK",
                f"Duplicate scenario endpoint in primary_links: ({p_cid}, {c_cid})",
                prev_index=p_idx,
                curr_index=c_idx,
            )
        if not eligible_matrix[p_idx][c_idx]:
            raise OracleValidationError(
                "INVALID_PRIMARY_LINK",
                f"Primary link ({p_cid}, {c_cid}) exceeds threshold {tau}",
                prev_index=p_idx,
                curr_index=c_idx,
            )
        used_p.add(p_idx)
        used_c.add(c_idx)
        idx_pairs.append((p_idx, c_idx))

    asserted_val = _matching_value(idx_pairs, core["dist_frac_matrix"])
    expected_val = (core["opt_count"], core["opt_sum_frac"])
    if asserted_val != expected_val:
        raise OracleValidationError(
            "BACKBONE_INCONSISTENCY",
            f"Asserted primary_links objective {asserted_val[0]}, {float(asserted_val[1])!r} "
            f"does not achieve global optimum ({expected_val[0]}, {core['opt_sum_rounded']!r})",
        )
    if sorted(idx_pairs) != sorted(core["chosen"]):
        raise OracleValidationError(
            "BACKBONE_INCONSISTENCY",
            "Asserted primary_links does not match canonical tie-breaking winner",
        )


def _compute_edge_strength(distance: float, tau: float) -> float:
    """Compute geometric proximity strength in [0, 1] with exact binary64 order."""
    if tau == 0.0:
        return 1.0
    r = _checked_ratio(distance, tau, "edge.distance_over_threshold")
    val = min(1.0, max(0.0, 1.0 - r))
    return 0.0 if val == 0.0 else val


def _evaluate_edge_uncertainty(
    *,
    ambiguous_tie: bool,
    tau: float,
    ref_scale: float | None,
    threshold_margin: float,
    optimality_margin: float | None,
    secondary_margin: float | None,
    eps_tau: float,
    eps_opt: float,
    eps_sec: float,
) -> tuple[bool, list[str]]:
    """Evaluate frozen `m3.uncertainty.v1` multiplicative flags and reasons for an edge."""
    reasons: list[str] = []
    if ambiguous_tie:
        reasons.append("ambiguous_tie")
    if tau == 0.0 or (ref_scale is not None and threshold_margin <= eps_tau * ref_scale):
        reasons.append("near_threshold")
    if optimality_margin is not None and (
        optimality_margin == 0.0
        or (ref_scale is not None and optimality_margin <= eps_opt * ref_scale)
    ):
        reasons.append("near_primary_switch")
    if secondary_margin is not None and (
        secondary_margin == 0.0
        or (ref_scale is not None and secondary_margin <= eps_sec * ref_scale)
    ):
        reasons.append("near_secondary_switch")
    return (len(reasons) > 0), reasons


def verify_berge_and_star_invariants(evolution_result: dict[str, Any]) -> dict[str, bool]:
    """Verify Berge Theorems 1-3 and 5-class star partition invariants on an EvolutionResult."""
    backbone = evolution_result["backbone"]
    tau = evolution_result["threshold"]
    dist_mat = backbone["distance_matrix"]
    prev_ids = evolution_result["prev_cluster_ids"]
    curr_ids = evolution_result["curr_cluster_ids"]
    p_id_to_idx = {cid: i for i, cid in enumerate(prev_ids)}
    c_id_to_idx = {cid: j for j, cid in enumerate(curr_ids)}

    primary_pairs = {
        (p_id_to_idx[e["prev_cluster_id"]], c_id_to_idx[e["curr_cluster_id"]])
        for e in evolution_result["edges"]
        if e["edge_role"] == "primary"
    }
    matched_p = {i for i, _ in primary_pairs}
    matched_c = {j for _, j in primary_pairs}

    # Theorem 1: No feasible edge between two unmatched scenarios; no parallel edges
    no_unmatched_cross_feasible = True
    for i in range(len(prev_ids)):
        if i not in matched_p:
            for j in range(len(curr_ids)):
                if j not in matched_c and dist_mat[i][j] <= tau:
                    no_unmatched_cross_feasible = False

    edge_endpoints = [(e["prev_cluster_id"], e["curr_cluster_id"]) for e in evolution_result["edges"]]
    no_parallel_edges = len(edge_endpoints) == len(set(edge_endpoints))
    all_edges_within_tau = all(e["distance"] <= tau for e in evolution_result["edges"])

    # Theorem 2: On every primary edge (i, j), split out of i and merge into j are mutually exclusive
    split_sources = {
        p_id_to_idx[e["prev_cluster_id"]]
        for e in evolution_result["edges"]
        if e["edge_role"] == "secondary_split"
    }
    merge_targets = {
        c_id_to_idx[e["curr_cluster_id"]]
        for e in evolution_result["edges"]
        if e["edge_role"] == "secondary_merge"
    }
    split_merge_disjoint = all(
        not (i in split_sources and j in merge_targets) for i, j in primary_pairs
    )
    node_flags_disjoint = all(
        not (n["participates_in_split"] and n["participates_in_merge"])
        for n in (*evolution_result["prev_nodes"], *evolution_result["curr_nodes"])
    )

    # Corollary: Star connected-component 5-class partition covers every prev and curr node once
    seen_prev_ev: list[int] = []
    seen_curr_ev: list[int] = []
    star_shapes_valid = True
    for ev in evolution_result["events"]:
        p_list = ev["prev_cluster_ids"]
        c_list = ev["curr_cluster_ids"]
        seen_prev_ev.extend(p_list)
        seen_curr_ev.extend(c_list)
        et = ev["event_type"]
        if et == "continuation":
            star_shapes_valid &= len(p_list) == 1 and len(c_list) == 1
        elif et == "split":
            star_shapes_valid &= len(p_list) == 1 and len(c_list) >= 2
        elif et == "merge":
            star_shapes_valid &= len(p_list) >= 2 and len(c_list) == 1
        elif et == "disappearance":
            star_shapes_valid &= len(p_list) == 1 and len(c_list) == 0
        elif et == "emergence":
            star_shapes_valid &= len(p_list) == 0 and len(c_list) == 1
        else:
            star_shapes_valid = False

    partition_complete = (sorted(seen_prev_ev) == sorted(prev_ids)) and (
        sorted(seen_curr_ev) == sorted(curr_ids)
    )

    return {
        "theorem1_no_unmatched_feasible_edge": no_unmatched_cross_feasible,
        "theorem1_no_parallel_edges": no_parallel_edges,
        "all_edges_within_threshold": all_edges_within_tau,
        "theorem2_split_merge_mutually_exclusive": split_merge_disjoint and node_flags_disjoint,
        "corollary_star_5class_partition": star_shapes_valid and partition_complete,
        "all_passed": (
            no_unmatched_cross_feasible
            and no_parallel_edges
            and all_edges_within_tau
            and split_merge_disjoint
            and node_flags_disjoint
            and star_shapes_valid
            and partition_complete
        ),
    }


def solve_evolution_oracle(task: dict[str, Any]) -> dict[str, Any]:
    """Solve single adjacent-pair multi-to-multi scenario evolution (`m3.evolve.v1`)."""
    cfg = validate_evolution_input(task)
    prev_scens = task["prev_scenarios"]
    curr_scens = task["curr_scenarios"]
    weights = task["weights"]
    is_scaled, tau, pair_scale, c_coeff, s_prev, s_curr, ref_scale = _resolve_threshold_and_scale(task)

    core = _solve_exhaustive_matching_core(prev_scens, curr_scens, weights, tau, pair_scale)

    if task.get("backbone") is not None:
        _verify_backbone_assertion(task["backbone"], prev_scens, curr_scens, tau, core)

    m = len(prev_scens)
    n = len(curr_scens)
    dist_matrix = core["dist_matrix"]
    eligible_matrix = core["eligible_matrix"]
    prev_rank = core["prev_rank"]
    curr_rank = core["curr_rank"]

    eps_tau = cfg["high_uncertainty_threshold_ratio"]
    eps_opt = cfg["high_uncertainty_optimality_ratio"]
    eps_sec = cfg["high_uncertainty_secondary_ratio"]

    # 1. Primary edges (from M2 global optimal matching)
    primary_partner_of_prev: dict[int, int] = {}
    primary_partner_of_curr: dict[int, int] = {}
    raw_edges: list[dict[str, Any]] = []

    for ld in core["link_details"]:
        p_idx = ld["prev_idx"]
        c_idx = ld["curr_idx"]
        primary_partner_of_prev[p_idx] = c_idx
        primary_partner_of_curr[c_idx] = p_idx

        status = ld["substitutability"]
        if status == "tied":
            opt_margin: float | None = 0.0
            ambig_tie = True
            rule_basis = "global_optimum_primary_canonical_tiebreak"
        elif status == "same_count":
            opt_margin = ld["substitutability_gap"]
            ambig_tie = False
            rule_basis = "global_optimum_primary"
        else:
            opt_margin = None
            ambig_tie = False
            rule_basis = "global_optimum_primary"

        raw_edges.append(
            {
                "prev_idx": p_idx,
                "curr_idx": c_idx,
                "edge_role": "primary",
                "rule_basis": rule_basis,
                "distance": ld["distance_k"],
                "primary_alternative_status": status,
                "optimality_margin": opt_margin,
                "secondary_margin": None,
                "ambiguous_tie": ambig_tie,
            }
        )

    # 2. Secondary split edges (`nearest_feasible_v1`: for each j in U_curr^comp)
    for j, u_info in core["unmatched_curr_details"].items():
        if u_info["reason"] != "not_selected_in_global_optimum":
            continue
        feas_rows: list[int] = u_info["feasible_partners"]
        d1 = min(dist_matrix[i][j] for i in feas_rows)
        tied_rows = [i for i in feas_rows if dist_matrix[i][j] == d1]
        best_i = min(tied_rows, key=lambda i: prev_rank[i])
        assert best_i in primary_partner_of_prev, "Theorem 1: secondary_split parent must be in V_prev(M*)"

        other_rows = [i for i in feas_rows if i != best_i]
        sec_margin: float | None = None
        if other_rows:
            d2 = min(dist_matrix[i][j] for i in other_rows)
            sec_margin = max(0.0, fsum_exact([d2, -d1]))

        has_sec_tie = len(tied_rows) > 1
        ambig_tie = bool(u_info["matched_in_some_optimum"]) or has_sec_tie
        rule_basis = (
            "nearest_feasible_split_canonical_tiebreak"
            if has_sec_tie
            else "nearest_feasible_split_unique"
        )
        raw_edges.append(
            {
                "prev_idx": best_i,
                "curr_idx": j,
                "edge_role": "secondary_split",
                "rule_basis": rule_basis,
                "distance": d1,
                "primary_alternative_status": "promotion_gap",
                "optimality_margin": u_info["promotion_gap"],
                "secondary_margin": sec_margin,
                "ambiguous_tie": ambig_tie,
            }
        )

    # 3. Secondary merge edges (`nearest_feasible_v1`: for each i in U_prev^comp)
    for i, u_info in core["unmatched_prev_details"].items():
        if u_info["reason"] != "not_selected_in_global_optimum":
            continue
        feas_cols: list[int] = u_info["feasible_partners"]
        d1 = min(dist_matrix[i][j] for j in feas_cols)
        tied_cols = [j for j in feas_cols if dist_matrix[i][j] == d1]
        best_j = min(tied_cols, key=lambda j: curr_rank[j])
        assert best_j in primary_partner_of_curr, "Theorem 1: secondary_merge target must be in V_curr(M*)"

        other_cols = [j for j in feas_cols if j != best_j]
        sec_margin_m: float | None = None
        if other_cols:
            d2 = min(dist_matrix[i][j] for j in other_cols)
            sec_margin_m = max(0.0, fsum_exact([d2, -d1]))

        has_sec_tie = len(tied_cols) > 1
        ambig_tie = bool(u_info["matched_in_some_optimum"]) or has_sec_tie
        rule_basis = (
            "nearest_feasible_merge_canonical_tiebreak"
            if has_sec_tie
            else "nearest_feasible_merge_unique"
        )
        raw_edges.append(
            {
                "prev_idx": i,
                "curr_idx": best_j,
                "edge_role": "secondary_merge",
                "rule_basis": rule_basis,
                "distance": d1,
                "primary_alternative_status": "promotion_gap",
                "optimality_margin": u_info["promotion_gap"],
                "secondary_margin": sec_margin_m,
                "ambiguous_tie": ambig_tie,
            }
        )

    # 4. Connected components (star decomposition around each primary edge + isolated U^none)
    split_children_by_prev: dict[int, list[int]] = {i: [] for i in range(m)}
    merge_parents_by_curr: dict[int, list[int]] = {j: [] for j in range(n)}
    for re in raw_edges:
        if re["edge_role"] == "secondary_split":
            split_children_by_prev[re["prev_idx"]].append(re["curr_idx"])
        elif re["edge_role"] == "secondary_merge":
            merge_parents_by_curr[re["curr_idx"]].append(re["prev_idx"])

    components: list[dict[str, Any]] = []
    for p_idx, c_idx in core["chosen"]:
        sp_kids = split_children_by_prev[p_idx]
        mg_pars = merge_parents_by_curr[c_idx]
        assert not (sp_kids and mg_pars), "Theorem 2 violation: primary link has both split and merge"
        if sp_kids:
            ev_type = "split"
            p_set = [p_idx]
            c_set = [c_idx, *sp_kids]
        elif mg_pars:
            ev_type = "merge"
            p_set = [p_idx, *mg_pars]
            c_set = [c_idx]
        else:
            ev_type = "continuation"
            p_set = [p_idx]
            c_set = [c_idx]
        components.append(
            {
                "event_type": ev_type,
                "prev_indices": p_set,
                "curr_indices": c_set,
                "primary_pair": (p_idx, c_idx),
            }
        )

    for i, u_info in core["unmatched_prev_details"].items():
        if u_info["reason"] == "no_candidate_within_threshold":
            components.append(
                {
                    "event_type": "disappearance",
                    "prev_indices": [i],
                    "curr_indices": [],
                    "primary_pair": None,
                }
            )

    for j, u_info in core["unmatched_curr_details"].items():
        if u_info["reason"] == "no_candidate_within_threshold":
            components.append(
                {
                    "event_type": "emergence",
                    "prev_indices": [],
                    "curr_indices": [j],
                    "primary_pair": None,
                }
            )

    # Sort components by canonical component key (data-model.md §2.2)
    def component_sort_key(comp: dict[str, Any]) -> tuple[int, int]:
        if comp["prev_indices"]:
            return (0, min(prev_rank[i] for i in comp["prev_indices"]))
        return (1, min(curr_rank[j] for j in comp["curr_indices"]))

    components.sort(key=component_sort_key)

    # Map each edge endpoint pair to its component and compute `star_min_mass_proportional_v1` flow
    prev_fracs = [normalize_neg_zero(float(s["member_fraction"])) for s in prev_scens]
    curr_fracs = [normalize_neg_zero(float(s["member_fraction"])) for s in curr_scens]

    pair_to_flow_and_event: dict[tuple[int, int], tuple[float, str]] = {}
    for comp in components:
        ev_type = comp["event_type"]
        p_idxs = comp["prev_indices"]
        c_idxs = comp["curr_indices"]
        if ev_type == "continuation":
            i0, j0 = p_idxs[0], c_idxs[0]
            flow = min(prev_fracs[i0], curr_fracs[j0])
            pair_to_flow_and_event[(i0, j0)] = (normalize_neg_zero(flow), "continuation")
        elif ev_type == "split":
            i0 = p_idxs[0]
            p_i = prev_fracs[i0]
            exact_p = Fraction.from_float(p_i)
            exact_q = sum((Fraction.from_float(curr_fracs[j]) for j in c_idxs), Fraction(0))
            q_round = fsum_exact(curr_fracs[j] for j in c_idxs)
            for j in c_idxs:
                q_j = curr_fracs[j]
                if exact_p >= exact_q:
                    flow = q_j
                else:
                    flow = min(min(p_i, q_j), (p_i * q_j) / q_round)
                pair_to_flow_and_event[(i0, j)] = (normalize_neg_zero(flow), "split")
        elif ev_type == "merge":
            j0 = c_idxs[0]
            q_j = curr_fracs[j0]
            exact_q = Fraction.from_float(q_j)
            exact_p = sum((Fraction.from_float(prev_fracs[i]) for i in p_idxs), Fraction(0))
            p_round = fsum_exact(prev_fracs[i] for i in p_idxs)
            for i in p_idxs:
                p_i = prev_fracs[i]
                if exact_q >= exact_p:
                    flow = p_i
                else:
                    flow = min(min(p_i, q_j), (q_j * p_i) / p_round)
                pair_to_flow_and_event[(i, j0)] = (normalize_neg_zero(flow), "merge")

    # 5. Finalize EvolutionEdge records sorted by (prev_cluster_id, curr_cluster_id)
    edges_out: list[dict[str, Any]] = []
    evol_idx_pairs: set[tuple[int, int]] = set()
    edges_by_prev_idx: dict[int, list[dict[str, Any]]] = {i: [] for i in range(m)}
    edges_by_curr_idx: dict[int, list[dict[str, Any]]] = {j: [] for j in range(n)}

    for re in raw_edges:
        p_idx = re["prev_idx"]
        c_idx = re["curr_idx"]
        evol_idx_pairs.add((p_idx, c_idx))
        d_val = re["distance"]
        p_frac = prev_fracs[p_idx]
        c_frac = curr_fracs[c_idx]
        att_flow, ev_type = pair_to_flow_and_event[(p_idx, c_idx)]
        thr_margin = max(0.0, normalize_neg_zero(tau - d_val))
        opt_margin = re["optimality_margin"]
        sec_margin = re["secondary_margin"]

        hi_unc, unc_reasons = _evaluate_edge_uncertainty(
            ambiguous_tie=re["ambiguous_tie"],
            tau=tau,
            ref_scale=ref_scale,
            threshold_margin=thr_margin,
            optimality_margin=opt_margin,
            secondary_margin=sec_margin,
            eps_tau=eps_tau,
            eps_opt=eps_opt,
            eps_sec=eps_sec,
        )

        edge_rec = {
            "prev_cluster_id": int(prev_scens[p_idx]["cluster_id"]),
            "curr_cluster_id": int(curr_scens[c_idx]["cluster_id"]),
            "edge_role": re["edge_role"],
            "event_type": ev_type,
            "rule_basis": re["rule_basis"],
            "distance": d_val,
            "distance_over_scale": _checked_opt_ratio(
                d_val, pair_scale, "edge.distance_over_scale"
            ),
            "prev_member_fraction": p_frac,
            "curr_member_fraction": c_frac,
            "edge_delta_fraction": normalize_neg_zero(c_frac - p_frac),
            "attributed_flow": att_flow,
            "edge_strength": _compute_edge_strength(d_val, tau),
            "threshold_margin": thr_margin,
            "threshold_margin_ratio": _checked_opt_ratio(
                thr_margin, ref_scale, "edge.threshold_margin_ratio"
            ),
            "primary_alternative_status": re["primary_alternative_status"],
            "optimality_margin": opt_margin,
            "optimality_margin_ratio": (
                _checked_ratio(opt_margin, ref_scale, "edge.optimality_margin_ratio")
                if (opt_margin is not None and ref_scale is not None)
                else None
            ),
            "secondary_margin": sec_margin,
            "secondary_margin_ratio": (
                _checked_ratio(sec_margin, ref_scale, "edge.secondary_margin_ratio")
                if (sec_margin is not None and ref_scale is not None)
                else None
            ),
            "ambiguous_tie": re["ambiguous_tie"],
            "high_uncertainty": hi_unc,
            "uncertainty_reasons": unc_reasons,
        }
        edges_out.append(edge_rec)
        edges_by_prev_idx[p_idx].append(edge_rec)
        edges_by_curr_idx[c_idx].append(edge_rec)

    edges_out.sort(key=lambda e: (e["prev_cluster_id"], e["curr_cluster_id"]))

    # 6. Build NodeEvolutionSummary for prev_nodes and curr_nodes (sorted by cluster_id)
    prev_nodes_out: list[dict[str, Any]] = []
    prev_u_none_high_unc: dict[int, bool] = {}
    for i in range(m):
        inc_edges = edges_by_prev_idx[i]
        prim_edges = [e for e in inc_edges if e["edge_role"] == "primary"]
        sec_edges = [e for e in inc_edges if e["edge_role"] != "primary"]
        p_deg = len(prim_edges)
        s_deg = len(sec_edges)

        p_frac = prev_fracs[i]
        att_out = fsum_exact(e["attributed_flow"] for e in inc_edges)
        unatt_out = max(0.0, fsum_exact([p_frac, -att_out]))
        min_cand = min(dist_matrix[i][j] for j in range(n))

        if p_deg == 1:
            partner_j = primary_partner_of_prev[i]
            partner_cid: int | None = int(curr_scens[partner_j]["cluster_id"])
            prim_delta: float | None = normalize_neg_zero(curr_fracs[partner_j] - p_frac)
            unmatched_reason: str | None = None
            min_cand_opt: float | None = None
            min_cand_scale_opt: float | None = None
            node_thr_margin: float | None = None
            if s_deg > 0:
                node_role = "split_parent"
                in_split, in_merge = True, False
            elif len(merge_parents_by_curr[partner_j]) > 0:
                node_role = "merge_primary_parent"
                in_split, in_merge = False, True
            else:
                node_role = "continuation_source"
                in_split, in_merge = False, False
            node_ambig = any(e["ambiguous_tie"] for e in inc_edges)
            node_hi_unc = any(e["high_uncertainty"] for e in inc_edges)
        else:
            partner_cid = None
            prim_delta = None
            u_info = core["unmatched_prev_details"][i]
            unmatched_reason = u_info["reason"]
            min_cand_opt = min_cand
            min_cand_scale_opt = _checked_opt_ratio(
                min_cand, pair_scale, "prev_nodes.min_candidate_distance_over_scale"
            )
            if s_deg > 0:
                node_role = "merge_secondary_parent"
                in_split, in_merge = False, True
                node_thr_margin = None
                node_ambig = any(e["ambiguous_tie"] for e in inc_edges)
                node_hi_unc = any(e["high_uncertainty"] for e in inc_edges)
            else:
                node_role = "disappeared"
                in_split, in_merge = False, False
                node_thr_margin = max(0.0, normalize_neg_zero(min_cand - tau))
                node_ambig = False
                node_hi_unc = tau == 0.0 or (
                    ref_scale is not None and node_thr_margin <= eps_tau * ref_scale
                )
                prev_u_none_high_unc[i] = node_hi_unc

        prev_nodes_out.append(
            {
                "cluster_id": int(prev_scens[i]["cluster_id"]),
                "canonical_rank": prev_rank[i],
                "member_count": int(prev_scens[i]["member_count"]),
                "member_fraction": p_frac,
                "primary_degree": p_deg,
                "secondary_degree": s_deg,
                "out_degree_primary": p_deg,
                "out_degree_secondary": s_deg,
                "node_role": node_role,
                "participates_in_split": in_split,
                "participates_in_merge": in_merge,
                "unmatched_reason": unmatched_reason,
                "min_candidate_distance": min_cand_opt,
                "min_candidate_distance_over_scale": min_cand_scale_opt,
                "primary_counterpart_cluster_id": partner_cid,
                "primary_delta_fraction": prim_delta,
                "attributed_flow_fraction": att_out,
                "unattributed_fraction": unatt_out,
                "threshold_margin": node_thr_margin,
                "ambiguous_tie": node_ambig,
                "high_uncertainty": node_hi_unc,
            }
        )
    prev_nodes_out.sort(key=lambda x: x["cluster_id"])

    curr_nodes_out: list[dict[str, Any]] = []
    curr_u_none_high_unc: dict[int, bool] = {}
    for j in range(n):
        inc_edges = edges_by_curr_idx[j]
        prim_edges = [e for e in inc_edges if e["edge_role"] == "primary"]
        sec_edges = [e for e in inc_edges if e["edge_role"] != "primary"]
        p_deg = len(prim_edges)
        s_deg = len(sec_edges)

        c_frac = curr_fracs[j]
        att_in = fsum_exact(e["attributed_flow"] for e in inc_edges)
        unatt_in = max(0.0, fsum_exact([c_frac, -att_in]))
        min_cand = min(dist_matrix[i][j] for i in range(m))

        if p_deg == 1:
            partner_i = primary_partner_of_curr[j]
            partner_cid_c: int | None = int(prev_scens[partner_i]["cluster_id"])
            prim_delta_c: float | None = normalize_neg_zero(c_frac - prev_fracs[partner_i])
            unmatched_reason_c: str | None = None
            min_cand_opt_c: float | None = None
            min_cand_scale_opt_c: float | None = None
            node_thr_margin_c: float | None = None
            if s_deg > 0:
                node_role_c = "merge_target"
                in_split_c, in_merge_c = False, True
            elif len(split_children_by_prev[partner_i]) > 0:
                node_role_c = "split_primary_child"
                in_split_c, in_merge_c = True, False
            else:
                node_role_c = "continuation_target"
                in_split_c, in_merge_c = False, False
            node_ambig_c = any(e["ambiguous_tie"] for e in inc_edges)
            node_hi_unc_c = any(e["high_uncertainty"] for e in inc_edges)
        else:
            partner_cid_c = None
            prim_delta_c = None
            u_info_c = core["unmatched_curr_details"][j]
            unmatched_reason_c = u_info_c["reason"]
            min_cand_opt_c = min_cand
            min_cand_scale_opt_c = _checked_opt_ratio(
                min_cand, pair_scale, "curr_nodes.min_candidate_distance_over_scale"
            )
            if s_deg > 0:
                node_role_c = "split_secondary_child"
                in_split_c, in_merge_c = True, False
                node_thr_margin_c = None
                node_ambig_c = any(e["ambiguous_tie"] for e in inc_edges)
                node_hi_unc_c = any(e["high_uncertainty"] for e in inc_edges)
            else:
                node_role_c = "emerged"
                in_split_c, in_merge_c = False, False
                node_thr_margin_c = max(0.0, normalize_neg_zero(min_cand - tau))
                node_ambig_c = False
                node_hi_unc_c = tau == 0.0 or (
                    ref_scale is not None and node_thr_margin_c <= eps_tau * ref_scale
                )
                curr_u_none_high_unc[j] = node_hi_unc_c

        curr_nodes_out.append(
            {
                "cluster_id": int(curr_scens[j]["cluster_id"]),
                "canonical_rank": curr_rank[j],
                "member_count": int(curr_scens[j]["member_count"]),
                "member_fraction": c_frac,
                "primary_degree": p_deg,
                "secondary_degree": s_deg,
                "in_degree_primary": p_deg,
                "in_degree_secondary": s_deg,
                "node_role": node_role_c,
                "participates_in_split": in_split_c,
                "participates_in_merge": in_merge_c,
                "unmatched_reason": unmatched_reason_c,
                "min_candidate_distance": min_cand_opt_c,
                "min_candidate_distance_over_scale": min_cand_scale_opt_c,
                "primary_counterpart_cluster_id": partner_cid_c,
                "primary_delta_fraction": prim_delta_c,
                "attributed_flow_fraction": att_in,
                "unattributed_fraction": unatt_in,
                "threshold_margin": node_thr_margin_c,
                "ambiguous_tie": node_ambig_c,
                "high_uncertainty": node_hi_unc_c,
            }
        )
    curr_nodes_out.sort(key=lambda x: x["cluster_id"])

    # 7. Build EvolutionEvent records
    events_out: list[dict[str, Any]] = []
    for ev_idx, comp in enumerate(components):
        ev_type = comp["event_type"]
        p_idxs = comp["prev_indices"]
        c_idxs = comp["curr_indices"]
        p_cids = sorted(int(prev_scens[i]["cluster_id"]) for i in p_idxs)
        c_cids = sorted(int(curr_scens[j]["cluster_id"]) for j in c_idxs)
        p_cid_set = set(p_cids)
        c_cid_set = set(c_cids)

        comp_edges = [
            e
            for e in edges_out
            if e["prev_cluster_id"] in p_cid_set and e["curr_cluster_id"] in c_cid_set
        ]
        if comp["primary_pair"] is not None:
            pi, cj = comp["primary_pair"]
            prim_link: tuple[int, int] | None = (
                int(prev_scens[pi]["cluster_id"]),
                int(curr_scens[cj]["cluster_id"]),
            )
        else:
            prim_link = None

        if ev_type == "continuation":
            ev_rule_basis = comp_edges[0]["rule_basis"]
        elif ev_type == "split":
            ev_rule_basis = (
                "primary_plus_nearest_feasible_split_canonical_tiebreak"
                if any(e["rule_basis"].endswith("_canonical_tiebreak") for e in comp_edges)
                else "primary_plus_nearest_feasible_split"
            )
        elif ev_type == "merge":
            ev_rule_basis = (
                "primary_plus_nearest_feasible_merge_canonical_tiebreak"
                if any(e["rule_basis"].endswith("_canonical_tiebreak") for e in comp_edges)
                else "primary_plus_nearest_feasible_merge"
            )
        else:
            ev_rule_basis = "no_candidate_within_threshold"

        max_morph = max((e["distance"] for e in comp_edges), default=0.0)
        p_sum = fsum_exact(prev_fracs[i] for i in p_idxs)
        c_sum = fsum_exact(curr_fracs[j] for j in c_idxs)
        net_delta = fsum_exact([*(curr_fracs[j] for j in c_idxs), *(-prev_fracs[i] for i in p_idxs)])
        flow_sum = fsum_exact(e["attributed_flow"] for e in comp_edges)

        margin_ratios: list[float] = []
        if ref_scale is not None:
            if comp_edges:
                for e in comp_edges:
                    if e["threshold_margin_ratio"] is not None:
                        margin_ratios.append(e["threshold_margin_ratio"])
                    if e["optimality_margin_ratio"] is not None:
                        margin_ratios.append(e["optimality_margin_ratio"])
                    if e["secondary_margin_ratio"] is not None:
                        margin_ratios.append(e["secondary_margin_ratio"])
            elif ev_type == "disappearance":
                i0 = p_idxs[0]
                u_m = max(0.0, normalize_neg_zero(min(dist_matrix[i0][j] for j in range(n)) - tau))
                margin_ratios.append(_checked_ratio(u_m, ref_scale, "disappearance.min_margin_ratio"))
            elif ev_type == "emergence":
                j0 = c_idxs[0]
                u_m = max(0.0, normalize_neg_zero(min(dist_matrix[i][j0] for i in range(m)) - tau))
                margin_ratios.append(_checked_ratio(u_m, ref_scale, "emergence.min_margin_ratio"))

        min_m_ratio = min(margin_ratios) if margin_ratios else None
        ev_ambig = any(e["ambiguous_tie"] for e in comp_edges)
        if comp_edges:
            ev_hi_unc = any(e["high_uncertainty"] for e in comp_edges)
        elif ev_type == "disappearance":
            ev_hi_unc = prev_u_none_high_unc[p_idxs[0]]
        else:
            ev_hi_unc = curr_u_none_high_unc[c_idxs[0]]

        events_out.append(
            {
                "event_index": ev_idx,
                "event_type": ev_type,
                "prev_cluster_ids": p_cids,
                "curr_cluster_ids": c_cids,
                "primary_link": prim_link,
                "rule_basis": ev_rule_basis,
                "max_morphology_distance": max_morph,
                "max_morphology_over_scale": _checked_opt_ratio(
                    max_morph, pair_scale, "events.max_morphology_over_scale"
                ),
                "prev_fraction_sum": p_sum,
                "curr_fraction_sum": c_sum,
                "net_delta_fraction": net_delta,
                "attributed_flow_sum": flow_sum,
                "min_margin_ratio": min_m_ratio,
                "ambiguous_tie": ev_ambig,
                "high_uncertainty": ev_hi_unc,
            }
        )

    # 8. Build TransitionEvolutionSummary (`summary`)
    prim_edges_all = [e for e in edges_out if e["edge_role"] == "primary"]
    split_edges_all = [e for e in edges_out if e["edge_role"] == "secondary_split"]
    merge_edges_all = [e for e in edges_out if e["edge_role"] == "secondary_merge"]

    prim_total_dist = fsum_exact(e["distance"] for e in prim_edges_all)
    prim_max_dist = max((e["distance"] for e in prim_edges_all), default=0.0)
    all_max_dist = max((e["distance"] for e in edges_out), default=0.0)

    # Branch TV redistribution: union of all primary branches across prev & curr
    branch_l1_terms: list[float] = []
    for p_idx, c_idx in core["chosen"]:
        branch_l1_terms.append(abs(curr_fracs[c_idx] - prev_fracs[p_idx]))
    for i in core["unmatched_prev_details"]:
        branch_l1_terms.append(prev_fracs[i])
    for j in core["unmatched_curr_details"]:
        branch_l1_terms.append(curr_fracs[j])
    tv_redistribution = normalize_neg_zero(0.5 * fsum_exact(branch_l1_terms))

    sec_att_flow = fsum_exact(
        e["attributed_flow"] for e in (*split_edges_all, *merge_edges_all)
    )
    tot_att_flow = fsum_exact(e["attributed_flow"] for e in edges_out)
    disapp_frac_sum = fsum_exact(
        prev_fracs[i]
        for i, u in core["unmatched_prev_details"].items()
        if u["reason"] == "no_candidate_within_threshold"
    )
    emerg_frac_sum = fsum_exact(
        curr_fracs[j]
        for j, u in core["unmatched_curr_details"].items()
        if u["reason"] == "no_candidate_within_threshold"
    )
    comp_tv_residual = normalize_neg_zero(
        0.5 * fsum_exact(abs(ev["net_delta_fraction"]) for ev in events_out)
    )

    # Active threshold margin over E_evol U {(i, j) : d_{i,j} > tau}
    active_thr_margins: list[float] = []
    raw_thr_margins: list[float] = []
    for i in range(m):
        for j in range(n):
            raw_m = abs(dist_matrix[i][j] - tau)
            raw_thr_margins.append(raw_m)
            if (i, j) in evol_idx_pairs or not eligible_matrix[i][j]:
                active_thr_margins.append(raw_m)

    min_thr_margin = normalize_neg_zero(min(active_thr_margins))
    raw_mat_thr_margin = normalize_neg_zero(min(raw_thr_margins))

    opt_margins_all = [e["optimality_margin"] for e in edges_out if e["optimality_margin"] is not None]
    min_opt_margin = min(opt_margins_all) if opt_margins_all else None

    sec_margins_all = [e["secondary_margin"] for e in edges_out if e["secondary_margin"] is not None]
    min_sec_margin = min(sec_margins_all) if sec_margins_all else None

    trans_ambig = (core["optima_count"] > 1) or any(e["ambiguous_tie"] for e in edges_out)
    hi_unc_edge_count = sum(1 for e in edges_out if e["high_uncertainty"])
    trans_hi_unc = (
        trans_ambig
        or hi_unc_edge_count > 0
        or any(prev_u_none_high_unc.values())
        or any(curr_u_none_high_unc.values())
        or tau == 0.0
        or (ref_scale is not None and min_thr_margin <= eps_tau * ref_scale)
    )

    summary = {
        "primary_edge_count": len(prim_edges_all),
        "secondary_split_edge_count": len(split_edges_all),
        "secondary_merge_edge_count": len(merge_edges_all),
        "continuation_count": sum(1 for ev in events_out if ev["event_type"] == "continuation"),
        "split_count": sum(1 for ev in events_out if ev["event_type"] == "split"),
        "merge_count": sum(1 for ev in events_out if ev["event_type"] == "merge"),
        "emergence_count": sum(1 for ev in events_out if ev["event_type"] == "emergence"),
        "disappearance_count": sum(1 for ev in events_out if ev["event_type"] == "disappearance"),
        "primary_total_distance": prim_total_dist,
        "primary_max_distance": prim_max_dist,
        "primary_max_distance_over_scale": _checked_opt_ratio(
            prim_max_dist, pair_scale, "summary.primary_max_distance_over_scale"
        ),
        "all_edges_max_distance": all_max_dist,
        "all_edges_max_distance_over_scale": _checked_opt_ratio(
            all_max_dist, pair_scale, "summary.all_edges_max_distance_over_scale"
        ),
        "transition_tv_redistribution": tv_redistribution,
        "secondary_attributed_flow": sec_att_flow,
        "total_attributed_flow": tot_att_flow,
        "disappeared_fraction_sum": disapp_frac_sum,
        "emerged_fraction_sum": emerg_frac_sum,
        "component_tv_residual": comp_tv_residual,
        "min_threshold_margin": min_thr_margin,
        "min_threshold_margin_ratio": _checked_opt_ratio(
            min_thr_margin, ref_scale, "summary.min_threshold_margin_ratio"
        ),
        "raw_matrix_threshold_margin": raw_mat_thr_margin,
        "min_optimality_margin": min_opt_margin,
        "min_optimality_margin_ratio": (
            _checked_ratio(min_opt_margin, ref_scale, "summary.min_optimality_margin_ratio")
            if (min_opt_margin is not None and ref_scale is not None)
            else None
        ),
        "min_secondary_margin": min_sec_margin,
        "min_secondary_margin_ratio": (
            _checked_ratio(min_sec_margin, ref_scale, "summary.min_secondary_margin_ratio")
            if (min_sec_margin is not None and ref_scale is not None)
            else None
        ),
        "ambiguous_tie": trans_ambig,
        "high_uncertainty_edge_count": hi_unc_edge_count,
        "high_uncertainty": trans_hi_unc,
    }

    backbone_res = solve_backbone_oracle(task)
    result = normalize_floats(
        {
            "algorithm_version": EVOLUTION_ALGORITHM_VERSION,
            "secondary_rule": SECONDARY_RULE_V1,
            "flow_rule": FLOW_RULE_V1,
            "uncertainty_rule": UNCERTAINTY_RULE_V1,
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
            "prev_cluster_ids": [int(s["cluster_id"]) for s in prev_scens],
            "curr_cluster_ids": [int(s["cluster_id"]) for s in curr_scens],
            "threshold": tau,
            "pair_scale": pair_scale,
            "threshold_coefficient": c_coeff,
            "prev_dispersion": s_prev,
            "curr_dispersion": s_curr,
            "reference_scale": ref_scale,
            "backbone": backbone_res,
            "edges": edges_out,
            "prev_nodes": prev_nodes_out,
            "curr_nodes": curr_nodes_out,
            "events": events_out,
            "summary": summary,
        }
    )

    invariants = verify_berge_and_star_invariants(result)
    assert invariants["all_passed"], f"Berge/star invariant failure: {invariants}"
    return result


# Aliases for ergonomic downstream usage
evolve_pair_oracle = solve_evolution_oracle
solve_oracle = solve_evolution_oracle


def verify_chain_invariants(chain_result: dict[str, Any]) -> dict[str, bool]:
    """Verify M3 global chain invariants E1-E8 on a solved chain oracle result."""
    planned_b = chain_result["planned_batch_count"]
    transitions = chain_result["transitions"]
    nodes = chain_result["nodes"]
    edges = chain_result["edges"]
    events = chain_result["events"]
    branches = chain_result["branch_summaries"]

    node_by_key = {(n["ordinal"], n["cluster_id"]): n for n in nodes}

    # E2: Primary edges preserve branch_id
    e2_primary_branch_preserved = all(
        e["prev_branch_id"] == e["curr_branch_id"]
        and node_by_key[(e["prev_ordinal"], e["prev_cluster_id"])]["branch_id"] == e["prev_branch_id"]
        and node_by_key[(e["curr_ordinal"], e["curr_cluster_id"])]["branch_id"] == e["curr_branch_id"]
        for e in edges
        if e["edge_role"] == "primary"
    )

    # E3: Secondary edge endpoint degrees & threshold feasibility
    e3_secondary_roles = True
    for e in edges:
        if e["distance"] > transitions[e["prev_ordinal"]]["threshold"]:
            e3_secondary_roles = False
        if e["edge_role"] == "secondary_split":
            cn = node_by_key[(e["curr_ordinal"], e["curr_cluster_id"])]
            if cn["in_degree_primary"] != 0 or cn["assignment_reason"] != "not_selected_in_global_optimum":
                e3_secondary_roles = False
        elif e["edge_role"] == "secondary_merge":
            pn = node_by_key[(e["prev_ordinal"], e["prev_cluster_id"])]
            if pn["out_degree_primary"] != 0:
                e3_secondary_roles = False

    for n in nodes:
        if n["assignment_reason"] == "no_candidate_within_threshold":
            if n["in_degree_primary"] + n["in_degree_secondary"] != 0:
                e3_secondary_roles = False

    # E4: Transition count == B - 1, zero cross-gap edges, contiguous branch ordinals
    gap_ords = {tr["prev_ordinal"] for tr in transitions if tr["chain_broken"]}
    e4_no_gap_edges = (len(transitions) == planned_b - 1) and all(
        e["prev_ordinal"] not in gap_ords for e in edges
    )
    ords_by_branch: dict[str, list[int]] = {}
    for n in nodes:
        ords_by_branch.setdefault(n["branch_id"], []).append(n["ordinal"])
    e4_contiguous_branches = all(
        max(ords) - min(ords) + 1 == len(ords) == len(set(ords))
        for ords in ords_by_branch.values()
    )

    # E5: Gap events strictly disjoint from ok events
    e5_gap_disjoint = True
    for ev in events:
        tr = transitions[ev["prev_ordinal"]]
        is_gap_ev = ev["event_type"] in ("gap_interrupted", "gap_restarted")
        if tr["transition_status"] == "gap" and not is_gap_ev:
            e5_gap_disjoint = False
        if tr["transition_status"] == "ok" and is_gap_ev:
            e5_gap_disjoint = False

    # E6: Node degrees equal incident edge counts
    e6_degree_conservation = True
    for n in nodes:
        o, c = n["ordinal"], n["cluster_id"]
        in_p = sum(1 for e in edges if e["curr_ordinal"] == o and e["curr_cluster_id"] == c and e["edge_role"] == "primary")
        in_s = sum(1 for e in edges if e["curr_ordinal"] == o and e["curr_cluster_id"] == c and e["edge_role"] != "primary")
        out_p = sum(1 for e in edges if e["prev_ordinal"] == o and e["prev_cluster_id"] == c and e["edge_role"] == "primary")
        out_s = sum(1 for e in edges if e["prev_ordinal"] == o and e["prev_cluster_id"] == c and e["edge_role"] != "primary")
        if (in_p, in_s, out_p, out_s) != (
            n["in_degree_primary"],
            n["in_degree_secondary"],
            n["out_degree_primary"],
            n["out_degree_secondary"],
        ):
            e6_degree_conservation = False

    # E7: Branch lifecycle conservation
    branch_ids_from_summaries = {b["branch_id"] for b in branches}
    e7_branch_lifecycle = (
        branch_ids_from_summaries == set(ords_by_branch.keys())
        and sum(b["node_count"] for b in branches) == len(nodes)
        and all(
            b["node_count"] == b["end_ordinal"] - b["birth_ordinal"] + 1 == b["step_count"] + 1
            for b in branches
        )
    )

    # E8: Non-empty disclaimer
    e8_disclaimer = (
        bool(chain_result.get("non_physical_disclaimer"))
        and all(bool(e.get("non_physical_disclaimer")) for e in edges)
        and all(bool(ev.get("non_physical_disclaimer")) for ev in events)
    )

    all_ok = (
        e2_primary_branch_preserved
        and e3_secondary_roles
        and e4_no_gap_edges
        and e4_contiguous_branches
        and e5_gap_disjoint
        and e6_degree_conservation
        and e7_branch_lifecycle
        and e8_disclaimer
    )
    return {
        "E2_primary_branch_preserved": e2_primary_branch_preserved,
        "E3_secondary_feasibility_and_roles": e3_secondary_roles,
        "E4_gap_breaks_and_contiguous_ordinals": e4_no_gap_edges and e4_contiguous_branches,
        "E5_gap_events_disjoint": e5_gap_disjoint,
        "E6_node_degree_conservation": e6_degree_conservation,
        "E7_branch_lifecycle_conservation": e7_branch_lifecycle,
        "E8_finite_and_disclaimer": e8_disclaimer,
        "all_passed": all_ok,
    }


def solve_chain_oracle(chain_task: dict[str, Any]) -> dict[str, Any]:
    """Solve multi-batch (`4..=8`) scenario evolution chain in pure Python."""
    cfg = validate_evolution_config(chain_task.get("config"))
    batches = chain_task.get("batches", ())
    if not (4 <= len(batches) <= 8):
        raise OracleValidationError(
            "INVALID_EVOLUTION_CONFIG",
            f"planned_batch_count must be in 4..=8, got {len(batches)}",
        )

    weights = chain_task.get("weights", ())
    if not weights:
        raise OracleValidationError("EMPTY_FEATURES", "Chain weights must not be empty")
    for g, w in enumerate(weights):
        if not is_finite_float_like(w) or float(w) < 0.0:
            raise OracleValidationError("INVALID_WEIGHTS", f"Invalid weight at {g}", feature_index=g)
    if fsum_exact(float(w) for w in weights) <= 0.0:
        raise OracleValidationError("INVALID_WEIGHTS", "Sum of spatial weights must be > 0.0")

    use_scaled = chain_task.get("threshold_coefficient") is not None
    if use_scaled:
        c_raw = chain_task["threshold_coefficient"]
        if not is_finite_float_like(c_raw) or float(c_raw) < 0.0:
            raise OracleValidationError("INVALID_THRESHOLD", f"Invalid threshold_coefficient: {c_raw!r}")
        c_coeff: float | None = normalize_neg_zero(float(c_raw))
        abs_tau: float | None = None
    else:
        t_raw = chain_task.get("threshold")
        if not is_finite_float_like(t_raw) or float(t_raw) < 0.0:
            raise OracleValidationError("INVALID_THRESHOLD", f"Invalid threshold: {t_raw!r}")
        c_coeff = None
        abs_tau = normalize_neg_zero(float(t_raw))

    run_key = str(chain_task.get("tracking_run_key") or chain_task.get("run_key") or "000000000000")
    prefix = run_key[:12]
    planned_b = len(batches)

    # Validate batch ordinals and per-batch scenarios
    resolved_batches: list[dict[str, Any]] = []
    for expected_ord, b in enumerate(batches):
        if int(b.get("ordinal", expected_ord)) != expected_ord:
            raise OracleValidationError(
                "INVALID_EVOLUTION_CONFIG",
                f"Batch ordinal {b.get('ordinal')} != expected {expected_ord}",
            )
        status = str(b.get("status", "ok"))
        if status not in ("ok", "missing", "failed"):
            raise OracleValidationError(
                "INVALID_EVOLUTION_CONFIG",
                f"Unsupported batch status at ordinal {expected_ord}: {status!r}",
            )
        if status != "ok":
            if status == "failed":
                if b.get("code") is not None or b.get("error_code") is not None:
                    raw_code = b.get("code") if b.get("code") is not None else b.get("error_code")
                    err_code = str(raw_code).strip()
                else:
                    raw_reason = str(b.get("status_reason") or b.get("reason") or "").strip()
                    m_tok = re.match(r"^([A-Za-z0-9_]+)(?:[^A-Za-z0-9_]|$)", raw_reason)
                    err_code = m_tok.group(1) if m_tok else ""
                if not err_code or err_code not in WHITELISTED_SINGLE_BATCH_FAILURE_CODES:
                    raise OracleValidationError(
                        "INVALID_EVOLUTION_CONFIG",
                        f"batch {expected_ord} Failed status carries non-whitelisted error_code '{err_code}'",
                        prev_index=expected_ord,
                    )
            resolved_batches.append(
                {
                    "ordinal": expected_ord,
                    "status": status,
                    "status_reason": b.get("status_reason") if b.get("status_reason") is not None else b.get("reason"),
                    "dispersion": None,
                    "scenarios": [],
                    "canonical_rank": {},
                }
            )
            continue

        scens = list(b.get("scenarios", ()))
        # Validate single batch against itself to enforce all scenario/weight/fraction rules
        _validate_scenarios_and_weights(
            scens,
            scens,
            weights,
            check_fractions=True,
            fraction_sum_tolerance=cfg["fraction_sum_tolerance"],
        )
        disp = b.get("dispersion")
        if disp is None and b.get("members") is not None:
            disp = compute_ensemble_dispersion_exact(b["members"], weights)
        if use_scaled:
            if not is_finite_float_like(disp):
                raise OracleValidationError("INVALID_THRESHOLD", f"Non-finite dispersion at batch {expected_ord}")
            if float(disp) <= 0.0:
                raise OracleValidationError("ZERO_DISPERSION", f"Non-positive dispersion at batch {expected_ord}")
            disp_f: float | None = normalize_neg_zero(float(disp))
        else:
            disp_f = normalize_neg_zero(float(disp)) if is_finite_float_like(disp) else None

        order = canonical_scenario_order(scens)
        c_rank = {int(scens[idx]["cluster_id"]): rank for rank, idx in enumerate(order)}
        scens_sorted = sorted(scens, key=lambda s: int(s["cluster_id"]))
        resolved_batches.append(
            {
                "ordinal": expected_ord,
                "status": "ok",
                "status_reason": b.get("status_reason"),
                "dispersion": disp_f,
                "scenarios": scens_sorted,
                "canonical_rank": c_rank,
            }
        )

    # Solve all ok adjacent transitions
    pair_results: dict[int, dict[str, Any]] = {}
    for prev_ord in range(planned_b - 1):
        b_prev = resolved_batches[prev_ord]
        b_curr = resolved_batches[prev_ord + 1]
        if b_prev["status"] == "ok" and b_curr["status"] == "ok":
            pair_task: dict[str, Any] = {
                "prev_scenarios": b_prev["scenarios"],
                "curr_scenarios": b_curr["scenarios"],
                "weights": weights,
                "config": cfg,
            }
            if use_scaled:
                pair_task["mode"] = "scaled"
                pair_task["threshold_coefficient"] = c_coeff
                pair_task["prev_dispersion"] = b_prev["dispersion"]
                pair_task["curr_dispersion"] = b_curr["dispersion"]
            else:
                pair_task["mode"] = "absolute"
                pair_task["threshold"] = abs_tau
            pair_results[prev_ord] = solve_evolution_oracle(pair_task)

    # Assign M2-identical deterministic branch_id & node records across all ok batches
    branch_id_map: dict[tuple[int, int], str] = {}
    assignment_reason_map: dict[tuple[int, int], str] = {}
    birth_reason_map: dict[str, str] = {}

    for ord_idx in range(planned_b):
        b_info = resolved_batches[ord_idx]
        if b_info["status"] != "ok":
            continue
        c_rank_map: dict[int, int] = b_info["canonical_rank"]

        if ord_idx == 0:
            for sc in b_info["scenarios"]:
                cid = int(sc["cluster_id"])
                rk = c_rank_map[cid]
                bid = f"{prefix}:b{ord_idx:02d}:k{rk:02d}"
                branch_id_map[(ord_idx, cid)] = bid
                assignment_reason_map[(ord_idx, cid)] = "first_batch"
                birth_reason_map[bid] = "first_batch"
        elif resolved_batches[ord_idx - 1]["status"] != "ok":
            for sc in b_info["scenarios"]:
                cid = int(sc["cluster_id"])
                rk = c_rank_map[cid]
                bid = f"{prefix}:b{ord_idx:02d}:k{rk:02d}"
                branch_id_map[(ord_idx, cid)] = bid
                assignment_reason_map[(ord_idx, cid)] = "previous_planned_batch_unavailable"
                birth_reason_map[bid] = "gap_restarted"
        else:
            pres = pair_results[ord_idx - 1]
            prim_in: dict[int, tuple[int, bool]] = {
                l["curr_cluster_id"]: (l["prev_cluster_id"], l["ambiguous"])
                for l in pres["backbone"]["links"]
            }
            unm_curr: dict[int, str] = {
                u["cluster_id"]: u["reason"] for u in pres["backbone"]["unmatched_curr"]
            }
            for sc in b_info["scenarios"]:
                cid = int(sc["cluster_id"])
                rk = c_rank_map[cid]
                if cid in prim_in:
                    prev_cid, is_amb = prim_in[cid]
                    bid = branch_id_map[(ord_idx - 1, prev_cid)]
                    branch_id_map[(ord_idx, cid)] = bid
                    assignment_reason_map[(ord_idx, cid)] = (
                        "inherited_ambiguous" if is_amb else "inherited"
                    )
                else:
                    bid = f"{prefix}:b{ord_idx:02d}:k{rk:02d}"
                    reason = unm_curr[cid]
                    branch_id_map[(ord_idx, cid)] = bid
                    assignment_reason_map[(ord_idx, cid)] = reason
                    birth_reason_map[bid] = (
                        "secondary_split"
                        if reason == "not_selected_in_global_optimum"
                        else "emergence"
                    )

    # Assemble nodes
    nodes_out: list[dict[str, Any]] = []
    for ord_idx in range(planned_b):
        b_info = resolved_batches[ord_idx]
        if b_info["status"] != "ok":
            continue
        in_res = pair_results.get(ord_idx - 1)
        out_res = pair_results.get(ord_idx)
        in_node_map = (
            {n["cluster_id"]: n for n in in_res["curr_nodes"]} if in_res is not None else {}
        )
        out_node_map = (
            {n["cluster_id"]: n for n in out_res["prev_nodes"]} if out_res is not None else {}
        )

        for sc in b_info["scenarios"]:
            cid = int(sc["cluster_id"])
            mf = normalize_neg_zero(float(sc["member_fraction"]))
            in_n = in_node_map.get(cid)
            out_n = out_node_map.get(cid)

            if ord_idx == 0:
                in_deg_p, in_deg_s = 0, 0
                in_role = "first_batch"
                has_prim_delta = False
                prim_delta_val = 0.0
                unatt_in = 0.0
                in_hi_unc = False
            elif in_n is None:
                in_deg_p, in_deg_s = 0, 0
                in_role = "gap_restarted"
                has_prim_delta = False
                prim_delta_val = 0.0
                unatt_in = 0.0
                in_hi_unc = False
            else:
                in_deg_p = in_n["in_degree_primary"]
                in_deg_s = in_n["in_degree_secondary"]
                in_role = in_n["node_role"]
                has_prim_delta = in_n["primary_delta_fraction"] is not None
                prim_delta_val = (
                    in_n["primary_delta_fraction"]
                    if in_n["primary_delta_fraction"] is not None
                    else 0.0
                )
                unatt_in = in_n["unattributed_fraction"]
                in_hi_unc = in_n["high_uncertainty"]

            if ord_idx == planned_b - 1:
                out_deg_p, out_deg_s = 0, 0
                out_role = "final_batch"
                unatt_out = 0.0
                out_hi_unc = False
            elif out_n is None:
                out_deg_p, out_deg_s = 0, 0
                out_role = "gap_interrupted"
                unatt_out = 0.0
                out_hi_unc = False
            else:
                out_deg_p = out_n["out_degree_primary"]
                out_deg_s = out_n["out_degree_secondary"]
                out_role = out_n["node_role"]
                unatt_out = out_n["unattributed_fraction"]
                out_hi_unc = out_n["high_uncertainty"]

            nodes_out.append(
                {
                    "ordinal": ord_idx,
                    "cluster_id": cid,
                    "canonical_rank": b_info["canonical_rank"][cid],
                    "branch_id": branch_id_map[(ord_idx, cid)],
                    "mean_features": [normalize_neg_zero(float(x)) for x in sc["mean_features"]],
                    "member_count": int(sc["member_count"]),
                    "member_fraction": mf,
                    "assignment_reason": assignment_reason_map[(ord_idx, cid)],
                    "in_degree_primary": in_deg_p,
                    "in_degree_secondary": in_deg_s,
                    "out_degree_primary": out_deg_p,
                    "out_degree_secondary": out_deg_s,
                    "incoming_role": in_role,
                    "outgoing_role": out_role,
                    "has_primary_delta_fraction": has_prim_delta,
                    "primary_delta_fraction": prim_delta_val,
                    "unattributed_inflow_fraction": unatt_in,
                    "unattributed_outflow_fraction": unatt_out,
                    "high_uncertainty": in_hi_unc or out_hi_unc,
                }
            )

    # Assemble transitions, edges, and events across all B - 1 pairs
    transitions_out: list[dict[str, Any]] = []
    edges_chain_out: list[dict[str, Any]] = []
    events_chain_out: list[dict[str, Any]] = []

    for prev_ord in range(planned_b - 1):
        curr_ord = prev_ord + 1
        b_prev = resolved_batches[prev_ord]
        b_curr = resolved_batches[curr_ord]
        if prev_ord in pair_results:
            pres = pair_results[prev_ord]
            psum = pres["summary"]
            transitions_out.append(
                {
                    "prev_ordinal": prev_ord,
                    "curr_ordinal": curr_ord,
                    "transition_status": "ok",
                    "chain_broken": False,
                    "gap_reason": None,
                    "pair_scale": pres["pair_scale"],
                    "threshold": pres["threshold"],
                    "primary_edge_count": psum["primary_edge_count"],
                    "secondary_split_edge_count": psum["secondary_split_edge_count"],
                    "secondary_merge_edge_count": psum["secondary_merge_edge_count"],
                    "transition_tv_redistribution": psum["transition_tv_redistribution"],
                    "min_threshold_margin": psum["min_threshold_margin"],
                    "ambiguous_tie": psum["ambiguous_tie"],
                    "high_uncertainty": psum["high_uncertainty"],
                    "evolve_result": pres,
                }
            )
            for e in pres["edges"]:
                pcid = e["prev_cluster_id"]
                ccid = e["curr_cluster_id"]
                edges_chain_out.append(
                    {
                        "prev_ordinal": prev_ord,
                        "prev_cluster_id": pcid,
                        "curr_ordinal": curr_ord,
                        "curr_cluster_id": ccid,
                        "prev_branch_id": branch_id_map[(prev_ord, pcid)],
                        "curr_branch_id": branch_id_map[(curr_ord, ccid)],
                        **e,
                        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                    }
                )
            for ev in pres["events"]:
                ev_branches = sorted(
                    {
                        *(branch_id_map[(prev_ord, pc)] for pc in ev["prev_cluster_ids"]),
                        *(branch_id_map[(curr_ord, cc)] for cc in ev["curr_cluster_ids"]),
                    }
                )
                prim_bid = (
                    branch_id_map[(prev_ord, ev["primary_link"][0])]
                    if ev["primary_link"] is not None
                    else None
                )
                events_chain_out.append(
                    {
                        "prev_ordinal": prev_ord,
                        **ev,
                        "branch_ids": ev_branches,
                        "primary_branch_id": prim_bid,
                        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                    }
                )
        else:
            prev_ok = b_prev["status"] == "ok"
            curr_ok = b_curr["status"] == "ok"
            if not prev_ok and not curr_ok:
                gap_reason = "both_unavailable"
            elif not prev_ok:
                gap_reason = "prev_unavailable"
            else:
                gap_reason = "curr_unavailable"

            transitions_out.append(
                {
                    "prev_ordinal": prev_ord,
                    "curr_ordinal": curr_ord,
                    "transition_status": "gap",
                    "chain_broken": True,
                    "gap_reason": gap_reason,
                    "pair_scale": None,
                    "threshold": None,
                    "primary_edge_count": 0,
                    "secondary_split_edge_count": 0,
                    "secondary_merge_edge_count": 0,
                    "transition_tv_redistribution": None,
                    "min_threshold_margin": None,
                    "ambiguous_tie": False,
                    "high_uncertainty": False,
                    "evolve_result": None,
                }
            )
            ev_counter = 0
            if prev_ok:
                prev_by_rank = sorted(
                    b_prev["scenarios"],
                    key=lambda s: b_prev["canonical_rank"][int(s["cluster_id"])],
                )
                for sc in prev_by_rank:
                    cid = int(sc["cluster_id"])
                    mf = normalize_neg_zero(float(sc["member_fraction"]))
                    bid = branch_id_map[(prev_ord, cid)]
                    events_chain_out.append(
                        {
                            "prev_ordinal": prev_ord,
                            "event_index": ev_counter,
                            "event_type": "gap_interrupted",
                            "prev_cluster_ids": [cid],
                            "curr_cluster_ids": [],
                            "primary_link": None,
                            "branch_ids": [bid],
                            "primary_branch_id": None,
                            "rule_basis": "next_planned_batch_unavailable",
                            "max_morphology_distance": 0.0,
                            "max_morphology_over_scale": 0.0,
                            "prev_fraction_sum": mf,
                            "curr_fraction_sum": 0.0,
                            "net_delta_fraction": normalize_neg_zero(-mf),
                            "attributed_flow_sum": 0.0,
                            "min_margin_ratio": None,
                            "ambiguous_tie": False,
                            "high_uncertainty": False,
                            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                        }
                    )
                    ev_counter += 1
            if curr_ok:
                curr_by_rank = sorted(
                    b_curr["scenarios"],
                    key=lambda s: b_curr["canonical_rank"][int(s["cluster_id"])],
                )
                for sc in curr_by_rank:
                    cid = int(sc["cluster_id"])
                    mf = normalize_neg_zero(float(sc["member_fraction"]))
                    bid = branch_id_map[(curr_ord, cid)]
                    events_chain_out.append(
                        {
                            "prev_ordinal": prev_ord,
                            "event_index": ev_counter,
                            "event_type": "gap_restarted",
                            "prev_cluster_ids": [],
                            "curr_cluster_ids": [cid],
                            "primary_link": None,
                            "branch_ids": [bid],
                            "primary_branch_id": None,
                            "rule_basis": "previous_planned_batch_unavailable",
                            "max_morphology_distance": 0.0,
                            "max_morphology_over_scale": 0.0,
                            "prev_fraction_sum": 0.0,
                            "curr_fraction_sum": mf,
                            "net_delta_fraction": mf,
                            "attributed_flow_sum": 0.0,
                            "min_margin_ratio": None,
                            "ambiguous_tie": False,
                            "high_uncertainty": False,
                            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
                        }
                    )
                    ev_counter += 1

    # Assemble EvolutionBranchSummary for every branch_id
    nodes_by_branch: dict[str, list[dict[str, Any]]] = {}
    for n in nodes_out:
        nodes_by_branch.setdefault(n["branch_id"], []).append(n)
    for b_nodes in nodes_by_branch.values():
        b_nodes.sort(key=lambda n: n["ordinal"])

    prim_edges_by_branch: dict[str, list[dict[str, Any]]] = {bid: [] for bid in nodes_by_branch}
    split_out_by_branch: dict[str, list[dict[str, Any]]] = {bid: [] for bid in nodes_by_branch}
    split_in_by_branch: dict[str, dict[str, Any]] = {}
    merge_in_by_branch: dict[str, list[dict[str, Any]]] = {bid: [] for bid in nodes_by_branch}
    merge_out_by_branch: dict[str, dict[str, Any]] = {}

    for e in edges_chain_out:
        er = e["edge_role"]
        if er == "primary":
            prim_edges_by_branch[e["prev_branch_id"]].append(e)
        elif er == "secondary_split":
            split_out_by_branch[e["prev_branch_id"]].append(e)
            split_in_by_branch[e["curr_branch_id"]] = e
        elif er == "secondary_merge":
            merge_in_by_branch[e["curr_branch_id"]].append(e)
            merge_out_by_branch[e["prev_branch_id"]] = e

    branch_summaries: list[dict[str, Any]] = []
    for bid in sorted(nodes_by_branch.keys()):
        b_nodes = nodes_by_branch[bid]
        b_prim = sorted(prim_edges_by_branch[bid], key=lambda e: e["prev_ordinal"])
        birth_n = b_nodes[0]
        end_n = b_nodes[-1]
        node_cnt = len(b_nodes)
        step_cnt = node_cnt - 1

        if end_n["ordinal"] == planned_b - 1:
            term_reason = "final_batch"
        elif resolved_batches[end_n["ordinal"] + 1]["status"] != "ok":
            term_reason = "gap_interrupted"
        elif bid in merge_out_by_branch:
            term_reason = "secondary_merge"
        else:
            term_reason = "disappearance"

        cum_dist = fsum_exact(e["distance"] for e in b_prim)
        cum_dist_scale = fsum_exact(
            e["distance_over_scale"] for e in b_prim if e["distance_over_scale"] is not None
        )
        max_step_dist = max((e["distance"] for e in b_prim), default=0.0)
        max_step_scale = max(
            (e["distance_over_scale"] for e in b_prim if e["distance_over_scale"] is not None),
            default=0.0,
        )
        e2e_dist = (
            0.0
            if step_cnt == 0
            else compute_weighted_rmse_exact(
                birth_n["mean_features"], end_n["mean_features"], weights
            )
        )

        frac_seq = [n["member_fraction"] for n in b_nodes]
        init_f = frac_seq[0]
        final_f = frac_seq[-1]
        net_df = normalize_neg_zero(final_f - init_f)
        cum_abs_df = fsum_exact(
            abs(frac_seq[t] - frac_seq[t - 1]) for t in range(1, len(frac_seq))
        )

        spawned_pairs = sorted(
            (e["prev_ordinal"], e["curr_branch_id"]) for e in split_out_by_branch[bid]
        )
        split_out_ords = {prev_ord for prev_ord, _ in spawned_pairs}
        spawned_ids = [c_bid for _, c_bid in spawned_pairs]
        split_parent_id = (
            split_in_by_branch[bid]["prev_branch_id"] if bid in split_in_by_branch else None
        )

        absorbed_pairs = sorted(
            (e["prev_ordinal"], e["prev_branch_id"]) for e in merge_in_by_branch[bid]
        )
        merge_in_ords = {prev_ord for prev_ord, _ in absorbed_pairs}
        absorbed_ids = [p_bid for _, p_bid in absorbed_pairs]
        merged_into_id = (
            merge_out_by_branch[bid]["curr_branch_id"] if bid in merge_out_by_branch else None
        )

        ambig_steps = sum(1 for e in b_prim if e["ambiguous_tie"])
        hi_unc_steps = sum(1 for e in b_prim if e["high_uncertainty"])
        hi_unc_ratio = (
            _checked_ratio(float(hi_unc_steps), float(step_cnt), "branch.high_uncertainty_step_ratio")
            if step_cnt > 0
            else 0.0
        )

        branch_summaries.append(
            {
                "branch_id": bid,
                "birth_ordinal": birth_n["ordinal"],
                "end_ordinal": end_n["ordinal"],
                "node_count": node_cnt,
                "step_count": step_cnt,
                "birth_reason": birth_reason_map[bid],
                "termination_reason": term_reason,
                "cumulative_morphology_distance": cum_dist,
                "cumulative_morphology_over_scale": cum_dist_scale,
                "max_step_morphology_distance": max_step_dist,
                "max_step_morphology_over_scale": max_step_scale,
                "end_to_end_morphology_distance": e2e_dist,
                "initial_member_fraction": init_f,
                "final_member_fraction": final_f,
                "min_member_fraction": min(frac_seq),
                "max_member_fraction": max(frac_seq),
                "net_delta_fraction": net_df,
                "cumulative_abs_delta_fraction": cum_abs_df,
                "split_out_event_count": len(split_out_ords),
                "spawned_secondary_branch_ids": spawned_ids,
                "split_from_parent_branch_id": split_parent_id,
                "merge_in_event_count": len(merge_in_ords),
                "absorbed_secondary_branch_ids": absorbed_ids,
                "merged_into_branch_id": merged_into_id,
                "ambiguous_step_count": ambig_steps,
                "high_uncertainty_step_count": hi_unc_steps,
                "high_uncertainty_step_ratio": hi_unc_ratio,
            }
        )

    # Assemble EvolutionChainSummary
    ok_b_cnt = sum(1 for b in resolved_batches if b["status"] == "ok")
    miss_b_cnt = sum(1 for b in resolved_batches if b["status"] == "missing")
    fail_b_cnt = sum(1 for b in resolved_batches if b["status"] == "failed")
    ok_trs = [tr for tr in transitions_out if tr["transition_status"] == "ok"]
    gap_trs = [tr for tr in transitions_out if tr["transition_status"] == "gap"]

    prim_cnt = sum(1 for e in edges_chain_out if e["edge_role"] == "primary")
    sec_sp_cnt = sum(1 for e in edges_chain_out if e["edge_role"] == "secondary_split")
    sec_mg_cnt = sum(1 for e in edges_chain_out if e["edge_role"] == "secondary_merge")

    cum_prim_dist = fsum_exact(
        e["distance"] for e in edges_chain_out if e["edge_role"] == "primary"
    )
    max_edge_dist = max((e["distance"] for e in edges_chain_out), default=0.0)
    max_edge_scale = max(
        (e["distance_over_scale"] for e in edges_chain_out if e["distance_over_scale"] is not None),
        default=0.0,
    )
    tv_list = [tr["transition_tv_redistribution"] for tr in ok_trs]
    mean_tv = (
        _checked_ratio(
            fsum_exact(tv_list),
            float(len(tv_list)),
            "chain.mean_transition_tv_redistribution",
        )
        if tv_list
        else 0.0
    )
    max_tv = max(tv_list, default=0.0)

    ambig_tr_cnt = sum(1 for tr in ok_trs if tr["ambiguous_tie"])
    hi_unc_tr_cnt = sum(1 for tr in ok_trs if tr["high_uncertainty"])
    hi_unc_tr_ratio = (
        _checked_ratio(
            float(hi_unc_tr_cnt),
            float(len(ok_trs)),
            "chain.high_uncertainty_transition_ratio",
        )
        if ok_trs
        else 0.0
    )
    hi_unc_e_cnt = sum(1 for e in edges_chain_out if e["high_uncertainty"])
    hi_unc_e_ratio = (
        _checked_ratio(
            float(hi_unc_e_cnt),
            float(len(edges_chain_out)),
            "chain.high_uncertainty_edge_ratio",
        )
        if edges_chain_out
        else 0.0
    )

    chain_summary = {
        "ok_batch_count": ok_b_cnt,
        "missing_batch_count": miss_b_cnt,
        "failed_batch_count": fail_b_cnt,
        "ok_transition_count": len(ok_trs),
        "gap_transition_count": len(gap_trs),
        "branch_count": len(branch_summaries),
        "primary_edge_count": prim_cnt,
        "secondary_split_edge_count": sec_sp_cnt,
        "secondary_merge_edge_count": sec_mg_cnt,
        "continuation_count": sum(1 for ev in events_chain_out if ev["event_type"] == "continuation"),
        "split_count": sum(1 for ev in events_chain_out if ev["event_type"] == "split"),
        "merge_count": sum(1 for ev in events_chain_out if ev["event_type"] == "merge"),
        "emergence_count": sum(1 for ev in events_chain_out if ev["event_type"] == "emergence"),
        "disappearance_count": sum(1 for ev in events_chain_out if ev["event_type"] == "disappearance"),
        "gap_interrupted_count": sum(1 for ev in events_chain_out if ev["event_type"] == "gap_interrupted"),
        "gap_restarted_count": sum(1 for ev in events_chain_out if ev["event_type"] == "gap_restarted"),
        "cumulative_primary_distance": cum_prim_dist,
        "max_edge_distance": max_edge_dist,
        "max_edge_distance_over_scale": max_edge_scale,
        "mean_transition_tv_redistribution": mean_tv,
        "max_transition_tv_redistribution": max_tv,
        "ambiguous_transition_count": ambig_tr_cnt,
        "high_uncertainty_transition_count": hi_unc_tr_cnt,
        "high_uncertainty_transition_ratio": hi_unc_tr_ratio,
        "high_uncertainty_edge_count": hi_unc_e_cnt,
        "high_uncertainty_edge_ratio": hi_unc_e_ratio,
    }

    chain_out = normalize_floats(
        {
            "algorithm_version": EVOLUTION_ALGORITHM_VERSION,
            "secondary_rule": SECONDARY_RULE_V1,
            "flow_rule": FLOW_RULE_V1,
            "uncertainty_rule": UNCERTAINTY_RULE_V1,
            "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
            "tracking_run_key": run_key,
            "planned_batch_count": planned_b,
            "batches": [
                {
                    "ordinal": b["ordinal"],
                    "status": b["status"],
                    "status_reason": b["status_reason"],
                    "dispersion": b["dispersion"],
                }
                for b in resolved_batches
            ],
            "nodes": nodes_out,
            "transitions": transitions_out,
            "edges": edges_chain_out,
            "events": events_chain_out,
            "branch_summaries": branch_summaries,
            "chain_summary": chain_summary,
        }
    )

    inv = verify_chain_invariants(chain_out)
    assert inv["all_passed"], f"Chain invariant failure: {inv}"
    return chain_out


def overlap_cluster_mapping(
    base_partitions: dict[int, set[str]],
    perturbed_partitions: dict[int, set[str]],
) -> tuple[dict[int, int], bool]:
    """Map perturbed cluster_ids -> baseline cluster_ids by max member overlap; detect ties."""
    base_ids = sorted(base_partitions.keys())
    pert_ids = sorted(perturbed_partitions.keys())
    if len(base_ids) != len(pert_ids):
        raise ValueError(
            f"Cannot map different cluster counts: {len(base_ids)} vs {len(pert_ids)}"
        )
    scored = [
        (
            sum(len(perturbed_partitions[p] & base_partitions[b]) for p, b in zip(pert_ids, perm)),
            perm,
        )
        for perm in itertools.permutations(base_ids)
    ]
    best_score = max(score for score, _ in scored)
    winners = [perm for score, perm in scored if score == best_score]
    return dict(zip(pert_ids, winners[0])), (len(winners) > 1)


def _float_ulp_pad(val: float, multiplier: int = 8) -> Fraction:
    """Return conservative rational envelope covering binary64 rounding error."""
    if not math.isfinite(val):
        raise ValueError(f"Non-finite value in ULP pad: {val}")
    ulp = math.ulp(abs(val)) if val != 0.0 else math.ulp(1.0)
    return Fraction.from_float(ulp) * multiplier


def evaluate_evolution_stability_certificate(
    evolution_result: dict[str, Any] | None = None,
    *,
    epsilon_abs: float,
    threshold_coefficient: float | None = None,
    delta_s: float = 0.0,
    evaluation_mapping_ambiguous: bool = False,
    batch_failed: bool = False,
    prev_scenarios: Sequence[dict[str, Any]] | None = None,
    curr_scenarios: Sequence[dict[str, Any]] | None = None,
    weights: Sequence[float] | None = None,
    # Direct scalar overrides when evolution_result is not supplied
    threshold: float | None = None,
    feature_scale: float = 0.0,
    active_threshold_margin: float | None = None,
    optimality_margin: float | None = None,
    q_star: int | None = None,
    has_same_count_alt: bool | None = None,
    optimal_solution_count: int | None = None,
    secondary_margin: float | None = None,
    has_multi_candidate_secondary: bool | None = None,
    any_secondary_tie: bool = False,
    secondary_edge_count: int | None = None,
    margin_scale: float = 1.0,
    sqrt_bits: int = 128,
) -> dict[str, Any]:
    """Evaluate 4-condition outward-rounded Fraction Evolution Stability Certificate.

    Implements `contracts/numerics.md` §4 (`m3.certificate.v1`):
      Cond 1: For all (i, j) in E_evol U {(i, j) : d_{i,j} > tau},
              lower(|d_{i,j} - tau|) > upper(2 * eps_abs + c * delta_s)
      Cond 2: For all `SameCount` primary edges e in E_primary,
              lower(Delta_Sigma_e) > upper(4 * q* * eps_abs)
              (vacuously True when q* == 0 or all primary edges are `CountDrop` with unique optimum)
      Cond 3: optimal_solution_count == 1
      Cond 4: !any_secondary_tie, and for all u in U^comp with |F(u)| >= 2,
              lower(d_{(2)}(u) - d_{(1)}(u)) > upper(4 * eps_abs)

    Note: Pure scalar-override callers (`evolution_result=None` without `prev_scenarios` /
    `curr_scenarios`) must supply `margin_scale` or `feature_scale >= max(|features|, |tau|, 1.0)`
    so outward binary64 ULP padding covers coordinate-scale rounding error.
    """
    if evaluation_mapping_ambiguous:
        return {
            "schema_version": CERTIFICATE_RULE_V1,
            "certified": False,
            "status": "evaluation_mapping_ambiguous",
            "reason": "evaluation_mapping_ambiguous",
            "evaluation_mapping_ambiguous": True,
            "batch_failed": batch_failed,
            "cond1_satisfied": False,
            "cond2_satisfied": False,
            "cond3_satisfied": False,
            "cond4_satisfied": False,
        }

    if batch_failed:
        return {
            "schema_version": CERTIFICATE_RULE_V1,
            "certified": False,
            "status": "batch_failed",
            "reason": "batch_failed",
            "evaluation_mapping_ambiguous": False,
            "batch_failed": True,
            "cond1_satisfied": False,
            "cond2_satisfied": False,
            "cond3_satisfied": False,
            "cond4_satisfied": False,
        }

    # Extract metrics from evolution_result if provided
    if evolution_result is not None:
        tau = float(evolution_result["threshold"])
        pair_scale = evolution_result.get("pair_scale")
        c_val = (
            float(threshold_coefficient)
            if threshold_coefficient is not None
            else float(evolution_result.get("threshold_coefficient") or 0.0)
        )
        summary = evolution_result["summary"]
        backbone = evolution_result.get("backbone", {})
        edges = evolution_result["edges"]

        if active_threshold_margin is None:
            active_threshold_margin = float(summary["min_threshold_margin"])
        if q_star is None:
            q_star = int(summary["primary_edge_count"])
        if optimal_solution_count is None:
            opt_cnt_raw = backbone.get("optima_count")
            if opt_cnt_raw is None:
                opt_cnt_raw = backbone.get("optimal_solution_count", 1)
            optimal_solution_count = int(opt_cnt_raw)

        prim_edges = [e for e in edges if e["edge_role"] == "primary"]
        sec_edges = [e for e in edges if e["edge_role"] != "primary"]
        secondary_edge_count = len(sec_edges)

        same_count_gaps: list[float] = []
        for pe in prim_edges:
            st = pe["primary_alternative_status"]
            if st == "tied":
                same_count_gaps.append(0.0)
            elif st == "same_count" and pe["optimality_margin"] is not None:
                same_count_gaps.append(float(pe["optimality_margin"]))

        if has_same_count_alt is None:
            has_same_count_alt = len(same_count_gaps) > 0
        if optimality_margin is None:
            optimality_margin = min(same_count_gaps) if same_count_gaps else 0.0

        any_sec_tie_detected = any(
            (se["secondary_margin"] is not None and float(se["secondary_margin"]) == 0.0)
            or bool(se["ambiguous_tie"])
            for se in sec_edges
        )
        any_secondary_tie = any_secondary_tie or any_sec_tie_detected

        sec_comp_gaps = [
            float(se["secondary_margin"])
            for se in sec_edges
            if se["secondary_margin"] is not None
        ]
        if has_multi_candidate_secondary is None:
            has_multi_candidate_secondary = len(sec_comp_gaps) > 0
        if secondary_margin is None:
            secondary_margin = min(sec_comp_gaps) if sec_comp_gaps else 0.0

        margin_scale = max(
            abs(float(margin_scale)),
            abs(tau),
            abs(float(pair_scale)) if pair_scale is not None else 0.0,
            1.0,
        )
    else:
        c_val = float(threshold_coefficient) if threshold_coefficient is not None else 0.0
        if active_threshold_margin is None:
            raise ValueError("active_threshold_margin is required when evolution_result is None")
        if q_star is None:
            raise ValueError("q_star is required when evolution_result is None")
        if optimal_solution_count is None:
            optimal_solution_count = 1
        if has_same_count_alt is None:
            has_same_count_alt = q_star > 0 and optimality_margin is not None
        if optimality_margin is None:
            optimality_margin = 0.0
        if secondary_edge_count is None:
            secondary_edge_count = (
                1 if (any_secondary_tie or bool(has_multi_candidate_secondary) or secondary_margin is not None) else 0
            )
        if has_multi_candidate_secondary is None:
            has_multi_candidate_secondary = secondary_margin is not None
        if secondary_margin is None:
            secondary_margin = 0.0

    numeric_checks: list[Any] = [
        epsilon_abs,
        c_val,
        delta_s,
        active_threshold_margin,
        optimality_margin,
        secondary_margin,
        margin_scale,
        feature_scale,
    ]
    if threshold is not None:
        numeric_checks.append(threshold)
    if (
        not all(is_finite_float_like(v) for v in numeric_checks)
        or float(epsilon_abs) < 0.0
        or float(c_val) < 0.0
        or float(delta_s) < 0.0
        or float(feature_scale) < 0.0
        or (threshold is not None and float(threshold) < 0.0)
        or int(q_star) < 0
        or int(optimal_solution_count) < 1
    ):
        return {
            "schema_version": CERTIFICATE_RULE_V1,
            "certified": False,
            "status": "invalid_or_non_finite_bound",
            "reason": "non_finite_or_negative_bound",
            "evaluation_mapping_ambiguous": False,
            "batch_failed": False,
            "cond1_satisfied": False,
            "cond2_satisfied": False,
            "cond3_satisfied": False,
            "cond4_satisfied": False,
        }

    scen_feat_scale = 0.0
    for sc_seq in (prev_scenarios, curr_scenarios):
        if sc_seq is not None:
            for sc in sc_seq:
                for x in sc.get("mean_features", ()):
                    if not is_finite_float_like(x):
                        return {
                            "schema_version": CERTIFICATE_RULE_V1,
                            "certified": False,
                            "status": "invalid_or_non_finite_bound",
                            "reason": "non_finite_or_negative_bound",
                            "evaluation_mapping_ambiguous": False,
                            "batch_failed": False,
                            "cond1_satisfied": False,
                            "cond2_satisfied": False,
                            "cond3_satisfied": False,
                            "cond4_satisfied": False,
                        }
                    scen_feat_scale = max(scen_feat_scale, abs(float(x)))

    eps_f = float(epsilon_abs)
    ds_f = float(delta_s)
    thr_m_f = float(active_threshold_margin)
    opt_m_f = float(optimality_margin)
    sec_m_f = float(secondary_margin)
    q_int = int(q_star)
    opt_sol_cnt = int(optimal_solution_count)
    tau_scale = (
        abs(float(evolution_result["threshold"]))
        if evolution_result is not None
        else abs(float(threshold or 0.0))
    )

    pad_scale = max(
        abs(float(margin_scale)),
        abs(float(feature_scale)),
        scen_feat_scale,
        tau_scale,
        abs(thr_m_f),
        abs(opt_m_f),
        abs(sec_m_f),
        abs(eps_f),
        1.0,
    )

    # Condition 1: Active threshold margin over E_evol U {d > tau} > 2 * eps_abs + c * delta_s
    cond1_pad = _float_ulp_pad(pad_scale, multiplier=16)
    lhs1_lo = Fraction.from_float(thr_m_f) - cond1_pad

    # If raw scenario features and weights are supplied, ALSO tighten lhs1_lo
    # using conservative rational `isqrt` square-root intervals on true real weighted RMSEs.
    if (
        evolution_result is not None
        and prev_scenarios is not None
        and curr_scenarios is not None
        and weights is not None
    ):
        tau_f = float(evolution_result["threshold"])
        tau_q = Fraction.from_float(tau_f)
        prev_ids = [int(cid) for cid in evolution_result["prev_cluster_ids"]]
        curr_ids = [int(cid) for cid in evolution_result["curr_cluster_ids"]]
        p_id_map = {cid: i for i, cid in enumerate(prev_ids)}
        c_id_map = {cid: j for j, cid in enumerate(curr_ids)}
        p_sc_by_id = {int(sc["cluster_id"]): sc for sc in prev_scenarios}
        c_sc_by_id = {int(sc["cluster_id"]): sc for sc in curr_scenarios}
        evol_pairs = {
            (p_id_map[int(e["prev_cluster_id"])], c_id_map[int(e["curr_cluster_id"])])
            for e in evolution_result["edges"]
        }
        dist_mat = evolution_result["backbone"]["distance_matrix"]
        elig_mat = evolution_result["backbone"].get("eligible_matrix")
        if elig_mat is None:
            elig_mat = [[float(d) <= tau_f for d in row] for row in dist_mat]

        rational_cell_lowers: list[Fraction] = []
        for i, p_cid in enumerate(prev_ids):
            p_sc = p_sc_by_id[p_cid]
            for j, c_cid in enumerate(curr_ids):
                c_sc = c_sc_by_id[c_cid]
                if (i, j) in evol_pairs or not elig_mat[i][j]:
                    d_lo, d_hi = compute_weighted_rmse_rational_bounds(
                        p_sc["mean_features"],
                        c_sc["mean_features"],
                        weights,
                        bits=sqrt_bits,
                    )
                    if (i, j) in evol_pairs:
                        rational_cell_lowers.append(tau_q - d_hi - cond1_pad)
                    else:
                        rational_cell_lowers.append(d_lo - tau_q - cond1_pad)
        if rational_cell_lowers:
            lhs1_lo = min(lhs1_lo, min(rational_cell_lowers))
    elif (
        evolution_result is None
        and prev_scenarios is not None
        and curr_scenarios is not None
        and weights is not None
        and threshold is not None
    ):
        tau_q = Fraction.from_float(float(threshold))
        tau_iv = RationalInterval(tau_q, tau_q)
        rational_cell_lowers = []
        for p_sc in prev_scenarios:
            for c_sc in curr_scenarios:
                d_lo, d_hi = compute_weighted_rmse_rational_bounds(
                    p_sc["mean_features"],
                    c_sc["mean_features"],
                    weights,
                    bits=sqrt_bits,
                )
                abs_diff_lo = (RationalInterval(d_lo, d_hi) - tau_iv).abs_interval().lo
                rational_cell_lowers.append(abs_diff_lo - cond1_pad)
        if rational_cell_lowers:
            lhs1_lo = min(lhs1_lo, min(rational_cell_lowers))

    eps_iv = RationalInterval.exact_float(eps_f)
    c_iv = RationalInterval.exact_float(c_val)
    ds_iv = RationalInterval.exact_float(ds_f)
    rhs1_iv = RationalInterval(Fraction(2), Fraction(2)) * eps_iv + (c_iv * ds_iv)
    rhs1_hi = rhs1_iv.hi + cond1_pad
    cond1 = lhs1_lo > rhs1_hi

    # Condition 2: SameCount primary optimality margin > 4 * q* * eps_abs
    if q_int == 0:
        cond2 = True
        cond2_status = "empty_matching_q_zero"
        lhs2_lo = Fraction(0)
        rhs2_hi = Fraction(0)
    elif not has_same_count_alt:
        if opt_sol_cnt > 1:
            cond2 = False
            cond2_status = "tied_optimum_present"
            lhs2_lo = Fraction(0)
            rhs2_hi = Fraction(4 * q_int) * eps_iv.hi
        else:
            cond2 = True
            cond2_status = "count_drop_only"
            lhs2_lo = Fraction(0)
            rhs2_hi = Fraction(0)
    else:
        cond2_pad = _float_ulp_pad(pad_scale, multiplier=max(16, 8 * q_int))
        lhs2_lo = Fraction.from_float(opt_m_f) - cond2_pad
        rhs2_hi = (RationalInterval(Fraction(4 * q_int), Fraction(4 * q_int)) * eps_iv).hi + cond2_pad
        cond2 = lhs2_lo > rhs2_hi
        cond2_status = "evaluated"

    # Condition 3: Unique global primary optimum
    cond3 = opt_sol_cnt == 1

    # Condition 4: No secondary tie, and for all |F(u)| >= 2: d_{(2)}(u) - d_{(1)}(u) > 4 * eps_abs
    if any_secondary_tie:
        cond4 = False
        cond4_status = "secondary_tie_present"
        lhs4_lo = Fraction(0)
        rhs4_hi = (RationalInterval(Fraction(4), Fraction(4)) * eps_iv).hi
    elif int(secondary_edge_count or 0) == 0:
        cond4 = True
        cond4_status = "no_secondary_edges"
        lhs4_lo = Fraction(0)
        rhs4_hi = Fraction(0)
    elif not has_multi_candidate_secondary:
        cond4 = True
        cond4_status = "single_candidate_only"
        lhs4_lo = Fraction(0)
        rhs4_hi = Fraction(0)
    else:
        cond4_pad = _float_ulp_pad(pad_scale, multiplier=16)
        lhs4_lo = Fraction.from_float(sec_m_f) - cond4_pad
        if (
            evolution_result is not None
            and prev_scenarios is not None
            and curr_scenarios is not None
            and weights is not None
        ):
            tau_f = float(evolution_result["threshold"])
            prev_ids = [int(cid) for cid in evolution_result["prev_cluster_ids"]]
            curr_ids = [int(cid) for cid in evolution_result["curr_cluster_ids"]]
            p_id_map = {cid: i for i, cid in enumerate(prev_ids)}
            c_id_map = {cid: j for j, cid in enumerate(curr_ids)}
            p_sc_by_id = {int(sc["cluster_id"]): sc for sc in prev_scenarios}
            c_sc_by_id = {int(sc["cluster_id"]): sc for sc in curr_scenarios}
            dist_mat = evolution_result["backbone"]["distance_matrix"]
            elig_mat = evolution_result["backbone"].get("eligible_matrix")
            if elig_mat is None:
                elig_mat = [[float(d) <= tau_f for d in row] for row in dist_mat]
            sec_lowers: list[Fraction] = []
            for se in evolution_result["edges"]:
                if se["edge_role"] == "secondary_split" and se["secondary_margin"] is not None:
                    best_p_cid = int(se["prev_cluster_id"])
                    c_cid = int(se["curr_cluster_id"])
                    best_i = p_id_map[best_p_cid]
                    j = c_id_map[c_cid]
                    _, d1_hi = compute_weighted_rmse_rational_bounds(
                        p_sc_by_id[best_p_cid]["mean_features"],
                        c_sc_by_id[c_cid]["mean_features"],
                        weights,
                        bits=sqrt_bits,
                    )
                    for i, other_p_cid in enumerate(prev_ids):
                        if i != best_i and elig_mat[i][j]:
                            d2_lo, _ = compute_weighted_rmse_rational_bounds(
                                p_sc_by_id[other_p_cid]["mean_features"],
                                c_sc_by_id[c_cid]["mean_features"],
                                weights,
                                bits=sqrt_bits,
                            )
                            sec_lowers.append(d2_lo - d1_hi - cond4_pad)
                elif se["edge_role"] == "secondary_merge" and se["secondary_margin"] is not None:
                    p_cid = int(se["prev_cluster_id"])
                    best_c_cid = int(se["curr_cluster_id"])
                    i = p_id_map[p_cid]
                    best_j = c_id_map[best_c_cid]
                    _, d1_hi = compute_weighted_rmse_rational_bounds(
                        p_sc_by_id[p_cid]["mean_features"],
                        c_sc_by_id[best_c_cid]["mean_features"],
                        weights,
                        bits=sqrt_bits,
                    )
                    for j, other_c_cid in enumerate(curr_ids):
                        if j != best_j and elig_mat[i][j]:
                            d2_lo, _ = compute_weighted_rmse_rational_bounds(
                                p_sc_by_id[p_cid]["mean_features"],
                                c_sc_by_id[other_c_cid]["mean_features"],
                                weights,
                                bits=sqrt_bits,
                            )
                            sec_lowers.append(d2_lo - d1_hi - cond4_pad)
            if sec_lowers:
                lhs4_lo = min(lhs4_lo, min(sec_lowers))

        rhs4_hi = (RationalInterval(Fraction(4), Fraction(4)) * eps_iv).hi + cond4_pad
        cond4 = lhs4_lo > rhs4_hi
        cond4_status = "evaluated"

    certified = cond1 and cond2 and cond3 and cond4
    failed_conditions = [
        name
        for ok, name in (
            (cond1, "cond1_active_threshold_margin"),
            (cond2, "cond2_primary_same_count_margin"),
            (cond3, "cond3_unique_primary_optimum"),
            (cond4, "cond4_secondary_nearest_margin"),
        )
        if not ok
    ]

    return normalize_floats(
        {
            "schema_version": CERTIFICATE_RULE_V1,
            "certified": certified,
            "status": "certified" if certified else "near_boundary_or_tied",
            "reason": "all_four_conditions_satisfied" if certified else ",".join(failed_conditions),
            "failed_conditions": failed_conditions,
            "evaluation_mapping_ambiguous": False,
            "batch_failed": False,
            "cond1_satisfied": cond1,
            "cond1_margin": thr_m_f,
            "cond1_lhs_lower": float(lhs1_lo),
            "cond1_bound": float(rhs1_hi),
            "cond2_satisfied": cond2,
            "cond2_status": cond2_status,
            "cond2_margin": opt_m_f,
            "cond2_lhs_lower": float(lhs2_lo),
            "cond2_bound": float(rhs2_hi),
            "cond3_satisfied": cond3,
            "optimal_solution_count": opt_sol_cnt,
            "cond4_satisfied": cond4,
            "cond4_status": cond4_status,
            "cond4_margin": sec_m_f,
            "cond4_lhs_lower": float(lhs4_lo),
            "cond4_bound": float(rhs4_hi),
            "q_star": q_int,
            "has_same_count_alt": bool(has_same_count_alt),
            "has_multi_candidate_secondary": bool(has_multi_candidate_secondary),
            "any_secondary_tie": bool(any_secondary_tie),
        }
    )


def _random_member_partition(k: int, total_members: int, rnd: random.Random) -> list[tuple[int, float]]:
    """Sample k positive member_counts summing to total_members with exact /total_members fractions."""
    cuts = sorted(rnd.sample(range(1, total_members), k - 1)) if k > 1 else []
    bounds = [0, *cuts, total_members]
    counts = [bounds[idx + 1] - bounds[idx] for idx in range(k)]
    return [(cnt, cnt / float(total_members)) for cnt in counts]


def generate_random_evolution_cases(
    count: int = 4000,
    seed: int = 20261125,
) -> list[dict[str, Any]]:
    """Generate deterministic random evolution test cases (`m, n in 1..=6`) with fixed seed."""
    rnd = random.Random(seed)
    cases: list[dict[str, Any]] = []
    for c_idx in range(count):
        m = rnd.randint(1, 6)
        n = rnd.randint(1, 6)
        n_features = rnd.choice([1, 2, 3, 9])
        raw_w = [round(rnd.uniform(0.1, 2.0), 4) for _ in range(n_features)]
        w_sum = math.fsum(raw_w)
        weights = [w / w_sum for w in raw_w]

        prev_parts = _random_member_partition(m, 50, rnd)
        curr_parts = _random_member_partition(n, 50, rnd)

        prev_scens = [
            {
                "cluster_id": i,
                "member_count": prev_parts[i][0],
                "member_fraction": prev_parts[i][1],
                "mean_features": [round(rnd.uniform(250.0, 310.0), 3) for _ in range(n_features)],
            }
            for i in range(m)
        ]
        curr_scens = [
            {
                "cluster_id": j,
                "member_count": curr_parts[j][0],
                "member_fraction": curr_parts[j][1],
                "mean_features": [round(rnd.uniform(250.0, 310.0), 3) for _ in range(n_features)],
            }
            for j in range(n)
        ]

        if c_idx % 2 == 0:
            s_prev = round(rnd.uniform(0.4, 8.0), 4)
            s_curr = round(rnd.uniform(0.4, 8.0), 4)
            c_coeff = round(rnd.uniform(0.25, 2.25), 3)
            cases.append(
                {
                    "case_id": f"m3_random_{c_idx:04d}",
                    "mode": "scaled",
                    "weights": weights,
                    "threshold_coefficient": c_coeff,
                    "prev_dispersion": s_prev,
                    "curr_dispersion": s_curr,
                    "prev_scenarios": prev_scens,
                    "curr_scenarios": curr_scens,
                }
            )
        else:
            tau = round(rnd.uniform(0.2, 18.0), 3)
            cases.append(
                {
                    "case_id": f"m3_random_{c_idx:04d}",
                    "mode": "absolute",
                    "weights": weights,
                    "threshold": tau,
                    "prev_scenarios": prev_scens,
                    "curr_scenarios": curr_scens,
                }
            )
    return cases


def _atomic_write_json_stdlib(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp_m3_oracle_", suffix=".json", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(normalize_floats(payload), handle, indent=2, allow_nan=False)
            handle.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--gen":
        out_file = (
            Path(sys.argv[2])
            if len(sys.argv) > 2
            else Path("validation/m3/.cache/random_evolution_cases.json")
        )
        generated = generate_random_evolution_cases(4000, seed=20261125)
        _atomic_write_json_stdlib(out_file, {"cases_count": len(generated), "cases": generated})
        print(f"Generated {len(generated)} random M3 evolution oracle cases to {out_file}")
    else:
        print("Usage: python3 validation/m3/oracle.py --gen [out_file]")
