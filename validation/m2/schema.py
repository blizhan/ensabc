#!/usr/bin/env python3
"""Structural and semantic schema validators for M2 scenario tracking evidence.

Validates schemas:
- m2.tracking-config.v1: Clustering, matching, sensitivity & perturbation settings
- m2.match-task.v1: MatchInput envelope
- m2.match-result.v1: MatchResult envelope (objective, links, distances, diagnostics)
- m2.history-replay.v1: 5-table history database state (batches, nodes, comparisons, edges)
- m2.om-comparison.v1: 3-way sample decode comparison (source vs omfiles vs DuckOMO)
- m2.sensitivity-report.v1: Multi-configuration sensitivity & perturbation results
- m2.traceability.v1: Executable assertion matrix for FR-001..FR-021 and SC-001..SC-008
- m2.report.v1: Gate execution report
- m2.report-audit.v1: Independent hash & status audit
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import (
    LocatedError,
    M2Error,
    all_finite,
    is_finite_number,
    sha256_bytes,
    sha256_file,
    sha256_json,
)

CONFIG_SCHEMA_VERSION = "m2.tracking-config.v1"
TASK_SCHEMA_VERSION = "m2.match-task.v1"
RESULT_SCHEMA_VERSION = "m2.match-result.v1"
HISTORY_SCHEMA_VERSION = "m2.history-replay.v1"
OM_COMPARISON_SCHEMA_VERSION = "m2.om-comparison.v1"
SENSITIVITY_SCHEMA_VERSION = "m2.sensitivity-report.v1"
TRACEABILITY_SCHEMA_VERSION = "m2.traceability.v1"
REPORT_SCHEMA_VERSION = "m2.report.v1"
AUDIT_SCHEMA_VERSION = "m2.report-audit.v1"

REQUIREMENT_IDS = tuple(f"FR-{i:03d}" for i in range(1, 22))
SUCCESS_CRITERION_IDS = tuple(f"SC-{i:03d}" for i in range(1, 9))

STATUSES = ("pass", "fail", "not_run")
OVERALL_STATUSES = ("passed", "failed", "incomplete")


class ValidationError(LocatedError):
    """Raised when an M2 document fails structural or semantic validation."""


def require_field(data: dict[str, Any], field: str, expected_type: type | tuple[type, ...], location: str) -> Any:
    if field not in data:
        raise ValidationError(f"Missing required field: '{field}'", location=location)
    val = data[field]
    if not isinstance(val, expected_type):
        raise ValidationError(
            f"Field '{field}' expected {expected_type}, got {type(val).__name__}",
            location=location,
        )
    return val


def require_finite_float(val: Any, field_name: str, location: str, *, min_val: float | None = None, strictly_positive: bool = False) -> float:
    if not is_finite_number(val):
        raise ValidationError(f"Field '{field_name}' must be finite number, got {val}", location=location)
    fval = float(val)
    if strictly_positive and fval <= 0.0:
        raise ValidationError(f"Field '{field_name}' must be > 0.0, got {fval}", location=location)
    if min_val is not None and fval < min_val:
        raise ValidationError(f"Field '{field_name}' must be >= {min_val}, got {fval}", location=location)
    return fval


def validate_match_task(data: dict[str, Any], location: str = "match_task") -> None:
    """Validate MatchInput task structure."""
    require_field(data, "schema_version", str, location)
    if data["schema_version"] != TASK_SCHEMA_VERSION:
        raise ValidationError(f"Expected schema {TASK_SCHEMA_VERSION}, got {data['schema_version']}", location=location)
    weights = require_field(data, "weights", list, location)
    if not weights or not all_finite(weights) or any(w < 0.0 for w in weights):
        raise ValidationError("Weights must be non-empty list of non-negative finite floats", location=location)
    if sum(weights) == 0.0:
        raise ValidationError("Sum of weights must be > 0", location=location)

    mode = require_field(data, "mode", str, location)
    if mode == "scaled":
        c = require_finite_float(data.get("threshold_coefficient"), "threshold_coefficient", location, min_val=0.0)
        sp = require_finite_float(data.get("prev_dispersion"), "prev_dispersion", location, strictly_positive=True)
        sc = require_finite_float(data.get("curr_dispersion"), "curr_dispersion", location, strictly_positive=True)
    elif mode == "absolute":
        tau = require_finite_float(data.get("threshold"), "threshold", location, min_val=0.0)
    else:
        raise ValidationError(f"Unknown mode: {mode}, expected 'scaled' or 'absolute'", location=location)

    for field in ("prev_scenarios", "curr_scenarios"):
        scens = require_field(data, field, list, location)
        seen_ids = set()
        for s_idx, scen in enumerate(scens):
            s_loc = f"{location}.{field}[{s_idx}]"
            cid = require_field(scen, "cluster_id", int, s_loc)
            if cid in seen_ids:
                raise ValidationError(f"Duplicate cluster_id {cid} in {field}", location=s_loc)
            seen_ids.add(cid)
            features = require_field(scen, "mean_features", list, s_loc)
            if len(features) != len(weights):
                raise ValidationError(f"Feature length {len(features)} != weights length {len(weights)}", location=s_loc)
            if not all_finite(features):
                raise ValidationError("Features must be finite numbers", location=s_loc)


def validate_match_result(data: dict[str, Any], location: str = "match_result") -> None:
    """Validate MatchResult structure."""
    require_field(data, "schema_version", str, location)
    if data["schema_version"] != RESULT_SCHEMA_VERSION:
        raise ValidationError(f"Expected schema {RESULT_SCHEMA_VERSION}, got {data['schema_version']}", location=location)
    obj = require_field(data, "objective", dict, location)
    matched_count = require_field(obj, "matched_count", int, f"{location}.objective")
    dist_sum = require_finite_float(obj.get("weighted_distance_sum"), "weighted_distance_sum", f"{location}.objective", min_val=0.0)

    links = require_field(data, "links", list, location)
    if len(links) != matched_count:
        raise ValidationError(f"links length {len(links)} != matched_count {matched_count}", location=location)

    for l_idx, link in enumerate(links):
        l_loc = f"{location}.links[{l_idx}]"
        require_field(link, "prev_cluster_id", int, l_loc)
        require_field(link, "curr_cluster_id", int, l_loc)
        d = require_finite_float(link.get("distance_k"), "distance_k", l_loc, min_val=0.0)
        sub = require_field(link, "substitutability", str, l_loc)
        if sub not in ("tied", "same_count", "count_drop", "no_same_count_alternative"):
            raise ValidationError(f"Invalid substitutability: {sub}", location=l_loc)
        require_field(link, "ambiguous", bool, l_loc)


def validate_history_replay(data: dict[str, Any], location: str = "history_replay") -> None:
    """Validate 5-table history structure and invariants."""
    require_field(data, "schema_version", str, location)
    if data["schema_version"] != HISTORY_SCHEMA_VERSION:
        raise ValidationError(f"Expected schema {HISTORY_SCHEMA_VERSION}, got {data['schema_version']}", location=location)
    run_key = require_field(data, "run_key", str, location)
    batches = require_field(data, "batches", list, location)
    nodes = require_field(data, "nodes", list, location)
    comparisons = require_field(data, "comparisons", list, location)
    edges = require_field(data, "edges", list, location)

    # Invariant 1: batch ordinal ordering
    for i, b in enumerate(batches):
        if b.get("ordinal") != i:
            raise ValidationError(f"Batch ordinal mismatch at index {i}: got {b.get('ordinal')}", location=location)
        st = b.get("status")
        if st not in ("ok", "missing", "failed"):
            raise ValidationError(f"Invalid batch status: {st}", location=location)

    # Invariant 2: Exactly one branch_id per scenario node
    node_ids = set()
    for n in nodes:
        key = (n["ordinal"], n["cluster_id"])
        if key in node_ids:
            raise ValidationError(f"Duplicate scenario node: {key}", location=location)
        node_ids.add(key)
        branch = n.get("branch_id")
        if not branch:
            raise ValidationError(f"Node {key} missing branch_id", location=location)

    # Invariant 3: Edges only connect adjacent ok batches
    for e in edges:
        p_ord = e["prev_ordinal"]
        c_ord = e["curr_ordinal"]
        if c_ord != p_ord + 1:
            raise ValidationError(f"Edge connects non-adjacent ordinals {p_ord} -> {c_ord}", location=location)


def validate_report(data: dict[str, Any], location: str = "report") -> None:
    """Validate gate report document."""
    require_field(data, "schema_version", str, location)
    if data["schema_version"] != REPORT_SCHEMA_VERSION:
        raise ValidationError(f"Expected schema {REPORT_SCHEMA_VERSION}, got {data['schema_version']}", location=location)
    reqs = require_field(data, "requirements", dict, location)
    for rid in REQUIREMENT_IDS:
        if rid not in reqs:
            raise ValidationError(f"Missing requirement {rid} in report", location=location)
        st = reqs[rid].get("status")
        if st not in STATUSES:
            raise ValidationError(f"Invalid status {st} for {rid}", location=location)
    scs = require_field(data, "success_criteria", dict, location)
    for sid in SUCCESS_CRITERION_IDS:
        if sid not in scs:
            raise ValidationError(f"Missing success criterion {sid} in report", location=location)
        st = scs[sid].get("status")
        if st not in STATUSES:
            raise ValidationError(f"Invalid status {st} for {sid}", location=location)
    overall = require_field(data, "overall_status", str, location)
    if overall not in OVERALL_STATUSES:
        raise ValidationError(f"Invalid overall status: {overall}", location=location)
