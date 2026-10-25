#!/usr/bin/env python3
"""Structural validators for M1 schema records and evidence artifacts.

Implements validation for:
- m1.config.v1 (ScenarioConfig envelope and calculation fields)
- m1.comparison.v1 (Comparison and tolerance rules)
- m1.task.v1 (TaskContext + MemberInput envelope)
- m1.result.v1 (ScenarioResult envelope, ClusterScenario, Diagnostics, RestartSummary)
- m1.alignment.v1 (Projection alignment matrix Q and subspace invariants)
- m1.trace.v1 (K-means Lloyd iteration trace records)
- m1.observer-check.v1 (Scikit-learn wrapped vs unwrapped kernel parity)
- m1.reproduction-child.v1 (Reproduction child subset record)
- m1.traceability.v1 (Executable traceability matrix for FR-001..FR-017 and SC-001..SC-008)
- m1.report.v1 (Gate report with 17 FRs, 8 SCs, refs, and overall verdict)
- m1.report-audit.v1 (Independent audit of report and evidence hashes)
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from typing import Any

from validation.m1.common import (
    M1Error,
    all_finite,
    is_finite_number,
    sha256_bytes,
    sha256_file,
    sha256_json,
)

CONFIG_SCHEMA_VERSION = "m1.config.v1"
COMPARISON_SCHEMA_VERSION = "m1.comparison.v1"
TASK_SCHEMA_VERSION = "m1.task.v1"
RESULT_SCHEMA_VERSION = "m1.result.v1"
ALIGNMENT_SCHEMA_VERSION = "m1.alignment.v1"
TRACE_SCHEMA_VERSION = "m1.trace.v1"
OBSERVER_CHECK_SCHEMA_VERSION = "m1.observer-check.v1"
REPRODUCTION_CHILD_SCHEMA_VERSION = "m1.reproduction-child.v1"
TRACEABILITY_SCHEMA_VERSION = "m1.traceability.v1"
REPORT_SCHEMA_VERSION = "m1.report.v1"
AUDIT_SCHEMA_VERSION = "m1.report-audit.v1"

REQUIREMENT_IDS = tuple(f"FR-{index:03d}" for index in range(1, 18))
SUCCESS_CRITERION_IDS = tuple(f"SC-{index:03d}" for index in range(1, 9))

STATUSES = ("pass", "fail", "not_run")
OVERALL_STATUSES = ("passed", "failed", "incomplete")


class ValidationError(M1Error):
    """Raised when an M1 document fails structural or semantic validation."""


def _require(condition: bool, message: str, *, location: str | None = None) -> None:
    if not condition:
        raise ValidationError(message, location=location, code="SCHEMA_VIOLATION")


def _require_keys(payload: dict[str, Any], keys: tuple[str, ...], context: str) -> None:
    missing = [k for k in keys if k not in payload]
    _require(not missing, f"{context}: missing required key(s) {missing}")


def _require_non_empty_string(payload: dict[str, Any], key: str, context: str) -> str:
    _require_keys(payload, (key,), context)
    val = payload[key]
    _require(isinstance(val, str) and bool(val.strip()), f"{context}: {key} must be a non-empty string")
    return val


def _require_int(payload: dict[str, Any], key: str, context: str, *, min_val: int | None = None) -> int:
    _require_keys(payload, (key,), context)
    val = payload[key]
    _require(isinstance(val, int) and not isinstance(val, bool), f"{context}: {key} must be an integer")
    if min_val is not None:
        _require(val >= min_val, f"{context}: {key} must be >= {min_val}, got {val}")
    return val


def _require_float(payload: dict[str, Any], key: str, context: str, *, min_val: float | None = None, strictly_positive: bool = False) -> float:
    _require_keys(payload, (key,), context)
    val = payload[key]
    _require(is_finite_number(val), f"{context}: {key} must be a finite float")
    val_f = float(val)
    if min_val is not None:
        _require(val_f >= min_val, f"{context}: {key} must be >= {min_val}, got {val_f}")
    if strictly_positive:
        _require(val_f > 0.0, f"{context}: {key} must be > 0, got {val_f}")
    return val_f


def _require_finite_matrix(matrix: Any, context: str, expected_cols: int | None = None) -> list[list[float]]:
    _require(isinstance(matrix, list) and bool(matrix), f"{context}: matrix must be a non-empty list of rows")
    rows: list[list[float]] = []
    for r_idx, row in enumerate(matrix):
        _require(isinstance(row, list) and bool(row), f"{context}: row {r_idx} must be a non-empty list")
        if expected_cols is not None:
            _require(len(row) == expected_cols, f"{context}: row {r_idx} length {len(row)} != expected {expected_cols}")
        _require(all_finite(row), f"{context}: row {r_idx} contains non-finite numbers")
        rows.append([float(x) for x in row])
    return rows


def validate_artifact_ref(ref: Any, run_dir: Path | None = None, context: str = "ref") -> dict[str, Any]:
    """Validate an artifact reference {path: str, sha256: str}."""
    _require(isinstance(ref, dict), f"{context} must be an object")
    _require_keys(ref, ("path", "sha256"), context)
    rel_path = _require_non_empty_string(ref, "path", context)
    expected_sha256 = _require_non_empty_string(ref, "sha256", context).lower()

    # Must be relative, normalized, no .. or leading slash
    _require(not rel_path.startswith("/"), f"{context}.path cannot be absolute: {rel_path}")
    _require(".." not in rel_path.split("/"), f"{context}.path cannot contain '..': {rel_path}")

    if run_dir is not None:
        target = run_dir / rel_path
        _require(target.is_file(), f"{context} target file does not exist: {target}")
        actual_sha256 = sha256_file(target)
        _require(actual_sha256 == expected_sha256, f"{context} sha256 mismatch for {rel_path}: expected {expected_sha256}, got {actual_sha256}")

    return {"path": rel_path, "sha256": expected_sha256}


def validate_scenario_config(payload: Any) -> dict[str, Any]:
    """Validate m1.config.v1 configuration dictionary."""
    _require(isinstance(payload, dict), "config must be an object")
    context = "config"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == CONFIG_SCHEMA_VERSION, f"{context}: schema_version must be {CONFIG_SCHEMA_VERSION}")

    _require_non_empty_string(payload, "algorithm_version", context)
    d = _require_int(payload, "projection_dim", context, min_val=1)
    k = _require_int(payload, "cluster_count", context, min_val=2)
    init = _require_non_empty_string(payload, "initialization", context)
    _require(init in ("kmeans++_chacha8_v1", "explicit_centers_v1"), f"{context}: invalid initialization {init}")

    seed = _require_int(payload, "seed", context, min_val=0)
    restarts = _require_int(payload, "restarts", context, min_val=1)
    _require_keys(payload, ("initial_centers",), context)
    centers = payload["initial_centers"]
    _require(isinstance(centers, list), f"{context}: initial_centers must be a list")

    if init == "kmeans++_chacha8_v1":
        _require(len(centers) == 0, f"{context}: initial_centers must be empty in random mode")
    else:
        _require(seed == 0, f"{context}: seed must be 0 for explicit_centers_v1")
        _require(len(centers) == restarts, f"{context}: explicit centers must have {restarts} restart sets")
        for r_idx, r_centers in enumerate(centers):
            _require(isinstance(r_centers, list) and len(r_centers) == k, f"{context}: restart {r_idx} centers must have length {k}")
            for c_idx, center in enumerate(r_centers):
                _require(isinstance(center, list) and len(center) == d, f"{context}: restart {r_idx} center {c_idx} must have dimension {d}")
                _require(all_finite(center), f"{context}: non-finite coordinate in restart {r_idx} center {c_idx}")

    stopping = _require_non_empty_string(payload, "stopping_rule", context)
    _require(stopping == "sklearn_variance_scaled_v1", f"{context}: invalid stopping rule {stopping}")
    _require_float(payload, "tolerance", context, min_val=0.0)
    _require_int(payload, "max_iterations", context, min_val=1)
    _require_float(payload, "rank_threshold", context, strictly_positive=True)

    return payload


def validate_task_input(payload: Any) -> dict[str, Any]:
    """Validate m1.task.v1 envelope."""
    _require(isinstance(payload, dict), "task must be an object")
    context = "task"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == TASK_SCHEMA_VERSION, f"{context}: schema_version must be {TASK_SCHEMA_VERSION}")

    _require_keys(payload, ("task_context", "config", "members"), context)
    ctx = payload["task_context"]
    _require(isinstance(ctx, dict), f"{context}: task_context must be an object")
    for key in ("task_id", "manifest_sha256", "payload_sha256", "source_matrix_sha256"):
        _require_non_empty_string(ctx, key, f"{context}.task_context")

    validate_scenario_config(payload["config"])

    members = payload["members"]
    _require(isinstance(members, list) and len(members) >= 2, f"{context}: members must be a list of at least 2 members")
    p_len = None
    seen_ids = set()
    for m_idx, m in enumerate(members):
        _require(isinstance(m, dict), f"{context}.members[{m_idx}] must be an object")
        _require_keys(m, ("member_id", "features"), f"{context}.members[{m_idx}]")
        mid = _require_non_empty_string(m, "member_id", f"{context}.members[{m_idx}]")
        _require(mid not in seen_ids, f"{context}: duplicate member_id {mid} at index {m_idx}")
        seen_ids.add(mid)

        feats = m["features"]
        _require(isinstance(feats, list) and bool(feats), f"{context}: features for member {mid} must be non-empty list")
        if p_len is None:
            p_len = len(feats)
        else:
            _require(len(feats) == p_len, f"{context}: ragged features: member {mid} has {len(feats)} != {p_len}")
        _require(all_finite(feats), f"{context}: non-finite features in member {mid}")

    return payload


def validate_scenario_result(payload: Any, n_members: int | None = None) -> dict[str, Any]:
    """Validate m1.result.v1 envelope."""
    _require(isinstance(payload, dict), "scenario result must be an object")
    context = "result"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == RESULT_SCHEMA_VERSION, f"{context}: schema_version must be {RESULT_SCHEMA_VERSION}")

    _require_non_empty_string(payload, "algorithm_version", context)
    _require_keys(payload, ("config", "members", "scenarios", "diagnostics"), context)

    config = validate_scenario_config(payload["config"])
    k = config["cluster_count"]
    d = config["projection_dim"]
    init_method = config["initialization"]

    # Members
    members = payload["members"]
    _require(isinstance(members, list) and bool(members), f"{context}: members must be non-empty list")
    n = len(members)
    if n_members is not None:
        _require(n == n_members, f"{context}: member count {n} != expected {n_members}")

    assigned_clusters = set()
    for m_idx, m in enumerate(members):
        _require_keys(m, ("member_index", "member_id", "cluster_id"), f"{context}.members[{m_idx}]")
        _require_int(m, "member_index", f"{context}.members[{m_idx}]", min_val=0)
        _require_non_empty_string(m, "member_id", f"{context}.members[{m_idx}]")
        cid = _require_int(m, "cluster_id", f"{context}.members[{m_idx}]", min_val=0)
        _require(cid < k, f"{context}: member {m['member_id']} cluster_id {cid} >= {k}")
        assigned_clusters.add(cid)

    _require(len(assigned_clusters) == k, f"{context}: not all {k} clusters assigned: {assigned_clusters}")

    # Scenarios
    scenarios = payload["scenarios"]
    _require(isinstance(scenarios, list) and len(scenarios) == k, f"{context}: scenarios must have length {k}")
    total_count = 0
    total_frac = 0.0
    for s_idx, sc in enumerate(scenarios):
        _require_keys(sc, ("cluster_id", "member_count", "member_fraction", "representative_member_id", "cluster_sse", "center", "mean_features"), f"{context}.scenarios[{s_idx}]")
        _require(sc["cluster_id"] == s_idx, f"{context}: canonical cluster_id must be {s_idx}, got {sc['cluster_id']}")
        cnt = _require_int(sc, "member_count", f"{context}.scenarios[{s_idx}]", min_val=1)
        total_count += cnt
        frac = _require_float(sc, "member_fraction", f"{context}.scenarios[{s_idx}]", strictly_positive=True)
        total_frac += frac
        _require_non_empty_string(sc, "representative_member_id", f"{context}.scenarios[{s_idx}]")
        _require_float(sc, "cluster_sse", f"{context}.scenarios[{s_idx}]", min_val=0.0)
        center = sc["center"]
        _require(isinstance(center, list) and len(center) == d and all_finite(center), f"{context}: scenario {s_idx} center must be {d}-dim finite list")
        mean_feat = sc["mean_features"]
        _require(isinstance(mean_feat, list) and bool(mean_feat) and all_finite(mean_feat), f"{context}: scenario {s_idx} mean_features must be finite list")

    _require(total_count == n, f"{context}: sum of member counts {total_count} != n {n}")
    _require(abs(total_frac - 1.0) <= 1e-12, f"{context}: sum of fractions {total_frac} differs from 1.0 by > 1e-12")

    # Diagnostics
    diag = payload["diagnostics"]
    _require(isinstance(diag, dict), f"{context}: diagnostics must be an object")
    _require_keys(diag, ("rank_after_scaling", "explained_variance_ratio", "total_sse", "restarts", "selected_restart"), f"{context}.diagnostics")
    _require_int(diag, "rank_after_scaling", f"{context}.diagnostics", min_val=d)
    evr = diag["explained_variance_ratio"]
    _require(isinstance(evr, list) and len(evr) == d and all_finite(evr), f"{context}.diagnostics: explained_variance_ratio must be {d}-dim")
    _require_float(diag, "total_sse", f"{context}.diagnostics", min_val=0.0)

    restarts = diag["restarts"]
    _require(isinstance(restarts, list) and len(restarts) == config["restarts"], f"{context}.diagnostics: restarts count {len(restarts)} != config restarts {config['restarts']}")

    for r_idx, r in enumerate(restarts):
        _require_keys(r, ("restart_index", "converged", "iterations", "total_sse", "initial_centers", "initial_member_indices", "stop_reason"), f"{context}.diagnostics.restarts[{r_idx}]")
        _require(r["restart_index"] == r_idx, f"restart index {r_idx} mismatch")
        _require(isinstance(r["converged"], bool) and r["converged"], f"restart {r_idx} must be converged")
        _require_int(r, "iterations", f"restart {r_idx}", min_val=1)
        _require_float(r, "total_sse", f"restart {r_idx}", min_val=0.0)
        _require_non_empty_string(r, "stop_reason", f"restart {r_idx}")

        init_indices = r["initial_member_indices"]
        _require(isinstance(init_indices, list), f"restart {r_idx}: initial_member_indices must be a list")

        if init_method == "kmeans++_chacha8_v1":
            _require(len(init_indices) == k, f"restart {r_idx}: random initial_member_indices length {len(init_indices)} != k {k}")
            for idx_val in init_indices:
                _require(isinstance(idx_val, int) and 0 <= idx_val < n, f"restart {r_idx}: initial index {idx_val} out of 0..{n-1}")
        else:
            _require(len(init_indices) == 0, f"restart {r_idx}: explicit initial_member_indices must be empty list")

    sel_r = _require_int(diag, "selected_restart", f"{context}.diagnostics", min_val=0)
    _require(sel_r < len(restarts), f"{context}.diagnostics: selected_restart {sel_r} >= total restarts")

    return payload


def validate_comparison_report(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SchemaValidationError("Comparison report must be a JSON object", code="INVALID_TYPE")
    if payload.get("schema_version") != "m1.comparison-report.v1":
        raise SchemaValidationError(
            f"Invalid schema_version for comparison report: {payload.get('schema_version')}",
            code="INVALID_SCHEMA_VERSION",
        )
    stage = payload.get("stage")
    if stage not in ("original", "shared"):
        raise SchemaValidationError(f"Invalid stage in comparison report: {stage}", code="INVALID_STAGE")
    status = payload.get("status")
    if status not in ("pass", "fail"):
        raise SchemaValidationError(f"Invalid status in comparison report: {status}", code="INVALID_STATUS")
    if not isinstance(payload.get("checks"), dict):
        raise SchemaValidationError("checks must be a dict in comparison report", code="INVALID_CHECKS")
    return payload


def validate_projection_alignment(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SchemaValidationError("Projection alignment must be a JSON object", code="INVALID_TYPE")
    if payload.get("schema_version") != "m1.projection-alignment.v1":
        raise SchemaValidationError(
            f"Invalid schema_version for projection alignment: {payload.get('schema_version')}",
            code="INVALID_SCHEMA_VERSION",
        )
    if "procrustes_q" not in payload or not isinstance(payload["procrustes_q"], list):
        raise SchemaValidationError("Missing or invalid procrustes_q in projection alignment", code="INVALID_Q")
    return payload


def validate_trace_record(payload: Any) -> dict[str, Any]:
    """Validate m1.trace.v1 record."""
    _require(isinstance(payload, dict), "trace must be an object")
    context = "trace"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == TRACE_SCHEMA_VERSION, f"{context}: schema_version must be {TRACE_SCHEMA_VERSION}")

    _require_keys(payload, ("events", "final_iteration_count", "stop_reason"), context)
    events = payload["events"]
    _require(isinstance(events, list) and bool(events), f"{context}: events must be non-empty list")

    for e_idx, e in enumerate(events):
        _require_keys(e, ("iteration", "event_type", "counts_as_iteration", "labels", "centers", "sse", "shift"), f"{context}.events[{e_idx}]")
        _require(e["event_type"] in ("update", "assignment_only"), f"invalid event_type in event {e_idx}")
        if e["event_type"] == "assignment_only":
            _require(e["counts_as_iteration"] is False, f"assignment_only must have counts_as_iteration=false in event {e_idx}")
            _require(e["shift"] is None, f"assignment_only must have shift=null in event {e_idx}")
        else:
            _require(e["counts_as_iteration"] is True, f"update must have counts_as_iteration=true in event {e_idx}")
            _require(is_finite_number(e["shift"]), f"update must have finite shift in event {e_idx}")

    return payload


def validate_observer_check(payload: Any) -> dict[str, Any]:
    """Validate m1.observer-check.v1 record."""
    _require(isinstance(payload, dict), "observer-check must be an object")
    context = "observer-check"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == OBSERVER_CHECK_SCHEMA_VERSION, f"{context}: schema_version must be {OBSERVER_CHECK_SCHEMA_VERSION}")

    _require_keys(payload, ("status", "kernel_symbol", "sklearn_version", "exact_match", "update_steps_match_n_iter", "assignment_only_steps"), context)
    _require(payload["status"] in ("pass", "fail"), f"{context}: status must be pass or fail")
    _require(isinstance(payload["exact_match"], bool) and payload["exact_match"], f"{context}: exact_match must be True")
    _require(isinstance(payload["update_steps_match_n_iter"], bool) and payload["update_steps_match_n_iter"], f"{context}: update_steps_match_n_iter must be True")

    return payload


def validate_reproduction_child(payload: Any) -> dict[str, Any]:
    """Validate m1.reproduction-child.v1 record."""
    _require(isinstance(payload, dict), "reproduction child must be an object")
    context = "reproduction child"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == REPRODUCTION_CHILD_SCHEMA_VERSION, f"{context}: schema_version must be {REPRODUCTION_CHILD_SCHEMA_VERSION}")

    _require(payload.get("scope") == "reproduction_subset", f"{context}: scope must be 'reproduction_subset'")
    _require(payload.get("status") in STATUSES, f"{context}: status must be one of {STATUSES}")
    _require_non_empty_string(payload, "source_manifest_sha256", context)
    _require_keys(payload, ("executed_steps", "excluded_steps", "artifacts"), context)
    _require(isinstance(payload["executed_steps"], list), f"{context}: executed_steps must be list")
    _require(isinstance(payload["excluded_steps"], list), f"{context}: excluded_steps must be list")

    # Reproduction child subset MUST NOT emit overall=passed
    _require("overall" not in payload, f"{context}: reproduction child cannot have overall verdict")

    return payload


def validate_traceability_matrix(payload: Any, run_dir: Path | None = None) -> dict[str, Any]:
    """Validate m1.traceability.v1 matrix."""
    _require(isinstance(payload, dict), "traceability must be an object")
    context = "traceability"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == TRACEABILITY_SCHEMA_VERSION, f"{context}: schema_version must be {TRACEABILITY_SCHEMA_VERSION}")

    _require_keys(payload, ("status", "assertions", "requirement_results", "success_criteria_results", "coverage"), context)
    _require(payload["status"] in STATUSES, f"{context}: status must be one of {STATUSES}")

    # Check requirement_results keys
    req_res = payload["requirement_results"]
    _require(isinstance(req_res, dict), f"{context}: requirement_results must be dict")
    _require(tuple(sorted(req_res.keys())) == REQUIREMENT_IDS, f"{context}: requirement_results must match exact 17 FRs")

    for fr_id, r in req_res.items():
        _require(r.get("status") in STATUSES, f"{context}: {fr_id} status invalid")
        _require(isinstance(r.get("assertion_ids"), list) and bool(r["assertion_ids"]), f"{context}: {fr_id} must have non-empty assertion_ids")

    # Check success_criteria_results keys
    sc_res = payload["success_criteria_results"]
    _require(isinstance(sc_res, dict), f"{context}: success_criteria_results must be dict")
    _require(tuple(sorted(sc_res.keys())) == SUCCESS_CRITERION_IDS, f"{context}: success_criteria_results must match exact 8 SCs")

    for sc_id, r in sc_res.items():
        _require(r.get("status") in STATUSES, f"{context}: {sc_id} status invalid")
        _require(isinstance(r.get("assertion_ids"), list) and bool(r["assertion_ids"]), f"{context}: {sc_id} must have non-empty assertion_ids")

    # Check assertions
    assertions = payload["assertions"]
    _require(isinstance(assertions, list) and bool(assertions), f"{context}: assertions must be non-empty list")
    seen_ids = set()
    for a in assertions:
        _require_keys(a, ("id", "status", "mapped_ids"), f"{context}.assertions")
        aid = a["id"]
        _require(aid not in seen_ids, f"{context}: duplicate assertion id {aid}")
        seen_ids.add(aid)
        _require(a["status"] in STATUSES, f"{context}: assertion {aid} status invalid")
        _require(isinstance(a["mapped_ids"], list) and bool(a["mapped_ids"]), f"{context}: assertion {aid} must map to at least one FR/SC")

    return payload


def validate_report(payload: Any, run_dir: Path | None = None) -> dict[str, Any]:
    """Validate m1.report.v1 gate report."""
    _require(isinstance(payload, dict), "report must be an object")
    context = "report"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == REPORT_SCHEMA_VERSION, f"{context}: schema_version must be {REPORT_SCHEMA_VERSION}")

    _require_keys(
        payload,
        (
            "run_id",
            "created_at",
            "overall",
            "algorithm_version",
            "source_identity_ref",
            "environment_ref",
            "input_refs",
            "config_refs",
            "steps_ref",
            "traceability_ref",
            "requirements",
            "success_criteria",
            "limitations",
        ),
        context,
    )

    _require(payload["overall"] in OVERALL_STATUSES, f"{context}: overall must be one of {OVERALL_STATUSES}")

    # Validate refs
    validate_artifact_ref(payload["source_identity_ref"], run_dir, f"{context}.source_identity_ref")
    validate_artifact_ref(payload["environment_ref"], run_dir, f"{context}.environment_ref")
    validate_artifact_ref(payload["steps_ref"], run_dir, f"{context}.steps_ref")
    validate_artifact_ref(payload["traceability_ref"], run_dir, f"{context}.traceability_ref")

    for idx, ref in enumerate(payload["input_refs"]):
        validate_artifact_ref(ref, run_dir, f"{context}.input_refs[{idx}]")
    for idx, ref in enumerate(payload["config_refs"]):
        validate_artifact_ref(ref, run_dir, f"{context}.config_refs[{idx}]")

    # Validate requirements
    reqs = payload["requirements"]
    _require(isinstance(reqs, list) and len(reqs) == 17, f"{context}: requirements must contain exactly 17 entries")
    req_ids = tuple(r["id"] for r in reqs)
    _require(req_ids == REQUIREMENT_IDS, f"{context}: requirements must exactly match FR-001..FR-017")

    req_failed = False
    req_incomplete = False
    for r in reqs:
        _require_keys(r, ("id", "status", "assertion_ids", "expected", "observed", "artifacts", "reason"), f"{context}.requirements[{r['id']}]")
        _require(r["status"] in STATUSES, f"{context}: {r['id']} status invalid")
        if r["status"] == "fail":
            req_failed = True
            _require(bool(r.get("reason")), f"{context}: {r['id']} failed but reason is empty")
        elif r["status"] == "not_run":
            req_incomplete = True
            _require(bool(r.get("reason")), f"{context}: {r['id']} not_run but reason is empty")

    # Validate success_criteria
    scs = payload["success_criteria"]
    _require(isinstance(scs, list) and len(scs) == 8, f"{context}: success_criteria must contain exactly 8 entries")
    sc_ids = tuple(sc["id"] for sc in scs)
    _require(sc_ids == SUCCESS_CRITERION_IDS, f"{context}: success_criteria must exactly match SC-001..SC-008")

    for sc in scs:
        _require_keys(sc, ("id", "status", "assertion_ids", "expected", "observed", "artifacts", "reason"), f"{context}.success_criteria[{sc['id']}]")
        _require(sc["status"] in STATUSES, f"{context}: {sc['id']} status invalid")
        if sc["status"] == "fail":
            req_failed = True
            _require(bool(sc.get("reason")), f"{context}: {sc['id']} failed but reason is empty")
        elif sc["status"] == "not_run":
            req_incomplete = True
            _require(bool(sc.get("reason")), f"{context}: {sc['id']} not_run but reason is empty")

    # Verify overall derivation
    if req_failed:
        _require(payload["overall"] == "failed", f"{context}: items failed but overall is {payload['overall']}")
    elif req_incomplete:
        _require(payload["overall"] == "incomplete", f"{context}: items incomplete but overall is {payload['overall']}")
    else:
        _require(payload["overall"] == "passed", f"{context}: all passed but overall is {payload['overall']}")

    return payload


def validate_report_audit(payload: Any, run_dir: Path | None = None) -> dict[str, Any]:
    """Validate m1.report-audit.v1 independent audit record."""
    _require(isinstance(payload, dict), "audit must be an object")
    context = "report-audit"
    version = _require_non_empty_string(payload, "schema_version", context)
    _require(version == AUDIT_SCHEMA_VERSION, f"{context}: schema_version must be {AUDIT_SCHEMA_VERSION}")

    _require_keys(payload, ("status", "report_sha256", "recalculated_overall", "audit_checks"), context)
    _require(payload["status"] in STATUSES, f"{context}: status must be one of {STATUSES}")
    _require(payload["recalculated_overall"] in OVERALL_STATUSES, f"{context}: recalculated_overall invalid")

    return payload
