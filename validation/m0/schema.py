#!/usr/bin/env python3
"""Structural validators for the M0 evidence schemas.

Two schemas are defined:

``m0.sample.v1``
    The frozen sample manifest described in
    [specs/001-m0-baseline-validation/data-model.md](../specs/001-m0-baseline-validation/data-model.md).

``m0.report.v1``
    The gate report described in
    [specs/001-m0-baseline-validation/contracts/evidence.md](../specs/001-m0-baseline-validation/contracts/evidence.md).

The validators are intentionally strict and hand written: the M0 gate must
reject a malformed evidence bundle rather than silently accept a partially
filled document. Real and synthetic samples are distinguished by `kind`, and
the two kinds have different mandatory fields, so a synthetic sample can never
be presented as a real ensemble gate.
"""

from __future__ import annotations

from typing import Any

from common import M0Error, all_finite, is_finite_number

SAMPLE_SCHEMA_VERSION = "m0.sample.v1"
REPORT_SCHEMA_VERSION = "m0.report.v1"

SAMPLE_KINDS = ("real_ensemble", "synthetic")

REQUIREMENT_IDS = tuple(f"FR-{index:03d}" for index in range(1, 15))
SUCCESS_CRITERION_IDS = tuple(f"SC-{index:03d}" for index in range(1, 7))

REQUIREMENT_STATUSES = ("pass", "fail", "not_run")
OVERALL_STATUSES = ("passed", "failed", "incomplete")


class ValidationError(M0Error):
    """Raised when a document does not satisfy its schema."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def _require_keys(payload: dict[str, Any], keys: tuple[str, ...], context: str) -> None:
    missing = [key for key in keys if key not in payload]
    _require(not missing, f"{context}: missing required key(s) {missing}")


def _require_non_empty_string(payload: dict[str, Any], key: str, context: str) -> str:
    _require_keys(payload, (key,), context)
    value = payload[key]
    _require(isinstance(value, str) and value.strip() != "", f"{context}: {key} must be a non-empty string")
    return value


def _require_finite_matrix(matrix: Any, context: str) -> None:
    _require(isinstance(matrix, list) and matrix, f"{context}: matrix must be a non-empty list of rows")
    for row_index, row in enumerate(matrix):
        _require(isinstance(row, list), f"{context}: row {row_index} must be a list")
        _require(bool(row), f"{context}: row {row_index} must not be empty")
        _require(
            all_finite(row),
            f"{context}: row {row_index} contains a non-finite or non-numeric value",
        )


def validate_sample_manifest(manifest: Any) -> dict[str, Any]:
    """Validate a frozen sample manifest and return a short summary.

    The manifest describes one frozen sample. The numeric payload lives in a
    separate file whose hash is recorded in `data_sha256`, so this validator
    only checks the manifest structure and the identity fields.
    """
    _require(isinstance(manifest, dict), "sample manifest must be a JSON object")
    context = "sample manifest"

    version = _require_non_empty_string(manifest, "schema_version", context)
    _require(
        version == SAMPLE_SCHEMA_VERSION,
        f"{context}: schema_version must be {SAMPLE_SCHEMA_VERSION!r}, got {version!r}",
    )

    kind = _require_non_empty_string(manifest, "kind", context)
    _require(kind in SAMPLE_KINDS, f"{context}: kind must be one of {SAMPLE_KINDS}, got {kind!r}")

    _require_non_empty_string(manifest, "sample_id", context)
    _require_non_empty_string(manifest, "init_time", context)
    _require_non_empty_string(manifest, "valid_time", context)
    _require_non_empty_string(manifest, "data_file", context)
    _require_non_empty_string(manifest, "data_sha256", context)

    _require_keys(
        manifest,
        ("source", "variable", "region", "points", "members", "layout", "provenance", "usage"),
        context,
    )

    lead_seconds = manifest.get("lead_seconds")
    _require(
        isinstance(lead_seconds, int) and not isinstance(lead_seconds, bool) and lead_seconds >= 0,
        f"{context}: lead_seconds must be a non-negative integer",
    )

    source = manifest["source"]
    _require(isinstance(source, dict), f"{context}: source must be an object")
    _require_non_empty_string(source, "provider", f"{context}.source")
    _require_non_empty_string(source, "product", f"{context}.source")
    _require_non_empty_string(source, "model", f"{context}.source")
    _require_non_empty_string(source, "uri", f"{context}.source")
    _require_non_empty_string(source, "retrieved_at", f"{context}.source")
    member_set = source.get("member_set")
    _require(
        isinstance(member_set, list) and bool(member_set),
        f"{context}.source.member_set must be a non-empty list",
    )
    for index, entry in enumerate(member_set):
        _require(
            isinstance(entry, dict) and "member_id" in entry and "role" in entry,
            f"{context}.source.member_set[{index}] must carry member_id and role",
        )
        _require(
            entry["role"] in ("control", "perturbed"),
            f"{context}.source.member_set[{index}].role must be 'control' or 'perturbed'",
        )

    variable = manifest["variable"]
    _require(isinstance(variable, dict), f"{context}: variable must be an object")
    for key in ("source_name", "meaning", "source_units", "normalized_units", "conversion"):
        _require_non_empty_string(variable, key, f"{context}.variable")

    region = manifest["region"]
    _require(isinstance(region, dict), f"{context}: region must be an object")
    for key in ("latitude_min", "latitude_max", "longitude_min", "longitude_max"):
        value = region.get(key)
        _require(
            is_finite_number(value),
            f"{context}.region.{key} must be a finite number",
        )
    _require_non_empty_string(region, "longitude_convention", f"{context}.region")

    points = manifest["points"]
    _require(isinstance(points, list) and bool(points), f"{context}: points must be a non-empty list")
    point_ids: set[str] = set()
    for index, point in enumerate(points):
        _require(isinstance(point, dict), f"{context}.points[{index}] must be an object")
        point_id = _require_non_empty_string(point, "point_id", f"{context}.points[{index}]")
        _require(point_id not in point_ids, f"{context}: duplicate point_id {point_id!r}")
        point_ids.add(point_id)
        _require(is_finite_number(point.get("latitude")), f"{context}.points[{index}].latitude must be finite")
        _require(is_finite_number(point.get("longitude")), f"{context}.points[{index}].longitude must be finite")

    members = manifest["members"]
    _require(isinstance(members, list) and bool(members), f"{context}: members must be a non-empty list")
    member_ids: set[str] = set()
    for index, member in enumerate(members):
        _require(isinstance(member, dict), f"{context}.members[{index}] must be an object")
        member_id = _require_non_empty_string(member, "member_id", f"{context}.members[{index}]")
        _require(member_id not in member_ids, f"{context}: duplicate member_id {member_id!r}")
        member_ids.add(member_id)
        _require(
            member.get("role") in ("control", "perturbed"),
            f"{context}.members[{index}].role must be 'control' or 'perturbed'",
        )
        _require_non_empty_string(member, "source_label", f"{context}.members[{index}]")

    declared_member_ids = {entry["member_id"] for entry in member_set}
    _require(
        declared_member_ids == member_ids,
        f"{context}: members must match source.member_set exactly",
    )

    layout = manifest["layout"]
    _require(isinstance(layout, dict), f"{context}: layout must be an object")
    for key in ("matrix_order", "member_order", "spatial_order", "dtype"):
        _require_non_empty_string(layout, key, f"{context}.layout")
    shape = layout.get("shape")
    _require(
        isinstance(shape, list) and len(shape) == 2 and all(isinstance(value, int) for value in shape),
        f"{context}.layout.shape must be a two-element integer list",
    )
    _require(
        shape[0] == len(members),
        f"{context}: layout.shape member count {shape[0]} does not match {len(members)} members",
    )
    _require(
        shape[1] == len(points),
        f"{context}: layout.shape point count {shape[1]} does not match {len(points)} points",
    )

    provenance = manifest["provenance"]
    _require(isinstance(provenance, dict), f"{context}: provenance must be an object")
    extraction_steps = provenance.get("extraction_steps")
    _require(
        isinstance(extraction_steps, list) and bool(extraction_steps),
        f"{context}.provenance.extraction_steps must be a non-empty list",
    )
    for index, step in enumerate(extraction_steps):
        _require(
            isinstance(step, str) and step.strip() != "",
            f"{context}.provenance.extraction_steps[{index}] must be a non-empty string",
        )
    _require_non_empty_string(provenance, "extraction_tool_version", f"{context}.provenance")
    _require(
        isinstance(provenance.get("transformations"), list),
        f"{context}.provenance.transformations must be a list",
    )

    usage = manifest["usage"]
    _require(isinstance(usage, dict), f"{context}: usage must be an object")
    for key in ("license", "attribution", "restrictions", "recovery"):
        _require_non_empty_string(usage, key, f"{context}.usage")

    if kind == "real_ensemble":
        _require(
            "init_time_evidence" in source,
            f"{context}: a real ensemble sample must record source.init_time_evidence",
        )
        _require(
            source.get("object_generations"),
            f"{context}: a real ensemble sample must record source.object_generations",
        )
    else:
        # The distinction between the two kinds has to hold in both
        # directions. A synthetic sample must not carry the evidence that
        # only a real source can provide, otherwise a real sample could be
        # relabelled as synthetic and would escape the real-sample checks.
        _require(
            "init_time_evidence" not in source,
            f"{context}: a synthetic sample must not claim source.init_time_evidence",
        )
        _require(
            not source.get("object_generations"),
            f"{context}: a synthetic sample must not claim source.object_generations",
        )
        _require(
            manifest.get("usage", {}).get("license_status")
            not in (None, "unresolved_for_redistribution"),
            f"{context}: a synthetic sample must not carry the real-sample licence status",
        )

    return {
        "schema_version": version,
        "kind": kind,
        "sample_id": manifest["sample_id"],
        "member_count": len(members),
        "point_count": len(points),
        "shape": shape,
    }


def validate_sample_payload(payload: Any, manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate the numeric payload of a frozen sample against its manifest."""
    _require(isinstance(payload, dict), "sample payload must be a JSON object")
    context = "sample payload"

    _require_keys(payload, ("member_ids", "point_ids", "values"), context)

    member_ids = payload["member_ids"]
    point_ids = payload["point_ids"]
    values = payload["values"]

    _require(isinstance(member_ids, list) and bool(member_ids), f"{context}: member_ids must be non-empty")
    _require(isinstance(point_ids, list) and bool(point_ids), f"{context}: point_ids must be non-empty")
    _require(
        len(set(member_ids)) == len(member_ids),
        f"{context}: member_ids must be unique",
    )
    _require(
        len(set(point_ids)) == len(point_ids),
        f"{context}: point_ids must be unique",
    )

    manifest_member_ids = [member["member_id"] for member in manifest["members"]]
    manifest_point_ids = [point["point_id"] for point in manifest["points"]]
    _require(
        member_ids == manifest_member_ids,
        f"{context}: member_ids must match the manifest member order",
    )
    _require(
        point_ids == manifest_point_ids,
        f"{context}: point_ids must match the manifest point order",
    )

    _require(
        isinstance(values, list) and len(values) == len(member_ids),
        f"{context}: values must have one row per member",
    )
    for row_index, row in enumerate(values):
        _require(
            isinstance(row, list) and len(row) == len(point_ids),
            f"{context}: values row {row_index} must have one entry per point",
        )
        _require(all_finite(row), f"{context}: values row {row_index} contains a non-finite value")

    return {
        "member_count": len(member_ids),
        "point_count": len(point_ids),
        "value_count": len(member_ids) * len(point_ids),
    }


def validate_requirement_record(record: Any, requirement_id: str) -> None:
    """Validate one requirement record inside a gate report.

    Every artifact reference carries the relative path and the SHA-256 of the
    referenced file, so that a reviewer can confirm that the evidence a
    requirement points at is the evidence that was actually produced.
    """
    context = f"report.requirements[{requirement_id}]"
    _require(isinstance(record, dict), f"{context} must be an object")
    _require_keys(
        record,
        (
            "requirement_id",
            "status",
            "scope",
            "input_hashes",
            "configuration_hash",
            "environment",
            "steps",
            "expected",
            "actual",
            "artifacts",
            "limitations",
        ),
        context,
    )
    _require(record["requirement_id"] == requirement_id, f"{context}: requirement_id must match its key")
    _require(
        record["status"] in REQUIREMENT_STATUSES,
        f"{context}: status must be one of {REQUIREMENT_STATUSES}",
    )
    _require(isinstance(record["input_hashes"], dict), f"{context}: input_hashes must be an object")
    _require(isinstance(record["artifacts"], list), f"{context}: artifacts must be a list")
    _require(isinstance(record["steps"], list) and bool(record["steps"]), f"{context}: steps must be non-empty")
    _require(isinstance(record["limitations"], list), f"{context}: limitations must be a list")
    _require(
        isinstance(record["environment"], dict) and bool(record["environment"]),
        f"{context}: environment must be a non-empty object",
    )
    for index, artifact in enumerate(record["artifacts"]):
        _require(
            isinstance(artifact, dict),
            f"{context}: artifacts[{index}] must be an object with a path and a sha256",
        )
        _require_keys(artifact, ("path", "sha256"), f"{context}.artifacts[{index}]")
        _require_non_empty_string(artifact, "path", f"{context}.artifacts[{index}]")
        digest = artifact["sha256"]
        _require(
            isinstance(digest, str)
            and len(digest) == 64
            and all(character in "0123456789abcdef" for character in digest),
            f"{context}.artifacts[{index}].sha256 must be a lowercase hex SHA-256 digest",
        )


def validate_gate_report(report: Any) -> dict[str, Any]:
    """Validate an `m0.report.v1` document and return a short summary."""
    _require(isinstance(report, dict), "gate report must be a JSON object")
    context = "gate report"

    version = _require_non_empty_string(report, "schema_version", context)
    _require(
        version == REPORT_SCHEMA_VERSION,
        f"{context}: schema_version must be {REPORT_SCHEMA_VERSION!r}, got {version!r}",
    )
    _require_non_empty_string(report, "run_id", context)
    _require_non_empty_string(report, "created_at", context)

    overall = _require_non_empty_string(report, "overall", context)
    _require(
        overall in OVERALL_STATUSES,
        f"{context}: overall must be one of {OVERALL_STATUSES}",
    )

    requirements = report.get("requirements")
    _require(isinstance(requirements, dict), f"{context}: requirements must be an object")
    for requirement_id in REQUIREMENT_IDS:
        _require(
            requirement_id in requirements,
            f"{context}: requirement {requirement_id} must appear exactly once",
        )
    unexpected = sorted(set(requirements) - set(REQUIREMENT_IDS))
    _require(not unexpected, f"{context}: unexpected requirement key(s) {unexpected}")
    for requirement_id, record in requirements.items():
        validate_requirement_record(record, requirement_id)

    _require(
        isinstance(report.get("limitations"), list),
        f"{context}: limitations must be a list",
    )
    handoff = report.get("m1_handoff")
    _require(isinstance(handoff, dict), f"{context}: m1_handoff must be an object")
    for key in (
        "sample_contract",
        "reference_config",
        "comparison_rules",
        "version_combination",
        "verified_environment",
        "dependency_decisions",
    ):
        _require_non_empty_string(handoff, key, f"{context}.m1_handoff")

    return {
        "schema_version": version,
        "run_id": report["run_id"],
        "overall": overall,
        "requirement_count": len(requirements),
        "status_counts": {
            status: sum(
                1 for record in requirements.values() if record["status"] == status
            )
            for status in REQUIREMENT_STATUSES
        },
    }


def aggregate_overall(requirements: dict[str, Any]) -> str:
    """Derive the overall status from the per-requirement statuses.

    A single failure makes the overall result `failed`. Otherwise a single
    unexecuted requirement makes it `incomplete`. Only an all-pass set yields
    `passed`. This mirrors [contracts/evidence.md](../specs/001-m0-baseline-validation/contracts/evidence.md).
    """
    statuses = [record["status"] for record in requirements.values()]
    if "fail" in statuses:
        return "failed"
    if "not_run" in statuses:
        return "incomplete"
    return "passed"
