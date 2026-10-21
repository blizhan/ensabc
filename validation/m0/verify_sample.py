#!/usr/bin/env python3
"""Offline verification of the frozen M0 sample.

The verifier answers one question: does the frozen material constitute a
qualified real regional ensemble sample?

It checks, without any network access:

* the manifest satisfies `m0.sample.v1`;
* the payload hash matches the manifest and the payload matches the manifest
  member and point ordering;
* the member set equals the set the source declares (no hard-coded 51, no
  mean-only or single-point substitute);
* every member carries the same ordered point set and all values are finite;
* the total frozen size stays within the 100 MiB budget;
* the float32 source matrix hash still matches the recorded source identity.

It also runs negative eligibility cases: a mean-only sample, a single-point
sample, a sample with a missing member, a sample with a duplicate point and a
sample whose initialization identity cannot be verified must all be rejected
with a stated reason, never silently accepted.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    FIXTURES_DIR,
    M0Error,
    read_json,
    sha256_bytes,
    sha256_file,
    write_json,
)
from schema import (  # noqa: E402
    ValidationError,
    validate_sample_manifest,
    validate_sample_payload,
)

MANIFEST_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_manifest.json"
PAYLOAD_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_data.json"

SIZE_BUDGET_BYTES = 100 * 1024 * 1024
EXPECTED_MATRIX_SHA256 = "09b43dfbcf9738f787e4ef65295de96b2f7a75956c51ef3e92909d5e5d2e42fe"
EXPECTED_MEMBER_COUNT = 50
EXPECTED_POINT_COUNT = 9


def _fail(message: str) -> None:
    raise M0Error(message)


def evaluate_eligibility(manifest: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """Apply every eligibility rule to an in-memory manifest/payload pair.

    The negative eligibility cases mutate a valid pair and call this same
    function, so a rejection reason is produced by exactly the code path that
    accepts the real sample.
    """
    summary = validate_sample_manifest(manifest)

    if manifest["kind"] != "real_ensemble":
        _fail(
            f"the frozen sample must be a real_ensemble sample, found kind={manifest['kind']!r}. "
            "A synthetic sample can never satisfy the real-sample gate."
        )

    payload_summary = validate_sample_payload(payload, manifest)

    member_count = len(manifest["members"])
    point_count = len(manifest["points"])
    if member_count != EXPECTED_MEMBER_COUNT:
        _fail(f"expected {EXPECTED_MEMBER_COUNT} members, found {member_count}")
    if point_count != EXPECTED_POINT_COUNT:
        _fail(f"expected {EXPECTED_POINT_COUNT} points, found {point_count}")

    # The member set must equal the set the source declares.
    declared = {entry["member_id"] for entry in manifest["source"]["member_set"]}
    actual = {member["member_id"] for member in manifest["members"]}
    if declared != actual:
        _fail(
            "the member set does not match the source declaration: "
            f"missing={sorted(declared - actual)}, extra={sorted(actual - declared)}"
        )

    # Every member must carry the same ordered point set.
    values = payload["values"]
    for row_index, row in enumerate(values):
        if len(row) != point_count:
            _fail(f"member {row_index} has {len(row)} points, expected {point_count}")

    # A mean-only or single-point substitute is not a regional ensemble sample.
    if point_count < 2:
        _fail("a single-point sample cannot stand in for a regional ensemble field")
    if member_count < 2:
        _fail("a single-member sample cannot stand in for an ensemble member set")

    # The initialization identity must be evidenced, not assumed.
    init_evidence = manifest["source"].get("init_time_evidence")
    if not init_evidence:
        _fail("the initialization time has no recorded evidence")
    init_time = str(init_evidence.get("init_time", ""))
    if "latest" in init_time.lower():
        _fail(f"the initialization identity must not be 'latest': {init_time!r}")
    if not manifest["source"].get("object_generations"):
        _fail("the source snapshot identity (object generations) is missing")
    if manifest["init_time"] != "2020-01-01T00:00:00Z":
        _fail(f"unexpected initialization time: {manifest['init_time']!r}")
    if manifest["valid_time"] != "2020-01-02T00:00:00Z":
        _fail(f"unexpected valid time: {manifest['valid_time']!r}")

    # The float32 source identity must still hold.
    import numpy as np

    matrix32 = np.asarray(values, dtype="<f4")
    matrix_sha = sha256_bytes(matrix32.tobytes(order="C"))
    if matrix_sha != EXPECTED_MATRIX_SHA256:
        _fail(
            "the float32 source matrix hash does not match the recorded source identity: "
            f"expected {EXPECTED_MATRIX_SHA256}, found {matrix_sha}"
        )

    return {
        "summary": summary,
        "payload_summary": payload_summary,
        "member_count": member_count,
        "point_count": point_count,
        "matrix_sha256_float32_le": matrix_sha,
    }


def check_manifest_and_payload(manifest_path: Path, payload_path: Path) -> dict[str, Any]:
    """Validate the manifest and payload and return the check details."""
    if not manifest_path.is_file():
        _fail(f"frozen sample manifest is missing: {manifest_path}")
    if not payload_path.is_file():
        _fail(
            f"frozen sample payload is missing: {payload_path}. "
            "Rebuild it with the setup_env.sh data interpreter: "
            "`\"${ENSABC_M0_DATA_VENV:-/tmp/ensabc-m0-data/.venv}/bin/python\" "
            "validation/m0/extract_sample.py --rebuild --from-source --force`."
        )

    manifest = read_json(manifest_path)
    payload = read_json(payload_path)

    evaluated = evaluate_eligibility(manifest, payload)
    summary = evaluated["summary"]
    payload_summary = evaluated["payload_summary"]
    member_count = evaluated["member_count"]
    point_count = evaluated["point_count"]
    matrix_sha = evaluated["matrix_sha256_float32_le"]

    actual_payload_sha = sha256_file(payload_path)
    if actual_payload_sha != manifest["data_sha256"]:
        _fail(
            f"payload hash mismatch: manifest records {manifest['data_sha256']}, "
            f"file hashes to {actual_payload_sha}"
        )

    expected_data_file = manifest["data_file"]
    if payload_path.name != expected_data_file:
        _fail(
            f"manifest data_file {expected_data_file!r} does not match the payload file "
            f"{payload_path.name!r}"
        )

    # The frozen size budget.
    total_bytes = manifest_path.stat().st_size + payload_path.stat().st_size
    if total_bytes > SIZE_BUDGET_BYTES:
        _fail(
            f"the frozen sample totals {total_bytes} bytes, which exceeds the "
            f"{SIZE_BUDGET_BYTES} byte budget"
        )

    return {
        "manifest_path": str(manifest_path.relative_to(manifest_path.parents[3])),
        "payload_path": str(payload_path.relative_to(payload_path.parents[3])),
        "manifest_sha256": sha256_file(manifest_path),
        "payload_sha256": actual_payload_sha,
        "sample_id": manifest["sample_id"],
        "kind": manifest["kind"],
        "member_count": member_count,
        "point_count": point_count,
        "shape": manifest["layout"]["shape"],
        "total_frozen_bytes": total_bytes,
        "size_budget_bytes": SIZE_BUDGET_BYTES,
        "matrix_sha256_float32_le": matrix_sha,
        "init_time": manifest["init_time"],
        "valid_time": manifest["valid_time"],
        "variable": manifest["variable"]["source_name"],
        "units": manifest["variable"]["normalized_units"],
        "license_status": manifest["usage"].get("license_status", "unknown"),
        "payload_summary": payload_summary,
        "schema": summary,
    }


def check_negative_eligibility(manifest: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Confirm that ineligible sample shapes are rejected with a stated reason.

    Each case mutates an otherwise valid manifest/payload pair and asserts that
    the eligibility check refuses it. The cases are the ones the specification
    names explicitly: mean-only, single-point, missing member, duplicate point
    and an unverified initialization identity.
    """
    cases: list[dict[str, Any]] = []

    def run_case(case_id: str, description: str, mutate) -> None:
        candidate_manifest = copy.deepcopy(manifest)
        candidate_payload = copy.deepcopy(payload)
        mutate(candidate_manifest, candidate_payload)
        try:
            evaluate_eligibility(candidate_manifest, candidate_payload)
        except (M0Error, ValidationError) as error:
            cases.append(
                {
                    "case_id": case_id,
                    "description": description,
                    "rejected": True,
                    "reason": str(error),
                }
            )
            return
        raise M0Error(f"ineligible sample was accepted: {case_id}")

    def mean_only(candidate_manifest, candidate_payload) -> None:
        # Replace the member matrix with a single mean member.
        rows = candidate_payload["values"]
        width = len(rows[0])
        mean_row = [sum(row[index] for row in rows) / len(rows) for index in range(width)]
        candidate_payload["values"] = [mean_row]
        candidate_payload["member_ids"] = ["mean"]
        candidate_manifest["members"] = [
            {
                "member_id": "mean",
                "source_label": "mean",
                "role": "perturbed",
            }
        ]
        candidate_manifest["source"]["member_set"] = [
            {"member_id": "mean", "source_label": "mean", "role": "perturbed"}
        ]
        candidate_manifest["layout"]["shape"] = [1, width]

    def single_point(candidate_manifest, candidate_payload) -> None:
        candidate_payload["values"] = [[row[0]] for row in candidate_payload["values"]]
        candidate_payload["point_ids"] = candidate_payload["point_ids"][:1]
        candidate_manifest["points"] = candidate_manifest["points"][:1]
        candidate_manifest["layout"]["shape"] = [len(candidate_manifest["members"]), 1]

    def missing_member(candidate_manifest, candidate_payload) -> None:
        candidate_payload["values"] = candidate_payload["values"][:-1]
        candidate_payload["member_ids"] = candidate_payload["member_ids"][:-1]
        candidate_manifest["members"] = candidate_manifest["members"][:-1]
        candidate_manifest["source"]["member_set"] = candidate_manifest["source"]["member_set"][:-1]
        candidate_manifest["layout"]["shape"] = [
            len(candidate_manifest["members"]),
            len(candidate_manifest["points"]),
        ]

    def duplicate_point(candidate_manifest, candidate_payload) -> None:
        candidate_manifest["points"][1]["point_id"] = candidate_manifest["points"][0]["point_id"]

    def unverified_init(candidate_manifest, candidate_payload) -> None:
        candidate_manifest["init_time"] = "latest"

    run_case("mean_only", "a mean-only series cannot replace a real ensemble sample", mean_only)
    run_case("single_point", "a single-point series cannot replace a regional field", single_point)
    run_case("missing_member", "a sample with a missing member is rejected", missing_member)
    run_case("duplicate_point", "a sample with a duplicate point key is rejected", duplicate_point)
    run_case("unverified_init_time", "an initialization identity of 'latest' is rejected", unverified_init)

    return cases


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--offline",
        action="store_true",
        help="verify the frozen sample without any network access (the only supported mode)",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=MANIFEST_PATH,
        help="path to the frozen sample manifest",
    )
    parser.add_argument(
        "--payload",
        type=Path,
        default=PAYLOAD_PATH,
        help="path to the frozen sample payload",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the verification record to this JSON file",
    )
    arguments = parser.parse_args()

    try:
        details = check_manifest_and_payload(arguments.manifest, arguments.payload)
        manifest = read_json(arguments.manifest)
        payload = read_json(arguments.payload)
        details["negative_eligibility_cases"] = check_negative_eligibility(manifest, payload)
        details["status"] = "pass"
    except (M0Error, ValidationError) as error:
        details = {"status": "fail", "error": str(error)}
        if arguments.output:
            write_json(arguments.output, details)
        print(f"FAIL: {error}")
        return 1

    if arguments.output:
        write_json(arguments.output, details)

    print("PASS offline sample verification")
    print(f"  sample_id:   {details['sample_id']} (kind={details['kind']})")
    print(f"  shape:       {details['shape']}")
    print(f"  members:     {details['member_count']}")
    print(f"  points:      {details['point_count']}")
    print(f"  init/valid:  {details['init_time']} -> {details['valid_time']}")
    print(f"  variable:    {details['variable']} [{details['units']}]")
    print(f"  matrix sha:  {details['matrix_sha256_float32_le']}")
    print(f"  frozen size: {details['total_frozen_bytes']} bytes (budget {details['size_budget_bytes']})")
    print(f"  license:     {details['license_status']}")
    print(
        "  negative:    "
        + ", ".join(
            f"{case['case_id']}=rejected" for case in details["negative_eligibility_cases"]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
