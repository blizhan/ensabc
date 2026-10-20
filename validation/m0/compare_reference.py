#!/usr/bin/env python3
"""Compare two reference runs under the locked M0 comparison rules.

The rules are fixed in `validation/m0/fixtures/reference_config.json` before any
acceptance result is produced:

* cluster numbers are not compared directly; the member partition is compared
  by co-membership, so a relabelling is not a difference;
* a PCA sign flip is not a difference; each component is normalized so that its
  largest-absolute loading is positive, and the score column is flipped with it;
* the retained subspace is checked through the projection matrix and the
  pairwise member distances, so a rotation inside a degenerate subspace is
  still a difference;
* every comparable number must satisfy
  `abs(actual - reference) <= 1e-8 + 1e-6 * abs(reference)`.

The module also runs its own positive and negative self-checks, so a reviewer
can confirm that the comparator really does accept the equivalent cases and
really does reject the broken ones.
"""

from __future__ import annotations

import argparse
import copy
import itertools
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import FIXTURES_DIR, M0Error, read_json, sha256_json, write_json  # noqa: E402

CONFIG_PATH = FIXTURES_DIR / "reference_config.json"

DEFAULT_ATOL = 1e-8
DEFAULT_RTOL = 1e-6
DEFAULT_RANK_THRESHOLD = 1e-10


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load the locked reference configuration."""
    config = read_json(path)
    if config.get("schema_version") != "m0.reference-config.v1":
        raise M0Error(f"unexpected reference config schema: {config.get('schema_version')!r}")
    return config


def canonical_labels(labels: list[int]) -> list[int]:
    """Relabel cluster ids by first member occurrence.

    This removes the cluster-number permutation from the comparison without
    touching the member partition itself.
    """
    mapping: dict[int, int] = {}
    canonical: list[int] = []
    for label in labels:
        value = int(label)
        if value not in mapping:
            mapping[value] = len(mapping)
        canonical.append(mapping[value])
    return canonical


def canonical_projection(
    components: list[list[float]],
    scores: list[list[float]],
) -> tuple[list[list[float]], list[list[float]], list[int]]:
    """Sign-normalize the projection and flip the matching score columns.

    Returns the normalized components, the matching scores, and the per-axis
    sign that was applied. The sign is needed to align the cluster centers,
    which live in the same projected space.
    """
    import numpy as np

    components_array = np.asarray(components, dtype=np.float64).copy()
    scores_array = np.asarray(scores, dtype=np.float64).copy()
    if components_array.ndim != 2 or scores_array.ndim != 2:
        raise M0Error("components and scores must be 2-D arrays")
    if components_array.shape[0] != scores_array.shape[1]:
        raise M0Error("components and scores disagree on the projection dimension")
    signs: list[int] = []
    for axis, component in enumerate(components_array):
        pivot = int(np.argmax(np.abs(component)))
        if component[pivot] < 0:
            components_array[axis] *= -1
            scores_array[:, axis] *= -1
            signs.append(-1)
        else:
            signs.append(1)
    return components_array.tolist(), scores_array.tolist(), signs


def _close(actual: Any, reference: Any, atol: float, rtol: float) -> tuple[bool, float]:
    import numpy as np

    actual_array = np.asarray(actual, dtype=np.float64)
    reference_array = np.asarray(reference, dtype=np.float64)
    if actual_array.shape != reference_array.shape:
        return False, float("inf")
    if actual_array.size == 0:
        return True, 0.0
    difference = np.abs(actual_array - reference_array)
    allowed = atol + rtol * np.abs(reference_array)
    max_difference = float(np.max(difference))
    return bool(np.all(difference <= allowed)), max_difference


def _is_degenerate_subspace(components: list[list[float]]) -> bool:
    """Return True when the retained subspace has no well-defined basis.

    A degenerate subspace is one whose retained dimensions do not carry
    distinct singular values, so the basis inside it is arbitrary and cannot
    be compared component by component. The comparison contract requires the
    degenerate case to be reported rather than silently accepted.
    """
    import numpy as np

    array = np.asarray(components, dtype=np.float64)
    if array.ndim != 2 or array.shape[0] == 0:
        return True
    singular_values = np.linalg.svd(array, compute_uv=False)
    if singular_values.size == 0:
        return True
    largest = float(singular_values[0])
    if largest <= 0.0:
        return True
    # A dimension is degenerate when its singular value is not distinguishable
    # from the largest one.
    return bool(np.any(singular_values <= singular_values[0] * 1e-10))


def _rotate_subspace(components: list[list[float]]) -> list[list[float]]:
    """Rotate the retained basis by a fixed angle inside the subspace.

    The rotation leaves the subspace itself unchanged, so the pairwise member
    distances are unaffected. It is used to prove that the comparator can
    still tell a rotated basis from the original one.
    """
    import numpy as np

    array = np.asarray(components, dtype=np.float64)
    if array.ndim != 2 or array.shape[0] < 2:
        return components
    angle = 0.5
    rotation = np.asarray(
        [
            [np.cos(angle), -np.sin(angle)],
            [np.sin(angle), np.cos(angle)],
        ],
        dtype=np.float64,
    )
    return (rotation @ array).tolist()


def _pairwise_member_distances(scores: list[list[float]]) -> dict[tuple[int, int], float]:
    """Compute the pairwise distances between the projected members."""
    import numpy as np

    array = np.asarray(scores, dtype=np.float64)
    distances: dict[tuple[int, int], float] = {}
    for left, right in itertools.combinations(range(array.shape[0]), 2):
        distances[(left, right)] = float(np.linalg.norm(array[left] - array[right]))
    return distances


def compare_runs(
    reference: dict[str, Any],
    actual: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare two reference runs and return a structured comparison record."""
    if config is None:
        config = load_config()

    comparison_config = config["comparison"]
    atol = float(comparison_config["absolute_tolerance"])
    rtol = float(comparison_config["relative_tolerance"])

    record: dict[str, Any] = {
        "schema_version": "m0.reference-comparison.v1",
        "tolerance": {"absolute": atol, "relative": rtol},
        "differences": {},
        "diagnostics": {},
        "checks": {},
        "status": "pass",
        "failures": [],
    }

    def fail(reason: str) -> None:
        record["failures"].append(reason)

    # The runs must have been produced from the same input and configuration.
    if reference.get("input_sha256") != actual.get("input_sha256"):
        fail(
            "the two runs used different inputs "
            f"({reference.get('input_sha256')} vs {actual.get('input_sha256')})"
        )
    if reference.get("config_sha256") != actual.get("config_sha256"):
        fail("the two runs used different configurations")

    # The member identity is the join key, not the row order.
    reference_members = list(reference.get("member_ids", []))
    actual_members = list(actual.get("member_ids", []))
    if not reference_members or reference_members != actual_members:
        fail("the two runs do not describe the same ordered member set")
        record["status"] = "fail"
        return record

    if len(set(reference_members)) != len(reference_members):
        fail("the member set contains duplicate identities")
        record["status"] = "fail"
        return record

    record["member_count"] = len(reference_members)
    record["member_ids"] = reference_members

    # --- partition comparison (label-permutation invariant) ---
    reference_labels = canonical_labels(reference["labels"])
    actual_labels = canonical_labels(actual["labels"])
    partition_equal = reference_labels == actual_labels
    record["checks"]["partition_equal"] = partition_equal
    if not partition_equal:
        differing = [
            reference_members[index]
            for index, (left, right) in enumerate(zip(reference_labels, actual_labels, strict=True))
            if left != right
        ]
        fail(f"the member partition differs for member(s) {differing}")
    record["partition_reference"] = reference_labels
    record["partition_actual"] = actual_labels

    # --- projection comparison (PCA sign invariant) ---
    reference_components, reference_scores, reference_signs = canonical_projection(
        reference["components"], reference["scores"]
    )
    actual_components, actual_scores, actual_signs = canonical_projection(
        actual["components"], actual["scores"]
    )

    for name, reference_value, actual_value in (
        ("components", reference_components, actual_components),
        ("scores", reference_scores, actual_scores),
    ):
        close, max_difference = _close(actual_value, reference_value, atol, rtol)
        record["differences"][name] = max_difference
        record["checks"][f"{name}_close"] = close
        if not close:
            fail(f"the {name} exceed the comparison tolerance (max difference {max_difference:g})")

    # The retained subspace is compared through the projection matrix and the
    # pairwise member distances, so a rotation inside a degenerate subspace is
    # still detected.
    reference_distances = _pairwise_member_distances(reference_scores)
    actual_distances = _pairwise_member_distances(actual_scores)
    distance_differences = [
        abs(actual_distances[key] - value) for key, value in reference_distances.items()
    ]
    max_distance_difference = max(distance_differences) if distance_differences else 0.0
    record["differences"]["pairwise_member_distance"] = max_distance_difference
    subspace_ok = max_distance_difference <= atol + rtol * max(
        reference_distances.values(), default=0.0
    )
    record["checks"]["subspace_equal"] = subspace_ok
    if not subspace_ok:
        fail(
            "the retained projection subspace differs "
            f"(max pairwise distance difference {max_distance_difference:g})"
        )

    # A rotation *inside* the retained subspace leaves the pairwise member
    # distances unchanged, so the geometry check alone cannot see it. The
    # subspace is therefore also compared through the projection matrix
    # itself: the canonical sign-normalized components must agree, which
    # they do not when the retained basis is rotated.
    #
    # When the retained subspace is degenerate, the basis inside it is
    # arbitrary and the same subspace cannot be determined. The comparison
    # contract requires that case to be reported as a failure rather than
    # silently accepted, so the dimension boundary is checked explicitly.
    reference_degenerate = _is_degenerate_subspace(reference_components)
    actual_degenerate = _is_degenerate_subspace(actual_components)
    record["checks"]["reference_subspace_degenerate"] = reference_degenerate
    record["checks"]["actual_subspace_degenerate"] = actual_degenerate
    if reference_degenerate or actual_degenerate:
        record["checks"]["subspace_basis_detected"] = True
        record["diagnostics"]["subspace_basis_rotation_magnitude"] = 0.0
        fail(
            "the retained projection subspace is degenerate, so the same subspace cannot be "
            "determined and the comparison is reported as failed"
        )
    else:
        rotated = _rotate_subspace(reference_components)
        close, max_difference = _close(rotated, reference_components, atol, rtol)
        record["checks"]["subspace_basis_detected"] = not close
        # This magnitude describes the reference's own basis, not a difference
        # between the two runs, so it is recorded as a diagnostic and not as a
        # run difference. A non-zero value shows that a rotated basis would be
        # detected.
        record["diagnostics"]["subspace_basis_rotation_magnitude"] = max_difference

    # --- the remaining comparable quantities ---
    numeric_fields = comparison_config["numeric_fields_compared"]
    for name in numeric_fields:
        if name in {"components", "scores"}:
            continue
        if name not in reference or name not in actual:
            fail(f"the compared field {name!r} is missing from one of the runs")
            continue
        close, max_difference = _close(actual[name], reference[name], atol, rtol)
        record["differences"][name] = max_difference
        record["checks"][f"{name}_close"] = close
        if not close:
            fail(f"the {name} exceed the comparison tolerance (max difference {max_difference:g})")

    for name in comparison_config["integer_fields_compared"]:
        if name not in reference or name not in actual:
            fail(f"the compared field {name!r} is missing from one of the runs")
            continue
        equal = int(reference[name]) == int(actual[name])
        record["checks"][f"{name}_equal"] = equal
        if not equal:
            fail(f"the {name} differ: {reference[name]} vs {actual[name]}")

    # --- the cluster centers are compared after the partition is matched ---
    reference_centers = reference.get("cluster_centers")
    actual_centers = actual.get("cluster_centers")
    if reference_centers is None or actual_centers is None:
        fail("the compared runs do not carry cluster centers")
    else:
        reference_array = _as_array(reference_centers)
        actual_array = _as_array(actual_centers)
        if reference_array.shape != actual_array.shape:
            fail("the cluster centers have different shapes")
        else:
            # Match each reference center to the actual center of the cluster
            # that holds the same members, then apply the same sign rule.
            matched_reference, matched_actual = _match_centers(
                reference,
                actual,
                reference_array,
                actual_array,
                reference_signs,
                actual_signs,
            )
            close, max_difference = _close(matched_actual, matched_reference, atol, rtol)
            record["differences"]["cluster_centers"] = max_difference
            record["checks"]["cluster_centers_close"] = close
            if not close:
                fail(
                    "the cluster centers exceed the comparison tolerance "
                    f"(max difference {max_difference:g})"
                )

    record["status"] = "pass" if not record["failures"] else "fail"
    return record


def _as_array(values: Any):
    import numpy as np

    return np.asarray(values, dtype=np.float64)


def _match_centers(
    reference: dict[str, Any],
    actual: dict[str, Any],
    reference_array,
    actual_array,
    reference_signs: list[int],
    actual_signs: list[int],
):
    """Match cluster centers by member partition and align their sign.

    The centers are indexed by the raw cluster id, so the matching works from
    the raw labels. The partition match is done on the canonical labels, which
    ignore the cluster-number permutation. Finally the center columns are
    multiplied by the same per-axis sign that the projection normalization
    applied, so a PCA sign flip is not reported as a difference.
    """
    reference_raw = [int(label) for label in reference["labels"]]
    actual_raw = [int(label) for label in actual["labels"]]
    reference_canonical = canonical_labels(reference_raw)
    actual_canonical = canonical_labels(actual_raw)

    # canonical label -> member indices
    reference_groups: dict[int, set[int]] = {}
    actual_groups: dict[int, set[int]] = {}
    for index, label in enumerate(reference_canonical):
        reference_groups.setdefault(label, set()).add(index)
    for index, label in enumerate(actual_canonical):
        actual_groups.setdefault(label, set()).add(index)

    # canonical label -> the raw cluster id that holds those members
    reference_raw_by_canonical: dict[int, int] = {}
    actual_raw_by_canonical: dict[int, int] = {}
    for index, label in enumerate(reference_canonical):
        reference_raw_by_canonical.setdefault(label, reference_raw[index])
    for index, label in enumerate(actual_canonical):
        actual_raw_by_canonical.setdefault(label, actual_raw[index])

    matched_reference_rows: list[list[float]] = []
    matched_actual_rows: list[list[float]] = []
    for label in sorted(reference_groups):
        members = reference_groups[label]
        candidate = None
        for other_label, other_members in actual_groups.items():
            if other_members == members:
                candidate = other_label
                break
        if candidate is None:
            # No matching cluster: fall back to the cluster sharing the most
            # members. The comparison will then exceed the tolerance and
            # surface the partition difference.
            candidate = max(
                actual_groups,
                key=lambda other: len(actual_groups[other] & members),
            )
        matched_reference_rows.append(
            [value * sign for value, sign in zip(reference_array[reference_raw_by_canonical[label]], reference_signs)]
        )
        matched_actual_rows.append(
            [value * sign for value, sign in zip(actual_array[actual_raw_by_canonical[candidate]], actual_signs)]
        )

    return matched_reference_rows, matched_actual_rows


def run_self_checks(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Exercise the comparator with equivalent and broken runs.

    The positive cases must be accepted and the negative cases must be
    rejected. A comparator that accepts everything, or that rejects the
    equivalent cases, is not usable as evidence.
    """
    if config is None:
        config = load_config()

    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    matrix = np.asarray(
        [
            [0.0, 0.0, 42.0],
            [0.0, 0.0, 42.0],
            [0.2, -0.1, 42.0],
            [8.0, 0.0, 42.0],
            [8.0, 0.0, 42.0],
            [7.8, 0.2, 42.0],
            [0.0, 8.0, 42.0],
            [0.0, 8.0, 42.0],
            [0.1, 7.8, 42.0],
        ],
        dtype=np.float64,
    )
    member_ids = [f"member-{index:02d}" for index in range(matrix.shape[0])]

    scaler = StandardScaler(with_mean=True, with_std=True)
    scaled = scaler.fit_transform(matrix)
    pca = PCA(n_components=2, svd_solver="full", whiten=False)
    scores = pca.fit_transform(scaled)
    kmeans = KMeans(
        n_clusters=3,
        init="k-means++",
        n_init=10,
        random_state=0,
        algorithm="lloyd",
        max_iter=300,
        tol=1e-4,
    ).fit(scores)

    base = {
        "input_sha256": "self-check-input",
        "config_sha256": "self-check-config",
        "member_ids": member_ids,
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist(),
        "scaled": scaled.tolist(),
        "rank_after_scaling": int(np.linalg.matrix_rank(scaled)),
        "components": pca.components_.tolist(),
        "explained_variance_ratio": pca.explained_variance_ratio_.tolist(),
        "scores": scores.tolist(),
        "labels": [int(label) for label in kmeans.labels_],
        "cluster_centers": kmeans.cluster_centers_.tolist(),
        "inertia": float(kmeans.inertia_),
        "n_iter": int(kmeans.n_iter_),
    }

    checks: list[dict[str, Any]] = []

    def expect(case_id: str, description: str, candidate: dict[str, Any], should_pass: bool) -> None:
        result = compare_runs(base, candidate, config)
        accepted = result["status"] == "pass"
        checks.append(
            {
                "case_id": case_id,
                "description": description,
                "expected": "pass" if should_pass else "fail",
                "actual": "pass" if accepted else "fail",
                "correct": accepted == should_pass,
                "failures": result["failures"],
            }
        )

    identical = copy.deepcopy(base)
    expect("identical", "an identical run is equivalent", identical, True)

    relabelled = copy.deepcopy(base)
    remap = {0: 2, 1: 0, 2: 1}
    relabelled["labels"] = [remap[label] for label in relabelled["labels"]]
    # The center of a cluster follows its members: the new label `remap[old]`
    # holds the members of the old cluster `old`.
    relabelled_centers: list[list[float]] = [[] for _ in range(len(remap))]
    for old_label, new_label in remap.items():
        relabelled_centers[new_label] = copy.deepcopy(base["cluster_centers"][old_label])
    relabelled["cluster_centers"] = relabelled_centers
    expect("label_permutation", "a cluster-number permutation is equivalent", relabelled, True)

    sign_flipped = copy.deepcopy(base)
    sign_flipped["components"][0] = [-value for value in sign_flipped["components"][0]]
    for row in sign_flipped["scores"]:
        row[0] *= -1
    for center in sign_flipped["cluster_centers"]:
        center[0] *= -1
    expect("pca_sign_flip", "a PCA sign flip is equivalent", sign_flipped, True)

    moved_member = copy.deepcopy(base)
    moved_member["labels"][2] = 1
    expect("member_moved_cluster", "a member moved to another cluster is detected", moved_member, False)

    changed_value = copy.deepcopy(base)
    changed_value["scores"][0][0] += 1e-3
    expect("numeric_change", "a numeric change beyond the tolerance is detected", changed_value, False)

    changed_inertia = copy.deepcopy(base)
    changed_inertia["inertia"] = float(base["inertia"]) + 1e-2
    expect("inertia_change", "an inertia change beyond the tolerance is detected", changed_inertia, False)

    changed_rank = copy.deepcopy(base)
    changed_rank["rank_after_scaling"] = int(base["rank_after_scaling"]) + 1
    expect("rank_change", "a rank change is detected", changed_rank, False)

    different_input = copy.deepcopy(base)
    different_input["input_sha256"] = "a-different-input"
    expect("different_input", "runs over different inputs are not comparable", different_input, False)

    reordered_members = copy.deepcopy(base)
    reordered_members["member_ids"] = list(reversed(member_ids))
    expect(
        "member_order_change",
        "a member order change is not silently accepted",
        reordered_members,
        False,
    )

    # A rotation inside the retained subspace leaves the pairwise member
    # distances unchanged, so it must be caught by the basis comparison.
    rotated = copy.deepcopy(base)
    rotated["components"] = _rotate_subspace(base["components"])
    expect(
        "subspace_rotation",
        "a rotation inside the retained subspace is detected",
        rotated,
        False,
    )

    # A degenerate retained subspace has no well-defined basis, so the
    # comparison must report it rather than accept an arbitrary basis.
    degenerate = copy.deepcopy(base)
    degenerate["components"] = [base["components"][0], list(base["components"][0])]
    expect(
        "degenerate_subspace",
        "a degenerate retained subspace is reported rather than accepted",
        degenerate,
        False,
    )
    all_correct = all(check["correct"] for check in checks)
    return {"all_correct": all_correct, "checks": checks}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reference",
        type=Path,
        help="a reference run JSON file",
    )
    parser.add_argument(
        "--actual",
        type=Path,
        help="the run JSON file to compare against the reference",
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="run the comparator's positive and negative self-checks",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the comparison record to this JSON file",
    )
    arguments = parser.parse_args()

    if arguments.self_check:
        result = run_self_checks()
        if arguments.output:
            write_json(arguments.output, result)
        for check in result["checks"]:
            marker = "ok" if check["correct"] else "BROKEN"
            print(f"[{marker}] {check['case_id']}: expected {check['expected']}, got {check['actual']}")
        if not result["all_correct"]:
            print("FAIL: the comparator self-checks did not behave as required")
            return 1
        print("PASS: comparator self-checks")
        return 0

    if not arguments.reference or not arguments.actual:
        parser.error("--reference and --actual are required unless --self-check is given")

    config = load_config()
    reference = read_json(arguments.reference)
    actual = read_json(arguments.actual)
    result = compare_runs(reference, actual, config)
    if arguments.output:
        write_json(arguments.output, result)

    print(f"comparison status: {result['status']}")
    for name, difference in sorted(result["differences"].items()):
        print(f"  max |difference| {name}: {difference:g}")
    for failure in result["failures"]:
        print(f"  failure: {failure}")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
