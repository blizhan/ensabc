#!/usr/bin/env python3
"""Reproducible synthetic StandardScaler -> PCA(2) -> KMeans(3) spike."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import scipy
import sklearn
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits


MEMBERS = [f"member-{index:02d}" for index in range(9)]
MATRIX = [
    [0.0, 0.0, 42.0],
    [0.0, 0.0, 42.0],
    [0.2, -0.1, 42.0],
    [8.0, 0.0, 42.0],
    [8.0, 0.0, 42.0],
    [7.8, 0.2, 42.0],
    [0.0, 8.0, 42.0],
    [0.0, 8.0, 42.0],
    [0.1, 7.8, 42.0],
]
CLUSTERS = 3
ATOL = 1e-8
RTOL = 1e-6
NUMERIC_FIELDS = (
    "mean",
    "scale",
    "scaled",
    "components",
    "explained_variance_ratio",
    "scores",
    "inertia",
)


def _as_matrix(values: Any, members: list[str], clusters: int) -> np.ndarray:
    try:
        matrix = np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError) as error:
        raise ValueError("matrix must be a rectangular numeric 2D array") from error
    if matrix.ndim != 2 or matrix.size == 0:
        raise ValueError("matrix must be non-empty and 2D")
    if not np.isfinite(matrix).all():
        raise ValueError("matrix must contain only finite values")
    if len(members) != matrix.shape[0] or len(set(members)) != len(members):
        raise ValueError("member IDs must be unique and match matrix rows")
    if matrix.shape[0] < clusters:
        raise ValueError("member count must be at least the cluster count")
    if np.unique(matrix, axis=0).shape[0] < clusters:
        raise ValueError("unique member rows must be at least the cluster count")
    return matrix


def _canonical_labels(labels: list[int] | np.ndarray) -> list[int]:
    mapping: dict[int, int] = {}
    canonical: list[int] = []
    for label in labels:
        value = int(label)
        if value not in mapping:
            mapping[value] = len(mapping)
        canonical.append(mapping[value])
    return canonical


def _canonical_projection(
    components: list[list[float]] | np.ndarray,
    scores: list[list[float]] | np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    components_array = np.asarray(components, dtype=np.float64).copy()
    scores_array = np.asarray(scores, dtype=np.float64).copy()
    for axis, component in enumerate(components_array):
        pivot = int(np.argmax(np.abs(component)))
        if component[pivot] < 0:
            components_array[axis] *= -1
            scores_array[:, axis] *= -1
    return components_array, scores_array


def _compare_runs(left: dict[str, Any], right: dict[str, Any]) -> tuple[bool, dict[str, float]]:
    if (left["input_sha256"], left["config_sha256"]) != (
        right["input_sha256"],
        right["config_sha256"],
    ):
        return False, {}

    left_components, left_scores = _canonical_projection(left["components"], left["scores"])
    right_components, right_scores = _canonical_projection(right["components"], right["scores"])
    left_labels = _canonical_labels(left["labels"])
    right_labels = _canonical_labels(right["labels"])
    if left_labels != right_labels:
        return False, {}

    differences: dict[str, float] = {}
    fields = {
        "components": (left_components, right_components),
        "scores": (left_scores, right_scores),
    }
    fields.update(
        {
            key: (np.asarray(left[key], dtype=np.float64), np.asarray(right[key], dtype=np.float64))
            for key in NUMERIC_FIELDS
            if key not in {"components", "scores"}
        }
    )
    for key, (left_values, right_values) in fields.items():
        if left_values.shape != right_values.shape:
            return False, differences
        differences[key] = float(np.max(np.abs(left_values - right_values)))
        if not np.allclose(right_values, left_values, atol=ATOL, rtol=RTOL):
            return False, differences
    return True, differences


def _run_pipeline(values: Any, members: list[str]) -> dict[str, Any]:
    matrix = _as_matrix(values, members, CLUSTERS)
    scaler = StandardScaler(with_mean=True, with_std=True)
    scaled = scaler.fit_transform(matrix)
    rank = int(np.linalg.matrix_rank(scaled))
    if rank < 2:
        raise ValueError("standardized matrix rank must be at least 2 for PCA(2)")

    pca = PCA(n_components=2, svd_solver="full", whiten=False)
    scores = pca.fit_transform(scaled)
    kmeans = KMeans(
        n_clusters=CLUSTERS,
        init="k-means++",
        n_init=10,
        random_state=0,
        algorithm="lloyd",
        max_iter=300,
        tol=1e-4,
    ).fit(scores)
    components, canonical_scores = _canonical_projection(pca.components_, scores)
    labels = _canonical_labels(kmeans.labels_)

    return {
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist(),
        "scaled": scaled.tolist(),
        "rank_after_scaling": rank,
        "components": components.tolist(),
        "explained_variance_ratio": pca.explained_variance_ratio_.tolist(),
        "scores": canonical_scores.tolist(),
        "labels": labels,
        "cluster_members": [
            [member for member, label in zip(members, labels) if label == cluster]
            for cluster in range(CLUSTERS)
        ],
        "inertia": float(kmeans.inertia_),
    }


def _assert_rejected(name: str, values: Any, members: list[str]) -> str:
    try:
        _run_pipeline(values, members)
    except ValueError as error:
        return str(error)
    raise AssertionError(f"invalid case was accepted: {name}")


def _validate_input_policy() -> dict[str, Any]:
    invalid = {
        "empty": _assert_rejected("empty", np.empty((0, 3)), []),
        "ragged": _assert_rejected("ragged", [[1.0, 2.0], [3.0]], ["a", "b"]),
        "nonfinite": _assert_rejected(
            "nonfinite",
            [[0.0, 0.0], [1.0, 0.0], [0.0, np.inf]],
            ["a", "b", "c"],
        ),
        "rank_insufficient": _assert_rejected(
            "rank_insufficient",
            [[0.0, 0.0, 42.0], [1.0, 2.0, 42.0], [2.0, 4.0, 42.0], [3.0, 6.0, 42.0]],
            ["a", "b", "c", "d"],
        ),
        "unique_rows_below_k": _assert_rejected(
            "unique_rows_below_k",
            [[0.0, 0.0], [0.0, 0.0], [1.0, 1.0], [1.0, 1.0]],
            ["a", "b", "c", "d"],
        ),
    }
    return {"rejected_cases": invalid, "all_rejected": len(invalid) == 5}


def _worker() -> None:
    with threadpool_limits(limits=1):
        result = _run_pipeline(MATRIX, MEMBERS)
    result["worker_pid"] = os.getpid()
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _config(script_sha256: str) -> dict[str, Any]:
    return {
        "standard_scaler": {"with_mean": True, "with_std": True},
        "pca": {"n_components": 2, "svd_solver": "full", "whiten": False},
        "kmeans": {
            "n_clusters": CLUSTERS,
            "init": "k-means++",
            "n_init": 10,
            "random_state": 0,
            "algorithm": "lloyd",
            "max_iter": 300,
            "tol": 1e-4,
        },
        "comparison": {
            "absolute_tolerance": ATOL,
            "relative_tolerance": RTOL,
            "formula": "abs(actual-reference) <= 1e-8 + 1e-6*abs(reference)",
            "label_rule": "canonicalize by first member occurrence",
            "pca_sign_rule": "largest-absolute component loading is positive; flip scores with component",
        },
        "thread_limit": 1,
        "python": platform.python_version(),
        "architecture": platform.machine(),
        "numpy": np.__version__,
        "scipy": scipy.__version__,
        "scikit_learn": sklearn.__version__,
        "script_sha256": script_sha256,
    }


def _main(output: Path | None) -> None:
    valid_input = _run_pipeline(MATRIX, MEMBERS)
    assert len(MEMBERS) == len(valid_input["labels"]) == 9
    assert len(set(MATRIX[0])) > 0 and MATRIX[0] == MATRIX[1]
    assert sum(map(len, valid_input["cluster_members"])) == len(MEMBERS)
    assert all(len(group) > 0 for group in valid_input["cluster_members"])

    validation = _validate_input_policy()
    assert validation["all_rejected"]

    script_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    config = _config(script_sha256)
    config_bytes = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    config_sha256 = hashlib.sha256(config_bytes).hexdigest()
    input_payload = {"members": MEMBERS, "matrix": MATRIX, "sample_kind": "synthetic"}
    input_bytes = json.dumps(input_payload, sort_keys=True, separators=(",", ":")).encode()
    input_sha256 = hashlib.sha256(input_bytes).hexdigest()

    command = [sys.executable, str(Path(__file__).resolve()), "--worker"]
    environment = os.environ.copy()
    environment["PYTHONHASHSEED"] = "0"
    processes = [
        subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
        for _ in range(3)
    ]
    workers: list[dict[str, Any]] = []
    worker_pids: list[int] = []
    for process in processes:
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"baseline worker failed ({process.returncode}): {stderr.strip()}")
        run = json.loads(stdout)
        worker_pids.append(int(run.pop("worker_pid")))
        run["input_sha256"] = input_sha256
        run["config_sha256"] = config_sha256
        workers.append(run)
    assert len(set(worker_pids)) == 3, f"workers were not independent: {worker_pids}"

    comparisons = []
    for run in workers[1:]:
        equivalent, differences = _compare_runs(workers[0], run)
        assert equivalent, f"independent run mismatch: {differences}"
        comparisons.append(differences)

    permuted = copy.deepcopy(workers[0])
    remap = {0: 2, 1: 0, 2: 1}
    permuted["labels"] = [remap[label] for label in permuted["labels"]]
    assert _compare_runs(workers[0], permuted)[0], "label permutation was not treated as equivalent"

    sign_flipped = copy.deepcopy(workers[0])
    sign_flipped["components"][0] = [-value for value in sign_flipped["components"][0]]
    for row in sign_flipped["scores"]:
        row[0] *= -1
    assert _compare_runs(workers[0], sign_flipped)[0], "PCA sign flip was not treated as equivalent"

    moved_member = copy.deepcopy(workers[0])
    moved_member["labels"][2] = 1
    assert not _compare_runs(workers[0], moved_member)[0], "member reassignment was not detected"

    changed = copy.deepcopy(workers[0])
    changed["scores"][0][0] += 1e-3
    assert not _compare_runs(workers[0], changed)[0], "numeric change was not detected"

    result = {
        "schema_version": 1,
        "sample_kind": "synthetic_only_not_a_real_sample_gate",
        "evidence_id": hashlib.sha256(f"{input_sha256}:{config_sha256}".encode()).hexdigest(),
        "input_sha256": input_sha256,
        "config_sha256": config_sha256,
        "config": config,
        "input": input_payload,
        "independent_processes": {"count": 3, "distinct_pids_verified": True},
        "comparison": {
            "equivalent_runs": True,
            "run_differences": comparisons,
            "label_permutation_self_check": True,
            "member_moved_cluster_detected": True,
            "pca_sign_self_check": True,
            "numeric_change_detected": True,
        },
        "input_policy": validation,
        "runs": workers,
    }

    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        existing = json.loads(output.read_text(encoding="utf-8"))
        if existing.get("evidence_id") != result["evidence_id"]:
            raise FileExistsError(f"refusing to overwrite evidence with a different identity: {output}")
        same = len(existing.get("runs", [])) == 3 and all(
            _compare_runs(existing["runs"][0], run)[0]
            for run in result["runs"]
        )
        if not same:
            raise FileExistsError(f"existing evidence differs; choose a new output path: {output}")
        print(f"existing equivalent evidence retained: {output}")
        return

    with output.open("x", encoding="utf-8") as result_file:
        result_file.write(rendered)
    print(f"wrote evidence: {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--output", type=Path, help="write a new result JSON without replacing changed evidence")
    arguments = parser.parse_args()
    if arguments.worker:
        _worker()
    else:
        _main(arguments.output)


if __name__ == "__main__":
    main()
