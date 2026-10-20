#!/usr/bin/env python3
"""Run the M0 numerical reference baseline in three independent processes.

The reference pipeline is fixed by `validation/m0/fixtures/reference_config.json`:

    StandardScaler(with_mean=True, with_std=True)
      -> PCA(n_components=2, svd_solver="full", whiten=False)
      -> KMeans(n_clusters=3, init="k-means++", n_init=10, random_state=0)

Each of the three runs is a separate operating-system process. Calling the
pipeline three times inside one process is not an acceptable substitute: the
evidence contract requires three independent processes, and the recorded pids
must be distinct.

The runner refuses to overwrite an existing run whose identity differs, so a
changed input or configuration never silently replaces earlier evidence.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    FIXTURES_DIR,
    REPO_ROOT,
    M0Error,
    read_json,
    sha256_bytes,
    sha256_file,
    sha256_json,
    utc_now,
    write_json,
)
from compare_reference import compare_runs, load_config  # noqa: E402
from schema import validate_sample_manifest, validate_sample_payload  # noqa: E402

MANIFEST_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_manifest.json"
PAYLOAD_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_data.json"
CONFIG_PATH = FIXTURES_DIR / "reference_config.json"

WORKER_MARKER = "--worker"


def _load_input(
    manifest_path: Path,
    payload_path: Path,
) -> tuple[dict[str, Any], list[str], list[list[float]]]:
    """Load and validate the frozen input for the reference pipeline."""
    manifest = read_json(manifest_path)
    validate_sample_manifest(manifest)
    payload = read_json(payload_path)
    validate_sample_payload(payload, manifest)

    member_ids = list(payload["member_ids"])
    matrix = [[float(value) for value in row] for row in payload["values"]]
    return manifest, member_ids, matrix


def _resolve_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate that the locked configuration is internally consistent."""
    if config.get("dtype") != "float64":
        raise M0Error(f"the reference configuration must use float64, found {config.get('dtype')!r}")
    if config["pca"]["n_components"] != 2:
        raise M0Error("the reference configuration must use a two-dimensional projection")
    if config["kmeans"]["n_clusters"] != 3:
        raise M0Error("the reference configuration must use three clusters")
    return config


def run_pipeline(
    matrix: list[list[float]],
    member_ids: list[str],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Run the reference pipeline on a validated matrix.

    The input policy is enforced here as well, so an ineligible input is
    rejected with a stated reason instead of producing a partial result.
    """
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    validity = config["input"]["requirements"]
    kmeans_config = config["kmeans"]

    try:
        values = np.asarray(matrix, dtype=np.float64)
    except (TypeError, ValueError) as error:
        raise M0Error(f"the input matrix must be a rectangular numeric 2-D array: {error}") from error
    if values.ndim != 2 or values.size == 0:
        raise M0Error("the input matrix must be a non-empty 2-D array")
    if not np.isfinite(values).all():
        raise M0Error("the input matrix contains a non-finite value")
    if len(member_ids) != values.shape[0]:
        raise M0Error("the member count does not match the number of matrix rows")
    if len(set(member_ids)) != len(member_ids):
        raise M0Error("the member identities are not unique")
    if values.shape[0] < validity["minimum_members"]:
        raise M0Error(
            f"at least {validity['minimum_members']} members are required, found {values.shape[0]}"
        )
    if values.shape[0] < kmeans_config["n_clusters"]:
        raise M0Error(
            f"the member count {values.shape[0]} is below the cluster count {kmeans_config['n_clusters']}"
        )
    distinguishable = int(np.unique(values, axis=0).shape[0])
    if distinguishable < validity["minimum_distinguishable_member_vectors"]:
        raise M0Error(
            "the number of distinguishable member vectors "
            f"({distinguishable}) is below the required {validity['minimum_distinguishable_member_vectors']}"
        )

    scaler = StandardScaler(
        with_mean=config["standard_scaler"]["with_mean"],
        with_std=config["standard_scaler"]["with_std"],
    )
    scaled = scaler.fit_transform(values)

    rank_threshold = float(config["validity"]["rank_threshold"])
    singular_values = np.linalg.svd(scaled, compute_uv=False)
    significant = singular_values[singular_values > rank_threshold * max(singular_values[0], 1.0)]
    rank_after_scaling = int(significant.size)
    if rank_after_scaling < config["validity"]["minimum_rank_after_scaling"]:
        raise M0Error(
            "the standardized matrix rank "
            f"({rank_after_scaling}) is below the required "
            f"{config['validity']['minimum_rank_after_scaling']}"
        )

    pca = PCA(
        n_components=config["pca"]["n_components"],
        svd_solver=config["pca"]["svd_solver"],
        whiten=config["pca"]["whiten"],
    )
    scores = pca.fit_transform(scaled)

    kmeans = KMeans(
        n_clusters=kmeans_config["n_clusters"],
        init=kmeans_config["init"],
        n_init=kmeans_config["n_init"],
        random_state=kmeans_config["random_state"],
        algorithm=kmeans_config["algorithm"],
        max_iter=kmeans_config["max_iter"],
        tol=kmeans_config["tol"],
    ).fit(scores)

    labels = [int(label) for label in kmeans.labels_]
    cluster_members = [
        [member for member, label in zip(member_ids, labels, strict=True) if label == cluster]
        for cluster in range(kmeans_config["n_clusters"])
    ]

    return {
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist(),
        "scaled": scaled.tolist(),
        "rank_after_scaling": rank_after_scaling,
        "components": pca.components_.tolist(),
        "explained_variance_ratio": pca.explained_variance_ratio_.tolist(),
        "scores": scores.tolist(),
        "labels": labels,
        "cluster_members": cluster_members,
        "cluster_centers": kmeans.cluster_centers_.tolist(),
        "inertia": float(kmeans.inertia_),
        "n_iter": int(kmeans.n_iter_),
        "distinguishable_member_vectors": distinguishable,
    }


def _worker(input_path: Path) -> int:
    """Execute one reference run in this process and print the result JSON."""
    from threadpoolctl import threadpool_limits

    payload = read_json(input_path)
    config = payload["config"]
    started_at = utc_now()
    started = time.perf_counter()
    with threadpool_limits(limits=config["execution"]["thread_limit"]):
        result = run_pipeline(payload["matrix"], payload["member_ids"], config)
    elapsed = time.perf_counter() - started
    result["worker_pid"] = os.getpid()
    result["started_at"] = started_at
    result["finished_at"] = utc_now()
    result["wall_clock_seconds"] = elapsed
    result["steps"] = [
        "StandardScaler(with_mean=True, with_std=True).fit_transform",
        "PCA(n_components=2, svd_solver='full', whiten=False).fit_transform",
        "KMeans(n_clusters=3, init='k-means++', n_init=10, random_state=0, algorithm='lloyd').fit",
    ]
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


def _environment_record(config: dict[str, Any]) -> dict[str, Any]:
    """Record the interpreter, the library versions and the thread setting."""
    import numpy
    import scipy
    import sklearn

    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform_machine": platform.machine(),
        "platform_system": platform.system(),
        "numpy": numpy.__version__,
        "scipy": scipy.__version__,
        "scikit_learn": sklearn.__version__,
        "thread_limit": config["execution"]["thread_limit"],
        "expected_environment": config["environment"],
    }


def _check_environment(config: dict[str, Any]) -> list[str]:
    """Return the deviations between the running and the declared environment."""
    import numpy
    import scipy
    import sklearn

    expected = config["environment"]
    actual = {
        "python": platform.python_version(),
        "numpy": numpy.__version__,
        "scipy": scipy.__version__,
        "scikit_learn": sklearn.__version__,
    }
    deviations = []
    for name, expected_value in expected.items():
        if actual.get(name) != expected_value:
            deviations.append(f"{name}: expected {expected_value}, found {actual.get(name)}")
    return deviations


def run_reference(
    manifest_path: Path,
    payload_path: Path,
    config_path: Path,
    output_dir: Path | None,
) -> dict[str, Any]:
    """Run the three independent reference processes and compare the results."""
    config = _resolve_config(load_config(config_path))
    manifest, member_ids, matrix = _load_input(manifest_path, payload_path)

    input_payload = {
        "sample_id": manifest["sample_id"],
        "kind": manifest["kind"],
        "member_ids": member_ids,
        "matrix": matrix,
    }
    input_sha256 = sha256_json(input_payload)
    config_sha256 = sha256_file(config_path)

    deviations = _check_environment(config)
    if deviations:
        raise M0Error(
            "the running environment does not match the locked reference configuration:\n  "
            + "\n  ".join(deviations)
        )

    import tempfile

    with tempfile.TemporaryDirectory(prefix="ensabc-m0-reference-") as temporary:
        worker_input = Path(temporary) / "worker-input.json"
        write_json(worker_input, {"config": config, "member_ids": member_ids, "matrix": matrix})

        command = [sys.executable, str(Path(__file__).resolve()), WORKER_MARKER, str(worker_input)]
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = "0"

        processes = [
            subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
            )
            for _ in range(config["execution"]["independent_processes"])
        ]

        runs: list[dict[str, Any]] = []
        pids: list[int] = []
        for index, process in enumerate(processes):
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                raise M0Error(
                    f"reference worker {index + 1} failed ({process.returncode}): {stderr.strip()}"
                )
            result = json.loads(stdout)
            pids.append(int(result.pop("worker_pid")))
            result["member_ids"] = member_ids
            result["input_sha256"] = input_sha256
            result["config_sha256"] = config_sha256
            runs.append(result)

    if len(set(pids)) != len(pids):
        raise M0Error(f"the reference runs were not independent processes: pids {pids}")

    comparisons = []
    for index, run in enumerate(runs[1:], start=2):
        comparison = compare_runs(runs[0], run, config)
        comparisons.append({"compared_run": index, **comparison})
        if comparison["status"] != "pass":
            raise M0Error(
                f"reference run {index} does not match reference run 1: {comparison['failures']}"
            )

    # The run files are written first, because their content hash can only be
    # computed once their content is final. Each run records the path and the
    # content hash of its own output file. The hash is of the file as written,
    # and the reference to the file is added to the separate run summary, so
    # that the run file does not have to contain its own hash.
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for index, run in enumerate(runs, start=1):
            run_path = output_dir / f"run-{index}.json"
            if run_path.exists():
                existing = read_json(run_path)
                if existing.get("input_sha256") != input_sha256 or existing.get(
                    "config_sha256"
                ) != config_sha256:
                    raise M0Error(
                        f"refusing to overwrite {run_path}: it holds a different evidence identity"
                    )
                if existing.get("status") != "pass":
                    raise M0Error(f"the existing run {run_path} is not a passing run")
            else:
                run["status"] = "pass"
                # The run records which file it will be written to. The
                # content hash of that file is recorded in the run summary,
                # because a file cannot contain its own hash.
                run["output_file"] = f"run-{index}.json"
                write_json(run_path, run)

    for index, run in enumerate(runs, start=1):
        run["output_file"] = f"run-{index}.json"
        if output_dir is not None:
            run["output_files"] = [
                {
                    "path": f"run-{index}.json",
                    "sha256": sha256_file(output_dir / f"run-{index}.json"),
                }
            ]

    environment = _environment_record(config)

    result = {
        "schema_version": "m0.reference-run.v1",
        "sample_id": manifest["sample_id"],
        "sample_kind": manifest["kind"],
        "input_sha256": input_sha256,
        "config_sha256": config_sha256,
        "config": config,
        "input": {
            "sample_id": manifest["sample_id"],
            "kind": manifest["kind"],
            "member_ids": member_ids,
            "shape": [len(member_ids), len(matrix[0]) if matrix else 0],
        },
        "independent_processes": {
            "count": len(pids),
            "distinct_pids_verified": True,
            "pids": pids,
        },
        "environment": environment,
        "comparisons": comparisons,
        "runs": runs,
        "status": "pass",
    }
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(WORKER_MARKER, action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("worker_input", nargs="?", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--payload", type=Path, default=PAYLOAD_PATH)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="directory that receives run-1.json, run-2.json and run-3.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the combined reference record to this JSON file",
    )
    arguments = parser.parse_args()

    if arguments.worker:
        if arguments.worker_input is None:
            parser.error("--worker requires the worker input path")
        return _worker(arguments.worker_input)

    try:
        result = run_reference(
            arguments.manifest,
            arguments.payload,
            arguments.config,
            arguments.output_dir,
        )
    except M0Error as error:
        print(f"FAIL: {error}")
        return 1

    if arguments.output:
        write_json(arguments.output, result)

    print("PASS reference baseline")
    print(f"  sample:    {result['sample_id']} (kind={result['sample_kind']})")
    print(f"  processes: {result['independent_processes']['count']} distinct pids {result['independent_processes']['pids']}")
    for index, run in enumerate(result["runs"], start=1):
        print(
            f"  run {index}: rank={run['rank_after_scaling']} "
            f"clusters={[len(group) for group in run['cluster_members']]} "
            f"inertia={run['inertia']!r} n_iter={run['n_iter']}"
        )
    for comparison in result["comparisons"]:
        worst = max(comparison["differences"].values()) if comparison["differences"] else 0.0
        print(f"  comparison run 1 vs {comparison['compared_run']}: status={comparison['status']} max|diff|={worst:g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
