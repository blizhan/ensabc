#!/usr/bin/env python3
"""Run reference baseline computations for M1 in original and shared stages.

Usage:
    python3 validation/m1/run_reference.py --run-dir DIR --stage original|shared

Stages:
  original:
    Executes the 3-process M0 reference pipeline using the configured reference
    interpreter in 3 independent subprocesses with distinct PIDs. Writes
    reference/original/run-{1,2,3}.json and reference/original/summary.json.
  shared:
    Consumes projection-alignment.json from original comparison. Derives shared
    starting centers from indices [0, n//3, 2*n//3]. Wraps
    _kmeans.lloyd_iter_chunked_dense to capture iteration trace, verifies
    observer parity against unwrapped execution, and writes
    reference/shared/initialization.json, reference/shared/reference.json,
    reference/shared/trace.json, and reference/shared/observer-check.json.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import (
    M1Error,
    read_json,
    sha256_file,
    sha256_json,
    utc_now,
    write_json,
)
from validation.m1.schema import (
    validate_observer_check,
    validate_trace_record,
)

REFERENCE_VENV = Path(os.environ.get("ENSABC_M0_REFERENCE_VENV", "/tmp/ensabc-m0-baseline"))
REFERENCE_PYTHON = REFERENCE_VENV / "bin" / "python"


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run M1 reference pipelines.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Target run directory")
    parser.add_argument(
        "--stage",
        required=True,
        choices=["original", "shared"],
        help="Stage to execute (original or shared)",
    )
    return parser.parse_args(args)


def _get_reference_interpreter() -> str:
    if REFERENCE_PYTHON.is_file() and os.access(REFERENCE_PYTHON, os.X_OK):
        return str(REFERENCE_PYTHON)
    if Path(sys.executable).is_file():
        return sys.executable
    raise M1Error(f"Reference Python interpreter not found at {REFERENCE_PYTHON}", code="INTERPRETER_NOT_FOUND")


# Worker snippet for original 3-process reference execution
ORIGINAL_WORKER_CODE = """
import sys, json, os
from pathlib import Path
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

task_path = Path(sys.argv[1])
task = json.loads(task_path.read_text(encoding="utf-8"))
members = task["members"]
member_ids = [m["member_id"] for m in members]
values = np.asarray([m["features"] for m in members], dtype=np.float64)

# 1. StandardScaler (with_mean=True, with_std=True, population ddof=0)
scaler = StandardScaler(with_mean=True, with_std=True)
scaled = scaler.fit_transform(values)

# 2. PCA (n_components=2, svd_solver="full", whiten=False)
pca = PCA(n_components=2, svd_solver="full", whiten=False)
scores = pca.fit_transform(scaled)

# Sign-normalize PCA components & scores
components = pca.components_.copy()
scores_aligned = scores.copy()
for i in range(2):
    max_idx = int(np.argmax(np.abs(components[i])))
    if components[i, max_idx] < 0:
        components[i] *= -1.0
        scores_aligned[:, i] *= -1.0

# 3. KMeans (n_clusters=3, init="k-means++", n_init=10, random_state=0)
kmeans = KMeans(n_clusters=3, init="k-means++", n_init=10, random_state=0, algorithm="lloyd", max_iter=300, tol=1e-4)
kmeans.fit(scores_aligned)
labels = kmeans.labels_.tolist()
centers = kmeans.cluster_centers_.tolist()

# Compute physical mean features per cluster
mean_features = []
for cid in range(3):
    mask = (kmeans.labels_ == cid)
    mean_features.append(np.mean(values[mask], axis=0).tolist())

output = {
    "pid": os.getpid(),
    "n_members": len(member_ids),
    "member_ids": member_ids,
    "mean": scaler.mean_.tolist(),
    "scale": scaler.scale_.tolist(),
    "scaled": scaled.tolist(),
    "rank_after_scaling": int(np.linalg.matrix_rank(scaled)),
    "components": components.tolist(),
    "explained_variance_ratio": pca.explained_variance_ratio_.tolist(),
    "scores": scores_aligned.tolist(),
    "labels": labels,
    "centers": centers,
    "inertia": float(kmeans.inertia_),
    "n_iter": int(kmeans.n_iter_),
    "mean_features": mean_features,
}
print(json.dumps(output))
"""


def run_stage_original(run_dir: Path) -> dict[str, Any]:
    task_path = run_dir / "inputs" / "task.json"
    if not task_path.is_file():
        raise M1Error(f"Missing task input at {task_path}", code="MISSING_INPUT")

    ref_dir = run_dir / "reference" / "original"
    ref_dir.mkdir(parents=True, exist_ok=True)

    interpreter = _get_reference_interpreter()
    runs = []
    pids = set()

    for idx in range(1, 4):
        run_file = ref_dir / f"run-{idx}.json"
        cmd = [interpreter, "-c", ORIGINAL_WORKER_CODE, str(task_path.resolve())]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise M1Error(f"Reference worker {idx} failed: {proc.stderr}", code="WORKER_FAILED")
        result = json.loads(proc.stdout.strip())
        pids.add(result["pid"])
        write_json(run_file, result)
        runs.append(result)

    if len(pids) != 3:
        raise M1Error(f"Reference runs must have 3 distinct PIDs, found {len(pids)}: {pids}", code="PID_COLLISION")

    # Verify 3 runs produced exact identical labels and consistent inertia within 1e-8
    for idx in (1, 2):
        if runs[idx]["labels"] != runs[0]["labels"]:
            raise M1Error(f"Reference run {idx+1} labels differ from run 1", code="REFERENCE_INCONSISTENCY")
        inertia_diff = abs(runs[idx]["inertia"] - runs[0]["inertia"])
        if inertia_diff > 1e-8:
            raise M1Error(f"Reference run {idx+1} inertia differs from run 1 by {inertia_diff} > 1e-8", code="REFERENCE_INCONSISTENCY")

    summary = {
        "status": "pass",
        "distinct_pids": sorted(list(pids)),
        "cluster_counts": [runs[0]["labels"].count(c) for c in range(3)],
        "inertia": runs[0]["inertia"],
        "n_iter": runs[0]["n_iter"],
        "created_at": utc_now(),
    }
    summary_path = ref_dir / "summary.json"
    write_json(summary_path, summary)

    return {"status": "pass", "summary": summary}


# Worker snippet for shared stage execution with observer wrapper
SHARED_WORKER_CODE = """
import sys, json, os
from pathlib import Path
import numpy as np
import sklearn
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import _kmeans, KMeans
from sklearn.cluster._kmeans import _kmeans_single_lloyd

task_path = Path(sys.argv[1])
alignment_path = Path(sys.argv[2])
initialization_out = Path(sys.argv[3])
reference_out = Path(sys.argv[4])
trace_out = Path(sys.argv[5])
observer_check_out = Path(sys.argv[6])

task = json.loads(task_path.read_text(encoding="utf-8"))
alignment = json.loads(alignment_path.read_text(encoding="utf-8"))
q_matrix = np.asarray(alignment["procrustes_q"], dtype=np.float64) # d x d

members = task["members"]
member_ids = [m["member_id"] for m in members]
values = np.asarray([m["features"] for m in members], dtype=np.float64)
n = len(members)

# Standardize and project to scores
scaler = StandardScaler(with_mean=True, with_std=True)
scaled = scaler.fit_transform(values)
pca = PCA(n_components=2, svd_solver="full", whiten=False)
scores = pca.fit_transform(scaled)

for i in range(2):
    max_idx = int(np.argmax(np.abs(pca.components_[i])))
    if pca.components_[i, max_idx] < 0:
        pca.components_[i] *= -1.0
        scores[:, i] *= -1.0
scores = np.ascontiguousarray(scores)

# Fixed initial member indices [0, floor(n/3), floor(2*n/3)]
start_indices = [0, int(n // 3), int(2 * n // 3)]
init_centers = np.ascontiguousarray(scores[start_indices].copy())

# Record initialization
init_record = {
    "schema_version": "m1.initialization.v1",
    "member_indices": start_indices,
    "reference_centers": init_centers.tolist(),
    "native_centers": (init_centers @ q_matrix.T).tolist(),
    "procrustes_q": q_matrix.tolist(),
}
initialization_out.parent.mkdir(parents=True, exist_ok=True)
initialization_out.write_text(json.dumps(init_record, indent=2) + "\\n", encoding="utf-8")

# 1. Unwrapped run for parity baseline
sample_weight = np.ones(n, dtype=np.float64)
labels_unwrapped, inertia_unwrapped, centers_unwrapped, n_iter_unwrapped = _kmeans_single_lloyd(
    scores,
    sample_weight=sample_weight,
    centers_init=init_centers.copy(),
    max_iter=300,
    verbose=False,
    tol=1e-4,
    n_threads=1,
)

# 2. Transparently wrapped kernel observation
original_lloyd_iter = _kmeans.lloyd_iter_chunked_dense
events = []

def observing_lloyd_iter(
    X, sample_weight, centers_old, centers_new, weight_in_clusters, labels, center_shift, n_threads, update_centers=True
):
    # Call original kernel exactly once
    original_lloyd_iter(
        X, sample_weight, centers_old, centers_new, weight_in_clusters, labels, center_shift, n_threads, update_centers=update_centers
    )
    # Observe and copy state
    it = len(events) + 1
    if update_centers:
        # Sum of squared center shifts
        shift_val = float(np.sum(center_shift ** 2))
        # Compute SSE with current labels and centers_new
        diff = X - centers_new[labels]
        sse_val = float(np.sum(diff ** 2))
        events.append({
            "iteration": it,
            "event_type": "update",
            "counts_as_iteration": True,
            "labels": labels.tolist(),
            "centers": centers_new.copy().tolist(),
            "sse": sse_val,
            "shift": shift_val,
        })
    else:
        # Final assignment-only step
        diff = X - centers_new[labels]
        sse_val = float(np.sum(diff ** 2))
        events.append({
            "iteration": it,
            "event_type": "assignment_only",
            "counts_as_iteration": False,
            "labels": labels.tolist(),
            "centers": centers_new.copy().tolist(),
            "sse": sse_val,
            "shift": None,
        })

_kmeans.lloyd_iter_chunked_dense = observing_lloyd_iter

try:
    labels_wrapped, inertia_wrapped, centers_wrapped, n_iter_wrapped = _kmeans_single_lloyd(
        scores,
        sample_weight=sample_weight,
        centers_init=init_centers.copy(),
        max_iter=300,
        verbose=False,
        tol=1e-4,
        n_threads=1,
    )
finally:
    _kmeans.lloyd_iter_chunked_dense = original_lloyd_iter

# Verify parity
exact_match = (
    np.array_equal(labels_unwrapped, labels_wrapped) and
    np.allclose(centers_unwrapped, centers_wrapped, atol=1e-12, rtol=1e-12) and
    abs(inertia_unwrapped - inertia_wrapped) <= 1e-12 and
    n_iter_unwrapped == n_iter_wrapped
)

update_events = [e for e in events if e["event_type"] == "update"]
assignment_events = [e for e in events if e["event_type"] == "assignment_only"]
update_match = (len(update_events) == n_iter_wrapped)

# Write observer check
observer_check = {
    "schema_version": "m1.observer-check.v1",
    "status": "pass" if (exact_match and update_match) else "fail",
    "kernel_symbol": "sklearn.cluster._kmeans.lloyd_iter_chunked_dense",
    "sklearn_version": sklearn.__version__,
    "exact_match": exact_match,
    "update_steps_match_n_iter": update_match,
    "assignment_only_steps": len(assignment_events),
}
observer_check_out.write_text(json.dumps(observer_check, indent=2) + "\\n", encoding="utf-8")

# Determine stop reason
stop_reason = "labels_stable"
if assignment_events:
    # If final shift was <= tolerance threshold
    stop_reason = "center_shift"

trace_record = {
    "schema_version": "m1.trace.v1",
    "events": events,
    "final_iteration_count": n_iter_wrapped,
    "stop_reason": stop_reason,
}
trace_out.write_text(json.dumps(trace_record, indent=2) + "\\n", encoding="utf-8")

# Reference output
ref_output = {
    "scores": scores.tolist(),
    "labels": labels_wrapped.tolist(),
    "centers": centers_wrapped.tolist(),
    "inertia": float(inertia_wrapped),
    "n_iter": int(n_iter_wrapped),
    "cluster_counts": [int(np.sum(labels_wrapped == c)) for c in range(3)],
}
reference_out.write_text(json.dumps(ref_output, indent=2) + "\\n", encoding="utf-8")

if not (exact_match and update_match):
    sys.exit(1)
"""


def run_stage_shared(run_dir: Path) -> dict[str, Any]:
    task_path = run_dir / "inputs" / "task.json"
    alignment_path = run_dir / "comparison" / "projection-alignment.json"

    if not task_path.is_file():
        raise M1Error(f"Missing task input at {task_path}", code="MISSING_INPUT")
    if not alignment_path.is_file():
        # Prerequisites missing returns exit code 2
        raise M1Error(f"Missing original projection alignment at {alignment_path}", code="MISSING_PREREQUISITE")

    shared_dir = run_dir / "reference" / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)

    init_out = shared_dir / "initialization.json"
    ref_out = shared_dir / "reference.json"
    trace_out = shared_dir / "trace.json"
    obs_out = shared_dir / "observer-check.json"

    interpreter = _get_reference_interpreter()
    cmd = [
        interpreter,
        "-c",
        SHARED_WORKER_CODE,
        str(task_path.resolve()),
        str(alignment_path.resolve()),
        str(init_out.resolve()),
        str(ref_out.resolve()),
        str(trace_out.resolve()),
        str(obs_out.resolve()),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise M1Error(f"Shared reference worker failed: {proc.stderr}", code="WORKER_FAILED")

    obs = read_json(obs_out)
    validate_observer_check(obs)
    trace = read_json(trace_out)
    validate_trace_record(trace)

    if obs.get("status") != "pass":
        raise M1Error("Observer check status is not pass", code="OBSERVER_CHECK_FAILED")

    return {"status": "pass", "observer_check": obs}


def main() -> int:
    try:
        args = parse_args()
    except SystemExit:
        return 2

    try:
        if args.stage == "original":
            run_stage_original(args.run_dir)
            print(f"Successfully completed reference stage original in {args.run_dir}")
        elif args.stage == "shared":
            run_stage_shared(args.run_dir)
            print(f"Successfully completed reference stage shared in {args.run_dir}")
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2 if e.code in ("MISSING_PREREQUISITE", "MISSING_INPUT", "INTERPRETER_NOT_FOUND") else 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
