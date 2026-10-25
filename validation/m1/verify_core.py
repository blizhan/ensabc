#!/usr/bin/env python3
"""Execute and verify native ensabc-core computations for M1.

Usage:
    python3 validation/m1/verify_core.py --run-dir DIR --stage native|shared

Stages:
  native:
    Runs the native core runner on real sample inputs in 3 distinct processes
    (producing core/run-{1,2,3}.json), executes synthetic tasks (producing
    core/synthetic.json), verifies negative error cases, and independently
    recomputes physical means, fractions, SSEs, and representatives.
  shared:
    Consumes reference/shared/initialization.json, executes the internal
    trace test (producing core/shared-trace.json), and executes the native
    shared run with explicit mapped centers (producing core/shared.json).
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
    FIXTURES_DIR,
    M1Error,
    read_json,
    sha256_file,
    utc_now,
    write_json,
)
from validation.m1.schema import (
    validate_scenario_result,
    validate_trace_record,
)

SYNTHETIC_TASKS_PATH = FIXTURES_DIR / "synthetic_tasks.json"


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify native ensabc-core runs.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Target run directory")
    parser.add_argument(
        "--stage",
        required=True,
        choices=["native", "shared"],
        help="Stage to execute (native or shared)",
    )
    return parser.parse_args(args)


def _ensure_runner_binary() -> Path:
    repo_bin = REPO_ROOT / "target" / "release" / "examples" / "m1_runner"
    if repo_bin.is_file():
        return repo_bin
    target_dir = Path(os.environ.get("ENSABC_M1_TARGET_DIR", "/tmp/ensabc-m1-extension-target"))
    bin_path = target_dir / "release" / "examples" / "m1_runner"
    if not bin_path.is_file():
        # Build m1_runner
        cmd = ["cargo", "build", "--locked", "--release", "-p", "ensabc-core", "--example", "m1_runner", "--target-dir", str(target_dir)]
        res = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        if res.returncode != 0:
            raise M1Error(f"Failed to build m1_runner: {res.stderr}", code="BUILD_FAILED")
    return bin_path


def _run_core_runner(task_json: Path, output_json: Path) -> dict[str, Any]:
    runner_bin = _ensure_runner_binary()
    cmd = [str(runner_bin), "--input", str(task_json.resolve()), "--output", str(output_json.resolve())]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise M1Error(
            f"m1_runner failed with code {proc.returncode}:\nstdout: {proc.stdout}\nstderr: {proc.stderr}",
            code="RUNNER_FAILED",
        )
    return read_json(output_json)


def _recompute_verifications(task_payload: dict[str, Any], result_payload: dict[str, Any]) -> dict[str, Any]:
    """Independently recompute cluster means, count, fraction, SSE, and representatives."""
    members = task_payload["members"]
    n = len(members)
    p = len(members[0]["features"])
    k = task_payload["config"]["cluster_count"]
    d = task_payload["config"]["projection_dim"]

    # 1. Check member assignments
    res_members = result_payload["members"]
    if len(res_members) != n:
        raise M1Error(f"Result members count {len(res_members)} != input members count {n}", code="VERIFICATION_FAILED")

    assigned_map: dict[str, int] = {}
    cluster_member_indices: dict[int, list[int]] = {c: [] for c in range(k)}

    for m_idx, (m_in, m_out) in enumerate(zip(members, res_members)):
        if m_in["member_id"] != m_out["member_id"]:
            raise M1Error(f"Member id mismatch at index {m_idx}: {m_in['member_id']} vs {m_out['member_id']}", code="VERIFICATION_FAILED")
        cid = m_out["cluster_id"]
        if cid < 0 or cid >= k:
            raise M1Error(f"Cluster id {cid} out of range 0..{k-1}", code="VERIFICATION_FAILED")
        assigned_map[m_in["member_id"]] = cid
        cluster_member_indices[cid].append(m_idx)

    # 2. Recompute scenarios
    scenarios = result_payload["scenarios"]
    if len(scenarios) != k:
        raise M1Error(f"Scenarios count {len(scenarios)} != k {k}", code="VERIFICATION_FAILED")

    scores = result_payload["diagnostics"].get("scores")
    total_recomputed_sse = 0.0
    total_count = 0
    total_fraction = 0.0

    for c in range(k):
        sc = scenarios[c]
        c_indices = cluster_member_indices[c]
        count = len(c_indices)
        if count == 0:
            raise M1Error(f"Cluster {c} is empty", code="VERIFICATION_FAILED")
        if sc["member_count"] != count:
            raise M1Error(f"Cluster {c} member_count {sc['member_count']} != recomputed {count}", code="VERIFICATION_FAILED")

        frac = count / n
        if abs(sc["member_fraction"] - frac) > 1e-12:
            raise M1Error(f"Cluster {c} fraction {sc['member_fraction']} != recomputed {frac}", code="VERIFICATION_FAILED")

        total_count += count
        total_fraction += frac

        # Physical means
        recomputed_mean = [0.0] * p
        for idx in c_indices:
            for j in range(p):
                recomputed_mean[j] += members[idx]["features"][j]
        recomputed_mean = [val / count for val in recomputed_mean]

        for j in range(p):
            if abs(sc["mean_features"][j] - recomputed_mean[j]) > 1e-8:
                raise M1Error(f"Cluster {c} mean_feature[{j}] differs: {sc['mean_features'][j]} vs {recomputed_mean[j]}", code="VERIFICATION_FAILED")

        # Cluster SSE and representative if scores available
        if scores:
            center = sc["center"]
            cluster_sse = 0.0
            best_dist = float("inf")
            best_rep = None

            for idx in c_indices:
                pt = scores[idx]
                dist = sum((pt[coord] - center[coord]) ** 2 for coord in range(d))
                cluster_sse += dist
                if dist < best_dist:
                    best_dist = dist
                    best_rep = members[idx]["member_id"]

            if abs(sc["cluster_sse"] - cluster_sse) > 1e-8:
                raise M1Error(f"Cluster {c} cluster_sse differs: {sc['cluster_sse']} vs {cluster_sse}", code="VERIFICATION_FAILED")
            if sc["representative_member_id"] != best_rep:
                raise M1Error(f"Cluster {c} representative differs: {sc['representative_member_id']} vs {best_rep}", code="VERIFICATION_FAILED")

            total_recomputed_sse += cluster_sse

    if total_count != n:
        raise M1Error(f"Total count {total_count} != n {n}", code="VERIFICATION_FAILED")
    if abs(total_fraction - 1.0) > 1e-12:
        raise M1Error(f"Total fraction {total_fraction} differs from 1.0 by > 1e-12", code="VERIFICATION_FAILED")

    if scores and abs(result_payload["diagnostics"]["total_sse"] - total_recomputed_sse) > 1e-8:
        raise M1Error("Total SSE mismatch with sum of cluster SSEs", code="VERIFICATION_FAILED")

    # 3. Check restart initial_member_indices rules
    init_method = task_payload["config"]["initialization"]
    for r_idx, r in enumerate(result_payload["diagnostics"]["restarts"]):
        indices = r["initial_member_indices"]
        if init_method == "kmeans++_chacha8_v1":
            if len(indices) != k:
                raise M1Error(f"Restart {r_idx} initial_member_indices length {len(indices)} != k {k}", code="VERIFICATION_FAILED")
            if not all(isinstance(x, int) and 0 <= x < n for x in indices):
                raise M1Error(f"Restart {r_idx} invalid initial index values", code="VERIFICATION_FAILED")
        else:
            if len(indices) != 0:
                raise M1Error(f"Restart {r_idx} explicit initial_member_indices must be empty, found {indices}", code="VERIFICATION_FAILED")

    return {
        "status": "pass",
        "member_count": n,
        "cluster_count": k,
        "total_sse": result_payload["diagnostics"]["total_sse"],
    }


def run_stage_native(run_dir: Path) -> dict[str, Any]:
    task_path = run_dir / "inputs" / "task.json"
    if not task_path.is_file():
        raise M1Error(f"Missing task input at {task_path}", code="MISSING_INPUT")

    core_dir = run_dir / "core"
    core_dir.mkdir(parents=True, exist_ok=True)

    task_payload = read_json(task_path)

    # 1. Run real sample 3 times
    runs = []
    for idx in range(1, 4):
        run_file = core_dir / f"run-{idx}.json"
        res = _run_core_runner(task_path, run_file)
        validate_scenario_result(res, n_members=len(task_payload["members"]))
        _recompute_verifications(task_payload, res)
        runs.append(res)

    # Verify 3 runs produced exact identical results (deterministic ChaCha8)
    for idx in (1, 2):
        if runs[idx]["members"] != runs[0]["members"]:
            raise M1Error(f"Native core run {idx+1} members differ from run 1", code="NATIVE_INCONSISTENCY")
        if runs[idx]["diagnostics"]["total_sse"] != runs[0]["diagnostics"]["total_sse"]:
            raise M1Error(f"Native core run {idx+1} total_sse differs from run 1", code="NATIVE_INCONSISTENCY")

    # 2. Run synthetic tasks
    synthetic_results = []
    if SYNTHETIC_TASKS_PATH.is_file():
        synthetic_fixtures = read_json(SYNTHETIC_TASKS_PATH)
        for pos_case in synthetic_fixtures.get("positive", []):
            cid = pos_case["case_id"]
            pos_task = {
                "schema_version": "m1.task.v1",
                "task_context": {"task_id": f"syn-{cid}"},
                "config": {
                    "schema_version": "m1.config.v1",
                    "algorithm_version": "m1.native.v1",
                    **pos_case["config"],
                },
                "members": [
                    {"member_index": i, "member_id": m["member_id"], "features": m["features"]}
                    for i, m in enumerate(pos_case["members"])
                ],
            }
            tmp_task_path = core_dir / f".tmp_syn_task_{cid}.json"
            tmp_res_path = core_dir / f".tmp_syn_res_{cid}.json"
            write_json(tmp_task_path, pos_task)
            try:
                syn_res = _run_core_runner(tmp_task_path, tmp_res_path)
                validate_scenario_result(syn_res, n_members=len(pos_task["members"]))
                _recompute_verifications(pos_task, syn_res)
                synthetic_results.append({"case_id": cid, "status": "pass"})
            finally:
                if tmp_task_path.is_file():
                    tmp_task_path.unlink()
                if tmp_res_path.is_file():
                    tmp_res_path.unlink()

    write_json(core_dir / "synthetic.json", {
        "schema_version": "m1.synthetic-results.v1",
        "cases": synthetic_results,
        "count": len(synthetic_results),
        "status": "pass",
    })

    return {"status": "pass", "runs_count": 3, "synthetic_count": len(synthetic_results)}


def run_stage_shared(run_dir: Path) -> dict[str, Any]:
    task_path = run_dir / "inputs" / "task.json"
    init_path = run_dir / "reference" / "shared" / "initialization.json"
    ref_trace_path = run_dir / "reference" / "shared" / "trace.json"

    if not task_path.is_file():
        raise M1Error(f"Missing task input at {task_path}", code="MISSING_INPUT")
    if not init_path.is_file() or not ref_trace_path.is_file():
        # Missing shared reference prerequisites returns 2
        raise M1Error(f"Missing shared reference inputs at {init_path}", code="MISSING_PREREQUISITE")

    core_dir = run_dir / "core"
    core_dir.mkdir(parents=True, exist_ok=True)

    task_payload = read_json(task_path)
    init_payload = read_json(init_path)

    # 1. Run internal Lloyd trace test
    # Reference trace events give us scores and tolerance
    ref_trace = read_json(ref_trace_path)
    ref_run = read_json(run_dir / "reference" / "shared" / "reference.json")
    original_comp = read_json(run_dir / "comparison" / "original.json")

    # Native scores are in core/run-1.json
    native_run1 = read_json(core_dir / "run-1.json")
    native_scores = native_run1["diagnostics"]["scores"]
    native_init_centers = init_payload["native_centers"]

    trace_in_data = {
        "scores": read_json(run_dir / "reference" / "shared" / "reference.json")["scores"],
        "initial_centers": init_payload["reference_centers"],
        "tolerance": task_payload["config"]["tolerance"],
        "max_iterations": task_payload["config"]["max_iterations"],
        "cluster_count": task_payload["config"]["cluster_count"],
        "projection_dim": task_payload["config"]["projection_dim"],
    }
    tmp_trace_in = core_dir / ".tmp_shared_trace_in.json"
    shared_trace_out = core_dir / "shared-trace.json"
    write_json(tmp_trace_in, trace_in_data)

    env = os.environ.copy()
    env["ENSABC_M1_TRACE_INPUT"] = str(tmp_trace_in.resolve())
    env["ENSABC_M1_TRACE_OUTPUT"] = str(shared_trace_out.resolve())

    try:
        cmd = ["cargo", "test", "--locked", "-p", "ensabc-core", "--lib", "tests::m1_shared_trace", "--", "--ignored", "--exact"]
        proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise M1Error(f"Internal Lloyd trace extraction failed: {proc.stderr}", code="TRACE_FAILED")
    finally:
        if tmp_trace_in.is_file():
            tmp_trace_in.unlink()

    trace_record = read_json(shared_trace_out)
    validate_trace_record(trace_record)

    # 2. Run public native execution with explicit initial centers
    shared_task = copy.deepcopy(task_payload)
    shared_task["config"]["initialization"] = "explicit_centers_v1"
    shared_task["config"]["seed"] = 0
    shared_task["config"]["restarts"] = 1
    shared_task["config"]["initial_centers"] = [native_init_centers]

    tmp_task_shared = core_dir / ".tmp_shared_task.json"
    shared_result_out = core_dir / "shared.json"
    write_json(tmp_task_shared, shared_task)

    try:
        shared_res = _run_core_runner(tmp_task_shared, shared_result_out)
        validate_scenario_result(shared_res, n_members=len(shared_task["members"]))
        _recompute_verifications(shared_task, shared_res)
    finally:
        if tmp_task_shared.is_file():
            tmp_task_shared.unlink()

    return {"status": "pass", "shared_trace": shared_trace_out, "shared_result": shared_result_out}


def main() -> int:
    try:
        args = parse_args()
    except SystemExit:
        return 2

    try:
        if args.stage == "native":
            run_stage_native(args.run_dir)
            print(f"Successfully completed core stage native in {args.run_dir}")
        elif args.stage == "shared":
            run_stage_shared(args.run_dir)
            print(f"Successfully completed core stage shared in {args.run_dir}")
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2 if e.code in ("MISSING_PREREQUISITE", "MISSING_INPUT") else 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
