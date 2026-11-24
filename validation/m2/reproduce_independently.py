#!/usr/bin/env python3
"""Independent reproduction harness for M2 scenario tracking (T049, SC-008).

Usage:
    Parent mode:
        python3 -B validation/m2/reproduce_independently.py --run-dir DIR [--work-dir DIR]
    Child mode:
        python3 -B validation/m2/reproduce_independently.py --reproduction-child \
            --output-dir DIR --source-manifest PATH --sample-manifest PATH \
            --experiment-config PATH --duckomo-target PATH --duckomo-baseline PATH
    Self-check:
        python3 -B validation/m2/reproduce_independently.py --self-check
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import atomic_write_json, load_json, sha256_file


def run_child(
    output_dir: Path,
    source_manifest: Path,
    sample_manifest: Path,
    experiment_config: Path,
    duckomo_target: Path,
    duckomo_baseline: Path,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    venv_py = REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"

    # 1. verify_matching_core
    print("[child] Running verify_matching_core...")
    core_dir = output_dir / "core"
    cmd_core = [str(venv_py), "-B", str(REPO_ROOT / "validation" / "m2" / "verify_matching_core.py"), "--run-dir", str(output_dir)]
    proc = subprocess.run(cmd_core, cwd=REPO_ROOT, capture_output=True, text=True)
    assert proc.returncode == 0, f"Child core matching failed: {proc.stderr}"

    # 2. verify_om_paths --stage cells
    print("[child] Running verify_om_paths --stage cells...")
    cmd_cells = [str(venv_py), "-B", str(REPO_ROOT / "validation" / "m2" / "verify_om_paths.py"), "--run-dir", str(output_dir), "--stage", "cells"]
    proc = subprocess.run(cmd_cells, cwd=REPO_ROOT, capture_output=True, text=True)
    assert proc.returncode == 0, f"Child cells check failed: {proc.stderr}"

    # 3. verify_sql_and_history --scope real
    print("[child] Running verify_sql_and_history --scope real...")
    cmd_sql = [sys.executable, "-B", str(REPO_ROOT / "validation" / "m2" / "verify_sql_and_history.py"), "--run-dir", str(output_dir), "--scope", "real"]
    proc = subprocess.run(cmd_sql, cwd=REPO_ROOT, capture_output=True, text=True)
    assert proc.returncode == 0, f"Child real SQL replay failed: {proc.stderr}"

    # 4. verify_om_paths --stage replay
    print("[child] Running verify_om_paths --stage replay...")
    cmd_replay = [str(venv_py), "-B", str(REPO_ROOT / "validation" / "m2" / "verify_om_paths.py"), "--run-dir", str(output_dir), "--stage", "replay"]
    proc = subprocess.run(cmd_replay, cwd=REPO_ROOT, capture_output=True, text=True)
    assert proc.returncode == 0, f"Child replay comparison failed: {proc.stderr}"

    summary = {
        "schema_version": "m2.reproduction-child-summary.v1",
        "scope": "reproduction_subset",
        "status": "pass",
        "steps_completed": ["core", "cells", "real", "replay"],
        "duckomo_target": str(duckomo_target),
        "duckomo_baseline": str(duckomo_baseline),
    }
    atomic_write_json(output_dir / "reproduction_summary.json", summary)
    print("[child] Reproduction subset completed successfully.")
    return 0


def semantic_history(results: dict[str, Any], *, expected_run_key: str | None = None) -> dict[str, Any]:
    """Compare numeric results and birth keys after verifying run_key-derived branch_id prefixes."""
    import copy
    normalized = copy.deepcopy(results)
    expected_prefix = f"{expected_run_key[:12]}:" if expected_run_key else None
    for node in normalized["nodes"]:
        bid = node["branch_id"]
        if expected_prefix is not None:
            if not bid.startswith(expected_prefix):
                raise ValueError(
                    f"Forged or mismatched branch_id prefix: expected '{expected_prefix}', got '{bid}'"
                )
            node["branch_id"] = bid[len(expected_prefix):]
        else:
            if ":" not in bid:
                raise ValueError(f"Malformed branch_id without prefix: '{bid}'")
            node["branch_id"] = bid.split(":", 1)[-1]
    return normalized


def run_parent(run_dir: Path, work_dir: Path | None = None) -> int:
    run_dir = run_dir.resolve()
    baseline = load_json(run_dir / "sql-history/tracking-history.json")["history_summary"]
    repro_root = (work_dir or run_dir / "reproduction").resolve()
    checkout = repro_root / "checkout"
    if checkout.exists():
        raise FileExistsError(f"Reproduction checkout already exists: {checkout}")
    repro_root.mkdir(parents=True, exist_ok=True)
    # Clone the committed baseline, then overlay the complete effective source snapshot.
    # This includes uncommitted/new files under review without committing user work.
    subprocess.run(["git", "clone", "--no-hardlinks", str(REPO_ROOT), str(checkout)], check=True,
                   capture_output=True, text=True)
    listed = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=REPO_ROOT)
    snapshot = {}
    for rel in sorted(set(listed.decode().split("\0")) - {""}):
        source = REPO_ROOT / rel
        target = checkout / rel
        if source.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            snapshot[rel] = sha256_file(source)
        elif target.is_file():
            target.unlink()
    atomic_write_json(repro_root / "effective-source.json", snapshot)
    child_dir = repro_root / "child"
    child_dir.mkdir()
    build_dir = child_dir / "build"
    # Source cache and frozen data are declared inputs, never copied build outputs.
    cache = REPO_ROOT / "validation/m2/.cache"
    sample = run_dir / "sample"
    if not sample.is_dir():
        sample = REPO_ROOT / "validation/m2/data/om_sample"
    python = REPO_ROOT / "validation/m2/.venv/bin/python"
    with (repro_root / "build.log").open("w", encoding="utf-8") as build_log:
        subprocess.run(
            ["bash", str(checkout / "validation/m2/build_extension.sh"), "--output-dir", str(build_dir)],
            cwd=checkout,
            check=True,
            stdout=build_log,
            stderr=subprocess.STDOUT,
        )
    scripts = checkout / "validation/m2"
    commands = [
        [str(python), str(scripts / "verify_matching_core.py"), "--run-dir", str(child_dir),
         "--runner", str(build_dir / "m2_runner")],
        [str(python), str(scripts / "verify_om_paths.py"), "--run-dir", str(child_dir), "--stage", "cells",
         "--sample-dir", str(sample), "--cache-dir", str(cache)],
        [str(python), str(scripts / "verify_sql_and_history.py"), "--run-dir", str(child_dir), "--scope", "real",
         "--extension", str(build_dir / "ensabc_m0_extension.duckdb_extension"),
         "--sample-dir", str(sample), "--cache-dir", str(cache)],
        [str(python), str(scripts / "verify_om_paths.py"), "--run-dir", str(child_dir), "--stage", "replay",
         "--sample-dir", str(sample), "--cache-dir", str(cache)],
    ]
    for index, command in enumerate(commands):
        with (repro_root / f"step-{index}.log").open("w", encoding="utf-8") as log:
            subprocess.run(command, cwd=checkout, check=True, stdout=log, stderr=subprocess.STDOUT)
    reproduced = load_json(child_dir / "sql-history/tracking-history.json")["history_summary"]
    base_sem = semantic_history(baseline["results"], expected_run_key=baseline["run_key"])
    repro_sem = semantic_history(reproduced["results"], expected_run_key=reproduced["run_key"])
    if base_sem != repro_sem:
        raise AssertionError("Independent rebuild changed scenario, distance, association or identity history")
    evidence = {
        "schema_version": "m2.independent-reproduction.v1", "status": "pass", "equivalence": True,
        "fresh_checkout": str(checkout), "source_manifest_sha256": sha256_file(repro_root / "effective-source.json"),
        "baseline_extension_sha256": baseline["binding"]["software"]["ensabc_extension_sha256"],
        "rebuilt_extension_sha256": reproduced["binding"]["software"]["ensabc_extension_sha256"],
        "comparison": "all scenario fields, pair diagnostics, link fields and birth keys",
        "baseline_summary": {k: baseline[k] for k in ("batches_count", "nodes_count", "pairs_count", "links_count")},
        "reproduced_summary": {k: reproduced[k] for k in ("batches_count", "nodes_count", "pairs_count", "links_count")},
    }
    atomic_write_json(run_dir / "independent-reproduction.json", evidence)
    return 0


def self_check() -> int:
    import unittest
    suite = unittest.defaultTestLoader.loadTestsFromName("validation.m2.test_review_regressions")
    return 0 if unittest.TextTestRunner().run(suite).wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="M2 independent reproduction harness")
    parser.add_argument("--run-dir", type=Path, default=None, help="Run directory")
    parser.add_argument("--work-dir", type=Path, default=None, help="Optional external work directory")
    parser.add_argument("--reproduction-child", action="store_true", help="Execute child mode")
    parser.add_argument("--output-dir", type=Path, default=None, help="Child output directory")
    parser.add_argument("--source-manifest", type=Path, default=None)
    parser.add_argument("--sample-manifest", type=Path, default=None)
    parser.add_argument("--experiment-config", type=Path, default=None)
    parser.add_argument("--duckomo-target", type=Path, default=None)
    parser.add_argument("--duckomo-baseline", type=Path, default=None)
    parser.add_argument("--self-check", action="store_true", help="Run self-check")
    args = parser.parse_args()

    if args.self_check:
        return self_check()

    if args.reproduction_child:
        if not args.output_dir:
            print("Error: --output-dir required for child mode", file=sys.stderr)
            return 2
        return run_child(
            args.output_dir,
            args.source_manifest,
            args.sample_manifest,
            args.experiment_config,
            args.duckomo_target,
            args.duckomo_baseline,
        )

    if not args.run_dir:
        print("Error: --run-dir required for parent mode", file=sys.stderr)
        return 2

    return run_parent(args.run_dir, args.work_dir)


if __name__ == "__main__":
    sys.exit(main())
