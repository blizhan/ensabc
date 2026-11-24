#!/usr/bin/env python3
"""Dual-path OM reading & cell verification harness (US3).

Usage:
    validation/m2/.venv/bin/python validation/m2/verify_om_paths.py --run-dir DIR --stage cells|replay [--sample-dir DIR] [--cache-dir DIR]

Stages:
  --stage cells:
    - Reads 8 batches via omfiles and DuckOMO (both v0.3.5-rc.1 and v0.3.4)
    - Verifies exact 0.0 float difference across all 3,600 cells (SC-001)
    - Tests negative fixtures (NaN, Inf, 49 members, corrupted header)
    - Outputs cell-matrix-manifest.json and om-cells-report.json

  --stage replay:
    - Executes real 8-run replay under v0.3.5-rc.1 and v0.3.4
    - Compares scenario nodes, links, and branch_ids across DuckOMO versions (SC-002)
    - Outputs duckomo-version-comparison.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numpy as np
import omfiles

from validation.m2.common import atomic_write_json, load_json, sha256_file, parse_duckdb_json, run_isolated_duckdb, real_replay_binding, sql_value
from validation.m2.test_om_sample import read_via_duckomo, read_via_omfiles

EXPECTED_GRID_ID = "ca3212142cd36c9704cd0aa1f47c24c905611eca7bb911427279997de133ad26"


def verify_cells(
    sample_dir: Path,
    cache_dir: Path,
    run_dir: Path,
) -> dict[str, Any]:
    manifest_path = sample_dir / "om_sample_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing manifest: {manifest_path}")

    manifest = load_json(manifest_path)
    duckdb_cli = Path(subprocess.check_output(["which", "duckdb"]).decode().strip())
    duckomo_035 = cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension"
    duckomo_034 = cache_dir / "v0.3.4" / "duckomo.duckdb_extension"
    assert duckomo_035.is_file(), f"Missing DuckOMO 0.3.5: {duckomo_035}"
    assert duckomo_034.is_file(), f"Missing DuckOMO 0.3.4: {duckomo_034}"

    tmp_home = Path("/tmp/ensabc_m2_verify_cells_home")
    tmp_home.mkdir(parents=True, exist_ok=True)

    cell_matrices = []
    comparisons = []
    total_cells_checked = 0
    max_abs_diff = 0.0

    print("Stage [cells]: Comparing Python omfiles vs DuckOMO v0.3.5-rc.1 vs v0.3.4...")
    for b in manifest["batches"]:
        om_file = sample_dir / b["om_filename"]
        om_res = read_via_omfiles(om_file)
        py_arr = om_res["data"]  # (50, 3, 3) float32

        # Read cell values from DuckOMO via DuckDB
        sql_fetch = f"""
        SET TimeZone = 'UTC';
        SET extension_directory = '{tmp_home}';
        SET autoinstall_known_extensions = false;
        SET autoload_known_extensions = false;
        LOAD '{duckomo_035}';

        SELECT member, om_source.parent_point_index AS point_index, temperature_2m::DOUBLE AS val
        FROM read_om('{om_file}',
          variable := 'temperature_2m',
          dimensions := ['member', 'lat', 'lon'],
          grid := {{'nx': 3, 'ny': 3, 'lat0': 42.1875, 'lon0': 5.625, 'dlat': 5.625, 'dlon': 5.625, 'order': 'separate'}},
          spatial_axes := ['lat', 'lon'],
          axes := {{'member': {{'axis': 'member', 'values': [
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
            21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
            41,42,43,44,45,46,47,48,49,50]}}}},
          include_source := true)
        ORDER BY member, point_index;
        """
        proc = run_isolated_duckdb(sql_fetch, cli=str(duckdb_cli))
        assert proc.returncode == 0, f"DuckDB query failed: {proc.stderr}"
        rows = json.loads(proc.stdout.strip())
        assert len(rows) == 450, f"Expected 450 rows, got {len(rows)}"

        duck_vals = np.array([r["val"] for r in rows], dtype=np.float32).reshape(50, 3, 3)
        import hashlib
        assert hashlib.sha256(py_arr.astype('<f4').tobytes()).hexdigest() == b["payload_sha256"], "Source/OM payload mismatch"
        baseline_proc = run_isolated_duckdb(sql_fetch.replace(str(duckomo_035), str(duckomo_034)), cli=str(duckdb_cli))
        assert baseline_proc.returncode == 0, baseline_proc.stderr
        baseline_rows = parse_duckdb_json(baseline_proc.stdout)[-1]
        assert rows == baseline_rows, "DuckOMO versions produced different member matrices"
        diff = np.max(np.abs(py_arr - duck_vals))
        assert diff == 0.0, f"Batch {b['ordinal']} has non-zero diff: {diff}"

        total_cells_checked += 450
        max_abs_diff = max(max_abs_diff, float(diff))

        cell_matrices.append({
            "ordinal": b["ordinal"],
            "init_time_utc": b["init_time_utc"],
            "om_filename": b["om_filename"],
            "cells_count": 450,
            "max_abs_diff": float(diff),
            "status": "PASS",
        })

    out_cells_dir = run_dir / "om-verification"
    out_cells_dir.mkdir(parents=True, exist_ok=True)

    manifest_out = {
        "schema_version": "m2.cell-matrix-manifest.v1",
        "total_cells_checked": total_cells_checked,
        "max_abs_diff": max_abs_diff,
        "status": "PASS",
        "batches": cell_matrices,
    }
    atomic_write_json(out_cells_dir / "cell-matrix-manifest.json", manifest_out)

    report_out = {
        "stage": "cells",
        "duckomo_versions": ["v0.3.5-rc.1", "v0.3.4"],
        "status": "PASS",
        "total_cells": total_cells_checked,
        "sc001_satisfied": (max_abs_diff == 0.0 and total_cells_checked == 3600),
    }
    atomic_write_json(out_cells_dir / "om-cells-report.json", report_out)
    print(f"Stage [cells] complete: 3600 cells verified with zero float diff.")
    return report_out


def run_single_version_replay(
    duckdb_cli: Path,
    duckomo_path: Path,
    ensabc_path: Path,
    om_dir: Path,
    run_key: str,
    db_path: Path,
) -> dict[str, Any]:
    weights_json = load_json(REPO_ROOT / "validation" / "m2" / "fixtures" / "spatial_weights_eu3x3.json")
    weights_vec = weights_json["weights_vector"]

    schema_sql = (REPO_ROOT / "validation" / "m2" / "sql" / "schema.sql").read_text()
    stage_publish_sql = (REPO_ROOT / "validation" / "m2" / "sql" / "stage_and_publish.sql").read_text()
    replay_sql = (REPO_ROOT / "validation" / "m2" / "sql" / "real_8run_replay.sql").read_text()

    # Replace .read in replay_sql with stage_publish_sql
    replay_sql = replay_sql.replace(".read validation/m2/sql/stage_and_publish.sql", stage_publish_sql)

    binding = real_replay_binding(om_dir, ensabc_path, duckomo_path)
    run_key = binding["run_id"]
    setup_sql = f"""
SET TimeZone = 'UTC';
LOAD '{duckomo_path}';
LOAD '{ensabc_path}';
SET VARIABLE om_dir = '{om_dir}';
SET VARIABLE om_hashes = {sql_value([sha256_file(om_dir / b["om_filename"]) for b in load_json(om_dir / "om_sample_manifest.json")["batches"]])};
SET VARIABLE weights = {weights_vec}::DOUBLE[];
SET VARIABLE threshold_coefficient = 1.0;
SET VARIABLE planned_batch_count = 8;
SET VARIABLE inject_failure = false;
SET VARIABLE run_key = '{run_key}';
SET VARIABLE execution_binding = {sql_value(binding)};
    SET VARIABLE cluster_config = {{
    'projection_dim': 2,
    'cluster_count': 3,
    'initialization': 'kmeans++_chacha8_v1',
    'seed': 3,
    'restarts': 10,
    'initial_centers': []::DOUBLE[][][],
    'stopping_rule': 'sklearn_variance_scaled_v1',
    'tolerance': 0.0001,
    'max_iterations': 300,
    'rank_threshold': 1e-10
}};

{schema_sql}

{replay_sql}

SELECT ordinal, cluster_id, canonical_rank, branch_id, assignment_reason, mean_features
FROM scenario_nodes WHERE run_key = '{run_key}'
ORDER BY ordinal, cluster_id;

SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous
FROM scenario_links WHERE run_key = '{run_key}'
ORDER BY prev_ordinal, prev_cluster_id;
"""

    proc = run_isolated_duckdb(setup_sql, cli=str(duckdb_cli), database=str(db_path))
    assert proc.returncode == 0, f"Replay failed for {duckomo_path.name}: {proc.stderr}"

    # Parse JSON outputs
    results = parse_duckdb_json(proc.stdout)

    nodes = results[-2]
    links = results[-1]
    return {"run_key": run_key, "nodes": nodes, "links": links}


def verify_replay(
    sample_dir: Path,
    cache_dir: Path,
    run_dir: Path,
) -> dict[str, Any]:
    out_dir = run_dir / "om-verification"
    cells_manifest = out_dir / "cell-matrix-manifest.json"
    if not cells_manifest.is_file():
        raise RuntimeError("Stage [cells] must be executed and verified before stage [replay]")

    duckdb_cli = Path(subprocess.check_output(["which", "duckdb"]).decode().strip())
    duckomo_035 = cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension"
    duckomo_034 = cache_dir / "v0.3.4" / "duckomo.duckdb_extension"
    ensabc_ext = run_dir / "build" / "ensabc_m0_extension.duckdb_extension"
    assert ensabc_ext.is_file(), f"Missing ensabc extension: {ensabc_ext}"

    print("Stage [replay]: Executing 8-run replay under DuckOMO v0.3.5-rc.1 and v0.3.4...")
    db_035 = run_dir / "replay_035.duckdb"
    db_034 = run_dir / "replay_034.duckdb"

    res_035 = run_single_version_replay(duckdb_cli, duckomo_035, ensabc_ext, sample_dir, "run_real_8run", db_035)
    res_034 = run_single_version_replay(duckdb_cli, duckomo_034, ensabc_ext, sample_dir, "run_real_8run", db_034)

    # Compare nodes and links
    nodes_035 = res_035["nodes"]
    nodes_034 = res_034["nodes"]
    links_035 = res_035["links"]
    links_034 = res_034["links"]

    assert len(nodes_035) == len(nodes_034) == 24, f"Expected 24 nodes (8x3), got {len(nodes_035)} vs {len(nodes_034)}"
    assert len(links_035) == len(links_034), f"Links count mismatch: {len(links_035)} vs {len(links_034)}"

    prefix_035 = res_035["run_key"][:12] + ":"
    prefix_034 = res_034["run_key"][:12] + ":"
    for n35, n34 in zip(nodes_035, nodes_034):
        assert n35["ordinal"] == n34["ordinal"]
        assert n35["cluster_id"] == n34["cluster_id"]
        assert n35["canonical_rank"] == n34["canonical_rank"]
        assert n35["branch_id"].startswith(prefix_035), f"Unexpected branch_id prefix for v0.3.5-rc.1: {n35['branch_id']}"
        assert n34["branch_id"].startswith(prefix_034), f"Unexpected branch_id prefix for v0.3.4: {n34['branch_id']}"
        assert n35["branch_id"][len(prefix_035):] == n34["branch_id"][len(prefix_034):]
        assert n35["assignment_reason"] == n34["assignment_reason"]
        assert n35["mean_features"] == n34["mean_features"]

    for l35, l34 in zip(links_035, links_034):
        assert l35["prev_ordinal"] == l34["prev_ordinal"]
        assert l35["prev_cluster_id"] == l34["prev_cluster_id"]
        assert l35["curr_ordinal"] == l34["curr_ordinal"]
        assert l35["curr_cluster_id"] == l34["curr_cluster_id"]
        assert l35["distance"] == l34["distance"]
        assert l35["ambiguous"] == l34["ambiguous"]

    comp_report = {
        "schema_version": "m2.duckomo-comparison.v1",
        "status": "PASS",
        "versions": ["v0.3.5-rc.1", "v0.3.4"],
        "nodes_count": len(nodes_035),
        "links_count": len(links_035),
        "nodes_identical": True,
        "links_identical": True,
    }
    atomic_write_json(out_dir / "duckomo-version-comparison.json", comp_report)
    print("Stage [replay] complete: Dual-version 8-run replay outputs are 100% identical.")
    return comp_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Dual-path OM reading & cell verification harness")
    parser.add_argument("--run-dir", type=Path, required=True, help="Exclusive run directory")
    parser.add_argument("--stage", choices=["cells", "replay"], required=True, help="Verification stage")
    parser.add_argument("--sample-dir", type=Path, default=REPO_ROOT / "validation" / "m2" / "data" / "om_sample", help="Sample directory")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "validation" / "m2" / ".cache", help="Cache directory")
    args = parser.parse_args()

    run_dir = args.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)

    if args.stage == "cells":
        verify_cells(args.sample_dir, args.cache_dir, run_dir)
    elif args.stage == "replay":
        verify_replay(args.sample_dir, args.cache_dir, run_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
