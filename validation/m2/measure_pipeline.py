#!/usr/bin/env python3
"""Measure M2 pipeline execution performance and memory across 5 stages (T047).

Stages:
1. Conversion (OM writing)
2. OM Reading (DuckOMO read_om)
3. Clustering and Dispersion (ens_cluster + ens_dispersion)
4. Cross-batch Matching (ens_match_scenarios)
5. Identity & History Publishing (recursive CTE + atomic publish)

Usage:
    validation/m2/.venv/bin/python validation/m2/measure_pipeline.py --run-dir DIR [--extension PATH] [--duckomo PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m2.common import atomic_write_json, load_json, sha256_file, run_isolated_duckdb, real_replay_binding, sql_value

REPEATS = 3


def run_measured_command(
    cmd: list[str],
    input_text: str | None = None,
    timeout: float = 60.0,
    *,
    env: dict[str, str] | None = None,
) -> tuple[int, float, int, str, str]:
    """Execute command directly (no Python wrapper) and measure wall time + Linux ru_maxrss via wait4."""
    start_time = time.perf_counter()
    with tempfile.TemporaryFile("w+") as f_in, tempfile.TemporaryFile("w+") as f_out, tempfile.TemporaryFile("w+") as f_err:
        if input_text:
            f_in.write(input_text)
            f_in.seek(0)
            stdin_arg = f_in
        else:
            stdin_arg = subprocess.DEVNULL

        proc = subprocess.Popen(cmd, stdin=stdin_arg, stdout=f_out, stderr=f_err, env=env)
        while True:
            pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
            if pid:
                exit_code = os.waitstatus_to_exitcode(status)
                break
            if time.perf_counter() - start_time > timeout:
                proc.kill()
                _, status, usage = os.wait4(proc.pid, 0)
                exit_code = os.waitstatus_to_exitcode(status)
                break
            time.sleep(0.005)

        wall_time = time.perf_counter() - start_time
        f_out.seek(0)
        stdout_txt = f_out.read()
        f_err.seek(0)
        stderr_txt = f_err.read()

        # On Linux, ru_maxrss is in KiB
        maxrss_kib = usage.ru_maxrss
        return exit_code, wall_time, maxrss_kib, stdout_txt, stderr_txt


def run_measured_duckdb(duckdb_cli: Path, sql: str, timeout: float = 120.0) -> tuple[int, float, int, str, str]:
    """Run DuckDB directly with isolated HOME/extension_directory so wait4 measures DuckDB itself."""
    import re
    from validation.m2.common import FIXTURES_DIR
    releases = load_json(FIXTURES_DIR / "duckomo_releases.json")
    for match in list(re.finditer(r"LOAD\s+'([^']*duckomo\.duckdb_extension)'\s*;", sql, re.I)):
        path = Path(match.group(1)).resolve()
        digest = sha256_file(path)
        release = next((r for r in releases.values() if r["extension_sha256"] == digest), None)
        if release is None:
            raise ValueError(f"Unverified DuckOMO extension: {path}")
        guard = (
            "\nCREATE OR REPLACE TEMP TABLE m2_extension_identity AS SELECT CASE WHEN count(*) = 1 THEN true "
            "ELSE error('DuckOMO identity mismatch') END FROM duckdb_extensions() "
            f"WHERE extension_name = 'duckomo' AND loaded AND extension_version = '{release['extension_version']}' "
            "AND install_mode = 'NOT_INSTALLED';"
        )
        sql = sql.replace(match.group(0), match.group(0) + guard)
    with tempfile.TemporaryDirectory(prefix="ensabc-m2-measure-") as directory:
        env = os.environ.copy()
        env["HOME"] = directory
        prefix = (
            f"SET extension_directory = '{directory}/extensions';\n"
            "SET autoinstall_known_extensions = false;\n"
            "SET autoload_known_extensions = false;\n"
            "SET TimeZone = 'UTC';\n"
        )
        return run_measured_command(
            [str(duckdb_cli), "-unsigned", "-bail", ":memory:"],
            input_text=prefix + sql,
            timeout=timeout,
            env=env,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure M2 pipeline execution stages")
    parser.add_argument("--run-dir", type=Path, required=True, help="Target run directory")
    parser.add_argument("--sample-dir", type=Path, default=None, help="OM sample directory (defaults to <run-dir>/sample)")
    parser.add_argument("--extension", type=Path, default=Path("/tmp/test-m2-ext/ensabc_m0_extension.duckdb_extension"))
    parser.add_argument("--duckomo", type=Path, default=REPO_ROOT / "validation" / "m2" / ".cache" / "v0.3.5-rc.1" / "duckomo.duckdb_extension")
    args = parser.parse_args()

    run_dir = args.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    ext_path = args.extension
    duckomo_path = args.duckomo
    if args.sample_dir is not None:
        sample_dir = args.sample_dir
    elif (run_dir / "sample" / "om_sample_manifest.json").is_file():
        sample_dir = run_dir / "sample"
    else:
        sample_dir = REPO_ROOT / "validation" / "m2" / "data" / "om_sample"
    duckdb_cli = Path(subprocess.check_output(["which", "duckdb"]).decode().strip())

    weights_json = load_json(REPO_ROOT / "validation" / "m2" / "fixtures" / "spatial_weights_eu3x3.json")
    weights_vec = weights_json["weights_vector"]
    schema_sql = (REPO_ROOT / "validation" / "m2" / "sql" / "schema.sql").read_text()
    stage_publish_sql = (REPO_ROOT / "validation" / "m2" / "sql" / "stage_and_publish.sql").read_text()
    replay_sql = (REPO_ROOT / "validation" / "m2" / "sql" / "real_8run_replay.sql").read_text()
    replay_sql = replay_sql.replace(".read validation/m2/sql/stage_and_publish.sql", stage_publish_sql)
    replay_sql = replay_sql.replace(".read validation/m2/sql/schema.sql", schema_sql)
    binding = real_replay_binding(sample_dir, ext_path, duckomo_path)
    run_key = binding["run_id"]
    om_hashes = [sha256_file(sample_dir / b["om_filename"]) for b in load_json(sample_dir / "om_sample_manifest.json")["batches"]]

    prefix_vars = f"""
    LOAD '{duckomo_path}';
    LOAD '{ext_path}';
    SET VARIABLE om_dir = '{sample_dir}';
    SET VARIABLE weights = {weights_vec}::DOUBLE[];
    SET VARIABLE threshold_coefficient = 1.0;
    SET VARIABLE planned_batch_count = 8;
    SET VARIABLE inject_failure = false;
    SET VARIABLE run_key = '{run_key}';
    SET VARIABLE execution_binding = {sql_value(binding)};
    SET VARIABLE om_hashes = {sql_value(om_hashes)};
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
    """
    read_prefix = replay_sql.split("-- Execute clustering and dispersion")[0]
    cluster_prefix = replay_sql.split("-- Execute Stage A and Stage B publish workflow")[0]
    match_prefix = (
        cluster_prefix
        + "\n"
        + stage_publish_sql.replace(".read validation/m2/sql/schema.sql", schema_sql).split(
            "-- 3. Recursive CTE Identity Inheritance"
        )[0]
    )

    stage_measurements = {}

    # Stage 1: Conversion (OM Writing, 8 real batches from verified cache, no network)
    print("Measuring Stage 1: OM Conversion (8 real batches, 3 repeats)...")
    conv_times = []
    conv_rss = []
    for _ in range(REPEATS):
        tmp_om = Path(tempfile.mkdtemp(prefix="measure_om_"))
        cmd = [
            str(REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"),
            str(REPO_ROOT / "validation" / "m2" / "prepare_om_sample.py"),
            "--out-dir", str(tmp_om),
        ]
        code, wall, rss, out, err = run_measured_command(cmd, timeout=30.0)
        assert code == 0, f"Conversion failed: {err}"
        conv_times.append(wall)
        conv_rss.append(rss)
        import shutil
        shutil.rmtree(tmp_om)

    stage_measurements["1_conversion"] = {
        "scope": "real_8_batches_offline_cache",
        "repeats": REPEATS,
        "mean_wall_seconds": sum(conv_times) / REPEATS,
        "min_wall_seconds": min(conv_times),
        "max_wall_seconds": max(conv_times),
        "peak_rss_kib": max(conv_rss),
    }

    # Stage 2: Cumulative prefix — OM Reading + preparation gates (all 8 real batches)
    print("Measuring Stage 2: DuckOMO read_om prefix (8 real batches, 3 repeats)...")
    read_times, read_rss = [], []
    for _ in range(REPEATS):
        code, wall, rss, out, err = run_measured_duckdb(duckdb_cli, prefix_vars + read_prefix)
        assert code == 0, f"Reading failed: {err}"
        read_times.append(wall)
        read_rss.append(rss)
    stage_measurements["2_reading"] = {
        "scope": "real_8_batches_cumulative_prefix",
        "repeats": REPEATS,
        "mean_wall_seconds": sum(read_times) / REPEATS,
        "min_wall_seconds": min(read_times),
        "max_wall_seconds": max(read_times),
        "peak_rss_kib": max(read_rss),
    }

    # Stage 3: Cumulative prefix — Read + Clustering + Dispersion (all 8 real batches)
    print("Measuring Stage 3: Read + Clustering + Dispersion prefix (8 real batches, 3 repeats)...")
    clust_times, clust_rss = [], []
    for _ in range(REPEATS):
        code, wall, rss, out, err = run_measured_duckdb(duckdb_cli, prefix_vars + cluster_prefix)
        assert code == 0, f"Clustering failed: {err}"
        clust_times.append(wall)
        clust_rss.append(rss)
    stage_measurements["3_clustering_and_dispersion"] = {
        "scope": "real_8_batches_cumulative_prefix",
        "repeats": REPEATS,
        "mean_wall_seconds": sum(clust_times) / REPEATS,
        "min_wall_seconds": min(clust_times),
        "max_wall_seconds": max(clust_times),
        "peak_rss_kib": max(clust_rss),
    }

    # Stage 4: Cumulative prefix — Read + Clustering + Dispersion + Matching (all 7 real pairs)
    print("Measuring Stage 4: Read + Cluster + Matching prefix (7 real pairs, 3 repeats)...")
    match_times, match_rss = [], []
    for _ in range(REPEATS):
        code, wall, rss, out, err = run_measured_duckdb(duckdb_cli, prefix_vars + match_prefix)
        assert code == 0, f"Matching failed: {err}"
        match_times.append(wall)
        match_rss.append(rss)
    stage_measurements["4_matching"] = {
        "scope": "real_8_batches_7_pairs_cumulative_prefix",
        "repeats": REPEATS,
        "mean_wall_seconds": sum(match_times) / REPEATS,
        "min_wall_seconds": min(match_times),
        "max_wall_seconds": max(match_times),
        "peak_rss_kib": max(match_rss),
    }

    # Stage 5: Full 8-Run Replay & 5-Table Atomic History Publishing
    print("Measuring Stage 5: Full 8-Run Replay & History Publishing (3 repeats)...")
    pub_times, pub_rss = [], []
    for _ in range(REPEATS):
        code, wall, rss, out, err = run_measured_duckdb(duckdb_cli, prefix_vars + schema_sql + "\n" + replay_sql)
        assert code == 0, f"Full replay failed: {err}"
        pub_times.append(wall)
        pub_rss.append(rss)
    stage_measurements["5_publishing"] = {
        "scope": "real_8_batches_full_pipeline_atomic_publish",
        "repeats": REPEATS,
        "mean_wall_seconds": sum(pub_times) / REPEATS,
        "min_wall_seconds": min(pub_times),
        "max_wall_seconds": max(pub_times),
        "peak_rss_kib": max(pub_rss),
    }

    doc = {
        "schema_version": "m2.measurements.v1",
        "status": "pass",
        "duckdb_version": "v1.5.5",
        "duckomo_version": "v0.3.5-rc.1",
        "sample_dir": str(sample_dir),
        "extension_sha256": sha256_file(ext_path),
        "duckomo_sha256": sha256_file(duckomo_path),
        "inseparable_boundaries": [
            "OS wait4 ru_maxrss measures process-wide peak RSS of each cumulative SQL prefix (not subtracted per stage).",
            "DuckDB process startup and extension loading are included in cold-start prefix wall times.",
        ],
        "stages": stage_measurements,
    }
    out_path = run_dir / "measurements.json"
    atomic_write_json(out_path, doc)
    print(f"Measurements completed successfully. Written to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
