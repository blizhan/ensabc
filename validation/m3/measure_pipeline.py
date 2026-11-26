#!/usr/bin/env python3
"""Measure M3 7-stage pipeline latency and peak RSS on the real 8-batch WB2 dataset (T040).

Uses `subprocess.Popen` + `os.wait4` (`ru_maxrss` KiB + `time.perf_counter`) across
`REPEATS = 3` runs per stage:
  1. `1_om_reading_prefix` (`read_om` + metadata preparation gate across 8 `.om` files)
  2. `2_clustering_dispersion_prefix` (`read_om` + `ens_cluster` + `ens_dispersion` across 8 batches)
  3. `3_m2_backbone_matching_prefix` (+ `ens_match_scenarios` across 7 pairs + recursive `branch_id` CTE)
  4. `4_m3_evolution_compute_prefix` (+ `ens_evolve_scenarios` across 7 pairs)
  5. `5_m3_history_publish_full` (complete 11-table atomic publish including `ens_fsum` rollups + `G1`–`G4` + `E1`–`E8` guards)
  6. `6_m3_core_runner_standalone` (`m3_runner --mode chain` pure-Rust execution)
  7. `7_visualization_render_and_verify` (`render_evolution_html.py` + `verify_visualization_traceability.py` on the published 8-batch DB)

Also measures separable in-session SQL statement wall-clock durations inside an
already-loaded DuckDB session (`epoch_ns(get_current_timestamp())`) without ever
subtracting cumulative `ru_maxrss` prefixes as fake incremental RSS.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m3.common import (
    M2_CACHE_DIR,
    M2_FIXTURES_DIR,
    NON_PHYSICAL_DISCLAIMER,
    SQL_DIR,
    atomic_write_json,
    default_evolution_config,
    default_scenario_config,
    load_json,
    parse_duckdb_json,
    real_evolution_binding,
    run_isolated_duckdb,
    sha256_file,
    sql_value,
)

SCHEMA_VERSION = "m3.measurements.v1"
REPEATS = 3


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_measured_command(
    cmd: list[str],
    input_text: str | None = None,
    timeout: float = 120.0,
    *,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> tuple[int, float, int, str, str]:
    """Execute command directly and measure wall time + Linux `ru_maxrss` (KiB) via `os.wait4`."""
    start_time = time.perf_counter()
    with (
        tempfile.TemporaryFile("w+", encoding="utf-8") as f_in,
        tempfile.TemporaryFile("w+", encoding="utf-8") as f_out,
        tempfile.TemporaryFile("w+", encoding="utf-8") as f_err,
    ):
        if input_text is not None:
            f_in.write(input_text)
            f_in.seek(0)
            stdin_arg: Any = f_in
        else:
            stdin_arg = subprocess.DEVNULL

        proc = subprocess.Popen(
            cmd,
            stdin=stdin_arg,
            stdout=f_out,
            stderr=f_err,
            env=env,
            cwd=str(cwd) if cwd is not None else None,
        )
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
            time.sleep(0.002)

        wall_time = time.perf_counter() - start_time
        f_out.seek(0)
        stdout_txt = f_out.read()
        f_err.seek(0)
        stderr_txt = f_err.read()

        # On Linux, ru_maxrss is reported in KiB
        maxrss_kib = int(usage.ru_maxrss)
        return exit_code, wall_time, maxrss_kib, stdout_txt, stderr_txt


def _inject_duckomo_guard(sql: str) -> str:
    releases = load_json(M2_FIXTURES_DIR / "duckomo_releases.json")
    for match in list(re.finditer(r"LOAD\s+'([^']*duckomo\.duckdb_extension)'\s*;", sql, re.I)):
        ext_path = Path(match.group(1)).resolve()
        digest = sha256_file(ext_path)
        release = next((r for r in releases.values() if r["extension_sha256"] == digest), None)
        if release is None:
            raise ValueError(f"Unverified DuckOMO extension binary: {ext_path}")
        guard = (
            "\nCREATE OR REPLACE TEMP TABLE m3_extension_identity AS "
            "SELECT CASE WHEN count(*) = 1 THEN true ELSE error('DuckOMO identity mismatch') END "
            "FROM duckdb_extensions() WHERE extension_name = 'duckomo' AND loaded "
            f"AND extension_version = '{release['extension_version']}' "
            "AND install_mode = 'NOT_INSTALLED';"
        )
        sql = sql.replace(match.group(0), match.group(0) + guard)
    return sql


def run_measured_duckdb(
    duckdb_cli: Path,
    sql: str,
    *,
    database: str | Path = ":memory:",
    json_mode: bool = False,
    timeout: float = 120.0,
) -> tuple[int, float, int, str, str]:
    """Run DuckDB CLI directly with isolated HOME/extension_directory and measure via `os.wait4`."""
    guarded_sql = _inject_duckomo_guard(sql)
    with tempfile.TemporaryDirectory(prefix="ensabc-m3-measure-") as directory:
        env = os.environ.copy()
        env["HOME"] = directory
        prefix = (
            f"SET extension_directory = '{directory}/extensions';\n"
            "SET autoinstall_known_extensions = false;\n"
            "SET autoload_known_extensions = false;\n"
            "SET TimeZone = 'UTC';\n"
        )
        cmd = [str(duckdb_cli), "-unsigned"]
        if json_mode:
            cmd.append("-json")
        cmd.extend(["-bail", str(database)])
        return run_measured_command(
            cmd,
            input_text=prefix + guarded_sql,
            timeout=timeout,
            env=env,
            cwd=REPO_ROOT,
        )


def _summarize_runs(
    stage_id: str,
    stage_index: int,
    description: str,
    rss_scope: str,
    wall_times: list[float],
    rss_kibs: list[int],
    *,
    in_session_times: list[float] | None = None,
    substep_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "stage": stage_id,
        "stage_id": stage_id,
        "stage_index": stage_index,
        "description": description,
        "rss_measurement_scope": rss_scope,
        "repeats": len(wall_times),
        "mean_wall_seconds": sum(wall_times) / len(wall_times),
        "min_wall_seconds": min(wall_times),
        "max_wall_seconds": max(wall_times),
        "peak_rss_kib": max(rss_kibs),
        "min_rss_kib": min(rss_kibs),
        "mean_rss_kib": sum(rss_kibs) / len(rss_kibs),
        "runs": [
            {
                "repeat": idx + 1,
                "wall_seconds": wt,
                "maxrss_kib": rss,
            }
            for idx, (wt, rss) in enumerate(zip(wall_times, rss_kibs))
        ],
    }
    if in_session_times is not None:
        record["in_session_statement_wall_seconds"] = {
            "scope": "warm_loaded_duckdb_session_separable_segment",
            "repeats": len(in_session_times),
            "mean": sum(in_session_times) / len(in_session_times),
            "min": min(in_session_times),
            "max": max(in_session_times),
            "runs": list(in_session_times),
        }
    if substep_details is not None:
        record["separable_substeps"] = substep_details
    return record


def _split_sql_stages() -> tuple[str, str, str, str, str, str]:
    """Deterministically slice `real_8run_evolution.sql` and `stage_and_publish.sql` into 5 blocks."""
    schema_sql = (SQL_DIR / "schema.sql").read_text(encoding="utf-8")
    real_sql = (SQL_DIR / "real_8run_evolution.sql").read_text(encoding="utf-8")
    stage_pub_sql = (
        (SQL_DIR / "stage_and_publish.sql")
        .read_text(encoding="utf-8")
        .replace(".read validation/m3/sql/schema.sql", "")
    )

    m_publish_marker = "-- 7. Execute Two-Stage M2+M3 11-Table Atomic Publish Workflow"
    if m_publish_marker not in real_sql:
        raise RuntimeError(f"Expected marker missing in real_8run_evolution.sql: {m_publish_marker}")
    block_1_om_read, real_tail = real_sql.split(m_publish_marker, 1)
    _, post_publish_select = real_tail.split(".read validation/m3/sql/stage_and_publish.sql", 1)

    m_a2 = "-- Stage A.2: M2 Pair Matching & Recursive CTE Persistent Identity (`branch_id`)"
    m_a3 = "-- Stage A.3: M3 Multi-to-Multi Pair Evolution (`ens_evolve_scenarios`)"
    m_a4 = "-- Stage A.4: Materialize M3 Nodes, Transitions, Edges, and Events"
    for marker in (m_a2, m_a3, m_a4):
        if marker not in stage_pub_sql:
            raise RuntimeError(f"Expected marker missing in stage_and_publish.sql: {marker}")

    block_2_cluster, rest_a2 = stage_pub_sql.split(m_a2, 1)
    block_3_m2_match, rest_a3 = rest_a2.split(m_a3, 1)
    block_4_m3_evolve, block_5_publish = rest_a3.split(m_a4, 1)

    block_3_m2_match = m_a2 + "\n" + block_3_m2_match
    block_4_m3_evolve = m_a3 + "\n" + block_4_m3_evolve
    block_5_publish = m_a4 + "\n" + block_5_publish + "\n" + post_publish_select

    return (
        schema_sql,
        block_1_om_read,
        block_2_cluster,
        block_3_m2_match,
        block_4_m3_evolve,
        block_5_publish,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure M3 7-stage pipeline wall-clock latency and peak RSS."
    )
    parser.add_argument("--run-dir", type=Path, required=True, help="Target validation run directory")
    parser.add_argument(
        "--extension",
        type=Path,
        default=Path("/tmp/ensabc-m3-extension-target/ensabc_m0_extension.duckdb_extension"),
        help="Path to compiled ensabc_m0_extension.duckdb_extension",
    )
    parser.add_argument(
        "--runner",
        type=Path,
        default=None,
        help="Path to compiled m3_runner binary (defaults alongside --extension)",
    )
    parser.add_argument(
        "--sample-dir",
        type=Path,
        default=None,
        help="Frozen 8-batch OM sample directory (defaults to <run-dir>/sample or validation/m2/data/om_sample)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=M2_CACHE_DIR,
        help="Verified DuckOMO cache directory (default: validation/m2/.cache)",
    )
    parser.add_argument(
        "--duckomo",
        type=Path,
        default=None,
        help="Optional explicit duckomo.duckdb_extension path",
    )
    args = parser.parse_args(argv)

    run_dir = args.run_dir.resolve()
    ext_path = args.extension.resolve()
    runner_path = (args.runner if args.runner is not None else (ext_path.parent / "m3_runner")).resolve()
    cache_dir = args.cache_dir.resolve()
    duckomo_path = (
        args.duckomo
        if args.duckomo is not None
        else (cache_dir / "v0.3.5-rc.1" / "duckomo.duckdb_extension")
    ).resolve()

    if args.sample_dir is not None:
        sample_dir = args.sample_dir.resolve()
    elif (run_dir / "sample" / "om_sample_manifest.json").is_file():
        sample_dir = (run_dir / "sample").resolve()
    else:
        sample_dir = (REPO_ROOT / "validation" / "m2" / "data" / "om_sample").resolve()

    for label, fp in (
        ("extension", ext_path),
        ("m3_runner", runner_path),
        ("duckomo", duckomo_path),
        ("om_sample_manifest", sample_dir / "om_sample_manifest.json"),
    ):
        if not fp.is_file():
            print(f"Error: required {label} missing at {fp}", file=sys.stderr)
            return 2

    duckdb_which = shutil.which("duckdb")
    if not duckdb_which:
        print("Error: duckdb CLI not found on PATH", file=sys.stderr)
        return 2
    duckdb_cli = Path(duckdb_which).resolve()

    venv_py = REPO_ROOT / "validation" / "m2" / ".venv" / "bin" / "python"
    python_bin = str(venv_py) if venv_py.is_file() else sys.executable

    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_json(sample_dir / "om_sample_manifest.json")
    om_hashes = [sha256_file(sample_dir / b["om_filename"]) for b in manifest["batches"]]
    weights_vec: list[float] = load_json(M2_FIXTURES_DIR / "spatial_weights_eu3x3.json")["weights_vector"]
    sc_cfg = default_scenario_config()
    ev_cfg = default_evolution_config()

    binding = real_evolution_binding(
        sample_dir,
        ext_path,
        duckomo_path,
        scenario_config=sc_cfg,
        threshold_coefficient=1.0,
        evolution_config=ev_cfg,
    )
    run_key = binding["tracking_run_key"]
    evolution_run_id = binding["evolution_run_id"]

    cluster_cfg_sql = {
        "projection_dim": int(sc_cfg["projection_dim"]),
        "cluster_count": int(sc_cfg["cluster_count"]),
        "initialization": str(sc_cfg["initialization"]),
        "seed": int(sc_cfg["seed"]),
        "restarts": int(sc_cfg["restarts"]),
        "initial_centers": sc_cfg["initial_centers"],
        "stopping_rule": str(sc_cfg["stopping_rule"]),
        "tolerance": float(sc_cfg["tolerance"]),
        "max_iterations": int(sc_cfg["max_iterations"]),
        "rank_threshold": float(sc_cfg["rank_threshold"]),
    }
    evolve_cfg_sql = {
        "secondary_rule": str(ev_cfg["secondary_rule"]),
        "flow_rule": str(ev_cfg["flow_rule"]),
        "fraction_sum_tolerance": float(ev_cfg["fraction_sum_tolerance"]),
        "high_uncertainty_threshold_ratio": float(ev_cfg["high_uncertainty_threshold_ratio"]),
        "high_uncertainty_optimality_ratio": float(ev_cfg["high_uncertainty_optimality_ratio"]),
        "high_uncertainty_secondary_ratio": float(ev_cfg["high_uncertainty_secondary_ratio"]),
    }

    prefix_vars = (
        f"LOAD {sql_value(str(duckomo_path))};\n"
        f"LOAD {sql_value(str(ext_path))};\n"
        f"SET VARIABLE om_dir = {sql_value(str(sample_dir))};\n"
        f"SET VARIABLE om_hashes = {sql_value(om_hashes)};\n"
        f"SET VARIABLE weights = {sql_value(weights_vec)}::DOUBLE[];\n"
        "SET VARIABLE threshold_coefficient = 1.0::DOUBLE;\n"
        "SET VARIABLE planned_batch_count = 8;\n"
        f"SET VARIABLE cluster_config = {sql_value(cluster_cfg_sql)};\n"
        f"SET VARIABLE evolve_config = {sql_value(evolve_cfg_sql)};\n"
        f"SET VARIABLE execution_binding = {sql_value(binding['tracking_binding'])};\n"
        f"SET VARIABLE run_key = {sql_value(run_key)};\n"
        f"SET VARIABLE evolution_binding = {sql_value(binding['evolution_binding'])};\n"
        f"SET VARIABLE evolution_run_id = {sql_value(evolution_run_id)};\n"
        "SET VARIABLE inject_failure = false;\n"
        "SET VARIABLE inject_m3_failure = 'false';\n"
    )

    (
        schema_sql,
        b1_om,
        b2_cluster,
        b3_match,
        b4_evolve,
        b5_publish,
    ) = _split_sql_stages()

    sql_prefix_1 = prefix_vars + schema_sql + "\n" + b1_om + "\nSELECT count(*) AS batches FROM stage_raw_batches;\n"
    sql_prefix_2 = (
        prefix_vars + schema_sql + "\n" + b1_om + "\n" + b2_cluster
        + "\nSELECT count(*) AS clustered FROM stage_cluster;\n"
    )
    sql_prefix_3 = (
        prefix_vars + schema_sql + "\n" + b1_om + "\n" + b2_cluster + "\n" + b3_match
        + "\nSELECT count(*) AS pairs FROM stage_match_results;\n"
    )
    sql_prefix_4 = (
        prefix_vars + schema_sql + "\n" + b1_om + "\n" + b2_cluster + "\n" + b3_match + "\n" + b4_evolve
        + "\nSELECT count(*) AS evolved FROM stage_evolve_results;\n"
    )
    sql_full_5 = (
        prefix_vars + schema_sql + "\n" + b1_om + "\n" + b2_cluster + "\n" + b3_match + "\n" + b4_evolve + "\n" + b5_publish
    )

    with tempfile.TemporaryDirectory(prefix="ensabc-m3-measure-work-") as work_str:
        work_dir = Path(work_str)

        # Measure warm in-session separable SQL statement durations (Stages 1..5) across REPEATS
        print(f"Measuring warm in-session SQL statement segments ({REPEATS} repeats)...")
        in_sess_s1: list[float] = []
        in_sess_s2: list[float] = []
        in_sess_s3: list[float] = []
        in_sess_s4: list[float] = []
        in_sess_s5: list[float] = []
        in_sess_total: list[float] = []

        for rep in range(REPEATS):
            in_sess_db = work_dir / f"in_session_{rep}.duckdb"
            timed_sql = (
                prefix_vars
                + schema_sql
                + "\nSET VARIABLE t0_ns = epoch_ns(get_current_timestamp());\n"
                + b1_om
                + "\nSET VARIABLE t1_ns = epoch_ns(get_current_timestamp());\n"
                + b2_cluster
                + "\nSET VARIABLE t2_ns = epoch_ns(get_current_timestamp());\n"
                + b3_match
                + "\nSET VARIABLE t3_ns = epoch_ns(get_current_timestamp());\n"
                + b4_evolve
                + "\nSET VARIABLE t4_ns = epoch_ns(get_current_timestamp());\n"
                + b5_publish
                + "\nSET VARIABLE t5_ns = epoch_ns(get_current_timestamp());\n"
                + """
                SELECT
                    (getvariable('t1_ns') - getvariable('t0_ns'))::DOUBLE / 1e9 AS s1_sec,
                    (getvariable('t2_ns') - getvariable('t1_ns'))::DOUBLE / 1e9 AS s2_sec,
                    (getvariable('t3_ns') - getvariable('t2_ns'))::DOUBLE / 1e9 AS s3_sec,
                    (getvariable('t4_ns') - getvariable('t3_ns'))::DOUBLE / 1e9 AS s4_sec,
                    (getvariable('t5_ns') - getvariable('t4_ns'))::DOUBLE / 1e9 AS s5_sec,
                    (getvariable('t5_ns') - getvariable('t0_ns'))::DOUBLE / 1e9 AS total_sec;
                """
            )
            code, _, _, out, err = run_measured_duckdb(
                duckdb_cli,
                timed_sql,
                database=in_sess_db,
                json_mode=True,
            )
            if code != 0:
                raise RuntimeError(f"Warm in-session SQL timing failed: {err}")
            parsed_t = parse_duckdb_json(out)[-1][0]
            in_sess_s1.append(max(0.0, float(parsed_t["s1_sec"])))
            in_sess_s2.append(max(0.0, float(parsed_t["s2_sec"])))
            in_sess_s3.append(max(0.0, float(parsed_t["s3_sec"])))
            in_sess_s4.append(max(0.0, float(parsed_t["s4_sec"])))
            in_sess_s5.append(max(0.0, float(parsed_t["s5_sec"])))
            in_sess_total.append(max(0.0, float(parsed_t["total_sec"])))

        # Extract 8-batch raw members from sample_dir once for Stage 6 standalone m3_runner
        extract_proc = run_isolated_duckdb(
            sql_prefix_1 + "\nSELECT ordinal, members FROM stage_raw_batches ORDER BY ordinal ASC;\n",
            timeout=120.0,
        )
        if extract_proc.returncode != 0:
            raise RuntimeError(f"Failed extracting 8-batch WB2 members: {extract_proc.stderr}")
        raw_member_rows = parse_duckdb_json(extract_proc.stdout)[-1]
        chain_input_path = work_dir / "wb2_8run_chain_input.json"
        atomic_write_json(
            chain_input_path,
            {
                "mode": "chain",
                "tracking_run_key": run_key,
                "threshold_coefficient": 1.0,
                "weights": weights_vec,
                "config": evolve_cfg_sql,
                "cluster_config": sc_cfg,
                "batches": [
                    {
                        "ordinal": int(r["ordinal"]),
                        "status": "ok",
                        "members": [
                            {
                                "member_index": idx,
                                "member_id": str(m["member_id"]),
                                "features": [float(x) for x in m["features"]],
                            }
                            for idx, m in enumerate(r["members"])
                        ],
                    }
                    for r in raw_member_rows
                ],
            },
        )

        # Stage 1: Cumulative subprocess prefix — OM reading + metadata preparation gate
        print(f"Measuring Stage 1/7: 1_om_reading_prefix ({REPEATS} repeats)...")
        s1_wall, s1_rss = [], []
        for _ in range(REPEATS):
            code, wall, rss, _, err = run_measured_duckdb(duckdb_cli, sql_prefix_1)
            if code != 0:
                raise RuntimeError(f"Stage 1 failed: {err}")
            s1_wall.append(wall)
            s1_rss.append(rss)

        # Stage 2: Cumulative subprocess prefix — Read + ens_cluster + ens_dispersion across 8 batches
        print(f"Measuring Stage 2/7: 2_clustering_dispersion_prefix ({REPEATS} repeats)...")
        s2_wall, s2_rss = [], []
        for _ in range(REPEATS):
            code, wall, rss, _, err = run_measured_duckdb(duckdb_cli, sql_prefix_2)
            if code != 0:
                raise RuntimeError(f"Stage 2 failed: {err}")
            s2_wall.append(wall)
            s2_rss.append(rss)

        # Stage 3: Cumulative subprocess prefix — + ens_match_scenarios (7 pairs) + recursive branch_id CTE
        print(f"Measuring Stage 3/7: 3_m2_backbone_matching_prefix ({REPEATS} repeats)...")
        s3_wall, s3_rss = [], []
        for _ in range(REPEATS):
            code, wall, rss, _, err = run_measured_duckdb(duckdb_cli, sql_prefix_3)
            if code != 0:
                raise RuntimeError(f"Stage 3 failed: {err}")
            s3_wall.append(wall)
            s3_rss.append(rss)

        # Stage 4: Cumulative subprocess prefix — + ens_evolve_scenarios across 7 pairs
        print(f"Measuring Stage 4/7: 4_m3_evolution_compute_prefix ({REPEATS} repeats)...")
        s4_wall, s4_rss = [], []
        for _ in range(REPEATS):
            code, wall, rss, _, err = run_measured_duckdb(duckdb_cli, sql_prefix_4)
            if code != 0:
                raise RuntimeError(f"Stage 4 failed: {err}")
            s4_wall.append(wall)
            s4_rss.append(rss)

        # Stage 5: Full 11-table atomic publish (including ens_fsum rollups + G1–G4 + E1–E8 guards)
        print(f"Measuring Stage 5/7: 5_m3_history_publish_full ({REPEATS} repeats)...")
        s5_wall, s5_rss = [], []
        published_db_for_viz = work_dir / "published_8run.duckdb"
        for rep in range(REPEATS):
            target_db = published_db_for_viz if rep == 0 else (work_dir / f"publish_rep_{rep}.duckdb")
            code, wall, rss, _, err = run_measured_duckdb(duckdb_cli, sql_full_5, database=target_db)
            if code != 0:
                raise RuntimeError(f"Stage 5 failed: {err}")
            s5_wall.append(wall)
            s5_rss.append(rss)

        # Stage 6: Standalone m3_runner --mode chain pure-Rust execution
        print(f"Measuring Stage 6/7: 6_m3_core_runner_standalone ({REPEATS} repeats)...")
        s6_wall, s6_rss = [], []
        for rep in range(REPEATS):
            chain_out = work_dir / f"chain_out_{rep}.json"
            code, wall, rss, _, err = run_measured_command(
                [
                    str(runner_path),
                    "--mode",
                    "chain",
                    "--input",
                    str(chain_input_path),
                    "--output",
                    str(chain_out),
                ],
                timeout=30.0,
            )
            if code != 0:
                raise RuntimeError(f"Stage 6 m3_runner failed: {err}")
            s6_wall.append(wall)
            s6_rss.append(rss)

        # Stage 7: Offline HTML rendering + 6-pass visualization traceability verification
        print(f"Measuring Stage 7/7: 7_visualization_render_and_verify ({REPEATS} repeats)...")
        s7_wall, s7_rss = [], []
        render_walls, render_rsss = [], []
        verify_walls, verify_rsss = [], []
        render_script = REPO_ROOT / "validation" / "m3" / "render_evolution_html.py"
        verify_script = REPO_ROOT / "validation" / "m3" / "verify_visualization_traceability.py"

        for rep in range(REPEATS):
            html_out = work_dir / f"viz_rep_{rep}.html"
            audit_out = work_dir / f"viz_audit_{rep}.json"
            c_ren, w_ren, r_ren, _, e_ren = run_measured_command(
                [
                    python_bin,
                    "-B",
                    str(render_script),
                    "--db",
                    str(published_db_for_viz),
                    "--evolution-run-id",
                    evolution_run_id,
                    "--output",
                    str(html_out),
                ],
                timeout=60.0,
                cwd=REPO_ROOT,
            )
            if c_ren != 0:
                raise RuntimeError(f"Stage 7 render_evolution_html.py failed: {e_ren}")

            c_ver, w_ver, r_ver, _, e_ver = run_measured_command(
                [
                    python_bin,
                    "-B",
                    str(verify_script),
                    "--html",
                    str(html_out),
                    "--db",
                    str(published_db_for_viz),
                    "--evolution-run-id",
                    evolution_run_id,
                    "--out-report",
                    str(audit_out),
                ],
                timeout=60.0,
                cwd=REPO_ROOT,
            )
            if c_ver != 0:
                raise RuntimeError(f"Stage 7 verify_visualization_traceability.py failed: {e_ver}")

            render_walls.append(w_ren)
            render_rsss.append(r_ren)
            verify_walls.append(w_ver)
            verify_rsss.append(r_ver)
            s7_wall.append(w_ren + w_ver)
            s7_rss.append(max(r_ren, r_ver))

    stages_list: list[dict[str, Any]] = [
        _summarize_runs(
            "1_om_reading_prefix",
            1,
            "DuckOMO read_om + 450-row/coordinate/Kelvin/valid_time preparation gate across 8 WB2 .om files",
            "cumulative_subprocess_prefix",
            s1_wall,
            s1_rss,
            in_session_times=in_sess_s1,
        ),
        _summarize_runs(
            "2_clustering_dispersion_prefix",
            2,
            "Cumulative prefix: read_om + ens_dispersion + ens_cluster + canonical scenario ranking across 8 batches",
            "cumulative_subprocess_prefix",
            s2_wall,
            s2_rss,
            in_session_times=in_sess_s2,
        ),
        _summarize_runs(
            "3_m2_backbone_matching_prefix",
            3,
            "Cumulative prefix: + ens_match_scenarios across 7 pairs + recursive CTE persistent branch_id inheritance",
            "cumulative_subprocess_prefix",
            s3_wall,
            s3_rss,
            in_session_times=in_sess_s3,
        ),
        _summarize_runs(
            "4_m3_evolution_compute_prefix",
            4,
            "Cumulative prefix: + ens_evolve_scenarios multi-to-multi evolution across 7 adjacent pairs",
            "cumulative_subprocess_prefix",
            s4_wall,
            s4_rss,
            in_session_times=in_sess_s4,
        ),
        _summarize_runs(
            "5_m3_history_publish_full",
            5,
            "Complete 11-table (5 M2 + 6 M3) atomic publish including ens_fsum branch/chain rollups + G1-G4 + E1-E8 guards",
            "cumulative_subprocess_prefix",
            s5_wall,
            s5_rss,
            in_session_times=in_sess_s5,
        ),
        _summarize_runs(
            "6_m3_core_runner_standalone",
            6,
            "Standalone pure-Rust m3_runner --mode chain across all 8 WB2 batches (50x9 raw members, zero DuckDB/Python)",
            "standalone_process",
            s6_wall,
            s6_rss,
        ),
        _summarize_runs(
            "7_visualization_render_and_verify",
            7,
            "Offline single-file HTML rendering (render_evolution_html.py) + 6-pass traceability audit (verify_visualization_traceability.py)",
            "standalone_process",
            s7_wall,
            s7_rss,
            substep_details={
                "render_evolution_html": {
                    "rss_measurement_scope": "standalone_process",
                    "mean_wall_seconds": sum(render_walls) / REPEATS,
                    "min_wall_seconds": min(render_walls),
                    "max_wall_seconds": max(render_walls),
                    "peak_rss_kib": max(render_rsss),
                },
                "verify_visualization_traceability": {
                    "rss_measurement_scope": "standalone_process",
                    "mean_wall_seconds": sum(verify_walls) / REPEATS,
                    "min_wall_seconds": min(verify_walls),
                    "max_wall_seconds": max(verify_walls),
                    "peak_rss_kib": max(verify_rsss),
                },
            },
        ),
    ]

    doc = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS",
        "measured_at_utc": _utc_now(),
        "repeats": REPEATS,
        "dataset": "WeatherBench2 IFS ENS 2m_temperature 8-batch x 50-member x 3x3 EU sample",
        "duckdb_version": "v1.5.5",
        "duckomo_version": "v0.3.5-rc.1",
        "sample_dir": str(sample_dir),
        "evolution_run_id": evolution_run_id,
        "tracking_run_key": run_key,
        "software_sha256s": {
            "ensabc_extension_sha256": sha256_file(ext_path),
            "m3_runner_sha256": sha256_file(runner_path),
            "duckomo_sha256": sha256_file(duckomo_path),
        },
        "measurement_methodology": {
            "wall_clock_source": "time.perf_counter() around direct subprocess.Popen + os.wait4",
            "rss_source": "Linux os.wait4 rusage.ru_maxrss (KiB)",
            "rss_scopes": {
                "cumulative_subprocess_prefix": (
                    "Stages 1-5 execute cumulative SQL prefixes in fresh DuckDB subprocesses; "
                    "peak_rss_kib is the monotonic process-wide high-water mark of each prefix "
                    "and is NEVER subtracted across prefixes as fake incremental RSS."
                ),
                "standalone_process": (
                    "Stages 6 and 7 execute standalone OS processes (m3_runner and HTML "
                    "render/verify CLI tools) with independent process-level ru_maxrss."
                ),
            },
            "in_session_sql_timing_source": (
                "DuckDB epoch_ns(get_current_timestamp()) recorded between Stage 1-5 SQL "
                "segments inside an already-loaded DuckDB session (excluding cold process "
                "spawn and extension LOAD overhead)."
            ),
        },
        "in_session_sql_total_wall_seconds": {
            "repeats": REPEATS,
            "mean": sum(in_sess_total) / REPEATS,
            "min": min(in_sess_total),
            "max": max(in_sess_total),
            "runs": in_sess_total,
        },
        "stages": stages_list,
        "stages_by_id": {st["stage_id"]: st for st in stages_list},
        "non_physical_disclaimer": NON_PHYSICAL_DISCLAIMER,
    }

    out_path = run_dir / "measurements.json"
    atomic_write_json(out_path, doc)
    print(f"M3 7-stage pipeline measurements: PASS -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
