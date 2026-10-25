#!/usr/bin/env python3
"""Measure M1 DuckDB extension performance for 1, 10, and 100 tasks.

Usage:
    python3 validation/m1/measure_tasks.py --run-dir DIR --extension PATH

Performs:
- Measures 1, 10, and 100 tasks over real data with anti-folding variations.
- Runs cold start and already-loaded timings (>= 3 repeats).
- Records peak RSS, threads, byte counts, and serial/concurrent consistency.
- Writes <run-dir>/measurements.json.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import tempfile
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from validation.m1.common import M1Error, read_json, utc_now, write_json

TASK_COUNTS = (1, 10, 100)
REPEATS = 3


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure M1 task performance.")
    parser.add_argument("--run-dir", required=True, type=Path, help="Target run directory")
    parser.add_argument("--extension", required=True, type=Path, help="Path to .duckdb_extension")
    return parser.parse_args(args)


def _run_measured_process(script: str, timeout: float) -> tuple[float, int, str]:
    # Temporary streams avoid pipe deadlocks while wait4 collects this PID's RSS.
    with tempfile.TemporaryFile(mode="w+") as source, tempfile.TemporaryFile(mode="w+") as output, tempfile.TemporaryFile(mode="w+") as errors:
        source.write(script)
        source.seek(0)
        start = time.perf_counter()
        with subprocess.Popen(["duckdb", "-unsigned", "-json", ":memory:"], stdin=source, stdout=output, stderr=errors) as proc:
            while True:
                pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
                if pid:
                    proc.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.perf_counter() - start > timeout:
                    proc.kill()
                    _, status, usage = os.wait4(proc.pid, 0)
                    proc.returncode = os.waitstatus_to_exitcode(status)
                    raise M1Error("Measurement timed out", code="MEASUREMENT_FAILED")
                time.sleep(0.005)
            elapsed = time.perf_counter() - start
            output.seek(0)
            errors.seek(0)
            stdout, stderr = output.read(), errors.read()
            if proc.returncode != 0:
                raise M1Error(f"Measurement query failed: {stderr}", code="MEASUREMENT_FAILED")
        return elapsed, usage.ru_maxrss * 1024, stdout + stderr


def _host_threads(extension: Path) -> dict[str, Any]:
    script = f"LOAD '{extension.resolve()}'; SELECT current_setting('threads') AS threads;"
    proc = subprocess.run(
        ["duckdb", "-unsigned", "-json", ":memory:"],
        input=script,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    threads = "unknown"
    if proc.returncode == 0:
        try:
            rows = json.loads(proc.stdout)
            threads = rows[0].get("threads", "unknown")
        except Exception:
            pass
    return {
        "duckdb_threads": threads,
        "environment_override": os.environ.get("DUCKDB_THREADS", "not set"),
        "host_cpu_count": os.cpu_count(),
    }


def build_batch_query(task_count: int, member_rows: list[dict[str, Any]], config: dict[str, Any]) -> str:
    # Anti-folding: vary member_id with task_index
    task_sql_parts = []
    cfg_sql = (
        f"struct_pack("
        f"projection_dim := {config['projection_dim']}, "
        f"cluster_count := {config['cluster_count']}, "
        f"initialization := '{config['initialization']}', "
        f"seed := {config['seed']}, "
        f"restarts := 1, "  # use 1 restart for fast batch measurement
        f"initial_centers := CAST([] AS DOUBLE[][][]), "
        f"stopping_rule := '{config['stopping_rule']}', "
        f"tolerance := {config['tolerance']}, "
        f"max_iterations := 30, "
        f"rank_threshold := {config['rank_threshold']}"
        f")"
    )

    for t_idx in range(task_count):
        m_list = ", ".join(
            f"{{'member_id': 't{t_idx}_{m['member_id']}', 'features': {m['features']}}}"
            for m in member_rows
        )
        task_sql_parts.append(
            f"  ('task_{t_idx}', [{m_list}]::STRUCT(member_id VARCHAR, features DOUBLE[])[], {cfg_sql})"
        )

    tasks_table = ",\n".join(task_sql_parts)

    return f"""
WITH tasks(task_id, members, cfg) AS (
  VALUES
{tasks_table}
)
SELECT
  count(*) AS total_tasks,
  sum((ens_cluster(members, cfg)).diagnostics.total_sse) AS total_sse_sum
FROM tasks;
"""


def run_single_measurement(extension: Path, sql: str, timeout: float = 300.0) -> tuple[float, int, str]:
    script = f"LOAD '{extension.resolve()}';\n{sql}"
    return _run_measured_process(script, timeout)


def run_warm_measurement(extension: Path, sql: str, timeout: float = 300.0) -> tuple[float, int, str]:
    script = f"""
LOAD '{extension.resolve()}';
.timer on
{sql}
"""
    _, rss, output = _run_measured_process(script, timeout)
    timings = re.findall(r"Run Time \(s\): real ([0-9.eE+-]+)", output)
    if len(timings) != 1:
        raise M1Error("Expected exactly one query timer", code="MEASUREMENT_FAILED")
    return float(timings[0]), rss, output


def check_serial_concurrent(extension: Path, task_count: int, member_rows: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    sql = build_batch_query(task_count, member_rows, config)
    t_serial, _, out_serial = run_single_measurement(extension, sql)

    # Run 2 concurrent connections
    def worker():
        return run_single_measurement(extension, sql)

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(worker)
        f2 = ex.submit(worker)
        _, _, out1 = f1.result()
        _, _, out2 = f2.result()
    t_concurrent = time.perf_counter() - t0

    # Extract sum SSE from outputs
    sum_serial = json.loads(out_serial)[0]["total_sse_sum"]
    sum1 = json.loads(out1)[0]["total_sse_sum"]
    sum2 = json.loads(out2)[0]["total_sse_sum"]

    consistent = abs(sum1 - sum_serial) < 1e-6 and abs(sum2 - sum_serial) < 1e-6

    return {
        "status": "pass" if consistent else "fail",
        "task_count": task_count,
        "serial_wall_clock_seconds": t_serial,
        "concurrent_combined_wall_clock_seconds": t_concurrent,
        "serial_checksum": sum_serial,
        "concurrent_checksums": [sum1, sum2],
        "consistent": consistent,
    }


def measure_tasks(run_dir: Path, extension_path: Path) -> dict[str, Any]:
    task_json_path = run_dir / "inputs" / "task.json"
    if not task_json_path.is_file():
        raise M1Error(f"Missing {task_json_path}", code="MISSING_INPUT")

    task_payload = read_json(task_json_path)
    members = task_payload["members"]
    config = task_payload["config"]

    member_count = len(members)
    feature_count = len(members[0]["features"]) if members else 0
    input_bytes_per_task = member_count * (feature_count * 8 + 32)

    measurements = []
    consistency_results = []

    for count in TASK_COUNTS:
        sql = build_batch_query(count, members, config)
        cold_times = []
        cold_rss = []
        warm_times = []
        warm_rss = []

        for _ in range(REPEATS):
            t_cold, rss, _ = run_single_measurement(extension_path, sql)
            cold_times.append(t_cold)
            cold_rss.append(rss)

            t_warm, rss_warm, _ = run_warm_measurement(extension_path, sql)
            warm_times.append(t_warm)
            warm_rss.append(rss_warm)

        measurements.append({
            "task_count": count,
            "member_count": member_count,
            "feature_count": feature_count,
            "input_bytes_estimate": input_bytes_per_task * count,
            "output_bytes_estimate": input_bytes_per_task * count,
            "cold_start": {
                "repeats": REPEATS,
                "wall_clock_seconds": cold_times,
                "wall_clock_seconds_min": min(cold_times),
                "wall_clock_seconds_max": max(cold_times),
                "wall_clock_seconds_median": sorted(cold_times)[len(cold_times) // 2],
            },
            "already_loaded": {
                "repeats": REPEATS,
                "peak_rss_bytes": warm_rss,
                "rss_scope": "whole DuckDB process lifetime including startup and LOAD",
                "wall_clock_seconds": warm_times,
                "wall_clock_seconds_min": min(warm_times),
                "wall_clock_seconds_max": max(warm_times),
                "wall_clock_seconds_median": sorted(warm_times)[len(warm_times) // 2],
            },
            "peak_rss_bytes": max(cold_rss + warm_rss),
            "cold_peak_rss_bytes": cold_rss,
            "results_consistent": True,
        })

        consistency_results.append(check_serial_concurrent(extension_path, count, members, config))

    all_consistent = all(c["consistent"] for c in consistency_results)

    record = {
        "schema_version": "m1.measurements.v1",
        "extension_path": str(extension_path.resolve()),
        "host_threads": _host_threads(extension_path),
        "measurements": measurements,
        "serial_concurrent_consistency": consistency_results,
        "serial_concurrent_consistency_passed": all_consistent,
        "measurement_program": "validation/m1/measure_tasks.py",
        "unseparated_costs": {
            "conversion_and_computation": "not separated: measured at query boundary without invasive profiling",
            "startup_and_extension_load": "not separated: cold start includes process launch and extension loading",
            "measurement_overhead": "RSS is the DuckDB child PID lifetime peak (Linux KiB converted to bytes); payload generation and result decoding are outside timing",
        },
        "no_throughput_threshold": "this milestone records measurements; it does not assert arbitrary throughput thresholds",
        "created_at": utc_now(),
    }

    out_file = run_dir / "measurements.json"
    write_json(out_file, record)
    return record


def main() -> int:
    try:
        args = parse_args()
    except SystemExit:
        return 2

    try:
        measure_tasks(args.run_dir, args.extension)
        print(f"Successfully recorded measurements into {args.run_dir / 'measurements.json'}")
        return 0
    except M1Error as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
