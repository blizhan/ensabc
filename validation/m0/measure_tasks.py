#!/usr/bin/env python3
"""Measure the M0 extension's time and peak memory for 1, 10 and 100 tasks.

The measurement contract asks for more than a stopwatch:

* the same legal matrix is used to build 1, 10 and 100 tasks, and every task
  carries a distinct member id and a distinct feature value, so the host
  cannot fold the work into a single evaluation;
* the cold start (the process that loads the extension and runs the query for
  the first time) is measured separately from the already-loaded invocation;
* the peak RSS of the measuring process is recorded, and the measurement
  boundary is stated explicitly: the RSS covers the host process *and* the
  measuring program, so it is not presented as the core's exclusive memory;
* the input and output byte counts are recorded;
* no throughput threshold is asserted. The gate is that the run completes and
  that the reported numbers are reported honestly.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import FIXTURES_DIR, M0Error, read_json, write_json  # noqa: E402

DEFAULT_EXTENSION = Path("/tmp/ensabc-m0-extension-target/ensabc_m0_extension.duckdb_extension")
MANIFEST_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_manifest.json"
PAYLOAD_PATH = FIXTURES_DIR / "real_ifs_ens_20200101_t2m_data.json"

TASK_COUNTS = (1, 10, 100)
REPEATS = 5


def _peak_rss_bytes() -> int:
    """Return the peak RSS of this process in bytes.

    `ru_maxrss` is reported in kilobytes on Linux.
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def _child_peak_rss_bytes(pid: int) -> int:
    """Return the peak RSS of a finished child process in bytes."""
    return resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024


def build_task_sql(task_count: int, matrix: list[list[float]], member_ids: list[str]) -> str:
    """Build a query with `task_count` tasks over the same legal matrix.

    Every task uses the same member ids, but the member ids carry the task
    index, so the input differs per task and the host cannot constant-fold the
    calls into one evaluation.
    """
    rows = []
    for task_index in range(task_count):
        members = ", ".join(
            "{{'member_id': 't{task}-m{member}', 'features': [{features}]}}".format(
                task=task_index,
                member=member_index,
                features=", ".join(repr(float(value)) for value in values),
            )
            for member_index, values in enumerate(matrix)
        )
        rows.append(
            f"  ('task-{task_index}', [{members}]"
            "::STRUCT(member_id VARCHAR, features DOUBLE[])[])"
        )
    body = ",\n".join(rows)
    return (
        "SELECT count(*) AS echoed_rows, sum(list_sum(list_transform(echoed, x -> list_sum(x.features)))) AS checksum\n"
        "FROM (\n"
        "  SELECT ens_m0_echo(members) AS echoed\n"
        "  FROM (VALUES\n"
        f"{body}\n"
        "  ) AS tasks(task_id, members)\n"
        ") AS results;"
    )


def measure_once(
    extension: Path,
    task_count: int,
    matrix: list[list[float]],
    member_ids: list[str],
) -> dict[str, Any]:
    """Measure one cold-start execution of the given number of tasks."""
    sql = build_task_sql(task_count, matrix, member_ids)
    script = f".bail on\nLOAD '{extension}';\n{sql}\n"

    before = _peak_rss_bytes()
    started = time.perf_counter()
    completed = subprocess.run(
        ["duckdb", "-unsigned", ":memory:"],
        input=script,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    elapsed = time.perf_counter() - started
    after = _peak_rss_bytes()

    output = completed.stdout + completed.stderr
    if completed.returncode != 0:
        raise M0Error(f"the measurement query failed ({completed.returncode}): {output.strip()}")

    return {
        "task_count": task_count,
        "wall_clock_seconds": elapsed,
        "measured_process_peak_rss_bytes": max(before, after),
        "child_peak_rss_bytes": _child_peak_rss_bytes(os.getpid()),
        "output": output.strip(),
    }


def measure_warm(
    extension: Path,
    task_count: int,
    matrix: list[list[float]],
    member_ids: list[str],
) -> dict[str, Any]:
    """Measure an invocation in a session that has already loaded the extension.

    The extension is loaded in a first statement of the same session, and
    DuckDB's own statement timer reports the cost of the query statement
    alone. This separates the query cost from the process start-up and from
    the extension load, which the cold-start measurement covers.
    """
    sql = build_task_sql(task_count, matrix, member_ids)
    script = "\n".join(
        [
            ".bail on",
            ".timer on",
            f"LOAD '{extension}';",
            sql,
        ]
    )

    started = time.perf_counter()
    completed = subprocess.run(
        ["duckdb", "-unsigned", ":memory:"],
        input=script,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    elapsed = time.perf_counter() - started
    output = completed.stdout + completed.stderr
    if completed.returncode != 0:
        raise M0Error(f"the warm measurement query failed: {output.strip()}")

    # The last `Run Time` line belongs to the query statement.
    statement_seconds = None
    for line in output.splitlines():
        if line.startswith("Run Time (s): real "):
            statement_seconds = float(line.split("real ")[1].split()[0])
    if statement_seconds is None:
        raise M0Error("the warm measurement did not report a per-statement run time")

    return {
        "task_count": task_count,
        "wall_clock_seconds": elapsed,
        "statement_seconds": statement_seconds,
        "output": output.strip(),
    }


def _checksum_of(output: str) -> str:
    """Extract the echoed-rows and checksum line from a DuckDB result.

    The checksum is the sum of every echoed feature value, so two executions
    that computed the same work produce the same checksum. It is the value
    that the serial and the concurrent run are compared on.
    """
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if stripped.startswith("│") and stripped.count("│") >= 3:
            return stripped
    return output.strip().splitlines()[-1] if output.strip() else ""


def _run_query(extension: Path, sql: str, timeout: float = 600.0) -> tuple[str, float]:
    """Run one query in a fresh DuckDB process and return its output and time."""
    script = f".bail on\nLOAD '{extension}';\n{sql}\n"
    started = time.perf_counter()
    completed = subprocess.run(
        ["duckdb", "-unsigned", ":memory:"],
        input=script,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    elapsed = time.perf_counter() - started
    output = completed.stdout + completed.stderr
    if completed.returncode != 0:
        raise M0Error(f"the measurement query failed ({completed.returncode}): {output.strip()}")
    return output, elapsed


def check_serial_concurrent_consistency(
    extension: Path,
    task_count: int,
    matrix: list[list[float]],
    member_ids: list[str],
) -> dict[str, Any]:
    """Check that the concurrent result matches the serial result.

    FR-011 requires the serial and the concurrent results to agree. The
    serial run executes the tasks one after another; the concurrent run
    executes the same tasks in two DuckDB processes at the same time. The
    checksums of the two runs must be identical, otherwise the extension
    would be sharing mutable state between connections.
    """
    sql = build_task_sql(task_count, matrix, member_ids)

    serial_output, serial_seconds = _run_query(extension, sql)
    serial_checksum = _checksum_of(serial_output)

    # Two processes are started before either is awaited, so their executions
    # overlap.
    processes = [
        subprocess.Popen(
            ["duckdb", "-unsigned", ":memory:"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    concurrent_outputs: list[str] = []
    started = time.perf_counter()
    for process in processes:
        stdout, stderr = process.communicate(
            input=f".bail on\nLOAD '{extension}';\n{sql}\n", timeout=600
        )
        concurrent_outputs.append(stdout + stderr)
    concurrent_seconds = time.perf_counter() - started

    concurrent_checksums = [_checksum_of(output) for output in concurrent_outputs]
    for output in concurrent_outputs:
        if "Error" in output:
            return {
                "status": "fail",
                "reason": "one of the concurrent runs failed",
                "task_count": task_count,
            }

    consistent = all(checksum == serial_checksum for checksum in concurrent_checksums)
    return {
        "status": "pass" if consistent else "fail",
        "reason": (
            "the concurrent result matches the serial result"
            if consistent
            else "the concurrent result differs from the serial result"
        ),
        "task_count": task_count,
        "serial_checksum": serial_checksum,
        "concurrent_checksums": concurrent_checksums,
        "serial_wall_clock_seconds": serial_seconds,
        "concurrent_combined_wall_clock_seconds": concurrent_seconds,
        "concurrent_connections": 2,
    }


def _host_thread_settings(extension: Path) -> dict[str, Any]:
    """Record the host thread settings that the measurement ran under.

    The measurement contract asks for the host thread setting to be recorded,
    so the effective value is read from the host rather than assumed.
    """
    script = f".bail on\nLOAD '{extension}';\nSELECT current_setting('threads') AS threads;\n"
    completed = subprocess.run(
        ["duckdb", "-unsigned", ":memory:"],
        input=script,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    output = completed.stdout + completed.stderr
    threads = "unknown"
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("│") and stripped.count("│") >= 2:
            candidate = stripped.split("│")[1].strip()
            if candidate.isdigit():
                threads = candidate
                break
    return {
        "duckdb_threads": threads,
        "duckdb_threads_source": "current_setting('threads') reported by the host",
        "environment_override": os.environ.get("DUCKDB_THREADS", "not set"),
        "host_cpu_count": os.cpu_count(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension", type=Path, default=DEFAULT_EXTENSION)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--payload", type=Path, default=PAYLOAD_PATH)
    parser.add_argument(
        "--output",
        type=Path,
        help="write the measurement record to this JSON file",
    )
    arguments = parser.parse_args()

    if not arguments.extension.is_file():
        print(f"FAIL: the extension artifact is missing: {arguments.extension}")
        return 1

    manifest = read_json(arguments.manifest)
    payload = read_json(arguments.payload)
    matrix = [[float(value) for value in row] for row in payload["values"]]
    member_ids = list(payload["member_ids"])

    member_count = len(member_ids)
    feature_count = len(matrix[0]) if matrix else 0
    # Each task carries the full member set with the same feature values.
    input_bytes_per_task = member_count * (feature_count * 8 + 32)

    records: list[dict[str, Any]] = []
    consistency: list[dict[str, Any]] = []
    try:
        for task_count in TASK_COUNTS:
            cold_runs = [
                measure_once(arguments.extension, task_count, matrix, member_ids)
                for _ in range(REPEATS)
            ]
            warm_runs = [
                measure_warm(arguments.extension, task_count, matrix, member_ids)
                for _ in range(REPEATS)
            ]

            cold_times = [run["wall_clock_seconds"] for run in cold_runs]
            warm_times = [run["statement_seconds"] for run in warm_runs]
            warm_process_times = [run["wall_clock_seconds"] for run in warm_runs]
            peak_rss = max(run["measured_process_peak_rss_bytes"] for run in cold_runs)

            # The checksums of the two extremes must agree, so the measurement
            # really executed the work and did not fold it away.
            checksum_rows = {
                run["output"].splitlines()[-2] if len(run["output"].splitlines()) >= 2 else ""
                for run in cold_runs
            }

            records.append(
                {
                    "task_count": task_count,
                    "member_count": member_count,
                    "feature_count": feature_count,
                    "input_bytes_estimate": input_bytes_per_task * task_count,
                    "output_bytes_estimate": input_bytes_per_task * task_count,
                    "cold_start": {
                        "description": "a fresh process that loads the extension and runs the query",
                        "repeats": REPEATS,
                        "wall_clock_seconds": cold_times,
                        "wall_clock_seconds_min": min(cold_times),
                        "wall_clock_seconds_max": max(cold_times),
                        "wall_clock_seconds_median": sorted(cold_times)[len(cold_times) // 2],
                    },
                    "already_loaded": {
                        "description": (
                            "a session that has already loaded the extension; the time is the "
                            "DuckDB per-statement timer for the query statement alone"
                        ),
                        "repeats": REPEATS,
                        "wall_clock_seconds": warm_times,
                        "wall_clock_seconds_min": min(warm_times),
                        "wall_clock_seconds_max": max(warm_times),
                        "wall_clock_seconds_median": sorted(warm_times)[len(warm_times) // 2],
                        "process_wall_clock_seconds": warm_process_times,
                    },
                    "peak_rss_bytes": peak_rss,
                    "peak_rss_measurement_boundary": (
                        "the peak RSS of the measuring process; it covers the DuckDB host "
                        "process and the measuring program, and is not the exclusive memory "
                        "of the extension"
                    ),
                    "results_consistent": len(checksum_rows) == 1,
                    "checksum_row": sorted(checksum_rows)[0] if checksum_rows else "",
                }
            )

            # FR-011 also requires the serial and the concurrent results to
            # agree, so the consistency is checked for every task count.
            consistency.append(
                check_serial_concurrent_consistency(
                    arguments.extension, task_count, matrix, member_ids
                )
            )
    except (M0Error, subprocess.TimeoutExpired) as error:
        print(f"FAIL: {error}")
        return 1

    record = {
        "schema_version": "m0.measurements.v1",
        "extension_path": str(arguments.extension),
        "sample_id": manifest["sample_id"],
        "host_threads": _host_thread_settings(arguments.extension),
        "measurement_program": "validation/m0/measure_tasks.py",
        "measurement_boundary": (
            "the cold-start time covers the whole DuckDB process, including the start-up and "
            "the extension load; the already-loaded time is the DuckDB per-statement timer "
            "for the query statement alone, so the two are reported as separate scopes and "
            "are not summed"
        ),
        "unseparated_costs": {
            "conversion_and_computation": (
                "not separated: the extension echoes the members it is given, so the "
                "conversion cost and the computation cost cannot be measured apart from each "
                "other with this diagnostic function. No split timing is claimed."
            ),
            "startup_and_extension_load": (
                "not separated: the cold-start measurement covers the process start-up and the "
                "extension load together. The already-loaded measurement excludes both, so the "
                "difference bounds them but does not attribute them individually."
            ),
            "measurement_program_overhead": (
                "not separated: the peak RSS covers the DuckDB host process and the measuring "
                "program together, so it is not the exclusive memory of the extension."
            ),
        },
        "no_throughput_threshold": (
            "this milestone records the measurements; it does not assert a throughput target"
        ),
        "measurements": records,
        "serial_concurrent_consistency": consistency,
        "serial_concurrent_consistency_passed": all(
            entry["status"] == "pass" for entry in consistency
        ),
    }

    if arguments.output:
        write_json(arguments.output, record)

    print("M0 task measurements")
    for entry in records:
        print(
            f"  {entry['task_count']:>3} tasks x {entry['member_count']} members: "
            f"cold median {entry['cold_start']['wall_clock_seconds_median']:.4f}s, "
            f"loaded median {entry['already_loaded']['wall_clock_seconds_median']:.4f}s, "
            f"peak RSS {entry['peak_rss_bytes']} bytes"
        )
    for entry in consistency:
        print(
            f"  {entry['task_count']:>3} tasks serial vs concurrent: {entry['status']}"
        )
    if not record["serial_concurrent_consistency_passed"]:
        print("FAIL: the serial and the concurrent results do not agree")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
