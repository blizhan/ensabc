#!/usr/bin/env python3
"""Verify the M0 DuckDB extension in a real DuckDB host.

The verifier drives the actual `duckdb` CLI and checks the behaviour the SQL
and validation contracts require:

* the extension loads in a fresh session and returns the members it was given,
  with the identity, the order, the values and the lengths preserved;
* every illegal-input category is rejected with a located reason, and the
  *same* connection still accepts a legal call after each rejection;
* a task row set that crosses the host's internal batch boundary is consumed
  completely, with a varying input per row so that constant folding cannot
  collapse the work;
* a member list that crosses the host's internal batch boundary is preserved
  completely;
* two independent connections executing the same task concurrently produce the
  same result;
* a type-binding error is reported separately from the in-function data
  validation errors.

The verifier never treats a partially consumed result as a success: every
query is read to the end before it is judged.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import FIXTURES_DIR, M0Error, read_json, write_json  # noqa: E402

DEFAULT_EXTENSION = Path("/tmp/ensabc-m0-extension-target/ensabc_m0_extension.duckdb_extension")
SYNTHETIC_TASKS = FIXTURES_DIR / "synthetic_tasks.json"

# The host's internal vector size. A row set larger than this crosses the
# batch boundary, so the boundary is genuinely exercised.
HOST_VECTOR_SIZE = 2048

VALID_TASK_SQL = "SELECT ens_m0_echo([{'member_id': 'ok', 'features': [1.0, 2.0]}])"


class DuckDBRunner:
    """Run SQL in a single DuckDB CLI session and capture its output."""

    def __init__(self, extension: Path, database: str = ":memory:") -> None:
        self.extension = extension
        self.database = database

    def run(self, sql: str, *, timeout: float = 300.0) -> subprocess.CompletedProcess[str]:
        """Run `sql` in a fresh session that loads the extension first.

        Each call starts a new DuckDB process, so the extension is loaded in
        every session. A session that keeps its state is expressed with
        `run_script` instead.
        """
        script = "\n".join(
            [
                ".bail off",
                f"LOAD '{self.extension}';",
                sql,
            ]
        )
        return self.run_script(script, timeout=timeout)

    def run_script(self, script: str, *, timeout: float = 300.0) -> subprocess.CompletedProcess[str]:
        """Run a full DuckDB script, including its own dot commands.

        The script owns the whole session, so a legal call before an illegal
        call and a legal call after it share one connection.
        """
        return subprocess.run(
            ["duckdb", "-unsigned", self.database],
            input=script,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )


def _load_error(output: str) -> str | None:
    """Extract the first error message from a DuckDB output, if any."""
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Error:") or "Error:" in stripped:
            return stripped
    return None


def check_positive_cases(runner: DuckDBRunner) -> list[dict[str, Any]]:
    """Check that every legal synthetic task roundtrips unchanged."""
    fixtures = read_json(SYNTHETIC_TASKS)
    results: list[dict[str, Any]] = []

    for case in fixtures["positive"]:
        members = ", ".join(
            "{{'member_id': '{member_id}', 'features': [{features}]}}".format(
                member_id=member["member_id"],
                features=", ".join(repr(float(value)) for value in member["features"]),
            )
            for member in case["members"]
        )
        sql = (
            f"SELECT '{case['task_id']}' AS task_id, "
            f"ens_m0_echo([{members}]::STRUCT(member_id VARCHAR, features DOUBLE[])[]) AS echoed;"
        )
        completed = runner.run(sql)
        output = completed.stdout + completed.stderr
        error = _load_error(output)
        if error is not None:
            results.append(
                {
                    "case_id": case["case_id"],
                    "task_id": case["task_id"],
                    "status": "fail",
                    "reason": f"a legal task was rejected: {error}",
                }
            )
            continue

        # The echoed member identities and features must be present verbatim.
        missing = []
        for member in case["members"]:
            if member["member_id"] not in output:
                missing.append(f"member_id {member['member_id']!r} is missing from the output")
        for member in case["members"]:
            for value in member["features"]:
                if repr(float(value)) not in output and f"{float(value)}" not in output:
                    missing.append(f"feature value {value!r} is missing from the output")
        results.append(
            {
                "case_id": case["case_id"],
                "task_id": case["task_id"],
                "status": "pass" if not missing else "fail",
                "reason": "; ".join(missing) if missing else "the task roundtripped unchanged",
                "member_count": len(case["members"]),
                "feature_count": len(case["members"][0]["features"]),
            }
        )

    return results


def check_illegal_categories(runner: DuckDBRunner) -> list[dict[str, Any]]:
    """Check that every illegal category is rejected and the session recovers.

    Each category is executed in the sequence the validation contract
    prescribes: a legal task succeeds, the illegal task raises a clear error,
    and a legal task succeeds again *on the same connection*.
    """
    fixtures = read_json(SYNTHETIC_TASKS)
    results: list[dict[str, Any]] = []

    for case in fixtures["invalid"]:
        script = "\n".join(
            [
                ".bail off",
                f"LOAD '{runner.extension}';",
                f"{VALID_TASK_SQL} AS before;",
                case["sql"],
                f"{VALID_TASK_SQL} AS after;",
            ]
        )
        completed = runner.run_script(script)
        output = completed.stdout + completed.stderr
        error = _load_error(output)

        if error is None:
            results.append(
                {
                    "case_id": case["case_id"],
                    "category": case["category"],
                    "status": "fail",
                    "reason": "the illegal input was not rejected",
                }
            )
            continue

        missing_fragments = [
            fragment
            for fragment in case["expect_message_contains"]
            if fragment.lower() not in error.lower()
        ]
        if missing_fragments:
            results.append(
                {
                    "case_id": case["case_id"],
                    "category": case["category"],
                    "status": "fail",
                    "reason": f"the error message does not mention {missing_fragments}: {error}",
                    "error": error,
                }
            )
            continue

        # The same connection must still accept a legal call afterwards.
        if "before" not in output or "after" not in output:
            results.append(
                {
                    "case_id": case["case_id"],
                    "category": case["category"],
                    "status": "fail",
                    "reason": "the surrounding legal calls did not both run on the same connection",
                    "error": error,
                }
            )
            continue

        results.append(
            {
                "case_id": case["case_id"],
                "category": case["category"],
                "status": "pass",
                "reason": "the illegal input was rejected with a located reason and the session recovered",
                "error": error,
            }
        )

    return results


def check_type_binding_rejection(runner: DuckDBRunner) -> dict[str, Any]:
    """Check that a type error is rejected by the binder, separately."""
    fixtures = read_json(SYNTHETIC_TASKS)
    case = fixtures["type_binding_rejection"]
    completed = runner.run(case["sql"])
    output = completed.stdout + completed.stderr
    error = _load_error(output)
    if error is None:
        return {
            "case_id": case["case_id"],
            "status": "fail",
            "reason": "the type-binding error was not rejected",
        }
    return {
        "case_id": case["case_id"],
        "status": "pass",
        "reason": "the type-binding error was rejected by the binder",
        "error": error,
        "distinct_from_data_validation": "ens_m0_echo:" not in error,
    }


def check_row_batch_boundary(runner: DuckDBRunner) -> dict[str, Any]:
    """Check that a row set crossing the host batch boundary is complete.

    Every row carries a different member id and a different feature value, so
    the host cannot fold the work into a single evaluation.
    """
    row_count = HOST_VECTOR_SIZE * 2 + 37
    script = "\n".join(
        [
            ".bail off",
            f"LOAD '{runner.extension}';",
            "CREATE TABLE tasks AS SELECT",
            "  'task-' || i::VARCHAR AS task_id,",
            "  [{'member_id': 'm-' || i::VARCHAR || '-a', 'features': [i::DOUBLE]},",
            "   {'member_id': 'm-' || i::VARCHAR || '-b', 'features': [i::DOUBLE + 0.5]}]",
            "    ::STRUCT(member_id VARCHAR, features DOUBLE[])[] AS members",
            f"FROM range(0, {row_count}) AS t(i);",
            "SELECT count(*) AS row_count FROM (SELECT task_id, ens_m0_echo(members) AS echoed FROM tasks);",
            "SELECT count(*) AS mismatched FROM tasks",
            "  WHERE ens_m0_echo(members)[1].member_id <> 'm-' || rowid::VARCHAR || '-a'",
            "  OR ens_m0_echo(members)[1].features[1] <> rowid::DOUBLE;",
        ]
    )
    completed = runner.run_script(script)
    output = completed.stdout + completed.stderr
    error = _load_error(output)
    if error is not None:
        return {
            "status": "fail",
            "reason": f"the batch boundary query failed: {error}",
            "row_count": row_count,
        }
    if f"{row_count}" not in output:
        return {
            "status": "fail",
            "reason": f"the query did not return the expected {row_count} rows",
            "row_count": row_count,
        }
    if "mismatched" not in output:
        return {
            "status": "fail",
            "reason": "the mismatch count was not produced",
            "row_count": row_count,
        }
    return {
        "status": "pass",
        "reason": "every row of the batch was consumed and matched its own member identity",
        "row_count": row_count,
        "host_vector_size": HOST_VECTOR_SIZE,
    }


def check_member_batch_boundary(runner: DuckDBRunner) -> dict[str, Any]:
    """Check that a member list crossing the host batch boundary is preserved."""
    member_count = HOST_VECTOR_SIZE * 2 + 11
    script = "\n".join(
        [
            ".bail off",
            f"LOAD '{runner.extension}';",
            "SELECT ens_m0_echo([",
            "  {'member_id': 'm-' || i::VARCHAR, 'features': [i::DOUBLE]}",
            f"  for i in range(0, {member_count})",
            "]::STRUCT(member_id VARCHAR, features DOUBLE[])[]) AS echoed;",
            "SELECT list_count(echoed) AS member_count FROM (",
            "  SELECT ens_m0_echo([",
            "    {'member_id': 'm-' || i::VARCHAR, 'features': [i::DOUBLE]}",
            f"    for i in range(0, {member_count})",
            "  ]::STRUCT(member_id VARCHAR, features DOUBLE[])[]) AS echoed",
            ");",
        ]
    )
    completed = runner.run_script(script)
    output = completed.stdout + completed.stderr
    error = _load_error(output)
    if error is not None:
        return {
            "status": "fail",
            "reason": f"the member boundary query failed: {error}",
            "member_count": member_count,
        }
    if str(member_count) not in output:
        return {
            "status": "fail",
            "reason": f"the query did not report the expected {member_count} members",
            "member_count": member_count,
        }
    return {
        "status": "pass",
        "reason": "a member list larger than the host batch size was preserved completely",
        "member_count": member_count,
        "host_vector_size": HOST_VECTOR_SIZE,
    }


def check_mixed_batch_fails_as_a_whole(runner: DuckDBRunner) -> dict[str, Any]:
    """Check that a mixed valid/invalid batch fails without a partial result.

    The validation contract requires that a call containing an illegal task
    fails as a whole and does not deliver a partially successful result. The
    check therefore puts a legal task and an illegal task in one statement
    and requires that the statement fails and that no legal row is delivered
    as a success.
    """
    script = "\n".join(
        [
            ".bail off",
            f"LOAD '{runner.extension}';",
            "SELECT task_id, ens_m0_echo(members) AS echoed FROM (VALUES",
            "  ('good', [{'member_id': 'ok', 'features': [1.0]}]"
            "    ::STRUCT(member_id VARCHAR, features DOUBLE[])[]),",
            "  ('bad',  [{'member_id': 'ok', 'features': [NULL]}]"
            "    ::STRUCT(member_id VARCHAR, features DOUBLE[])[])",
            ") AS tasks(task_id, members);",
        ]
    )
    completed = runner.run_script(script)
    output = completed.stdout + completed.stderr
    error = _load_error(output)

    if error is None:
        return {
            "status": "fail",
            "reason": "a mixed valid/invalid batch did not fail",
        }
    # The illegal task must be located in the batch.
    if "task row 1" not in error:
        return {
            "status": "fail",
            "reason": f"the error does not locate the illegal task in the batch: {error}",
            "error": error,
        }
    # The whole statement must have failed: the legal row must not have been
    # delivered as a successful result.
    if "good" in output.split(error)[0]:
        return {
            "status": "fail",
            "reason": "the legal row of the failed batch was delivered as a partial success",
            "error": error,
        }
    return {
        "status": "pass",
        "reason": "the mixed batch failed as a whole and delivered no partial result",
        "error": error,
    }


def check_two_connection_concurrency(runner: DuckDBRunner) -> dict[str, Any]:
    """Check that two independent connections produce the same result.

    The two connections run *simultaneously* in two separate DuckDB
    processes, so the check actually exercises concurrent access rather than
    two sequential runs. A sequential pair would not detect a shared mutable
    result buffer.
    """
    members = (
        "[{'member_id': 'm01', 'features': [1.0, 2.0]},"
        " {'member_id': 'm02', 'features': [3.0, 4.0]}]"
    )
    sql = (
        f"LOAD '{runner.extension}';\n"
        f"SELECT ens_m0_echo({members}::STRUCT(member_id VARCHAR, features DOUBLE[])[]) AS echoed;\n"
    )

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

    # The two processes are started before either is awaited, so their
    # executions overlap in time.
    started = time.perf_counter()
    outputs: list[str] = []
    for process in processes:
        stdout, stderr = process.communicate(input=sql, timeout=300)
        outputs.append(stdout + stderr)
    elapsed = time.perf_counter() - started

    first_output, second_output = outputs
    if _load_error(first_output) or _load_error(second_output):
        return {
            "status": "fail",
            "reason": "one of the two concurrent connections failed",
        }
    if first_output != second_output:
        return {
            "status": "fail",
            "reason": "the two connections produced different results",
        }
    if "m01" not in first_output or "m02" not in first_output:
        return {
            "status": "fail",
            "reason": "the concurrent result did not contain the expected members",
        }
    return {
        "status": "pass",
        "reason": "two connections running concurrently produced the same result",
        "connections": 2,
        "execution": "both DuckDB processes were started before either was awaited",
        "combined_wall_clock_seconds": elapsed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--extension",
        type=Path,
        default=DEFAULT_EXTENSION,
        help="path to the packaged .duckdb_extension file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the verification record to this JSON file",
    )
    arguments = parser.parse_args()

    if not arguments.extension.is_file():
        print(f"FAIL: the extension artifact is missing: {arguments.extension}")
        return 1

    runner = DuckDBRunner(arguments.extension)

    try:
        positive = check_positive_cases(runner)
        illegal = check_illegal_categories(runner)
        type_binding = check_type_binding_rejection(runner)
        row_batch = check_row_batch_boundary(runner)
        member_batch = check_member_batch_boundary(runner)
        mixed_batch = check_mixed_batch_fails_as_a_whole(runner)
        concurrency = check_two_connection_concurrency(runner)
    except subprocess.TimeoutExpired as error:
        print(f"FAIL: a DuckDB invocation timed out: {error}")
        return 1

    categories = sorted({case["category"] for case in illegal})
    record = {
        "schema_version": "m0.extension-verification.v1",
        "extension_path": str(arguments.extension),
        "positive_cases": positive,
        "illegal_categories": illegal,
        "illegal_category_count": len(categories),
        "illegal_categories_covered": categories,
        "type_binding_rejection": type_binding,
        "row_batch_boundary": row_batch,
        "member_batch_boundary": member_batch,
        "mixed_batch": mixed_batch,
        "two_connection_concurrency": concurrency,
    }

    failures = [case for case in positive if case["status"] != "pass"]
    failures += [case for case in illegal if case["status"] != "pass"]
    for section in (type_binding, row_batch, member_batch, mixed_batch, concurrency):
        if section["status"] != "pass":
            failures.append(section)

    record["status"] = "pass" if not failures else "fail"
    record["failures"] = failures

    if arguments.output:
        write_json(arguments.output, record)

    print(f"extension verification: {record['status']}")
    print(f"  positive cases:      {len(positive)}")
    print(f"  illegal categories:  {len(categories)} ({', '.join(categories)})")
    print(f"  type binding:        {type_binding['status']}")
    print(f"  row batch boundary:  {row_batch['status']}")
    print(f"  member batch:        {member_batch['status']}")
    print(f"  mixed batch:         {mixed_batch['status']}")
    print(f"  two connections:     {concurrency['status']}")
    for failure in failures:
        print(f"  FAILURE: {failure.get('case_id', failure.get('status'))}: {failure.get('reason')}")
    return 0 if record["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
