#!/usr/bin/env python3
"""Prepare SQL and check captured output on the host; no Python enters the container."""
from __future__ import annotations

import argparse
from pathlib import Path

from common import FIXTURES_DIR, read_json


def _members(members: list[dict]) -> str:
    items = ["{'member_id': '" + member['member_id'].replace("'", "''") + "', 'features': [" + ', '.join(repr(float(value)) for value in member['features']) + ']}' for member in members]
    return '[' + ', '.join(items) + ']::STRUCT(member_id VARCHAR, features DOUBLE[])[]'


def prepare(directory: Path) -> None:
    fixtures = read_json(FIXTURES_DIR / 'synthetic_tasks.json')
    sql = ['.bail off', '.headers off', '.mode csv', "LOAD '/runtime/opt/ensabc_m0_extension.duckdb_extension';"]
    for case in fixtures['positive']:
        members = _members(case['members'])
        sql.append(f"SELECT CASE WHEN ens_m0_echo({members}) IS NOT DISTINCT FROM {members} THEN 'PASS:{case['case_id']}' ELSE 'FAIL:{case['case_id']}' END;")
    valid = "[{'member_id': 'recovery', 'features': [3.0, 4.0]}]::STRUCT(member_id VARCHAR, features DOUBLE[])[]"
    invalid = fixtures['invalid'] + [fixtures['type_binding_rejection']]
    for case in invalid:
        case_id = case['case_id']
        sql += [f'.print BEGIN:{case_id}', f"SELECT CASE WHEN ens_m0_echo({valid}) IS NOT DISTINCT FROM {valid} THEN 'PASS:before:{case_id}' ELSE 'FAIL:before:{case_id}' END;", case['sql'], f"SELECT CASE WHEN ens_m0_echo({valid}) IS NOT DISTINCT FROM {valid} THEN 'PASS:after:{case_id}' ELSE 'FAIL:after:{case_id}' END;", f'.print END:{case_id}']
    sql += [
        "SELECT CASE WHEN count(*) = 4133 AND bool_and(echoed IS NOT DISTINCT FROM members) THEN 'PASS:row-boundary' ELSE 'FAIL:row-boundary' END FROM (SELECT members, ens_m0_echo(members) echoed FROM (SELECT [{'member_id': 'm-' || i::VARCHAR, 'features': [i::DOUBLE]}] members FROM range(4133) t(i)));",
        "SELECT CASE WHEN ens_m0_echo(members) IS NOT DISTINCT FROM members THEN 'PASS:member-boundary' ELSE 'FAIL:member-boundary' END FROM (SELECT [{'member_id': 'm-' || i::VARCHAR, 'features': [i::DOUBLE]} FOR i IN range(4133)] members);",
        '.print BEGIN:mixed-batch',
        "SELECT task_id, ens_m0_echo(members) FROM (VALUES ('partial-success', [{'member_id': 'ok', 'features': [1.0]}]::STRUCT(member_id VARCHAR, features DOUBLE[])[]), ('bad', [{'member_id': 'bad', 'features': [NULL]}]::STRUCT(member_id VARCHAR, features DOUBLE[])[])) tasks(task_id, members);",
        f"SELECT CASE WHEN ens_m0_echo({valid}) IS NOT DISTINCT FROM {valid} THEN 'PASS:after:mixed-batch' ELSE 'FAIL:after:mixed-batch' END;",
        '.print END:mixed-batch',
    ]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'cases.sql').write_text('\n'.join(sql) + '\n')
    (directory / 'concurrent.sql').write_text('\n'.join(['.bail on', '.headers off', '.mode csv', "LOAD '/runtime/opt/ensabc_m0_extension.duckdb_extension';", f"SELECT CASE WHEN bool_and(ens_m0_echo(members) IS NOT DISTINCT FROM members) THEN 'PASS:concurrent' ELSE 'FAIL:concurrent' END FROM (SELECT [{{'member_id': 'm-' || i::VARCHAR, 'features': [i::DOUBLE]}}] members FROM range(10000) t(i));"]) + '\n')


def check(directory: Path) -> list[str]:
    fixtures = read_json(FIXTURES_DIR / 'synthetic_tasks.json')
    cases_path = directory / 'cases.txt'
    if not cases_path.is_file():
        return ['the isolated SQL output is missing']
    output = cases_path.read_text()
    lines = output.splitlines()
    failures = []
    expected_passes = [case['case_id'] for case in fixtures['positive']] + ['row-boundary', 'member-boundary', 'after:mixed-batch']
    invalid = fixtures['invalid'] + [fixtures['type_binding_rejection']]
    for case in invalid:
        case_id = case['case_id']
        expected_passes += [f'before:{case_id}', f'after:{case_id}']
        begin, end = f'BEGIN:{case_id}', f'END:{case_id}'
        if lines.count(begin) != 1 or lines.count(end) != 1:
            failures.append(f'missing error/recovery segment: {case_id}')
            continue
        segment_lines = lines[lines.index(begin) + 1:lines.index(end)]
        segment = '\n'.join(segment_lines)
        if 'Error:' not in segment:
            failures.append(f'illegal case was not rejected: {case_id}')
        errors = '\n'.join(line for line in segment_lines if 'Error:' in line)
        if case in fixtures['invalid'] and 'ens_m0_echo:' not in errors:
            failures.append(f'illegal case lacks located function error: {case_id}')
        if case_id == fixtures['type_binding_rejection']['case_id']:
            if 'ens_m0_echo:' in errors or 'Conversion Error:' not in errors or "Could not convert string 'not-a-number' to DOUBLE" not in errors:
                failures.append('type-binding case did not fail at the expected conversion boundary')
        for fragment in case['expect_message_contains']:
            if fragment.lower() not in errors.lower():
                failures.append(f'{case_id}: missing error fragment {fragment}')
    for marker in expected_passes:
        if lines.count(f'PASS:{marker}') != 1:
            failures.append(f'exact roundtrip/recovery failed: {marker}')
    if 'FAIL:' in output:
        failures.append('a SQL value assertion failed')
    if lines.count('BEGIN:mixed-batch') != 1 or lines.count('END:mixed-batch') != 1:
        failures.append('mixed-batch segment missing')
    else:
        segment = '\n'.join(lines[lines.index('BEGIN:mixed-batch') + 1:lines.index('END:mixed-batch')])
        if 'task row 1' not in segment or 'partial-success' in segment:
            failures.append('mixed batch did not fail as a whole with a located error')
    # Every intentional error must occur exactly once; unexpected SQL/load
    # failures cannot be hidden by a later successful recovery.
    if output.count('Error:') != len(invalid) + 1:
        failures.append('unexpected number of SQL errors')
    for index in (1, 2):
        path = directory / f'concurrent-{index}.txt'
        if not path.is_file() or path.read_text().splitlines() != ['PASS:concurrent']:
            failures.append(f'concurrent connection {index} did not preserve every value')
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'check'))
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(args.directory)
        return 0
    failures = check(args.directory)
    for failure in failures:
        print(f'FAIL: {failure}')
    print(f"minimal runtime SQL checks: {'fail' if failures else 'pass'}")
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
