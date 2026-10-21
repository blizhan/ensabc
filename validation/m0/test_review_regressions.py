"""Focused offline regressions for the M0 review findings.

Run with: python3 -m unittest discover -s validation/m0 -p 'test_*.py'
"""
from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import reproduce_independently as reproduction
import run_m0_gate as gate
from common import read_json, write_json
from schema import REQUIREMENT_IDS, ValidationError, aggregate_overall, validate_gate_report
from verify_report import audit_report, build_synthetic_report
import minimal_runtime_cases
import traceability


class GateTraceabilityTests(unittest.TestCase):
    def run_gate(self, traceability_status):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'gate'
            requirements = build_synthetic_report('passed', {})['requirements']
            order = []

            def environment(run_dir):
                write_json(run_dir / 'environment.json', {})
                return gate.StepOutcome('pass', 'test environment')

            def traceability(command, **kwargs):
                if traceability_status is not None:
                    write_json(directory / 'traceability.json', {
                        'status': traceability_status,
                        'matrix': {rid: {'status': 'pass', 'failed_assertions': []} for rid in REQUIREMENT_IDS},
                        'failed_assertions': [],
                        'failed_success_criterion_assertions': ([{'assertion_id': 'SC002-2'}] if traceability_status == 'fail' else []),
                    })
                return subprocess.CompletedProcess(command, 0 if traceability_status == 'pass' else 1, 'traceability result', '')

            def step(name):
                def run(*args):
                    order.append(name)
                    return gate.StepOutcome('pass', 'focused test')
                return run

            names = ('sample', 'sample_recovery', 'reference', 'independent_reproduction', 'comparator_self_check', 'build_extension', 'extension', 'measurements', 'minimal_runtime')
            with ExitStack() as stack:
                stack.enter_context(patch.object(gate, 'step_environment', environment))
                for name in names:
                    stack.enter_context(patch.object(gate, f'step_{name}', step(name)))
                stack.enter_context(patch.object(gate, 'build_requirement_records', return_value=copy.deepcopy(requirements)))
                stack.enter_context(patch.object(gate, 'run_script', side_effect=traceability))
                stack.enter_context(patch.object(sys, 'argv', ['run_m0_gate.py', '--output-dir', str(directory)]))
                code = gate.main()
            return code, read_json(directory / 'report.json'), order

    def test_sc_only_failure_prevents_gate_pass_without_failing_unrelated_fr(self):
        code, report, order = self.run_gate('fail')
        self.assertEqual(code, gate.EXIT_FAILED)
        self.assertEqual(report['overall'], 'failed')
        self.assertTrue(all(value['status'] == 'pass' for value in report['requirements'].values()))
        self.assertEqual(report['traceability']['failed_success_criterion_assertions'][0]['assertion_id'], 'SC002-2')
        self.assertEqual(aggregate_overall(report['requirements'], report['traceability']['status']), 'failed')
        self.assertLess(order.index('extension'), order.index('independent_reproduction'))
        self.assertLess(order.index('build_extension'), order.index('independent_reproduction'))

    def test_missing_traceability_leaves_report_incomplete(self):
        code, report, _ = self.run_gate(None)
        self.assertEqual(code, gate.EXIT_INCOMPLETE)
        self.assertEqual(report['overall'], 'incomplete')
        self.assertEqual(report['traceability']['status'], 'not_run')

    def test_passing_traceability_keeps_gate_pass(self):
        code, report, _ = self.run_gate('pass')
        self.assertEqual(code, gate.EXIT_PASSED)
        self.assertEqual(report['overall'], 'passed')


class BaselineComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name) / 'work'
        self.baseline = Path(self.temp.name) / 'baseline'
        sample = {'status': 'pass', 'sample_id': 'sample', 'matrix_sha256_float32_le': reproduction.EXPECTED_SOURCE_MATRIX_SHA256}
        summary = {'status': 'pass', 'input_sha256': 'input', 'config_sha256': 'config', 'runs': [{'labels': [0, 1, 0]}]}
        exchange = {'status': 'pass', 'illegal_categories_covered': ['top_level_null']}
        for directory in (self.work, self.baseline):
            write_json(directory / 'sample-validation.json', sample)
            name = 'summary.json' if directory == self.work else 'reference-summary.json'
            write_json(directory / 'reference' / name, summary)
            write_json(directory / 'exchange.json', exchange)

    def test_complete_comparison_covers_all_three_sections(self):
        result = reproduction.compare_with_baseline(self.work, self.baseline)
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(len(result['notes']), 3)

    def test_each_missing_input_prevents_pass(self):
        for directory, relative in ((self.work, 'sample-validation.json'), (self.baseline, 'sample-validation.json'), (self.work, 'reference/summary.json'), (self.baseline, 'reference/reference-summary.json'), (self.work, 'exchange.json'), (self.baseline, 'exchange.json')):
            with self.subTest(path=str(directory / relative)):
                path = directory / relative
                saved = path.read_bytes()
                path.unlink()
                result = reproduction.compare_with_baseline(self.work, self.baseline)
                self.assertEqual(result['status'], 'not_run')
                self.assertIn(str(path), ' '.join(result['notes']))
                path.write_bytes(saved)

    def test_extension_difference_is_detected(self):
        write_json(self.work / 'exchange.json', {'status': 'pass', 'illegal_categories_covered': ['other']})
        self.assertEqual(reproduction.compare_with_baseline(self.work, self.baseline)['status'], 'fail')

    def test_failed_or_empty_exchange_cannot_pass_comparison(self):
        for record in ({'status': 'fail', 'illegal_categories_covered': ['top_level_null']}, {'status': 'pass', 'illegal_categories_covered': []}):
            with self.subTest(record=record):
                write_json(self.work / 'exchange.json', record)
                write_json(self.baseline / 'exchange.json', record)
                self.assertEqual(reproduction.compare_with_baseline(self.work, self.baseline)['status'], 'fail')


class SampleInterpreterTests(unittest.TestCase):
    def test_sample_verification_uses_data_environment(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            interpreter = directory / 'python'
            interpreter.touch()
            payload = directory / 'payload.json'
            payload.touch()

            def run(command, **kwargs):
                self.assertEqual(command[0], str(interpreter))
                write_json(directory / 'sample-validation.json', {'status': 'pass'})
                return subprocess.CompletedProcess(command, 0, 'pass', '')

            with patch.object(gate, 'DATA_PYTHON', interpreter), patch.object(gate, 'PAYLOAD_PATH', payload), patch.object(gate, 'run_script', side_effect=run):
                self.assertEqual(gate.step_sample(directory).status, 'pass')

    def test_missing_data_environment_is_not_run(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(gate, 'DATA_PYTHON', Path(temp) / 'absent'):
            self.assertEqual(gate.step_sample(Path(temp)).status, 'not_run')
            self.assertEqual(gate.step_sample_recovery(Path(temp)).status, 'not_run')


class MinimalRuntimeOutputTests(unittest.TestCase):
    def test_missing_recovery_or_wrong_value_fails_even_with_case_labels(self):
        fixtures = read_json(minimal_runtime_cases.FIXTURES_DIR / 'synthetic_tasks.json')
        lines = [f"PASS:{case['case_id']}" for case in fixtures['positive']]
        for case in fixtures['invalid'] + [fixtures['type_binding_rejection']]:
            case_id = case['case_id']
            message = ' '.join(case['expect_message_contains'])
            error = ("Conversion Error: Could not convert string 'not-a-number' to DOUBLE" if case_id == 'type_binding_rejection' else f'Invalid Input Error: ens_m0_echo: {message}')
            lines += [f'BEGIN:{case_id}', f'PASS:before:{case_id}', error, f'PASS:after:{case_id}', f'END:{case_id}']
        lines += ['PASS:row-boundary', 'PASS:member-boundary', 'BEGIN:mixed-batch', 'Invalid Input Error: ens_m0_echo: task row 1', 'PASS:after:mixed-batch', 'END:mixed-batch']
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            for index in (1, 2):
                (directory / f'concurrent-{index}.txt').write_text('PASS:concurrent\n')
            path = directory / 'cases.txt'
            output = '\n'.join(lines)
            path.write_text(output)
            self.assertEqual(minimal_runtime_cases.check(directory), [])
            path.write_text(output.replace("Conversion Error: Could not convert string 'not-a-number' to DOUBLE", 'Invalid Input Error: ens_m0_echo: bad value'))
            self.assertIn('type-binding case did not fail at the expected conversion boundary', minimal_runtime_cases.check(directory))
            path.write_text(output.replace('PASS:after:top_level_null', 'FAIL:after:top_level_null'))
            self.assertTrue(minimal_runtime_cases.check(directory))
            path.write_text(output.replace('PASS:row-boundary', 'FAIL:row-boundary'))
            self.assertTrue(minimal_runtime_cases.check(directory))


def write_traceability_evidence(directory):
    """Minimal portable records for the real FR/SC evaluator, not recorded verdicts."""
    records = {
        'sample-validation.json': {
            'status': 'pass', 'kind': 'real_ensemble', 'member_count': 50, 'point_count': 9,
            'variable': '2m_temperature', 'init_time': '2020-01-01T00:00:00Z',
            'valid_time': '2020-01-02T00:00:00Z', 'units': 'K', 'payload_sha256': 'sample-hash',
            'total_frozen_bytes': 1234, 'matrix_sha256_float32_le': reproduction.EXPECTED_SOURCE_MATRIX_SHA256,
            'schema': {'member_count': 50}, 'negative_eligibility_cases': [{'rejected': True, 'reason': 'invalid'}],
        },
        'sample-recovery.json': {'status': 'pass', 'comparison': {'byte_identical': True}, 'source_objects': [{'matches': True}]},
        'environment.json': {'rustc_version': 'rustc 1.96.0', 'duckdb_version': 'v1.5.5', 'python_version': 'Python 3.13.13', 'setup_check_status': 'pass'},
        'source-hashes.json': {'git_commit': 'test-commit', 'tracked_files': {'file': 'hash'}},
        'reference/reference-summary.json': {'independent_processes': {'count': 3, 'pids': [1, 2, 3]}, 'runs': [{'output_files': [{'sha256': 'output-hash'}]}]},
        'reference/run-1.json': {'scaled': [[1]], 'components': [[1]], 'explained_variance_ratio': [1], 'labels': [0] * 50, 'cluster_centers': [[1], [2], [3]], 'inertia': 1, 'n_iter': 1, 'input_sha256': 'input', 'config_sha256': 'config', 'output_file': 'run-1.json'},
        'reference/comparison.json': {'status': 'pass', 'tolerance': {'absolute': 1e-8, 'relative': 1e-6}, 'checks': {'partition_equal': True, 'subspace_equal': True, 'cluster_centers_close': True}},
        'reference/comparator-self-check.json': {'checks': [{'correct': True}]},
        'exchange.json': {'status': 'pass', 'positive_cases': [{'status': 'pass', 'member_count': 2}] * 6, 'illegal_category_count': 10, 'extension_path': 'test-extension', 'row_batch_boundary': {'status': 'pass', 'row_count': 4133}, 'member_batch_boundary': {'status': 'pass', 'member_count': 4133}, 'two_connection_concurrency': {'status': 'pass', 'execution': 'overlapped'}, 'mixed_batch': {'status': 'pass'}, 'type_binding_rejection': {'distinct_from_data_validation': True}, 'illegal_categories': [{'status': 'pass', 'error': 'located error'}] * 10},
        'measurements.json': {'measurements': [{'peak_rss_bytes': 100, 'cold_start': {'wall_clock_seconds_median': 1}, 'results_consistent': True}] * 3, 'serial_concurrent_consistency_passed': True, 'host_threads': {'duckdb_threads': 2}, 'unseparated_costs': ['load'], 'no_throughput_threshold': 'none'},
        'minimal-runtime.json': {'status': 'pass', 'reference_tools_present': [], 'method': 'isolated container'},
        'independent-reproduction.json': {'status': 'pass', 'source_matrix_sha256': reproduction.EXPECTED_SOURCE_MATRIX_SHA256},
    }
    for path, record in records.items():
        write_json(directory / path, record)


class RealTraceabilityIntegrationTests(unittest.TestCase):
    def run_gate(self, *, skip=False, blocked=False, sc_failure=False, missing_runtime=False):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'gate'

            def environment(run_dir):
                write_traceability_evidence(run_dir)
                if skip or missing_runtime:
                    (run_dir / 'minimal-runtime.json').unlink()
                if blocked:
                    write_json(run_dir / 'sample-recovery.json', {'status': 'not_run'})
                if sc_failure:
                    path = run_dir / 'reference/reference-summary.json'
                    record = read_json(path)
                    record['independent_processes']['pids'] = [1, 1, 1]
                    write_json(path, record)
                return gate.StepOutcome('pass', 'portable evidence')

            def run_traceability(command, **kwargs):
                result = traceability.run_traceability(directory)
                write_json(directory / 'traceability.json', result)
                return subprocess.CompletedProcess(command, {'pass': 0, 'fail': 1, 'not_run': 2}[result['status']], result['status'], '')

            names = ('sample', 'sample_recovery', 'reference', 'independent_reproduction', 'comparator_self_check', 'build_extension', 'extension', 'measurements', 'minimal_runtime')
            argv = ['run_m0_gate.py', '--output-dir', str(directory)] + (['--skip-minimal-runtime'] if skip else [])
            with ExitStack() as stack:
                stack.enter_context(patch.object(gate, 'step_environment', environment))
                for name in names:
                    status = 'not_run' if blocked and name == 'sample_recovery' else 'pass'
                    stack.enter_context(patch.object(gate, f'step_{name}', return_value=gate.StepOutcome(status, 'portable step')))
                stack.enter_context(patch.object(gate, 'run_script', side_effect=run_traceability))
                stack.enter_context(patch.object(sys, 'argv', argv))
                code = gate.main()
            return code, read_json(directory / 'report.json'), read_json(directory / 'traceability.json')

    def test_skipped_runtime_is_incomplete_with_real_traceability(self):
        code, report, trace = self.run_gate(skip=True)
        self.assertEqual((code, report['overall'], trace['status']), (2, 'incomplete', 'not_run'))
        self.assertEqual(trace['failed_assertions'], [])
        self.assertEqual(trace['failed_success_criterion_assertions'], [])
        self.assertEqual(report['requirements']['FR-012']['status'], 'not_run')

    def test_blocked_recovery_is_incomplete_with_real_traceability(self):
        code, report, trace = self.run_gate(blocked=True)
        self.assertEqual((code, report['overall'], trace['status']), (2, 'incomplete', 'not_run'))
        self.assertEqual(report['requirements']['FR-003']['status'], 'not_run')
        self.assertIn('sample_recovery: not_run', report['requirements']['FR-003']['steps'])
        self.assertEqual(trace['matrix']['FR-003']['status'], 'not_run')
        self.assertEqual(trace['consistency_with_report']['mismatches'], [])
        self.assertEqual(trace['failed_assertions'], [])
        self.assertEqual(trace['failed_success_criterion_assertions'], [])

    def test_real_sc_only_failure_still_fails(self):
        code, report, trace = self.run_gate(sc_failure=True)
        self.assertEqual((code, report['overall'], trace['status']), (1, 'failed', 'fail'))
        self.assertEqual([item['assertion_id'] for item in trace['failed_success_criterion_assertions']], ['SC002-2'])
        self.assertTrue(all(record['status'] == 'pass' for record in report['requirements'].values()))

    def test_incomplete_run_does_not_mask_concrete_sc_failure(self):
        code, report, trace = self.run_gate(skip=True, sc_failure=True)
        self.assertEqual((code, report['overall'], trace['status']), (1, 'failed', 'fail'))
        self.assertIn('SC002-2', [item['assertion_id'] for item in trace['failed_success_criterion_assertions']])

    def test_blocked_recovery_does_not_mask_concrete_sc_failure(self):
        code, report, trace = self.run_gate(blocked=True, sc_failure=True)
        self.assertEqual((code, report['overall'], trace['status']), (1, 'failed', 'fail'))
        self.assertEqual(report['requirements']['FR-003']['status'], 'not_run')
        self.assertIn('SC002-2', [item['assertion_id'] for item in trace['failed_success_criterion_assertions']])

    def test_complete_portable_evidence_passes(self):
        code, report, trace = self.run_gate()
        self.assertEqual((code, report['overall'], trace['status']), (0, 'passed', 'pass'))

    def test_missing_evidence_from_claimed_pass_is_still_failure(self):
        code, report, trace = self.run_gate(missing_runtime=True)
        self.assertEqual((code, report['overall'], trace['status']), (1, 'failed', 'fail'))
        self.assertEqual(report['requirements']['FR-012']['status'], 'fail')
        self.assertIn('FR012-1', [item['assertion_id'] for item in trace['failed_assertions']])


class ReportSchemaTests(unittest.TestCase):
    def test_explicit_null_traceability_is_rejected_but_absent_is_compatible(self):
        report = build_synthetic_report('passed', {})
        validate_gate_report(report)
        report['traceability'] = None
        with self.assertRaises(ValidationError):
            validate_gate_report(report)
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            write_json(directory / 'report.json', report)
            with self.assertRaises(ValidationError):
                audit_report(directory)


class EnvironmentOverrideTests(unittest.TestCase):
    def test_empty_and_nonempty_environment_overrides(self):
        code = ('import json, run_m0_gate as g, reproduce_independently as r, verify_sample_recovery as s; '
                'print(json.dumps([str(g.DATA_VENV), str(g.REFERENCE_VENV), str(r.DATA_VENV), str(r.REFERENCE_VENV), str(s.DATA_VENV)]))')
        for value in ('', '/tmp/custom-m0-venv'):
            with self.subTest(value=value):
                env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent), ENSABC_M0_DATA_VENV=value, ENSABC_M0_REFERENCE_VENV=value)
                result = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True, check=True)
                data = value or '/tmp/ensabc-m0-data/.venv'
                reference = value or '/tmp/ensabc-m0-baseline'
                self.assertEqual(json.loads(result.stdout), [data, reference, data, reference, data])


if __name__ == '__main__':
    unittest.main()
