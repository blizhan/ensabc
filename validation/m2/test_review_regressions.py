"""Regression coverage for review findings: evidence, isolation, binding and identity mapping."""
import copy
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from validation.m2.common import atomic_write_json, parse_duckdb_json, run_isolated_duckdb, real_replay_binding
from validation.m2.reproduce_independently import semantic_history
from validation.m2.verify_report import generate_report, execution_complete, REQUIRED_STEPS


class ReviewRegressions(unittest.TestCase):
    def test_cli_json_multiple_results(self):
        self.assertEqual(parse_duckdb_json(' \n[{"a": 1}]\n[] '), [[{"a": 1}], []])
        with self.assertRaises(ValueError):
            parse_duckdb_json('[] broken')

    def test_duckdb_home_is_fresh_and_removed(self):
        homes = []
        def check(*args, **kwargs):
            home = Path(kwargs['env']['HOME'])
            homes.append(home)
            self.assertEqual(list(home.iterdir()), [])
            self.assertIn('SET autoload_known_extensions = false', kwargs['input'])
            self.assertIn('-bail', args[0])
            return None
        with patch('validation.m2.common.subprocess.run', side_effect=check):
            run_isolated_duckdb('SELECT 1;')
            run_isolated_duckdb('SELECT 1;')
        self.assertNotEqual(homes[0], homes[1])
        self.assertTrue(all(not p.exists() for p in homes))

    def test_version_binds_actual_payload_and_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'batch.om').write_bytes(b'first')
            atomic_write_json(root / 'om_sample_manifest.json', {'batches': [{'ordinal': 0, 'om_filename': 'batch.om'}]})
            extension, duckomo = root / 'ensabc', root / 'duckomo'
            extension.write_bytes(b'extension')
            duckomo.write_bytes(b'duckomo')
            first = real_replay_binding(root, extension, duckomo)
            self.assertEqual(first, real_replay_binding(root, extension, duckomo))
            (root / 'batch.om').write_bytes(b'changed')
            self.assertNotEqual(first['run_id'], real_replay_binding(root, extension, duckomo)['run_id'])

    def test_missing_execution_cannot_pass_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            from validation.m2.schema import REQUIREMENT_IDS, SUCCESS_CRITERION_IDS
            atomic_write_json(root / 'traceability.json', {
                'requirements': {k: {'status': 'pass'} for k in REQUIREMENT_IDS},
                'success_criteria': {k: {'status': 'pass'} for k in SUCCESS_CRITERION_IDS},
            })
            report = generate_report(root)
            self.assertEqual(report['overall_status'], 'failed')
            self.assertEqual(report['steps'], [])
            steps = [{'step': k, 'status': 'pass', 'exit_code': 0} for k in REQUIRED_STEPS]
            self.assertTrue(execution_complete(steps))
            steps[0]['exit_code'] = 2
            self.assertFalse(execution_complete(steps))

    def test_gate_preserves_missing_prerequisite_exit_code(self):
        from validation.m2.run_m2_gate import run_step
        import subprocess
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"ENSABC_M2_RUN_DIR": directory}):
            with self.assertRaises(SystemExit) as raised:
                run_step("missing", ["python3", "-c", "raise SystemExit(2)"])
            self.assertEqual(raised.exception.code, 2)
            import json
            record = json.loads((Path(directory) / "execution-steps.json").read_text())["steps"][0]
            self.assertEqual(record["exit_code"], 2)
            self.assertEqual(record["status"], "not_run")

    def test_reproduction_compares_numbers_and_births(self):
        baseline = {'nodes': [{'branch_id': 'build1111111:b00:k01', 'mean_features': [280.0]}], 'links': [{'distance': 1.0}]}
        rebuilt = copy.deepcopy(baseline)
        rebuilt['nodes'][0]['branch_id'] = 'build2222222:b00:k01'
        self.assertEqual(
            semantic_history(baseline, expected_run_key='build1111111abcd'),
            semantic_history(rebuilt, expected_run_key='build2222222efgh'),
        )
        with self.assertRaises(ValueError):
            semantic_history(rebuilt, expected_run_key='forgedprefix0000')
        rebuilt['links'][0]['distance'] = 1.1
        self.assertNotEqual(
            semantic_history(baseline, expected_run_key='build1111111abcd'),
            semantic_history(rebuilt, expected_run_key='build2222222efgh'),
        )

    def test_audit_rejects_path_traversal_evidence(self):
        from validation.m2.verify_report import audit_report
        from validation.m2.schema import REQUIREMENT_IDS, SUCCESS_CRITERION_IDS, REPORT_SCHEMA_VERSION
        with tempfile.TemporaryDirectory() as outer:
            outer_path = Path(outer)
            escaped = outer_path / 'secret.json'
            escaped.write_text('{}')
            run_dir = outer_path / 'run'
            run_dir.mkdir()
            from validation.m2.common import sha256_file
            esc_sha = sha256_file(escaped)
            report = {
                'schema_version': REPORT_SCHEMA_VERSION,
                'run_id': 'run',
                'created_at_utc': '2026-10-30T00:00:00Z',
                'overall_status': 'passed',
                'requirements': {
                    k: {'status': 'pass', 'assertion_id': f'A_{k}', 'observed': 'ok',
                        'evidence_path': '../secret.json', 'evidence_sha256': esc_sha}
                    for k in REQUIREMENT_IDS
                },
                'success_criteria': {
                    k: {'status': 'pass', 'assertion_id': f'A_{k}', 'observed': 'ok',
                        'evidence_path': '../secret.json', 'evidence_sha256': esc_sha}
                    for k in SUCCESS_CRITERION_IDS
                },
                'steps': [],
            }
            atomic_write_json(run_dir / 'report.json', report)
            audit = audit_report(run_dir)
            self.assertEqual(audit['audit_status'], 'failed')
            self.assertTrue(any('escapes run_dir' in f for f in audit['findings']))

    def test_overlap_mapping_ignores_cluster_labels(self):
        from validation.m2.run_case_and_sensitivity import compare_mapped_history
        def batch(reverse):
            return {'res': {'scenarios': [{'cluster_id': 0}, {'cluster_id': 1}],
                            'members': [{'member_id': 'a', 'cluster_id': int(reverse)},
                                        {'member_id': 'b', 'cluster_id': int(not reverse)}]}}
        base = {'batches': [batch(False), batch(False)], 'pairs': [
            {'prev_ordinal': 0, 'curr_ordinal': 1, 'links': [
                {'prev_cluster_id': 0, 'curr_cluster_id': 0}, {'prev_cluster_id': 1, 'curr_cluster_id': 1}]}]}
        current = {'batches': [batch(True), batch(False)], 'pairs': [
            {'prev_ordinal': 0, 'curr_ordinal': 1, 'links': [
                {'prev_cluster_id': 1, 'curr_cluster_id': 0}, {'prev_cluster_id': 0, 'curr_cluster_id': 1}]}]}
        result = compare_mapped_history(base, current)
        self.assertFalse(result['pairs'][0]['changed'])
        self.assertEqual(result['identity_nodes_checked'], 4)
        self.assertEqual(result['identity_changes'], 0)


if __name__ == '__main__':
    unittest.main()
