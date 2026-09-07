"""Failure-path tests of the probe's actual helpers/finalizer, without starting PG."""
import ast
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class ProbeCleanupTests(unittest.TestCase):
    def helpers(self, root):
        tree = ast.parse(Path(__file__).with_name('s1_switch_probe.py').read_text())
        namespace = {'B': Path('/test-only-pg'), 'ROOT': root, 'ENV': {}, 'COMMANDS': [],
                     'SERVERS': [], 'subprocess': subprocess, 'json': json, 'primary_error': None}
        definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
        exec(compile(ast.Module(body=definitions, type_ignores=[]), '<actual-probe-helpers>', 'exec'), namespace)
        return namespace, tree

    def test_timeout_records_partial_output_before_propagating(self):
        with tempfile.TemporaryDirectory() as tmp:
            ns, _ = self.helpers(Path(tmp))
            error = subprocess.TimeoutExpired(['pg_ctl'], 60, output=b'partial stdout', stderr=b'partial stderr')
            with patch('subprocess.run', side_effect=error):
                with self.assertRaises(subprocess.TimeoutExpired):
                    ns['run']('pg_ctl', 'status')
            self.assertEqual(len(ns['COMMANDS']), 1)
            command = ns['COMMANDS'][0]
            self.assertIsNone(command['exit'])
            self.assertEqual(command['stdout'], 'partial stdout')
            self.assertEqual(command['stderr'], 'partial stderr')
            self.assertEqual(command['timeout_seconds'], 60)

    def test_failed_fast_stop_falls_back_and_verifies_owned_server(self):
        for timeout in (False, True):
            with self.subTest(timeout=timeout), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                data = root / 'owned'; data.mkdir(); (data / 'postmaster.pid').touch()
                ns, _ = self.helpers(root); ns['SERVERS'][:] = [data, data]
                calls = []
                def run(argv, **kwargs):
                    calls.append(argv)
                    if argv[-1] == 'status':
                        return subprocess.CompletedProcess(argv, 0 if (data / 'postmaster.pid').exists() else 3, '', '')
                    self.assertIn('-t', argv)
                    self.assertLessEqual(kwargs['timeout'], 15)
                    if 'fast' in argv:
                        if timeout:
                            raise subprocess.TimeoutExpired(argv, kwargs['timeout'], output=b'stop pending')
                        return subprocess.CompletedProcess(argv, 1, '', 'fast stop failed')
                    self.assertIn('immediate', argv)
                    (data / 'postmaster.pid').unlink()
                    return subprocess.CompletedProcess(argv, 0, 'stopped', '')
                with patch('subprocess.run', side_effect=run):
                    self.assertFalse(ns['cleanup']('original trial failure'))
                self.assertEqual(len(calls), 4)
                self.assertTrue((root / 'commands.json').exists())
                report = json.loads((root / 'cleanup.json').read_text())
                self.assertEqual(report['primary_error'], 'original trial failure')
                self.assertTrue(report['servers'][0]['stopped'])
                self.assertTrue(report['servers'][0]['errors'])

    def test_unresolved_cleanup_does_not_replace_original_trial_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / 'owned'; data.mkdir(); (data / 'postmaster.pid').touch()
            ns, tree = self.helpers(root); ns['SERVERS'][:] = [data]
            # Execute the actual top-level exception/finalizer ownership, with
            # only its trial body replaced by a deterministic original failure.
            finalizer = copy.deepcopy(next(node for node in tree.body if isinstance(node, ast.Try)))
            finalizer.body = ast.parse("raise AssertionError('original trial failure')").body
            code = compile(ast.fix_missing_locations(ast.Module(body=[finalizer], type_ignores=[])), '<actual-finalizer>', 'exec')
            def run(argv, **kwargs):
                return subprocess.CompletedProcess(argv, 0 if argv[-1] == 'status' else 1, '', 'still running')
            with patch('subprocess.run', side_effect=run):
                with self.assertRaisesRegex(AssertionError, '^original trial failure$'):
                    exec(code, ns)
            self.assertEqual(len(json.loads((root / 'commands.json').read_text())), 4)
            report = json.loads((root / 'cleanup.json').read_text())
            self.assertFalse(report['servers'][0]['stopped'])
            self.assertIn('original trial failure', report['primary_error'])
            # Conversely, an otherwise successful trial cannot hide unresolved
            # owned-server cleanup behind its earlier positive assertions.
            ns['primary_error'] = None
            ns['COMMANDS'] = []
            finalizer.body = [ast.Pass()]
            code = compile(ast.fix_missing_locations(ast.Module(body=[finalizer], type_ignores=[])), '<actual-finalizer>', 'exec')
            with patch('subprocess.run', side_effect=run):
                with self.assertRaisesRegex(RuntimeError, 'cleanup unresolved'):
                    exec(code, ns)
            self.assertIsNone(json.loads((root / 'cleanup.json').read_text())['primary_error'])


if __name__ == '__main__':
    unittest.main()
