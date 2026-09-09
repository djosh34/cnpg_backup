"""Exercise native fault injection through the smoke caller, not status spelling."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import backup_smoke


class ObservedKill(Exception):
    """Stop after the first KILL's complete existing safety oracles."""


class NativeFaultObservationTests(unittest.TestCase):
    def exercise(self, *, exit_code=137, reason='Error', never_exits=False,
                 replacement_pod=False, child_survives=False, committed=False,
                 stop_signal='KILL', wrong_container=False, missing_record=False):
        state = {'count': 2, 'id': 'original-term', 'fault': None, 'name': None,
                 'native': True, 'inspections': 0}
        checks = []
        scratch = {'roots': ['capture', 'repository'], 'allocated_bytes': 1024,
                   'native_locks': {'81': '/cnpg-backup/work/native.lock'}}
        holder = {'kind': 'backup', 'operation_id': 'full-signal-kill-1'}

        def pod():
            return {'metadata': {'uid': 'replacement' if replacement_pod and state['fault'] == 'KILL' else 'original-pod'},
                    'status': {'initContainerStatuses': [{'name': 'cnpg-backup',
                        'containerID': 'containerd://' + state['id'], 'restartCount': state['count'],
                        'state': {'running': {}}, 'lastState': {}}]}}

        def die(signal):
            state.update(original_id=state['id'], fault=signal, count=state['count'] + 1,
                         id='replacement-' + str(state['count'] + 1), inspections=0,
                         native=child_survives and signal == 'KILL')

        def kube(*args, **kwargs):
            if args[:2] == ('get', 'pod'):
                return json.dumps(pod())
            if args[0] == 'get' and args[1].startswith('backup/'):
                return json.dumps({'metadata': {'uid': state['name']},
                                   'status': {'phase': 'failed' if state['fault'] else 'running'}})
            if args[0] == 'exec':
                action = args[args.index('--') + 2]
                if action == 'instance':
                    return ''
                if action == 'native':
                    checks.append('child')
                    return json.dumps([81] if state['native'] else [])
                if action == 'signal-sidecar':
                    die('TERM')
                    return ''
                if action == 'pause-native':
                    return '[81]'
                if action == 'oom':
                    die('OOM')
                    return json.dumps({'bounded_cgroup_precondition': True})
                if action == 'scratch':
                    return json.dumps(scratch if not state['fault'] else {'roots': [], 'allocated_bytes': 0})
            raise AssertionError(('unexpected kube', args))

        def run(*args, **kwargs):
            if args[3:5] == ('crictl', 'inspect'):
                if not state['fault']:
                    return json.dumps({'status': {'id': state['id'], 'state': 'CONTAINER_RUNNING'}, 'info': {'pid': 123}})
                # The original CRI container is retained even when the API has
                # projected the replacement without lastState. First inspect
                # can still precede CRI's terminal-state update.
                self.assertEqual(args[5], state['original_id'])
                self.assertEqual(kwargs['timeout'], 10)
                if missing_record:
                    raise RuntimeError('original CRI record unavailable')
                state['inspections'] += 1
                exited = state['inspections'] > 1 and not never_exits
                return json.dumps({'status': {'id': 'wrong' if wrong_container else args[5],
                    'state': 'CONTAINER_EXITED' if exited else 'CONTAINER_RUNNING',
                    'exitCode': exit_code if state['fault'] == stop_signal else 137,
                    'reason': reason if state['fault'] == stop_signal else 'Error'}})
            if args[3:5] == ('kill', '-9'):
                die('KILL')
                return ''
            raise AssertionError(('unexpected run', args))

        def apply(document):
            state.update(name=document['metadata']['name'], fault=None, native=True)
            holder['operation_id'] = state['name']

        def wait(predicate, description, seconds=180):
            self.assertLessEqual(seconds, 360)
            for _ in range(3):
                if predicate():
                    return
            raise RuntimeError('barrier timed out: ' + description)

        def s3(method, key):
            if key.endswith('gate.json'):
                return json.dumps({'holders': [holder]})
            checks.append('no-commit')
            contents = '<Contents><Key>commit.json</Key></Contents>' if committed and state['fault'] == 'KILL' else ''
            return '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>' + contents + '</ListBucketResult>'

        def sql(pod, query):
            if query.startswith('UPDATE'):
                return ''
            if 'pg_stat_progress_basebackup' in query:
                return '1'
            if 'pg_postmaster_start_time' in query:
                checks.append('postmaster')
                return 'unchanged'
            raise AssertionError(query)

        def assert_failed(name, before, **kwargs):
            checks.append('metrics')
            if state['fault'] in ('KILL', 'OOM'):
                self.assertGreaterEqual(state['inspections'], 2)
                self.assertEqual(checks[-4:], ['postmaster', 'child', 'no-commit', 'metrics'])
                if state['fault'] == stop_signal:
                    raise ObservedKill()

        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory) / 'control'
            control.write_bytes(b'test actor')
            h = SimpleNamespace(NS='test', NAME='kind', WORK=Path(directory), kube=kube,
                                run=run, apply=apply, wait=wait)
            wal = SimpleNamespace(primary=lambda: 'database-1', sql=sql, s3=s3)
            metrics = SimpleNamespace(snapshot=Mock(return_value={}), assert_failed=assert_failed)
            report = {'full_fault_preconditions': [], 'full_remaining': ['SIGTERM', 'process-death'], 'full_completed': []}
            with patch.object(backup_smoke.subprocess, 'run', return_value=SimpleNamespace(returncode=0)):
                backup_smoke.capture_faults(h, wal, report, metrics, control)

    def test_missing_api_last_state_still_proves_original_cri_exit(self):
        with self.assertRaises(ObservedKill):
            self.exercise()

    def test_wrong_exit_is_not_waived(self):
        with self.assertRaisesRegex(AssertionError, 'not killed'):
            self.exercise(exit_code=1)

    def test_no_terminal_proof_is_bounded_failure(self):
        with self.assertRaisesRegex(RuntimeError, 'barrier timed out'):
            self.exercise(never_exits=True)

    def test_oom_requires_original_oom_exit_not_just_137(self):
        with self.assertRaises(ObservedKill):
            self.exercise(stop_signal='OOM', reason='OOMKilled')
        with self.assertRaisesRegex(AssertionError, 'not actually OOM-killed'):
            self.exercise(stop_signal='OOM', reason='Error')
        with self.assertRaisesRegex(AssertionError, 'not killed'):
            self.exercise(stop_signal='OOM', reason='OOMKilled', exit_code=1)

    def test_missing_or_wrong_original_cri_record_fails(self):
        with self.assertRaisesRegex(RuntimeError, 'original CRI record unavailable'):
            self.exercise(missing_record=True)
        with self.assertRaisesRegex(AssertionError, 'container identity changed'):
            self.exercise(wrong_container=True)

    def test_same_name_replacement_pod_is_not_original_restart(self):
        with self.assertRaisesRegex(AssertionError, 'Pod.*replaced'):
            self.exercise(replacement_pod=True)

    def test_native_child_and_commit_oracles_remain_required(self):
        with self.assertRaisesRegex(AssertionError, 'native child survived'):
            self.exercise(child_survives=True)
        with self.assertRaisesRegex(AssertionError, 'selectable incomplete'):
            self.exercise(committed=True)
