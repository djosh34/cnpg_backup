import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import cnpg_smoke
import godeps


class LifecycleHarness(unittest.TestCase):
    def test_manifest_remaining_matches_asserted_named_completions(self):
        expected = cnpg_smoke.MANDATORY_SCENARIOS
        self.assertEqual(len(expected), 15)
        for missing in (None, *expected):
            with self.subTest(missing=missing):
                report = {'completed': [name for name in expected if name != missing],
                          'remaining_mandatory': ['stale text'], 'release_qualified': False, 'pr_d_complete': False}
                cnpg_smoke.reconcile_scenarios(report)
                self.assertEqual(report['remaining_mandatory'], [missing] if missing else [])
                self.assertFalse(set(report['completed']) & set(report['remaining_mandatory']))
                self.assertFalse(report['release_qualified'])
                self.assertFalse(report['pr_d_complete'])
        for completed in ([expected[0], expected[0]], ['invented-case']):
            with self.assertRaises(AssertionError):
                cnpg_smoke.reconcile_scenarios({'completed': completed})

    def test_cnpg_image_keeps_version_tag_and_immutable_digest(self):
        self.assertRegex(cnpg_smoke.LOCK['database'], r':18\.6@sha256:[0-9a-f]{64}$')
        with self.assertRaises(RuntimeError):
            cnpg_smoke.admission_ready('The Cluster "database" is invalid: spec.imageName: Invalid value: "digest": Can\'t use just the image sha as we can\'t detect upgrades')
        self.assertTrue(cnpg_smoke.admission_ready('cluster.postgresql.cnpg.io/database serverside-applied (server dry run)'))
        self.assertFalse(cnpg_smoke.admission_ready('plugin connection not ready'))

    def test_live_identity_has_only_namespaced_pod_read_permissions(self):
        image = 'test/image@sha256:' + '1' * 64
        objects = cnpg_smoke.renderer.render(image, image, 'cnpg-system', 'managed', ['auth'])['items']
        grants = [(obj['kind'], obj['metadata']['namespace'], rule['verbs'])
                  for obj in objects for rule in obj.get('rules', []) if 'pods' in rule['resources']]
        self.assertEqual(len(grants), 1)
        self.assertEqual(grants[0][:2], ('Role', 'managed'))
        self.assertIn('get', grants[0][2])
        # G's all-retry-Pod completion evidence adds list/watch, not Pod writes,
        # exec, wildcard scope or a cluster-wide role. D's get-only subset remains
        # valid for the disjoint base; actual G requires completion in campaign.
        self.assertLessEqual(set(grants[0][2]), {'get', 'list', 'watch'})
        self.assertFalse(any('pods/exec' in rule['resources'] or '*' in rule['resources']
                             for obj in objects for rule in obj.get('rules', [])))

    def test_live_rollout_oracle_rejects_in_place_snapshot_rewrites(self):
        pod = {'metadata': {'uid': 'old', 'annotations': {
            'cnpg-backup.djosh34.github.io/owner': 'cluster',
            'cnpg-backup.djosh34.github.io/config': 'original-config'}},
            'spec': {'initContainers': [{'name': 'cnpg-backup', 'image': 'original-image',
                'admissionField': {'opaque': 'preserve'}, 'volumeMounts': [
                    {'name': 'kube-api-access-test', 'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount'}]}],
                'volumes': [{'name': 'config', 'projected': {'sources': ['original-config']}}]}}
        before = {'old': cnpg_smoke.placement_snapshot(pod)}
        cnpg_smoke.assert_live_snapshots(before, [copy.deepcopy(pod)])
        for change in ('image', 'admission', 'mount', 'volume', 'config'):
            with self.subTest(change=change):
                changed = copy.deepcopy(pod)
                sidecar = changed['spec']['initContainers'][0]
                if change == 'image':
                    sidecar['image'] = 'new-image'
                elif change == 'admission':
                    del sidecar['admissionField']
                elif change == 'mount':
                    sidecar['volumeMounts'] = []
                elif change == 'volume':
                    changed['spec']['volumes'][0]['projected']['sources'] = ['new-config']
                else:
                    changed['metadata']['annotations']['cnpg-backup.djosh34.github.io/config'] = 'new-config'
                with self.assertRaises(AssertionError):
                    cnpg_smoke.assert_live_snapshots(before, [changed])
                # A replacement Pod may (and must) use the newly evaluated spec.
                changed['metadata']['uid'] = 'replacement'
                cnpg_smoke.assert_live_snapshots(before, [changed])

    def native_matrix(self, fault=None):
        # Models only the pinned operator's configuration seam, not real CNPG
        # acceptance. The hosted matrix still executes real SQL and Go preflight.
        state = {'allow': 'off', 'summary': 'on', 'checks': [], 'fault_seen': False}
        def kube(*args, **kwargs):
            if args[:2] == ('get', 'cluster'):
                return json.dumps({'spec': {'postgresql': {'parameters': {'summarize_wal': 'on'}}}})
            if args[:2] == ('patch', 'cluster'):
                config = json.loads(args[args.index('-p') + 1])['spec']['postgresql']
                self.assertEqual(set(config), {'enableAlterSystem'})
                state['allow'] = 'on' if config['enableAlterSystem'] else 'off'
                return ''
            self.assertEqual(args[0], 'exec')
            if '--check-native' in args:
                state['checks'].append((state['summary'], state['allow']))
                if state['summary'] == 'off':
                    state['fault_seen'] = True
                    if fault == 'probe-error':
                        raise RuntimeError('injected probe transport error')
                    if fault == 'wrong-rejection':
                        return 'unrelated certificate failure'
                    if fault == 'false-success':
                        # Actual run() must enforce this, not just diagnostic text.
                        self.assertTrue(kwargs.get('expect_failure'))
                        raise RuntimeError('command unexpectedly succeeded')
                    return 'sidecar: unsupported actual PostgreSQL settings'
                return ''
            command = args[-1]
            if command.startswith('ALTER SYSTEM'):
                if state['allow'] != 'on':
                    raise RuntimeError('ERROR: ALTER SYSTEM is not allowed in this environment')
                if command.startswith('ALTER SYSTEM SET '):
                    state['summary'] = 'off'
                    if fault == 'set-response-loss':
                        raise RuntimeError('injected lost SET response')
                else:
                    state['summary'] = 'on'
                return ''
            if command == 'SELECT pg_reload_conf()':
                return 't\n'
            if command == 'SELECT pg_is_in_recovery()':
                return 'f\n'
            if command == 'SHOW summarize_wal':
                return state['summary'] + '\n'
            if command == 'SHOW allow_alter_system':
                return state['allow'] + '\n'
            self.fail('unexpected SQL: ' + command)
        report = {'completed': []}
        def wait(test, description, *args):
            self.assertTrue(test(), description)
        with tempfile.TemporaryDirectory() as temp, patch.object(cnpg_smoke, 'OUT', Path(temp)), \
                patch.object(cnpg_smoke, 'main_pods', return_value=[{'metadata': {'name': 'database-1'}}]), \
                patch.object(cnpg_smoke, 'kube', side_effect=kube), patch.object(cnpg_smoke, 'wait', side_effect=wait):
            try:
                cnpg_smoke.native_metadata_matrix(report)
            finally:
                self.assertEqual(state['summary'], 'on', 'summary fault was not restored')
                self.assertEqual(state['allow'], 'off', 'test-only ALTER SYSTEM permission leaked')
        return state, report

    def test_native_summary_fault_uses_cnpg_opt_in_and_restores_default(self):
        state, report = self.native_matrix()
        self.assertTrue(state['fault_seen'])
        self.assertEqual(state['checks'][0], ('on', 'off'))
        self.assertEqual(state['checks'][-1], ('on', 'off'))
        self.assertTrue(report['completed'])

    def test_native_summary_fault_cleanup_and_oracle_negative_controls(self):
        for fault, error in [('probe-error', 'injected probe transport error'),
                             ('wrong-rejection', 'unrelated certificate failure'),
                             ('false-success', 'command unexpectedly succeeded'),
                             ('set-response-loss', 'injected lost SET response')]:
            with self.subTest(fault=fault), self.assertRaisesRegex((RuntimeError, AssertionError), error):
                self.native_matrix(fault)

    def replay_recovery_matrix(self, report, zero_exits=(), wrong_cause=None):
        observations = json.loads((Path(__file__).parent / 'testdata/recovery-placement/observations.json').read_text())
        state = {'case': None, 'pv': 0, 'file_checks': []}
        def apply(document):
            if document.get('kind') == 'Cluster':
                state['case'] = document['metadata']['name'].removeprefix('recover-')
        def kube(*args, **kwargs):
            case = state['case']
            if args[:2] == ('get', 'pv'):
                state['pv'] += 1
                return json.dumps({'items': [{'metadata': {'name': 'fixture-' + str(state['pv'])},
                    'spec': {'storageClassName': 'cnpg-backup-bounded', 'local': {'path': '/fixture/' + str(state['pv'])}},
                    'status': {'phase': 'Available'}}]})
            if args[:2] == ('get', 'pods'):
                containers = copy.deepcopy(observations[case]['containers'])
                main = next(c for c in containers if c['name'] == 'full-recovery')
                if case in zero_exits:
                    main['state']['terminated'].update(exitCode=0, reason='Completed')
                return json.dumps({'items': [{'metadata': {'name': 'recover-' + case, 'uid': 'fixture-uid'},
                    'spec': {'containers': [{'name': 'full-recovery', 'command': [
                        '/cnpg-backup/bin/cnpg-backup', 'recovery-guard', '--', '/controller/manager', 'instance', 'restore']}]},
                    'status': {'containerStatuses': [main], 'initContainerStatuses': [c for c in containers if c != main]}}]})
            if args[0] == 'logs':
                logs = observations[case]['logs']
                if case == 'fresh':
                    # Preserve historical D logs on disk. This unit-only G
                    # projection is NOT executed CNPG materialization evidence.
                    logs = logs.replace('no plugin supports the restore job hooks capability',
                                        'rpc error: code = FailedPrecondition desc = protected full restore failed'
                                        if wrong_cause is None else wrong_cause)
                return logs
            self.assertIn(args[0], ('patch', 'annotate', 'delete'))
            return ''
        def run(*args, **kwargs):
            self.assertEqual(args[:3], ('docker', 'exec', cnpg_smoke.NAME + '-control-plane'))
            if args[3] == 'sh':
                self.assertEqual(args[4], '-ec')  # Fixture seeding only.
                return ''
            self.assertEqual(args[3], 'test')
            state['file_checks'].append((state['case'], args[4:]))
            if state['case'] == 'fresh':
                self.assertEqual(args[4:6], ('!', '-e'))
            else:
                self.assertEqual(args[4], '-f')
                self.assertTrue(args[-1].endswith('/preflight-sentinel'))
            return ''
        def wait(test, description, *args):
            self.assertTrue(test(), description)
        cluster = {'kind': 'Cluster', 'metadata': {}, 'spec': {'plugins': [{'parameters': {}}]}}
        repository = {'kind': 'Repository', 'metadata': {}, 'spec': {}}
        with tempfile.TemporaryDirectory() as temp, patch.object(cnpg_smoke, 'OUT', Path(temp)), \
                patch.object(cnpg_smoke, 'apply', side_effect=apply), patch.object(cnpg_smoke, 'kube', side_effect=kube), \
                patch.object(cnpg_smoke, 'run', side_effect=run), patch.object(cnpg_smoke, 'wait', side_effect=wait):
            cnpg_smoke.recovery_placement_matrix(cluster, repository, report)
        self.assertEqual(len(state['file_checks']), 14)  # 3 per poison, 2 preflight + 3 clean Drain markers.

    def test_recovery_matrix_historical_poison_and_synthetic_G_failure_complete_four_cases(self):
        report = {'completed': []}
        self.replay_recovery_matrix(report)
        self.assertEqual(report['completed'], list(cnpg_smoke.MANDATORY_SCENARIOS[10:14]))

    def test_recovery_matrix_rejects_zero_main_exits_before_completion(self):
        cases = ('pgdata', 'wal', 'tablespace', 'fresh')
        for changed in [(case,) for case in cases] + [cases]:
            with self.subTest(changed=changed):
                report = {'completed': []}
                with self.assertRaisesRegex(AssertionError, 'full-recovery.*nonzero'):
                    self.replay_recovery_matrix(report, zero_exits=changed)
                self.assertEqual(report['completed'], list(cnpg_smoke.MANDATORY_SCENARIOS[10:10 + cases.index(changed[0])]))

    def test_recovery_matrix_rejects_wrong_fresh_failure_cause_before_completion(self):
        for cause in ('unrelated certificate failure', 'TargetOwnershipUncertain', '', 'no plugin supports the restore job hooks capability'):
            with self.subTest(cause=cause):
                report = {'completed': []}
                with self.assertRaisesRegex(AssertionError, 'invalid materialization'):
                    self.replay_recovery_matrix(report, wrong_cause=cause)
                self.assertEqual(report['completed'], list(cnpg_smoke.MANDATORY_SCENARIOS[10:13]))

    def test_expected_command_failure_cannot_pass_on_diagnostic_text_alone(self):
        result = subprocess.CompletedProcess([], 0, stdout='unsupported actual PostgreSQL')
        with patch.object(cnpg_smoke.subprocess, 'run', return_value=result):
            with self.assertRaisesRegex(RuntimeError, 'unexpectedly succeeded'):
                cnpg_smoke.run('fixture', expect_failure=True)
        result.returncode = 1
        with patch.object(cnpg_smoke.subprocess, 'run', return_value=result):
            self.assertEqual(cnpg_smoke.run('fixture', expect_failure=True), result.stdout)

    def test_command_failure_and_timeout_never_reflect_secret_argv_or_output(self):
        args = ('kubectl', 'patch', 'secret', 'auth', '-p', 'DO-NOT-COPY')
        result = subprocess.CompletedProcess(args, 1, stdout='DO-NOT-COPY')
        for outcome in (result, subprocess.TimeoutExpired(args, 1, output='DO-NOT-COPY')):
            with self.subTest(outcome=type(outcome).__name__):
                options = {'side_effect': outcome} if isinstance(outcome, Exception) else {'return_value': outcome}
                with patch.object(cnpg_smoke.subprocess, 'run', **options):
                    with self.assertRaises(RuntimeError) as raised:
                        cnpg_smoke.run(*args)
                    self.assertNotIn('DO-NOT-COPY', str(raised.exception))
                    self.assertIn('withheld', str(raised.exception))

    def test_pod_collector_is_bounded_redacted_and_keeps_previous_failure(self):
        pod = {'metadata': {'name': 'database-1', 'uid': 'pod-uid', 'annotations': {'secret': 'DO-NOT-COPY'}},
               'spec': {'containers': [{'name': 'postgres', 'env': [{'name': 'PASSWORD', 'value': 'DO-NOT-COPY'}]}]},
               'status': {'phase': 'Running', 'initContainerStatuses': [
                   {'name': 'cnpg-backup', 'ready': False, 'restartCount': 1,
                    'state': {'waiting': {'reason': 'CrashLoopBackOff'}},
                    'lastState': {'terminated': {'exitCode': 1, 'reason': 'Error', 'message': 'token=DO-NOT-COPY'}}}]}}
        calls = []
        def kube(*args, **kwargs):
            calls.append(args)
            self.assertLessEqual(kwargs['timeout'], 10)
            self.assertTrue(any(str(arg).startswith('--request-timeout=') for arg in args))
            self.assertNotIn('secret', args)
            if args[:2] == ('get', 'pods'):
                return json.dumps({'items': [pod]})
            self.assertEqual(args[0], 'logs')
            self.assertIn('--limit-bytes=65536', args)
            self.assertIn('--tail=100', args)
            return ('runtime failure: native settings\nAuthorization: Bearer DO-NOT-COPY\n'
                    '-----BEGIN PRIVATE KEY-----\nDO-NOT-COPY\n-----END PRIVATE KEY-----\n'
                    'disposable-test-only-secret\n' + 'x' * 70000)
        with tempfile.TemporaryDirectory() as temp, patch.object(cnpg_smoke, 'OUT', Path(temp)), \
                patch.object(cnpg_smoke, 'kube', side_effect=kube):
            cnpg_smoke.collect_pod_evidence()
            files = list(Path(temp).glob('*'))
            self.assertTrue(files)
            text = '\n'.join(path.read_text() for path in files)
            self.assertNotIn('DO-NOT-COPY', text)
            self.assertNotIn('disposable-test-only-secret', text)
            self.assertIn('runtime failure: native settings', text)
            self.assertIn('CrashLoopBackOff', text)
            self.assertIn('<REDACTED>', text)
            self.assertTrue(any('--previous' in args for args in calls))
            self.assertTrue(all(path.stat().st_size <= 65536 for path in files if path.suffix == '.log'))

    def test_pod_collector_stops_requests_at_total_deadline(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(cnpg_smoke, 'OUT', Path(temp)), \
                patch.object(cnpg_smoke.time, 'monotonic', side_effect=[0, 61, 62, 63]), \
                patch.object(cnpg_smoke, 'kube') as kube:
            cnpg_smoke.collect_pod_evidence()
            kube.assert_not_called()
            self.assertTrue(json.loads((Path(temp) / 'pod-collection.json').read_text())['deadlineReached'])

    def test_pod_collector_errors_do_not_replace_original_failure(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(cnpg_smoke, 'OUT', Path(temp)), \
                patch.object(cnpg_smoke, 'kube', side_effect=RuntimeError('secret=DO-NOT-COPY')):
            cnpg_smoke.collect_pod_evidence()
            text = '\n'.join(path.read_text() for path in Path(temp).glob('*'))
            self.assertIn('RuntimeError', text)
            self.assertNotIn('DO-NOT-COPY', text)

    def test_kernel_known_missing_loop_node_uses_same_minor(self):
        self.assertEqual(cnpg_smoke.loop_device('/dev/loop8 (lost)\n'), '/dev/loop8')
        self.assertEqual(cnpg_smoke.loop_device('/dev/loop37\n'), '/dev/loop37')
        for value in ('/dev/sda', '/dev/loop8 (busy)', '/dev/loop8;evil', '/dev/loop../8', ''):
            with self.assertRaises(RuntimeError):
                cnpg_smoke.loop_device(value)

    def test_notice_obligations_do_not_become_go_packages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'go.mod').write_text('module example.test/fixture\n\ngo 1.27.1\n')
            (root / 'main.go').write_text('package main\nfunc main() {}\n')
            output = root / 'build/out'
            notice = output / 'go-notices/example.test/module@v1.0.0/LICENSE.go'
            notice.parent.mkdir(parents=True)
            notice.write_text('// unmodified package-local legal text\npackage legal\n')
            original = notice.read_bytes()
            def packages():
                return subprocess.run(['go', 'list', './...'], cwd=root, text=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      env={**os.environ, 'GOWORK': 'off', 'GOTOOLCHAIN': 'local'}, timeout=30)
            # Distinguishing negative control reproduces the hosted package glob
            # failure without needing race's unavailable local C compiler.
            self.assertNotEqual(packages().returncode, 0)
            godeps.isolate_output(output)
            result = packages()
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertEqual(result.stdout.strip(), 'example.test/fixture')
            self.assertEqual(notice.read_bytes(), original)
