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
    def test_cnpg_image_keeps_version_tag_and_immutable_digest(self):
        self.assertRegex(cnpg_smoke.LOCK['database'], r':18\.6@sha256:[0-9a-f]{64}$')
        with self.assertRaises(RuntimeError):
            cnpg_smoke.admission_ready('The Cluster "database" is invalid: spec.imageName: Invalid value: "digest": Can\'t use just the image sha as we can\'t detect upgrades')
        self.assertTrue(cnpg_smoke.admission_ready('cluster.postgresql.cnpg.io/database serverside-applied (server dry run)'))
        self.assertFalse(cnpg_smoke.admission_ready('plugin connection not ready'))

    def test_live_identity_has_only_namespaced_pod_get_permission(self):
        image = 'test/image@sha256:' + '1' * 64
        objects = cnpg_smoke.renderer.render(image, image, 'cnpg-system', 'managed', ['auth'])['items']
        grants = [(obj['kind'], obj['metadata']['namespace'], rule['verbs'])
                  for obj in objects for rule in obj.get('rules', []) if 'pods' in rule['resources']]
        self.assertEqual(grants, [('Role', 'managed', ['get'])])
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

    def test_expected_command_failure_cannot_pass_on_diagnostic_text_alone(self):
        result = subprocess.CompletedProcess([], 0, stdout='unsupported actual PostgreSQL')
        with patch.object(cnpg_smoke.subprocess, 'run', return_value=result):
            with self.assertRaisesRegex(RuntimeError, 'unexpectedly succeeded'):
                cnpg_smoke.run('fixture', expect_failure=True)
        result.returncode = 1
        with patch.object(cnpg_smoke.subprocess, 'run', return_value=result):
            self.assertEqual(cnpg_smoke.run('fixture', expect_failure=True), result.stdout)

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
