import copy
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import cnpg_smoke
import godeps


class LifecycleHarness(unittest.TestCase):
    def test_cnpg_image_keeps_version_tag_and_immutable_digest(self):
        self.assertRegex(cnpg_smoke.LOCK['database'], r':18\.6@sha256:[0-9a-f]{64}$')
        with self.assertRaises(RuntimeError):
            cnpg_smoke.admission_ready('The Cluster "database" is invalid: spec.imageName: Invalid value: "digest": Can\'t use just the image sha as we can\'t detect upgrades')
        self.assertTrue(cnpg_smoke.admission_ready('cluster.postgresql.cnpg.io/database serverside-applied (server dry run)'))
        self.assertFalse(cnpg_smoke.admission_ready('plugin connection not ready'))

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
