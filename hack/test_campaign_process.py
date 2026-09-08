"""Real child-process regressions, not modeled campaign coverage."""
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from campaign_process import Commands, CommandFailure


class ProcessTests(unittest.TestCase):
    def test_empty_nonzero_is_not_a_successful_barrier(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d))
            result = commands.command('probe', sys.executable, '-c', 'raise SystemExit(7)')
            self.assertEqual(result.returncode, 7)
            self.assertFalse(result.ok)
            self.assertEqual(result.stdout, '')
            with self.assertRaisesRegex(CommandFailure, 'probe.*exit=7'):
                result.require()

    def test_wait_caps_child_and_reports_layer_and_last_status(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d))
            start = time.monotonic()
            with self.assertRaisesRegex(CommandFailure, 'adoption.*deadline'):
                commands.wait(lambda: commands.command('process-observation', sys.executable, '-c',
                    'import time; time.sleep(30)', timeout=30).ok, 'adoption', .15)
            self.assertLess(time.monotonic() - start, 2)
            self.assertIn('process-observation', (Path(d) / 'commands.jsonl').read_text())

    def test_output_is_bounded_and_separated_and_secrets_do_not_hide_admission_reason(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d), output_limit=4096)
            result = commands.command('admission', sys.executable, '-c',
                'import sys; print("x"*100000); print("Invalid: spec.volumes duplicate secret reference; password=hidden", file=sys.stderr)')
            self.assertTrue(result.ok)
            self.assertLessEqual(len(result.stdout.encode()), 4096)
            self.assertGreater(result.dropped_bytes, 90000)
            with self.assertRaisesRegex(CommandFailure, 'incomplete oracle input'):
                result.require()
            self.assertIn('duplicate secret reference', result.stderr)
            self.assertNotIn('hidden', result.stderr)
            self.assertNotIn('hidden', (Path(d) / 'commands.jsonl').read_text())

    def test_expected_failure_cannot_accept_timeout(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d))
            with self.assertRaisesRegex(CommandFailure, 'deadline'):
                commands.run(sys.executable, '-c', 'import time; time.sleep(30)',
                             expect_failure=True, timeout=.1)

    def test_actual_helper_255_with_stderr_keeps_exit_marker_in_stdout(self):
        from recovery_cases import Campaign
        with tempfile.TemporaryDirectory() as d:
            result = Commands(Path(d)).command('actual-helper-output-control', sys.executable, '-c',
                'import sys; print("wal-fetch: recovery fetch failed", file=sys.stderr); print("\\nCAMPAIGN_EXIT=255")')
            fixture = Mock()
            fixture.kube.return_value = result.stdout + result.stderr
            fixture.kube_result.return_value = result
            campaign = Campaign(None, Mock(), fixture)
            campaign.helper({'pod': 'owned-negative'}, '000000010000000000000003', 255)
            with self.assertRaisesRegex(AssertionError, 'helper exit differs'):
                campaign.helper({'pod': 'owned-negative'}, '000000010000000000000003', 1)

    def test_archive_config_identity_and_node_consumption_do_not_use_docker_display_id(self):
        import hashlib, io, json, tarfile
        from campaign_fixture import Fixture
        from campaign_plan import image_config_digest
        from campaign_process import CommandResult
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            tag = 'cb-repair-minio:test'
            raw = json.dumps({'os': 'linux', 'architecture': 'amd64', 'rootfs': {'type': 'layers', 'diff_ids': []}}).encode()
            expected = 'sha256:' + hashlib.sha256(raw).hexdigest()
            archive = directory / 'minio.tar'
            with tarfile.open(archive, 'w') as out:
                for name, data in [('config.json', raw), ('manifest.json', json.dumps([{'Config': 'config.json', 'RepoTags': [tag], 'Layers': []}]).encode())]:
                    member = tarfile.TarInfo(name); member.size = len(data)
                    out.addfile(member, io.BytesIO(data))
            self.assertEqual(image_config_digest(archive, tag), expected)
            with self.assertRaisesRegex(ValueError, 'requested image'):
                image_config_digest(archive, 'wrong:tag')
            for wrong in (False, True):
                fixture = Fixture.__new__(Fixture)
                fixture.NAME, fixture.WORK = 'cb-repair-aaaaaaaaaaaa', directory
                fixture.bundle = {'directory': directory, 'images': {'minio': {'tag': tag, 'archive': 'minio.tar', 'config_digest': expected}},
                    'files': {'minio.tar': hashlib.sha256(archive.read_bytes()).hexdigest()}}
                manifest = json.dumps({'config': {'digest': 'sha256:' + 'f' * 64 if wrong else expected}})
                manifest_digest = 'sha256:' + hashlib.sha256(manifest.encode()).hexdigest()
                calls = []
                def run(*args, **kwargs):
                    calls.append(args)
                    if 'list' in args:
                        return f'docker.io/library/{tag} media-type {manifest_digest} 100'
                    return ''
                fixture.run = run
                fixture.commands = Mock()
                fixture.commands.command.return_value = CommandResult('node-manifest', 0, manifest, '', 0, False, 0)
                fixture.m = Mock(data={})
                if wrong:
                    with self.assertRaisesRegex(ValueError, 'config differs'):
                        fixture.image_digest('minio')
                else:
                    fixture.image_digest('minio')
                    self.assertEqual(fixture.m.data['fixture_images']['minio']['config_digest'], expected)
                self.assertTrue(any('image-archive' in args for args in calls))
                self.assertFalse(any('inspect' in args or 'docker-image' in args for args in calls))

    def test_container_absence_requires_successful_list_not_error_spelling(self):
        from campaign_fixture import Fixture
        fixture = Fixture.__new__(Fixture)
        fixture.NAME = 'cb-repair-aaaaaaaaaaaa'
        name = fixture.NAME + '-control-plane'
        fixture.run = Mock(return_value='')
        self.assertFalse(fixture.container_exists(name))
        self.assertEqual(fixture.run.call_args.args[:3], ('docker', 'container', 'ls'))
        fixture.run.return_value = name + '\n'
        self.assertTrue(fixture.container_exists(name))
        fixture.run.side_effect = CommandFailure('daemon unavailable, empty stdout')
        with self.assertRaises(CommandFailure):
            fixture.container_exists(name)

    def test_quiesced_owned_bind_disposal_and_unknown_mount_rejection(self):
        from campaign_fixture import Fixture
        from types import SimpleNamespace
        for foreign in (False, True):
            fixture = Fixture.__new__(Fixture)
            fixture.NAME = 'cb-repair-aaaaaaaaaaaa'
            fixture.quiesced = True
            path, device = '/var/local/cnpg-backup-work-1', '/dev/loop105'
            bind = '/unowned/data' if foreign else '/var/lib/kubelet/pods/1c0a27c8-00da-4e6a-bf00-0c677b62ef90/volumes/kubernetes.io~local-volume/campaign-1'
            calls = []
            def run(*args, **kwargs):
                if 'losetup' in args:
                    return device
                if 'observe-owned-mounts' in args:
                    return path + ('\n' + bind if not calls else '')
                if 'umount' in args:
                    calls.append(('unmount', args[-1]))
                if 'retire-owned' in args:
                    calls.append(('retired', path))
                return ''
            def wait(predicate, *args):
                if not predicate():
                    raise CommandFailure('consumer bind remains')
            fixture.run = run
            fixture.commands = SimpleNamespace(wait=wait)
            fixture.m, fixture.record_ownership = Mock(), Mock()
            allocation = {'path': path, 'device': device, 'state': 'mounted'}
            if foreign:
                with self.assertRaisesRegex(CommandFailure, 'unexpected.*mount'):
                    fixture.retire_backing(allocation)
                self.assertEqual(calls, [])
            else:
                fixture.retire_backing(allocation)
                self.assertEqual(calls, [('unmount', bind), ('retired', path)])
                self.assertEqual(allocation['state'], 'retired')

    def test_missing_kubeconfig_blocks_dependent_collectors_without_fake_failures(self):
        from campaign_fixture import Fixture
        with tempfile.TemporaryDirectory() as d:
            fixture = Fixture.__new__(Fixture)
            fixture.WORK = Path(d)
            fixture.m = Mock(data={'failures': [{'phase': 'setup', 'id': 1}]})
            fixture.save_log = Mock()
            self.assertEqual(fixture.collect(), [])
            self.assertIn('blocked', fixture.save_log.call_args.args[1])
            fixture.m.event.assert_called_once_with('collection-blocked', requirement='fixture kubeconfig', caused_by=[1])

    def test_absent_node_cleanup_is_idempotent_without_accepting_daemon_failure(self):
        from campaign_fixture import Fixture
        with tempfile.TemporaryDirectory() as d:
            fixture = Fixture.__new__(Fixture)
            fixture.NAME = 'cb-repair-aaaaaaaaaaaa'
            fixture.WORK = Path(d) / 'private'
            fixture.WORK.mkdir()
            fixture.run = Mock(return_value='')
            fixture.listener = None
            fixture.allocations = []
            fixture.record_ownership = Mock()
            fixture.close()
            self.assertTrue(fixture.closed)
            self.assertFalse(fixture.WORK.exists())
            fixture.run.side_effect = CommandFailure('Docker authorization denied')
            with self.assertRaises(CommandFailure):
                fixture.close()
