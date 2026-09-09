"""Required operational evidence rejects missing measurements and false zeros."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from recovery_cases import Campaign, SOURCE_ID

from campaign_fixture import resource_values
from campaign_process import CommandFailure

SAMPLE = '''rss_kib=30000
hwm_kib=40000
memory.current=80000000
memory.peak=100000000
memory.max=3221225472
oom=0
oom_kill=0
native_processes=0
native_work_groups=0
blocks=2000000
available=1900000
free=1950000
block_size=4096
'''


class ResourceEvidence(unittest.TestCase):
    def test_kernel_process_cgroup_and_workspace_are_distinct(self):
        got = resource_values(SAMPLE)
        self.assertEqual(got['go_rss_bytes'], 30720000)
        self.assertEqual(got['go_hwm_bytes'], 40960000)
        self.assertEqual(got['cgroup_peak_bytes'], 100000000)
        self.assertEqual(got['workspace_used_bytes'], 204800000)
        self.assertEqual(got['workspace_capacity_bytes'], 8192000000)

    def test_actual_case_enforces_process_and_native_cgroup_independently(self):
        c = Campaign(None, None, SimpleNamespace(product_resources=Mock()))
        good = dict(resource_values(SAMPLE), native_processes=2, native_work_groups=1)
        c.h.product_resources.return_value = good
        self.assertEqual(c.operational_sample('pod', idle=True), good)
        for field, value in [('go_rss_bytes', 128 << 20), ('go_hwm_bytes', 256 << 20),
                             ('cgroup_peak_bytes', (3 << 30) + 1), ('oom', 1),
                             ('native_work_groups', 2), ('workspace_capacity_bytes', 100 << 30)]:
            c.h.product_resources.return_value = dict(good, **{field: value})
            with self.subTest(field=field), self.assertRaises(AssertionError):
                c.operational_sample('pod', idle=True)

    def test_initial_fixture_export_and_replay_preserve_json_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = SimpleNamespace(WORK=root / 'work', OUT=root / 'out')
            fixture.WORK.mkdir(); fixture.OUT.mkdir()
            payload = b'retained initial-format bytes'
            imported = []
            def curl(*args):
                if '--output' in args:
                    Path(args[args.index('--output')+1]).write_bytes(payload)
                    Path(args[args.index('--dump-header')+1]).write_text('x-amz-meta-checksum: unchanged\n')
                else:
                    imported.append(Path(args[args.index('--upload-file')+1]).read_bytes())
            fixture.run = curl
            campaign = Campaign(None, None, fixture)
            campaign.wal = SimpleNamespace(directory=root, endpoint='https://store', s3=Mock())
            campaign.initial_candidate = {'images': {'pg18': 'test-image'}}
            campaign.primary = Mock(return_value='database-1')
            campaign.sql = Mock(return_value='18.6')
            campaign.differential_expected = '1:expected'
            campaign.base = {'backup_uid': 'full'}; campaign.d2 = {'backup_uid': 'd2'}
            campaign.inventory = Mock(return_value=['smoke/v1/' + SOURCE_ID + '/object'])
            campaign.gate = Mock(return_value={'holders': [], 'owner': None})
            archive, manifest = campaign.retain_initial_fixture({'backupID': 'd2'})
            campaign.replay_initial_fixture(archive, manifest)
            self.assertEqual(imported, [payload])
            manifest['target']['backupID'] = 'changed'
            with self.assertRaisesRegex(AssertionError, 'provenance changed'):
                campaign.replay_initial_fixture(archive, manifest)

    def test_transfer_wal_requires_real_callback_retry_and_remote_byte_verification(self):
        campaign = Campaign(None, None, None)
        campaign.sql = Mock(side_effect=['', '000000010000000000000012'])
        campaign.event = Mock()
        wal = Mock()
        segment, latency = campaign.wal_during_transfer('database-1', wal)
        self.assertEqual(segment, '000000010000000000000012')
        self.assertGreaterEqual(latency, 0)
        self.assertEqual(wal.method_calls, [unittest.mock.call.rpc('database-1', 'archive', segment),
            unittest.mock.call.rpc('database-1', 'archive', segment), unittest.mock.call.verify('database-1', segment)])
        for method in ('rpc', 'verify'):
            campaign.sql.side_effect = ['', segment]
            wal = Mock()
            getattr(wal, method).side_effect = CommandFailure('actual callback/byte oracle failed')
            with self.subTest(method=method), self.assertRaises(CommandFailure):
                campaign.wal_during_transfer('database-1', wal)

    def test_source_ready_wait_uses_existing_accounted_wait_not_blocking_kubectl(self):
        import json
        calls = []
        def kube(*args):
            self.assertEqual(args[:3], ('get', 'cluster', 'database'))
            # Readiness appears only once finite backing has been replenished.
            return json.dumps({'status': {'conditions': [{'type': 'Ready', 'status': 'True' if calls else 'False'}]}})
        def wait(predicate, description, seconds):
            self.assertFalse(predicate())
            calls.append('account/replenish')
            self.assertTrue(predicate())
        campaign = Campaign(None, None, SimpleNamespace(kube=kube, wait=wait))
        campaign.wait_source_ready()
        self.assertEqual(calls, ['account/replenish'])

    def test_missing_unbounded_or_impossible_is_not_healthy_zero(self):
        for invalid in (SAMPLE.replace('rss_kib=30000\n', ''), SAMPLE.replace('3221225472', 'max'),
                        SAMPLE.replace('rss_kib=30000', 'rss_kib=0'),
                        SAMPLE.replace('free=1950000', 'free=2100000')):
            with self.subTest(invalid=invalid), self.assertRaises(CommandFailure):
                resource_values(invalid)


if __name__ == '__main__':
    unittest.main()
