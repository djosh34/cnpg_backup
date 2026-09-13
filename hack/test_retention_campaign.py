import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from campaign_plan import selected
from backup_metrics_smoke import samples


class RetentionCampaignTests(unittest.TestCase):
    def test_retention_metrics_coexist_without_unbounded_labels(self):
        text = 'cnpg_backup_repository_holders{repository_id="r",namespace="n",cluster="c"} 2\n'
        self.assertEqual(samples(text)[('cnpg_backup_repository_holders', 'r', 'n', 'c')], 2)
        with self.assertRaises(AssertionError):
            samples(text.replace('cluster="c"', 'cluster="c",operation_id="unbounded"'))

    def test_J_operational_metrics_coexist_with_backup_failure_scrapes(self):
        # Both b408 smoke runs and five differential negatives stopped in the
        # shared parser after J added these existing manager-owned series.
        retention = ('cnpg_backup_retention_workspace_available',
                     'cnpg_backup_retention_checked_timestamp_seconds')
        restore = ('cnpg_backup_restore_observation_known', 'cnpg_backup_restore_active',
                   'cnpg_backup_restore_uncertain', 'cnpg_backup_restore_lifetime_release_pending')
        text = 'cnpg_backup_failures_total{repository_id="r",namespace="n",cluster="c",backup_type="full"} 1\n'
        for name in retention:
            text += name + '{repository_id="r",namespace="n",cluster="c"} 1\n'
        for name in restore:
            text += name + '{namespace="n",cluster="c"} 1\n'
        parsed = samples(text)
        self.assertEqual(len(parsed), 7)
        self.assertEqual(parsed[('cnpg_backup_failures_total', 'r', 'n', 'c', 'full')], 1)
        for name in restore:
            self.assertEqual(parsed[(name, 'n', 'c')], 1)
        for damaged in (text.replace('cluster="c"', 'cluster="c",operation_id="unbounded"'),
                        text.replace('cnpg_backup_restore_active', 'cnpg_backup_unknown'),
                        text + text):
            with self.assertRaises(AssertionError):
                samples(damaged)

    def test_periodic_accounting_does_not_scan_unrelated_host_loop_devices(self):
        # Observed local I failure: all du/cgroup/df output arrived, then the
        # global losetup enumeration exhausted the10s sample budget. Actual
        # owned device association/teardown and the disk floor remain required.
        from campaign_fixture import Fixture
        from campaign_process import CommandFailure
        from recovery_campaign import Manifest
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            m = Manifest(root / 'evidence', {}, [])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {'disk_floor_gib': 5}, float('inf'))
            f.allocations = [{'state': 'mounted', 'device': '/dev/loop-owned'}]
            def run(*args, **kwargs):
                if 'losetup -l -n' in args[-1]:
                    raise CommandFailure('observed global-loop enumeration deadline')
                return 'owned backing bytes and cgroup/df observations'
            with patch('campaign_fixture.snapshot', return_value={'disk_available': 6 << 30}), patch.object(f, 'run', side_effect=run):
                f.account(force=True, maintain=False)
            with patch('campaign_fixture.snapshot', return_value={'disk_available': 4 << 30}), patch.object(f, 'run', side_effect=run):
                with self.assertRaisesRegex(CommandFailure, 'emergency free-space floor'):
                    f.account(force=True, maintain=False)

    def test_repeated_actor_install_keeps_verified_executable_bytes(self):
        # Actual non-root POSIX regression for local I failure3: the first
        # install chmod0555 made the second direct truncation fail EACCES.
        import hashlib
        import subprocess
        from types import SimpleNamespace
        from recovery_cases import Campaign
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            bundle = root / 'bundle'
            bundle.mkdir()
            target = root / 'installed-actor'
            def kube(*args, **kwargs):
                result = subprocess.run(args[args.index('--') + 1:], input=kwargs.get('input'),
                                        text=True, capture_output=True, timeout=5, check=True)
                return result.stdout
            fixture = SimpleNamespace(bundle={'directory': bundle, 'files': {}}, kube=kube)
            campaign = Campaign(None, None, fixture)
            for body in (b'first verified actor', b'next verified actor'):
                (bundle / 'actor').write_bytes(body)
                fixture.bundle['files']['actor'] = hashlib.sha256(body).hexdigest()
                campaign.install_actor('source-pod', str(target))
                self.assertEqual(target.read_bytes(), body)
                self.assertEqual(target.stat().st_mode & 0o777, 0o555)
            self.assertFalse(target.with_name('installed-actor.next').exists())

    def test_archive_boundary_writes_wal_before_switching_idle_segment(self):
        # Observed after I's independent full. Pinned PG18 probe: idle switch
        # names current segment at0/2000028 but never archives it; writing a
        # restore-point record first produces the actual completed segment.
        from types import SimpleNamespace
        from unittest.mock import Mock
        from recovery_cases import Campaign
        c = object.__new__(Campaign)
        written = False
        segment = '000000010000000000000002'
        def sql(namespace, pod, query):
            nonlocal written
            if 'pg_create_restore_point' in query:
                written = True
                return '0/2000028'
            if 'pg_switch_wal' in query:
                return segment
            if 'pg_ls_dir' in query:
                return '1' if written else '0'
            raise AssertionError(query)
        def wait(predicate, *args):
            self.assertTrue(predicate(), 'idle WAL switch did not create an archiveable boundary')
        c.h = SimpleNamespace(wait=wait)
        c.primary = lambda: 'source-primary'
        c.sql = sql
        c.fetch_archive = Mock(return_value='verified remote WAL bytes')
        self.assertEqual(c.archive(), 'verified remote WAL bytes')
        c.fetch_archive.assert_called_once_with(segment)

    def test_retention_is_actual_explicit_fixture_scope(self):
        cases = selected('recovery', ['retention-runtime'])
        self.assertEqual([c['id'] for c in cases], ['retention-runtime'])
        self.assertEqual(set(cases[0]['branches']), {'expiration-window', 'protected-replay', 'crashed-guard'})
        self.assertEqual(cases[0]['fixtures'], ['source', 'differential'])
