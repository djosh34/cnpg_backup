import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import campaign_ci
from campaign_plan import selected
from backup_metrics_smoke import samples
from premerge_candidate import trust, REPO


class RetentionCampaignTests(unittest.TestCase):
    def test_I_trusted_subject_uses_same_shared_recipe(self):
        # Regression for the observed first hosted failure: the caller admitted
        # I, but the shared adapter rejected its branch before provisioning.
        branch = 'refs/heads/implementation/pr-i'
        env = {'GITHUB_REPOSITORY': REPO, 'GITHUB_REF': branch, 'GITHUB_EVENT_NAME': 'push',
               'GITHUB_SHA': 'c' * 40, 'GITHUB_WORKFLOW_SHA': 'c' * 40,
               'GITHUB_WORKFLOW_REF': REPO + '/.github/workflows/pr-g-candidate.yml@' + branch,
               'TRUSTED_REF': 'implementation/pr-i', 'SUBJECT_SHA': 'c' * 40,
               'MANAGER_IMAGE': 'ghcr.io/djosh34/cnpg-backup-manager@sha256:' + 'a' * 64,
               'DATA_IMAGE': 'ghcr.io/djosh34/cnpg-backup-pg18@sha256:' + 'b' * 64,
               'GITHUB_RUN_ID': '1', 'SEEDS': '[1806]', 'PROFILE': 'recovery'}
        trust(env, 'c' * 40, 'https://github.com/' + REPO)
        with tempfile.TemporaryDirectory() as d:
            env['GITHUB_OUTPUT'] = str(Path(d) / 'outputs')
            inputs = Path(d) / 'inputs'
            with patch.dict(os.environ, env), patch.object(campaign_ci, 'INPUTS', inputs), patch.object(campaign_ci.subprocess, 'run') as git:
                campaign_ci.prepare()
                git.assert_called_once_with(['git', 'merge-base', '--is-ancestor', 'c' * 40, 'refs/remotes/origin/implementation/pr-i'], check=True)
            self.assertEqual(json.loads((inputs / 'series.json').read_text())['attempts'], [{'attempt': 1, 'seed': 1806, 'layout': 'monolithic'}])
        bad = dict(env, GITHUB_WORKFLOW_SHA='d' * 40)
        with self.assertRaises(RuntimeError):
            trust(bad, 'c' * 40, 'https://github.com/' + REPO)

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
        import base64
        import subprocess
        from recovery_cases import RETENTION_ACTOR_INSTALL
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / 'actor'
            for body in (b'first verified actor', b'next verified actor'):
                result = subprocess.run(['sh', '-ec', RETENTION_ACTOR_INSTALL, 'retention-install', str(target)],
                                        input=base64.b64encode(body), capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(target.read_bytes(), body)
                self.assertEqual(target.stat().st_mode & 0o777, 0o555)
            self.assertFalse(target.with_name('actor.next').exists())

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
