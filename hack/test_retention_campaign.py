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

    def test_retention_is_actual_explicit_fixture_scope(self):
        cases = selected('recovery', ['retention-runtime'])
        self.assertEqual([c['id'] for c in cases], ['retention-runtime'])
        self.assertEqual(set(cases[0]['branches']), {'expiration-window', 'protected-replay', 'crashed-guard'})
        self.assertEqual(cases[0]['fixtures'], ['source', 'differential'])
