"""Cheap H oracle/registry checks before provisioning real immutable subjects."""
import contextlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from campaign_plan import selected
from recovery_cases import Campaign
from premerge_candidate import trust, REPO


class DifferentialCampaignTests(unittest.TestCase):
    def test_explicit_independent_branches_use_only_real_source_prerequisites(self):
        cases = selected('recovery', ['differential-native'])
        self.assertEqual(len(cases), 1)
        case = cases[0]
        self.assertEqual(case['fixtures'], ['source', 'differential'])
        self.assertEqual(case['requires'], [])
        self.assertEqual(case['branches'], ['reconstruction', 'remote-PITR-source-loss', 'missing-full', 'missing-summary', 'checksum', 'promotion', 'cancellation'])
        self.assertNotIn('same-segment', case['fixtures'])

    def test_each_branch_dispatches_its_distinguishing_actual_case_only(self):
        for branch in selected('recovery', ['differential-native'])[0]['branches']:
            manifest = SimpleNamespace(current_branch=branch, case=lambda name: contextlib.nullcontext(), event=lambda *args, **kwargs: None)
            campaign = Campaign(SimpleNamespace(), manifest, fixture=object())
            campaign.primary = lambda: 'source-pod'
            campaign.native_backup_commands = lambda pod: ['BASE_BACKUP', 'BASE_BACKUP INCREMENTAL', 'BASE_BACKUP INCREMENTAL']
            campaign.base = {'backup_uid': 'full', 'manifest_sha256': 'f' * 64}
            campaign.d1, campaign.d2 = [dict(kind='differential', backup_uid=uid, parent_backup_uid='full',
                                           root_backup_uid='full', root_manifest_sha256='f' * 64) for uid in ('d1', 'd2')]
            campaign.differential_sizes = {'F': 100, 'D1': 10, 'D2': 10}
            calls = []
            campaign.differential_restore = lambda remote=False: calls.append(('restore', remote))
            campaign.differential_failed = lambda fault: calls.append(('fault', fault))
            campaign.case_differential_native()
            expected = ('restore', branch == 'remote-PITR-source-loss') if branch in ('reconstruction', 'remote-PITR-source-loss') else ('fault', branch)
            self.assertEqual(calls, [expected])

    def test_checksum_helper_requests_fit_shared_node_without_changing_limits(self):
        pods = []
        def kube(*args, **kwargs):
            if args[:3] == ('get', 'pod', 'source-pod'):
                return json.dumps({'spec': {'volumes': [{'name': 'pgdata', 'persistentVolumeClaim': {'claimName': 'source-data'}}]}})
            if args[:3] == ('get', 'pod', 'h-checksums'):
                return json.dumps({'status': {'phase': 'Succeeded'}})
            if args[:2] == ('get', 'pods'):
                return json.dumps({'items': []})
            return ''
        fixture = SimpleNamespace(kube=kube, quiesce_pods=lambda namespace: None,
                                  apply=pods.append, LOCK={'database': 'pinned-PG18'},
                                  wait=lambda predicate, *args: self.assertTrue(predicate()))
        campaign = Campaign(SimpleNamespace(), SimpleNamespace(event=lambda *args, **kwargs: None), fixture=fixture)
        campaign.primary = lambda: 'source-pod'
        campaign.image_pull_secrets = []
        campaign.differential_checksum_change()
        resources = pods[0]['spec']['containers'][0]['resources']
        self.assertEqual(resources['limits'], {'memory': '128Mi', 'cpu': '1'})
        # Missing requests default to the1-CPU limit and the actual4-CPU fixture
        # rejected this Pod as Insufficient cpu before pg_checksums ever ran.
        self.assertEqual(resources.get('requests'), {'memory': '32Mi', 'cpu': '100m'})

    def test_H_live_source_is_not_deleted_before_capture_faults(self):
        campaign = Campaign(SimpleNamespace(fixtures=['source', 'differential']), None, fixture=object())
        with patch.object(campaign, 'destroy_source') as destroy:
            campaign.prepare_recovery()
            destroy.assert_not_called()
        campaign.args.fixtures = ['source']
        with patch.object(campaign, 'destroy_source') as destroy:
            campaign.prepare_recovery()
            destroy.assert_called_once_with()

    def test_command_oracle_distinguishes_full_from_incremental(self):
        text = '\n'.join(['unrelated log', 'received replication command: BASE_BACKUP (WAL true)',
                          'received replication command: BASE_BACKUP (INCREMENTAL true)',
                          'received replication command: START_REPLICATION'])
        h = SimpleNamespace(kube=lambda *args: text)
        campaign = Campaign(SimpleNamespace(), None, fixture=h)
        observed = campaign.native_backup_commands('pod')
        self.assertEqual(len(observed), 2)
        self.assertEqual(sum('INCREMENTAL' not in c for c in observed), 1)
        # A replacement full is rejected by the SAME counter used after faults.
        corrupted = observed + ['received replication command: BASE_BACKUP (WAL true)']
        self.assertNotEqual(sum('INCREMENTAL' not in c for c in corrupted), 1)

    def test_trusted_H_caller_does_not_admit_other_workflows_or_branches(self):
        sha = 'a' * 40
        branch = 'refs/heads/implementation/pr-h'
        env = {'GITHUB_REPOSITORY': REPO, 'GITHUB_EVENT_NAME': 'push', 'GITHUB_REF': branch,
               'GITHUB_WORKFLOW_REF': REPO + '/.github/workflows/pr-g-candidate.yml@' + branch,
               'GITHUB_SHA': sha, 'GITHUB_WORKFLOW_SHA': sha}
        trust(env, sha, 'https://github.com/' + REPO + '.git')
        for key, value in [('GITHUB_REF', 'refs/heads/attacker'), ('GITHUB_WORKFLOW_REF', 'other'), ('GITHUB_EVENT_NAME', 'pull_request_target')]:
            with self.assertRaises(RuntimeError):
                trust({**env, key: value}, sha, 'https://github.com/' + REPO + '.git')


if __name__ == '__main__':
    unittest.main()
