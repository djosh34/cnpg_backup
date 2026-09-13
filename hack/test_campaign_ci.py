"""Offline checks for trusted campaign inputs and private registry credentials."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import campaign_ci
import recovery_cases as cases


class CampaignCITests(unittest.TestCase):
    def environment(self, directory):
        return dict(GITHUB_REPOSITORY='djosh34/cnpg_backup', GITHUB_EVENT_NAME='workflow_dispatch',
                    GITHUB_REF='refs/heads/main', TRUSTED_REF='main', SUBJECT_SHA='c' * 40,
                    MANAGER_IMAGE='ghcr.io/djosh34/cnpg-backup-manager@sha256:' + 'a' * 64,
                    DATA_IMAGE='ghcr.io/djosh34/cnpg-backup-pg18@sha256:' + 'b' * 64,
                    GITHUB_RUN_ID='1', GITHUB_OUTPUT=str(directory / 'outputs'),
                    SEEDS='[1806, 1806, 1807]', PROFILE='recovery')

    def test_trusted_subject_prepares_each_requested_attempt(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / 'inputs'
            with patch.dict(os.environ, self.environment(root), clear=True), \
                 patch.object(campaign_ci, 'INPUTS', inputs), patch.object(campaign_ci.subprocess, 'run') as git:
                campaign_ci.prepare()
                git.assert_called_once_with(['git', 'merge-base', '--is-ancestor', 'c' * 40,
                                             'refs/remotes/origin/main'], check=True)
            self.assertEqual(json.loads((inputs / 'series.json').read_text())['attempts'], [
                {'attempt': n, 'seed': seed, 'layout': 'monolithic'}
                for n, seed in enumerate((1806, 1806, 1807), 1)])
            subject = json.loads((inputs / 'subject.json').read_text())
            self.assertEqual(subject['revision'], 'c' * 40)
            self.assertEqual(subject['images']['manager'], self.environment(root)['MANAGER_IMAGE'])
            matrix = json.loads((root / 'outputs').read_text().removeprefix('matrix='))
            self.assertEqual(matrix['include'], [{'attempt': n} for n in (1, 2, 3)])

    def test_untrusted_or_invalid_inputs_never_create_recipes(self):
        changes = [('GITHUB_REPOSITORY', 'fork/repo'), ('GITHUB_EVENT_NAME', 'pull_request_target'),
                   ('GITHUB_EVENT_NAME', 'pull_request'), ('GITHUB_REF', 'refs/heads/untrusted'),
                   ('TRUSTED_REF', '../untrusted'), ('SUBJECT_SHA', 'main'),
                   ('MANAGER_IMAGE', 'ghcr.io/djosh34/cnpg-backup-manager:latest'),
                   ('DATA_IMAGE', 'other.invalid/pg18@sha256:' + 'b' * 64),
                   ('SEEDS', '[]'), ('SEEDS', '[true]'), ('SEEDS', '[-1]'),
                   ('SEEDS', '[2147483648]'), ('SEEDS', '[1,2,3,4,5]'),
                   ('PROFILE', 'qualification')]
        for field, value in changes:
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as d:
                root = Path(d)
                inputs = root / 'inputs'
                with patch.dict(os.environ, {**self.environment(root), field: value}, clear=True), \
                     patch.object(campaign_ci, 'INPUTS', inputs), patch.object(campaign_ci.subprocess, 'run'):
                    with self.assertRaises(ValueError):
                        campaign_ci.prepare()
                self.assertFalse(inputs.exists())

    def test_failed_ancestry_check_never_creates_recipes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / 'inputs'
            with patch.dict(os.environ, self.environment(root), clear=True), \
                 patch.object(campaign_ci, 'INPUTS', inputs), \
                 patch.object(campaign_ci.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'git')):
                with self.assertRaises(subprocess.CalledProcessError):
                    campaign_ci.prepare()
            self.assertFalse(inputs.exists())

    def test_private_auth_is_scoped_and_removed_even_on_failure(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / 'config.json').write_text(json.dumps({'auths': {
                'ghcr.io': {'auth': 'test-token'}, 'other.invalid': {'auth': 'never-project'}}}))
            seen = []
            def kube(*args):
                text = (root / 'registry-auth.json').read_text()
                self.assertNotIn('never-project', text)
                self.assertEqual((root / 'registry-auth.json').stat().st_mode & 0o777, 0o600)
                self.assertNotIn('test-token', ' '.join(args))
                seen.append(args)
            with patch.dict(os.environ, DOCKER_CONFIG=d), patch.object(cases.h, 'WORK', root), patch.object(cases.h, 'kube', side_effect=kube):
                self.assertEqual(cases.registry_pull_secrets(), [{'name': 'campaign-ghcr'}])
                self.assertEqual(len(seen), 3)
                self.assertFalse((root / 'registry-auth.json').exists())
            with patch.dict(os.environ, DOCKER_CONFIG=d), patch.object(cases.h, 'WORK', root), patch.object(cases.h, 'kube', side_effect=RuntimeError):
                with self.assertRaises(RuntimeError):
                    cases.registry_pull_secrets()
                self.assertFalse((root / 'registry-auth.json').exists())
