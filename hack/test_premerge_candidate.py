"""Trust/publication/auth unit controls, not actual campaign acceptance."""
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import premerge_candidate as c
import recovery_cases as cases


class CandidateTests(unittest.TestCase):
    def test_trusted_exact_push_only(self):
        sha = 'a' * 40
        env = dict(GITHUB_REPOSITORY=c.REPO, GITHUB_EVENT_NAME='push', GITHUB_REF=c.BRANCH,
                   GITHUB_WORKFLOW_REF=c.WORKFLOW, GITHUB_SHA=sha, GITHUB_WORKFLOW_SHA=sha)
        origin = 'https://github.com/' + c.REPO + '.git'
        c.trust(env, sha, origin)
        for field, bad in [('GITHUB_REPOSITORY', 'fork/repo'), ('GITHUB_EVENT_NAME', 'pull_request_target'),
                           ('GITHUB_EVENT_NAME', 'pull_request'), ('GITHUB_REF', 'refs/heads/main'),
                           ('GITHUB_WORKFLOW_REF', c.WORKFLOW.replace('implementation/pr-g', 'main')),
                           ('GITHUB_SHA', 'b' * 40), ('GITHUB_WORKFLOW_SHA', 'b' * 40)]:
            with self.subTest(field=field, bad=bad), self.assertRaises(RuntimeError):
                c.trust({**env, field: bad}, sha, origin)
        with self.assertRaises(RuntimeError):
            c.trust(env, sha, 'https://github.com/fork/repo')

    def test_existing_and_ambiguous_tags_never_allow_rebuild(self):
        for code, text in [(0, ''), (1, 'unauthorized'), (1, 'connection reset'), (1, 'denied')]:
            with self.assertRaises(RuntimeError):
                c.require_absent(SimpleNamespace(returncode=code, stderr=text))
        c.require_absent(SimpleNamespace(returncode=1, stderr='manifest unknown'))
        self.assertTrue(all(':sha-' in v and ':v0.' not in v for v in c.references('a' * 40).values()))

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
            with patch.dict(os.environ, DOCKER_CONFIG=d), patch.object(cases, 'WORK', root), patch.object(cases.h, 'kube', side_effect=kube):
                self.assertEqual(cases.registry_pull_secrets(), [{'name': 'campaign-ghcr'}])
                self.assertEqual(len(seen), 3)
                self.assertFalse((root / 'registry-auth.json').exists())
            with patch.dict(os.environ, DOCKER_CONFIG=d), patch.object(cases, 'WORK', root), patch.object(cases.h, 'kube', side_effect=RuntimeError):
                with self.assertRaises(RuntimeError):
                    cases.registry_pull_secrets()
                self.assertFalse((root / 'registry-auth.json').exists())


if __name__ == '__main__':
    unittest.main()
