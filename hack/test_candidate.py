"""Offline controls for trusted candidate publication and immutable image identity."""
import contextlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import candidate as c


class CandidateTests(unittest.TestCase):
    def environment(self):
        return dict(GITHUB_REPOSITORY=c.REPO, GITHUB_EVENT_NAME='workflow_dispatch', GITHUB_REF=c.BRANCH,
                    GITHUB_WORKFLOW_REF=c.WORKFLOW, GITHUB_SHA='a' * 40, GITHUB_WORKFLOW_SHA='a' * 40,
                    GITHUB_RUN_ID='1', GITHUB_RUN_ATTEMPT='1', GITHUB_OUTPUT='outputs', GITHUB_STEP_SUMMARY='summary')

    def test_only_exact_main_workflow_can_publish(self):
        env = self.environment()
        origin = 'https://github.com/' + c.REPO + '.git'
        c.trust(env, 'a' * 40, origin)
        for field, bad in [('GITHUB_REPOSITORY', 'fork/repo'), ('GITHUB_EVENT_NAME', 'pull_request_target'),
                           ('GITHUB_EVENT_NAME', 'pull_request'), ('GITHUB_EVENT_NAME', 'push'),
                           ('GITHUB_REF', 'refs/heads/other'), ('GITHUB_WORKFLOW_REF', 'other-workflow'),
                           ('GITHUB_SHA', 'b' * 40), ('GITHUB_WORKFLOW_SHA', 'b' * 40)]:
            with self.subTest(field=field, bad=bad), self.assertRaises(RuntimeError):
                c.trust({**env, field: bad}, 'a' * 40, origin)
        for head, remote in [('main', origin), ('a' * 40, 'https://github.com/fork/repo')]:
            with self.assertRaises(RuntimeError):
                c.trust(env, head, remote)

    def test_only_positive_tag_absence_permits_publication(self):
        for code, text in [(0, ''), (1, 'unauthorized'), (1, 'connection reset'), (1, 'denied')]:
            with self.subTest(code=code, text=text), self.assertRaises(RuntimeError):
                c.require_absent(SimpleNamespace(returncode=code, stderr=text))
        for text in ('manifest unknown', 'no such manifest'):
            c.require_absent(SimpleNamespace(returncode=1, stderr=text))
        self.assertEqual(c.references('a' * 40), {
            f: 'ghcr.io/djosh34/cnpg-backup-' + f + ':sha-' + 'a' * 40 for f in ('manager', 'pg18')})

    def exercise(self, phase, fault=None):
        with tempfile.TemporaryDirectory() as d, contextlib.chdir(d), patch.dict(os.environ, self.environment(), clear=True):
            images = {f: 'sha256:' + digit * 64 for f, digit in [('manager', 'b'), ('pg18', 'c')]}
            path = Path('build/out/images.json')
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(images))
            calls = []
            def command(*args, **kwargs):
                calls.append(args)
                if args[:2] == ('git', 'rev-parse'):
                    return SimpleNamespace(stdout='a' * 40)
                if args[:3] == ('git', 'remote', 'get-url'):
                    return SimpleNamespace(stdout='https://github.com/' + c.REPO)
                if args[:3] == ('docker', 'manifest', 'inspect'):
                    if '--verbose' not in args:
                        present = fault == 'existing' and 'pg18' in args[-1]
                        return SimpleNamespace(returncode=0 if present else 1, stderr='manifest unknown')
                    flavor = 'pg18' if 'pg18' in args[-1] else 'manager'
                    manifest = {'Descriptor': {'digest': 'sha256:' + 'd' * 64},
                                'SchemaV2Manifest': {'config': {'digest': images[flavor]}}}
                    if fault == 'wrong-config':
                        manifest['SchemaV2Manifest']['config']['digest'] = 'sha256:' + 'f' * 64
                    if fault == 'invalid-digest':
                        manifest['Descriptor']['digest'] = 'mutable-tag'
                    return SimpleNamespace(stdout=json.dumps(manifest))
                if args[:2] == ('docker', 'push') and fault == 'second-push' and 'pg18' in args[-1]:
                    raise subprocess.CalledProcessError(1, args)
                return SimpleNamespace(stdout='')
            with patch.object(c, 'run', side_effect=command):
                if fault:
                    with self.assertRaises((RuntimeError, subprocess.CalledProcessError)):
                        c.main([phase])
                else:
                    c.main([phase])
            record = Path('artifacts/candidate/candidate.json')
            if phase == 'preflight' or fault in ('existing', 'wrong-config', 'invalid-digest'):
                self.assertFalse(record.exists())
            else:
                result = json.loads(record.read_text())
                self.assertEqual(set(result['images']), {'manager'} if fault == 'second-push' else {'manager', 'pg18'})
                self.assertEqual(result['subject_sha'], 'a' * 40)
                self.assertFalse(result['release_qualified'])
                for flavor, image in result['images'].items():
                    self.assertEqual(image['config_digest'], images[flavor])
                    self.assertEqual(image['digest_reference'], 'ghcr.io/djosh34/cnpg-backup-' + flavor + '@sha256:' + 'd' * 64)
                    self.assertIn(flavor + '=' + image['digest_reference'], Path('outputs').read_text())
            if phase == 'preflight' or fault == 'existing':
                self.assertFalse(any(args[:2] in (('docker', 'tag'), ('docker', 'push')) for args in calls))
            else:
                self.assertEqual([args[:3] for args in calls[2:4]], [('docker', 'manifest', 'inspect')] * 2)

    def test_preflight_checks_both_tags_before_build_or_publication(self):
        self.exercise('preflight')
        self.exercise('preflight', 'existing')
        self.exercise('publish', 'existing')

    def test_published_images_must_match_audited_configs_and_valid_digests(self):
        self.exercise('publish')
        self.exercise('publish', 'wrong-config')
        self.exercise('publish', 'invalid-digest')

    def test_partial_publication_preserves_first_image_record(self):
        self.exercise('publish', 'second-push')
