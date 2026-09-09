"""Offline negative controls; never claimed as final-image/scanner acceptance."""
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import campaign_ci
import native_metadata as native
import security as s


class SecurityTests(unittest.TestCase):
    def test_native_control_source_version_not_binary_name_guess(self):
        text = b'Package: libpq5\nVersion: 18.6-3.pgdg24.04+1\nArchitecture: amd64\nSource: postgresql-18 (18.6-3.pgdg24.04+1)\n'
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w:gz') as tar:
            member = tarfile.TarInfo('./control')
            member.size = len(text)
            tar.addfile(member, io.BytesIO(text))
        data = archive.getvalue()
        header = f'{"control.tar.gz/":<16}{0:<12}{0:<6}{0:<6}{100644:<8}{len(data):<10}`\n'.encode()
        fields = native.control(b'!<arch>\n' + header + data)
        self.assertEqual(native.source_identity(fields), ('postgresql-18', '18.6-3.pgdg24.04+1'))
        self.assertIn('Source: postgresql-18 (18.6-3.pgdg24.04+1)', native.status_record(
            {'name': 'libpq5', 'version': fields['Version']}, fields))
        with self.assertRaises(ValueError):
            native.status_record({'name': 'libpq5', 'version': 'other'}, fields)
        self.assertEqual(native.source_identity({'Package': 'zlib1g', 'Version': '1:1.3', 'Source': 'zlib'}), ('zlib', '1:1.3'))

    def test_native_control_uses_existing_dpkg_for_python312_zstd(self):
        with patch.object(native.shutil, 'which', return_value='/usr/bin/dpkg-deb'), \
             patch.object(native.subprocess, 'check_output', return_value='Package: libc6\nVersion: 1\n') as tool:
            self.assertEqual(native.control_file(Path('locked.deb'))['Package'], 'libc6')
            tool.assert_called_once_with(['dpkg-deb', '--field', 'locked.deb'], text=True, timeout=30)

    def test_real_source_lock_covers_every_potential_runtime_package(self):
        packages = json.loads((s.REPO / 'build/inputs.lock.json').read_text())['packages']
        sources = json.loads((s.REPO / 'build/native-sources.lock.json').read_text())['sources']
        required = {p['name'] for p in packages if not p.get('test_only')}
        self.assertEqual(required, {p for source in sources for p in source['binary_packages']})
        for source in sources:
            self.assertTrue(source['files'][0]['url'].endswith('.dsc'))
            self.assertGreater(len(source['files']), 1)

    def test_exact_trusted_subject_not_fork_tag_or_privileged_pr(self):
        images = ['ghcr.io/djosh34/cnpg-backup-' + f + '@sha256:' + 'b' * 64 for f in s.FLAVORS]
        env = dict(GITHUB_REPOSITORY='djosh34/cnpg_backup', GITHUB_EVENT_NAME='workflow_dispatch',
                   GITHUB_REF='refs/heads/implementation/pr-j', GITHUB_RUN_ID='1', GITHUB_RUN_ATTEMPT='1')
        with patch.dict(os.environ, env, clear=True), patch.object(s, 'run') as run, \
             patch.object(s.subprocess, 'check_output', return_value='c' * 40):
            subject = s.subject('a' * 40, *images, 'implementation/pr-j')
            self.assertNotEqual(subject['revision'], subject['harness_revision'])
            run.assert_called_once_with('git', 'merge-base', '--is-ancestor', 'a' * 40, 'refs/remotes/origin/implementation/pr-j')
            for key, value in [('GITHUB_REPOSITORY', 'fork/repo'), ('GITHUB_EVENT_NAME', 'pull_request_target'),
                               ('GITHUB_REF', 'refs/heads/implementation/pr-j-security')]:
                with patch.dict(os.environ, {key: value}), self.assertRaises(ValueError):
                    s.subject('a' * 40, *images, 'implementation/pr-j')
            with self.assertRaises(ValueError):
                s.subject('a' * 40, images[0].split('@')[0] + ':latest', images[1], 'main')

    def test_high_unknown_and_any_secret_block_and_matches_are_redacted(self):
        report = {'Results': [{'Target': '/file', 'Vulnerabilities': [
            {'VulnerabilityID': 'CVE-test', 'PkgName': 'libc6', 'InstalledVersion': '1', 'Severity': 'HIGH'}],
            'Secrets': [{'RuleID': 'key', 'Severity': 'LOW', 'Match': 'private-value',
                         'Code': {'Lines': [{'Content': 'private-value'}]}}]}]}
        blockers = s.trivy_gate(report)
        self.assertEqual(len(blockers), 2)
        self.assertNotIn('private-value', json.dumps(report))
        self.assertNotIn('private-value', json.dumps(blockers))
        self.assertEqual(len(s.trivy_gate({'Results': []})), 0)
        report['Results'][0]['Vulnerabilities'][0]['Severity'] = 'UNKNOWN'
        self.assertEqual(len(s.trivy_gate(report)), 2)

    def test_no_findings_is_not_proof_scanner_saw_native_packages(self):
        p = {'name': 'libpq5', 'version': '18.6-3.pgdg24.04+1'}
        report = {'Metadata': {'OS': {'Family': 'ubuntu', 'Name': '24.04'}}, 'Results': [
            {'Class': 'os-pkgs', 'Packages': [{'Name': p['name'], 'Version': p['version']}]},
            {'Type': 'gobinary', 'Packages': [{'Name': 'stdlib'}]}]}
        s.scan_coverage(report, [p])
        for broken in ({'Results': []}, {**report, 'Metadata': {}},
                       {**report, 'Results': report['Results'][1:]},
                       {**report, 'Results': report['Results'][:1]}):
            with self.assertRaises(ValueError):
                s.scan_coverage(broken, [p])
        with self.assertRaises(ValueError):
            s.scan_coverage(report, [{**p, 'version': 'wrong'}])

    def test_actual_export_inventory_rejects_added_shell_and_native_tamper(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(s, 'WORK', root), patch.object(s, 'OUT', root / 'evidence'):
                def archive(extra=None):
                    # docker export includes Moby's empty executable marker,
                    # unlike the prepared root used by the old fixture.
                    files = {'.dockerenv': (b'', 0o755),
                             'usr/local/bin/cnpg-backup': (b'ELF fixture', 0o755),
                             'usr/share/cnpg-backup/native-packages.json': (b'[{"name":"ca-certificates"}]', 0o644)}
                    if extra:
                        files.update(extra)
                    path = root / 'image.tar'
                    with tarfile.open(path, 'w') as tar:
                        for name, (content, mode) in files.items():
                            entry = tarfile.TarInfo(name)
                            entry.mode, entry.size = mode, len(content)
                            tar.addfile(entry, io.BytesIO(content))
                    return path
                self.assertEqual(s.image_files(archive(), 'manager'), [{'name': 'ca-certificates'}])
                inventory = json.loads((root / 'evidence/manager-files.json').read_text())
                self.assertEqual(inventory['/.dockerenv'], {
                    'sha256': hashlib.sha256(b'').hexdigest(), 'bytes': 0, 'mode': '0o755'})
                for name, content, mode in (('bin/sh', b'shell', 0o755),
                                            ('.other', b'', 0o755),
                                            ('.dockerenv', b'shell', 0o755),
                                            ('.dockerenv', b'', 0o777),
                                            ('.dockerenv', b'', 0o4755)):
                    with self.subTest(name=name, content=content, mode=mode), self.assertRaises(ValueError):
                        s.image_files(archive({name: (content, mode)}), 'manager')
                with self.assertRaises(ValueError):
                    s.image_files(archive(), 'pg18')
                tools = ('pg_basebackup', 'pg_verifybackup', 'pg_combinebackup', 'pg_waldump', 'pg_controldata', 'psql')
                extra = {'usr/lib/postgresql/18/bin/' + tool: (b'tool', 0o755) for tool in tools}
                entries = [{'path': '/' + name, 'sha256': hashlib.sha256(content).hexdigest()}
                           for name, (content, _) in extra.items()]
                extra['usr/share/cnpg-backup/native-files.json'] = (json.dumps(entries).encode(), 0o644)
                s.image_files(archive(extra), 'pg18')
                extra['usr/lib/postgresql/18/bin/psql'] = (b'tampered', 0o755)
                with self.assertRaisesRegex(ValueError, 'differs from recorded closure'):
                    s.image_files(archive(extra), 'pg18')

    def test_download_checksum_failure_never_executes_or_silently_replaces(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'scanner'
            path.write_bytes(b'bad')
            with patch.object(s.urllib.request, 'urlopen') as network, self.assertRaises(ValueError):
                s.fetch({'url': 'https://example.invalid', 'sha256': hashlib.sha256(b'good').hexdigest()}, path)
            network.assert_not_called()
            self.assertEqual(path.read_bytes(), b'bad')

    def test_prior_release_requires_record_exact_bytes_and_tag_ancestry(self):
        images = {f: 'ghcr.io/djosh34/cnpg-backup-' + f + '@sha256:' + 'b' * 64 for f in s.FLAVORS}
        subject = {'revision': 'a' * 40, 'images': images}
        with patch.object(campaign_ci.subprocess, 'check_output', return_value=json.dumps(subject)) as read, \
             patch.object(campaign_ci.subprocess, 'run') as run:
            self.assertEqual(campaign_ci.prior_release_subject('v0.1.0', 'a' * 40, images, 'main'), subject)
            self.assertIn('refs/remotes/origin/main:.github/release-subjects/v0.1.0.json', read.call_args.args[0])
            self.assertEqual(run.call_count, 2)
            for tag, sha, refs, ref in [('v0.1.0', 'c' * 40, images, 'main'),
                                      ('../../x', 'a' * 40, images, 'main'),
                                      ('v0.1.0', 'a' * 40, images, 'implementation/pr-j'),
                                      ('v0.1.0', 'a' * 40, {**images, 'manager': 'other'}, 'main')]:
                with self.assertRaises(ValueError):
                    campaign_ci.prior_release_subject(tag, sha, refs, ref)
            run.side_effect = RuntimeError('not ancestor')
            with self.assertRaises(RuntimeError):
                campaign_ci.prior_release_subject('v0.1.0', 'a' * 40, images, 'main')


if __name__ == '__main__':
    unittest.main()
