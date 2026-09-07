"""Local feedback uses the CI harness without starting expensive dependencies."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import bootstrap


class FeedbackEntrypoint(unittest.TestCase):
    def invoke(self, *args, fail_harness=False):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'hack').mkdir()
            shutil.copyfile(bootstrap.REPO / 'hack/test', root / 'hack/test')
            tools = root / 'tools'
            tools.mkdir()
            # Simulate external commands, not the shell's profile dispatch or
            # pipefail behavior. No recursive test suite, downloads or daemon.
            tool = tools / 'tool'
            tool.write_text('''#!/usr/bin/env bash
printf '%s %s\\n' "${0##*/}" "$*" >> "$CALLS"
if [[ ${0##*/} = python3 && $* = '-m unittest'* && ${FAIL_HARNESS:-} = 1 ]]; then exit 7; fi
if [[ ${0##*/} = python3 && $1 = hack/test_tools.py ]]; then printf '%s\\n' "$PROMTOOL"; fi
''')
            tool.chmod(0o755)
            for name in ('python3', 'go', 'gofmt', 'promtool'):
                (tools / name).symlink_to(tool)
            (root / 'build').mkdir()
            (root / 'build/test-tools.lock.json').write_text('{}')
            env = dict(os.environ, PATH=str(tools) + ':' + os.environ['PATH'],
                       CNPG_BUILD_CACHE=str(root / 'cache'), CALLS=str(root / 'calls'),
                       PROMTOOL=str(tools / 'promtool'), FAIL_HARNESS='1' if fail_harness else '0')
            result = subprocess.run(['bash', str(root / 'hack/test'), *args], env=env,
                                    capture_output=True, text=True, timeout=10)
            calls = (root / 'calls').read_text() if (root / 'calls').exists() else ''
            return result.returncode, calls

    def test_harness_does_not_bootstrap_toolchains(self):
        code, calls = self.invoke('harness')
        self.assertEqual(code, 0)
        self.assertIn('unittest discover', calls)
        self.assertIn('backup_metrics_smoke.py --self-test', calls)
        self.assertIn('repository_crd.py --check', calls)
        self.assertNotIn('bootstrap.py', calls)
        self.assertNotIn('go test', calls)

    def test_targeted_oracle_runs_directly(self):
        code, calls = self.invoke('harness', 'test_recovery.CurlInvocation')
        self.assertEqual(code, 0)
        self.assertEqual(calls, 'python3 -m unittest test_recovery.CurlInvocation\n')

    def test_unit_verifies_go_but_does_not_build_images_or_fetch_native(self):
        code, calls = self.invoke('unit')
        self.assertEqual(code, 0)
        self.assertIn('bootstrap.py --go-only', calls)
        self.assertIn('go vet ./...', calls)
        self.assertIn('go test -count=1 -timeout=120s ./...', calls)
        self.assertNotIn('hack/build.py', calls)
        self.assertNotIn('recovery.py', calls)

    def test_harness_failure_stops_fast_before_downloads(self):
        code, calls = self.invoke('fast', fail_harness=True)
        self.assertEqual(code, 7)
        self.assertNotIn('bootstrap.py', calls)

    def test_unknown_profile_fails_before_work(self):
        code, calls = self.invoke('unknown')
        self.assertEqual(code, 2)
        self.assertEqual(calls, '')

    def test_go_only_bootstrap_never_requests_native_inputs(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = Path(temp)
            (cache / 'go/bin').mkdir(parents=True)
            (cache / 'go/bin/go').touch()
            with patch.object(bootstrap, 'CACHE', cache), patch.object(bootstrap, 'download') as download:
                bootstrap.main(['--go-only'])
                download.assert_called_once_with(bootstrap.LOCK['go'], 'go.tar.gz')
            self.assertFalse((cache / 'pgroot').exists())
