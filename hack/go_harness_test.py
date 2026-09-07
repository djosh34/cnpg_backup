"""Harness regression needing real Go; run by hack/test after Go bootstrap."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import godeps


class GoHarness(unittest.TestCase):
    def test_notice_obligations_do_not_become_go_packages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'go.mod').write_text('module example.test/fixture\n\ngo 1.27.1\n')
            (root / 'main.go').write_text('package main\nfunc main() {}\n')
            output = root / 'build/out'
            notice = output / 'go-notices/example.test/module@v1.0.0/LICENSE.go'
            notice.parent.mkdir(parents=True)
            notice.write_text('// unmodified package-local legal text\npackage legal\n')
            original = notice.read_bytes()
            def packages():
                return subprocess.run(['go', 'list', './...'], cwd=root, text=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      env={**os.environ, 'GOWORK': 'off', 'GOTOOLCHAIN': 'local'}, timeout=30)
            # Distinguishing negative control reproduces the hosted package glob
            # failure without needing race's unavailable local C compiler.
            self.assertNotEqual(packages().returncode, 0)
            godeps.isolate_output(output)
            result = packages()
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertEqual(result.stdout.strip(), 'example.test/fixture')
            self.assertEqual(notice.read_bytes(), original)
