"""Regressions at the actual Docker export inventory boundary (no daemon needed)."""
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import images


class ImageInventoryTests(unittest.TestCase):
    def check_export(self, extra):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w') as tar:
            for entry in extra:
                tar.addfile(entry)
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            for flavor in ('manager', 'pg18'):
                (out / (flavor + '-files.json')).write_text('[]')

            def docker(*args, **kwargs):
                code, data = 0, b''
                if args[:2] == ('image', 'inspect'):
                    data = json.dumps([{'Id': 'sha256:test', 'Config': {'User': '26:26'}}]).encode()
                elif args[0] == 'create':
                    data = b'container'
                elif args[0] == 'export':
                    data = archive.getvalue()
                elif args[0] == 'run':
                    if args[-1] in ('manager', 'instance', 'recovery-job', 'recovery-guard'):
                        code = 2
                    elif args[-1] == 'wal-fetch':
                        code = 255
                    elif args[-1] == '--version':
                        data = b'PostgreSQL 18.6'
                return subprocess.CompletedProcess(args, code, data, b'')

            with patch.object(images, 'OUT', out), patch.object(images, 'docker', docker), patch.object(images.subprocess, 'run'):
                images.main()

    def test_docker_generated_mtab_and_console(self):
        mtab = tarfile.TarInfo('etc/mtab')
        mtab.type, mtab.linkname = tarfile.SYMTYPE, '/proc/mounts'
        self.check_export([mtab, tarfile.TarInfo('dev/console')])

    def test_other_links_still_rejected(self):
        for name, target in [('etc/mtab', '/unexpected'), ('bin/sh', '/proc/mounts')]:
            with self.subTest(name=name, target=target):
                link = tarfile.TarInfo(name)
                link.type, link.linkname = tarfile.SYMTYPE, target
                with self.assertRaisesRegex(RuntimeError, 'unexpected image link'):
                    self.check_export([link])

    def test_unexpected_executable_still_rejected(self):
        shell = tarfile.TarInfo('bin/sh')
        shell.mode = 0o755
        with self.assertRaisesRegex(RuntimeError, 'differs from audited root'):
            self.check_export([shell])


if __name__ == '__main__':
    unittest.main()
