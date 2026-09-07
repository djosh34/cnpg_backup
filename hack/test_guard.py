import os
from pathlib import Path
import tempfile
import unittest

import guard


class GuardImageModes(unittest.TestCase):
    def test_staged_executable_is_usable_by_container_uid_under_private_umask(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            destination = Path(directory) / 'copied'
            source.write_bytes(b'executable fixture')
            source.chmod(0o755)
            old = os.umask(0o077)
            try:
                guard.stage_binary(source, destination)
            finally:
                os.umask(old)
            self.assertEqual(destination.read_bytes(), source.read_bytes())
            self.assertEqual(destination.stat().st_mode & 0o777, 0o555)


if __name__ == '__main__':
    unittest.main()
