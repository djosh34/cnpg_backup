"""The rollout keeps the original PVC actor; never stream it over exec again."""
import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import backup_smoke


class ControlRolloutTests(unittest.TestCase):
    def test_persisted_actor_checked_without_reinstallation(self):
        with tempfile.TemporaryDirectory() as directory:
            control = Path(directory) / 'actor'
            control.write_bytes(b'original actor bytes')
            digest = hashlib.sha256(control.read_bytes()).hexdigest()
            harness = SimpleNamespace(NS='test', kube=Mock(return_value=digest + '  /data/actor\n'))
            backup_smoke.verify_persisted_actor(harness, 'new-primary', control, '/data/actor')
            harness.kube.assert_called_once_with('exec', '-n', 'test', 'new-primary', '-c',
                                                'postgres', '--', 'sha256sum', '/data/actor')
            for output in ('', '0' * 64 + '  /data/actor\n'):
                harness.kube.return_value = output
                with self.assertRaises(AssertionError):
                    backup_smoke.verify_persisted_actor(harness, 'new-primary', control, '/data/actor')
            harness.kube.side_effect = RuntimeError('missing actor')
            with self.assertRaises(RuntimeError):
                backup_smoke.verify_persisted_actor(harness, 'new-primary', control, '/data/actor')
