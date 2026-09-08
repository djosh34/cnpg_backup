"""Real child-process regressions, not modeled campaign coverage."""
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from campaign_process import Commands, CommandFailure


class ProcessTests(unittest.TestCase):
    def test_empty_nonzero_is_not_a_successful_barrier(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d))
            result = commands.command('probe', sys.executable, '-c', 'raise SystemExit(7)')
            self.assertEqual(result.returncode, 7)
            self.assertFalse(result.ok)
            self.assertEqual(result.stdout, '')
            with self.assertRaisesRegex(CommandFailure, 'probe.*exit=7'):
                result.require()

    def test_wait_caps_child_and_reports_layer_and_last_status(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d))
            start = time.monotonic()
            with self.assertRaisesRegex(CommandFailure, 'adoption.*deadline'):
                commands.wait(lambda: commands.command('process-observation', sys.executable, '-c',
                    'import time; time.sleep(30)', timeout=30).ok, 'adoption', .15)
            self.assertLess(time.monotonic() - start, 2)
            self.assertIn('process-observation', (Path(d) / 'commands.jsonl').read_text())

    def test_output_is_bounded_and_separated_and_secrets_do_not_hide_admission_reason(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d), output_limit=4096)
            result = commands.command('admission', sys.executable, '-c',
                'import sys; print("x"*100000); print("Invalid: spec.volumes duplicate secret reference; password=hidden", file=sys.stderr)')
            self.assertTrue(result.ok)
            self.assertLessEqual(len(result.stdout.encode()), 4096)
            self.assertGreater(result.dropped_bytes, 90000)
            with self.assertRaisesRegex(CommandFailure, 'incomplete oracle input'):
                result.require()
            self.assertIn('duplicate secret reference', result.stderr)
            self.assertNotIn('hidden', result.stderr)
            self.assertNotIn('hidden', (Path(d) / 'commands.jsonl').read_text())

    def test_expected_failure_cannot_accept_timeout(self):
        with tempfile.TemporaryDirectory() as d:
            commands = Commands(Path(d))
            with self.assertRaisesRegex(CommandFailure, 'deadline'):
                commands.run(sys.executable, '-c', 'import time; time.sleep(30)',
                             expect_failure=True, timeout=.1)

    def test_container_absence_requires_successful_list_not_error_spelling(self):
        from campaign_fixture import Fixture
        fixture = Fixture.__new__(Fixture)
        fixture.NAME = 'cb-repair-aaaaaaaaaaaa'
        name = fixture.NAME + '-control-plane'
        fixture.run = Mock(return_value='')
        self.assertFalse(fixture.container_exists(name))
        self.assertEqual(fixture.run.call_args.args[:3], ('docker', 'container', 'ls'))
        fixture.run.return_value = name + '\n'
        self.assertTrue(fixture.container_exists(name))
        fixture.run.side_effect = CommandFailure('daemon unavailable, empty stdout')
        with self.assertRaises(CommandFailure):
            fixture.container_exists(name)

    def test_missing_kubeconfig_blocks_dependent_collectors_without_fake_failures(self):
        from campaign_fixture import Fixture
        with tempfile.TemporaryDirectory() as d:
            fixture = Fixture.__new__(Fixture)
            fixture.WORK = Path(d)
            fixture.m = Mock(data={'failures': [{'phase': 'setup', 'id': 1}]})
            fixture.save_log = Mock()
            self.assertEqual(fixture.collect(), [])
            self.assertIn('blocked', fixture.save_log.call_args.args[1])
            fixture.m.event.assert_called_once_with('collection-blocked', requirement='fixture kubeconfig', caused_by=[1])

    def test_absent_node_cleanup_is_idempotent_without_accepting_daemon_failure(self):
        from campaign_fixture import Fixture
        with tempfile.TemporaryDirectory() as d:
            fixture = Fixture.__new__(Fixture)
            fixture.NAME = 'cb-repair-aaaaaaaaaaaa'
            fixture.WORK = Path(d) / 'private'
            fixture.WORK.mkdir()
            fixture.run = Mock(return_value='')
            fixture.listener = None
            fixture.allocations = []
            fixture.record_ownership = Mock()
            fixture.close()
            self.assertTrue(fixture.closed)
            self.assertFalse(fixture.WORK.exists())
            fixture.run.side_effect = CommandFailure('Docker authorization denied')
            with self.assertRaises(CommandFailure):
                fixture.close()
