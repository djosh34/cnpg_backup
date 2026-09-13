"""Behavioral checks for the terminal-failure smoke oracle."""
import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from backup_metrics_smoke import BackupMetricsSmoke, METRICS


class TerminalFailureTests(unittest.TestCase):
    def setUp(self):
        self.backup = {
            'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Backup',
            'metadata': {'name': 'capture', 'namespace': 'test',
                         'uid': 'fbec456a-c176-4d91-870e-a059c04c2c9e'},
            'status': {'phase': 'failed'},
        }
        self.event = {
            'apiVersion': 'v1', 'kind': 'Event',
            'metadata': {'name': 'cnpg-backup-failed-abcde', 'namespace': 'test'},
            'involvedObject': {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Backup',
                               **self.backup['metadata']},
            'reason': 'BackupFailed', 'type': 'Warning',
            'source': {'component': 'cnpg-backup'},
            'message': 'The backup could not finish. Check its status for details.',
            'firstTimestamp': '2026-09-13T12:00:00Z',
            'lastTimestamp': '2026-09-13T12:00:00Z', 'count': 1,
        }
        self.events = [self.event]
        self.before = {
            'full': dict(zip(METRICS, (2, 1, 1700000000, 3600))),
            'differential': dict(zip(METRICS, (7, 1, 1700001000, 600))),
        }
        self.current = copy.deepcopy(self.before)
        for current in self.current.values():
            current[METRICS[0]] += 1

        def kube(*args):
            if args == ('get', 'backup/capture', '-n', 'test', '-o', 'json'):
                return json.dumps(self.backup)
            self.assertEqual(args, ('get', 'events', '-n', 'test', '--field-selector',
                'involvedObject.uid=' + self.backup['metadata']['uid'] + ',reason=BackupFailed', '-o', 'json'))
            # Return unfiltered events so the oracle must verify event identity.
            return json.dumps({'apiVersion': 'v1', 'kind': 'EventList', 'items': self.events})

        def wait(predicate, description, seconds=180):
            for _ in range(3):
                if predicate():
                    return
            raise RuntimeError('barrier timed out: ' + description)

        self.report = {}
        self.h = SimpleNamespace(NS='test', kube=Mock(side_effect=kube), wait=wait)
        self.metrics = BackupMetricsSmoke(self.h, self.report, 'repository')
        self.metrics.snapshot = Mock(side_effect=lambda kind: self.current[kind])
        sleep_patch = patch('backup_metrics_smoke.time.sleep')
        sleep_patch.start()
        self.addCleanup(sleep_patch.stop)

    def test_independently_worded_warning_accepts_both_backup_types(self):
        for kind in self.before:
            with self.subTest(kind=kind):
                self.metrics.snapshot.reset_mock()
                result = self.metrics.assert_failed('capture', self.before[kind], kind=kind)
                self.assertEqual(result, self.current[kind])
                self.metrics.snapshot.assert_called_with(kind)
                for call in self.metrics.snapshot.call_args_list:
                    self.assertEqual(call.args, (kind,))
                self.assertEqual(self.report['backup_metrics_completed'][-1], {
                    'case': 'actual-terminal-failure', 'backup_type': kind,
                    'warning_asserted': True, 'durable_commit_response_loss': False})

    def test_wrong_or_missing_event_is_rejected(self):
        for kind in self.before:
            for field, value in (('involvedObject', {**self.event['involvedObject'], 'uid': 'another-backup'}),
                                 ('reason', 'BackupCompleted'), ('type', 'Normal'),
                                 ('source', {'component': 'cloudnative-pg'}), ('absent', None)):
                with self.subTest(kind=kind, field=field):
                    self.events = [] if field == 'absent' else [{**self.event, field: value}]
                    with self.assertRaisesRegex(RuntimeError, 'actual supplemental BackupFailed Warning'):
                        self.metrics.assert_failed('capture', self.before[kind], kind=kind)
                    self.assertEqual(self.report['backup_metrics_completed'], [])

    def test_matching_event_can_follow_unrelated_events(self):
        self.events = [{**self.event, 'reason': 'BackupStarted'}, self.event]
        self.metrics.assert_failed('capture', self.before['full'])

    def test_nonfailed_backup_is_rejected(self):
        for phase in ('running', 'completed'):
            with self.subTest(phase=phase):
                self.backup['status']['phase'] = phase
                with self.assertRaisesRegex(AssertionError, 'CNPG did not report terminal invocation failure'):
                    self.metrics.assert_failed('capture', self.before['full'])
                self.metrics.snapshot.assert_not_called()
                self.assertEqual(self.report['backup_metrics_completed'], [])

    def test_wrong_failure_count_or_success_metrics_are_rejected(self):
        for kind, before in self.before.items():
            valid = {**before, METRICS[0]: before[METRICS[0]] + 1}
            for field, value in ((METRICS[0], before[METRICS[0]]),
                                 (METRICS[0], before[METRICS[0]] + 2),
                                 (METRICS[1], 0), (METRICS[2], before[METRICS[2]] + 1)):
                with self.subTest(kind=kind, field=field, value=value):
                    self.current[kind] = {**valid, field: value}
                    with self.assertRaisesRegex(RuntimeError, 'one terminal failure without fabricated success'):
                        self.metrics.assert_failed('capture', before, kind=kind)
                    self.assertEqual(self.report['backup_metrics_completed'], [])
            self.current[kind] = valid

    def test_resync_cannot_recount_failure_or_refresh_success(self):
        for kind in self.before:
            for field in (METRICS[0], METRICS[2]):
                with self.subTest(kind=kind, field=field):
                    valid = self.current[kind]
                    changed = {**valid, field: valid[field] + 1}
                    self.metrics.snapshot.side_effect = [valid, valid, changed]
                    with self.assertRaisesRegex(AssertionError, 'requeue/resync recounted failure or refreshed uncommitted success'):
                        self.metrics.assert_failed('capture', self.before[kind], kind=kind)
                    self.assertEqual(self.report['backup_metrics_completed'], [])

    def test_lost_response_requires_independently_observed_commit_timestamp(self):
        for kind in self.before:
            with self.subTest(kind=kind):
                published_at = 1700002000
                with self.assertRaisesRegex(RuntimeError, 'one terminal failure without fabricated success'):
                    self.metrics.assert_failed('capture', self.before[kind], kind=kind,
                                               committed_after_loss=published_at)
                self.current[kind][METRICS[2]] = published_at
                self.metrics.assert_failed('capture', self.before[kind], kind=kind,
                                           committed_after_loss=published_at)
                self.assertTrue(self.report['backup_metrics_completed'][-1]['durable_commit_response_loss'])

    def test_throttled_warning_opt_out_still_checks_metrics(self):
        self.events = []
        self.metrics.assert_failed('capture', self.before['full'], require_warning=False)
        self.h.kube.assert_called_once_with('get', 'backup/capture', '-n', 'test', '-o', 'json')
        self.assertFalse(self.report['backup_metrics_completed'][-1]['warning_asserted'])
        self.current['full'][METRICS[0]] += 1
        with self.assertRaisesRegex(RuntimeError, 'one terminal failure without fabricated success'):
            self.metrics.assert_failed('capture', self.before['full'], require_warning=False)


if __name__ == '__main__':
    unittest.main()
