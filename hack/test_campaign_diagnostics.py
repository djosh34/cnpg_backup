"""Optional forensic callers, not real CNPG qualification; no runtime access."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from campaign_fixture import Fixture
from campaign_process import CommandFailure, CommandResult
from recovery_campaign import Manifest, main
from recovery_cases import Campaign


class DiagnosticTests(unittest.TestCase):
    def test_cli_collect_and_clean_continue_after_optional_exception(self):
        for operation in ('collect', 'clean'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / '.cache').mkdir()
                m = Manifest(root / 'evidence', {}, [])
                f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
                with patch('recovery_campaign.Path.home', return_value=root), patch.object(Fixture, 'owned', return_value=f), \
                     patch.object(f, 'run') as run, patch.object(f, 'collect', side_effect=OSError('optional artifact unavailable')), \
                     patch.object(f, 'close') as close:
                    self.assertEqual(main([operation, '--owner', str(f.WORK.parent / 'owner.json')]), 0)
                self.assertEqual(close.call_count, int(operation == 'clean'))
                self.assertEqual(run.call_args.args[1], 'unpause' if operation == 'clean' else 'pause')
                saved = json.loads((m.directory / 'manifest.json').read_text())
                self.assertFalse(saved['failures'])
                self.assertEqual(saved['diagnostics'][0]['error_type'], 'OSError')

    def test_required_manifest_persistence_is_not_optional(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = Manifest(Path(tmp), {}, [])
            with patch.object(m, 'save', side_effect=OSError('required manifest unavailable')):
                with self.assertRaisesRegex(OSError, 'required manifest'):
                    m.collect_diagnostics('logs', lambda: [])

    def test_optional_status_event_and_artifact_errors_keep_independent_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = Manifest(root / 'manifest', {}, [])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
            (f.WORK / 'kubeconfig').touch()
            def result(*args, **kwargs):
                if 'campaign-target' in args:
                    raise CommandFailure('status forbidden')
                return CommandResult('pods', 0, '{"items": []}', '', 0, False, 0)
            with patch.object(f, 'container_exists', side_effect=CommandFailure('daemon unavailable')), \
                 patch.object(f, 'kube_result', side_effect=result) as statuses, \
                 patch.object(f, 'collect_events', side_effect=CommandFailure('events forbidden')) as events, \
                 patch.object(f, 'save_log', side_effect=OSError('optional disk write')):
                m.collect_diagnostics('fixture', f.collect)
            self.assertEqual(statuses.call_count, 4)
            self.assertEqual(events.call_count, 4)
            self.assertFalse(m.data['failures'])
            self.assertEqual(len(m.data['diagnostics']), 10)
            self.assertTrue(all(e['error_type'] in ('CommandFailure', 'OSError') for e in m.data['diagnostics']))

    def test_missing_required_ownership_log_still_fails_actual_oracle(self):
        for logs in ('unrelated startup error', None):
            with self.subTest(logs=logs), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                m = Manifest(root / 'manifest', {}, ['required'])
                f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
                c = Campaign(None, m, f)
                pod = {'metadata': {'name': 'p'}, 'spec': {'containers': [{'name': 'full-recovery'}]}}
                def kube(*args, **kwargs):
                    if args[0] == 'logs':
                        if logs is None:
                            raise CommandFailure('required logs unavailable')
                        return logs
                    return ''
                with patch.object(c, 'pods', return_value=[pod]), patch.object(c, 'target_snapshot', return_value=[]), \
                     patch.object(f, 'wait'), patch.object(f, 'kube', side_effect=kube):
                    with self.assertRaises((AssertionError, CommandFailure)):
                        with m.case('required'):
                            c.replacement({'name': 'g', 'pod': 'p'})
                self.assertEqual(len(m.data['failures']), 1)
                self.assertEqual(m.data['scenarios']['required']['status'], 'failed')
                self.assertFalse(m.data['diagnostics'])

    def test_optional_logs_do_not_authorize_unsafe_retirement(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = Manifest(root / 'manifest', {}, ['retire'])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
            c = Campaign(None, m, f)
            pod = {'metadata': {'name': 'p', 'uid': 'uid'}, 'spec': {'containers': [{'name': 'full-recovery'}]}}
            with patch.object(c, 'pods', return_value=[pod]), patch.object(f, 'kube_result', side_effect=CommandFailure('optional logs')), \
                 patch.object(f, 'kube'), patch.object(f, 'wait', side_effect=CommandFailure('owned Pods still live')), \
                 patch.object(f, 'retire_claims') as retire:
                with self.assertRaisesRegex(CommandFailure, 'owned Pods still live'):
                    with m.case('retire'):
                        c.retire_target({'name': 'g', 'pvc_uids': []})
                retire.assert_not_called()
            self.assertEqual([e['diagnostic'] for e in m.data['failures']], ['owned Pods still live'])
            self.assertEqual(len(m.data['diagnostics']), 1)
