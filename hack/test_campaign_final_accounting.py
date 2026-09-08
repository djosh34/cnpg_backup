"""Offline controls for the final CI accounting findings; not G acceptance."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from campaign_fixture import Fixture
from campaign_process import CommandResult, Commands, cleanup
from recovery_campaign import Manifest, atomic_json
from campaign_ci import aggregate
from campaign_plan import REGISTRY, digest, selected


class FinalAccountingTests(unittest.TestCase):
    def test_first_failure_write_retry_keeps_earliest_primary_without_duplicates(self):
        for via_cleanup in (True, False):
            with self.subTest(via_cleanup=via_cleanup), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                m = Manifest(root, {}, ['fault'])
                primary = AssertionError('primary SQL mismatch')
                writes = []
                def fail_once(path, data):
                    if path.name == 'first-failure.json':
                        writes.append(data['diagnostic'])
                        if len(writes) == 1:
                            raise OSError('transient evidence write failure')
                    atomic_json(path, data)
                with patch('recovery_campaign.atomic_json', side_effect=fail_once):
                    if via_cleanup:
                        with self.assertRaises(BaseExceptionGroup):
                            with m.case('fault'):
                                with cleanup([], lambda error: m.failure(error, 'case', 'fault')):
                                    raise primary
                    else:
                        with self.assertRaisesRegex(OSError, 'transient evidence write failure'):
                            m.failure(primary, 'case', 'fault')
                        m.failure(primary, 'case', 'fault')
                    first = (root / 'first-failure.json').read_bytes()
                    m.failure(primary, 'case', 'fault')
                    self.assertEqual((root / 'first-failure.json').read_bytes(), first)
                self.assertEqual(json.loads(first)['diagnostic'], 'primary SQL mismatch')
                self.assertEqual(json.loads(first)['id'], 1)
                self.assertEqual(writes, ['primary SQL mismatch', 'primary SQL mismatch'])
                expected = ['primary SQL mismatch'] + (['transient evidence write failure'] if via_cleanup else [])
                for failures in (m.data['failures'], json.loads((root / 'failures.json').read_text()),
                                 json.loads((root / 'manifest.json').read_text())['failures']):
                    self.assertEqual([f['diagnostic'] for f in failures], expected)
                    self.assertEqual([f['id'] for f in failures], list(range(1, len(expected) + 1)))
                if via_cleanup:
                    self.assertEqual(m.data['scenarios']['fault']['status'], 'failed')
                    self.assertEqual(m.data['failures'][1]['phase'], 'failure-recording')
                    self.assertEqual(m.data['failures'][1]['caused_by'], 1)

    def test_real_close_attempts_independent_resources_but_blocks_unsafe_dependencies(self):
        for fault in ('stops', 'remaining', 'backings', None):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                m = Manifest(root / 'manifest', {}, [])
                f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
                f.allocations = [{'path': f'/var/local/cnpg-backup-work-{n}',
                                  'device': f'/dev/loop{n}', 'state': 'mounted'} for n in (1, 2)]
                f.record_ownership()
                sandboxes = ['sandbox-A', 'sandbox-B']
                stopped, retired, deleted = [], [], []
                def command(commands, operation, *argv, **kwargs):
                    args = [str(a) for a in argv]
                    output, code = '', 0
                    if args[:3] == ['docker', 'container', 'ls']:
                        if 'name=^/' + f.NAME + '-control-plane$' in args and not deleted:
                            output = f.NAME + '-control-plane'
                    elif args[3:] == ['systemctl', 'stop', 'kubelet']:
                        pass
                    elif args[3:] == ['crictl', 'pods', '-q']:
                        output = '\n'.join(sandboxes)
                    elif args[3:5] == ['crictl', 'stopp']:
                        stopped.append(args[-1])
                        if fault == 'stops':
                            code, output = 1, 'stop failed ' + args[-1]
                    elif args[3:5] == ['crictl', 'rmp']:
                        if fault != 'remaining':
                            sandboxes.remove(args[-1])
                    elif args[3:5] == ['losetup', '-j']:
                        self.assertFalse(sandboxes, 'backing touched before CRI quiescence')
                        output = next(a['device'] for a in f.allocations if args[5] == a['path'] + '.img')
                    elif 'observe-owned-mounts' in args:
                        output = next(a['path'] for a in f.allocations if args[-1] == a['device'])
                    elif 'retire-owned' in args:
                        self.assertFalse(sandboxes, 'unsafe unmount before CRI quiescence')
                        retired.append(args[-1])
                        if fault == 'backings':
                            code, output = 1, 'unmount failed ' + args[-1]
                    elif args[1:3] == ['delete', 'cluster']:
                        self.assertFalse(sandboxes)
                        self.assertTrue(all(a['state'] == 'retired' for a in f.allocations))
                        deleted.append(f.NAME)
                    else:
                        raise AssertionError('unexpected offline command: ' + repr(args))
                    return CommandResult(operation, code, output, '', 0, False, 0)
                with patch.object(Commands, 'command', autospec=True, side_effect=command):
                    if fault:
                        with self.assertRaises(BaseException):
                            f.close()
                    else:
                        f.close()
                self.assertEqual(stopped, ['sandbox-A', 'sandbox-B'])
                self.assertEqual(retired, ['/dev/loop1', '/dev/loop2'] if fault in ('backings', None) else [])
                self.assertEqual(deleted, [f.NAME] if fault is None else [])
                self.assertEqual(f.closed, fault is None)
                self.assertEqual(f.WORK.exists(), fault is not None)
                owner = json.loads((f.WORK.parent / 'owner.json').read_text())
                self.assertEqual(owner['closed'], fault is None)
                failures = json.loads((m.directory / 'manifest.json').read_text())['failures']
                if fault in ('stops', 'backings'):
                    expected = (['stop failed sandbox-A', 'stop failed sandbox-B'] if fault == 'stops'
                                else ['unmount failed /dev/loop1', 'unmount failed /dev/loop2'])
                    self.assertEqual(len(failures), 2)
                    for failure, diagnostic in zip(failures, expected):
                        self.assertIn(diagnostic, failure['diagnostic'])
                        self.assertEqual(failure['phase'], 'teardown')
                elif fault == 'remaining':
                    self.assertEqual(len(failures), 1)
                    self.assertIn('CRI sandboxes remain', failures[0]['diagnostic'])
                else:
                    self.assertEqual(failures, [])

    def test_hosted_aggregate_rejects_reused_execution_across_attempt_artifacts(self):
        cases = selected('recovery')
        self.assertEqual(len(cases), 35)
        self.assertEqual(sum(len(c['branches']) for c in cases), 57)
        images = {n: {'config_digest': n + '-config', 'archive': n + '.tar'}
                  for n in ('minio', 'walproxy', 'recoveryactor')}
        files = {n + '.tar': n + '-archive' for n in images}
        limits = {'node_cpus': 4, 'node_memory_gib': 5}
        base = {'harness': {'schema': 2, 'images': images, 'files': files},
                'recipe': {'registry_hash': digest(REGISTRY), 'fixture_mode': 'fresh',
                           'resources': limits, 'duration_minutes': 120},
                'cases': [c['id'] for c in cases]}
        attempts = [{'attempt': n, 'seed': seed, 'layout': layout} for n, seed, layout in
                    [(1, 1806, 'monolithic'), (2, 1806, 'monolithic'),
                     (3, 1807, 'monolithic'), (4, 1806, 'grouped')]]
        for duplicate, failed_first, bad_branch in [(None, False, False), (2, False, False),
                (3, False, False), (4, False, False), (2, True, False), (None, False, True)]:
            with self.subTest(duplicate=duplicate, failed_first=failed_first, bad_branch=bad_branch), \
                 tempfile.TemporaryDirectory() as tmp, contextlib.chdir(tmp), contextlib.redirect_stdout(io.StringIO()):
                inputs = Path('.work/inputs/artifacts/repair-inputs')
                inputs.mkdir(parents=True)
                (inputs / 'series.json').write_text(json.dumps({'attempts': attempts}))
                for attempt in attempts:
                    number = attempt['attempt']
                    plan = copy.deepcopy(base)
                    plan['recipe'].update(seed=attempt['seed'], layout=attempt['layout'])
                    (inputs / f'plan-{number}.json').write_text(json.dumps(plan))
                    result = {'plan': plan, 'execution_id': f'{1 if number == duplicate else number:032x}',
                              'host': 'hosted', 'fixture_mode': 'fresh', 'duration_minutes': 120,
                              'scope_passed': True, 'teardown_complete': True, 'failures': [],
                              'fixture_envelopes': {'owned': limits},
                              'fixture_images': {n: {'config_digest': image['config_digest'],
                                  'archive_sha256': files[image['archive']]} for n, image in images.items()},
                              'scenarios': {c['id']: {'status': 'passed', 'branches': {
                                  b: {'status': 'passed'} for b in c['branches']}} for c in cases}}
                    if failed_first and number == 1:
                        result['failures'] = [{'id': 1, 'diagnostic': 'original SQL mismatch'}]
                    if bad_branch and number == 3:
                        result['scenarios'][cases[0]['id']]['branches'].pop(cases[0]['branches'][0])
                    path = Path(f'.work/results/artifact-{number}/attempt-{number}/manifest.json')
                    path.parent.mkdir(parents=True)
                    path.write_text(json.dumps(result))
                expected_pass = duplicate is None and not bad_branch
                code = aggregate()
                report = json.loads(Path('artifacts/repair-aggregate.json').read_text())
                self.assertEqual(len(report['attempts']), 4)
                self.assertEqual(code, 0 if expected_pass else 1)
                self.assertEqual(report['passed'], expected_pass)
                self.assertFalse(report['release_qualified'])
                if duplicate:
                    record = report['attempts'][duplicate - 1]
                    self.assertFalse(record['passed'])
                    self.assertIn('duplicate execution', record['diagnostic'])
                if failed_first:
                    self.assertFalse(report['attempts'][0]['passed'])
                    self.assertEqual(report['attempts'][0]['failures'],
                                     [{'id': 1, 'diagnostic': 'original SQL mismatch'}])
                if bad_branch:
                    self.assertIn('required branch', report['attempts'][2]['diagnostic'])
