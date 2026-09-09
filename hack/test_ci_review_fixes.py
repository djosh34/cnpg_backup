"""Accepted CI review controls exercise scenario/runner paths, never qualify G."""
import contextlib
import json
import hashlib
import io
import os
import subprocess
import sys
import tarfile
import time
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from campaign_process import CommandFailure, CommandResult, Commands
from campaign_fixture import Fixture
from recovery_campaign import Manifest, run_plan, main, check_rows
from campaign_plan import REGISTRY, selected, digest
from recovery_cases import Campaign


class DockerBoundary:
    """Offline daemon I/O double; real scenario, Commands.run and Fixture disposal."""
    def __init__(self, fixture, code=2, text='WAL rejected', hang=False, remove_failure=False):
        self.fixture, self.code, self.text = fixture, code, text
        self.hang, self.remove_failure = hang, remove_failure
        self.containers, self.runs, self.calls = {}, [], []
        self.verifications = 0

    def command(self, commands, operation, *argv, **kwargs):
        args = [str(a) for a in argv]
        self.calls.append(args)
        output, code = '', 0
        if args[:2] == ['docker', 'create']:
            self.containers[args[args.index('--name') + 1]] = self.fixture.NAME
        elif args[:2] == ['docker', 'export']:
            with tarfile.open(args[args.index('--output') + 1], 'w'):
                pass
        elif args[:3] == ['docker', 'container', 'ls']:
            name = next(a for a in args if a.startswith('name=^/'))[7:-1]
            output = name if name in self.containers else ''
        elif args[:2] == ['docker', 'inspect']:
            output = json.dumps({'cnpg-backup-fixture': self.containers[args[2]]})
        elif args[:2] == ['docker', 'rm']:
            name = args[-1]
            if self.remove_failure and '-tool-' in name:
                code, output = 1, 'daemon remove failed'
            else:
                self.containers.pop(name, None)
        elif args[:2] == ['docker', 'run']:
            self.runs.append((args, json.loads((self.fixture.WORK.parent / 'owner.json').read_text())))
            name = args[args.index('--name') + 1] if '--name' in args else 'anonymous'
            self.containers[name] = self.fixture.NAME
            if self.hang:
                raise CommandFailure('verifier client deadline; daemon container survives')
            if '/input/verify' in args:
                self.verifications += 1
                if self.verifications > 1:
                    code, output = self.code, self.text
            if 'version' in args:
                output = 'revision=' + 'a' * 40 + ' version=test'
            if '--rm' in args:
                self.containers.pop(name, None)
        return CommandResult(operation, code, output, '', 0, False, 0)


class ReviewFixTests(unittest.TestCase):
    def test_owned_claim_gc_between_list_and_delete_is_idempotent_only_for_absence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = Manifest(root / 'evidence', {}, [])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
            state = {'retired_pod_uids': ['pod-uid'], 'pvc_uids': ['owned-uid']}
            claims = [{'metadata': {'name': 'owned', 'uid': 'owned-uid'}},
                      {'metadata': {'name': 'unrelated', 'uid': 'other-uid'}}]
            def kube(*args):
                if args[:2] == ('get', 'pods'):
                    return json.dumps({'items': []})
                if args[:2] == ('get', 'pvc'):
                    return json.dumps({'items': claims})
                self.assertEqual(args[:3], ('delete', 'pvc', 'owned'))
                if '--ignore-not-found=true' not in args:
                    raise CommandFailure('Error from server (NotFound): persistentvolumeclaims "owned" not found')
                return ''  # authenticated kubectl NotFound after Cluster GC
            with patch.object(f, 'quiesce_pods') as quiesce, patch.object(f, 'kube', side_effect=kube), \
                 patch.object(f, 'reclaim') as reclaim:
                f.retire_claims(state)
                quiesce.assert_called_once_with('campaign-target', ['pod-uid'])
                reclaim.assert_called_once_with()
            def denied(*args):
                if args[0] == 'delete':
                    raise CommandFailure('Forbidden')
                return kube(*args)
            with patch.object(f, 'quiesce_pods'), patch.object(f, 'kube', side_effect=denied), \
                 patch.object(f, 'reclaim') as reclaim:
                with self.assertRaisesRegex(CommandFailure, 'Forbidden'):
                    f.retire_claims(state)
                reclaim.assert_not_called()

    def verification_fixture(self, root, **options):
        m = Manifest(root / 'manifest', {}, ['shell-free-original-verification'])
        bundle = root / 'bundle'
        bundle.mkdir()
        (bundle / 'verify').write_text('test-only driver placeholder')
        f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {'directory': bundle}, {}, float('inf'))
        c = Campaign(SimpleNamespace(data_image='subject@sha256:frozen'), m, f)
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w') as archive:
            member = tarfile.TarInfo('000000010000000000000002')
            member.size = 16384
            archive.addfile(member, io.BytesIO(b'x' * member.size))
        raw = data.getvalue()
        c.base = {'backup_uid': 'base', 'attempt_id': 'attempt', 'manifest_sha256': 'manifest',
                  'artifacts': [{'index': 1, 'role': 'wal', 'compression': 'none', 'raw_sha256': hashlib.sha256(raw).hexdigest(),
                                 'tablespace_oid': 0}]}
        def s3(method, key, path):
            path.write_bytes(b'{}' if key.endswith('manifest.pg.json') else raw)
        c.wal = SimpleNamespace(s3=s3)
        docker = DockerBoundary(f, **options)
        return c, f, m, docker

    def test_R0_actual_scenario_accepts_only_native_rejection_not_runtime_failure(self):
        for code, text in [(2, 'WAL rejected'), (125, 'docker daemon unavailable'), (126, 'cannot invoke'),
                           (127, 'not found'), (137, 'WAL rejected'), (-9, 'WAL rejected'), (2, 'manifest rejected'), (0, 'WAL rejected')]:
            with self.subTest(code=code, text=text), tempfile.TemporaryDirectory() as tmp:
                c, f, m, docker = self.verification_fixture(Path(tmp), code=code, text=text)
                with patch.object(Commands, 'command', autospec=True, side_effect=docker.command):
                    if code == 2 and text == 'WAL rejected':
                        c.case_shell_free_original_verification()
                        self.assertEqual(docker.verifications, 3)
                        self.assertTrue(m.finish())
                    else:
                        with self.assertRaises(BaseException):
                            c.case_shell_free_original_verification()
                        self.assertFalse(m.finish())
                        self.assertEqual(m.data['failures'][0]['assertion']['id'], 'shell-free-native-rejection')
                self.assertFalse(docker.containers)
                self.assertFalse(f.containers)

    def test_R0_client_failure_disposes_named_bounded_daemon_containers_and_refuses_leak(self):
        for removal_fails in (False, True):
            with self.subTest(removal_fails=removal_fails), tempfile.TemporaryDirectory() as tmp:
                c, f, m, docker = self.verification_fixture(Path(tmp), hang=True, remove_failure=removal_fails)
                with patch.object(Commands, 'command', autospec=True, side_effect=docker.command):
                    with self.assertRaises(BaseException):
                        c.case_shell_free_original_verification()
                    if removal_fails:
                        with self.assertRaises(BaseException):
                            f.close()
                        self.assertFalse(f.closed)
                    else:
                        f.close()
                        self.assertTrue(f.closed)
                        self.assertFalse(docker.containers)
                args, owner = docker.runs[0]
                self.assertIn('--name', args)
                name = args[args.index('--name') + 1]
                self.assertIn(name, owner['containers'], 'identity must be owned BEFORE daemon allocation')
                for limit in ('--cpus=1', '--memory=512m', '--memory-swap=512m', '--pids-limit=64'):
                    self.assertIn(limit, args)
                self.assertIn('cnpg-backup-fixture=' + f.NAME, args)
                self.assertEqual(m.data['failures'][0]['diagnostic'], 'verifier client deadline; daemon container survives')
                self.assertEqual(any(e['phase'] == 'container-cleanup' for e in m.data['failures']), removal_fails)
                self.assertTrue(any(a[:3] == ['docker', 'container', 'ls'] and name in ' '.join(a) for a in docker.calls))

    def test_CLI_aggregation_rejects_duplicate_executions_and_self_or_same_host_comparison(self):
        cases = selected('recovery')
        images = {n: {'config_digest': n + '-config', 'archive': n + '.tar'} for n in ('minio', 'walproxy', 'recoveryactor')}
        files = {n + '.tar': n + '-archive' for n in images}
        limits = {'node_cpus': 4, 'node_memory_gib': 5}
        plan = {'harness': {'schema': 2, 'images': images, 'files': files},
                'recipe': {'registry_hash': digest(REGISTRY), 'fixture_mode': 'fresh', 'resources': limits, 'duration_minutes': 120},
                'cases': [c['id'] for c in cases]}
        result = {'plan': plan, 'execution_id': 'a' * 32, 'host': 'local', 'fixture_mode': 'fresh',
                  'duration_minutes': 120, 'scope_passed': True, 'teardown_complete': True,
                  'fixture_envelopes': {'owned': limits},
                  'fixture_images': {n: {'config_digest': i['config_digest'], 'archive_sha256': files[i['archive']]} for n, i in images.items()},
                  'scenarios': {c['id']: {'status': 'passed', 'branches': {b: {'status': 'passed'} for b in c.get('branches', ['main'])}} for c in cases}}
        with tempfile.TemporaryDirectory() as tmp, patch('sys.stdout', new_callable=io.StringIO):
            root = Path(tmp)
            p, left, right = root / 'plan.json', root / 'left.json', root / 'right.json'
            p.write_text(json.dumps(plan))
            left.write_text(json.dumps(result))
            right.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, 'duplicate execution'):
                main(['aggregate', '--plan', str(p), *([str(left)] * 4)])
            with self.assertRaisesRegex(ValueError, 'duplicate execution'):
                main(['compare', '--left', str(left), '--right', str(right)])
            result['execution_id'] = 'b' * 32
            right.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, 'one local and one hosted'):
                main(['compare', '--left', str(left), '--right', str(right)])
            result['host'] = 'hosted'
            right.write_text(json.dumps(result))
            self.assertEqual(main(['compare', '--left', str(left), '--right', str(right)]), 0)

    def test_fatal_replay_preserves_primary_and_reset_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = Manifest(Path(tmp), {}, ['fault'])
            c = Campaign(None, m)
            durable_before_reset = []
            def control(*args):
                if args == ('',):
                    first = Path(tmp) / 'first-failure.json'
                    durable_before_reset.append(first.exists() and json.loads(first.read_text())['diagnostic'] == 'original SQL failure')
                    raise CommandFailure('fault reset failed')
                return {'blocked': 1}
            c.wal = SimpleNamespace(control=control)
            with patch.object(c, 'replay_failure', side_effect=AssertionError('original SQL failure')):
                with self.assertRaises(BaseException):
                    with m.case('fault'):
                        c.fatal_replay({}, 'wal', 'corrupt-wal-get', 'fault')
            self.assertEqual([f['diagnostic'] for f in m.data['failures']],
                             ['original SQL failure', 'fault reset failed'])
            self.assertEqual(json.loads((Path(tmp) / 'first-failure.json').read_text())['diagnostic'], 'original SQL failure')
            self.assertEqual(m.data['failures'][1]['phase'], 'fault-reset')
            self.assertEqual(durable_before_reset, [True])

    def test_archive_restoration_attempts_every_object_and_verification_after_primary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = Manifest(root / 'manifest', {}, ['archive'])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
            c = Campaign(None, m, f)
            c.wal = SimpleNamespace(directory=root, endpoint='https://fixture', s3=Mock())
            restored = []
            def run(*args, **kwargs):
                if '--dump-header' in args:
                    args[args.index('--dump-header') + 1].write_text('x-amz-meta-sha256: abc\n')
                    args[args.index('--output') + 1].write_bytes(b'original')
                if 'PUT' in args:
                    restored.append(args[-1])
                    raise CommandFailure('restore failed ' + args[-1].rsplit('/', 1)[1])
            with patch.object(f, 'run', side_effect=run), patch.object(c, 'inventory', side_effect=[['wal1', 'wal2'], [], CommandFailure('inventory verification failed')]):
                with self.assertRaises(BaseException):
                    with m.case('archive'):
                        with c.archive_absent():
                            raise AssertionError('original archive SQL failure')
            self.assertEqual(len(restored), 2)
            self.assertEqual([e['diagnostic'] for e in m.data['failures']],
                             ['original archive SQL failure', 'restore failed wal1', 'restore failed wal2', 'inventory verification failed'])
            self.assertTrue(all(e['caused_by'] == 1 for e in m.data['failures'][1:]))

    def test_version_probe_callers_use_owned_container_lifecycle_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = Manifest(root / 'manifest', {}, [])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {'node_cpus': 4, 'node_memory_gib': 5}, float('inf'))
            c = Campaign(SimpleNamespace(manager_image='manager', data_image='data', subject_sha='a' * 40), m, f)
            docker = DockerBoundary(f)
            def download(name):
                (f.WORK / name).write_text('manifest')
            def run(*args, **kwargs):
                if args[:2] == ('docker', 'inspect') and args[2].endswith('-control-plane'):
                    return '{"NanoCpus":4000000000,"Memory":5368709120}'
                return f.commands.run(*args, **kwargs)
            def kube(*args, **kwargs):
                if args[0] == 'version':
                    return '{"serverVersion":{"gitVersion":"v1.35.8"}}'
                raise CommandFailure('stop offline setup after version probes')
            with patch('recovery_cases.shutil.which', return_value='/unused/docker'), patch.object(f, 'download', side_effect=download), \
                 patch.object(f, 'create_node'), patch.object(f, 'run', side_effect=run), patch.object(f, 'kube', side_effect=kube), \
                 patch.object(Commands, 'command', autospec=True, side_effect=docker.command):
                with self.assertRaisesRegex(CommandFailure, 'stop offline setup'):
                    c.setup()
            self.assertEqual(len(docker.runs), 2)
            self.assertFalse(docker.containers)
            for args, owner in docker.runs:
                self.assertIn('--name', args)
                self.assertIn(args[args.index('--name') + 1], owner['containers'])
                self.assertIn('--memory=512m', args)

    def test_ineffective_fatal_fault_is_not_a_product_assertion(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = Manifest(Path(tmp), {}, ['fault'])
            h = SimpleNamespace(kube=Mock(return_value='unrelated early PostgreSQL error'), save_log=Mock())
            c = Campaign(None, m, h)
            c.wal = SimpleNamespace(control=Mock(return_value={'blocked': 0}))
            with patch.object(c, 'release'), patch.object(c, 'barrier'), patch.object(c, 'file', return_value='{"event":"actual-cnpg-exit","exit":1}'), patch.object(c, 'holds'):
                with self.assertRaises(BaseException):
                    with m.case('fault'):
                        c.fatal_replay({'pod': 'p', 'name': 'g'}, 'wal', 'corrupt-wal-get', 'fault')
            self.assertIn(unittest.mock.call(), c.wal.control.call_args_list)
            self.assertEqual(m.data['failures'][0]['classification'], 'fixture')
            self.assertIn('blocked', (Path(tmp) / 'events.jsonl').read_text())

    def test_effective_fault_keeps_original_safety_oracle_and_timeout_receipt(self):
        for timeout in (False, True):
            with self.subTest(timeout=timeout), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                m = Manifest(root, {}, ['fault'])
                h = SimpleNamespace(kube=Mock(return_value='wrong replay failure without 255'), save_log=Mock())
                c = Campaign(None, m, h)
                c.wal = SimpleNamespace(control=Mock(return_value={'blocked': 1}))
                trace = '{"event":"wal-request","name":"wal"}\n{"event":"actual-cnpg-exit","exit":1}'
                with patch.object(c, 'release'), patch.object(c, 'barrier', side_effect=CommandFailure('terminal deadline') if timeout else None), \
                     patch.object(c, 'file', return_value=trace), patch.object(c, 'holds'):
                    with self.assertRaises(BaseException):
                        with m.case('fault'):
                            c.fatal_replay({'pod': 'p', 'name': 'g'}, 'wal', 'auth-wal-get', 'fault')
                events = [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines()]
                receipt = next(e for e in events if e['event'] == 'fatal-WAL-fault-receipt')
                self.assertEqual(receipt['proxy']['blocked'], 1)
                if not timeout:
                    self.assertLessEqual(receipt['epoch'], m.data['failures'][0]['epoch'])
                self.assertEqual(m.data['failures'][0]['classification'], 'infrastructure' if timeout else 'product')
                if not timeout:
                    self.assertEqual(m.data['failures'][0]['assertion']['id'], 'bef39bc6301a9e81')
                self.assertFalse(m.data['failures'][0]['product_defect_proven'])

    def test_collectors_do_not_accept_error_text_or_suppress_other_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            m = Manifest(root / 'manifest', {}, [])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
            (f.WORK / 'kubeconfig').touch()
            pod = {'metadata': {'name': 'p', 'uid': 'uid', 'creationTimestamp': 'now'},
                   'spec': {'containers': [{'name': 'postgres'}]},
                   'status': {'containerStatuses': [{'name': 'postgres', 'state': {'running': {}}}]}}
            def result(*args, **kwargs):
                if args[0] == 'logs':
                    return CommandResult('logs', 1, '', 'API forbidden', 0, False, 0)
                return CommandResult('pods', 0, json.dumps(pod if args[:2] == ('get', 'pod') else {'items': [pod]}), '', 0, False, 0)
            with patch.object(f, 'container_exists', return_value=True), patch.object(f, 'run', return_value='sandbox'), patch.object(f, 'kube', return_value=json.dumps({'items': [pod]})), patch.object(f, 'kube_result', side_effect=result) as requests, patch.object(f, 'collect_events', side_effect=CommandFailure('events forbidden')):
                errors = f.collect()
            self.assertEqual(sum(call.args[0] == 'logs' for call in requests.call_args_list), 4)
            self.assertEqual(len(errors), 8)
            self.assertEqual(sum('API forbidden' in e['diagnostic'] for e in errors), 4)

    def test_unavailable_optional_logs_are_warnings_not_retirement_gates(self):
        for state in ('running', 'waiting', 'deleted', 'forbidden'):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                m = Manifest(root / 'manifest', {}, ['retire'])
                f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, float('inf'))
                c = Campaign(None, m, f)
                pod = {'metadata': {'name': 'p', 'uid': 'uid'}, 'spec': {'containers': [{'name': 'postgres'}, {'name': 'cnpg-backup'}]},
                       'status': {'phase': 'Pending' if state == 'waiting' else 'Running'}}
                if state == 'running':
                    pod['status']['containerStatuses'] = [{'name': 'postgres', 'containerID': 'actual', 'state': {'running': {}}}]
                calls = []
                def kube_result(*args, **kwargs):
                    calls.append(args)
                    if args[0] == 'logs' or state == 'forbidden':
                        return CommandResult('logs', 1, '', 'API unavailable', 0, False, 0)
                    return CommandResult('status', 0, '' if state == 'deleted' else json.dumps(pod), '', 0, False, 0)
                with patch.object(c, 'pods', side_effect=[[pod], []]), patch.object(f, 'kube_result', side_effect=kube_result), \
                     patch.object(f, 'kube') as mutate, patch.object(f, 'wait', side_effect=lambda check, *a: self.assertTrue(check())), \
                     patch.object(c, 'operation_state', return_value={'state': 'uncertain'}), \
                     patch.object(c, 'gate', return_value={'holders': ['original-holder']}), patch.object(f, 'retire_claims') as retire:
                    with m.case('retire'):
                        c.retire_target({'name': 'g', 'pvc_uids': []})
                    retire.assert_called_once()
                    self.assertTrue(mutate.called)
                    self.assertFalse(m.data['failures'])
                    self.assertEqual(m.data['scenarios']['retire']['status'], 'passed')
                    self.assertEqual(len(m.data['diagnostics']), 2)
                    self.assertTrue(all(e['classification'] == 'DIAGNOSTICS' and e['error_type'] == 'CommandFailure'
                                        for e in m.data['diagnostics']))
                self.assertEqual(sum(a[0] == 'logs' for a in calls), 2)

    def test_actual_runner_finishes_independent_branches_on_certified_fresh_fixtures(self):
        families = ['full-latest-remote-SQL', 'full-time-inclusive-exclusive', 'full-LSN-inclusive-exclusive',
                    'full-XID-inclusive-exclusive', 'same-bundled-segment-post-EndLSN-archive-preferred',
                    'negative-controls-EOF-and-bundle-as-success']
        cases = selected('recovery', families)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            actions, modes = [], []
            class Owned(Fixture):
                def collect(self):
                    return []
                def close(self):
                    actions.append(('close', self.NAME))
            def setup(c):
                actions.append(('certify', c.h.NAME))
                c.base, c.newest = {'backup_uid': 'base'}, {'backup_uid': 'newest'}
                c.target = {'time_rfc3339': 'time', 'commit_lsn': '0/1', 'xid': '1'}
                c.same_commit = {'backup_uid': 'same'}
                c.same_witness = {'archive_sha256': 'hash'}
                def control(*args):
                    if len(args) == 2:
                        modes.append((args[0], c.h.NAME))
                    return {'blocked': 1}
                c.wal = SimpleNamespace(control=control, close=lambda: None)
                c.h.kube = lambda *a, **kw: 'unrelated PostgreSQL error'
            def restore(c, *args, **kwargs):
                actions.append(('restore', c.h.NAME, getattr(c.m, 'current_branch', None), args[0]))
                check_rows('', [(1, 'missing-required-row')])
            states = {}
            def start(c, target):
                state = {'name': 'g', 'pod': 'p'}
                states[c.h.NAME] = state
                return state
            def release(c, pod, barrier):
                if barrier == 'incorrect-bundle-success':
                    states[c.h.NAME]['wrong'] = True
            def finish(c, state, *args):
                state.update(recovery_events=[{'event': 'actual-upstream-WAL', 'name': 'wal', 'sha256': 'hash'},
                    {'event': 'deliberately-incorrect-bundle-as-archive-success', 'name': 'wal'}],
                    replay_endpoint=1, same_segment_floor=1)
            harness = {'content_hash': 'same', 'diagnostic': True}
            plan = {'subject': {'revision': 'a' * 40, 'images': {'manager': 'm', 'pg18': 'p'}}, 'harness': harness,
                    'recipe': {'fixture_mode': 'diagnostic', 'registry_hash': digest(REGISTRY), 'resources': {},
                               'profile': 'recovery', 'seed': 1806, 'layout': 'monolithic'}, 'cases': [c['id'] for c in cases]}
            with patch('campaign_fixture.Fixture', Owned), patch('campaign_fixture.preflight', return_value={'errors': []}), \
                 patch('recovery_campaign.Commands.run', return_value=''), patch('recovery_campaign.Path.home', return_value=root), \
                 patch('recovery_campaign.load_bundle', return_value=harness), patch('recovery_campaign.make_plan', return_value=plan), \
                 patch.object(Campaign, 'setup', setup), patch.object(Campaign, 'prepare_recovery'), \
                 patch.object(Campaign, 'restore', restore), patch.object(Campaign, 'start', start), \
                 patch.object(Campaign, 'padded_same_bundle', lambda c: contextlib.nullcontext((c.same_commit, 'wal'))), \
                 patch.object(Campaign, 'same_segment_plan'), patch.object(Campaign, 'finish', finish), \
                 patch.object(Campaign, 'same_endpoint_reached', lambda c, s: not s.get('wrong')), \
                 patch.object(Campaign, 'release', release), patch.object(Campaign, 'barrier'), \
                 patch.object(Campaign, 'file', return_value='{"event":"wal-request","name":"wal"}\n{"event":"actual-cnpg-exit","exit":1}'), \
                 patch.object(Campaign, 'retire_target'), patch.object(Campaign, 'holds'):
                self.assertEqual(run_plan(plan, root / 'run', {**harness, 'directory': root}, duration=11), 1)
            result = json.loads((root / 'run/evidence/manifest.json').read_text())
            self.assertEqual(set(result['scenarios']), set(plan['cases']))
            self.assertEqual([m[0] for m in modes], ['missing-wal-get', 'corrupt-wal-get', 'auth-wal-get', 'reset-wal-get', 'tls-wal-get'])
            for case in cases[:-1]:
                branches = result['scenarios'][case['id']]['branches']
                self.assertEqual(set(branches), set(case['branches']))
                self.assertTrue(all(b['status'] in ('passed', 'failed') for b in branches.values()))
            self.assertEqual(result['scenarios'][families[-1]]['branches']['main']['status'], 'blocked')
            self.assertEqual(len({m[1] for m in modes}), 5)
            # No failed branch was retried, and every fixture transition closes
            # the old fixture before independently certifying its replacement.
            restored = [a for a in actions if a[0] == 'restore']
            self.assertEqual(len(restored), 8)
            self.assertEqual(len({a[1] for a in restored}), 8)
            lifecycle = [a[0] for a in actions if a[0] in ('close', 'certify')]
            self.assertEqual(lifecycle, ['certify', 'close'] * (len(lifecycle) // 2))
            events = [json.loads(line) for line in (root / 'run/evidence/events.jsonl').read_text().splitlines()]
            self.assertEqual(sum(e['event'] == 'fixture-certified' for e in events), len(lifecycle) // 2)

    def test_pending_actual_caller_reaps_ignoring_TERM_after_expired_remote_reset(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as safety:
            root = Path(tmp)
            m = Manifest(root / 'manifest', {}, ['pending-sidecar-task-same-incarnation-drain'])
            f = Fixture(root / 'fixture', 'cb-repair-123456789abc', m, {}, {}, time.monotonic() + 20)
            # Real child, no kubectl/Docker. It ignores TERM and floods output;
            # actual Commands capture/cancel/KILL/reap must own this caller.
            child = f.WORK / 'kubectl'
            pidfile = root / 'pid'
            child.write_text('#!' + sys.executable + '\nimport os,signal,time\nfrom pathlib import Path\n'
                             'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
                             + 'Path(' + repr(str(pidfile)) + ').write_text(str(os.getpid()))\n'
                             + 'print("secret=disposable-test-only-secret", flush=True)\n'
                             + 'while True:\n os.write(1, b"x" * 8192)\n time.sleep(.001)\n')
            child.chmod(0o700)
            # The red (old-code) control deliberately leaks this exec. Test
            # ownership still kills/reaps it even when the regression fails.
            original_popen = subprocess.Popen
            def spawn(*args, **kwargs):
                process = original_popen(*args, **kwargs)
                def reap_test_child():
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=2)
                safety.callback(reap_test_child)
                return process
            safety.enter_context(patch('campaign_process.subprocess.Popen', side_effect=spawn))
            c = Campaign(None, m, f)
            c.base, c.remote = {'backup_uid': 'base'}, 'wal'
            c.wal = SimpleNamespace(control=Mock())
            def control(*args):
                if args == ('',):
                    return f.commands.run(sys.executable, '-c', 'pass', operation='fault-reset')
                return {'blocked': 1}
            c.wal.control.side_effect = control
            def wait(*args):
                end = time.monotonic() + 3
                while not pidfile.exists() and time.monotonic() < end:
                    time.sleep(.01)
                self.assertTrue(pidfile.exists())
                time.sleep(.2)
                f.commands.deadline = 0
                raise CommandFailure('source WAL task response held: deadline')
            with patch.object(c, 'start', return_value={'name': 'g', 'pod': 'p'}), patch.object(c, 'materialize'), \
                 patch.object(c, 'no_retries'), patch.object(c, 'markers', return_value=['present'] * 3), patch.object(f, 'wait', side_effect=wait):
                start_time = time.monotonic()
                with self.assertRaises(BaseException):
                    c.case_pending_sidecar_task_same_incarnation_drain()
                self.assertLess(time.monotonic() - start_time, 5)
            pid = int(pidfile.read_text())
            self.assertFalse(Path('/proc', str(pid)).exists(), 'pending exec was not killed/reaped')
            errors = m.data['failures']
            self.assertEqual(errors[0]['diagnostic'], 'source WAL task response held: deadline')
            self.assertTrue(any(e['phase'] == 'fault-reset' for e in errors))
            log = f.OUT / 'g-pending-helper.log'
            self.assertTrue(log.exists())
            self.assertLessEqual(log.stat().st_size, 2 << 20)
            self.assertNotIn('disposable-test-only-secret', log.read_text())
