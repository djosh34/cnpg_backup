"""Exact accounting, real runner failures, and fail-closed parity."""
import contextlib
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from campaign_plan import selected, validate_results, digest, REGISTRY
from recovery_campaign import run_plan
from campaign_process import CommandResult
from recovery_cases import Campaign as RealCampaign


class PlanTests(unittest.TestCase):
    def test_full_includes_both_supplementals_and_dependency_is_not_implicit_s1(self):
        cases = selected('recovery')
        self.assertEqual(len(cases), 37)
        self.assertEqual(len({c['id'] for c in cases}), 37)
        self.assertEqual([c['id'] for c in cases[-2:]], ['seeded-XID-1', 'seeded-XID-2'])
        self.assertFalse(any('same-segment' in c['fixtures'] for c in selected('retry')))

    def exercise_runner(self, directory, cleanup_failure=False, source_failure=False, setup_failure=False, collector_failure=False, optional_only=False, retain=False, differential=None, branches=()):
        source = 'source-namespace-catalog-loss-S3-only'
        cases = [dict(id=name, method=method, requires=requires, fixtures=['source'], group='targets', seconds=30, requirement='test independent SQL')
                 for name, method, requires in [(source, 'case_source_namespace_catalog_loss_S3_only', []),
                                                ('a', 'a', []), ('b', 'b', ['a']), ('c', 'c', [])]]
        if differential:
            cases = [copy.deepcopy(c) for c in REGISTRY if c['id'] in ('differential-native', 'seeded-XID-1')]
        calls, closed = [], []
        class Commands:
            deadline = float('inf')
            def budget(self, seconds):
                return contextlib.nullcontext()
        class Fixture:
            def __init__(self, path, name, manifest, bundle, resources, deadline):
                self.WORK, self.OUT = path / 'private', path / 'evidence'
                self.WORK.mkdir(parents=True)
                self.OUT.mkdir()
                self.commands = Commands()
                self.NAME = name
            def kube_result(self, *args, **kwargs):
                return CommandResult('logs', 1, '', 'PodInitializing', 0, False, 0)
            def run(self, *args, **kwargs):
                calls.append(args[1])
            def record_ownership(self):
                calls.append('ownership saved')
            def collect(self):
                if collector_failure:
                    raise RuntimeError('collector deadline')
                return []
            def close(self):
                closed.append(True)
                if cleanup_failure:
                    raise RuntimeError('owned mount remains')
        class Campaign(RealCampaign):
            wal = None
            def __init__(self, args, manifest, fixture):
                self.args, self.m, self.h = args, manifest, fixture
            def setup(self):
                calls.append('fresh setup')
                if setup_failure:
                    raise AssertionError('ineffective source fixture')
                if differential and 'differential' in self.args.fixtures:
                    self.make_differential_workload()
            # External SQL/capture/recovery I/O only is stubbed. The H setup,
            # product assertions, branch dispatch and run_plan are production
            # harness code; these are accounting controls, not CNPG acceptance.
            def primary(self):
                return 'source-pod'
            def sql(self, *args):
                return getattr(self, 'differential_expected', '')
            def acknowledge(self, *args):
                pass
            def full(self, name, backup_type='full'):
                if differential == 'baseline' and name == 'h-d1':
                    raise RuntimeError('differential baseline unavailable')
                return dict(kind=backup_type, backup_uid=name, root_backup_uid='h-full',
                            parent_backup_uid='h-d1' if differential == 'parent' and name == 'h-d2' else 'h-full',
                            root_manifest_sha256='f' * 64, manifest_sha256='f' * 64,
                            artifacts=[{'stored_bytes': 100 if backup_type == 'full' or differential == 'transfer' else 10}],
                            manifest_bytes=1)
            def native_backup_commands(self, pod):
                return ['BASE_BACKUP', 'BASE_BACKUP INCREMENTAL', 'BASE_BACKUP INCREMENTAL']
            def differential_restore(self, remote=False):
                calls.append('remote-PITR-source-loss' if remote else 'reconstruction')
            def differential_failed(self, fault):
                calls.append(fault)
            def case_seeded_XID_1(self):
                with self.m.case('seeded-XID-1'):
                    calls.append('seeded-XID-1')
            def prepare_recovery(self):
                pass
            def case_source_namespace_catalog_loss_S3_only(self):
                with self.m.case(source):
                    calls.append('source proved')
                    if source_failure:
                        raise AssertionError('source-loss SQL mismatch')
            def a(self):
                with self.m.case('a'):
                    calls.append('a')
                    if optional_only:
                        RealCampaign.collect_target_logs(self, [{'metadata': {'name': 'p', 'uid': 'uid'},
                            'spec': {'containers': [{'name': 'full-recovery'}]}}])
                    else:
                        raise AssertionError('SQL mismatch A')
            def b(self):
                if not optional_only:
                    raise AssertionError('dependent must never execute')
                with self.m.case('b'):
                    calls.append('b')
            def c(self):
                with self.m.case('c'):
                    calls.append('c')
                    if not optional_only:
                        raise AssertionError('SQL mismatch C')
        harness = {'content_hash': 'unchanged', 'diagnostic': True}
        plan = {'subject': {'revision': 'a' * 40, 'images': {'manager': 'manager', 'pg18': 'pg18'}}, 'harness': harness,
                'recipe': {'fixture_mode': 'diagnostic', 'registry_hash': digest(cases), 'resources': {},
                           'profile': 'recovery', 'seed': 1806, 'layout': 'monolithic'}, 'cases': [c['id'] for c in cases]}
        def select(profile, names):
            if names == ['c']:
                return [cases[0], cases[3]]
            return cases
        with patch('campaign_fixture.Fixture', Fixture), patch('campaign_fixture.preflight', return_value={'errors': []}), \
             patch('recovery_cases.Campaign', Campaign), patch('recovery_campaign.REGISTRY', cases), \
             patch('recovery_campaign.selected', side_effect=select), patch('recovery_campaign.Commands.run', return_value=''), \
             patch('recovery_campaign.Path.home', return_value=directory.parent), patch('recovery_campaign.load_bundle', return_value=harness), \
             patch('recovery_campaign.make_plan', return_value=plan):
            self.assertEqual(run_plan(plan, directory, {**harness, 'directory': directory}, duration=11, retain=retain, branches=branches),
                             0 if optional_only and not cleanup_failure else 1)
        return json.loads((directory / 'evidence/manifest.json').read_text()), calls, closed

    def test_two_independent_failures_and_dependent_blocking_in_one_real_run(self):
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d) / 'run'
            result, calls, closed = self.exercise_runner(directory)
            self.assertEqual(calls, ['fresh setup', 'source proved', 'a', 'fresh setup', 'c'])
            self.assertEqual([f['diagnostic'] for f in result['failures']], ['SQL mismatch A', 'SQL mismatch C'])
            self.assertEqual(result['scenarios']['b']['status'], 'blocked')
            self.assertEqual(result['scenarios']['b']['blocked_by'], ['a'])
            self.assertEqual(len(closed), 2)
            self.assertTrue(result['teardown_complete'])
            self.assertEqual(json.loads((directory / 'evidence/first-failure.json').read_text())['diagnostic'], 'SQL mismatch A')

    def test_source_loss_product_failure_does_not_suppress_independent_cases(self):
        self.assertFalse(any('source-namespace-catalog-loss-S3-only' in c['requires'] for c in REGISTRY))
        with tempfile.TemporaryDirectory() as d:
            result, calls, _ = self.exercise_runner(Path(d) / 'run', source_failure=True)
            self.assertEqual([f['diagnostic'] for f in result['failures']], ['source-loss SQL mismatch', 'SQL mismatch A', 'SQL mismatch C'])
            self.assertIn('c', calls)

    def test_actual_source_fixture_failure_blocks_without_cascading_product_errors(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls, _ = self.exercise_runner(Path(d) / 'run', setup_failure=True)
            self.assertEqual(calls, ['fresh setup'])
            self.assertEqual(len(result['failures']), 1)
            self.assertEqual(result['failures'][0]['classification'], 'fixture')
            self.assertTrue(all(r['status'] == 'blocked' for r in result['scenarios'].values()))

    def test_H_product_oracles_fail_only_their_branch_and_independent_work_continues(self):
        for fault in ('transfer', 'parent'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as d:
                result, calls, closed = self.exercise_runner(Path(d) / 'run', differential=fault)
                branches = result['scenarios']['differential-native']['branches']
                self.assertEqual(branches['reconstruction']['status'], 'failed')
                for branch, record in branches.items():
                    if branch != 'reconstruction':
                        self.assertEqual(record['status'], 'passed')
                        self.assertIn(branch, calls)
                self.assertEqual(result['scenarios']['seeded-XID-1']['status'], 'passed')
                self.assertIn('seeded-XID-1', calls)
                self.assertEqual(len(result['failures']), 1)
                self.assertEqual(result['failures'][0]['phase'], 'case')
                self.assertEqual(result['failures'][0]['classification'], 'product')
                self.assertEqual(len(closed), 8)
                self.assertTrue(result['teardown_complete'])

    def test_diagnostic_branch_replay_does_not_claim_unexecuted_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls, closed = self.exercise_runner(Path(d) / 'run', differential='healthy',
                                                         branches=['differential-native/checksum'])
            branches = result['scenarios']['differential-native']['branches']
            self.assertEqual(branches['checksum']['status'], 'passed')
            self.assertTrue(all(v['status'] == 'not_executed' for k, v in branches.items() if k != 'checksum'))
            self.assertEqual(result['scenarios']['seeded-XID-1']['status'], 'not_executed')
            self.assertEqual(result['diagnostic_branches'], ['differential-native/checksum'])
            self.assertFalse(result['scope_passed'])
            self.assertFalse(result['release_qualified'])
            self.assertFalse(result['failures'])
            self.assertEqual(calls, ['fresh setup', 'checksum'])
            self.assertEqual(len(closed), 1)
            self.assertTrue(result['teardown_complete'])

    def test_branch_replay_rejects_fresh_and_unknown_selection(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(ValueError, 'diagnostic-only'):
                run_plan({'recipe': {'fixture_mode': 'fresh'}}, Path(d) / 'fresh', {}, branches=['differential-native/checksum'])
            self.assertFalse((Path(d) / 'fresh').exists())
            with self.assertRaisesRegex(ValueError, 'unknown branch'):
                self.exercise_runner(Path(d) / 'unknown', differential='healthy', branches=['differential-native/typo'])
            self.assertFalse((Path(d) / 'unknown').exists())

    def test_H_baseline_failure_blocks_only_actual_differential_dependents(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls, closed = self.exercise_runner(Path(d) / 'run', differential='baseline')
            self.assertTrue(all(b['status'] == 'blocked' for b in result['scenarios']['differential-native']['branches'].values()))
            self.assertEqual(result['scenarios']['seeded-XID-1']['status'], 'passed')
            self.assertIn('seeded-XID-1', calls)
            self.assertEqual(len(result['failures']), 1)
            self.assertEqual(result['failures'][0]['fixture_requirement'], 'differential')
            self.assertEqual(len(closed), 2)
            self.assertTrue(result['teardown_complete'])

    def test_collector_deadlines_are_diagnostics_not_additional_failures(self):
        with tempfile.TemporaryDirectory() as d:
            result, _, _ = self.exercise_runner(Path(d) / 'run', collector_failure=True)
            self.assertEqual([f['diagnostic'] for f in result['failures']], ['SQL mismatch A', 'SQL mismatch C'])
            self.assertEqual([d['diagnostic'] for d in result['diagnostics']], ['collector deadline'] * 2)
            self.assertTrue(all(d['error_type'] == 'RuntimeError' and d['classification'] == 'DIAGNOSTICS' for d in result['diagnostics']))
            self.assertTrue(result['teardown_complete'])

    def test_optional_errors_keep_real_runner_case_fixture_dependencies_and_acceptance(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls, closed = self.exercise_runner(Path(d) / 'run', collector_failure=True, optional_only=True)
            self.assertEqual(calls, ['fresh setup', 'source proved', 'a', 'b', 'c'])
            self.assertEqual(len(closed), 1)
            self.assertTrue(result['scope_passed'])
            self.assertFalse(result['failures'])
            self.assertEqual(len(result['diagnostics']), 2)
            self.assertTrue(result['teardown_complete'])

    def test_optional_errors_do_not_interrupt_retention_or_replace_primary(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls, closed = self.exercise_runner(Path(d) / 'run', collector_failure=True, retain=True)
            self.assertEqual([f['diagnostic'] for f in result['failures']], ['SQL mismatch A'])
            self.assertEqual([e['diagnostic'] for e in result['diagnostics']], ['collector deadline'])
            self.assertIn('pause', calls)
            self.assertIn('ownership saved', calls)
            self.assertIn('retained_fixture', result)
            self.assertFalse(result['teardown_complete'])
            self.assertFalse(closed)

    def test_cleanup_failure_blocks_unsafe_slot_reuse_and_preserves_primary(self):
        with tempfile.TemporaryDirectory() as d:
            result, calls, closed = self.exercise_runner(Path(d) / 'run', cleanup_failure=True)
            self.assertEqual([f['diagnostic'] for f in result['failures']], ['SQL mismatch A', 'owned mount remains'])
            self.assertEqual(result['scenarios']['c']['status'], 'blocked')
            self.assertNotIn('c', calls)
            self.assertFalse(result['teardown_complete'])

    def test_parity_rejects_partial_retained_mismatched_and_missing_supplemental(self):
        images = {name: {'config_digest': name + '-immutable', 'archive': name + '.tar'} for name in ('minio', 'walproxy', 'recoveryactor')}
        files = {name + '.tar': name + '-archive-sha256' for name in images}
        limits = {'node_cpus': 4, 'node_memory_gib': 5}
        plan = {'subject': {'digest': 'same'}, 'harness': {'schema': 2, 'digest': 'same', 'images': images, 'files': files},
                'recipe': {'seed': 1806, 'registry_hash': digest(REGISTRY), 'fixture_mode': 'fresh', 'resources': limits, 'duration_minutes': 120},
                'cases': [c['id'] for c in selected('recovery')]}
        result = {'plan': plan, 'execution_id': 'a' * 32, 'host': 'local', 'fixture_mode': 'fresh', 'scope_passed': True, 'teardown_complete': True, 'duration_minutes': 120,
                  'fixture_envelopes': {'owned-node': limits},
                  'fixture_images': {name: {'config_digest': image['config_digest'], 'archive_sha256': files[image['archive']]} for name, image in images.items()},
                  'scenarios': {c['id']: {'status': 'passed', 'branches': {b: {'status': 'passed'} for b in c['branches']}}
                                for c in selected('recovery')}}
        validate_results(plan, [result])
        for duplicates in ([result, result], [result] * 4):
            with self.assertRaisesRegex(ValueError, 'duplicate execution'):
                validate_results(plan, duplicates)
        hosted = copy.deepcopy(result)
        hosted.update(execution_id='b' * 32, host='hosted')
        validate_results(plan, [result, hosted], cross_environment=True)
        hosted['host'] = 'local'
        with self.assertRaisesRegex(ValueError, 'one local and one hosted'):
            validate_results(plan, [result, hosted], cross_environment=True)
        for mutate in (lambda r: r.update(fixture_mode='retained'),
                       lambda r: r['scenarios'].pop('seeded-XID-2'),
                       lambda r: r['plan']['recipe'].update(seed=1807),
                       lambda r: r.update(teardown_complete=False),
                       lambda r: r.update(duration_minutes=135),
                       lambda r: r['fixture_images']['minio'].update(config_digest='wrong'),
                       lambda r: r['fixture_envelopes'].update(other={'node_cpus': 8, 'node_memory_gib': 5}),
                       lambda r: r.update(failures=[{'diagnostic': 'hidden failure'}])):
            bad = copy.deepcopy(result)
            mutate(bad)
            with self.assertRaises(ValueError):
                validate_results(plan, [bad])
