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


class PlanTests(unittest.TestCase):
    def test_full_includes_both_supplementals_and_dependency_is_not_implicit_s1(self):
        cases = selected('recovery')
        self.assertEqual(len(cases), 33)
        self.assertEqual(len({c['id'] for c in cases}), 33)
        self.assertEqual([c['id'] for c in cases[-2:]], ['seeded-XID-1', 'seeded-XID-2'])
        self.assertFalse(any('same-segment' in c['fixtures'] for c in selected('retry')))

    def exercise_runner(self, directory, cleanup_failure=False, source_failure=False, setup_failure=False, collector_failure=False):
        source = 'source-namespace-catalog-loss-S3-only'
        cases = [dict(id=name, method=method, requires=requires, fixtures=['source'], group='targets', seconds=30, requirement='test independent SQL')
                 for name, method, requires in [(source, 'case_source_namespace_catalog_loss_S3_only', []),
                                                ('a', 'a', []), ('b', 'b', ['a']), ('c', 'c', [])]]
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
            def collect(self):
                if collector_failure:
                    raise RuntimeError('collector deadline')
                return []
            def close(self):
                closed.append(True)
                if cleanup_failure:
                    raise RuntimeError('owned mount remains')
        class Campaign:
            wal = None
            def __init__(self, args, manifest, fixture):
                self.m = manifest
            def setup(self):
                calls.append('fresh setup')
                if setup_failure:
                    raise AssertionError('ineffective source fixture')
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
                    raise AssertionError('SQL mismatch A')
            def b(self):
                raise AssertionError('dependent must never execute')
            def c(self):
                with self.m.case('c'):
                    calls.append('c')
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
            self.assertEqual(run_plan(plan, directory, {**harness, 'directory': directory}, duration=11), 1)
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

    def test_collector_deadlines_are_additional_failures_not_primary_replacements(self):
        with tempfile.TemporaryDirectory() as d:
            result, _, _ = self.exercise_runner(Path(d) / 'run', collector_failure=True)
            self.assertEqual([f['diagnostic'] for f in result['failures']], ['SQL mismatch A', 'collector deadline', 'SQL mismatch C', 'collector deadline'])
            self.assertTrue(result['teardown_complete'])

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
        result = {'plan': plan, 'fixture_mode': 'fresh', 'scope_passed': True, 'teardown_complete': True, 'duration_minutes': 120,
                  'fixture_envelopes': {'owned-node': limits},
                  'fixture_images': {name: {'config_digest': image['config_digest'], 'archive_sha256': files[image['archive']]} for name, image in images.items()},
                  'scenarios': {s: {'status': 'passed'} for s in plan['cases']}}
        validate_results(plan, [result])
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
