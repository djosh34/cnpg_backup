"""Campaign control-plane tests; these do not count as actual G coverage."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import recovery_campaign as c


class CampaignTests(unittest.TestCase):
    def test_ownership_slice_never_claims_all_g_or_qualification(self):
        self.assertEqual(c.scenarios('recovery'), c.MANDATORY)
        self.assertEqual(c.scenarios('qualification'), c.MANDATORY)
        self.assertEqual(c.scenarios('smoke'), c.SMOKE)
        selected = c.scenarios('ownership')
        self.assertEqual(len(selected), 13)
        self.assertIn('source-namespace-catalog-loss-S3-only', selected)
        self.assertIn('controller-all-Job-retry-Pods-terminated', selected)
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'ownership'}, selected)
            for name in selected[:-1]:
                with m.case(name):
                    pass
            self.assertFalse(m.finish())
            with m.case(selected[-1]):
                pass
            self.assertTrue(m.finish())
            self.assertFalse(m.data['all_g_families_passed'])
            self.assertFalse(m.data['release_qualified'])
            self.assertEqual(set(m.data['not_requested_scenarios']), set(c.MANDATORY) - set(selected))

    def test_ownership_slice_runs_source_loss_then_all_remaining_methods(self):
        from recovery_cases import Campaign
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            m = c.Manifest(out, {'profile': 'ownership'}, c.scenarios('ownership'))
            campaign = Campaign(SimpleNamespace(profile='ownership'), m)
            campaign.base = {'backup_uid': 'original'}
            calls = []
            with patch('recovery_cases.OUT', out), patch.object(campaign, 'inventory', return_value=[]), \
                 patch('recovery_cases.h.kube', return_value='{"items":[]}') as kube, \
                 patch.object(campaign, 'restore', side_effect=lambda *a: calls.append('source-restore')), \
                 patch.object(campaign, 'ownership', side_effect=lambda: calls.append('ownership')), \
                 patch.object(campaign, 'process_drain', side_effect=lambda: calls.append('drain')), \
                 patch.object(campaign, 'protection', side_effect=lambda: calls.append('protection')):
                campaign.run()
            self.assertEqual(kube.call_args_list[0].args[:3], ('delete', 'namespace', 'campaign-source'))
            self.assertEqual(calls, ['source-restore', 'ownership', 'drain', 'protection'])
            self.assertFalse(m.finish())  # Mocked methods are not actual coverage.

    def test_subject_is_exact_and_does_not_accept_other_registry_or_tag(self):
        good = 'ghcr.io/djosh34/cnpg-backup-manager@sha256:' + 'a' * 64
        self.assertEqual(c.subject_image(good, 'manager'), good)
        for bad in (good + ';id', good.replace('@sha256:', ':sha-'), good.replace('djosh34', 'other'), good + '\n'):
            with self.assertRaises(ValueError):
                c.subject_image(bad, 'manager')
        with self.assertRaises(ValueError):
            c.subject_image(good, 'pg18')

    def test_real_fresh_target_event_accepts_named_cluster_fact(self):
        from recovery_cases import Campaign
        with tempfile.TemporaryDirectory() as tmp:
            manifest = c.Manifest(Path(tmp), {'profile': 'smoke'}, ['one'])
            Campaign(None, manifest).event('fresh-target', name='g-001', pod='g-001-recovery', pvc_uids=['fresh'])
            event = json.loads((Path(tmp) / 'events.jsonl').read_text())
            self.assertEqual(event['event'], 'fresh-target')
            self.assertEqual(event['name'], 'g-001')

    def test_missing_or_running_mandatory_is_not_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'smoke'}, ['one', 'two'])
            with m.case('one'):
                m.event('SQL-oracle', rows=[1])
            self.assertFalse(m.finish())
            saved = json.loads((Path(tmp) / 'manifest.json').read_text())
            self.assertFalse(saved['release_qualified'])
            self.assertEqual(saved['remaining_mandatory'], ['two'])
            self.assertEqual(saved['scenarios']['one']['status'], 'passed')

    def test_first_failure_survives_and_secret_is_redacted(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'recovery'}, ['one'])
            with self.assertRaises(AssertionError):
                with m.case('one'):
                    raise AssertionError('password=do-not-collect')
            self.assertFalse(m.finish())
            text = (Path(tmp) / 'manifest.json').read_text()
            self.assertNotIn('do-not-collect', text)
            self.assertIn('AssertionError', text)
            self.assertTrue((Path(tmp) / 'first-failure.json').exists())

    def test_qualification_never_passes_g_even_when_scope_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'qualification'}, ['one'])
            with m.case('one'):
                pass
            self.assertFalse(m.finish())
            self.assertFalse(m.data['release_qualified'])

    def test_independent_sql_oracle_detects_truncation_and_post_target_leak(self):
        self.assertEqual(c.check_rows('1:base,2:before', [(1, 'base'), (2, 'before')]), '1:base,2:before')
        for actual in ('1:base', '1:base,2:before,3:after', '1:wrong,2:before'):
            with self.assertRaises(AssertionError):
                c.check_rows(actual, [(1, 'base'), (2, 'before')])

    def test_missing_docker_fails_before_downloads_or_scenarios(self):
        from recovery_cases import Campaign
        with patch('recovery_cases.shutil.which', return_value=None), patch('recovery_cases.h.run') as run:
            with self.assertRaisesRegex(RuntimeError, 'no scenarios executed'):
                Campaign(None, None).setup()
            run.assert_not_called()

    def test_target_secret_grants_are_exact_for_every_bounded_target(self):
        from recovery_cases import target_secret_names, MAX_TARGETS, TARGET
        import cnpg_smoke as h
        names = target_secret_names()
        self.assertNotIn('*', names)
        self.assertEqual(len(names), 1 + 2 * MAX_TARGETS)
        rendered = h.renderer.render('manager@sha256:' + 'a' * 64, 'pg18@sha256:' + 'b' * 64,
                                     'cnpg-system', TARGET, names)
        role = next(o for o in rendered['items'] if o['kind'] == 'Role' and o['metadata']['namespace'] == TARGET)
        rule = next(r for r in role['rules'] if r['resources'] == ['secrets'])
        self.assertEqual(rule['verbs'], ['get'])
        self.assertEqual(set(rule['resourceNames']), set(names))
        config = next(o for o in rendered['items'] if o['kind'] == 'ConfigMap')
        self.assertEqual(set(json.loads(config['data']['config.json'])['secretNames'][TARGET]), set(names))

    def test_restart_oracle_requires_own_reader_drain_but_retains_uncertain_stable(self):
        from recovery_cases import Campaign
        campaign = Campaign(None, None)
        state = {'name': 'g-001', 'plan': {'plan': {'reader_hold_id': 'reader', 'lifetime_hold_id': 'stable'}}}
        pods = [{'metadata': {'uid': 'pod', 'name': 'pod'},  'spec': {'containers': [{'name': 'full-recovery'}]},
                 'status': {'initContainerStatuses': [{'state': {'terminated': {}}}],
                            'containerStatuses': [{'state': {'terminated': {}}}]}}]
        jobs = {'items': [{'metadata': {'uid': 'job'}, 'status': {'conditions': [{'type': 'Complete', 'status': 'True'}]}}]}
        # No polling in this unit test: inspect the exact verdict immediately.
        def wait(predicate, *args):
            self.assertTrue(predicate())
        with patch.object(campaign, 'pods', return_value=pods), patch.object(campaign, 'event'), \
             patch('recovery_cases.h.kube', return_value=json.dumps(jobs)), patch('recovery_cases.h.wait', side_effect=wait), \
             patch('recovery_cases.h.pod_evidence', return_value={}), patch('recovery_cases.h.save_log'), \
             patch.object(campaign, 'operation_state', return_value={'state': 'uncertain', 'lifetimeReleased': False}), \
             patch.object(campaign, 'gate', return_value={'holders': [{'id': 'stable'}, {'id': 'unrelated'}]}) as gate:
            campaign.terminated(state, stable_retained=True)
            # An oracle that removes uncertain protection must be caught.
            gate.return_value = {'holders': [{'id': 'unrelated'}]}
            with self.assertRaises(AssertionError):
                campaign.terminated(state, stable_retained=True)
            campaign.terminated(state)  # uninterrupted release uses a different oracle
            gate.return_value = {'holders': [{'id': 'reader'}, {'id': 'stable'}]}
            with self.assertRaises(AssertionError):
                campaign.terminated(state, stable_retained=True)

    def test_marker_recheck_after_cleanup_uses_previously_resolved_mounts(self):
        from recovery_cases import Campaign
        campaign = Campaign(None, None)
        state = {'backing_paths': ['/var/local/cnpg-backup-work-' + str(i) for i in range(3)]}
        with patch.object(campaign, 'pods', side_effect=AssertionError('original Pod was naturally deleted')), \
             patch('recovery_cases.h.run', return_value='absent\nabsent\nabsent\n') as run:
            self.assertEqual(campaign.markers(state), ['absent'] * 3)
            self.assertEqual(list(run.call_args.args[-3:]), state['backing_paths'])

    def test_terminal_pods_wait_for_asynchronous_job_complete_condition(self):
        from recovery_cases import Campaign
        campaign = Campaign(None, None)
        state = {'name': 'g-001', 'plan': {'plan': {'reader_hold_id': 'reader', 'lifetime_hold_id': 'stable'}}}
        pods = [{'metadata': {'uid': 'pod', 'name': 'pod'}, 'spec': {'containers': [{'name': 'full-recovery'}]},
                 'status': {'initContainerStatuses': [{'state': {'terminated': {}}}],
                            'containerStatuses': [{'state': {'terminated': {}}}]}}]
        queries = []
        def kube(*args, **kwargs):
            if args[:2] == ('get', 'jobs'):
                queries.append(True)
                conditions = [] if len(queries) == 1 else [{'type': 'Complete', 'status': 'True'}]
                return json.dumps({'items': [{'metadata': {'uid': 'job'}, 'status': {'conditions': conditions}}]})
            return ''
        def wait(predicate, *args):
            for _ in range(3):
                if predicate():
                    return
            self.fail('asynchronous Job condition did not arrive')
        with patch.object(campaign, 'pods', return_value=pods), patch.object(campaign, 'event'), \
             patch('recovery_cases.h.kube', side_effect=kube), patch('recovery_cases.h.wait', side_effect=wait), \
             patch('recovery_cases.h.save_log'), patch('recovery_cases.h.pod_evidence', return_value={}), \
             patch.object(campaign, 'gate', return_value={'holders': []}):
            campaign.terminated(state)
        self.assertEqual(len(queries), 2)

    def test_ordinary_cleanup_needs_matching_durable_proof_and_real_gate_release(self):
        from recovery_cases import Campaign
        campaign = Campaign(None, None)
        state = {'name': 'g-001', 'job_uid': 'original-job', 'pod_uid': 'original-pod',
                 'plan': {'plan': {'reader_hold_id': 'reader', 'lifetime_hold_id': 'stable'}}}
        proof = {'state': 'completed', 'lifetimeReleased': True, 'completedJobUID': 'original-job',
                 'terminatedPodUIDs': ['original-pod']}
        def wait(predicate, *args):
            self.assertTrue(predicate())
        with tempfile.TemporaryDirectory() as tmp, patch('recovery_cases.OUT', Path(tmp)), \
             patch('recovery_cases.h.wait', side_effect=wait), patch.object(campaign, 'event'), \
             patch.object(campaign, 'pods', return_value=[]), patch.object(campaign, 'markers', return_value=['absent'] * 3), \
             patch.object(campaign, 'operation_state', return_value=proof) as operation, \
             patch.object(campaign, 'gate', return_value={'holders': [{'id': 'unrelated'}]}) as gate:
            campaign.ordinary_completion(state)  # Job/Pod naturally gone, proof survives
            for bad in ({**proof, 'state': 'uncertain'}, {**proof, 'completedJobUID': 'other'},
                        {**proof, 'terminatedPodUIDs': []}, {**proof, 'terminatedPodUIDs': ['other']},
                        {**proof, 'lifetimeReleased': False}):
                operation.return_value = bad
                with self.assertRaises(AssertionError):
                    campaign.ordinary_completion(state)
            operation.return_value = proof
            for holder in ('stable', 'reader'):
                gate.return_value = {'holders': [{'id': holder}]}
                with self.assertRaises(AssertionError):
                    campaign.ordinary_completion(state)

    def test_ordinary_finish_survives_cleanup_after_exact_durable_proof(self):
        from recovery_cases import Campaign, BASE, SOURCE_ID
        campaign = Campaign(None, None)
        state = {'name': 'g-001', 'pod': 'original-pod-name', 'pod_uid': 'original-pod',
                 'job_uid': 'original-job', 'repository_id': 'destination',
                 'plan': {'plan': {'reader_hold_id': 'reader', 'lifetime_hold_id': 'stable',
                                   'target': {'timeline': 1}}}}
        proof = {'state': 'completed', 'lifetimeReleased': True,
                 'completedJobUID': 'original-job', 'terminatedPodUIDs': ['original-pod']}
        shutdown = []
        def release(pod, barrier):
            if barrier == 'release-shutdown':
                shutdown.append(True)  # natural Job/Pod cleanup now wins every LIST
        def wait(predicate, *args):
            self.assertTrue(predicate())
        def kube(*args, **kwargs):
            self.assertNotEqual(args[:2], ('get', 'jobs'))
            self.assertNotEqual(args[0], 'annotate')  # ordinary operator never paused
            return '/var/lib/postgresql/wal/pg_wal' if args[0] == 'exec' else ''
        segment = '000000020000000000000003'
        def inventory(prefix):
            return [] if SOURCE_ID in prefix else [prefix + segment[:8] + '/' + segment]
        with tempfile.TemporaryDirectory() as tmp, patch('recovery_cases.OUT', Path(tmp)), \
             patch('recovery_cases.h.wait', side_effect=wait), patch('recovery_cases.h.kube', side_effect=kube), \
             patch('recovery_cases.h.save_log'), patch.object(campaign, 'event'), \
             patch.object(campaign, 'release', side_effect=release), patch.object(campaign, 'barrier'), \
             patch.object(campaign, 'file', return_value=json.dumps({'event': 'actual-cnpg-exit', 'exit': 0})), \
             patch.object(campaign, 'holds'), patch.object(campaign, 'pods', return_value=[]), \
             patch.object(campaign, 'markers', side_effect=lambda s: ['absent' if shutdown else 'present'] * 3), \
             patch.object(campaign, 'operation_state', return_value=proof), \
             patch.object(campaign, 'gate', return_value={'holders': [{'id': 'unrelated'}]}), \
             patch.object(campaign, 'terminated', side_effect=AssertionError('ephemeral termination LIST after proof')) as terminated, \
             patch.object(campaign, 'primary', return_value='new-primary'), \
             patch.object(campaign, 'sql', side_effect=['1:base', 'f', 'f', '/var/lib/postgresql/tablespaces/fast_space/data', '2', '', segment]), \
             patch.object(campaign, 'inventory', side_effect=inventory), patch.object(campaign, 'retire_target'):
            self.assertIs(campaign.finish(state, BASE, ordinary=True), state)
            terminated.assert_not_called()
            self.assertEqual(json.loads((Path(tmp) / 'g-001-ordinary-operation.json').read_text()), proof)

    def test_real_switch_witness_requires_post_end_bytes_in_final_filename(self):
        from recovery_cases import switch_witness
        commit = {'backup_uid': 'full', 'timeline': 1, 'bundled_wal_end_lsn': '0/2000120'}
        dump = 'rmgr: XLOG len (rec/tot): 24/24, tx: 0, lsn: 0/02000120, prev 0/020000F8, desc: SWITCH \n'
        witness = switch_witness(commit, dump)
        self.assertEqual(witness['filename'], '000000010000000000000002')
        self.assertEqual(witness['replay_end_lsn'], '0/3000000')
        self.assertEqual(witness['switch_start'], '0/02000120')
        for bad in ('', dump.replace('SWITCH', 'RESTORE_POINT named'),
                    dump.replace('0/02000120', '0/02000118'), dump.replace('0/02000120', '0/03000120'),
                    dump + dump):
            with self.assertRaises(AssertionError):
                switch_witness(commit, bad)
        with self.assertRaises(AssertionError):
            switch_witness({**commit, 'bundled_wal_end_lsn': '0/3000000'}, dump)

    def test_durable_history_oracle_rejects_wrong_bundle_despite_identical_sql(self):
        from recovery_cases import history_fork, lsn
        path = [{'id': 1}]
        healthy = '1\t0/3000000\tno recovery target specified\n'
        wrong = '1\t0/2000120\tno recovery target specified\n'
        floor = lsn('0/3000000')
        self.assertGreaterEqual(history_fork(healthy, 2, path), floor)
        self.assertLess(history_fork(wrong, 2, path), floor)
        for text, timeline in ((healthy, 1), ('2 0/3000000 wrong-parent', 3),
                               ('', 2), (healthy + healthy, 2), ('1 nonsense reason', 2)):
            with self.assertRaises((AssertionError, ValueError)):
                history_fork(text, timeline, path)

    def test_same_file_fault_stays_active_through_actual_replay_failure(self):
        from recovery_cases import Campaign
        campaign = Campaign(None, None)
        from unittest.mock import Mock
        campaign.wal = Mock()
        active = []
        name = '000000010000000000000002'
        state = {'pod': 'pod', 'name': 'g-001'}
        def control(*args):
            if args:
                active[:] = list(args)
            return {'blocked': 1}
        campaign.wal.control.side_effect = control
        def barrier(*args):
            self.assertEqual(active, ['missing-wal-get', name])
        trace = '\n'.join(json.dumps(e) for e in [{'event': 'wal-request', 'name': name},
                                                    {'event': 'actual-cnpg-exit', 'exit': 1}])
        with patch.object(campaign, 'release'), patch.object(campaign, 'barrier', side_effect=barrier), \
             patch.object(campaign, 'file', return_value=trace), patch.object(campaign, 'holds'), \
             patch.object(campaign, 'event'), patch.object(campaign, 'retire_target'), \
             patch('recovery_cases.h.save_log'), patch('recovery_cases.h.kube', return_value='FATAL restore command exit 255'):
            campaign.fatal_replay(state, name, 'missing-wal-get', 'same-segment')
        self.assertEqual(active, [''])

    def test_local_bundle_absence_survives_until_real_replay_fails(self):
        import contextlib
        from unittest.mock import Mock
        from recovery_cases import Campaign
        campaign = Campaign(None, Mock())
        campaign.m.case.side_effect = lambda name: contextlib.nullcontext()
        name = '000000010000000000000002'
        absent, withheld, replayed, retired = [], [], [], []
        @contextlib.contextmanager
        def archive_absent():
            absent.append(True)
            try:
                yield
            finally:
                absent.pop()
        states = [{'name': 'g-' + str(i), 'pod': 'p' + str(i),
                   'plan': {'plan': {'required_archive': []}, 'bundled': {name: {}}}}
                  for i in range(1, 4)]
        def barrier(pod, *args):
            if pod == 'p3':
                self.assertTrue(absent and withheld, 'required absence repaired before real PG replay')
                replayed.append(pod)
        def kube(*args, **kwargs):
            if 'mv' in args:
                if args[-1].endswith('.withheld'):
                    withheld.append(True)
                else:
                    self.assertEqual(replayed, ['p3'], 'local bundle restored before fatal replay evidence')
                    withheld.pop()
            return 'FATAL restore_command exit 255'
        trace = '\n'.join(json.dumps(e) for e in [{'event': 'wal-request', 'name': name},
                                                  {'event': 'actual-cnpg-exit', 'exit': 1}])
        def finish(state, *args):
            self.assertEqual(state['pod'], 'p1', 'repaired negative reused as a positive')
        with patch.object(campaign, 'archive_absent', side_effect=archive_absent), \
             patch.object(campaign, 'start', side_effect=states), patch.object(campaign, 'materialize'), \
             patch.object(campaign, 'helper'), patch.object(campaign, 'finish', side_effect=finish), \
             patch.object(campaign, 'release'), patch.object(campaign, 'barrier', side_effect=barrier), \
             patch.object(campaign, 'file', return_value=trace), patch.object(campaign, 'holds'), \
             patch.object(campaign, 'event'), patch.object(campaign, 'retire_target', side_effect=lambda s: retired.append(s['pod'])), \
             patch('recovery_cases.h.kube', side_effect=kube), patch('recovery_cases.h.save_log'):
            campaign.base = {'backup_uid': 'base'}
            campaign.bundle_fallback()
        self.assertEqual(replayed, ['p3'])
        self.assertEqual(retired, ['p2', 'p3'])
        self.assertFalse(absent or withheld)

    def test_native_commit_lsn_target_uses_same_numeric_canonical_spelling(self):
        from recovery_cases import canonical_lsn, lsn
        native = '0/050002C0'  # actual first full-LSN hosted failure, pg_waldump output
        self.assertEqual(canonical_lsn(native), '0/50002C0')
        self.assertEqual(lsn(canonical_lsn(native)), lsn(native))
        self.assertEqual(canonical_lsn('0000000A/00000000'), 'A/0')

    def test_budget_rejects_fault_before_it_is_generated(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'recovery'}, ['one'], deadline=0)
            with self.assertRaises(c.Deadline):
                with m.case('one'):
                    self.fail('expired campaign dispatched work')
            self.assertFalse(m.finish())


if __name__ == '__main__':
    unittest.main()
