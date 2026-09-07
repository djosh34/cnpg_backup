"""Campaign control-plane tests; these do not count as actual G coverage."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import recovery_campaign as c


class CampaignTests(unittest.TestCase):
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
                        {**proof, 'terminatedPodUIDs': []}, {**proof, 'lifetimeReleased': False}):
                operation.return_value = bad
                with self.assertRaises(AssertionError):
                    campaign.ordinary_completion(state)
            operation.return_value = proof
            gate.return_value = {'holders': [{'id': 'stable'}]}
            with self.assertRaises(AssertionError):
                campaign.ordinary_completion(state)

    def test_budget_rejects_fault_before_it_is_generated(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'recovery'}, ['one'], deadline=0)
            with self.assertRaises(c.Deadline):
                with m.case('one'):
                    self.fail('expired campaign dispatched work')
            self.assertFalse(m.finish())


if __name__ == '__main__':
    unittest.main()
