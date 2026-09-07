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

    def test_budget_rejects_fault_before_it_is_generated(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = c.Manifest(Path(tmp), {'profile': 'recovery'}, ['one'], deadline=0)
            with self.assertRaises(c.Deadline):
                with m.case('one'):
                    self.fail('expired campaign dispatched work')
            self.assertFalse(m.finish())


if __name__ == '__main__':
    unittest.main()
