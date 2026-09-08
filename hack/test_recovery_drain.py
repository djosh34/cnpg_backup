"""Process-drain barrier regression; unit-only, not actual PostgreSQL coverage."""
import contextlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from recovery_cases import Campaign


class ProcessDrainTests(unittest.TestCase):
    def exercise(self, adopted):
        # Actual topology: CNPG -> pg_ctl -> postmaster. CNPG exit does not
        # imply pg_ctl exit. The detached postmaster is adopted only later.
        cnpg = dict(kind='cnpg', pid=54, parent=23, group=1, session=1)
        pg = dict(kind='postgres', pid=147, parent=142, group=147, session=147)
        orphan = dict(pg, parent=1)
        samples = [[pg], [orphan] if adopted else [pg]]
        seen = []
        campaign = Campaign(None, SimpleNamespace(case=lambda _: contextlib.nullcontext()))
        campaign.base = {'backup_uid': 'backup'}
        campaign.event = Mock()
        state = {'pod': 'actual-recovery'}

        def kube(*args):
            if args[-1] == 'stop-cnpg':
                return json.dumps([cnpg, pg])
            self.assertEqual(args[-1], 'processes')
            sample = samples[min(len(seen), 1)]
            seen.append(sample)
            return json.dumps(sample)

        def wait(predicate, description, seconds):
            # Two scheduling turns, no wall-clock sleeps. Permanent missing
            # adoption must exhaust a bounded wait, never become success.
            for _ in range(2):
                if predicate():
                    return
            raise RuntimeError('barrier timed out: ' + description)

        class DetachedCaseFinished(Exception):
            pass

        with patch.object(campaign, 'start', return_value=state), \
             patch.object(campaign, 'materialize'), \
             patch.object(campaign, 'no_retries'), \
             patch.object(campaign, 'release') as release, \
             patch.object(campaign, 'barrier'), \
             patch.object(campaign, 'markers', side_effect=[['present'] * 3, ['absent'] * 3]), \
             patch.object(campaign, 'replacement') as replacement, \
             patch.object(campaign, 'main_terminated', return_value=True), \
             patch.object(campaign, 'retire_target', side_effect=DetachedCaseFinished), \
             patch('recovery_cases.h.kube', side_effect=kube), \
             patch('recovery_cases.h.wait', side_effect=wait):
            if adopted:
                with self.assertRaises(DetachedCaseFinished):
                    campaign.case_detached_PG_descendants()
                self.assertEqual(seen, [[pg], [orphan]])
                replacement.assert_called_once_with(state)
                self.assertIn(('actual-recovery', 'release-shutdown'), [x.args for x in release.call_args_list])
            else:
                with self.assertRaisesRegex(RuntimeError, 'barrier timed out: .*PostgreSQL orphan adoption'):
                    campaign.case_detached_PG_descendants()
                replacement.assert_not_called()
                self.assertNotIn(('actual-recovery', 'release-shutdown'), [x.args for x in release.call_args_list])
                self.assertEqual(len(seen), 2)

    def test_waits_for_pg_ctl_exit_and_actual_postgres_adoption(self):
        self.exercise(adopted=True)

    def test_missing_adoption_still_fails_without_releasing_ownership(self):
        self.exercise(adopted=False)


if __name__ == '__main__':
    unittest.main()
