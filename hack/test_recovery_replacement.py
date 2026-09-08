"""Replacement fixture/oracle tests, not CNPG or Pod execution coverage."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from recovery_cases import Campaign


def construct(original):
    """Intercept only API apply, after the actual campaign constructor runs."""
    class Captured(Exception):
        pass
    result = []
    def apply(pod):
        result.append(pod)
        raise Captured()
    campaign = Campaign(None, None)
    state = {'name': 'g-032', 'pod': original['metadata']['name']}
    with patch.object(campaign, 'pods', return_value=[original]), \
         patch.object(campaign, 'target_snapshot', return_value=['snapshot'] * 3), \
         patch('recovery_cases.h.apply', side_effect=apply):
        try:
            campaign.replacement(state)
        except Captured:
            pass
    assert len(result) == 1
    return result[0]


class ReplacementTests(unittest.TestCase):
    def test_constructor_retains_observer_on_fresh_controller_emptydir(self):
        original = {'metadata': {'name': 'old', 'namespace': 'campaign-target', 'uid': 'old-uid',
                                 'labels': {'cnpg.io/cluster': 'g-032', 'batch.kubernetes.io/controller-uid': 'job'},
                                 'ownerReferences': [{'kind': 'Job', 'uid': 'job', 'controller': True}]},
                    'spec': {'nodeName': 'node', 'restartPolicy': 'OnFailure',
                             'volumes': [{'name': 'controller', 'emptyDir': {}},
                                         {'name': 'pgdata', 'persistentVolumeClaim': {'claimName': 'data'}}],
                             'containers': [{'name': 'full-recovery'}],
                             'initContainers': [{'name': 'campaign-observer-install'}]},
                    'status': {'phase': 'Running'}}
        saved = copy.deepcopy(original)
        replacement = construct(original)
        self.assertEqual(original, saved)
        self.assertEqual(replacement['spec']['volumes'], original['spec']['volumes'])
        self.assertEqual(replacement['spec']['initContainers'], original['spec']['initContainers'])
        self.assertEqual(replacement['metadata'], {'name': 'g-032-replacement', 'namespace': 'campaign-target'})
        self.assertNotIn('status', replacement)
        self.assertNotIn('nodeName', replacement['spec'])
        self.assertEqual(replacement['spec']['restartPolicy'], 'Never')


if __name__ == '__main__':
    unittest.main()
