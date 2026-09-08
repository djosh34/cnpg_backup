"""Audit completeness is executable; unclassified new assertions fail cheaply."""
import ast
from collections import Counter
import json
from pathlib import Path
import unittest

from campaign_plan import ROOT, REGISTRY
from recovery_cases import Campaign


def required_records():
    records = []
    for file in ('hack/recovery_cases.py', 'hack/recovery_campaign.py', 'hack/wal_smoke.py', 'hack/campaign_fixture.py'):
        def visit(node, function='module'):
            if isinstance(node, ast.FunctionDef):
                if file.endswith('wal_smoke.py') and node.name not in ('__init__', 'setup', 's3', 'control', 'close', 'ready', 'bucket_ready'):
                    return
                function = node.name
            if isinstance(node, ast.Assert):
                records.append((file, function, ast.unparse(node.test)))
            elif isinstance(node, ast.If) and ((function == 'tool' and 'result.returncode' in ast.unparse(node.test))
                                               or (function == 'replay_failure' and 'receipt[' in ast.unparse(node.test))):
                records.append((file, function, ast.unparse(node.test)))
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and (node.func.attr in ('wait', 'barrier') or
                    (function == 'case_pending_sidecar_task_same_incarnation_drain' and node.func.attr == 'result')):
                records.append((file, function, ast.unparse(node)))
            for child in ast.iter_child_nodes(node):
                visit(child, function)
        visit(ast.parse((ROOT / file).read_text()))
    return Counter(records)


class AuditTests(unittest.TestCase):
    def test_every_assertion_and_observable_wait_has_a_disposition(self):
        records = json.loads((ROOT / 'docs/campaign-assertion-audit.json').read_text())['assertions']
        observed = Counter((r['file'], r['function'], r['expression']) for r in records)
        self.assertEqual(observed, required_records())
        for record in records:
            self.assertIn(record['category'], ('safety-invariant', 'eventual-outcome', 'fixture-precondition', 'implementation-detail'))
            self.assertTrue(record['requirement'])
            self.assertTrue(record['temporal_semantics'])
        # Deliberate missing-audit control is rejected by the same completeness oracle.
        observed.subtract([next(iter(observed))])
        self.assertNotEqual(observed, required_records())

    def test_conditional_and_fixture_requirement_mappings(self):
        records = json.loads((ROOT / 'docs/campaign-assertion-audit.json').read_text())['assertions']
        ordinary = next(r for r in records if r['id'] == 'd75da2f47d486740')
        self.assertEqual(ordinary['category'], 'eventual-outcome')
        source = next(r for r in records if r['id'] == '514b2f52518ae625')
        self.assertIn('source namespace finalized', source['requirement'])
        for r in records:
            if r['function'] in ('ready', 'bucket_ready'):
                self.assertNotIn('full-size independently archived WAL', r['requirement'])
            if r['function'] == 'replay_failure' and r['failure_layer'] == 'product':
                self.assertIn('fault receipt', r['temporal_semantics'])
        tool = next(r for r in records if r['function'] == 'tool' and 'result.returncode' in r['expression'])
        self.assertIn('exit2', tool['requirement'])
        self.assertIn('WAL rejected', tool['requirement'])

    def test_all_registered_cases_resolve_to_real_oracles_and_explicit_requirements(self):
        for case in REGISTRY:
            self.assertTrue(callable(getattr(Campaign, case['method'])))
            self.assertTrue(case['requirement'])
            self.assertGreater(case['seconds'], 0)
        self.assertFalse(hasattr(Campaign, 'run'), 'superseded monolithic control must not survive')
