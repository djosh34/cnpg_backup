import importlib.util
import json
from pathlib import Path
import unittest

import repository_crd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('install_render', ROOT / 'config/render.py')
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)


class ConfigurationManifests(unittest.TestCase):
    def test_crd_is_structural_namespaced_and_has_no_escape_hatches(self):
        doc = repository_crd.document()
        self.assertEqual(doc, json.loads((ROOT / 'config/repository-crd.json').read_text()))
        self.assertEqual(doc['spec']['scope'], 'Namespaced')
        schema = doc['spec']['versions'][0]['schema']['openAPIV3Schema']
        self.assertEqual(schema['type'], 'object')
        fields = schema['properties']['spec']['properties']
        self.assertEqual(set(fields), {'repositoryID', 's3', 'compression', 'workspace', 'native', 'io', 'retention', 'resources', 'backupFreshness'})
        self.assertNotIn('conversion', doc['spec'])
        self.assertNotIn('x-kubernetes-preserve-unknown-fields', json.dumps(doc))
        self.assertIn('self == oldSelf', json.dumps(doc))

    def test_cross_field_cel_defaults_are_complete_before_child_defaulting(self):
        # Kubernetes validates the declared composite default itself. It does
        # not first fill that object from each child's default for CEL validation.
        fields = repository_crd.schema()['properties']['spec']['properties']
        for name in ('native', 'retention'):
            node = fields[name]
            expected = {key: value['default'] for key, value in node['properties'].items() if 'default' in value}
            self.assertEqual(node['default'], expected, name + ' has incomplete CEL default inputs')
        native = fields['native']['default']
        retention = fields['retention']['default']
        self.assertLessEqual(native['maxBootstrapWALBytes'], native['maxBackupBytes'])
        self.assertFalse(retention['enabled'])
        self.assertTrue(retention['dryRun'])

    def test_consumer_source_and_recovery_keep_cnpg_version_tag_and_exact_digest(self):
        cluster = json.loads((ROOT / 'config/cluster-example.json').read_text())
        pinned = json.loads((ROOT / 'build/kubernetes-inputs.lock.json').read_text())['database']
        # CNPG needs the tag to infer PG major even when the digest is pinned.
        self.assertEqual(cluster['spec']['imageName'], pinned)
        self.assertIn(':18.6@sha256:', pinned)
        recovered = renderer.recovery_cluster(cluster, 'database', 'recovered', 'source', 'destination', {})
        self.assertEqual(recovered['spec']['imageName'], pinned)

    def test_consumer_recovery_render_discards_identity_and_rejects_bound_targets(self):
        source = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster',
                  'metadata': {'name': 'database', 'namespace': 'test', 'uid': 'old', 'annotations': {'private': 'discard'}},
                  'status': {'currentPrimary': 'old'},
                  'spec': {'instances': 3, 'storage': {'size': '8Gi'}, 'bootstrap': {'initdb': {}},
                           'plugins': [{'name': 'other'}]}}
        c = renderer.recovery_cluster(source, 'test', 'recovered', 'source', 'destination', {'targetTime': '2026-09-01T12:00:00Z'})
        self.assertEqual(c['metadata'], {'name': 'recovered', 'namespace': 'test'})
        self.assertNotIn('status', c)
        self.assertEqual(c['spec']['instances'], 1)
        self.assertEqual(c['spec']['bootstrap']['recovery']['recoveryTarget'], {'targetTime': '2026-09-01T12:00:00Z'})
        self.assertEqual(c['spec']['plugins'][0]['parameters']['repository'], 'destination')
        self.assertEqual(source['spec']['instances'], 3)
        for name, source_repo, destination_repo in [('database', 'source', 'destination'), ('fresh', 'same', 'same')]:
            with self.assertRaises(ValueError):
                renderer.recovery_cluster(source, 'test', name, source_repo, destination_repo, {})
        source['spec']['storage']['pvcTemplate'] = {'volumeName': 'old-pv'}
        with self.assertRaises(ValueError):
            renderer.recovery_cluster(source, 'test', 'fresh', 'source', 'destination', {})

    def test_install_discovery_recreate_mtls_and_secret_get_allowlist(self):
        # Deliberate non-existent digest fixture, never pulled or called a pin.
        image = 'example.invalid/test-fixture@sha256:' + '1' * 64
        objects = renderer.render(image, image, 'cnpg-system', 'test', ['auth'])['items']
        deployment = next(o for o in objects if o['kind'] == 'Deployment')
        self.assertEqual(deployment['spec']['replicas'], 1)
        self.assertEqual(deployment['spec']['strategy'], {'type': 'Recreate'})
        container = deployment['spec']['template']['spec']['containers'][0]
        self.assertTrue(container['securityContext']['readOnlyRootFilesystem'])
        self.assertEqual(container['securityContext']['capabilities']['drop'], ['ALL'])
        service = next(o for o in objects if o['kind'] == 'Service')
        self.assertEqual(service['metadata']['labels']['cnpg.io/pluginName'], 'cnpg-backup.djosh34.github.io')
        self.assertEqual(service['metadata']['annotations']['cnpg.io/pluginServerName'], 'cnpg-backup.cnpg-system.svc')
        role = next(o for o in objects if o['kind'] == 'Role' and o['metadata']['name'] == 'cnpg-backup')
        secrets = next(r for r in role['rules'] if r['resources'] == ['secrets'])
        self.assertEqual(secrets['verbs'], ['get'])
        self.assertEqual(secrets['resourceNames'], ['auth'])
        self.assertNotIn('*', json.dumps(role))
        with self.assertRaises(ValueError):
            renderer.render(image, image, 'cnpg-system', 'test', [])
        with self.assertRaises(ValueError):
            renderer.render('test:latest', image, 'cnpg-system', 'test', ['auth'])


if __name__ == '__main__':
    unittest.main()
