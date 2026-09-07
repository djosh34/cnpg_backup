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
        role = next(o for o in objects if o['kind'] == 'Role')
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
