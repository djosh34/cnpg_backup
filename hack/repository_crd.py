#!/usr/bin/env python3
"""Deterministic structural Repository CRD; no conversion/admission webhook."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def obj(properties, required=(), default=None, rules=()):
    result = {'type': 'object', 'properties': properties}
    if required:
        result['required'] = list(required)
    if default is not None:
        result['default'] = default
    if rules:
        result['x-kubernetes-validations'] = [{'rule': rule, 'message': message} for rule, message in rules]
    return result


def string(default=None, enum=None, **extra):
    result = {'type': 'string', **extra}
    if default is not None:
        result['default'] = default
    if enum:
        result['enum'] = enum
    return result


def integer(default, maximum, minimum=1):
    return {'type': 'integer', 'format': 'int64', 'default': default, 'minimum': minimum, 'maximum': maximum}


def duration(default, low, high):
    return string(default, maxLength=32, **{'x-kubernetes-validations': [
        {'rule': f"duration(self) >= duration('{low}') && duration(self) <= duration('{high}')", 'message': 'duration outside supported range'}]})


def schema():
    selector = obj({'name': string(minLength=1, maxLength=253, pattern=r'^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$'),
                    'key': string(minLength=1, maxLength=253, pattern=r'^[-._a-zA-Z0-9]+$')}, ('name', 'key'))
    immutable = {'x-kubernetes-validations': [{'rule': 'self == oldSelf', 'message': 'storage identity is immutable; create a new Repository'}]}
    s3 = {
        'endpoint': string(minLength=9, maxLength=2048, pattern=r'^https://[^/@?#]+/?$', **immutable),
        'bucket': string(minLength=3, maxLength=63, pattern=r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', **immutable),
        'prefix': string(minLength=1, maxLength=512, **immutable),
        'signature': string('v4', ['v2', 'v4'], **immutable), 'addressing': string('path', ['path'], **immutable),
        'region': string('us-east-1', minLength=1, maxLength=64, **immutable),
        'encryption': string('bucket-default', ['bucket-default']),
        'accessKeySecret': selector, 'secretKeySecret': selector, 'sessionTokenSecret': selector, 'caConfigMap': selector,
    }
    resources = obj({key: obj({'cpu': string(value[0]), 'memory': string(value[1])}, default={})
                     for key, value in {'requests': ('100m', '256Mi'), 'limits': ('2', '3Gi')}.items()}, default={})
    spec = obj({
        'repositoryID': string(pattern=r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', **immutable),
        's3': obj(s3, ('endpoint', 'bucket', 'prefix', 'accessKeySecret', 'secretKeySecret'), rules=[
            ("self.signature != 'v2' || !has(self.sessionTokenSecret)", 'SigV2 cannot use a session token')]),
        'compression': string('gzip', ['none', 'gzip']),
        'workspace': obj({'storageClassName': string(minLength=1, maxLength=253), 'size': string('256Gi', minLength=1, maxLength=32)}, ('storageClassName',)),
        'native': obj({'maxBackupBytes': integer(32 << 30, 1 << 40), 'maxBootstrapWALBytes': integer(8 << 30, 256 << 30),
                       'maxRestoredBytes': integer(32 << 30, 1 << 40), 'maxReferenceAge': duration('192h', '1h', '720h'),
                       'captureTimeout': duration('6h', '1m', '6h')}, default={}, rules=[
                           ('self.maxBootstrapWALBytes <= self.maxBackupBytes', 'bootstrap WAL is included in the raw backup budget')]),
        'io': obj({'connectTimeout': duration('10s', '1s', '60s'), 'metadataTimeout': duration('30s', '1s', '120s'),
                   'dataRequestTimeout': duration('15m', '10s', '1h'), 'operationTimeout': duration('24h', '1m', '168h'),
                   'walUploadTimeout': duration('120s', '10s', '15m'), 'artifactUploads': integer(2, 2),
                   'partWorkers': integer(2, 2), 'walUploads': integer(2, 2)}, default={}),
        'retention': obj({'enabled': {'type': 'boolean', 'default': False}, 'dryRun': {'type': 'boolean', 'default': True},
                          'window': duration(None, '1h', '87600h'), 'minimumFulls': integer(2, 100),
                          'interval': duration('1h', '5m', '24h')}, default={}, rules=[
                              ('!self.enabled || has(self.window)', 'enabling retention requires a window')]),
        'resources': resources,
        'backupFreshness': obj({'fullMaxAge': duration(None, '1m', '87600h'), 'differentialMaxAge': duration(None, '1m', '87600h')}),
    }, ('repositoryID', 's3', 'workspace'))
    condition = obj({'type': string(), 'status': string(enum=['True', 'False', 'Unknown']), 'reason': string(),
                     'message': string(), 'lastTransitionTime': string(format='date-time'), 'observedGeneration': {'type': 'integer', 'format': 'int64'}},
                    ('type', 'status', 'reason', 'message', 'lastTransitionTime'))
    status = obj({'observedGeneration': {'type': 'integer', 'format': 'int64'}, 'configurationHash': string(),
                  'conditions': {'type': 'array', 'items': condition, 'x-kubernetes-list-type': 'map', 'x-kubernetes-list-map-keys': ['type']}})
    return obj({'apiVersion': string(), 'kind': string(), 'metadata': {'type': 'object'}, 'spec': spec, 'status': status}, ('spec',))


def document():
    return {'apiVersion': 'apiextensions.k8s.io/v1', 'kind': 'CustomResourceDefinition',
            'metadata': {'name': 'repositories.backup.cnpg-backup.djosh34.github.io'},
            'spec': {'group': 'backup.cnpg-backup.djosh34.github.io', 'scope': 'Namespaced',
                     'names': {'plural': 'repositories', 'singular': 'repository', 'kind': 'Repository'},
                     'versions': [{'name': 'v1alpha1', 'served': True, 'storage': True, 'subresources': {'status': {}},
                                   'schema': {'openAPIV3Schema': schema()}}]}}


if __name__ == '__main__':
    destination = ROOT / 'config/repository-crd.json'
    text = json.dumps(document(), indent=2) + '\n'
    if '--check' in sys.argv:
        if destination.read_text() != text:
            raise SystemExit('Repository CRD differs from its generator')
    else:
        destination.parent.mkdir(exist_ok=True)
        destination.write_text(text)
