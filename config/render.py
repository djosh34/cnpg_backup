#!/usr/bin/env python3
"""Small explicit install renderer. No arbitrary Pod templates or image tags.

Prints JSON manifests; does not deploy anything. Repository schema is separate.
"""
import argparse
import json
import re


def resource(api, kind, name, namespace, **body):
    return {'apiVersion': api, 'kind': kind, 'metadata': {'name': name, 'namespace': namespace}, **body}


def render(manager_image, data_image, namespace, managed_namespace, secret_names):
    if not secret_names:
        raise ValueError('an explicit nonempty Secret allowlist is required')
    for image in (manager_image, data_image):
        if not re.fullmatch(r'[A-Za-z0-9./:_-]+@sha256:[0-9a-f]{64}', image):
            raise ValueError('install images must be resolved immutable digests')
    for name in [namespace, managed_namespace, *secret_names]:
        if not re.fullmatch(r'[a-z0-9]([a-z0-9.-]*[a-z0-9])?', name):
            raise ValueError('invalid namespace or Secret name')
    objects = []
    objects.append(resource('v1', 'ServiceAccount', 'cnpg-backup', namespace))
    objects.append(resource('cert-manager.io/v1', 'Issuer', 'cnpg-backup-selfsigned', namespace, spec={'selfSigned': {}}))
    objects.append(resource('cert-manager.io/v1', 'Certificate', 'cnpg-backup-ca', namespace, spec={
        'isCA': True, 'commonName': 'cnpg-backup-private-ca', 'secretName': 'cnpg-backup-ca',
        'duration': '8760h', 'renewBefore': '720h', 'privateKey': {'algorithm': 'ECDSA', 'size': 256},
        'issuerRef': {'name': 'cnpg-backup-selfsigned'}}))
    objects.append(resource('cert-manager.io/v1', 'Issuer', 'cnpg-backup-ca', namespace, spec={'ca': {'secretName': 'cnpg-backup-ca'}}))
    service_name = f'cnpg-backup.{namespace}.svc'
    for role in ('server', 'client'):
        spec = {'secretName': f'cnpg-backup-{role}-tls', 'commonName': service_name if role == 'server' else 'cnpg-backup-client',
                'privateKey': {'algorithm': 'ECDSA', 'size': 256, 'rotationPolicy': 'Always'},
                'issuerRef': {'name': 'cnpg-backup-ca'}, 'usages': ['digital signature', role + ' auth']}
        if role == 'server':
            spec['dnsNames'] = [service_name]
        objects.append(resource('cert-manager.io/v1', 'Certificate', f'cnpg-backup-{role}', namespace, spec=spec))
    manager_config = {'namespaces': [managed_namespace], 'secretNames': {managed_namespace: sorted(set(secret_names))},
                      'image': data_image, 'clientName': 'cnpg-backup-client'}
    objects.append(resource('v1', 'ConfigMap', 'cnpg-backup-manager', namespace, data={'config.json': json.dumps(manager_config)}))
    service = resource('v1', 'Service', 'cnpg-backup', namespace, spec={'selector': {'app': 'cnpg-backup'}, 'ports': [{'name': 'grpc', 'port': 9090, 'targetPort': 9090}]})
    service['metadata'].update(labels={'cnpg.io/pluginName': 'cnpg-backup.djosh34.github.io'}, annotations={
        'cnpg.io/pluginPort': '9090', 'cnpg.io/pluginClientSecret': 'cnpg-backup-client-tls',
        'cnpg.io/pluginServerSecret': 'cnpg-backup-server-tls', 'cnpg.io/pluginServerName': service_name})
    objects.append(service)
    security = {'runAsUser': 26, 'runAsGroup': 26, 'runAsNonRoot': True, 'readOnlyRootFilesystem': True,
                'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}}
    objects.append(resource('apps/v1', 'Deployment', 'cnpg-backup', namespace, spec={
        'replicas': 1, 'strategy': {'type': 'Recreate'}, 'selector': {'matchLabels': {'app': 'cnpg-backup'}},
        'template': {'metadata': {'labels': {'app': 'cnpg-backup'}}, 'spec': {'serviceAccountName': 'cnpg-backup',
            'securityContext': {'fsGroup': 26, 'runAsUser': 26, 'runAsGroup': 26, 'runAsNonRoot': True},
            'containers': [{'name': 'manager', 'image': manager_image, 'imagePullPolicy': 'IfNotPresent',
                'command': ['/usr/local/bin/cnpg-backup', 'manager'], 'ports': [{'containerPort': 9090, 'name': 'grpc'}],
                'readinessProbe': {'tcpSocket': {'port': 9090}, 'periodSeconds': 2, 'timeoutSeconds': 1},
                'securityContext': security, 'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'}, 'limits': {'cpu': '500m', 'memory': '256Mi'}},
                'volumeMounts': [{'name': 'config', 'mountPath': '/cnpg-backup/manager', 'readOnly': True},
                                 {'name': 'tls', 'mountPath': '/cnpg-backup/tls', 'readOnly': True}]}],
            'volumes': [{'name': 'config', 'configMap': {'name': 'cnpg-backup-manager', 'defaultMode': 0o440}},
                        {'name': 'tls', 'projected': {'defaultMode': 0o440, 'sources': [
                            {'secret': {'name': 'cnpg-backup-server-tls', 'items': [{'key': 'tls.crt', 'path': 'tls.crt'}, {'key': 'tls.key', 'path': 'tls.key'}]}},
                            {'secret': {'name': 'cnpg-backup-ca', 'items': [{'key': 'tls.crt', 'path': 'client-ca.crt'}]}}]}}]}}}))
    rules = [
        {'apiGroups': ['backup.cnpg-backup.djosh34.github.io'], 'resources': ['repositories'], 'verbs': ['get']},
        {'apiGroups': ['postgresql.cnpg.io'], 'resources': ['clusters'], 'verbs': ['get']},
        {'apiGroups': [''], 'resources': ['persistentvolumeclaims'], 'verbs': ['get']},
        {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['get', 'create']},
        {'apiGroups': [''], 'resources': ['secrets'], 'resourceNames': sorted(set(secret_names)), 'verbs': ['get']},
    ]
    objects.append(resource('rbac.authorization.k8s.io/v1', 'Role', 'cnpg-backup', managed_namespace, rules=rules))
    objects.append(resource('rbac.authorization.k8s.io/v1', 'RoleBinding', 'cnpg-backup', managed_namespace,
                            roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'cnpg-backup'},
                            subjects=[{'kind': 'ServiceAccount', 'name': 'cnpg-backup', 'namespace': namespace}]))
    return {'apiVersion': 'v1', 'kind': 'List', 'items': objects}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--manager-image', required=True)
    parser.add_argument('--data-image', required=True)
    parser.add_argument('--namespace', default='cnpg-system')
    parser.add_argument('--managed-namespace', required=True)
    parser.add_argument('--secret-name', action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(render(args.manager_image, args.data_image, args.namespace, args.managed_namespace, args.secret_name), indent=2))
