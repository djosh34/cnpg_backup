#!/usr/bin/env python3
"""Small explicit install renderer. No arbitrary Pod templates or image tags.

Prints JSON manifests; does not deploy anything. Repository schema is separate.
"""
import argparse
import copy
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
                      'image': data_image, 'clientName': 'cnpg-backup-client', 'operatorNamespace': namespace}
    objects.append(resource('v1', 'ConfigMap', 'cnpg-backup-manager', namespace, data={'config.json': json.dumps(manager_config)}))
    service = resource('v1', 'Service', 'cnpg-backup', namespace, spec={'selector': {'app': 'cnpg-backup'}, 'ports': [{'name': 'grpc', 'port': 9090, 'targetPort': 9090}]})
    service['metadata'].update(labels={'cnpg.io/pluginName': 'cnpg-backup.djosh34.github.io'}, annotations={
        'cnpg.io/pluginPort': '9090', 'cnpg.io/pluginClientSecret': 'cnpg-backup-client-tls',
        'cnpg.io/pluginServerSecret': 'cnpg-backup-server-tls', 'cnpg.io/pluginServerName': service_name})
    objects.append(service)
    # Separate internal metrics Service: never a CNPG-discoverable plugin/data proxy.
    metrics = resource('v1', 'Service', 'cnpg-backup-metrics', namespace, spec={
        'selector': {'app': 'cnpg-backup'}, 'ports': [{'name': 'metrics', 'port': 9091, 'targetPort': 'metrics'}]})
    metrics['metadata']['labels'] = {'app': 'cnpg-backup-metrics'}
    objects.append(metrics)
    security = {'runAsUser': 26, 'runAsGroup': 26, 'runAsNonRoot': True, 'readOnlyRootFilesystem': True,
                'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}}
    objects.append(resource('apps/v1', 'Deployment', 'cnpg-backup', namespace, spec={
        'replicas': 1, 'strategy': {'type': 'Recreate'}, 'selector': {'matchLabels': {'app': 'cnpg-backup'}},
        'template': {'metadata': {'labels': {'app': 'cnpg-backup'}}, 'spec': {'serviceAccountName': 'cnpg-backup',
            'securityContext': {'fsGroup': 26, 'runAsUser': 26, 'runAsGroup': 26, 'runAsNonRoot': True},
            'containers': [{'name': 'manager', 'image': manager_image, 'imagePullPolicy': 'IfNotPresent',
                'command': ['/usr/local/bin/cnpg-backup', 'manager'], 'ports': [{'containerPort': 9090, 'name': 'grpc'}, {'containerPort': 9091, 'name': 'metrics'}],
                'readinessProbe': {'tcpSocket': {'port': 9090}, 'periodSeconds': 2, 'timeoutSeconds': 1},
                'securityContext': security, 'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'}, 'limits': {'cpu': '500m', 'memory': '256Mi'}},
                'volumeMounts': [{'name': 'config', 'mountPath': '/cnpg-backup/manager', 'readOnly': True},
                                 {'name': 'tls', 'mountPath': '/cnpg-backup/tls', 'readOnly': True},
                                 {'name': 'control', 'mountPath': '/cnpg-backup/control'},
                                 {'name': 'retention', 'mountPath': '/cnpg-backup/retention'}]}],
            'volumes': [{'name': 'control', 'emptyDir': {'sizeLimit': '16Mi'}},
                        {'name': 'retention', 'emptyDir': {'sizeLimit': '512Mi'}},
                        {'name': 'config', 'configMap': {'name': 'cnpg-backup-manager', 'defaultMode': 0o440}},
                        {'name': 'tls', 'projected': {'defaultMode': 0o440, 'sources': [
                            {'secret': {'name': 'cnpg-backup-server-tls', 'items': [{'key': 'tls.crt', 'path': 'tls.crt'}, {'key': 'tls.key', 'path': 'tls.key'}]}},
                            {'secret': {'name': 'cnpg-backup-ca', 'items': [{'key': 'tls.crt', 'path': 'client-ca.crt'}]}}]}}]}}}))
    objects.append(resource('rbac.authorization.k8s.io/v1', 'Role', 'cnpg-backup-version', namespace, rules=[
        {'apiGroups': ['apps'], 'resources': ['deployments'], 'resourceNames': ['cnpg-controller-manager'], 'verbs': ['get']}]))
    objects.append(resource('rbac.authorization.k8s.io/v1', 'RoleBinding', 'cnpg-backup-version', namespace,
                            roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'cnpg-backup-version'},
                            subjects=[{'kind': 'ServiceAccount', 'name': 'cnpg-backup', 'namespace': namespace}]))
    rules = [
        {'apiGroups': ['backup.cnpg-backup.djosh34.github.io'], 'resources': ['repositories'], 'verbs': ['get', 'list']},
        {'apiGroups': ['backup.cnpg-backup.djosh34.github.io'], 'resources': ['repositories/status'], 'verbs': ['patch']},
        {'apiGroups': [''], 'resources': ['events'], 'verbs': ['create']},
        {'apiGroups': ['postgresql.cnpg.io'], 'resources': ['clusters'], 'verbs': ['get', 'list']},
        {'apiGroups': ['postgresql.cnpg.io'], 'resources': ['backups'], 'verbs': ['list', 'watch']},
        {'apiGroups': [''], 'resources': ['persistentvolumeclaims'], 'verbs': ['get']},
        {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list', 'watch']},
        {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'list']},
        {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['get', 'create', 'update']},
        {'apiGroups': [''], 'resources': ['secrets'], 'resourceNames': sorted(set(secret_names)), 'verbs': ['get']},
    ]
    objects.append(resource('rbac.authorization.k8s.io/v1', 'Role', 'cnpg-backup', managed_namespace, rules=rules))
    objects.append(resource('rbac.authorization.k8s.io/v1', 'RoleBinding', 'cnpg-backup', managed_namespace,
                            roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'cnpg-backup'},
                            subjects=[{'kind': 'ServiceAccount', 'name': 'cnpg-backup', 'namespace': namespace}]))
    return {'apiVersion': 'v1', 'kind': 'List', 'items': objects}


def recovery_cluster(template, namespace, name, source_repository, destination_repository, target):
    """Fresh consumer Cluster; never copy source identity/status/PVC bindings."""
    if template.get('kind') != 'Cluster' or template.get('apiVersion') != 'postgresql.cnpg.io/v1':
        raise ValueError('a CNPG Cluster template is required')
    for value in (namespace, name, source_repository, destination_repository):
        if not re.fullmatch(r'[a-z0-9]([a-z0-9.-]*[a-z0-9])?', value):
            raise ValueError('invalid recovery resource name')
    if source_repository == destination_repository:
        raise ValueError('source and destination Repositories must be distinct')
    if not isinstance(target, dict):
        raise ValueError('target must be a CNPG recoveryTarget object')
    if template.get('metadata', {}).get('name') == name and template.get('metadata', {}).get('namespace', namespace) == namespace:
        raise ValueError('recovery requires a fresh Cluster name')
    spec = copy.deepcopy(template['spec'])
    storage = [spec.get('storage', {}), spec.get('walStorage', {})]
    storage += [t.get('storage', {}) for t in spec.get('tablespaces', [])]
    if any(s.get('pvcTemplate', {}).get(k) for s in storage for k in ('volumeName', 'selector', 'dataSource', 'dataSourceRef')):
        raise ValueError('recovery requires fresh unbound target PVCs, not source volume bindings or snapshots')
    # Only declared Cluster spec is reused. Kubernetes allocates new Cluster/PVC
    # identities; neither generated metadata nor status is copied from a live GET.
    spec['instances'] = 1
    spec['bootstrap'] = {'recovery': {'source': 'origin', 'recoveryTarget': target}}
    spec['externalClusters'] = [{'name': 'origin', 'plugin': {
        'name': 'cnpg-backup.djosh34.github.io', 'parameters': {'repository': source_repository}}}]
    spec['plugins'] = [{'name': 'cnpg-backup.djosh34.github.io', 'isWALArchiver': True,
                        'parameters': {'repository': destination_repository}}]
    return resource('postgresql.cnpg.io/v1', 'Cluster', name, namespace, spec=spec)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--manager-image', required=True)
    parser.add_argument('--data-image', required=True)
    parser.add_argument('--namespace', default='cnpg-system')
    parser.add_argument('--managed-namespace', required=True)
    parser.add_argument('--secret-name', action='append', required=True)
    args = parser.parse_args()
    print(json.dumps(render(args.manager_image, args.data_image, args.namespace, args.managed_namespace, args.secret_name), indent=2))
