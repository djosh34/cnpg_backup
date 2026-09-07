#!/usr/bin/env python3
"""Real pinned kind/CNPG lifecycle smoke; NOT complete PR D qualification.

No model, production endpoint, mocked lifecycle or fabricated artifact pin.
Additional mandatory private-CA/operator-wire recovery scenarios remain explicit.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / '.work/cnpg-smoke'
OUT = ROOT / 'artifacts/cnpg-smoke'
LOCK = json.loads((ROOT / 'build/kubernetes-inputs.lock.json').read_text())
NAME = 'cnpg-backup-smoke'
NS = 'backup-smoke'
spec = importlib.util.spec_from_file_location('install_render', ROOT / 'config/render.py')
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)


def run(*args, check=True, timeout=300, input=None):
    result = subprocess.run([str(a) for a in args], cwd=ROOT, input=input, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'{args}: {result.stdout[-12000:]}')
    return result.stdout


def kube(*args, **kwargs):
    return run(WORK / 'kubectl', '--kubeconfig', WORK / 'kubeconfig', *args, **kwargs)


def apply(document):
    return kube('apply', '--server-side', '--field-manager=cnpg-backup-smoke', '-f', '-', input=json.dumps(document))


def wait(test, description, seconds=180):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if test():
            return
        time.sleep(2)
    raise RuntimeError('barrier timed out: ' + description)


def download(name):
    entry = LOCK[name]
    data = urllib.request.urlopen(entry['url'], timeout=60).read()
    if hashlib.sha256(data).hexdigest() != entry['sha256']:
        raise RuntimeError('download checksum mismatch: ' + name)
    path = WORK / name
    path.write_bytes(data)
    if name in ('kind-linux-amd64', 'kubectl'):
        path.chmod(0o555)
    return path


def image_digest(flavor):
    sha = run('git', 'rev-parse', 'HEAD').strip()
    tag = f'cnpg-backup-smoke-{flavor}:{sha}'
    run('docker', 'tag', f'cnpg-backup-foundation-{flavor}:test', tag)
    run(WORK / 'kind-linux-amd64', 'load', 'docker-image', '--name', NAME, tag)
    full = 'docker.io/library/' + tag
    lines = run('docker', 'exec', NAME + '-control-plane', 'ctr', '-n', 'k8s.io', 'images', 'list').splitlines()
    parts = next(line.split() for line in lines if line.split()[0] == full)
    digest = parts[2]
    if not digest.startswith('sha256:'):
        raise RuntimeError('containerd did not resolve an immutable image digest')
    ref = 'docker.io/library/' + tag.split(':')[0] + '@' + digest
    run('docker', 'exec', NAME + '-control-plane', 'ctr', '-n', 'k8s.io', 'images', 'tag', full, ref)
    return ref


def bounded_workspaces():
    # Test orchestration only. kind's default local-path directories have no hard
    # capacity limit, so they MUST NOT masquerade as bounded native workspace.
    # Each test workspace is an independent ext4 filesystem on a sparse 1Gi disk.
    apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'cnpg-backup-bounded'},
           'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
    for i in range(12):
        path = f'/var/local/cnpg-backup-work-{i}'
        run('docker', 'exec', NAME + '-control-plane', 'sh', '-ec',
            f'truncate -s 1G {path}.img; mkfs.ext4 -q -F {path}.img; mkdir {path}; mount -o loop {path}.img {path}')
        apply({'apiVersion': 'v1', 'kind': 'PersistentVolume', 'metadata': {'name': f'cnpg-backup-work-{i}'},
               'spec': {'capacity': {'storage': '1Gi'}, 'accessModes': ['ReadWriteOnce'], 'volumeMode': 'Filesystem',
                        'storageClassName': 'cnpg-backup-bounded', 'persistentVolumeReclaimPolicy': 'Retain', 'local': {'path': path},
                        'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [
                            {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': [NAME + '-control-plane']}]}]}}}})


def main_pods():
    objects = json.loads(kube('get', 'pods', '-n', NS, '-l', 'cnpg.io/cluster=database', '-o', 'json'))['items']
    return [p for p in objects if any(c['name'] == 'postgres' for c in p['spec']['containers'])]


def pod_uids():
    return sorted(p['metadata']['uid'] for p in main_pods())


def assert_placement():
    pods = main_pods()
    assert len(pods) == 2, 'multi-instance smoke did not create two actual instances'
    for pod in pods:
        init = pod['spec']['initContainers']
        assert [c['name'] for c in init].count('cnpg-backup') == 1
        assert [c['name'] for c in init].index('cnpg-backup-socket-init') < [c['name'] for c in init].index('cnpg-backup')
        sidecar = next(c for c in init if c['name'] == 'cnpg-backup')
        assert sidecar['restartPolicy'] == 'Always'
        mounts = {m['mountPath'] for m in sidecar['volumeMounts']}
        assert {'/var/lib/postgresql/data', '/var/lib/postgresql/wal', '/var/lib/postgresql/tablespaces/fast_space'} <= mounts
        assert sidecar['securityContext']['runAsUser'] == 26
        assert sidecar['securityContext']['readOnlyRootFilesystem']
        kube('exec', '-n', NS, pod['metadata']['name'], '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup', 'instance', '--probe')


def main():
    WORK.mkdir(parents=True, exist_ok=False)
    OUT.mkdir(parents=True, exist_ok=True)
    for name in ('kind-linux-amd64', 'kubectl', 'cnpg-1.30.0.yaml', 'cert-manager.yaml'):
        download(name)
    # Replace upstream tag references only with locally verified registry digest
    # resolutions. Checksummed release manifests are retained as original inputs.
    for name in ('cnpg-1.30.0.yaml', 'cert-manager.yaml'):
        text = (WORK / name).read_text()
        for tag, digest in LOCK['images'].items():
            text = text.replace(tag, digest)
        (WORK / ('pinned-' + name)).write_text(text)
    kind_config = {'kind': 'Cluster', 'apiVersion': 'kind.x-k8s.io/v1alpha4', 'nodes': [{'role': 'control-plane'}]}
    (WORK / 'kind.json').write_text(json.dumps(kind_config))
    created = False
    report = {'subject_sha': run('git', 'rev-parse', 'HEAD').strip(), 'inputs': LOCK, 'completed': [],
              'profile': 'real-cnpg-lifecycle-smoke', 'release_qualified': False, 'pr_d_complete': False,
              'remaining_mandatory': ['operator private-CA overlap rotation', 'real recovery Job Begin/Drain/poison placement',
                                      'full lifecycle rollout/defaulting matrix and repository status reconciliation']}
    try:
        run(WORK / 'kind-linux-amd64', 'create', 'cluster', '--name', NAME, '--image', LOCK['kindNode'],
            '--config', WORK / 'kind.json', '--kubeconfig', WORK / 'kubeconfig', '--wait', '120s')
        created = True
        version = json.loads(kube('version', '-o', 'json'))
        assert version['serverVersion']['gitVersion'] == 'v1.35.8'
        kube('apply', '--server-side', '-f', WORK / 'pinned-cert-manager.yaml')
        for deployment in ('cert-manager', 'cert-manager-webhook', 'cert-manager-cainjector'):
            kube('rollout', 'status', '-n', 'cert-manager', 'deployment/' + deployment, '--timeout=180s')
        kube('apply', '--server-side', '-f', WORK / 'pinned-cnpg-1.30.0.yaml')
        kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-controller-manager', '--timeout=180s')
        kube('apply', '--server-side', '-f', ROOT / 'config/repository-crd.json')
        kube('wait', '--for=condition=Established', 'crd/repositories.backup.cnpg-backup.djosh34.github.io', '--timeout=60s')
        kube('create', 'namespace', NS)
        bounded_workspaces()
        manager, data = image_digest('manager'), image_digest('pg18')
        report['subject_images'] = {'manager': manager, 'pg18': data}
        install = renderer.render(manager, data, 'cnpg-system', NS, ['s3-auth', 'database-ca', 'database-replication'])
        (WORK / 'install.json').write_text(json.dumps(install))
        apply(install)
        kube('wait', '-n', 'cnpg-system', '--for=condition=Ready', 'certificate/cnpg-backup-server', 'certificate/cnpg-backup-client', '--timeout=180s')
        kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
        apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-auth', 'namespace': NS},
               'stringData': {'access': 'disposable-test-only-access', 'secret': 'disposable-test-only-secret'}})
        repository = {'apiVersion': 'backup.cnpg-backup.djosh34.github.io/v1alpha1', 'kind': 'Repository',
                      'metadata': {'name': 'destination', 'namespace': NS}, 'spec': {
                          'repositoryID': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                          's3': {'endpoint': 'https://storage-not-used.invalid', 'bucket': 'test-bucket', 'prefix': 'smoke',
                                 'accessKeySecret': {'name': 's3-auth', 'key': 'access'}, 'secretKeySecret': {'name': 's3-auth', 'key': 'secret'}},
                          'workspace': {'storageClassName': 'cnpg-backup-bounded', 'size': '1Gi'}}}
        apply(repository)
        observed = json.loads(kube('get', 'repository', 'destination', '-n', NS, '-o', 'json'))
        assert observed['spec']['retention'] == {'enabled': False, 'dryRun': True, 'minimumFulls': 2, 'interval': '1h'}
        changed = json.loads(json.dumps(repository)); changed['spec']['repositoryID'] = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
        rejected = kube('apply', '--server-side', '--field-manager=cnpg-backup-smoke', '-f', '-', input=json.dumps(changed), check=False)
        assert 'immutable' in rejected, 'CEL immutable repository identity was not enforced'
        cluster = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'metadata': {'name': 'database', 'namespace': NS},
                   'spec': {'instances': 2, 'imageName': LOCK['database'], 'storage': {'size': '1Gi'}, 'walStorage': {'size': '1Gi'},
                            'tablespaces': [{'name': 'fast_space', 'storage': {'size': '1Gi'}}],
                            'plugins': [{'name': 'cnpg-backup.djosh34.github.io', 'parameters': {'repository': 'destination'}}]}}
        # Discovery is asynchronous in the real operator. Use its actual
        # validating admission path as the startup barrier, before creating data.
        admission_attempts = []
        def discovered():
            result = kube('apply', '--server-side', '--dry-run=server', '--field-manager=cnpg-backup-smoke', '-f', '-',
                          input=json.dumps(cluster), check=False)
            admission_attempts.append(result[-4000:])
            (OUT / 'discovery.log').write_text('\n'.join(admission_attempts))
            return 'serverside-applied (server dry run)' in result or 'created (server dry run)' in result
        wait(discovered, 'CNPG mTLS plugin discovery and validation')
        apply(cluster)
        kube('wait', '-n', NS, '--for=condition=Ready', 'cluster/database', '--timeout=360s', timeout=400)
        assert_placement()
        report['completed'].append('real-CNPG-1.30-two-instance-initdb-join-WAL-tablespace-startup-and-CRD-CEL')
        before = pod_uids()
        kube('rollout', 'restart', '-n', 'cnpg-system', 'deployment/cnpg-backup')
        kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
        for i in range(3):
            kube('annotate', '-n', NS, 'cluster/database', 'smoke-reconcile=' + str(i), '--overwrite')
            time.sleep(5)
            assert_placement()
            assert pod_uids() == before, 'reconcile/restart caused unwanted Pod churn'
        report['completed'].append('manager-restart-reconcile-idempotency')
        # Force cert-manager leaf reissuance; operator's actual Secret watch must
        # rebuild the leaf-trusting CNPG connection, not merely a test TLS client.
        old = json.loads(kube('get', 'secret', '-n', 'cnpg-system', 'cnpg-backup-server-tls', '-o', 'json'))['data']['tls.crt']
        kube('delete', 'secret', '-n', 'cnpg-system', 'cnpg-backup-server-tls')
        def changed_leaf():
            result = kube('get', 'secret', '-n', 'cnpg-system', 'cnpg-backup-server-tls', '-o', 'json', check=False)
            try:
                return json.loads(result).get('data', {}).get('tls.crt') not in (None, old)
            except json.JSONDecodeError:
                return False  # cert-manager has not recreated the deleted Secret
        wait(changed_leaf, 'new server leaf')
        time.sleep(15)  # kubelet Secret propagation; validation below is the actual oracle
        kube('annotate', '-n', NS, 'cluster/database', 'leaf-rotation=observed', '--overwrite')
        assert_placement()
        assert pod_uids() == before
        report['completed'].append('actual-operator-server-leaf-rotation')
        kube('delete', '-n', NS, 'cluster/database', '--wait=true', '--timeout=120s')
        kube('delete', '-f', WORK / 'install.json', '--wait=true', '--timeout=120s')
        kube('delete', '-f', ROOT / 'config/repository-crd.json', '--wait=true', '--timeout=120s')
        report['completed'].append('plugin-uninstall')
        print('PASS real CNPG lifecycle smoke; NOT complete PR D acceptance')
    finally:
        (OUT / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')
        if created:
            (OUT / 'resources.log').write_text(kube('get', 'pods,jobs,pvc,clusters', '-A', '-o', 'wide', check=False)[-256000:])
            (OUT / 'events.log').write_text(kube('get', 'events', '-A', '--sort-by=.metadata.creationTimestamp', check=False)[-256000:])
            for namespace, name in [('cnpg-system', 'cnpg-backup'), ('cnpg-system', 'cnpg-controller-manager')]:
                (OUT / (name + '.log')).write_text(kube('logs', '-n', namespace, 'deployment/' + name, '--all-containers', '--tail=500', check=False)[-256000:])
            run(WORK / 'kind-linux-amd64', 'delete', 'cluster', '--name', NAME, check=False)


if __name__ == '__main__':
    main()
