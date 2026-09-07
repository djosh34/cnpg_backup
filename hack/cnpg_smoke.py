#!/usr/bin/env python3
"""Real pinned kind/CNPG lifecycle smoke; NOT complete PR D qualification.

No model, production endpoint, mocked lifecycle or fabricated artifact pin.
Additional mandatory private-CA/operator-wire recovery scenarios remain explicit.
"""
import hashlib
import base64
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid
import sys
import wal_smoke
import backup_smoke

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / '.work/cnpg-smoke'
OUT = ROOT / 'artifacts/cnpg-smoke'
LOCK = json.loads((ROOT / 'build/kubernetes-inputs.lock.json').read_text())
NAME = 'cnpg-backup-smoke'
NS = 'backup-smoke'
# Named acceptance evidence, appended only after each family's assertions pass.
# This registry is not a declaration of data/replay or release qualification.
MANDATORY_SCENARIOS = (
    'real-CNPG-1.30-two-instance-initdb-join-WAL-tablespace-startup-and-CRD-CEL',
    'actual-native-streaming-replica-cert-local-SAN-auth-settings-control-layout-and-standby-rejection',
    'real-Repository-status-generation-secret-rotation-and-Warning-throttle',
    'actual-capacity-finite-accepted',
    'actual-capacity-emptydir-rejected',
    'actual-capacity-localpath-rejected',
    'manager-restart-reconcile-idempotency',
    'actual-operator-server-leaf-rotation',
    'actual-operator-leaf-and-private-CA-overlap-rotation-and-retirement',
    'real-defaulted-live-Pod-config-and-immutable-image-rollout-idempotency',
    'actual-CNPG-before-preflight-pgdata-poison-rejected-with-no-target-mutation',
    'actual-CNPG-before-preflight-wal-poison-rejected-with-no-target-mutation',
    'actual-CNPG-before-preflight-tablespace-poison-rejected-with-no-target-mutation',
    'actual-fresh-Cluster-all-fresh-PVC-guard-Begin-CNPG-preflight-clean-Drain',
    'plugin-uninstall',
)
spec = importlib.util.spec_from_file_location('install_render', ROOT / 'config/render.py')
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)


def reconcile_scenarios(report):
    completed = report['completed']
    assert len(completed) == len(set(completed)), 'duplicate scenario evidence'
    assert set(completed) <= set(MANDATORY_SCENARIOS), 'unknown scenario evidence'
    report['remaining_mandatory'] = [name for name in MANDATORY_SCENARIOS if name not in completed]


def redact_diagnostics(text):
    # Logs from this disposable fixture only. Never collect Secret objects,
    # commands, environments or arbitrary Pod annotations. Drop sensitive lines
    # and PEM blocks rather than try to preserve credential-bearing context.
    text = re.sub(r'-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|\Z)',
                  '<REDACTED>', text, flags=re.S)
    for value in ('disposable-test-only-access', 'disposable-test-only-secret'):
        text = text.replace(value, '<REDACTED>').replace(base64.b64encode(value.encode()).decode(), '<REDACTED>')
    return '\n'.join('<REDACTED>' if re.search(r'password|secret|token|authorization|credential|access.?key|://[^/\s]+@',
                                              line, re.I) else line for line in text.splitlines())


def run(*args, check=True, timeout=300, input=None, expect_failure=False):
    try:
        result = subprocess.run([str(a) for a in args], cwd=ROOT, input=input, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    except subprocess.TimeoutExpired:
        # TimeoutExpired's default rendering reflects the full command argv,
        # which can contain a Secret patch. Do not propagate that exception.
        raise RuntimeError('test command timed out; argv/input withheld') from None
    if expect_failure:
        if result.returncode == 0:
            raise RuntimeError('test command unexpectedly succeeded; argv/input withheld')
    elif check and result.returncode:
        sensitive = any(re.search(r'secret|password|token', str(arg), re.I) for arg in args)
        sensitive = sensitive or (input is not None and bool(re.search(r'secret|password|token', input, re.I)))
        diagnostic = '<REDACTED>' if sensitive else redact_diagnostics(result.stdout)[-12000:]
        raise RuntimeError(f'test command exited {result.returncode}; argv/input withheld: ' + diagnostic)
    return result.stdout


def save_log(filename, text, limit=128000):
    (OUT / filename).write_bytes(redact_diagnostics(text).encode()[:limit])


def pod_evidence(pod):
    status = pod.get('status', {})
    result = {'name': pod['metadata']['name'], 'uid': pod['metadata']['uid'], 'phase': status.get('phase'),
              'containers': []}
    for kind in ('initContainerStatuses', 'containerStatuses'):
        for container in status.get(kind, [])[:8]:
            item = {key: container.get(key) for key in ('name', 'ready', 'restartCount', 'imageID')}
            for field in ('state', 'lastState'):
                item[field] = {state: {key: value for key, value in details.items()
                                      if key in ('reason', 'exitCode', 'signal', 'startedAt', 'finishedAt')}
                               for state, details in container.get(field, {}).items()}
            result['containers'].append(item)
    return result


def collect_pod_evidence():
    # A 60s total budget, <=24 Pods/namespace, <=16 container statuses/Pod,
    # <=64KiB per current/previous log. Only two owned fixture namespaces.
    # API/exec errors are best-effort diagnostics, never a new test verdict.
    deadline = time.monotonic() + 60
    errors = []
    def collect(filename, *args):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            return kube(*args, '--request-timeout=8s', timeout=min(10, remaining))
        except Exception as error:
            errors.append({'artifact': filename, 'errorType': type(error).__name__})
            return None  # never reflect argv/output/exception text
    for namespace in (NS, 'cnpg-system'):
        raw = collect(namespace + '-pods', 'get', 'pods', '-n', namespace, '-o', 'json')
        if raw is None:
            continue
        try:
            pods = json.loads(raw)['items'][:24]
            for pod in pods:
                info = pod_evidence(pod)
                prefix = namespace + '-' + info['name']
                (OUT / (prefix + '-status.json')).write_text(json.dumps(info, indent=2) + '\n')
                for container in info['containers']:
                    for previous in (False, True) if container['restartCount'] else (False,):
                        filename = prefix + '-' + container['name'] + ('-previous' if previous else '') + '.log'
                        logs = collect(filename, 'logs', info['name'], '-n', namespace, '-c', container['name'],
                                       '--tail=100', '--limit-bytes=65536', '--timestamps', *(['--previous'] if previous else []))
                        if logs is not None:
                            save_log(filename, logs, 65536)
        except Exception as error:
            errors.append({'artifact': namespace + '-pods', 'errorType': type(error).__name__})
    (OUT / 'pod-collection.json').write_text(json.dumps({'errors': errors, 'deadlineReached': time.monotonic() >= deadline}, indent=2) + '\n')


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


def image_digest(flavor, source=None):
    sha = run('git', 'rev-parse', 'HEAD').strip()
    tag = f'cnpg-backup-smoke-{flavor}:{sha}'
    run('docker', 'tag', source or f'cnpg-backup-foundation-{flavor}:test', tag)
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


def loop_diagnostics(label):
    # Kernel loop allocation is host-wide, but /dev nodes are container-local.
    # Preserve both views before provisioning and on the original failure.
    text = run('docker', 'exec', NAME + '-control-plane', 'sh', '-c',
               'ls -l /dev/loop*; losetup --list; '
               'for p in /sys/class/block/loop*/dev; do echo "$p $(cat "$p")"; done; '
               'findmnt -t ext4; df -h /var/local', check=False)
    save_log('loops-' + label + '.log', text)


def loop_device(value):
    # util-linux decorates a kernel-known loop with " (lost)" when its device
    # node is absent in this /dev namespace. Validate, don't use that display
    # suffix as a device path or invent a different/free minor.
    match = re.fullmatch(r'(/dev/loop[0-9]+)(?: \(lost\))?', value.strip())
    if not match:
        raise RuntimeError('unexpected losetup device response')
    return match.group(1)


def provision_filesystem(path, size='1G'):
    # mount -o loop relies on container udev creating new loop device nodes.
    # kind has no such udev. Ask the kernel for a free minor, expose its actual
    # sysfs major/minor, then explicitly attach and mount. Never guess a minor,
    # detach a foreign loop, or fall back to an unbounded directory.
    device = loop_device(run('docker', 'exec', NAME + '-control-plane', 'losetup', '--find'))
    script = r'''set -eu
path="$1"
device="$2"
truncate -s "$3" "$path.img"
mkfs.ext4 -q -F "$path.img"
mkdir "$path"
if [ ! -b "$device" ]; then
    numbers=$(cat "/sys/class/block/${device##*/}/dev")
    mknod "$device" b "${numbers%:*}" "${numbers#*:}"
fi
# An intervening host allocator causes a safe failure, never reassignment of
# a busy device. losetup without --detach refuses an already-associated loop.
losetup "$device" "$path.img"
echo "FINITE_FS backing=$path.img device=$device"
mount "$device" "$path"
findmnt -n -o SOURCE,FSTYPE,SIZE --target "$path"
'''
    return run('docker', 'exec', NAME + '-control-plane', 'sh', '-ec', script, 'finite-fs', path, device, size)


def bounded_workspaces():
    # Test orchestration only. kind's default local-path directories have no hard
    # capacity limit, so they MUST NOT masquerade as bounded native workspace.
    # Each test workspace is an independent ext4 filesystem on a sparse 1Gi disk.
    apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'cnpg-backup-bounded'},
           'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
    loop_diagnostics('before')
    for i in range(40):
        path = f'/var/local/cnpg-backup-work-{i}'
        try:
            evidence = provision_filesystem(path)
            save_log(f'finite-fs-{i}.log', evidence)
        except Exception:
            loop_diagnostics(f'failure-{i}')
            raise
        apply({'apiVersion': 'v1', 'kind': 'PersistentVolume', 'metadata': {'name': f'cnpg-backup-work-{i}'},
               'spec': {'capacity': {'storage': '1Gi'}, 'accessModes': ['ReadWriteOnce'], 'volumeMode': 'Filesystem',
                        'storageClassName': 'cnpg-backup-bounded', 'persistentVolumeReclaimPolicy': 'Retain', 'local': {'path': path},
                        'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [
                            {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': [NAME + '-control-plane']}]}]}}}})


def admission_ready(result):
    if 'spec.imageName:' in result and 'invalid' in result.lower():
        raise RuntimeError('permanent CNPG image admission failure: ' + result[-4000:])
    return 'serverside-applied (server dry run)' in result or 'created (server dry run)' in result


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
        kube('exec', '-n', NS, pod['metadata']['name'], '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup', 'instance', '--check-capacity')


def native_metadata_matrix(report):
    primary = None
    for pod in main_pods():
        name = pod['metadata']['name']
        standby = kube('exec', '-n', NS, name, '-c', 'postgres', '--', 'psql', '-U', 'postgres', '-d', 'postgres', '-Atqc',
                       'SELECT pg_is_in_recovery()').strip() == 't'
        result = kube('exec', '-n', NS, name, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup', 'instance', '--check-native', check=not standby)
        save_log(name + '-native-preflight.log', result)
        if standby:
            assert 'unsupported actual PostgreSQL' in result, result
        else:
            assert 'error' not in result.lower() and 'failed' not in result.lower() and 'sidecar:' not in result, result
            primary = name
    assert primary, 'no primary native authentication was tested'
    # Pinned CNPG disables ALTER SYSTEM on PG17+. Use its explicit opt-in ONLY
    # on this disposable test Cluster; no product setting, RBAC or validation
    # changes and no ad hoc edits of operator-owned files. Keep the declarative
    # summarize_wal=on so the negative exercises actual runtime drift, not just
    # admission rejection. Always reset the local fault before retiring opt-in.
    def sql(command):
        return kube('exec', '-n', NS, primary, '-c', 'postgres', '--', 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                    '-U', 'postgres', '-d', 'postgres', '-Atqc', command)
    config = json.loads(kube('get', 'cluster', 'database', '-n', NS, '-o', 'json'))['spec']['postgresql']
    original = config.get('enableAlterSystem')
    assert original in (None, False), 'fixture unexpectedly permits ALTER SYSTEM already'
    assert config['parameters']['summarize_wal'] == 'on'
    assert sql('SHOW allow_alter_system').strip() == 'off'
    assert sql('SHOW summarize_wal').strip() == 'on'
    evidence = []
    def record(stage):
        # Fixed public settings only; never serialize config files or Secrets.
        evidence.append({'stage': stage, 'pod': primary,
                         'summarize_wal': sql('SHOW summarize_wal').strip(),
                         'allow_alter_system': sql('SHOW allow_alter_system').strip()})
        (OUT / 'native-summary-fault.json').write_text(json.dumps(evidence, indent=2) + '\n')
    def enable(value):
        kube('patch', 'cluster', 'database', '-n', NS, '--type=merge', '-p',
             json.dumps({'spec': {'postgresql': {'enableAlterSystem': value}}}))
    record('baseline')
    fault_attempted = False
    try:
        enable(True)
        wait(lambda: sql('SHOW allow_alter_system').strip() == 'on', 'CNPG test-only ALTER SYSTEM opt-in')
        fault_attempted = True  # SET may succeed even if exec loses its response.
        sql("ALTER SYSTEM SET summarize_wal = 'off'")
        assert sql('SELECT pg_reload_conf()').strip() == 't'
        wait(lambda: sql('SHOW summarize_wal').strip() == 'off', 'actual disabled WAL summaries')
        assert sql('SELECT pg_is_in_recovery()').strip() == 'f', 'fault target is no longer primary'
        record('fault-observed')
        result = kube('exec', '-n', NS, primary, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup',
                      'instance', '--check-native', expect_failure=True)
        save_log('native-summary-rejection.log', result, 12000)
        assert 'unsupported actual PostgreSQL' in result, result
        assert sql('SHOW summarize_wal').strip() == 'off', 'fault cleared before rejection was observed'
    finally:
        try:
            if fault_attempted:
                sql('ALTER SYSTEM RESET summarize_wal')
                assert sql('SELECT pg_reload_conf()').strip() == 't'
                wait(lambda: sql('SHOW summarize_wal').strip() == 'on', 'restore CNPG summary settings')
        finally:
            enable(original)
            wait(lambda: sql('SHOW allow_alter_system').strip() == 'off', 'restore CNPG ALTER SYSTEM prohibition')
            record('restored')
    kube('exec', '-n', NS, primary, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup', 'instance', '--check-native')
    report['completed'].append('actual-native-streaming-replica-cert-local-SAN-auth-settings-control-layout-and-standby-rejection')


def repository_status_matrix(report):
    def status_is(kind):
        obj = json.loads(kube('get', 'repository', 'destination', '-n', NS, '-o', 'json'))
        status = obj.get('status', {})
        return status.get('observedGeneration') == obj['metadata']['generation'] and any(
            c['type'] == kind and c['status'] == 'True' for c in status.get('conditions', []))
    wait(lambda: status_is('Ready'), 'Repository observedGeneration and Ready')
    kube('patch', 'secret', 's3-auth', '-n', NS, '--type=merge', '-p', json.dumps({'data': {'secret': ''}}))
    wait(lambda: status_is('Invalid'), 'invalid credential rotation status')
    def warnings():
        return [e for e in json.loads(kube('get', 'events', '-n', NS, '-o', 'json'))['items']
                if e.get('reason') == 'ConfigurationInvalid' and e.get('involvedObject', {}).get('name') == 'destination']
    wait(lambda: len(warnings()) == 1, 'rate-limited Repository Warning')
    time.sleep(12)  # multiple bounded reconciliation passes, not the state oracle
    assert len(warnings()) == 1, 'Repository Warning storm'
    kube('patch', 'secret', 's3-auth', '-n', NS, '--type=merge', '-p', json.dumps(
        {'data': {'secret': base64.b64encode(b'disposable-test-only-secret').decode()}}))
    wait(lambda: status_is('Ready'), 'credential rotation recovery')
    report['completed'].append('real-Repository-status-generation-secret-rotation-and-Warning-throttle')


def capacity_matrix(image, report):
    for name, volume, success in [
        ('finite', {'ephemeral': {'volumeClaimTemplate': {'spec': {'accessModes': ['ReadWriteOnce'],
            'storageClassName': 'cnpg-backup-bounded', 'resources': {'requests': {'storage': '1Gi'}}}}}}, True),
        ('emptydir', {'emptyDir': {'sizeLimit': '1Gi'}}, False),
        ('localpath', {'ephemeral': {'volumeClaimTemplate': {'spec': {'accessModes': ['ReadWriteOnce'],
            'storageClassName': 'standard', 'resources': {'requests': {'storage': '1Gi'}}}}}}, False),
    ]:
        podname = 'capacity-' + name
        apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': podname, 'namespace': NS},
               'data': {'capacity.json': json.dumps([{'mount': '/cnpg-backup/work', 'limitBytes': 1 << 30, 'requiredBytes': 0}])}})
        apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': podname, 'namespace': NS}, 'spec': {
            'restartPolicy': 'Never', 'securityContext': {'runAsUser': 26, 'runAsGroup': 26, 'fsGroup': 26},
            'containers': [{'name': 'check', 'image': image, 'command': ['/usr/local/bin/cnpg-backup', 'instance', '--check-capacity'],
                'securityContext': {'runAsNonRoot': True, 'readOnlyRootFilesystem': True, 'allowPrivilegeEscalation': False,
                                    'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}},
                'volumeMounts': [{'name': 'work', 'mountPath': '/cnpg-backup/work'},
                                 {'name': 'config', 'mountPath': '/cnpg-backup/projection', 'readOnly': True}]}],
            'volumes': [{'name': 'work', **volume}, {'name': 'config', 'configMap': {'name': podname}}]}})
        def done():
            pod = json.loads(kube('get', 'pod', podname, '-n', NS, '-o', 'json'))
            return pod.get('status', {}).get('phase') in ('Succeeded', 'Failed')
        wait(done, 'actual kernel capacity check ' + name)
        pod = json.loads(kube('get', 'pod', podname, '-n', NS, '-o', 'json'))
        code = pod['status']['containerStatuses'][0]['state']['terminated']['exitCode']
        assert (code == 0) == success, f'capacity {name} returned {code}'
        logs = kube('logs', podname, '-n', NS, check=False)
        save_log(podname + '.log', logs)
        if not success:
            assert 'dedicated writable finite ext4/xfs filesystem' in logs, logs
        kube('delete', 'pod', podname, '-n', NS, '--wait=true')
        report['completed'].append('actual-capacity-' + name + ('-accepted' if success else '-rejected'))


def private_ca_rotation(discovered, report):
    apply(renderer.resource('cert-manager.io/v1', 'Certificate', 'cnpg-backup-next-ca', 'cnpg-system', spec={
        'isCA': True, 'commonName': 'next-private-ca', 'secretName': 'cnpg-backup-next-ca',
        'privateKey': {'algorithm': 'ECDSA', 'size': 256}, 'issuerRef': {'name': 'cnpg-backup-selfsigned'}}))
    apply(renderer.resource('cert-manager.io/v1', 'Issuer', 'cnpg-backup-next-ca', 'cnpg-system', spec={'ca': {'secretName': 'cnpg-backup-next-ca'}}))
    kube('wait', '-n', 'cnpg-system', '--for=condition=Ready', 'certificate/cnpg-backup-next-ca', '--timeout=180s')
    deployment = json.loads(kube('get', 'deployment', 'cnpg-backup', '-n', 'cnpg-system', '-o', 'json'))
    volumes = deployment['spec']['template']['spec']['volumes']
    tls = next(v for v in volumes if v['name'] == 'tls')['projected']
    tls['sources'].append({'secret': {'name': 'cnpg-backup-next-ca', 'items': [{'key': 'tls.crt', 'path': 'client-ca-next.crt'}]}})
    kube('patch', 'deployment', 'cnpg-backup', '-n', 'cnpg-system', '--type=merge', '-p',
         json.dumps({'spec': {'template': {'spec': {'volumes': volumes}}}}))
    kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
    wait(discovered, 'old client under overlapping private CA trust')
    for role in ('server', 'client'):
        old = json.loads(kube('get', 'secret', f'cnpg-backup-{role}-tls', '-n', 'cnpg-system', '-o', 'json'))['data']['tls.crt']
        kube('patch', 'certificate', f'cnpg-backup-{role}', '-n', 'cnpg-system', '--type=merge', '-p',
             json.dumps({'spec': {'issuerRef': {'name': 'cnpg-backup-next-ca'}}}))
        wait(lambda: json.loads(kube('get', 'secret', f'cnpg-backup-{role}-tls', '-n', 'cnpg-system', '-o', 'json'))['data']['tls.crt'] != old,
             'cert-manager ' + role + ' switches private CA')
        wait(discovered, 'actual operator discovery after ' + role + ' private CA switch')
    # Retire old client trust only after both leaves and real operator RPCs work.
    tls['sources'] = [s for s in tls['sources'] if s['secret']['name'] != 'cnpg-backup-next-ca']
    trust = next(s for s in tls['sources'] if s['secret']['name'] == 'cnpg-backup-ca')
    trust['secret']['name'] = 'cnpg-backup-next-ca'
    kube('patch', 'deployment', 'cnpg-backup', '-n', 'cnpg-system', '--type=merge', '-p',
         json.dumps({'spec': {'template': {'spec': {'volumes': volumes}}}}))
    kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
    wait(discovered, 'actual operator after old CA retirement')
    assert_placement()
    report['completed'].append('actual-operator-leaf-and-private-CA-overlap-rotation-and-retirement')


def placement_snapshot(pod):
    # Preserve the entire plugin containers, including API defaults/webhook fields,
    # all projected/target volumes, and immutable delivery identity. Do not just
    # test one serviceaccount mount: image/config changes must also await rollout.
    return {'containers': [c for c in pod['spec']['initContainers']
                           if c['name'] in ('cnpg-backup', 'cnpg-backup-socket-init')],
            'volumes': pod['spec']['volumes'],
            'annotations': {key: pod['metadata']['annotations'][key] for key in (
                'cnpg-backup.djosh34.github.io/owner', 'cnpg-backup.djosh34.github.io/config')}}


def assert_live_snapshots(before, pods):
    for pod in pods:
        uid = pod['metadata']['uid']
        if uid in before:
            assert placement_snapshot(pod) == before[uid], 'live Pod image/config/admission fields changed before CNPG replacement: ' + uid


def rollout_matrix(repository, report):
    before = {p['metadata']['uid']: placement_snapshot(p) for p in main_pods()}
    kube('patch', 'repository', 'destination', '-n', NS, '--type=merge', '-p', '{"spec":{"compression":"none"}}')
    kube('annotate', '-n', NS, 'cluster/database', 'configuration-rollout=requested', '--overwrite')
    def replaced():
        pods = main_pods()
        assert_live_snapshots(before, pods)
        return len(pods) == 2 and not set(before).intersection(p['metadata']['uid'] for p in pods)
    wait(replaced, 'CNPG-owned configuration rollout of both defaulted live Pods', 420)
    kube('wait', '-n', NS, '--for=condition=Ready', 'cluster/database', '--timeout=180s')
    assert_placement()
    before = {p['metadata']['uid']: placement_snapshot(p) for p in main_pods()}
    # A real different immutable image manifest, same tested binary. This tests
    # image rollout, not a fictional plugin release/version upgrade.
    run('docker', 'build', '-t', 'cnpg-backup-rollout:test', '-', input=
        'FROM cnpg-backup-foundation-pg18:test\nLABEL cnpg-backup.lifecycle-test=rollout\n')
    next_image = image_digest('rollout', 'cnpg-backup-rollout:test')
    manager = json.loads(kube('get', 'configmap', 'cnpg-backup-manager', '-n', 'cnpg-system', '-o', 'json'))
    config = json.loads(manager['data']['config.json']); config['image'] = next_image
    kube('patch', 'configmap', 'cnpg-backup-manager', '-n', 'cnpg-system', '--type=merge', '-p',
         json.dumps({'data': {'config.json': json.dumps(config)}}))
    kube('rollout', 'restart', '-n', 'cnpg-system', 'deployment/cnpg-backup')
    kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
    kube('annotate', '-n', NS, 'cluster/database', 'image-rollout=requested', '--overwrite')
    wait(replaced, 'CNPG-owned immutable image rollout of both instances', 420)
    kube('wait', '-n', NS, '--for=condition=Ready', 'cluster/database', '--timeout=180s')
    assert_placement()
    assert all(next(c for c in p['spec']['initContainers'] if c['name'] == 'cnpg-backup')['image'] == next_image for p in main_pods())
    before = {p['metadata']['uid']: placement_snapshot(p) for p in main_pods()}
    for i in range(3):
        kube('annotate', '-n', NS, 'cluster/database', 'post-rollout-idempotency=' + str(i), '--overwrite')
        time.sleep(5)
        assert_live_snapshots(before, main_pods())
        assert pod_uids() == sorted(before), 'defaulted live Pod caused perpetual churn'
    report['rollout_image'] = next_image
    report['completed'].append('real-defaulted-live-Pod-config-and-immutable-image-rollout-idempotency')


def recovery_placement_matrix(cluster, repository, report):
    # These are real CNPG-generated recovery Jobs and the actual CNPG binary,
    # not guardfixture's replacement command. No Restore capability is added:
    # clean cases must fail at unavailable materialization, after proving guard
    # ownership covered CNPG's mutating pre-RPC preflight. Full replay belongs G.
    for poison in ('pgdata', 'wal', 'tablespace', None):
        name = 'recover-' + (poison or 'fresh')
        target_repo = json.loads(json.dumps(repository))
        target_repo['metadata']['name'] = name
        target_repo['spec']['repositoryID'] = str(uuid.uuid5(uuid.NAMESPACE_URL, 'cnpg-backup-smoke/' + name))
        apply(target_repo)
        target = json.loads(json.dumps(cluster))
        target['metadata'] = {'name': name, 'namespace': NS, 'annotations': {'cnpg.io/reconciliationLoop': 'disabled'}}
        target['spec']['instances'] = 1
        target['spec']['plugins'][0]['parameters']['repository'] = name
        target['spec']['bootstrap'] = {'recovery': {'source': 'origin'}}
        target['spec']['externalClusters'] = [{'name': 'origin', 'plugin': {
            'name': 'cnpg-backup.djosh34.github.io', 'parameters': {'repository': 'destination'}}}]
        apply(target)
        volumes = [('pgdata', name + '-1', 'pgdata'), ('wal', name + '-1-wal', 'pg_wal'),
                   ('tablespace', name + '-1-tbs-fast-space', 'data')]
        backing = {}
        for role, claim, directory in volumes:
            available = [p for p in json.loads(kube('get', 'pv', '-o', 'json'))['items']
                         if p['spec'].get('storageClassName') == 'cnpg-backup-bounded'
                         and not p['spec'].get('claimRef') and p.get('status', {}).get('phase') == 'Available']
            assert available, 'finite PV pool exhausted; never fall back to localpath'
            pv = sorted(available, key=lambda p: p['metadata']['name'])[0]
            kube('patch', 'pv', pv['metadata']['name'], '--type=merge', '-p',
                 json.dumps({'spec': {'claimRef': {'name': claim, 'namespace': NS}}}))
            path = pv['spec']['local']['path']; backing[role] = (path, directory)
            # Isolated runner-owned backing files only. Seeding before the claim
            # mounts gives an independent oracle for CNPG's destructive preflight.
            script = 'mkdir -p "$1/$2"; printf sentinel > "$1/$2/preflight-sentinel"; chown -R 26:26 "$1"'
            if poison == role:
                script += '; mkdir -m 700 "$1/.cnpg-backup"; printf uncertain > "$1/.cnpg-backup/owner.json"; chmod 600 "$1/.cnpg-backup/owner.json"; chown -R 26:26 "$1/.cnpg-backup"'
            run('docker', 'exec', NAME + '-control-plane', 'sh', '-ec', script, 'seed-owned-fixture', path, directory)
        kube('annotate', 'cluster/' + name, '-n', NS, 'cnpg.io/reconciliationLoop-', '--overwrite')
        def terminated():
            pods = json.loads(kube('get', 'pods', '-n', NS, '-l', 'cnpg.io/cluster=' + name, '-o', 'json'))['items']
            return any(c['name'] == 'full-recovery' and 'terminated' in c.get('state', {})
                       for p in pods for c in p.get('status', {}).get('containerStatuses', []))
        wait(terminated, 'actual guarded CNPG recovery main ' + name, 240)
        pods = json.loads(kube('get', 'pods', '-n', NS, '-l', 'cnpg.io/cluster=' + name, '-o', 'json'))['items']
        job = next(p for p in pods if any(c['name'] == 'full-recovery' and 'terminated' in c.get('state', {})
                                        for c in p.get('status', {}).get('containerStatuses', [])))
        main = next(c for c in job['spec']['containers'] if c['name'] == 'full-recovery')
        assert main['command'][:6] == ['/cnpg-backup/bin/cnpg-backup', 'recovery-guard', '--', '/controller/manager', 'instance', 'restore']
        assert not job['spec'].get('hostPID') and not job['spec'].get('shareProcessNamespace')
        logs = kube('logs', job['metadata']['name'], '-n', NS, '-c', 'full-recovery', check=False)
        save_log(name + '-main.log', logs)
        (OUT / (name + '-pod.json')).write_text(json.dumps(pod_evidence(job), indent=2))
        termination = next(c for c in job['status']['containerStatuses'] if c['name'] == 'full-recovery')['state']['terminated']
        assert termination['exitCode'] != 0, name + ': full-recovery must terminate with a nonzero exit'
        if poison:
            assert 'TargetOwnershipUncertain' in logs, logs[-4000:]
            assert 'cleaning up existing' not in logs
            for path, directory in backing.values():
                run('docker', 'exec', NAME + '-control-plane', 'test', '-f', path + '/' + directory + '/preflight-sentinel')
            report['completed'].append('actual-CNPG-before-preflight-' + poison + '-poison-rejected-with-no-target-mutation')
        else:
            # This case distinguishes a working fence from a guard that never
            # admits even a fresh owner. CNPG itself deletes the invalid seeded
            # PGDATA/WAL, then reports unsupported plugin materialization.
            assert 'cleaning up existing data directory' in logs and 'cleaning up existing WAL directory' in logs, logs[-6000:]
            records = [json.loads(line) for line in logs.splitlines() if line.startswith('{')]
            assert any(record.get('level') == 'error' and record.get('msg') == 'restore error'
                       and record.get('error') == 'while restoring cluster: no plugin supports the restore job hooks capability'
                       for record in records), 'fresh recovery must fail for unsupported materialization: ' + logs[-6000:]
            for role in ('pgdata', 'wal'):
                path, directory = backing[role]
                run('docker', 'exec', NAME + '-control-plane', 'test', '!', '-e', path + '/' + directory + '/preflight-sentinel')
            for path, _ in backing.values():
                run('docker', 'exec', NAME + '-control-plane', 'test', '!', '-e', path + '/.cnpg-backup/owner.json')
            report['completed'].append('actual-fresh-Cluster-all-fresh-PVC-guard-Begin-CNPG-preflight-clean-Drain')
        # Stop real Job retries; poison markers are never removed for reuse.
        kube('annotate', 'cluster/' + name, '-n', NS, 'cnpg.io/reconciliationLoop=disabled', '--overwrite')
        kube('delete', 'cluster/' + name, '-n', NS, '--wait=true', '--timeout=120s')


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
    wal_fixture = None
    report = {'subject_sha': run('git', 'rev-parse', 'HEAD').strip(), 'inputs': LOCK, 'completed': [],
              'profile': 'real-cnpg-lifecycle-smoke', 'release_qualified': False, 'pr_d_complete': False,
              'remaining_mandatory': list(MANDATORY_SCENARIOS)}
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
        backup_smoke.bounded_capture_workspaces(sys.modules[__name__])
        manager, data = image_digest('manager'), image_digest('pg18')
        report['subject_images'] = {'manager': manager, 'pg18': data}
        install = renderer.render(manager, data, 'cnpg-system', NS, ['s3-auth', 'database-ca', 'database-replication'])
        (WORK / 'install.json').write_text(json.dumps(install))
        apply(install)
        kube('wait', '-n', 'cnpg-system', '--for=condition=Ready', 'certificate/cnpg-backup-server', 'certificate/cnpg-backup-client', '--timeout=180s')
        kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
        apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-auth', 'namespace': NS},
               'stringData': {'access': 'disposable-test-only-access', 'secret': 'disposable-test-only-secret'}})
        wal_fixture = wal_smoke.WALFixture(sys.modules[__name__], report)
        wal_storage = wal_fixture.setup()
        kube('apply', '-n', NS, '-f', ROOT / 'config/wal-monitoring.yaml')
        repository = {'apiVersion': 'backup.cnpg-backup.djosh34.github.io/v1alpha1', 'kind': 'Repository',
                      'metadata': {'name': 'destination', 'namespace': NS}, 'spec': {
                          'repositoryID': 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                          's3': {**wal_storage, 'bucket': 'test-bucket', 'prefix': 'smoke',
                                 'accessKeySecret': {'name': 's3-auth', 'key': 'access'}, 'secretKeySecret': {'name': 's3-auth', 'key': 'secret'}},
                          'workspace': {'storageClassName': 'cnpg-backup-capture', 'size': '8Gi'},
                          'native': {'maxBackupBytes': 256 * 1024**2, 'maxBootstrapWALBytes': 128 * 1024**2}}}
        apply(repository)
        observed = json.loads(kube('get', 'repository', 'destination', '-n', NS, '-o', 'json'))
        assert observed['spec']['retention'] == {'enabled': False, 'dryRun': True, 'minimumFulls': 2, 'interval': '1h'}
        changed = json.loads(json.dumps(repository)); changed['spec']['repositoryID'] = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
        rejected = kube('apply', '--server-side', '--field-manager=cnpg-backup-smoke', '-f', '-', input=json.dumps(changed), check=False)
        assert 'immutable' in rejected, 'CEL immutable repository identity was not enforced'
        cluster = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'metadata': {'name': 'database', 'namespace': NS},
                   'spec': {'instances': 2, 'imageName': LOCK['database'],
                            'monitoring': {'customQueriesConfigMap': [{'name': 'cnpg-backup-wal-monitoring', 'key': 'queries'}]},
                            'storage': {'size': '1Gi', 'storageClass': 'cnpg-backup-bounded'},
                            'walStorage': {'size': '1Gi', 'storageClass': 'cnpg-backup-bounded'},
                            'tablespaces': [{'name': 'fast_space', 'storage': {'size': '1Gi', 'storageClass': 'cnpg-backup-bounded'}}],
                            'postgresql': {'parameters': {'summarize_wal': 'on', 'wal_summary_keep_time': '14d', 'archive_timeout': '60s'}},
                            'plugins': [{'name': 'cnpg-backup.djosh34.github.io', 'isWALArchiver': True, 'parameters': {'repository': 'destination'}}]}}
        # Discovery is asynchronous in the real operator. Use its actual
        # validating admission path as the startup barrier, before creating data.
        admission_attempts = []
        def discovered():
            result = kube('apply', '--server-side', '--dry-run=server', '--field-manager=cnpg-backup-smoke', '-f', '-',
                          input=json.dumps(cluster), check=False)
            admission_attempts.append(result[-4000:])
            save_log('discovery.log', '\n'.join(admission_attempts))
            return admission_ready(result)
        wait(discovered, 'CNPG mTLS plugin discovery and validation')
        apply(cluster)
        kube('wait', '-n', NS, '--for=condition=Ready', 'cluster/database', '--timeout=360s', timeout=400)
        assert_placement()
        report['completed'].append('real-CNPG-1.30-two-instance-initdb-join-WAL-tablespace-startup-and-CRD-CEL')
        wal_fixture.segment()
        wal_fixture.fault_matrix()
        native_metadata_matrix(report)
        repository_status_matrix(report)
        backup_smoke.run(sys.modules[__name__], wal_fixture, report, data)
        capacity_matrix(data, report)
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
        wait(discovered, 'actual operator validation after server leaf rotation')
        report['completed'].append('actual-operator-server-leaf-rotation')
        private_ca_rotation(discovered, report)
        rollout_matrix(repository, report)
        recovery_placement_matrix(cluster, repository, report)
        wal_fixture.failover()
        assert not report['wal_remaining'], 'mandatory E WAL scenarios incomplete'
        kube('delete', '-n', NS, 'cluster/database', '--wait=true', '--timeout=120s')
        kube('delete', '-f', WORK / 'install.json', '--wait=true', '--timeout=120s')
        kube('delete', '-f', ROOT / 'config/repository-crd.json', '--wait=true', '--timeout=120s')
        report['completed'].append('plugin-uninstall')
        reconcile_scenarios(report)
        assert not report['remaining_mandatory'], 'mandatory lifecycle scenarios not completed'
        print('PASS real CNPG lifecycle smoke; NOT complete PR D acceptance')
    finally:
        if wal_fixture is not None:
            wal_fixture.close()
        reconcile_scenarios(report)
        (OUT / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')
        if created:
            collect_pod_evidence()
            save_log('resources.log', kube('get', 'pods,jobs,pvc,clusters', '-A', '-o', 'wide', check=False), 256000)
            save_log('events.log', kube('get', 'events', '-A', '--sort-by=.metadata.creationTimestamp', check=False), 256000)
            for namespace, name in [('cnpg-system', 'cnpg-backup'), ('cnpg-system', 'cnpg-controller-manager')]:
                save_log(name + '.log', kube('logs', '-n', namespace, 'deployment/' + name, '--all-containers', '--tail=500', check=False), 256000)
            loop_diagnostics('final')
            # Only detach loops backed by this disposable node's own files.
            run('docker', 'exec', NAME + '-control-plane', 'sh', '-c',
                'for p in /var/local/cnpg-backup-work-*.img /var/local/cnpg-backup-capture-*.img; do '
                'umount "${p%.img}" 2>/dev/null; '
                'losetup -j "$p" -O NAME --noheadings | xargs -r losetup -d; done', check=False)
            run(WORK / 'kind-linux-amd64', 'delete', 'cluster', '--name', NAME, check=False)


if __name__ == '__main__':
    main()
