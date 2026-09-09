"""Owned kind fixture, finite demand-allocated backing, and bounded disposal.

Namespaces are fixed *inside a unique node*. All kubectl calls use that node's
explicit kubeconfig. No ambient endpoint, namespace override, or external data.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import urllib.request

import cnpg_smoke
from campaign_process import Commands, CommandFailure, redact, cleanup

GIB = 1 << 30


def snapshot(path):
    stat = os.statvfs(path)
    memory = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, value, *_ = line.split()
        if key in ('MemTotal:', 'MemAvailable:'):
            memory[key[:-1]] = int(value) * 1024
    root = Path('/sys/fs/cgroup')
    membership = next((line.split('::', 1)[1] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::')), '/')
    current = root / membership.lstrip('/')
    if not current.is_dir():  # private cgroup namespace already exposes its root
        current = root
    cgroup = []
    cpus, memory_max = len(os.sched_getaffinity(0)), memory['MemTotal']
    while current.is_relative_to(root):
        values = {name: (current / name).read_text().strip() for name in
                  ('cpu.max', 'cpu.stat', 'memory.max', 'memory.current', 'memory.events', 'memory.peak') if (current / name).exists()}
        cgroup.append({'path': str(current.relative_to(root)), 'values': values})
        quota = values.get('cpu.max', 'max 100000').split()
        if quota[0] != 'max':
            cpus = min(cpus, int(quota[0]) / int(quota[1]))
        if values.get('memory.max', 'max') != 'max':
            memory_max = min(memory_max, int(values['memory.max']))
        if current == root:
            break
        current = current.parent
    return {'disk_available': stat.f_bavail * stat.f_frsize, 'inodes_available': stat.f_favail,
            'memory': memory, 'effective_memory': memory_max,
            'effective_cpus': cpus, 'cgroup': cgroup, 'uid': os.getuid(), 'groups': os.getgroups(),
            'kernel': os.uname().release,
            'pressure': {n: (Path('/proc/pressure') / n).read_text() for n in ('cpu', 'memory', 'io')}}


def resource_values(text):
    values = dict(line.split('=', 1) for line in text.splitlines())
    required = {'rss_kib', 'hwm_kib', 'memory.current', 'memory.peak', 'memory.max',
                'oom', 'oom_kill', 'blocks', 'available', 'free', 'block_size', 'native_processes'}
    if set(values) != required or any(not re.fullmatch('[0-9]+', v) for v in values.values()):
        raise CommandFailure('incomplete or unbounded product resource observation')
    v = {k: int(n) for k, n in values.items()}
    if not 0 < v['rss_kib'] <= v['hwm_kib'] or not 0 < v['memory.current'] <= v['memory.peak'] or v['memory.max'] <= 0:
        raise CommandFailure('invalid kernel resource observation')
    if not 0 <= v['available'] <= v['free'] <= v['blocks'] or v['block_size'] <= 0:
        raise CommandFailure('invalid filesystem observation')
    return {'go_rss_bytes': v['rss_kib'] * 1024, 'go_hwm_bytes': v['hwm_kib'] * 1024,
            'cgroup_current_bytes': v['memory.current'], 'cgroup_peak_bytes': v['memory.peak'],
            'cgroup_limit_bytes': v['memory.max'], 'oom': v['oom'], 'oom_kill': v['oom_kill'],
            'native_processes': v['native_processes'],
            'workspace_capacity_bytes': v['blocks'] * v['block_size'],
            'workspace_available_bytes': v['available'] * v['block_size'],
            'workspace_used_bytes': (v['blocks'] - v['free']) * v['block_size']}


def preflight(resources, path, commands):
    endpoint = os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock')
    if endpoint != 'unix:///var/run/docker.sock' or os.environ.get('DOCKER_CONTEXT', 'default') != 'default':
        raise ValueError('recipe requires the local rootful Docker socket, not an ambient remote context')
    os.environ['DOCKER_HOST'] = endpoint
    observed = snapshot(path)
    info = json.loads(commands.run('docker', 'info', '--format', '{{json .}}', timeout=30))
    observed['docker'] = {k: info.get(k) for k in ('ServerVersion', 'Driver', 'DockerRootDir', 'CgroupDriver', 'CgroupVersion', 'NCPU', 'MemTotal')}
    root = Path(info['DockerRootDir'])
    # No container/download is allowed in preflight. Inaccessible daemon storage
    # accounting is a precondition failure, not an invented shared-filesystem fact.
    observed['docker_disk_available'] = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
    errors = []
    if observed['effective_memory'] < (resources['node_memory_gib'] + resources['host_headroom_gib']) * GIB:
        errors.append('effective memory cannot support node limit plus host headroom')
    if min(observed['disk_available'], observed['docker_disk_available']) < resources['disk_start_gib'] * GIB:
        errors.append('insufficient aggregate disk headroom before provisioning')
    if observed['inodes_available'] < 100000:
        errors.append('insufficient free inodes')
    if min(observed['effective_cpus'], info['NCPU']) < resources['node_cpus']:
        errors.append('effective CPUs cannot support the declared node envelope')
    observed['errors'] = errors
    return observed


class Fixture:
    ROOT = cnpg_smoke.ROOT
    LOCK = cnpg_smoke.LOCK
    renderer = cnpg_smoke.renderer
    pod_evidence = staticmethod(cnpg_smoke.pod_evidence)
    admission_ready = staticmethod(cnpg_smoke.admission_ready)

    def __init__(self, run_dir, name, manifest, bundle, resources, deadline):
        if not re.fullmatch(r'cb-repair-[a-f0-9]{12}', name):
            raise ValueError('invalid owned fixture name')
        self.WORK, self.OUT, self.NAME, self.NS = run_dir / 'private', run_dir / 'evidence', name, 'campaign-source'
        self.WORK.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.OUT.mkdir(parents=True, exist_ok=False)
        self.commands = Commands(self.OUT, cwd=self.ROOT, deadline=deadline)
        self.run = self.commands.run
        self.m, self.bundle, self.resources = manifest, bundle, resources
        self.allocations = []
        self.last_sample = 0
        self.captures_enabled = False
        self.closed = False
        self.listener = None
        self.containers = []
        self.record_ownership()

    @classmethod
    def owned(cls, handle, manifest):
        handle = handle.resolve()
        record = json.loads(handle.read_text())
        if handle.name != 'owner.json' or record['uid'] != os.getuid() or not re.fullmatch(r'cb-repair-[a-f0-9]{12}', record['name']):
            raise ValueError('unknown fixture ownership')
        if record['work'] != str(handle.parent / 'private') or record['out'] != str(handle.parent / 'evidence'):
            raise ValueError('ownership handle path mismatch')
        for allocation in record['allocations']:
            if not re.fullmatch(r'/var/local/cnpg-backup-(?:work|capture)-[0-9]+', allocation['path']) or not re.fullmatch(r'/dev/loop[0-9]+', allocation['device']):
                raise ValueError('unowned backing in handle')
        fixture = cls.__new__(cls)
        fixture.WORK, fixture.OUT = Path(record['work']), Path(record['out'])
        fixture.NAME, fixture.NS = record['name'], 'campaign-source'
        fixture.m, fixture.resources = manifest, record['resources']
        fixture.allocations, fixture.closed = record['allocations'], record['closed']
        fixture.listener = record.get('listener')
        fixture.containers = record.get('containers', [])
        if any(not re.fullmatch(re.escape(fixture.NAME) + r'-tool-[a-f0-9]{12}', name) for name in fixture.containers):
            raise ValueError('unowned standalone container in handle')
        fixture.commands = Commands(fixture.OUT, cwd=cls.ROOT, deadline=time.monotonic() + 600)
        fixture.run = fixture.commands.run
        if fixture.container_exists(fixture.NAME + '-control-plane'):
            labels = json.loads(fixture.run('docker', 'inspect', fixture.NAME + '-control-plane', '--format', '{{json .Config.Labels}}', timeout=10)) or {}
            if labels.get('io.x-k8s.kind.cluster') != fixture.NAME:
                raise ValueError('container ownership label mismatch')
        return fixture

    def record_ownership(self):
        from recovery_campaign import atomic_json
        atomic_json(self.WORK.parent / 'owner.json', {'schema': 1, 'name': self.NAME, 'uid': os.getuid(),
                    'work': str(self.WORK), 'out': str(self.OUT), 'resources': self.resources,
                    'listener': self.listener, 'containers': self.containers,
                    'allocations': self.allocations, 'closed': self.closed})

    def kube(self, *args, **kwargs):
        return self.run(self.WORK / 'kubectl', '--kubeconfig', self.WORK / 'kubeconfig', *args,
                        operation='kubernetes/' + str(args[0]), **kwargs)

    def kube_result(self, *args, timeout=30, input=None):
        return self.commands.command('kubernetes/' + str(args[0]), self.WORK / 'kubectl', '--kubeconfig',
                                     self.WORK / 'kubeconfig', *args, timeout=timeout, input=input)

    def apply(self, document):
        return self.kube('apply', '--server-side', '--field-manager=cnpg-backup-campaign', '-f', '-', input=json.dumps(document))

    def wait(self, predicate, description, seconds=180):
        def observe():
            self.account()
            return predicate()
        try:
            return self.commands.wait(observe, description, seconds)
        except CommandFailure as error:
            # A timeout is not automatically a product failure. Attach concrete
            # scheduling/admission reasons while the failed target still exists.
            try:
                with self.commands.budget(10):
                    result = self.kube_result('get', '--raw', '/api/v1/namespaces/campaign-target/events?limit=100&fieldSelector=type%3DWarning', timeout=8)
                if result.ok:
                    data = json.loads(result.require().stdout)
                    events = data.get('items', [])
                    reasons = [{'reason': e.get('reason'), 'object': e.get('involvedObject', {}).get('name'),
                                'message': e.get('message')} for e in events if e.get('type') == 'Warning'][-12:]
                    self.save_log('last-barrier-events.json', json.dumps({'warnings': reasons, 'has_more': bool(data['metadata'].get('continue'))}))
                    error = CommandFailure(str(error) + '; observed warning reasons: ' + json.dumps(reasons))
            except Exception as diagnostic_error:
                self.commands.record({'diagnostic_unavailable': type(diagnostic_error).__name__, 'barrier': description})
            raise error

    def save_log(self, filename, text, limit=128000):
        if Path(filename).name != filename:
            raise ValueError('artifact path must be one owned filename')
        total = sum(p.stat().st_size for p in self.OUT.iterdir() if p.is_file())
        if total >= 90 << 20:
            self.m.data['artifact_cap_reached'] = True
            return
        safe = redact(text).encode()
        (self.OUT / filename).write_bytes(safe[-limit:])
        self.m.data.setdefault('artifacts', {})[filename] = {'bytes': min(len(safe), limit), 'dropped_bytes': max(0, len(safe) - limit)}
        self.m.save()

    def download(self, name):
        entry = self.LOCK[name]
        cache = Path(os.environ.get('CNPG_BUILD_CACHE', self.ROOT / '.work/tools')) / 'campaign-downloads'
        cache.mkdir(parents=True, exist_ok=True)
        saved = cache / entry['sha256']
        if not saved.exists():
            with urllib.request.urlopen(entry['url'], timeout=60) as response:
                data = response.read(128 << 20)
            if hashlib.sha256(data).hexdigest() != entry['sha256']:
                raise ValueError('download checksum mismatch: ' + name)
            saved.write_bytes(data)
        if hashlib.sha256(saved.read_bytes()).hexdigest() != entry['sha256']:
            raise ValueError('cached download checksum mismatch: ' + name)
        target = self.WORK / name
        shutil.copyfile(saved, target)
        if name in ('kind-linux-amd64', 'kubectl'):
            target.chmod(0o555)
        return target

    def container_exists(self, name):
        if name not in (self.NAME + '-control-plane', self.NAME + '-image-audit', *getattr(self, 'containers', [])):
            raise ValueError('container observation outside fixture ownership')
        names = self.run('docker', 'container', 'ls', '-a', '--filter', 'name=^/' + name + '$', '--format', '{{.Names}}', timeout=10).split()
        if names not in ([], [name]):
            raise CommandFailure('unexpected Docker container selection')
        return bool(names)  # absence ONLY after a successful list operation

    def remove_tool(self, name):
        if self.container_exists(name):
            labels = json.loads(self.run('docker', 'inspect', name, '--format', '{{json .Config.Labels}}', timeout=10)) or {}
            if labels.get('cnpg-backup-fixture') != self.NAME:
                raise CommandFailure('cleanup: standalone container ownership mismatch')
            self.run('docker', 'rm', '--force', name, timeout=30)
        if self.container_exists(name):
            raise CommandFailure('cleanup: owned standalone container remains')
        self.containers.remove(name)
        self.record_ownership()

    def tool(self, image, entry, *args, mounts=(), rejection=False):
        import uuid
        name = self.NAME + '-tool-' + uuid.uuid4().hex[:12]
        self.containers.append(name)
        self.record_ownership()  # Docker may create it even if its client fails.
        self.m.event('owned-standalone-tool', name=name, cpus=1, memory_mib=512, pids=64)
        def remove():
            # A private cleanup budget, never the expired case command budget.
            previous = self.commands
            self.commands = Commands(self.OUT, cwd=self.ROOT, deadline=time.monotonic() + 60)
            self.run = self.commands.run
            try:
                self.remove_tool(name)
            finally:
                self.commands = previous
                self.run = previous.run
        with cleanup([('container-cleanup', remove)], lambda error:
                     self.m.failure(error, 'case' if getattr(self.m, 'current_case', None) else 'setup', getattr(self.m, 'current_case', None))):
            result = self.commands.command('shell-free-verifier' if rejection else 'subject-tool',
                'docker', 'run', '--name', name, '--label', 'cnpg-backup-fixture=' + self.NAME,
                '--cpus=1', '--memory=512m', '--memory-swap=512m', '--pids-limit=64',
                '--network=none', '--read-only', '--cap-drop=ALL', '--user', str(os.getuid()),
                *mounts, '--entrypoint', entry, image, *args)
            if rejection:
                if result.timed_out or result.dropped_bytes:
                    result.require()
                # backupverify's precise native rejection, not Docker 125/126/
                # 127, SIGKILL, or a verifier invocation/manifest failure.
                if result.returncode != 2 or result.stdout.strip() != 'WAL rejected':
                    error = CommandFailure('shell-free-verifier: expected exit=2 and WAL rejected; observed exit='
                                           + str(result.returncode) + ': ' + (result.stdout + result.stderr)[-4000:])
                    error.campaign_oracle = 'shell-free-native-rejection'
                    raise error
            else:
                result.require()
            return result.stdout + result.stderr

    def create_node(self):
        # kind exposes no node resource-limit flag. Cap the owned container as
        # soon as Docker creates it, before waiting for Kubernetes bootstrap.
        # One bounded tool task, not a second campaign or scheduler framework.
        kind_commands = Commands(self.OUT / 'kind', cwd=self.ROOT, deadline=self.commands.deadline)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(kind_commands.run, self.WORK / 'kind-linux-amd64', 'create', 'cluster', '--name', self.NAME,
                '--image', self.LOCK['kindNode'], '--config', self.WORK / 'kind.json', '--kubeconfig', self.WORK / 'kubeconfig', '--wait', '120s', timeout=300)
            try:
                def exists():
                    if future.done():
                        future.result()  # fail immediately on actual provisioning error
                    return self.container_exists(self.NAME + '-control-plane')
                self.commands.wait(exists, 'owned kind container allocated for resource cap', 180)
                self.run('docker', 'update', '--cpus', str(self.resources['node_cpus']), '--memory', str(self.resources['node_memory_gib']) + 'g',
                         '--memory-swap', str(self.resources['node_memory_gib']) + 'g', self.NAME + '-control-plane')
                future.result(timeout=300)
            finally:
                kind_commands.cancelled.set()  # TERM/KILL/reap before leaving the fixture

    def image_digest(self, flavor, source=None):
        # Import the verified archive directly, avoiding host Docker tag state
        # and the classic/config versus containerd/manifest .Id ambiguity.
        image = self.bundle['images'][flavor]
        archive = self.bundle['directory'] / image['archive']
        with archive.open('rb') as stream:
            if hashlib.file_digest(stream, 'sha256').hexdigest() != self.bundle['files'][image['archive']]:
                raise ValueError('fixture archive changed before consumption')
        self.run(self.WORK / 'kind-linux-amd64', 'load', 'image-archive', '--name', self.NAME, archive)
        full = 'docker.io/library/' + image['tag']
        lines = self.run('docker', 'exec', self.NAME + '-control-plane', 'ctr', '-n', 'k8s.io', 'images', 'list').splitlines()
        digest = next(line.split()[2] for line in lines if line.split()[0] == full)
        # Verify the canonical config identity inside the consuming node. A
        # bounded single-platform index traversal also handles OCI save layouts.
        current = digest
        for _ in range(3):
            result = self.commands.command('fixture/consumed-image-manifest', 'docker', 'exec', self.NAME + '-control-plane',
                'ctr', '-n', 'k8s.io', 'content', 'get', current, timeout=10).require()
            if 'sha256:' + hashlib.sha256(result.stdout.encode()).hexdigest() != current:
                raise ValueError('consumed fixture manifest digest mismatch')
            manifest = json.loads(result.stdout)
            if 'config' in manifest:
                actual = manifest['config']['digest']
                break
            candidates = [m for m in manifest.get('manifests', []) if m.get('platform', {}).get('os') == 'linux'
                          and m.get('platform', {}).get('architecture') == 'amd64']
            if len(candidates) != 1:
                raise ValueError('ambiguous consumed fixture image platform')
            current = candidates[0]['digest']
        else:
            raise ValueError('nested fixture image index exceeds supported bound')
        if actual != image['config_digest']:
            raise ValueError('consumed fixture config differs from immutable archive')
        ref = full.split(':')[0] + '@' + digest
        self.run('docker', 'exec', self.NAME + '-control-plane', 'ctr', '-n', 'k8s.io', 'images', 'tag', full, ref)
        self.m.data.setdefault('fixture_images', {})[flavor] = {'config_digest': actual, 'manifest': ref,
            'archive_sha256': self.bundle['files'][image['archive']]}
        self.m.save()
        return ref

    def provision_filesystem(self, path, size='3G'):
        if not re.fullmatch(r'/var/local/cnpg-backup-(?:work|capture)-[0-9]+', path):
            raise ValueError('unowned filesystem path')
        device = cnpg_smoke.loop_device(self.run('docker', 'exec', self.NAME + '-control-plane', 'losetup', '--find'))
        allocation = {'path': path, 'device': device, 'size': size, 'state': 'allocating'}
        self.allocations.append(allocation)
        self.record_ownership()  # even partial allocation belongs to this run
        script = '''set -eu
path="$1"; device="$2"
test ! -e "$path.img"; test ! -e "$path"
truncate -s "$3" "$path.img"
mkfs.ext4 -q -F "$path.img"
mkdir "$path"
if [ ! -b "$device" ]; then
 numbers=$(cat "/sys/class/block/${device##*/}/dev")
 mknod "$device" b "${numbers%:*}" "${numbers#*:}"
fi
losetup "$device" "$path.img"
mount "$device" "$path"
findmnt -n -o SOURCE,FSTYPE,SIZE --target "$path"
'''
        text = self.run('docker', 'exec', self.NAME + '-control-plane', 'sh', '-ec', script, 'allocate-owned', path, device, size)
        allocation['state'] = 'mounted'
        self.record_ownership()
        return text

    def allocate(self, count, capture=False):
        storage = 'cnpg-backup-capture' if capture else 'campaign-target'
        for _ in range(count):
            self.account(force=True, maintain=False)
            index = len(self.allocations)
            path = f'/var/local/cnpg-backup-{"capture" if capture else "work"}-{index}'
            size = '8G' if capture else '3G'
            self.save_log(f'finite-fs-{index}.log', self.provision_filesystem(path, size))
            name = f'campaign-{index}'
            self.allocations[-1].update(pv=name, capture=capture)
            self.record_ownership()
            self.apply({'apiVersion': 'v1', 'kind': 'PersistentVolume', 'metadata': {'name': name},
                        'spec': {'capacity': {'storage': '8Gi' if capture else '3Gi'}, 'accessModes': ['ReadWriteOnce'],
                                 'volumeMode': 'Filesystem', 'storageClassName': storage, 'persistentVolumeReclaimPolicy': 'Retain',
                                 'local': {'path': path}, 'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [
                                     {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': [self.NAME + '-control-plane']}]}]}}}})

    def capture_workspaces(self):
        self.apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'cnpg-backup-capture'},
                    'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
        self.allocate(2, capture=True)
        self.captures_enabled = True

    def reclaim(self):
        volumes = json.loads(self.kube('get', 'pv', '-o', 'json'))['items']
        claims = json.loads(self.kube('get', 'pvc', '-A', '-o', 'json'))['items']
        pods = json.loads(self.kube('get', 'pods', '-A', '-o', 'json'))['items']
        consumers = {(p['metadata']['namespace'], v['persistentVolumeClaim']['claimName']) for p in pods
                     for v in p['spec'].get('volumes', []) if 'persistentVolumeClaim' in v}
        for allocation in self.allocations:
            if allocation['state'] != 'mounted':
                continue
            pv = next((v for v in volumes if v['metadata']['name'] == allocation.get('pv')), None)
            if pv is None:
                continue
            claim = pv['spec'].get('claimRef')
            if not claim:
                continue  # unconsumed spare
            identity = (claim['namespace'], claim['name'])
            if identity in consumers:
                continue
            pvc = next((c for c in claims if c['metadata']['uid'] == claim['uid']), None)
            if pvc is not None:
                # Live target claims are retired only by retire_target, after
                # durable closure. Generic capture claims die with their Pod.
                continue
            self.kube('delete', 'pv', allocation['pv'], '--wait=true', '--timeout=30s')
            self.retire_backing(allocation)
        available = sum(v.get('status', {}).get('phase') == 'Available' and v['spec'].get('storageClassName') == 'cnpg-backup-capture' for v in volumes)
        active = sum(a['state'] == 'mounted' and a.get('capture') for a in self.allocations)
        if active + max(0, 2 - available) > 8:
            raise CommandFailure('fixture/workspace: demand exceeds eight live finite capture filesystems')
        if available < 2:
            self.allocate(2 - available, capture=True)

    def retire_backing(self, allocation):
        path, device = allocation['path'], allocation['device']
        associated = self.run('docker', 'exec', self.NAME + '-control-plane', 'losetup', '-j', path + '.img', '-n', '-O', 'NAME').split()
        if not associated and allocation['state'] == 'allocating':
            # Failed before attachment: remove only this newly owned disposable
            # file/directory, never the intervening allocator's loop device.
            self.run('docker', 'exec', self.NAME + '-control-plane', 'sh', '-ec',
                     'rm -f -- "$1.img"; if test -d "$1"; then rmdir "$1"; fi', 'unattached-owned', path)
        else:
            if associated != [device]:
                raise CommandFailure('cleanup: finite backing association differs from ownership ledger')
            def mounts():
                text = self.run('docker', 'exec', self.NAME + '-control-plane', 'sh', '-ec',
                    'numbers=$(cat "/sys/class/block/${1##*/}/dev"); findmnt -rn -o MAJ:MIN,TARGET | awk -v d="$numbers" \'$1 == d {print $2}\'',
                    'observe-owned-mounts', device, timeout=10)
                return text.splitlines()
            if getattr(self, 'quiesced', False):
                # Whole-node disposal has stopped kubelet AND removed every CRI
                # sandbox. Kubelet cannot now unmount its residual bind mounts.
                # Remove only mounts of this exact verified backing beneath the
                # owned node's Kubernetes Pod volume roots, deepest first.
                targets = [target for target in mounts() if target != path]
                pattern = (r'/var/lib/kubelet/pods/[a-f0-9-]{36}/(?:'
                           r'volumes/kubernetes.io~local-volume/campaign-[0-9]+|'
                           r'volume-subpaths/[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+/[0-9]+)')
                if any(not re.fullmatch(pattern, target) for target in targets):
                    raise CommandFailure('cleanup: unexpected consumer mount; refusing unowned unmount')
                for target in sorted(targets, key=len, reverse=True):
                    self.run('docker', 'exec', self.NAME + '-control-plane', 'umount', '--', target, timeout=15)
                    self.m.event('owned-bind-unmounted', device=device, path=target, cri_quiesced=True)
            # Kubernetes API deletion is not an unmount oracle. CRI consumers
            # have been stopped; wait for kubelet's remaining bind mounts too.
            self.commands.wait(lambda: mounts() in ([path], []), 'no remaining consumer mounts for ' + path, 60)
            script = '''set -eu
path="$1"; device="$2"
test "$(losetup -n -O BACK-FILE "$device")" = "$path.img"
if mountpoint -q "$path"; then umount "$path"; fi
losetup -d "$device"
remaining=$(losetup -j "$path.img" -n -O NAME)
test -z "$remaining"
rm -- "$path.img"
rmdir "$path"
'''
            self.run('docker', 'exec', self.NAME + '-control-plane', 'sh', '-ec', script, 'retire-owned', path, device)
        allocation['state'] = 'retired'
        self.record_ownership()

    def stop_sandbox(self, identity):
        # Kubelet may remove an already-deleted Pod between listing and stopping
        # its sandbox. Never infer absence from a CLI error's spelling/status:
        # require a fresh successful CRI list, or retain the original failure.
        for operation in ('stopp', 'rmp'):
            result = self.commands.command('cleanup/cri-' + operation, 'docker', 'exec', self.NAME + '-control-plane',
                'crictl', operation, identity, timeout=30)
            if not result.ok:
                remaining = self.commands.command('cleanup/cri-absence-observation', 'docker', 'exec', self.NAME + '-control-plane',
                    'crictl', 'pods', '-q', timeout=10)
                if remaining.ok and not remaining.dropped_bytes and identity not in remaining.stdout.split():
                    self.m.event('owned-sandbox-already-removed', identity=identity, operation=operation, observed_absent=True)
                    return
                result.require()

    def quiesce_pods(self, namespace, uids=None):
        if namespace != 'campaign-source' and not uids:
            raise ValueError('exact target Pod UIDs required for CRI disposal')
        sandboxes = json.loads(self.run('docker', 'exec', self.NAME + '-control-plane', 'crictl', 'pods', '-o', 'json'))['items']
        for sandbox in sandboxes:
            metadata = sandbox['metadata']
            if metadata['namespace'] != namespace or (uids is not None and metadata['uid'] not in uids):
                continue
            # Only already-deleted Kubernetes Pod identities. The maintained CRI
            # stop/remove operations reap their sandboxes before filesystem disposal.
            self.stop_sandbox(sandbox['id'])

    def retire_claims(self, state):
        self.quiesce_pods('campaign-target', state['retired_pod_uids'])
        pods = json.loads(self.kube('get', 'pods', '-n', 'campaign-target', '-o', 'json'))['items']
        claims = json.loads(self.kube('get', 'pvc', '-n', 'campaign-target', '-o', 'json'))['items']
        for claim in claims:
            if claim['metadata']['uid'] not in state['pvc_uids']:
                continue
            name = claim['metadata']['name']
            if any(v.get('persistentVolumeClaim', {}).get('claimName') == name for p in pods for v in p['spec'].get('volumes', [])):
                raise CommandFailure('cleanup: target PVC still has a consumer')
            self.kube('delete', 'pvc', name, '-n', 'campaign-target', '--wait=true', '--timeout=60s')
        self.reclaim()

    def account(self, force=False, maintain=True):
        if not force and time.monotonic() - self.last_sample < 10:
            return
        self.last_sample = time.monotonic()
        observed = snapshot(self.WORK)
        observed['active_backing'] = sum(a['state'] == 'mounted' for a in self.allocations)
        if self.allocations:
            observed['node_accounting'] = self.run('docker', 'exec', self.NAME + '-control-plane', 'sh', '-c',
                'du -B1 -c /var/local/cnpg-backup-*.img 2>/dev/null | tail -n1; '
                'for f in memory.current memory.max memory.events cpu.stat; do echo "$f"; cat /sys/fs/cgroup/$f; done; '
                # Global host loop enumeration is unrelated to this fixture
                # and can stall behind another device's I/O. Owned associations
                # are already recorded/verified at allocation and retirement.
                'df -B1 -i /var/local', timeout=10)
        self.commands.record({'resources': observed})
        if observed['disk_available'] < self.resources['disk_floor_gib'] * GIB:
            raise CommandFailure('infrastructure/disk: emergency free-space floor; no new faults')
        if self.captures_enabled and maintain:
            self.reclaim()

    def product_resources(self, namespace, pod, container, workspace):
        """Required per-process/cgroup evidence, read from the owned node only.

        No exec shell in the product image, metrics-server dependency or host-RAM
        proxy for process RSS. Kernel HWM/peak complement periodic observations.
        """
        if workspace not in ('/cnpg-backup/work', '/cnpg-backup/retention'):
            raise ValueError('unsupported resource observation mount')
        document = json.loads(self.kube('get', 'pod', pod, '-n', namespace, '-o', 'json'))
        statuses = document['status'].get('containerStatuses', []) + document['status'].get('initContainerStatuses', [])
        status = next(s for s in statuses if s['name'] == container)
        if not status.get('state', {}).get('running'):
            raise CommandFailure('resource subject is not running')
        cid = status['containerID'].removeprefix('containerd://')
        if not re.fullmatch('[a-f0-9]{64}', cid):
            raise CommandFailure('missing original CRI container identity')
        inspect = json.loads(self.run('docker', 'exec', self.NAME + '-control-plane', 'crictl', 'inspect', cid, timeout=10))
        pid = int(inspect['info']['pid'])
        if pid <= 1:
            raise CommandFailure('invalid live product PID')
        text = self.run('docker', 'exec', self.NAME + '-control-plane', 'sh', '-ec',
            'p=$1; root=/proc/$p; test "$(basename "$(readlink "$root/exe")")" = cnpg-backup; '
            'awk \'/^VmRSS:/ {print "rss_kib=" $2} /^VmHWM:/ {print "hwm_kib=" $2}\' "$root/status"; '
            'cg=/sys/fs/cgroup$(awk -F: \'$1 == "0" {print $3}\' "$root/cgroup"); '
            'for pair in memory.current memory.peak memory.max; do printf "%s=" "$pair"; cat "$cg/$pair"; done; '
            'awk \'$1 == "oom" || $1 == "oom_kill" {print $1 "=" $2}\' "$cg/memory.events"; '
            'n=0; for child in $(cat "$cg/cgroup.procs"); do '
            'case "$(cat /proc/$child/comm 2>/dev/null || true)" in pg_basebackup|pg_verifybackup|pg_combinebackup|pg_waldump|pg_controldata|psql) n=$((n+1));; esac; done; '
            'printf "native_processes=%s\\n" "$n"; '
            'stat -f -c "blocks=%b\navailable=%a\nfree=%f\nblock_size=%S" "$root/root$2"',
            'product-resource-observation', str(pid), workspace, timeout=15)
        observed = resource_values(text)
        observed.update(namespace=namespace, pod_uid=document['metadata']['uid'], container_id=cid,
                        container=container, workspace=workspace, observed_epoch=time.time())
        self.commands.record({'product_resources': observed})
        return observed

    def projection_digest(self, namespace, pod, relative):
        if relative not in ('destination/accessKey', 'destination/ca.crt'):
            raise ValueError('unsupported projection observation')
        document = json.loads(self.kube('get', 'pod', pod, '-n', namespace, '-o', 'json'))
        cid = next(s['containerID'] for s in document['status']['initContainerStatuses'] if s['name'] == 'cnpg-backup').removeprefix('containerd://')
        if not re.fullmatch('[a-f0-9]{64}', cid):
            raise CommandFailure('invalid projection container identity')
        pid = int(json.loads(self.run('docker', 'exec', self.NAME + '-control-plane', 'crictl', 'inspect', cid))['info']['pid'])
        # Only a digest leaves the private projection. Never log Secret values.
        value = self.run('docker', 'exec', self.NAME + '-control-plane', 'sha256sum',
                         f'/proc/{pid}/root/cnpg-backup/projection/' + relative).split()[0]
        if not re.fullmatch('[a-f0-9]{64}', value):
            raise CommandFailure('invalid projection digest')
        return value

    def collect_events(self, namespace):
        # Paginate at the API, not kubectl's auto-aggregated list. Full campaigns
        # have MiB of managedFields/event history; only bounded useful event
        # projections belong in diagnostics, never truncated oracle JSON.
        import urllib.parse
        continuation = ''
        for page in range(50):
            query = {'limit': 100}
            if continuation:
                query['continue'] = continuation
            url = '/api/v1/namespaces/' + namespace + '/events?' + urllib.parse.urlencode(query)
            result = self.kube_result('get', '--raw', url, timeout=10).require()
            data = json.loads(result.stdout)
            events = []
            for event in data['items']:
                item = {key: event.get(key) for key in ('type', 'reason', 'message', 'count', 'firstTimestamp', 'lastTimestamp', 'eventTime', 'series', 'involvedObject')}
                message = (item.get('message') or '').encode()
                item.update(message=message[:4096].decode(errors='replace'), message_truncated=len(message) > 4096)
                events.append(item)
            continuation = data['metadata'].get('continue', '')
            self.save_log(f'{namespace}-events-{page + 1:03d}.json', json.dumps({'events': events, 'has_more': bool(continuation)}, ensure_ascii=False), 768000)
            if not continuation:
                return
        self.m.event('collection-event-cap', namespace=namespace, max_events=5000, more=True)

    def collect_logs(self, namespace, pods):
        errors = []
        for pod in pods:
            containers = pod.get('spec', {}).get('initContainers', []) + pod.get('spec', {}).get('containers', [])
            for container in containers[:16]:
                name = pod['metadata']['name']
                collector = namespace + '/' + name + '/' + container['name']
                try:
                    result = self.kube_result('logs', name, '-n', namespace, '-c', container['name'],
                                              '--tail=300', '--limit-bytes=65536', timeout=10)
                    # A failed optional read is a diagnostic, regardless of
                    # startup races, absence, transport, auth or output limits.
                    # Required log oracles use explicit kube() reads elsewhere.
                    result.require()
                    self.save_log(name + '-' + container['name'] + '.log', result.stdout + result.stderr, 65536)
                except Exception as error:
                    errors.append({'collector': collector, 'namespace': namespace, 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
        return errors

    def collect(self):
        errors = []
        def blocked(requirement):
            self.save_log('collection.json', json.dumps({'errors': [], 'status': 'blocked', 'required': requirement}))
            self.m.event('collection-blocked', requirement=requirement, caused_by=[f['id'] for f in self.m.data.get('failures', []) if f['phase'] in ('setup', 'teardown')])
            return errors
        if not (self.WORK / 'kubeconfig').is_file():
            return blocked('fixture kubeconfig')
        try:
            if not self.container_exists(self.NAME + '-control-plane'):
                return blocked('owned Kubernetes node')
            if not self.run('docker', 'exec', self.NAME + '-control-plane', 'crictl', 'pods', '-q', timeout=10).strip():
                return blocked('Kubernetes sandboxes (already stopped during disposal)')
        except Exception as error:
            # A diagnostic availability probe grants no disposal authority.
            # Still attempt independent Kubernetes reads; close() checks safety.
            errors.append({'collector': 'availability', 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
        with self.commands.budget(300):
            for namespace in ('campaign-target', 'campaign-source', 'campaign-store', 'cnpg-system'):
                try:
                    pods = json.loads(self.kube_result('get', 'pods', '-n', namespace, '-o', 'json', timeout=10).require().stdout)['items']
                except Exception as error:
                    pods = []
                    errors.append({'namespace': namespace, 'collector': 'statuses', 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
                    self.m.event('collector-blocked', collector=namespace + '/logs', reason='Pod inventory unavailable')
                else:
                    try:
                        self.save_log(namespace + '-statuses.json', json.dumps([self.pod_evidence(p) for p in pods]))
                    except Exception as error:
                        errors.append({'namespace': namespace, 'collector': 'status-artifact', 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
                try:
                    self.collect_events(namespace)
                except Exception as error:
                    errors.append({'namespace': namespace, 'collector': 'events', 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
                try:
                    errors.extend(self.collect_logs(namespace, sorted(pods, key=lambda p: p['metadata']['creationTimestamp'], reverse=True)[:24]))
                except Exception as error:
                    errors.append({'namespace': namespace, 'collector': 'logs', 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
        try:
            self.save_log('collection.json', json.dumps({'classification': 'DIAGNOSTICS', 'errors': errors}))
        except Exception as error:
            errors.append({'collector': 'collection-artifact', 'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:]})
        return errors

    def close(self):
        actions = [('container-cleanup', lambda name=name: self.remove_tool(name))
                   for name in list(getattr(self, 'containers', []))]
        actions += [('container-cleanup', self.close_audit), ('child-reap', self.close_listener), ('teardown', self.close_node)]
        report = (lambda error: self.m.failure(error, 'teardown')) if getattr(self, 'm', None) else None
        # Each independent resource is attempted even if another cleanup fails.
        # Inside close_node, backing/CRI prerequisites still fail closed.
        with cleanup(actions, report):
            pass
        if self.WORK.exists():
            shutil.rmtree(self.WORK)  # only private fixture data, never evidence
        self.closed = True
        self.record_ownership()

    def close_audit(self):
        audit = self.NAME + '-image-audit'
        if self.container_exists(audit):
            labels = json.loads(self.run('docker', 'inspect', audit, '--format', '{{json .Config.Labels}}', timeout=10)) or {}
            if labels.get('cnpg-backup-fixture') != self.NAME:
                raise CommandFailure('cleanup: audit container ownership mismatch')
            self.run('docker', 'rm', audit, timeout=30)
        if self.container_exists(audit):
            raise CommandFailure('cleanup: owned audit container remains')

    def close_listener(self):
        listener = self.listener
        if listener:
            proc = Path('/proc') / str(listener['pid'])
            if proc.exists():
                command = (proc / 'cmdline').read_bytes().split(b'\0')
                if (proc / 'stat').read_text().split()[21] != listener['start_ticks'] or str(self.WORK / 'kubeconfig').encode() not in command:
                    raise CommandFailure('cleanup: listener identity changed; refusing PID reuse')
                os.killpg(listener['pid'], signal.SIGTERM)
                end = time.monotonic() + 5
                while proc.exists() and time.monotonic() < end:
                    time.sleep(.1)
                if proc.exists():
                    os.killpg(listener['pid'], signal.SIGKILL)
    def close_node(self):
        # Stop kubelet and EVERY owned Pod sandbox before unmounting. containerd
        # remains alive to reap sandboxes, then finite filesystems are retired.
        if self.container_exists(self.NAME + '-control-plane'):
            self.run('docker', 'exec', self.NAME + '-control-plane', 'systemctl', 'stop', 'kubelet', timeout=30)
            ids = self.run('docker', 'exec', self.NAME + '-control-plane', 'crictl', 'pods', '-q', timeout=30).split()
            with cleanup([('teardown', lambda identity=identity: self.stop_sandbox(identity)) for identity in ids]):
                pass
            if self.run('docker', 'exec', self.NAME + '-control-plane', 'crictl', 'pods', '-q', timeout=30).strip():
                raise CommandFailure('cleanup: CRI sandboxes remain; no backing may be unmounted')
            self.quiesced = True
            with cleanup([('teardown', lambda allocation=allocation: self.retire_backing(allocation))
                          for allocation in self.allocations if allocation['state'] in ('mounted', 'allocating')]):
                pass
            self.run(self.WORK / 'kind-linux-amd64', 'delete', 'cluster', '--name', self.NAME, timeout=60)
            if self.container_exists(self.NAME + '-control-plane'):
                raise CommandFailure('cleanup: owned kind container remains')
        elif any(a['state'] != 'retired' for a in self.allocations):
            raise CommandFailure('cleanup: node disappeared with unverified finite backing')
