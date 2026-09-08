"""Disposable real CNPG G scenarios. Test arrangements never replace product I/O."""
import contextlib
import copy
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import time
import tarfile
import io
from types import SimpleNamespace
import uuid
import xml.etree.ElementTree as ET

import backup_smoke
import cnpg_smoke as h
import wal_smoke
from recovery_campaign import (WORK, OUT, SOURCE, TARGET, STORE, SOURCE_ID, PLUGIN,
                               check_rows, atomic_json)

BASE = [(1, 'base')]
BEFORE = BASE + [(2, 'before')]
INCLUSIVE = BEFORE + [(3, 'target')]
LATEST = INCLUSIVE + [(4, 'after')]
MAX_TARGETS = 64


def target_secret_names():
    # Manager loads its exact allowlist at startup: predeclare only the bounded
    # synthetic target identities, rather than restarting active observers.
    return ['s3-auth'] + [f'g-{i:03d}-{suffix}' for i in range(1, MAX_TARGETS + 1)
                          for suffix in ('ca', 'replication')]


VOLUMES = ('/var/lib/postgresql/data', '/var/lib/postgresql/wal',
           '/var/lib/postgresql/tablespaces/fast_space')


def lsn(text):
    hi, lo = text.split('/')
    return int(hi, 16) << 32 | int(lo, 16)


def canonical_lsn(text):
    position = lsn(text)
    return f'{position >> 32:X}/{position & 0xffffffff:X}'


def switch_witness(commit, dump):
    """Independent native-record precondition; never alter native EndLSN."""
    size = 16 << 20  # This campaign's explicitly checked source segment size.
    end = lsn(commit['bundled_wal_end_lsn'])
    segment = (end - 1) // size
    matches = re.findall(r'len \(rec/tot\):\s*24/\s*24,.*lsn: ([0-9A-F]+/[0-9A-F]+).*desc: SWITCH\s*$', dump, re.M)
    assert len(matches) == 1, 'ineffective S1 fixture: no unique real SWITCH in final bundle filename'
    start = lsn(matches[0])
    assert end % size and end <= start and start // size == segment, 'ineffective S1 fixture: SWITCH not after native EndLSN in same file'
    boundary = (segment + 1) * size
    return {'backup_uid': commit['backup_uid'], 'timeline': commit['timeline'],
            'end_lsn': commit['bundled_wal_end_lsn'], 'switch_start': matches[0],
            'replay_end_lsn': f'{boundary >> 32:X}/{boundary & 0xffffffff:X}',
            'filename': f"{commit['timeline']:08X}{segment // 256:08X}{segment % 256:08X}"}


def history_fork(text, new_timeline, source_path):
    """PostgreSQL-generated immediate-parent fork, not current write/checkpoint LSN."""
    assert len(text.encode()) <= 65536
    entries = []
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        fields = line.split(None, 2)
        assert len(fields) >= 2 and re.fullmatch(r'[0-9A-F]+/[0-9A-F]+', fields[1])
        entries.append((int(fields[0]), lsn(fields[1])))
    assert entries and [e[0] for e in entries] == [p['id'] for p in source_path], 'history does not belong to selected source ancestry'
    assert new_timeline > entries[-1][0] and len({e[0] for e in entries}) == len(entries)
    for i in range(len(entries) - 1):
        assert entries[i][1] == lsn(source_path[i + 1]['fork_lsn']), 'history source fork changed'
    return entries[-1][1]


def registry_pull_secrets():
    """Project only GHCR auth to disposable kubelet pulls, never artifacts/argv."""
    config = Path(os.environ.get('DOCKER_CONFIG', Path.home() / '.docker')) / 'config.json'
    if not config.exists():
        return []  # Public/local anonymous pulls remain supported.
    auth = json.loads(config.read_text()).get('auths', {}).get('ghcr.io', {})
    if not auth.get('auth'):
        return []
    path = WORK / 'registry-auth.json'
    with open(path, 'x', opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
        json.dump({'auths': {'ghcr.io': {'auth': auth['auth']}}}, stream)
    try:
        for namespace in ('cnpg-system', SOURCE, TARGET):
            h.kube('create', 'secret', 'generic', 'campaign-ghcr', '-n', namespace,
                   '--type=kubernetes.io/dockerconfigjson', '--from-file=.dockerconfigjson=' + str(path))
    finally:
        path.unlink()
    return [{'name': 'campaign-ghcr'}]


class Campaign:
    def __init__(self, args, manifest):
        self.args, self.m = args, manifest
        self.wal = None
        self.created = False
        self.count = 0
        self.targets = []
        self.journal = []
        self.next_pv = 0

    def event(self, event_name, **facts):
        self.m.event(event_name, **facts)

    def sql(self, namespace, pod, query, container='postgres'):
        return h.kube('exec', '-n', namespace, pod, '-c', container, '--', 'psql', '-XAtq',
                      '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'postgres', '-c', query).strip()

    def primary(self, name='database', namespace=SOURCE):
        return json.loads(h.kube('get', 'cluster', name, '-n', namespace, '-o', 'json'))['status']['currentPrimary']

    def install_namespace(self, namespace, ca):
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-auth', 'namespace': namespace},
                 'stringData': {'access': 'disposable-test-only-access', 'secret': 'disposable-test-only-secret'}})
        h.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'wal-minio-ca', 'namespace': namespace},
                 'data': {'ca.crt': ca}})

    def pool(self, count):
        # New PV/PVC identities for every target, including retry after poison.
        # Sparse backing, but actual ext4 hard ceiling. No root-disk pressure.
        for _ in range(count):
            index = self.next_pv
            self.next_pv += 1
            path = f'/var/local/cnpg-backup-work-{index}'
            h.save_log(f'finite-fs-{index}.log', h.provision_filesystem(path, '3G'))
            h.apply({'apiVersion': 'v1', 'kind': 'PersistentVolume', 'metadata': {'name': f'campaign-{index}'},
                     'spec': {'capacity': {'storage': '3Gi'}, 'accessModes': ['ReadWriteOnce'], 'volumeMode': 'Filesystem',
                              'storageClassName': 'campaign-target', 'persistentVolumeReclaimPolicy': 'Retain',
                              'local': {'path': path}, 'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [
                                  {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': [h.NAME + '-control-plane']}]}]}}}})

    def setup(self):
        if shutil.which('docker') is None:
            raise RuntimeError('Docker is required for the actual campaign; no scenarios executed')
        # Bootstrap provisions only checksum-pinned TEST tools. Deliberately do
        # not call hack/build.py, images.py, or hack/test fast here.
        h.run('python3', 'hack/bootstrap.py', timeout=900)
        cache = Path(os.environ.get('CNPG_BUILD_CACHE', h.ROOT / '.work/tools'))
        os.environ.update(PATH=str(cache / 'go/bin') + ':' + os.environ['PATH'], CGO_ENABLED='0', GOTOOLCHAIN='local',
                          GOMAXPROCS='2', GOFLAGS='-p=2', GOWORK='off', GOOS='linux', GOARCH='amd64',
                          GOTMPDIR=str(h.ROOT / '.work/tmp'), TMPDIR=str(h.ROOT / '.work/tmp'))
        Path(os.environ['TMPDIR']).mkdir(parents=True, exist_ok=True)
        for name in ('kind-linux-amd64', 'kubectl', 'cnpg-1.30.0.yaml', 'cert-manager.yaml'):
            h.download(name)
        for name in ('cnpg-1.30.0.yaml', 'cert-manager.yaml'):
            text = (WORK / name).read_text()
            for tag, digest in h.LOCK['images'].items():
                text = text.replace(tag, digest)
            (WORK / ('pinned-' + name)).write_text(text)
        (WORK / 'kind.json').write_text(json.dumps({'kind': 'Cluster', 'apiVersion': 'kind.x-k8s.io/v1alpha4',
                                                 'nodes': [{'role': 'control-plane'}]}))
        h.run(WORK / 'kind-linux-amd64', 'create', 'cluster', '--name', h.NAME, '--image', h.LOCK['kindNode'],
              '--config', WORK / 'kind.json', '--kubeconfig', WORK / 'kubeconfig', '--wait', '120s')
        self.created = True
        assert json.loads(h.kube('version', '-o', 'json'))['serverVersion']['gitVersion'] == 'v1.35.8'
        # CONSUME selected registry manifests directly. Never load rebuilt HEAD
        # under the subject name, and never mistake a Docker config ID for digest.
        for flavor, image in [('manager', self.args.manager_image), ('pg18', self.args.data_image)]:
            h.run('docker', 'pull', '--platform=linux/amd64', image)
            version = h.run('docker', 'run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL',
                            '--entrypoint=/usr/local/bin/cnpg-backup', image, 'version')
            assert 'revision=' + self.args.subject_sha + ' ' in version, 'selected image binary revision differs from subject SHA'
            # Kubelet pulls these SAME registry manifests using the scoped
            # imagePullSecret below. Never docker-save/import/re-tag a subject:
            # that can replace its manifest bytes and lose registry identity.
            self.event('consumed-subject-image', flavor=flavor, image=image, version=version.strip())
        h.kube('apply', '--server-side', '-f', WORK / 'pinned-cert-manager.yaml')
        for deploy in ('cert-manager', 'cert-manager-webhook', 'cert-manager-cainjector'):
            h.kube('rollout', 'status', '-n', 'cert-manager', 'deployment/' + deploy, '--timeout=180s')
        h.kube('apply', '--server-side', '-f', WORK / 'pinned-cnpg-1.30.0.yaml')
        h.kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-controller-manager', '--timeout=180s')
        h.kube('apply', '--server-side', '-f', h.ROOT / 'config/repository-crd.json')
        h.kube('wait', '--for=condition=Established', 'crd/repositories.backup.cnpg-backup.djosh34.github.io', '--timeout=60s')
        for ns in (SOURCE, TARGET, STORE):
            h.apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': ns, 'labels': {'campaign': ns}}})
        self.image_pull_secrets = registry_pull_secrets()
        h.apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'campaign-target'},
                 'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
        self.pool(3)
        # One fresh capture workspace per source/target identity; the local
        # ownership slice needs fewer than the full matrix, never smaller quotas.
        workspace_count = {'smoke': 16, 'ownership': 32}.get(self.args.profile, 80)
        backup_smoke.bounded_capture_workspaces(h, count=workspace_count)
        install = h.renderer.render(self.args.manager_image, self.args.data_image, 'cnpg-system', SOURCE,
                                    ['s3-auth', 'database-ca', 'database-replication'])
        target_install = h.renderer.render(self.args.manager_image, self.args.data_image, 'cnpg-system', TARGET, target_secret_names())
        install['items'] += [obj for obj in target_install['items'] if obj['metadata']['namespace'] == TARGET]
        cm = next(obj for obj in install['items'] if obj['kind'] == 'ConfigMap')
        conf = json.loads(cm['data']['config.json'])
        conf['namespaces'].append(TARGET)
        conf['secretNames'][TARGET] = target_secret_names()
        cm['data']['config.json'] = json.dumps(conf)
        for obj in install['items']:
            if obj['kind'] == 'Deployment':
                obj['spec']['template']['spec']['imagePullSecrets'] = self.image_pull_secrets
        h.apply(install)
        h.kube('wait', '-n', 'cnpg-system', '--for=condition=Ready', 'certificate/cnpg-backup-server',
               'certificate/cnpg-backup-client', '--timeout=180s')
        h.kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
        # Keep MinIO in an independently surviving namespace, not in the source
        # namespace which the disaster scenario actually deletes.
        storage_h = SimpleNamespace(**{k: v for k, v in vars(h).items() if not k.startswith('__')})
        storage_h.NS = STORE
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-auth', 'namespace': STORE},
                 'stringData': {'access': 'disposable-test-only-access', 'secret': 'disposable-test-only-secret'}})
        self.wal = wal_smoke.WALFixture(storage_h, self.m.data)
        storage = self.wal.setup()
        # WALFixture's E smoke registry is not this G campaign's requested scope.
        self.m.data.pop('wal_completed', None)
        self.m.data.pop('wal_remaining', None)
        ca = (self.wal.directory / 'ca.crt').read_text()
        for ns in (SOURCE, TARGET):
            self.install_namespace(ns, ca)
        self.repository = {'apiVersion': 'backup.cnpg-backup.djosh34.github.io/v1alpha1', 'kind': 'Repository',
                           'metadata': {'name': 'destination', 'namespace': SOURCE}, 'spec': {
                               'repositoryID': SOURCE_ID, 's3': {**storage, 'bucket': 'test-bucket', 'prefix': 'smoke',
                                   'accessKeySecret': {'name': 's3-auth', 'key': 'access'},
                                   'secretKeySecret': {'name': 's3-auth', 'key': 'secret'}},
                               'workspace': {'storageClassName': 'cnpg-backup-capture', 'size': '8Gi'},
                               'native': {'maxBackupBytes': 256 << 20, 'maxBootstrapWALBytes': 128 << 20, 'maxRestoredBytes': 512 << 20}}}
        h.apply(self.repository)
        source = copy.deepcopy(self.repository)
        source['metadata'] = {'name': 'source', 'namespace': TARGET}
        h.apply(source)
        self.cluster = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'metadata': {'name': 'database', 'namespace': SOURCE},
                        'spec': {'instances': 1, 'imageName': h.LOCK['database'],
                                 'imagePullSecrets': self.image_pull_secrets,
                                 'storage': {'size': '3Gi', 'storageClass': 'campaign-target'},
                                 'walStorage': {'size': '3Gi', 'storageClass': 'campaign-target'},
                                 'tablespaces': [{'name': 'fast_space', 'storage': {'size': '3Gi', 'storageClass': 'campaign-target'}}],
                                 'postgresql': {'parameters': {'summarize_wal': 'on', 'wal_summary_keep_time': '14d',
                                                              'archive_timeout': '60s', 'track_commit_timestamp': 'on'}},
                                 'plugins': [{'name': PLUGIN, 'isWALArchiver': True, 'parameters': {'repository': 'destination'}}]}}
        h.wait(lambda: h.admission_ready(h.kube('apply', '--server-side', '--dry-run=server', '-f', '-',
                                                input=json.dumps(self.cluster), check=False)), 'actual mTLS discovery')
        h.apply(self.cluster)
        h.kube('wait', '-n', SOURCE, '--for=condition=Ready', 'cluster/database', '--timeout=360s', timeout=400)
        self.install_observer()
        self.make_workload()

    def install_observer(self):
        d = WORK / 'observer'
        d.mkdir()
        h.run('go', 'build', '-trimpath', '-o', d / 'actor', './hack/recoveryactor', timeout=600)
        (d / 'Dockerfile').write_text('FROM scratch\nCOPY --chmod=0555 actor /actor\nENTRYPOINT ["/actor"]\n')
        h.run('docker', 'build', '--network=none', '-t', 'cnpg-backup-campaign-actor:test', d)
        image = h.image_digest('recoveryactor', 'cnpg-backup-campaign-actor:test')
        self.event('test-observer', sha256=hashlib.sha256((d / 'actor').read_bytes()).hexdigest(), image=image,
                   changes_subject_images=False, arrangement='test init copies observer over CNPG controller; original retained and executed')
        h.apply({'apiVersion': 'cert-manager.io/v1', 'kind': 'Issuer', 'metadata': {'name': 'actor', 'namespace': TARGET}, 'spec': {'selfSigned': {}}})
        h.apply({'apiVersion': 'cert-manager.io/v1', 'kind': 'Certificate', 'metadata': {'name': 'actor', 'namespace': TARGET},
                 'spec': {'secretName': 'actor-tls', 'dnsNames': ['actor.' + TARGET + '.svc'], 'issuerRef': {'name': 'actor'}}})
        h.kube('wait', '-n', TARGET, '--for=condition=Ready', 'certificate/actor', '--timeout=180s')
        h.apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'actor', 'namespace': TARGET, 'labels': {'app': 'actor'}},
                 'spec': {'containers': [{'name': 'actor', 'image': image, 'args': ['webhook'],
                                          'env': [{'name': 'ACTOR_IMAGE', 'value': image}],
                                          'resources': {'requests': {'memory': '32Mi', 'cpu': '25m'},
                                                        'limits': {'memory': '128Mi', 'cpu': '1'}},
                                          'volumeMounts': [{'name': 'tls', 'mountPath': '/tls', 'readOnly': True}]}],
                          'volumes': [{'name': 'tls', 'secret': {'secretName': 'actor-tls'}}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'actor', 'namespace': TARGET},
                 'spec': {'selector': {'app': 'actor'}, 'ports': [{'port': 443, 'targetPort': 9443}]}})
        h.kube('wait', '-n', TARGET, '--for=condition=Ready', 'pod/actor', '--timeout=120s')
        cert = json.loads(h.kube('get', 'secret', 'actor-tls', '-n', TARGET, '-o', 'json'))['data']['tls.crt']
        h.apply({'apiVersion': 'admissionregistration.k8s.io/v1', 'kind': 'MutatingWebhookConfiguration', 'metadata': {'name': 'campaign-observer'},
                 'webhooks': [{'name': 'observer.campaign.test', 'admissionReviewVersions': ['v1'], 'sideEffects': 'None',
                               'failurePolicy': 'Fail', 'timeoutSeconds': 10, 'namespaceSelector': {'matchLabels': {'campaign': TARGET}},
                               'clientConfig': {'service': {'namespace': TARGET, 'name': 'actor', 'path': '/'}, 'caBundle': cert},
                               'rules': [{'operations': ['CREATE'], 'apiGroups': [''], 'apiVersions': ['v1'], 'resources': ['pods']}]}]})

    def acknowledge(self, key, value):
        pod = self.primary()
        # Single top-level transaction; resolve ambiguous exec by querying the
        # keyed row. Never invent acknowledgment from a successful backup RPC.
        xid = self.sql(SOURCE, pod, f"INSERT INTO g_oracle VALUES ({key},'{value}') RETURNING pg_current_xact_id()::text")
        assert re.fullmatch(r'[0-9]+', xid)
        assert self.sql(SOURCE, pod, f'SELECT value FROM g_oracle WHERE id={key}') == value
        committed = self.sql(SOURCE, pod, f"SELECT pg_xact_commit_timestamp('{xid}'::xid)::text")
        after = self.sql(SOURCE, pod, 'SELECT pg_current_wal_insert_lsn()')
        record = {'id': key, 'value': value, 'xid': xid, 'commit_time': committed, 'after_commit_lsn': after}
        self.journal.append(record)
        self.m.data['workload_commit_count'] = len(self.journal)
        atomic_json(OUT / 'acknowledged-transactions.json', self.journal)
        return record

    def archive(self):
        pod = self.primary()
        segment = self.sql(SOURCE, pod, 'SELECT pg_walfile_name(pg_switch_wal())')
        h.wait(lambda: self.sql(SOURCE, pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + segment + ".done'") == '1',
               'known durable archive boundary', 120)
        return self.fetch_archive(segment)

    def fetch_archive(self, segment):
        path = WORK / segment
        self.wal.s3('GET', self.wal_key(segment), path)
        raw = path.read_bytes()
        raw = gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw
        assert len(raw) == 16 << 20
        path.write_bytes(raw)
        self.event('archived-barrier', segment=segment, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        return segment

    @staticmethod
    def wal_key(name):
        return 'smoke/v1/' + SOURCE_ID + '/wal/' + name[:8] + '/' + name

    def full(self, name):
        d = backup_smoke.definition(h, name)
        d['metadata']['namespace'] = SOURCE
        h.apply(d)
        def done():
            b = json.loads(h.kube('get', 'backup', name, '-n', SOURCE, '-o', 'json'))
            state = b.get('status', {}).get('phase')
            assert state != 'failed', 'actual full capture failed'
            return state == 'completed'
        h.wait(done, 'actual CNPG committed full', 600)
        b = json.loads(h.kube('get', 'backup', name, '-n', SOURCE, '-o', 'json'))
        uid = b['status']['backupId']
        assert uid == b['metadata']['uid']
        commit = json.loads(self.wal.s3('GET', 'smoke/v1/' + SOURCE_ID + '/backups/' + uid + '/commit.json'))
        atomic_json(OUT / (name + '-commit.json'), commit)
        self.m.data['native_backup_count'] = self.m.data.get('native_backup_count', 0) + 1
        self.m.save()
        return commit

    def make_workload(self):
        pod = self.primary()
        self.sql(SOURCE, pod, 'CREATE TABLE g_oracle(id integer PRIMARY KEY,value text) TABLESPACE fast_space; '
                             'CREATE TABLE g_drop(id integer PRIMARY KEY); INSERT INTO g_drop VALUES (71)')
        self.acknowledge(1, 'base')
        self.base = self.full('g-base')
        self.archive() # force later sentinels beyond bundled segment, not guesses.
        self.acknowledge(2, 'before')
        self.point = self.sql(SOURCE, pod, "SELECT pg_create_restore_point('g_pre_drop')")
        self.target = self.acknowledge(3, 'target')
        self.acknowledge(4, 'after')
        self.sql(SOURCE, pod, 'DROP TABLE g_drop')
        self.remote = self.archive()
        # A following archived segment makes removal of remote an interior gap,
        # not apparent new EOF at a shortened inventory frontier.
        self.sql(SOURCE, pod, 'CHECKPOINT')
        self.frontier = self.archive()
        assert lsn(self.point) > lsn(self.base['bundled_wal_end_lsn'])
        last_bundle = (lsn(self.base['bundled_wal_end_lsn']) - 1) // (16 << 20)
        assert int(self.remote[8:16], 16) * 256 + int(self.remote[16:], 16) > last_bundle, 'post-backup regression failed to cross bundle boundary'
        self.event('target-boundaries', backup_uid=self.base['backup_uid'], backup_end=self.base['bundled_wal_end_lsn'],
                   name_lsn=self.point, target=self.target, remote=self.remote, frontier=self.frontier)
        dump = h.run('docker', 'run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL',
                     '--mount', 'type=bind,source=' + str(WORK) + ',target=/fixture,readonly',
                     '--entrypoint=/usr/lib/postgresql/18/bin/pg_waldump', self.args.data_image,
                     '--path=/fixture', '--rmgr=Transaction', '--xid=' + self.target['xid'], self.remote)
        matches = re.findall(r'lsn: ([0-9A-F]+/[0-9A-F]+).*desc: COMMIT', dump)
        assert len(matches) == 1, 'independent native WAL commit-record oracle is ambiguous'
        self.target['commit_lsn_native'] = matches[0]
        # pg_waldump zero-pads its low half; selectors use the same numeric LSN
        # in canonical repository spelling. Keep the raw independent observation.
        self.target['commit_lsn'] = canonical_lsn(matches[0])
        # CNPG time syntax is RFC3339, derived from PostgreSQL commit time itself.
        import datetime
        self.target['time_rfc3339'] = datetime.datetime.fromisoformat(self.target['commit_time']).isoformat()
        atomic_json(OUT / 'acknowledged-transactions.json', self.journal)
        h.save_log('target-commit-waldump.log', dump)
        self.newest = self.full('g-newer-than-target')
        assert lsn(self.newest['stop_lsn']) > lsn(self.target['commit_lsn'])
        self.archive()
        if self.args.profile != 'smoke':
            self.capture_same_segment()

    def capture_same_segment(self):
        # Fresh PRODUCT capture, not a retained planning fixture. Native stop
        # normally appends a real SWITCH after EndLSN; observe, never assume it.
        commit = self.full('g-same-switch')
        assert self.sql(SOURCE, self.primary(), 'SHOW wal_segment_size') == '16MB'
        end = lsn(commit['bundled_wal_end_lsn'])
        segment = (end - 1) // (16 << 20)
        name = f"{commit['timeline']:08X}{segment // 256:08X}{segment % 256:08X}"
        h.wait(lambda: self.sql(SOURCE, self.primary(), "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '1',
               'actual product same-final-file archive durable', 120)
        self.fetch_archive(name)  # no extra switch or invented frontier
        dump = h.run('docker', 'run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL',
                     '--mount', 'type=bind,source=' + str(WORK) + ',target=/fixture,readonly',
                     '--entrypoint=/usr/lib/postgresql/18/bin/pg_waldump', self.args.data_image,
                     '--path=/fixture', '--rmgr=XLOG', '--start=' + commit['bundled_wal_end_lsn'], name)
        h.save_log('same-segment-switch-waldump.log', dump)
        self.same_witness = switch_witness(commit, dump)
        self.same_witness['archive_sha256'] = hashlib.sha256((WORK / name).read_bytes()).hexdigest()
        self.same_commit = commit
        atomic_json(OUT / 'same-segment-witness.json', self.same_witness)
        self.event('same-segment-real-switch-observed', **self.same_witness, original_manifest_unchanged=True)

    def pods(self, name):
        return json.loads(h.kube('get', 'pods', '-n', TARGET, '-l', 'cnpg.io/cluster=' + name, '-o', 'json'))['items']

    def file(self, pod, path, content=None, check=True):
        if content is None:
            return h.kube('exec', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'cat', path, check=check)
        return h.kube('exec', '-i', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'sh', '-ec',
                      'cat > "$1"', 'campaign-file', path, input=content, check=check)

    def barrier(self, pod, name, seconds=300):
        def observed():
            result = h.kube('exec', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'test', '-f', '/controller/campaign/' + name, check=False)
            return not result.strip()
        h.wait(observed, 'observed test boundary: ' + name, seconds)
        self.event('fault-precondition', pod=pod, barrier=name, observed=True)

    def release(self, pod, name):
        self.file(pod, '/controller/campaign/' + name, 'release\n')

    def start(self, target, hold_replay=False):
        self.m.budget()
        self.count += 1
        assert self.count <= MAX_TARGETS, 'bounded exact target/Secret allocation exhausted'
        self.m.data['restore_attempt_count'] = self.count
        name = f'g-{self.count:03d}'
        self.pool(3)
        repo = copy.deepcopy(self.repository)
        repo['metadata'] = {'name': name, 'namespace': TARGET}
        repo['spec']['repositoryID'] = str(uuid.uuid5(uuid.NAMESPACE_URL, 'campaign/' + name))
        h.apply(repo)
        c = copy.deepcopy(self.cluster)
        c['metadata'] = {'name': name, 'namespace': TARGET}
        c['spec']['bootstrap'] = {'recovery': {'source': 'origin', 'recoveryTarget': target}}
        c['spec']['externalClusters'] = [{'name': 'origin', 'plugin': {'name': PLUGIN, 'parameters': {'repository': 'source'}}}]
        c['spec']['plugins'][0]['parameters']['repository'] = name
        h.apply(c)
        def started():
            return any(any(x['name'] == 'full-recovery' and x.get('state', {}).get('running') for x in p.get('status', {}).get('containerStatuses', [])) for p in self.pods(name))
        h.wait(started, 'fresh CNPG guarded recovery Pod', 300)
        p = next(p for p in self.pods(name) if any(c['name'] == 'full-recovery' for c in p['spec']['containers']))
        pod = p['metadata']['name']
        self.barrier(pod, 'before-rpc')
        main = next(c for c in p['spec']['containers'] if c['name'] == 'full-recovery')
        assert main['command'][:6] == ['/cnpg-backup/bin/cnpg-backup', 'recovery-guard', '--', '/controller/manager', 'instance', 'restore']
        assert not p['spec'].get('hostPID') and not p['spec'].get('shareProcessNamespace')
        assert next(c for c in p['spec']['initContainers'] if c['name'] == 'cnpg-backup')['image'] == self.args.data_image
        claims = [v['persistentVolumeClaim']['claimName'] for v in p['spec']['volumes'] if 'persistentVolumeClaim' in v]
        pvc = [json.loads(h.kube('get', 'pvc', claim, '-n', TARGET, '-o', 'json')) for claim in claims]
        target_uids = {x['metadata']['uid'] for x in pvc}
        for previous in self.targets:
            assert not target_uids.intersection(previous['pvc_uids']), 'retry reused a target PVC identity'
        cluster_uid = json.loads(h.kube('get', 'cluster', name, '-n', TARGET, '-o', 'json'))['metadata']['uid']
        job_uid = next(o['uid'] for o in p['metadata']['ownerReferences'] if o.get('kind') == 'Job' and o.get('controller'))
        state = {'name': name, 'cluster_uid': cluster_uid, 'pod': pod, 'pod_uid': p['metadata']['uid'], 'job_uid': job_uid, 'pvc_uids': sorted(target_uids),
                 'repository_id': repo['spec']['repositoryID'], 'target': target}
        self.targets.append(state)
        self.event('fresh-target', **state)
        if hold_replay:
            self.release(pod, 'hold-replay')
        return state

    def plan(self, state):
        plan = json.loads(self.file(state['pod'], '/cnpg-backup/state/recovery.json'))
        assert plan['materialized'] and plan['bundled'], 'actual verified materialization absent'
        selection = plan['plan']
        assert selection['source']['repository_id'] == SOURCE_ID
        assert selection['destination_repository_id'] == state['repository_id'] != SOURCE_ID
        durable = json.loads(self.file(state['pod'], '/var/lib/postgresql/data/.cnpg-backup/' + selection['operation_id'] + '/recovery.json'))
        assert durable == plan, 'helper projection differs from durable target plan'
        atomic_json(OUT / (state['name'] + '-plan.json'), plan)
        state['plan'] = plan
        self.holds(state, 'materialized')
        return plan

    def gate(self):
        return json.loads(self.wal.s3('GET', 'smoke/v1/' + SOURCE_ID + '/gate.json'))

    def holds(self, state, stage):
        plan = state['plan']['plan']
        gate = self.gate()
        by_id = {x['id']: x for x in gate['holders']}
        assert gate['owner'] is None
        for key in ('lifetime_hold_id', 'reader_hold_id'):
            holder = by_id[plan[key]]
            assert holder['operation_id'] == plan['operation_id'] and holder['target_cluster_uid'] == plan['target_cluster_uid']
        self.event('source-protection', stage=stage, operation=plan['operation_id'], holders=list(by_id), generation=gate['generation'])

    def materialize(self, state):
        self.release(state['pod'], 'release-before')
        self.barrier(state['pod'], 'restore-response', 600)
        return self.plan(state)

    def finish(self, state, rows, drop_present=False, stable_retained=False, ordinary=False):
        pod = state['pod']
        self.release(pod, 'release-response')
        self.release(pod, 'release-replay')
        self.barrier(pod, 'cnpg-exited', 600)
        events = [json.loads(x) for x in self.file(pod, '/controller/campaign/rpc.jsonl').splitlines()]
        assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] == 0 for x in events), 'actual CNPG recovery failed'
        h.save_log(state['name'] + '-rpc.jsonl', json.dumps(events))
        state['recovery_events'] = events
        self.holds(state, 'actual-CNPG-exited-before-guard-drain')
        # Resolve/validate immutable PVC backing paths while the original Pod
        # still exists. Ordinary CNPG cleanup may remove it before the final
        # read-only marker check; disappearance is not a new mount authority.
        assert self.markers(state) == ['present'] * 3, 'live guard must own every target before shutdown'
        # CNPG automatically deletes completed Jobs. Hold ONLY its reconciliation
        # while the real Job controller completes and the original plugin watch
        # records all terminal containers. This controlled completion barrier is
        # not permission to certify disappeared/force-deleted API evidence.
        if not ordinary:
            h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET,
                   'cnpg.io/reconciliationLoop=disabled', '--overwrite')
            self.event('CNPG-cleanup-paused-before-Job-completion', cluster=state['name'])
        else:
            assert not stable_retained, 'ordinary normal-completion case requires automatic stable release'
            self.event('ordinary-CNPG-cleanup-uninterrupted', cluster=state['name'])
        self.release(pod, 'release-shutdown')
        # The ordinary observer's durable exact-UID proof survives natural
        # cleanup. Do not first require ephemeral Job/Pod LIST evidence after
        # that proof. Paused/retry cases still inspect every live API status.
        if ordinary:
            self.ordinary_completion(state)
        else:
            self.terminated(state, stable_retained=stable_retained)
        assert self.markers(state) == ['absent'] * 3, 'successful Job did not cleanly release every target marker'
        if not ordinary:
            h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET, 'cnpg.io/reconciliationLoop-', '--overwrite')
        h.kube('wait', '-n', TARGET, '--for=condition=Ready', 'cluster/' + state['name'], '--timeout=360s', timeout=400)
        primary = self.primary(state['name'], TARGET)
        actual = self.sql(TARGET, primary, "SELECT string_agg(id::text||':'||value,',' ORDER BY id) FROM g_oracle")
        check_rows(actual, rows)
        assert self.sql(TARGET, primary, 'SELECT pg_is_in_recovery()') == 'f'
        assert self.sql(TARGET, primary, "SELECT to_regclass('public.g_drop') IS NOT NULL") == ('t' if drop_present else 'f')
        if drop_present:
            assert self.sql(TARGET, primary, 'SELECT id FROM g_drop') == '71'
        assert self.sql(TARGET, primary, "SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='fast_space'") == '/var/lib/postgresql/tablespaces/fast_space/data'
        wal_path = h.kube('exec', '-n', TARGET, primary, '-c', 'postgres', '--', 'readlink', '/var/lib/postgresql/data/pgdata/pg_wal').strip()
        assert wal_path == '/var/lib/postgresql/wal/pg_wal'
        timeline = int(self.sql(TARGET, primary, 'SELECT timeline_id FROM pg_control_checkpoint()'))
        assert timeline > state['plan']['plan']['target']['timeline']
        if 'same_segment_floor' in state:
            history = h.kube('exec', '-n', TARGET, primary, '-c', 'postgres', '--', 'cat',
                             f'/var/lib/postgresql/wal/pg_wal/{timeline:08X}.history')
            state['replay_endpoint'] = history_fork(history, timeline, state['plan']['plan']['path'])
            h.save_log(state['name'] + '-promotion.history', history)
            self.event('actual-promotion-endpoint', cluster=state['name'], cluster_uid=state['cluster_uid'],
                       source_timeline=state['plan']['plan']['target']['timeline'], new_timeline=timeline,
                       history=history, history_sha256=hashlib.sha256(history.encode()).hexdigest(),
                       endpoint=state['replay_endpoint'], admitted_frontier=state['same_segment_floor'])
        self.event('recovered-SQL', cluster=state['name'], rows=actual, drop_present=drop_present, timeline=timeline, wal_path=wal_path)
        # New lineage archives through ordinary instance mode, source declaration
        # remains in Cluster. S3 source inventory must not change from that write.
        before = self.inventory('smoke/v1/' + SOURCE_ID + '/wal/')
        self.sql(TARGET, primary, 'INSERT INTO g_oracle VALUES (9001,\'destination-only\')')
        segment = self.sql(TARGET, primary, 'SELECT pg_walfile_name(pg_switch_wal())')
        key = 'smoke/v1/' + state['repository_id'] + '/wal/' + segment[:8] + '/' + segment
        h.wait(lambda: key in self.inventory('smoke/v1/' + state['repository_id'] + '/wal/'), 'new lineage archive', 120)
        if state.get('expect_promotion_partial'):
            self.verify_promotion_archive(state, primary, segment)
        assert self.inventory('smoke/v1/' + SOURCE_ID + '/wal/') == before, 'destination write changed source archive'
        self.retire_target(state)
        return state

    def verify_promotion_archive(self, state, primary, full):
        """Fresh product promotion bytes, not historical/synthetic fixture replay."""
        plan = state['plan']['plan']
        assert plan['target']['kind'] == 'immediate' and not plan['required_archive']
        size = plan['source']['wal_segment_bytes']
        selected = plan['chain'][-1]
        end = lsn(selected['bundled_wal_end_lsn'])
        assert end % size, 'partial fixture must promote inside the bundled segment'
        number = (end - 1) // size
        per_log = (1 << 32) // size
        old = f"{selected['timeline']:08X}{number // per_log:08X}{number % per_log:08X}"
        partial = old + '.partial'
        expected = state['plan']['bundled'][old]
        assert expected['Size'] == size
        assert re.fullmatch('[0-9A-F]{24}', full) and int(full[:8], 16) > selected['timeline'], 'new lineage witness is not a complete new-TLI WAL filename'
        prefix = 'smoke/v1/' + state['repository_id'] + '/wal/'
        assert state['repository_id'] != SOURCE_ID
        names = self.inventory(prefix)
        assert prefix + old[:8] + '/' + old not in names, 'promotion manufactured an old complete WAL slot'
        assert prefix + partial[:8] + '/' + partial in names, 'promotion partial not durably stored in new repository'
        directory = WORK / (state['name'] + '-promotion-archive')
        directory.mkdir(mode=0o700)
        observations = []
        for name in (partial, full):
            key = prefix + name[:8] + '/' + name
            payload, headers = directory / name, directory / (name + '.headers')
            d = self.wal.directory
            h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '30',
                  '--max-filesize', str(size + (1 << 20)), '--config', d / 'curl-private.conf',
                  '--cacert', d / 'ca.crt', '--dump-header', headers, '--output', payload,
                  'https://localhost:19000/test-bucket/' + key)
            metadata = {k.lower().strip(): v.strip() for line in headers.read_text().splitlines()
                        if ':' in line for k, v in [line.split(':', 1)]}
            stored = payload.read_bytes()
            assert metadata['x-amz-meta-cnpg-format'] == 'wal-v1'
            assert metadata['x-amz-meta-cnpg-system-id'] == plan['source']['system_identifier']
            assert metadata['x-amz-meta-cnpg-stored-sha256'] == hashlib.sha256(stored).hexdigest()
            compression = metadata['x-amz-meta-cnpg-compression']
            assert compression in ('none', 'gzip')
            if compression == 'gzip':
                with gzip.GzipFile(fileobj=io.BytesIO(stored)) as stream:
                    raw = stream.read(size + 1)  # expansion bound, including a corrupt fixture
            else:
                raw = stored
            checksum = hashlib.sha256(raw).hexdigest()
            assert len(raw) == size == int(metadata['x-amz-meta-cnpg-raw-bytes'])
            assert checksum == metadata['x-amz-meta-cnpg-raw-sha256']
            if name == partial:
                assert checksum == expected['SHA256'], 'promotion did not preserve actual verified bundled bytes'
            else:
                local = h.kube('exec', '-n', TARGET, primary, '-c', 'postgres', '--', 'sha256sum',
                               '/var/lib/postgresql/wal/pg_wal/' + name).split()[0]
                assert checksum == local, 'new timeline full archive differs from PostgreSQL file'
            observations.append({'name': name, 'key': key, 'raw_bytes': len(raw), 'raw_sha256': checksum})
        # PG's .done is observational acknowledgment, not our durability oracle;
        # independent GET/hash verification above must succeed as well.
        h.kube('exec', '-n', TARGET, primary, '-c', 'postgres', '--', 'test', '-f',
               '/var/lib/postgresql/wal/pg_wal/archive_status/' + partial + '.done')
        self.event('promotion-auxiliary-and-new-full-durable', cluster=state['name'],
                   repository_id=state['repository_id'], objects=observations,
                   required_archive=plan['required_archive'], original_full_slot_absent=True)

    def ordinary_completion(self, state):
        # Natural CNPG cleanup may have removed Job/Pod by Ready. Its original
        # exact identities were observed before starting replay; require surviving
        # durable completed proof AND actual source gate release, not readiness.
        def completed():
            op = self.operation_state(state)
            atomic_json(OUT / (state['name'] + '-ordinary-operation.json'), op)
            assert op['state'] != 'uncertain', 'ordinary cleanup lost termination evidence; source hold must remain (not a passing normal-release case)'
            return op['state'] == 'completed' and op['lifetimeReleased']
        h.wait(completed, 'uninterrupted ordinary automatic stable release', 120)
        op = self.operation_state(state)
        assert op['completedJobUID'] == state['job_uid']
        assert op['terminatedPodUIDs'] == [state['pod_uid']]
        plan = state['plan']['plan']
        assert not {plan['reader_hold_id'], plan['lifetime_hold_id']}.intersection(x['id'] for x in self.gate()['holders'])
        assert self.markers(state) == ['absent'] * 3
        self.event('ordinary-automatic-stable-release', cluster=state['name'], operation=op,
                   operator_paused=False, current_pods=[h.pod_evidence(p) for p in self.pods(state['name'])])

    def terminated(self, state, stable_retained=False):
        # Job Complete is necessary but not sufficient; enumerate ALL retry Pods
        # and every restartable init/main status, preserving actual terminations.
        def all_done():
            pods = [p for p in self.pods(state['name']) if any(c['name'] == 'full-recovery' for c in p['spec']['containers'])]
            if not pods:
                return False
            for p in pods:
                for section in ('initContainerStatuses', 'containerStatuses'):
                    statuses = p.get('status', {}).get(section, [])
                    if not statuses or any('terminated' not in c.get('state', {}) for c in statuses):
                        return False
            return True
        h.wait(all_done, 'all actual recovery/retry Pod containers terminated', 120)
        jobs = []
        def job_complete():
            nonlocal jobs
            jobs = json.loads(h.kube('get', 'jobs', '-n', TARGET, '-l', 'cnpg.io/cluster=' + state['name'], '-o', 'json'))['items']
            return any(any(c['type'] == 'Complete' and c['status'] == 'True' for c in j.get('status', {}).get('conditions', [])) for j in jobs)
        # Pod terminal status precedes the Job controller's condition update.
        # Require that later observation, rather than assert they are atomic.
        h.wait(job_complete, 'actual Job Complete after all terminal Pod statuses', 120)
        plan = state['plan']['plan']
        h.wait(lambda: plan['reader_hold_id'] not in {x['id'] for x in self.gate()['holders']},
               'original sidecar conclusively drains only its own reader', 120)
        if stable_retained:
            assert plan['lifetime_hold_id'] in {x['id'] for x in self.gate()['holders']}, 'uncertain stable hold was cleared'
            op = self.operation_state(state)
            assert op['state'] == 'uncertain' and not op['lifetimeReleased']
        else:
            h.wait(lambda: plan['lifetime_hold_id'] not in {x['id'] for x in self.gate()['holders']},
                   'uninterrupted original observer releases stable lifetime after all terminations', 120)
        state['completion_pods'] = self.pods(state['name'])
        for pod in state['completion_pods']:
            for container in ('full-recovery', 'cnpg-backup'):
                text = h.kube('logs', pod['metadata']['name'], '-n', TARGET, '-c', container,
                              '--tail=300', '--limit-bytes=65536', check=False)
                h.save_log(pod['metadata']['name'] + '-' + container + '.log', text, 65536)
        self.event('completion-evidence', cluster=state['name'], job_uids=[j['metadata']['uid'] for j in jobs],
                   pods=[h.pod_evidence(p) for p in state['completion_pods']])

    def retire_target(self, state):
        # Bound live PostgreSQL/sidecar memory. Preserve status/log evidence first;
        # PVCs and durable poison markers are NEVER cleared or reused here.
        for pod in self.pods(state['name']):
            for container in ('full-recovery', 'cnpg-backup', 'postgres'):
                if not any(c['name'] == container for c in pod['spec'].get('containers', []) + pod['spec'].get('initContainers', [])):
                    continue
                text = h.kube('logs', pod['metadata']['name'], '-n', TARGET, '-c', container, '--tail=300', '--limit-bytes=65536', check=False)
                h.save_log(pod['metadata']['name'] + '-' + container + '.log', text, 65536)
        h.kube('delete', 'cluster', state['name'], '-n', TARGET, '--wait=true', '--timeout=120s')
        h.wait(lambda: not self.pods(state['name']), 'owned target Pods stopped before next scenario', 120)
        self.event('target-retired-after-evidence', cluster=state['name'], pvc_uids=state['pvc_uids'], target_markers_removed_by_harness=False)

    def inventory(self, prefix):
        # Fixture intentionally stays below one complete 1000-item page. A
        # truncated list is a failed oracle, never an inferred continuous archive.
        tree = ET.fromstring(self.wal.s3('GET', '?list-type=2&prefix=' + prefix))
        ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        assert tree.findtext('s:IsTruncated', namespaces=ns) == 'false'
        return sorted(x.text for x in tree.findall('s:Contents/s:Key', ns))

    def restore(self, target, rows, drop_present=False, ordinary=False):
        state = self.start(target)
        self.materialize(state)
        return self.finish(state, rows, drop_present, ordinary=ordinary)

    def run(self):
        atomic_json(OUT / 'source-inventory.json', {'keys': self.inventory('smoke/v1/' + SOURCE_ID + '/')})
        # Delete catalog and namespace BEFORE any recovery. MinIO/config survive
        # in separate namespaces; no target has source connection credentials.
        with self.m.case('source-namespace-catalog-loss-S3-only'):
            # CNPG's default source Pod grace is 1800s; Kubernetes can delay
            # namespace requeue by half that estimate even after Pods are gone.
            # Wait for real finalization, never strip finalizers or infer deletion.
            h.kube('delete', 'namespace', SOURCE, '--wait=true', '--timeout=1200s', timeout=1220)
            assert not json.loads(h.kube('get', 'pods', '-n', SOURCE, '-o', 'json'))['items']
            self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)
        if self.args.profile == 'ownership':
            self.ownership()
            self.process_drain()
            self.protection()
            return  # Manifest still fails on any missing requested family.
        with self.m.case('full-latest-remote-SQL'):
            selected = self.restore({}, LATEST, ordinary=True)
            newest = getattr(self, 'same_commit', self.newest)
            assert selected['plan']['plan']['chain'][0]['backup_uid'] == newest['backup_uid']
            self.restore({'backupID': self.base['backup_uid']}, LATEST)
        with self.m.case('full-name-pre-DROP-SQL'):
            self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)
        if self.args.profile == 'smoke':
            return
        for family, field, value in [('full-time-inclusive-exclusive', 'targetTime', self.target['time_rfc3339']),
                                      ('full-LSN-inclusive-exclusive', 'targetLSN', self.target['commit_lsn']),
                                      ('full-XID-inclusive-exclusive', 'targetXID', self.target['xid'])]:
            with self.m.case(family):
                for exclusive in (False, True):
                    self.restore({'backupID': self.base['backup_uid'], field: value, 'exclusive': exclusive},
                                 BEFORE if exclusive else INCLUSIVE, True)
        with self.m.case('full-explicit-immediate'):
            self.restore({'backupID': self.base['backup_uid'], 'targetImmediate': True}, BASE, True)
        with self.m.case('newest-base-too-new'):
            state = self.restore({'targetLSN': self.target['commit_lsn']}, INCLUSIVE, True)
            assert state['plan']['plan']['chain'][0]['backup_uid'] == self.base['backup_uid']
            self.reject({'backupID': self.newest['backup_uid'], 'targetLSN': self.target['commit_lsn']}, 'too-new-base')
        with self.m.case('target-unreached'):
            self.reject({'backupID': self.base['backup_uid'], 'targetName': 'g_never_created'}, 'target-unreached', materialized=True)
        self.shell_free()
        self.bundle_fallback()
        self.same_segment()
        self.faults()
        self.ownership()
        self.process_drain()
        self.protection()
        # Reconcile the registry, including future additions. No elapsed-time,
        # modeled result or missed injection can substitute for actual coverage.
        self.m.data['unexecuted_scenarios'] = [name for name, result in self.m.data['scenarios'].items()
                                              if result['status'] != 'passed']
        self.m.save()
        assert not self.m.data['unexecuted_scenarios'], 'mandatory actual G scenario evidence incomplete'
        # Exploration is supplemental and happens ONLY after the fixed corpus.
        rng = random.Random(self.args.seed)
        for _ in range(2):
            self.m.budget()
            exclusive = bool(rng.getrandbits(1))
            self.restore({'backupID': self.base['backup_uid'], 'targetXID': self.target['xid'], 'exclusive': exclusive},
                         BEFORE if exclusive else INCLUSIVE, True)

    def reject(self, target, reason, materialized=False):
        state = self.start(target)
        if materialized:
            self.materialize(state)
            self.release(state['pod'], 'release-response')
        else:
            self.release(state['pod'], 'release-before')
            self.release(state['pod'], 'release-response')
        self.barrier(state['pod'], 'cnpg-exited', 600)
        trace = self.file(state['pod'], '/controller/campaign/rpc.jsonl')
        records = [json.loads(x) for x in trace.splitlines()]
        assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] != 0 for x in records), 'false successful recovery'
        logs = h.kube('logs', state['pod'], '-n', TARGET, '-c', 'full-recovery')
        if reason == 'target-unreached':
            assert 'recovery ended before configured recovery target was reached' in logs
        assert 'database system is ready to accept connections' not in logs or 'recovery target' in logs
        h.save_log(state['name'] + '-negative-main.log', logs)
        self.event('expected-recovery-rejection', reason=reason, trace=records)
        # Keep wrapper and guard alive for collection. Disable CNPG reconciliation
        # and forbid accidental Job retry progress; poison is never cleared.
        h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET, 'cnpg.io/reconciliationLoop=disabled', '--overwrite')
        self.retire_target(state)
        return state

    def shell_free(self):
        with self.m.case('shell-free-original-verification'):
            directory = WORK / 'original-verification'
            directory.mkdir(mode=0o700)
            exported = directory / 'subject.tar'
            name = 'cnpg-campaign-image-audit'
            h.run('docker', 'create', '--name', name, self.args.data_image, 'version')
            try:
                with exported.open('wb') as out:
                    result = subprocess.run(['docker', 'export', name], stdout=out, stderr=subprocess.PIPE, timeout=120)
                assert result.returncode == 0
                with tarfile.open(exported) as archive:
                    paths = {m.name.lstrip('./') for m in archive}
                assert not paths.intersection({'bin/sh', 'bin/bash', 'usr/bin/sh', 'usr/bin/bash', 'usr/bin/python3'})
            finally:
                h.run('docker', 'rm', name, check=False)
            tar_dir, wal_dir = directory / 'tar', directory / 'wal'
            tar_dir.mkdir()
            wal_dir.mkdir()
            commit = self.base
            prefix = 'smoke/v1/' + SOURCE_ID + '/backups/' + commit['backup_uid'] + '/attempts/' + commit['attempt_id'] + '/'
            self.wal.s3('GET', prefix + 'manifest.pg.json', tar_dir / 'backup_manifest')
            for artifact in commit['artifacts']:
                stored = directory / (str(artifact['index']) + '.stored')
                suffix = '.tar.gz' if artifact['compression'] == 'gzip' else '.tar'
                self.wal.s3('GET', prefix + 'data/' + str(artifact['index']) + suffix, stored)
                raw = gzip.decompress(stored.read_bytes()) if artifact['compression'] == 'gzip' else stored.read_bytes()
                assert hashlib.sha256(raw).hexdigest() == artifact['raw_sha256']
                filename = {'base': 'base.tar', 'wal': 'pg_wal.tar'}.get(artifact['role'], str(artifact['tablespace_oid']) + '.tar')
                (tar_dir / filename).write_bytes(raw)
                if artifact['role'] == 'wal':
                    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                        for member in archive:
                            assert member.isdir() or member.isfile()
                            assert not member.name.startswith('/') and '..' not in Path(member.name).parts
                            archive.extract(member, wal_dir, filter='data')
            driver = directory / 'verify'
            h.run('go', 'build', '-o', driver, './hack/backupverify')
            def tool(entry, *args, failure=False):
                return h.run('docker', 'run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL', '--user', str(os.getuid()),
                             '--mount', 'type=bind,source=' + str(directory) + ',target=/input,readonly', '--entrypoint', entry,
                             self.args.data_image, *args, expect_failure=failure)
            tool('/usr/lib/postgresql/18/bin/pg_verifybackup', '--exit-on-error', '--no-parse-wal', '/input/tar')
            tool('/input/verify', '/input/tar/backup_manifest', '/input/wal')
            segment = next(p for p in wal_dir.iterdir() if re.fullmatch('[0-9A-F]{24}', p.name))
            hidden = directory / 'withheld'
            segment.rename(hidden)
            try:
                tool('/input/verify', '/input/tar/backup_manifest', '/input/wal', failure=True)
            finally:
                hidden.rename(segment)
            original = segment.read_bytes()
            try:
                segment.write_bytes(bytes(8192) + original[8192:])
                tool('/input/verify', '/input/tar/backup_manifest', '/input/wal', failure=True)
            finally:
                segment.write_bytes(original)
            self.event('shell-free-original-native-verification', image=self.args.data_image,
                       original_manifest_sha256=commit['manifest_sha256'], missing_and_corrupt_range_rejected=True,
                       data_image_has_shell=False, driver_sha256=hashlib.sha256(driver.read_bytes()).hexdigest())

    def helper(self, state, name, expected):
        text = h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', 'sh', '-c',
                      '"$@"; code=$?; printf "\\nCAMPAIGN_EXIT=%s\\n" "$code"', 'capture-helper-exit',
                      '/cnpg-backup/bin/cnpg-backup', 'wal-fetch', '--plan', '/cnpg-backup/state/recovery.json',
                      '--', name, 'pg_wal/RECOVERYXLOG')
        match = re.search(r'CAMPAIGN_EXIT=([0-9]+)\s*$', text)
        assert match and int(match[1]) == expected, 'actual product helper exit differs: ' + text[-1000:]
        self.event('actual-helper-exit', pod=state['pod'], name=name, exit=expected)

    @contextlib.contextmanager
    def archive_absent(self):
        # Explicit fault actor, NOT retention or valid coordinated deletion. The
        # source namespace is gone; only this disposable synthetic bucket changes.
        directory = WORK / ('withheld-' + str(self.count))
        directory.mkdir(mode=0o700)
        keys = self.inventory('smoke/v1/' + SOURCE_ID + '/wal/')
        saved = []
        try:
            for i, key in enumerate(keys):
                payload, headers = directory / str(i), directory / (str(i) + '.headers')
                d = self.wal.directory
                h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '15',
                      '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '--dump-header', headers,
                      '--output', payload, 'https://localhost:19000/test-bucket/' + key)
                metadata = []
                for line in headers.read_text().splitlines():
                    if line.lower().startswith('x-amz-meta-'):
                        assert re.fullmatch(r'[A-Za-z0-9-]+: [A-Za-z0-9_./: -]+', line), 'unexpected test object metadata'
                        metadata += ['--header', line]
                assert metadata, 'WAL integrity metadata snapshot missing'
                saved.append((key, payload, metadata))
                self.wal.s3('DELETE', key)
            assert self.inventory('smoke/v1/' + SOURCE_ID + '/wal/') == []
            self.event('effective-archive-removal', object_count=len(saved), source_namespace_deleted=True,
                       legitimate_GC_claim=False)
            yield
        finally:
            for key, payload, metadata in saved:
                d = self.wal.directory
                h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '30',
                      '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '-X', 'PUT', '--upload-file', payload,
                      *metadata, 'https://localhost:19000/test-bucket/' + key)
            assert self.inventory('smoke/v1/' + SOURCE_ID + '/wal/') == keys

    def bundle_fallback(self):
        with self.m.case('bundle-duplicate-absent-local-fallback'):
            with self.archive_absent():
                state = self.start({'backupID': self.base['backup_uid'], 'targetImmediate': True})
                self.materialize(state)
                assert not state['plan']['plan']['required_archive'], 'immediate no-archive fixture unexpectedly has remote-required intervals'
                name = sorted(state['plan']['bundled'])[0]
                self.helper(state, name, 1) # actual archive-first miss, NOT archive success
                state['expect_promotion_partial'] = True
                self.finish(state, BASE, True)
                negative = self.start({'backupID': self.base['backup_uid'], 'targetImmediate': True})
                self.materialize(negative)
                self.release(negative['pod'], 'incorrect-all-fatal')
                self.helper(negative, name, 255)
                self.release(negative['pod'], 'release-response')
                self.barrier(negative['pod'], 'cnpg-exited', 300)
                trace = [json.loads(x) for x in self.file(negative['pod'], '/controller/campaign/rpc.jsonl').splitlines()]
                assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] != 0 for x in trace), 'all-required255 negative oracle was insensitive'
                self.event('distinguishing-all-required255-negative', trace=trace)
                self.retire_target(negative)
        with self.m.case('bundle-local-missing-fatal'):
            with self.archive_absent():
                state = self.start({'backupID': self.base['backup_uid'], 'targetImmediate': True})
                self.materialize(state)
                name = sorted(state['plan']['bundled'])[0]
                source = '/var/lib/postgresql/wal/pg_wal/' + name
                h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', 'mv', source, source + '.withheld')
                try:
                    # Both actual archive absence and local withholding survive
                    # the PG request, helper255 and terminal no-promotion proof.
                    self.replay_failure(state, name)
                    self.event('effective-local-bundle-absence', filename=name, pod=state['pod'])
                finally:
                    h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', 'mv', source + '.withheld', source)
                # The intact exit1/SQL positive is the separate fresh target
                # above, never this repaired negative attempt.
                self.retire_target(state)

    def put_fixture(self, key, path):
        d = self.wal.directory
        return h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '30',
                     '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '-X', 'PUT',
                     '--upload-file', path, 'https://localhost:19000/test-bucket/' + key)

    @contextlib.contextmanager
    def padded_same_bundle(self):
        # Controlled padding AFTER the authenticated native range only. Original
        # manifests/SQL data are unchanged. This is an explicitly labeled test
        # fixture mutation, never a claim production may rewrite immutable data.
        commit = copy.deepcopy(self.same_commit)
        d = WORK / 'same-padding'
        d.mkdir(mode=0o700)
        prefix = 'smoke/v1/' + SOURCE_ID + '/backups/' + commit['backup_uid'] + '/'
        artifact = next(a for a in commit['artifacts'] if a['role'] == 'wal')
        key = prefix + 'attempts/' + commit['attempt_id'] + '/data/' + str(artifact['index']) + ('.tar.gz' if artifact['compression'] == 'gzip' else '.tar')
        old, old_commit = d / 'original-object', d / 'original-commit'
        self.wal.s3('GET', key, old)
        self.wal.s3('GET', prefix + 'commit.json', old_commit)
        raw = gzip.decompress(old.read_bytes()) if artifact['compression'] == 'gzip' else old.read_bytes()
        end = lsn(commit['bundled_wal_end_lsn'])
        segno = (end - 1) // (16 << 20)
        name = f"{commit['timeline']:08X}{segno // 256:08X}{segno % 256:08X}"
        target_offset = end % (16 << 20)
        assert target_offset > 0 and lsn(self.same_witness['switch_start']) >= end
        output = io.BytesIO()
        found = False
        with tarfile.open(fileobj=io.BytesIO(raw)) as source, tarfile.open(fileobj=output, mode='w', format=tarfile.USTAR_FORMAT) as dest:
            for member in source:
                assert member.isfile() or member.isdir()
                content = source.extractfile(member).read() if member.isfile() else None
                if Path(member.name).name == name:
                    assert len(content) == 16 << 20
                    content = content[:target_offset] + bytes(len(content) - target_offset)
                    found = True
                dest.addfile(member, io.BytesIO(content) if content is not None else None)
        assert found, 'required same-final-segment file not in selected native bundle'
        raw = output.getvalue()
        stored = gzip.compress(raw, compresslevel=1, mtime=0) if artifact['compression'] == 'gzip' else raw
        artifact.update(raw_bytes=len(raw), raw_sha256=hashlib.sha256(raw).hexdigest(),
                        stored_bytes=len(stored), stored_sha256=hashlib.sha256(stored).hexdigest())
        changed, changed_commit = d / 'padded-object', d / 'padded-commit'
        changed.write_bytes(stored)
        atomic_json(changed_commit, commit)
        try:
            self.put_fixture(key, changed)
            self.put_fixture(prefix + 'commit.json', changed_commit)
            self.event('same-segment-padding-arranged', filename=name, end_lsn=commit['bundled_wal_end_lsn'],
                       target=self.same_witness, original_manifest_unchanged=True,
                       synthetic_padding=True, original_object_sha256=hashlib.sha256(old.read_bytes()).hexdigest(),
                       padded_object_sha256=hashlib.sha256(stored).hexdigest())
            yield commit, name
        finally:
            self.put_fixture(key, old)
            self.put_fixture(prefix + 'commit.json', old_commit)

    def same_segment_plan(self, state):
        self.materialize(state)
        plan = state['plan']['plan']
        witness = self.same_witness
        assert plan['target']['kind'] == 'latest' and plan['target']['timeline'] == witness['timeline']
        assert plan['chain'][-1]['backup_uid'] == witness['backup_uid']
        assert witness['filename'] in state['plan']['bundled']
        required = plan['required_archive']
        assert any(w['timeline'] == witness['timeline'] and lsn(w['start_lsn']) <= lsn(witness['end_lsn'])
                   and lsn(w['end_lsn']) >= lsn(witness['replay_end_lsn']) for w in required), 'actual plan does not require same-file post-EndLSN interval'
        # Actual admitted frontier, not a shortened/invented S1 plan.
        state['same_segment_floor'] = lsn(required[-1]['end_lsn'])
        self.file(state['pod'], '/controller/campaign/observe-wal', witness['filename'])
        self.event('same-segment-required-plan', cluster=state['name'], required=required, witness=witness)

    def same_endpoint_reached(self, state):
        return state['replay_endpoint'] >= max(state['same_segment_floor'], lsn(self.same_witness['replay_end_lsn']))

    def same_segment(self):
        with self.m.case('same-bundled-segment-post-EndLSN-archive-preferred'):
            with self.padded_same_bundle() as (commit, name):
                target = {'backupID': commit['backup_uid']}  # explicit-base/latest; no equality LSN target
                for control in ('healthy', 'wrong-bundle', 'healthy-after-negative'):
                    state = self.start(target)
                    self.same_segment_plan(state)
                    if control == 'wrong-bundle':
                        self.release(state['pod'], 'incorrect-bundle-success')
                    self.finish(state, LATEST)
                    trace = state['recovery_events']
                    assert any(x.get('event') == 'actual-upstream-WAL' and x['name'] == name
                               and x['sha256'] == self.same_witness['archive_sha256'] for x in trace), 'real archive delivery identity not observed during replay'
                    reached = self.same_endpoint_reached(state)
                    if control == 'wrong-bundle':
                        assert any(x.get('event') == 'deliberately-incorrect-bundle-as-archive-success' and x['name'] == name for x in trace)
                        assert not reached, 'negative bundle-as-success oracle was insensitive'
                    else:
                        assert reached, 'latest recovery did not reach actual admitted archive frontier'
                    self.event('same-segment-endpoint-oracle', control=control, reached=reached,
                               endpoint=state['replay_endpoint'], frontier=state['same_segment_floor'])
                # All faults act on this SAME required bundled filename and stay
                # active through actual PostgreSQL replay/terminal observation.
                for mode in ('missing-wal-get', 'corrupt-wal-get', 'auth-wal-get', 'reset-wal-get', 'tls-wal-get'):
                    state = self.start(target)
                    self.same_segment_plan(state)
                    self.fatal_replay(state, name, mode, 'same-segment-' + mode)
        with self.m.case('negative-controls-EOF-and-bundle-as-success'):
            # Same-segment negative above must already have passed.
            assert self.m.data['scenarios']['same-bundled-segment-post-EndLSN-archive-preferred']['status'] == 'passed'
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            self.release(state['pod'], 'incorrect-EOF')
            self.wal.control('auth-wal-get', self.remote)
            try:
                self.release(state['pod'], 'release-response')
                self.barrier(state['pod'], 'cnpg-exited', 300)
                trace = [json.loads(x) for x in self.file(state['pod'], '/controller/campaign/rpc.jsonl').splitlines()]
                assert self.wal.control()['blocked'] > 0
                assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] == 0 for x in trace), 'unsafe EOF negative failed to reach false promotion'
            finally:
                self.wal.control('')
            # Deliberately faulty proxy allowed data loss. Query the actual
            # promoted cluster: the positive independent LATEST oracle rejects.
            self.finish(state, BASE, True)
            self.event('distinguishing-EOF-negative', lost_ids=[2, 3, 4], actual_rows=BASE)

    def faults(self):
        for family, mode in [('required-corrupt-no-latest-promotion', 'corrupt-wal-get'),
                             ('required-missing-no-latest-promotion', 'missing-wal-get'),
                             ('bundle-auth-fatal', 'auth-wal-get'), ('bundle-transport-fatal', 'reset-wal-get'),
                             ('bundle-TLS-fatal', 'tls-wal-get')]:
            with self.m.case(family):
                state = self.start({'backupID': self.base['backup_uid']})
                self.materialize(state)
                requested = self.remote if family.startswith('required-') else sorted(state['plan']['bundled'])[0]
                self.fatal_replay(state, requested, mode, family)

    def fatal_replay(self, state, requested, mode, family):
        self.wal.control(mode, requested)
        try:
            self.replay_failure(state, requested)
            assert self.wal.control()['blocked'] > 0, 'ineffective fault injection is not product failure'
            self.event('effective-fatal-WAL', family=family, filename=requested, proxy=self.wal.control())
        finally:
            self.wal.control('')
        self.retire_target(state)

    def replay_failure(self, state, requested):
        self.release(state['pod'], 'release-response')
        self.barrier(state['pod'], 'cnpg-exited', 240)
        logs = h.kube('logs', state['pod'], '-n', TARGET, '-c', 'full-recovery')
        trace = self.file(state['pod'], '/controller/campaign/rpc.jsonl')
        h.save_log(state['name'] + '-fatal-main.log', logs)
        h.save_log(state['name'] + '-fatal-rpc.jsonl', trace)
        assert '255' in logs and ('FATAL' in logs or 'fatal' in logs)
        assert 'database system is ready to accept connections' not in logs, 'false latest promotion'
        events = [json.loads(x) for x in trace.splitlines()]
        assert any(x.get('event') == 'wal-request' and x['name'] == requested for x in events)
        assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] != 0 for x in events)
        self.holds(state, 'fatal-source-WAL')

    def ownership(self):
        # Real guard and actual CNPG original command, with external pauses at
        # named barriers. Same-PVC replacement may not reach even preflight.
        for family, stage in [('guard-before-RPC-same-PVC-no-mutation', 'before'),
                              ('guard-delayed-response-same-PVC-no-mutation', 'response'),
                              ('guard-replay-pause-same-PVC-no-mutation', 'replay'),
                              ('guard-shutdown-pause-same-PVC-no-mutation', 'shutdown')]:
            with self.m.case(family):
                state = self.start({'backupID': self.base['backup_uid']}, hold_replay=stage == 'replay')
                if stage != 'before':
                    self.materialize(state)
                if stage in ('replay', 'shutdown'):
                    self.release(state['pod'], 'release-response')
                    self.barrier(state['pod'], 'replay-held' if stage == 'replay' else 'cnpg-exited', 600)
                self.replacement(state)
                if stage == 'before':
                    self.materialize(state)
                self.finish(state, LATEST)
        for family, victim in [('sidecar-death-poisons', 'cnpg-backup'), ('guard-death-poisons', 'full-recovery')]:
            with self.m.case(family):
                state = self.start({'backupID': self.base['backup_uid']}, hold_replay=True)
                self.materialize(state)
                self.release(state['pod'], 'release-response')
                self.barrier(state['pod'], 'replay-held')
                observed = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
                all_status = observed['status']['containerStatuses'] + observed['status']['initContainerStatuses']
                old = next(c for c in all_status if c['name'] == victim)
                container = old['containerID'].split('://')[1]
                pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', container))['info']['pid'])
                assert pid > 1
                h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-9', str(pid))
                def killed():
                    p = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
                    statuses = p['status']['containerStatuses'] + p['status']['initContainerStatuses']
                    c = next(c for c in statuses if c['name'] == victim)
                    return any(c.get(k, {}).get('terminated', {}).get('exitCode') == 137 for k in ('state', 'lastState'))
                h.wait(killed, 'actual ancestor namespace SIGKILL exit137', 120)
                self.event('actual-process-death', victim=victim, container=container, exit=137)
                self.replacement(state, poisoned=True)
                self.holds(state, 'crashed-reader-keeps-protection')
                self.retire_target(state)
        with self.m.case('poison-fresh-Cluster-all-fresh-PVC-retry'):
            self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)

    def no_retries(self, state):
        h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET, 'cnpg.io/reconciliationLoop=disabled', '--overwrite')
        jobs = json.loads(h.kube('get', 'jobs', '-n', TARGET, '-l', 'cnpg.io/cluster=' + state['name'], '-o', 'json'))['items']
        assert jobs
        for job in jobs:
            h.kube('patch', 'job', job['metadata']['name'], '-n', TARGET, '--type=merge', '-p', '{"spec":{"backoffLimit":0}}')

    def node_pid(self, state, container):
        pod = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
        statuses = pod['status']['containerStatuses'] + pod['status']['initContainerStatuses']
        selected = next(c for c in statuses if c['name'] == container)
        cid = selected['containerID'].split('://')[1]
        pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', cid))['info']['pid'])
        assert pid > 1
        return pid, cid

    def markers(self, state):
        if 'backing_paths' not in state:
            pod = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
            volumes = {v['name']: v for v in pod['spec']['volumes']}
            main = next(c for c in pod['spec']['containers'] if c['name'] == 'full-recovery')
            paths = []
            for mount in main['volumeMounts']:
                if mount['mountPath'] not in VOLUMES:
                    continue
                claim = volumes[mount['name']]['persistentVolumeClaim']['claimName']
                pvc = json.loads(h.kube('get', 'pvc', claim, '-n', TARGET, '-o', 'json'))
                pv = json.loads(h.kube('get', 'pv', pvc['spec']['volumeName'], '-o', 'json'))
                path = pv['spec']['local']['path']
                assert re.fullmatch(r'/var/local/cnpg-backup-work-[0-9]+', path)
                paths.append(path)
            assert len(paths) == 3
            state['backing_paths'] = paths
        text = h.run('docker', 'exec', h.NAME + '-control-plane', 'sh', '-c',
                     'for p in "$@"; do if test -f "$p/.cnpg-backup/owner.json"; then echo present; else echo absent; fi; done',
                     'observe-only-owned-markers', *state['backing_paths'])
        return text.splitlines()

    def main_terminated(self, state):
        pod = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
        return any(c['name'] == 'full-recovery' and 'terminated' in c.get('state', {}) for c in pod.get('status', {}).get('containerStatuses', []))

    def process_drain(self):
        with self.m.case('detached-PG-descendants'):
            state = self.start({'backupID': self.base['backup_uid']}, hold_replay=True)
            self.materialize(state)
            self.no_retries(state)
            self.release(state['pod'], 'release-response')
            self.barrier(state['pod'], 'replay-held')
            before = json.loads(h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'stop-cnpg'))
            self.barrier(state['pod'], 'cnpg-exited')
            after = json.loads(h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'processes'))
            cnpg = next(p for p in before if p['kind'] == 'cnpg')
            orphans = [p for p in after if p['kind'] == 'postgres' and p['parent'] == 1]
            assert orphans, 'actual PostgreSQL orphan adoption not observed'
            assert any(p['group'] != cnpg['group'] or p['session'] != cnpg['session'] for p in orphans), 'detached PG process-group/session injection not established'
            assert self.markers(state) == ['present'] * 3
            self.replacement(state)
            self.event('actual-detached-PostgreSQL-descendants', before=before, adopted=orphans)
            self.release(state['pod'], 'release-shutdown')
            h.wait(lambda: self.main_terminated(state), 'guard reaps every main-namespace descendant', 60)
            assert self.markers(state) == ['absent'] * 3, 'same-sidecar clean drain failed after reaping'
            self.retire_target(state)
        with self.m.case('pending-sidecar-task-same-incarnation-drain'):
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            self.no_retries(state)
            assert self.markers(state) == ['present'] * 3
            self.wal.control('hold-wal-get-response', self.remote)
            command = [str(WORK / 'kubectl'), '--kubeconfig', str(WORK / 'kubeconfig'), 'exec', '-n', TARGET,
                       state['pod'], '-c', 'full-recovery', '--', '/cnpg-backup/bin/cnpg-backup', 'wal-fetch',
                       '--plan', '/cnpg-backup/state/recovery.json', '--', self.remote, 'pg_wal/RECOVERYXLOG']
            log_path = OUT / (state['name'] + '-pending-helper.log')
            paused = None
            with log_path.open('w') as log:
                request = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                try:
                    h.wait(lambda: self.wal.control()['blocked'] > 0, 'actual source WAL task response held', 60)
                    self.holds(state, 'WAL-reader-task-in-flight')
                    pid, cid = self.node_pid(state, 'cnpg-backup')
                    h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-STOP', str(pid))
                    paused = pid
                    self.event('same-sidecar-paused-with-pending-task', container=cid, tuple=state['plan']['tuple'], proxy=self.wal.control())
                    h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'stop-cnpg')
                    self.barrier(state['pod'], 'cnpg-exited')
                    self.release(state['pod'], 'release-shutdown')
                    time.sleep(2) # observation window, never a holder expiry/unlock oracle
                    assert self.markers(state) == ['present'] * 3, 'guard released without original sidecar drain'
                    self.wal.control('')
                    h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-CONT', str(pid))
                    paused = None
                    h.wait(lambda: self.main_terminated(state), 'same incarnation drains real pending work', 60)
                    assert self.markers(state) == ['absent'] * 3
                    reader = state['plan']['plan']['reader_hold_id']
                    assert reader not in {x['id'] for x in self.gate()['holders']}
                    assert request.wait(timeout=30) != 0, 'canceled helper unexpectedly acknowledged a pending read'
                finally:
                    if paused:
                        h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-CONT', str(paused), check=False)
                    self.wal.control('')
                    if request.poll() is None:
                        request.terminate()
                        request.wait(timeout=15)
                    h.save_log(log_path.name, log_path.read_text())
            self.retire_target(state)

    def operation_state(self, state):
        cm = json.loads(h.kube('get', 'configmap', state['name'] + '-cb-recovery', '-n', TARGET, '-o', 'json'))
        return json.loads(cm['data']['operation.json'])

    def protection(self):
        with self.m.case('stale-tuple-rejected'):
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            p = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
            before = self.target_snapshot(p)
            text = h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'stale')
            assert 'stale tuple rejected: FailedPrecondition' in text
            assert self.target_snapshot(p) == before
            self.finish(state, LATEST)
        with self.m.case('source-lifetime-and-reader-through-replay'):
            state = self.start({'backupID': self.base['backup_uid']}, hold_replay=True)
            self.wal.control('hold-artifact-get-response')
            try:
                self.release(state['pod'], 'release-before')
                h.wait(lambda: self.wal.control()['blocked'] > 0, 'source input read before materialization response', 180)
                readers = [x for x in self.gate()['holders'] if x['target_cluster_uid'] == state['cluster_uid']]
                assert {x['kind'] for x in readers} == {'restore-lifetime', 'restore-reader'}
                assert len(readers) == 2
                self.event('protected-before-source-input-read-completes', holders=readers, proxy=self.wal.control())
            finally:
                self.wal.control('')
            self.materialize(state)
            assert {x['id'] for x in readers} == {state['plan']['plan']['lifetime_hold_id'], state['plan']['plan']['reader_hold_id']}
            self.release(state['pod'], 'release-response')
            self.barrier(state['pod'], 'replay-held')
            old_holders = {x['id'] for x in self.gate()['holders']}
            h.kube('rollout', 'restart', '-n', 'cnpg-system', 'deployment/cnpg-backup')
            h.kube('rollout', 'status', '-n', 'cnpg-system', 'deployment/cnpg-backup', '--timeout=180s')
            self.holds(state, 'controller-restart-during-actual-replay')
            assert old_holders <= {x['id'] for x in self.gate()['holders']}, 'manager removed an uncertain other-reader holder'
            h.wait(lambda: self.operation_state(state)['state'] == 'uncertain', 'durable observer loss after manager restart', 180)
            self.finish(state, LATEST, stable_retained=True)
            ours = {state['plan']['plan']['lifetime_hold_id'], state['plan']['plan']['reader_hold_id']}
            assert old_holders - ours <= {x['id'] for x in self.gate()['holders']}, 'completion erased another process holder'
        with self.m.case('controller-all-Job-retry-Pods-terminated'):
            state = self.start({'backupID': self.base['backup_uid']})
            jobs = json.loads(h.kube('get', 'jobs', '-n', TARGET, '-l', 'cnpg.io/cluster=' + state['name'], '-o', 'json'))['items']
            job = next(j for j in jobs if any(o.get('uid') == j['metadata']['uid']
                       for p in self.pods(state['name']) if p['metadata']['name'] == state['pod']
                       for o in p['metadata'].get('ownerReferences', [])))
            assert job['spec']['completions'] == job['spec']['parallelism'] == 1
            h.kube('patch', 'job', job['metadata']['name'], '-n', TARGET, '--type=merge', '-p', '{"spec":{"backoffLimit":2}}')
            first_uid = state['pod_uid']
            # Fail BEFORE original CNPG preflight, then let the real guard cleanly
            # drain. No materialized files/poison need clearing on these PVCs.
            self.release(state['pod'], 'fail-before-preflight')
            self.release(state['pod'], 'release-before')
            def failed_first():
                first = next(p for p in self.pods(state['name']) if p['metadata']['uid'] == first_uid)
                return first.get('status', {}).get('phase') == 'Failed' and all(
                    'terminated' in c.get('state', {}) for c in first['status'].get('containerStatuses', []) + first['status'].get('initContainerStatuses', []))
            h.wait(failed_first, 'actual failed first attempt with all containers terminated', 180)
            first = next(p for p in self.pods(state['name']) if p['metadata']['uid'] == first_uid)
            main = next(c for c in first['status']['containerStatuses'] if c['name'] == 'full-recovery')
            assert main['state']['terminated']['exitCode'] == 42
            self.event('actual-failed-first-attempt', job_uid=job['metadata']['uid'], pod=h.pod_evidence(first))
            def retry_started():
                return any(p['metadata']['uid'] != first_uid and any(
                    c['name'] == 'full-recovery' and 'running' in c.get('state', {})
                    for c in p.get('status', {}).get('containerStatuses', [])) for p in self.pods(state['name']))
            h.wait(retry_started, 'actual Job-controller replacement after failed attempt', 180)
            retry = next(p for p in self.pods(state['name']) if p['metadata']['uid'] != first_uid)
            assert any(o.get('uid') == job['metadata']['uid'] for o in retry['metadata'].get('ownerReferences', []))
            state['pod'], state['pod_uid'] = retry['metadata']['name'], retry['metadata']['uid']
            self.barrier(state['pod'], 'before-rpc')
            self.materialize(state)
            self.holds(state, 'first-attempt-terminated-retry-reader-live')
            self.release(state['pod'], 'release-response')
            self.barrier(state['pod'], 'cnpg-exited', 600)
            self.holds(state, 'CNPG-success-but-retry-guard-still-live')
            assert self.operation_state(state)['state'] == 'active'
            self.finish(state, LATEST)
            recovery_pods = [p for p in state['completion_pods'] if any(c['name'] == 'full-recovery' for c in p['spec']['containers'])]
            assert len(recovery_pods) > 1, 'mandatory actual retry-Pod evidence disappeared'
            self.event('all-Job-attempts-terminated-before-completion-release', job_uid=job['metadata']['uid'],
                       pods=[h.pod_evidence(p) for p in recovery_pods])

    def replacement(self, state, poisoned=False):
        original = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
        replacement = copy.deepcopy(original)
        name = state['name'] + '-replacement'
        # Same immutable volume projections/PVCs, fresh Pod UID, actual guarded
        # command. Keep all init containers: /controller is a fresh emptyDir,
        # so the observer installer must run on the replacement too.
        replacement.pop('status', None)
        replacement['metadata'] = {'name': name, 'namespace': TARGET}
        replacement['spec'].pop('nodeName', None)
        replacement['spec']['restartPolicy'] = 'Never'
        # No cluster label: CNPG does not adopt or act on this negative control.
        # The admission observer still recognizes its actual full-recovery main.
        before = self.target_snapshot(original)
        h.apply(replacement)
        def stopped():
            p = json.loads(h.kube('get', 'pod', name, '-n', TARGET, '-o', 'json'))
            return any(c['name'] == 'full-recovery' and 'terminated' in c.get('state', {}) for c in p.get('status', {}).get('containerStatuses', []))
        h.wait(stopped, 'same-PVC replacement refused before preflight', 240)
        logs = h.kube('logs', name, '-n', TARGET, '-c', 'full-recovery')
        assert 'TargetOwnershipUncertain' in logs or 'TargetOwnershipBusy' in logs, logs[-2000:]
        assert 'cleaning up existing' not in logs
        assert self.target_snapshot(original) == before, 'replacement renamed/deleted/reconstructed target content'
        self.event('same-PVC-replacement-rejected', original=state['pod'], replacement=name, poisoned=poisoned, snapshot=before)
        h.save_log(name + '.log', logs)
        h.kube('delete', 'pod', name, '-n', TARGET, '--wait=true', '--timeout=120s')

    def target_snapshot(self, pod):
        # Inspect fixed disposable backing files from the node, not through the
        # possibly dead main. Include directory names/inodes and content hashes.
        volumes = {v['name']: v for v in pod['spec']['volumes']}
        main = next(c for c in pod['spec']['containers'] if c['name'] == 'full-recovery')
        result = []
        for mount in main['volumeMounts']:
            if mount['mountPath'] not in VOLUMES:
                continue
            claim = volumes[mount['name']]['persistentVolumeClaim']['claimName']
            pvc = json.loads(h.kube('get', 'pvc', claim, '-n', TARGET, '-o', 'json'))
            pv = json.loads(h.kube('get', 'pv', pvc['spec']['volumeName'], '-o', 'json'))
            path = pv['spec']['local']['path']
            assert re.fullmatch(r'/var/local/cnpg-backup-work-[0-9]+', path)
            result.append(h.run('docker', 'exec', h.NAME + '-control-plane', 'sh', '-ec',
                'cd "$1"; find . -xdev -printf "%y %p %i %s\\n" | sort; '
                'find . -xdev -type f ! -path "./lost+found/*" -exec sha256sum {} + | sort', 'snapshot-owned-target', path, timeout=120))
        assert len(result) == 3
        return [hashlib.sha256(x.encode()).hexdigest() for x in result]

    def close(self):
        if self.wal:
            self.wal.close()
        # Preserve kind for the outer/always collector. Hosted runner is
        # ephemeral; local users may remove only this recorded cluster afterward.
        self.m.save()
