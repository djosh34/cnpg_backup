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
import time
import tarfile
import io
import uuid
import xml.etree.ElementTree as ET

from campaign_process import cleanup, CommandFailure
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


def registry_pull_secrets(h=h):
    WORK = h.WORK
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
    def __init__(self, args, manifest, fixture=None):
        self.h = fixture or h
        self.args, self.m = args, manifest
        self.wal = None
        self.count = 0
        self.targets = []
        self.journal = []

    def record_failure(self, error):
        if self.m is not None:
            return self.m.failure(error, 'case', getattr(self.m, 'current_case', None))

    def cleanup(self, actions):
        return cleanup(actions, self.record_failure)

    def event(self, event_name, **facts):
        h = self.h
        self.m.event(event_name, **facts)

    def sql(self, namespace, pod, query, container='postgres'):
        h = self.h
        return h.kube('exec', '-n', namespace, pod, '-c', container, '--', 'psql', '-XAtq',
                      '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'postgres', '-c', query).strip()

    def primary(self, name='database', namespace=SOURCE):
        h = self.h
        return json.loads(h.kube('get', 'cluster', name, '-n', namespace, '-o', 'json'))['status']['currentPrimary']

    def install_namespace(self, namespace, ca):
        h = self.h
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-auth', 'namespace': namespace},
                 'stringData': {'access': 'disposable-test-only-access', 'secret': 'disposable-test-only-secret'}})
        h.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'wal-minio-ca', 'namespace': namespace},
                 'data': {'ca.crt': ca}})

    def pool(self, count):
        self.h.allocate(count)

    def setup(self):
        h = self.h
        WORK = h.WORK
        if shutil.which('docker') is None:
            raise RuntimeError('Docker is required for the actual campaign; no scenarios executed')
        # All test executables/images come from the verified immutable bundle.
        # No Go/native/product build or bootstrap occurs during a campaign.
        for name in ('kind-linux-amd64', 'kubectl', 'cnpg-1.30.0.yaml', 'cert-manager.yaml'):
            h.download(name)
        for name in ('cnpg-1.30.0.yaml', 'cert-manager.yaml'):
            text = (WORK / name).read_text()
            for tag, digest in h.LOCK['images'].items():
                text = text.replace(tag, digest)
            (WORK / ('pinned-' + name)).write_text(text)
        (WORK / 'kind.json').write_text(json.dumps({'kind': 'Cluster', 'apiVersion': 'kind.x-k8s.io/v1alpha4',
                                                 'nodes': [{'role': 'control-plane'}]}))
        h.create_node()
        limits = json.loads(h.run('docker', 'inspect', h.NAME + '-control-plane', '--format', '{{json .HostConfig}}'))
        envelope = {'node_cpus': limits['NanoCpus'] / 1e9, 'node_memory_gib': limits['Memory'] / (1 << 30)}
        if any(envelope[key] != h.resources[key] for key in envelope):
            raise RuntimeError('fixture resource envelope differs from immutable recipe')
        self.m.data.setdefault('fixture_envelopes', {})[h.NAME] = envelope
        self.m.save()
        assert json.loads(h.kube('version', '-o', 'json'))['serverVersion']['gitVersion'] == 'v1.35.8'
        # CONSUME selected registry manifests directly. Never load rebuilt HEAD
        # under the subject name, and never mistake a Docker config ID for digest.
        for flavor, image in [('manager', self.args.manager_image), ('pg18', self.args.data_image)]:
            h.run('docker', 'pull', '--platform=linux/amd64', image)
            version = h.tool(image, '/usr/local/bin/cnpg-backup', 'version')
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
        self.image_pull_secrets = registry_pull_secrets(h)
        h.apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'campaign-target'},
                 'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
        self.pool(3)
        # One fresh capture workspace per source/target identity; the local
        # ownership slice needs fewer than the full matrix, never smaller quotas.
        h.capture_workspaces()
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
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 's3-auth', 'namespace': STORE},
                 'stringData': {'access': 'disposable-test-only-access', 'secret': 'disposable-test-only-secret'}})
        self.wal = wal_smoke.WALFixture(h, self.m.data, namespace=STORE)
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
                                 'smartShutdownTimeout': 30, 'stopDelay': 60,
                                 'imagePullSecrets': self.image_pull_secrets,
                                 'storage': {'size': '3Gi', 'storageClass': 'campaign-target'},
                                 'walStorage': {'size': '3Gi', 'storageClass': 'campaign-target'},
                                 'tablespaces': [{'name': 'fast_space', 'storage': {'size': '3Gi', 'storageClass': 'campaign-target'}}],
                                 'postgresql': {'parameters': {'summarize_wal': 'on', 'wal_summary_keep_time': '14d',
                                                              'archive_timeout': '60s', 'track_commit_timestamp': 'on'}},
                                 'plugins': [{'name': PLUGIN, 'isWALArchiver': True, 'parameters': {'repository': 'destination'}}]}}
        if 'differential' in getattr(self.args, 'fixtures', []):
            self.cluster['spec']['postgresql']['parameters']['log_replication_commands'] = 'on'
        h.wait(lambda: h.admission_ready(h.kube('apply', '--server-side', '--dry-run=server', '-f', '-',
                                                input=json.dumps(self.cluster), check=False)), 'actual mTLS discovery')
        h.apply(self.cluster)
        h.kube('wait', '-n', SOURCE, '--for=condition=Ready', 'cluster/database', '--timeout=360s', timeout=400)
        self.install_observer()
        if 'differential' in getattr(self.args, 'fixtures', []):
            self.make_differential_workload()
        else:
            self.make_workload()

    def install_observer(self):
        h = self.h
        image = h.image_digest('recoveryactor')
        self.event('test-observer', sha256=h.bundle['files']['actor'], image=image,
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
        h = self.h
        OUT = h.OUT
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
        h = self.h
        pod = self.primary()
        segment = self.sql(SOURCE, pod, 'SELECT pg_walfile_name(pg_switch_wal())')
        h.wait(lambda: self.sql(SOURCE, pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + segment + ".done'") == '1',
               'known durable archive boundary', 120)
        return self.fetch_archive(segment)

    def fetch_archive(self, segment):
        h = self.h
        WORK = h.WORK
        directory = WORK / 'native-wal'
        directory.mkdir(mode=0o755, exist_ok=True)
        path = directory / segment
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

    def full(self, name, backup_type='full'):
        h = self.h
        OUT = h.OUT
        d = backup_smoke.definition(h, name, backup_type=backup_type)
        d['metadata']['namespace'] = SOURCE
        h.apply(d)
        def done():
            b = json.loads(h.kube('get', 'backup', name, '-n', SOURCE, '-o', 'json'))
            state = b.get('status', {}).get('phase')
            assert state != 'failed', 'actual requested capture failed: ' + backup_type
            return state == 'completed'
        h.wait(done, 'actual CNPG committed ' + backup_type, 600)
        b = json.loads(h.kube('get', 'backup', name, '-n', SOURCE, '-o', 'json'))
        uid = b['status']['backupId']
        assert uid == b['metadata']['uid']
        commit = json.loads(self.wal.s3('GET', 'smoke/v1/' + SOURCE_ID + '/backups/' + uid + '/commit.json'))
        atomic_json(OUT / (name + '-commit.json'), commit)
        self.m.data['native_backup_count'] = self.m.data.get('native_backup_count', 0) + 1
        self.m.save()
        return commit

    def make_differential_workload(self):
        h = self.h
        pod = self.primary()
        self.sql(SOURCE, pod, "CREATE TABLE g_oracle(id integer PRIMARY KEY,value text) TABLESPACE fast_space; "
                             "CREATE TABLE g_drop(id integer PRIMARY KEY); INSERT INTO g_drop VALUES(71); "
                             "CREATE TABLE h_data(id integer PRIMARY KEY,value text); "
                             "INSERT INTO h_data SELECT i,repeat(md5(i::text),16) FROM generate_series(1,60000)i; "
                             "CREATE TABLE h_space(value text) TABLESPACE fast_space; INSERT INTO h_space VALUES('full'); "
                             "CREATE TABLE h_truncated(id int); INSERT INTO h_truncated VALUES(1); "
                             "CREATE TABLE h_recreated(id int); INSERT INTO h_recreated VALUES(1)")
        self.acknowledge(1, 'base')
        self.base = self.full('h-full')
        self.sql(SOURCE, pod, "UPDATE h_data SET value='d1' WHERE id=1; DELETE FROM h_data WHERE id=2; "
                             "TRUNCATE h_truncated; INSERT INTO h_truncated VALUES(2); "
                             "DROP TABLE h_recreated; CREATE TABLE h_recreated(id int); INSERT INTO h_recreated VALUES(2); "
                             "UPDATE h_space SET value='d1'")
        self.d1 = self.full('h-d1', 'differential')
        self.sql(SOURCE, pod, "UPDATE h_data SET value='d2' WHERE id=3; DELETE FROM h_data WHERE id=4; "
                             "TRUNCATE h_truncated; INSERT INTO h_truncated VALUES(3); "
                             "DROP TABLE h_recreated; CREATE TABLE h_recreated(id int); INSERT INTO h_recreated VALUES(3); "
                             "UPDATE h_space SET value='d2'")
        self.d2 = self.full('h-d2', 'differential')
        commands = self.native_backup_commands(pod)
        assert len(commands) == 3 and sum('INCREMENTAL' in c.upper() for c in commands) == 2, 'native F/D/D command oracle absent'
        for c in (self.d1, self.d2):
            assert c['kind'] == 'differential' and c['parent_backup_uid'] == c['root_backup_uid'] == self.base['backup_uid']
            assert c['root_manifest_sha256'] == self.base['manifest_sha256'], 'not the exact original full manifest'
        sizes = {name: sum(a['stored_bytes'] for a in c['artifacts']) + c['manifest_bytes']
                 for name, c in (('F', self.base), ('D1', self.d1), ('D2', self.d2))}
        assert sizes['D2'] < sizes['F'], 'largely unchanged fixture did not reduce actual S3 transfer'
        self.event('differential-direct-F-transfers', stored_bytes=sizes, full=self.base['backup_uid'], d1=self.d1['backup_uid'], d2=self.d2['backup_uid'])
        digest = hashlib.md5()
        count = 0
        for i in range(1, 60001):
            if i in (2, 4):
                continue
            value = {1: 'd1', 3: 'd2'}.get(i, hashlib.md5(str(i).encode()).hexdigest() * 16)
            digest.update(((',' if count else '') + str(i) + ':' + value).encode())
            count += 1
        self.differential_expected = str(count) + ':' + digest.hexdigest()
        assert self.sql(SOURCE, pod, "SELECT count(*)||':'||md5(string_agg(id::text||':'||value,',' ORDER BY id)) FROM h_data") == self.differential_expected
        atomic_json(h.OUT / 'differential-oracle.json', {'data_hash': self.differential_expected, 'stored_bytes': sizes})

    def differential_restore(self, remote=False):
        h = self.h
        # Delete only the explicitly unrelated D1 test namespace. F+D2 must be
        # sufficient; this is an injected object-loss case, not retention code.
        prefix = 'smoke/v1/' + SOURCE_ID + '/backups/' + self.d1['backup_uid'] + '/'
        keys = self.inventory(prefix)
        assert keys and all(k.startswith(prefix) for k in keys)
        for key in keys:
            self.wal.s3('DELETE', key)
        assert not self.inventory(prefix), 'unrelated D1 was not actually removed'
        target = {'backupID': self.d2['backup_uid'], 'targetImmediate': True}
        rows = BASE
        if remote:
            self.archive()
            self.acknowledge(2, 'before')
            point = self.sql(SOURCE, self.primary(), "SELECT pg_create_restore_point('h_after_d2')")
            self.acknowledge(3, 'target')
            self.acknowledge(4, 'after')
            remote_file = self.archive()
            assert lsn(point) > lsn(self.d2['bundled_wal_end_lsn'])
            assert int(remote_file[8:16], 16) * 256 + int(remote_file[16:], 16) > (lsn(self.d2['bundled_wal_end_lsn']) - 1) // (16 << 20)
            self.event('differential-remote-sentinel', bundle_end=self.d2['bundled_wal_end_lsn'], target_lsn=point, remote_file=remote_file)
            target = {'backupID': self.d2['backup_uid'], 'targetName': 'h_after_d2'}
            rows = BEFORE
        else:
            for c in (self.base, self.d2):
                self.case_shell_free_original_verification(c, ('missing-WAL', 'corrupt-WAL'))
        self.destroy_source()
        state = self.start(target)
        state['differential_expected'] = self.differential_expected
        plan = self.materialize(state)
        assert [c['backup_uid'] for c in plan['plan']['chain']] == [self.base['backup_uid'], self.d2['backup_uid']]
        if remote:
            assert plan['plan']['required_archive'], 'D PITR did not require remote post-bundle WAL'
        self.finish(state, rows, drop_present=True)

    def native_backup_commands(self, pod):
        # REQUIRED command oracle, not optional forensic collection. Positive
        # F/D/D setup proves the log source distinguishes full from incremental.
        text = self.h.kube('logs', '-n', SOURCE, pod, '-c', 'postgres', '--limit-bytes=1048576')
        commands = [line for line in text.splitlines() if 'received replication command: BASE_BACKUP' in line]
        return commands

    def differential_failed(self, fault):
        from backup_metrics_smoke import BackupMetricsSmoke
        h = self.h
        metrics = BackupMetricsSmoke(h, self.m.data, SOURCE_ID)
        try:
            metrics.start()
            before = metrics.assert_committed(backup_smoke.publication_epoch(self.wal, self.d2['backup_uid']), 'differential')
            full_before = metrics.snapshot('full')
            pod = self.primary()
            restore = []
            with self.cleanup(restore):
                if fault == 'missing-full':
                    key = 'smoke/v1/' + SOURCE_ID + '/backups/' + self.base['backup_uid'] + '/attempts/' + self.base['attempt_id'] + '/manifest.pg.json'
                    saved = h.WORK / 'withheld-full-manifest'
                    self.wal.s3('GET', key, saved)
                    restore.append(('restore-full-manifest', lambda: self.put_fixture(key, saved)))
                    self.wal.s3('DELETE', key)
                    assert key not in self.inventory(key), 'full manifest loss did not fire'
                elif fault == 'missing-summary':
                    count = self.sql(SOURCE, pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/summaries') n WHERE n LIKE '%.summary'")
                    assert int(count) > 0 and self.sql(SOURCE, pod, 'SHOW summarize_wal') == 'on'
                    h.kube('exec', '-n', SOURCE, pod, '-c', 'postgres', '--', 'sh', '-ec',
                           'find "$PGDATA/pg_wal/summaries" -type f -name "*.summary" -delete')
                    self.event('differential-summary-loss', removed_count=int(count), summarize_wal='on')
                elif fault == 'checksum':
                    self.differential_checksum_change()
                    pod = self.primary()
                    assert self.sql(SOURCE, pod, 'SHOW data_checksums') == 'off'
                elif fault == 'promotion':
                    self.pool(3)
                    h.kube('patch', 'cluster/database', '-n', SOURCE, '--type=merge', '-p', '{"spec":{"instances":2}}')
                    h.wait(lambda: len(json.loads(h.kube('get', 'pods', '-n', SOURCE, '-l', 'cnpg.io/cluster=database,cnpg.io/podRole=instance', '-o', 'json'))['items']) == 2
                           and self.sql(SOURCE, self.primary(), 'SELECT count(*) FROM pg_stat_replication WHERE state=\'streaming\'') == '1', 'differential promotion standby streaming', 360)
                    old = self.primary()
                    h.kube('delete', 'pod', old, '-n', SOURCE, '--grace-period=0', '--force', '--wait=false')
                    h.wait(lambda: self.primary() != old, 'differential primary promotion', 180)
                    pod = self.primary()
                    h.wait(lambda: self.sql(SOURCE, pod, 'SELECT NOT pg_is_in_recovery()') == 't', 'promoted primary accepts SQL', 180)
                    assert int(self.sql(SOURCE, pod, 'SELECT timeline_id FROM pg_control_checkpoint()')) > self.base['timeline']
                commands_before = self.native_backup_commands(pod)
                name = 'h-failed-' + fault
                if fault == 'cancellation':
                    self.sql(SOURCE, pod, "UPDATE h_data SET value=repeat(md5(id::text||'cancel'),16)")
                h.apply(backup_smoke.definition(h, name, backup_type='differential'))
                if fault == 'cancellation':
                    h.wait(lambda: self.sql(SOURCE, pod, 'SELECT count(*) FROM pg_stat_progress_basebackup') == '1', 'real differential native capture active', 180)
                    active = self.sql(SOURCE, pod, "SELECT query FROM pg_stat_activity WHERE backend_type='walsender' AND query LIKE 'BASE_BACKUP%'")
                    assert 'INCREMENTAL' in active.upper(), 'fault did not hit a native differential command'
                    observed = json.loads(h.kube('get', 'pod', pod, '-n', SOURCE, '-o', 'json'))
                    sidecar = next(c for c in observed['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
                    container = sidecar['containerID'].split('://')[1]
                    pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', container))['info']['pid'])
                    assert pid > 1
                    h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-TERM', str(pid))
                    self.event('differential-cancellation-fired', container_id=container, native_query=active)
                def failed():
                    b = json.loads(h.kube('get', 'backup', name, '-n', SOURCE, '-o', 'json'))
                    assert b.get('status', {}).get('phase') != 'completed', 'requested D fault falsely succeeded'
                    return b.get('status', {}).get('phase') == 'failed'
                h.wait(failed, 'terminal requested differential failure', 360)
                b = json.loads(h.kube('get', 'backup', name, '-n', SOURCE, '-o', 'json'))
                uid = b['metadata']['uid']
                keys = self.inventory('smoke/v1/' + SOURCE_ID + '/backups/' + uid + '/')
                assert not any('/data/' in k or k.endswith('/commit.json') or k.endswith('/manifest.pg.json') for k in keys), 'failure uploaded/published a replacement backup'
                if fault == 'cancellation':
                    h.wait(lambda: self.sql(SOURCE, pod, 'SELECT count(*) FROM pg_stat_progress_basebackup') == '0', 'canceled native replication drained', 60)
                commands_after = self.native_backup_commands(pod)
                assert sum('INCREMENTAL' not in c.upper() for c in commands_after) == sum('INCREMENTAL' not in c.upper() for c in commands_before), 'requested D started a replacement full command'
                self.event('requested-differential-failed-closed', fault=fault, uid=uid, keys=keys, native_commands=commands_after, no_replacement_upload=True)
            metrics.assert_failed(name, before, 'differential', require_warning=True)
            after_full = metrics.snapshot('full')
            assert after_full == full_before, 'failed D changed full success/failure metrics'
        finally:
            metrics.close()

    def differential_checksum_change(self):
        h = self.h
        pod = json.loads(h.kube('get', 'pod', self.primary(), '-n', SOURCE, '-o', 'json'))
        volume = next(v for v in pod['spec']['volumes'] if v['name'] == 'pgdata')
        h.kube('annotate', 'cluster/database', '-n', SOURCE, 'cnpg.io/hibernation=on', '--overwrite')
        h.wait(lambda: not json.loads(h.kube('get', 'pods', '-n', SOURCE, '-l', 'cnpg.io/cluster=database', '-o', 'json'))['items'], 'source clean hibernation before checksum tool', 360)
        h.quiesce_pods(SOURCE)
        name = 'h-checksums'
        h.apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': SOURCE},
                 'spec': {'restartPolicy': 'Never', 'securityContext': {'runAsUser': 26, 'runAsGroup': 26, 'fsGroup': 26},
                          'imagePullSecrets': self.image_pull_secrets,
                          'containers': [{'name': 'checksums', 'image': h.LOCK['database'],
                                          'command': ['/usr/lib/postgresql/18/bin/pg_checksums', '--disable', '-D', '/var/lib/postgresql/data/pgdata'],
                                          'resources': {'limits': {'memory': '128Mi', 'cpu': '1'}},
                                          'volumeMounts': [{'name': 'pgdata', 'mountPath': '/var/lib/postgresql/data'}]}], 'volumes': [volume]}})
        h.wait(lambda: json.loads(h.kube('get', 'pod', name, '-n', SOURCE, '-o', 'json')).get('status', {}).get('phase') in ('Succeeded', 'Failed'), 'actual offline checksum change', 120)
        changed = json.loads(h.kube('get', 'pod', name, '-n', SOURCE, '-o', 'json'))
        assert changed['status']['phase'] == 'Succeeded', 'offline checksum tool failed'
        h.kube('delete', 'pod', name, '-n', SOURCE, '--wait=true', '--timeout=120s')
        h.kube('annotate', 'cluster/database', '-n', SOURCE, 'cnpg.io/hibernation-', '--overwrite')
        h.kube('wait', '-n', SOURCE, '--for=condition=Ready', 'cluster/database', '--timeout=360s', timeout=400)
        self.event('actual-checksum-transition', old=1, new=0, source_restart=True)

    def case_differential_native(self):
        with self.m.case('differential-native'):
            for branch in self.variants(('reconstruction', 'remote-PITR-source-loss', 'missing-full', 'missing-summary', 'checksum', 'promotion', 'cancellation')):
                if branch in ('reconstruction', 'remote-PITR-source-loss'):
                    self.differential_restore(remote=branch == 'remote-PITR-source-loss')
                else:
                    self.differential_failed(branch)

    def make_workload(self):
        h = self.h
        WORK, OUT = h.WORK, h.OUT
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
                     '--mount', 'type=bind,source=' + str(WORK / 'native-wal') + ',target=/fixture,readonly',
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
        if 'same-segment' in getattr(self.args, 'fixtures', []):
            self.capture_same_segment()

    def capture_same_segment(self):
        # Fresh PRODUCT capture, not a retained planning fixture. Native stop
        # normally appends a real SWITCH after EndLSN; observe, never assume it.
        h = self.h
        WORK, OUT = h.WORK, h.OUT
        commit = self.full('g-same-switch')
        assert self.sql(SOURCE, self.primary(), 'SHOW wal_segment_size') == '16MB'
        end = lsn(commit['bundled_wal_end_lsn'])
        segment = (end - 1) // (16 << 20)
        name = f"{commit['timeline']:08X}{segment // 256:08X}{segment % 256:08X}"
        h.wait(lambda: self.sql(SOURCE, self.primary(), "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '1',
               'actual product same-final-file archive durable', 120)
        self.fetch_archive(name)  # no extra switch or invented frontier
        dump = h.run('docker', 'run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL',
                     '--mount', 'type=bind,source=' + str(WORK / 'native-wal') + ',target=/fixture,readonly',
                     '--entrypoint=/usr/lib/postgresql/18/bin/pg_waldump', self.args.data_image,
                     '--path=/fixture', '--rmgr=XLOG', '--start=' + commit['bundled_wal_end_lsn'], name)
        h.save_log('same-segment-switch-waldump.log', dump)
        self.same_witness = switch_witness(commit, dump)
        self.same_witness['archive_sha256'] = hashlib.sha256((WORK / 'native-wal' / name).read_bytes()).hexdigest()
        self.same_commit = commit
        atomic_json(OUT / 'same-segment-witness.json', self.same_witness)
        self.event('same-segment-real-switch-observed', **self.same_witness, original_manifest_unchanged=True)

    def pods(self, name):
        h = self.h
        return json.loads(h.kube('get', 'pods', '-n', TARGET, '-l', 'cnpg.io/cluster=' + name, '-o', 'json'))['items']

    def file(self, pod, path, content=None, check=True):
        h = self.h
        if content is None:
            return h.kube('exec', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'cat', path, check=check)
        return h.kube('exec', '-i', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'sh', '-ec',
                      'cat > "$1"', 'campaign-file', path, input=content, check=check)

    def barrier(self, pod, name, seconds=300):
        h = self.h
        def observed():
            result = h.kube_result('exec', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'test', '-f', '/controller/campaign/' + name)
            # exit1 is the explicit absent-file predicate; kubectl/API failures
            # are not readiness and an empty stderr is never success evidence.
            if result.returncode != 0 and not (result.returncode == 1 and result.stderr.strip() == 'command terminated with exit code 1'):
                result.require()
            if result.timed_out:
                result.require()
            return result.ok
        h.wait(observed, 'observed test boundary: ' + name, seconds)
        self.event('fault-precondition', pod=pod, barrier=name, observed=True)

    def release(self, pod, name):
        h = self.h
        if name == 'release-shutdown':
            # Atomically publish this exec writer's PID. The actor waits for its
            # exit before allowing PID1 to terminate all remaining descendants.
            h.kube('exec', '-n', TARGET, pod, '-c', 'full-recovery', '--', 'sh', '-ec',
                   'printf "%s" "$$" > "$1.next"; mv "$1.next" "$1"', 'shutdown-writer', '/controller/campaign/' + name)
            return
        self.file(pod, '/controller/campaign/' + name, 'release\n')

    def start(self, target, hold_replay=False):
        h = self.h
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
        h = self.h
        OUT = h.OUT
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
        h = self.h
        return json.loads(self.wal.s3('GET', 'smoke/v1/' + SOURCE_ID + '/gate.json'))

    def holds(self, state, stage):
        h = self.h
        plan = state['plan']['plan']
        gate = self.gate()
        by_id = {x['id']: x for x in gate['holders']}
        assert gate['owner'] is None
        for key in ('lifetime_hold_id', 'reader_hold_id'):
            holder = by_id[plan[key]]
            assert holder['operation_id'] == plan['operation_id'] and holder['target_cluster_uid'] == plan['target_cluster_uid']
        self.event('source-protection', stage=stage, operation=plan['operation_id'], holders=list(by_id), generation=gate['generation'])

    def materialize(self, state):
        h = self.h
        self.release(state['pod'], 'release-before')
        self.barrier(state['pod'], 'restore-response', 600)
        return self.plan(state)

    def finish(self, state, rows, drop_present=False, stable_retained=False, ordinary=False):
        h = self.h
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
        if 'differential_expected' in state:
            expected = state['differential_expected']
            observed = self.sql(TARGET, primary, "SELECT count(*)||':'||md5(string_agg(id::text||':'||value,',' ORDER BY id)) FROM h_data")
            assert observed == expected, 'differential update/delete data hash differs'
            assert self.sql(TARGET, primary, "SELECT (SELECT id FROM h_truncated)||':'||(SELECT id FROM h_recreated)||':'||(SELECT value FROM h_space)") == '3:3:d2', 'differential truncate/drop/recreate/tablespace changes lost'
            self.event('differential-datahash-SQL', expected=expected, observed=observed)
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
        h = self.h
        WORK = h.WORK
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
                  self.wal.endpoint + '/test-bucket/' + key)
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
        h = self.h
        OUT = h.OUT
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
        h = self.h
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
        self.collect_target_logs(state['completion_pods'])
        self.event('completion-evidence', cluster=state['name'], job_uids=[j['metadata']['uid'] for j in jobs],
                   pods=[h.pod_evidence(p) for p in state['completion_pods']])

    def collect_target_logs(self, pods):
        from campaign_fixture import Fixture
        with self.h.commands.budget(30):
            self.m.collect_diagnostics('target-logs', lambda: Fixture.collect_logs(self.h, TARGET, pods))

    def retire_target(self, state):
        # Bound live PostgreSQL/sidecar memory. Logs are optional forensics;
        # Pod/operation/holder identities and safe retirement remain mandatory.
        # PVCs and durable poison markers are NEVER cleared or reused here.
        h = self.h
        observed_pods = self.pods(state['name'])
        state['retired_pod_uids'] = sorted({p['metadata']['uid'] for p in observed_pods + state.get('completion_pods', [])}
                                           | ({state['pod_uid']} if 'pod_uid' in state else set()))
        self.collect_target_logs(observed_pods)
        # Keep the operation object until the original observer records closure.
        # Deleting its Cluster first can GC that object before uncertainty is
        # durable, correctly retaining an in-memory fence/capacity indefinitely.
        h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET, 'cnpg.io/reconciliationLoop=disabled', '--overwrite')
        for resource in ('jobs', 'pods'):
            h.kube('delete', resource, '-n', TARGET, '-l', 'cnpg.io/cluster=' + state['name'],
                   '--ignore-not-found=true', '--wait=true', '--timeout=120s')
        h.wait(lambda: not self.pods(state['name']), 'owned target Pods stopped before next scenario', 120)
        h.wait(lambda: self.operation_state(state)['state'] in ('uncertain', 'completed'),
               'original observer durably closed before test Cluster cleanup', 120)
        self.event('target-cleanup-operation-closed', cluster=state['name'], operation=self.operation_state(state))
        h.kube('delete', 'cluster', state['name'], '-n', TARGET, '--wait=true', '--timeout=120s')
        if hasattr(h, 'retire_claims'):
            holders = self.gate()['holders']
            h.retire_claims(state)
            assert self.gate()['holders'] == holders, 'fixture disposal changed source protection'
        self.event('target-retired-after-evidence', cluster=state['name'], pvc_uids=state['pvc_uids'], target_markers_removed_by_harness=False)

    def inventory(self, prefix):
        # Fixture intentionally stays below one complete 1000-item page. A
        # truncated list is a failed oracle, never an inferred continuous archive.
        h = self.h
        tree = ET.fromstring(self.wal.s3('GET', '?list-type=2&prefix=' + prefix))
        ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        assert tree.findtext('s:IsTruncated', namespaces=ns) == 'false'
        return sorted(x.text for x in tree.findall('s:Contents/s:Key', ns))

    def restore(self, target, rows, drop_present=False, ordinary=False):
        h = self.h
        state = self.start(target)
        self.materialize(state)
        return self.finish(state, rows, drop_present, ordinary=ordinary)


    def reject(self, target, reason, materialized=False):
        h = self.h
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
        h.save_log(state['name'] + '-negative-main.log', logs)
        self.event('expected-recovery-rejection', reason=reason, trace=records)
        # Keep wrapper and guard alive for collection. Disable CNPG reconciliation
        # and forbid accidental Job retry progress; poison is never cleared.
        h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET, 'cnpg.io/reconciliationLoop=disabled', '--overwrite')
        self.retire_target(state)
        return state


    def helper(self, state, name, expected):
        h = self.h
        result = h.kube_result('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', 'sh', '-c',
                      '"$@"; code=$?; printf "\\nCAMPAIGN_EXIT=%s\\n" "$code"', 'capture-helper-exit',
                      '/cnpg-backup/bin/cnpg-backup', 'wal-fetch', '--plan', '/cnpg-backup/state/recovery.json',
                      '--', name, 'pg_wal/RECOVERYXLOG', timeout=300)
        result.require()
        # stderr is a separate stream, not text after the shell's stdout marker.
        # A missing marker is an ineffective fixture, not product exit evidence.
        match = re.search(r'CAMPAIGN_EXIT=([0-9]+)\s*$', result.stdout)
        assert match, 'fixture helper exit marker missing from stdout'
        observed = int(match[1])
        self.event('actual-helper-exit', pod=state['pod'], name=name, exit=observed, expected=expected,
                   stderr=result.stderr, destination='pg_wal/RECOVERYXLOG')
        assert observed == expected, f'actual product helper exit differs: expected={expected} observed={observed}; ' + result.stderr[-1000:]

    @contextlib.contextmanager
    def archive_absent(self):
        # Explicit fault actor, NOT retention or valid coordinated deletion. The
        # source namespace is gone; only this disposable synthetic bucket changes.
        h = self.h
        WORK = h.WORK
        directory = WORK / ('withheld-' + str(self.count))
        directory.mkdir(mode=0o700)
        keys = self.inventory('smoke/v1/' + SOURCE_ID + '/wal/')
        saved = []
        def verify_archive():
            assert self.inventory('smoke/v1/' + SOURCE_ID + '/wal/') == keys
        actions = [('fixture-restore', verify_archive)]
        with self.cleanup(actions):
            for i, key in enumerate(keys):
                payload, headers = directory / str(i), directory / (str(i) + '.headers')
                d = self.wal.directory
                h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '15',
                      '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '--dump-header', headers,
                      '--output', payload, self.wal.endpoint + '/test-bucket/' + key)
                metadata = []
                for line in headers.read_text().splitlines():
                    if line.lower().startswith('x-amz-meta-'):
                        assert re.fullmatch(r'[A-Za-z0-9-]+: [A-Za-z0-9_./: -]+', line), 'unexpected test object metadata'
                        metadata += ['--header', line]
                assert metadata, 'WAL integrity metadata snapshot missing'
                saved.append((key, payload, metadata))
                actions.insert(-1, ('fixture-restore', lambda key=key, payload=payload, metadata=metadata:
                    h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '30',
                          '--config', self.wal.directory / 'curl-private.conf', '--cacert', self.wal.directory / 'ca.crt',
                          '-X', 'PUT', '--upload-file', payload, *metadata, self.wal.endpoint + '/test-bucket/' + key)))
                self.wal.s3('DELETE', key)
            assert self.inventory('smoke/v1/' + SOURCE_ID + '/wal/') == []
            self.event('effective-archive-removal', object_count=len(saved), source_namespace_deleted=True,
                       legitimate_GC_claim=False)
            yield


    def put_fixture(self, key, path):
        h = self.h
        d = self.wal.directory
        return h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '30',
                     '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '-X', 'PUT',
                     '--upload-file', path, self.wal.endpoint + '/test-bucket/' + key)

    @contextlib.contextmanager
    def padded_same_bundle(self):
        # Controlled padding AFTER the authenticated native range only. Original
        # manifests/SQL data are unchanged. This is an explicitly labeled test
        # fixture mutation, never a claim production may rewrite immutable data.
        h = self.h
        WORK = h.WORK
        commit = copy.deepcopy(self.same_commit)
        d = WORK / ('same-padding-' + uuid.uuid4().hex[:12])
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
        with self.cleanup([('fixture-restore', lambda: self.put_fixture(key, old)),
                      ('fixture-restore', lambda: self.put_fixture(prefix + 'commit.json', old_commit))]):
            self.put_fixture(key, changed)
            self.put_fixture(prefix + 'commit.json', changed_commit)
            self.event('same-segment-padding-arranged', filename=name, end_lsn=commit['bundled_wal_end_lsn'],
                       target=self.same_witness, original_manifest_unchanged=True,
                       synthetic_padding=True, original_object_sha256=hashlib.sha256(old.read_bytes()).hexdigest(),
                       padded_object_sha256=hashlib.sha256(stored).hexdigest())
            yield commit, name

    def same_segment_plan(self, state):
        h = self.h
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
        h = self.h
        return state['replay_endpoint'] >= max(state['same_segment_floor'], lsn(self.same_witness['replay_end_lsn']))



    def fatal_replay(self, state, requested, mode, family):
        h = self.h
        with self.cleanup([('fault-reset', lambda: self.wal.control(''))]):
            self.wal.control(mode, requested)
            self.replay_failure(state, requested, fault_family=family)
        self.retire_target(state)

    def replay_failure(self, state, requested, fault_family=None):
        h = self.h
        receipt = None
        # Observe effectiveness even if terminal readiness itself fails.
        def observe_fault():
            nonlocal receipt
            if fault_family:
                receipt = self.wal.control()
                self.event('fatal-WAL-fault-receipt', family=fault_family, filename=requested, proxy=receipt,
                           outcome_assertions='eligible' if receipt['blocked'] > 0 else 'blocked')
        with self.cleanup([('fault-observation', observe_fault)]):
            self.release(state['pod'], 'release-response')
            self.barrier(state['pod'], 'cnpg-exited', 240)
        logs = h.kube('logs', state['pod'], '-n', TARGET, '-c', 'full-recovery')
        trace = self.file(state['pod'], '/controller/campaign/rpc.jsonl')
        h.save_log(state['name'] + '-fatal-main.log', logs)
        h.save_log(state['name'] + '-fatal-rpc.jsonl', trace)
        events = [json.loads(x) for x in trace.splitlines()]
        if fault_family:
            requested_seen = any(x.get('event') == 'wal-request' and x['name'] == requested for x in events)
            self.event('fatal-WAL-outcome-prerequisites', filename=requested, requested_seen=requested_seen,
                       outcome_assertions='eligible' if receipt['blocked'] > 0 and requested_seen else 'blocked')
            if receipt['blocked'] <= 0 or not requested_seen:
                raise RuntimeError('ineffective fault fixture: outcome assertions blocked; requested WAL/fault receipt not established')
        assert '255' in logs and ('FATAL' in logs or 'fatal' in logs)
        assert 'database system is ready to accept connections' not in logs, 'false latest promotion'
        assert any(x.get('event') == 'wal-request' and x['name'] == requested for x in events)
        assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] != 0 for x in events)
        self.holds(state, 'fatal-source-WAL')


    def no_retries(self, state):
        h = self.h
        h.kube('annotate', 'cluster/' + state['name'], '-n', TARGET, 'cnpg.io/reconciliationLoop=disabled', '--overwrite')
        jobs = json.loads(h.kube('get', 'jobs', '-n', TARGET, '-l', 'cnpg.io/cluster=' + state['name'], '-o', 'json'))['items']
        assert jobs
        for job in jobs:
            h.kube('patch', 'job', job['metadata']['name'], '-n', TARGET, '--type=merge', '-p', '{"spec":{"backoffLimit":0}}')

    def node_pid(self, state, container):
        h = self.h
        pod = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
        statuses = pod['status']['containerStatuses'] + pod['status']['initContainerStatuses']
        selected = next(c for c in statuses if c['name'] == container)
        cid = selected['containerID'].split('://')[1]
        pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', cid))['info']['pid'])
        assert pid > 1
        return pid, cid

    def markers(self, state):
        h = self.h
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
        h = self.h
        pod = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
        return any(c['name'] == 'full-recovery' and 'terminated' in c.get('state', {}) for c in pod.get('status', {}).get('containerStatuses', []))


    def operation_state(self, state):
        h = self.h
        cm = json.loads(h.kube('get', 'configmap', state['name'] + '-cb-recovery', '-n', TARGET, '-o', 'json'))
        return json.loads(cm['data']['operation.json'])



    def replacement(self, state, poisoned=False):
        h = self.h
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
        h.kube('apply', '--server-side', '--dry-run=server', '-f', '-', input=json.dumps(replacement))
        h.apply(replacement)
        def stopped():
            p = json.loads(h.kube('get', 'pod', name, '-n', TARGET, '-o', 'json'))
            return any(c['name'] == 'full-recovery' and 'terminated' in c.get('state', {}) for c in p.get('status', {}).get('containerStatuses', []))
        h.wait(stopped, 'same-PVC replacement refused before preflight', 240)
        logs = h.kube('logs', name, '-n', TARGET, '-c', 'full-recovery')
        assert 'TargetOwnershipUncertain' in logs or 'TargetOwnershipBusy' in logs, logs[-2000:]
        assert self.target_snapshot(original) == before, 'replacement renamed/deleted/reconstructed target content'
        self.event('same-PVC-replacement-rejected', original=state['pod'], replacement=name, poisoned=poisoned, snapshot=before)
        h.save_log(name + '.log', logs)
        h.kube('delete', 'pod', name, '-n', TARGET, '--wait=true', '--timeout=120s')

    def target_snapshot(self, pod):
        # Inspect fixed disposable backing files from the node, not through the
        # possibly dead main. Include directory names/inodes and content hashes.
        h = self.h
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


    def prepare_recovery(self):
        if 'differential' in getattr(self.args, 'fixtures', []):
            return  # H faults need their fresh live primary; H PITR deletes it explicitly.
        self.destroy_source()

    def destroy_source(self):
        # Source disaster is a fixture precondition, NOT a dependency on a
        # particular named-point restore passing. A named-target defect must
        # not prevent independent latest/time/ownership cases from exercising.
        h = self.h
        atomic_json(h.OUT / 'source-inventory.json', {'keys': self.inventory('smoke/v1/' + SOURCE_ID + '/')})
        # Explicit CNPG smart/stop30/60s; include namespace-controller requeue.
        h.kube('delete', 'namespace', SOURCE, '--wait=true', '--timeout=300s', timeout=320)
        assert not json.loads(h.kube('get', 'pods', '-n', SOURCE, '-o', 'json'))['items']
        h.quiesce_pods(SOURCE)
        self.event('source-disaster-precondition', namespace=SOURCE, actual_finalization=True)

    def case_source_namespace_catalog_loss_S3_only(self):
        with self.m.case('source-namespace-catalog-loss-S3-only'):
            self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)

    def variants(self, names):
        branch = getattr(self.m, 'current_branch', None)
        if isinstance(branch, str):
            if branch not in names:
                raise ValueError('unregistered branch: ' + branch)
            return [branch]
        return names

    def case_full_latest_remote_SQL(self):
        h = self.h
        with self.m.case('full-latest-remote-SQL'):
            for branch in self.variants(('newest', 'explicit-base')):
                if branch == 'newest':
                    selected = self.restore({}, LATEST, ordinary=True)
                    newest = getattr(self, 'same_commit', self.newest)
                    assert selected['plan']['plan']['chain'][0]['backup_uid'] == newest['backup_uid']
                else:
                    self.restore({'backupID': self.base['backup_uid']}, LATEST)

    def case_full_name_pre_DROP_SQL(self):
        h = self.h
        with self.m.case('full-name-pre-DROP-SQL'):
            self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)

    def case_full_time_inclusive_exclusive(self):
        self.temporal_case('full-time-inclusive-exclusive', 'targetTime', self.target['time_rfc3339'])

    def case_full_LSN_inclusive_exclusive(self):
        self.temporal_case('full-LSN-inclusive-exclusive', 'targetLSN', self.target['commit_lsn'])

    def case_full_XID_inclusive_exclusive(self):
        self.temporal_case('full-XID-inclusive-exclusive', 'targetXID', self.target['xid'])

    def case_full_explicit_immediate(self):
        h = self.h
        with self.m.case('full-explicit-immediate'):
            self.restore({'backupID': self.base['backup_uid'], 'targetImmediate': True}, BASE, True)

    def case_newest_base_too_new(self):
        h = self.h
        with self.m.case('newest-base-too-new'):
            for branch in self.variants(('earlier-base', 'reject-explicit-newest')):
                if branch == 'earlier-base':
                    state = self.restore({'targetLSN': self.target['commit_lsn']}, INCLUSIVE, True)
                    assert state['plan']['plan']['chain'][0]['backup_uid'] == self.base['backup_uid']
                else:
                    self.reject({'backupID': self.newest['backup_uid'], 'targetLSN': self.target['commit_lsn']}, 'too-new-base')

    def case_target_unreached(self):
        h = self.h
        with self.m.case('target-unreached'):
            self.reject({'backupID': self.base['backup_uid'], 'targetName': 'g_never_created'}, 'target-unreached', materialized=True)

    def case_shell_free_original_verification(self, commit=None, controls=None):
        h = self.h
        WORK = h.WORK
        with self.m.case('shell-free-original-verification') if commit is None else contextlib.nullcontext():
            directory = WORK / ('original-verification-' + uuid.uuid4().hex[:12])
            directory.mkdir(mode=0o700)
            exported = directory / 'subject.tar'
            name = h.NAME + '-image-audit'
            # The fixture owns this stopped audit container on EVERY error path;
            # disposal reports removal errors separately from the original oracle.
            h.run('docker', 'create', '--name', name, '--label', 'cnpg-backup-fixture=' + h.NAME, self.args.data_image, 'version')
            h.run('docker', 'export', '--output', exported, name, timeout=120)
            with tarfile.open(exported) as archive:
                paths = {m.name.lstrip('./') for m in archive}
            assert not paths.intersection({'bin/sh', 'bin/bash', 'usr/bin/sh', 'usr/bin/bash', 'usr/bin/python3'})
            h.run('docker', 'rm', name)
            tar_dir, wal_dir = directory / 'tar', directory / 'wal'
            tar_dir.mkdir()
            wal_dir.mkdir()
            commit = commit or self.base
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
            shutil.copy2(h.bundle['directory'] / 'verify', driver)
            def tool(entry, *args, failure=False):
                return h.tool(self.args.data_image, entry, *args,
                              mounts=('--mount', 'type=bind,source=' + str(directory) + ',target=/input,readonly'), rejection=failure)
            tool('/usr/lib/postgresql/18/bin/pg_verifybackup', '--exit-on-error', '--no-parse-wal', '/input/tar')
            tool('/input/verify', '/input/tar/backup_manifest', '/input/wal')
            segment = next(p for p in wal_dir.iterdir() if re.fullmatch('[0-9A-F]{24}', p.name))
            controls = controls or self.variants(('missing-WAL', 'corrupt-WAL'))
            for control in controls:
                if control == 'missing-WAL':
                    hidden = directory / 'withheld'
                    segment.rename(hidden)
                    with self.cleanup([('fixture-restore', lambda: hidden.rename(segment))]):
                        tool('/input/verify', '/input/tar/backup_manifest', '/input/wal', failure=True)
                else:
                    original = segment.read_bytes()
                    with self.cleanup([('fixture-restore', lambda: segment.write_bytes(original))]):
                        segment.write_bytes(bytes(8192) + original[8192:])
                        tool('/input/verify', '/input/tar/backup_manifest', '/input/wal', failure=True)
            self.event('shell-free-original-native-verification', image=self.args.data_image,
                       original_manifest_sha256=commit['manifest_sha256'], rejected_controls=list(controls),
                       data_image_has_shell=False, driver_sha256=hashlib.sha256(driver.read_bytes()).hexdigest())

    def case_bundle_duplicate_absent_local_fallback(self):
        h = self.h
        with self.m.case('bundle-duplicate-absent-local-fallback'):
            with self.archive_absent():
                for control in self.variants(('intact-local-fallback', 'all-required-255')):
                    state = self.start({'backupID': self.base['backup_uid'], 'targetImmediate': True})
                    self.materialize(state)
                    assert not state['plan']['plan']['required_archive'], 'immediate no-archive fixture unexpectedly has remote-required intervals'
                    name = sorted(state['plan']['bundled'])[0]
                    if control == 'intact-local-fallback':
                        self.helper(state, name, 1) # actual archive-first miss, NOT archive success
                        state['expect_promotion_partial'] = True
                        self.finish(state, BASE, True)
                    else:
                        self.release(state['pod'], 'incorrect-all-fatal')
                        self.helper(state, name, 255)
                        self.release(state['pod'], 'release-response')
                        self.barrier(state['pod'], 'cnpg-exited', 300)
                        trace = [json.loads(x) for x in self.file(state['pod'], '/controller/campaign/rpc.jsonl').splitlines()]
                        assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] != 0 for x in trace), 'all-required255 negative oracle was insensitive'
                        self.event('distinguishing-all-required255-negative', trace=trace)
                        self.retire_target(state)

    def case_bundle_local_missing_fatal(self):
        h = self.h
        with self.m.case('bundle-local-missing-fatal'):
            with self.archive_absent():
                state = self.start({'backupID': self.base['backup_uid'], 'targetImmediate': True})
                self.materialize(state)
                name = sorted(state['plan']['bundled'])[0]
                source = '/var/lib/postgresql/wal/pg_wal/' + name
                h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', 'mv', source, source + '.withheld')
                with self.cleanup([('fixture-restore', lambda: h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery',
                              '--', 'mv', source + '.withheld', source))]):
                    # Both actual archive absence and local withholding survive
                    # the PG request, helper255 and terminal no-promotion proof.
                    self.replay_failure(state, name)
                    self.event('effective-local-bundle-absence', filename=name, pod=state['pod'])
                # The intact exit1/SQL positive is the separate fresh target
                # above, never this repaired negative attempt.
                self.retire_target(state)

    def case_same_bundled_segment_post_EndLSN_archive_preferred(self):
        h = self.h
        with self.m.case('same-bundled-segment-post-EndLSN-archive-preferred'):
            with self.padded_same_bundle() as (commit, name):
                target = {'backupID': commit['backup_uid']}  # explicit-base/latest; no equality LSN target
                for control in self.variants(('healthy', 'wrong-bundle', 'healthy-after-negative', 'missing-wal-get',
                                              'corrupt-wal-get', 'auth-wal-get', 'reset-wal-get', 'tls-wal-get')):
                    if control.endswith('-wal-get'):
                        state = self.start(target)
                        self.same_segment_plan(state)
                        self.fatal_replay(state, name, control, 'same-segment-' + control)
                        continue
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

    def case_negative_controls_EOF_and_bundle_as_success(self):
        h = self.h
        with self.m.case('negative-controls-EOF-and-bundle-as-success'):
            # Exact registry dependency requires the same-segment negative first.
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            self.release(state['pod'], 'incorrect-EOF')
            with self.cleanup([('fault-reset', lambda: self.wal.control(''))]):
                self.wal.control('auth-wal-get', self.remote)
                self.release(state['pod'], 'release-response')
                self.barrier(state['pod'], 'cnpg-exited', 300)
                trace = [json.loads(x) for x in self.file(state['pod'], '/controller/campaign/rpc.jsonl').splitlines()]
                assert self.wal.control()['blocked'] > 0
                assert any(x.get('event') == 'actual-cnpg-exit' and x['exit'] == 0 for x in trace), 'unsafe EOF negative failed to reach false promotion'
            # Deliberately faulty proxy allowed data loss. Query the actual
            # promoted cluster: the positive independent LATEST oracle rejects.
            self.finish(state, BASE, True)
            self.event('distinguishing-EOF-negative', lost_ids=[2, 3, 4], actual_rows=BASE)

    def case_required_corrupt_no_latest_promotion(self):
        self.fault_case('required-corrupt-no-latest-promotion', 'corrupt-wal-get')

    def case_required_missing_no_latest_promotion(self):
        self.fault_case('required-missing-no-latest-promotion', 'missing-wal-get')

    def case_bundle_auth_fatal(self):
        self.fault_case('bundle-auth-fatal', 'auth-wal-get')

    def case_bundle_transport_fatal(self):
        self.fault_case('bundle-transport-fatal', 'reset-wal-get')

    def case_bundle_TLS_fatal(self):
        self.fault_case('bundle-TLS-fatal', 'tls-wal-get')

    def case_guard_before_RPC_same_PVC_no_mutation(self):
        self.ownership_stage('guard-before-RPC-same-PVC-no-mutation', 'before')

    def case_guard_delayed_response_same_PVC_no_mutation(self):
        self.ownership_stage('guard-delayed-response-same-PVC-no-mutation', 'response')

    def case_guard_replay_pause_same_PVC_no_mutation(self):
        self.ownership_stage('guard-replay-pause-same-PVC-no-mutation', 'replay')

    def case_guard_shutdown_pause_same_PVC_no_mutation(self):
        self.ownership_stage('guard-shutdown-pause-same-PVC-no-mutation', 'shutdown')

    def case_sidecar_death_poisons(self):
        self.crash_case('sidecar-death-poisons', 'cnpg-backup')

    def case_guard_death_poisons(self):
        self.crash_case('guard-death-poisons', 'full-recovery')

    def case_poison_fresh_Cluster_all_fresh_PVC_retry(self):
        h = self.h
        with self.m.case('poison-fresh-Cluster-all-fresh-PVC-retry'):
            self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)

    def case_detached_PG_descendants(self):
        h = self.h
        with self.m.case('detached-PG-descendants'):
            state = self.start({'backupID': self.base['backup_uid']}, hold_replay=True)
            self.materialize(state)
            self.no_retries(state)
            self.release(state['pod'], 'release-response')
            self.barrier(state['pod'], 'replay-held')
            before = json.loads(h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'stop-cnpg'))
            self.event('PostgreSQL-before-orphan-adoption', processes=before)
            self.barrier(state['pod'], 'cnpg-exited')
            cnpg = next(p for p in before if p['kind'] == 'cnpg')
            postgres_pids = {p['pid'] for p in before if p['kind'] == 'postgres'}
            orphans = []
            def adopted():
                nonlocal orphans
                after = json.loads(h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'processes'))
                self.event('PostgreSQL-orphan-adoption-observation', processes=after)
                orphans = [p for p in after if p['kind'] == 'postgres' and p['parent'] == 1 and p['pid'] in postgres_pids]
                return bool(orphans)
            # CNPG's pg_ctl can still parent the postmaster after CNPG exits.
            # Wait for actual adoption, not a guessed delay or CNPG's exit alone.
            h.wait(adopted, 'actual PostgreSQL orphan adoption', 30)
            assert any(p['group'] != cnpg['group'] or p['session'] != cnpg['session'] for p in orphans), 'detached PG process-group/session injection not established'
            assert self.markers(state) == ['present'] * 3
            self.replacement(state)
            self.event('actual-detached-PostgreSQL-descendants', before=before, adopted=orphans)
            self.release(state['pod'], 'release-shutdown')
            h.wait(lambda: self.main_terminated(state), 'guard reaps every main-namespace descendant', 60)
            assert self.markers(state) == ['absent'] * 3, 'same-sidecar clean drain failed after reaping'
            self.retire_target(state)

    def case_pending_sidecar_task_same_incarnation_drain(self):
        h = self.h
        WORK, OUT = h.WORK, h.OUT
        with self.m.case('pending-sidecar-task-same-incarnation-drain'):
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            self.no_retries(state)
            assert self.markers(state) == ['present'] * 3
            command = [str(WORK / 'kubectl'), '--kubeconfig', str(WORK / 'kubeconfig'), 'exec', '-n', TARGET,
                       state['pod'], '-c', 'full-recovery', '--', '/cnpg-backup/bin/cnpg-backup', 'wal-fetch',
                       '--plan', '/cnpg-backup/state/recovery.json', '--', self.remote, 'pg_wal/RECOVERYXLOG']
            paused = None
            def resume():
                if paused:
                    h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-CONT', str(paused))
            with self.cleanup([('fault-reset', resume), ('fault-reset', lambda: self.wal.control(''))]):
                self.wal.control('hold-wal-get-response', self.remote)
                with h.commands.background(state['name'] + '-pending-helper', *command, save_log=h.save_log, report=self.record_failure) as request:
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
                    result = request.result(timeout=30)
                    if result.timed_out or result.dropped_bytes:
                        result.require()
                    assert result.returncode != 0, 'canceled helper unexpectedly acknowledged a pending read'
            self.retire_target(state)

    def case_stale_tuple_rejected(self):
        h = self.h
        with self.m.case('stale-tuple-rejected'):
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            p = next(p for p in self.pods(state['name']) if p['metadata']['name'] == state['pod'])
            before = self.target_snapshot(p)
            text = h.kube('exec', '-n', TARGET, state['pod'], '-c', 'full-recovery', '--', '/controller/manager', 'stale')
            assert 'stale tuple rejected: FailedPrecondition' in text
            assert self.target_snapshot(p) == before
            self.finish(state, LATEST)

    def case_source_lifetime_and_reader_through_replay(self):
        h = self.h
        with self.m.case('source-lifetime-and-reader-through-replay'):
            state = self.start({'backupID': self.base['backup_uid']}, hold_replay=True)
            with self.cleanup([('fault-reset', lambda: self.wal.control(''))]):
                self.wal.control('hold-artifact-get-response')
                self.release(state['pod'], 'release-before')
                h.wait(lambda: self.wal.control()['blocked'] > 0, 'source input read before materialization response', 180)
                readers = [x for x in self.gate()['holders'] if x['target_cluster_uid'] == state['cluster_uid']]
                assert {x['kind'] for x in readers} == {'restore-lifetime', 'restore-reader'}
                assert len(readers) == 2
                self.event('protected-before-source-input-read-completes', holders=readers, proxy=self.wal.control())
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

    def case_controller_all_Job_retry_Pods_terminated(self):
        h = self.h
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
            self.finish(state, LATEST)
            recovery_pods = [p for p in state['completion_pods'] if any(c['name'] == 'full-recovery' for c in p['spec']['containers'])]
            assert len(recovery_pods) > 1, 'mandatory actual retry-Pod evidence disappeared'
            self.event('all-Job-attempts-terminated-before-completion-release', job_uid=job['metadata']['uid'],
                       pods=[h.pod_evidence(p) for p in recovery_pods])


    def case_seeded_XID_1(self):
        h = self.h
        self.seeded_xid(1)

    def case_seeded_XID_2(self):
        h = self.h
        self.seeded_xid(2)

    def seeded_xid(self, ordinal):
        h = self.h
        rng = random.Random(self.args.seed)
        exclusive = [bool(rng.getrandbits(1)) for _ in range(ordinal)][-1]
        with self.m.case('seeded-XID-' + str(ordinal)):
            self.event('seed-choice', ordinal=ordinal, exclusive=exclusive)
            self.restore({'backupID': self.base['backup_uid'], 'targetXID': self.target['xid'], 'exclusive': exclusive},
                         BEFORE if exclusive else INCLUSIVE, True)

    def ownership_stage(self, family, stage):
        h = self.h
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

    def crash_case(self, family, victim):
        h = self.h
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

    def fault_case(self, family, mode):
        h = self.h
        with self.m.case(family):
            state = self.start({'backupID': self.base['backup_uid']})
            self.materialize(state)
            requested = self.remote if family.startswith('required-') else sorted(state['plan']['bundled'])[0]
            self.fatal_replay(state, requested, mode, family)

    def temporal_case(self, family, field, value):
        h = self.h
        with self.m.case(family):
            for branch in self.variants(('inclusive', 'exclusive')):
                exclusive = branch == 'exclusive'
                self.restore({'backupID': self.base['backup_uid'], field: value, 'exclusive': exclusive},
                             BEFORE if exclusive else INCLUSIVE, True)


    def case_retirement_20(self):
        h = self.h
        with self.m.case('retirement-20'):
            for operation in range(20):
                previous = {holder['id'] for holder in self.gate()['holders']}
                if operation % 2 == 0:
                    self.reject({'backupID': self.newest['backup_uid'], 'targetLSN': self.target['commit_lsn']}, 'too-new-base')
                else:
                    self.restore({'backupID': self.base['backup_uid'], 'targetName': 'g_pre_drop'}, BEFORE, True)
                assert previous <= {holder['id'] for holder in self.gate()['holders']}, 'retirement erased an uncertain source holder'
                h.account(force=True)
                live = [a for a in h.allocations if a['state'] == 'mounted']
                assert len(live) <= 8, 'retired fixture backing accumulated beyond live workspace bound'
                self.event('retirement-stress-admitted-and-closed', operation=operation + 1, live_backing=len(live),
                           uncertain_holders=len(self.gate()['holders']), fresh_pvc_uids=self.targets[-1]['pvc_uids'])
