"""F's real CNPG Backup/ScheduledBackup and downloaded-full SQL oracle.

Only disposable test inputs are used. This is not G materialization/PITR, and a
successful short profile is never release qualification.
"""
import gzip
import datetime
from backup_metrics_smoke import BackupMetricsSmoke
import hashlib
import json
import os
import shutil
from pathlib import Path
import tarfile
import time
import xml.etree.ElementTree as ET


def bounded_capture_workspaces(h):
    h.apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'cnpg-backup-capture'},
             'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
    for i in range(20):
        path = f'/var/local/cnpg-backup-capture-{i}'
        h.save_log(f'capture-finite-fs-{i}.log', h.provision_filesystem(path, '8G'))
        h.apply({'apiVersion': 'v1', 'kind': 'PersistentVolume', 'metadata': {'name': f'cnpg-backup-capture-{i}'},
                 'spec': {'capacity': {'storage': '8Gi'}, 'accessModes': ['ReadWriteOnce'], 'volumeMode': 'Filesystem',
                          'storageClassName': 'cnpg-backup-capture', 'persistentVolumeReclaimPolicy': 'Retain', 'local': {'path': path},
                          'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [
                              {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': [h.NAME + '-control-plane']}]}]}}}})


def definition(h, name, kind='Backup', backup_type='full'):
    d = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': kind, 'metadata': {'name': name, 'namespace': h.NS},
         'spec': {'cluster': {'name': 'database'}, 'method': 'plugin', 'target': 'primary',
                  'pluginConfiguration': {'name': 'cnpg-backup.djosh34.github.io', 'parameters': {'backupType': backup_type}}}}
    if kind == 'ScheduledBackup':
        d['spec'].update({'schedule': '0 0 0 1 1 *', 'immediate': True, 'backupOwnerReference': 'none'})
    return d


def run(h, wal, report, data_image):
    metrics = BackupMetricsSmoke(h, report, 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa').start()
    try:
        _run(h, wal, report, data_image, metrics)
    finally:
        metrics.close()


def publication_epoch(wal, uid):
    key = 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/backups/' + uid + '/commit.json'
    listing = ET.fromstring(wal.s3('GET', '?list-type=2&prefix=' + key))
    ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    assert listing.findtext('s:IsTruncated', namespaces=ns) == 'false'
    entries = listing.findall('s:Contents', ns)
    assert len(entries) == 1 and entries[0].findtext('s:Key', namespaces=ns) == key
    return datetime.datetime.fromisoformat(entries[0].findtext('s:LastModified', namespaces=ns)).timestamp()


def _run(h, wal, report, data_image, metrics):
    report['full_completed'] = []
    report['full_remaining'] = ['on-demand', 'scheduled', 'S3-only-download-native-verification-SQL',
                                'SIGTERM', 'OOM-process-death', 'full-workspace', 'credentials',
                                'timeout-after-commit-idempotent-retry', 'WAL-under-transfer', 'metrics-alerts']
    report['full_subject_docker_id'] = h.run('docker', 'image', 'inspect', '--format={{.Id}}', 'cnpg-backup-foundation-pg18:test').strip()
    root = h.WORK / 'full-fixture'
    root.mkdir(mode=0o700)
    driver = root / 'backupverify'
    h.run('go', 'build', '-o', driver, './hack/backupverify')
    report['full_driver_sha256'] = hashlib.sha256(driver.read_bytes()).hexdigest()
    pod = wal.primary()
    wal.sql(pod, 'CREATE TABLE full_oracle(id integer PRIMARY KEY, value text) TABLESPACE fast_space; '
                 "INSERT INTO full_oracle SELECT n, md5(n::text) FROM generate_series(1,100) n; "
                 'CREATE TABLE full_capture_writes(id integer PRIMARY KEY); '
                 'CREATE TABLE full_load AS SELECT n, repeat(md5(n::text),128) payload FROM generate_series(1,5000) n;')
    expected = wal.sql(pod, "SELECT count(*)::text || ':' || md5(string_agg(id::text || ':' || value, ',' ORDER BY id)) FROM full_oracle")
    commits = []
    wal_serviced = False
    for name, kind in [('full-demand', 'Backup'), ('full-schedule', 'ScheduledBackup')]:
        if kind == 'Backup':
            wal.control('hold-artifact-put')
        h.apply(definition(h, name, kind))
        if kind == 'ScheduledBackup':
            def scheduled():
                items = json.loads(h.kube('get', 'backups', '-n', h.NS, '-o', 'json'))['items']
                return any(b['metadata']['name'].startswith(name + '-') for b in items)
            h.wait(scheduled, 'actual CNPG ScheduledBackup invocation', 90)
            items = json.loads(h.kube('get', 'backups', '-n', h.NS, '-o', 'json'))['items']
            name = next(b['metadata']['name'] for b in items if b['metadata']['name'].startswith(name + '-'))
        writes = []
        def completed():
            nonlocal wal_serviced
            if kind == 'Backup' and not wal_serviced and wal.control().get('blocked', 0) > 0:
                started = time.monotonic()
                segment = wal.sql(pod, 'SELECT pg_walfile_name(pg_current_wal_lsn())')
                wal.sql(pod, 'SELECT pg_switch_wal()')
                h.wait(lambda: wal.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + segment + ".done'") == '1',
                       'WAL acknowledgment while real artifact transfer is blocked', 90)
                assert wal.control()['blocked'] > 0, 'artifact barrier disappeared before WAL completion'
                wal.verify(pod, segment)
                report.setdefault('full_fault_preconditions', []).append({'case': 'WAL-under-transfer', 'partial_artifact_body_forwarded': True,
                    'durably_archived_segment': segment, 'wal_duration_seconds': time.monotonic() - started})
                wal.control('')
                wal_serviced = True
                report['full_completed'].append('WAL-under-transfer')
                report['full_remaining'].remove('WAL-under-transfer')
            b = json.loads(h.kube('get', 'backup', name, '-n', h.NS, '-o', 'json'))
            state = b.get('status', {}).get('phase')
            if state == 'failed':
                h.save_log(name + '-failed.json', json.dumps(b.get('status', {})))
                raise AssertionError('actual full Backup failed: ' + name)
            if state == 'completed':
                commits.append(b)
                return True
            progress = wal.sql(pod, 'SELECT count(*) FROM pg_stat_progress_basebackup')
            value = len(writes) + (100000 if kind == 'ScheduledBackup' else 1)
            wal.sql(pod, f'INSERT INTO full_capture_writes VALUES ({value})')
            writes.append({'id': value, 'native_active': progress == '1', 'time': time.time()})
            (h.OUT / (name + '-workload.json')).write_text(json.dumps(writes, indent=2))
            return False
        h.wait(completed, 'actual durable full Backup completion', 600)
        assert any(w['native_active'] for w in writes), 'no committed workload write observed during native capture'
        assert commits[-1]['status']['backupId'] == commits[-1]['metadata']['uid']
        metrics.assert_committed(publication_epoch(wal, commits[-1]['metadata']['uid']))
        report['full_completed'].append('on-demand' if kind == 'Backup' else 'scheduled')
        report['full_remaining'].remove(report['full_completed'][-1])
    assert wal_serviced, 'real artifact-transfer/WAL overlap barrier never fired'
    # Remove Kubernetes Backup objects before authoritative S3-only enumeration.
    for b in commits:
        h.kube('delete', 'backup', b['metadata']['name'], '-n', h.NS)
    listing = ET.fromstring(wal.s3('GET', '?list-type=2&prefix=smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/backups/'))
    ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    assert listing.findtext('s:IsTruncated', namespaces=ns) == 'false'
    keys = [v.text for v in listing.findall('s:Contents/s:Key', ns) if v.text.endswith('/commit.json')]
    assert len(keys) == 2, 'S3 catalog not exactly two complete full entries'
    observations = []
    for index, key in enumerate(keys):
        commit = json.loads(wal.s3('GET', key))
        observation = verify_restore(h, wal, root, driver, data_image, index, commit, expected)
        observations.append(observation)
        (h.OUT / 'full-download-oracles.json').write_text(json.dumps(observations, indent=2))
    report['full_completed'].append('S3-only-download-native-verification-SQL')
    report['full_remaining'].remove('S3-only-download-native-verification-SQL')
    credential_failure(h, wal, report, metrics)


def credential_failure(h, wal, report, metrics):
    import base64
    before = metrics.snapshot('full')
    pod = wal.primary()
    attempted = False
    try:
        h.kube('patch', 'secret', 's3-auth', '-n', h.NS, '--type=merge', '-p', json.dumps({'data': {'secret': ''}}))
        def invalid_projection():
            result = h.kube('exec', '-n', h.NS, pod, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup',
                            'instance', '--check-native', check=False)
            return 'invalid credential snapshot' in result
        h.wait(invalid_projection, 'actual missing-credential native projection')
        report.setdefault('full_fault_preconditions', []).append({'case': 'credentials', 'actual_invalid_projection': True})
        h.apply(definition(h, 'full-missing-credentials'))
        attempted = True
        def failed():
            b = json.loads(h.kube('get', 'backup/full-missing-credentials', '-n', h.NS, '-o', 'json'))
            if b.get('status', {}).get('phase') == 'completed':
                raise AssertionError('missing credentials falsely succeeded')
            return b.get('status', {}).get('phase') == 'failed'
        h.wait(failed, 'actual CNPG failed full invocation')
    finally:
        h.kube('patch', 'secret', 's3-auth', '-n', h.NS, '--type=merge', '-p', json.dumps(
            {'data': {'secret': base64.b64encode(b'disposable-test-only-secret').decode()}}))
    assert attempted
    metrics.assert_failed('full-missing-credentials', before)
    report['full_completed'].append('credentials')
    report['full_remaining'].remove('credentials')


def verify_restore(h, wal, root, driver, data_image, index, commit, expected):
    directory = root / str(index)
    tar_dir, base, wal_dir = directory / 'tar', directory / 'base', directory / 'wal'
    for d in (tar_dir, base, wal_dir):
        d.mkdir(parents=True, mode=0o700)
    prefix = 'smoke/v1/' + commit['repository_id'] + '/backups/' + commit['backup_uid'] + '/attempts/' + commit['attempt_id'] + '/'
    manifest = tar_dir / 'backup_manifest'
    wal.s3('GET', prefix + 'manifest.pg.json', manifest)
    assert manifest.stat().st_size == commit['manifest_bytes']
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == commit['manifest_sha256']
    native = json.loads(manifest.read_text())
    assert len(native['WAL-Ranges']) == 1
    ts = []
    for artifact in commit['artifacts']:
        stored = directory / (str(artifact['index']) + '.stored')
        suffix = '.tar.gz' if artifact['compression'] == 'gzip' else '.tar'
        wal.s3('GET', prefix + 'data/' + str(artifact['index']) + suffix, stored)
        data = stored.read_bytes()
        assert len(data) == artifact['stored_bytes'] and hashlib.sha256(data).hexdigest() == artifact['stored_sha256']
        raw = gzip.decompress(data) if artifact['compression'] == 'gzip' else data
        assert len(raw) == artifact['raw_bytes'] and hashlib.sha256(raw).hexdigest() == artifact['raw_sha256']
        name = {'base': 'base.tar', 'wal': 'pg_wal.tar'}.get(artifact['role'], str(artifact['tablespace_oid']) + '.tar')
        archive = tar_dir / name
        archive.write_bytes(raw)
        destination = base if artifact['role'] == 'base' else wal_dir
        if artifact['role'] == 'tablespace':
            destination = directory / ('ts-' + str(artifact['tablespace_oid']))
            destination.mkdir(mode=0o700)
            ts.append((artifact['tablespace_oid'], destination))
        with tarfile.open(archive) as reader:
            # Independent test extractor, only regulars/directories. Product
            # confinement was exercised by capture's Go scan before publication.
            for member in reader:
                assert member.isfile() or member.isdir()
                assert not member.name.startswith('/') and '..' not in Path(member.name).parts
                reader.extract(member, destination, filter='data')
    def subject(*args, expected_failure=False):
        return h.run('docker', 'run', '--rm', '--network=none', '--user', str(os.getuid()),
                     '-v', str(directory) + ':/input', '-v', str(driver) + ':/verify:ro',
                     '--entrypoint', args[0], 'cnpg-backup-foundation-pg18:test', *args[1:], expect_failure=expected_failure)
    subject('/usr/lib/postgresql/18/bin/pg_verifybackup', '--exit-on-error', '--no-parse-wal', '/input/tar')
    subject('/verify', '/input/tar/backup_manifest', '/input/wal')
    segment = next(p for p in wal_dir.iterdir() if len(p.name) == 24)
    hidden = directory / 'withheld-wal'
    segment.rename(hidden)
    try:
        subject('/verify', '/input/tar/backup_manifest', '/input/wal', expected_failure=True)
    finally:
        hidden.rename(segment)
    # Verified original input is now transformed only inside the disposable SQL
    # oracle: trusted tablespace links, separate WAL and replacement configs.
    for oid, destination in ts:
        (base / 'pg_tblspc' / str(oid)).symlink_to('/input/' + destination.name)
    (base / 'tablespace_map').unlink(missing_ok=True)
    shutil.rmtree(base / 'pg_wal')
    (base / 'pg_wal').symlink_to('/input/wal')
    (base / 'postgresql.auto.conf').write_text('')
    (directory / 'oracle.conf').write_text("listen_addresses=''\nunix_socket_directories='/input'\nport=5544\nssl=off\narchive_mode=off\nshared_preload_libraries=''\nhba_file='/input/pg_hba.conf'\nident_file='/input/pg_ident.conf'\n")
    (directory / 'pg_hba.conf').write_text('local all all trust\n')
    (directory / 'pg_ident.conf').write_text('')
    # The independent oracle uses CNPG's known PostgreSQL UID, not an absent
    # runner UID in the database image's passwd database. This test-only chown
    # touches only this disposable downloaded fixture, after native verification.
    base.chmod(0o700)
    h.run('docker', 'run', '--rm', '--network=none', '--user', '0', '-v', str(directory) + ':/input',
          '--entrypoint', '/bin/chown', h.LOCK['database'], '-R', '26:26', '/input')
    container = 'cnpg-full-oracle-' + str(index)
    try:
        h.run('docker', 'run', '-d', '--name', container, '--network=none', '--user', '26',
              '-v', str(directory) + ':/input', '--entrypoint', '/usr/lib/postgresql/18/bin/postgres', h.LOCK['database'],
              '-D', '/input/base', '-c', 'config_file=/input/oracle.conf')
        def query(sql, check=True):
            return h.run('docker', 'exec', container, '/usr/lib/postgresql/18/bin/psql', '-XAt', '-h', '/input', '-p', '5544',
                         '-U', 'postgres', '-d', 'postgres', '-c', sql, check=check).strip()
        h.wait(lambda: query('SELECT pg_is_in_recovery()', False) == 'f', 'downloaded full SQL recovery', 60)
        actual = query("SELECT count(*)::text || ':' || md5(string_agg(id::text || ':' || value, ',' ORDER BY id)) FROM full_oracle")
        assert actual == expected, 'downloaded native full tablespace SQL oracle mismatch'
        assert query("SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname='fast_space'").startswith('/input/ts-')
        return {'backup_uid': commit['backup_uid'], 'manifest_sha256': commit['manifest_sha256'], 'sql_oracle': actual,
                'native_range': native['WAL-Ranges'], 'missing_bootstrap_wal_rejected': True,
                'input_integrity_verified': True, 'post_backup_PITR_claim': False}
    finally:
        h.save_log(container + '.log', h.run('docker', 'logs', container, check=False))
        h.run('docker', 'rm', '-f', container, check=False)
