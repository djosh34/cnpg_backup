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
import subprocess
from pathlib import Path
import tarfile
import time
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo


def bounded_capture_workspaces(h, count=20):
    h.apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass', 'metadata': {'name': 'cnpg-backup-capture'},
             'provisioner': 'kubernetes.io/no-provisioner', 'volumeBindingMode': 'WaitForFirstConsumer'})
    for i in range(count):
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
                                'SIGTERM', 'process-death', 'actual-OOM', 'full-workspace', 'credentials',
                                'timeout-after-commit-idempotent-retry', 'WAL-under-transfer', 'metrics-alerts']
    report['full_subject_docker_id'] = h.run('docker', 'image', 'inspect', '--format={{.Id}}', 'cnpg-backup-foundation-pg18:test').strip()
    root = h.WORK / 'full-fixture'
    root.mkdir(mode=0o700)
    driver = root / 'backupverify'
    h.run('go', 'build', '-o', driver, './hack/backupverify')
    control = root / 'backupcontrol'
    h.run('go', 'build', '-o', control, './hack/backupcontrol')
    report['full_driver_sha256'] = hashlib.sha256(driver.read_bytes()).hexdigest()
    pod = wal.primary()
    wal.sql(pod, 'CREATE TABLE full_oracle(id integer PRIMARY KEY, value text) TABLESPACE fast_space; '
                 "INSERT INTO full_oracle SELECT n, md5(n::text) FROM generate_series(1,100) n; "
                 'CREATE TABLE full_capture_writes(id integer PRIMARY KEY); '
                 'CREATE TABLE full_load AS SELECT n, repeat(md5(n::text),128) payload FROM generate_series(1,5000) n;')
    expected = wal.sql(pod, "SELECT count(*)::text || ':' || md5(string_agg(id::text || ':' || value, ',' ORDER BY id)) FROM full_oracle")
    commits = []
    source_bounds = {}
    assert wal.sql(pod, 'SHOW log_timezone') == 'America/New_York'
    report['full_log_timezone'] = 'America/New_York'
    journal = []
    def acknowledge(value, capture_uid, native_active=False, after_native_capture=False):
        item = {'id': value, 'capture_uid': capture_uid, 'native_active': native_active,
                'after_native_capture': after_native_capture,
                'after_commit_lsn': wal.sql(pod, 'SELECT pg_current_wal_insert_lsn()'), 'time': time.time()}
        journal.append(item)
        (h.OUT / 'full-acknowledged-workload.json').write_text(json.dumps(journal, indent=2))
        return item
    wal_serviced = False
    for name, kind in [('full-demand', 'Backup'), ('full-schedule', 'ScheduledBackup')]:
        # The preceding full flushes almost every fixture page. Dirty the real
        # load again so each spread checkpoint exposes an observable native
        # phase, including the operator-created ScheduledBackup child.
        wal.sql(pod, "UPDATE full_load SET payload=repeat(md5(n::text || '" + name + "'),128)")
        if kind == 'Backup':
            wal.control('hold-artifact-put')
        before_capture = float(wal.sql(pod, 'SELECT extract(epoch FROM clock_timestamp())'))
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
                # Native backup may leave an empty new segment. Insert a real
                # record and derive the just-switched filename from switch LSN,
                # not a pre-switch boundary/current-LSN guess.
                wal.sql(pod, 'INSERT INTO full_capture_writes VALUES (900000)')
                uid = json.loads(h.kube('get', 'backup', name, '-n', h.NS, '-o', 'json'))['metadata']['uid']
                acknowledge(900000, uid, after_native_capture=True)
                segment = wal.sql(pod, 'SELECT pg_walfile_name(pg_switch_wal())')
                pending = {'case': 'WAL-under-transfer', 'partial_artifact_body_forwarded': True,
                           'requested_segment': segment, 'started_epoch': time.time()}
                report.setdefault('full_fault_preconditions', []).append(pending)
                (h.OUT / 'full-transfer-precondition.json').write_text(json.dumps(pending, indent=2))
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
            writes.append(acknowledge(value, b['metadata']['uid'], native_active=progress == '1'))
            (h.OUT / (name + '-workload.json')).write_text(json.dumps(writes, indent=2))
            return False
        h.wait(completed, 'actual durable full Backup completion', 600)
        assert any(w['native_active'] for w in writes), 'no committed workload write observed during native capture'
        uid = commits[-1]['metadata']['uid']
        source_bounds[uid] = (before_capture, float(wal.sql(pod, 'SELECT extract(epoch FROM clock_timestamp())')))
        assert commits[-1]['status']['backupId'] == uid
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
        start = datetime.datetime.fromisoformat(commit['started_at'])
        stop = datetime.datetime.fromisoformat(commit['stopped_at'])
        before, after = source_bounds[commit['backup_uid']]
        assert int(before) <= start.timestamp() <= stop.timestamp() <= after, 'source capture clocks disagree'
        label_time = next(line.removeprefix('START TIME: ') for line in commit['backup_label'].splitlines() if line.startswith('START TIME: '))
        assert label_time == start.astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z'), 'non-UTC label normalization differs from independent zoneinfo'
        report.setdefault('full_timezone_oracles', []).append({'backup_uid': commit['backup_uid'], 'label_time': label_time,
            'started_at': commit['started_at'], 'stopped_at': commit['stopped_at'], 'source_before_epoch': before, 'source_after_epoch': after})
        observation = verify_restore(h, wal, root, driver, data_image, index, commit, expected, journal)
        observations.append(observation)
        (h.OUT / 'full-download-oracles.json').write_text(json.dumps(observations, indent=2))
    report['full_completed'].append('S3-only-download-native-verification-SQL')
    report['full_remaining'].remove('S3-only-download-native-verification-SQL')
    credential_failure(h, wal, report, metrics)
    capture_faults(h, wal, report, metrics, control)
    assert not report['full_remaining'], 'mandatory F native/fault/observability cases incomplete'


def capture_faults(h, wal, report, metrics, control):
    pod = wal.primary()
    actor = '/var/lib/postgresql/data/full-fixture-control'
    cluster_file = '/var/lib/postgresql/data/full-fixture-cluster.json'
    backup_file = '/var/lib/postgresql/data/full-fixture-backup.json'
    def install_actor():
        command = [str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'), 'exec', '-i', '-n', h.NS,
                   pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > ' + actor + '; chmod 0555 ' + actor]
        installed = subprocess.run(command, input=control.read_bytes(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        assert installed.returncode == 0, 'test actor installation failed'
    install_actor()
    report['full_control_sha256'] = hashlib.sha256(control.read_bytes()).hexdigest()
    def act(action, *args, **kwargs):
        return h.kube('exec', '-n', h.NS, pod, '-c', 'cnpg-backup', '--', actor, action, *args, **kwargs)
    def backup(name):
        return json.loads(h.kube('get', 'backup/' + name, '-n', h.NS, '-o', 'json'))
    def failed(name):
        def terminal():
            b = backup(name)
            phase = b.get('status', {}).get('phase')
            if phase == 'completed':
                raise AssertionError('faulted native invocation falsely succeeded: ' + name)
            return phase == 'failed'
        h.wait(terminal, 'actual CNPG native failure: ' + name, 180)
        return backup(name)
    def start_native(name):
        # Dirty real pages make the native spread-checkpoint phase observable;
        # never replace the runtime command with a sleep or fake capture.
        wal.sql(pod, "UPDATE full_load SET payload=repeat(md5(n::text || '" + name + "'),128)")
        h.apply(definition(h, name))
        def active():
            b = backup(name)
            assert b.get('status', {}).get('phase') not in ('failed', 'completed'), 'native fault barrier missed'
            return wal.sql(pod, 'SELECT count(*) FROM pg_stat_progress_basebackup') == '1'
        h.wait(active, 'actual native pg_basebackup active: ' + name, 180)
        return backup(name)
    def restart_count():
        p = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
        return next(c['restartCount'] for c in p['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
    def restarted(before):
        # Repeated same-PVC deaths now reach kubelet's normal 300s maximum
        # CrashLoopBackOff. Bound the observation above that delay; still require
        # an actual new incarnation and probe, never treat elapsed time as one.
        h.wait(lambda: restart_count() > before, 'actual sidecar process restart', 360)
        h.wait(lambda: h.kube('exec', '-n', h.NS, pod, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup',
                              'instance', '--probe', check=False) == '', 'replacement native sidecar socket')
    def no_commit(uid):
        prefix = 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/backups/' + uid + '/commit.json'
        listing = ET.fromstring(wal.s3('GET', '?list-type=2&prefix=' + prefix))
        ns = {'s': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        assert listing.findtext('s:IsTruncated', namespaces=ns) == 'false'
        assert not listing.findall('s:Contents', ns), 'fault produced a selectable incomplete entry'
    for attempt, (signal, case) in enumerate([('TERM', 'SIGTERM'), ('KILL', 'process-death'),
                                            ('KILL', 'process-death'), ('KILL', 'process-death'), ('OOM', 'actual-OOM')]):
        before = metrics.snapshot('full')
        name = 'full-signal-' + signal.lower() + '-' + str(attempt)
        b = start_native(name)
        processes = json.loads(act('native'))
        assert processes, 'native subprocess not alive before signal'
        count = restart_count()
        postmaster = wal.sql(pod, 'SELECT pg_postmaster_start_time()')
        oom = None
        scratch = None
        remote_holders = None
        if signal == 'OOM':
            act('pause-native')
            output = act('oom', check=False, timeout=90)
            records = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
            assert len(records) == 1 and records[0]['bounded_cgroup_precondition'], 'actual bounded OOM precondition missing'
            oom = records[0]
        elif signal == 'KILL':
            # PID-namespace init ignores namespace-local SIGKILL. Use the same
            # independently scoped CRI/node actor as E, with native work paused
            # so API/CRI lookup cannot race past the actual capture phase.
            act('pause-native')
            scratch = json.loads(act('scratch'))
            assert len(scratch['roots']) == 2 and scratch['allocated_bytes'] > 0, 'actual owned capture/repository scratch absent'
            assert len(scratch['native_locks']) == len(processes), 'native lock lifetime not observed'
            assert all(p == '/cnpg-backup/work/native.lock' for p in scratch['native_locks'].values()), 'native child did not inherit workspace exclusion'
            remote_holders = [holder for holder in json.loads(wal.s3('GET', 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/gate.json'))['holders'] if holder['kind'] == 'backup']
            assert any(holder['operation_id'] == b['metadata']['uid'] for holder in remote_holders), 'capture holder not observed'
            observed = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
            sidecar = next(c for c in observed['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
            container_id = sidecar['containerID'].split('://')[1]
            pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', container_id))['info']['pid'])
            assert pid > 1
            h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-9', str(pid))
            report['full_kill_precondition'] = {'container_id': container_id, 'node_pid': pid, 'native_paused': True}
        else:
            act('signal-sidecar', signal, check=False)
        restarted(count)  # prove the fault actually hit before judging outcome
        failed(name)
        if signal in ('KILL', 'OOM'):
            observed = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
            last = next(c['lastState']['terminated'] for c in observed['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
            assert last['exitCode'] == 137, 'native sidecar was not killed'
            if signal == 'OOM':
                assert last['reason'] == 'OOMKilled', 'native sidecar was not actually OOM-killed'
                oom['termination'] = {k: last.get(k) for k in ('reason', 'exitCode', 'signal')}
        assert wal.sql(pod, 'SELECT pg_postmaster_start_time()') == postmaster, 'fault accidentally restarted source PostgreSQL'
        assert not json.loads(act('native')), 'native child survived sidecar process death'
        if scratch is not None:
            reclaimed = json.loads(act('scratch'))
            assert not reclaimed['roots'] and reclaimed['allocated_bytes'] == 0, 'dead native scratch accumulates after restart'
            after_holders = json.loads(wal.s3('GET', 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/gate.json'))['holders']
            assert all(holder in after_holders for holder in remote_holders), 'local cleanup erased remote uncertainty'
            report.setdefault('full_scratch_recovery', []).append({'attempt': attempt, 'before': scratch, 'after': reclaimed,
                                                                  'remote_holders_preserved': True})
        no_commit(b['metadata']['uid'])
        metrics.assert_failed(name, before, require_warning=False)
        report['full_fault_preconditions'].append({'case': case, 'signal': signal, 'native_pids_observed': processes,
                                                   'source_postmaster_continued': True, 'actual_OOM': oom})
        if case in report['full_remaining']:
            report['full_completed'].append(case)
            report['full_remaining'].remove(case)
    assert len(report['full_scratch_recovery']) == 3, 'repeated same-PVC death/reclaim regression incomplete'
    before = metrics.snapshot('full')
    b = start_native('full-workspace-exhaustion')
    paused = json.loads(act('pause-native'))
    try:
        filled = json.loads(act('fill-workspace'))
        # Successful native cancellation can already reclaim operation files
        # between the kernel ENOSPC and this observation; don't reject cleanup.
        assert filled['enospc'] and filled['allocated'] > 7 * 1024**3, 'bounded workspace did not actually reach ENOSPC'
        failed('full-workspace-exhaustion')
        assert not json.loads(act('native')), 'paused native writer was not killed/reaped at capacity limit'
        no_commit(b['metadata']['uid'])
        report['full_fault_preconditions'].append({'case': 'full-workspace', 'native_paused_pids': paused, **filled})
    finally:
        act('clear-workspace')
    metrics.assert_failed('full-workspace-exhaustion', before, require_warning=False)
    report['full_completed'].append('full-workspace')
    report['full_remaining'].remove('full-workspace')
    # Set the supported one-minute operation budget through ordinary CNPG
    # configuration rollout. The final fault must hit a real deadline after
    # durable commit, not be relabeled from another SIGTERM test.
    old_pods = set(h.pod_uids())
    h.kube('patch', 'repository', 'destination', '-n', h.NS, '--type=merge', '-p',
           json.dumps({'spec': {'io': {'operationTimeout': '1m', 'metadataTimeout': '1m'}}}))
    h.kube('annotate', 'cluster/database', '-n', h.NS, 'full-deadline-rollout=requested', '--overwrite')
    h.wait(lambda: len(h.pod_uids()) == 2 and old_pods.isdisjoint(h.pod_uids()), 'CNPG native operation-deadline rollout', 420)
    h.kube('wait', '-n', h.NS, '--for=condition=Ready', 'cluster/database', '--timeout=180s')
    pod = wal.primary()
    install_actor()
    # Lose the actual callback after MinIO has durably accepted commit.json.
    # This is a failed CNPG invocation AND successful historical publication.
    before = metrics.snapshot('full')
    name = 'full-commit-response-loss'
    wal.control('hold-commit-response')
    deadline_started = time.monotonic()
    try:
        original = start_native(name)
        h.kube('exec', '-i', '-n', h.NS, pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > ' + cluster_file,
               input=h.kube('get', 'cluster/database', '-n', h.NS, '-o', 'json'))
        h.kube('exec', '-i', '-n', h.NS, pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > ' + backup_file,
               input=json.dumps(original))
        h.wait(lambda: wal.control().get('blocked', 0) > 0, 'durable commit response held before callback result', 600)
        uid = original['metadata']['uid']
        published = publication_epoch(wal, uid)
        key = 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/backups/' + uid + '/commit.json'
        commit = json.loads(wal.s3('GET', key))
        assert backup(name)['status']['phase'] != 'completed'
        failed(name)  # real operationTimeout cancels the held response
        assert time.monotonic() - deadline_started >= 59, 'fault did not reach the configured native operation deadline'
        assert not json.loads(act('native')), 'deadline left a native child behind'
    finally:
        wal.control('')
    metrics.assert_failed(name, before, committed_after_loss=published, require_warning=False)
    retry = json.loads(act('backup', cluster_file, backup_file))
    assert retry['backup_id'] == uid and retry['begin_lsn'] == commit['start_lsn'] and retry['end_lsn'] == commit['stop_lsn']
    assert retry['started_at'] == int(datetime.datetime.fromisoformat(commit['started_at']).timestamp())
    assert retry['stopped_at'] == int(datetime.datetime.fromisoformat(commit['stopped_at']).timestamp())
    assert backup(name)['status']['phase'] == 'failed', 'test retry must not rewrite CNPG terminal status'
    assert json.loads(wal.s3('GET', key)) == commit, 'same-UID retry changed the durable winner'
    report['full_fault_preconditions'].append({'case': 'timeout-after-commit-idempotent-retry', 'durable_commit_before_response_loss': True,
        's3_publication_epoch': published, 'actual_operation_timeout_seconds': 60, 'same_UID_plugin_retry_not_CNPG_auto_retry': True})
    report['full_completed'].append('timeout-after-commit-idempotent-retry')
    report['full_remaining'].remove('timeout-after-commit-idempotent-retry')
    report['full_completed'].append('metrics-alerts')
    report['full_remaining'].remove('metrics-alerts')


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
    # Restoring the API Secret does not synchronously update kubelet's volume.
    # Require the next operation to see the actual valid native generation.
    h.wait(lambda: h.kube('exec', '-n', h.NS, pod, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup',
                          'instance', '--check-native', check=False) == '', 'actual restored native credential projection', 180)
    metrics.assert_failed('full-missing-credentials', before)
    report['full_completed'].append('credentials')
    report['full_remaining'].remove('credentials')


def verify_restore(h, wal, root, driver, data_image, index, commit, expected, journal):
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
    assert (base / 'backup_label').read_bytes() == commit['backup_label'].encode(), 'original native label bytes changed'
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
        def lsn(value):
            high, low = value.split('/')
            return (int(high, 16) << 32) | int(low, 16)
        stop = lsn(commit['stop_lsn'])
        required = {w['id'] for w in journal if lsn(w['after_commit_lsn']) <= stop}
        during = {w['id'] for w in journal if w['capture_uid'] == commit['backup_uid'] and w['native_active']
                  and lsn(w['after_commit_lsn']) <= stop}
        excluded = {w['id'] for w in journal if w['capture_uid'] == commit['backup_uid'] and w['after_native_capture']}
        recovered = {int(n) for n in query("SELECT id FROM full_capture_writes ORDER BY id").splitlines()}
        assert during, 'no acknowledged native-phase transaction precedes captured stop LSN'
        assert required <= recovered, 'downloaded recovery lost acknowledged capture-time transactions'
        assert not excluded.intersection(recovered), 'oracle accidentally used later source data or later WAL'
        return {'backup_uid': commit['backup_uid'], 'manifest_sha256': commit['manifest_sha256'], 'sql_oracle': actual,
                'native_range': native['WAL-Ranges'], 'missing_bootstrap_wal_rejected': True,
                'input_integrity_verified': True, 'acknowledged_capture_ids_required': sorted(required),
                'native_phase_committed_ids': sorted(during), 'post_capture_ids_excluded': sorted(excluded),
                'recovered_write_ids': sorted(recovered), 'post_backup_PITR_claim': False}
    finally:
        h.save_log(container + '.log', h.run('docker', 'logs', container, check=False))
        h.run('docker', 'rm', '-f', container, check=False)
