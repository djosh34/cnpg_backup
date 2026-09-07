"""E's actual CNPG/MinIO WAL extension. No replacement product data paths."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

SCENARIOS = ('actual-cnpg-minio-segment-byte-oracle-and-duplicate-conflict',
             'actual-forced-failover-timeline-history-destination-routing',
             'actual-killed-partial-upload-no-ack-and-retry',
             'actual-transient-and-TLS-are-not-NotFound',
             'actual-finite-WAL-filesystem-full-restore-no-success',
             'actual-WAL-lag-failure-observability')

class WALFixture:
    def __init__(self, h, report):
        self.h, self.report, self.forward = h, report, None
        self.metrics_forward = None
        self.directory = h.WORK / 'wal-fixture'
        self.directory.mkdir(mode=0o700)
        report['wal_completed'] = []
        report['wal_remaining'] = list(SCENARIOS)

    def completed(self, name):
        assert name in SCENARIOS and name not in self.report['wal_completed']
        self.report['wal_completed'].append(name)
        self.report['wal_remaining'] = [x for x in SCENARIOS if x not in self.report['wal_completed']]

    def setup(self):
        h, d = self.h, self.directory
        lock = json.loads((h.ROOT / 'build/inputs.lock.json').read_text())
        cache = Path(os.environ.get('CNPG_BUILD_CACHE', h.ROOT / '.work/tools'))
        binary = cache / 'downloads/minio'
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == lock['minio']['sha256']
        shutil.copy2(binary, d / 'minio')
        (d / 'Dockerfile').write_text('FROM scratch\nCOPY minio /minio\nENTRYPOINT ["/minio"]\n')
        h.run('docker', 'build', '-t', 'cnpg-backup-wal-minio:test', d)
        image = h.image_digest('minio', 'cnpg-backup-wal-minio:test')
        self.report['wal_minio_image'] = image
        self.report['wal_minio_binary'] = lock['minio']
        h.run('go', 'build', '-o', d / 'wal-client', './hack/walclient')
        h.run('go', 'build', '-o', d / 'wal-proxy', './hack/walproxy')
        (d / 'Proxyfile').write_text('FROM scratch\nCOPY wal-proxy /wal-proxy\nENTRYPOINT ["/wal-proxy"]\n')
        h.run('docker', 'build', '-f', d / 'Proxyfile', '-t', 'cnpg-backup-wal-proxy:test', d)
        proxy_image = h.image_digest('walproxy', 'cnpg-backup-wal-proxy:test')
        self.report['wal_test_proxy_image'] = proxy_image
        # Generated keys remain only in this private work directory/Kubernetes
        # Secret, never in the built image, reports or test command diagnostics.
        h.run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2', '-subj', '/CN=WAL fixture CA',
              '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', d / 'ca.key', '-out', d / 'ca.crt')
        h.run('openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=minio', '-keyout', d / 'private.key', '-out', d / 'server.csr')
        (d / 'extensions').write_text('subjectAltName=DNS:minio.' + h.NS + '.svc,DNS:minio-backend.' + h.NS + '.svc,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
        h.run('openssl', 'x509', '-req', '-in', d / 'server.csr', '-CA', d / 'ca.crt', '-CAkey', d / 'ca.key',
              '-CAcreateserial', '-days', '2', '-extfile', d / 'extensions', '-out', d / 'public.crt')
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'wal-minio-tls', 'namespace': h.NS},
                 'stringData': {'public.crt': (d / 'public.crt').read_text(), 'private.key': (d / 'private.key').read_text(), 'ca.crt': (d / 'ca.crt').read_text()}})
        h.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'wal-minio-ca', 'namespace': h.NS},
                 'data': {'ca.crt': (d / 'ca.crt').read_text()}})
        h.apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'wal-minio', 'namespace': h.NS, 'labels': {'app': 'wal-minio'}},
                 'spec': {'securityContext': {'runAsUser': 26, 'runAsGroup': 26, 'fsGroup': 26},
                          'containers': [{'name': 'minio', 'image': image, 'args': ['server', '--address', ':9000', '--certs-dir', '/certs', '/data'],
                                          'env': [{'name': 'MINIO_ROOT_USER', 'valueFrom': {'secretKeyRef': {'name': 's3-auth', 'key': 'access'}}},
                                                  {'name': 'MINIO_ROOT_PASSWORD', 'valueFrom': {'secretKeyRef': {'name': 's3-auth', 'key': 'secret'}}},
                                                  {'name': 'MINIO_BROWSER', 'value': 'off'}],
                                          'resources': {'limits': {'memory': '512Mi', 'cpu': '1'}},
                                          'volumeMounts': [{'name': 'data', 'mountPath': '/data'}, {'name': 'tls', 'mountPath': '/certs', 'readOnly': True}]}],
                          'volumes': [{'name': 'data', 'emptyDir': {'sizeLimit': '2Gi'}}, {'name': 'tls', 'secret': {'secretName': 'wal-minio-tls'}}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'minio-backend', 'namespace': h.NS},
                 'spec': {'selector': {'app': 'wal-minio'}, 'ports': [{'port': 9000, 'targetPort': 9000}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'wal-proxy', 'namespace': h.NS, 'labels': {'app': 'wal-proxy'}},
                 'spec': {'containers': [{'name': 'proxy', 'image': proxy_image,
                                          'env': [{'name': 'FIXTURE_BACKEND', 'value': 'https://minio-backend.' + h.NS + '.svc:9000'}],
                                          'resources': {'limits': {'cpu': '1', 'memory': '128Mi'}},
                                          'volumeMounts': [{'name': 'tls', 'mountPath': '/certs', 'readOnly': True}]}],
                          'volumes': [{'name': 'tls', 'secret': {'secretName': 'wal-minio-tls'}}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'minio', 'namespace': h.NS},
                 'spec': {'selector': {'app': 'wal-proxy'}, 'ports': [{'port': 9000, 'targetPort': 9000}]}})
        for pod in ('wal-minio', 'wal-proxy'):
            h.kube('wait', 'pod/' + pod, '-n', h.NS, '--for=condition=Ready', '--timeout=120s')
        log = open(d / 'forward.log', 'w')
        self.forward = subprocess.Popen([str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'),
                                        'port-forward', '-n', h.NS, 'service/minio', '19000:9000'], stdout=log, stderr=log)
        log.close()
        # Use curl's maintained signer as the independent remote byte oracle.
        # D's synthetic throwaway credential values are never production inputs.
        config = d / 'curl-private.conf'
        config.write_text('user = "disposable-test-only-access:disposable-test-only-secret"\naws-sigv4 = "aws:amz:us-east-1:s3"\nnoproxy = "*"\n')
        config.chmod(0o600)
        def ready():
            result = h.run('curl', '-q', '--silent', '--show-error', '--write-out', '%{http_code}', '--max-time', '3', '--cacert', d / 'ca.crt',
                           'https://localhost:19000/minio/health/ready', check=False)
            return self.forward.poll() is None and result == '200'
        h.wait(ready, 'MinIO TLS port forward')
        # Exact uninitialized503 is the only bucket-startup retry. Preserve the
        # first failure text; unrelated auth/transport failures are not retried.
        for attempt in range(30):
            result = self.s3('PUT', '', check=False)
            if 'XMinioServerNotInitialized' not in result:
                assert '<Error>' not in result and 'curl:' not in result, 'MinIO bucket setup rejected'
                break
            if attempt == 0:
                h.save_log('wal-minio-first-startup-response.log', result)
            time.sleep(.1)
        else:
            raise AssertionError('MinIO initialization deadline')
        return {'endpoint': 'https://minio.' + h.NS + '.svc:9000', 'caConfigMap': {'name': 'wal-minio-ca', 'key': 'ca.crt'}}

    def s3(self, method, key, dest=None, **kwargs):
        d = self.directory
        args = ['curl', '-q', '--silent', '--show-error', '--fail-with-body', '--max-time', '15',
                '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '-X', method]
        if dest:
            args += ['--output', dest]
        return self.h.run(*args, 'https://localhost:19000/test-bucket/' + key, **kwargs)

    def sql(self, pod, query):
        return self.h.kube('exec', '-n', self.h.NS, pod, '-c', 'postgres', '--', 'psql', '-XAt', '-U', 'postgres', '-d', 'postgres', '-c', query).strip()

    def primary(self):
        return json.loads(self.h.kube('get', 'cluster/database', '-n', self.h.NS, '-o', 'json'))['status']['currentPrimary']

    def command(self, pod, verb, *args, **kwargs):
        return self.h.kube('exec', '-n', self.h.NS, pod, '-c', 'postgres', '--', '/controller/manager', verb, *args, **kwargs)

    def shell(self, pod, script):
        # Test arrangement in upstream PG image only; product shell-free sidecar
        # still executes its exact compiled Archive/Restore paths over Unix RPC.
        return self.h.kube('exec', '-n', self.h.NS, pod, '-c', 'postgres', '--', 'sh', '-ec', 'cd /var/lib/postgresql/data/pgdata; ' + script)

    def verify(self, pod, name):
        h = self.h
        path = 'pg_wal/' + name
        before = self.sql(pod, "SELECT md5(pg_read_binary_file('" + path + "'))")
        remote = self.directory / name
        key = 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/wal/' + name[:8] + '/' + name
        self.s3('GET', key, remote)
        data = remote.read_bytes()
        if data[:2] == b'\x1f\x8b':
            data = gzip.decompress(data)
        assert hashlib.md5(data).hexdigest() == before, 'independent actual remote WAL bytes differ'
        self.command(pod, 'wal-restore', name, 'pg_wal/RECOVERYXLOG')
        restored = self.sql(pod, "SELECT md5(pg_read_binary_file('pg_wal/RECOVERYXLOG'))")
        assert restored == before, 'actual plugin Restore returned different bytes'
        self.report.setdefault('wal_byte_oracles', []).append({'name': name, 'raw_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(), 'pod': pod})

    def install_driver(self, pod):
        h = self.h
        command = [str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'), 'exec', '-i', '-n', h.NS,
                   pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > /run/wal-client; chmod 0555 /run/wal-client']
        result = subprocess.run(command, input=(self.directory / 'wal-client').read_bytes(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        assert result.returncode == 0, 'test-only RPC driver installation failed'
        definition = h.kube('get', 'cluster/database', '-n', h.NS, '-o', 'json')
        h.kube('exec', '-i', '-n', h.NS, pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > /run/wal-cluster.json', input=definition)
        self.report['wal_test_driver_sha256'] = hashlib.sha256((self.directory / 'wal-client').read_bytes()).hexdigest()

    def rpc(self, pod, verb, name, expect=None):
        arguments = ['/run/wal-client', '/run/wal-cluster.json', verb]
        arguments += ['/var/lib/postgresql/data/pgdata/pg_wal/' + name] if verb == 'archive' else [name, '/var/lib/postgresql/data/pgdata/pg_wal/RECOVERYXLOG']
        result = self.h.kube('exec', '-n', self.h.NS, pod, '-c', 'postgres', '--', *arguments,
                             expect_failure=expect is not None)
        if expect is not None:
            assert expect in result and (expect == 'NotFound' or 'NotFound' not in result), 'fault was misclassified: ' + result
        return result

    def control(self, mode=None):
        args = ['curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '5', '--cacert', self.directory / 'ca.crt']
        if mode is not None:
            args += ['-X', 'POST']
        return json.loads(self.h.run(*args, 'https://localhost:19000/fixture-control' + ('?mode=' + mode if mode is not None else '')))

    def metrics(self, pod):
        h = self.h
        if self.metrics_forward is None:
            log = open(self.directory / 'metrics-forward.log', 'w')
            self.metrics_forward = subprocess.Popen([str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'),
                                                    'port-forward', '-n', h.NS, 'pod/' + pod, '19187:9187'], stdout=log, stderr=log)
            log.close()
        result = h.run('curl', '-q', '--silent', '--show-error', '--max-time', '5', 'http://localhost:19187/metrics', check=False)
        return [line for line in result.splitlines() if line.startswith(('cnpg_wal_archive_', 'cnpg_pg_stat_archiver_'))]

    def fault_matrix(self):
        h = self.h
        pod = self.primary()
        self.install_driver(pod)
        self.rpc(pod, 'restore', '7FFFFFFE.history', expect='NotFound')
        failed_before = int(self.sql(pod, 'SELECT failed_count FROM pg_stat_archiver'))
        self.report['wal_failures_before_kill'] = failed_before
        self.control('hold-wal-put')
        try:
            self.sql(pod, 'INSERT INTO wal_e_oracle VALUES (1001)')
            name = self.sql(pod, 'SELECT pg_walfile_name(pg_current_wal_insert_lsn())')
            self.sql(pod, 'SELECT pg_switch_wal()')
            h.wait(lambda: self.control()['blocked'] > 0, 'real partial WAL PUT fault barrier')
            assert self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '0', 'premature archive acknowledgment'
            self.report['wal_kill_precondition'] = {'name': name, 'proxy': self.control(), 'done': False}
            def lag_visible():
                lines = self.metrics(pod)
                self.report['wal_metrics_pending'] = lines
                return any(line.startswith('cnpg_wal_archive_ready_count') and float(line.split()[-1]) > 0 for line in lines) and any(
                    line.startswith('cnpg_wal_archive_oldest_ready_seconds') and float(line.split()[-1]) > 0 for line in lines)
            h.wait(lag_visible, 'actual CNPG exporter observes WAL backlog and lag', 60)
            observed = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
            sidecar = next(c for c in observed['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
            container_id = sidecar['containerID'].split('://')[1]
            pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', container_id))['info']['pid'])
            assert pid > 1
            h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-9', str(pid))
            def restarted():
                p = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
                s = next(c for c in p['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
                return s['restartCount'] > sidecar['restartCount'] and s.get('ready', False)
            h.wait(restarted, 'killed upload sidecar starts fresh incarnation')
            assert self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '0', 'killed upload acknowledged while fault remains'
        finally:
            self.control('')
        h.wait(lambda: self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '1', 'actual retry after killed partial upload', 180)
        self.verify(pod, name)
        self.completed(SCENARIOS[2])
        h.wait(lambda: int(self.sql(pod, 'SELECT failed_count FROM pg_stat_archiver')) > failed_before, 'PostgreSQL failure counter observes killed upload')
        self.report['wal_metrics_after_retry'] = self.metrics(pod)
        assert any(line.startswith('cnpg_pg_stat_archiver_failed_count') and float(line.split()[-1]) > failed_before for line in self.report['wal_metrics_after_retry'])
        self.completed(SCENARIOS[5])
        self.control('fail-wal-get')
        try:
            result = self.rpc(pod, 'restore', name, expect='Unavailable')
            h.save_log('wal-transient-not-EOF.log', result)
        finally:
            self.control('')
        # Rotate only the public S3 trust projection to another valid CA. No
        # insecure transport, malformed-PEM substitute or production credential.
        d = self.directory
        h.run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2', '-subj', '/CN=Unrelated WAL test CA',
              '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', d / 'unrelated.key', '-out', d / 'unrelated.crt')
        before_tls = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
        sidecar_before_tls = next(c['containerID'] for c in before_tls['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
        try:
            h.kube('patch', 'configmap', 'wal-minio-ca', '-n', h.NS, '--type=merge', '-p', json.dumps({'data': {'ca.crt': (d / 'unrelated.crt').read_text()}}))
            def rejected_tls():
                result = h.kube('exec', '-n', h.NS, pod, '-c', 'postgres', '--', '/run/wal-client', '/run/wal-cluster.json',
                                'restore', name, '/var/lib/postgresql/data/pgdata/pg_wal/RECOVERYXLOG', check=False)
                if 'Unavailable' in result:
                    h.save_log('wal-TLS-not-EOF.log', result)
                    return True
                assert result.strip() == 'OK', 'trust fault returned wrong category'
                return False
            h.wait(rejected_tls, 'current invalid S3 trust fails actual WAL callback', 120)
            self.rpc(pod, 'archive', name, expect='Unavailable')
            # Distinguish TLS rejection from an unrelated process/network outage.
            during_tls = json.loads(h.kube('get', 'pod', pod, '-n', h.NS, '-o', 'json'))
            assert next(c['containerID'] for c in during_tls['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup') == sidecar_before_tls
            h.kube('exec', '-n', h.NS, pod, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup', 'instance', '--check-native')
            assert self.control()['mode'] == ''
            self.s3('GET', 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/wal/' + name[:8] + '/' + name, d / 'available-during-TLS-fault')
        finally:
            h.kube('patch', 'configmap', 'wal-minio-ca', '-n', h.NS, '--type=merge', '-p', json.dumps({'data': {'ca.crt': (d / 'ca.crt').read_text()}}))
        def trust_recovered():
            result = h.kube('exec', '-n', h.NS, pod, '-c', 'postgres', '--', '/run/wal-client', '/run/wal-cluster.json',
                            'restore', name, '/var/lib/postgresql/data/pgdata/pg_wal/RECOVERYXLOG', check=False)
            return result.strip() == 'OK'
        h.wait(trust_recovered, 'restored S3 trust generation', 120)
        self.completed(SCENARIOS[3])
        # Only the already-verified 1GiB disposable WAL filesystem is filled,
        # never the runner disk. Record real ENOSPC, restore failure, unchanged
        # destination bytes, and normal progress after clearing the fault.
        before = self.sql(pod, "SELECT md5(pg_read_binary_file('pg_wal/RECOVERYXLOG'))")
        block, count = map(int, self.shell(pod, "stat -f -c '%S:%b' pg_wal").strip().split(':'))
        assert 0 < block * count <= 1 << 30
        try:
            self.shell(pod, 'dd if=/dev/zero of=pg_wal/.wal-e-full bs=1M count=2048 status=none 2>/run/wal-enospc.log && exit 3; grep "No space left on device" /run/wal-enospc.log')
            self.report['wal_diskfull_precondition'] = {'filesystem_bytes': block * count, 'ENOSPC': True}
            result = self.rpc(pod, 'restore', name, expect='FailedPrecondition')
            h.save_log('wal-diskfull-no-success.log', result)
        finally:
            self.shell(pod, 'rm -f pg_wal/.wal-e-full')
        assert self.sql(pod, "SELECT md5(pg_read_binary_file('pg_wal/RECOVERYXLOG'))") == before, 'failed local restore changed destination'
        self.rpc(pod, 'restore', name)
        self.completed(SCENARIOS[4])

    def segment(self):
        h = self.h
        pod = self.primary()
        self.sql(pod, 'CREATE TABLE wal_e_oracle AS SELECT generate_series(1,1000) AS id')
        name = self.sql(pod, 'SELECT pg_walfile_name(pg_current_wal_insert_lsn())')
        self.sql(pod, 'SELECT pg_switch_wal()')
        h.wait(lambda: self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '1', 'actual CNPG Archive durable acknowledgment', 180)
        self.verify(pod, name)
        self.command(pod, 'wal-archive', 'pg_wal/' + name)  # same bytes succeeds
        # Preserve original segment before a differing retry; no SQL workload
        # runs until the original file is restored in finally.
        self.shell(pod, 'cp pg_wal/' + name + ' /run/wal-e-original')
        try:
            self.shell(pod, "printf '\\001' | dd of=pg_wal/" + name + ' bs=1 seek=64 conv=notrunc status=none')
            result = self.command(pod, 'wal-archive', 'pg_wal/' + name, expect_failure=True)
            h.save_log('wal-different-content-rejected.log', result)
            assert 'AlreadyExists' in result, 'differing retry failed for wrong reason'
        finally:
            self.shell(pod, 'cp /run/wal-e-original pg_wal/' + name + '; rm /run/wal-e-original')
        self.verify(pod, name)
        self.completed(SCENARIOS[0])

    def failover(self):
        h = self.h
        old = self.primary()
        self.report['wal_failover_precondition'] = {'old_primary': old, 'pods': h.pod_uids()}
        h.kube('delete', 'pod', old, '-n', h.NS, '--grace-period=0', '--force', '--wait=false')
        h.wait(lambda: self.primary() != old, 'forced primary loss promotes standby', 180)
        pod = self.primary()
        h.wait(lambda: self.sql(pod, 'SELECT NOT pg_is_in_recovery()') == 't', 'new primary accepts writes')
        tli = int(self.sql(pod, 'SELECT timeline_id FROM pg_control_checkpoint()'))
        assert tli > 1
        history = f'{tli:08X}.history'
        h.wait(lambda: self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + history + ".done'") == '1', 'failover history archived', 180)
        self.verify(pod, history)
        self.report['wal_failover_result'] = {'new_primary': pod, 'timeline': tli}
        self.completed(SCENARIOS[1])

    def close(self):
        for process in (self.metrics_forward, self.forward):
            if process is not None:
                process.terminate()
                process.wait(timeout=10)
