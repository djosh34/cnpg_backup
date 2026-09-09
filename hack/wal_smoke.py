"""E's actual CNPG/MinIO WAL extension. No replacement product data paths."""
import gzip
import hashlib
import json
import os
import re
import signal
from pathlib import Path
import shutil
import subprocess
import time

SCENARIOS = ('actual-cnpg-minio-segment-byte-oracle-and-duplicate-conflict',
             'actual-forced-failover-timeline-history-destination-routing',
             'actual-killed-partial-upload-no-ack-and-retry',
             'actual-transient-and-TLS-are-not-NotFound',
             'actual-finite-WAL-filesystem-full-restore-no-success',
             'actual-WAL-lag-failure-observability',
             'actual-low-space-archive-backlog-drains-without-restore-reservation')

class WALFixture:
    def __init__(self, h, report, namespace=None):
        self.namespace = namespace or h.NS
        self.endpoint = 'https://localhost:19000'
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
        binary = h.bundle['directory'] / 'minio' if hasattr(h, 'bundle') else cache / 'downloads/minio'
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == lock['minio']['sha256']
        if hasattr(h, 'bundle'):
            image = h.image_digest('minio')
            proxy_image = h.image_digest('walproxy')
            shutil.copy2(h.bundle['directory'] / 'wal-client', d / 'wal-client')
        else:
            shutil.copy2(binary, d / 'minio')
            (d / 'Dockerfile').write_text('FROM scratch\nCOPY minio /minio\nENTRYPOINT ["/minio"]\n')
            h.run('docker', 'build', '-t', 'cnpg-backup-wal-minio:test', d)
            image = h.image_digest('minio', 'cnpg-backup-wal-minio:test')
            h.run('go', 'build', '-o', d / 'wal-client', './hack/walclient')
            h.run('go', 'build', '-o', d / 'wal-proxy', './hack/walproxy')
            (d / 'Proxyfile').write_text('FROM scratch\nCOPY wal-proxy /wal-proxy\nENTRYPOINT ["/wal-proxy"]\n')
            h.run('docker', 'build', '-f', d / 'Proxyfile', '-t', 'cnpg-backup-wal-proxy:test', d)
            proxy_image = h.image_digest('walproxy', 'cnpg-backup-wal-proxy:test')
        self.report['wal_minio_image'] = image
        self.report['wal_minio_binary'] = lock['minio']
        self.report['wal_test_proxy_image'] = proxy_image
        # Generated keys remain only in this private work directory/Kubernetes
        # Secret, never in the built image, reports or test command diagnostics.
        h.run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2', '-subj', '/CN=WAL fixture CA',
              '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', d / 'ca.key', '-out', d / 'ca.crt')
        h.run('openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=minio', '-keyout', d / 'private.key', '-out', d / 'server.csr')
        (d / 'extensions').write_text('subjectAltName=DNS:minio.' + self.namespace + '.svc,DNS:minio-backend.' + self.namespace + '.svc,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
        h.run('openssl', 'x509', '-req', '-in', d / 'server.csr', '-CA', d / 'ca.crt', '-CAkey', d / 'ca.key',
              '-CAcreateserial', '-days', '2', '-extfile', d / 'extensions', '-out', d / 'public.crt')
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'wal-minio-tls', 'namespace': self.namespace},
                 'stringData': {'public.crt': (d / 'public.crt').read_text(), 'private.key': (d / 'private.key').read_text(), 'ca.crt': (d / 'ca.crt').read_text()}})
        h.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'wal-minio-ca', 'namespace': self.namespace},
                 'data': {'ca.crt': (d / 'ca.crt').read_text()}})
        h.apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'wal-minio', 'namespace': self.namespace, 'labels': {'app': 'wal-minio'}},
                 'spec': {'securityContext': {'runAsUser': 26, 'runAsGroup': 26, 'fsGroup': 26},
                          'containers': [{'name': 'minio', 'image': image, 'args': ['server', '--address', ':9000', '--certs-dir', '/certs', '/data'],
                                          'env': [{'name': 'MINIO_ROOT_USER', 'valueFrom': {'secretKeyRef': {'name': 's3-auth', 'key': 'access'}}},
                                                  {'name': 'MINIO_ROOT_PASSWORD', 'valueFrom': {'secretKeyRef': {'name': 's3-auth', 'key': 'secret'}}},
                                                  {'name': 'MINIO_BROWSER', 'value': 'off'}],
                                          'resources': {'limits': {'memory': '512Mi', 'cpu': '1'}},
                                          'volumeMounts': [{'name': 'data', 'mountPath': '/data'}, {'name': 'tls', 'mountPath': '/certs', 'readOnly': True}]}],
                          'volumes': [{'name': 'data', 'emptyDir': {'sizeLimit': '2Gi'}}, {'name': 'tls', 'secret': {'secretName': 'wal-minio-tls'}}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'minio-backend', 'namespace': self.namespace},
                 'spec': {'selector': {'app': 'wal-minio'}, 'ports': [{'port': 9000, 'targetPort': 9000}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'wal-proxy', 'namespace': self.namespace, 'labels': {'app': 'wal-proxy'}},
                 'spec': {'containers': [{'name': 'proxy', 'image': proxy_image,
                                          'env': [{'name': 'FIXTURE_BACKEND', 'value': 'https://minio-backend.' + self.namespace + '.svc:9000'}],
                                          'resources': {'limits': {'cpu': '1', 'memory': '128Mi'}},
                                          'volumeMounts': [{'name': 'tls', 'mountPath': '/certs', 'readOnly': True}]}],
                          'volumes': [{'name': 'tls', 'secret': {'secretName': 'wal-minio-tls'}}]}})
        h.apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'minio', 'namespace': self.namespace},
                 'spec': {'selector': {'app': 'wal-proxy'}, 'ports': [{'port': 9000, 'targetPort': 9000}]}})
        for pod in ('wal-minio', 'wal-proxy'):
            h.kube('wait', 'pod/' + pod, '-n', self.namespace, '--for=condition=Ready', '--timeout=120s')
        log = open(d / 'forward.log', 'w')
        self.forward = subprocess.Popen([str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'),
                                        'port-forward', '--address=127.0.0.1', '-n', self.namespace, 'service/minio',
                                        '0:9000' if hasattr(h, 'bundle') else '19000:9000'], stdout=log, stderr=log,
                                        start_new_session=True)
        log.close()
        # Use curl's maintained signer as the independent remote byte oracle.
        # D's synthetic throwaway credential values are never production inputs.
        config = d / 'curl-private.conf'
        config.write_text('user = "disposable-test-only-access:disposable-test-only-secret"\naws-sigv4 = "aws:amz:us-east-1:s3"\nnoproxy = "*"\n')
        config.chmod(0o600)
        def ready():
            if hasattr(h, 'bundle'):
                match = re.search(r'Forwarding from 127\.0\.0\.1:([0-9]+)', (d / 'forward.log').read_text()[:4096])
                if not match:
                    assert self.forward.poll() is None, 'fixture port-forward exited before listener allocation'
                    return False
                self.endpoint = 'https://localhost:' + match[1]
                h.listener = {'pid': self.forward.pid, 'port': int(match[1]),
                    'start_ticks': Path(f'/proc/{self.forward.pid}/stat').read_text().split()[21]}
                self.report.setdefault('fixture_listeners', {})[h.NAME] = h.listener
                h.record_ownership()
            result = h.run('curl', '-q', '--silent', '--show-error', '--write-out', '%{http_code}', '--max-time', '3', '--cacert', d / 'ca.crt',
                           self.endpoint + '/minio/health/ready', check=False)
            return self.forward.poll() is None and result == '200'
        h.wait(ready, 'MinIO TLS port forward')
        # Exact uninitialized503 is the only bucket-startup retry. Preserve the
        # first failure text; unrelated auth/transport failures are not retried.
        first_initializing = True
        def bucket_ready():
            nonlocal first_initializing
            result = self.s3('PUT', '', check=False)
            if 'XMinioServerNotInitialized' in result:
                if first_initializing:
                    h.save_log('wal-minio-first-startup-response.log', result)
                    first_initializing = False
                return False
            if hasattr(h, 'commands'):
                h.commands.last.require()
            assert '<Error>' not in result and 'curl:' not in result, 'MinIO bucket setup rejected'
            return True
        h.wait(bucket_ready, 'MinIO object layer initialized and fresh bucket created', 60)
        return {'endpoint': 'https://minio.' + self.namespace + '.svc:9000', 'caConfigMap': {'name': 'wal-minio-ca', 'key': 'ca.crt'}}

    def s3(self, method, key, dest=None, **kwargs):
        d = self.directory
        args = ['curl', '-q', '--silent', '--show-error', '--fail-with-body', '--max-time', '15',
                '--config', d / 'curl-private.conf', '--cacert', d / 'ca.crt', '-X', method]
        if dest:
            args += ['--output', dest]
        return self.h.run(*args, self.endpoint + '/test-bucket/' + key, **kwargs)

    def sql(self, pod, query):
        return self.h.kube('exec', '-n', self.namespace, pod, '-c', 'postgres', '--', 'psql', '-XAt', '-U', 'postgres', '-d', 'postgres', '-c', query).strip()

    def primary(self):
        return json.loads(self.h.kube('get', 'cluster/database', '-n', self.namespace, '-o', 'json'))['status']['currentPrimary']

    def command(self, pod, verb, *args, **kwargs):
        return self.h.kube('exec', '-n', self.namespace, pod, '-c', 'postgres', '--', '/controller/manager', verb, *args, **kwargs)

    def shell(self, pod, script):
        # Test arrangement in upstream PG image only; product shell-free sidecar
        # still executes its exact compiled Archive/Restore paths over Unix RPC.
        return self.h.kube('exec', '-n', self.namespace, pod, '-c', 'postgres', '--', 'sh', '-ec', 'cd /var/lib/postgresql/data/pgdata; ' + script)

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
        command = [str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'), 'exec', '-i', '-n', self.namespace,
                   pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > /run/wal-client; chmod 0555 /run/wal-client']
        result = subprocess.run(command, input=(self.directory / 'wal-client').read_bytes(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        assert result.returncode == 0, 'test-only RPC driver installation failed'
        definition = h.kube('get', 'cluster/database', '-n', self.namespace, '-o', 'json')
        h.kube('exec', '-i', '-n', self.namespace, pod, '-c', 'postgres', '--', 'sh', '-ec', 'cat > /run/wal-cluster.json', input=definition)
        self.report['wal_test_driver_sha256'] = hashlib.sha256((self.directory / 'wal-client').read_bytes()).hexdigest()

    def rpc(self, pod, verb, name, expect=None):
        arguments = ['/run/wal-client', '/run/wal-cluster.json', verb]
        arguments += ['/var/lib/postgresql/data/pgdata/pg_wal/' + name] if verb == 'archive' else [name, '/var/lib/postgresql/data/pgdata/pg_wal/RECOVERYXLOG']
        result = self.h.kube('exec', '-n', self.namespace, pod, '-c', 'postgres', '--', *arguments,
                             expect_failure=expect is not None)
        if expect is not None:
            assert expect in result and (expect == 'NotFound' or 'NotFound' not in result), 'fault was misclassified: ' + result
        return result

    def control(self, mode=None, name=None):
        args = ['curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '5', '--cacert', self.directory / 'ca.crt']
        if mode is not None:
            args += ['-X', 'POST']
        if name is not None:
            import re
            assert re.fullmatch('[0-9A-F]{24}', name), 'unsafe test WAL filter'
        return json.loads(self.h.run(*args, self.endpoint + '/fixture-control' +
                                    ('?mode=' + mode if mode is not None else '') + ('&name=' + name if name else '')))

    def overlap_certificate(self):
        """A new private root/leaf with an old-root cross-sign: both OLD running
        snapshots and NEW snapshots can open valid connections during drainage.
        This is not the ineffective old-root leaf-only rotation fixture.
        """
        h, d = self.h, self.directory
        h.run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
              '-subj', '/CN=J new S3 private CA', '-addext', 'basicConstraints=critical,CA:TRUE',
              '-keyout', d / 'new-ca.key', '-out', d / 'new-ca.crt')
        h.run('openssl', 'x509', '-x509toreq', '-in', d / 'new-ca.crt', '-signkey', d / 'new-ca.key', '-out', d / 'new-ca.csr')
        (d / 'ca-extensions').write_text('basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n')
        h.run('openssl', 'x509', '-req', '-in', d / 'new-ca.csr', '-CA', d / 'ca.crt', '-CAkey', d / 'ca.key',
              '-CAcreateserial', '-days', '2', '-extfile', d / 'ca-extensions', '-out', d / 'cross-ca.crt')
        h.run('openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=minio',
              '-keyout', d / 'new-private.key', '-out', d / 'new-server.csr')
        h.run('openssl', 'x509', '-req', '-in', d / 'new-server.csr', '-CA', d / 'new-ca.crt', '-CAkey', d / 'new-ca.key',
              '-CAcreateserial', '-days', '2', '-extfile', d / 'extensions', '-out', d / 'new-public.crt')
        for root in ('ca.crt', 'new-ca.crt'):
            h.run('openssl', 'verify', '-CAfile', d / root, '-untrusted', d / 'cross-ca.crt', d / 'new-public.crt')
        return {'public.crt': (d / 'new-public.crt').read_text() + (d / 'cross-ca.crt').read_text(),
                'private.key': (d / 'new-private.key').read_text(), 'ca.crt': (d / 'ca.crt').read_text() + (d / 'new-ca.crt').read_text()}

    def session_credentials(self):
        """Mint overlapping VALID MinIO STS sessions using curl's maintained
        signer. Root credentials and STS response stay in private files only.
        This does not rotate MinIO root/environment values by restarting it.
        """
        import xml.etree.ElementTree as ET
        d = self.directory
        config, output = d / 'sts-private.conf', d / 'sts-private.xml'
        config.write_text((d / 'curl-private.conf').read_text().replace('us-east-1:s3', 'us-east-1:sts'))
        config.chmod(0o600)
        output.touch(mode=0o600)
        try:
            self.h.run('curl', '-q', '--silent', '--show-error', '--fail', '--max-time', '15',
                       '--config', config, '--cacert', d / 'ca.crt', '--output', output,
                       '--data', 'Action=AssumeRole&Version=2011-06-15&DurationSeconds=3600', self.endpoint + '/')
            if output.stat().st_size > 65536:
                raise RuntimeError('unbounded STS fixture response')
            root = ET.fromstring(output.read_bytes())
            values = {element.tag.rsplit('}', 1)[-1]: element.text for element in root.iter()}
            if any(not values.get(k) for k in ('AccessKeyId', 'SecretAccessKey', 'SessionToken')):
                raise RuntimeError('MinIO did not create a valid STS session')
            return {'access': values['AccessKeyId'], 'secret': values['SecretAccessKey'], 'token': values['SessionToken']}
        finally:
            config.unlink(missing_ok=True)
            output.unlink(missing_ok=True)

    def metrics(self, pod):
        h = self.h
        if self.metrics_forward is None:
            log = open(self.directory / 'metrics-forward.log', 'w')
            self.metrics_forward = subprocess.Popen([str(h.WORK / 'kubectl'), '--kubeconfig', str(h.WORK / 'kubeconfig'),
                                                    'port-forward', '-n', self.namespace, 'pod/' + pod, '19187:9187'], stdout=log, stderr=log)
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
            observed = json.loads(h.kube('get', 'pod', pod, '-n', self.namespace, '-o', 'json'))
            sidecar = next(c for c in observed['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
            container_id = sidecar['containerID'].split('://')[1]
            pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', container_id))['info']['pid'])
            assert pid > 1
            h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-9', str(pid))
            def restarted():
                p = json.loads(h.kube('get', 'pod', pod, '-n', self.namespace, '-o', 'json'))
                s = next(c for c in p['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
                return s['restartCount'] > sidecar['restartCount'] and s.get('ready', False)
            h.wait(restarted, 'killed upload sidecar starts fresh incarnation')
            assert self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '0', 'killed upload acknowledged while fault remains'
            # Pressure only the bounded disposable WAL volume, while S3 is still
            # unavailable. Archive reads this volume; its spools live elsewhere.
            block, count, available = map(int, self.shell(pod, "stat -f -c '%S:%b:%a' pg_wal").strip().split(':'))
            assert 0 < block * count <= 1 << 30 and block * available > 32 << 20
            self.shell(pod, 'fallocate -l ' + str(block * available - (32 << 20)) + ' pg_wal/.wal-e-pressure')
            pending = self.sql(pod, "SELECT string_agg(n, ',') FROM pg_ls_dir('pg_wal/archive_status') n WHERE n LIKE '%.ready'").split(',')
            assert name + '.ready' in pending
            sentinel = self.sql(pod, "SELECT md5(pg_read_binary_file('pg_wal/RECOVERYXLOG'))")
            self.report['wal_low_space_pending'] = pending
            # A callback admitted before fallocate could drain after fault clear
            # even with the old bug. Kill again UNDER pressure: all subsequent
            # callbacks must re-run capacity preflight on the pressured mount.
            observed = json.loads(h.kube('get', 'pod', pod, '-n', self.namespace, '-o', 'json'))
            sidecar = next(c for c in observed['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
            container_id = sidecar['containerID'].split('://')[1]
            pid = int(json.loads(h.run('docker', 'exec', h.NAME + '-control-plane', 'crictl', 'inspect', container_id))['info']['pid'])
            assert pid > 1
            h.run('docker', 'exec', h.NAME + '-control-plane', 'kill', '-9', str(pid))
            h.wait(restarted, 'fresh capacity preflight incarnation under WAL pressure')
            assert self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + name + ".done'") == '0'
            block, available = map(int, self.shell(pod, "stat -f -c '%S:%a' pg_wal").strip().split(':'))
            assert block * available < 48 << 20
            self.report['wal_low_space_restart'] = {'killed_container': container_id, 'available_bytes': block * available, 'proxy': self.control()}
        finally:
            self.control('')
        try:
            def drained():
                return all(self.sql(pod, "SELECT count(*) FROM pg_ls_dir('pg_wal/archive_status') n WHERE n='" + n.removesuffix('.ready') + ".done'") == '1' for n in pending)
            h.wait(drained, 'actual low-space backlog drains after S3 recovery', 180)
            block, available = map(int, self.shell(pod, "stat -f -c '%S:%a' pg_wal").strip().split(':'))
            assert block * available < 48 << 20, 'pressure disappeared before archive regression'
            self.report['wal_low_space_drained'] = {'available_bytes': block * available, 'restore_required_bytes': 48 << 20, 'proxy': self.control()}
            assert self.control()['mode'] == ''
            result = self.rpc(pod, 'restore', name, expect='FailedPrecondition')
            h.save_log('wal-low-space-restore-no-success.log', result)
            assert self.sql(pod, "SELECT md5(pg_read_binary_file('pg_wal/RECOVERYXLOG'))") == sentinel
            self.completed(SCENARIOS[6])
        finally:
            self.shell(pod, 'rm -f pg_wal/.wal-e-pressure')
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
        before_tls = json.loads(h.kube('get', 'pod', pod, '-n', self.namespace, '-o', 'json'))
        sidecar_before_tls = next(c['containerID'] for c in before_tls['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup')
        try:
            h.kube('patch', 'configmap', 'wal-minio-ca', '-n', self.namespace, '--type=merge', '-p', json.dumps({'data': {'ca.crt': (d / 'unrelated.crt').read_text()}}))
            def rejected_tls():
                result = h.kube('exec', '-n', self.namespace, pod, '-c', 'postgres', '--', '/run/wal-client', '/run/wal-cluster.json',
                                'restore', name, '/var/lib/postgresql/data/pgdata/pg_wal/RECOVERYXLOG', check=False)
                if 'Unavailable' in result:
                    h.save_log('wal-TLS-not-EOF.log', result)
                    return True
                assert result.strip() == 'OK', 'trust fault returned wrong category'
                return False
            h.wait(rejected_tls, 'current invalid S3 trust fails actual WAL callback', 120)
            self.rpc(pod, 'archive', name, expect='Unavailable')
            # Distinguish TLS rejection from an unrelated process/network outage.
            during_tls = json.loads(h.kube('get', 'pod', pod, '-n', self.namespace, '-o', 'json'))
            assert next(c['containerID'] for c in during_tls['status']['initContainerStatuses'] if c['name'] == 'cnpg-backup') == sidecar_before_tls
            h.kube('exec', '-n', self.namespace, pod, '-c', 'cnpg-backup', '--', '/usr/local/bin/cnpg-backup', 'instance', '--check-native')
            assert self.control()['mode'] == ''
            self.s3('GET', 'smoke/v1/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/wal/' + name[:8] + '/' + name, d / 'available-during-TLS-fault')
        finally:
            h.kube('patch', 'configmap', 'wal-minio-ca', '-n', self.namespace, '--type=merge', '-p', json.dumps({'data': {'ca.crt': (d / 'ca.crt').read_text()}}))
        def trust_recovered():
            result = h.kube('exec', '-n', self.namespace, pod, '-c', 'postgres', '--', '/run/wal-client', '/run/wal-cluster.json',
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
        h.kube('delete', 'pod', old, '-n', self.namespace, '--grace-period=0', '--force', '--wait=false')
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
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if process is self.forward:
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait(timeout=5)
