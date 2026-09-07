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
             'actual-forced-failover-timeline-history-destination-routing')

class WALFixture:
    def __init__(self, h, report):
        self.h, self.report, self.forward = h, report, None
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
        # Generated keys remain only in this private work directory/Kubernetes
        # Secret, never in the built image, reports or test command diagnostics.
        h.run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2', '-subj', '/CN=WAL fixture CA',
              '-addext', 'basicConstraints=critical,CA:TRUE', '-keyout', d / 'ca.key', '-out', d / 'ca.crt')
        h.run('openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=minio', '-keyout', d / 'private.key', '-out', d / 'server.csr')
        (d / 'extensions').write_text('subjectAltName=DNS:minio.' + h.NS + '.svc,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
        h.run('openssl', 'x509', '-req', '-in', d / 'server.csr', '-CA', d / 'ca.crt', '-CAkey', d / 'ca.key',
              '-CAcreateserial', '-days', '2', '-extfile', d / 'extensions', '-out', d / 'public.crt')
        h.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'wal-minio-tls', 'namespace': h.NS},
                 'stringData': {'public.crt': (d / 'public.crt').read_text(), 'private.key': (d / 'private.key').read_text()}})
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
        h.apply({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'minio', 'namespace': h.NS},
                 'spec': {'selector': {'app': 'wal-minio'}, 'ports': [{'port': 9000, 'targetPort': 9000}]}})
        h.kube('wait', 'pod/wal-minio', '-n', h.NS, '--for=condition=Ready', '--timeout=120s')
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
        if self.forward is not None:
            self.forward.terminate()
            self.forward.wait(timeout=10)
