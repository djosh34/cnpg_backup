#!/usr/bin/env python3
"""PR A native recovery oracle, not production backup/storage/CNPG code.
Runs as an ordinary user. Only its private Unix socket and loopback MinIO are
accepted. --images verifies using direct Go exec in the actual scratch image.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import subprocess
import tarfile
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET

from bootstrap import CACHE, LOCK, REPO
from build import OUT


def s3_command(config, endpoint, method, key, source=None, dest=None):
    # -q must be first: an ambient .curlrc can add URLs or credential traces.
    command = ['curl', '-q', '--silent', '--show-error', '--fail-with-body', '--max-time', '60', '--config', config, '-X', method]
    if source:
        command += ['--upload-file', source]
    if dest:
        command += ['--output', dest]
    return [*command, endpoint + '/foundation/' + key]


def create_bucket(run, command, sleep=time.sleep):
    """Retry only MinIO's exact startup response; retain every first failure."""
    for attempt in range(30):
        code, text = run([*command, '--write-out', '\nCNPG_HTTP=%{http_code}\n'], check=False)
        if code == 0:
            return
        retry = False
        if code == 22 and text.endswith('CNPG_HTTP=503'):
            start, end = text.find('<Error'), text.find('</Error>')
            if 0 <= start < end and end - start <= 65536:
                try:
                    error = ET.fromstring(text[start:end + len('</Error>')])
                    retry = error.findtext('Code') == 'XMinioServerNotInitialized'
                except ET.ParseError:
                    pass
        if not retry:
            raise RuntimeError('MinIO bucket setup rejected; original response retained in commands.log')
        sleep(.1)
    raise RuntimeError('MinIO bucket initialization deadline; all responses retained')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--seed', type=int, default=1806)
    parser.add_argument('--images', action='store_true')
    parser.add_argument('--native-only', action='store_true', help='diagnostic only; never satisfies MinIO integration acceptance')
    args = parser.parse_args()
    if os.getuid() == 0:
        raise RuntimeError('run recovery as non-root')
    os.umask(0o077)
    parent = REPO / '.work'
    parent.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='recovery-', dir=parent))
    evidence = REPO / 'artifacts' / work.name
    evidence.mkdir(parents=True)
    sock = work / 'socket'
    sock.mkdir()
    if len(str(sock)) > 95 or not re.fullmatch(r'[A-Za-z0-9_./-]+', str(work)):
        raise RuntimeError('use a short checkout path without SQL/shell metacharacters')
    bin_dir = CACHE / 'pgroot/usr/lib/postgresql/18/bin'
    env = {k: v for k, v in os.environ.items() if not k.startswith(('PG', 'MINIO_')) and k not in ('LD_PRELOAD', 'LD_LIBRARY_PATH')}
    env.update(LD_LIBRARY_PATH=str(OUT / 'test-libs'), LC_ALL='C', PGHOST=str(sock), PGPORT='55486',
               PGUSER='fixture', PGDATABASE='postgres', PGCONNECT_TIMEOUT='10')
    # Disable ambient proxies/credentials; no configurable external endpoint.
    for k in list(env):
        if k.lower().endswith('_proxy'):
            del env[k]
    events = (evidence / 'events.jsonl').open('w')
    journal = (evidence / 'journal.jsonl').open('w')
    started = time.monotonic()
    seq = 0
    servers = []
    minio = None
    secrets = []
    result = {'schema': 1, 'profile': 'native-diagnostic' if args.native_only else 'foundation-smoke', 'release_qualified': False,
              'minio_coverage': 'unexecuted diagnostic' if args.native_only else 'required',
              'seed': args.seed, 'harness_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
              'dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO)),
              'pins': LOCK, 'scenarios': [], 'status': 'failed',
              'image_regression': 'required' if args.images else 'unexecuted (host-tools profile)',
              'replay': f'./hack/test integration --seed {args.seed}' + (' --images' if args.images else '') + (' --native-only' if args.native_only else ''),
              'resources': {'cpu_count': os.cpu_count(), 'disk_free': shutil.disk_usage(work).free},
              'run_id': os.environ.get('GITHUB_RUN_ID', 'local'), 'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT', '1')}

    def event(kind, **fields):
        nonlocal seq
        seq += 1
        events.write(json.dumps({'seq': seq, 'elapsed_seconds': round(time.monotonic()-started, 3), 'kind': kind, **fields}) + '\n')
        events.flush()

    def run(argv, check=True, timeout=120):
        argv = list(map(str, argv))
        p = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            output, _ = p.communicate(timeout=timeout)
        except BaseException:
            os.killpg(p.pid, signal.SIGKILL)
            p.communicate()
            event('command_interrupted', argv=argv)
            raise
        text = output.decode(errors='replace')
        for secret in secrets:
            text = text.replace(secret, '[REDACTED]')
        event('command', argv=argv, exit=p.returncode)
        with (evidence / 'commands.log').open('a') as f:
            f.write(json.dumps(argv) + '\n' + text + '\n')
        if check and p.returncode:
            raise RuntimeError(f'command failed ({p.returncode}): {argv[0]}: {text[-2000:]}')
        return p.returncode, text.strip()

    def pg(tool, *argv, **kw):
        return run([bin_dir / tool, *argv], **kw)

    def sql(query):
        return pg('psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', query)[1]

    def transaction(label, query):
        sql('BEGIN; ' + query + '; COMMIT;')
        journal.write(json.dumps({'label': label, 'sql': query, 'commit': 'acknowledged'}) + '\n')
        journal.flush()
        event('transaction_committed', label=label)

    def verify(data, wal=None, check=True):
        wal = wal or data / 'pg_wal'
        if args.images:
            cmd = ['docker', 'run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL',
                   '--security-opt=no-new-privileges', '--user', f'{os.getuid()}:{os.getgid()}',
                   '--mount', f'type=bind,src={work},dst={work},readonly',
                   '--mount', f'type=bind,src={OUT / "verify"},dst=/test-verify,readonly',
                   '--entrypoint', '/test-verify', result['images']['pg18'],
                   '/usr/lib/postgresql/18/bin', data, wal]
        else:
            cmd = [OUT / 'verify', bin_dir, data, wal]
        return run(cmd, check=check)

    def unpack(archive, dest):
        dest.mkdir(parents=True, exist_ok=True)
        # Only our own locally captured, checksum-verified fixtures. This is
        # deliberately not the product's hostile-backup extraction seam.
        with tarfile.open(archive) as tar:
            for m in tar:
                if not (m.isfile() or m.isdir()):
                    raise RuntimeError('unexpected native fixture member')
                tar.extract(m, dest, filter='data')

    def passed(name):
        result['scenarios'].append(name)
        event('scenario_pass', name=name)

    def interrupted(signum, _frame):
        raise InterruptedError(f'signal {signum}')

    signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.images:
            result['images'] = json.loads((OUT / 'images.json').read_text())
        versions = {}
        for tool in ('postgres', 'initdb', 'pg_ctl', 'pg_basebackup', 'pg_verifybackup', 'pg_combinebackup', 'pg_waldump', 'pg_controldata', 'psql'):
            versions[tool] = pg(tool, '--version')[1]
            if not re.search(r'\b18\.6\b', versions[tool]):
                raise RuntimeError('tool version mismatch')
        versions['go'] = run([CACHE / 'go/bin/go', 'version'])[1]
        if not args.native_only:
            versions['minio'] = run([CACHE / 'downloads/minio', '--version'])[1]
            if LOCK['minio']['version'] not in versions['minio']:
                raise RuntimeError('MinIO version mismatch')
        result['versions'] = versions
        # Record actual host-native dependency resolution separately from image
        # closure: host orchestration is not shell-free runtime evidence.
        run(['ldd', bin_dir / 'postgres'])
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        endpoint = f'http://127.0.0.1:{port}'
        user = 'foundation-' + os.urandom(8).hex()
        password = os.urandom(24).hex()
        secrets.extend([user, password])
        minenv = env | {'MINIO_ROOT_USER': user, 'MINIO_ROOT_PASSWORD': password, 'MINIO_BROWSER': 'off', 'MINIO_UPDATE': 'off'}
        if not args.native_only:
            with (work / 'minio.log').open('w') as log:
                minio = subprocess.Popen([CACHE / 'downloads/minio', 'server', '--address', f'127.0.0.1:{port}',
                                          '--console-address', '127.0.0.1:0', work / 'minio-data'], env=minenv, stdout=log, stderr=subprocess.STDOUT)
            for _ in range(100):
                if minio.poll() is not None:
                    raise RuntimeError('MinIO exited during setup')
                try:
                    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                    with opener.open(endpoint + '/minio/health/ready', timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(.1)
            else:
                raise RuntimeError('MinIO readiness timeout')
        # curl's maintained SigV4 implementation is test-only. PR B supplies
        # and tests the production SDK, V2/V4/TLS/private-CA adapter.
        config = work / 'curl-secret.conf'
        config.write_text(f'user = "{user}:{password}"\naws-sigv4 = "aws:amz:us-east-1:s3"\nnoproxy = "*"\n')

        def s3(method, key, source=None, dest=None):
            return run(s3_command(config, endpoint, method, key, source, dest))

        if not args.native_only:
            create_bucket(run, s3_command(config, endpoint, 'PUT', ''))
            passed('disposable_minio_sigv4_bucket')
        source = work / 'source'
        pg('initdb', '-D', source, '--waldir', work / 'source-wal', '-U', 'fixture', '-A', 'trust', '--no-locale', '--encoding=UTF8', '--data-checksums')
        with (source / 'postgresql.conf').open('a') as f:
            f.write(f"\nlisten_addresses=''\nunix_socket_directories='{sock}'\nport=55486\n"
                    "wal_level=replica\nsummarize_wal=on\nwal_summary_keep_time='30d'\n"
                    "max_wal_senders=5\nmax_replication_slots=5\narchive_mode=off\n")
        servers.append(source)
        pg('pg_ctl', '-D', source, '-l', work / 'source.log', '-w', '-t', '30', 'start')
        if sql('SHOW server_version_num') != '180006':
            raise RuntimeError('wrong live server version')
        sql('CREATE ROLE backup LOGIN REPLICATION')
        run(['env', f'CNPG_TEST_PG_BIN={bin_dir}', f'CNPG_TEST_PG_SOCKET={sock}',
             f'CNPG_TEST_PG_LIBS={OUT / "test-libs"}', 'CNPG_TEST_PG_USER=fixture', 'CNPG_TEST_PG_ROLE=backup',
             CACHE / 'go/bin/go', 'test', './internal/postgres', '-run', '^TestNativeLabelTimeZones$', '-count=1', '-v'])
        passed('source_timezone_DST_label_normalization')
        salt = random.Random(args.seed).randrange(1, 2**31)
        transaction('full', f"CREATE TABLE t(id int PRIMARY KEY, v text); INSERT INTO t SELECT i, repeat(md5('{salt}:' || i::text),32) FROM generate_series(1,20000) i; CREATE TABLE reset_me(v text); INSERT INTO reset_me VALUES('old'); CREATE TABLE recreated(v text); INSERT INTO recreated VALUES('old')")

        def capture(name, reference=None):
            sql('CHECKPOINT')
            cmd = ['--no-password', '-U', 'backup', '--pgdata', work / (name + '-tar'), '--format=tar',
                   '--wal-method=stream', '--checkpoint=fast', '--manifest-checksums=SHA256']
            if reference:
                cmd += ['--incremental=' + str(reference)]
            pg('pg_basebackup', *cmd)
            wal = work / (name + '-walcheck')
            unpack(work / (name + '-tar/pg_wal.tar'), wal)
            verify(work / (name + '-tar'), wal)
            manifest = json.loads((work / (name + '-tar/backup_manifest')).read_text())
            (evidence / (name + '-manifest.json')).write_text(json.dumps(manifest, indent=2) + '\n')
            return work / (name + '-tar/backup_manifest')

        full = capture('F')
        transaction('D1', "UPDATE t SET v='d1' WHERE id=1; DELETE FROM t WHERE id=2")
        capture('D1', full)
        transaction('D2', "UPDATE t SET v='d2' WHERE id=3; TRUNCATE reset_me; INSERT INTO reset_me VALUES('new'); DROP TABLE recreated; CREATE TABLE recreated(v text); INSERT INTO recreated VALUES('new')")
        capture('D2', full)
        # This table state is deliberately outside the selected backup.
        transaction('after-D2', "INSERT INTO t VALUES(20001,'must-not-restore')")
        pg('pg_ctl', '-D', source, '-m', 'fast', '-w', 'stop')
        shutil.rmtree(work / 'D1-tar')
        shutil.rmtree(work / 'D1-walcheck')
        artifacts = []
        for name in ('F', 'D2'):
            download = work / (name + '-download')
            download.mkdir()
            for p in sorted((work / (name + '-tar')).iterdir()):
                target = download / p.name
                if args.native_only:
                    shutil.copyfile(p, target)
                else:
                    s3('PUT', name + '/' + p.name, source=p)
                    s3('GET', name + '/' + p.name, dest=target)
                h = hashlib.sha256(p.read_bytes()).hexdigest()
                if hashlib.sha256(target.read_bytes()).hexdigest() != h:
                    raise RuntimeError('S3 artifact mismatch')
                artifacts.append({'backup': name, 'file': p.name, 'bytes': p.stat().st_size, 'sha256': h})
            # Delete capture copies: recovery consumes only the MinIO download.
            shutil.rmtree(work / (name + '-tar'))
            data = work / name
            unpack(download / 'base.tar', data)
            unpack(download / 'pg_wal.tar', data / 'pg_wal')
            verify(download, data / 'pg_wal')
            shutil.copyfile(download / 'backup_manifest', data / 'backup_manifest')
            verify(data)
        (evidence / 'backup-artifacts.json').write_text(json.dumps(artifacts, indent=2) + '\n')
        passed('F_D1_D2_full_reference_capture' + ('_local_copy_diagnostic' if args.native_only else '_and_minio_roundtrip'))
        if not args.native_only:
            result['minio_coverage'] = 'passed: SigV4 native artifact PUT/GET and byte hashes'
        damaged = next(p for p in sorted((work / 'D2/base').glob('*/INCREMENTAL.*')) if p.stat().st_size > 8192)

        def flip():
            with damaged.open('r+b') as f:
                f.seek(-1, 2)
                old = f.read(1)
                f.seek(-1, 2)
                f.write(bytes([old[0] ^ 1]))

        flip()
        code, text = verify(work / 'D2', check=False)
        if code == 0 or 'checksum mismatch' not in text:
            raise RuntimeError('corruption oracle failed')
        passed('original_incremental_corruption_rejected')
        # Explicit negative control: a fresh synthetic manifest can bless bad
        # input. This output is never started and is removed immediately.
        pg('pg_combinebackup', '--copy', '--manifest-checksums=SHA256', '-o', work / 'bad-synthetic', work / 'F', work / 'D2')
        verify(work / 'bad-synthetic')
        shutil.rmtree(work / 'bad-synthetic')
        passed('synthetic_manifest_alone_accepts_corrupt_input_negative_control')
        flip()
        verify(work / 'F')
        verify(work / 'D2')
        wal = next(p for p in (work / 'D2/pg_wal').iterdir() if re.fullmatch('[0-9A-F]{24}', p.name))
        held = work / 'withheld-wal'
        wal.rename(held)
        code, text = verify(work / 'D2', check=False)
        if code == 0 or not ('could not find' in text or 'could not open' in text):
            raise RuntimeError('missing-WAL oracle failed')
        held.rename(wal)
        passed('required_bundled_wal_rejected')
        with wal.open('r+b') as f:
            first = f.read(1)
            f.seek(0)
            f.write(bytes([first[0] ^ 1]))
        code, text = verify(work / 'D2', check=False)
        if code == 0 or 'pg_waldump' not in text or not ('invalid' in text or 'could not find a valid record' in text):
            raise RuntimeError('corrupt-WAL oracle failed')
        with wal.open('r+b') as f:
            f.write(first)
        passed('corrupt_bundled_wal_rejected')
        # Test-only multi-range input with a valid native self-checksum: native
        # file verification succeeds, first WAL range succeeds, later one fails.
        # This does not expand production's primary-only capture support.
        manifest_path = work / 'D2/backup_manifest'
        original_manifest = manifest_path.read_bytes()
        m = json.loads(original_manifest)
        del m['Manifest-Checksum']
        m['WAL-Ranges'].append(m['WAL-Ranges'][0] | {'Timeline': 2})
        prefix = (json.dumps(m)[:-1] + ',\n').encode()
        manifest_path.write_bytes(prefix + ('"Manifest-Checksum":"' + hashlib.sha256(prefix).hexdigest() + '"}\n').encode())
        code, text = verify(work / 'D2', check=False)
        if code == 0 or '--timeline=2' not in text or 'could not find' not in text:
            raise RuntimeError('later-range WAL oracle failed')
        manifest_path.write_bytes(original_manifest)
        passed('later_manifest_range_missing_wal_rejected')
        restored = work / 'restored'
        pg('pg_combinebackup', '--copy', '--manifest-checksums=SHA256', '-o', restored, work / 'F', work / 'D2')
        verify(restored)
        # Configuration is trusted harness-generated input. Verification precedes
        # startup transforms. Bundled WAL alone supplies consistency, not PITR.
        with (restored / 'postgresql.conf').open('a') as f:
            f.write("\narchive_mode=off\n")
        servers.append(restored)
        pg('pg_ctl', '-D', restored, '-l', work / 'restore.log', '-w', '-t', '30', 'start')
        expected = {i: hashlib.md5(f'{salt}:{i}'.encode()).hexdigest()*32 for i in range(1, 20001)}
        expected[1] = 'd1'
        del expected[2]
        expected[3] = 'd2'
        expected_hash = hashlib.md5('\n'.join(f'{i}:{expected[i]}' for i in sorted(expected)).encode()).hexdigest()
        actual_hash = sql("SELECT md5(string_agg(id::text || ':' || v, E'\\n' ORDER BY id)) FROM t")
        assertions = {'expected_rows': len(expected), 'actual_rows': int(sql('SELECT count(*) FROM t')),
                      'expected_md5': expected_hash, 'actual_md5': actual_hash,
                      'in_recovery': sql('SELECT pg_is_in_recovery()'), 'reset': sql('SELECT v FROM reset_me'),
                      'recreated': sql('SELECT v FROM recreated'), 'post_backup_rows': sql('SELECT count(*) FROM t WHERE id=20001')}
        (evidence / 'sql-assertions.json').write_text(json.dumps(assertions, indent=2) + '\n')
        if (actual_hash != expected_hash or assertions['actual_rows'] != len(expected)
                or assertions['in_recovery'] != 'f' or assertions['reset'] != 'new'
                or assertions['recreated'] != 'new' or assertions['post_backup_rows'] != '0'):
            raise RuntimeError('restored SQL state differs from independent oracle')
        pg('pg_ctl', '-D', restored, '-m', 'fast', '-w', 'stop')
        passed('F_plus_D2_without_D1_SQL_recovery')
        if args.images:
            result['image_regression'] = 'passed: tar/original/synthetic no-shell direct Go WAL verification and negative controls'
        result['status'] = 'passed'
    except BaseException as error:
        result['error'] = str(error)
        event('failure', error=str(error))
        raise
    finally:
        for data in reversed(servers):
            if (data / 'postmaster.pid').exists():
                try:
                    pg('pg_ctl', '-D', data, '-m', 'immediate', '-w', '-t', '15', 'stop')
                except Exception as error:
                    result['status'] = 'failed'
                    result['cleanup_error'] = str(error)
        if minio is not None:
            minio.terminate()
            try:
                minio.wait(timeout=10)
            except subprocess.TimeoutExpired:
                minio.kill()
                minio.wait()
        for path in work.glob('*.log'):
            text = path.read_text(errors='replace')[-4*1024*1024:]
            for secret in secrets:
                text = text.replace(secret, '[REDACTED]')
            (evidence / path.name).write_text(text)
        result['events'] = seq
        result['elapsed_seconds'] = round(time.monotonic()-started, 3)
        (evidence / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        events.close()
        journal.close()
        print(f'{result["status"]}: evidence {evidence}', flush=True)
        # Retain only small curated evidence, never credentials or database files.
        if not any((data / 'postmaster.pid').exists() for data in servers):
            shutil.rmtree(work)
    if result['status'] != 'passed':
        raise RuntimeError('recovery cleanup failed')


if __name__ == '__main__':
    main()
