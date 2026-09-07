#!/usr/bin/env python3
"""Research only; private PG sockets, direct verification, archive-first tests.
PG_BIN/PG_SHARE/LD_LIBRARY_PATH select read-only PG18 tools.
Default is deterministic post-backup WAL; no race workload. Optional
RECOVERY_FIXTURE names the retained same-segment capture described in the report.
Keeps logs/commands, removes each stopped restore directory to bound disk/inodes.
"""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

BIN = Path(os.environ['PG_BIN'])
ROOT = Path(tempfile.mkdtemp(prefix='recovery-review-pg-'))
SOCKET = ROOT / 'socket'
SOCKET.mkdir()
PORT = '65432'
# The retained native fixture was initialized by this test-only role. New
# fixtures use a fixed role rather than the runner's operating-system username.
DBUSER = 'starlord' if os.environ.get('RECOVERY_FIXTURE') else 'cnpg_research'
servers = []
commands = []


def run(tool, *args, check=True):
    argv = [str(BIN / tool), *map(str, args)]
    result = subprocess.run(argv, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    commands.append({'argv': argv, 'exit': result.returncode, 'output': result.stdout})
    if check and result.returncode:
        raise RuntimeError(result.stdout)
    return result


def sql(q):
    return run('psql', '-XAt', '-h', SOCKET, '-p', PORT, '-U', DBUSER, '-d', 'postgres',
               '-v', 'ON_ERROR_STOP=1', '-c', q).stdout.strip()


def start(data, log):
    servers.append(data)
    return run('pg_ctl', '-D', data, '-l', log, '-t', '20', '-w', 'start', check=False)


def stop(data):
    return run('pg_ctl', '-D', data, '-m', 'immediate', '-w', 'stop', check=False)


def verify(data):
    run('pg_verifybackup', '--exit-on-error', '--no-parse-wal', data)
    for r in json.loads((data / 'backup_manifest').read_text())['WAL-Ranges']:
        run('pg_waldump', '--quiet', f'--path={data / "pg_wal"}',
            f'--timeline={r["Timeline"]}', f'--start={r["Start-LSN"]}',
            f'--end={r["End-LSN"]}')


print('artifacts:', ROOT, flush=True)
try:
    seed = ROOT / 'seed'
    original = ROOT / 'original-bundle'
    archive = ROOT / 'archive'
    fixture = os.environ.get('RECOVERY_FIXTURE')
    if fixture:
        # This exact capture has a real post-EndLSN restore point in its final
        # segment. Only padding after EndLSN was constructed by the researcher.
        old = Path(fixture)
        shutil.copytree(old / 'seed-1', seed)
        shutil.copytree(old / 'original-bundle', original)
        shutil.copytree(old / 'archive', archive)
        point = re.findall(r'RESTORE_POINT (r\d+_\d+)',
                           (old / 'post-end-1.log').read_text())[0]
        kind = 'same-segment-synthetic-padding'
    else:
        source = ROOT / 'source'
        args = ['-D', source, '-U', DBUSER, '-A', 'trust', '--no-locale']
        if os.environ.get('PG_SHARE'):
            args += ['-L', os.environ['PG_SHARE']]
        run('initdb', *args)
        with (source / 'postgresql.conf').open('a') as f:
            f.write(f"\nlisten_addresses=''\nport={PORT}\nunix_socket_directories='{SOCKET}'\n"
                    "wal_level=replica\nmax_wal_senders=5\nmax_replication_slots=5\narchive_mode=off\n")
        assert start(source, ROOT / 'source.log').returncode == 0
        sql('CREATE TABLE sentinel(id int); INSERT INTO sentinel VALUES (1)')
        run('pg_basebackup', '-h', SOCKET, '-p', PORT, '-U', DBUSER, '-D', seed, '-X', 'stream', '-c', 'fast')
        shutil.copytree(seed, original)
        sql('INSERT INTO sentinel VALUES (2)')
        point = 'postbackup_target'
        sql(f"SELECT pg_create_restore_point('{point}')")
        sql('SELECT pg_switch_wal()')
        archive.mkdir()
        for f in (source / 'pg_wal').iterdir():
            if re.fullmatch('[0-9A-F]{24}', f.name):
                shutil.copyfile(f, archive / f.name)
        assert stop(source).returncode == 0
        shutil.rmtree(source)
        kind = 'deterministic-postbackup'
    # Override the old fixture's private socket paths before starting clones.
    verify(original)
    verify(seed)
    r = json.loads((seed / 'backup_manifest').read_text())['WAL-Ranges'][0]
    results = {'version': run('postgres', '--version').stdout.strip(),
               'fixture_kind': kind, 'range': r, 'target_name': point,
               'direct_verification': 'PASS: no-parse-wal plus direct pg_waldump'}
    cases = [('bundle-miss', 'miss', 'immediate', True),
             ('bundle-fatal-negative', 'fatal', 'immediate', False),
             ('archive-preference', 'archive', point, True),
             ('required-archive-missing', 'fatal', point, False),
             ('bundle-as-archive-negative', 'bundle', point, False),
             ('transport-with-bundle', 'fatal', 'immediate', False)]
    for label, mode, target, success in cases:
        data = ROOT / label
        shutil.copytree(original if target == 'immediate' else seed, data)
        helper = ROOT / f'{label}.sh'
        action = 'exit 1'
        if mode == 'fatal':
            action = 'exit 255'
        elif mode in ('archive', 'bundle'):
            src = archive if mode == 'archive' else seed / 'pg_wal'
            action = f'if test -f "{src}/$1"; then cp "{src}/$1" "$2" || exit 255; exit 0; fi; exit 1'
        helper.write_text(f'#!/bin/sh\nprintf "%s\\n" "$1" >> "{ROOT}/{label}-requests.log"\ncase "$1" in *.history) exit 1;; esac\n{action}\n')
        helper.chmod(0o700)
        (data / 'postgresql.auto.conf').write_text('')
        (data / 'recovery.signal').touch()
        setting = "recovery_target='immediate'" if target == 'immediate' else f"recovery_target_name='{target}'"
        with (data / 'postgresql.conf').open('a') as f:
            f.write(f"\nlisten_addresses=''\nport={PORT}\nunix_socket_directories='{SOCKET}'\n"
                    f"restore_command='exec {helper} \"%f\" \"%p\"'\nrecovery_target_timeline='1'\nrecovery_target_action='promote'\n{setting}\n")
        log = ROOT / f'{label}.log'
        started = start(data, log)
        text = log.read_text()
        assert (started.returncode == 0) == success, text
        assert re.search('[0-9A-F]{24}', (ROOT / f'{label}-requests.log').read_text())
        if success:
            # pg_ctl may acknowledge hot-standby readiness before promotion.
            for _ in range(100):
                if sql('SELECT pg_is_in_recovery()') == 'f':
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(log.read_text())
            text = log.read_text()
            expected = '1,2' if not fixture and mode == 'archive' else '1'
            assert sql("SELECT string_agg(id::text, ',' ORDER BY id) FROM sentinel") == expected
            if mode == 'archive':
                assert f'recovery stopping at restore point "{point}"' in text, text
            assert stop(data).returncode == 0
        elif mode == 'fatal':
            assert 'FATAL' in text and '255' in text, text
        else:
            assert 'recovery ended before configured recovery target was reached' in text, text
        results[label] = 'PASS (recovered)' if success else 'PASS (startup rejected)'
        # Only remove after successful stop or confirmed failed startup.
        shutil.rmtree(data)
    wal = next(f for f in (seed / 'pg_wal').iterdir() if re.fullmatch('[0-9A-F]{24}', f.name))
    wal.rename(seed / 'withheld-wal')
    failed = run('pg_waldump', '--quiet', f'--path={seed / "pg_wal"}',
                 f'--timeline={r["Timeline"]}', f'--start={r["Start-LSN"]}',
                 f'--end={r["End-LSN"]}', check=False)
    assert failed.returncode != 0
    results['direct_missing_wal'] = 'PASS (parser rejected)'
    (ROOT / 'result.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2), flush=True)
finally:
    for data in servers:
        if data.exists():
            stop(data)
    (ROOT / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
