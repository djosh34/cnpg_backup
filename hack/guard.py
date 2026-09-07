#!/usr/bin/env python3
"""Real Docker PID1/private-namespace tests. Not CNPG/kind acceptance.

Uses the production guard/sidecar and a clearly test-only original-command
fixture. No pulled image, production endpoint, privileged container or host PID.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/guard'
WORK = ROOT / '.work/guard'
EVENTS = []
CONTAINERS = []
IMAGE = 'cnpg-backup-guard-test:' + subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()


def run(*args, check=True):
    p = subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
    if check and p.returncode:
        raise RuntimeError(f'{args}: {p.stdout}')
    return p.stdout.strip()


def event(name):
    EVENTS.append({'scenario': name, 'observed_at': time.time()})
    (OUT / 'events.json').write_text(json.dumps(EVENTS, indent=2) + '\n')


def wait_for(test, description, seconds=20):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if test():
            return
        time.sleep(0.05)
    raise RuntimeError('barrier timed out: ' + description)


def alive(name):
    return run('docker', 'inspect', '-f', '{{.State.Running}}', name) == 'true'


def exit_code(name, expected):
    wait_for(lambda: not alive(name), 'exit ' + name, 40)
    actual = int(run('docker', 'inspect', '-f', '{{.State.ExitCode}}', name))
    if actual != expected:
        raise RuntimeError(f'{name}: exit {actual}, expected {expected}: ' + run('docker', 'logs', name))


class Fixture:
    def __init__(self, name, mode='clean'):
        self.name, self.mode, self.pod = name, mode, str(uuid.uuid4())
        self.root = WORK / name
        self.root.mkdir()
        self.targets = [('data', '/var/lib/postgresql/data'), ('wal', '/var/lib/postgresql/wal'),
                        ('tbs', '/var/lib/postgresql/tablespaces/fast_space')]
        for path in ['config', 'bin', 'plugins', 'test', *[a for a, _ in self.targets]]:
            (self.root / path).mkdir(mode=0o700)
        config = {'clusterUID': str(uuid.uuid4()), 'operationUID': str(uuid.uuid4()),
                  'targets': [{'pvcUID': str(uuid.uuid4()), 'mount': b} for _, b in self.targets]}
        (self.root / 'config/guard.json').write_text(json.dumps(config))
        self.sidecar = self.start('sidecar', self.pod, fixture=(mode == 'writes'))
        wait_for(lambda: self.probe(), 'startup-probed helper + same socket')
        self.main = self.start('main', self.pod)
        self.wait('main-started')
        self.before = self.preflight()
        assert all(x == (self.pod + '\n').encode() for x in self.before)
        original = json.loads((self.root / 'test/original-argv.json').read_text())
        assert original == ['/controller/manager', 'instance', 'restore', '--pg-wal', '/var/lib/postgresql/wal/pg_wal']

    def start(self, role, pod, fixture=False):
        name = 'cnpg-guard-' + self.name + '-' + role + '-' + uuid.uuid4().hex[:6]
        args = ['docker', 'run', '-d', '--name', name, '--network=none', '--read-only', '--cap-drop=ALL',
                '--security-opt=no-new-privileges', '--user', f'{os.getuid()}:{os.getgid()}',
                '--memory=256m', '--cpus=1', '--pids-limit=128', '-e', 'POD_UID=' + pod, '-e', 'SCENARIO=' + self.mode]
        mounts = [('config', '/cnpg-backup/config', True), ('bin', '/cnpg-backup/bin', role != 'sidecar'),
                  ('plugins', '/plugins', False), ('test', '/test', False)]
        mounts += [(a, b, False) for a, b in self.targets]
        for source, target, readonly in mounts:
            args += ['--mount', f'type=bind,source={self.root / source},target={target}' + (',readonly' if readonly else '')]
        if role == 'sidecar':
            command = ['/controller/manager', 'sidecar-writes'] if fixture else ['/usr/local/bin/cnpg-backup', 'recovery-job']
        else:
            command = ['/cnpg-backup/bin/cnpg-backup', 'recovery-guard', '--', '/controller/manager', 'instance', 'restore',
                       '--pg-wal', '/var/lib/postgresql/wal/pg_wal']
        run(*args, IMAGE, *command)
        CONTAINERS.append(name)
        return name

    def probe(self):
        return subprocess.run(['docker', 'exec', self.sidecar, '/usr/local/bin/cnpg-backup', 'recovery-job', '--probe'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5).returncode == 0

    def wait(self, name):
        wait_for(lambda: (self.root / 'test' / name).exists(), name)

    def touch(self, name):
        (self.root / 'test' / name).touch()

    def preflight(self):
        return [(self.root / a / 'preflight').read_bytes() for a, _ in self.targets]

    def markers(self):
        return [(self.root / a / '.cnpg-backup/owner.json').exists() for a, _ in self.targets]

    def reject_replacement(self):
        replacement = self.start('replacement', str(uuid.uuid4()))
        exit_code(replacement, 2)
        assert self.preflight() == self.before, 'replacement mutated target before rejection'
        assert all(self.markers()), 'poison marker missing'

    def finish(self, expected=0):
        self.touch('main-release')
        exit_code(self.main, expected)
        assert not any(self.markers()), 'clean ownership was not released'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    # Refuse stale test state rather than deleting unknown resources.
    WORK.mkdir(parents=True, exist_ok=False)
    context = WORK / 'image'
    context.mkdir()
    shutil.copyfile(ROOT / 'build/out/cnpg-backup', context / 'cnpg-backup')
    run('go', 'build', '-trimpath', '-o', str(context / 'fixture'), './hack/guardfixture')
    (context / 'Dockerfile').write_text('FROM scratch\nCOPY cnpg-backup /usr/local/bin/cnpg-backup\nCOPY fixture /controller/manager\n')
    run('docker', 'build', '--network=none', '-t', IMAGE, str(context))
    manifest = {'subject_sha': run('git', 'rev-parse', 'HEAD'), 'image_id': run('docker', 'image', 'inspect', '-f', '{{.Id}}', IMAGE),
                'profile': 'guard-private-pid-namespace', 'real_cnpg': False, 'release_qualified': False,
                'limitations': ['CNPG command is a test fixture; real CNPG/kind acceptance is separate and mandatory.']}
    (OUT / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    try:
        f = Fixture('live')
        f.reject_replacement()
        f.finish()
        event('live-lock-blocks-replacement-preflight-and-clean-release')
        # Terminal sidecar cannot rebind even when markers have been cleaned.
        replacement = f.start('same-sidecar', str(uuid.uuid4()))
        exit_code(replacement, 2)
        assert f.preflight() == f.before and all(f.markers())
        event('terminal-sidecar-rejects-new-session-and-poisons-uncertain-begin')
        f = Fixture('detached', 'detached')
        f.wait('detached-ready')
        f.touch('main-release')
        time.sleep(0.5)
        assert alive(f.main) and all(f.markers()), 'released before detached descendant died'
        f.reject_replacement()
        exit_code(f.main, 0)
        assert not any(f.markers())
        event('setsid-orphan-reaped-before-release')
        f = Fixture('writes', 'writes')
        f.wait('before-begin-rejected')
        f.wait('write-pending')
        f.touch('main-release')
        f.wait('draining')
        assert alive(f.main) and all(f.markers())
        f.reject_replacement()
        f.touch('write-release')
        f.wait('stale-rejected')
        exit_code(f.main, 0)
        assert not any(f.markers()) and (f.root / 'data/delayed-write').exists()
        event('same-session-terminal-drain-waits-for-real-delayed-write')
        for victim in ('sidecar', 'main'):
            f = Fixture(victim + '-crash', 'detached')
            f.wait('detached-ready')
            run('docker', 'kill', '--signal=KILL', getattr(f, victim))
            exit_code(f.main, 137 if victim == 'main' else 2)
            assert all(f.markers())
            f.reject_replacement()
            event(victim + '-crash-retains-poison-on-all-targets')
        f = Fixture('signal')
        run('docker', 'kill', '--signal=TERM', f.main)
        exit_code(f.main, 2)
        f.reject_replacement()
        event('guard-term-is-uncertain-not-clean-release')
        f = Fixture('failure', 'exit7')
        f.finish(7)
        event('clean-drain-preserves-original-failure-exit')
        Fixture('fresh-cluster-all-fresh-pvcs').finish()
        event('fresh-cluster-all-fresh-targets-succeeds-after-poison')
        print('PASS: real PID1/Unix-session guard tests; NOT real CNPG acceptance')
    finally:
        for name in CONTAINERS:
            (OUT / (name + '.log')).write_text(run('docker', 'logs', name, check=False)[-128000:])
            run('docker', 'rm', '-f', name, check=False)
        run('docker', 'image', 'rm', IMAGE, check=False)


if __name__ == '__main__':
    main()
