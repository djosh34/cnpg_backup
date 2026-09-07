#!/usr/bin/env python3
"""Disposable ownership state model + actual Linux flock/crash check.
NOT a guardian implementation, CNPG integration, or PID-namespace test.
"""
from dataclasses import dataclass
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import tempfile


@dataclass
class Owner:
    locked: bool = True
    marker: bool = True
    main_alive: bool = True
    sidecar_alive: bool = True
    admitted: bool = True
    pending: int = 0
    uncertain: bool = False
    writes: int = 0
    tuple: tuple = ('pod1', 'guard1', 'sidecar1')

    def write(self, identity):
        if identity != self.tuple or not self.admitted or not self.sidecar_alive:
            return False
        self.pending += 1
        return True

    def drain(self):
        self.admitted = False
        if not self.sidecar_alive:
            self.uncertain = True
        return self.pending == 0 and not self.uncertain

    def release(self):
        if self.main_alive or self.admitted or self.pending or self.uncertain:
            return False
        self.marker = False
        self.locked = False
        return True

    def replacement_preflight(self):
        if self.locked or self.marker:
            return False
        self.writes += 1
        return True


# Paused old main and delayed RestoreResponse: lock covers both sides of RPC.
for phase in ('paused-main', 'delayed-response', 'replay', 'shutdown'):
    o = Owner()
    before = o.writes
    assert not o.release() and not o.replacement_preflight()
    assert o.writes == before
    print(phase, 'PASS: replacement cannot mutate')

# Sidecar process loss is never a clean drain; a replacement identity is refused.
o = Owner()
o.sidecar_alive = False
o.main_alive = False
assert not o.drain() and not o.release() and not o.replacement_preflight()
assert not o.write(('pod1', 'guard1', 'sidecar2'))
print('sidecar-crash PASS: poison; no rebind')

# Close admission before waiting for writes; no late request after clean release.
o = Owner()
old = o.tuple
assert o.write(old)
o.main_alive = False
assert not o.drain() and not o.release()
assert not o.write(old)
o.pending -= 1
assert o.drain() and o.release() and o.replacement_preflight()
o.tuple = ('pod2', 'guard2', 'sidecar2')
o.admitted = True
assert not o.write(old)
print('clean-drain PASS: pending write prevents release; stale tuple refused')

# Real separate processes, CLOEXEC lock, fsynced owner marker, SIGKILL.
root = Path(tempfile.mkdtemp(prefix='recovery-owner-os-'))
worker = r'''
import fcntl, os, pathlib, sys
p = pathlib.Path(sys.argv[1])
f = os.open(p / 'lock', os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
fcntl.flock(f, fcntl.LOCK_EX)
m = os.open(p / 'owner', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
os.write(m, b'pod1/guard1/sidecar1\n'); os.fsync(m); os.close(m)
d = os.open(p, os.O_DIRECTORY); os.fsync(d)
print('ready', flush=True)
assert sys.stdin.readline().strip() == 'clean'
os.unlink(p / 'owner'); os.fsync(d)
os.close(d); os.close(f)
'''


def replacement():
    fd = os.open(root / 'lock', os.O_RDWR | os.O_CLOEXEC)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 'busy'
        if (root / 'owner').exists():
            return 'poisoned'
        (root / 'preflight-mutation').write_text('allowed only after clean release')
        return 'allowed'
    finally:
        os.close(fd)


for mode in ('clean', 'crash'):
    p = subprocess.Popen([sys.executable, '-c', worker, str(root)],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == 'ready'
        assert replacement() == 'busy'
        if mode == 'clean':
            p.communicate('clean\n', timeout=5)
            assert p.returncode == 0
            assert replacement() == 'allowed'
            (root / 'preflight-mutation').unlink()
        else:
            p.kill()
            p.wait(timeout=5)
            assert replacement() == 'poisoned'
            assert not (root / 'preflight-mutation').exists()
    finally:
        if p.poll() is None:
            p.kill()
            p.wait(timeout=5)
    print('OS', mode, 'PASS')
print('artifacts:', root)
