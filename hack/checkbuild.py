#!/usr/bin/env python3
"""Check produced artifacts, independently of the Dockerfile/copy algorithm."""
import json
import os
from pathlib import Path
import struct
import subprocess

from build import OUT


def main():
    binary = (OUT / 'cnpg-backup').read_bytes()
    if binary[:6] != b'\x7fELF\x02\x01':
        raise RuntimeError('expected little-endian ELF64 binary')
    phoff = struct.unpack_from('<Q', binary, 32)[0]
    phsize, phnum = struct.unpack_from('<HH', binary, 54)
    if any(struct.unpack_from('<I', binary, phoff + i*phsize)[0] in (2, 3) for i in range(phnum)):
        raise RuntimeError('Go binary has PT_DYNAMIC or PT_INTERP; not static')
    tools = {'pg_basebackup', 'pg_verifybackup', 'pg_combinebackup', 'pg_waldump', 'pg_controldata', 'psql'}
    for flavor in ('manager', 'pg18'):
        root = OUT / flavor
        for path in [root, *root.rglob('*')]:
            mode = path.stat().st_mode
            required = 0o555 if path.is_dir() else 0o444
            if mode & required != required:
                raise RuntimeError('runtime UID cannot read/traverse: ' + str(path))
        native = json.loads((root / 'usr/share/cnpg-backup/native-files.json').read_text()) if flavor == 'pg18' else []
        allowed = {'/usr/local/bin/cnpg-backup'} | {entry['path'] for entry in native}
        actual = {'/' + str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p.stat().st_mode & 0o111}
        if actual != allowed:
            raise RuntimeError('unexpected executable files: ' + repr(actual ^ allowed))
        if flavor == 'pg18':
            if {p.name for p in (root / 'usr/lib/postgresql/18/bin').iterdir()} != tools:
                raise RuntimeError('native executable allowlist differs')
            env = {k: v for k, v in os.environ.items() if not k.startswith('LD_')}
            loader = root / 'lib64/ld-linux-x86-64.so.2'
            for tool in sorted(tools):
                result = subprocess.check_output([loader, '--inhibit-cache', '--library-path', root / 'lib/x86_64-linux-gnu',
                                                  root / 'usr/lib/postgresql/18/bin' / tool, '--version'], env=env, text=True)
                if '18.6' not in result:
                    raise RuntimeError('wrong runtime native version')
    print('PASS: static ELF, exact executable allowlist, pinned loader/library tool execution')


if __name__ == '__main__':
    main()
