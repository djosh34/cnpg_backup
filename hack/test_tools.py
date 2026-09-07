#!/usr/bin/env python3
"""Checksum-pinned test-only PromQL engine. Nothing enters runtime image roots."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]

def promtool():
    pin = json.loads((ROOT / 'build/test-tools.lock.json').read_text())['promtool']
    cache = Path(os.environ.get('CNPG_TEST_TOOL_CACHE', ROOT / '.work/test-tools')).resolve()
    cache.mkdir(parents=True, exist_ok=True)
    executable = cache / 'promtool'
    def digest(path):
        with path.open('rb') as source:
            return hashlib.file_digest(source, 'sha256').hexdigest()
    if executable.exists():
        if digest(executable) != pin['binary_sha256']:
            raise RuntimeError('cached promtool checksum mismatch')
        return executable
    archive = cache / 'prometheus.tar.gz'
    if not archive.exists():
        temporary = cache / 'download.partial'
        with urllib.request.urlopen(pin['url'], timeout=120) as source, temporary.open('wb') as output:
            shutil.copyfileobj(source, output, 128 * 1024)
        temporary.replace(archive)
    if digest(archive) != pin['sha256']:
        raise RuntimeError('promtool archive checksum mismatch')
    with tarfile.open(archive) as reader:
        member = reader.getmember(pin['member'])
        if not member.isfile() or member.size > 256 * 1024**2:
            raise RuntimeError('unexpected test executable member')
        with reader.extractfile(member) as source, executable.open('xb') as output:
            shutil.copyfileobj(source, output, 128 * 1024)
    if digest(executable) != pin['binary_sha256']:
        raise RuntimeError('promtool executable checksum mismatch')
    executable.chmod(0o555)
    return executable

if __name__ == '__main__':
    print(promtool())
