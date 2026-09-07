#!/usr/bin/env python3
"""Fetch checksum-locked build/test inputs into a private, user-owned cache.
No package installation, maintainer scripts, daemon, or ambient apt resolution.
Python >=3.12 + dpkg-deb, or Python >=3.14 (zstd support), required.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import urllib.request

REPO = Path(__file__).resolve().parents[1]
LOCK = json.loads((REPO / 'build/inputs.lock.json').read_text())
CACHE = Path(os.environ.get('CNPG_BUILD_CACHE', REPO / '.work/tools')).resolve()


def download(spec, name):
    dest = CACHE / 'downloads' / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        temp = dest.with_suffix('.partial')
        with urllib.request.urlopen(spec['url'], timeout=120) as src, temp.open('wb') as out:
            shutil.copyfileobj(src, out)
        temp.replace(dest)
    with dest.open('rb') as f:
        actual = hashlib.file_digest(f, 'sha256').hexdigest()
    if actual != spec['sha256']:
        raise RuntimeError(f'checksum mismatch: {dest}; remove it before retry')
    return dest


def deb_extract(package, root):
    if shutil.which('dpkg-deb'):
        subprocess.run(['dpkg-deb', '--extract', package, root], check=True)
        proc = subprocess.Popen(['dpkg-deb', '--fsys-tarfile', package], stdout=subprocess.PIPE)
        with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
            names = [m.name.removeprefix('./') for m in tar if m.isfile() or m.issym() or m.islnk()]
        proc.stdout.close()
        if proc.wait() != 0:
            raise RuntimeError('package inventory failed')
        return names
    if sys.version_info < (3, 14):
        raise RuntimeError('need dpkg-deb or Python >=3.14 for locked zstd packages')
    data = package.read_bytes()
    assert data[:8] == b'!<arch>\n'
    pos = 8
    while pos < len(data):
        head = data[pos:pos + 60]
        size = int(head[48:58])
        name = head[:16].decode().strip().rstrip('/')
        body = data[pos + 60:pos + 60 + size]
        pos += 60 + size + size % 2
        if name.startswith('data.tar'):
            # Only checksum-authenticated distro packages, never backup inputs.
            with tarfile.open(fileobj=io.BytesIO(body)) as tar:
                tar.extractall(root, filter='fully_trusted')
                return [m.name.removeprefix('./') for m in tar.getmembers() if m.isfile() or m.issym() or m.islnk()]
    raise RuntimeError(f'no data archive: {package}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go-only', action='store_true', help='unit feedback: verify Go without fetching native/MinIO inputs')
    args = parser.parse_args(argv)
    os.umask(0o022)
    if os.uname().machine != 'x86_64' or sys.platform != 'linux':
        raise RuntimeError('initial build supports Linux amd64 only')
    CACHE.mkdir(parents=True, exist_ok=True)
    go = download(LOCK['go'], 'go.tar.gz')
    if not (CACHE / 'go/bin/go').exists():
        with tarfile.open(go) as tar:
            tar.extractall(CACHE, filter='data')
    if args.go_only:
        print(f'Go input verified in {CACHE}; native/MinIO inputs not requested')
        return
    minio = download(LOCK['minio'], 'minio')
    minio.chmod(0o755)
    root = CACHE / 'pgroot'
    stamp = CACHE / 'packages.sha256'
    lock_hash = hashlib.sha256((REPO / 'build/inputs.lock.json').read_bytes()).hexdigest()
    packages = [(p, download(p, p['name'] + '.deb')) for p in LOCK['packages']]
    owners_path = CACHE / 'package-owners.json'
    if not stamp.exists() or stamp.read_text() != lock_hash or not owners_path.exists():
        if root.exists():
            shutil.rmtree(root)
        root.mkdir()
        owners = {}
        for package, path in packages:
            for name in deb_extract(path, root):
                owners[name] = package['name']
        owners_path.write_text(json.dumps(owners, sort_keys=True) + '\n')
        stamp.write_text(lock_hash)
    print(f'Inputs verified in {CACHE}')


if __name__ == '__main__':
    main()
