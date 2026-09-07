#!/usr/bin/env python3
"""Build static Go artifacts and deterministic minimal image filesystem inputs."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

from bootstrap import CACHE, LOCK, REPO

OUT = REPO / 'build/out'


def run(*argv, output=None):
    result = subprocess.run(argv, cwd=REPO, check=True, stdout=subprocess.PIPE if output else None)
    if output:
        Path(output).write_bytes(result.stdout)


def inventory(root):
    records = []
    for p in sorted(root.rglob('*')):
        if p.is_file():
            records.append({'path': '/' + str(p.relative_to(root)), 'bytes': p.stat().st_size,
                            'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
                            'mode': oct(p.stat().st_mode & 0o777)})
    return records


if __name__ == '__main__':
    os.umask(0o022)  # Runtime UID 26 must traverse/read roots built by any host UID.
    os.environ.update(CGO_ENABLED='0', GOTOOLCHAIN='local', GOOS='linux', GOARCH='amd64',
                      GOAMD64='v1', GOEXPERIMENT='', GOFLAGS='', GOWORK='off')
    os.environ['PATH'] = str(CACHE / 'go/bin') + ':' + os.environ['PATH']
    (REPO / '.work/tmp').mkdir(parents=True, exist_ok=True)
    os.environ['GOTMPDIR'] = str(REPO / '.work/tmp')
    if subprocess.check_output(['go', 'env', 'GOVERSION'], text=True).strip() != 'go1.27.1':
        raise RuntimeError('wrong Go toolchain')
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    binary = OUT / 'cnpg-backup'
    args = ['go', 'build', '-trimpath', '-buildvcs=false', '-ldflags=-buildid= -X main.revision=' + revision]
    run(*args, '-o', binary, './cmd/cnpg-backup')
    run(*args, '-o', OUT / 'reproducibility-check', './cmd/cnpg-backup')
    if binary.read_bytes() != (OUT / 'reproducibility-check').read_bytes():
        raise RuntimeError('non-reproducible Go build')
    (OUT / 'reproducibility-check').unlink()
    run(binary, 'version', output=OUT / 'version.txt')
    run('go', 'version', '-m', binary, output=OUT / 'go-version.txt')
    run('go', 'list', '-m', '-json', 'all', output=OUT / 'go-modules.json')
    run('go', 'list', '-deps', '-json', './cmd/cnpg-backup', output=OUT / 'go-linked.json')
    cgo = subprocess.check_output(['go', 'list', '-deps', '-f', '{{if .CgoFiles}}{{.ImportPath}}{{end}}', './cmd/cnpg-backup'], text=True)
    if cgo.strip():
        raise RuntimeError('CGO packages linked: ' + cgo)
    run('go', 'build', '-trimpath', '-buildvcs=false', '-o', OUT / 'verify', './hack/verify')
    run('go', 'run', './hack/imagebuild', CACHE / 'pgroot', OUT / 'pg18')
    (OUT / 'manager').mkdir()
    owners = json.loads((CACHE / 'package-owners.json').read_text())
    native_path = OUT / 'pg18/usr/share/cnpg-backup/native-files.json'
    native_files = json.loads(native_path.read_text())
    for entry in native_files:
        entry['package'] = owners[entry['package_path']]
    native_path.write_text(json.dumps(native_files, indent=2) + '\n')
    for flavor in ('manager', 'pg18'):
        root = OUT / flavor
        (root / 'usr/local/bin').mkdir(parents=True, exist_ok=True)
        shutil.copyfile(binary, root / 'usr/local/bin/cnpg-backup')
        (root / 'usr/local/bin/cnpg-backup').chmod(0o755)
        etc = root / 'etc'
        (etc / 'ssl/certs').mkdir(parents=True)
        (etc / 'passwd').write_text('postgres:x:26:26:PostgreSQL:/nonexistent:/nonexistent\n')
        (etc / 'group').write_text('postgres:x:26:\n')
        certs = sorted((CACHE / 'pgroot/usr/share/ca-certificates/mozilla').glob('*.crt'))
        if not certs:
            raise RuntimeError('missing pinned CA certificates')
        (etc / 'ssl/certs/ca-certificates.crt').write_bytes(b''.join(p.read_bytes() for p in certs))
        notices = root / 'usr/share/cnpg-backup/notices'
        notices.mkdir(parents=True, exist_ok=True)
        for name in ('LICENSE', 'THIRD_PARTY_NOTICES.md'):
            shutil.copyfile(REPO / name, notices / name)
        for name in ('LICENSE', 'PATENTS'):
            shutil.copyfile(CACHE / 'go' / name, notices / ('Go-' + name))
        shutil.copytree(CACHE / 'pgroot/usr/share/common-licenses', notices / 'common-licenses', symlinks=False)
        shipped = {'ca-certificates'}
        if flavor == 'pg18':
            shipped.update(entry['package'] for entry in native_files)
        native_packages = [p for p in LOCK['packages'] if p['name'] in shipped]
        (root / 'usr/share/cnpg-backup/native-packages.json').write_text(json.dumps(native_packages, indent=2) + '\n')
        status = root / 'var/lib/dpkg/status'
        status.parent.mkdir(parents=True)
        status.write_text('\n'.join(f'Package: {p["name"]}\nStatus: install ok installed\nArchitecture: amd64\nVersion: {p["version"]}\nDescription: selected files only; see native-files.json\n' for p in native_packages))
        for package in native_packages:
            copyright = CACHE / 'pgroot/usr/share/doc' / package['name'] / 'copyright'
            if not copyright.exists():
                raise RuntimeError('missing notice: ' + package['name'])
            shutil.copyfile(copyright, notices / (package['name'] + '.copyright'))
        shutil.copyfile(REPO / 'build/inputs.lock.json', notices / 'inputs.lock.json')
        (OUT / (flavor + '-files.json')).write_text(json.dumps(inventory(root), indent=2) + '\n')
    # Sparse test library path: deliberately does not replace the host's libc.
    # Server-only libraries and host tools remain test dependencies, not images.
    lib = OUT / 'test-libs'
    lib.mkdir()
    for pattern in ('libpq.so.*', 'libicu*.so.*', 'liburing.so.*'):
        for p in (CACHE / 'pgroot/usr/lib/x86_64-linux-gnu').glob(pattern):
            shutil.copyfile(p, lib / p.name)
    print('Built static binary, reproducibility check, linked inventory and scratch roots')
