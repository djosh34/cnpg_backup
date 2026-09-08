#!/usr/bin/env python3
"""Build and inventory actual scratch images; no registry writes."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

from bootstrap import REPO
from build import OUT


def docker(*args, check=True):
    return subprocess.run(['docker', *map(str, args)], check=check, capture_output=True)


def main():
    images = {}
    for flavor in ('manager', 'pg18'):
        tag = 'cnpg-backup-foundation-' + flavor + ':test'
        subprocess.run(['docker', 'build', '--network=none', '--target', flavor,
                        '--label', 'org.opencontainers.image.source=https://github.com/djosh34/cnpg_backup',
                        '--label', 'org.opencontainers.image.revision=' + subprocess.check_output(
                            ['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
                        '-f', 'build/Dockerfile', '-t', tag, '.'], cwd=REPO, check=True)
        image = docker('image', 'inspect', tag).stdout
        info = json.loads(image)[0]
        image_id = info['Id']
        if info['Config']['User'] != '26:26':
            raise RuntimeError('wrong runtime UID')
        container = docker('create', image_id, 'version').stdout.decode().strip()
        try:
            # Bounded by these small foundation roots; save actual export inventory,
            # not merely a Dockerfile assertion or a listing of the staging root.
            exported = docker('export', container).stdout
            expected = {p['path']: p for p in json.loads((OUT / (flavor + '-files.json')).read_text())}
            actual = {}
            with tarfile.open(fileobj=io.BytesIO(exported)) as tar:
                for m in tar:
                    name = '/' + m.name.removeprefix('./')
                    # Moby's init layer adds this exact link and empty console
                    # placeholder even to scratch containers (not image layers).
                    if name == '/etc/mtab' and m.issym() and m.linkname == '/proc/mounts':
                        continue
                    if name == '/dev/console' and m.isfile() and m.size == 0:
                        continue
                    if m.isfile():
                        if name in ('/.dockerenv', '/etc/hosts', '/etc/hostname', '/etc/resolv.conf'):
                            continue  # Docker-injected files, not image layers.
                        content = tar.extractfile(m).read()
                        actual[name] = {'path': name, 'bytes': len(content),
                                        'sha256': hashlib.sha256(content).hexdigest(),
                                        'mode': oct(m.mode & 0o777)}
                    elif not m.isdir():
                        raise RuntimeError('unexpected image link/special file: ' + m.name)
            if actual != expected:
                raise RuntimeError(f'{flavor}: exported image differs from audited root')
            (OUT / (flavor + '-export.json')).write_text(json.dumps(list(actual.values()), indent=2) + '\n')
        finally:
            docker('rm', container)
        docker('run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL', image_id, 'version')
        for mode, expected_exit in [('manager', 2), ('instance', 2), ('recovery-job', 2), ('recovery-guard', 2), ('wal-fetch', 255)]:
            result = docker('run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL', image_id, mode, check=False)
            if result.returncode != expected_exit:
                raise RuntimeError(f'unsafe {mode} exit: {result.returncode}')
        if flavor == 'pg18':
            for name in ('pg_basebackup', 'pg_verifybackup', 'pg_combinebackup', 'pg_waldump', 'pg_controldata', 'psql'):
                result = docker('run', '--rm', '--network=none', '--read-only', '--cap-drop=ALL', '--entrypoint',
                                '/usr/lib/postgresql/18/bin/' + name, image_id, '--version')
                if b'18.6' not in result.stdout:
                    raise RuntimeError('wrong tool version')
        images[flavor] = image_id
        (OUT / (flavor + '-inspect.json')).write_bytes(image)
    (OUT / 'images.json').write_text(json.dumps(images, indent=2) + '\n')


if __name__ == '__main__':
    main()
