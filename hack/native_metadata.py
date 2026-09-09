#!/usr/bin/env python3
"""Read authenticated Debian control metadata without installing a package."""
import io
import re
import shutil
import subprocess
import tarfile


def fields(text):
    return dict(line.split(': ', 1) for line in text.splitlines()
                if line and not line[0].isspace() and ': ' in line)


def control_file(path):
    # Match bootstrap's Python 3.12 + dpkg-deb / Python 3.14 contract.
    if shutil.which('dpkg-deb'):
        return fields(subprocess.check_output(['dpkg-deb', '--field', str(path)], text=True, timeout=30))
    return control(path.read_bytes())


def control(data):
    if data[:8] != b'!<arch>\n':
        raise ValueError('not a Debian ar package')
    pos = 8
    while pos + 60 <= len(data):
        header = data[pos:pos + 60]
        size = int(header[48:58])
        name = header[:16].decode().strip().rstrip('/')
        body = data[pos + 60:pos + 60 + size]
        pos += 60 + size + size % 2
        if name.startswith('control.tar'):
            with tarfile.open(fileobj=io.BytesIO(body)) as tar:
                for member in tar:
                    if member.name.removeprefix('./') == 'control' and member.isfile():
                        text = tar.extractfile(member).read(1 << 20).decode()
                        return fields(text)
    raise ValueError('missing Debian control record')


def source_identity(fields):
    match = re.fullmatch(r'([a-z0-9+.-]+)(?: \(([^\s()]+)\))?', fields.get('Source', fields['Package']))
    if not match:
        raise ValueError('invalid Debian source identity')
    return match[1], match[2] or fields['Version']


def status_record(package, fields):
    if fields['Package'] != package['name'] or fields['Version'] != package['version']:
        raise ValueError('locked package/control mismatch')
    name, version = source_identity(fields)
    return (f'Package: {package["name"]}\nStatus: install ok installed\n'
            f'Architecture: {fields["Architecture"]}\nVersion: {package["version"]}\n'
            f'Source: {name} ({version})\n'
            'Description: selected files only; see native-files.json\n')
