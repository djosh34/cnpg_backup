#!/usr/bin/env python3
"""Inventory production packages, tests and executable linkage separately.

Copy unmodified third-party license/notice texts (including package-local
licenses) for the modules actually used by production source. Do not import an
unused adapter into the executable to manufacture linkage evidence.
"""
import json
from pathlib import Path
import shutil
import subprocess

from bootstrap import REPO


def records(text):
    decoder = json.JSONDecoder()
    while text.strip():
        text = text.lstrip()
        value, end = decoder.raw_decode(text)
        yield value
        text = text[end:]


def inventory(out):
    scopes = {
        'executable': ['-deps', './cmd/cnpg-backup'],
        'production': ['-deps', './cmd/...', './internal/...'],
        'tests': ['-deps', '-test', './...'],
    }
    modules = {}
    for scope, args in scopes.items():
        raw = subprocess.check_output(['go', 'list', '-json', *args], cwd=REPO, text=True)
        (out / ('go-' + scope + '-packages.json')).write_text(raw)
        for package in records(raw):
            if package.get('CgoFiles'):
                raise RuntimeError('CGO package in ' + scope + ': ' + package['ImportPath'])
            module = package.get('Module', {})
            if not module or module.get('Main'):
                continue
            if module.get('Replace'):
                raise RuntimeError('unreviewed module replacement')
            key = module['Path'] + '@' + module['Version']
            entry = modules.setdefault(key, {'path': module['Path'], 'version': module['Version'],
                                            'dir': module['Dir'], 'scopes': set()})
            entry['scopes'].add(scope)
    dest = out / 'go-notices'
    dest.mkdir()
    result = []
    for key, entry in sorted(modules.items()):
        root = Path(entry.pop('dir'))
        entry['scopes'] = sorted(entry['scopes'])
        entry['notices'] = []
        if 'production' in entry['scopes']:
            for path in sorted(root.rglob('*')):
                if path.is_file() and path.name.upper().startswith(('LICENSE', 'NOTICE', 'COPYING', 'PATENTS')):
                    relative = Path(key) / path.relative_to(root)
                    target = dest / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(path, target)
                    entry['notices'].append(str(relative))
            if not entry['notices']:
                raise RuntimeError('missing third-party notice: ' + key)
        result.append(entry)
    (out / 'go-dependency-scopes.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    inventory(REPO / 'build/out')
