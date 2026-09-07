"""Copy license/notice texts only for external modules actually linked at runtime."""
import json
from pathlib import Path
import shutil


def linked_modules(inventory):
    decoder = json.JSONDecoder()
    text = inventory.read_text()
    modules = {}
    while text.strip():
        record, end = decoder.raw_decode(text.lstrip())
        text = text.lstrip()[end:]
        module = record.get('Module', {})
        if module and not module.get('Main'):
            if module.get('Replace'):
                raise RuntimeError('runtime module replacements are not release inputs')
            modules[module['Path']] = module
    return [modules[key] for key in sorted(modules)]


def copy_notices(inventory, root):
    records = []
    for module in linked_modules(inventory):
        source = Path(module['Dir'])
        name = module['Path'].replace('/', '_')
        target = root / 'usr/share/cnpg-backup/notices/go' / name
        target.mkdir(parents=True)
        files = [p for p in source.iterdir() if p.is_file() and p.name.upper().split('.')[0] in
                 ('LICENSE', 'LICENCE', 'NOTICE', 'COPYING', 'COPYRIGHT', 'PATENTS')]
        if not any(p.name.upper().startswith(('LICENSE', 'LICENCE', 'COPYING')) for p in files):
            raise RuntimeError('missing runtime module license: ' + module['Path'])
        for path in files:
            shutil.copyfile(path, target / path.name)
        records.append({'path': module['Path'], 'version': module['Version'], 'sum': module['Sum'],
                        'notices': sorted(str((target / p.name).relative_to(root)) for p in files)})
    (root / 'usr/share/cnpg-backup/go-runtime-modules.json').write_text(json.dumps(records, indent=2) + '\n')
