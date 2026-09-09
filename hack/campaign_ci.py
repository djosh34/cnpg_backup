#!/usr/bin/env python3
"""Small Actions adapter; the runner, recipe and oracles are the local ones."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

from recovery_campaign import atomic_json, subject_image, load_bundle
from campaign_plan import make_plan, validate_results

INPUTS = Path('artifacts/repair-inputs')


def prepare():
    if os.environ.get('GITHUB_REPOSITORY') != 'djosh34/cnpg_backup':
        raise ValueError('repository-owned execution required')
    if os.environ.get('GITHUB_REF') not in ('refs/heads/main', 'refs/heads/implementation/pr-g', 'refs/heads/implementation/pr-h', 'refs/heads/implementation/pr-i', 'refs/heads/implementation/ci-reliability'):
        raise ValueError('untrusted harness branch')
    ref = os.environ['TRUSTED_REF']
    if ref not in ('main', 'implementation/pr-g', 'implementation/pr-h', 'implementation/pr-i', 'implementation/ci-reliability'):
        raise ValueError('untrusted subject branch')
    sha = os.environ['SUBJECT_SHA']
    if not re.fullmatch('[a-f0-9]{40}', sha):
        raise ValueError('exact subject commit required')
    subprocess.run(['git', 'merge-base', '--is-ancestor', sha, 'refs/remotes/origin/' + ref], check=True)
    images = {'manager': subject_image(os.environ['MANAGER_IMAGE'], 'manager'),
              'pg18': subject_image(os.environ['DATA_IMAGE'], 'pg18')}
    subject = {'revision': sha, 'images': images,
               'publication': {'run': 'https://github.com/djosh34/cnpg_backup/actions/runs/' + os.environ['GITHUB_RUN_ID']}}
    if os.environ.get('GITHUB_REF') == 'refs/heads/implementation/ci-reliability':
        frozen = json.loads(Path('.github/ci-repair-subject.json').read_text())
        if frozen['revision'] != sha or frozen['images'] != images:
            raise ValueError('CI REPAIR product subject is frozen; no alternate image/build')
        subject = frozen
    seeds = json.loads(os.environ['SEEDS'])
    if not isinstance(seeds, list) or not 1 <= len(seeds) <= 4 or any(type(s) is not int or not 0 <= s < 2**32 for s in seeds):
        raise ValueError('bounded explicit seed list required')
    if os.environ.get('GITHUB_REF') == 'refs/heads/implementation/ci-reliability' and seeds != [1806, 1806, 1807]:
        raise ValueError('CI REPAIR requires the exact repeat series plus grouped control')
    if os.environ['PROFILE'] != 'recovery':
        raise ValueError('shared full-fresh workflow requires recovery; use local focused diagnostics for partial scope')
    INPUTS.mkdir(parents=True, exist_ok=False)
    atomic_json(INPUTS / 'subject.json', subject)
    attempts = [{'attempt': n, 'seed': seed, 'layout': 'monolithic'} for n, seed in enumerate(seeds, 1)]
    if os.environ.get('GITHUB_REF') == 'refs/heads/implementation/ci-reliability':
        attempts.append({'attempt': len(attempts) + 1, 'seed': seeds[0], 'layout': 'grouped'})
    atomic_json(INPUTS / 'series.json', {'attempts': attempts})
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write('matrix=' + json.dumps({'include': [{'attempt': a['attempt']} for a in attempts]}) + '\n')


def plans():
    bundle = load_bundle(Path('.work/repair-bundle'))
    bundle.pop('directory')
    subject = json.loads((INPUTS / 'subject.json').read_text())
    series = json.loads((INPUTS / 'series.json').read_text())
    for attempt in series['attempts']:
        atomic_json(INPUTS / f"plan-{attempt['attempt']}.json", make_plan(subject, bundle, seed=attempt['seed'], layout=attempt['layout']))


def collect():
    attempt = os.environ['ATTEMPT']
    if not re.fullmatch('[1-4]', attempt):
        raise ValueError('invalid attempt identity')
    root = Path('.work') / ('repair-run-' + attempt)
    target = Path('artifacts/repair-results') / ('attempt-' + attempt)
    target.mkdir(parents=True, exist_ok=False)
    copied, dropped, size = [], [], 0
    roots = [(root / 'evidence', target)] + [(p, target / p.parent.name) for p in sorted(root.glob('fixture-*/evidence'))]
    for source, destination in roots:
        paths = sorted(source.rglob('*'), key=lambda p: (p.name not in ('manifest.json', 'first-failure.json', 'failures.json', 'events.jsonl'), p.suffix != '.json', str(p)))
        for path in paths:
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(source)
            if size + path.stat().st_size > 100 << 20:
                dropped.append(str(path))
                continue
            dest = destination / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, dest)
            size += path.stat().st_size
            copied.append(str(dest))
    atomic_json(target / 'upload.json', {'bytes': size, 'copied': len(copied), 'dropped': dropped, 'cap_bytes': 100 << 20})
    summary = root / 'evidence/summary.md'
    if summary.exists() and os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as output:
            output.write(summary.read_text())
    if not (target / 'manifest.json').exists():
        raise RuntimeError('missing run manifest; attempt incomplete')


def aggregate():
    inputs = Path('.work/inputs/artifacts/repair-inputs')
    series = json.loads((inputs / 'series.json').read_text())
    report = {'attempts': [], 'passed': False, 'release_qualified': False}
    executions = set()
    for attempt in series['attempts']:
        number = attempt['attempt']
        plan = json.loads((inputs / f'plan-{number}.json').read_text())
        paths = [p for p in Path('.work/results').rglob('manifest.json') if p.parent.name == f'attempt-{number}']
        record = {'attempt': number, 'seed': attempt['seed'], 'manifests': [str(p) for p in paths]}
        try:
            if len(paths) != 1:
                raise ValueError('missing or duplicate attempt artifact')
            result = json.loads(paths[0].read_text())
            record['failures'] = result.get('failures', [])
            identity = result.get('execution_id')
            if isinstance(identity, str):
                if identity in executions:
                    raise ValueError('duplicate execution identity across attempt artifacts')
                # Failed attempts own their identity too, across seeds/layouts.
                executions.add(identity)
            record.update(validate_results(plan, [result]), passed=True)
        except Exception as error:
            record.update(passed=False, diagnostic=str(error))
        report['attempts'].append(record)
    report['passed'] = all(a['passed'] for a in report['attempts'])
    Path('artifacts').mkdir(exist_ok=True)
    atomic_json(Path('artifacts/repair-aggregate.json'), report)
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    commands = {'prepare': prepare, 'plans': plans, 'collect': collect, 'aggregate': aggregate}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        raise SystemExit('usage: campaign_ci.py prepare|plans|collect|aggregate')
    raise SystemExit(commands[sys.argv[1]]() or 0)
