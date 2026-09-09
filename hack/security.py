#!/usr/bin/env python3
"""Scan exact existing candidates. Hosted tooling only; never rebuild the subject."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import urllib.request

from bootstrap import REPO
from godeps import records

OUT = REPO / 'artifacts/security'
WORK = REPO / '.work/security'
PINS = json.loads((REPO / 'build/security-tools.lock.json').read_text())
FLAVORS = ('manager', 'pg18')


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def run(*args, output=None, allowed=(0,)):
    # Never echo subprocess output on error (registry or scanner secret context).
    with (output or WORK / 'command.log').open('wb') as stream:
        result = subprocess.run([str(a) for a in args], stdout=stream, stderr=subprocess.PIPE, timeout=1200)
    if result.returncode not in allowed:
        raise RuntimeError(f'{Path(args[0]).name} failed (exit {result.returncode}); no success evidence')
    return result.returncode


def fetch(spec, target, cap=256 << 20):
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        part = target.with_suffix('.partial')
        size = 0
        with urllib.request.urlopen(spec['url'], timeout=120) as src, part.open('wb') as dst:
            while chunk := src.read(1 << 20):
                size += len(chunk)
                if size > cap:
                    raise ValueError('download exceeds security input bound')
                dst.write(chunk)
        part.replace(target)
    if sha256(target) != spec['sha256']:
        raise ValueError('security input checksum mismatch')
    return target


def tools():
    WORK.mkdir(parents=True, exist_ok=True)
    bindir = WORK / 'bin'
    bindir.mkdir(exist_ok=True)
    for name in ('syft', 'trivy'):
        archive = fetch(PINS[name], WORK / (name + '.tar.gz'))
        with tarfile.open(archive) as tar:
            member = tar.getmember(name)
            if not member.isfile() or member.size > 256 << 20:
                raise ValueError('invalid scanner executable')
            with tar.extractfile(member) as src, (bindir / name).open('wb') as dst:
                shutil.copyfileobj(src, dst)
        (bindir / name).chmod(0o755)
    pin = PINS['govulncheck']
    module = pin['module'] + '@' + pin['version']
    run('go', 'mod', 'download', '-json', module, output=WORK / 'vuln-module.json')
    if json.loads((WORK / 'vuln-module.json').read_text())['Sum'] != pin['sum']:
        raise ValueError('govulncheck module checksum mismatch')
    os.environ['GOBIN'] = str(bindir)
    run('go', 'install', pin['module'] + '/cmd/govulncheck@' + pin['version'])


def subject(sha, manager, data, ref):
    if os.environ.get('GITHUB_REPOSITORY') != 'djosh34/cnpg_backup':
        raise ValueError('repository-owned execution required')
    trusted = ('main', 'implementation/pr-j', 'implementation/pr-i')
    if os.environ.get('GITHUB_EVENT_NAME') not in ('push', 'workflow_dispatch') or os.environ.get('GITHUB_REF') not in tuple('refs/heads/' + r for r in trusted):
        raise ValueError('untrusted scanner workflow')
    if ref not in trusted or not re.fullmatch('[a-f0-9]{40}', sha):
        raise ValueError('untrusted subject revision/ref')
    run('git', 'merge-base', '--is-ancestor', sha, 'refs/remotes/origin/' + ref)
    images = dict(zip(FLAVORS, (manager, data)))
    for flavor, image in images.items():
        if not re.fullmatch('ghcr.io/djosh34/cnpg-backup-' + flavor + '@sha256:[a-f0-9]{64}', image):
            raise ValueError('canonical immutable image required')
    return {'revision': sha, 'images': images, 'release_qualified': False,
            'harness_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
            'run_id': os.environ['GITHUB_RUN_ID'], 'run_attempt': os.environ['GITHUB_RUN_ATTEMPT']}


def image_files(archive, flavor):
    """Inventory actual exported bytes and verify the selected native closure."""
    inventory, native, packages = {}, [], None
    executable = set()
    with tarfile.open(archive) as tar:
        for member in tar:
            name = '/' + member.name.removeprefix('./')
            if member.issym() and name == '/etc/mtab' and member.linkname == '/proc/mounts':
                continue  # Moby-injected init layer, not image payload.
            if not member.isfile():
                if not member.isdir():
                    raise ValueError('unexpected image link/special file')
                continue
            if name in inventory or '..' in Path(name).parts:
                raise ValueError('duplicate or unsafe exported path')
            src = tar.extractfile(member)
            digest = hashlib.file_digest(src, 'sha256').hexdigest()
            inventory[name] = {'sha256': digest, 'bytes': member.size, 'mode': oct(member.mode & 0o777)}
            if member.mode & 0o111:
                executable.add(name)
            if name == '/usr/local/bin/cnpg-backup':
                with tar.extractfile(member) as src, (WORK / (flavor + '-binary')).open('wb') as dst:
                    shutil.copyfileobj(src, dst)
            if name.startswith('/usr/share/cnpg-backup/') and name.endswith(('native-files.json', 'native-packages.json', 'go-dependency-scopes.json')):
                if member.size > 8 << 20:
                    raise ValueError('oversized closure metadata')
                value = json.load(tar.extractfile(member))
                save(OUT / (flavor + '-' + Path(name).name), value)
                if name.endswith('/native-files.json'):
                    native = value
                if name.endswith('/native-packages.json'):
                    packages = value
    allowed = {'/usr/local/bin/cnpg-backup'} | {entry['path'] for entry in native}
    if executable != allowed or not packages:
        raise ValueError('image executable/package closure differs')
    if flavor == 'manager' and native:
        raise ValueError('native code in manager')
    if flavor == 'pg18':
        tools = {Path(e['path']).name for e in native if '/postgresql/18/bin/' in e['path']}
        if tools != {'pg_basebackup', 'pg_verifybackup', 'pg_combinebackup', 'pg_waldump', 'pg_controldata', 'psql'}:
            raise ValueError('six-tool closure differs')
    for entry in native:
        if inventory.get(entry['path'], {}).get('sha256') != entry['sha256']:
            raise ValueError('native file differs from recorded closure')
    save(OUT / (flavor + '-files.json'), inventory)
    return packages


def scan_coverage(report, packages):
    os_info = report.get('Metadata', {}).get('OS', {})
    if os_info.get('Family') != 'ubuntu' or os_info.get('Name') != '24.04':
        raise ValueError('scanner did not identify the pinned Ubuntu package origin')
    results = report.get('Results', [])
    detected = {(p['Name'], p['Version']) for r in results if r.get('Class') == 'os-pkgs'
                for p in r.get('Packages', [])}
    if {(p['name'], p['version']) for p in packages} - detected:
        raise ValueError('scanner omitted shipped native/CA package versions')
    if not any(r.get('Type') == 'gobinary' and r.get('Packages') for r in results):
        raise ValueError('scanner omitted linked Go binary')


def trivy_gate(report):
    blockers = []
    for result in report.get('Results', []):
        for vuln in result.get('Vulnerabilities', []):
            if vuln.get('Severity') in ('HIGH', 'CRITICAL', 'UNKNOWN'):
                blockers.append({'kind': 'vulnerability', 'id': vuln['VulnerabilityID'],
                                 'package': vuln['PkgName'], 'version': vuln['InstalledVersion'],
                                 'severity': vuln['Severity']})
        for secret in result.get('Secrets', []):
            blockers.append({'kind': 'secret', 'rule': secret.get('RuleID'), 'target': result.get('Target')})
            # Scanner snippets/matches can contain the discovered credential.
            for key in list(secret):
                if key not in ('RuleID', 'Category', 'Severity', 'Title', 'StartLine', 'EndLine'):
                    del secret[key]
    return blockers


def sources(packages, revision):
    # Prior subject bytes use THEIR locked corresponding source, not today's
    # harness dependency versions. Missing historical locks are not a pass.
    lock = json.loads(subprocess.check_output(['git', 'show', revision + ':build/native-sources.lock.json'], text=True))
    inputs = json.loads(subprocess.check_output(['git', 'show', revision + ':build/inputs.lock.json'], text=True))
    locked = {p['name']: p for p in inputs['packages']}
    if any(p != locked.get(p['name']) for p in packages):
        raise ValueError('image packages differ from subject build lock')
    required = {p['name'] for p in packages}
    selected = [s for s in lock['sources'] if required.intersection(s['binary_packages'])]
    if required - {p for s in selected for p in s['binary_packages']}:
        raise ValueError('unattributed native source package')
    total = sum(f['bytes'] for s in selected for f in s['files'])
    if total > 512 << 20:
        raise ValueError('source bundle exceeds bound')
    save(OUT / 'native-sources.json', selected)
    with tarfile.open(OUT / 'native-sources.tar', 'w') as tar:
        for source in selected:
            for spec in source['files']:
                name = spec['url'].rsplit('/', 1)[-1]
                path = fetch(spec, WORK / 'sources' / source['name'] / name)
                if path.stat().st_size != spec['bytes']:
                    raise ValueError('source size mismatch')
                tar.add(path, arcname=source['name'] + '/' + name, recursive=False)


def scan(args):
    OUT.mkdir(parents=True, exist_ok=False)
    WORK.mkdir(parents=True, exist_ok=True)
    selected = subject(args.sha, args.manager, args.data, args.ref)
    save(OUT / 'subject.json', selected)
    save(OUT / 'tool-pins.json', PINS)
    os.environ['PATH'] = str(WORK / 'bin') + ':' + os.environ['PATH']
    blockers, packages = [], []
    for tool in ('syft', 'trivy'):
        run(tool, 'version', output=OUT / (tool + '-version.txt'))
    # Fresh cache per hosted run; never --skip-db-update or a repo ignore file.
    cache = WORK / ('trivy-db-' + selected['run_id'] + '-' + selected['run_attempt'])
    run('trivy', 'image', '--cache-dir', cache, '--download-db-only')
    run('trivy', 'version', '--cache-dir', cache, '--format', 'json', output=OUT / 'trivy-database.json')
    database = json.loads((cache / 'db/metadata.json').read_text())
    updated = datetime.datetime.fromisoformat(database['UpdatedAt'].replace('Z', '+00:00'))
    if datetime.datetime.now(datetime.timezone.utc) - updated > datetime.timedelta(hours=48):
        raise ValueError('Trivy database older than 48 hours')
    save(OUT / 'trivy-db-metadata.json', database)
    for flavor, image in selected['images'].items():
        run('docker', 'pull', '--platform=linux/amd64', image)
        run('docker', 'image', 'inspect', image, output=WORK / 'inspect.json')
        info = json.loads((WORK / 'inspect.json').read_text())[0]
        if (info['Config']['Labels'].get('org.opencontainers.image.revision') != selected['revision']
                or info['Config']['Labels'].get('org.opencontainers.image.source') != 'https://github.com/djosh34/cnpg_backup'
                or info['Architecture'] != 'amd64' or info['Os'] != 'linux' or info['Config']['User'] != '26:26'
                or image not in info['RepoDigests']):
            raise ValueError('image source/platform/digest mismatch')
        save(OUT / (flavor + '-inspect.json'), {k: info[k] for k in ('Id', 'RepoDigests', 'Architecture', 'Os')})
        run('docker', 'create', '--network=none', image, 'version', output=WORK / 'container-id')
        container = (WORK / 'container-id').read_text().strip()
        archive = WORK / (flavor + '.tar')
        try:
            run('docker', 'export', '--output', archive, container)
            if archive.stat().st_size > 512 << 20:
                raise ValueError('unexpected image size')
            image_packages = image_files(archive, flavor)
            packages.extend(image_packages)
        finally:
            run('docker', 'rm', container)
            archive.unlink(missing_ok=True)
        run('syft', 'scan', 'registry:' + image, '-o', 'spdx-json=' + str(OUT / (flavor + '.spdx.json')))
        raw = WORK / (flavor + '-trivy.json')
        run('trivy', 'image', '--cache-dir', cache, '--scanners', 'vuln,secret', '--ignorefile', '/dev/null',
            '--parallel', '2', '--timeout', '15m', '--list-all-pkgs', '--format', 'json', '--output', raw, image)
        report = json.loads(raw.read_text())
        blockers.extend({'subject': flavor, **b} for b in trivy_gate(report))
        save(OUT / (flavor + '-trivy.json'), report)
        raw.unlink()
        scan_coverage(report, image_packages)
    if sha256(WORK / 'manager-binary') != sha256(WORK / 'pg18-binary'):
        raise ValueError('images do not contain the same executable')
    binary = WORK / 'manager-binary'
    run('go', 'version', '-m', binary, output=OUT / 'go-version.txt')
    run('syft', 'scan', 'file:' + str(binary), '-o', 'spdx-json=' + str(OUT / 'binary.spdx.json'))
    code = run('govulncheck', '-mode=binary', '-json', binary, output=OUT / 'govulncheck.json', allowed=(0, 3))
    findings = list(records((OUT / 'govulncheck.json').read_text()))
    linked = [v['finding'] for v in findings if 'finding' in v and any(t.get('function') for t in v['finding']['trace'])]
    if code or linked:
        blockers.append({'subject': 'binary', 'kind': 'linked-go', 'findings': linked})
    sources(packages, selected['revision'])
    save(OUT / 'gate.json', {'passed': not blockers, 'blockers': blockers, 'release_qualified': False,
                            'checkmarx': 'unavailable: no configured license/credentials/pipeline; not passed'})
    # Do not publish possibly credential-bearing binaries on a failed scan.
    if blockers:
        raise ValueError('security findings require remediation or evidence-backed independent disposition')
    shutil.copyfile(binary, OUT / 'cnpg-backup')
    (OUT / 'SHA256SUMS').write_text(sha256(binary) + '  cnpg-backup\n')
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        for flavor, image in selected['images'].items():
            output.write(flavor + '_digest=' + image.split('@')[1] + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('tools', 'scan'))
    parser.add_argument('--sha', default=os.environ.get('SUBJECT_SHA'))
    parser.add_argument('--manager', default=os.environ.get('MANAGER_IMAGE'))
    parser.add_argument('--data', default=os.environ.get('DATA_IMAGE'))
    parser.add_argument('--ref', default=os.environ.get('TRUSTED_REF', 'main'))
    args = parser.parse_args()
    if args.phase == 'tools':
        tools()
    else:
        scan(args)
