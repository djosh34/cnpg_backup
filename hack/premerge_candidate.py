#!/usr/bin/env python3
"""Repository-owned premerge candidate publication, never release publication."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

REPO = 'djosh34/cnpg_backup'
BRANCH = 'refs/heads/implementation/pr-g'
WORKFLOW = REPO + '/.github/workflows/pr-g-candidate.yml@' + BRANCH
OUT = Path('artifacts/candidate')
SUBJECT_RECORDS = {
    BRANCH: '.github/ci-repair-subject.json',
    **{'refs/heads/implementation/pr-' + letter: '.github/pr-' + letter + '-subject.json'
       for letter in ('h', 'i', 'j')},
}


def trust(env, head, origin):
    if (env.get('GITHUB_REPOSITORY') != REPO or env.get('GITHUB_EVENT_NAME') != 'push'
            or env.get('GITHUB_REF') not in SUBJECT_RECORDS
            or env.get('GITHUB_WORKFLOW_REF') != REPO + '/.github/workflows/pr-g-candidate.yml@' + env.get('GITHUB_REF', '')
            or not re.fullmatch('[0-9a-f]{40}', head) or env.get('GITHUB_SHA') != head
            or env.get('GITHUB_WORKFLOW_SHA') != head
            or origin not in ('https://github.com/' + REPO, 'https://github.com/' + REPO + '.git')):
        raise RuntimeError('not the trusted exact-SHA repository-owned premerge push')


def run(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True, timeout=300)


def references(sha):
    return {f: 'ghcr.io/djosh34/cnpg-backup-' + f + ':sha-' + sha for f in ('manager', 'pg18')}


def require_absent(result):
    # Authentication/network failures are NOT permission to overwrite a tag.
    if result.returncode == 0:
        raise RuntimeError('immutable candidate already exists; consume its recorded digests, never rebuild')
    if not any(x in result.stderr.lower() for x in ('manifest unknown', 'no such manifest')):
        raise RuntimeError('cannot positively establish candidate tag absence')


def reuse_subject(head, branch=BRANCH):
    """Reuse only an explicitly recorded candidate with unchanged build inputs."""
    if branch not in SUBJECT_RECORDS:
        raise RuntimeError('untrusted candidate branch')
    record = Path(SUBJECT_RECORDS[branch])
    if not record.exists():
        return None
    subject = json.loads(record.read_text())
    sha = subject['revision']
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise RuntimeError('invalid frozen product revision')
    run('git', 'merge-base', '--is-ancestor', sha, head)
    # Permit only known non-product changes. Unknown/new build inputs take the
    # existing audited publication path, rather than assuming a partial closure.
    harness = {'hack/test', 'hack/backup_smoke.py', 'hack/wal_smoke.py',
               'hack/premerge_candidate.py', 'hack/campaign_ci.py',
               'hack/campaign_fixture.py', 'hack/campaign_plan.py',
               'hack/campaign_process.py', 'hack/campaign_trust.py',
               'hack/recovery_campaign.py', 'hack/recovery_cases.py'}
    paths = run('git', 'diff', '--name-only', sha, head).stdout.splitlines()
    if any(not (path.startswith(('docs/', '.github/')) or path in harness or
                (path.startswith('hack/test_') and path.endswith('.py'))) for path in paths):
        return None
    for flavor in ('manager', 'pg18'):
        if not re.fullmatch('ghcr.io/djosh34/cnpg-backup-' + flavor + '@sha256:[0-9a-f]{64}', subject['images'][flavor]):
            raise RuntimeError('invalid frozen image digest')
    return subject


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=('trust', 'preflight', 'publish', 'reuse'))
    phase = p.parse_args().phase
    sha = run('git', 'rev-parse', 'HEAD').stdout.strip()
    trust(os.environ, sha, run('git', 'remote', 'get-url', 'origin').stdout.strip())
    OUT.mkdir(parents=True, exist_ok=True)
    if phase == 'trust':
        (OUT / 'trust.json').write_text(json.dumps({'subject_sha': sha, 'trusted': True, 'release_qualified': False}) + '\n')
        return
    if phase == 'reuse':
        subject = reuse_subject(sha, os.environ['GITHUB_REF'])
        (OUT / 'reused-subject.json').write_text(json.dumps({
            'harness_revision': sha, 'subject': subject, 'release_qualified': False}, indent=2) + '\n')
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write('reuse=' + str(subject is not None).lower() + '\n')
            output.write('sha=' + (subject['revision'] if subject else sha) + '\n')
            for flavor, image in (subject['images'] if subject else {}).items():
                output.write(flavor + '=' + image + '\n')
        return
    tags = references(sha)
    # Both phases refuse all existing tags, including partial prior publication.
    for tag in tags.values():
        require_absent(run('docker', 'manifest', 'inspect', tag, check=False))
    if phase == 'preflight':
        return
    images = json.loads(Path('build/out/images.json').read_text())
    record = {'subject_sha': sha, 'run_id': os.environ['GITHUB_RUN_ID'],
              'release_qualified': False, 'images': {}}
    for flavor, tag in tags.items():
        image_id = images[flavor]
        run('docker', 'tag', image_id, tag)
        # Docker uses its private login config. No credentials in argv/output.
        run('docker', 'push', tag)
        manifest = json.loads(run('docker', 'manifest', 'inspect', '--verbose', tag).stdout)
        digest = manifest['Descriptor']['digest']
        if not re.fullmatch('sha256:[0-9a-f]{64}', digest):
            raise RuntimeError('invalid published manifest digest')
        if manifest['SchemaV2Manifest']['config']['digest'] != image_id:
            raise RuntimeError('published manifest is not the audited image')
        ref = tag.split(':sha-')[0] + '@' + digest
        record['images'][flavor] = {'tag': tag, 'digest_reference': ref, 'config_digest': image_id}
        # Preserve partial publication if the second push fails.
        (OUT / 'candidate.json').write_text(json.dumps(record, indent=2) + '\n')
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write(flavor + '=' + ref + '\n')
            output.write(flavor + '_digest=' + digest + '\n')
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
        summary.write('Unqualified premerge candidate `' + sha + '` (no version tags):\n\n')
        for image in record['images'].values():
            summary.write('- `' + image['digest_reference'] + '`\n')


if __name__ == '__main__':
    main()
