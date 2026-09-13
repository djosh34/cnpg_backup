#!/usr/bin/env python3
"""Publish audited candidate images from an exact trusted main checkout."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

REPO = 'djosh34/cnpg_backup'
BRANCH = 'refs/heads/main'
WORKFLOW = REPO + '/.github/workflows/candidate.yml@' + BRANCH
OUT = Path('artifacts/candidate')


def trust(env, head, origin):
    if (env.get('GITHUB_REPOSITORY') != REPO or env.get('GITHUB_EVENT_NAME') != 'workflow_dispatch'
            or env.get('GITHUB_REF') != BRANCH or env.get('GITHUB_WORKFLOW_REF') != WORKFLOW
            or not re.fullmatch('[0-9a-f]{40}', head) or env.get('GITHUB_SHA') != head
            or env.get('GITHUB_WORKFLOW_SHA') != head
            or origin not in ('https://github.com/' + REPO, 'https://github.com/' + REPO + '.git')):
        raise RuntimeError('not the trusted exact-SHA main candidate workflow')


def run(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True, timeout=300)


def references(sha):
    return {f: 'ghcr.io/djosh34/cnpg-backup-' + f + ':sha-' + sha for f in ('manager', 'pg18')}


def require_absent(result):
    # Authentication/network failures are not permission to overwrite a tag.
    if result.returncode == 0:
        raise RuntimeError('immutable candidate already exists; consume its digests, never rebuild')
    if not any(x in result.stderr.lower() for x in ('manifest unknown', 'no such manifest')):
        raise RuntimeError('cannot positively establish candidate tag absence')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('preflight', 'publish'))
    phase = parser.parse_args(argv).phase
    sha = run('git', 'rev-parse', 'HEAD').stdout.strip()
    trust(os.environ, sha, run('git', 'remote', 'get-url', 'origin').stdout.strip())
    tags = references(sha)
    # Repeat immediately before publication; a partial earlier run is not a
    # license to replace either candidate. Workflow concurrency serializes SHA.
    for tag in tags.values():
        require_absent(run('docker', 'manifest', 'inspect', tag, check=False))
    if phase == 'preflight':
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write('sha=' + sha + '\n')
        return
    images = json.loads(Path('build/out/images.json').read_text())
    record = {'subject_sha': sha, 'run_id': os.environ['GITHUB_RUN_ID'],
              'run_attempt': os.environ['GITHUB_RUN_ATTEMPT'], 'release_qualified': False, 'images': {}}
    OUT.mkdir(parents=True, exist_ok=True)
    for flavor, tag in tags.items():
        image_id = images[flavor]
        run('docker', 'tag', image_id, tag)
        # Docker reads private login config; credentials never enter argv/output.
        run('docker', 'push', tag)
        manifest = json.loads(run('docker', 'manifest', 'inspect', '--verbose', tag).stdout)
        digest = manifest['Descriptor']['digest']
        if not re.fullmatch('sha256:[0-9a-f]{64}', digest):
            raise RuntimeError('invalid published manifest digest')
        if manifest['SchemaV2Manifest']['config']['digest'] != image_id:
            raise RuntimeError('published manifest is not the audited image')
        ref = tag.split(':sha-')[0] + '@' + digest
        record['images'][flavor] = {'tag': tag, 'digest_reference': ref, 'config_digest': image_id}
        # Preserve the first publication if the second push fails.
        (OUT / 'candidate.json').write_text(json.dumps(record, indent=2) + '\n')
        with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
            output.write(flavor + '=' + ref + '\n')
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
        summary.write('Unqualified candidate `' + sha + '`:\n\n')
        for image in record['images'].values():
            summary.write('- `' + image['digest_reference'] + '`\n')


if __name__ == '__main__':
    main()
