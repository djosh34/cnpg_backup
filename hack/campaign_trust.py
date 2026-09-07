#!/usr/bin/env python3
"""Trusted workflow preflight. Does not check out or execute the subject ref."""
import os
import subprocess

from recovery_campaign import options, OUT, atomic_json


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    # Even input rejection leaves bounded artifact evidence for the always step.
    atomic_json(OUT / 'trust.json', {'trusted': False, 'release_qualified': False})
    assert os.environ.get('GITHUB_REPOSITORY') == 'djosh34/cnpg_backup', 'wrong repository'
    assert os.environ.get('WORKFLOW_REF') == 'djosh34/cnpg_backup/.github/workflows/recovery-campaign.yml@refs/heads/main', 'use the trusted main workflow'
    branch = os.environ['TRUSTED_REF']
    assert branch in ('main', 'implementation/pr-g'), 'subject branch is not allowlisted'
    args = options(['--subject-sha', os.environ['SUBJECT_SHA'], '--manager-image', os.environ['MANAGER_IMAGE'],
                    '--data-image', os.environ['DATA_IMAGE'], '--profile', os.environ['PROFILE'],
                    '--seed', os.environ['SEED'], '--duration-minutes', os.environ['DURATION']])
    def git(*args):
        return subprocess.check_output(['git', *args], text=True, timeout=90).strip()
    assert git('remote', 'get-url', 'origin') in ('https://github.com/djosh34/cnpg_backup', 'https://github.com/djosh34/cnpg_backup.git'), 'unexpected checkout origin'
    # Only literal allowlisted refs are ever passed to git. Remote branch cannot
    # smuggle options, tags, pull refs or shell syntax into source selection.
    for name in sorted(set(('main', branch))):
        git('fetch', '--no-tags', 'origin', 'refs/heads/' + name + ':refs/remotes/origin/' + name)
    git('merge-base', '--is-ancestor', args.subject_sha, 'refs/remotes/origin/' + branch)
    harness = git('rev-parse', 'HEAD')
    git('merge-base', '--is-ancestor', harness, 'refs/remotes/origin/main')
    atomic_json(OUT / 'trust.json', {'trusted': True, 'harness_revision': harness, 'subject_sha': args.subject_sha,
                                   'trusted_ref': branch, 'release_qualified': False})


if __name__ == '__main__':
    main()
