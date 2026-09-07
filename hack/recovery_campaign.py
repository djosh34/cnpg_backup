#!/usr/bin/env python3
"""Actual G campaign entry point. No subject builds, external endpoints or secrets.

The seed fixes workload choices, not kernel/network scheduling. Missing evidence
fails the selected profile. H–K are unimplemented: never release qualification.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

import cnpg_smoke as h

ROOT = h.ROOT
OUT = ROOT / 'artifacts/recovery-campaign'
WORK = ROOT / '.work/recovery-campaign'
NAME = 'cnpg-backup-campaign'
PLUGIN = 'cnpg-backup.djosh34.github.io'
SOURCE_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
SOURCE = 'campaign-source'
TARGET = 'campaign-target'
STORE = 'campaign-store'
# Every named family is mandatory in recovery; smoke is explicitly a short slice.
SMOKE = ('full-latest-remote-SQL', 'full-name-pre-DROP-SQL', 'source-namespace-catalog-loss-S3-only')
MANDATORY = SMOKE + (
    'full-time-inclusive-exclusive', 'full-LSN-inclusive-exclusive',
    'full-XID-inclusive-exclusive', 'full-explicit-immediate',
    'newest-base-too-new', 'target-unreached',
    'bundle-duplicate-absent-local-fallback', 'bundle-local-missing-fatal',
    'same-bundled-segment-post-EndLSN-archive-preferred',
    'required-missing-no-latest-promotion', 'required-corrupt-no-latest-promotion',
    'bundle-TLS-fatal', 'bundle-auth-fatal', 'bundle-transport-fatal',
    'negative-controls-EOF-and-bundle-as-success',
    'shell-free-original-verification',
    'guard-before-RPC-same-PVC-no-mutation', 'guard-delayed-response-same-PVC-no-mutation',
    'guard-replay-pause-same-PVC-no-mutation', 'guard-shutdown-pause-same-PVC-no-mutation',
    'sidecar-death-poisons', 'guard-death-poisons', 'detached-PG-descendants',
    'pending-sidecar-task-same-incarnation-drain', 'stale-tuple-rejected',
    'poison-fresh-Cluster-all-fresh-PVC-retry',
    'source-lifetime-and-reader-through-replay', 'controller-all-Job-retry-Pods-terminated',
)


class Deadline(RuntimeError):
    pass


def subject_image(value, flavor):
    if not re.fullmatch(r'ghcr\.io/djosh34/cnpg-backup-' + flavor + r'@sha256:[0-9a-f]{64}', value):
        raise ValueError('subject must be the canonical immutable ' + flavor + ' image digest')
    return value


def check_rows(actual, expected):
    wanted = ','.join(f'{key}:{value}' for key, value in sorted(expected))
    assert actual == wanted, f'independent SQL oracle differs: expected {wanted}, observed {actual}'
    return actual


def atomic_json(path, data):
    temp = path.with_suffix('.next')
    with temp.open('w') as stream:
        stream.write(json.dumps(data, indent=2) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)


class Manifest:
    def __init__(self, directory, inputs, scenarios, deadline=float('inf')):
        self.directory, self.deadline = directory, deadline
        directory.mkdir(parents=True, exist_ok=True)
        self.data = {'schema': 1, **inputs, 'release_qualified': False,
                     'not_implemented': ['H differential', 'I retention', 'J operations/security', 'K qualification'],
                     'scenarios': {s: {'status': 'not_executed'} for s in scenarios},
                     'requested_scenarios': list(scenarios),
                     'not_requested_scenarios': [s for s in MANDATORY if s not in scenarios],
                     'remaining_mandatory': list(scenarios), 'events': 0, 'started_epoch': time.time()}
        self.save()

    def save(self):
        self.data['remaining_mandatory'] = [s for s, result in self.data['scenarios'].items() if result['status'] != 'passed']
        atomic_json(self.directory / 'manifest.json', self.data)

    def event(self, event_name, **facts):
        event = {'event': event_name, 'epoch': time.time(), **facts}
        text = h.redact_diagnostics(json.dumps(event))
        if text == '<REDACTED>':
            text = json.dumps({'event': 'redacted-event', 'epoch': event['epoch']})
        with (self.directory / 'events.jsonl').open('a') as stream:
            stream.write(text + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        self.data['events'] += 1
        self.save()

    def budget(self):
        if time.monotonic() >= self.deadline:
            raise Deadline('inner deadline: no further fault generation')

    @contextlib.contextmanager
    def case(self, name):
        self.budget()
        result = self.data['scenarios'][name]
        assert result['status'] == 'not_executed', 'duplicate scenario execution'
        result.update(status='running', started_epoch=time.time())
        self.save()
        try:
            yield
        except BaseException as error:
            result.update(status='failed', error_type=type(error).__name__,
                          diagnostic=h.redact_diagnostics(str(error))[-4000:])
            first = self.directory / 'first-failure.json'
            if not first.exists():
                atomic_json(first, {'scenario': name, **result})
            raise
        else:
            result['status'] = 'passed'
        finally:
            result['finished_epoch'] = time.time()
            self.save()

    def finish(self):
        self.data['finished_epoch'] = time.time()
        self.save()
        self.data['all_g_families_passed'] = set(MANDATORY) <= {s for s, r in self.data['scenarios'].items() if r['status'] == 'passed'}
        passed = not self.data['remaining_mandatory'] and self.data.get('profile') != 'qualification'
        self.data['scope_passed'] = passed
        self.save()
        return passed


def configure():
    h.WORK, h.OUT, h.NAME, h.NS = WORK, OUT, NAME, SOURCE


def collect():
    """Bounded, idempotent always collector; never dumps Secret/spec/env objects."""
    configure()
    worker_log = OUT / 'worker.log'
    if worker_log.exists():
        with worker_log.open() as stream:
            h.save_log('worker.log', stream.read(1 << 20), 256000)
    if not (WORK / 'kubectl').exists() or not (WORK / 'kubeconfig').exists():
        return
    for namespace in (SOURCE, TARGET, STORE):
        h.NS = namespace
        h.collect_pod_evidence()
        for resource in ('events', 'jobs', 'pvc', 'clusters'):
            try:
                h.save_log(namespace + '-' + resource + '.log', h.kube('get', resource, '-n', namespace,
                           '-o', 'wide', '--request-timeout=8s', check=False, timeout=10), 64000)
            except Exception:
                pass
    # Only the bounded, nonsecret product operation envelope, never ConfigMap
    # inventories or Secret projections. Needed to distinguish observer loss
    # from materialization/guard failure on the first failed run.
    try:
        clusters = json.loads(h.kube('get', 'clusters', '-n', TARGET, '-o', 'json', '--request-timeout=8s', timeout=10))['items']
        end = time.monotonic() + 30
        for cluster in clusters[:64]:
            if time.monotonic() >= end:
                break
            name = cluster['metadata']['name']
            if re.fullmatch(r'g-[0-9]{3}', name):
                text = h.kube('get', 'configmap', name + '-cb-recovery', '-n', TARGET,
                              '-o', 'jsonpath={.data.operation\\.json}', '--request-timeout=8s', check=False, timeout=10)
                h.save_log(name + '-operation.json', text, 128 << 10)
    except Exception:
        pass
    h.NS = SOURCE
    try:
        h.save_log('node-resources.log', h.run('docker', 'stats', '--no-stream', '--format', '{{json .}}', NAME + '-control-plane', check=False, timeout=15), 64000)
    except Exception:
        pass


def options(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--profile', choices=('smoke', 'recovery', 'qualification'), default='recovery')
    p.add_argument('--seed', type=int, default=1806)
    p.add_argument('--duration-minutes', type=int, default=120)
    p.add_argument('--subject-sha', required=True)
    p.add_argument('--manager-image', required=True)
    p.add_argument('--data-image', required=True)
    p.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    p.add_argument('--collect', action='store_true', help='bounded diagnostics only, no new faults')
    args = p.parse_args(argv)
    if not re.fullmatch('[0-9a-f]{40}', args.subject_sha):
        p.error('subject-sha must be exact lowercase 40-hex')
    if not 10 <= args.duration_minutes <= 135 or not 0 <= args.seed <= 2**32 - 1:
        p.error('duration must be 10..135 minutes; seed must be uint32')
    try:
        subject_image(args.manager_image, 'manager')
        subject_image(args.data_image, 'pg18')
    except ValueError as e:
        p.error(str(e))
    return args


def execute(args):
    configure()
    if args.collect:
        collect()
        return 0
    if args.worker:
        from recovery_cases import Campaign
        m = Manifest(OUT, json.loads((WORK / 'inputs.json').read_text()),
                     SMOKE if args.profile == 'smoke' else MANDATORY,
                     deadline=time.monotonic() + args.duration_minutes * 60 - 300)
        campaign = Campaign(args, m)
        try:
            started = time.monotonic()
            campaign.setup()
            m.data['phase_timings'] = {'setup_seconds': time.monotonic() - started}
            m.save()
            started = time.monotonic()
            campaign.run()
            m.data['phase_timings']['fixed_and_exploration_seconds'] = time.monotonic() - started
        except BaseException as e:
            m.data['failure'] = {'type': type(e).__name__, 'diagnostic': h.redact_diagnostics(str(e))[-4000:]}
            if not (OUT / 'first-failure.json').exists():
                atomic_json(OUT / 'first-failure.json', m.data['failure'])
            m.save()
            return 1
        finally:
            campaign.close()
            m.finish()
        return 0 if m.finish() else 1
    # Never overwrite earlier run evidence or clean an unknown kind cluster.
    WORK.mkdir(parents=True, exist_ok=False)
    OUT.mkdir(parents=True, exist_ok=True)
    if any(p.name != 'trust.json' for p in OUT.iterdir()):
        raise RuntimeError('existing campaign evidence: preserve/move it before a new run')
    sha = h.run('git', 'rev-parse', 'HEAD').strip()
    inputs = {'subject_sha': args.subject_sha, 'harness_revision': sha,
              'subject_images': {'manager': args.manager_image, 'pg18': args.data_image},
              'profile': args.profile, 'seed': args.seed, 'duration_minutes': args.duration_minutes,
              'run_id': os.getenv('GITHUB_RUN_ID'), 'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT'),
              'compatibility': h.LOCK, 'build_inputs': json.loads((ROOT / 'build/inputs.lock.json').read_text()),
              'resources': {'cpu_count': os.cpu_count(), 'memory': Path('/proc/meminfo').read_text().splitlines()[:3],
                            'disk_available': os.statvfs(WORK).f_bavail * os.statvfs(WORK).f_frsize},
              'replay': ['python3', 'hack/recovery_campaign.py', *sys.argv[1:]],
              'real_system_seed_is_not_deterministic': True}
    atomic_json(WORK / 'inputs.json', inputs)
    Manifest(OUT, inputs, SMOKE if args.profile == 'smoke' else MANDATORY)
    # Child owns provisioning/faults. A process-group deadline covers blocked
    # kubectl, Go build, downloads and test actors; five minutes remain to collect.
    command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], '--worker']
    with (OUT / 'worker.log').open('w') as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=args.duration_minutes * 60 - 300)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=10)
            code = 124
        finally:
            collect()
    data = json.loads((OUT / 'manifest.json').read_text())
    data.update(worker_exit=code, release_qualified=False)
    if code:
        data['scope_passed'] = False
        if not (OUT / 'first-failure.json').exists():
            atomic_json(OUT / 'first-failure.json', {'worker_exit': code, 'remaining': data['remaining_mandatory']})
    atomic_json(OUT / 'manifest.json', data)
    # Raw child exceptions never enter the upload unredacted.
    h.save_log('worker.log', (OUT / 'worker.log').read_text(), 256000)
    summary = 'G campaign: ' + ('PASS scoped' if code == 0 and data.get('scope_passed') else 'FAIL/incomplete')
    summary += '\nrelease_qualified=false; H–K not implemented.\n'
    summary += 'Remaining mandatory: ' + ', '.join(data['remaining_mandatory']) + '\n'
    (OUT / 'summary.md').write_text(summary)
    print(summary)
    return code or (0 if data.get('scope_passed') else 1)


if __name__ == '__main__':
    raise SystemExit(execute(options()))
