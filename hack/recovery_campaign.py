#!/usr/bin/env python3
"""Digest-only CI repair runner. Fresh fixtures, exact records, all independent failures."""
import argparse
from collections import deque
import contextlib
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import time
import traceback
import uuid
from types import SimpleNamespace

import cnpg_smoke as h
from campaign_plan import (ROOT, MANDATORY, SMOKE, REGISTRY, SUPPLEMENTAL, selected,
                           content_hash, digest, image_config_digest, make_plan, validate_results)
from campaign_process import Commands, CommandFailure, redact

# Compatibility constants for independently checking scenario helpers/tests.
WORK = ROOT / '.work/recovery-campaign'
OUT = ROOT / 'artifacts/recovery-campaign'
SOURCE, TARGET, STORE = 'campaign-source', 'campaign-target', 'campaign-store'
SOURCE_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
PLUGIN = 'cnpg-backup.djosh34.github.io'


class Deadline(RuntimeError):
    pass


def scenarios(profile):
    return tuple(c['id'] for c in selected(profile))


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


def failure_frames(error):
    return [{'file': Path(f.filename).name, 'line': f.lineno, 'function': f.name}
            for f in traceback.extract_tb(error.__traceback__)[-12:]]


def assertion_record(error):
    import ast
    audit = json.loads((ROOT / 'docs/campaign-assertion-audit.json').read_text())['assertions']
    if getattr(error, 'campaign_oracle', None):
        return next(a for a in audit if a['id'] == error.campaign_oracle)
    for frame in reversed(traceback.extract_tb(error.__traceback__)):
        path = Path(frame.filename)
        if path.name not in ('recovery_cases.py', 'recovery_campaign.py', 'wal_smoke.py'):
            continue
        node = next((n for n in ast.walk(ast.parse(path.read_text())) if isinstance(n, ast.Assert) and n.lineno == frame.lineno), None)
        if node is not None:
            return next((a for a in audit if a['file'] == 'hack/' + path.name and a['function'] == frame.name
                         and a['expression'] == ast.unparse(node.test)), None)
    return None


def classify(error, phase):
    if isinstance(error, AssertionError):
        audit = assertion_record(error)
        if audit:
            return audit['failure_layer']
    if phase in ('setup', 'preflight'):
        return 'fixture' if isinstance(error, AssertionError) else 'infrastructure'
    if phase in ('collection', 'teardown', 'fault-reset', 'fault-observation', 'child-reap', 'container-cleanup', 'fixture-restore'):
        return 'infrastructure'
    if isinstance(error, AssertionError):
        if any(word in str(error).lower() for word in ('ineffective', 'injection', 'fixture', 'ambiguous')):
            return 'fixture'
        return 'product'
    # A timeout alone does not demonstrate a product defect. Preserve the layer
    # and actual observations; adjudication can promote it with causal evidence.
    return 'infrastructure' if isinstance(error, (CommandFailure, Deadline)) else 'fixture'


class Manifest:
    def __init__(self, directory, inputs, scenarios, deadline=float('inf')):
        names = list(scenarios)
        if len(names) != len(set(names)):
            raise ValueError('duplicate scenario registration')
        self.directory, self.deadline = directory, deadline
        directory.mkdir(parents=True, exist_ok=True)
        self.data = {'schema': 2, **inputs, 'execution_id': uuid.uuid4().hex, 'release_qualified': False,
                     'scenarios': {s: {'status': 'not_executed', 'branches': {
                         b: {'status': 'not_executed', 'required': True} for b in next((c.get('branches', ['main']) for c in REGISTRY if c['id'] == s), ['main'])}}
                         for s in names},
                     'requested_scenarios': names,
                     'not_requested_scenarios': [s for s in (*MANDATORY, *SUPPLEMENTAL) if s not in names],
                     'remaining_mandatory': names, 'events': 0, 'failures': [], 'diagnostics': [], 'phase_timings': [],
                     'started_epoch': time.time(), 'teardown_complete': False}
        self.save()

    @classmethod
    def open(cls, directory):
        manifest = cls.__new__(cls)
        manifest.directory, manifest.deadline = directory, float('inf')
        manifest.data = json.loads((directory / 'manifest.json').read_text())
        return manifest

    def save(self):
        self.data['remaining_mandatory'] = [s for s, result in self.data['scenarios'].items() if result['status'] != 'passed']
        atomic_json(self.directory / 'manifest.json', self.data)

    def event(self, event_name, **facts):
        event = {'event': event_name, 'epoch': time.time(), 'monotonic': time.monotonic(),
                 'fixture': self.data.get('active_fixture'), 'scenario': getattr(self, 'current_case', None), **facts}
        path = self.directory / 'events.jsonl'
        if path.exists() and path.stat().st_size >= 10 << 20:
            self.data['events_dropped'] = self.data.get('events_dropped', 0) + 1
        else:
            with path.open('a') as stream:
                stream.write(redact(json.dumps(event)) + '\n')
                stream.flush()
                os.fsync(stream.fileno())
        self.data['events'] += 1
        self.save()

    def collect_diagnostics(self, collector, action):
        """Optional forensics only; never wrap an oracle or owned cleanup here."""
        try:
            errors = action() or []
        except Exception as error:
            errors = [{'collector': collector, 'error_type': type(error).__name__,
                       'diagnostic': redact(str(error))[-4000:]}]
        for error in errors:
            self.data.setdefault('diagnostics', []).append({
                **error, 'classification': 'DIAGNOSTICS', 'severity': 'warning',
                'fixture': self.data.get('active_fixture'),
                'scenario': getattr(self, 'current_case', None),
                'branch': getattr(self, 'current_branch', None), 'epoch': time.time()})
        # Required manifest persistence is deliberately OUTSIDE the optional catch.
        self.save()

    def failure(self, error, phase, scenario=None, fixture_requirement=None, caused_by=None):
        # Mark the exception, not a long-lived map holding its traceback and
        # potentially large archived fixture buffers alive for the entire run.
        records = getattr(error, 'campaign_records', {})
        identity = self.data.get('execution_id', str(id(self)))
        if identity in records:
            # An allocated record may have outlived a failed evidence write.
            self.save_failures()
            return records[identity]
        phase = getattr(error, 'campaign_phase', phase)
        if isinstance(error, BaseExceptionGroup):
            first = None
            for child in error.exceptions:
                record = self.failure(child, phase, scenario, fixture_requirement, first['id'] if first else caused_by)
                first = first or record
            error.campaign_records = {**records, identity: first}
            return first
        # Preserve unexpected Python exception chains too (including finally
        # masking), but never duplicate a primary already recorded above.
        primary = getattr(error, 'campaign_primary', None)
        context = primary or error.__cause__ or (None if error.__suppress_context__ else error.__context__)
        if context is not None and (primary is not None or not isinstance(context, BaseExceptionGroup)):
            prior = self.failure(context, phase, scenario, fixture_requirement)
            caused_by = caused_by or prior['id']
        spec = next((c for c in REGISTRY if c['id'] == scenario), {})
        record = {'id': len(self.data['failures']) + 1, 'phase': phase, 'scenario': scenario,
                  'fixture_requirement': fixture_requirement, 'caused_by': caused_by,
                  'branch': getattr(self, 'current_branch', None),
                  'classification': classify(error, phase),
                  'classification_status': 'unadjudicated-requirement-layer' if classify(error, phase) == 'product' else 'observed-harness-layer',
                  'product_defect_proven': False,
                  'requirement': spec.get('requirement', 'owned fixture lifecycle'),
                  'error_type': type(error).__name__, 'diagnostic': redact(str(error))[-4000:],
                  'frames': failure_frames(error), 'epoch': time.time(), 'fixture': self.data.get('active_fixture'),
                  'assertion': assertion_record(error) if isinstance(error, AssertionError) or getattr(error, 'campaign_oracle', None) else None}
        error.campaign_records = {**records, identity: record}
        self.data['failures'].append(record)
        self.save_failures()
        return record

    def save_failures(self):
        first = self.directory / 'first-failure.json'
        if not first.exists():
            atomic_json(first, self.data['failures'][0])
        atomic_json(self.directory / 'failures.json', self.data['failures'])
        self.save()

    def budget(self):
        if time.monotonic() >= self.deadline:
            raise Deadline('inner deadline: no further fault generation')

    @contextlib.contextmanager
    def phase(self, name):
        timing = {'phase': name, 'fixture': self.data.get('active_fixture'), 'started_epoch': time.time(), 'status': 'running'}
        start = time.monotonic()
        self.data['phase_timings'].append(timing)
        self.save()
        try:
            yield
        except BaseException:
            timing['status'] = 'failed'
            raise
        else:
            timing['status'] = 'passed'
        finally:
            timing['seconds'] = time.monotonic() - start
            timing['finished_epoch'] = time.time()
            self.save()

    @contextlib.contextmanager
    def case(self, name):
        self.budget()
        family = self.data['scenarios'][name]
        branch = getattr(self, 'current_branch', None)
        result = family['branches'][branch] if branch else family
        if result['status'] != 'not_executed':
            raise ValueError('duplicate scenario execution: ' + name)
        result.update(status='running', started_epoch=time.time(), fixture=self.data.get('active_fixture'))
        self.current_case = name
        self.save()
        try:
            yield
        except BaseException as error:
            record = self.failure(error, 'case', name)
            result.update(status='failed', failure_id=record['id'])
            raise
        else:
            result['status'] = 'passed'
        finally:
            result['finished_epoch'] = time.time()
            if branch:
                self.update_family(name)
            else:
                for child in family['branches'].values():
                    child.update(status='passed' if result['status'] == 'passed' else 'blocked',
                                 blocked_by=[] if result['status'] == 'passed' else [result.get('failure_id')])
            self.current_case = None
            self.save()

    def update_family(self, name):
        family = self.data['scenarios'][name]
        states = {r['status'] for r in family['branches'].values()}
        family['status'] = ('passed' if states == {'passed'} else 'failed' if 'failed' in states
                            else 'blocked' if 'blocked' in states else 'running')

    def block(self, name, reasons, branch=None, **facts):
        family = self.data['scenarios'][name]
        for key, result in family['branches'].items():
            if result['status'] == 'not_executed' and (branch is None or key == branch):
                result.update(status='blocked', classification='blocked', blocked_by=reasons, **facts)
        family.update(blocked_by=reasons, **facts)
        self.update_family(name)
        self.save()

    def finish(self):
        self.data['finished_epoch'] = time.time()
        self.data['all_g_families_passed'] = set((*MANDATORY, *SUPPLEMENTAL)) <= {s for s, r in self.data['scenarios'].items() if r['status'] == 'passed'}
        self.save()
        passed = not self.data['remaining_mandatory'] and not self.data['failures'] and self.data.get('profile') != 'qualification'
        self.data['scope_passed'] = passed
        self.save()
        text = 'CI REPAIR: ' + ('PASS scoped' if passed else 'FAIL/incomplete') + '\n'
        text += f"Failures: {len(self.data['failures'])}; unproved scenarios: {len(self.data['remaining_mandatory'])}; release_qualified=false\n"
        for failure in self.data['failures']:
            label = 'unadjudicated product-requirement assertion' if failure['classification'] == 'product' else failure['classification']
            text += f"- {label}/{failure['phase']} {failure['scenario']}: {failure['diagnostic']}\n"
        text += f"DIAGNOSTICS: {len(self.data.get('diagnostics', []))} optional collector warnings (not failures; see manifest.json)\n"
        (self.directory / 'summary.md').write_text(text)
        return passed


def load_bundle(directory):
    directory = directory.resolve()
    record = json.loads((directory / 'harness.json').read_text())
    if record.get('schema') != 2:
        raise ValueError('unsupported harness image-identity schema')
    if record['content_hash'] != content_hash():
        raise ValueError('harness content mismatch')
    if record['python'] != sys.version.split()[0]:
        raise ValueError('Python version differs from immutable harness recipe')
    if not record['diagnostic'] and record['revision'] != subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, timeout=30).decode().strip():
        raise ValueError('checkout revision differs from immutable harness')
    import hashlib
    images = ('minio', 'walproxy', 'recoveryactor')
    if set(record['images']) != set(images) or set(record['files']) != {'actor', 'wal-proxy', 'wal-client', 'verify', 'minio', *(n + '.tar' for n in images)}:
        raise ValueError('incomplete fixture bundle')
    for flavor, image in record['images'].items():
        if image['archive'] != flavor + '.tar' or not re.fullmatch(r'sha256:[a-f0-9]{64}', image['config_digest']):
            raise ValueError('invalid immutable fixture image identity')
    for name, wanted in record['files'].items():
        path = directory / name
        if path.stat().st_size > 300 << 20:
            raise ValueError('oversized harness bundle file: ' + name)
        with path.open('rb') as stream:
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
        if actual != wanted:
            raise ValueError('tampered harness bundle: ' + name)
    for image in record['images'].values():
        if image_config_digest(directory / image['archive'], image['tag']) != image['config_digest']:
            raise ValueError('fixture config identity differs from verified archive')
    return {**record, 'directory': directory}


def fixture_source_hash():
    import hashlib
    names = subprocess.check_output(['git', 'ls-files', '-z', '*.go', 'go.mod', 'go.sum', 'build/inputs.lock.json'], cwd=ROOT).decode().split('\0')
    return digest({n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in names if n})


def build_bundle(directory, diagnostic=False, reuse=None):
    import hashlib
    import shutil
    dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).decode().strip()
    if dirty and not diagnostic:
        raise ValueError('dirty harness: commit first or explicitly create diagnostic bundle')
    directory.mkdir(parents=True, exist_ok=False)
    if reuse:
        previous = json.loads((reuse / 'harness.json').read_text())
        if previous.get('fixture_source_hash') != fixture_source_hash():
            raise ValueError('fixture tool sources changed; cannot reuse bundle images')
        for name, checksum in previous['files'].items():
            if Path(name).name != name or hashlib.sha256((reuse / name).read_bytes()).hexdigest() != checksum:
                raise ValueError('tampered reused fixture bundle')
            os.link(reuse / name, directory / name)  # immutable bytes, never modified
        record = {**previous, 'schema': 2, 'content_hash': content_hash(), 'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
                  'diagnostic': bool(dirty or diagnostic), 'python': sys.version.split()[0]}
        record['images'] = {flavor: {'tag': image['tag'], 'archive': image['archive'],
            'config_digest': image_config_digest(directory / image['archive'], image['tag'])}
            for flavor, image in previous['images'].items()}
        atomic_json(directory / 'harness.json', record)
        return record
    commands = Commands(directory / 'build-evidence', cwd=ROOT)
    commands.run(sys.executable, 'hack/bootstrap.py', '--go-only', timeout=900)
    cache = Path(os.environ.get('CNPG_BUILD_CACHE', ROOT / '.work/tools'))
    os.environ.update(PATH=str(cache / 'go/bin') + ':' + os.environ['PATH'], CGO_ENABLED='0', GOTOOLCHAIN='local',
                      GOMAXPROCS='2', GOFLAGS='-p=2', GOWORK='off', GOOS='linux', GOARCH='amd64')
    lock = json.loads((ROOT / 'build/inputs.lock.json').read_text())
    import bootstrap
    shutil.copyfile(bootstrap.download(lock['minio'], 'minio'), directory / 'minio')
    (directory / 'minio').chmod(0o555)
    if hashlib.sha256((directory / 'minio').read_bytes()).hexdigest() != lock['minio']['sha256']:
        raise ValueError('MinIO binary checksum mismatch')
    for name, package in [('actor', 'recoveryactor'), ('wal-proxy', 'walproxy'), ('wal-client', 'walclient'), ('verify', 'backupverify')]:
        commands.run('go', 'build', '-trimpath', '-o', directory / name, './hack/' + package, timeout=600)
    images = {}
    for flavor, executable in [('recoveryactor', 'actor'), ('walproxy', 'wal-proxy'), ('minio', 'minio')]:
        dockerfile = directory / 'Dockerfile'
        dockerfile.write_text(f'FROM scratch\nCOPY --chmod=0555 {executable} /{executable}\nENTRYPOINT ["/{executable}"]\n')
        tag = 'cb-repair-' + flavor + ':' + hashlib.sha256((directory / executable).read_bytes()).hexdigest()[:24]
        commands.run('docker', 'build', '--network=none', '-t', tag, directory, timeout=300)
        archive = flavor + '.tar'
        commands.run('docker', 'save', '-o', directory / archive, tag, timeout=120)
        images[flavor] = {'config_digest': image_config_digest(directory / archive, tag), 'tag': tag, 'archive': archive}
    (directory / 'Dockerfile').unlink()
    record = {'schema': 2, 'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
              'content_hash': content_hash(), 'fixture_source_hash': fixture_source_hash(),
              'diagnostic': bool(dirty or diagnostic), 'python': sys.version.split()[0],
              'images': images, 'files': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir() if p.is_file()}}
    atomic_json(directory / 'harness.json', record)
    return record


def run_plan(plan, directory, bundle, duration=120, retain=False):
    from campaign_fixture import Fixture, preflight
    from recovery_cases import Campaign
    if not 11 <= duration <= 135:
        raise ValueError('duration must be 11..135 minutes (ten minutes reserved for disposal)')
    if retain and plan['recipe']['fixture_mode'] != 'diagnostic':
        raise ValueError('retention is diagnostic-only and cannot enter fresh acceptance')
    if bundle['content_hash'] != plan['harness']['content_hash'] or {k: v for k, v in bundle.items() if k != 'directory'} != plan['harness']:
        raise ValueError('selected harness bundle differs from plan')
    recipe = plan['recipe']
    if recipe['fixture_mode'] == 'fresh' and duration != recipe['duration_minutes']:
        raise ValueError('fresh execution cannot change the immutable run deadline')
    canonical = make_plan(plan['subject'], plan['harness'], recipe['profile'], recipe['seed'], plan['cases'], recipe['fixture_mode'], recipe['layout'])
    if plan != canonical:
        raise ValueError('plan differs from the canonical immutable recipe')
    if plan['recipe']['fixture_mode'] == 'fresh':
        if bundle['diagnostic'] or subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
            raise ValueError('dirty/diagnostic harness cannot execute qualifying fresh plan')
    frozen = ROOT / '.github/ci-repair-subject.json'
    if frozen.exists() and plan['subject']['revision'] == json.loads(frozen.read_text())['revision']:
        if subprocess.check_output(['git', 'diff', plan['subject']['revision'], '--', 'cmd', 'internal', 'go.mod', 'go.sum'], cwd=ROOT).strip():
            raise ValueError('CI REPAIR cannot change frozen product sources')
    directory.mkdir(parents=True, exist_ok=False)
    # Cross-worktree lock on the same host. Unknown/leaked nodes also block reuse.
    lockpath = Path.home() / '.cache/cnpg-backup-campaign.lock'
    lockpath.parent.mkdir(parents=True, exist_ok=True)
    with lockpath.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        end = time.monotonic() + duration * 60
        manifest = Manifest(directory / 'evidence', {'plan': plan, 'profile': plan['recipe']['profile'],
                            'fixture_mode': plan['recipe']['fixture_mode'], 'seed': plan['recipe']['seed'], 'duration_minutes': duration,
                            'host': os.getenv('GITHUB_ACTIONS') and 'hosted' or 'local'}, plan['cases'], end - 600)
        commands = Commands(manifest.directory, cwd=ROOT, deadline=end - 600)
        try:
            with manifest.phase('preflight'):
                observed = preflight(plan['recipe']['resources'], directory, commands)
                atomic_json(manifest.directory / 'preflight.json', observed)
                manifest.data['host_fingerprint'] = observed
                if observed['errors']:
                    raise RuntimeError('; '.join(observed['errors']))
                stale = commands.run('docker', 'ps', '-a', '--filter', 'label=io.x-k8s.kind.role=control-plane', '--format', '{{.Names}}').split()
                stale += commands.run('docker', 'ps', '-a', '--filter', 'label=cnpg-backup-fixture', '--format', '{{.Names}}').split()
                if any(name.startswith(('cb-repair-', 'cnpg-backup-campaign-')) for name in stale):
                    raise ValueError('uncollected owned/retained campaign container blocks fresh slot')
                shutil.copyfile(ROOT / 'docs/campaign-assertion-audit.json', manifest.directory / 'assertion-audit.json')
        except Exception as error:
            failure = manifest.failure(error, 'preflight')
            for name in manifest.data['scenarios']:
                manifest.block(name, [failure['id']])
            manifest.data['teardown_complete'] = True  # preflight provisions nothing
            manifest.finish()
            return 1
        fixture = campaign = None
        healthy = True
        group = None
        serial = 0
        cases = selected(plan['recipe']['profile'], plan['cases'])
        def dispose():
            nonlocal fixture, campaign, healthy
            if fixture is None:
                return
            fixture.commands.deadline = min(end, time.monotonic() + 600)
            try:
                with manifest.phase('collection'), fixture.commands.budget(300):
                    manifest.collect_diagnostics('fixture', fixture.collect)
            except Exception as error:
                manifest.failure(error, 'failure-recording')
            try:
                if campaign and campaign.wal:
                    campaign.wal.close()
                    fixture.listener = None
                    fixture.record_ownership()
            except Exception as error:
                manifest.failure(error, 'child-reap')
            try:
                with manifest.phase('teardown'), fixture.commands.budget(300):
                    fixture.close()
            except Exception as error:
                healthy = False
                manifest.failure(error, 'teardown')
            fixture = campaign = None
        def expired(signum, frame):
            raise Deadline('campaign fault-generation deadline; collection/teardown reserve begins')
        old_handler = signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, max(.01, manifest.deadline - time.monotonic()))
        pending = deque((case, branch) for case in cases for branch in case.get('branches', ['main']))
        try:
            while pending:
                case, branch = pending.popleft()
                name = case['id']
                manifest.current_branch = branch
                if manifest.data['scenarios'][name]['branches'][branch]['status'] != 'not_executed':
                    continue
                blocked = [d for d in case['requires'] if manifest.data['scenarios'][d]['status'] != 'passed']
                if not healthy or blocked or time.monotonic() >= manifest.deadline:
                    manifest.block(name, blocked or ['owned cleanup failed' if not healthy else 'campaign deadline'],
                                   branch, requirement=case['requirement'])
                    continue
                # H's live-primary mutation branches each require a fresh source;
                # never reuse a promoted/checksum-changed/disaster fixture. The
                # existing verified disposal path owns every boundary.
                if fixture and (case['group'] == 'differential' or group == 'differential' or
                                (plan['recipe']['layout'] == 'grouped' and group != case['group'])):
                    dispose()
                    if not healthy:
                        manifest.block(name, ['owned cleanup failed'], branch)
                        continue
                if fixture is None:
                    serial += 1
                    fixture_name = 'cb-repair-' + uuid.uuid4().hex[:12]
                    manifest.data['active_fixture'] = fixture_name
                    fixture = Fixture(directory / f'fixture-{serial:03d}', fixture_name, manifest, bundle,
                                      plan['recipe']['resources'], manifest.deadline)
                    # First monolithic fixture gets the full declared closure;
                    # after a failure only prerequisites for remaining cases.
                    needed = {f for c in cases if any(b['status'] == 'not_executed' for b in manifest.data['scenarios'][c['id']]['branches'].values())
                              and ((c['group'] == 'differential') == (case['group'] == 'differential'))
                              and (plan['recipe']['layout'] != 'grouped' or c['group'] == case['group']) for f in c['fixtures']}
                    args = SimpleNamespace(profile=plan['recipe']['profile'], seed=plan['recipe']['seed'], fixtures=needed,
                                           subject_sha=plan['subject']['revision'], manager_image=plan['subject']['images']['manager'],
                                           data_image=plan['subject']['images']['pg18'])
                    campaign = Campaign(args, manifest, fixture)
                    preparing_case = None
                    try:
                        with manifest.phase('setup'):
                            campaign.setup()
                            campaign.prepare_recovery()
                        if case['requires']:
                            # New independent fixture must prove its OWN disaster
                            # prerequisite without overwriting original evidence.
                            dependencies = [c for c in selected('recovery', [name]) if c['id'] != name]
                            prerequisite = Manifest(fixture.OUT / 'prerequisite', {}, [c['id'] for c in dependencies], manifest.deadline)
                            campaign.m = prerequisite
                            for dependency in dependencies:
                                preparing_case = dependency
                                for required_branch in dependency.get('branches', ['main']):
                                    prerequisite.current_branch = required_branch
                                    with fixture.commands.budget(dependency['seconds']):
                                        getattr(campaign, dependency['method'])()
                            if not prerequisite.finish():
                                raise RuntimeError('fresh fixture prerequisite failed')
                            campaign.m = manifest
                        manifest.event('fixture-certified', fixtures=sorted(needed), prerequisites='passed',
                                       independent_branch=branch)
                    except Exception as error:
                        campaign.m = manifest
                        failed_fixture = 'same-segment' if any(f.name == 'capture_same_segment' for f in traceback.extract_tb(error.__traceback__)) else 'source'
                        failure = manifest.failure(error, 'prerequisite' if preparing_case else 'setup',
                                                   preparing_case['id'] if preparing_case else None,
                                                   fixture_requirement=None if preparing_case else failed_fixture)
                        for dependent in cases:
                            needs_failed = preparing_case['id'] in dependent['requires'] if preparing_case else failed_fixture in dependent['fixtures']
                            if needs_failed:
                                manifest.block(dependent['id'], [failure['id']],
                                               failed_fixture=None if preparing_case else failed_fixture)
                        # A failure in an optional S1 prerequisite must not
                        # suppress independent source/target tests. Restart only
                        # unexercised work, without ever retrying the failed S1.
                        if manifest.data['scenarios'][name]['branches'][branch]['status'] == 'not_executed':
                            pending.appendleft((case, branch))
                        dispose()
                        manifest.save()
                        continue
                group = case['group']
                try:
                    with fixture.commands.budget(case['seconds']):
                        getattr(campaign, case['method'])()
                except Exception as error:
                    if manifest.data['scenarios'][name]['branches'][branch]['status'] != 'failed':
                        failure = manifest.failure(error, 'case', name)
                        manifest.data['scenarios'][name]['branches'][branch].update(status='failed', failure_id=failure['id'])
                        manifest.update_family(name)
                    # Never continue fault generation on a possibly poisoned
                    # shared fixture. Independent cases get a new kind/source.
                    if retain:
                        # Freeze the entire owned node after collection. Retained
                        # forensics cannot continue generating WAL/disk growth.
                        fixture.commands.deadline = min(end, time.monotonic() + 300)
                        with fixture.commands.budget(300):
                            manifest.collect_diagnostics('fixture', fixture.collect)
                        if campaign.wal:
                            campaign.wal.close()
                            fixture.listener = None
                        fixture.run('docker', 'pause', fixture.NAME + '-control-plane', timeout=30)
                        fixture.record_ownership()
                        healthy = False
                        manifest.data['retained_fixture'] = str(fixture.WORK.parent / 'owner.json')
                        fixture = campaign = None
                    else:
                        dispose()
                manifest.save()
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)
            dispose()
            try:
                verified = load_bundle(bundle['directory'])
                if {k: v for k, v in verified.items() if k != 'directory'} != plan['harness']:
                    raise ValueError('harness record changed during execution')
            except Exception as error:
                manifest.failure(error, 'provenance')
            manifest.data['teardown_complete'] = healthy
            manifest.finish()
        return 0 if manifest.data['scope_passed'] and healthy else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    bundle = sub.add_parser('bundle')
    bundle.add_argument('--out', type=Path, required=True)
    bundle.add_argument('--diagnostic', action='store_true')
    bundle.add_argument('--reuse', type=Path, help='reuse verified unchanged immutable fixture-tool bytes')
    plan = sub.add_parser('plan')
    plan.add_argument('--subject', type=Path, required=True)
    plan.add_argument('--bundle', type=Path, required=True)
    plan.add_argument('--out', type=Path, required=True)
    plan.add_argument('--profile', choices=('smoke', 'retry', 'ownership', 'recovery', 'qualification'), default='recovery')
    plan.add_argument('--seed', type=int, default=1806)
    plan.add_argument('--case', action='append', default=[])
    plan.add_argument('--mode', choices=('fresh', 'diagnostic'), default='fresh')
    plan.add_argument('--layout', choices=('monolithic', 'grouped'), default='monolithic')
    run = sub.add_parser('run')
    run.add_argument('--plan', type=Path, required=True)
    run.add_argument('--bundle', type=Path, required=True)
    run.add_argument('--run-dir', type=Path, required=True)
    run.add_argument('--duration-minutes', type=int, default=120)
    run.add_argument('--retain-on-failure', action='store_true')
    pre = sub.add_parser('preflight')
    pre.add_argument('--plan', type=Path, required=True)
    pre.add_argument('--out', type=Path, required=True)
    for operation in ('collect', 'clean'):
        command = sub.add_parser(operation)
        command.add_argument('--owner', type=Path, required=True, help='exact owned fixture owner.json')
    compare = sub.add_parser('compare')
    compare.add_argument('--left', type=Path, required=True)
    compare.add_argument('--right', type=Path, required=True)
    aggregate = sub.add_parser('aggregate')
    aggregate.add_argument('--plan', type=Path, required=True)
    aggregate.add_argument('results', type=Path, nargs='+')
    args = parser.parse_args(argv)
    if args.command == 'bundle':
        build_bundle(args.out.resolve(), args.diagnostic, args.reuse)
    elif args.command == 'plan':
        harness = load_bundle(args.bundle)
        harness.pop('directory')
        value = make_plan(json.loads(args.subject.read_text()), harness, args.profile, args.seed, args.case, args.mode, args.layout)
        atomic_json(args.out, value)
    elif args.command == 'run':
        os.environ['KIND_EXPERIMENTAL_PROVIDER'] = 'docker'
        code = run_plan(json.loads(args.plan.read_text()), args.run_dir.resolve(), load_bundle(args.bundle), args.duration_minutes, args.retain_on_failure)
        print((args.run_dir / 'evidence/summary.md').read_text(), flush=True)
        return code
    elif args.command == 'preflight':
        from campaign_fixture import preflight
        args.out.parent.mkdir(parents=True, exist_ok=True)
        plan = json.loads(args.plan.read_text())
        result = preflight(plan['recipe']['resources'], args.out.parent, Commands(args.out.parent, cwd=ROOT))
        atomic_json(args.out, result)
        return int(bool(result['errors']))
    elif args.command in ('collect', 'clean'):
        from campaign_fixture import Fixture
        lockpath = Path.home() / '.cache/cnpg-backup-campaign.lock'
        with lockpath.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            manifest = Manifest.open(args.owner.resolve().parent.parent / 'evidence')
            fixture = Fixture.owned(args.owner, manifest)
            if fixture.closed:
                print('owned fixture already closed')
                return 0
            fixture.run('docker', 'unpause', fixture.NAME + '-control-plane', check=False, timeout=10)
            with fixture.commands.budget(300):
                manifest.collect_diagnostics('fixture', fixture.collect)
            if args.command == 'clean':
                with fixture.commands.budget(300):
                    fixture.close()
                manifest.event('explicit-owned-cleanup', owner=str(args.owner), complete=True)
            else:
                fixture.run('docker', 'pause', fixture.NAME + '-control-plane', timeout=10)
            return 0
    elif args.command == 'compare':
        left, right = json.loads(args.left.read_text()), json.loads(args.right.read_text())
        print(json.dumps(validate_results(left['plan'], [left, right], cross_environment=True), indent=2))
    elif args.command == 'aggregate':
        print(json.dumps(validate_results(json.loads(args.plan.read_text()), [json.loads(p.read_text()) for p in args.results]), indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
