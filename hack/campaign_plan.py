"""Exact campaign recipe, dependency closure, and fail-closed aggregation."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile

from campaign_process import redact

ROOT = Path(__file__).resolve().parents[1]
SOURCE_LOSS = 'source-namespace-catalog-loss-S3-only'
SUPPLEMENTAL = ('seeded-XID-1', 'seeded-XID-2')
SMOKE = ('full-latest-remote-SQL', 'full-name-pre-DROP-SQL', SOURCE_LOSS)
# Each function is an existing real-system oracle, not a modeled substitute.
REGISTRY = (
    {'id': 'source-namespace-catalog-loss-S3-only', 'method': 'case_source_namespace_catalog_loss_S3_only', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 1200, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G source-namespace-catalog-loss-S3-only'},
    {'id': 'full-latest-remote-SQL', 'method': 'case_full_latest_remote_SQL', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G full-latest-remote-SQL'},
    {'id': 'full-name-pre-DROP-SQL', 'method': 'case_full_name_pre_DROP_SQL', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G full-name-pre-DROP-SQL'},
    {'id': 'full-time-inclusive-exclusive', 'method': 'case_full_time_inclusive_exclusive', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G full-time-inclusive-exclusive'},
    {'id': 'full-LSN-inclusive-exclusive', 'method': 'case_full_LSN_inclusive_exclusive', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G full-LSN-inclusive-exclusive'},
    {'id': 'full-XID-inclusive-exclusive', 'method': 'case_full_XID_inclusive_exclusive', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G full-XID-inclusive-exclusive'},
    {'id': 'full-explicit-immediate', 'method': 'case_full_explicit_immediate', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G full-explicit-immediate'},
    {'id': 'newest-base-too-new', 'method': 'case_newest_base_too_new', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G newest-base-too-new'},
    {'id': 'target-unreached', 'method': 'case_target_unreached', 'group': 'targets', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G target-unreached'},
    {'id': 'shell-free-original-verification', 'method': 'case_shell_free_original_verification', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G shell-free-original-verification'},
    {'id': 'bundle-duplicate-absent-local-fallback', 'method': 'case_bundle_duplicate_absent_local_fallback', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G bundle-duplicate-absent-local-fallback'},
    {'id': 'bundle-local-missing-fatal', 'method': 'case_bundle_local_missing_fatal', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G bundle-local-missing-fatal'},
    {'id': 'same-bundled-segment-post-EndLSN-archive-preferred', 'method': 'case_same_bundled_segment_post_EndLSN_archive_preferred', 'group': 'wal', 'fixtures': ['source', 'same-segment'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G same-bundled-segment-post-EndLSN-archive-preferred'},
    {'id': 'negative-controls-EOF-and-bundle-as-success', 'method': 'case_negative_controls_EOF_and_bundle_as_success', 'group': 'wal', 'fixtures': ['source', 'same-segment'], 'requires': ['same-bundled-segment-post-EndLSN-archive-preferred'], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G negative-controls-EOF-and-bundle-as-success'},
    {'id': 'required-corrupt-no-latest-promotion', 'method': 'case_required_corrupt_no_latest_promotion', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G required-corrupt-no-latest-promotion'},
    {'id': 'required-missing-no-latest-promotion', 'method': 'case_required_missing_no_latest_promotion', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G required-missing-no-latest-promotion'},
    {'id': 'bundle-auth-fatal', 'method': 'case_bundle_auth_fatal', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G bundle-auth-fatal'},
    {'id': 'bundle-transport-fatal', 'method': 'case_bundle_transport_fatal', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G bundle-transport-fatal'},
    {'id': 'bundle-TLS-fatal', 'method': 'case_bundle_TLS_fatal', 'group': 'wal', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G bundle-TLS-fatal'},
    {'id': 'guard-before-RPC-same-PVC-no-mutation', 'method': 'case_guard_before_RPC_same_PVC_no_mutation', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G guard-before-RPC-same-PVC-no-mutation'},
    {'id': 'guard-delayed-response-same-PVC-no-mutation', 'method': 'case_guard_delayed_response_same_PVC_no_mutation', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G guard-delayed-response-same-PVC-no-mutation'},
    {'id': 'guard-replay-pause-same-PVC-no-mutation', 'method': 'case_guard_replay_pause_same_PVC_no_mutation', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G guard-replay-pause-same-PVC-no-mutation'},
    {'id': 'guard-shutdown-pause-same-PVC-no-mutation', 'method': 'case_guard_shutdown_pause_same_PVC_no_mutation', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G guard-shutdown-pause-same-PVC-no-mutation'},
    {'id': 'sidecar-death-poisons', 'method': 'case_sidecar_death_poisons', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G sidecar-death-poisons'},
    {'id': 'guard-death-poisons', 'method': 'case_guard_death_poisons', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G guard-death-poisons'},
    {'id': 'poison-fresh-Cluster-all-fresh-PVC-retry', 'method': 'case_poison_fresh_Cluster_all_fresh_PVC_retry', 'group': 'ownership', 'fixtures': ['source'], 'requires': ['sidecar-death-poisons', 'guard-death-poisons'], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G poison-fresh-Cluster-all-fresh-PVC-retry'},
    {'id': 'detached-PG-descendants', 'method': 'case_detached_PG_descendants', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G detached-PG-descendants'},
    {'id': 'pending-sidecar-task-same-incarnation-drain', 'method': 'case_pending_sidecar_task_same_incarnation_drain', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G pending-sidecar-task-same-incarnation-drain'},
    {'id': 'stale-tuple-rejected', 'method': 'case_stale_tuple_rejected', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G stale-tuple-rejected'},
    {'id': 'source-lifetime-and-reader-through-replay', 'method': 'case_source_lifetime_and_reader_through_replay', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G source-lifetime-and-reader-through-replay'},
    {'id': 'controller-all-Job-retry-Pods-terminated', 'method': 'case_controller_all_Job_retry_Pods_terminated', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 4, 'requirement': 'design §3/5; testing mandatory G controller-all-Job-retry-Pods-terminated'},
    {'id': 'differential-native', 'method': 'case_differential_native', 'group': 'differential', 'fixtures': ['source', 'differential'], 'requires': [], 'seconds': 1200, 'max_targets': 1, 'requirement': 'design §3/5/6; PR H direct-F reconstruction, source-loss remote PITR and fail-closed prerequisite/cancellation metrics'},
    {'id': 'seeded-XID-1', 'method': 'case_seeded_XID_1', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 1, 'requirement': 'design §5 inclusive/exclusive XID; testing seeded supplement'},
    {'id': 'seeded-XID-2', 'method': 'case_seeded_XID_2', 'group': 'ownership', 'fixtures': ['source'], 'requires': [], 'seconds': 900, 'max_targets': 1, 'requirement': 'design §5 inclusive/exclusive XID; testing seeded supplement'},
)

BRANCHES = {
    'differential-native': ('reconstruction', 'remote-PITR-source-loss', 'missing-full', 'missing-summary', 'checksum', 'promotion', 'cancellation'),
    'full-latest-remote-SQL': ('newest', 'explicit-base'),
    'full-time-inclusive-exclusive': ('inclusive', 'exclusive'),
    'full-LSN-inclusive-exclusive': ('inclusive', 'exclusive'),
    'full-XID-inclusive-exclusive': ('inclusive', 'exclusive'),
    'newest-base-too-new': ('earlier-base', 'reject-explicit-newest'),
    'shell-free-original-verification': ('missing-WAL', 'corrupt-WAL'),
    'bundle-duplicate-absent-local-fallback': ('intact-local-fallback', 'all-required-255'),
    'same-bundled-segment-post-EndLSN-archive-preferred': (
        'healthy', 'wrong-bundle', 'healthy-after-negative', 'missing-wal-get',
        'corrupt-wal-get', 'auth-wal-get', 'reset-wal-get', 'tls-wal-get'),
}
for case in REGISTRY:
    case['branches'] = list(BRANCHES.get(case['id'], ('main',)))

MANDATORY = tuple(c['id'] for c in REGISTRY if c['id'] not in SUPPLEMENTAL)
REGISTRY += ({'id': 'retirement-20', 'method': 'case_retirement_20', 'group': 'ownership',
              'fixtures': ['source'], 'requires': [], 'seconds': 3600, 'max_targets': 20,
              'diagnostic': True, 'requirement': 'testing: >16 sequential negative/restore admissions, source uncertain holds preserved, bounded fixture retirement'},)


def selected(profile, requested=()):
    registry = {c['id']: c for c in REGISTRY}
    if len(registry) != len(REGISTRY):
        raise ValueError('duplicate scenario registration')
    names = set(requested)
    if not names:
        if profile == 'smoke':
            names.update(SMOKE)
        elif profile == 'retry':
            names.update((SOURCE_LOSS, 'controller-all-Job-retry-Pods-terminated'))
        elif profile == 'ownership':
            names.add(SOURCE_LOSS)
            names.update(c['id'] for c in REGISTRY if c['group'] == 'ownership' and c['id'] in MANDATORY)
        else:
            names.update((*MANDATORY, *SUPPLEMENTAL))
    if not names <= registry.keys():
        raise ValueError('unknown requested scenario')
    def expand(name, stack):
        if name in stack:
            raise ValueError('cyclic scenario prerequisite')
        for dependency in registry[name]['requires']:
            names.add(dependency)
            expand(dependency, stack + [name])
    for name in list(names):
        expand(name, [])
    return [c for c in REGISTRY if c['id'] in names]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def image_config_digest(archive, tag):
    """Canonical config identity from a single-image Docker/OCI save archive.

    Docker classic calls this .Id; containerd-backed Docker may call a manifest
    .Id instead. Neither a mutable tag nor that display field is byte identity.
    """
    with tarfile.open(archive) as source:
        def member(name):
            entry = source.getmember(name)
            if not entry.isfile() or entry.size > 1 << 20:
                raise ValueError('invalid bounded fixture image metadata')
            return source.extractfile(entry).read()
        manifests = json.loads(member('manifest.json'))
        if len(manifests) != 1 or tag not in manifests[0]['RepoTags']:
            raise ValueError('fixture archive does not select exactly the requested image')
        raw = member(manifests[0]['Config'])
        config = json.loads(raw)
        if config.get('os') != 'linux' or config.get('architecture') != 'amd64' or config['rootfs']['type'] != 'layers':
            raise ValueError('fixture image differs from declared platform')
        return 'sha256:' + hashlib.sha256(raw).hexdigest()


def content_hash():
    inputs = ['hack', 'config', 'build', 'go.mod', 'go.sum', 'internal', 'docs/campaign-assertion-audit.json', '.github/workflows']
    files = subprocess.check_output(['git', 'ls-files', '-z', *inputs], cwd=ROOT).decode().split('\0')
    # Include untracked implementation files so diagnostics cannot masquerade as
    # the committed harness. Qualification separately rejects any dirty tree.
    files += subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z', *inputs], cwd=ROOT).decode().split('\0')
    return digest({name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                   for name in sorted(set(files)) if name and (ROOT / name).is_file()})


def make_plan(subject, harness, profile='recovery', seed=1806, requested=(), mode='fresh', layout='monolithic'):
    if profile not in ('smoke', 'recovery', 'retry', 'ownership') or type(seed) is not int or not 0 <= seed < 2**31:
        raise ValueError('invalid campaign profile/seed')
    if not re.fullmatch('[0-9a-f]{40}', subject['revision']):
        raise ValueError('subject revision must be immutable')
    for flavor in ('manager', 'pg18'):
        if not re.fullmatch(r'ghcr\.io/djosh34/cnpg-backup-' + flavor + r'@sha256:[0-9a-f]{64}', subject['images'][flavor]):
            raise ValueError('invalid immutable subject digest')
    if harness['content_hash'] != content_hash():
        raise ValueError('harness content differs from immutable record')
    if mode not in ('fresh', 'diagnostic') or layout not in ('monolithic', 'grouped'):
        raise ValueError('invalid fixture mode/layout')
    cases = selected(profile, requested)
    return {'schema': 2, 'subject': subject, 'harness': harness,
            'recipe': {'seed': seed, 'profile': profile, 'fixture_mode': mode, 'layout': layout, 'duration_minutes': 120,
                       'registry_hash': digest(REGISTRY), 'resources': {'node_cpus': 4, 'node_memory_gib': 5,
                       'host_headroom_gib': 2, 'disk_start_gib': 14, 'disk_floor_gib': 5,
                       'target_gib': 3, 'capture_gib': 8, 'capture_spares': 2},
                       'compatibility': json.loads((ROOT / 'build/kubernetes-inputs.lock.json').read_text())},
            'cases': [c['id'] for c in cases]}




def validate_results(plan, results, cross_environment=False):
    if plan['harness'].get('schema') != 2:
        raise ValueError('unsupported harness image-identity schema')
    expected = set(plan['cases'])
    if len(plan['cases']) != len(expected) or plan['recipe'].get('registry_hash') != digest(REGISTRY):
        raise ValueError('duplicate cases or mismatched scenario registry')
    if expected != set((*MANDATORY, *SUPPLEMENTAL)):
        raise ValueError('partial plan is ineligible for full-fresh aggregation')
    if not results:
        raise ValueError('no results')
    identities = [r.get('execution_id') for r in results]
    if any(not isinstance(i, str) or not re.fullmatch('[a-f0-9]{32}', i) for i in identities) or len(set(identities)) != len(identities):
        raise ValueError('missing or duplicate execution identity')
    if cross_environment and (len(results) != 2 or {r.get('host') for r in results} != {'local', 'hosted'}):
        raise ValueError('comparison requires one local and one hosted execution')
    for result in results:
        if result.get('plan') != plan:
            raise ValueError('subject/harness/recipe/scenario inputs differ')
        if result.get('fixture_mode') != 'fresh' or plan['recipe'].get('fixture_mode') != 'fresh':
            raise ValueError('retained/imported/diagnostic fixtures are ineligible')
        if result.get('duration_minutes') != plan['recipe']['duration_minutes']:
            raise ValueError('actual run deadline differs from immutable recipe')
        if not result.get('scope_passed') or not result.get('teardown_complete') or result.get('failures'):
            raise ValueError('failed/incomplete/leaked attempt')
        scenarios = result.get('scenarios', {})
        if set(scenarios) != expected or any(r['status'] != 'passed' for r in scenarios.values()):
            raise ValueError('missing, duplicate or failed mandatory/supplemental evidence')
        for case in REGISTRY:
            if case['id'] not in expected:
                continue
            branches = scenarios[case['id']].get('branches', {})
            if set(branches) != set(case['branches']) or any(b.get('status') != 'passed' for b in branches.values()):
                raise ValueError('missing or failed required branch evidence')
        envelopes = result.get('fixture_envelopes', {})
        if not envelopes or any(set(limits) != {'node_cpus', 'node_memory_gib'} or any(v != plan['recipe']['resources'][k] for k, v in limits.items()) for limits in envelopes.values()):
            raise ValueError('missing or mismatched actual resource envelope')
        observed = result.get('fixture_images', {})
        if set(observed) != set(plan['harness']['images']) or any(observed[k].get('config_digest') != image['config_digest'] or observed[k].get('archive_sha256') != plan['harness']['files'][image['archive']] for k, image in plan['harness']['images'].items()):
            raise ValueError('missing or mismatched consumed fixture image bytes')
    return {'same_inputs': True, 'full_fresh_attempts': len(results), 'release_qualified': False,
            'host_fingerprints': [r.get('host_fingerprint', {}) for r in results],
            'physical_timing_equivalence_claimed': False}
