"""Callable assertions for F's REAL CNPG backup harness, not a backup fixture.

Call start() before triggering backups so Prometheus counters have a baseline.
Supply independent S3 commit LastModified to assert_committed(), never CNPG's
status completion timestamp. No local self-test is live integration evidence.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time
import unittest
import urllib.request

LABELS = {'repository_id', 'namespace', 'cluster', 'backup_type'}
RETENTION_METRICS = ('cnpg_backup_retention_blocked', 'cnpg_backup_repository_admission_blocked',
                     'cnpg_backup_repository_holders', 'cnpg_backup_retention_workspace_available',
                     'cnpg_backup_retention_checked_timestamp_seconds')
RESTORE_METRICS = ('cnpg_backup_restore_observation_known', 'cnpg_backup_restore_active',
                   'cnpg_backup_restore_uncertain', 'cnpg_backup_restore_lifetime_release_pending')
METRICS = ('cnpg_backup_failures_total', 'cnpg_backup_success_history_known',
           'cnpg_backup_last_success_timestamp_seconds', 'cnpg_backup_freshness_max_age_seconds')


def samples(text):
    result = {}
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        match = re.fullmatch(r'([a-z_]+)\{(.*)\} ([0-9.eE+-]+)', line)
        assert match and match[1] in (*METRICS, *RETENTION_METRICS, *RESTORE_METRICS), 'unexpected manager metric/format'
        labels = dict(re.findall(r'([a-z_]+)="([^"\\]*)"', match[2]))
        if match[1] in RESTORE_METRICS:
            assert set(labels) == {'namespace', 'cluster'}, 'unbounded restore metric labels'
            key = (match[1], labels['namespace'], labels['cluster'])
        elif match[1] in RETENTION_METRICS:
            assert set(labels) == LABELS - {'backup_type'}, 'unbounded retention metric labels'
            key = (match[1], labels['repository_id'], labels['namespace'], labels['cluster'])
        else:
            assert set(labels) == LABELS and labels['backup_type'] in ('full', 'differential'), 'unbounded metric labels'
            key = (match[1], labels['repository_id'], labels['namespace'], labels['cluster'], labels['backup_type'])
        assert key not in result, 'duplicate manager-owned metric sample'
        result[key] = float(match[3])
    return result


class BackupMetricsSmoke:
    def __init__(self, h, report, repository_id, cluster='database', port=19091):
        self.h, self.report, self.repository_id, self.cluster, self.port = h, report, repository_id, cluster, port
        self.forward = None
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        report.setdefault('backup_metrics_completed', [])

    def start(self):
        directory = self.h.WORK / 'backup-metrics'
        directory.mkdir(mode=0o700, exist_ok=True)
        with (directory / 'forward.log').open('w') as log:
            self.forward = subprocess.Popen([str(self.h.WORK / 'kubectl'), '--kubeconfig', str(self.h.WORK / 'kubeconfig'),
                'port-forward', '-n', 'cnpg-system', 'service/cnpg-backup-metrics', f'{self.port}:9091'], stdout=log, stderr=log)
        self.h.wait(lambda: self.snapshot('full') is not None, 'manager backup metrics baseline')
        return self

    def close(self):
        if self.forward:
            self.forward.terminate()
            try:
                self.forward.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.forward.kill()
                self.forward.wait(timeout=5)
            self.forward = None

    def snapshot(self, kind):
        assert kind in ('full', 'differential')
        try:
            with self.http.open(f'http://127.0.0.1:{self.port}/metrics', timeout=3) as response:
                data = response.read(2 << 20)
            parsed = samples(data.decode())
        except (OSError, TimeoutError):
            return None
        result = {name: parsed[(name, self.repository_id, self.h.NS, self.cluster, kind)]
                  for name in METRICS if (name, self.repository_id, self.h.NS, self.cluster, kind) in parsed}
        return result if METRICS[0] in result else None

    def assert_committed(self, published_at, kind='full'):
        """published_at is independent actual S3 LastModified epoch seconds."""
        assert published_at > 0
        def committed():
            current = self.snapshot(kind)
            return current and current.get(METRICS[1]) == 1 and current.get(METRICS[2]) == int(published_at)
        self.h.wait(committed, 'manager freshness equals actual durable S3 commit', seconds=180)
        self.report['backup_metrics_completed'].append({'case': 'actual-committed-freshness', 'backup_type': kind,
                                                       's3_last_modified': int(published_at)})
        return self.snapshot(kind)

    def assert_failed(self, backup_name, before, kind='full', committed_after_loss=None, require_warning=True):
        """An isolated terminal failure; throttle can suppress later same-type Warnings.

        For a lost RPC response supply the independently observed durable commit
        epoch as committed_after_loss. For an uncommitted failure omit it: success
        must remain unchanged, not be refreshed from CNPG's failed status.
        """
        backup = json.loads(self.h.kube('get', 'backup/' + backup_name, '-n', self.h.NS, '-o', 'json'))
        assert backup['status']['phase'] == 'failed', 'CNPG did not report terminal invocation failure'
        uid = backup['metadata']['uid']
        expected_success = before.get(METRICS[2]) if committed_after_loss is None else int(committed_after_loss)
        def failed():
            current = self.snapshot(kind)
            return (current and current.get(METRICS[0]) == before[METRICS[0]] + 1
                    and current.get(METRICS[1]) == 1 and current.get(METRICS[2]) == expected_success)
        self.h.wait(failed, 'one terminal failure without fabricated success', seconds=180)
        if require_warning:
            def warned():
                events = json.loads(self.h.kube('get', 'events', '-n', self.h.NS,
                    '--field-selector', f'involvedObject.uid={uid},reason=BackupFailed', '-o', 'json'))['items']
                return any(e.get('type') == 'Warning' and e.get('source', {}).get('component') == 'cnpg-backup'
                           and e.get('message', '').startswith('Requested ' + kind + ' backup invocation failed;') for e in events)
            self.h.wait(warned, 'actual supplemental BackupFailed Warning')
        for _ in range(3):
            time.sleep(1)
            assert failed(), 'requeue/resync recounted failure or refreshed uncommitted success'
        self.report['backup_metrics_completed'].append({'case': 'actual-terminal-failure', 'backup_type': kind,
            'warning_asserted': require_warning, 'durable_commit_response_loss': committed_after_loss is not None})
        return self.snapshot(kind)


class HelperUnitTests(unittest.TestCase):
    def test_parser_positive_and_unbounded_negative(self):
        labels = 'repository_id="r",namespace="n",cluster="c",backup_type="full"'
        self.assertEqual(samples(METRICS[0] + '{' + labels + '} 1')[(METRICS[0], 'r', 'n', 'c', 'full')], 1)
        for damaged in (labels + ',uid="secret"', labels.replace('full', 'secret-type')):
            with self.assertRaises(AssertionError):
                samples(METRICS[0] + '{' + damaged + '} 1')
        text = METRICS[0] + '{' + labels + '} 1'
        with self.assertRaises(AssertionError):
            samples(text + '\n' + text)

    def test_install_wires_only_manager_metrics_and_scoped_backup_watch(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('renderer', Path(__file__).resolve().parents[1] / 'config/render.py')
        renderer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(renderer)
        image = 'test@sha256:' + 'a' * 64
        objects = renderer.render(image, image, 'cnpg-system', 'test', ['auth'])['items']
        service = next(o for o in objects if o['kind'] == 'Service' and o['metadata']['name'] == 'cnpg-backup-metrics')
        self.assertEqual(service['spec']['ports'], [{'name': 'metrics', 'port': 9091, 'targetPort': 'metrics'}])
        self.assertNotIn('cnpg.io/pluginName', service['metadata']['labels'])
        role = next(o for o in objects if o['kind'] == 'Role' and o['metadata']['name'] == 'cnpg-backup')
        self.assertIn({'apiGroups': ['postgresql.cnpg.io'], 'resources': ['backups'], 'verbs': ['list', 'watch']}, role['rules'])
        self.assertFalse(any('backups/status' in r['resources'] for r in role['rules']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true', required=True)
    parser.parse_args()
    unittest.main(argv=['backup_metrics_smoke.py'])
