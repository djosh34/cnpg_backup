# Backup monitoring (manager-owned)

The one-replica manager exposes `GET /metrics` on internal HTTP port **9091**,
through `Service/cnpg-backup-metrics`. CNPG's mTLS discovery remains on 9090;
there is no sidecar TCP service or backup-byte proxy. Restrict 9091 to your
monitoring workloads with your installation's NetworkPolicy; it is not an
Internet-facing authenticated endpoint. `config/backup-monitoring.yaml` is an
optional Prometheus Operator ServiceMonitor. Adapt its namespaces/selectors to
your stack. Preserve `honorLabels: true`: metric `namespace` means the managed
Cluster namespace, not the manager Service namespace. Monitor scrape `up`
separately; a missing manager target cannot emit an unknown-history gauge.

All four metrics use only `{repository_id,namespace,cluster,backup_type}`. Type
is exactly `full` or `differential`, never a UID, backup name, key, error or secret.
Full capture ships in F; differential execution is H's responsibility. Merely
exporting its series/threshold does not advertise differential Backup capability.

| Metric | Meaning |
| --- | --- |
| `cnpg_backup_last_success_timestamp_seconds` | Latest immutable commit object's **S3 LastModified** per requested type. Includes retired commits. Omitted for unknown or never-successful history. |
| `cnpg_backup_success_history_known` | 1 after a complete validated metadata scan; 0 on scan/configuration failure, before first scan, or if the last successful scan is over five minutes old. |
| `cnpg_backup_failures_total` | Each observed terminal failed Backup UID once per manager lifetime. |
| `cnpg_backup_freshness_max_age_seconds` | Explicit per-type configured schedule budget; omitted when that type is not configured. |

The success scan reads only permanent repository identity, request, claim,
commit and retirement metadata. Repository code owns key derivation/validation.
It retains two maxima, not a catalog in RAM; reads no tar/manifest payloads and
never creates a deletion hold or writes S3. Retention can delete payloads without
erasing historical success. **Freshness is not current recoverability or WAL
coverage evidence.** Uninitialized/missing repository identity is unknown, not a
known-empty success history. An initialized, complete empty history is known=1
with no timestamp sample. Wrong writer identity, malformed metadata, missing
LastModified or a failed/partial list produces unknown, not partial maxima.

One serial sweep processes one Cluster per API page/turn, with five-second pacing
and a **60-second total API+S3 deadline** per turn. Scrapes, lifecycle reconciliation
and failure observation are independent of S3. Large/slow histories may exceed
that budget and stay unknown; increasing backup capacity is not permission to
block metrics indefinitely. Kubernetes configuration is resolved afresh; Secrets
are uncached get-only and allowlisted, with a single read per referenced Secret
within a snapshot. No native replication credentials are needed for history.

## Invocation failure is a different fact from durable success

The Backup informer establishes its initial list as a baseline: preexisting
terminal failures do not replay into a restarted counter. A known nonterminal UID
that later becomes failed counts once, including a transition discovered on relist
after a watch interruption. Requeues/resync/relist, API/S3 retries and pathological
phase regression do not count that UID again. A newly observed failed object after
initial synchronization counts once. Deletion evicts UID observation state, not
already aggregated failures. Transitions wholly missed across downtime are not
invented. Ordinary Prometheus counter-reset semantics apply. This is monitoring,
not a durable exactly-once audit ledger. Cluster deletion/repository reassignment
removes obsolete label sets after a completed configuration sweep.

If a commit was durable but CNPG lost the invocation response, the same operation
can correctly yield **fresh committed success AND one failed invocation**. Failed
uncommitted attempts never advance success. The manager does not infer success
from CNPG phase `completed`, and does not relabel terminal failed Backups.

CNPG exclusively owns `Backup.status`; its failure details remain the primary
invocation diagnosis. The manager supplements CNPG's Normal event with best-effort
Warning **`BackupFailed`**, using constant redacted text identifying only the
requested type. Warnings are rate-limited to one per label set per five minutes;
all recognized failures still increment the counter. Event delivery failure is
not retried into a warning storm. Existing Repository configuration/retention
conditions remain untouched: the current CRD does not admit per-type success
status fields, and a separate status writer must not overwrite other conditions.

## Configure schedules and alerts independently by requested type

Use native CNPG ScheduledBackup, with explicit `target: primary`, plugin method
and required `pluginConfiguration.parameters.backupType`. There is no plugin cron
loop. Set a freshness budget **only for a type you schedule**:

```yaml
# Repository.spec fragment, merged with required repository/storage configuration
backupFreshness:
  fullMaxAge: 192h       # example weekly full + capture/queue/outage allowance
  # differentialMaxAge: 26h  # enable with a daily differential schedule in H
```

Thresholds opt a type into monitoring; the manager does not parse cron or infer a
schedule from an on-demand Backup. Disable the corresponding threshold if you
remove/suspend that schedule. Choose budgets above the actual schedule interval
plus capture duration and expected jitter. No threshold means no never/stale/
failure/history-unknown alert for that type. It does not disable metrics/events.

Install `config/backup-alerts.yaml` where your stack selects PrometheusRules. Each
type has separate never-successful, stale-success, invocation-failing and
history-unknown alerts. Never-successful uses known history **and absence** of a
timestamp, never a fabricated timestamp zero. Unknown does not pretend never
successful. Stale checks known history and the matching type's budget. Failure
uses counter increase over 15 minutes and does not erase fresh committed success.
WAL alerts in `config/wal-alerts.yaml` remain separate.

## Evidence and native harness integration

Go tests execute the actual informer, fake Kubernetes API, metrics handler and
repository metadata reader. Repository tests publish synthetic commits with the
real publication protocol, retire/delete payloads under GC, and check history
without admission/mutation/payload reads. These are **module tests, not real PG
backups or MinIO/CNPG acceptance**.

Actual alert-engine controls (16 cases / 128 alert assertions, including absent
threshold, opposite-type threshold, unknown, healthy and counter reset):

```sh
CNPG_PROMTOOL=/path/to/promtool CGO_ENABLED=0 go test ./internal/cnpgi -run TestBackupAlertsPromtool -v
python3 hack/backup_metrics_smoke.py --self-test
```

Promtool is a test-only executable, not a runtime Go dependency. The alert test
explicitly skips without `CNPG_PROMTOOL`; a skipped test is not alert qualification.

F's native harness author imports `hack/backup_metrics_smoke.py` and uses:

```python
metrics = BackupMetricsSmoke(h, report, repository_id).start()
try:
    # Take a REAL backup; independently obtain its S3 commit LastModified epoch.
    before = metrics.assert_committed(commit_last_modified)
    # Trigger a REAL isolated failed invocation and wait for CNPG phase failed.
    metrics.assert_failed(failed_backup_name, before)
    # For durable-commit/lost-response injection, additionally supply:
    # committed_after_loss=independently_observed_commit_last_modified
finally:
    metrics.close()
```

Start the helper before triggering Backup transitions. Its assertions read the
actual manager HTTP endpoint, CNPG failed status and supplemental Warning Event;
it neither manufactures Backup resources nor commits. Use an isolated first
failure per type when asserting Warning delivery, because later same-type failures
within five minutes are intentionally throttled. Manager restarts break the
port-forward: close/restart the helper and establish a new reset-counter baseline.
Real CNPG/PG18/MinIO on-demand/scheduled capture, native faults, restart and
lost-response acceptance remain the native F author's integration gate.
