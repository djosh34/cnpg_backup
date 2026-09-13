# Monitoring reference

The one-replica manager exposes HTTP `GET /metrics` on port 9091 through `Service/cnpg-backup-metrics`. CNPG discovery uses mTLS on 9090. There is no sidecar metrics TCP service or backup-byte proxy. Restrict the unauthenticated metrics port to monitoring workloads.

## Installation files

| File | Purpose |
|---|---|
| [backup-monitoring.yaml](../config/backup-monitoring.yaml) | Optional Prometheus Operator ServiceMonitor |
| [backup-alerts.yaml](../config/backup-alerts.yaml) | Separate full and differential freshness, failure, never-successful, and unknown-history alerts |
| [wal-monitoring.yaml](../config/wal-monitoring.yaml) | CNPG custom queries for pending `.ready` files and oldest pending age |
| [wal-alerts.yaml](../config/wal-alerts.yaml) | Archive failure, backlog age, and filesystem-pressure alerts |
| [operational-alerts.yaml](../config/operational-alerts.yaml) | Restore, admission, retention, and workspace alerts |

The ServiceMonitor uses `honorLabels: true`. Metric `namespace` identifies the managed Cluster namespace, not the manager Service namespace. Selectors and namespaces need to match the consuming Prometheus installation. Scrape `up` alerts are separate because a dead target cannot emit an unknown-state gauge. The WAL query file documents its `Cluster.spec.monitoring.customQueriesConfigMap` reference.

## Backup metrics

These metrics use only `{repository_id,namespace,cluster,backup_type}`. `backup_type` is `full` or `differential`, never a UID, key, backup name, or error string.

| Metric | Meaning |
|---|---|
| `cnpg_backup_last_success_timestamp_seconds` | Latest immutable commit's S3 LastModified per type, including retired commits. Omitted for unknown or never-successful history |
| `cnpg_backup_success_history_known` | 1 after a complete validated metadata scan. 0 before the first scan, after a scan/configuration failure, or when the successful scan is over five minutes old |
| `cnpg_backup_failures_total` | Each observed terminal failed Backup UID once per manager process lifetime |
| `cnpg_backup_freshness_max_age_seconds` | Explicit configured per-type schedule budget. Omitted when unset |

Success history reads permanent identity, request, claim, commit, and retirement metadata, not tar or manifest payloads. It uses no deletion hold or S3 writes. A complete initialized empty history is known with no success timestamp. Missing identity, malformed records, missing LastModified, or partial lists produce unknown, never partial maxima. The serial sweep has a 60-second API-plus-S3 deadline per Cluster turn and five-second pacing. Large or slow histories can remain unknown.

Freshness is not proof of current recoverability or continuous WAL coverage. Retention can delete payloads without erasing historical success.

### Invocation failures

The Backup informer treats its initial list as a baseline, so old failures do not reappear in a restarted counter. An observed transition to failed counts once, including one found after relist. Requeues and storage retries do not count again. Transitions entirely missed during downtime are not invented. Counter resets have ordinary Prometheus semantics; this is not a durable audit ledger.

A durable commit with a lost callback response can produce both fresh committed success and a failed invocation. Uncommitted failures never advance success. CNPG owns `Backup.status`; the manager does not relabel it to match S3.

The supplemental Warning `BackupFailed` uses redacted per-type text and is limited to one event per label set per five minutes. All recognized failures still increment the counter. Event-delivery failure does not alter repository authority or status.

### Schedule budgets

`Repository.spec.backupFreshness.fullMaxAge` and `differentialMaxAge` enable alerts independently. A weekly full can use `192h`, and a daily differential can use `26h`, with adjustments for measured duration and expected delay. The manager does not infer cron schedules from CNPG resources. Removing or suspending a schedule requires removing its budget to disable its schedule alerts.

No budget means no never-successful, stale, failure, or history-unknown alert for that type. Metrics and events remain available. Never-successful alerts require known history and absence of a timestamp, not a fabricated timestamp zero. Unknown history is distinct from never successful. Failure alerts use counter increase over 15 minutes and can coexist with fresh success.

## WAL metrics

PostgreSQL's synchronous callback outcome feeds CNPG's existing `cnpg_pg_stat_archiver_*` metrics, including archived count, failed count, and last archive/failure times. Custom queries add pending count and oldest-pending age. Idle clusters do not become lagging solely because they generate no new WAL.

Archive latency includes queue and upload time beyond `archive_timeout=60s`. Current-segment and backlog loss remain possible. Outages have no fixed RPO. Last archive success does not establish a continuous PITR frontier.

Filesystem-pressure alerts depend on kubelet PVC volume metrics. A CSI driver that omits these metrics leaves capacity unobserved, not healthy. Structured callback logs contain elapsed time and bounded status codes, not object keys, paths, credentials, or SDK messages.

## Restore and retention metrics

| Metric | Meaning |
|---|---|
| `cnpg_backup_restore_observation_known` | Whether the manager has a current observation of this Cluster's selected plugin restore |
| `cnpg_backup_restore_active` | An observed active restore |
| `cnpg_backup_restore_uncertain` | An observed uncertain restore |
| `cnpg_backup_restore_lifetime_release_pending` | Completed observation whose stable lifetime release is still pending |
| `cnpg_backup_retention_blocked` | Observed retention blockage |
| `cnpg_backup_repository_admission_blocked` | Observed exclusive repository owner blocks admission |
| `cnpg_backup_repository_holders` | Observed holder count |
| `cnpg_backup_retention_workspace_available` | Last reservation outcome, not live disk free space |
| `cnpg_backup_retention_checked_timestamp_seconds` | Time of the periodic retention observation |

Restore metrics use only namespace and Cluster labels. They describe the selected external source using this plugin, not unused source declarations or unrelated restores. Observations older than five minutes become unknown and state gauges disappear. A missing operation is not completed. Completed lifetime release does not prove that every process-reader holder is gone.

Repository metrics use bounded repository, namespace, and Cluster labels. Unobserved holder/admission gauges are omitted rather than set to healthy zero. The workspace outcome is omitted when unobserved and corresponds to the `RetentionWorkspaceAvailable` Repository condition. Retention observations follow the configured interval, which can be 24 hours, and expire after 48 hours.

Warnings and conditions help diagnose `RetentionBlocked` and `RepositoryAdmissionBlocked`. Durable gate state remains authoritative. [Uncertain-operation handling](operations.md#handle-uncertain-recovery-or-retention) never uses metric values, Pod deletion, or elapsed time to clear protection.

## Alert validation

The normal `./hack/test unit` profile installs the pinned Promtool and tests the actual rule files. A targeted invocation is:

```sh
CNPG_PROMTOOL=/path/to/promtool CGO_ENABLED=0 go test ./internal/cnpgi -run TestBackupAlertsPromtool -v
python3 hack/backup_metrics_smoke.py --self-test
```

The Go alert test skips without `CNPG_PROMTOOL`. A skipped test is not alert-engine coverage. Real recovery campaigns compare manager metrics with S3 commit timestamps, actual CNPG failures, and Warning events.
