# Native differentials, retention policies and restore/deletion coordination

Status: primary-source planning research, **not an executed backup/storage experiment or a finalized lock protocol**. Two Paseo Astra/high researchers returned the findings below; their reports were collected and both agents were archived with `Archived: true` verified. Agent IDs: `5de17e4b-c799-4401-a6a7-990db6c886a3` and `7ca7dce7-be35-4aa6-aac9-51ecf9b42394`.

## PostgreSQL supplies the differential building blocks

[PG18 pg_basebackup](https://www.postgresql.org/docs/18/app-pgbasebackup.html) accepts an earlier backup's manifest via `--incremental`. It need not be the immediately preceding backup. Repeatedly supplying the same full F's manifest creates D1 and D2 each relative to F. This is native **incremental backup with a full-reference policy**, not a separate native differential command or a project-owned diff engine.

[pg_combinebackup](https://www.postgresql.org/docs/18/app-pgcombinebackup.html) reconstructs the required dependency chain oldest-to-newest: F + D2 suffices for D2; D1 is unnecessary. PostgreSQL supplies changed-block detection and reconstruction. This plugin still owns scheduling, reference selection, manifests/catalog, safe publication, verification, WAL handling and retention. The combine tool's dependency checks are not backup integrity verification; use `pg_verifybackup` separately.

The feature was [introduced in PG17](https://www.postgresql.org/docs/17/release-17.html), and is available in the agreed PG18 target. The reason for bounded differential-to-full support is both native reuse and avoiding long restore/deletion dependency chains, not a PostgreSQL prohibition on deeper chains.

[Required WAL summaries](https://www.postgresql.org/docs/18/continuous-archiving.html#BACKUP-INCREMENTAL-BACKUP) must cover every LSN from the reference backup's start to the new backup's start. PostgreSQL waits for summarization, but fails if coverage was removed or cannot catch up. It does **not** automatically fall back to full. [Settings](https://www.postgresql.org/docs/18/runtime-config-wal.html#GUC-WAL-SUMMARY-KEEP-TIME): `summarize_wal` defaults off; `wal_summary_keep_time` defaults to 10 days. Retain summaries comfortably longer than the age of the full used as reference. Remote archived WAL is not a substitute for those local summaries. The owner subsequently rejected full fallback: invalid differential prerequisites must fail visibly, with per-type failure/freshness metrics, rather than unexpectedly consume full-backup storage.

Checksum-state changes require a new full; standby restartpoints/promotion require separate validation. No source-only conclusion is a successful PG18/CNPG recovery test.

## pgBackRest comparison

Official [full retention](https://pgbackrest.org/configuration.html#section-repository/option-repo-retention-full) and [full retention type](https://pgbackrest.org/configuration.html#section-repository/option-repo-retention-full-type):

- Count is the default mode. With N retained full backups, expiration needs N+1 successful full backups; failed attempts do not earn deletion of a good root.
- Time mode is in days and retains an anchor to cover the period. For 30-day retention with full backups aged 25 and 35 days, both remain. This is not age-only object deletion.
- Expiring a full expires its associated dependent backups.

[Separate differential retention](https://pgbackrest.org/configuration.html#section-repository/option-repo-retention-diff) counts full plus differential backups; expiring a differential also expires its dependent incrementals. Without that setting, differentials remain until their parent full expires.

[Archive retention](https://pgbackrest.org/configuration.html#section-repository/option-repo-retention-archive) and [archive retention type](https://pgbackrest.org/configuration.html#section-repository/option-repo-retention-archive-type) conservatively follow backup retention by default. Time mode ordinarily removes WAL preceding the oldest retained full. WAL needed to make a retained backup consistent is always retained, but aggressive independent archive settings can still remove *later* WAL required for PITR from that backup. The docs explicitly discourage such aggressive settings.

**Design implication, not a superiority claim:** the proposed recovery window and dependency preservation substantially overlap pgBackRest's time-based approach. An independent minimum-full floor alongside a time window is an extra safety policy, not proof of better backups. Favor the small window/floor interface and conservative coupled WAL retention rather than reproduce every independent expiration knob. Actual promised recovery coverage still needs valid backups, continuous WAL/timelines and recovery tests.

## A global deletion lock needs an admission protocol

The owner's requested global scope is **one repository**, not every repository in a shared bucket. Multiple restores may hold protection concurrently; deletion is blocked until every relevant hold is safely released. Backup and WAL uploads need not stop merely because retention is paused, subject to the finalized mutation protocol.

[Kubernetes Leases](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/lease-v1/) are namespaced resources in one Kubernetes API. Same-name objects in independent clusters are not a shared lock. [client-go leader election](https://github.com/kubernetes/client-go/blob/master/tools/leaderelection/leaderelection.go) explicitly does not provide fencing; lease expiry alone cannot stop an old actor's effects.

A marker alone is unsafe even with strong object visibility:

1. Retention lists holds and sees none.
2. Restore writes a hold and starts reading.
3. Retention issues the DELETE it already authorized.

A safe protocol must acknowledge admission **after** new destructive work is stopped and existing deletes/retries/commit removal have drained. HTTP timeout/cancellation is not proof a remote delete cannot still complete. Restore must wait for that acknowledgment before selecting/downloading a backup; uncertain deletion outcomes block admission.

[CNPG recovery](https://cloudnative-pg.io/documentation/current/recovery/#how-recovery-works-under-the-hood) starts PostgreSQL after the base-restore Job; it fetches WAL through `restore_command`. [PostgreSQL PITR](https://www.postgresql.org/docs/18/continuous-archiving.html#BACKUP-PITR-RECOVERY) can pause. Protection therefore extends through the last source-dependent WAL use, restarts and paused recovery—not only the bootstrap Job. Release requires completed recovery or confirmed cancellation with no unprotected restarted reader. Timeout alone is not that evidence.

**Candidate, not approved implementation:** durable restore requests and acknowledgments under the existing single deletion authority, potentially transported through unique S3 coordination keys. This is a deletion-admission protocol, not a general distributed lock service. It requires defined consistency, authority/fencing, permissions, recovery from lost source control-plane state and durable completion/abort semantics before READY. SigV2 authentication or SDK availability supplies none of those guarantees by itself. AWS's [per-key consistency documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel) is not evidence of transactional updates across keys or identical semantics on every S3-compatible endpoint.

Truly read-only clients cannot register a request without a writable coordination channel. They need an already-acknowledged pause or a verified non-pruned recovery source. Whether managed restores may write a narrowly scoped coordination prefix is an owner choice still pending. Source-cluster-loss recovery must remain possible under an explicit safe contract, not become silently dependent on an unavailable original Kubernetes API.

On uncertain/crashed recovery, a durable hold favors recoverability over timely expiration. Emit a rate-limited Kubernetes Warning event (candidate reason `RetentionBlocked`) plus durable status/metrics. [Events](https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/) are best-effort observations, never authoritative lock state. The owner approved fail-closed protection with Warning events and explicitly requested a small, non-overengineered mechanism. The concrete acknowledgment/cleanup protocol and cross-cluster/read-only scope remain to be specified before READY.

For comparison only, pgBackRest's moving [command configuration](https://github.com/pgbackrest/pgbackrest/blob/main/build/config.yaml) selects different lock types for restore and expire; its [lock naming](https://github.com/pgbackrest/pgbackrest/blob/main/src/command/lock.c) and [filesystem flock implementation](https://github.com/pgbackrest/pgbackrest/blob/main/src/common/lock.c) do not establish global cross-cluster S3 reader protection. These are unpinned source observations, not a certified behavior matrix. No Dell endpoint experiment was needed or performed.
