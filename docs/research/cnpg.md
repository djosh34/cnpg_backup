# CNPG upstream protocol notes

These notes explain upstream behavior that constrains the plugin. See [architecture](../design.md) and [configuration](../configuration.md) for the plugin's behavior.

Source references use CNPG v1.30.0 at `4b5e244a7d031f67e025c83c1555e7726ecbbfa1`, CNPG-I v0.6.0 at `844d20b2b783a4804705b4abcb767beee6abf893`, and PostgreSQL 18.6 at `724edf9bde9d356724ad384a2e196edc3c9f80f7`.

## Wire compatibility

CNPG v1.30.0 imports CNPG-I v0.5.0. The v0.6.0 library adds optional archive-empty guards and WAL Restore mode without changing the old fields. The old operator omits those fields and WAL parameters. Their absence cannot authorize skipping repository ownership checks.

The pinned operator does not call WAL `Status` or `SetFirstRequired`. Retention cannot depend on those callbacks. Its `connection.Ping` ignores the Identity.Probe response boolean, so `ready=false` alone does not report a failed socket probe.

Sources:

- [Operator dependency](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/go.mod#L8-L18)
- [WAL schema](https://github.com/cloudnative-pg/cnpg-i/blob/844d20b2b783a4804705b4abcb767beee6abf893/proto/wal.proto#L40-L120) and [old WAL caller](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/wal.go)
- [Old restore-job request](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/restore_job.go#L57-L74)
- [Probe caller](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/connection/connection.go#L432-L438)

## WAL failures and archive order

CNPG aggregates every plugin WAL error without distinguishing gRPC `NotFound` from storage failure. Its `wal-restore` command falls through to legacy configuration and ultimately exits 1. PostgreSQL can interpret that ordinary failure as end-of-archive and promote latest recovery with missing data.

`RestoreResponse.restore_config` allows the plugin to supply its own `restore_command`. The direct Go `wal-fetch` helper uses the existing Unix WAL RPC. It returns 0 for a verified file, 1 only for an allowed authenticated absence, and 255 for required gaps or other failures. PostgreSQL's Linux wait-status handling treats shell exits above 125 as fatal.

PostgreSQL tries the archive before local `pg_wal`. An intact local bundle can therefore require exit 1 for a missing archive duplicate. Bundle coverage ends at the manifest End-LSN, not at the end of its final segment. PostgreSQL writes BACKUP_END before requesting SWITCH, so required later records can share that filename. A padded bundle cannot replace the full archive. Transport, authentication, and corruption failures remain fatal even when a bundle exists.

Sources and executable regressions:

- [CNPG WAL error aggregation](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/wal.go#L88-L163), [command fallback](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/walrestore/cmd.go#L110-L167), and [exit status](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/cmd/manager/main.go#L45-L74)
- [PG archive failure handling](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogarchive.c#L240-L280) and [fatal wait-status predicate](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/common/wait_error.c#L111-L131)
- [Archive-before-local order](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L4390-L4409) and [BACKUP_END before SWITCH](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L9300-L9317)
- [Exit-status negative control](experiments/cnpgi-wal-exit.py), [same-segment fixture](fixtures/README.md), and [latest-recovery endpoint oracle](wal-switch.md)

## Recovery Job and target ownership

Primary recovery replays PostgreSQL inside the CNPG recovery Job. After the plugin returns, CNPG starts PostgreSQL, waits for `pg_is_in_recovery()` to become false, stops it, and writes normal destination replication settings. `WithActiveInstance` can log shutdown errors without propagating them. The Restore RPC response or main command exit alone does not prove all source readers have terminated.

CNPG calls `EnsureTargetDirectoriesDoNotExist` before the Restore RPC. That preflight can rename or remove PGDATA and WAL. A lock acquired only inside the sidecar RPC starts too late. The plugin's main-container guard must precede CNPG's command and retain ownership through descendant termination and sidecar drain.

The plugin branch does not call CNPG's `restoreCustomWalDir`. The plugin must materialize tablespaces and relocate WAL itself. CNPG appends `RecoveryTarget.BuildPostgresOptions()` after plugin configuration, so explicit `current` or `latest` timeline strings would override a frozen numeric timeline. The RecoveryTarget API has no action field.

Sources:

- [Restore and completion](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L180-L350), [replay and shutdown](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L909-L1059), and [shutdown error handling](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/instance.go#L818-L875)
- [Pre-RPC preflight](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/instance/restore/restore.go#L72-L83) and [target mutation](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/initdb.go#L150-L194)
- [Recovery command generation](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/specs/jobs.go#L208-L224), [WAL relocation branch](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L291-L437), and [target option append order](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L618-L626)
- [Target schema](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/cluster_types.go#L2205-L2245) and [option generation](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/cluster_funcs.go#L1522-L1559)
- Linux [PID namespace lifetime](https://man7.org/linux/man-pages/man7/pid_namespaces.7.html), [kill scope](https://man7.org/linux/man-pages/man2/kill.2.html), and [flock lifetime](https://man7.org/linux/man-pages/man2/flock.2.html)

## Discovery, mounts, and certificates

The discovery label is `cnpg.io/pluginName`. Service annotations are `cnpg.io/pluginPort`, `cnpg.io/pluginClientSecret`, `cnpg.io/pluginServerSecret`, and `cnpg.io/pluginServerName`. The default Unix socket directory is `/plugins`, and CNPG scans every entry as a socket. Helpers and plans need separate volumes.

CNPG builds plugin server trust from the server Secret's `tls.crt`, not `ca.crt`, and presents the client Secret's `tls.crt` and `tls.key`. Secret updates recreate connection pools. CA rotation must account for that trust source.

Generated mounts are `pgdata` at `/var/lib/postgresql/data`, `pg-wal` at `/var/lib/postgresql/wal`, and `tbs-<normalized-name>` at `/var/lib/postgresql/tablespaces/<name>`. Actual data directories append `pgdata`, `pg_wal`, and `data`, respectively.

Sources:

- [Service discovery and TLS](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/plugin_controller.go#L100-L383)
- [Socket directory scan and connection replacement](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/repository/setup.go#L177-L231)
- [Generated volume names and mounts](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/specs/volumes.go)
- [Kubernetes restartable-sidecar startup ordering](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/#sidecar-containers-and-pod-lifecycle)

## Replication authentication and backup status

`streaming_replica` is the PostgreSQL LOGIN/REPLICATION role. `cnpg_streaming_replica` is its pg_ident map, not a role. Its replication certificate supports physical backup and temporary replication slots. Libpq `host` supplies the TLS hostname while `hostaddr=127.0.0.1` pins both backup connections to the selected local primary.

An empty Backup or ScheduledBackup target can select a standby. Primary-only capture requires explicit `target: primary`. CNPG copies schedule plugin parameters to each Backup and owns status. Plugin backups can remain `started` until completion. CNPG can mark a started backup failed after instance-manager restart, emits a Normal failure event, and does not automatically rerun terminal failed Backup resources. Durable commit success and a failed CNPG invocation can coexist after response loss.

Sources:

- [HBA and ident rules](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/postgres/configuration.go#L100-L145), [role creation](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/instance/run/lifecycle/run.go#L217-L253), and [replication certificate](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/cluster_pki.go#L273-L302)
- [Backup target selection](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L876-L928), [terminal guard](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L145-L158), and [restart failure](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L545-L582)
- [RPC result and failure event](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/webserver/plugin_backup.go#L67-L173) and [schedule parameter copying](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/scheduledbackup_funcs.go#L70-L98)
