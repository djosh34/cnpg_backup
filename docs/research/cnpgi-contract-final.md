# CNPG contract: final scoped technical findings

Effort: `cnpg-finalize-20260907`. Scope: decisions #4, #8 and the CNPG portions of #10/#12. This is a **source-backed design resolution proposal for the parent**, not product implementation, qualification, a GitHub resolution, or READY. All owner Q1–Q12 answers remain unchanged. No agents were spawned; no GitHub objects were changed. Release policy belongs to the parent: v0.1.0, Linux amd64, GitHub release OCI archives plus two GHCR images, verified OIDC publication.

## 1. Freeze the real compatibility set

| Component | Initial pin and verified evidence |
| --- | --- |
| OS/architecture | Linux amd64 only; no implied arm64, Windows, older PostgreSQL or alternate architecture restores. |
| CNPG | **v1.30.0**, commit `4b5e244a7d031f67e025c83c1555e7726ecbbfa1`; GitHub release published 2026-06-29, not prerelease. |
| Plugin CNPG-I dependency | **v0.6.0**, commit `844d20b2b783a4804705b4abcb767beee6abf893`; release published 2026-07-20, not prerelease. |
| Actual operator protocol dependency | CNPG v1.30.0 imports **v0.5.0**, commit `5f6976f7c0bb7330652099f4a2ecd27f79c8ce28`, NOT v0.6.0. Use the backwards-compatible wire subset and the concrete adaptations below. |
| Kubernetes | **v1.35.8**, commit `1c2e10a409eb1b03f2f28f401ce935312e20d9fb`; release published 2026-08-20. CNPG's pinned support table includes 1.35 and PG18. Native restartable init sidecars are available on this version. Do not substitute 1.30 merely because sidecars were beta there. |
| PostgreSQL and native utilities | **18.6**, source `REL_18_6`, commit `724edf9bde9d356724ad384a2e196edc3c9f80f7`; pinned release notes say 2026-08-13. Match server/tools minor for initial qualification: `pg_basebackup`, `pg_combinebackup`, `pg_verifybackup`, `pg_waldump`, `pg_controldata`, and justified `psql` for preflight. |
| Manager certificate provisioner | **cert-manager v1.21.1**, commit `24e33194fb39488eff2bbf10c6dc640f407cad44`; release published 2026-07-29. Install declarative Certificates/namespace Issuer, not a product certificate controller. |

These releases/tags were actually retrieved. Container digests/package locks must be resolved by the parent's packaging work and recorded before artifact qualification; source pins are not invented image digests. The PG experiment used an existing unpacked PGDG Ubuntu package, **18.6-3.pgdg24.04+1**, read-only. Its package build is experimental evidence, not selection of a production base image.

Primary evidence: [CNPG go.mod L8–18](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/go.mod#L8-L18), [CNPG support table L90–100](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/docs/src/supported_releases.md#L90-L100), [PG release date L4–10](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/release-18.sgml#L4-L10), release records [CNPG](https://github.com/cloudnative-pg/cloudnative-pg/releases/tag/v1.30.0), [CNPG-I](https://github.com/cloudnative-pg/cnpg-i/releases/tag/v0.6.0), [Kubernetes](https://github.com/kubernetes/kubernetes/releases/tag/v1.35.8), [cert-manager](https://github.com/cert-manager/cert-manager/releases/tag/v1.21.1).

No deployed owner Kubernetes/CNPG manifests were supplied. This is a chosen initial qualification set, not a claim about their deployment. Newer patches require qualification, not an unbounded compatibility promise.

## 2. The critical WAL exit-status contract — concrete workaround

### What the released operator actually does

1. `client.innerRestoreWAL` invokes each capable plugin, appends **every** returned gRPC error to a multi-error, and returns `(false, error)` if none succeed. It does not classify `NotFound`, `Unavailable`, `PermissionDenied`, or `DataLoss`.
2. `walrestore.run` logs that error and falls through to legacy Barman configuration. With a plugin-only Cluster, `GetRecoverConfiguration` returns `ErrNoBackupConfigured`.
3. Cobra returns an error; **`cmd/manager/main.go` exits 1 for any Execute error**.
4. PostgreSQL interprets an ordinary unsuccessful `restore_command` as possible end-of-archive. For latest recovery, once consistent, this can promote an incomplete recovery. A different gRPC code alone does NOT fix it.

Exact sources: [CNPG WAL client L88–163](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/wal.go#L88-L163), [walrestore.run L110–167](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/walrestore/cmd.go#L110-L167), [GetRecoverConfiguration L330–383](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/walrestore/cmd.go#L330-L383), [main L45–74](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/cmd/manager/main.go#L45-L74), [PG RestoreArchivedFile L240–280](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogarchive.c#L240-L280).

### Required implementation, not a future protocol question

Use the **existing `RestoreResponse.restore_config` escape hatch** for primary recovery Jobs. Return a command that invokes the project's CGO-free Go executable directly, not `/controller/manager wal-restore`:

```text
restore_command = 'exec /cnpg-backup/bin/cnpg-backup wal-fetch --plan /cnpg-backup/state/recovery.json -- "%f" "%p"'
recovery_target_action = 'promote'
recovery_target_timeline = '<resolved numeric source timeline>'
```

`wal-fetch` is a narrow client of the **existing CNPG-I WAL.Restore RPC over the existing Unix socket**. No new RPC, daemon, network service, or object-byte proxy in the manager. It reads the nonsecret local recovery plan, supplies the exact `cluster_definition`, requested WAL name/destination and opaque `parameters.recoveryID`, and calls `/plugins/cnpg-backup.djosh34.github.io`. The sidecar binds that ID to its admitted, immutable source plan. `Mode=MODE_RECOVERY` may be sent because this client is ours; the operator's old requests still omit it.

| Sidecar/helper result | Go helper process exit | PostgreSQL consequence |
| --- | --- | --- |
| Verified decompression/checksum/size and atomic durable destination publication | **0** | File usable. |
| Authenticated, correctly scoped S3 `NoSuchKey` for an allowed absent history/future-tail file, not a planned required file | **1** | Ordinary archive miss; PostgreSQL may finish latest recovery. |
| Missing a WAL/history file required by the admitted plan; checksum, gzip or length corruption | **255** | Fatal recovery error, not end-of-archive. |
| S3 timeout/5xx, DNS, TLS, credentials, 403, missing bucket, incomplete LIST, I/O error, gRPC unavailable/deadline/cancellation, invalid plan, unexpected server status | **255** after bounded retries | Fatal recovery error, not end-of-archive. |

All Go helper failure paths, including argument/plan parsing and dialing, must explicitly use 255; do not route them through a generic Cobra `os.Exit(1)`. Panic/default exit 2 is also unsafe: top-level helper recovery converts an unexpected panic to a redacted error and 255. Signal/command-not-found exits are already fatal in PG on Linux. For a known absence the server uses gRPC `NotFound`; other errors use `Unavailable`/`DeadlineExceeded`, `PermissionDenied`, `DataLoss`, `FailedPrecondition` or `InvalidArgument` as appropriate. **The helper maps all non-NotFound errors to 255.** No SDK error becomes absence merely because HTTP status is 404: a missing bucket, ambiguous endpoint, or malformed XML is not `NoSuchKey`.

PG's `wait_result_is_any_signal(exit_status, true)` treats shell exit values **>125** as fatal; `RestoreArchivedFile` calls that function with `true`. Thus 255 is not convention by analogy to Barman; it follows exact pinned PostgreSQL code: [wait_error.c L111–131](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/common/wait_error.c#L111-L131).

Retry bounds: reserve WAL capacity separately from backup transfers; use a finite per-request retry budget (initial profile 60 seconds) and helper RPC deadline longer than that budget (75 seconds). Exhaustion is 255. A subsequent Job retry may recover after the outage; never let an outage silently relax durability.

### Helper placement, startup ordering and quoting

The lifecycle patch adds **two separate disk-backed emptyDir volumes**, `cnpg-backup-bin` and `cnpg-backup-state`, beside the socket volume. Mount `/cnpg-backup/bin` read-only in the CNPG recovery main container and read-write in the restartable init sidecar. Mount `/cnpg-backup/state` read-only in main and read-write in sidecar. Do NOT place the executable or plan inside `/plugins`: CNPG scans every entry there as a plugin socket.

Append the restartable init sidecar after CNPG's bootstrap init container, before the recovery main container starts. At sidecar startup, copy its own exact-version static executable to a temporary file on `cnpg-backup-bin`, fsync, chmod 0555, rename to `cnpg-backup`, fsync the directory; then bind the socket. The sidecar startup probe succeeds only once executable installation and socket service are ready. Kubernetes gates main startup on that probe. The probe MUST NOT require a running PostgreSQL server or a completed base restore: that would deadlock bootstrap. On socket discovery failure return a gRPC error, not merely Identity.Probe `ready=false` (the pinned `connection.Ping` ignores the response boolean).

Before returning `RestoreResponse`, persist `/cnpg-backup/state/recovery.json` atomically with restricted permissions. It contains source repository identity, immutable target/backup selection, Cluster JSON and hold identity, **never S3 credentials**. Secret files are mounted only in the sidecar. The helper has no S3 access and receives no credential argv/env.

The command has a constant absolute executable/plan path; `%f` and `%p` are each double-quoted shell words. Restrict supported PostgreSQL/CNPG generated filenames and destinations to the pinned WAL paths and alphabet (no quote, dollar, backtick, newline or path traversal); additionally validate in the helper and sidecar before opening anything. This restriction is necessary because PostgreSQL substitutes `%p` before the shell parses it; shell quoting is not a license to accept arbitrary paths. Render the entire command as a PostgreSQL configuration literal with correct escaping separately from shell quoting. Do not interpolate endpoint, namespace, secret, repository name, target name or other user input into this shell command.

Source for placement opportunity: [CNPG CreatePrimaryJob L328–420](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/specs/jobs.go#L328-L420), [lifecycle JSON patch application L125–190](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/lifecycle.go#L125-L190), [socket directory L79–95](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/configuration/configuration.go#L79-L95), [directory scan L194–231](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/repository/setup.go#L194-L231), [Probe caller L432–438](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/connection/connection.go#L432-L438). Kubernetes [sidecar startup ordering](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/#sidecar-containers-and-pod-lifecycle) supplies the container sequencing contract; qualify it on the exact pinned cluster.

### Actual negative-control experiment

Executed [`experiments/cnpgi-wal-exit.py`](experiments/cnpgi-wal-exit.py) against read-only native researcher binaries. It initializes private Unix-socket-only PG18.6 data, creates sentinel 1, takes a native base backup, preserves completed base WAL, commits sentinel 2 in later WAL, then injects a WAL transport outage for that later segment. Both callbacks return 1 for truly missing `.history`; only the WAL error status differs.

```sh
cd /home/starlord/cnpg-run/cnpgi
PG_BIN=/tmp/cnpg-native-research/pgroot/usr/lib/postgresql/18/bin \
PG_SHARE=/tmp/cnpg-native-research/pgroot/usr/share/postgresql/18 \
LD_LIBRARY_PATH=/tmp/cnpg-native-research/pgroot/usr/lib/x86_64-linux-gnu \
python3 /home/starlord/cnpg-run/cnpgi/docs/research/experiments/cnpgi-wal-exit.py
```

**PASS**, artifacts `/tmp/cnpgi-exit-z0khm03y`:

```json
{
  "version": "postgres (PostgreSQL) 18.6 (Ubuntu 18.6-3.pgdg24.04+1)",
  "base_wals": ["000000010000000000000002"],
  "source_rows": "1,2",
  "ordinary_exit_1": "PROMOTED; row 2 lost despite existing on source",
  "fatal_exit_255": "startup FAILED; no promotion; FATAL exit 255"
}
```

The successful ordinary-error restore was queried: `pg_is_in_recovery() = false`, sentinel rows only `1`. The 255 case failed startup, logged FATAL/255, and did not select a new timeline. All servers were stopped in `finally`. This is a real PG negative control proving the exit-status consequence. It does **not** claim that a product Go helper, CNPG lifecycle injection, mTLS or real S3 fault path has run. Those exact integrations must exercise this same oracle during implementation. The same source experiment also passed publicly in [hosted run 34070742412](https://github.com/djosh34/cnpg_backup/actions/runs/34070742412), research commit `cdba7a0`, using the hash-pinned rootless PG18.6 tools.

## 3. Recovery lifecycle and deletion protection

**Correction to the previous research:** for this pinned CNPG's normal primary recovery, PostgreSQL replay happens **inside the recovery Job**, not first in the ordinary instance Pod after a download-only Job.

`Restore` calls the plugin; the plugin materializes and verifies PGDATA/tablespaces/WAL and returns configuration. `concludeRestore` generates configuration and calls `ConfigureInstanceAfterRestore`. That starts PostgreSQL with `WithActiveInstance`, polls **`SELECT pg_catalog.pg_is_in_recovery()` until false**, stops that instance, writes normal replication override configuration and optionally starts/stops it again to configure the application. The ordinary override replaces `restore_command` with CNPG's local command. Sources: [Restore/concludeRestore L180–350](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L180-L350), [ConfigureInstanceAfterRestore L909–962](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L909-L962), [waitUntilRecoveryFinishes L1030–1059](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L1030-L1059), [WithActiveInstance L818–875](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/instance.go#L818-L875), [normal override L268–296](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/configuration.go#L268-L296).

Freeze these boundaries for the storage researcher's admission/release protocol:

1. **Before selection/listing/downloading**, obtain an acknowledged source-repository deletion hold. The acknowledgment must drain already authorized deletion; an unacknowledged marker is not admission. S3 coordination permissions are required even when payload access is read-only. Source Kubernetes must not be required.
2. Use target Cluster UID plus immutable bootstrap fingerprint as the stable recovery operation identity, shared across retry Job/Pod UIDs. Record source/target repository IDs and selected chain durably. A retry must reuse its admitted plan or explicitly acquire a new hold before reading; never silently change selection after partial materialization.
3. Keep protection after the plugin's Restore RPC returns, throughout all helper WAL reads, transient failures, target replay and pauses. Returning the base-restore response is NOT release evidence.
4. The target manager may record completion and release **only the stable lifecycle holder**, never another incarnation's process-reader holder, after the **matching recovery Job is Complete and all its Pods/containers are terminated**, including retry Pods, with its main command successful and the immutable operation identity matching. CNPG's completed recovery check plus total reader termination is the required boundary. Do not rely only on Job main exit: `WithActiveInstance` logs shutdown errors rather than necessarily propagating them. Job termination removes that ambiguity. No job TTL cleanup should precede durable completion observation; if the Job disappears before evidence is recorded, retain the hold.
5. Ordinary instance mode reads WAL only from the **destination** repository, even though the Cluster retains an `externalClusters` source declaration. Only recovery-job mode with the admitted recovery ID may read the source. This ensures ordinary restarts, standby joins, rewind and promotion cannot resume unprotected source reads after release.
6. Every late/retried source request verifies admission for its operation; a released/completed operation cannot reopen access by reusing an old token. A new materialization attempt must re-enter admission before reads. Durable completion prevents a restarted manager from treating old Job state as a new unprotected reader.
7. Missing API evidence, deleted target Kubernetes, controller/Pod crash, force-deleted/uncertain Pod termination, source coordination failure or paused replay means **keep deletion paused without expiry**, emit rate-limited `RetentionBlocked` Warning and status. A timer, Lease expiry, Cluster readiness or a successful `pg_combinebackup` is not sufficient evidence.

CNPG `RecoveryTarget` has no target-action field. **Initial supported action is promote**, supplied in RestoreResponse; do not advertise a pause/shutdown API CNPG does not expose. Tests must still inject paused recovery and prove the hold never expires; the manager cannot infer completion from elapsed time. Intentional out-of-band pause leaves the Job incomplete until resumed; cancellation with uncertain termination leaves protection in place. Replica-cluster/streaming bootstrap is outside this primary-recovery contract: `concludeRestore` has an early return for replica clusters, so it must be rejected rather than inherit this completion rule accidentally.

The parent/storage researcher must reconcile the storage admission protocol with these exact begin/end events. This document does not replace or invent its global deletion fencing algorithm.

## 4. Minimal topology, lifecycle and mounts

Choose two released images, one Go executable with manager, instance, recovery-job and `wal-fetch` modes:

- **Manager Deployment:** one replica, Recreate strategy, in the **same namespace as CNPG's operator**. TCP gRPC 9090 with mandatory TLS 1.3/mTLS. No backup/WAL byte traffic. Namespace-allowlisted config reconciliation, lifecycle mutation, status/metrics and the selected retention control work. Its availability gates new reconciliation/validation, not an existing Pod's archive callbacks. One replica is an operational limit, **not a storage fencing primitive**.
- **All ordinary database Pods:** restartable init sidecar, including standbys, ready before the main container. Base backup is primary-only; WAL service remains present for failover and old-primary rewind/ready-WAL flushing. Never inject only on current primary.
- **Bootstrap Jobs:** inject the same socket-ready sidecar in supported initdb/join Jobs as well as recovery Jobs when advertised as job-injected. Non-recovery modes must not demand source credentials or touch source storage. Recovery mode owns materialization and source WAL service through Job termination. No separate scheduler or product Restore Job.
- **Capabilities:** manager Identity + Operator validate-create/change + Lifecycle + instance/job-sidecar-injection flags; concrete lifecycle hooks for core Pod and batch Job creation, and Pod evaluate/update/patch needed for CNPG-controlled rollout. Sidecars Identity + Backup + WAL Archive/Restore; recovery sidecars additionally RestoreJobHooks. Never advertise a service with an absent implementation. Metrics can be served at the manager's Prometheus endpoint without advertising a duplicate CNPG metrics service.

Use **`cnpg-backup.djosh34.github.io`** as plugin Identity name and socket filename, `cnpg-backup` as sidecar/Service/Deployment name. Metadata version is the actual release, with `license = All rights reserved` and a repository notice URL, not an Apache project license. Names are proposed concrete project-owned namespace strings, not registration of upstream `cloudnative-pg.io` ownership.

There are two misleading prose examples upstream: the discovery label is **`cnpg.io/pluginName`**, not `cnpg.io/plugin`; the default Unix directory is **`/plugins`**, not `/plugin`. Follow source. Example Service metadata:

```yaml
metadata:
  name: cnpg-backup
  namespace: cnpg-system
  labels:
    cnpg.io/pluginName: cnpg-backup.djosh34.github.io
  annotations:
    cnpg.io/pluginPort: "9090"
    cnpg.io/pluginClientSecret: cnpg-backup-client-tls
    cnpg.io/pluginServerSecret: cnpg-backup-server-tls
    cnpg.io/pluginServerName: cnpg-backup.cnpg-system.svc
```

Source: [plugin controller L100–270](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/plugin_controller.go#L100-L270), [label constant L148–150](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/utils/labels_annotations.go#L148-L150), [instance/job capability selection L93–140](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/cluster_funcs.go#L93-L140).

### Volume contract

Patch **the actual generated mount names and paths**, not a guessed PVC convention:

| Data | Pinned CNPG path/mount | Sidecar requirement |
| --- | --- | --- |
| PGDATA | `pgdata` at `/var/lib/postgresql/data`; PGDATA `/var/lib/postgresql/data/pgdata` | Read-write for atomic WAL restore/rewind destinations; recovery writes verified restored data. Never stage a backup inside live PGDATA. |
| Optional separate WAL | `pg-wal` at `/var/lib/postgresql/wal`; actual WAL `/var/lib/postgresql/wal/pg_wal` | Mount when present; same paths as main, writable for restore. Handle PGDATA/pg_wal symlink deliberately, not by generic unsafe tar extraction. |
| Tablespaces | `tbs-<normalized-name>` at `/var/lib/postgresql/tablespaces/<name>`; data under `/data` | Preserve **all** CNPG managed tablespaces. Regular backup need not read their live files because pg_basebackup owns capture; recovery requires writable target mounts and explicit OID/name/path mapping. |
| Local scratch/runtime | CNPG `scratch-data` at `/run` and `/controller` as applicable | Share only needed runtime/cert/socket paths; don't duplicate all mounts mechanically. PostgreSQL socket is not the CNPG-I socket. |
| Plugin socket | own `plugins` emptyDir at `/plugins` in both main and sidecar | Socket directory 0700, socket 0600 with matching CNPG UID/GID; no unrelated files here. |
| Workspace | own disk-backed volume at `/cnpg-backup/work` | Generic ephemeral PVC with explicit storageClass/size and verified finite backing, independently provisioned per Pod/Job. Finalization rejects ordinary emptyDir.sizeLimit as a hard database-workspace limit. Never one RWO workspace claim shared across all replicas. No memory-backed database workspace. |
| Credential/config projections | own read-only projected volumes in sidecar | Only referenced S3 credential keys/CA, native replication cert/key/server CA when needed, and generated nonsecret config. Not the PostgreSQL server private key or application/superuser secret. |

Native recovery must move/place `pg_wal` onto the optional WAL volume and create the correct symlink itself. CNPG's `restoreCustomWalDir` is called only on the **non-plugin branch** of `Restore`; assuming CNPG relocates plugin data loses WAL-volume support. Similarly the plugin must implement tablespace materialization, exact native-tool remapping and final verification, not assume CNPG extracts it.

Sources: [volume names/paths L35–88](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/specs/volumes.go#L35-L88), [generated mounts L237–299](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/specs/volumes.go#L237-L299), [restore branch/relocation L291–437](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L291-L437).

For comparison only, Barman v0.15.0, commit `f94016fcff43d780a308a2da435a84bc2da97504`, uses restartable init sidecars and copies main-container mounts. We inspected [lifecycle L452–603](https://github.com/cloudnative-pg/plugin-barman-cloud/blob/f94016fcff43d780a308a2da435a84bc2da97504/internal/cnpgi/operator/lifecycle.go#L452-L603); this is not a dependency or template to copy wholesale.

### Failover, rolling update and bootstrap behavior

Lifecycle mutation must be deterministic/idempotent and operate on owned fields only. Reject collisions rather than replacing user containers. Support CNPG's Pod EVALUATE hook so an image/config projection change leads to CNPG's normal rolling policy; don't directly delete live database Pods. Immutable Jobs retain their exact image/config snapshot; a retry resolves the original recovery operation. Upgrade manager first, then request CNPG-controlled sidecar rollout with both wire versions compatible. Do not claim in-flight backups survive instance-manager restarts: CNPG can mark them failed. Existing sidecars must continue WAL traffic while the manager is unavailable. Startup/liveness must not restart a healthy sidecar merely because S3 is down; expose storage readiness separately and fail callbacks honestly.

## 5. Native replication authentication: yes, with the correct name

**`cnpg_streaming_replica` is the pg_ident map, not the PostgreSQL role name.** Use the CNPG replication client certificate for **`streaming_replica`**. CNPG provisions this role with LOGIN/REPLICATION, generates a client certificate for it, and installs:

```text
hostssl postgres streaming_replica all cert map=cnpg_streaming_replica
hostssl replication streaming_replica all cert map=cnpg_streaming_replica
cnpg_streaming_replica streaming_replica streaming_replica
```

This is sufficient for native physical `pg_basebackup`, streamed WAL and temporary physical slots; no superuser password is necessary. It is already a highly privileged CNPG role (CNPG also grants pg_rewind file-reading functions), not a claim of novel least-privilege isolation.

Use projected `Cluster.spec.certificates.replicationTLSSecret` or CNPG's resolved default replication Secret, plus resolved server CA. Copy one consistent certificate/key snapshot to private mode-0600 files for the native command, removing it afterwards. Connect to **this Pod** using libpq `host=<cluster>-rw.<namespace>.svc hostaddr=127.0.0.1 sslmode=verify-full`, not a floating Service IP. The DNS host supplies SAN verification; hostaddr pins the local capture endpoint so failover cannot redirect the second WAL connection to another server. Respect actual PG port. Reject custom certificates without the required SAN rather than downgrading TLS. Pass paths via a protected libpq service file/environment, never key/password material in argv. Do not use the inherited `PGHOST=/controller` Unix socket for certificate authentication.

Before and after capture, verify local role/primary state, PG18.6, timeline/system ID, checksum mode, `summarize_wal`, summary coverage requirements and workspace capacity. `psql` with this same certificate is a justified native preflight tool for public queries such as `pg_is_in_recovery()` and settings; use `pg_controldata` for local control data, not an assumed privilege to call all SQL control functions. Explicitly request `target: primary` in both Backup and ScheduledBackup; empty target selects a standby if available. Promotion/demotion or timeline change during capture fails the requested operation; a differential never launches a full fallback.

Sources: [HBA/ident L100–145](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/postgres/configuration.go#L100-L145), [role setup L217–253](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cmd/manager/instance/run/lifecycle/run.go#L217-L253), [replication certificate L273–302](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/cluster_pki.go#L273-L302), [server SAN construction L1141–1167](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/cluster_funcs.go#L1141-L1167), [PG replication privilege check L950–965](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/utils/init/postinit.c#L950-L965), [CNPG target selection L876–928](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L876-L928).

This authentication choice is source-proven but has **not** been exercised with actual CNPG-issued certificates by this task. Require native full+differential integration using that certificate, verify-full and separate WAL/tablespaces; authentication failure must not trigger password/superuser fallback.

## 6. Backup invocation, retries, status and terminal metrics

Use CNPG `Backup`/`ScheduledBackup`, with method `plugin`, explicit primary target and opaque parameters containing **only `backupType: full|differential`**. Required `backupType` has no implicit full default; invalid/missing types fail visibly. Repository selection is Cluster configuration, not an overridable per-backup credential/endpoint map. A ScheduledBackup copies its plugin configuration into each Backup.

One synchronous `Backup` RPC covers one attempt. The instance manager launches it asynchronously relative to the operator HTTP request, but the plugin must not return success until durable immutable commit. Populate `backup_id` from Backup UID, actual native start/stop timestamps, begin/end WAL and LSN, online=true, label/map and metadata `{backupType, repositoryID, rootBackupID, formatVersion}`. Do not substitute request time or RPC return time for native boundaries. CNPG owns Backup status and ignores returned instance ID in favor of its own selected instance.

CNPG plugin backups remain **started**, not necessarily running, until completed/failed. An RPC error invokes `markBackupAsFailed`; the built-in event is **Normal**, not Warning. The controller explicitly detects instance-manager session changes and may fail a started backup. Failed/completed resources return before restarting execution. Thus **there is no protocol guarantee of automatically retrying a failed Backup**. A new requested attempt uses a new Backup UID; a schedule provides later attempts.

Implement same-UID deduplication nonetheless: coalesce concurrent calls, check authoritative commit first, return the same committed result after ambiguous response, and never overwrite differing content. If no commit and previous work was abandoned, cleanup/retry only under the repository's operation-ownership protocol while the CNPG resource is nonterminal. A failed terminal Backup is not silently re-executed or relabeled successful. Local retries inside one RPC do not create new attempts. Keep result/status-disagreement visible when the commit succeeds but Kubernetes or response delivery fails.

**Metrics owner is the single manager**, observing CNPG Backup phases, not every sidecar incrementing on each gRPC/S3 error:

- `cnpg_backup_failures_total{repository_id,namespace,cluster,backup_type}` increments once for each observed transition of a matching Backup UID into terminal `failed` during that process lifetime, including CNPG failure before RPC and instance-manager restart failures. Requeues, relists, duplicate errors, resyncs, S3 retries and repeated reads of the same terminal state do not increment. Deletion evicts observation state.
- Initial informer synchronization establishes a baseline: existing terminal failures are **not replayed** into the reset counter. Watch/relist deduplication uses informer UID/phase state. Failure transitions wholly missed across manager downtime may be absent; this is monitoring, not an exactly-once durable audit log. Ordinary counter-reset semantics apply. Never add a durable metrics ledger.
- `cnpg_backup_last_success_timestamp_seconds` is reconciled from **durable commits** per requested type, not status timestamps or process start time. The timestamp is the immutable S3 commit object's LastModified publication time (including preserved metadata of subsequently retired backups); native capture start/end remain separate metadata. A failed, uncommitted attempt never advances it. If a commit is durable but its CNPG result was lost, report the committed success timestamp **and** the failed CNPG attempt: these measure different facts. The parent must retain this distinction rather than require deleting/ignoring a valid backup to match failed status.
- Never-successful/unknown: omit last-success value and expose a bounded `cnpg_backup_success_history_known` gauge (0 on failed catalog reconciliation, 1 after a complete valid scan). A complete empty catalog is known with no success sample. No fabricated timestamp zero interpreted as an actual success.
- Emit rate-limited Warning `BackupFailed` on terminal failure with requested type and redacted reason, supplementing CNPG's Normal event. Unknown invalid type is validation failure, never a third metric-label type. Ship independent schedule-aware freshness/missing alerts for full and differential; repository optional max-age thresholds enable only scheduled types. WAL alerting is separate.

Sources: [Backup protobuf L68–139](https://github.com/cloudnative-pg/cnpg-i/blob/844d20b2b783a4804705b4abcb767beee6abf893/proto/backup.proto#L68-L139), [instance invocation/status L67–173](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/webserver/plugin_backup.go#L67-L173), [controller terminal guard L145–158](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L145-L158), [started backup recheck L269–281](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L269-L281), [restart failure L545–582](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/backup_controller.go#L545-L582), [schedule parameter copying L70–98](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/scheduledbackup_funcs.go#L70-L98).

## 7. WAL service: capability limits, guards and rewind

The v0.5→v0.6 protocol diff adds optional archive-empty fields and Restore mode **without changing the old fields**. CNPG v1.30.0 sets none of them and sends no WAL parameter map. Resolve ordinary requests from Cluster JSON. Never infer that v0.6's comment “operator always sets it” describes this older caller.

- **Archive:** acknowledge only durable publication or verified identical existing content. Fail conflicting content. Bound gzip, I/O/retries and concurrent uploads; no asynchronous acknowledgment queue. Preserve segment size and recognized history/backup-history names, not a 16-MiB-only assumption. Name parsing/extraction limits must be agreed with native/storage work.
- **Empty guard:** absence of the optional field means **safe default enabled**, not disabled. Establish durable writer ownership tied to target Cluster UID/repository lineage, checking that an unclaimed destination has no backup/WAL payload before first adoption. Same-owner initialization retries use that durable claim; do not require the archive to remain empty after legitimate first writes. Check source/destination separation before Restore RPC success and again on first archive. An existing foreign owner or ambiguous legacy payload fails closed. Reject the operator's skip-empty-check annotation in initial support; an optional false flag can never bypass writer identity/no-clobber. Exact atomic ownership publication belongs to the storage resolution.
- **Restore/rewind:** no prefetch, no negative cache and no adjacent-WAL assumptions in either mode. Fetch exactly the requested object to a private temporary file, verify then atomic publish. For MODE_UNSPECIFIED use recovery-compatible behavior. The old operator cannot identify rewind, but the no-prefetch/no-miss-cache algorithm is safe for both; explicit MODE_REWIND from a newer caller must take the identical exact-file path.
- **Ordinary instance limitation:** CNPG's own standby/rewind command still collapses gRPC errors to exit 1. Do not falsely claim otherwise. Initial plugin-managed **primary PITR/latest recovery uses the direct helper** and cannot suffer this truncation. Ordinary standby recovery remains in standby mode and may retry/fall back to streaming; `pg_rewind --restore-target-wal` treats failed retrieval as failure, not a successful latest-PITR promotion. Do not route an external source/latest bootstrap through that command or promise replica-cluster restore until separately qualified. CNPG controls any subsequent rebuild/rewind retry behavior.
- **Status and SetFirstRequired:** schema exists, but a source search found **no CNPG v1.30.0 call to either WALClient().Status or WALClient().SetFirstRequired**. Initial implementation does **not advertise** those optional capabilities; unknown calls return Unimplemented. Report archive inventory/retention status through Repository status/metrics. Do not make retention or automatic backup discoverability depend on a nonexistent caller. Future first-required hints can only strengthen, never weaken, repository dependencies/holds. First/last WAL are not gap-free coverage evidence.

Sources: [v0.6 WAL schema L40–120](https://github.com/cloudnative-pg/cnpg-i/blob/844d20b2b783a4804705b4abcb767beee6abf893/proto/wal.proto#L40-L120), [restore optional guard L53–76](https://github.com/cloudnative-pg/cnpg-i/blob/844d20b2b783a4804705b4abcb767beee6abf893/proto/restore_job.proto#L53-L76), [old Archive request L67–83](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/wal.go#L67-L83), [old Restore request L57–74](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/client/restore_job.go#L57-L74), [CNPG rewind L1232–1280](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/instance.go#L1232-L1280), [archiver routing L163–186](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/archiver/archiver.go#L163-L186).

RPO remains asynchronous: current unarchived WAL plus backlog. Keep the owner-approved healthy profile `archive_timeout=60s`; upload latency is additional and outages have no fixed bound. Bundled WAL proves base consistency, not post-backup PITR coverage. Parent native/storage work owns required archive coverage at commit and conservative retention; source recovery helper must prioritize the validated source archive where replay needs more than bundled coverage and classify missing planned-required files as fatal.

## 8. Exact backup/PITR target mapping

CNPG **appends** `RecoveryTarget.BuildPostgresOptions()` after plugin restore configuration. Therefore the plugin must not contradict it with a second target-setting API. Only numeric source timelines or an omitted timeline are supported initially; explicitly reject `targetTLI: current|latest` because CNPG would append and override our frozen numeric selection with a dynamic choice. Omission is supported: resolve the source's unambiguous latest descendant at admission and emit its numeric timeline in RestoreResponse. If history has multiple competing eligible leaves, require an explicit numeric timeline. Timeline IDs refer to the **source**, never the new target archive, and are not globally comparable outside their repository lineage.

Selection uses committed manifests, verified identity, complete parent closure and valid timeline history. Parse LSNs numerically. A backup on an ancestor timeline is eligible only when its stop point precedes that path's fork; backup data containing future changes cannot be undone by WAL. Differential reference must be a compatible full on the same capture timeline in the initial product. A timeline transition requires a new full for subsequent differentials, but full/previous differential + forward WAL replay across a valid history path remains supported.

| CNPG field | Selection rule / emitted PostgreSQL setting |
| --- | --- |
| No target, optional `backupID` | Latest archive recovery on resolved numeric source timeline. Without ID choose newest usable base on that path; with ID use exactly that base. Validate continuous known WAL coverage beyond bundled end through the observed archive frontier; a hole before that frontier is failure, not a tail. True future-tail misses may end recovery. Do not promise “latest transaction” or stop a live archive at an invented timestamp. |
| `backupID` | Exact repository commit ID, never Kubernetes object name. Missing/uncommitted/expired/wrong-lineage ID fails; it does not suppress time/LSN eligibility checks. Read no Kubernetes Backup catalog for selection. |
| `targetTime` | Choose newest eligible backup whose **conservative server-observed capture-completion timestamp is strictly earlier** than target, then let PG enforce `recovery_target_time`. Require RFC3339 with timezone; no client-local timezone guessing. Native worker must preserve capture completion separately from later S3 commit time. |
| `targetLSN` | Choose newest eligible base whose native end LSN is **strictly less** than target on the selected timeline path; emit `recovery_target_lsn`. This conservative inequality avoids boundary/inclusive ambiguity at backup consistency. |
| `targetName` | **Require explicit backupID** and a base known to precede the point. Emit `recovery_target_name`. PG stops at the first matching restore-point record after the base, not a globally unique name. Require unique application-created names in this supported workflow, valid UTF-8 <=63 bytes; no timestamp inference. Reject exclusive=true as inapplicable. |
| `targetXID` | **Require explicit backupID** and a base preceding that transaction's commit. Emit `recovery_target_xid`. Initial semantics cover an explicitly identified top-level committed transaction in the relevant WAL interval, 32-bit normal XID, with no wraparound ambiguity; no ordering by numeric XID, subtransaction-target promise or automatic base selection from XID. PostgreSQL itself can also recognize abort records; that is not the documented committed-data workflow. |
| `targetImmediate: true` | First consistent point of the selected exact backup; **require backupID**. Emit `recovery_target='immediate'`. No implication of latest data. Reject exclusive=true. |
| `exclusive` | CNPG maps true to `recovery_target_inclusive=false`, otherwise true; meaningful for time/LSN/XID, not names/immediate. |
| Multiple targets | Reject; CNPG target types are mutually exclusive. `backupID` and numeric `targetTLI` are selectors, not competing stop criteria. |
| Target not reached / missing required WAL | Let PostgreSQL fail recovery; **not** successful restore because files downloaded or base verified. Never clear a target to make startup pass. |

For time selection the native research must supply a conservative server clock observation after capture completion. Plugin wall-clock time is not interchangeable across hosts; if valid completion metadata is unavailable, require an explicit known-eligible base or fail selection, rather than choose a newer backup optimistically.

Sources: [CNPG RecoveryTarget schema L2205–2245](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/cluster_types.go#L2205-L2245), [BuildPostgresOptions L1522–1559](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/api/v1/cluster_funcs.go#L1522-L1559), [append order L618–626](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/pkg/management/postgres/restore.go#L618-L626), [PG target-end failure L1920–1932](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L1920-L1932), [PG stop-after name/LSN/XID L2750–2908](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlogrecovery.c#L2750-L2908).

## 9. Small namespaced configuration and security model

Choose **one namespaced CRD**, `Repository`, group/version **`backup.cnpg-backup.djosh34.github.io/v1alpha1`**, plus Secrets. Structural schema/defaults/CEL reject malformed configuration; no conversion webhook, custom Backup/Schedule/Restore CRD, ConfigMap-only JSON API or opaque user pod-template patches. Typed validation and observable status justify the one CRD. A generated ConfigMap is a read-only delivery projection of a Repository snapshot, not a second editable authority.

Concrete schema boundaries (canonical final field spellings are the storage §8 fields nested under Repository.spec, reconciled in design.md):

- `spec.repositoryID`: immutable UUID; `spec.s3.{endpoint,bucket,prefix,region,signature,addressing}`; signature enum v2/v4, no fallback; normalized HTTPS endpoint with no userinfo/query/fragment. Endpoint/bucket/prefix/identity immutable after use. Separate source and destination Repository objects, always same namespace as referencing Cluster; cross-**Kubernetes** restore uses a local source Repository pointing at S3, not cross-namespace Secret references.
- `spec.s3.accessKeySecret` and `spec.s3.secretKeySecret` use `{name,key}`; optional V4 `sessionTokenSecret` and public CA PEM `caConfigMap` use the same selectors. Only explicit same-namespace references, no ambient/anonymous credentials; `encryption: bucket-default` adds no SSE headers. No arbitrary SDK kwargs.
- `spec.retention`: optional window/minimum-full/interval/dryRun, absent means deletion disabled. No independent WAL expiration or hold TTL. The storage resolution owns operational authority fields.
- `spec.workspace`: generic ephemeral PVC `{storageClassName,size}` with actual finite backing capacity; the finalization rejects ordinary emptyDir.sizeLimit as a hard database-workspace bound. Pod resources are Kubernetes ResourceRequirements, with requests/limits and workspace capacity explicit. One backup per repository, small bounded transfer concurrency with reserved WAL capacity. Parent-reconciled limits: default **3 GiB data-path cgroup**, capture budget **Amax+Cmax+Wmax**, known-length compressed spool uploaded via Core multipart operations, **64 MiB manifest / 100,000 files / 64 tablespaces** caps, shared **operationTimeout=24h**, **captureTimeout=6h**. These operation limits do not replace the shorter per-WAL callback/helper failure deadlines.
- `spec.compression`: `none|gzip`, fixed gzip level1 as selected in the storage resolution. No compression plugin framework.
- `spec.backupFreshness`: optional independent full/differential max-age thresholds for schedule-aware alerts; not scheduling itself.
- `status`: observedGeneration, conditions Ready/Invalid/RetentionBlocked, last validated nonsecret configuration hash, known per-type success, archive inventory/coverage warnings and redacted operation diagnostics. S3 catalog remains recovery authority.

Cluster and Backup example (omitted standard storage/image fields remain required):

```yaml
# Cluster.spec
backup:
  target: primary
postgresql:
  parameters:
    archive_timeout: 60s
    summarize_wal: 'on'
    wal_summary_keep_time: 30d
plugins:
- name: cnpg-backup.djosh34.github.io
  isWALArchiver: true
  parameters:
    repository: destination
# On a newly recovered Cluster, additionally:
bootstrap:
  recovery:
    source: origin
    recoveryTarget:
      backupID: '<committed-source-backup-id>'
      targetLSN: '0/9000100'
      targetTLI: '1'
externalClusters:
- name: origin
  plugin:
    name: cnpg-backup.djosh34.github.io
    parameters:
      repository: source
---
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: requested-differential
spec:
  cluster:
    name: database
  method: plugin
  target: primary
  pluginConfiguration:
    name: cnpg-backup.djosh34.github.io
    parameters:
      backupType: differential
```

### Certificates, Secrets, reload and RBAC

- Cert-manager issues server/client leaves with server-auth/client-auth usages. Server SAN is exactly the Service annotation above. CNPG **builds its server trust pool from server Secret `tls.crt`, not `ca.crt`**, and presents client Secret tls.crt/tls.key. Manager requires/verifies the intended client chain/identity using its mounted trust bundle, never any arbitrary cluster workload certificate. No shared server/client key. See plugin controller L201–267 cited above.
- Configure projected volume mounts without subPath. Manager reloads complete valid leaf/key/trust snapshots; CNPG watches referenced Secrets and drops/recreates connection pools. Rotation may briefly fail validation/reconciliation, which fails closed; existing sidecar archiving continues. Leaf/private-CA rotation has explicit rolling overlap of trusted old/new CA bundles before switching leaves and removing old trust; no insecure verification fallback. Cert-manager handles issuance, but the package's rotation tests must check CNPG's leaf-trust behavior, not assume it reads ca.crt. [CNPG Secret watch L319–383](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/controller/plugin_controller.go#L319-L383), [connection replacement L177–192](https://github.com/cloudnative-pg/cloudnative-pg/blob/4b5e244a7d031f67e025c83c1555e7726ecbbfa1/internal/cnpi/plugin/repository/setup.go#L177-L192).
- Sidecars reload projected config/credentials/CA before new operations, validating into one immutable in-memory snapshot. Bad/missing updates fail new work rather than silently use stale settings indefinitely. An in-flight native backup holds its starting cert/key/config snapshot; revocation can fail it. WAL callbacks load current valid snapshots. Do not log snapshot contents or native environment. Config/resource/image or Secret-reference changes require normal CNPG rollout; Secret value rotation does not require a Pod restart.
- Manager watches only configured namespaces, not every Secret cluster-wide. Per-namespace Roles: get/list/watch Repository, Cluster, Backup, ScheduledBackup, Job and Pod; patch Repository/status; create/update owned config projections; create/patch Events. Read S3 Secrets through uncached **get**, with install-time resourceNames allowlist for configured Secret names, not list/watch Secret data. Manager installation changes that allowlist when adding references. No permission to exec into Pods, read app/superuser credentials, modify CNPG Backup status, delete data PVCs or bypass admission. No wildcard ClusterRole for Secret access.
- Sidecar receives S3/config/native-cert projections via kubelet and does not need repository Secret API get/list/watch. It shares CNPG's Pod service account but does not receive an extra broad product ClusterRole; any CNPG existing token permission is not a new plugin grant. The main recovery helper receives only socket, binary and nonsecret plan volumes.
- Run as CNPG's configured non-root UID/GID; read-only root filesystem, drop all capabilities, allowPrivilegeEscalation=false, RuntimeDefault seccomp. API/secret references must remain within the Cluster namespace; validation checks actual caller Cluster identity before returning Secret-mount patches. Config-map projections contain no credentials. Network policy allows manager operator mTLS, Kubernetes API and necessary storage-control egress; sidecars get only configured S3/DNS and required API/control traffic. No general data-path TCP service.

**KISS limits:** no CGO production Go, no Barman/Python/pgBackRest runtime, no production database deployment, no arbitrary user command injection or native-extension support claim. Native PG tools/libraries are the explicit exception and must be SBOM/scanned by the parent's release process. Reject unsupported PG majors, architecture, standby capture, replica-cluster bootstrap, unmanaged tablespaces/unsafe paths, unsupported WAL layout, multiple competing archivers, absent required resource limits, invalid source/target identity, and unconfigured restore-coordination rights before claiming a successful operation. Do not silently drop the owner-required CNPG tablespace/separate-WAL scope.

## 10. Checks performed and remaining qualification

Executed source clones/tag retrieval, GitHub release/annotated-tag resolution, source searches and v0.5.0→v0.6.0 protobuf diff under `/tmp/cnpgi-research`. Commands included:

```sh
cd /home/starlord/cnpg-run/cnpgi
# Clones already exist at these paths; pin checks are repeatable:
git -C /tmp/cnpgi-research/cloudnative-pg rev-parse HEAD
git -C /tmp/cnpgi-research/cnpg-i diff v0.5.0 v0.6.0 -- proto/wal.proto proto/restore_job.proto
gh api repos/cloudnative-pg/cloudnative-pg/releases/tags/v1.30.0
gh api repos/cloudnative-pg/cnpg-i/releases/tags/v0.6.0
rg -n 'WALClient\(\)\.(Status|SetFirstRequired)' /tmp/cnpgi-research/cloudnative-pg --glob '*.go'
```

The last search returns no matches. Eighteen stdlib-Python source assertions passed: actual dependency; optional field schema; absent old-caller guard/mode; error aggregation/fallthrough; plugin restore config/target append; Job recovery completion; ordinary override replacement; PG fatal status predicate and its use; unreachable-target failure; CNPG replication role/HBA; real discovery label/socket directory; absent Status/SetFirstRequired callers. Three Linux shell wait-status checks passed: exit 0 → raw 0/nonfatal; exit 1 → raw 256/nonfatal; exit 255 → raw 65280/fatal under pinned PG predicate. These are **source/OS contract checks**, not gRPC runtime compatibility tests. The separate real PG experiment above provides the distinguishing behavioral evidence.

Mandatory implementation regressions, not completed evidence:

1. Load v0.6 server through v0.5 client, old absent guards, capability negotiation, dynamic Service discovery/mTLS, leaf+CA rotation and manager outage.
2. Actual generated Pod/Job lifecycle patches with exact mount paths, restartable sidecar startup, helper executable before main, quoted `%f/%p`, same UID, no credentials in argv/logs and idempotent rollout evaluation.
3. Actual Go helper/gRPC path plus real MinIO fault: post-backup required WAL 5xx/TLS/auth/timeout must end in **255 and no promotion**; authenticated future miss exits 1; required missing/corrupt WAL fails. Keep the negative-control ordinary-exit test so an oracle cannot pass by never reaching post-backup replay.
4. PG18 native full/differential using CNPG replication certificate, local hostaddr+verify-full, failover during backup, native tablespaces and separate WAL recovery. Missing summaries never launch full fallback.
5. Backup phase/UID duplicate tests, commit-response-loss mismatch, terminal failure counting versus retries/resync/restart; independent freshness alerts.
6. SQL assertions for latest/time/LSN/named/XID/immediate and explicit ID; wrong lineage, newer base, fork boundary, missing history, branch ambiguity, target not reached and post-backup sentinel DROP recovery.
7. Holds throughout live/paused replay and retries; Job/Pod termination requirement; normal Pods cannot read source after release; source Kubernetes loss and target-manager loss never expire protection. Storage admission/deletion interleavings are owned by the storage resolution.

### Parent reconciliation items — no hidden protocol promises

- Replace the stale “CNPG v1.30 speaks v0.6 fields” assumption and generic WAL error-category prose with the exact old caller + helper exit contract. Helper binary injection is required scope, not optional optimization.
- Replace the stale download-only recovery Job model with pinned Job replay/completion. Feed this boundary into storage holds; retain uncertain holds forever.
- Reconcile numeric source timeline selection, conservative server completion timestamp and same-timeline differential policy with native metadata. Named/XID require explicit base; no invented discovery RPC.
- Distinguish durable committed-success freshness from terminal CNPG invocation failures after a lost response. This is essential when authoritative commits survive a failed Kubernetes result.
- Do not depend on WAL Status/SetFirstRequired being called. Use repository-owned retention/status.
- Freeze package image digests/certificate installation inputs with the parent's two-image release policy. This task selected real upstream source/version pins and proved the critical PG error behavior; it did not qualify product containers, Kubernetes, CNPG-issued replication certificates or MinIO end to end.
