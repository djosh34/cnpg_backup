# Architecture and recovery safety

`cnpg_backup` sends physical backups and WAL directly from CNPG instance sidecars to object storage. The manager handles Kubernetes configuration, lifecycle, monitoring, and retention. It never proxies backup bytes. [Configuration](configuration.md), [repository format](repository.md), and [operations](operations.md) describe the external interfaces.

## Supported runtime

The [v0.1.0 release matrix](releases/v0.1.0/README.md#compatibility) covers Linux amd64, PostgreSQL and tools 18.6, CNPG 1.30.0, Kubernetes 1.35.8, cert-manager 1.21.1, and MinIO. PostgreSQL uses standard 8 KiB blocks and 1 GiB relation segments. The plugin uses CNPG-I 0.6.0 with the operator's 0.5 wire subset.

Full and direct-full-parent differential capture run on the primary. Eligible instance Pods have sidecars so that WAL archiving continues after failover and during manager outages. Recovery runs in CNPG's primary recovery Job, including PostgreSQL replay. Separate WAL volumes and CNPG-managed tablespaces are supported. Standby capture, replica-cluster bootstrap, unmanaged layouts, and shared-writer recovery targets are outside this support matrix.

The Go executable is static and CGO-free. The manager image contains no native tools. The data image adds only `pg_basebackup`, `pg_verifybackup`, `pg_combinebackup`, `pg_waldump`, `pg_controldata`, `psql`, and their loader/library dependencies. There is no runtime shell, Python, Barman, pgBackRest, custom replication engine, or backup server.

## Module responsibilities

| Module | Responsibility |
|---|---|
| `internal/cnpgi` | CNPG hooks, Pod placement, configuration projections, restore observation, metrics |
| `internal/configuration` | Repository validation, complete credential snapshots, mounted-capacity checks |
| `internal/postgres` | Controlled native tools, capture, hostile-input verification, reconstruction |
| `internal/recoveryguard` | Target-volume ownership before CNPG starts, local admission and write drain |
| `internal/repository` | Identity, derived keys, publication, protected catalogs and recovery plans, destructive admission |
| `internal/wal` | One-file archive and verified atomic restore |
| `internal/retention` | Pure recovery-window planning and bounded repository execution |
| `internal/s3store` | Concrete SDK transport, conditional writes, multipart and bounded reads |

Simulation and fault scheduling belong in tests at these I/O boundaries. Production code owns its requests, subprocesses, temporary files, and errors.

## Lifecycle and isolation

The executable has `manager`, `instance`, `recovery-job`, `recovery-guard`, and `wal-fetch` modes. Restartable init sidecars appear in supported instance, initdb, join, and recovery workloads. The startup probe waits for the helper and Unix socket, not PostgreSQL or a completed restore. `/plugins` contains only sockets. Small helper and state volumes are separate from database-sized workspace PVCs, and only sidecars receive secret projections.

Fresh CREATE/EVALUATE templates undergo placement, volume identity, security, and version checks. PATCH/UPDATE callbacks authenticate existing Cluster-owned Pods and preserve their admitted specs, including their original configuration snapshot. CNPG compares desired templates and rolls Pods. The manager does not change a live Pod's data access when a Repository changes.

Manager RPCs require TLS 1.3 and the configured client identity. Each new RPC revalidates current trust, including on an existing connection. Invalid leaf, key, or trust updates reject new work. Data operations retain a complete credential and CA snapshot through drain, so rotation requires overlap.

## Full and differential capture

A capture first obtains repository deletion protection, validates the selected primary and physical layout, and reserves finite workspace. The Backup UID freezes the semantic request. A differential also freezes one eligible retained full, never a newer full on retry or another differential.

Native authentication uses CNPG's `streaming_replica` certificate. Libpq verifies the primary Service SAN with `host=<cluster>-rw.<namespace>.svc` while `hostaddr=127.0.0.1` pins the actual connection to the selected local primary. Private operation files and a sanitized environment prevent password, socket, or remote-Service fallback. `cnpg_streaming_replica` is an ident map, not the login role. Streamed WAL uses two replication connections and a temporary slot.

Capture uses tar format, streamed WAL, a spread checkpoint, and SHA256 manifests. A differential adds `--incremental` with the exact original full manifest. Missing summaries or parent, excessive reference age, or lost Pod/postmaster/timeline/checksum continuity fails the differential. There is no full fallback.

Preflight and postflight compare source identity, role, settings, tablespaces, timeline, checksum state, and postmaster start. A configuration reload during capture also fails conservatively. PostgreSQL's own timezone rules resolve native label timestamps within the source-clock capture interval, including DST ambiguity. Native capture completion remains distinct from later S3 publication time.

Go scans bounded archive structure and manifest entries before native verification. It preserves original inputs, stages gzip level 1 or raw known-length files, and computes raw and stored sizes and SHA256. Repository publication verifies remote artifacts and commits last. Cancellation kills and reaps native process groups. An incomplete capture never publishes a commit.

## Original verification and reconstruction

Every `pg_verifybackup` invocation uses `--no-parse-wal`. Go invokes matching `pg_waldump` directly for each accepted manifest WAL range, because the verifier's default WAL path would invoke a shell. The primary capture format accepts one range and rejects unsupported multi-range inputs rather than skipping them. Bundled WAL is checked separately because original manifest file checksums do not cover it.

Restore verifies every original full and differential before combination, both as archives and as extracted inputs. Synthetic-output verification alone is insufficient. `pg_combinebackup` can generate a valid new manifest over corrupted original bytes.

Go extraction rejects traversal, duplicate paths, archive-provided links, devices, sparse encodings, and size/count excess. Only the trusted layout builder creates tablespace links. Differential reconstruction uses ordinary-copy `pg_combinebackup F D`, with the selected differential's extracted tablespace mappings. Intervening differentials are absent. Synthetic WAL entries without file checksums are accepted only for the precise native `pg_wal/<segment>` form and still require direct WAL parsing and selected-bundle hashes.

After synthetic verification, restoration removes the historical `tablespace_map`, creates target-owned mappings, copies and verifies WAL onto its separate volume, fsyncs files and directories, and writes recovery configuration. PostgreSQL never starts on partially transformed data. Finite filesystem capacity is the hard stop for native writers. Polling, byte accounting, and reservations detect exhaustion but do not turn a PVC request into a quota.

## Target-volume ownership

CNPG's main command can rename or remove PGDATA and WAL before calling the plugin. `recovery-guard` therefore runs as PID 1 before that command, in its private PID namespace. It acquires advisory locks in PVC-UID order and fsyncs owner markers in private `.cnpg-backup` directories outside the data directories on every target volume.

A busy lock means an active owner. Any existing marker after acquiring the lock means uncertain ownership, even if a PID is dead. Another recovery cannot infer safe takeover from time, Pod deletion, or Job state. A poisoned retry needs a new Cluster and all-new target PVCs.

A local Begin/Drain session binds Cluster, operation, Pod, guard, and sidecar incarnation identities. The sidecar admits no target work before Begin. Drain irreversibly closes admission and waits for callbacks, target writes, subprocesses, and the original reader's cleanup. A restarted sidecar cannot adopt the session.

The guard retains locks through CNPG preflight, reconstruction, RestoreResponse, replay, and descendant termination. It reaps detached descendants across the main PID namespace. Only complete descendant reap and the same sidecar's successful drain permit marker removal, fsync, and unlock. Session loss, guard crash, or uncertain native failure leaves markers. These target locks are separate from source repository protection.

The sidecar's native scratch area has its own Pod-bound marker and inherited lock. A replacement can reclaim only that marked subtree after acquiring the lock. Detached native children retain the descriptor. This scratch cleanup never clears recovery markers or remote holders.

## Source protection and restore completion

Before any source catalog read, the manager establishes a stable restore lifetime holder. The sidecar independently confirms it and obtains its own fresh process-reader holder. Protection is repository-wide and does not need the source controller or Kubernetes Backup catalog.

The operation derives from target Cluster UID and the immutable bootstrap fingerprint. The manager records its operation and terminal state in `<cluster>-cb-recovery`. Before materialization, the sidecar atomically saves a credential-free plan outside PGDATA at `.cnpg-backup/<operation>/recovery.json` and projects a helper copy to `/cnpg-backup/state/recovery.json`. The plan freezes exact inputs, target, timeline path, required intervals, bundle hashes, and holder identities. A new admitted attempt validates the saved selection and uses its own reader identity.

RestoreResponse does not release either holder. The same sidecar process drains and removes only its reader holder. The manager records completion only after a matching successful Job and all original/retry Pods and containers have reliably terminated, then releases only the stable lifetime holder. Watch loss, disappearance, force deletion, or active manager incarnation loss retains protection. A new manager may finish an already recorded completed release, but cannot adopt an interrupted observer or remove another process's holder.

## WAL and recovery correctness

Archive acknowledgment means remotely durable, verified content. Same-name identical raw bytes can succeed on retry, including after a compression change. Different content is fatal. There is no early-ack queue. Restore verifies stored and raw content, fsyncs a private file, atomically renames it within the confined WAL directory, and syncs the directory. A directory lock protects the single private publication temporary. No negative cache or prefetch is used.

Ordinary standby and rewind reads use destination storage only. Callback names and paths are validated before I/O. Supported WAL roots are CNPG's `/var/lib/postgresql/data/pgdata/pg_wal` and the declared separate `/var/lib/postgresql/wal/pg_wal`, with the actual symlink layout checked. Restore permits the requested basename, `RECOVERYXLOG`, or `RECOVERYHISTORY`, never an arbitrary destination.

Primary recovery uses the injected `wal-fetch` helper because stock CNPG collapses WAL RPC failures to ordinary exit 1. The helper has no S3 credentials and calls the existing Unix RPC with its admitted plan. RestoreResponse supplies this fixed command plus the frozen numeric timeline and `promote` action:

```text
exec /cnpg-backup/bin/cnpg-backup wal-fetch --plan /cnpg-backup/state/recovery.json -- "%f" "%p"
```

The sidecar verifies the guard session identity before source reads. Ordinary instance and rewind callbacks reject recovery tokens and cannot use a retained externalCluster declaration to read source storage.

| Result | Helper exit |
|---|---:|
| Verified archive bytes, atomically published and synced | 0 |
| Authenticated optional future-tail or history absence | 1 |
| Authenticated archive-duplicate absence, with the actual local selected bundle still verified and no required remote interval in that segment | 1 |
| Required gap, retired content, corruption, local I/O, TLS, authentication, transport, deadline, RPC, plan, argument error, or panic | 255 |

Archive lookup always comes first, even for immediate recovery. HEAD 404 needs a confirming GET `NoSuchKey`; missing buckets and malformed errors are not absence. Other HEAD failures cannot be hidden by a later miss. PostgreSQL tries archive before local WAL, so exit 1 permits verified bundled-WAL fallback. A bundle never becomes archive success.

Required coverage is a timeline/LSN interval, not a filename set. The final bundled filename can also contain required post-backup archive bytes. A missing archive copy then exits 255, even if a zero-padded bundle exists locally. Selection rejects interior gaps through the admitted archive frontier. PostgreSQL must reach explicit targets. Latest means latest durably archived coverage, not unarchived source transactions.

Promotion `.partial` files retain their distinct names and full physical segment size. They never substitute for complete-segment coverage or establish a latest timeline. History and promotion partials are retained conservatively.

## Availability trade-offs

The [repository gate](repository.md#deletion-admission) favors retained data over automatic recovery from uncertain destructive work. Crashed backup or restore holders block GC but allow other protected operations. An uncertain GC owner blocks new admission and never expires or transfers.

Healthy `archive_timeout=60s` adds upload and queue latency. The current segment and backlog can be lost, and an outage has no fixed loss bound. Freshness metrics, successful materialization, and a Kubernetes Ready condition do not prove continuous recoverability. [Recovery tests](testing.md) establish SQL target attainment and distinguishing failure behavior within the published matrix, not every hardware or network failure.
