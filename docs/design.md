# Direct-to-S3 CNPG-I backup plugin — proposed design

Status: **proposal for review, not an approved specification or implemented product**.
The [GitHub Wayfinder map](https://github.com/djosh34/cnpg_backup/issues/1) is the canonical decision index; resolutions live in its child issues. This document is the initial design asset, [published for review](https://github.com/djosh34/cnpg_backup/issues/2), and the [separate delivery graph](https://github.com/djosh34/cnpg_backup/issues/3) describes proposed PR boundaries. Use [the execution brief](EXECUTE.md) for the fresh-thread handoff and autonomous PR/review loop. All technical decisions and the owner's final design grill must finish during planning before READY; the implementation thread receives a finalized design, not deferred architecture questions.

## 1. Destination and accepted constraints

Design the complete solution and divide implementation into reasonably sized PRs with explicit goals, requirements, dependencies, and executable acceptance criteria. Prefer simple runtime code and unusually strong recovery testing.

- Our binaries and Go dependencies must build with `CGO_ENABLED=0`. No Python, Barman, pgBackRest runtime, C archive extension, or generic backup daemon.
- PostgreSQL utilities are the sole permitted non-Go runtime exception, only where justified by simpler, safer implementation. This exception includes their native runtime libraries, which still require patching and scanning.
- PostgreSQL 18 is the first required version. Older versions are not implicitly supported.
- Intended deployment includes Dell ECS with AWS **Signature Version 2** authentication. **MinIO is the integration/release test target; Dell access, version discovery, testing and certification are not requirements or blockers.** Use a widely adopted maintained Go SDK with SigV2 support. Report MinIO-tested S3 compatibility, not Dell certification. Signature V2 is not AWS SDK for Go v2.
- Direct object storage traffic; no external backup service. Kubernetes plugin manager workloads, instance sidecars, restore jobs, and temporary disk are allowed.
- Full and space-efficient physical backups, compressed WAL archiving, scheduling through CNPG, automatic dependency-safe retention, restore and PITR are all in the planned product. No claim of complete Barman/pgBackRest feature parity.
- Small initial database, but no whole-database RAM buffering or unbounded concurrency. Measure scaling rather than assume that small test fixtures demonstrate it.

### Owner decisions from the final grill

- **Accepted:** PostgreSQL native utilities and their temporary-disk cost, specifically to reduce implementation risk and version drift.
- **Accepted:** differential-to-full topology, conditional on native support. [Primary-source verification](research/native-differentials-retention-locks.md) satisfies that condition: each native incremental can use the same full manifest, and reconstruction needs that full plus the chosen differential. Real-system qualification is still required.
- **Accepted:** PG18, primary capture, failover-ready sidecars; test CNPG-managed tablespaces and separate WAL volumes. Older majors and standby capture are outside the first release.
- **Accepted:** honest asynchronous WAL archive/RPO semantics and the proposed healthy-operation timeout profile.
- **Accepted:** repository-wide deletion protection during restore. Uncertain/crashed recovery keeps deletion paused and emits a Kubernetes Warning event; use a small conservative mechanism, not an elaborate lease/recovery framework. **Scope resolved:** automatically protect every restore performed through this plugin, including cross-cluster restores. Arbitrary external S3 readers are out of scope. Coordination permissions/protocol are technical planning work, not another owner questionnaire; verify the smallest safe mechanism before READY.
- **Accepted:** configurable recovery window plus independent minimum-full safety floor, with WAL retained for the required recovery coverage. Deletion is off until configured; examples use 14 days and two full backups. No independent aggressive WAL-expiration knob initially.
- **Accepted:** a requested differential succeeds as a differential or fails. **No full fallback**, including missing WAL summaries; unexpected full-sized storage use is unacceptable. Every backup type has last-success and failure metrics plus operator-facing alerts.
- **Accepted:** automatically publish versioned releases and qualified images; never deploy to a production environment. Production use belongs to consuming teams, not this project's execution plan.
- **Accepted:** all rights reserved for original project work, not Apache-2.0 or another open-source license. Third-party components keep their own licenses/notice requirements; publishing source/images does not grant a general project license to consuming teams.

These answers resolve product directions, not untested algorithms. The design remains IN PROGRESS until the remaining decisions and technical verification are complete.

## 2. Proposed architecture

```text
CNPG operator -- CNPG-I gRPC --> plugin manager Deployment
                                    |
                           lifecycle patches/config
                                    v
PostgreSQL pod: [CNPG instance manager + PostgreSQL] <-> [Go plugin sidecar]
                                                           |
                        pg_basebackup -> disk staging -> Go S3 client
                        WAL callback -> Go gzip + checksum -> S3

CNPG recovery Job: [CNPG bootstrap] <-> [Go restore mode + PostgreSQL tools]
                                           |
                         S3 -> verify/extract -> pg_combinebackup
                                           |
                         PGDATA -> PostgreSQL WAL replay via plugin
```

One codebase, a Go executable with manager/instance/restore modes if this remains the simplest packaging. Two image variants are acceptable: CGO-free manager image and a PG18 tool-equipped data-path image. Do not add a network backup service to save a sidecar.

The manager performs discovery, identity/capability negotiation, lifecycle mutation and configuration reconciliation. It does not proxy backup bytes. Sidecars expose CNPG-I over the expected shared Unix socket and access only required shared volumes. The reference plugin uses a restartable init-container sidecar; validate the minimum Kubernetes and CNPG versions before adopting it. Keep sidecars on eligible database pods so failover does not depend on injecting a new container after promotion. Start with one small manager replica; determine safe upgrade/restart behavior from contract tests, not a home-grown HA framework.

Use CNPG `Backup`, `ScheduledBackup`, `Cluster.spec.plugins` and `externalClusters[].plugin`. Do not build a scheduler, replace CNPG status ownership, or mutate database settings behind the operator. Publish required PostgreSQL settings in Cluster manifests and validate them with actionable errors.

### Internal modules, not a framework

- `cnpgi`: translate CNPG requests/results, lifecycle and error conventions; no repository algorithms.
- `repository`: durable backup catalog, publication, identity, dependencies and recovery plans. Callers should not manipulate S3 keys or invent commit ordering.
- `postgres`: controlled subprocess execution, native manifests, capture/reconstruction and version checks.
- `wal`: archive/restore one requested file, checksums, compression and archive status.
- `retention`: deterministic keep/delete planning; execution revalidates repository state.
- `s3store`: the real storage adapter, not a generic multi-cloud framework.

These are provisional package seams. Prefer concrete implementations and a few small internal test seams for faults, clocks and subprocesses. Stdlib `testing`, `httptest`, `os`, `io`, `archive/tar`, `compress/gzip`, `crypto/sha256`, `log/slog`, `flag` and `encoding/json` cover the ordinary work.

## 3. Backup engine: native increments, bounded chains

**Owner-approved engine direction:** use PG18 `pg_basebackup`, `pg_combinebackup`, and `pg_verifybackup`. This is the strongest justification for the permitted PostgreSQL-tool exception: PostgreSQL owns changing-block discovery and physical reconstruction rather than this project copying those internals.

Native incremental backup exists from PG17. That makes PG17 a plausible later addition, not a tested compatibility promise. Use utilities matching the supported server major and maintain the image as part of this project's releases.

Proposed first supported backup modes:

- `full`: no reference manifest.
- `differential`: invoke native incremental backup using a retained full backup's exact PostgreSQL manifest. Restore requires that full plus the selected differential, not every intervening differential.

Do not initially expose arbitrarily long incremental-to-incremental chains. Metadata records parents from day one, so deeper chains are a future product decision, not a repository redesign. Native PostgreSQL may send entire non-relation files or choose full representations of relation files; differential does not mean every byte is deduplicated.

Enable `summarize_wal`; size `wal_summary_keep_time` to cover the age of the reference full plus operational slack. Remote WAL retention does not preserve local WAL summaries. Missing summaries, a missing parent, incompatible identity/checksum state, or promotion during capture must fail the requested differential explicitly. **Never fall back to full, even with a warning or an opt-in fallback setting in this initial product.** The owner rejected that storage expansion. A full backup runs only when explicitly requested/scheduled as full; report failed differentials through status, Warning events and the metrics below. Require a new full after major upgrade or checksum-state change; initially also after a timeline transition unless cross-timeline differential behavior is specifically proven.

### Capture path

1. Resolve one immutable configuration snapshot and deterministic operation identity from the CNPG Backup UID; enforce per-repository backup/retention coordination.
2. Check PG major, repository identity, credentials, free workspace, and reference eligibility. Never write the backup into live PGDATA/tablespace locations.
3. Run `pg_basebackup` in tar format to a dedicated disk workspace, with required WAL streamed into the backup (`-X stream`) and PostgreSQL manifests enabled. Use explicit replication authentication; validate the supplied CNPG certificate/user can make the required connection. Do not use a superuser credential as an unexplained default.
4. Preserve the original manifest bytes. Check process completion and checksums; upload completed tar artifacts using bounded multipart buffers and Go compression. A later measured optimization may upload completed artifacts earlier, but no stdout magic or custom replication client is required for the initial design.
5. Validate backup integrity with the approved PG tool/manifest workflow and prove required WAL availability before publishing it as recoverable. Bundled bootstrap WAL makes the base backup self-contained, but does not establish continuous remote PITR coverage after it. Specify exactly how bundled and archived WAL interact, including archive lag and deadlines, in the recovery decision.
6. Publish the immutable commit metadata last. Return CNPG success only after durable publication; a timed-out response after publication must be safely repeatable using the same operation identity.
7. Clean workspace and abandoned multipart uploads. Cancellation kills/reaps the subprocess and never marks incomplete backup data complete.

The simple path incurs local staging roughly proportional to the backup output. This is deliberate, reviewable disk cost, not a claim of zero-copy streaming. Raw pg_basebackup tar output to stdout cannot cover all tablespace/WAL-streaming combinations. If staging is unacceptable, investigate a Go replication-protocol client in a separate design decision instead of quietly dropping consistency or tablespace coverage.

### Why not a Go filesystem diff library?

Go content-defined chunking exists (for example `restic/chunker`), but chunking is not a PostgreSQL online backup protocol. Using it adds chunk indexes, content-addressed garbage collection, reconstruction, file-lifecycle edge cases and a second backup format; live files also require PostgreSQL backup consistency rules. Native block incrementals remove more code than a generic diff library does. Do not clone pgBackRest's engine just to reproduce its user-visible full/differential behavior.

## 4. S3 repository and durability

**Candidate:** `minio-go/v7`, which exposes `credentials.NewStaticV2` and `NewStaticV4`. Select a maintained release after `CGO_ENABLED=0` builds, linked dependency inspection and MinIO integration tests. Exercise SigV2 and SigV4, including multipart and private CA, through the SDK; require no Dell-specific experiment. Its module file includes substantial tooling dependencies; distinguish shipped packages from development tools rather than either counting every module as runtime or ignoring transitive dependencies. Do not promise a stdlib-only executable or write a custom signer by default.

Configuration: explicit endpoint, bucket, prefix, signing version, addressing style, region where needed, CA reference, Secret references, timeouts and bounded upload concurrency. TLS verification is mandatory in production; permit private CAs. Fail on missing credentials rather than fall through to anonymous access. Do not silently downgrade SigV4 to SigV2. Signature V2 is an explicit compatibility mode, not the future default for AWS.

Proposed versioned layout (exact schema is gated on the repository decision):

```text
<prefix>/v1/<repository-id>/repository.json
<prefix>/v1/<repository-id>/backups/<backup-id>/manifest.pg.json
<prefix>/v1/<repository-id>/backups/<backup-id>/data/<artifact>.tar.gz
<prefix>/v1/<repository-id>/backups/<backup-id>/commit.json
<prefix>/v1/<repository-id>/wal/<timeline>/<original-name>.gz
```

A repository ID identifies an archive lineage, not just a Kubernetes cluster name. Track PostgreSQL system identifier, major, initial owner and timeline context. A PITR clone can retain the PostgreSQL system identifier: system ID alone cannot prevent divergent writers. Restores read the source repository and archive to a distinct destination identity by default.

Commit metadata includes schema version, backup/operation ID, kind, parent and root IDs, system identifier, PG/tool versions, timeline, start/stop times and LSNs, WAL requirements, tablespaces, checksum mode, artifact sizes/checksums/compression and original manifest checksum. Keep authoritative recovery metadata in S3, not only in Kubernetes Backup objects. A new Kubernetes cluster with source credentials/config must be able to restore after all original Kubernetes objects disappear.

Immutable data keys plus commit-last publication avoid a central database and mutable global catalog. List committed backups with pagination; an optional cache is never authoritative. Reject unknown format versions and malformed/cyclic/missing parent relationships. Ignore interrupted uploads for backup selection; conservative orphan cleanup must not race an active upload.

**Storage correctness gate:** documented S3/SDK behavior plus MinIO tests for atomic create/no-clobber, multipart completion, conditional publication, list/read consistency and ambiguous timeout outcomes. HEAD-then-PUT is not atomic and ETag is not generally a content checksum. Prefer portable standard operations over provider-specific features; validate required capabilities and fail clearly on unsupported endpoints. Widely used SDK code reduces protocol ownership but does not replace our publication/retention tests. No Dell evidence is required.

**Coordination decision in progress:** retain the proposed one-writer-cluster-per-repository boundary; restored clusters archive separately. The owner requests a repository-wide deletion lock while restoring and a Kubernetes Warning event when it blocks retention. Determine the smallest safe protocol before READY: acquire/acknowledge protection before selecting/downloading inputs, quiesce already-running deletes, and protect all required data through PostgreSQL's final source-WAL use—not merely through the bootstrap download job. Protect plugin-managed restores only; arbitrary external S3 readers need no discovery, protection or special read-only workflow. Finalize technical details for cross-cluster plugin coordination, durable ownership, stale deleters and interrupted recovery. Preserve disaster recovery after source Kubernetes objects disappear; do not silently require a surviving original Kubernetes API. A process mutex or unacknowledged S3 marker is not a sufficient protocol. Uncertain/crashed recovery must not silently lose protection via a TTL. Do not build a general distributed lock service; missing/uncertain coordination stops deletion.

Never allow independent bucket lifecycle expiration of live backup/WAL objects. An age-only lifecycle rule cannot understand backup dependencies. Abandoned multipart cleanup and independently retained object versions are separate administrative policies.

## 5. WAL shipping, recovery and loss semantics

WAL transport needs no `pg_receivewal`, shell compressor or C archive library: handle CNPG-I Archive/Restore callbacks in Go. The plugin receives completed files, compresses them using stdlib gzip, computes a content checksum and uploads them directly.

- Acknowledge Archive only after remote durable success or verification of an identical already-persisted object. Same WAL name/different content is a hard failure; never overwrite it. Coordinate duplicate/concurrent callbacks with the verified storage protocol.
- Bound memory, retries and in-flight work. WAL must not starve behind a base-backup upload or retention scan. PostgreSQL remains responsible for retrying failed archive requests; do not add a second asynchronous durability queue that acknowledges early.
- Preserve timeline `.history` and backup-history files as applicable; do not hard-code all objects to 16 MiB. Validate names/paths against the actual CNPG contract.
- Restore into a private temporary file, verify decompression, byte count and checksum, then atomically publish to the requested destination with required file/directory synchronization. Never return success after a partial download.
- Distinguish an expected missing future WAL/history file from TLS/authentication failure, timeout or corruption using the exact CNPG error conventions. A temporary S3 failure must not be reported as end-of-archive.
- Honor supported rewind context and destination-empty safeguards. Initially no prefetch/cache avoids stale miss and pg_rewind hazards.
- Archive status first/last objects are not proof of gap-free recovery. Expose archive failures/backlog and conservative known recovery availability; do not advertise an exact latest transaction time from filenames alone.

**RPO:** comparable *asynchronous archive semantics*, not an invented universal Barman/pgBackRest SLA. Loss may include the current unarchived segment plus backlog. Under healthy operation, `archive_timeout` bounds low-traffic segment-switch delay, with upload/queue latency added; during outages there is no fixed loss bound. Propose a documented 60-second archive timeout profile for review, not zero RPO. Alert on lag/failure and WAL filesystem pressure. This matters especially for a tiny, low-write database.

**Restore:** CNPG bootstraps a new cluster, not in-place recovery. Resolve a committed full/differential chain that can reach the requested timeline/target. A base backup must have completed sufficiently early for the target; never choose the newest backup blindly. Time/LSN/latest selection and explicit backup ID must be defined; named restore point/XID targets may require an explicit backup because their ordering cannot be inferred from backup timestamps. Preserve CNPG's target/inclusive/action handling rather than invent a second PITR controller.

Download and verify inputs, securely extract into isolated directories, run `pg_combinebackup` for differential restore, then verify the reconstructed full before CNPG starts PostgreSQL. Let PostgreSQL replay WAL and enforce the recovery target. A successful file download or combine command is not a successful PITR result. Integration tests must query recovered data and verify inclusion/exclusion around the target.

Workspace initially uses ordinary copies rather than depending on reflinks or hardlinks. Account for compressed download, extracted full and differential, and synthetic output, plus tablespaces. Require a capacity preflight and configurable disk workspace (e.g. dedicated PVC); never use memory-backed emptyDir for database-sized data. No fixed RAM/RTO promise until benchmarked.

## 6. Retention: preserve recoverability, fail closed

Recommend a recovery-window setting plus a minimum full-backup count safety floor, with no automatic deletion until configured. [pgBackRest's time retention already preserves a window anchor and dependencies](research/native-differentials-retention-locks.md); this proposal adopts similar conservative reasoning with a smaller configuration surface, not a claim of superior recovery safety. The owner approved the window/floor policy, deletion disabled until configured, examples using 14 days/two full backups, and no independent aggressive WAL-expiration knob.

For window cutoff C:

1. Select restorable backups/timeline paths needed for targets in the promised window, including an anchor backup completed before the cutoff where available.
2. Retain the transitive parent closure of every retained differential. Retain minimum full roots and data protected by the finalized restore/deletion coordination contract.
3. Retain WAL from the earliest required start/redo position across those backups and supported timeline paths; retain timeline history conservatively. Never delete by wall-clock object age or compare filenames lexicographically across timelines.
4. Produce an explainable deterministic keep/delete plan. Revalidate and coordinate before execution. Unknown metadata, missing parents, active operations, list errors or uncertain timeline requirements stop destructive work.
5. Remove expiration eligibility/commit visibility before payload deletion under the approved coordination protocol. Interrupted deletion must be repeatable without exposing an incomplete backup as selectable. Protect plugin-managed restores using the finalized repository-wide deletion-lock protocol. Cross-cluster transport and crash cleanup remain technical planning tasks; lock expiry does not prove a restore stopped. Unannounced external S3 readers are explicitly outside the protection guarantee.
6. Prune only WAL proven unnecessary, after dependency updates succeed. Never delete the last usable backup merely because successful backups stopped arriving.

Example: full F is 10 days old, differential D is 1 day old, window is 7 days. F cannot be removed just because it is older than the window: D needs it. Keep any additional anchor/WAL needed to cover the beginning of the window. If the only eligible anchor is missing, report a shortened/broken recovery window rather than claim the setting was met.

Retention runs as a bounded, cancellable periodic activity under one authority, and can be triggered after successful backup. It is not a second scheduling controller. CNPG WAL retention hints cannot override stronger backup-chain or active-reader requirements.

## 7. User-facing configuration

Recommend one namespaced repository configuration CRD, analogous in role (not wire compatibility) to Barman's ObjectStore, plus ordinary Secrets. Reuse CNPG scheduling and recovery objects; no custom Backup/Schedule CRD. Whether a smaller ConfigMap-based start wins is an explicit deployment decision.

Configuration surface should include:

- S3 connection/auth/CA/Secret references and stable repository identity.
- Full/differential mode as CNPG per-backup parameters; differential failures never change the requested type to full.
- Recovery source repository and optional backup ID; standard CNPG recovery targets.
- Retention window/minimum roots, enable/dry-run and interval.
- Workspace and CPU/memory/ephemeral-storage settings, rate limit and small concurrency bounds.
- Gzip on/off or a small validated level range; no compressor plugin framework.

Start with Linux, PG18 and the approved CNPG/Kubernetes pair. The owner approved primary-only base backups initially; replicas still receive sidecars for failover. Standby backup support is a separate compatibility decision because native incrementals depend on restartpoints and promotion behavior. CNPG-managed tablespaces and separate WAL volumes are in the agreed initial scope and must pass recovery tests. Unsupported layouts must fail preflight rather than be silently omitted; difficulty implementing an agreed layout is not permission to drop it.

### Backup observability — required with each backup feature

Expose per-repository, per-backup-type Prometheus metrics; `backup_type` has only `full` and `differential` values. Repository/cluster identity must be unambiguous without backup IDs, timestamps, object keys or error strings as labels.

- `cnpg_backup_last_success_timestamp_seconds`: timestamp of the last durably committed successful backup of that type. Failed attempts/retries must not advance it. Reconcile it with authoritative committed backup metadata after restart; if history is unavailable, report no known success rather than fabricate a fresh timestamp. Never-successful/unknown history must be distinguishable from a recent success.
- `cnpg_backup_failures_total`: failed terminal backup attempts of that requested type, not every individual S3/subprocess retry. Ordinary Prometheus counter-reset semantics apply; do not build a durable metrics ledger. Define retry/reconciliation counting precisely before implementation.
- Actionable CNPG failure status, redacted diagnostics and a rate-limited Warning event for failed backups. Warning events complement metrics/status, not replace them.
- Ship example alerts for backup failures and overdue/missing successful backups **separately by type**, with thresholds matching the configured full/differential schedules. A recent differential must not hide an overdue full, or vice versa. Disabled/unscheduled types should not generate misleading freshness alerts. Keep WAL-archive lag/failure alerts separate.

Add metrics and regressions when each backup mode lands; do not defer observability until the final operations PR. Inject missing summaries and prove failure leaves both success timestamps unchanged, reports the differential failure and never launches a replacement full backup. This bans fallback, not PostgreSQL's native choice to include full representations of some files inside a legitimate differential; workspace limits still apply to actual output.

## 8. Security and maintainability

Use upstream CNPG-I protobuf/gRPC and maintained Kubernetes libraries where they remove protocol machinery; do not hand-roll gRPC, Kubernetes authentication/watch logic or S3 signing to reduce a dependency counter. Do not mechanically import the whole Barman plugin and its unrelated cloud features. Add a pure-Go SQL client only if an identified operation requires it and it beats justified tool reuse.

Non-root containers, read-only root filesystems, restricted capabilities/seccomp, narrow mounts, scoped RBAC and no cluster-wide Secret cache by default. Respect CNPG's required TLS/authentication at the manager interface; local sockets use restrictive permissions. Secret rotation and CA rotation must have a documented reload/rollout contract. No credentials in arguments, logs, errors, manifests or metric labels. Reject unsafe redirects/endpoints and archive path traversal/symlink escapes. Enforce limits on manifests, artifact counts, expansion sizes and malformed archive headers.

SSE via documented provider support is preferred to inventing client-side encryption/key management. Test relevant client options against MinIO. Bucket encryption/access policy remain operator responsibilities; no inspection of a Dell installation is required. Product-managed object lock, WORM administration and custom client-side encryption are outside the initial scope unless separately requested. A compromised writer able to overwrite payload and checksums is outside accidental-corruption protection: optional object-lock/versioning policies need separate validation, not a claim that SHA256 authenticates hostile storage.

Build an SBOM, scan linked Go code with govulncheck and scan both final images (including PostgreSQL native packages). Use Checkmarx according to the owner's available pipeline/license; document actionable findings/exceptions. Pin dependencies and tool images, retain reproducible build inputs and release provenance, and test patch upgrades against old backups. Testing/tool dependencies must not enter the production image simply for CI convenience.

## 9. Test and release strategy

[Testing and recovery campaigns](testing.md) is the authoritative test plan: per-PR checks, deterministic simulation of actual Go modules, native fuzzing, real PG18/CNPG/MinIO recovery and a manually dispatchable two-hour GitHub Actions campaign reusable for release qualification and existing release images.

Every PR adds its own tests and fault cases. The final reliability PR assembles cross-feature qualification; it does not introduce the harness for the first time. Keep simulation/fault machinery in test code behind the few real I/O seams. Production code must not become a simulator framework.

Release requires SQL-verified full/differential/PITR restores, retention/publication failure evidence, MinIO SigV2/SigV4 tests, reproducible failure artifacts, measured resource use and review of supported versions/security findings. No actual Dell/AWS deployment test is required, and no Dell certification is claimed. Deterministic simulation and real-system seeded chaos provide complementary evidence; neither makes the whole distributed system deterministic or proves all possible failures absent.

## 10. Remaining design gates

The technical tickets settle the exact CNPG contract/support pins, native tool workflow/workspace, portable S3 publication, WAL error mapping, retention and release evidence. They are focused verification tasks rather than an invitation to redesign the product endlessly.

Planning must resolve and freeze these choices before the [READY handoff](EXECUTE.md): native full/differential tools and workspace, initial PG/layout/backup support, portable publication/concurrency, recovery/retention coordination, configuration, test gates and delivery endpoint. The owner settles product trade-offs in the final design grill; source research and targeted experiments establish technical facts. No unresolved mechanism is passed to the implementation thread as a planning task.

After that handoff the orchestrator implements, reviews, tests and merges autonomously. It may revise PR boundaries and internal structure when evidence warrants it, documenting why and preserving the approved external behavior and safety. This adaptation rule is not permission to silently omit features or weaken recovery guarantees.

## Sources

Primary-source reconnaissance; no code, backend experiment or compatibility test has run yet. GitHub reference releases observed during planning: Barman plugin v0.15.0, CNPG v1.30.0, CNPG-I v0.6.0, minio-go v7.3.0. These are research anchors, not approved version pins.

1. [PG18 pg_basebackup: incrementals, formats, stdout limitations, WAL methods, manifests](https://www.postgresql.org/docs/18/app-pgbasebackup.html).
2. [PG18 pg_combinebackup: dependency chain, workspace, checksum restrictions](https://www.postgresql.org/docs/18/app-pgcombinebackup.html).
3. [PG18 continuous archiving: acknowledgment, duplicate safety, archive_timeout, summaries, recovery targets and timelines](https://www.postgresql.org/docs/18/continuous-archiving.html).
4. [PG18 WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html).
5. [Barman plugin concepts at v0.15.0](https://github.com/cloudnative-pg/plugin-barman-cloud/blob/v0.15.0/web/docs/concepts.md) and [usage](https://github.com/cloudnative-pg/plugin-barman-cloud/blob/v0.15.0/web/docs/usage.md).
6. [Barman lifecycle implementation: manager, instance/restore sidecars and mounts](https://github.com/cloudnative-pg/plugin-barman-cloud/blob/v0.15.0/internal/cnpgi/operator/lifecycle.go).
7. [Barman restore implementation](https://github.com/cloudnative-pg/plugin-barman-cloud/blob/v0.15.0/internal/cnpgi/restore/restore.go).
8. [CNPG-I v0.6.0 WAL contract](https://github.com/cloudnative-pg/cnpg-i/blob/v0.6.0/proto/wal.proto), [backup](https://github.com/cloudnative-pg/cnpg-i/blob/v0.6.0/proto/backup.proto), [restore](https://github.com/cloudnative-pg/cnpg-i/blob/v0.6.0/proto/restore_job.proto).
9. [minio-go static V2/V4 credential providers](https://github.com/minio/minio-go/blob/v7.3.0/pkg/credentials/static.go) and [module dependencies](https://github.com/minio/minio-go/blob/v7.3.0/go.mod).
10. [Dell ECS authentication guide](https://www.dell.com/support/manuals/en-us/ecs-appliance-/ecs_pub_data_access_guide_3_3_to_3_6/authenticating-with-the-s3-service?guid=guid-d21ee42b-ca1d-4425-a29d-386320cad31a&lang=en-us): search surfaced Dell's V2/V4 support statement, but direct retrieval returned HTTP 403. Actual appliance version and request semantics remain unverified.
11. [Go content-defined chunking example](https://pkg.go.dev/github.com/restic/chunker); existence does not establish suitability as a PostgreSQL backup engine.
12. [pgBackRest guide](https://pgbackrest.org/user-guide.html#backup): comparison target for full/differential behavior, not a blanket parity commitment.
