# Repository and S3 reference

`internal/repository` owns format v1, all derived keys, immutable publication, protected catalogs, restore plans, and destructive admission. `internal/s3store` implements its concrete storage seam with minio-go. Unknown formats and malformed metadata fail closed. There is no automatic format rewrite.

## Storage requirements

The backend must provide TLS, atomic object visibility, atomic `If-None-Match: *` creation and ETag `If-Match` replacement, and strong reads and complete ordered lists. ETags are opaque concurrency tokens, not checksums. The plugin never substitutes HEAD-then-PUT for conditional creation.

Writer and retention startup checks require an unversioned bucket with no Object Lock or lifecycle rules. Versioning suspension is also rejected. Bucket safety checks that are denied or unsupported do not count as safe. No unrelated process or bucket administrator may mutate or expire repository data while the plugin uses it.

Initialization probes conditional create, rejected create, missing-object CAS, successful CAS, stale CAS, and subsequent GET/HEAD/LIST under a unique `probes/` path. Failed capability checks return `UnsupportedStorageSemantics`. Finite probes do not prove an arbitrary endpoint's consistency under every failure. MinIO with both signers and private CA is the tested backend, not a claim of universal S3 compatibility.

Destination credentials need bucket configuration reads, prefix object reads/list, conditional writes, multipart initiation/upload/completion/list, and destructive operations for retention. Bucket checks use `GetBucketVersioning`, `GetLifecycleConfiguration`, and `GetBucketObjectLockConfiguration`. Buckets must already exist; production does not need `CreateBucket` or `ListAllMyBuckets`.

GC deletion permissions apply only to attempt payload keys. Explicitly deny `DeleteObject` on repository identity, gate, request, commit, retirement, and WAL slot keys. Source recovery needs prefix reads/list and GET/conditional PUT on the exact source `gate.json`, not payload deletion. It opens existing identity and gate metadata without initializing or repairing them.

Gate writers are trusted cooperative plugin participants. S3 policy cannot distinguish a legitimate holder update from a malicious owner change. This protocol protects against accidental concurrency, not compromised credentials or arbitrary external readers. Source recovery cannot use read-only credentials because it must acknowledge deletion protection.

The adapter uses explicit credential/CA snapshots, exact configured authority, path addressing, and no proxy, redirect, credential-chain, or signer fallback. Fresh HTTP/1 connections avoid automatic replay of bodyless destructive requests on reused connections. Transport error messages omit endpoints, keys, server messages, and secrets.

## Key layout

All keys are under `<configured-prefix>/v1/<repository-UUID>/`. Repository code derives the following paths from validated IDs, indices, and PostgreSQL filenames.

| Relative key | Meaning |
|---|---|
| `repository.json` | Immutable schema, repository UUID, PG major, system identifier, WAL segment size, writer Cluster UID, creation time |
| `gate.json` | CAS generation, nonce, nonexpiring holders, optional destructive owner |
| `backups/<Backup-UID>/request.json` | Immutable requested type, full root if differential, writer and configuration identity |
| `backups/<Backup-UID>/attempts/<attempt-UUID>/claim.json` | Ownership of one fresh attempt namespace |
| `backups/<Backup-UID>/attempts/<attempt-UUID>/manifest.pg.json` | Exact original native manifest |
| `backups/<Backup-UID>/attempts/<attempt-UUID>/data/<index>.tar[.gz]` | Attempt-owned backup artifacts |
| `backups/<Backup-UID>/commit.json` | Immutable winning manifest/artifact inventory and capture metadata |
| `backups/<Backup-UID>/retired.json` | Permanent exclusion from recovery selection |
| `wal/<8-hex-timeline>/<PostgreSQL-filename>` | Verified WAL/history content, or a permanent same-key WAL tombstone |
| `gc/<operation-UUID>.json` | Immutable destructive plan, not permission for a new process to resume it |
| `probes/<probe-UUID>/...` | Conditional-capability diagnostics |

WAL keys have no compression suffix. Live object metadata uses `cnpg-format: wal-v1`, `cnpg-system-id`, `cnpg-raw-bytes`, `cnpg-raw-sha256`, `cnpg-stored-sha256`, and `cnpg-compression`. The body is raw or gzip data, without HTTP Content-Encoding. A retired slot has `cnpg-format: wal-retired-v1` and a JSON tombstone recording identity, original name/hash/size, and GC operation. Reads verify actual bytes rather than trusting metadata alone.

Identity is a lineage, not a Cluster name or PostgreSQL system identifier alone. One cooperative Cluster writes it. A restored cluster archives to a distinct destination UUID. Missing identity or gate over used data cannot be repaired as if the repository were empty.

## Publication and retries

Backup admission precedes parent selection and capture. `request.json` freezes the Backup UID's semantics. Each attempt claims a new namespace. Publication uploads the original manifest and known-length artifacts, verifies remote raw/stored length and SHA256, validates the full parent, and creates `commit.json` last. The native caller has already verified archives, WAL, capacity, and source continuity.

The first valid commit wins. Losing or ambiguous retries verify and return that durable winner, not their own candidate bytes. A lost response can leave a valid commit and a failed CNPG invocation. Its success timestamp is the immutable commit object's S3 LastModified, distinct from source capture completion.

WAL uses a seekable disk spool and conditional single PUT with precomputed Content-MD5. It needs no global holder or per-file gate write. An existing initialized writer can keep archiving while holders or an uncertain GC owner block other work. Acknowledgment requires verified remote content. Same-name identical raw bytes can succeed on retry; different content or a retired slot cannot. No ETag-as-hash or local queue acknowledgment is used.

Artifacts use Core multipart operations in fresh claimed keys, 64 MiB parts, at most two workers per artifact, and ordered completion. High-level SDK automatic abort is not used. Lost initiation may have no UploadID. Failed attempts are not resumed or reused, and errors do not trigger destructive cleanup without GC admission.

Read retries restart verified downloads from byte zero, with at most five attempts. Mutations have one attempt. A timeout, transport loss, short response, malformed control response, or unknown result may mean a mutation applied. A successful retry or later HEAD absence does not prove the original request drained. Conservative uncertainty remains attached to its owner.

## Deletion admission

`gate.json` contains either holders or one process-specific GC owner, never both. Add-holder CAS success acknowledges admission. Each update changes generation and nonce. Ambiguous CAS outcomes require a fresh no-op CAS barrier before retry or absence inference, so delayed requests cannot acquire stale authority.

Backup holders cover producers. Restore uses both a stable lifetime holder and a fresh process-reader holder. Hold close stops local admission and drains admitted calls before release. A possibly applied producer mutation latches its holder even if a later read finds a valid winner. Another process can admit beside it but cannot clear it.

GC owns every retirement, payload DELETE, and multipart abort. Its owner never expires or transfers to another incarnation. It releases only after all dispatched destructive requests conclusively drain. Crash or ambiguity leaves the owner set and blocks new admission. No clock, retry, HEAD absence, or GC log authorizes takeover.

Crashed reader or backup holders block GC, but allow new protected backups and restores. Arbitrary external S3 readers do not obtain this protection. There is no force-clear or TTL option. Status and Warning events describe lock state but do not grant authority.

## Catalogs and recovery plans

A catalog requires a complete validated inventory under acknowledged protection. Metadata and original manifests are authenticated. Artifact HEADs prove presence and size, not content integrity. Selected inputs are downloaded and verified before use. Differential edges require a live exact full parent with matching physical and capture continuity.

Catalogs spool to bounded disk rather than keeping all manifest payloads in memory. A failed, partial, malformed, or canceled list yields no authoritative catalog. Protected visitation and dependency scans check cancellation between records. Permanent retired commit metadata remains available for diagnosis and success history.

Recovery freezes a full or full-plus-differential chain, target, numeric timeline path, and required archive intervals. Bundle coverage and remote coverage remain separate, including when both intersect the same segment. The [architecture reference](design.md#wal-and-recovery-correctness) defines helper failure semantics and target ownership.

## Retention

`internal/retention.Plan` keeps backups inside the configured recovery window, a usable pre-cutoff anchor along each known timeline path, the minimum full roots, the latest usable backup, and every retained differential's full parent. Without a pre-cutoff anchor it keeps the earliest usable candidate and reports a shortened window. Prolonged failed backups do not remove the last usable root.

WAL retention uses native redo/start/fork floors and continuous replay coverage, never object age or cross-timeline lexical ordering. Competing latest timeline leaves, inconsistent metadata, or required coverage gaps stop destructive work. Format v1 conservatively retains non-current-timeline WAL, history, backup-history, and promotion partials.

Execution acquires GC before authoritative inventory. It validates the complete plan, retires dependent differentials before their parent, and completes backup retirement before WAL retirement. Retirement permanently excludes a backup before payload deletion. Commit, request, and retirement history remain. WAL retirement replaces the permanent key with a conditional tombstone, so a late no-clobber upload cannot resurrect it.

`GCOwner.Cleanup` completes object and multipart inventories before returning at most 128 eligible payload deletions or aborts. Each victim requires a valid claimed attempt with no admitted producer and must be retired, nonwinning, or uncommitted. Reaching the victim limit does not permit skipping the rest of the inventory. A fresh batch can resume cleanup of permanent retirements only after the previous owner conclusively released, never by adopting that owner.

A completely validated catalog with no live backups permits only proven orphan cleanup, reports unavailable recovery coverage, and never retires WAL. Partial or corrupt inventory authorizes nothing.

A batch dispatches at most 128 serial destructive requests. Its 30-second destructive-phase clock starts at first dispatch. After a conclusively drained release it yields at least one second, then replans. These are work limits, not lease expiration. Manager work has a separate 90-second context including inventory reads.

The manager uses a separate 512 MiB disk-backed retention mount. It reserves 384 MiB before admission and accounts for existing spools, a 256 MiB catalog, sequential 64 MiB manifests, and control/history overhead. Bounded Go writes enforce that allocation budget; `emptyDir.sizeLimit` alone is not its enforcement mechanism. Capacity failure before destructive dispatch permits clean live-owner release.

## Bounds and errors

| Resource | Limit |
|---|---:|
| Small repository records | 64 KiB |
| Gate | 1 MiB, 1,024 holders |
| Commit and repository plan | 4 MiB |
| Original manifest | 64 MiB |
| Active catalog records | 1,000,000 |
| Complete adapter LIST | 1,000,000 entries, 10,001 pages |
| Stored artifact | 512 GiB, 8,192 multipart parts |
| Control response body and headers | 4 MiB body, 64 KiB headers |
| Hash/copy buffer | 128 KiB |

Permanent attempts, retirements, holders, and probe objects consume finite storage and LIST capacity. Exhaustion fails explicitly, not with a partial inventory. Strict metadata decoding rejects duplicate, unknown, missing, null-required, invalid Unicode, noncanonical identity/LSN, and trailing fields or documents.

`RepositoryAdmissionBlocked` indicates an exclusive owner prevents admission. `RepositoryOperationUncertain` retains protection after ambiguous work. `BackupAlreadyExpired` rejects retired selections. `RepositoryCapacityExceeded`, `RepositoryCorruption`, and `InvalidRepositoryMetadata` stop the affected operation. Only a fully consumed GET 404 XML `NoSuchKey` is authenticated object absence. Missing buckets, bare 404s, and malformed XML remain failures.
