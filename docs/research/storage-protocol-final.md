# Storage protocol, coordination and retention — final planning findings

Status: concrete technical resolution proposed for **#6, #9, #11 and the S3/security portion of #12**. Planning research, not production implementation, final owner approval, or release qualification. Read the authoritative owner answers on [#14](https://github.com/djosh34/cnpg_backup/issues/14#issuecomment-5563180143); this report does not replace issue resolutions. No agents were spawned and no GitHub writes were made by this researcher. Parent ran the supplied disposable experiment in hosted CI.

## 1. Decisions and important boundaries

1. Select **minio-go/v7 v7.3.0**, commit `ce0e323c55c64964e6ad820ef0c6f5b286446aae`, Apache-2.0. Use its static V4 credentials by default, explicitly configured V2 for compatible endpoints. No signer implementation, CGO or RDMA build tag. Go **1.27.1** is the experimentally verified compiler. Dependencies are not stdlib-only.
2. The required backend contract is linearizable conditional single-object PUT (`If-None-Match: *`, `If-Match: <ETag>`), atomic object visibility, strongly consistent GET/HEAD/LIST after completed mutations, and standard multipart/DELETE response semantics. **No HEAD-then-PUT.** Conditional headers must survive the transport. Endpoint preflight proves the positive and negative conditions on disposable keys before repository initialization; failure is `UnsupportedStorageSemantics`, never a downgrade.
3. **SigV2 portability is conditional, not universal.** Current AWS documentation explicitly requires SigV4 for conditional writes [S1]. MinIO actually accepts both signatures in the experiment below. Claim “MinIO-tested S3 compatibility, optional SigV2 on endpoints satisfying the capability contract,” not “AWS SigV2 supported,” universal S3 portability, or Dell certification. The SDK comments calling conditional PUT a MinIO extension are older than current AWS conditional-write APIs; their header implementation is the useful fact [S2].
4. Commit-last immutable backup artifacts, a conditional immutable commit per Backup UID, and conditional single-PUT WAL publication. Multipart is used **only for unique attempt-owned artifacts**, not a shared mutable catalog, WAL logical names, or the gate. Preserve small permanent retirement records so delayed writers cannot resurrect selectable expired backups/WAL.
5. One S3 `gate.json` coordinates **repository-wide** backup readers/producers, restore lifetimes/readers and the sole destructive operation owner. Owner and holders never expire. Successful holder admission is its own acknowledgement; no original Kubernetes API/controller is needed. The owner may clear itself only after all its destructive requests have conclusively completed and it has stopped issuing new ones. An ambiguous/crashed deleter is a genuine fail-closed admission barrier, not a lease someone can take over.
6. Retention remains off until explicitly configured; window plus minimum-full floor, dependencies and conservative WAL coverage. Unknown metadata or incomplete lists stop deletion. First release intentionally retains non-current-timeline WAL and small history/retirement metadata indefinitely. This limits reclamation, not recovery safety.

## 2. Executed evidence, pins and reproduction

### Real unmodified MinIO + SDK

Hosted run: [34069649315](https://github.com/djosh34/cnpg_backup/actions/runs/34069649315), **passed**, exact head `ff094d18dd7b7dc4e96549f7ee6c5ec20fb43a06`. Parent supplied the complete log and it was read. Ubuntu 24.04.4 / runner image `20260831.293.1`, Go 1.27.1 linux/amd64. Server: **RELEASE.2025-09-07T16-13-09Z**, source commit `07c3a429bfed433e49018cb0f78a52145d4bedeb`; downloaded unmodified binary SHA256 `7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f`. Server itself reports Go 1.24.6 and AGPLv3; it is a disposable test dependency, not shipped inside the product.

Actual assertions, separately passing under V2 and V4:

- Private-CA HTTPS succeeds with the configured CA; missing trust is rejected (no insecure TLS flag).
- Conditional create rejects different bytes with `PreconditionFailed`/412 and leaves original bytes unchanged; correct ETag CAS succeeds; stale ETag CAS fails.
- **16 concurrent** creates of one Backup-UID-like key produce **exactly one winner** per signature.
- A transport consumes a successful remote PUT response then returns an error: GET recovers the exact persisted bytes; repeating conditional create returns 412. This is actual remote success/response loss, not a fake storage implementation.
- **12 MiB**, **three parts** (5 MiB part setting, two workers), multipart upload; SHA256 verified on download. Lost initiation response leaves an orphan upload, as expected. Lost successful completion response still leaves readable data. Explicit abort followed by ListParts gives `NoSuchUpload`.
- Immediate reads/listing; **5 objects, 3 explicit two-object pages** per signature. Hosted explicit pagination used ListObjects V1; the ordinary SDK enumeration also used its default V2 path. The final snapshot uses explicit V2 pagination and corrected test-package CGO inspection; both subsequently passed in [hosted run 34070504941](https://github.com/djosh34/cnpg_backup/actions/runs/34070504941) at `fd21e31`, separately from this original V1-pagination run.
- DELETE is held **before reaching MinIO**, not just before returning a response. Gate remains owned while the request is pending. Only after remote response is consumed does the experiment release the gate and admit a reader. The DELETE transport in this subtest uses V4 in both outer subtests; it does **not** establish a V2-specific DELETE fault result.
- `TestSDK` passed in **1.261 s** (V2 0.74 s, V4 0.51 s). `CGO_ENABLED=0 go test -c` built the linked SDK test executable. The hosted package inventory enumerates **19 external modules**, not the hundreds of SDK linter dependencies.

Reproduce on disposable Linux amd64 with Go 1.27.1, curl, openssl:

```sh
bash docs/research/storage-experiment/run.sh
python3 docs/research/storage-experiment/gate_model.py
```

The script generates credentials into environment variables, never subprocess arguments; creates a private CA; starts only loopback MinIO; and kills/removes its own disposable process/files. Do not enable shell tracing. It is not a general external-endpoint testing command.

Local sandbox: official binary download and checksum succeeded, but even `minio --version` fails during `net.Interfaces()` initialization: `route ip+net: netlinkrib: address family not supported by protocol`. No host/network changes or server patches were made. Hosted execution resolved this environment-only gap. Local `CGO_ENABLED=0 go test -c` succeeded; corrected `go list -deps -test -f '{{if .CgoFiles}}{{.ImportPath}}: {{.CgoFiles}}{{end}}' ./...` produced **no CgoFiles**. (`-test` matters because this research module imports the SDK from its test.) Linked-module inventory matches the 19-module hosted inventory. The SDK's optional `rdma.go` imports C but is excluded by `!rdma` defaults; neither that tag nor RDMA options are permitted [S2]. This is build evidence, **not govulncheck/image scanning or approval of every dependency vulnerability**.

### Bounded state exploration

`gate_model.py` explored **620 states and 1,049 transitions**, two readers and one deleter, delayed/stale gate CAS, separate DELETE dispatch/effect/response, crash before/after effect, completed release, and durable crashed holds. Zero remote DELETE effects under an admitted hold. Reader pause is a stutter with the same durable hold. No Kubernetes state appears in the model, so deleting source Kubernetes objects cannot remove a hold or manufacture ownership.

The intentionally unsafe TTL variant produces this counterexample:

```text
D read gate → D acquire CAS → D send DELETE → D crash
→ timeout clears owner → R0 read gate → R0 admit CAS
→ original remote DELETE executes
```

Additional fixed assertions demonstrate: a successful DELETE retry plus HEAD absence cannot discharge the original pending DELETE; same-UID first-winner commit; permanent backup retirement; late conditional WAL creation rejected by a same-key retirement tombstone; unique gate generations avoiding empty-state ABA. **This is a bounded abstract planning model, not production-module DST, an exhaustive distributed proof, or a PostgreSQL recovery test.** Implementation must run the mandated fault/DST tests against actual Go orchestration.

## 3. Storage/SDK contract and failure ownership

### Required options and transport

- `Options{Secure:true, Region:<explicit>, BucketLookup:BucketLookupPath, MaxRetries:1, Creds:NewStaticV4(...)}`; `NewStaticV2` only when configured. `MaxRetries:1` means one SDK attempt, **not** one retry; zero selects the SDK default, so zero is wrong for destructive calls [S2]. Use one operation-aware retry loop, no nested blind retries.
- First release supports **path-style addressing only** and a fixed explicit region (`us-east-1` default for MinIO). Reject virtual-host/automatic addressing configuration rather than make untested DNS/TLS claims. No anonymous/ambient credential fallback, automatic signer switching, STS, presigned URLs, or discovery through redirects.
- TLS >=1.2, system roots plus configured PEM CA bundle. Reject endpoint userinfo, query, fragment, non-root path and redirects, including cross-host/HTTP downgrade. Endpoint host/port is an administrator-trusted destination, not backup metadata. No automatic environment HTTP proxy: explicit direct transport initially. A RoundTripper must reject 3xx responses before SDK/http.Client redirect processing; do not rely on default redirect behavior [S2]. Never enable SDK HTTP trace in product logs.
- Small conditional objects: `PutObjectOptions{DisableMultipart:true, SendContentMd5:true}`, then `SetMatchETagExcept("*")` or `SetMatchETag(etag)`; the SDK adds quotes. ETags are opaque CAS tokens, never SHA256. Use exact original JSON bytes for retries.
- Large artifacts: known disk file length, **explicit SDK `Core.NewMultipartUpload` / `PutObjectPart` / `CompleteMultipartUpload`**, 64 MiB parts, two part workers, maximum two concurrent artifact uploads. Supply per-part precomputed `Md5Base64` and bounded `io.SectionReader` inputs; keep ordered part-number/ETag results. Do not use the high-level multipart `PutObject` in production: its error-path deferred abort can issue deletion outside the GC gate, even with a canceled context. Failed uploads are left for gate-owned explicit abort. The experiment exercises both high-level buffering and these Core primitives; no custom S3 signing/protocol is needed. Compressed WAL is staged to a **seekable `*os.File`** and uploaded with single PUT. SDK `SendContentMd5` buffers the whole object when the reader is not a suitable ReaderAt/Seeker; passing a gzip pipe here would violate the memory bound [S2]. Disable trailing-header checksum features; V2 cannot use the SDK's forced trailing checksums. SHA256 application verification plus Content-MD5 transport checks work in both modes.
- Data reads: at most five total transient attempts, exponential jitter 200 ms to 5 s within deadline; data PUT/MPU retries must follow identity rules below. Gate CAS: at most ten conflict/reconciliation rounds per API call; return retryable contention without pretending admission. A callback retry does not release uncertain ownership.
- Defaults: connect/TLS 10 s, metadata request 30 s, individual data request 15 min, whole backup/restore work 24 h (configurable 1 min–7 d), WAL callback upload 120 s (configurable 10 s–15 min). Two dedicated WAL upload slots, two artifact streams/two part workers each, at most eight total data HTTP requests per process. No background early-ack WAL queue. These are bounds, not throughput/RTO guarantees. Timeouts **never** expire gate protection.

### Error classes

412 is a concurrency result: GET and compare/reconcile; not “exists therefore success.” 409 requires fresh state and, for multipart completion, a fresh attempt/upload rather than blindly completing the same upload [S1,S3]. Unknown upload on completion is ambiguous until the unique object is GET-verified; failed abort is not completed cleanup. 404/NoSuchKey is absence only when authenticated storage actually returned that error (and GET lazy errors are consumed). 403, invalid signature/region, TLS/certificate, malformed responses, corruption and unknown versions never become “end of WAL.” Connection loss, cancellation and 5xx after sending a mutation are **potentially applied**. Lists must consume all pages/errors; a partial list is not an empty catalog.

### Ambiguous destructive requests: a deliberately strict rule

All destructive requests (including retirement PUT, DELETE and MPU abort) are serial under one owner. No SDK/proxy retries, speculative duplicate DELETEs or S3 multi-delete batch API in v1. Observe the entire terminal response for each request before authorizing another. A successful response means that **that request's** effect has completed; it does not prove an earlier timed-out request completed. A definitively rejected precondition/auth request has no destructive effect, but any transport/5xx/body uncertainty leaves owner set. A connection timeout or local context cancellation is not a remote drain certificate [S3,S4].

If any sent destructive request's outcome becomes uncertain, **stop issuing destructive work and never clear this owner automatically**, even if HEAD subsequently says the victim is absent, an idempotent retry returns 204, or the clock advances. A later original response conclusively received by the same still-live request owner may resolve an outstanding request; a reconstructed request is not that response. The simple implementation can conservatively latch uncertainty for the owner lifetime. An in-process failure known to occur before dispatch can safely abort the operation. Process loss erases that proof, so a different incarnation never adopts the owner.

## 4. Exact repository identity, layout and schema

First supported repository format is **v1**. Schema fields below are required unless explicitly nullable; all plugin JSON is UTF-8, duplicate-field/unknown-field rejection, no trailing JSON, bounded before decode. IDs are lowercase canonical UUIDs; hashes lowercase 64-hex SHA256; PostgreSQL uint64 identifiers/LSNs are strings (system ID decimal; LSN canonical `HEX/HEX`), times UTC RFC3339Nano. Numeric sizes/counts are nonnegative integers fitting signed 64 bits. Opaque metadata cannot supply arbitrary S3 keys: derive and validate all keys from these IDs and enumerated artifact indices.

```text
<prefix>/v1/<repository_uuid>/repository.json
<prefix>/v1/<repository_uuid>/gate.json
<prefix>/v1/<repository_uuid>/backups/<backup_uid>/request.json
<prefix>/v1/<repository_uuid>/backups/<backup_uid>/attempts/<attempt_uuid>/claim.json
<prefix>/v1/<repository_uuid>/backups/<backup_uid>/attempts/<attempt_uuid>/manifest.pg.json
<prefix>/v1/<repository_uuid>/backups/<backup_uid>/attempts/<attempt_uuid>/data/<index>.tar[.gz]
<prefix>/v1/<repository_uuid>/backups/<backup_uid>/commit.json
<prefix>/v1/<repository_uuid>/backups/<backup_uid>/retired.json
<prefix>/v1/<repository_uuid>/wal/<8-uppercase-hex-timeline>/<validated-original-name>
<prefix>/v1/<repository_uuid>/gc/<operation_uuid>.json
<prefix>/v1/<repository_uuid>/probes/<random_uuid>/...
```

WAL keys have no `.gz` suffix because the key is also the permanent no-recreation slot. Live WAL body is raw/gzip bytes according to metadata; retired body is a small JSON tombstone. `.history` and `.backup` are accepted only by the validated PostgreSQL filename grammar; never concatenate callback paths. `repository_uuid` identifies lineage plus original writer cluster, not cluster name or system ID alone. Restored/PITR clones always use a **new destination repository UUID**, including when their system ID matches the source. No automatic reassignment of the original writer identity after source Kubernetes loss.

| Object | Required fields/meaning |
|---|---|
| `repository.json` | `schema:1, repository_id, postgres_major:18, system_identifier, wal_segment_bytes, writer_cluster_uid, created_at`. Immutable create, exact identity match on retry. Writer cluster UID is immutable and unique in the repository trust domain; copied names/config do not authorize a new lineage. |
| `gate.json` | `schema:1, repository_id, generation` (decimal uint64 string), `nonce` (fresh UUID for **every** replacement), `owner` (null or `{operation_id, process_id, kind:"gc"}`), `holders` (sorted array of `{id, kind:"backup"|"restore-lifetime"|"restore-reader", target_cluster_uid, operation_id, process_id}`). `owner!=null` requires zero holders. Generation overflow fails closed. Times may be diagnostics outside authoritative state; no expiry field. |
| `request.json` | `schema:1, repository_id, backup_uid, writer_cluster_uid, requested_kind:"full"|"differential", root_backup_uid` (null for full), `config_sha256`. First conditional winner freezes root and non-secret semantic config. Hash covers kind, root, identity, compression and capture-affecting settings, **not credentials/CA/timeouts**. Same UID with changed semantics is `BackupIdentityConflict`. |
| `claim.json` | `schema:1, repository_id, backup_uid, attempt_id, process_id, request_sha256`. Conditional create claims unique attempt namespace; never reuse a failed/crashed attempt namespace. |
| `commit.json` | `schema:1, repository_id, backup_uid, attempt_id, request_sha256, kind, parent_backup_uid` (null/full), `root_backup_uid` (self/full), `system_identifier, postgres_major:18, tool_version, timeline, checksum_version, started_at, stopped_at, start_lsn, stop_lsn, redo_lsn, bundled_wal_start_lsn, bundled_wal_end_lsn, manifest_bytes, manifest_sha256, tablespaces, artifacts`. Tablespace entries `{oid,name}`; paths are restored only through approved target layout mapping. Artifact entries `{index,role:"base"|"tablespace"|"wal", tablespace_oid` (nullable), `compression:"none"|"gzip", stored_bytes, raw_bytes, stored_sha256, raw_sha256}`. Original native manifest bytes remain unmodified. Parent must be a full in the same system/major/timeline/checksum state; no chains beyond full→differential. Native capture/recovery findings own exact extraction and tool invocation, not an invented filesystem format here. |
| WAL live metadata | `x-amz-meta-cnpg-format: wal-v1`, `...-system-id`, `...-raw-bytes`, `...-raw-sha256`, `...-stored-sha256`, `...-compression: none|gzip`. SHA256/length computed before upload. Content-Type octet-stream, not HTTP Content-Encoding gzip (avoid transparent HTTP decompression). Existing object GET/decompression must verify actual bytes, not just trust metadata. |
| WAL retired body | `schema:1, state:"retired", repository_id, name, raw_bytes, raw_sha256, gc_operation_id`; metadata `cnpg-format: wal-retired-v1`, JSON Content-Type. Permanent. Reads report expired/unavailable, not EOF disguised as a valid file. |
| `retired.json` | `schema:1, repository_id, backup_uid, commit_sha256, gc_operation_id`. Immutable permanent exclusion from selection. Commit and retirement records are never physically deleted by v1. |
| `gc/<id>.json` | `schema:1, repository_id, operation_id, process_id, cutoff, policy` (`window_seconds, minimum_fulls`), `victims` (derived backup/attempt IDs or WAL names/expected ETags and hashes). Immutable bounded plan, not a resumable ownership grant. A later **new** owner can replan remaining payload cleanup only after the old owner conclusively released. |

Serialization ceilings (reject above limits before publication/extraction, do not truncate); the finalized design selects stricter initial native caps of 64 MiB manifest, 100,000 files, 64 tablespaces, 66 artifacts and 1,023-byte native paths: prefix <=128 ASCII bytes, complete key <=1,024 bytes; repository/request/claim/tombstone <=64 KiB each; gate <=1 MiB and **1,024 holders**; commit/GC plan <=4 MiB; original PG manifest <=256 MiB streamed, <=2 million manifest file entries; <=4,096 artifacts and <=1,024 tablespaces per backup; artifact stored size <=512 GiB (64 MiB multipart gives <=8,192 parts, below S3 10,000); total declared extracted backup <=16 TiB. Configured workspace must still fit actual staging/reconstruction needs; these maxima do not reserve disk or promise workload performance. Tar entry paths <=4,096 bytes, reject absolute/traversal/device/FIFO/hardlink entries and unapproved symlinks; validate permitted native tablespace symlinks only inside isolated approved mappings. Expanded bytes cannot exceed both declared sizes and workspace cap; exact count/hash verified even when a gzip stream ends normally. Accept one gzip member only.

WAL segment size is validated from repository/native PostgreSQL identity (power of two **1 MiB–1 GiB**), never hardcoded 16 MiB [S7]. History/backup-history source files <=1 MiB; compressed segment <=raw bound +1 MiB. Chunked file I/O, not whole WAL in RAM. Repository inventories are disk-spooled/sorted with a configured 256 MiB memory ceiling; >1 million active commit records fails closed with an actionable capacity error rather than unbounded RAM. Retirement records remain outside active-selection memory but cost LIST/storage over time; no automatic compaction format in v1.

## 5. Publication and stale writers

### Initialize

Conditional-create `repository.json`; compare the entire stable identity if it already exists. Then conditional-create an empty gate. **Never recreate a missing gate for an already usable repository**: partial *first initialization* may finish only after a complete list proves no backups/WAL/GC data have ever been published; malformed/missing gate otherwise fails closed. Never delete or lifecycle-expire gate/identity objects. UUID nonce/generation makes each gate body different, even after returning to empty; otherwise content-derived ETags permit ABA and a delayed old CAS can become valid again.

### Backup

1. Admit a nonexpiring `backup` holder before selecting a parent, reading its manifest, capture or artifact writes. Backup holders and restore holders coexist; all block GC. One CNPG writer cluster per repository, verified against immutable identity on every callback; use a new random **process incarnation** on every process start.
2. Conditional-create UID request; on 412 read/reuse the identical winning request. Requested differential freezes a valid full reference; failure never invokes a full capture. Competing callbacks for the same UID may do duplicate capture, but use different claimed attempt UUIDs. They cannot write the same artifact keys.
3. Stage/verify native backup, preserve manifest, stream/compress to disk, compute both stored/raw hashes. Upload each completed artifact to its unique attempt key. A failed/ambiguous multipart attempt can be abandoned and a new attempt started; do not reuse that key for changed bytes or reuse an unknown UploadID. If HEAD/GET finds a completed unique artifact after an ambiguous response, fully verify its size/hash before using it. Unknown completion is not a committed backup.
4. Verify every referenced artifact and the capture/recovery prerequisites before conditional-create commit. The first valid commit wins. A same-UID losing candidate is **not** allowed to compare its bytes and overwrite the winner: read the winning commit, check request identity and validate its referenced data. Return the winner's durable result or a hard conflict/corruption error. Original success times come from that commit, not the retry clock.
5. Exact-byte GET after ambiguous commit PUT can establish successful publication; a 404 cannot establish that an earlier PUT will never arrive. Keep attempt holder on cancellation/uncertain remote writes unless all requests have conclusively drained. A crashed process's backup holder remains forever unless actual independent fencing is established outside this automatic protocol. This sacrifices GC liveness, **not restore admission**. It also protects a stale paused producer's parent. A restart may retry with a fresh holder/attempt, leaving the uncertain hold intact.
6. Live producer releases only its own holder after ceasing all further work and draining local tasks/remote writes. Losing/abandoned attempt payloads are eligible for GC only under exclusive gate ownership (no backup holder). Never delete a prefix immediately on cancellation while an MPU completion could still arrive.

### WAL

WAL archiving needs no global holder or global gate write; this avoids serializing every archive segment with recovery/retention. Logical WAL name is a permanent no-clobber slot. Single PUT with `If-None-Match:*`; on ambiguity or 412 GET and verify raw bytes/hash/length against local source. Identical live content is success; different content is `WALContentConflict`; a retired slot is `WALAlreadyExpired` (not a fresh archive success). New name/archive or live duplicate can continue while restores or an uncertain GC owner block retention.

To reclaim an unneeded WAL object's payload, the GC owner **replaces the same key** with the small retired JSON body using `If-Match:<live ETag>`. It never DELETEs the WAL slot. A delayed `If-None-Match:*` request remains false forever after retirement, so it cannot recreate a retired slot. A delayed replacement cannot retire a new incarnation because WAL slots are never reused. Preserve hash/name in the tombstone for diagnostics; do not present it as a restorable archive file. If retention listed an absent WAL key and a late old writer creates it afterwards, the result is merely an unreferenced live object to retire next time, not a lost dependency. Retention uses only proven coverage/keep floors, never a wall-clock age of newly appearing WAL.

This bounds stale effects without a general fencing service: gate-owned GC never gets taken over; backup stale work keeps its holder; WAL/shared commit writes cannot clobber or reanimate retired logical slots. A malicious/split-brain independent writer able to issue arbitrary S3 PUT is outside this cooperative protocol. A paused old primary on a **different** timeline is not authorized to change the new primary's timeline data; same-name/different-content must fail. New source cluster/clone identity cannot acquire original writer authority merely because system ID matches.

## 6. Gate protocol and restore lifecycle

### Admission and ownership rules

All callers GET exact gate bytes and ETag, validate identity/schema/invariants, copy the full state, increment generation and replace nonce, then conditionally PUT using that ETag. Never overwrite another holder while merging an update. No object-lock service, list-of-locks discovery or Kubernetes lease is involved.

- **Add holder:** permitted iff owner is null, preserving all other holders. Successful CAS (or reconciliation proving this exact holder present) is the durable admission acknowledgement. Only **after** acknowledgement may a restore select/list usable backups or fetch required payload. Concurrent readers serialize only the tiny CAS, then read concurrently.
- **Acquire GC owner:** permitted iff owner is null and holders empty. Random operation/process IDs are unique to this live execution, not a restart-reusable lease name. Ownership includes planning, retirement visibility changes, WAL retirement, payload DELETE and MPU abort.
- **Release holder:** only its live owning participant after its no-more-source-work boundary; never remove another incarnation's uncertain hold. The stable `restore-lifetime` holder has `process_id:null` and is jointly established by the owning target controller/sidecar using the deterministic Cluster-UID/bootstrap operation ID. Only the target controller's durable completion rule may remove that lifecycle holder; it is never permission to clear another incarnation's process-reader holder. Backup/restore-reader holders always carry a non-null fresh process UUID.
- **Release owner:** only same live execution, after it irrevocably stops sending destructive requests and all sent requests conclusively drain. It CASes owner to null, preserving any validated state. Release attempt uncertainty cannot justify sending new DELETEs. A new owner cannot appear until that release has actually happened.

On ambiguous gate CAS, do **not** infer “not applied” from a GET that lacks the token: the original request might still be delayed. Retry the **same** candidate/precondition, or CAS a fresh no-op generation/nonce barrier based on the latest GET. A successful barrier fences every older request against its old ETag; inspect the barrier's preserved state, then either recognize one's effect or retry admission. No data action before positively established admission. A holder found present remains present until its owner releases it, so later unrelated holder additions do not lose evidence of admission. Never repeat old JSON bytes/nonce at a newer generation.

A GC invocation issues at most **128 serial destructive requests** or 30 seconds of work between acknowledged requests, whichever comes first, then drains/releases. It yields at least one second before trying another batch, while pending callbacks retry admission. This is bounded batching/fairness, not a time lease; an ambiguous request never becomes safe at 30 seconds. Retention default interval one hour, configurable 5 min–24 h, at most one local worker; cross-cluster competing workers still use the same gate. No guaranteed lock-free progress under infinite contention.

### Durable restore through bootstrap, replay, pause and crash

Before restore selection, target plugin creates a **restore-lifetime** holder identified by target Cluster UID + restore operation UUID. Persist that ID in target recovery configuration **before** triggering bootstrap. This hold survives every bootstrap/replay phase boundary and an idle recovery pause. Parent's pinned CNPG research reports that replay actually occurs inside its recovery Job (including a direct-helper exit-255 workaround); wire these storage lifetime rules to that concrete lifecycle, not the stale assumption that all replay starts after the Job. Each bootstrap/restore-command reader incarnation also acquires a **restore-reader** hold before source reads; a new process never silently adopts or clears an old process's hold. A WAL callback may use its sidecar process holder for its entire source-reader lifetime, not per-file lock traffic.

Normal release is automatic but narrowly evidenced:

1. Target controller durably records the recovery operation as terminal and closes future source admission for that operation. Configure the target so restarted PostgreSQL/CNPG workers cannot resume that completed operation or use source restore_command; any genuinely new recovery gets a new operation ID and must reacquire.
2. Each live reader stops admitting callbacks, drains reads/subprocesses, and releases its own reader holder. **`pg_is_in_recovery()=false` alone is not enough** to clear another paused reader's hold. Target recovery completion/abort and absence of future source consumers must both be established by the lifecycle integration.
3. The lifecycle holder is removed only after that durable closed state and all known live-reader drains. Unknown/crashed reader holds remain, even after this target comes up successfully. A process retry or target Kubernetes-loss recovery can acquire fresh holds and restore; it does not erase the old ones.

Failed bootstrap, failed PITR target, paused `recovery_target_action`, worker/controller crash, lost target API, and uncertain cancellation all preserve protection. Never release from a `defer` merely because the bootstrap RPC returned, because a pod was deleted from an API, or because a Job finished downloading. A deleted/partitioned Kubernetes pod can still have an old process; a control-plane object disappearance is not S3 request fencing. If source WAL remains configured for standby/archive recovery, its reader hold stays until that use is permanently removed. The reconciled design wires these boundaries to CNPG's recovery Job: own reader drains on graceful sidecar shutdown; target controller releases only the stable lifetime holder after matching successful Job/all-Pod termination and durable completion recording. Another process's uncertain reader is never cleared from Kubernetes status; no timeout substitutes.

**Source Kubernetes disaster recovery:** with `owner:null`, a new target cluster with source S3 configuration, payload-read permissions and gate read/write can add its own hold even if every source Kubernetes object and controller is gone. Old backup/restore holders do not prevent new holders, so even lost source control-plane state plus crashed readers permits restoring. Source credentials/config and S3 identity are authoritative, not Backup CRs.

**Proven uncertainty boundary:** if gate has a GC owner whose process is lost/unreachable, it might have queued a DELETE that will reach S3 later, or might resume and send an authorized delete. Another cluster cannot distinguish that from a dead process with no remaining requests by GET/HEAD/LIST, time or a Kubernetes Lease. Consequently no automatic owner clearing/takeover, no admission, and no claim of always-available recovery in this state. Owner's brief explicitly permits this fail-closed case. External operator recovery would need independent proof of old-process/request fencing and a separate reviewed repair; credential revocation alone does not prove already-authorized requests drained. **No routine manual step in the normal supported path, and no pretend automatic repair for impossible uncertainty.**

Events/status: rate-limited `RetentionBlocked` Warning on participating/retention clusters, `RepositoryAdmissionBlocked` when GC uncertainty blocks a new restore; durable status mirrors gate operation ID/reason; metrics for blocked retention/admission and holder counts without operation-ID labels. Events/metrics are not the lock. No forced-clear or TTL configuration in v1.

## 7. Retention policy and concrete deletion sequence

Configuration: `retention.enabled:false`, `dryRun:true`, `window` required when enabling (1 h–3,650 d), `minimumFulls` 1–100 (example **14 d / 2**), interval above. Enabling requires explicit dryRun false to execute. Validate bucket capabilities and operator prohibition on lifecycle/admin deletion; do not grant writer credentials bucket policy/lifecycle administration.

Planner uses native start/stop/redo LSNs and parsed history ancestry, not lexical WAL names across timelines. Inventory includes every non-retired valid commit and full/differential closure, completed retirement records, and relevant WAL/history. Acquire exclusive owner **before** authoritative inventory/plan; a pre-lock dry-run is advisory only. Whole inventory/list/validation failure aborts without destructive action. Verify kept fulls/parents/manifests, identity, bundled-WAL boundaries and archive coverage requirements before deletion; metadata alone does not establish a gap-free recovery guarantee. Unknown/corrupt metadata or a missing required object stops deletion and reports degraded recovery coverage. Full artifact checksums remain restore/qualification checks; a HEAD does not prove payload integrity.

For cutoff C, on **each known timeline lineage with usable backups**:

1. Keep every usable backup completed at or after C. Keep the latest completed usable backup at/before C reachable on that path as window anchor; if none, keep earliest usable and explicitly report a shortened window.
2. Independently keep the newest `minimumFulls` usable full roots on that lineage (or all if fewer). Keep the latest usable backup/root even after all are older than C. Union across branches, then transitive full-parent closure. A differential never contributes an independent full count.
3. Keep all timeline `.history` and backup-history objects indefinitely. **No non-current-timeline WAL pruning in v1.** On the current writer timeline, keep from the earliest required redo/start segment across kept backups; for a kept ancestor-timeline backup, project its requirement onto the current timeline at the recorded fork segment and keep from there. If ancestry/fork/coverage cannot be proven, retain all WAL and block any dependent backup expiration that would reduce known coverage. A CNPG first-required-WAL hint may only lower this retention floor (keep more), never raise it past backup requirements. Missing/unknown hint does not create a new deletion entitlement.
4. Retire only backups outside the keep union; retire a full only after all differentials referring to it are retired. Parent checks use IDs/graph, not timestamp sorting. Remove no WAL at/after the floor. Unknown timelines never get aggressive pruning. Lack of new successes never ages out the last usable root.

Execution under that same owner:

- Persist bounded immutable GC plan. For an expired backup conditional-create `retired.json` **before** deleting any payload; its existence permanently removes commit eligibility. Never physically delete its immutable request/commit/retired records. A selected differential requires unretired full parent; corrupt contradictory records stop deletion.
- Serial DELETE only unique attempt manifest/artifact keys authorized by this plan. Check every response. Incomplete expiration keeps the backup unselectable and may leak payload; after conclusive owner release a new owner can inventory/replan leaked payload safely. A lost owner cannot be resumed merely from its GC plan.
- WAL retirement is the same-key conditional tombstone PUT described above, after backup retirements have conclusively completed. No physical WAL-key DELETE.
- Orphan attempt cleanup and MPU abort are also destructive and require the gate. With holders empty, no conforming backup producer is still active; do not infer inactivity from object age. Known upload IDs may be explicitly aborted, with a fresh cleanup context only while this owner remains held. A lost MPU-initiation response has no known ID: enumerate incomplete uploads under the unique attempt prefix, validate ownership, and abort under the gate. Do not use autonomous bucket abort lifecycle for the repository; it cannot obey restore's global “no deletion” promise.
- Any ambiguous destructive request poisons this operation as §3; leave owner and emit warnings. A conclusively acknowledged interrupted batch can release, permitting source-loss restores and another bounded cleanup pass. Never restore visibility to a backup after any payload removal.

Examples:

| Case | Result/reason |
|---|---|
| F 10 days old, D 1 day old, window 7 days | Keep D because in window, keep F as parent, keep anchor/WAL covering cutoff; F age alone does not authorize deletion. |
| Newest backup starts before target but stops after it | Not an anchor for that target. Keep/select an earlier completed backup; absent one, report unavailable rather than start from a too-new base. |
| Only old full after a month of failures | Keep it and its required WAL regardless of window; stale success metrics alert independently by requested backup type. |
| Promotion T1→T2 | Preserve history and all T1 WAL; T2 requirements start no later than needed fork segment for any retained T1 anchor. Requested differential needs a new same-timeline full first. Other observed branches retained conservatively, never cross-timeline lexical comparison. |
| GC gets owner immediately before long restore | Restore has no permission to read yet. GC finishes bounded batch/drains/releases; restore CAS adds hold and selects the post-GC catalog. |
| Restore admitted first, then pauses/crashes | GC cannot acquire while any holder remains. Other restores/backups may still add holders; WAL publication continues. |
| Backup retirement acknowledged, payload DELETE acknowledged, process crashes before gate release | Fail closed: owner persists even though this run's actual requests happened to finish. A different process cannot prove that fact from the surviving generic state. No unsafe recovery-from-log inference. |

## 8. Permissions, credentials and storage administration

Freeze the storage configuration fields below (the parent chooses the enclosing namespaced CRD/GVK, not different storage semantics). References are `{name,key}` in the same namespace; durations are validated Go-duration strings, UUID/size rules above. Unknown fields are rejected. Retention window is mandatory only when enabling it. Workspace capacity is explicit and cannot be inferred from this example:

```yaml
repositoryID: "00000000-0000-4000-8000-000000000001" # real deployment uses fresh UUID
s3:
  endpoint: "https://s3.example.invalid:443"
  bucket: "cnpg-backups"
  prefix: "production"
  signature: "v4"                  # v2 explicit compatibility mode
  addressing: "path"               # only supported value in v1
  region: "us-east-1"
  accessKeySecret: {name: s3-auth, key: accessKey}
  secretKeySecret: {name: s3-auth, key: secretKey}
  # sessionTokenSecret: {name: s3-auth, key: sessionToken} # optional V4 only
  # caConfigMap: {name: s3-ca, key: ca.crt}               # optional additive roots
  encryption: "bucket-default"     # only supported value in v1
compression: "gzip"                # none|gzip; gzip level fixed 1
io:
  connectTimeout: "10s"
  metadataTimeout: "30s"
  dataRequestTimeout: "15m"
  operationTimeout: "24h"
  walUploadTimeout: "120s"
  artifactUploads: 2                # 1..2
  partWorkers: 2                    # 1..2 per artifact; part size fixed 64MiB
  walUploads: 2                     # 1..2, separate from artifact work
retention:
  enabled: false
  dryRun: true
  window: "336h"
  minimumFulls: 2
  interval: "1h"
```

`endpoint`, bucket, prefix, signer, addressing and repository ID changes require a new configuration identity and are forbidden during admitted work. Only compression none/gzip is persisted; no pluggable compressor or arbitrary gzip level. Enforce connectTimeout 1–60 s, metadataTimeout 1–120 s, dataRequestTimeout 10 s–1 h and other ranges in §3. No TTL, unsafe TLS, forced clear, independent WAL expiry, client-managed encryption or fallback field exists.

Restore participants need **GetObject on source identity/gate/commits/manifests/WAL/artifacts/retirements**, prefix-constrained **ListBucket**, and **GetObject + PutObject on the exact gate key**. They need no payload PutObject, DeleteObject, bucket administration or source Kubernetes access. No read-only workaround is promised: automatic participation minimally requires gate writes. A policy cannot distinguish JSON holder updates from malicious owner changes; all clients allowed to write the gate are trusted cooperative plugin participants. Keep credentials scoped per repository; this is accidental-concurrency safety, not Byzantine protection against a compromised gate writer.

Writer adds PutObject on own repository backup/WAL prefixes; GC authority adds DeleteObject **only on attempt payload keys**, PutObject on backup retirement/GC/WAL slots, ListBucketMultipartUploads/ListMultipartUploadParts/AbortMultipartUpload on allowed artifact prefixes. Explicitly deny DeleteObject on repository.json, gate.json, requests/commits/retirements and WAL slot keys. Restoration alone does not need GC permissions. Minimal metadata checks require GetBucketVersioning/GetLifecycleConfiguration/GetBucketObjectLockConfiguration at writer/GC preflight; access denial stops destructive readiness, not silently assumed safe. No ListAllMyBuckets/CreateBucket in production: buckets preexist (the research root credentials are test-only).

First release supports **unversioned, no-Object-Lock repository buckets**; reject versioning Enabled/Suspended and managed WORM configuration for writes/GC. Versioned delete markers break permanent current-slot assumptions and do not reclaim payload versions. No independent live-object expiration, replication-induced deletion, admin mutation of gate/data, proxy retries or external writers within the repository prefix. S3-compatible storage must document the required consistency; finite probes cannot prove it universally [S1,S4]. Bucket default server-side encryption is transparent and operator-owned; client adds **no SSE headers** in v1. No SSE-C, client encryption/key service, per-object KMS selection or WORM bypass. Encrypted backends must satisfy the same capability tests and operator KMS rights; hosted test did not exercise encryption/KMS, versioning, Object Lock or appliance behavior. Do not claim those qualified.

Endpoint/bucket/prefix/repository/signature/addressing/region are immutable for an admitted operation. Secret fields are accessKey/secretKey (nonempty) plus optional sessionToken **only for V4 if explicitly supplied**, not ambient chain; V2 rejects token input. Secrets and PEM references are same-namespace and explicitly named, not a cluster-wide cache. Secret/CA changes build a fully validated replacement client snapshot for **new operations**; current operations retain theirs until completion. Old credentials and CA trust must overlap until old operations finish; removal may fail a running operation and leave a protective hold/owner, never change its semantics silently. Never expose secret values in args, errors, status, logs, manifests or metrics. A rollout is not permission to clear old uncertainty. TLS protects both V2 headers/payload and credentials in flight; SHA256 detects accidental corruption, not hostile storage able to rewrite payload and checksum together.

Non-S3 manager RBAC, native-image packages, release signing and CNPG security are the other scoped researchers' work. Reuse upstream libraries; retain SDK notices; MinIO server remains test-only with its own AGPL notice. Product remains all rights reserved. Run govulncheck and image/SBOM scans on actual product artifacts before release; this experiment is no waiver.

## 9. Reconciliation required in parent decisions

Replace stale design layout/lock proposals with this actual protocol: one gate, no surviving-source-controller dependency, permanent retirement records, no lease takeovers, no unconditional commit/WAL writes, no safe-delete claim based on HEAD or retry acknowledgement. Exact normal restore release must include durable terminal state **and** per-reader drain/uncertainty; the native/CNPG decisions must not release at bootstrap completion. Keep event/status ownership separate from S3 authority.

Explicit limitations are part of this chosen safe contract, not tasks left for implementation to invent:

- Ambiguous GC owner blocks all new protected restores; a crashed backup/reader holder blocks GC but **not** new restores. No automatic cleanup of uncertain process holders, no TTL. Hold-array exhaustion fails closed. This is the simplicity/liveness cost.
- Tombstones/history and non-current-timeline WAL may accumulate indefinitely. Data-size limits are conservative software bounds, not benchmarks. No online compactor/general distributed lock service is proposed.
- Current AWS conditional-write documentation requires V4. V2 compatibility is actually MinIO-tested, capability-gated elsewhere, and not Dell-certified.
- Hosted tests establish SDK/storage primitives and a concrete delayed DELETE trace. They do not qualify encryption, scoped IAM, virtual-host style, process/network partitions, production restoration, native PG checksums or the entire orchestration. Production DST, crash/replay integration, least-privilege policy tests, memory bounds and full recovery campaigns remain delivery acceptance tests, not unresolved storage algorithms.

Parent owns GitHub issue resolutions and final READY. This report supplies decisions/evidence without publishing product code or marking READY.

## Primary sources (pinned where source control permits)

- **S1** AWS, [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), retrieved 2026-09-07. If-None-Match/If-Match, concurrent winners, 409/412, multipart re-initiation, current-version/delete-marker rules, and explicit SigV4 requirement. AWS prose is a dated live reference, not an immutable source pin.
- **S2** minio-go at [`ce0e323c55c64964e6ad820ef0c6f5b286446aae`](https://github.com/minio/minio-go/tree/ce0e323c55c64964e6ad820ef0c6f5b286446aae): [static credentials](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/pkg/credentials/static.go), [conditional options/limits](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-put-object.go), [single PUT buffering and MPU cleanup](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-put-object-streaming.go), [complete MPU response/error parsing](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api-put-object-multipart.go), [client retries/transport](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/api.go), [explicit Core multipart primitives](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/core.go), [go.mod](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/go.mod), [non-RDMA stub](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/rdma_stub.go), [license](https://github.com/minio/minio-go/blob/ce0e323c55c64964e6ad820ef0c6f5b286446aae/LICENSE). Source clone and Go module used directly, not SDK popularity as semantic proof.
- **S3** AWS [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) (200 can contain an embedded error, request completion/parts/conditional conflicts), [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html), [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html) (in-flight part operations can complicate cleanup). No API supplies a “all earlier requests from failed process have drained” primitive.
- **S4** AWS [consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel): per-key atomicity and post-success read/list guarantees, not a multi-key transaction or fencing guarantee. Late-delete impossibility argument above is derived from that request model, not an undocumented AWS failure claim.
- **S5** MinIO server at [`07c3a429bfed433e49018cb0f78a52145d4bedeb`](https://github.com/minio/minio/tree/07c3a429bfed433e49018cb0f78a52145d4bedeb): [PUT conditions](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/object-handlers-common.go), [handler wiring](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/object-handlers.go), [conditional check under namespace lock](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/erasure-object.go), [network initialization](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/net.go). These were read locally; hosted run supplies behavior evidence.
- **S6** Go 1.27.1 [HTTP transport](https://github.com/golang/go/blob/go1.27.1/src/net/http/transport.go) and [request replayability](https://github.com/golang/go/blob/go1.27.1/src/net/http/request.go). Do not attach idempotency-key headers to destructive calls or insert a retrying proxy. Single SDK attempt is necessary but must also be enforced through transport configuration/review.
- **S7** PostgreSQL 18 [continuous archiving](https://www.postgresql.org/docs/18/continuous-archiving.html), [initdb WAL segment size](https://www.postgresql.org/docs/18/app-initdb.html), [recovery target pause](https://www.postgresql.org/docs/18/runtime-config-wal.html#GUC-RECOVERY-TARGET-ACTION); native workflow specifics remain in the parallel native findings. [Prior retention comparison](native-differentials-retention-locks.md) records pgBackRest's already-established window-anchor/dependency reasoning; this report does not claim that reasoning is novel.
