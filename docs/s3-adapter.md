# Concrete S3 adapter (PR B)

`internal/s3store` implements the I/O boundary selected by [storage protocol
§§2–5,8](research/storage-protocol-final.md). Repository identity, gate admission,
publication reconciliation, WAL compression, catalog and retention are **not**
implemented here. The executable does not import this package yet.

## Caller contract

- Construct `New(Config)` from one fully validated explicit credential/CA
  snapshot. New work may use a replacement Store; old work keeps the original.
  HTTPS, explicit region/signature and path addressing are mandatory. CA roots
  are additive; malformed bundles fail. No anonymous chain, proxy, redirects,
  signer downgrade, Express/Outposts session acquisition, SSE or RDMA options.
  AWS V2 is rejected because this SDK would silently override it with V4.
- Keys are relative to the configured prefix. The repository caller derives
  `v1/<repository UUID>/...`; the adapter only validates the I/O boundary.
- `PutFile` requires an immutable regular `*os.File`, exact length and SHA256,
  plus either `Condition{Create:true}` or `Condition{Match:etag}`. It checks the
  spool before one Core single PUT with Content-MD5. No unconditional shared-name
  PUT exists. The returned ETag is an opaque CAS token, **not** content integrity.
  A successful mutation response is transport durability evidence; repository
  callers still verify content before publication/acknowledgment as specified.
- `UploadFile` is only for a fresh, already-claimed attempt-owned artifact key.
  It checks the complete file, initiates MPU, uploads 64 MiB sections using two
  workers and precomputed per-part MD5, drains both workers, then completes with
  ordered ETags. No high-level SDK MPU or error-path abort is used. Empty
  artifacts use conditional single PUT. Maximum artifact: 512 GiB/8192 parts.
- Returned `Upload` preserves key and known UploadID on failure. Lost initiation
  may have no ID; enumerate with `ListUploads` later. Do not reuse a failed
  attempt namespace, blindly retry completion, or infer completion from MPU
  absence. `ListParts` is diagnostic, not automatic resume.
- `Read` consumes bounded metadata GET bytes, including errors, before returning.
  `Download`/`DownloadWAL` write a **private caller-owned** file, verify exact
  stored length/SHA256 and fsync. Partial files on error remain untrusted. The
  caller verifies raw/gzip contents and owns atomic destination publication.
  Downloads restart from byte zero only on bounded transient read retries.
  `DownloadWAL` retains dedicated WAL slots; callers supply the helper deadline.
- Only an explicit GET 404 XML `NoSuchKey` is absence. HEAD 404 is deliberately
  unknown (it cannot distinguish bucket/key); auth/TLS/transport/malformed data
  never becomes archive EOF. Core GET avoids the SDK's lazy Object error trap.
- `List`, `ListUploads`, `ListParts` consume bounded pages. Object lists are
  lexically ordered. Callbacks must spool data and **not use a partial inventory**
  unless the entire call returns nil. At most one page is held; maximum one
  million entries/10001 pages, 8192 parts for one upload. Malformed roots,
  nonadvancing markers, cancellation, response-size excess and partial failure
  fail closed. Inventory sorting/active-record limits belong to the caller.
- `CheckBucketSafety` is required for writer/GC readiness, not read-only source
  restores. It rejects versioned/suspended buckets, Object Lock and conservatively
  any lifecycle rules. Denied/unsupported checks are not assumed safe.
  `CheckConditions(ctx, probeRoot, workspace)` checks create, failed create,
  missing-object CAS, successful CAS, stale CAS and subsequent GET/HEAD/LIST in
  a fresh UUID probe namespace under the caller-derived repository probes path.
  It returns cleanup keys even on error and never deletes them. Finite probes
  cannot establish arbitrary endpoint consistency or prohibit admin mutation.
- `Delete` and `Abort` are **explicit serial destructive-owner operations**.
  This package cannot grant admission: only the later repository module may
  invoke them after acquiring the gate and validating victims. No multi-delete,
  autonomous cleanup, retry, or cleanup-in-defer exists. Local probe temporary
  files are not remote objects and are removed by their creating call.

`*Error` exposes bounded `Kind` and `Ambiguous` fields. Error strings include no
SDK/server messages, endpoints, keys or secrets. Any uncertain mutation (including
transport loss, 5xx, cancellation, malformed/short response or unknown upload)
may still have applied. A subsequent successful retry or HEAD absence does **not**
prove that original request drained. The caller must retain uncertain ownership;
412/409 are concurrency outcomes, not duplicate success.

## Bounds and pinned SDK findings

SDK v7.3.0, commit `ce0e323c55c64964e6ad820ef0c6f5b286446aae`, was read directly
from the checksum-verified module cache:

- `api.go` uses `MaxRetries:1` as one attempt. Explicit region prevents location
  discovery. AWS endpoints override V2 and enable dual-stack rewriting; V2 AWS
  is rejected and dual-stack rewriting disabled. Transport also rejects any
  destination other than the exact configured authority.
- `core.go` exposes the required MPU primitives. Its object-list V2 function
  drops caller context, so object enumeration uses the context-aware SDK
  iterator with per-page response validation and bounds instead.
- `api-put-object-streaming.go` buffers unsuitable MD5 readers and high-level
  multipart paths defer abort. Core single PUT with a precomputed MD5 and a
  section of the validated file avoids both paths; the transport still receives
  exact content length, never an unbounded gzip pipe.
- `api-put-object-multipart.go` parses embedded errors in HTTP 200 completion.
  The adapter also consumes every bounded control response before SDK handling,
  because SDK `closeResponse` discards drain errors (including DELETE responses).
- `api-list.go` Core part pages retain quoted ETags; the adapter strips only the
  S3 quotes before validating opaque tokens. Missing/malformed list roots and
  truncation markers must not produce a false empty inventory.

Two artifact slots, two WAL slots and eight HTTP requests are hard **process**
ceilings, including overlapping Store snapshots; per-Store options can lower
concurrency. Each artifact has at most two workers. Hash/copy buffers are 128 KiB,
not part-sized or object-sized. Control response bodies are capped at 4 MiB,
headers at 64 KiB. Read retry count is five total, with exponential jitter; writes
are never retried. Default connect/TLS 10s, metadata 30s, data request 15m, operation
24h, WAL upload 120s; shorter caller deadlines always apply. No timeout releases
repository protection. Small conditional PUTs use the metadata request deadline.

Transport uses fresh HTTP/1 connections, no proxy, no compression and no HTTP/2:
this prevents net/http replay of bodyless destructive requests on reused
connections. This is a deliberate safety-over-handshake-throughput choice, not
custom signing. TLS/credential overlap must last until old operations drain.

## Tests and honest evidence scope

Use the existing `./hack/test fast` and `./hack/test integration --seed 1806
--images`. The integration entry now first runs production-adapter tests against
the same checksum-pinned MinIO download as the native harness. It generates a
private CA, ephemeral loopback endpoint, random credentials and isolated bucket;
no production endpoint input is accepted. `CNPG_S3_INTEGRATION=1` is internal test
selection, **not** a skip-on-failure switch. Research scripts are not product tests.

HTTP regressions exercise actual adapter methods: remote commit/response loss,
short mutation responses, embedded completion errors, interrupted MPU with no
abort, cancellation, two part workers and independent WAL service, >128 MiB
spools with a bounded allocation assertion, eight-request cap, credentials and
TLS, rejected redirects/proxies, five-attempt throttling, interrupted verified
GET, corruption, malformed/partial/paginated lists, bucket safety and capability
probe negative controls. Real MinIO tests repeat both signers/private CA with
all used production primitives, >1000-object pagination and concurrent creates.

Local environments that prohibit MinIO AF_NETLINK initialization cannot execute
those real tests. Hosted `foundation` integration and race checks remain mandatory
before merge; neither an unexecuted workflow nor green HTTP tests is qualification.
No Dell, encrypted/KMS or scoped-IAM compatibility is claimed by this slice.

`hack/godeps.py` inventories executable, production package and test closures
separately, checks all for CGO under production build flags, and copies unmodified
module/package LICENSE/NOTICE texts. `go-dependency-scopes.json` honestly records
that SDK code is currently production-package/test code, not executable linkage.
No unused import was added just to make inventory appear complete.
