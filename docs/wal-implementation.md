# WAL callbacks (PR E)

Authority: issue19, issue10's resolution, [design §5](design.md) and the frozen
[CNPG WAL contract](research/cnpgi-contract-final.md#7-wal-service-capability-limits-guards-and-rewind).
This is WAL delivery, **not full backup/materialization, PITR or release qualification**.

## Concrete boundaries

- `internal/wal.Files` stages gzip **level1** or raw bytes into known-length
  private disk files, hashes stored/raw bytes, and uses conditional single PUT.
  It verifies remote bytes even after a successful PUT. A lost response or412
  succeeds only after full stored/raw verification against the source digest.
  Upload, verification and download spools are unlinked immediately after open;
  FD-only consumers retain seekability, and Linux reclaims their blocks on death.
  Failure to unlink fails the operation before writing scratch or acknowledging.
  Different raw content is `WALContentConflict`; a permanent retired slot never
  becomes successful archive or ordinary absence. Compression changes do not
  change identical-raw retry identity. No ETag-as-checksum or HEAD-then-PUT.
- The repository owns names/keys and immutable writer identity. `OpenWriter`
  checks Cluster UID, repository UUID, actual system ID, PG major and actual
  segment size. First initialization retains complete empty-inventory and
  conditional capability checks; timestamp races reuse only matching stable
  identity. Missing/invalid gate over used data remains a failure.
- Ordinary sidecars advertise only WAL Archive/Restore, not Status,
  SetFirstRequired, Backup or primary RestoreJobHooks. Recovery-job mode retains
  D's closed guard admission and unadvertised data services until G. Neither
  optional false nor absent archive-empty flags bypass immutable ownership.
  Ordinary callbacks reject parameter maps/source recovery tokens and read only
  the destination; retained externalCluster declarations confer no source access.
- Manager-owned immutable `wal.json` binds namespace/Cluster UID/repository and
  physical WAL directory. Callback names/paths are rejected before native/S3
  work. The two supported absolute paths are CNPG's PGDATA/pg_wal path and its
  declared separate WAL path. Physical symlink layout is checked; source files
  use confined nonblocking/no-follow opens, not arbitrary callback paths.
- Actual certificate-authenticated PG18.6 metadata checks include archive mode,
  WAL level, full-page writes, archive timeout and physical format. Standby/old
  primary flushing is allowed without relaxing primary-only native capture.
  Ordinary rewind reads mounted control identity even with PostgreSQL stopped.
  Source credentials and arbitrary SQL are never used.
- Restore reads exactly one requested filename (segment, promotion partial,
  history, backup-history),
  including nondefault power-of-two1MiB–1GiB segments. It verifies stored and raw
  hashes/exact lengths, accepts one gzip member only, fsyncs a private file,
  renames through an already-open confined WAL root, and fsyncs that directory.
  Allowed final basenames are the exact requested name, RECOVERYXLOG or
  RECOVERYHISTORY. It never follows the old destination symlink or reads adjacent
  segments, and has no negative cache/prefetch. Failure never acknowledges a
  partially verified file; directory-sync uncertainty is still failure.
  Publication uses one fixed `.cnpg-wal-restore` temporary per physical WAL root.
  A nonblocking exclusive flock on the open directory inode covers stale-temp
  removal, download, rename and sync. Contending callbacks fail/retry without
  touching the active writer. A dead process releases the lock; the next owner
  reclaims only that fixed private name. No prefix sweep, PID/age heuristic or
  recovery-guard/reader-holder takeover is involved. Old-version random temp
  names and unrelated files are deliberately not swept.
- Only a fully consumed authenticated GET `NoSuchKey` maps to NotFound. A bare
  HEAD404 is followed by bounded GET for classification; a missing repository or
  gate is **not** a WAL miss. TLS/auth/transport/corruption/retirement/local errors
  retain non-NotFound gRPC codes. Stock CNPG ordinary standby/rewind still folds
  errors to exit1; **do not use that command for primary latest/PITR recovery**.
  The product `wal-fetch` remains fatal255/unadvertised pending G's admitted plan,
  required-interval/bundled-local rules and target ownership integration.

## Native promotion compatibility (PR G)

A legitimate PG18 promotion `.partial` callback is durably stored byte-for-byte
in the restored Cluster's **new repository**, under its distinct original name.
Its physical size must equal the repository's WAL segment size; the suffix does
not imply a short file. Existing conditional publication, verification and
identical-retry rules apply. This auxiliary never supplies full-segment coverage,
frontier or latest-timeline evidence, and never substitutes for a missing full
filename. V1 retains it conservatively without GC pruning or new admission holds.
This is an implementation compatibility correction, not a change to the frozen
research grammar or evidence of executed CNPG qualification.

## Capacity and observability

Two whole WAL callbacks per process bound native metadata checks and spools,
independent of the adapter's two artifact streams/four part workers. The adapter
also retains independent WAL transfer slots and an eight-request process cap.
Each operation keeps a complete current Secret/CA generation and closes its own
client only after work returns. No local durability queue, background uploader,
cleanup DELETE/abort or premature acknowledgment exists.

Kernel finite-filesystem checks remain mandatory. WAL reserves up to six
segment-plus1MiB workspace files for two concurrent archive/verification paths,
plus16MiB overhead. Only Restore reserves two output segments plus16MiB on the
physical WAL filesystem. Archive and its earlier native metadata probe validate
finite source backing/identity without reserving unwritten output on PGDATA,
WAL or tablespaces. This permits backlog drainage under source disk pressure
with healthy storage and sufficient independent workspace. These are WAL-only
bounds, not F's database-sized native reservations. The actual
finite filesystem remains the hard stop under competing consumers. Upload work
uses configured WAL timeout (default120s), restore60s; shorter caller deadlines
win. Invalid trust/capacity never changes startup/Identity liveness into a
PostgreSQL/S3-dependent bootstrap deadlock.

Observability reuses **CNPG's existing PostgreSQL exporter** rather than adding
another per-sidecar counter ledger or network service. The source of archive
success/failure is PostgreSQL's actual synchronous callback outcome:
`cnpg_pg_stat_archiver_archived_count`, `failed_count`, last archival/failure
and elapsed-time gauges. Structured callback logs contain only success, elapsed
seconds and bounded gRPC code—no keys, paths, credentials or SDK strings.

[wal-monitoring.yaml](../config/wal-monitoring.yaml) adds pending `.ready` count
and oldest-pending age through CNPG's custom-query mechanism; reference it from
`Cluster.spec.monitoring.customQueriesConfigMap` as shown in that file. The actual
CNPG harness installs and checks these metrics. [wal-alerts.yaml](../config/wal-alerts.yaml)
provides failure, backlog-age and filesystem-pressure rules. Select the rules in
your existing monitoring stack and scope its PVC rule as appropriate. Idle
clusters are not declared lagging merely because no new WAL was generated.
Healthy `archive_timeout=60s` still adds upload/queue latency; current segment and
backlog may be lost, and an outage has no fixed RPO. No continuous PITR frontier is
inferred from last-success timestamps. F/H retain the manager-owned backup metric
contract; this reuse of stock WAL measurements does not create competing plugin
metric ownership.

## Tests and exact scope

`go test ./...` includes real adapter/HTTP WAL tests for duplicates with actual
byte oracles, gzip/truncation/multiple-member/corruption rejection, path/symlink
confinement, local failure, retired slots, lost/killed requests, TLS and transient
classification, immutable writer binding, fixed-seed72-operation replay and
independent artifact-saturation barriers. Regressions SIGKILL three subprocesses
at each upload/verification/download/restore I/O boundary, assert zero surviving
workspace WAL files/allocated blocks, at most one target publication temporary,
unchanged destination sentinels, and successful byte-identical retry. A live
contending restore cannot delete the active writer's temporary or foreign files.
These use the actual module with controlled storage, not a remote durability
claim. A separate controlled-storage DST
executes each of three24-operation traces twice, preserves durable state across
fresh Repository opens, and compares identical traces and independent raw-byte
oracles. A dishonest test store acknowledging nonexistent data is a negative
control. Two real SDK artifact streams/four
part requests stay blocked while the same production WAL module archives and
restores. Fuzz targets cover WAL names and actual verified gzip publication;
the latter uses the controlled store to avoid spending its exploration budget
on HTTP/TLS machinery. The seeded HTTP replay is reproducible input/protocol
evidence, not deterministic kernel/network scheduling or PostgreSQL recovery.
`hack/test integration` additionally requires actual MinIO WAL tests under both
V2 and V4: nondefault1MiB segments, compression-changing identical retries,
conflicts, history/backup-history, authenticated absence, and actual repository
GC same-key retirement followed by rejected late publication.

`hack/test cnpg-smoke` extends the existing actual kind1.35.8/CNPG1.30.0/PG18.6
matrix, without replacing any of D's15 required families. It builds checksum-pinned
MinIO and a **test-only** streaming proxy, consumes immutable subject images,
compares real archived bytes using independent curl/decompression/hash oracles,
and exercises stock operator callbacks plus a separate test-only Unix RPC driver
in the upstream main container. Neither test binary is in a product image or
substitutes for the product's unimplemented primary-recovery helper.

The E matrix requires segment/duplicate/conflict, forced failover/history routing,
a partial-PUT barrier followed by actual sidecar SIGKILL/no `.done`/fresh-incarnation
retry, transient/TLS non-NotFound, real bounded WAL-filesystem ENOSPC/no successful
restore and unchanged destination, and actual backlog/failure metrics. The
low-space regression restores S3 while WAL free space remains below Restore's
48MiB reservation (16MiB segments), requires the actual CNPG `.ready` backlog to
become `.done`, and verifies Restore still fails without changing its sentinel.
The proxy
changes only disposable test traffic; production has no fault controls. MinIO data
survives proxy and sidecar failures. First-failure evidence is retained rather than
silently rerunning failed scenarios.

First actual E segment/retry/conflict/failover-history plus all D15 families:
[run34097048872](https://github.com/djosh34/cnpg_backup/actions/runs/34097048872),
subject `dd0663871606e72c32d9f4ea35cd92fddf6ee992`, PASS. Foundation and real guard
namespace runs at that SHA also passed. This predates the expanded fault matrix;
The expanded six-family E matrix and all D15 also passed at
`291847c2c283809419c82ed5a72a69abc574e186` in
[run34099228544](https://github.com/djosh34/cnpg_backup/actions/runs/34099228544).
Current-SHA results/image digests belong to the delivery report/CI artifacts.
Local Docker/GCC/MinIO execution is unavailable; hosted checks provide those
separate system/race results. No full primary restore, SQL PITR replay,
source-plan materialization, retention policy or release qualification is claimed.

Preserved local first failures: the placement golden initially lacked the new
immutable WAL authorization projection (updated only that owned delivery shape);
the saturation mock returned another object's multipart-initiation Key and the
real adapter correctly rejected it before any part. The corrected test returns
the exact requested Key and reports early upload errors at its barrier. No
production integrity check, concurrency bound or test timeout was weakened.

Foundation34099228512 failed in the preexisting adapter's bucket setup before
product assertions: its product transport sanitized MinIO's startup503 to
TransientStorageFailure, hiding the exact XMinioServerNotInitialized code from
the narrow test barrier. A local regression reproduced one attempt/failure on
that exact caller. Setup now uses the same verified TLS transport with an
explicit-credential, single-attempt plain SDK fixture client. Only that startup
code is retried; auth, unrelated503 and already-owned-bucket negatives still
fail immediately. Product transport classification/retries are unchanged. The
original hosted log/artifacts and local red/green evidence are retained.
