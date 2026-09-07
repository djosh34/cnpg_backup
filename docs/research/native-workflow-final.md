# Native PostgreSQL workflow: final scoped findings

Research date: **2026-09-07**. Scope: resolutions for #5/#7 and PostgreSQL capture/recovery/workspace portions of #10/#12. These are concrete technical recommendations for the parent's issue resolutions, **not READY, product implementation, or release qualification**. Owner Q1–Q12 prevail over older issue text: native tools/disk, primary-only PG18, full-reference differentials, no fallback, tablespaces/separate WAL, asynchronous RPO. No agents, GitHub writes, production access, root services, or product code were used in this worktree.

## 1. Decision and exact pins

**Select PG18.6**, released 2026-08-13, the newest stable PG18 release listed by PostgreSQL's source archive when checked. Do not select snapshot packages whose version starts `18.6-4~...`. Pin the server and all data-path utilities to the same **18.6** minor for initial qualification; matching major is the upstream compatibility requirement, matching minor is our narrower first-release policy. Refuse a capture/reconstruction tool/server mismatch, including a partially rolled-out patch set. Qualify later patch updates against retained backups before changing the accepted set. PG17 introduced incremental backups but is explicitly not in first-release support. [S1–S4]

Reproducible native source pin:

- PostgreSQL tag `REL_18_6`, commit **`724edf9bde9d356724ad384a2e196edc3c9f80f7`**.
- `https://ftp.postgresql.org/pub/source/v18.6/postgresql-18.6.tar.bz2`
- SHA256 **`555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f`**, checked against upstream's `.sha256` and the downloaded bytes.

Actual local binary/server pin: **`PostgreSQL 18.6 (Ubuntu 18.6-3.pgdg24.04+1)`**, x86_64, from PGDG packages, not binaries built from the source tarball. Server reports GCC `13.3.0-6ubuntu2~24.04.1`. This distinguishes the inspected upstream revision from distro build inputs. Binary downloads and SHA256s are pinned in `experiments/native/local-tools.sh`; server/client/libpq are the same package release. Final CNPG/tool image digests and the base-image SBOM belong to the parent's packaging matrix; this experiment is not a container-image certification. [S1, S11]

Production native executable allowlist for this workflow:

| Tool | Justification |
| --- | --- |
| `pg_basebackup` | PostgreSQL's online consistency, replication transport, native changed-block discovery, manifests, bootstrap WAL |
| `pg_verifybackup` | Verify original full **and original native incremental** inputs and reconstructed output |
| `pg_combinebackup` | Native reconstruction, not our own page/diff engine |
| `pg_waldump` | Required-WAL parser; also invoked by `pg_verifybackup` |
| `pg_controldata` | Read version/system/checksum/WAL-segment metadata from privately extracted `global/pg_control` |
| `psql` | Fixed preflight/postflight SQL using upstream libpq, avoiding another SQL driver; no arbitrary user SQL or interactive use |

Keep `pg_verifybackup` and matching `pg_waldump` beside each other. `postgres`, `initdb`, `pg_ctl`, `pg_checksums`, shell, Python, package extractors and test `cp` are **experiment tooling**, not additions to the plugin's runtime allowlist. PostgreSQL itself remains CNPG's database container. Go binaries/dependencies remain `CGO_ENABLED=0`; runtime compression/extraction/S3 operations are Go, not external compressors or shell scripts.

The six allowlisted distro executables totaled about **1.5 MiB allocated** locally; this is **not** image size. `ldd` showed libpq, libc/loader, OpenSSL, zlib/LZ4/Zstd, Kerberos/GSSAPI, LDAP and transitive dependencies; `psql` adds readline/tinfo. PG server experiments additionally use ICU, io_uring and other server libraries. Packaging must inventory/scan the actual linked closure and preserve third-party notices; do not claim a pure-Go data image or ship the whole server package merely because some tools come from it. Extraction of a distro package does not execute its maintainer scripts. [S11]

## 2. Capture protocol: full and differential

### Eligibility and identity

Use an immutable operation/config snapshot and one base-backup worker per source/repository, protected by the repository decision's coordination. Connect to the **selected local primary instance**, not a load-balanced service that could silently switch peers between preflight, the backup connection, the WAL connection, and postflight.

Use a LOGIN/REPLICATION role and replication `pg_hba.conf` authorization. `-X stream` uses two replication connections; reserve **two walsenders and one temporary replication slot**, in addition to CNPG's other consumers. Do not specify `--no-slot`, create a permanent slot, or reuse a CNPG standby slot. A REPLICATION-only role successfully performed all three local captures. SQL monitoring separately needs CONNECT and access to the fixed system functions/settings queried; grant narrow EXECUTE privileges if the deployment has revoked defaults. Do not silently use a superuser. Exact CNPG role/certificate mapping remains the parent's CNPG-contract scope. [S2]

Supply libpq connection settings via a private `PGSERVICEFILE`/`PGPASSFILE` or certificate/key file paths and a sanitized environment; never secret-bearing `--dbname` arguments. Set `--no-password`, bounded connection timeout, `psql -X -A -t -v ON_ERROR_STOP=1`, a fixed search path and fixed SQL. For TLS connections use verified CA/hostname and client certificates as required by CNPG; the disposable Unix-socket trust experiment is not TLS evidence.

Preflight and postflight query server version, `pg_is_in_recovery()`, `pg_postmaster_start_time()`, `clock_timestamp()`, relevant public settings and tablespace OID/name/location mapping. The reconciled CNPG contract uses its `streaming_replica` certificate, without assuming EXECUTE grants on SQL control functions: read system/checkpoint/checksum/WAL-size control metadata using `pg_controldata` on mounted/extracted control files, retrying or failing on inconsistent reads. Compare with CNPG pod/primary identity. Commit capture Pod UID and postmaster start time so reference eligibility can reject loss of checksum continuity across server restarts. Validate:

- Major/minor = admitted PG18.6; normal 8 KiB database blocks, 1 GiB relation segments and matching physical architecture/build format. Initial binary experiment is Linux/amd64. WAL segment size comes from control metadata, not an assumed 16 MiB.
- Primary throughout, `wal_level >= replica`, `full_page_writes=on`, expected system identifier/lineage, checksum mode 0 or 1, and allowed CNPG-managed tablespaces/WAL layout. Reject unknown layouts, not silently omit them.
- A differential's root is a **committed full**, not D1, a synthetic full, or an arbitrary previous backup. Fetch its exact original manifest, check object size/hash and native manifest identity, and supply those bytes unchanged. Record root ID and manifest SHA256 in D2's commit. Root remains protected until D2 publication completes.
- Same timeline and checksum mode as F. No differential across a timeline transition; promotion/failover requires a separately requested new full. A primary identity/postmaster change during capture invalidates the operation even if a subprocess happened to exit zero. A new full on a restarted capture instance is the conservative initial rule when checksum-setting continuity cannot be established; no custom checksum-transition audit service. Preflight/postflight and `pg_control` checks must agree. Native tools alone do **not** enforce this whole policy. [S3, S7–S9]
- `summarize_wal=on`. Set initial `maxReferenceAge=8d` (maximum configurable 30d), `wal_summary_keep_time=14d` in the example profile, and require configured summary retention to exceed `maxReferenceAge + captureTimeout + 24h` slack. Do not treat summary setting/age as proof of gap-free coverage. The server checks actual summaries after choosing the new backup start LSN; missing/incomplete/stalled summaries fail. No remote-WAL download or full retry is a substitute. [S6, S7]

The postmaster continuity rule is intentionally conservative: a restart may cause an explicitly failed differential/new-full requirement, not surprise full-size fallback. Normal backup failures leave type-specific success timestamps unchanged and increment terminal requested-type failure metrics, with actionable status/Warning events.

### Commands and successful-publication requirements

Logical commands below are argument vectors launched directly, **not** shell-expanded production commands. `PGSERVICE` names a private fixed local-primary connection; `T` is a new private workspace. Choose spread checkpoint in production to avoid an unnecessary I/O spike (the experiment uses fast checkpoints).

```sh
pg_basebackup --no-password --pgdata="$T/tar" --format=tar \
  --wal-method=stream --checkpoint=spread --manifest-checksums=SHA256
# Differential: same command, with precisely this extra argument:
# --incremental="$T/reference/backup_manifest"

pg_verifybackup --exit-on-error --no-parse-wal "$T/tar"
```

Do not use `-D -`: stdout tar is incompatible with extra tablespaces and `-X stream`. `-T` is **ignored in tar mode**; `--waldir` is **plain-mode only**. `base.tar`, `<tablespace-OID>.tar`, `pg_wal.tar` and `backup_manifest` are completed local outputs. Keep default fsync and page-checksum verification; never pass `--no-sync`, `--no-manifest`, `--skip-checksums` or `--no-verify-checksums`. A checksum failure may leave files behind: directory existence is not success. [S2]

**PG18 can verify tar backups directly**, including native incrementals. But `pg_verifybackup` cannot parse WAL in tar format, even with `--wal-directory`; `--no-parse-wal` is mandatory for that format. Therefore perform a second, explicit required-WAL verification:

1. Safely extract **only** `pg_wal.tar` to `$T/walcheck`, and small metadata files (`backup_label`, `tablespace_map`, `global/pg_control`) from `base.tar` into private metadata directories. Preserve original manifest/map/label bytes; no live source paths are followed.
2. Validate the manifest and its `WAL-Ranges`. For each range run the same parser/range arguments that plain `pg_verifybackup` uses:

   ```sh
   pg_waldump --quiet --path="$T/walcheck" --timeline="$timeline" \
     --start="$start_lsn" --end="$end_lsn"
   ```

3. Require every parser exit to succeed. Catalog, label, control-file identity and the observed primary timeline must agree. First release expects a single range from primary capture; multiple ranges invalidate that support contract.
4. Go scans each completed tar, records per-member lengths/counts, and writes a bounded compressed disk spool while hashing both stored and raw archive bytes. Upload known-length seekable files with the explicit SDK Core multipart protocol in the storage resolution, not the SDK's auto-aborting high-level multipart path. Finalization reconciliation deliberately accepts the extra spool disk to provide exact pre-upload hashes/lengths, bounded file-backed reads and explicit error ownership; no whole-database memory buffer. Original native per-file SHA256 verification and transport SHA256 serve different purposes. WAL is **not** individually covered by the native manifest's file checksums; range parsing plus artifact hashes are mandatory. [S2–S5]
5. Only after all native verification, metadata checks, postflight and durable object upload does the repository publish commit metadata. A differential publishes as differential or fails. Do not publish a full under its UID or retry without `--incremental`.

Initial deadlines: connection 10s; fixed metadata query 30s; total capture/native verification 6h; whole backup/restore operation including transfer 24h (the shared validated operation timeout in the storage contract). Cancel/reap the whole process group (including the WAL-streaming child); do not leave a slot/client running or classify timeouts as completed backups. Transfer rate is separately configurable; PG's `--max-rate` **does not throttle streamed WAL**. Deadlines, WAL budget and actual-volume limits, not rate estimates, bound scratch growth. [S2]

## 3. Backup boundaries and required WAL

Store original manifest `WAL-Ranges` and original `backup_label` plus normalized start/end LSN, timeline, system identifier, checksum mode, server/tool version, segment size, root manifest digest, tablespace mapping and measured artifact inventory. Manifest `Start-LSN` is replay's required beginning; `End-LSN` is the earliest point replay may end while using the backup. The range is not a claim that no later transaction exists. Determine required segment numbers arithmetically for `[Start-LSN, End-LSN)`, using recorded segment size, and preserve timeline history for the selected recovery path. Do not compare WAL names lexicographically across timelines or use object timestamps as LSNs. [S5]

For timestamp selection retain the label's source start time and a **source-server timestamp read after successful pg_basebackup completion** as conservative completion upper bound. The native manifest has no wall-clock backup-completion field. Do not invent one from a file mtime or the plugin host clock. An authenticated backup-history file, when available, can provide the native stop time, but must not introduce a required asynchronous archive dependency. Conservative time selection excludes any backup whose recorded completion bound is after the target; it may select an older usable backup. Source clock assumptions/clock anomalies must be visible; PostgreSQL replay remains the ultimate target oracle.

**Commit does not wait for a second copy of bundled WAL in the ordinary WAL-object namespace.** `-X stream` bundles the bootstrap WAL and PG's client requests no archive wait for this mode. Uploaded, verified `pg_wal.tar` makes the base self-contained. Requiring the archive callback to catch up first would add an unnecessary availability gate; declaring the base recoverable does **not** establish post-backup PITR coverage. [S2, S10]

Keep bundled WAL with every committed backup. For a D2 restore, reconstructed output carries **D2's** bundled WAL; F's bootstrap WAL is needed to verify F but not as a request to replay from F's checkpoint. Do not merge every input's `pg_wal` directory or republish bundled segments into the archive namespace. Restore carries the selected base's local bootstrap WAL and invokes the plugin for subsequent archive WAL. Retention must preserve bootstrap artifacts and all source-archive segments needed by supported recovery coverage; a latest archived filename alone is not continuous coverage. Protection extends through actual PostgreSQL WAL use, not just combine completion.

Post-backup remote archive proof remains mandatory: write targets beyond D2's bundle, switch/confirm archival, then restore and query SQL. The local experiment proves this using a filesystem archive (not S3); CNPG/MinIO must repeat it, including missing/corrupt **post-backup** required segments. There is no bounded outage RPO. Healthy `archive_timeout=60s` only bounds low-traffic segment-switch delay, plus queue/upload time; it does not bound backlog under failure.

## 4. Reconstruction and tablespace/WAL mapping

Take the repository restore hold **before selection/download** under the parent's protocol. Download only F and selected D2, never D1. Verify recorded artifact SHA256/length and each original native manifest before reconstruction; perform native tar verification plus WAL-range verification as above. Decompress/extract with cumulative limits into **distinct** input roots. Ordinary files in the manifest refer to `pg_tblspc/<OID>/...`, independent of physical relocated path.

For each input, extract tablespace tar into a private per-input OID directory and manufacture `input/pg_tblspc/<OID>` symlinks to those trusted directories. Preserve the original `tablespace_map` bytes at this stage so original-manifest verification remains possible. Do not let the map dictate writes to source paths. Keep input `pg_wal` as a **directory**, not a symlink: `pg_combinebackup` skips ordinary symlinks, including a `pg_wal` link. [S8]

```sh
pg_verifybackup --exit-on-error "$T/F"
pg_verifybackup --exit-on-error "$T/D2"
pg_combinebackup --copy --manifest-checksums=SHA256 \
  --tablespace-mapping="$T/D2-ts/<OID>=$FINAL_TS/<OID>" \
  --output="$FINAL_PGDATA" "$T/F" "$T/D2"
pg_verifybackup --exit-on-error "$FINAL_PGDATA"
```

Repeat `-T` for **every tablespace in D2**, with absolute paths and tool-required escaping for `=`. Mapping's old path is the link target in the **last input**, not the historical server path or F's extracted location. No hardlinks/reflinks/copy-file-range optimization initially. Inputs are never started or mutated after verification. Output PGDATA and mapped tablespace directories must be empty/private and PostgreSQL must remain stopped. `--dry-run` can catch relationship/mapping errors first; it is not a size or integrity oracle. [S3, S8]

**Critical mapping correction:** combine copies the original `tablespace_map`; it does not rewrite it to the `-T` paths. Verify the output first, then remove the already-verified `tablespace_map` before startup so PostgreSQL keeps the explicitly constructed mapped links instead of recreating historical source links. Likewise full-only restore verifies its original manifest/map first, creates trusted final links, then removes that map. Keep original manifests/maps in immutable backup objects and provenance; do not silently modify a native manifest to conceal verification errors. Mapping removal and CNPG recovery-configuration injection are explicit post-verification bootstrap transforms.

For a separate WAL PVC, combine initially writes ordinary `$FINAL_PGDATA/pg_wal`. After output verification, copy its completed files into a private directory on the target WAL PVC, verify copied lengths/hashes, fsync files/directories, remove the old directory and replace it with the trusted final symlink. Use same-filesystem rename only where actually valid. Budget duplicate WAL while crossing volumes. The experiment's `mv` covers local same-filesystem relocation, **not** cross-PVC copy crash safety. CNPG must not start PostgreSQL until relocation/configuration/fsync has completed. Restart of an incomplete restore rebuilds incomplete target contents under the retained hold; no partly transformed output is declared ready.

Tablespace OIDs present only in F remain input history, not extra output tablespaces. A newly added/dropped tablespace must be mapped from the actual selected backup inventory and CNPG desired volumes. Unknown source links, unmanaged symlinks, overlapping/nested destination roots and missing PVC mappings fail preflight. Tablespace DDL during capture must be reconciled against the actual backup map/inventory, not assumed identical to the preflight query.

## 5. Integrity and secure extraction rules

**Verify each original input, not only the synthetic full.** In the executed negative control, flipping a byte in D2's `INCREMENTAL.1249` caused native input verification to fail, but `pg_combinebackup` followed by `pg_verifybackup` of the synthetic output succeeded. Combine had computed a new manifest over corrupted reconstructed bytes. Verification of that new manifest cannot authenticate its inputs. This is observed behavior, not just a documentation caveat. [S3, S8]

Native tool verification checks manifests/file inventory/file checksums/WAL parsability. It does not prove SQL recovery, detect every semantic WAL error, authenticate a malicious writer that rewrites both data and metadata, or verify every ignored file. Page checksums are separately checked by capture when enabled. Combine does not recalculate PostgreSQL page checksums; a reference with checksum mode off followed by mode on can yield invalid pages. Require a new full on checksum-state changes; never invoke `pg_checksums` to repair a backup silently. [S2–S4]

Production extractor requirements (Go, not this research script's trusted-fixture extraction):

- New mode-0700 operation root, exclusive writer, directory-relative confined filesystem operations; no string-prefix-only path checks. Reject absolute paths, `..`, empty/ambiguous components, NUL, overlong paths, duplicate entries, sparse encodings, hardlinks, devices, FIFOs and all archive-provided symlinks. Accept only supported regular/directory headers; canonicalize before duplicate detection. Do not preserve owner IDs, setuid/setgid or executable modes from backup metadata.
- No writes through previously extracted links. Create the **only allowed** tablespace/WAL symlinks from validated CNPG destination mappings after extraction, outside the generic extractor. Inspect archive structure/counts before passing hostile archives to native tools; all input roots and native subprocesses remain unprivileged/resource-limited.
- Decode native manifest `Path`/`Encoded-Path` with limits, enforce supported schema (observed PG18 version 2), no duplicate files/OIDs or integer overflow, and reject unexpected tablespace artifact names. Preserve original bytes, including the native self-checksum. Unknown archive encodings/formats fail closed.
- Bound compressed length, decompressed tar length, member count, member length, total extracted bytes and actual filesystem use independently. Do not use a gzip expansion ratio: very compressible legitimate relations/WAL exist. Read to completion to detect gzip CRC/truncation/trailing material; a valid tar end marker is not permission to ignore unbounded trailing compressed bytes.
- Verify originals before sanctioned recovery/configuration transforms. Never apply arbitrary `--ignore`/`--skip-checksums` to get a damaged backup past the gate. Refuse unexpected ignored-file payloads; CNPG replaces recovery connection/configuration, never executes archived settings blindly.

## 6. Concrete capacity contract (no native binary-header parser)

Use explicit byte budgets, not a presumed differential compression ratio. **Do not parse `INCREMENTAL.*` binary headers to estimate output.** PostgreSQL owns their interpretation. Native output may approach a full backup even for a legitimate differential, and data/WAL can grow during capture. A capacity estimate is not a reservation.

Definitions, all sums including tablespaces and metadata unless called out:

- `A`: uncompressed tar artifacts plus original manifest; `W`: extracted bundled WAL.
- `Cmax = Amax + max(1 GiB, ceil(Amax/100))`: enforced aggregate compressed-spool cap for capture (also valid for compression=none). Exceeding it fails the operation, rather than relying on a compression ratio. This disk-backed spool is the reconciled storage protocol's known-length/hash input.
- `C_F`, `C_D`: completed compressed downloads (use exact committed lengths).
- `E_F`, `E_D`: extracted input sizes, conservatively rounded per entry to filesystem allocation units; obtain lengths from bounded tar/original-manifest inventories, not native incremental semantics.
- `Rmax`: configured **`maxRestoredBytes`**, a hard output budget covering PGDATA, all tablespaces, bootstrap WAL and generated manifest. It is not inferred from D's small byte size.
- `L`: duplicated final WAL during cross-volume relocation, bounded by selected backup's WAL budget.
- `H(n)`: conservative filesystem accounting reserve, `8 KiB * (regular files + directories)` in that phase. For other filesystems use the larger documented allocation unit/metadata overhead.
- `S(x) = max(1 GiB, ceil(0.10*x))`: spare-space margin, separate from hard file/data limits.

Peak conservative reservations:

| Operation | Required capacity before phase starts |
| --- | --- |
| Capture | `Amax + Cmax + Wmax + H + S(Amax + Cmax + Wmax + H)`; includes bounded compressed spool |
| Full restore | `C_F + E_F + L + H + S(...)`; E_F is extracted directly into isolated final destinations, not copied twice |
| Differential restore | `C_F + C_D + E_F + E_D + Rmax + L + H + S(...)` |

Differential scratch PVC holds downloads and both inputs; PGDATA/tablespace target PVCs hold Rmax; WAL PVC holds final WAL. Validate **per-filesystem** budgets/free space (including each tablespace and the WAL volume), not merely the sum. Aggregate formula covers a conservative peak even when files could be deleted earlier. Budget Rmax among CNPG target volume limits explicitly; each target filesystem must have its allocation plus margin. Full-only restores know exact extracted sizes, whereas native synthetic output uses the configured maximum. A manifest-count-based RAM estimate or SQL database size never replaces these hard budgets.

Initial bounded configuration selected for implementation (limits are policy, not measured scalability promises):

| Limit | Default / initial allowed maximum |
| --- | --- |
| Uncompressed backup artifacts `maxBackupBytes` (= Amax) | 32 GiB / 1 TiB |
| Bundled WAL `maxBootstrapWALBytes` (= Wmax) | 8 GiB / 256 GiB; also inside Amax |
| Synthetic output `maxRestoredBytes` (= Rmax) | 32 GiB / 1 TiB; explicit per-volume allocation |
| Operation aggregate workspace ceiling | 256 GiB / 8 TiB; insufficient formula fails before work |
| Manifest bytes / file entries | 64 MiB / 100,000 entries per input |
| Tablespaces / tar artifacts | 64 / 66 (base + WAL + tablespaces) |
| Path bytes | 1,023, consistent with PG `MAXPGPATH`; reject unsupported names |
| One regular file | 1 GiB + 1 MiB; enough for native relation/incremental-file overhead; unexpected larger arbitrary PGDATA files unsupported |
| Concurrent native operation | 1 per instance/repository; WAL callbacks have independent bounded resources |
| Data-path cgroup memory limit | 3 GiB initial including Go and all native children; no fictional separately enforced per-child RSS limit; OOM is failure, never success |

Set finite byte limits before launching native writers; no memory-backed emptyDir. Dedicated filesystem/PVC capacity or filesystem quota is the **hard stop** for native tools. `emptyDir.sizeLimit`, periodic `statfs`, Kubernetes ephemeral-storage eviction and process polling alone are **not instantaneous allocation enforcement**. Use an exclusive disk-backed workspace with a real finite filesystem/quota; preflight requires free bytes after margin and reservations for concurrent non-backup use. Poll actual use/free space at most once per second, stop at the budget/margin threshold, and let the hard backing capacity catch growth between polls. Shared unbounded scratch without a hard backing limit is unsupported. Do not assume Kubernetes PVC requests are filesystem quotas: inspect the mounted filesystem and storage provisioner's limit.

Use bounded stdout/stderr rings (1 MiB per subprocess) and terminate on any input/manifest/decompression/native-process/space limit breach. Clean only owned operation roots, preserve diagnostics and committed repository objects. On restore, no PostgreSQL startup until all output validation/relocation passes. During capture a full filesystem may cause native ENOSPC before the monitor fires; that is an expected failed backup with cleanup and no commit, not evidence the limit was never reached. No hardlinks, reflinks, compression savings or thin-provisioning optimism in capacity preflight.

## 7. Executed experiment and durable evidence status

Reproduction (use fresh absolute directories on a disk-backed filesystem):

```sh
bash docs/research/experiments/native/local-tools.sh "$TOOLS"
source "$TOOLS/env.sh"
bash docs/research/experiments/native/native-workflow.sh "$EXPERIMENT"
```

`local-tools.sh` downloads pinned PGDG Ubuntu packages and ICU/io_uring runtime packages without installation. It prefers `dpkg-deb --extract` on Ubuntu; the fallback requires **Python >=3.14** for zstd package members. The actual workflow needs Python >=3.12 for its uncompressed tar fixtures. All remaining native shared libraries must pass the script's complete `ldd` preflight. No TCP listener: a private Unix socket, mode-0700 experiment root, non-root UID, synthetic data, and disposable archive directory. `trap` stops both owned servers; output data/logs remain for collection.

**Local actual results:** Fedora 43, x86_64, Python 3.14.3; matching PG18.6 server/client packages executed successfully on 2026-09-07. The final native workflow ran in `.native-experiment-run3` (an uncommitted temporary directory). All assertions passed:

- A REPLICATION-only role captured tar F, D1 from F, D2 from **the same F**; page checksums on; one external tablespace and separate source `pg_wal` path.
- Tar verification succeeded for all three; tar WAL verification without `-n` failed with the expected limitation. Extracted F/D2 passed original-manifest and WAL verification; standalone `pg_waldump` range verification passed.
- D2 contained **670 native incremental files**. Byte corruption failed original-input verification. Deliberately combining that corrupted input and verifying the synthetic output succeeded: the negative control proves why original-input verification is mandatory. Damaged synthetic output was never started.
- Withholding required bundled WAL caused `pg_waldump: could not find any WAL file` / `WAL parsing failed for timeline 1`.
- F+D2-only ordinary-copy combination and synthetic verification passed. D1 was not extracted or supplied. Tablespace mapping and separate WAL relocation worked.
- D2 range: timeline 1, **`0/7000028`–`0/7000120`**. Later named target: **`0/8000238`**, segment **`000000010000000000000008`**, proved absent from D2's bundle and present in the archive. Recovery log confirms archive segment reads and stopping at `native_target`.
- SQL after recovery: `pg_is_in_recovery() = false`; main table changes from **both D1-era and D2-era** transactions present (`d1,d2`); tablespace rows exactly **`1:d2,2:before-target`**; `3:after-target` absent; tablespace path and separate WAL symlink match trusted restore destinations.
- Removing previously completed summaries while summarization remained enabled caused native failure: summaries required from **`0/3000028` to `0/B000028`** but incomplete. No replacement full command exists in that failure path.
- Offline `pg_checksums --disable` changed control checksum version **1 → 0**. This demonstrates the metadata signal, **not** an implemented plugin checksum-transition rejection test. Both servers were stopped.

Measured uncompressed artifact bytes (tiny fixture, not a benchmark):

| Artifact | F | D1 | D2 |
| --- | ---: | ---: | ---: |
| `base.tar` | 48,184,320 | 3,918,848 | 3,918,848 |
| tablespace tar | 19,968 | 12,288 | 12,288 |
| `pg_wal.tar` | 16,778,752 | 16,778,752 | 16,778,752 |
| native manifest | 193,920 | 200,311 | 200,311 |

F's range was `0/3000028`–`0/3000120`; D1's was `0/5000028`–`0/5000120`. Small differentials still incurred an entire WAL segment. No claimed throughput, RTO, universal compression ratio, power-loss durability, S3 behavior or memory scalability is derived from these sizes.

**Hosted research PASS:** [run 34070325630](https://github.com/djosh34/cnpg_backup/actions/runs/34070325630), exact parent research SHA **`9cfa3492ce1dcca7611f0a7e04d5860150bb31f8`**, Ubuntu 24.04.4 runner image `20260831.293.1`. Inspected the returned full job log: all native assertions above passed, including the corruption negative control, F+D2 SQL PITR, missing-WAL and missing-summary failures. The hosted F `base.tar` was 48,135,168 bytes (local 48,184,320); D1/D2 sizes and tested WAL/target boundaries matched. This is durable public **research experiment** evidence, not product qualification. Preserve the real first failures:

1. [Run 34069841922](https://github.com/djosh34/cnpg_backup/actions/runs/34069841922): setup failed because Ubuntu Python3.12 tarfile cannot unpack zstd `data.tar` inside PGDG packages. Fixed by preferring `dpkg-deb --extract`, fallback Python >=3.14.
2. [Run 34069998633](https://github.com/djosh34/cnpg_backup/actions/runs/34069998633): package extraction passed, PostgreSQL loader failed for missing `liburing.so.2`. Fixed by adding checksum-pinned Ubuntu liburing2 and collecting all missing dependencies with `ldd` before starting any binary.

Neither earlier failure reached a PostgreSQL backup operation; the successful third run confirms the diagnosed harness portability fixes. Parent executed/published these hosted jobs; this researcher made no GitHub writes. `native-workflow.sh` remained unchanged across these setup fixes (10,092 bytes; SHA256 `9619d0d23e7afe7f460c37d6ff230ce03e0698cc9587440dec7bc23647a4961a`). Current `local-tools.sh` is 3,252 bytes; SHA256 `3b9ed0c1e2e4114974a83cece1a5a0894bdfa42bf6d813749637ca60cfdbff89`. That final setup script was also rerun locally: all five package hashes, all native dependency checks and all ten tool version invocations passed. `bash -n` passed for both scripts.

## 8. Contradictions resolved and remaining evidence boundaries

- Older #7 text allowing configured fallback is superseded: **no fallback configuration**.
- “Verify after combine” alone is unsafe; originals must be verified independently. Tar verification support exists in PG18, but tar WAL parsing does not. Capture need not extract all data merely to verify it.
- Tar `-T`/`--waldir` are not the mapping mechanism. `pg_combinebackup -T` does not rewrite `tablespace_map`, and a WAL symlink in input is skipped. Explicit extraction/layout transforms above resolve both.
- Native cross-timeline differentials can work (upstream timeline/promotion tests exist); refusing them initially is an explicit product support boundary, not a false statement that PostgreSQL never supports them. Standby promotion-during-backup fails upstream; primary capture additionally needs CNPG identity fencing checks. [S7–S9]
- Go chunking would still require the online-backup protocol, changed-file lifecycle/reconstruction, manifest/GC/dependency integrity, WAL handling and retention. Native tools remove that engine, not repository publication or restore orchestration. No custom native binary-header reader or chunk-store abstraction is justified for capacity estimation.
- **Not yet experimentally qualified here:** CNPG mounts/TLS/failover fencing and promotion during capture; checksum-transition plugin rejection; cross-PVC WAL-copy crash handling; malicious extraction/decompression/resource-limit tests; non-16-MiB WAL; tablespace add/drop races; S3/MinIO archive replay and corruption; remote post-backup missing-WAL negative recovery; retention/restore holds; packaging SBOM/vulnerability checks. These are explicit implementation/qualification tests of the selected mechanisms, not algorithms left for the owner to design. The parent must decide whether any specific planning evidence gap blocks READY; this researcher makes no READY claim.

## Pinned primary sources

Unless explicitly stated otherwise, source links below use upstream commit `724edf9bde9d356724ad384a2e196edc3c9f80f7`, not moving PG18 documentation.

- **S1** [18.6 release source/date](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/release-18.sgml), [official source directory/checksums](https://ftp.postgresql.org/pub/source/v18.6/).
- **S2** [pg_basebackup reference](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/ref/pg_basebackup.sgml): formats, summaries, permissions, WAL connections/slots, checksum/fsync/rate options, stdout/mapping restrictions.
- **S3** [pg_combinebackup reference](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/ref/pg_combinebackup.sgml): ordered inputs, copying, checksums, verification limits.
- **S4** [pg_verifybackup reference](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/ref/pg_verifybackup.sgml), [implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_verifybackup/pg_verifybackup.c): tar support/limitation, native manifest verification, ignored files, `pg_waldump` range invocation.
- **S5** [manifest schema/WAL ranges](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/backup-manifest.sgml).
- **S6** [physical backup/PITR/incremental requirements](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/backup.sgml), [WAL settings](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/doc/src/sgml/config.sgml).
- **S7** [server incremental preparation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/backup/basebackup_incremental.c): identity/timeline validation, summary coverage and explicit failures.
- **S8** [combine implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/pg_combinebackup.c): `scan_for_existing_tablespaces`, mapping requirement, WAL/symlink handling, copied map and reused/generated checksums.
- **S9** [upstream timeline test](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/t/003_timeline.pl), [promotion test](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/t/008_promote.pl), [input integrity/chain tests](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_combinebackup/t/005_integrity.pl). Inspected source, not a claim these upstream tests were executed locally.
- **S10** [pg_basebackup implementation](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/bin/pg_basebackup/pg_basebackup.c): `WAIT 0` when streaming/fetching WAL, end-position streaming completion.
- **S11** [PGDG binary pool](https://apt.postgresql.org/pub/repos/apt/pool/main/p/postgresql-18/), [Ubuntu ICU pool](https://archive.ubuntu.com/ubuntu/pool/main/i/icu/), [Ubuntu liburing pool](https://archive.ubuntu.com/ubuntu/pool/main/libu/liburing/): exact filenames/hashes in the rootless setup script; package `control.tar` dependency lists inspected. These are package sources, not proof all target images have those dependencies.
