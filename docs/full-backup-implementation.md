# Native full capture (PR F)

`internal/postgres` owns fixed certificate-authenticated local-primary capture,
bounded Go input scans, disk compression, and direct shell-free verification.
`internal/cnpgi/backup.go` translates the actual CNPG Backup RPC and delegates
commit-last publication to `internal/repository`. Differential and primary
RestoreJobHooks remain unimplemented; there is no fallback option.

Every callback validates explicit `backupType` and primary target, frozen Cluster
placement and CNPG's selected Pod. One process-wide slot serializes native
captures, independently of the two WAL callbacks. Same-UID callbacks serialize
and reuse the repository's verified durable winner. CNPG terminal Backup objects
are not silently restarted. Each new attempt retains its own certificate/config
snapshot and repository holder until native/remote work drains. Uncertain remote
mutations leave the repository holder intact.

Native pre/post checks compare PG18.6, physical control identity/checksum/timeline,
source role/settings/tablespace OIDs, and postmaster start. `pg_basebackup` uses tar,
streamed WAL, spread checkpoint, SHA256 manifests and default fsync/checksum
verification. Original manifest/label/map/control bytes remain in committed
inputs. Every native tar/manifest is bounded and scanned before verification;
`pg_verifybackup --no-parse-wal` is followed by direct Go `pg_waldump`. The initial
primary-only format rejects multiple WAL ranges explicitly, not by skipping later
ranges. Go compresses to known-length gzip level1 or uncompressed disk spools.

## Capacity adaptation

The frozen capture formula reserves Amax+Cmax+Wmax+H+S. The existing repository
publisher additionally downloads each uploaded object serially for independent
remote verification. This implementation reserves **another Cmax** for that
readback file, plus `6*(segmentBytes+1MiB)+16MiB` for concurrent WAL callbacks.
This is an explicit conservative increase, not an assumption that a PVC request
or emptyDir is a quota. The actual mounted dedicated ext4/xfs capacity must fit
all allocations before capture; native output is polled each second and the
finite backing filesystem supplies the hard stop. Source data volumes are never
scratch. Raw/spooled artifacts remain operation-owned until publication/drain.

## Evidence boundaries

`hack/backup_smoke.py` extends the real pinned CNPG/MinIO harness with Backup and
ScheduledBackup, concurrent SQL writes, S3-only discovery after Backup deletion,
downloaded artifact hashes, shell-free native verification/required-WAL failure,
and a test-only downloaded-full SQL restore with tablespace/separate WAL. This
is not the PR G production restore path or post-backup PITR qualification.

The harness records completed versus remaining fault families explicitly.
Unexecuted SIGTERM/OOM/workspace/credential/late-commit and observability cases
are not implied by unit tests, workflow existence, or a successful native process.
Exact-SHA run results and first failures belong to the author report/CI artifacts.
Do not treat this implementation document as an F completion or release claim.
