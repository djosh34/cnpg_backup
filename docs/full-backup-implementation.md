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

Test-only `hack/backupcontrol` observes real native processes and injects SIGTERM,
SIGKILL, bounded cgroup-v2 group OOM, and actual finite-workspace ENOSPC. The OOM
case requires an observed `OOMKilled` termination, not just exit 137. Native
processes are paused only to make the OOM/space fault preconditions deterministic.
The proxy can hold an artifact body or an acknowledged commit response; the latter
must reach the configured operation deadline and then replay the same UID's
unchanged durable winner. None of these actors or controls ship in runtime images.

The manager's independent S3 history metrics are compared with S3 LastModified,
including a failed callback with a durable commit. Pinned promtool 3.5.0 runs all
16 alert scenarios/128 assertions through `hack/test`; race CI provisions it too.
Native parser fuzz targets also receive bounded hosted fuzz runs.

### Preserved initial failures

At `1d34133b1c4c2c44681c4b7b29ce3375da16c48f`, hosted
[run 34110970396](https://github.com/djosh34/cnpg_backup/actions/runs/34110970396)
failed the existing WAL acknowledgment regression before F capture. PG's JSON
conversion emits `oid` values as strings, unlike `oid::bigint`. The added numeric
OID map made metadata decoding fail. A local PG18.6 SQL regression reproduced
that exact error; explicit bigint conversion corrects the wire type without
changing role privileges or weakening the E gate. Foundation and namespace-guard
checks passed at that first SHA; they were not F acceptance.

A real local PG18.6 tar fixture also exposed two server-generated directory names,
`./pg_wal/archive_status` and `./pg_wal/summaries`. The scanner normalizes **only**
these two empty-directory names before duplicate/ancestor checks. Arbitrary dot
paths, regular files with that prefix, and canonical alias duplicates remain
rejected. This narrow native-format reconciliation preserves confinement and
original tar bytes; it is not a general traversal exception. Local Unix-socket
fixtures are parser/process evidence, not CNPG certificate/image qualification.

At `6b15ceff3eff37e1a70c34d01f578e950554e90f`, all three hosted workflows passed,
including both native captures and downloaded-input SQL oracles. At `3ebbe1e`,
actual WAL-under-artifact-transfer, both capture types, S3-only SQL verification,
and credential-failure freshness/counter/Warning assertions passed. That run then
correctly failed because the next fault started before kubelet restored the valid
credential generation. The harness now requires actual native projection recovery,
not API Secret update acknowledgment. Earlier transfer evidence also exposed an
empty-segment LSN guess; the oracle now inserts real WAL and uses the switch LSN.
These failures remain in hosted artifacts, rather than being relabeled as passes.
At `74fbb2d`, actual SIGTERM cancellation/restart also passed. The next case exposed
that namespace-local SIGKILL does not kill PID-namespace init; the harness now uses
E's established node/CRI PID actor and proves restart before judging the outcome.
The downloaded SQL oracle also correlates capture-time acknowledged transactions
with native stop LSN, and excludes a transaction written after artifact transfer
began (without fetching later WAL).
At `37c5be6`, both native callbacks succeeded but the ScheduledBackup finished in
2.4 seconds on the now-clean fixture, before a poll observed native activity.
The workload-overlap assertion correctly failed. Each capture now dirties the
real load table before requesting its spread checkpoint; the native command and
strict overlap/SQL assertions are unchanged.

The harness records completed versus remaining fault families explicitly.
Unexecuted SIGTERM/OOM/workspace/credential/late-commit and observability cases
are not implied by unit tests, workflow existence, or a successful native process.
Exact-SHA run results and first failures belong to the author report/CI artifacts.
Do not treat this implementation document as an F completion or release claim.
