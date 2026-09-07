# Full restore and PITR implementation (PR G)

The frozen [design §5](design.md) and [CNPG/native/storage contracts](research/cnpgi-contract-final.md) are authoritative. This document describes runtime seams, not release qualification. Differential capture/reconstruction and automated retention remain outside G; differential restore returns Unimplemented, with no full fallback.

## Configuration and ownership

Use the existing CNPG primary `bootstrap.recovery` and local `externalClusters[].plugin.parameters.repository` to select a source Repository. The ordinary Cluster plugin selects a **distinct destination repository UUID**. No source database, source Kubernetes resources or Kubernetes Backup catalog is read during recovery. Source credentials require identity/catalog/data reads/list and exact gate Get/Put; destination initialization needs its normal writer permissions.

Lifecycle validates all target PVC UIDs and wraps the original CNPG recovery command with the existing PID1 `recovery-guard`. The guard owns every PGDATA, managed tablespace and separate-WAL volume **before CNPG preflight**, throughout materialization, replay, detached main descendants and original sidecar drain. Sidecar startup installs its helper/socket and reclaims only its Pod-bound native workspace; it never mutates a target. Busy locks reject competing preflight. Any uncertain marker remains poisoned: use a fresh Cluster and **all fresh target PVCs**, never delete a marker or infer takeover from process/API disappearance.

One authoritative operation derivation is `repository.RestoreOperationID(Cluster UID, SHA256(canonical bootstrap JSON))`. `<cluster>-cb-targets` remains the immutable guard projection. `<cluster>-cb-recovery` is an owned, manager-written ConfigMap containing `data["operation.json"]`:

- immutable `placement`: Cluster identity, operation/fingerprint, source/destination Repository names and nonsecret specification hashes;
- immutable `targets`: the exact guard PVC UID/mount set;
- `observerID`: the originating manager incarnation;
- monotonic `state`: active → completed or uncertain, never reopened;
- completed proof: `completedJobUID`, sorted `terminatedPodUIDs`, then `lifetimeReleased` after acknowledged source-gate removal.

Updates use Kubernetes resourceVersion. ConfigMaps are projections/status, not editable configuration authority. Recovery Job templates carry `cnpg-backup.djosh34.github.io/recovery: <operation UUID>`. The manager establishes stable source lifetime admission before returning the creation patch. The sidecar **independently** establishes that lifetime and its own fresh reader before any catalog/payload read; neither projection lag nor controller success substitutes for source-gate acknowledgment.

## Selection and durable plan

`repository.Hold.Resolve` uses a complete protected catalog, a complete bounded WAL-name inventory and authenticated history bytes. It parses numeric ancestry/forks, rejects missing/contradictory histories and competing leaves without an explicit numeric timeline, and applies strict base eligibility. `current`/`latest` timeline strings are rejected. Time targets require timezone-qualified RFC3339 and source completion strictly before target; LSN targets require native end strictly before target. Named point/XID/immediate require exact backupID; exclusive applies only to time/LSN/XID. PostgreSQL, not selection/RPC success, must actually reach explicit targets.

For latest/non-immediate recovery the plan freezes continuous post-bundle intervals through the observed archive frontier, crossing only verified forks. Every segment intersecting those intervals is remote-required, **including the final bundled filename when the interval starts inside it**. An interior gap fails selection; retired slots are not removed from the observed frontier to manufacture a shorter successful recovery. Immediate still performs archive-first WAL lookup during replay.

Before native materialization the sidecar atomically writes/fsyncs:

```
/var/lib/postgresql/data/.cnpg-backup/<operation>/recovery.json
/cnpg-backup/state/recovery.json
```

The first is outside actual PGDATA and survives CNPG preflight. Its credential-free `RecoveryPlan` envelope contains `plan` (repository identity, exact chain, target, timeline path, required intervals and holder IDs), original `tuple` (Cluster/operation/Pod/guard/sidecar identities), `cluster_definition` (canonical sanitized semantic Cluster), `bundled` raw size/SHA256 records and `materialized`. Arbitrary annotations, other plugins' parameters and PostgreSQL connection GUCs are not copied; they may contain unrelated inline credentials. The envelope is capped at 16MiB, with a separate 64MiB helper-state emptyDir for atomic metadata writes. The initial state is `materialized:false`; both copies become true only after verified materialization. A new, cleanly admitted attempt validates and reuses the exact persisted selection with its **own** reader/tuple, never adopts another process holder or silently chooses a newer base. The helper rejects partial/invalid plans.

Catalog/winner verification has a separate finite workspace preflight and input-byte/type checks before database-sized downloads. Native full restoration reuses the F bounded archive scanner, original manifests and direct PostgreSQL verifiers; every original input is verified before trusted tablespace-map/WAL/configuration transforms. Each target filesystem and workspace must pass actual finite-mount/capacity checks. Missing mappings, unexpected files/links, oversized input/output, damaged originals, tool failures and partial transformations cannot return RestoreResponse. See the native package for synchronous extraction, original verification, cross-volume copy/hash/fsync and native child-reap implementation.

## Archive-first helper contract

RestoreResponse returns only the constant direct command and frozen numeric timeline/action:

```
restore_command = 'exec /cnpg-backup/bin/cnpg-backup wal-fetch --plan /cnpg-backup/state/recovery.json -- "%f" "%p"'
recovery_target_action = 'promote'
recovery_target_timeline = '<numeric source timeline>'
```

CNPG appends its validated target options and performs replay **inside the guarded Job**. The helper has no credentials and uses existing Unix CNPG-I WAL.Restore with MODE_RECOVERY and `parameters.recoveryID` containing the exact guard tuple. File names and destinations are checked before I/O; only the supported PG relative/absolute WAL paths are accepted. Arbitrary plan paths/commands are not supported.

| Result | Helper exit |
|---|---:|
| Verified archive bytes, fsynced atomic destination publication | 0 |
| Authenticated optional future/history absence | 1 |
| Authenticated duplicate absence with the actual local selected bundle still matching its exact size/hash **and no required remote interval** | 1 |
| Required absence, retirement, corruption, local I/O, TLS/auth/transport/deadline/gRPC/plan/argument errors or panic | 255 |

Archive lookup is always first. A bare HEAD404 is only `HeadMissing`, not authenticated absence: it requires a confirming GET `NoSuchKey`. Other HEAD failures return directly, so a later GET miss cannot hide exhausted transport, TLS/auth or corruption errors. Missing buckets, bare GET404 and malformed XML remain errors. A bundle never becomes archive success, never hides same-segment post-EndLSN records, and never converts an outage into EOF. Local fallback requires verification of the actual current guarded file, not merely a filename in metadata. WAL tasks have the existing two independent callback slots, a 60-second server budget and 75-second helper deadline. Ordinary instance/rewind continues destination-only and refuses the recovery token; retained externalClusters configuration does not authorize source reads.

## Drain and conservative automatic release

Every callback binds only the **original** local Begin tuple and holds a guard Task through actual synchronous source I/O, target publication and native reap. Successful materialization/RestoreResponse does not release either source hold. Drain irrevocably closes admission, cancels and waits for all tasks, then invokes the bound same-process reader cleanup **before** acknowledging to the guard. Control/server loss never invokes this cleanup. Reader-release failure makes Drain fail and leaves target ownership uncertain. Any error/panic after entering native materialization (including final plan fsync) closes admission conservatively rather than treating a generic native error as a clean-drain certificate; its reader and poisoned targets remain protected.

The manager starts an ordered Pod LIST/WATCH before authorizing Job creation. It requires one matching complete, successful Job and all original/retry Pods with their latest observed terminal statuses, every regular/init/restartable-sidecar/ephemeral container terminated, no deleting/disappeared/NodeLost Pod, matching owner UIDs and accounted successful/failed Pod counts. It records completed state durably before removing **only the stable lifetime holder**. Unknown process-reader holds remain even if a later retry succeeds.

Observation is intentionally conservative: watch loss, active-manager incarnation loss, disappearance or force deletion preserves protection and records uncertain state/Warning when API access permits. A new manager does **not** adopt an interrupted active observer. It may finish an already durably recorded completed lifetime release. No elapsed-time expiry, force-clear, PID/API-based fencing or manual repair path exists. Observation retains only UID/resourceVersion pairs, not Pod bodies, and consumes complete Job/Pod lists one page at a time. At most 16 active/uncertain observers and 1024 Pods per observer are retained; gate capacities are independently bounded. Exhaustion fails closed. These availability costs do not block another fresh protected restore when the source gate contains only holders, rather than an uncertain GC owner.

## Tests and evidence boundary

Go regressions exercise production numeric selection/frontier classification, original source-reader admission/drain, archive-first verified filesystem publication and local fallback, all fatal helper status mappings including actual Unix RPC, strict durable envelopes, target syntax, immutable operation binding, actual Kubernetes fake-client LIST/WATCH/terminal transitions and all retry-container negative controls. The integrated CGO-disabled suite includes `TestActualNativeFullMaterialization` against the local PG18 installation. The actual CNPG/PG18/MinIO campaign is separately recorded against exact subject images; it has not run locally. Compiler-only overlays used during disjoint development are **not native/integration evidence**.

Actual SQL target attainment, post-backup remote replay, no false promotion under injected archive faults, source Kubernetes loss, PID1/paused/detached target fencing and exact-image qualification remain mandatory campaign evidence. A successful unit suite, materialization call or helper RPC is not a qualified G recovery campaign or v0.1.0 release.
