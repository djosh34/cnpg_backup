# PR D implementation evidence and frontier

This is implementation status, not a replacement for the frozen design or an
issue18 acceptance/closure claim. Real CNPG/kind acceptance remains mandatory.

## Local target fence milestone

`internal/recoveryguard` implements strict canonical target/argv validation,
nonblocking PVC-UID-ordered permanent lock files, fsynced whole-set owner markers,
and a terminal local Begin/Drain stream on the same Unix gRPC socket as CNPG-I.
The guard runs only as PID1, before the original CNPG command. One wait4 owner
reaps the whole private PID namespace, including detached orphans; SIGTERM then
SIGKILL is escalation, never permission to unlock. Only complete descendant reap
and the original sidecar's Drain acknowledgment remove markers. Failed original
CNPG commands keep their exit status even after clean drain. Uncertainty/crashes
leave poison; retry requires a fresh Cluster and all fresh target PVCs.

The configuration projection is `/cnpg-backup/config/guard.json`, containing
`clusterUID`, `operationUID`, and `targets: [{pvcUID,mount}]`; `POD_UID` must come
from the downward API. This is an internal owned projection, NOT a user API.
Kubernetes placement must establish the immutable, nonoverlapping whole PVC set
with uncached UID reads. The guard cannot prove exclusive Cluster/PVC ownership
from a standalone JSON file, so manual invocation is not a supported deployment.

`instance`/`recovery-job` install the same static executable atomically outside
`/plugins`, serve CNPG-I v0.6 Identity, and provide `--probe`. Recovery mode also
serves the private control stream. No Backup/WAL/Restore data capability is
advertised; absent handlers return gRPC Unimplemented. `wal-fetch` remains fatal
255. The small bounded Task admission API must surround actual I/O and native
child reaping when data handlers arrive. Cancellation alone never completes work.

## Tests

- Go tests exercise actual locks, marker poison, symlink/hardlink/FIFO rejection,
  argv preservation, strict configuration, atomic helper installation, and real
  Unix gRPC Begin/Drain with delayed writes, stale identities and canceled streams.
- `hack/test guard` builds the production executable plus a **separate test-only**
  command fixture, then exercises real Docker private PID1 namespaces, setsid
  orphans, sidecar/guard death, pending target writes and replacement preflight.
  The fixture replaces CNPG, so this is explicitly **not** real CNPG acceptance.
  `.github/workflows/guard.yml` runs that profile. No fixture enters either product
  image; both product inventories retain their existing exact executable policy.
- Local vet/unit/static image build checks passed at this milestone. Race testing
  could not compile locally because GCC is unavailable. Docker is unavailable
  locally; hosted namespace tests are defined but unexecuted here.

## Remaining mandatory PR D frontier

Repository CRD/defaults/CEL/typed validation; Secret/CA operation snapshots;
manager discovery/mTLS/reload and Operator validation; owned projections and
idempotent Pod/Job injection/rollout with uncached PVC identities, per-Pod finite
workspace, security/mount allowlists and source/destination routing; pinned real
kind/CNPG/cert-manager install/uninstall, multi-instance, manager restart,
leaf/private-CA rotation and golden lifecycle acceptance. Full CNPG recovery
fault scenarios extend in PR G, without deferring the guard itself.

No current milestone is release-qualified or sufficient to close issue18.
