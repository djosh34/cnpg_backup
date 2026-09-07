# CNPG lifecycle implementation and evidence

PR D integrates lifecycle/configuration, not Backup/WAL/Restore data services.
Those sidecar capabilities remain **unadvertised**, their RPCs Unimplemented,
and `wal-fetch` fatal255. No product recovery or release qualification is implied.
The frozen design and issue resolution comments remain authoritative.

## Runtime boundaries

- `internal/cnpgi` uses the CNPG-I0.6 library with the pinned operator's0.5 wire
  subset. Manager Identity/Operator/Lifecycle and Pod/Job injection are available.
  Kubernetes1.35.8 and the actual fully rolled-out immutable CNPG1.30.0 Deployment
  are checked, including new lifecycle/validation calls after startup. Deployment
  lookup has a separate get-only named-resource Role beside the operator.
- A serial, paginated, namespace-allowlisted Repository status sweep uses one
  object per5s tick and a30s pass deadline. Expired LIST snapshots resume with
  the API's replacement continuation; independent configuration diagnostics do
  not require a consistent catalog. Errors without a replacement yield to the
  next namespace and restart later. No informer/framework or Secret watch.
  `Ready`/`Invalid`, observedGeneration and validated nonsecret configuration hash
  describe configuration only, **not storage health or native capacity**.
  RetentionBlocked is Unknown until storage/retention exists. Status patches use
  resourceVersion; unchanged results do not churn. Warning ConfigurationInvalid
  is throttled per Repository to5m with the throttle persisted before best-effort
  Event creation. Failure to deliver an Event is not a permission/fencing signal.
- Manager requires TLS1.3 and the intended client certificate identity. Each new
  RPC revalidates current trust even on an already-open TLS connection. Broken
  leaf/key/trust updates fail closed. Test-only TCP/gRPC regression proves one
  handshake, successful overlap, then rejection after retiring the old CA.
- For private-CA overlap, project a second issuer's public certificate as
  `client-ca-next.crt` alongside `client-ca.crt`. Only its absence is optional;
  malformed/read failures reject new work. Roll out the extra trust projection,
  rotate server and client Certificates, confirm actual operator validation, then
  replace the old trust source and remove overlap. Do not edit cert-manager's CA
  private-key Secret into an ad hoc trust bundle. CNPG trusts server Secret
  `tls.crt`, not `ca.crt`; real operator tests exercise that behavior.

## Placement and finite capacity

All supported ordinary/initdb/join/recovery instances get restartable init
sidecars. Static helper installation and socket readiness precede the main
container, without waiting for PostgreSQL or data recovery. A restricted socket
preparer creates a UID-owned0700 child below the kubelet-owned small emptyDir.
Only the socket mount uses subPath; Secret projections never do. Helpers/config
stay outside `/plugins`. Containers match CNPG's non-root identity, have read-only
root filesystems, dropped capabilities and restricted projections.

Lifecycle rejects missing/overlapping managed data mounts, duplicate container/
volume/mount paths, normalized tablespace-name collisions, replaced main argv,
unsafe PID namespaces, wrong identities and foreign recovery PVC owners. It
preserves CNPG's generated logging flags and its managed tablespace naming.
Owned immutable ConfigMaps contain checked delivery snapshots, not another
configuration authority. Source projections appear only in recovery Jobs.
CREATE/EVALUATE build fresh desired templates with all placement/security checks.
PATCH/UPDATE authenticate the existing Cluster-owned Pod identity and return no
mutation: they must preserve the entire admitted spec, unknown admission fields,
and original image/config snapshot, even when today's Repository is unavailable.
They grant no new mounts or data access. CNPG compares EVALUATE's fresh spec and
owns rolling replacement; the manager does not patch live images or projections.

Workspace remains one generic ephemeral PVC per Pod/Job, never a shared RWO
claim. Lifecycle projects the explicit declared workspace/PGDATA/WAL/tablespace
limits as nonsecret `capacity.json`. A claim request is **not** proof of a quota.
`instance|recovery-job --check-capacity` runs the actual kernel preflight:

- each path must be a canonical dedicated writable ext4/xfs filesystem root;
- inspect bounded `/proc/self/mountinfo` plus open-descriptor device/statfs data;
- reject local-path directory binds, emptyDir.sizeLimit/tmpfs, subpaths, NFS,
  shared device aliases and capacities larger than the declared hard ceiling;
- check each filesystem's available bytes against its phase allocation, not
  an aggregate sum that could conceal a full WAL/tablespace filesystem.

`configuration.PreflightCapture` loads a complete native/repository snapshot and
reserves raw archives + bounded compression spool + extracted WAL +100,000-entry
metadata reserve + max(1GiB,10%) spare space before any future native writer.
Native restore callers must supply their actual selected plan's explicit
per-volume allocations (including native output/WAL duplication) to
`CheckCapacity` before writing. No native handler exists yet and none may be
advertised without these checks and operation fencing. Filesystem/quota-only
subdirectory backends are not supported without an actual quota verifier.
Startup/Identity probes deliberately do not reserve database-sized phase space:
WAL service/bootstrap must not depend on an idle backup's workspace reservation.

## Native metadata/authentication

`instance --check-native` is a bounded metadata diagnostic, **not a Backup RPC**.
It uses `internal/postgres` fixed psql/pg_controldata commands, no shell or SQL
callback. One retained kubelet generation supplies Repository credentials, the
`streaming_replica` client cert/key, distinct client-CA validation, public server
CA, local Service hostname, declared targets and budgets. The ident-map name
`cnpg_streaming_replica` and superuser identities are rejected.

Private0600 files/service configuration, `hostaddr=127.0.0.1`, Service SAN
`host=<cluster>-rw.<namespace>.svc`, verify-full, fixed port5432 and a sanitized
libpq environment prohibit ambient passwords/socket/remote-Service fallback.
Metadata subprocesses have30s cancellation/process-group reap and1MiB stdout/
stderr bounds. Runtime checks query actual PG18.6, replication role, primary
state, standard block/segment format, WAL level/full-page writes/summarization,
summary retention slack/archive timeout and the exact managed tablespace map.
Mounted pg_controldata supplies physical identity/checksum/WAL-size validation
without assuming privileged SQL control-function grants. Native capture must
add its pre/post identity continuity checks when the handler is implemented.

## Recovery target ownership

`internal/recoveryguard` runs PID1 before the exact original CNPG recovery argv.
It acquires permanent nonblocking locks in PVC-UID order, checks the entire target
set, fsyncs fresh owner markers outside all data directories, then begins a
terminal local control session on the same Unix socket. Pod/Cluster/operation/
guard/sidecar-incarnation identities bind admission; sidecar restart cannot adopt
an old session. Namespace-wide wait4 reaps detached descendants. Only complete
main-descendant reap and that sidecar's acknowledged write drain permit marker
removal/fsync/unlock. Original CNPG failure stays failure after clean drain.
Crashes or ambiguous ownership poison targets; retry requires a fresh Cluster
and **all fresh target PVCs**, never marker removal based on a clock/dead PID.

## Tests and honest evidence boundaries

- Local Go tests cover real locks/poison, Unix gRPC Begin/Drain, canceled sessions,
  pending writes, stale identities, exact helper installation, snapshots, actual
  TCP mTLS/CA rotation and reauthorization on one already-open connection.
  Fake-API golden cases cover placement, source isolation, UID binding, image
  evaluation, version/settings/layout negatives and status/Warning throttling.
- `hack/test guard` runs production guard/control code in real Docker PID
  namespaces with a **test-only replacement CNPG command**. That is not CNPG
  recovery evidence. Guard/foundation hosted runs through7c0b35d passed; logs and
  exact SHAs are retained by the orchestrator.
- `hack/test cnpg-smoke` installs actual pinned kind/CNPG/cert-manager, consuming
  immutable built image digests. The current matrix includes two instances,
  initdb/join, WAL/tablespaces, metadata certificate auth/negative settings,
  Repository status, real finite/emptyDir/localpath checks, manager restart,
  server-leaf/private-CA overlap, defaulted live config/image rollout and uninstall.
- Actual CNPG-generated recovery Jobs test poison on each target volume before
  CNPG preflight and a fresh/all-fresh-PVC Begin → actual CNPG preflight → clean
  Drain case. Unavailable materialization must still fail; this is **not** a
  full restore/replay test. Full data/replay/pause/crash integration extends in G.
- The smoke collector captures allowlisted Pod/container status (including last
  termination), bounded current/previous container logs and fault-setting stages.
  Two disposable namespaces, a60s total request budget and64KiB/container-log cap;
  no Secret objects, Pod env/commands/annotations or termination messages are
  exported. Log redaction removes private-key blocks and credential-bearing
  lines; command failures/timeouts never reflect argv/input, and Secret command
  failures suppress output. Collection API errors record only exception types.
- Real lifecycle matrix execution remains in progress. Definitions and local
  fakes do not constitute a hosted CNPG PASS. Local Docker and GCC are unavailable;
  hosted CI supplies real namespaces/CNPG and test-only race compilation.

## Preserved first failures and dependency reconciliation

1. Initial guard test copied an executable without its mode; corrected explicit
   0555 fixture modes, without weakening runtime security.
2. Repository composite CEL defaults were incomplete; materialized child defaults
   before CEL validation. No CEL rule was disabled.
3. Ninth finite filesystem failed: real logs prove sysfs loop8=7:8 exists while
   container `/dev/loop8` does not, with82GiB free. util-linux reports
   `/dev/loop8 (lost)`. The harness strictly normalizes that display suffix,
   exposes the exact kernel major/minor and explicitly attaches the same free
   device. Races fail closed. All finite filesystems subsequently provisioned.
4. After merging main/B6605de8, copied package-local notice filenames made Go
   `./...` walk generated `module@version` directories. A nested output-module
   boundary preserves notice bytes while excluding generated roots from package
   discovery; a real go-list negative control distinguishes the fix. One
   `hack/godeps.py` inventory now owns executable/production/test scopes and
   notices; duplicate `gonotices.py` was removed. Both SDK and CNPG dependencies
   survive MVS/tidy and static build checks.
5. The apparent mTLS-discovery timeout was permanent CNPG image admission:
   digest-only PG images lack upgrade-version metadata. Add `:18.6` while keeping
   the exact immutable digest, and fail permanent image admission immediately
   rather than inflate the discovery timeout.
6. Runs34086181231/34086586700 reached real operator metadata reconciliation,
   which rejected removal of serviceaccount mounts from both plugin containers.
   D-SPEC-1/D-KISS-2: the shared injection path also rewrote live immutable config
   volumes after Repository changes. Manager-hook regressions distinguish fresh
   templates from admitted live Pods, preserving all admission fields rather than
   special-casing a token mount. Local red/green evidence is retained in the
   author's `.work/fix-2`; exact-current-SHA hosted matrix remains pending.
7. D-KISS-1: a one-object/five-second sweep outlived Kubernetes LIST snapshots,
   repeatedly restarting before the tail. A virtual-time production-sweep
   regression with200+3 Repositories, repeated five-minute expirations and a
   transient API failure now reaches every status/Warning recipient twice in46
   virtual minutes. Missing replacement/error paths yield namespaces without
   writing uncertain status. No per-resource ledger or unbounded LIST was added.

8. Run34088507100 at96132af reached ClusterReady, real primary replication
   certificate metadata and standby rejection, then failed the summary-negative
   arrangement: pinned CNPG sets `allow_alter_system=off` on PG17+. The disposable
   smoke Cluster now temporarily uses CNPG's `enableAlterSystem` opt-in, waits for
   the actual setting, injects `summarize_wal=off`, requires a nonzero production
   preflight rejection, then resets/reloads the fault and restores the original
   opt-in field in nested `finally` blocks. Declarative summaries stay on; no
   product validation/RBAC/default changes. Local harness red/green covers the
   exact original error, cleanup after lost SET response/probe failure, and
   wrong-reason/zero-exit negative controls. Hosted confirmation remains pending.

C mergecca29ed was reconciled into D at3074da6 without conflicts; both MinIO
integration targets and D's guard/CNPG entry points remain. Repository/S3 source
matches C exactly. Full local Go tests/vet, Python harness, CRD and static/native
build checks pass at3074da6. This is not hosted acceptance or G's lifecycle hold
integration; data capabilities remain safely unadvertised.

No production deployment/data/bucket operations, GitHub writes or reviewer/worker
delegation were performed by this scoped author. Two fresh independent reviews
and exact-current-SHA CI remain orchestrator gates, not claims supplied here.
