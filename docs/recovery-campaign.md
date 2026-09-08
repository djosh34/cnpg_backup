# Digest-only recovery campaigns

The CI repair replaces campaign control and fixture lifecycle, **not the product
or its independent SQL/WAL/PostgreSQL/PID1/source-holder oracles**. PR G remains
paused until repair validation and independent review. A workflow file, mocked
fixture, diagnostic slice or matched configuration is not full recovery proof.

## Immutable inputs, one local/hosted recipe

`subject.json` records the original product revision, canonical manager/data
manifest digests and publication evidence. `.github/ci-repair-subject.json` freezes
the repair subject to G49a. The repair never runs `build.py`/`images.py` or publishes
product images. Binary revision is checked when consuming each digest; this is
not a substitute for the later release signing/provenance gates.

`campaign-bundle` builds only the test actor, WAL proxy/client, native verification
driver and checksum-pinned MinIO fixture. It exports their immutable image bytes
and hashes into `harness.json`, separately from the subject. `--reuse OLD_BUNDLE`
reuses verified identical fixture-tool bytes when Go/tool inputs are unchanged;
Python-only harness changes do not rebuild any product or fixture executable.
The harness record binds source content, revision, Python version and all bundle
bytes. Fixtures import these verified archives directly into kind and check the
canonical config digest in consuming containerd; Docker classic and containerd
image stores expose different objects as `.Id`, which is not a portable oracle. Dirty bundles are explicitly diagnostic. Fresh acceptance rejects them.

```sh
# Create records once (subject.json comes from the audited publication).
./hack/test campaign-bundle --out BUNDLE
./hack/test campaign-plan --subject subject.json --bundle BUNDLE \
  --profile recovery --seed 1806 --mode fresh --out plan.json
./hack/test campaign-preflight --plan plan.json --out preflight.json

# SAME command locally and in the reusable hosted workflow; no subject build.
./hack/test recovery-campaign --plan plan.json --bundle BUNDLE \
  --run-dir NEW_RUN --duration-minutes 120

# Fresh focused prerequisites; partial scope never counts as all-G.
./hack/test campaign-plan --subject subject.json --bundle BUNDLE \
  --case detached-PG-descendants --out focused.json
./hack/test recovery-campaign --plan focused.json --bundle BUNDLE --run-dir NEW_FOCUSED

# Diagnostic dirty harness requires BOTH explicit labels; still uses new fixtures.
./hack/test campaign-bundle --out DIAGNOSTIC_BUNDLE --diagnostic
./hack/test campaign-plan --subject subject.json --bundle DIAGNOSTIC_BUNDLE \
  --mode diagnostic --profile retry --out diagnostic.json

# Collection/cleanup of an explicitly retained owned fixture only.
./hack/test campaign-collect --owner RUN/fixture-001/owner.json
./hack/test campaign-clean --owner RUN/fixture-001/owner.json
./hack/test campaign-compare --left LOCAL/evidence/manifest.json --right HOSTED/manifest.json
./hack/test campaign-aggregate --plan plan.json RESULT1/manifest.json RESULT2/manifest.json
```

Obtain the official shared bundle and plans from the `immutable-harness-RUN-ATTEMPT`
artifact of **CI REPAIR validation**, rather than rebuilding a supposedly similar
local subject/harness. Artifact transport loses executable bits: restore `555` on
`actor,minio,wal-proxy,wal-client,verify`, not on credentials or arbitrary files.
Use the recorded Python version. Native database bytes/UIDs/timestamps naturally
vary: compare recipe and independent invariants, not fresh database byte identity.

## Scope and independent failures

`campaign_plan.REGISTRY` is the sole execution/dependency registry: all **31 fixed
G families + two individually named seeded XID supplemental restores**. Failed or
missing supplemental work fails the full scope. `smoke` selects three families;
`retry` and `ownership` expand only their actual prerequisites, without hidden S1
capture. `--case` selects a named dependency closure. `retirement-20` is a separate
explicit diagnostic gate exercising twenty sequential negative/restore operations
in one fixture, not a substitute for any of the33 full cases.

Required branch records remain beneath those33 families: newest/explicit-base
latest, inclusive/exclusive time/LSN/XID, earlier/explicit-too-new selection,
R0 missing/corrupt WAL, intact-fallback/all-required255, and each same-file
healthy/negative/fault variant. Independent unexecuted branches
survive a sibling failure, but run only after verified disposal and fresh fixture
certification. Failed branches are never retried; true dependents are blocked.

`--layout grouped` uses separate fresh fixtures for targets/source loss, S1/WAL/R0,
and ownership plus final exploration. Monolithic is the full-order default to
expose accumulation/order defects. Local fixtures are serial under one cross-
worktree host lock. The hosted matrix uses separate runners and `fail-fast: false`.

Every case failure is retained in `failures.json`, with its phase, classification,
requirement, audited assertion and causal location. `first-failure.json` is never
overwritten. Failed prerequisites mark dependent work **blocked**, not falsely
exercised or a cascading product failure. Independent remaining cases run only on
a fresh kind/source/MinIO fixture after successful owned teardown. Their fresh
prerequisites have separate records; prior failed cases are not retried to green.
A leaked fixture blocks slot reuse. An audited product requirement is not causal
proof of a product defect: automatic assertion classifications are explicitly
unadjudicated. Preserve the original first-failure record and add separately bound
causal adjudication after checking fixture/caller inputs and actual state. Collection and teardown errors are additional
failures and never obscure the primary error. Primary failures are saved before
fault/object reset; every independent cleanup is attempted and recorded with its
phase/association, including nested failures. Deadlines leave unexercised work
blocked/incomplete. An unresolved timeout alone is not a demonstrated product bug.

The exhaustive [assertion/barrier audit](campaign-assertion-audit.json) classifies
safety invariants, eventual outcomes, fixture preconditions and removed incidental
assumptions. A cheap AST completeness regression rejects unclassified assertions.
Keep exact SQL inclusion/exclusion, native WAL/hash/history endpoints, source
holder identity, same-PID adoption/reaping and all-three-volume ownership checks.
Readiness, deleted Pods, elapsed time and API projection spelling do not replace
these requirements. Fatal-WAL assertions require an observed fault receipt and
actual requested filename; absent prerequisites block the outcome assertions.
R0 missing/corrupt controls require the native test driver's exit2 and exact
`WAL rejected` receipt, never Docker invocation errors or killed verification. CNPG source smart/stop shutdown is explicitly30/60 seconds;
namespace deletion waits for actual finalization within300 seconds. No finalizer
stripping, marker clearing or invented completion proof is allowed.

## Owned resources and bounded diagnostics

Every fresh run creates a unique `cb-repair-*` kind node, explicit kubeconfig,
fresh namespaces/captures/target PVCs and a dynamically allocated loopback MinIO
forward. There are no external endpoint or ambient kubeconfig inputs. Cached
immutable downloads/images are allowed; retained/imported source data is not.
Diagnostic mode still provisions fresh data; arbitrary retained scenario drivers
are intentionally not supported. `--retain-on-failure` is for bounded collection
and explicit owned cleanup, never full-fresh acceptance.

Preflight precedes provisioning and checks CPU/memory, Docker storage/free bytes,
inodes and host cgroup/pressure observations. The provisional measured-local
recipe caps the node at4CPU/5GiB, reserves2GiB host headroom and requires14GiB
starting disk with a5GiB emergency floor. These thresholds need full-run peak
validation; they are not promises for arbitrary hosts. Host kernel/runtime/storage
fingerprints and ten-second/boundary accounting remain evidence, not assertions
that local and hosted physical timing is identical.

Capture backing is demand-allocated with two available spares and a hard maximum
of eight live capture filesystems. Capture/target ext4 ceilings remain8GiB/3GiB.
Allocated blocks, not sparse logical ceilings alone, are measured. After evidence,
stop target Jobs/Pods while the original operation exists; require its original
durable completed/uncertain closure, then delete the Cluster, verify no consumers,
and retire exact PVC/PV/backing identities. Source holders are not cleared. Never
rebind a poisoned target. Normal teardown stops owned sandboxes, verifies exact
loop/backing associations, unmounts/detaches only owned backing and removes the
owned node/private data. After stopping kubelet and removing every CRI sandbox,
whole-node disposal explicitly unmounts residual binds of verified owned devices
under kubelet Pod volume roots; it does not wait for a stopped kubelet to act.
Concurrent kubelet sandbox removal is accepted only after a fresh successful CRI
list proves that exact sandbox absent, never because an error says NotFound. Unknown ownership or unmount failure is a reported leak.

Commands preserve return code and bounded separate output; child groups are killed
and reaped on deadline. The pending exec uses that same bounded capture and
TERM/KILL/reap path, independently of remote resets or an expired case budget.
Standalone image-version/verifier containers have pre-recorded owned names and
labels, 1CPU/512MiB/64PID limits, explicit forced removal and verified absence.
Removal failure blocks fixture reuse; stale labeled containers also block a new
run. Logs/events/status collectors proceed independently with typed results;
unavailable logs are blocked only after fresh original-Pod absence or unstarted
container evidence. Waits cap children by remaining case/run time and report
last operation/status plus available scheduling/admission observations. Failures
are collected before teardown. Fresh runs bind the120-minute budget into the immutable plan; only diagnostic
runs can override it, and their actual budget is recorded. The campaign reserves five
minutes each for collection and teardown; hosted jobs have a150-minute hard cap.
Event diagnostics use bounded API pages (100 events, maximum50 pages), omit bulky
managedFields and explicitly mark any remaining pages. They do not concatenate
an unbounded event list or turn a truncated JSON payload into oracle input.
Progress is incremental. Hard runner death cannot guarantee final collection and
never counts as success. Evidence uploads cap at100MiB with explicit truncation/
dropped-artifact records; Secrets, kubeconfigs, auth files, private keys and raw
backup archives are not uploaded. Ordinary artifacts expire after14 days.

## CI and acceptance boundary

`ci-repair.yml` responds only to an explicit repair-request file change on the
repair branch. It consumes the frozen subject, delegates to the shared reusable
workflow, and records1806/1806/1807 monolithic attempts plus a grouped attempt.
Normal foundation/guard/lifecycle G delivery is not triggered for the repair
branch. The normal candidate caller also delegates digest-only recovery to this
same recipe instead of maintaining copied smoke/full orchestration.

Local/CI cheap gates are `./hack/test harness`, `race`, `fuzz-smoke` and
`campaign-focused` (100 repetitions of focused API/actor/harness regressions).
Production builds remain CGO-free; race uses test-only CGO. The full-fresh
aggregator rejects partial, retained, failed, missing, mismatched and leaked
attempts, including missing/failed required child branches. Each run has a unique
execution ID; duplicate executions cannot count as repeats. `campaign-compare`
requires distinct local and hosted executions, not self-comparison. Full G evidence still does **not** qualify H–K or a release.

Required repair evidence remains: exhaustive audit/red controls, all independent
failure reporting and blocking, focused repeats, same immutable full local/hosted
series, grouped/order/retirement stress, actionable deliberate failures, normal
resource recovery and fresh independent review. Preserve every failed attempt;
corrections start a new explicit validation series. A separately demonstrated
product defect keeps G green/merge criteria unmet; do not alter product behavior
inside the CI repair to manufacture acceptance.
