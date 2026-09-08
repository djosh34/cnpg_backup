# Actual full/PITR campaign (PR G)

**Implementation, not executed qualification.** The disjoint campaign writer could
compile/unit-test its observers but had no local Docker/kind. Integrated G runtime
and native materialization images must pass hosted execution before acceptance.
The authoritative contract remains [testing.md](testing.md) and design §3–5.

## One entry point; existing images only

```sh
./hack/test recovery-campaign \
  --subject-sha <40-hex-trusted-commit> \
  --manager-image ghcr.io/djosh34/cnpg-backup-manager@sha256:<64-hex> \
  --data-image ghcr.io/djosh34/cnpg-backup-pg18@sha256:<64-hex> \
  --profile recovery --seed 1806 --duration-minutes 120
```

After the local fixture checks, the campaign bypasses `hack/test`'s source-build
pipeline. It pulls the supplied
canonical digests directly into kind, checks each binary's reported revision,
and injects those exact images. There is **no subject rebuild or tag substitution**.
The binary revision check is not release provenance/signature verification (J/K).
The harness revision, subject SHA/digests, upstream compatibility pins and separate
test-actor/MinIO images are recorded independently. Test actors are built from the
harness revision; none is added to a subject image.

Requirements: disposable Linux amd64 Docker host, disk-backed checkout, upstream
public registry access, and the pinned tool downloads used by the existing smoke.
No production endpoint, credential, kubeconfig, arbitrary command or image
registry input is accepted. Only the newly created `cnpg-backup-campaign` kind
cluster is used. MinIO and its synthetic credentials live in `campaign-store`,
independently of the deleted `campaign-source` namespace. Recovery runs in
`campaign-target`. Existing D/E/F smoke and guard entry points remain separate.

`smoke` runs three named families (source loss, latest, named/pre-DROP), including
both automatic latest selection and explicit-old-base remote replay. `recovery`
runs all **31 fixed G families**, then two seed-selected inclusive/exclusive XID
restores. The seed controls choices, not distributed scheduling. `qualification`
runs the same G work but **fails qualification**: H differential, I retention,
J operations/security and K certification are not implemented here. Every profile
always reports `release_qualified=false`; absent mandatory evidence fails its
scope, even after earlier successful cases.

For fast local failure diagnosis, `--profile ownership` runs the source-loss
recovery prerequisite and all 12 guard/crash/drain/protection families using the
same setup, methods and oracles. Its manifest lists the 18 unrequested families
and cannot claim all-G coverage. Run `--profile recovery` afterward for the full
mandatory gate; hosted candidate and qualification defaults are unchanged.

The reusable/manual `.github/workflows/recovery-campaign.yml` has `contents: read`,
pinned Actions, a 150-minute job deadline and an earlier process-group deadline.
Duration is constrained to 10–135 minutes; default 120 includes collection, with
five minutes reserved before the inner worker deadline. A canceled/hard-killed
runner cannot guarantee upload. Progress is fsynced incrementally and collection
also runs with `always()`.

The trusted main workflow validates the exact SHA against `main` or the explicitly
allowlisted repository-owned `implementation/pr-g` candidate branch; it never
checks out subject code in place of the harness. The harness checkout is the
immutable workflow revision. Manual dispatch is **not available until this
workflow is published on main**. Pre-merge integrated execution can call the same
local entry point in trusted hosted CI; a workflow file is not acceptance evidence.
Once available on main, an example dispatch is:

```sh
gh workflow run recovery-campaign.yml --ref main \
  -f trusted_ref=implementation/pr-g -f subject_sha=<candidate-sha> \
  -f manager_image=ghcr.io/djosh34/cnpg-backup-manager@sha256:<digest> \
  -f data_image=ghcr.io/djosh34/cnpg-backup-pg18@sha256:<digest> \
  -f profile=recovery -f seed=1806 -f duration_minutes=120
```

The repository-owned `pr-g-candidate.yml` push caller permits only the exact
`implementation/pr-g` push SHA and workflow revision in this repository. It
builds/audits each candidate once, refuses existing or ambiguously absent
`sha-<commit>` tags, and publishes the audited image IDs to the canonical GHCR
packages. It records manifest/config digests incrementally (including partial
publication failure). The smoke job runs first; only its success admits all31
recovery plus seeded supplementary on the **same digest outputs**, without
rebuilding. Per-SHA concurrency prevents competing publication from this caller.
A rerun must not overwrite or rebuild an existing candidate; use its recorded
digests in a trusted campaign caller instead. No version tags or release are
published, and G evidence is not H–K qualification.

Only the publishing job has packages:write; test jobs use packages:read. After
trust validation, the ephemeral GITHUB_TOKEN authenticates Docker via stdin in a
private `.work` config, never a new owner secret. The harness projects only GHCR
auth to disposable Kubernetes imagePullSecrets so kubelet pulls the same registry
manifests even for private packages. No docker-save/import/re-tag substitution.
Credential files are removed and excluded from artifacts; no deployment/model
credentials are accepted. The main-only reusable workflow trust contract remains
unchanged. Portable release archives and qualification remain later delivery work.

## Actual assertions and arrangements

| Families | Distinguishing actual-system oracle |
| --- | --- |
| Full latest/time/LSN/name/XID/immediate/explicit | CNPG Backup produces original native input; source SQL commits and top-level XIDs/timestamps are journaled outside PG. Native `pg_waldump` independently identifies the target commit-record LSN. Fresh CNPG recovery Jobs replay, then SQL checks rows, pre-DROP table, promotion timeline, tablespace location and separate WAL symlink. |
| Inclusive/exclusive, newer base, unreachable target | Time/LSN/XID recover on both sides of the same committed transaction. A later full is ineligible for the older target; automatic selection chooses the older base and an explicitly too-new base fails. Missing named target must fail with PostgreSQL's target-unreached diagnostic. |
| Remote WAL, source disaster and new lineage | Commit before/after sentinels only after the selected base finishes, beyond its bundled segment. Observe actual archive `.done` plus independently downloaded bytes. Delete the entire source namespace/catalog before any recovery. Target archives into a different UUID and cannot change the frozen source inventory. |
| S1 local fallback and fatal failures | Withhold actual archive objects, preserving/restoring exact payload and integrity metadata. Intact bundle + authenticated absence must give helper exit1 and actual immediate recovery on a fresh target. A separate negative keeps both archive absence and the required local file withheld through actual replay255/nonzero CNPG exit/no promotion; restoring the file afterward does not turn that negative into a positive. Incorrect all-required255 is an actual failing-recovery negative control. Missing required archive, payload corruption with intact metadata, auth, socket reset and TLS-handshake faults must give255/no latest promotion despite a bundle. |
| Same final bundled segment | A **new actual product full** must have an independently decoded real following SWITCH at/after unchanged native EndLSN in the same final filename. Explicit-backup/latest uses the actual admitted frontier; test-only padding outside the native range leaves original manifest/archive/SQL bytes unchanged. Product replay must deliver the original archive hash, recover SQL/promote, and reach the frontier through a PostgreSQL-generated new-timeline history fork with exact source ancestry. The same endpoint oracle must reject wrong padded-bundle-as-archive-success, even if PG promotes with identical rows; a subsequent healthy control must pass. Same-file missing/corrupt/auth/TLS/transport faults remain255/no promotion throughout actual replay. [Native red/green and source semantics](research/s1-switch-latest.md); old ineffective named-point trials remain evidence, not waived coverage. |
| Unsafe EOF negative | A deliberately faulty test-only RPC mapping converts a required error to NotFound. Actual PG can promote with only the base rows; the independently expected latest rows distinguish that loss. Product faults retain their real status in positive/safety cases. |
| R0 | Export the selected data image to check no shell; verify its real downloaded original tar via `pg_verifybackup --no-parse-wal` and the existing direct-Go WAL verifier. Missing/corrupt required bundled ranges must fail. Actual G materialization also executes in that unchanged shell-free image. |
| K1/S2 | A test-only admission observer in the disposable target namespace installs a wrapper on CNPG's controller **volume**, preserves the original pinned CNPG executable and executes it. The unchanged product guard remains private-namespace PID1. Pause after Begin/before original preflight, delay the real RestoreResponse, hold actual replay and pause after CNPG shutdown. Same-PVC replacement runs the actual guard and must fail without any directory/inode/content changes on all three target filesystems. |
| Crash, descendant, pending task, stale tuple | Kill actual guard/sidecar from the node's ancestor PID namespace and require observed137, poison and same-PVC refusal. Kill the actual CNPG command during replay, observe real orphaned/detached PG descendants under PID1 and require reaping before clean release. Hold a real source WAL task, pause its original sidecar, and require markers until that same process resumes/drains. An existing Unix WAL RPC carrying a stale incarnation must fail without target writes. Poison retry uses a fresh Cluster and all fresh PVC UIDs. |
| Source protection and controller completion | Observe source lifetime + reader holders while an actual source input response is held, then through materialization/replay and manager restart. An uninterrupted clean completion removes only those owned holders, not crashed readers from prior cases. After manager restart SQL/own-reader drain must succeed but durable uncertainty and stable protection remain. For retries the observer fails the first attempt before CNPG preflight, lets the actual guard drain, then requires a real Job-controller replacement (completions/parallelism stay1); all matching/retry Pod containers must terminate before stable completion release. |

The local-fallback positive also requires the native promotion `.partial` to be
durably stored in the **new repository** as a distinct exact-segment-size
auxiliary: downloaded bytes/metadata must match the verified bundle, PostgreSQL
must mark `.partial.done`, and no old full-name slot may be manufactured. The
subsequently switched complete new-timeline archive must independently match
PostgreSQL's bytes; partial/history presence cannot replace that gate. Partials
provide no full coverage, frontier or latest-timeline evidence and are retained
conservatively, without GC pruning. This compatibility correction leaves all31
families, source isolation and fatal255 rules intact; earlier campaign captures
do not qualify the corrected image.

The socket observer mounts the full plugin volume outside `/plugins`, verifies
both aliases identify the original socket, and renames via two paths on that
**same mount**. Renaming directly between the `/plugins` subPath bind mount and
`/controller` (or even a second alias of the plugin volume) is invalid EXDEV.
Target certificate Secret names are explicitly predeclared for bounded g-001
through g-064 in both manager startup configuration and Secret-get-only RBAC.
No manager restart is needed to expand authorization mid-campaign.

The wrapper/proxy is observation/fault arrangement, not a replacement PostgreSQL
server, recovery engine or production test endpoint. It records the original
CNPG command's actual exit. A proxy's detached process alone does **not** count as
a detached PG regression: the latter requires actual PostgreSQL process identity,
orphan adoption and distinguishing process-group/session evidence. Likewise,
requesting a pause/fault without its observable barrier fails the case.

The shutdown barrier is after CNPG's own start/replay/stop sequence, before the
wrapped main exits and guard drains. Pinned CNPG automatically deletes completed
Jobs: normal-completion cases pause only CNPG reconciliation at this barrier,
let the real Job controller finish and the original plugin observer record all
Pod/container terminations, then resume CNPG for normal instance SQL. The Job
controller and plugin manager remain active; no missing/deleted-object evidence
is certified. Run34148247516 preserves the post-Ready harness failure to find
completed Pods; its runtime uncertainty occurred35s BEFORE Job Complete, not a
proof that cleanup raced a healthy observer.
A dedicated runtime streaming client fixes that run's10s HTTP WATCH termination;
actual watch loss still closes observation without reconnect. Terminal Pod events
also eagerly run the existing bounded completion predicate, retaining the2s
fallback. API-seam tests distinguish delivered proof before cleanup, Job Complete
after the final Pod event, and disappeared/running Pod uncertainty. They do not
claim that eager Pod events solve every cleanup ordering.
The automatic-latest case additionally runs **without pausing CNPG**, with normal
Job cleanup. It requires surviving durable completion for the exact preobserved
Job/Pod UIDs, actual removal of its stable/own-reader source holders, clean target
markers and recovered SQL. This ordinary path waits directly on that surviving
proof, without first requiring a separate live Job/Pod LIST after cleanup. The
paused all-container/retry observation paths remain unchanged. A call-site
regression covers cleanup after durable proof; uncertain/wrong-UID/retained-hold
proof still fails. If natural cleanup wins before durable observation,
record that real uncertainty boundary and fail this normal-release case; neither
readiness nor missing objects is fabricated release evidence. No pause/shutdown recovery-target API is
advertised by this test. This is a single-node kind experiment, not multi-node
storage/power-loss certification.

## Evidence, resource bounds and next execution

`artifacts/recovery-campaign/` contains incremental `manifest.json`, fsynced
`events.jsonl`, immutable first-failure evidence, workload journal, original commit
metadata, inventory, selected target plans, original CNPG/RPC/PG/sidecar/operator
logs, actual Pod/Job termination evidence, version pins and resource observations.
Logs are bounded/redacted; Secrets, raw environments, private keys and raw database
archives are not uploaded. Artifacts expire after 14 days and are not permanent
release evidence. Unit-test results do not populate real scenario completion.

`.work/recovery-campaign/` holds owned disk-backed inputs, downloads and raw test
fixtures. Stale work/evidence is refused rather than overwritten. Successful
Cluster Pods are removed **after** evidence and clean completion checks to bound
live memory; target PVCs/poison markers are never cleared/reused by the harness.
The collector leaves kind/PVC state for diagnosis. After collecting a local run,
its recorded cluster can be deleted with the downloaded kind executable; never
clear a product poison marker to rerun a case.

The first integrated run must validate the observer's actual CNPG mount/command
placement, same-segment SWITCH/history endpoint distinction, real PG descendant isolation,
Job retry scheduling, source-reader lifetime and terminal-controller evidence.
None of these was executed by the disjoint writer. Preserve the first failure and
distinguish ineffective test injection from product failure. If the fixed matrix
exceeds the budget, split hosted execution with explicit aggregate coverage;
never remove families or call a partial timer outcome qualified.
