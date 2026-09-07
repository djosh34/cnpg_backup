# Testing and recovery campaigns

Status: implementation specification. Disposable planning experiments have run and are linked from [design.md](design.md); no product implementation or qualified product artifacts are implied by those results. Required backend is **real MinIO**, accessed through the maintained Go SDK with SigV2/SigV4. **Dell hardware/access/version discovery and Dell-specific tests are not required.** Compatibility claims distinguish MinIO-tested behavior from intended standard-S3 deployments.

## Keep the product small; exercise it hard

Production code contains a few real I/O seams, explicit operation ownership and small understandable state transitions. Test code owns fault scheduling, scenario generation, independent oracles and replay tooling. Do not add a production scheduler, general virtual filesystem, pluggable storage framework or fault-control endpoint just for tests.

Three complementary methods:

1. **Deterministic simulation (DST):** execute actual repository/WAL/retention orchestration code against controlled fake storage/time/completion order. Same seed, operation count, binaries and harness version must produce the same modeled trace/result. The oracle is independently written, not the production retention/selection algorithm called twice.
2. **Real-system seeded fault campaign:** real PG18, CNPG, Kubernetes and MinIO; generate a repeatable workload and fault plan, record observations and replay the plan. Kernel/network/process/Go scheduling is not fully deterministic. Seeded chaos is not identical to deterministic simulation and does not guarantee an identical failure interleaving.
3. **Fuzzing:** stdlib Go fuzz tests for parsing/validation/extraction/planning inputs, with corpus retention and minimized failing inputs. A fuzz duration does not make the search deterministic; the failing input is the reproduction artifact.

## Test tiers and where they arrive

| Tier | Trigger / initial target duration | Required behavior |
| --- | --- | --- |
| Fast | Every PR, roughly 5–10 minutes | CGO-free build, unit/property/regression corpus, vet, dependency checks; small fixed-seed DST corpus as modules arrive |
| Integration | Relevant PRs, roughly 15–25 minutes | Actual PG18/native tools + MinIO, full/differential/PITR SQL assertions; lightweight CNPG smoke once lifecycle lands |
| Recovery campaign | `workflow_dispatch`; default total 120 minutes | Fixed mandatory real-CNPG recovery/fault scenarios, seeded exploration, DST and long fuzz; exact artifact/seed report |
| Qualification | Same reusable workflow before supported candidate publication | Full required profile at the candidate SHA/image digest, plus previous-release restore fixtures and all declared support cases |

Durations are execution targets, not excuses to skip a slow required scenario. This public repository uses GitHub-hosted CI for automatic checks and qualification; no owner dispatch or financial approval step is required. Short profiles may pass their own scope but must report **not release-qualified**. Every product PR adds regression coverage immediately. PR A lays the harness foundation, PR C adds DST against real module logic, later feature PRs extend both real-system and simulated scenarios; the final certification PR completes automation/evidence.

Use `CGO_ENABLED=0` for production builds and runtime-dependency validation. A separate Go race-detector job may use its required C toolchain/test-only CGO setting; that does not authorize CGO application dependencies or native libraries in the shipped Go executable. Document this tooling distinction rather than turn off race detection or relax release builds.

## Local-first feedback

Run the cheapest distinguishing check **locally before pushing an expensive matrix**, using the same `hack/test` and test modules as CI:

```sh
./hack/test harness                         # Python fixtures/oracles, metrics self-test, generated CRD; no downloads/Docker
./hack/test harness test_lifecycle_harness  # targeted unittest module, class or method
./hack/test unit                            # above + pinned Go vet/unit/DST corpus and Promtool; no native/image build
./hack/test fast                            # also native input/build/runtime-closure checks
./hack/test integration --seed 1806         # actual host PG18 + MinIO, no Docker
./hack/test integration --seed 1806 --images # same integration plus actual shell-free images
./hack/test guard                           # actual Docker PID1/namespace tests
./hack/test cnpg-smoke                      # actual Docker/kind/CNPG/MinIO
```

For a Go regression, run the relevant package/test directly with the pinned toolchain after bootstrap, then `unit`/`fast`. For a campaign oracle, run its `test_*.py` module through `harness` before provisioning; new harness tests are automatically discovered locally and in CI. Keep negative controls that catch wrong SQL, missing faults or unsafe cleanup. Python fixtures do not establish real Kubernetes/recovery coverage. `unit` and `harness` are feedback profiles, not qualification or substitutes for required CI.

See [build-and-harness.md](build-and-harness.md#run) for prerequisites and native-only diagnostics. Check actual capabilities (Docker socket/daemon access, rootless namespaces/cgroups, kernel restrictions, CPU/RAM/disk) before choosing local real-system profiles. A missing Docker CLI or a blocked kernel feature is a **current-host limitation**, not proof local integration is permanently impossible; do not install/reset a daemon or provision the system as a workaround. Run the existing harness unchanged on a suitable disposable local host when available.

On the assessed Fedora worker host, unprivileged user namespaces work, but Docker/Podman/kind and Docker sockets are absent; the pinned MinIO binary fails even `--version` because route netlink is unavailable. Native PG tools can use the harness's private extracted libraries. Thus Python/Go/native diagnostics are feasible here, but real MinIO, Docker PID1/shell-free-image and kind/CNPG acceptance still need a capable host (currently hosted Linux CI). Preserve failed-attempt evidence; a native-only run never counts as MinIO/CNPG coverage. Recheck resources: this host's free disk is shared with active work and `/tmp` is tmpfs.

The mandatory hosted checks still run; avoid using them to discover simple fixture/assertion bugs. Release/security/resource gates remain: exact candidate image recovery/fault/layout/version matrices, race tooling where a C toolchain is available, vulnerability scans, measured cgroup/workspace behavior and trusted Actions OIDC/publication. Most tests can also run locally with the same prerequisites; trusted release authority/provenance is not replaced by a local green run. Long agent/CI observations use [1800-second waits](agents/paseo.md#wait-for-events-not-short-polling); local assertions and scenario readiness keep short, meaningful deadlines.

## Deterministic simulation contract

Use stdlib `testing` and a small seeded scenario driver. `testing/synctest` may supply virtual time for suitable Go goroutine tests; it does **not** make arbitrary scheduling/external I/O deterministic. Control storage completions and simulated events explicitly at existing I/O seams. Sort map-derived operations; avoid wall-clock/random global inputs in replayable scenarios. Prefer an explicit clock argument for retention cutoff over a clock interface threaded through every package.

Exercise production operations, not just an abstract model:

- Store durability separated from response delivery: PUT/commit succeeds remotely but response is lost; duplicate retry, differing-content retry, partial multipart and crash before/after publication.
- Controlled missing/corrupt objects, failed/partial list pages, delays and permanent/transient failures. Model the documented storage contract; adversarial contract violations are a separately labeled fail-closed test, not a claim that standard S3 behaves that way.
- Restart discards in-memory state but preserves committed fake-store state; reload production catalog and resume operations.
- Interleave backup publication, retention planning/execution, restore protection, failover/stale work and cancellation at explicit completion points.
- Check independent invariants after every event: no premature WAL success; no visible incomplete backup; no loss of parents/WAL for retained plans; no deletion under uncertain/pause state; same-content retries safe; differing-content writes refused.
- Track bounded progress after faults clear, buffer/concurrency limits and eventual cleanup eligibility. Distinguish safety under failure from liveness under explicitly restored prerequisites.

Reproduce with a machine-readable seed plus operation count/fault trace and version pins. A fixed operation count, rather than elapsed wall time, defines the deterministic replay length. Shrink failures by removing operations/faults where possible. Promote each minimized failure to the checked-in corpus. Exercise negative controls (e.g. intentionally damaged fixtures or a test-only deliberately faulty store/result) to establish that the oracle can actually detect the class of fault; do not expose insecure production toggles.

## Real system and workload oracle

On a standard hosted Linux runner, use Docker and a pinned single-node kind cluster with the actual CNPG operator/plugin manifests, actual PG18 instances and a pinned MinIO image. Use at least primary + standby in failover scenarios; add a second standby where useful and where the runner's measured RAM/disk capacity supports it. A one-node test does not establish multi-node hardware/storage resilience.

Use isolated namespaces/buckets/prefixes and ephemeral test credentials. Keep MinIO data on a test persistent volume when killing/restarting its process so that "restart" does not accidentally mean "delete all backups". Full runner/node loss and physical disk power failure are outside this environment's evidence. Query actual CPU/RAM/disk capacity; size fixtures above application buffers while leaving space for backup inputs and reconstructed outputs. No production endpoint or secret is accepted by the campaign.

A simple single-writer transactional workload provides a known order: keyed rows, checksums, updates/deletes/truncates/drop-recreate cases and named/sentinel recovery targets. Journal transaction IDs and observed commits **outside the source database**. Resolve ambiguous commit outcomes before treating them as oracle facts; discard/reclassify an ambiguous target rather than invent an expected committed state. Prefer explicit barriers/LSN/named points over guessed sleeps or host clocks; include time-target tests using PostgreSQL time and ordered commits.

Recovery creates a fresh Cluster/PVC, restores full or full+differential, replays WAL and compares SQL contents against the independent expected state. It must show pre-target effects present and post-target effects absent. Check timeline/role and target completion, not just pod readiness, RPC success or pg_combinebackup's exit code. For latest recovery compare against a known durably archived barrier, not transactions that were never acknowledged into the archive.

Faults are scheduled at observable barriers (e.g. backup upload active, differential selected, archive backlog established). Record the requested fault, observed precondition and actual timestamps/events; a fault that never fired does not count as covered. Use a small test-only proxy to delay/drop/fail S3 requests where useful; real socket/TLS integration runs separately from fakes. Avoid a generic chaos platform unless it demonstrably removes more harness work than it adds.

### Mandatory scenario families

- Full backup + latest restore; two independent differentials from one full; chosen differential + WAL PITR; on-demand and ScheduledBackup invocation. Verify original full/incremental inputs before combine: preserve the negative control demonstrating that a new synthetic manifest can validate already-corrupted input.
- **S1 bundle/archive classification:** intact selected bundle with no archive duplicate must recover via exit1/local fallback; an unavailable local required bundle fails. Prefer full archive bytes for a post-EndLSN target in the **same** final bundled segment; padding cannot hide required remote bytes. Required remote absence is 255; TLS/auth/transport/corruption remain 255 with a bundle present. Retain negative controls for erroneous all-required-255 and bundle-as-archive-success handling.
- **K1/S2 target ownership:** actual guard runs before CNPG preflight. Pause old main before RPC, delay RestoreResponse, pause replay/shutdown, launch replacement on the same PVCs and assert no target rename/delete/reconstruction. Kill sidecar while main PG survives; kill guard; both leave markers blocking reuse. Test detached PG descendants, pending sidecar writes, incarnation mismatch and delayed callbacks; release only after all main descendants are reaped and the original sidecar closes/drains admission. Clean release permits a new owner; poisoned retry uses fresh Cluster/all-fresh PVCs. No timer/API deletion unlock. Require real PID1/private-namespace tests, not only the disposable model.
- **R0 shell-free verification:** actual data image has no shell; every tar/extracted original/synthetic verification uses `pg_verifybackup --no-parse-wal` plus direct Go `exec.Command` of `pg_waldump` for every validated range. Missing/corrupt range must fail, including a later range in any accepted multi-range input; rejecting unsupported ranges is not silently skipping them. Historical host-shell research runs are not this image test.
- **Prove remote WAL replay, not just bundled-WAL recovery:** after the selected full/differential finishes, write before/after-target sentinels beyond its bundled-WAL coverage, force segment switches and verify required remote archival. Restore into a fresh cluster with only repository/configuration access. Record the backup WAL boundary and target, verify SQL inclusion/exclusion, then repeat with a required post-backup archived segment missing/corrupt and require recovery failure.
- Deliberate DROP followed by pre-DROP PITR; time/LSN/explicit-backup and approved named-point/XID semantics. Newest base too new for target selects an earlier usable backup or fails clearly.
- Source namespace/Kubernetes catalog loss followed by restore using only S3/config; new restored cluster archives to a different repository.
- Kill/restart the plugin during upload and after remote commit but before response; repeat callbacks; kill manager during reconciliation. Verify actual acknowledgment and retry behavior.
- MinIO restart/outage, connection reset, lost response and slow/rate-limited transfers; WAL backlog grows honestly and drains when faults clear without being starved by base backup.
- Switchover/failover during workload/backup; archive/retrieve timeline history and recover across the supported timeline path. Retry/fail differential according to approved timeline policy.
- Missing/corrupt WAL, parent, manifest and tar data; truncated gzip; unsupported schema. The actual injected Go wal-fetch helper must use existing Unix RPC, exit1 only for authenticated allowed absence and exit255 for required gaps/storage/helper failures; prove no latest-recovery promotion on transient faults. Stock CNPG v1.30 error exits are not a passing substitute. Expected missing future WAL is not conflated with authentication/network errors or corruption of required WAL.
- Failed full/differential backups leave their last-success timestamps unchanged and report the requested type in failure metrics, status and Warning events. Missing-summary/parent faults must fail the differential without invoking/uploading a replacement full. Test retry versus terminal-failure counting, restart/unknown-history behavior and independent full/differential freshness alerts with schedule-aware thresholds.
- Concurrent retention and backup; interrupted deletion; retained old full supporting a recent differential; last usable full retained after prolonged failure; repository-wide deletion protection through the last required WAL read, using the protocol finalized before READY. Exercise a delete already in flight when protection is requested, multiple restores, cross-cluster participants, controller/recovery crashes, paused PITR, and unsafe premature release/expiry. A lost destructive owner must block new admission, while lost reader/backup holders block GC but permit new protected restores. Normal controller completion releases only stable lifetime protection, never a different process's uncertain reader holder. Uncertain/crashed restores remain protected rather than lose their hold on a timer; keep the production mechanism small and put interleaving complexity in tests. Assert rate-limited `RetentionBlocked` Warning events and durable status; event delivery is not lock state. All protected restores are performed through this plugin, including cross-cluster and source-cluster-loss recovery. Test automatic coordination and recovery after original Kubernetes objects disappear. Arbitrary external S3 readers and special protection for unannounced/read-only third-party downloads are out of scope.
- Bounded workspace exhaustion and credential/private CA rotation; no secret leakage. Avoid indiscriminately filling the runner root disk—use a size-limited test workspace/quota.
- Supported separate WAL volume/tablespaces; unsupported layouts rejected before claiming success. PG/native-tool dependency update and old-to-new backup/WAL restore.

Mandatory fixed regressions run before random exploration. Selected randomized campaigns add coverage rather than replace named scenarios. Long fuzz targets include manifests/parent graphs, WAL/history names, tar paths/symlink escapes, gzip truncation/expansion limits and restore selection. Archive extraction tests must actually attempt escapes and verify no file outside the destination changes.

## GitHub Actions interface

Implement one test entry point that runs locally and in Actions, plus a reusable recovery workflow invoked by `workflow_dispatch` and `workflow_call`. Suggested path `.github/workflows/recovery-campaign.yml`; names and implementation details may change without changing this contract.

Inputs:

- exact trusted code ref/SHA and optional existing plugin image **digest** (resolve tags once and record the digest);
- profile (`smoke`, `recovery`, `qualification`), seed/seed list, bounded duration (default 120 minutes), optional replay-artifact identifier;
- pinned compatibility set (PG18, CNPG/Kubernetes/MinIO/SDK/tool versions); don't expose arbitrary shell commands or secret-bearing endpoints as inputs.

Manual dispatch must be available from the default branch before users can invoke it. Print an example `gh workflow run ... -f seed=... -f duration_minutes=120` after the workflow exists, not a pretend runnable command before it does. The trusted reusable workflow can run against release-candidate artifacts before publishing. Existing release images can be tested by digest; distinguish harness revision from subject-image revision, and ensure tests consume the selected image rather than silently rebuilding HEAD.

Default total campaign target is 120 minutes including setup/collection. Example allocation: 15 minutes provisioning, 20 DST, 20 fuzz, 55 real-system scenarios and 10 collection; measure/rebalance within the cap. **Job hard timeout 150 minutes** leaves a safety margin. Implement an earlier inner deadline and stop generating faults with enough time for final recovery/diagnostics. A killed/canceled job cannot guarantee artifact upload, so save progress incrementally and use an always-run collector where possible. If mandatory coverage is not finished before the inner deadline, mark qualification incomplete/failing; the agent diagnoses and splits/rebalances the campaign or adjusts its runtime within platform limits. No manual approval is part of that process. The hard timeout is not a passing outcome.

Separate short PR checks from long campaigns. Use concurrency groups to avoid canceling required release evidence; independent seeds/version sets may run as matrix jobs on standard public-repository hosted runners. The agent dispatches qualification and regression reruns automatically, preserves first-failure evidence and uses timeouts to detect hangs. Manual dispatch remains available as an additional entry point, not a mandatory human step. Scheduled runs may reuse the same workflow without a separate test system.

Use least-privilege `GITHUB_TOKEN`, `contents: read` by default, minimal explicit artifact permissions, and pinned Actions. No deployment/cloud secrets. Never combine untrusted PR code with `pull_request_target` privileges. Avoid inserting untrusted workflow inputs directly into shell syntax. Test workflows need no AI/model credentials; agent orchestration runs through Paseo in the user's agent environment, not a model loop inside CI.

## Artifacts, replay and pass criteria

Each campaign writes a machine-readable manifest and concise job summary:

- one run record associating run ID/attempt with the subject revision and actual image digests; record a distinct harness revision when different, all version pins, seed(s), operation count, selected profile, observed resource limits and phase timings;
- requested/executed/skipped scenarios and fault-precondition evidence; test exit codes, assertions, independent workload journal, minimized DST/fuzz reproductions;
- redacted plugin/PostgreSQL/MinIO/operator logs, Kubernetes events, backup metadata/inventory and relevant metrics; small failure fixtures where safe and useful;
- exact local replay command and Actions inputs. A seed alone is insufficient for real-system failures; retain the observed event/fault trace too.

Keep subject identity once in the run record and link it from summaries/reviews; do not add repeated SHA-format tests or per-assertion revision fields. Trusted-ref validation, input/backup checksums and exact qualified image digests remain safety checks. Dirty local source is labeled diagnostic; a source SHA alone cannot identify dirty bytes or authorize release of rebuilt images.

Capture evidence on success and failure, with bounded artifact size and configurable retention (propose 14 days for ordinary runs; release summaries and minimized regressions persist in the repo/release evidence). Runner artifacts are not permanent release evidence. Do not publish credentials, raw agent sessions or unrelated production data; fixtures are synthetic.

**First-release bootstrap:** if no previous project release exists, previous-release upgrade checks are explicitly inapplicable, not silently skipped or an impossible gate. Record that reason and establish initial-format backup/WAL fixtures during implementation for later releases. All first-release recovery/fault scenarios remain mandatory. From the next release onward, qualifying against retained previous-release artifacts is required.

Release-qualified means the exact candidate artifact passed every applicable mandatory scenario, required unit/integration/DST corpus, required fuzz duration, upgrade fixtures and current independent review gates. Record completed workload and scenario counts; elapsed two hours alone is not evidence. A deliberate corrupt-input test passes only when the expected failure is detected safely. Unexpected timeout/flake is triaged; retain the first failure and replay evidence instead of hiding it behind an eventual green rerun. Stable regressions become mandatory corpus cases.

The initial campaign duration fits under GitHub-hosted runners' documented six-hour job limit; recheck current platform execution limits when implementing. No claim that CI chaos simulates physical power failure, every network interleaving or Dell-specific implementation behavior.

## Primary references

- [GitHub Actions limits](https://docs.github.com/en/actions/reference/limits).
- [Manual/reusable workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows).
- [Go testing time and synctest](https://go.dev/blog/testing-time).
- [Go fuzzing](https://go.dev/doc/security/fuzz/).
