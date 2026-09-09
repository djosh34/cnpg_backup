# Execute the CNPG backup project

This is the **fresh-thread implementation orchestrator brief**. Read this first, not the chat history.

**State: finalized technical specification; launch is authorized only by the explicit READY resolution on [issue #14](https://github.com/djosh34/cnpg_backup/issues/14) linking the independently reviewed, pushed frozen commit.** All owner Q1–Q12 answers are recorded. Read the reconciled [design](design.md), its scoped evidence and [release/security contract](release-policy.md). Missing READY still prohibits implementation; planning finalization and independent review are not product qualification.

## Authority and completion boundary

The owner has authorized autonomous commits, branch pushes, PR creation, review/fix cycles, CI and merges **after the design is complete**. There are no routine owner approval or manual dispatch steps in implementation. Review findings may be rejected with recorded, defensible reasons; all findings must be addressed, not necessarily implemented. The repository is public and hosted CI runs automatically as needed.

The owner selected automatic **versioned GitHub releases and qualified container images**, with **no production deployment**: consuming teams run the product in their environments. Original project work is **all rights reserved**, not open source; preserve third-party licenses/notices and do not invent a downstream project license. The [release policy](release-policy.md) selects v0.1.0, Linux amd64, canonical GitHub releases with portable OCI archives and qualified `ghcr.io/djosh34/cnpg-backup-manager` / `cnpg-backup-pg18` images; the design/handoff resolution activates that contract. No production database/bucket operations belong to implementation or CI. Respect existing platform protections. [Executed platform preflight](research/platform-preflight.md) verified push/Actions/release/GHCR authority (and historical OIDC capability) without changing protections; recheck unexpected drift, never invent approvals or bypass rules.

**Owner release-closeout supersession:** [the explicit J/K resolution](https://github.com/djosh34/cnpg_backup/issues/24#issuecomment-5598040876) removes mandatory artifact/image signing, Sigstore/OIDC attestations and signature-verification gates from earlier READY/document wording. Historical signatures are not gates; generate no new ones. GitHub releases/GHCR, exact tested bytes/checksums, source/build/run traceability and actual security/correctness gates remain. Parent removes signing-only workflow requirements without rebuilding/requalifying unchanged images; no new design or approval phase.

## Sources of truth

- [Wayfinder map](https://github.com/djosh34/cnpg_backup/issues/1): planning resolutions and readiness. Every design/research blocker must be resolved before implementation starts.
- [Design](design.md), [remote design asset](https://github.com/djosh34/cnpg_backup/issues/2): agreed architecture and safety invariants; update it from resolution comments before freeze.
- [PR plan](pr-plan.md), [delivery graph](https://github.com/djosh34/cnpg_backup/issues/3): requirements, acceptance and native dependencies. PR sizes/package seams may adapt to evidence without dropping agreed behavior.
- [Testing](testing.md): load when implementing tests, CI or qualification.
- [Release/security contract](release-policy.md): exact endpoint, first-release policy, licensing, scans/byte traceability and resource gates (no artifact-signing gate). Research experiments are evidence for chosen mechanisms, not a substitute for actual product recovery.
- [Review contract](agents/review.md): load for every review, disposition and merge.
- [Paseo lifecycle](agents/paseo.md): load **before creating, waiting for, resuming or cleaning up any subagent**.

The [2026-09-07 owner policy update](https://github.com/djosh34/cnpg_backup/issues/14#issuecomment-5576280720) supersedes frozen operational role/wait/evidence wording while preserving READY and product safety/release gates. Newest owner directions override stale proposals. Git, issue resolutions, current PR/CI evidence and the latest progress comment are durable state; a model transcript is not.

## Model and child lifecycle — mandatory

**Agent managers/coordinators use Astra/medium; implementers and reviewers use Astra/high.** The exact role table, explicit separate Luna/xhigh cleanup exception, effective-setting verification and 1800-second event-driven waits are authoritative in [agents/paseo.md](agents/paseo.md). These are agent settings, not changes to production Go manager mode.

**Use Paseo for every subagent.** Do not bypass it with raw `pi` subprocesses. The orchestrator enforces **at most five concurrent child agents in total** across every role/task in this effort, by instruction and its task ledger—not a new limiter service or code. Count a spawned child until it has been archived successfully; idle/completed-but-unarchived children still consume a slot. Only the orchestrator spawns children; workers/reviewers do not recursively delegate. One writer per worktree.

**Archive every owned child immediately after collecting its result, including failed, canceled, timed-out or superseded work.** Paseo can retain enough memory to OOM if completed agents accumulate. Waiting/stopping is not archival. Capture report/evidence, archive, verify archived status, then reuse the slot. On interruption/resume, clean up owned leftover children before spawning more; never archive the parent or unrelated user agents. An archival failure blocks new spawns until repaired. The detailed commands and ownership rules live in [agents/paseo.md](agents/paseo.md).

## Readiness gate — completed during planning

Before declaring this project ready for the new implementation thread:

1. Finish the owner design grill and record actual answers. Resolve all open CNPG/native-backup/S3/storage/WAL/retention/security/test decisions with primary sources and targeted disposable experiments where necessary. MinIO is sufficient for the required validation.
2. Freeze a coherent design: exact initially supported versions/layouts, capture/reconstruction sequence, workspace requirements, configuration, portable publication/no-clobber and retry behavior, retention/restore coordination, PITR semantics, failure behavior, delivery endpoint and test gates. No unresolved algorithm is hidden behind "implementation will decide".
3. Reconcile design, PR graph and test requirements. State approved adaptation rules for new facts: preserve behavior and safety; change internal structure/PR boundaries and add regression coverage rather than ask routine implementation questions.
4. Verify Paseo Astra/high dispatch/result retrieval/archival, git/Actions permissions, merge-rule compatibility and credentials for the agreed release/image publication endpoint. Complete this preflight before launch.
5. Run independent fresh-context design reviews, dispose of their findings and archive reviewers. Commit the final plan and close the final handoff ticket with an explicit **READY** resolution linking the frozen commit and agreed endpoint.

The execution thread checks this gate; it does **not** complete missing up-front planning. If the gate is absent, report "design not ready" honestly rather than launch the build under assumed approval.

## Autonomous implementation loop

### 1. Orient and resume

Read readiness resolution and relevant repo instructions; inspect dirty files, branches, PRs, Actions and Paseo task ownership. Preserve unrelated changes. Reconcile existing work and archive owned orphaned children. Fetch only the next delivery issue and prerequisite resolutions, not every issue body.

**Done:** the latest progress comment names current frontier, branch/worktree ownership, Paseo children and next action. No duplicate work starts after restart.

### 2. Implement a frontier slice

Claim one unblocked delivery issue. Create an isolated branch/worktree from merged prerequisites. Dispatch a fresh Paseo worker with the issue/spec, base SHA, relevant design decisions, ownership, safety invariants, tests and non-goals. Let it choose ordinary implementation details within the finalized design. Its report gives exact HEAD SHA, actual commands/results and limitations. Collect that report and archive the worker.

**Done:** scoped implementation and relevant tests are committed on the branch, and the orchestrator verifies the diff and evidence. A green summary alone is insufficient. Parallel workers are useful only for genuinely disjoint work.

### 3. Independently review and adjudicate

Follow [agents/review.md](agents/review.md): two **fresh Paseo Astra/high** reviewers, correctness/spec and KISS/maintainability. Pin base/head and supply source/spec/evidence, not the author's reasoning transcript or each other's findings. Collect and archive each as soon as it finishes.

Fix substantiated findings with regressions; reject incorrect/unjustified findings with code or test evidence. Use a fresh adjudicator for substantive disputes. Resolve uncertainty by diagnosis and distinguishing tests, not by demanding the owner decide ordinary technical arguments. Serious unresolved defects keep the PR unmerged while agents investigate.

**Done:** every finding has a disposition under one current review target, relevant fixes are independently rechecked and all review/adjudication children are archived. No "find N issues" quota or endless stylistic rewrite loop.

### 4. Run CI and merge

Open/update the PR with its issue and evidence. Run the [local feedback ladder](testing.md#local-first-feedback) before expensive hosted matrices, then applicable CI and automated recovery campaigns. Recheck the current HEAD and changed merge behavior after fixes/base changes; reuse evidence for unchanged content as described in [agents/review.md](agents/review.md). Observe long CI jobs at 1800-second intervals per [tracker operations](agents/issue-tracker.md), not repeated short polls. Merge once the independent reviews are addressed and required checks pass; close the delivery issue and record the merged SHA. Do not ask the owner to click merge or dispatch a routine test.

**Done:** merged code, current review dispositions and CI evidence are linked to the tested subject. Never bypass branch protection or count same-account agent comments as another human's GitHub approval; any incompatible rule should already have been resolved during planning preflight.

### 5. Advance and qualify

Repeat through the entire graph. Exercise recovery continuously, not just at the end. Qualify exact immutable candidate images with the real-system campaign, DST/fuzz corpus, restore SQL assertions and upgrade fixtures. Produce install/recovery runbooks and version/resource evidence. Publish the agreed versioned release and qualified image digests automatically, retaining third-party notices and the owner's all-rights-reserved project policy, without an extra approval ceremony or production deployment.

**Done:** every required slice is merged, every applicable mandatory test passes for the delivered artifacts, findings are addressed, the agreed artifacts/docs/release exist, and all owned Paseo children are archived. A workflow file without a successful run is not qualification. Report genuine incompletion instead of inventing success.

## Adaptation, fault handling and continuity

- Maintain one short progress comment per meaningful transition: issue, branch/worktree, current subject, PR/review/CI links, active/archived Paseo IDs, next action and blockers. Link the existing evidence record instead of copying SHAs into every finding/log/comment. Update active run briefs/checkpoints with current role and wait policy before resume; historical observations remain historical. Use the existing tracker; no custom task database or orchestration platform.
- New evidence may change internal algorithms, package seams, test arrangements or PR boundaries while preserving the frozen external contract and safety. Record why, update affected graph edges/docs and rerun the relevant independent review/regressions. This is autonomous engineering, not a return to an open-ended design interview.
- After repeated no-progress attempts, switch to focused diagnosis/fresh adjudication rather than blind reruns. Continue independent unblocked work. Archive failed/superseded workers before replacing them.
- Unexpected infrastructure/authentication loss or a proven contradiction that prevents safe delivery is a genuine blocker, not a planned human approval step. Repair within existing authority, retain evidence and stop the affected operation if no safe path exists; never weaken durability, disable failing tests or invent permission to claim completion.
- Checkpoint before context/time exhaustion and archive owned idle/completed children. Resume from tracker/git state in another Paseo thread if necessary. Do not promise that one finite context can hold the entire project.
- A fresh **orchestration** context may take exclusive dispatch ownership from a supervising planning context; this is not recursive worker delegation. Exactly one dispatcher is active. If that successor is itself an unarchived child, count it in the shared five slots and allow at most four additional workers/reviewers. It returns a durable checkpoint with its own children collected/archived; the supervisor then collects/archives/verifies the successor before launching a replacement. Workers/reviewers never dispatch children.

## Launch prompt — valid only after READY

> Read `docs/EXECUTE.md` and the READY resolution on the design/handoff issue. Implement the complete finalized design and delivery graph autonomously: Paseo subagents only, managers/coordinators Astra/medium and implementers/reviewers Astra/high with the explicit cleanup exception in `docs/agents/paseo.md`, 1800-second event-driven waits, at most five concurrent children total, and mandatory verified archival after every task. Run independent fresh reviews, address or reject findings with evidence, run CI/recovery qualification and merge the PRs without routine human intervention. Complete the agreed delivery endpoint. Adapt internal details to new evidence without weakening the approved contract; checkpoint and resume when necessary.
