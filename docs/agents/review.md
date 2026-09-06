# Independent PR review and adjudication

Load from `docs/EXECUTE.md` when a PR is ready for review, findings arrive, or merge is considered. This is an execution contract, not evidence that reviews already happened.

## Pin the review target

The orchestrator records base SHA/merge-base, HEAD SHA, commit list, changed paths and `git diff <base>...<head>`. Fetch the originating delivery issue, prerequisite decisions and relevant repo standards. Provide a scoped brief and a clean snapshot/diff file to each reviewer. The snapshot must include context needed to trace behavior beyond changed lines.

Start **two independent fresh Paseo sessions**, neither forked/resumed from the author nor supplied the author's private reasoning or the other reviewer's findings. Follow [paseo.md](paseo.md): Astra/high for both, at most five concurrent children total across the effort, and collect → archive → verify immediately after each reviewer finishes.

- **Correctness/spec reviewer — GPT-6 Astra, high:** missing/wrong requirements, backup consistency, durable acknowledgment, restore selection, retention/dependencies, crash/retry/concurrency behavior, security and whether tests could pass while the product is broken. Trace through real callers. Select only relevant risks for the PR.
- **KISS/maintainability reviewer — GPT-6 Astra, high:** small understandable interfaces, clear ownership, justified dependencies, bounded resource/cancellation paths, unnecessary abstraction/configuration and testability. Prefer simplifying code over hypothetical flexibility. Style preferences are not blockers unless a documented standard or concrete maintenance problem supports them.

Read-only reviewers do not edit the branch, commit, post approvals or launch more agents. They can request a specific test from the orchestrator if evidence is missing. CI and prior author tests are supplied as evidence, not assumed to be correct. The independent review sees the diff, source, requirements and actual results, not instructions to endorse the author.

## Finding format

Each finding has an ID, category/severity, file/line, concrete scenario, requirement/invariant violated, reasoning/evidence and a minimal proposed correction. Label uncertainty and distinguish observed failure, reasoned bug, missing evidence and optional suggestion. Zero findings is valid. Do not invent findings to meet a quota or demand a rewrite merely because another architecture is possible.

Required checks live in CI; avoid flooding the report with already-enforced formatting issues. A missing critical regression scenario is a real finding even if current tests are green.

## Disposition loop

The orchestrator reads the code/evidence and records each finding on the PR:

| Finding | Disposition | Evidence | Current SHA |
| --- | --- | --- | --- |
| review ID | accepted/fixed, rejected, superseded, or deferred nonblocking | regression/result, code trace or explicit trade-off | SHA |

- **Accept/fix:** implement the smallest safe correction, add the relevant regression and rerun affected checks. Authors may rebut with evidence; they do not have the final word on their own correctness.
- **Reject:** cite why the scenario is impossible, already covered, outside approved scope, factually wrong or costs more complexity than its justified benefit. "I disagree", "too much work" or "CI is green" alone are insufficient. Preserve the original finding and response.
- **Defer:** only an optional/nonblocking improvement can move to a follow-up issue. A plausible data-loss, recovery, auth or deletion defect stays blocking until disproven or fixed.
- **Disputed serious finding:** use one fresh Paseo GPT-6 Astra/high adjudicator with both positions, code and tests. It may uphold or reject either side; prefer an executable distinguishing case. If still unresolved, keep the PR blocked while agents run focused diagnosis and distinguishing experiments; do not vote by agent count or route an ordinary technical dispute to the owner. Archive the adjudicator after collecting its report.

A follow-up fresh review verifies material fixes and interactions against the new SHA. Reviewers need not adopt suggestions without merit. Track accepted/rejected findings separately from platform review threads; never dismiss a human review or bypass branch protections as an automation shortcut.

## Merge gate

The orchestrator verifies current-HEAD evidence, required CI, all blocking findings resolved with reasons, no unrelated change and approved merge authority. If the base changed, update/recheck the merge result and seek review of changed behavior. Keep review reports small; logs and reproducible evidence are linked.

The same GitHub account may create the PR and post agent review comments. That supplies independent **contexts**, not independent human identities or platform approvals. Planning preflight must establish that repository protections permit the agreed autonomous workflow. If an unexpected protection requires another person's approval, report a genuine infrastructure blocker without pretending it is satisfied; do not design that human step into the normal delivery loop. Never manufacture approvals or claim the author self-review fulfilled the two-context requirement.
