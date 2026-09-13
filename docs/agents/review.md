# Review and merge PRs

Within the approved task scope, agents may commit, push branches, create PRs, run CI, resolve findings, and merge autonomously. Routine owner approval or manual test dispatch is not required. Respect task-specific limits and repository protections. Release authority follows [release-policy.md](../release-policy.md) and does not include production deployment.

## Prepare independent reviews

Record the review target once: base or merge-base, HEAD, and the diff. Give reviewers the originating issue, relevant decisions and standards, source context, and actual test evidence.

Use two fresh, read-only contexts under the [Paseo lifecycle](paseo.md). Neither reviewer receives the author's private reasoning or the other reviewer's findings.

- The correctness reviewer traces requirements, backup consistency, durable acknowledgment, recovery selection, retention dependencies, concurrency, and security through real callers. Check whether tests could pass while the product is broken.
- The maintainability reviewer checks interface size, ownership, resource bounds, cancellation, dependencies, and unnecessary abstraction. Style preferences are not blockers without a documented standard or concrete maintenance cost.

Reviewers do not edit the branch or post platform approvals. Missing critical regression coverage is a finding even when CI passes. Zero findings is valid.

Each finding names its severity, file and line, failure scenario, violated requirement, supporting evidence, and smallest proposed correction. Distinguish observed failure, reasoned risk, missing evidence, and optional suggestions.

## Resolve findings

Record each finding's disposition on the PR, under the shared review target:

| Disposition | Required evidence |
| --- | --- |
| Fixed | Correction, regression, and affected test results |
| Rejected | Code trace, test, scope limit, or justified complexity trade-off |
| Deferred | Nonblocking follow-up issue |
| Superseded | Change that makes the original finding inapplicable |

A plausible data-loss, recovery, authorization, or deletion defect remains blocking until fixed or disproven. For a substantive dispute, use a fresh adjudicator with both positions, code, and tests. Prefer an executable distinguishing case. Keep unresolved serious defects unmerged rather than voting by reviewer count or escalating ordinary technical disagreements to the owner.

Use fresh review to verify material fixes and their interactions. Preserve original findings and responses. Platform review threads and human approvals are separate from agent dispositions.

## Merge with current evidence

Merge when required CI passes, blocking findings have dispositions, the diff contains no unrelated work, and evidence applies to current HEAD. If the base changed, inspect the merge diff and review or test changed behavior.

A message-only change or identical tested merge tree does not require repeating an expensive matrix. Record the content comparison once and link prior results while satisfying platform-required checks. Material source or harness changes need relevant new evidence. Dirty-tree results are diagnostics, not release qualification.

Keep subject identity once in the review or run record. Record a distinct harness revision when it differs. Preserve exact image digests, input and backup checksums, and promotion of the same qualified bytes. A rebuilt image does not inherit qualification from a matching source SHA.

Same-account agent comments provide independent contexts, not independent human identities or GitHub approvals. Never manufacture approvals, dismiss a human review, or bypass branch protection. Unexpected permissions or protection requirements are blockers to repair within existing authority, not checks to pretend have passed.
