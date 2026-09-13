# Working on cnpg_backup

- For implementation, read the task issue and relevant sections of `docs/design.md`. Use `CONTEXT.md` for domain terms. Record changed decisions in the issue and update affected product docs.
- Keep production Go CGO-free. PostgreSQL tools are the only permitted native runtime exception. Prefer concrete modules, small interfaces, bounded I/O and concurrency, and explicit error ownership. Keep fault orchestration in tests.
- For tests, CI, or qualification, read `docs/testing.md`. Use the existing `hack/test` profiles. MinIO is the required integration target; additional platforms are not a gate.
- Before creating, waiting for, resuming, or cleaning up subagents, read `docs/agents/paseo.md`.
- For PR review, adjudication, or merge, read `docs/agents/review.md`, including autonomous delivery authority and evidence requirements.
- For GitHub issue, PR, or dependency operations, read `docs/agents/issue-tracker.md`.
- For releases or dependency changes, read `docs/release-policy.md`. Preserve the project's all-rights-reserved license and third-party notices. Tests and delivery do not authorize production database or bucket operations.
