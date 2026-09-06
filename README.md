# cnpg_backup

Planning a CGO-free Go CNPG-I plugin for direct PostgreSQL 18 backups to S3-compatible storage. MinIO is the integration target; a maintained SDK provides Signature V2/V4 support for intended deployments including Dell ECS. Dell access/testing is not required.

**Status:** planning snapshot; final design grill and technical resolutions are still in progress. No backup implementation yet. All design questions must be resolved before the autonomous implementation handoff.

**New agent thread:** start with [docs/EXECUTE.md](docs/EXECUTE.md). It requires a finalized READY design, then covers Paseo-only Astra/high workers, a five-child instruction limit, mandatory archival, independent review, automatic PR delivery and resumable progress.

## Planning

- [Wayfinder decision map](https://github.com/djosh34/cnpg_backup/issues/1) — canonical decision index and native blockers.
- [Design proposal](https://github.com/djosh34/cnpg_backup/issues/2) — architecture, trade-offs, safety requirements and sources.
- [PR delivery graph](https://github.com/djosh34/cnpg_backup/issues/3) — eleven planned PRs with requirements and acceptance criteria.

Local planning assets:

- [Proposed design](docs/design.md)
- [Proposed PR plan](docs/pr-plan.md)
- [Recovery testing, DST/fuzz and the two-hour Actions campaign](docs/testing.md)
- [Independent review and adjudication](docs/agents/review.md)
- [Initial research and unverified assumptions](docs/research/initial-reconnaissance.md)
- [Domain glossary](CONTEXT.md)

Unresolved decisions remain open in GitHub. Planned PR issues are not opened pull requests or evidence of completed implementation.
