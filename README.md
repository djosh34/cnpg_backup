# cnpg_backup

A CGO-free Go CNPG-I plugin in development for direct PostgreSQL 18 backups to S3-compatible storage. MinIO is the integration target; a maintained SDK provides Signature V2/V4 support for S3-compatible deployments that satisfy the documented capability contract. Claims are limited to tested behavior and stated capability requirements.

**Status:** [READY](https://github.com/djosh34/cnpg_backup/issues/14#issuecomment-5564016816) authorized implementation. Repository/storage primitives, CNPG lifecycle and [synchronous WAL Archive/Restore](docs/wal-implementation.md) are implemented. Native full/differential capture, protected primary restore/PITR and conservative retention are implemented. Operational/security acceptance is in progress; no qualified release is claimed.

## Operate

- [Install, backup, fresh-cluster recovery, rotation, update and resource runbook](docs/operations.md)
- [Backup monitoring](docs/backup-monitoring.md) and [operational alerts](config/operational-alerts.yaml)
- [Install renderer](config/render.py), [fresh recovery renderer](config/recovery.py), and [network-policy example](config/network-policy-example.json)

## Build and test

```sh
./hack/test fast
./hack/test integration --seed 1806 --images
./hack/test cnpg-smoke # actual lifecycle + WAL faults/failover, not release qualification
```

See [build inputs, dependency inventories and recovery evidence](docs/build-and-harness.md) for prerequisites, local diagnostics and exact scope.

**New agent thread:** start with [docs/EXECUTE.md](docs/EXECUTE.md). It requires a finalized READY design, then covers Paseo-only role-based thinking, 1800-second waits, local-first test feedback, a five-child instruction limit, mandatory archival, independent review, automatic PR delivery and resumable progress.

## Planning

- [Wayfinder decision map](https://github.com/djosh34/cnpg_backup/issues/1) — canonical decision index and native blockers.
- [Design specification](https://github.com/djosh34/cnpg_backup/issues/2) — architecture, trade-offs, safety requirements and sources.
- [PR delivery graph](https://github.com/djosh34/cnpg_backup/issues/3) — eleven planned PRs with requirements and acceptance criteria.

Local planning assets:

- [Implementation specification and research evidence](docs/design.md)
- [PR plan](docs/pr-plan.md)
- [Release endpoints, licensing and security gates](docs/release-policy.md)
- [Recovery testing, DST/fuzz and the two-hour Actions campaign](docs/testing.md)
- [Independent review and adjudication](docs/agents/review.md)
- [Initial research and unverified assumptions](docs/research/initial-reconnaissance.md)
- [Domain glossary](CONTEXT.md)

GitHub resolutions and current PR/CI/release evidence are the durable delivery state. Planned PR issues and planning experiments are not evidence of completed product implementation.

## License

**All rights reserved** for original project work. This is not an open-source license grant. Third-party components retain their own licenses and required notices.
