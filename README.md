# cnpg_backup

A CloudNativePG plugin for direct PostgreSQL 18 backups to S3-compatible storage. It supports full backups, differentials that depend on one full, fresh-cluster recovery with point-in-time targets, WAL archiving, and conservative retention.

[v0.1.0](docs/releases/v0.1.0/README.md) is available for Linux amd64. The [release qualification record](docs/releases/v0.1.0/qualification.md) describes the tested versions and evidence limits. MinIO is tested with Signature V2, Signature V4, and a private CA. Other endpoints must meet the [storage requirements](docs/repository.md#storage-requirements).

## Operate

- [Install, back up, recover, rotate credentials, and update](docs/operations.md)
- [Repository and CNPG configuration](docs/configuration.md)
- [Backup, WAL, restore, and retention monitoring](docs/backup-monitoring.md)
- [Release assets and installation](docs/releases/v0.1.0/README.md)

A differential never falls back to a full. Restore requires a new destination repository lineage and fresh target PVCs. Uncertain recovery holders and target markers do not expire. Healthy `archive_timeout=60s` still adds queue and upload latency, and outages have no fixed recovery-point bound.

## Develop

```sh
./hack/test harness
./hack/test unit
./hack/test fast
./hack/test integration --seed 1806 --images
```

See [build prerequisites](docs/build-and-harness.md), [test profiles and safety coverage](docs/testing.md), and [exact-image recovery campaigns](docs/recovery-campaign.md). Production Go builds use `CGO_ENABLED=0`. Only the data image includes native PostgreSQL client tools and their libraries.

## References

- [Architecture and recovery safety](docs/design.md)
- [Repository format, publication, and retention](docs/repository.md)
- [Domain glossary](CONTEXT.md)
- [Security packaging](docs/security-packaging.md) and [release policy](docs/release-policy.md)

## License

All rights reserved for original project work. This is not an open-source license grant. Third-party components retain their own licenses. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
