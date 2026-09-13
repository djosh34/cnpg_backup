# Install, back up, recover, and update

Use the [v0.1.0 release guide](releases/v0.1.0/README.md) for downloads and the [configuration reference](configuration.md) for fields and limits. The supported matrix is Linux amd64, Kubernetes 1.35.8, CNPG 1.30.0, cert-manager 1.21.1, and PostgreSQL/tools 18.6. MinIO is tested. Other endpoints must meet the [storage requirements](repository.md#storage-requirements).

## Prepare storage and credentials

1. Install the pinned CNPG operator and cert-manager in the operator namespace.
2. Check `kubectl config current-context`. Use a non-production namespace and bucket for the first recovery drill.
3. Provision an unversioned, non-Object-Lock bucket with no lifecycle rules or unrelated mutators. Require TLS and atomic conditional writes.
4. Provision dedicated finite ext4 or xfs filesystems for workspace, PGDATA, WAL, and tablespaces. Verify actual capacity, locking, and fsync semantics. Do not use shared NFS, local-path directory binds into larger filesystems, or `emptyDir.sizeLimit` as a database quota.
5. Give destination credentials the [writer and retention permissions](repository.md#storage-requirements). Source recovery credentials need prefix reads/list and conditional access to the source gate, not payload deletion.

Use same-namespace Secret and ConfigMap selectors. Do not mount application credentials, the superuser password, or the server private key in the plugin.

## Install the plugin

Run from a checkout or distribution containing `config/`. Set `MANAGER_IMAGE` and `DATA_IMAGE` to the published immutable references in [release-subject.json](../.github/release-subjects/v0.1.0.json). Do not substitute rebuilt images. If GHCR requires authentication, configure consumer `imagePullSecrets` or mirror the portable OCI archives while preserving their manifest digests.

```sh
kubectl create namespace database
kubectl -n database create secret generic s3-auth \
	--from-file=access=/private/access-key --from-file=secret=/private/secret-key
kubectl -n database create configmap s3-ca --from-file=ca.crt=/private/s3-ca.pem
kubectl apply --server-side -f config/repository-crd.json
python3 config/render.py --manager-image "$MANAGER_IMAGE" --data-image "$DATA_IMAGE" \
	--managed-namespace database --secret-name s3-auth \
	--secret-name database-ca --secret-name database-replication \
	--secret-name recovered-ca --secret-name recovered-replication > install.json
kubectl apply --server-side -f install.json
kubectl -n cnpg-system wait --for=condition=Ready certificate/cnpg-backup-server \
	certificate/cnpg-backup-client --timeout=180s
kubectl -n cnpg-system rollout status deployment/cnpg-backup --timeout=180s
```

Read secret values from private files, not command arguments or shell history. Add every selected S3 and CNPG replication/public-CA Secret to the explicit allowlist before starting work. The default renderer places the manager beside the operator in `cnpg-system`.

Copy `config/repository-example.json` to `repository.json`. Replace its UUID with a new lineage UUID, then set the endpoint, bucket, prefix, CA selector, finite-workspace StorageClass, and capacity. Keep retention off for the initial drill. Remove freshness budgets for schedules you will not install.

Copy `config/cluster-example.json` to `cluster.json`. Set finite storage classes and capacities for PGDATA, separate WAL, and every tablespace. Keep the digest-pinned database image. Check the [phase reservations](configuration.md#native-work-and-capacity), not just the raw backup budget. Example sizes do not guarantee that a database fits.

```sh
kubectl apply --server-side -f repository.json
kubectl apply --server-side -f cluster.json
kubectl -n database wait --for=condition=Ready cluster/database --timeout=600s
kubectl -n database get repository destination -o yaml
```

Repository `Ready` validates configuration, not storage health or mounted capacity.

## Schedule backups and monitor them

Apply `config/full-backup-example.yaml` for an on-demand full and daily full schedule. To use weekly fulls, change CNPG's six-field schedule to `0 0 2 * * 0`. For daily differentials, copy the ScheduledBackup with a distinct name, schedule `0 0 3 * * *`, and `backupType: differential`. Keep `target: primary` explicit.

After a restart, promotion, or checksum-state change, request a new full before a differential. Missing summaries or an eligible full fails a differential. It never falls back to a full.

Inspect `Backup.status.phase`, failure details, and events. Compare invocation outcomes with durable committed freshness. A lost response can produce a failed invocation with a valid commit. Same-UID callback retries verify the existing winner. Do not edit terminal Backup status or repository bytes to manufacture success.

Install the ServiceMonitor, custom WAL queries, and Prometheus rules described in [monitoring](backup-monitoring.md). Adapt selectors and namespace labels. Configure scrape `up` alerts separately. Set full and differential freshness budgets only for installed schedules, with allowance for capture duration and scheduling delay.

## Enable retention after a recovery drill

Set `retention.enabled: true`, an explicit `window`, and `minimumFulls`, while keeping `dryRun: true`. Inspect Repository conditions and retention logs for coverage, admission, and capacity failures. Only after verifying the plan and recovery, set `dryRun: false`.

Deleting Kubernetes Backup objects does not delete repository backups. Do not add bucket lifecycle expiration or an independent WAL age policy. The planner preserves required full parents, usable window anchors, minimum roots, and WAL dependencies.

## Recover into a fresh Cluster

Keep the source Repository UUID, endpoint, bucket, prefix, trust, credentials, and reviewed Cluster template in consumer-controlled disaster recovery configuration. Source Kubernetes objects and the Backup catalog need not survive.

1. In the target namespace, create a `source` Repository pointing to the original lineage.
2. Create `recovered-destination` with a new repository UUID and destination storage settings.
3. Install the target credentials and allowlist its CNPG replication/public-CA Secrets before recovery starts. Do not restart a manager observing an active restore merely to expand its allowlist.
4. Render a new single-instance Cluster using fresh, unbound target PVCs.

```sh
python3 config/recovery.py --cluster-file cluster.json --namespace database \
	--name recovered --source-repository source \
	--destination-repository recovered-destination --target-json '{}' > recovery.json
kubectl apply --server-side -f recovery.json
kubectl -n database wait --for=condition=Ready cluster/recovered --timeout=900s
kubectl -n database get configmap recovered-cb-recovery -o json
kubectl -n database get jobs,pods,events
```

`{}` means the latest durably archived frontier, not unarchived transactions. For a time target, use `{"targetTime":"2026-09-01T12:00:00Z"}`. For a known named point, use `{"backupID":"<Backup-UID>","targetName":"before_drop"}`. See [target restrictions](configuration.md#recovery-targets).

Query the recovered database. Require pre-target effects to be present, post-target effects to be absent, and the expected role and timeline. Use a workload journal retained outside the source database. Ready, download, and combine success do not establish PITR success. Confirm that the recovered writer archives to its new lineage. Add standbys after recovery succeeds.

## Handle uncertain recovery or retention

Inspect the durable recovery operation, Repository conditions, Warning events, and original Job/Pod/container observations. Preserve evidence before changing resources.

Never remove target markers, force-clear holders or GC owners, expire them by time, or treat Pod deletion as fencing. Retry poisoned targets with a new Cluster and all-new target PVCs.

Crashed backup and reader holders block GC but permit other protected operations. An uncertain destructive owner blocks new admission. There is no supported automatic takeover or force-clear procedure. A manager can finish a durably recorded completed lifetime release, but cannot erase another process-reader holder. See [protection ownership](design.md#source-protection-and-restore-completion).

## Rotate credentials and trust

For S3 credentials, provision the new principal first. Keep the old principal valid through in-flight work, then update all associated Secret keys together. New operations load a complete snapshot. Running operations retain theirs.

For S3 TLS, publish an old-plus-new CA bundle before changing the endpoint certificate. Wait for projected snapshots, rotate the certificate, and retire old trust only after old operations and connections drain. Existing snapshots do not learn a new independent root from a later ConfigMap update. Use overlapping or cross-signed chains when old snapshots must still connect. Never disable certificate verification.

For operator-manager mTLS, project a second issuer's public certificate as `client-ca-next.crt` beside `client-ca.crt`. Only absence of the next file is optional; malformed trust rejects new work. Stage peer trust before rotating server and client Certificates. Confirm operator calls, replace the old trust source, then remove overlap. CNPG uses the server Secret's `tls.crt`, not `ca.crt`, as its server trust. Do not turn the cert-manager CA private-key Secret into an ad hoc bundle.

After each transition, verify an actual durable WAL callback and a new backup. Recover old bytes into a fresh target. Never log Secret values.

## Update the plugin

Before changing images, record the current digests, a committed full and differential, an archived WAL barrier, and expected SQL results. Wait for native work and restore observation to finish. Uncertain operations retain their protection.

Render and apply installation resources with the new qualified digests. Keep one manager replica with Recreate strategy. Observe CNPG's database Pod rollout and the actual sidecar image digests on every instance. Existing sidecars keep archiving during manager outages. Capture a new full after a postmaster restart before requesting more differentials.

Restore the recorded pre-update chain and remote WAL into a fresh target and compare SQL. Unknown repository formats fail closed. Do not rewrite repository objects. Roll back only to an explicitly compatible qualified reader. A restart using unchanged image bytes is not predecessor compatibility testing.

## Check resources and network isolation

Measure manager and data-process RSS separately from the sidecar cgroup, which includes native children. Default sidecar limits are 2 CPUs and 3 GiB. Check OOM events, actual filesystem capacity, free-space margins, and transfer size. During sustained backup transfer, observe successful WAL acknowledgment and backlog. Healthy `archive_timeout=60s` adds queue and upload delay. Outages have no fixed RPO.

Adapt `config/network-policy-example.json` before applying it. Replace documentation IPs with actual API and S3 addresses, then check operator, Prometheus, DNS, database, and application selectors. Standard NetworkPolicy has no DNS-name rules. Account for API Service DNAT and node-local DNS using the CNI's behavior.

Allow operator traffic to manager 9090, monitoring to 9091, and required API/DNS/S3 egress. Database Pods also need CNPG 8000, metrics 9187, client/replication 5432, and peer traffic. Apply a policy to each fresh recovery Cluster. Policies are additive, so an allow-all policy can defeat isolation. Verify allowed and denied paths on an enforcing CNI. Default kind networking is not isolation evidence.
