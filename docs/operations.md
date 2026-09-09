# Install, protect, recover and update

**Development delivery, not a qualified release or production rollout.** Use the
exact manager/data digests from the candidate/release evidence, never guessed
SHAs, tags or rebuilt substitutes. [Support/pins](design.md#exact-initial-matrix):
Linux amd64, Kubernetes 1.35.8, CNPG 1.30.0, cert-manager 1.21.1, PG/tools 18.6.
MinIO is tested; another endpoint must satisfy the [storage contract](research/storage-protocol-final.md).
The first release has no predecessor: rolling a candidate is not N→N+1 proof.

## Before install

- Install the pinned CNPG operator and cert-manager using their upstream manifests.
  Use a non-production namespace/bucket for a drill. Do not use ambient kubeconfig
  contexts without checking `kubectl config current-context`.
- Use unversioned, non-Object-Lock S3 storage with **no lifecycle expiration or
  unrelated mutators**. Require TLS, atomic conditional writes, strong GET/LIST
  and bucket-default encryption. No redirect/proxy/credential-chain fallback.
- Provision a **hard finite disk-backed filesystem/quota per workspace PVC**;
  ordinary emptyDir limits or a large shared local-path filesystem are insufficient.
  Verify actual mounted capacity, locking and fsync semantics on every PGDATA,
  WAL and tablespace volume. Do not use shared NFS or reuse another writer's PVCs.
- Destination credentials need prefix reads/list, conditional writes, multipart
  upload/list/abort and deletion for configured retention. Source restore credentials
  need prefix reads/list and exact `gate.json` GET/conditional PUT, not payload delete.
  Keep credentials and CA in same-namespace Secret/ConfigMap selectors. Never mount
  application credentials, the superuser password or private server key in the plugin.

## Clean install (consumer paths)

Run from a checkout/distribution containing `config/`. Set `MANAGER_IMAGE` and
`DATA_IMAGE` to the **published immutable digest references**. If GHCR anonymous
pull is unavailable, use documented `read:packages` pull authentication or mirror
release OCI archives without rebuilding. Configure `imagePullSecrets` on the
manager and CNPG Cluster as needed; these are consumer registry credentials.

```sh
kubectl create namespace database
# Read secret values from private files, not argv/history; do not commit them.
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

Review/edit `config/repository-example.json` into `repository.json`: replace the
example UUID with a new lineage UUID, TLS endpoint/bucket/prefix, CA selector and
finite-workspace StorageClass. UUID and storage identity are immutable; a restored
writer **always** uses a different destination UUID. The example retention is off,
dry-run on; remove freshness budgets for schedules you will not install.
Review `config/cluster-example.json` into `cluster.json`: select actual finite
storage classes/capacities and digest-qualified database image. These sizes are
examples, not a database-size/RTO guarantee. Keep native backup budgets below
measured phase space; defaults cap raw backup/restored bytes at 32GiB.

```sh
kubectl apply --server-side -f repository.json
kubectl apply --server-side -f cluster.json
kubectl -n database wait --for=condition=Ready cluster/database --timeout=600s
kubectl -n database get repository destination -o yaml
```

`Ready` on Repository validates configuration, **not** storage health/capacity.
The renderer restricts manager Secret access to explicit get-only resourceNames,
uses nonroot UID26, read-only root, dropped capabilities, seccomp, required mTLS
on9090 and separate internal HTTP metrics9091. CNPG injects the same hardened
sidecars; native runtime tools are the only [dependency exception](release-policy.md).
Database workloads and the operator need their own security review.

## Backup, monitoring and retention

Apply `config/full-backup-example.yaml` in `database` for on-demand/daily fulls.
For weekly fulls adjust the CNPG six-field schedule to `0 0 2 * * 0`. To schedule
a daily differential, copy the ScheduledBackup with a distinct name, schedule
`0 0 3 * * *`, and **`backupType: differential`**. A differential requires a
retained full from the same uninterrupted timeline/checksum/postmaster state.
After restart/promotion request a new explicit full. Missing summaries/parent
fails the differential; there is no full fallback.

Check `Backup.status.phase`, events and durable committed freshness. CNPG failed
invocation can coexist with a durable commit after response loss: retry the same
Backup UID to verify/return the winning bytes, never manufacture success.

Install `config/backup-monitoring.yaml`, `config/wal-monitoring.yaml`, and the
`backup-alerts.yaml`, `wal-alerts.yaml`, `operational-alerts.yaml` rules where your
Prometheus Operator selects them. Adapt namespace/selector labels. Configure
scrape `up` alerts separately: a dead target cannot emit Unknown. Keep `honorLabels`.
[Backup monitoring](backup-monitoring.md) distinguishes scheduled full/differential
stale, never, failed and unknown; budgets are explicit, not inferred from cron.
The example192h/26h budgets suit weekly full/daily differential, not arbitrary schedules.

Manager operational gauges:

- `cnpg_backup_restore_{observation_known,active,uncertain,lifetime_release_pending}`:
  only namespace/cluster labels; observation older than5m is Unknown and state
  gauges are omitted. A missing operation is not completed/healthy. Completed
  lifetime release does not prove another process-reader hold is gone.
- `cnpg_backup_retention_blocked`, `cnpg_backup_repository_admission_blocked`,
  `cnpg_backup_repository_holders`: bounded repository/namespace/cluster labels.
  Unobserved admission/holder gauges are omitted, not healthy zero.
- `cnpg_backup_retention_workspace_available`: **last reservation outcome**, omitted
  when unobserved; not live disk free space. `RetentionWorkspaceAvailable` is the
  equivalent Repository condition. `cnpg_backup_retention_checked_timestamp_seconds`
  dates periodic observations (configured interval can be24h); caches expire48h.
- Native/workspace/WAL live free space uses kubelet PVC volume metrics and the
  existing filesystem-pressure rule. Verify the CSI driver actually exports them;
  absent kubelet metrics are **unobserved**, not proof of free space.

After proving recovery, explicitly configure retention window/minimumFulls and
enable dry-run; inspect Repository conditions/plans before setting dryRun false.
Deleting Kubernetes Backup objects is not repository retention. No aggressive
independent WAL expiry. A last usable full/required parent/WAL closure is retained.

## Fresh-cluster restore drill / disaster recovery

Keep `cluster.json`, source Repository identity/endpoint/prefix/trust and credentials
in consumer-controlled disaster recovery configuration. Source Kubernetes objects
and the Backup catalog need not survive. In the target namespace create `source`
Repository pointing to the original UUID and create `recovered-destination` with
**a new UUID**. Install/allowlist target S3 and `recovered-ca`/`recovered-replication`
Secrets using the install renderer **before** recovery starts. Never restart an
active manager just to expand its allowlist. No source-side controller is required.

```sh
# Latest means durably archived frontier, not unarchived source transactions.
python3 config/recovery.py --cluster-file cluster.json --namespace database \
  --name recovered --source-repository source \
  --destination-repository recovered-destination --target-json '{}' > recovery.json
kubectl apply --server-side -f recovery.json
kubectl -n database wait --for=condition=Ready cluster/recovered --timeout=900s
kubectl -n database get configmap recovered-cb-recovery -o json
kubectl -n database get jobs,pods,events
```

For a time target use `{"targetTime":"2026-09-01T12:00:00Z"}`; for a known named
point use `{"backupID":"<Backup-UID>","targetName":"before_drop"}`. Explicit targets
must follow the selected backup's native boundary. Require SQL assertions showing
pre-target effects present and post-target effects absent, target reached, correct
role/timeline; readiness or download/combine success alone is insufficient. Retain
the workload journal outside source PG. Confirm the recovered writer archives to
its new UUID, not the source. Add standbys only after this recovery succeeds.

On uncertain/crashed restore: inspect durable operation and Repository gate status,
Warning events and original Pod/Job/container evidence. **Never remove markers,
force-clear holders/GC owners, expire by time, or interpret Pod deletion as fencing.**
Holders pause GC but allow other admitted restores; an uncertain destructive owner
blocks new admission. Retry poisoned targets with a **new Cluster and all new PVCs**.
A manager may finish durably recorded completed lifetime release, not adopt an old
active observer or erase another process-reader. Lost destructive admission is an
explicit fail-closed availability boundary, not an automatic repair promise.

## Rotation and rolling plugin update

S3 rotation requires **overlap**: provision the new valid principal first, keep the
old valid through all in-flight work, then atomically update all keys in the Secret.
For TLS trust publish old+new CA bundle, wait for projected complete snapshots,
rotate the endpoint certificate, and only retire old trust after old operations
and connections drain. Prefer server cross-signing/overlapping chains when old
snapshots must still open connections. A bundle update alone cannot teach an
already running snapshot a new independent root. Do not disable certificate checks.
New operations load a complete validated Secret/CA snapshot; in-flight operations
retain theirs. Invalid new credentials/trust fail closed, not stale forever.
Verify a durable WAL callback and a new backup after each transition; preserve
acknowledgments/identical retries and old-byte SQL recovery. Never log Secret values.
Operator-manager mTLS is a distinct rotation: stage peer trust before leaf renewal,
then retire old trust after established calls drain. See [lifecycle TLS behavior](lifecycle-implementation.md).

Before plugin update, record image digests, a committed full/differential, archived
WAL barrier and expected recovery SQL. Wait for all restore observers and native
work to finish; uncertain operations retain protection. Render install.json using
new qualified digests and apply it; manager remains **one replica/Recreate**.
CNPG evaluates sidecar desired images and rolls database Pods with availability
checks; observe rollout and actual init-container image digests on every instance.
Existing sidecars keep archiving while manager is unavailable. Capture new full
explicitly after any source postmaster restart before requesting differentials.
Recover the recorded **pre-update** chain/WAL on a fresh target and compare SQL.
Do not call an annotation-only restart predecessor compatibility. Unknown format
fails closed; no automatic repository rewrite. Rollback only to an explicitly
compatible qualified reader; never repair by editing repository bytes.

## Resource and network boundaries

Manager defaults50m/64Mi request,500m/256Mi limit; sidecar100m/256Mi request,
2CPU/3GiB limit including native children, one native operation. Measured Go RSS
must be below128MiB idle and256MiB transfer, separately from full sidecar/native
cgroup peak≤3GiB/configured limit and OOM events. Workspace includes raw archives,
compressed spool, extraction/combine output and duplicated WAL plus margin; source
PGDATA is not staging. Allocation benchmarks or node RAM are not process RSS.
During a sustained transfer larger than buffers, measure a real WAL callback's
latency/backlog and successful durable acknowledgment. Healthy archive_timeout60s
adds queue/upload delay; faults have **no fixed RPO**. Report actual bytes/timings,
not an unlimited throughput/RTO promise.

`config/network-policy-example.json` is an **adapt-before-apply example**, not a
claim of CNI enforcement. Replace documentation IPs192.0.2.10/20 with actual API/S3
addresses and ports; verify operator/Prometheus/DNS labels, Cluster/namespace and
application selectors. Standard NetworkPolicy has no DNS-name rules; account for
API Service DNAT/node-local DNS and endpoint changes using your CNI's semantics.
Manager allows operator9090/metrics9091 plus API/DNS/S3 egress; database Pods allow
CNPG8000, metrics9187, clients/replication5432 and API/DNS/S3/peer egress. Sidecar
Unix sockets remain local. Apply a policy for **each fresh recovery Cluster** too.
Policies are additive: another allow-all policy defeats isolation. Kubelet/node
traffic exceptions are CNI-dependent. Verify allowed paths and denied unrelated
Pods on an enforcing CNI before claiming isolation; default kind networking alone
is not such evidence. No extra platform purchase/access is a delivery gate.
