# Run exact-image recovery campaigns

The campaign runs real CNPG, PostgreSQL, and MinIO against existing manager and data image digests. It builds test tools, not product images. Use [testing.md](testing.md) for safety obligations and [build prerequisites](build-and-harness.md) for the host requirements.

## Prepare immutable inputs

Obtain a subject record from a trusted publication. It identifies the product revision and canonical manager/data manifest digests. For v0.1.0, use [.github/release-subjects/v0.1.0.json](../.github/release-subjects/v0.1.0.json). A source revision alone does not identify rebuilt image bytes.

Create the test bundle and freeze a plan:

```sh
./hack/test campaign-bundle --out BUNDLE
./hack/test campaign-plan --subject .github/release-subjects/v0.1.0.json \
	--bundle BUNDLE --profile recovery --seed 1806 --mode fresh --out plan.json
./hack/test campaign-preflight --plan plan.json --out preflight.json
./hack/test recovery-campaign --plan plan.json --bundle BUNDLE \
	--run-dir NEW_RUN --duration-minutes 120
```

Use a new bundle/run path. `campaign-bundle --reuse OLD_BUNDLE` can reuse verified fixture-tool bytes when their build inputs are unchanged. The bundle contains the actor, WAL proxy/client, verifier, and pinned MinIO fixture, plus hashes and a harness identity distinct from the product. The runner checks product binary revision and imported image identity before testing.

For local/hosted comparison, use the same exported bundle and plans rather than rebuilding similar tools. If artifact transport removed executable bits, restore mode `555` only on `actor`, `minio`, `wal-proxy`, `wal-client`, and `verify`. Use the recorded Python version. Fresh database bytes, UIDs, and timestamps vary; compare the recipe and independent invariants, not byte identity of newly generated databases.

## Select a focused scope

The registry in [hack/campaign_plan.py](../hack/campaign_plan.py) defines cases, branches, fixtures, and prerequisites. List the available identifiers without provisioning:

```sh
PYTHONPATH=hack python3 -c 'from campaign_plan import REGISTRY; print("\n".join(c["id"] for c in REGISTRY))'
```

Use `--case` for a case and its dependency closure:

```sh
./hack/test campaign-plan --subject .github/release-subjects/v0.1.0.json \
	--bundle BUNDLE --case detached-PG-descendants --out focused.json
./hack/test recovery-campaign --plan focused.json --bundle BUNDLE --run-dir NEW_FOCUSED
```

`smoke`, `retry`, and `ownership` select smaller profiles. `recovery` selects the full registry scope. `--layout grouped` separates fixture groups rather than the default monolithic ordering. Partial or focused scope does not qualify a release. Release qualification also needs the security, fuzz, compatibility, review, and resource gates in [release-policy.md](release-policy.md).

A dirty harness requires explicit diagnostic labeling:

```sh
./hack/test campaign-bundle --out DIAGNOSTIC_BUNDLE --diagnostic
./hack/test campaign-plan --subject .github/release-subjects/v0.1.0.json \
	--bundle DIAGNOSTIC_BUNDLE --mode diagnostic --profile retry --out diagnostic.json
```

Diagnostic mode still creates fresh data. It does not authorize a retained fixture as fresh acceptance. Diagnostic branch replay and duration overrides remain diagnostic.

## Run in GitHub Actions

The manual and reusable `.github/workflows/recovery-campaign.yml` workflow consumes existing trusted image digests and invokes the same local recipe. From a trusted checkout, set the actual published subject and digests:

```sh
gh workflow run recovery-campaign.yml --ref main \
	-f subject_sha="$SUBJECT_SHA" -f trusted_ref=main \
	-f manager_image="$MANAGER_IMAGE" -f data_image="$DATA_IMAGE" \
	-f seeds='[1806]' -f profile=recovery
```

The optional `prior_release` input must match a version tag and its trusted main release-subject record. It does not trigger a fallback build. Tests use read-only repository/package permissions and no production secrets. The hosted run has a 150-minute hard timeout.

## Interpret failures and evidence

Inspect the run's evidence manifest, events, `failures.json`, and `first-failure.json`. Each required branch has its own outcome. A failed prerequisite blocks dependents rather than counting them as exercised. Independent work proceeds only after verified fixture disposal and fresh setup. Failed branches are not retried to green within the run.

Distinguish a failed product invariant from a failed fixture precondition. An assertion classification alone is not causal proof. Preserve the original failure and add the diagnosis separately. Mandatory SQL, WAL/hash/history, holder identity, target ownership, descendant reap, and teardown observations must remain conclusive. Readiness, Pod deletion, or elapsed time is not a substitute.

Optional log/event/status collection uses bounded reads and records diagnostic warnings. Such warnings do not fail an otherwise valid oracle. Missing required evidence, failed teardown, unknown resource ownership, and leaked fixtures do fail the run or block reuse. A diagnostic collector must not be used for an oracle-required read.

Each run has a distinct execution identity. Compare or aggregate only compatible records:

```sh
./hack/test campaign-compare --left LOCAL/evidence/manifest.json --right HOSTED/manifest.json
./hack/test campaign-aggregate --plan plan.json RESULT1/manifest.json RESULT2/manifest.json
```

Aggregation rejects failed, missing, partial, retained, mismatched, duplicate, or leaked attempts. A successful focused replay does not automatically become a full-run pass. The [v0.1.0 qualification record](releases/v0.1.0/qualification.md) explicitly records its reviewed exception without changing raw outcomes.

## Preserve ownership during cleanup

Local fixtures are serial under a cross-worktree host lock. Every run creates an owned kind node, explicit kubeconfig, fresh namespaces and PVCs, and a loopback MinIO forward. The runner accepts no external endpoint or ambient kubeconfig. Cached immutable images and tools are allowed; retained source data is not.

Preflight checks Docker storage, free bytes/inodes, CPU, memory, and cgroup/pressure observations. The plan records its node limits and host headroom. Fixture filesystems have real finite backing, not merely sparse logical sizes. Measure allocated blocks and peak usage. Fixture measurements are not product capacity promises.

Retire targets only after evidence and original-operation closure. Stop their workloads, delete the Cluster, verify no remaining consumers, then retire the exact PVC/PV/backing identities. Never clear source holders or rebind a poisoned target. Whole-fixture disposal stops owned sandboxes and verifies device associations before unmounting and detaching backing. Unknown ownership or unmount failure is a leak, not successful cleanup.

For a fixture explicitly retained with `--retain-on-failure`, collect and clean using its exact ownership record:

```sh
./hack/test campaign-collect --owner RUN/fixture-001/owner.json
./hack/test campaign-clean --owner RUN/fixture-001/owner.json
```

Do not use namespace deletion, finalizer removal, shared-daemon pruning, or marker deletion as a shortcut. Preserve the first failure before reset. The campaign reserves five minutes each for collection and teardown within its 120-minute budget. Evidence uploads are capped at 100 MiB with explicit truncation records and normally expire after 14 days. Credentials, kubeconfigs, private keys, and raw backup archives stay out of uploads. Runner death can prevent final collection and is never success.
