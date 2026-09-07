# Autonomous delivery platform preflight

Planning evidence, not product qualification. Checked 2026-09-07 against `djosh34/cnpg_backup`.

## Observed access

- Local orchestrator environment: `openai-codex/gpt-6-astra`, reasoning `high`.
- GitHub repository is public; authenticated account has admin/maintain/push access. `git push origin main` successfully published the four prior owner planning commits through `b543a7d270745d6af66957cc29f81d07366c2389`.
- Rulesets API returned `[]`; main protection API returned `404 Branch not protected`. No protection was changed or bypassed. Independent context reviews remain mandatory even though platform human approvals are not required.
- Actions is enabled, all actions allowed, default token permission read-only. Explicit per-job `contents: write` and `packages: write` worked. No protected deployment environments exist; no repository Actions secrets were listed. Checkmarx credentials/license are not available in this repository: integration is conditional on availability, not a gate requiring an owner action. Mandatory Go/image security scans are unaffected.
- Hosted Ubuntu 24.04 runner had Docker 28.0.4, GH CLI 2.98.0, approximately 15 GiB RAM and 87 GiB free disk. Resource evidence is a provisioning observation, not a product resource benchmark.

## Executed publication experiment

Disposable branch `planning/platform-preflight`, commit [`2ee5c377ffa1a210b56ded05c0e44ec227078d9a`](https://github.com/djosh34/cnpg_backup/commit/2ee5c377ffa1a210b56ded05c0e44ec227078d9a).

[Successful Actions run 34069348074](https://github.com/djosh34/cnpg_backup/actions/runs/34069348074) executed:

1. Created a **draft**, explicitly non-product release and uploaded a 66-byte synthetic probe asset. Queried the draft and its uploaded asset successfully.
2. Built and pushed a `FROM scratch` image containing only a non-product NOTICE to `ghcr.io/djosh34/cnpg-backup-planning-probe`, using the ephemeral repository Actions token via stdin. Returned digest `sha256:bbf60eb466b92a69dcc13ee99a2350559174f224efc1d5ae3a0799dec40ef51a`.
3. Logged out of GHCR and deleted the draft release. No versioned product release, qualified-product image, database operation or production deployment occurred.

This proves the existing Actions credential can create releases/upload assets and publish repository-linked GHCR packages. GitHub's [release create/update APIs](https://docs.github.com/en/rest/releases/releases) use the same contents-write authority to publish the eventual release. The probe is not release qualification; the required exact-artifact recovery campaign still precedes product publication.

An additional [successful OIDC preflight run 34069704258](https://github.com/djosh34/cnpg_backup/actions/runs/34069704258), commit `0ad698c`, created [GitHub/Sigstore build provenance](https://github.com/djosh34/cnpg_backup/attestations/45626376) for synthetic text with ephemeral `id-token: write`/`attestations: write` authority. Anonymous registry retrieval of the first probe digest returned HTTP 200 with the matching Docker-Content-Digest. This proves public retrieval for the probe, not an assumption about all future packages.

Cleanup evidence is intentionally not hidden: earlier runs [34069605667](https://github.com/djosh34/cnpg_backup/actions/runs/34069605667) and [34069649303](https://github.com/djosh34/cnpg_backup/actions/runs/34069649303) passed publication and provenance but failed optional probe-version deletion. GitHub returned HTTP 400 saying the public version exceeded its deletion download threshold. Other deletable probe versions were removed; synthetic version `1216776669` remains explicitly non-product. The final probe records this refusal as a warning rather than implying cleanup succeeded. No product workflow depends on deleting registry versions, and no protection/access setting was weakened. All draft releases were removed.

The local `gh` credential lacks `read:packages` (package-version enumeration returned 403). Do not print/replace credentials or design local package access into the pipeline. Registry operations belong in Actions using its verified token. GHCR package visibility is not implicitly public merely because source is public; consumer access must be documented honestly. Portable image archives in public GitHub releases provide distribution independent of package visibility. See [GitHub container registry authentication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

## Paseo lifecycle verification

Local daemon authentication used the existing private credential without printing or changing it. Existing unrelated sessions were preserved. Three new research sessions were inspected: each reported `Model: openai-codex/gpt-6-astra`, `Thinking: high`. Their IDs and eventual collection/verified archival are recorded in the handoff issue's progress comments. Earlier planning already verified archival; current finalization must also verify each owned child's archival before READY.

Installed Paseo inherits the caller's workspace despite `--cwd` for agent-scoped invocations. Workers were immediately instructed to use explicit absolute paths and `cd` to their isolated worktrees. Subsequent dispatch must select an explicit workspace or verify the resulting Cwd and enforce assigned worktree ownership; command-line intent alone is not evidence.
