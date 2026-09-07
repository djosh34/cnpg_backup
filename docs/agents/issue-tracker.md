# GitHub tracker operations

Repository: infer from `git remote -v` and verify it is the intended repository before writes. Use `gh`; credentials stay in the local credential store, never issue bodies or agent briefs.

## Sources and ownership

- Wayfinder map: issue labeled `wayfinder:map`; Notes define planning scope and final readiness. Decision details live in resolution comments on its native child issues.
- Delivery epic: issue labeled `delivery:epic`; native children labeled `delivery:pr` are planned implementation slices, not opened PRs.
- Current progress: delivery-epic comments plus linked issues/PRs/CI. Read current state before editing; avoid overwriting another session's updated body from an old snapshot.
- Claim before work with `gh issue edit <number> --add-assignee @me` and record task/worktree ownership in a comment. An assignee is the claim; coordinate subagents under one orchestrator rather than having them claim the same GitHub identity independently.

## Wayfinding operations

Create/read/edit/comment/close with `gh issue` and Markdown body files. Render references to humans as descriptive issue titles linked to their GitHub URLs, not walls of bare numbers.

For parent/child and blockers, use numeric database IDs from `gh api repos/<owner>/<repo>/issues/<number> --jq .id`, not issue numbers or GraphQL node IDs:

```sh
# Attach child to map/epic.
gh api --method POST repos/<owner>/<repo>/issues/<parent-number>/sub_issues \
  -F sub_issue_id=<child-database-id>
# Child cannot proceed until blocker closes.
gh api --method POST repos/<owner>/<repo>/issues/<child-number>/dependencies/blocked_by \
  -F issue_id=<blocker-database-id>
```

Create issues first, wire dependencies second. Native relationships are canonical; Mermaid comments are readable snapshots only. Query paginated `sub_issues` and `dependencies/blocked_by`; the frontier is open, unassigned children whose blockers are all closed. When changing ordering, verify the resulting graph is acyclic and implementation cannot outrun required decisions. Planned experimental evidence can be a PR acceptance gate without a circular dependency on that very PR's implementation.

Resolve a decision with an evidence-backed comment, close it, and append one named summary link to that issue/comment in the map. This means a brief summary, not publication to GitHub Gist. All technical tickets are resolved during planning, before the READY handoff in `docs/EXECUTE.md`. Factual research can run AFK; product trade-offs are settled with the owner in the final design grill. Implementation must not begin by completing unresolved planning. Record evidence-driven internal adaptations explicitly while preserving the finalized external contract; normal PR engineering/reviews/merges need no further owner decision.

## Long-running observations

Prefer event-driven completion notifications. Where GitHub exposes polling, use 1800-second refreshes: `gh run watch RUN_ID --interval 1800 --exit-status` or `gh pr checks PR --watch --interval 1800`. Unlike Paseo's event wait, these polling commands may notice completion only at the next refresh; do not add a 30-minute sleep before the initial observation. Inspect failed logs and collect first-failure artifacts immediately when completion is observed. Short local tests and scenario readiness checks keep short deadlines; this policy governs agent/hosted-job observation, not runtime/test timing.

## PRs and reviews

Use one branch/worktree per active writer, base on merged prerequisites, and link the originating delivery issue in each PR. `gh pr view --json ...`, `gh pr checks` and `gh run view` supply state/evidence; check HEAD SHA as well as a green status.

Independent agent reports name the reviewed subject once and list findings; later progress/disposition comments link that record rather than repeat every SHA. Same-account comments do not satisfy platform-required human approval. Follow `docs/agents/review.md` and approved authority before merging; never bypass branch protection. Close the delivery issue only after merge (or record an explicit blocked/waiting state), then advance the frontier.
