# GitHub tracker operations

Use `gh`. Verify the intended repository with `git remote -v` before writes. Keep credentials in the local credential store, not issue bodies or briefs.

## Issues and dependencies

Read the current issue, linked decisions, PRs, and CI before changing state. Claim work with `gh issue edit NUMBER --add-assignee @me` and record task and worktree ownership. Keep one short progress record with the next action and blockers. Avoid overwriting another session's updates from an old snapshot.

Use Markdown body files for issue and PR text. Record decisions in issue comments and update affected product docs. Link existing evidence instead of copying full logs or revision inventories into each transition.

Use native GitHub sub-issues and blocker relationships when a task needs dependencies. Fetch numeric database IDs, not issue numbers or GraphQL node IDs:

```sh
gh api repos/OWNER/REPO/issues/NUMBER --jq .id
gh api --method POST repos/OWNER/REPO/issues/PARENT_NUMBER/sub_issues \
  -F sub_issue_id=CHILD_DATABASE_ID
gh api --method POST repos/OWNER/REPO/issues/CHILD_NUMBER/dependencies/blocked_by \
  -F issue_id=BLOCKER_DATABASE_ID
```

Create issues before connecting them. Query paginated `sub_issues` and `dependencies/blocked_by` responses. Verify dependencies are acyclic and prerequisites are complete before starting dependent work. Diagrams are explanatory, not another authoritative dependency graph.

## PRs and CI

Use a separate branch and worktree for each active writer. Link the originating issue in the PR. Inspect `gh pr view`, `gh pr checks`, and `gh run view`, including the checked HEAD rather than only the green status. Follow the [review and merge procedure](review.md).

Use the [long-running observation policy](paseo.md#wait-for-events-not-short-polling) for hosted jobs. Collect failed logs and first-failure artifacts when completion is observed. Close implementation issues after merge, or record why they remain blocked.
