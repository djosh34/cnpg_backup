# Paseo subagent lifecycle

Read before **creating, waiting for, resuming or cleaning up** a child. Use Paseo for every role; no raw Pi process fallback. This lifecycle is operationally essential: unarchived completed children can retain memory until Paseo is OOM-killed.

## Ownership and capacity — agent instructions, not new code

- The orchestrator alone creates children. Researchers, implementers, reviewers and adjudicators may not recursively spawn others.
- **Maximum five concurrent children total for this effort**, not five per role/PR. Count every created, not-yet-confirmed-archived child, including idle, failed and waiting children. The parent is not a child. Use fewer when concurrency provides no benefit.
- Before spawning, inspect owned Paseo agents and reconcile the small task ledger in the progress comment/local task record. Other orchestrators participating in this effort share the same five slots; do not create a second independently managed pool.
- Tag every child with a stable effort/run ownership label and role/task identity. Record returned agent ID, worktree, task, outcome and archival state. Resume from these records rather than guess from agent titles.
- Do not build a semaphore daemon, admission controller, extension or limiter to enforce this. The AI maintains the limit as an instruction. Never archive the parent, unrelated user agents or an agent whose ownership is uncertain.

## Verified interface and model selection

Inspect installed help if versions change. This environment provides `paseo run`, `wait`, `logs`, `inspect` and `archive`. `paseo wait --timeout` uses seconds; `run --wait-timeout` accepts a duration. Background run returns an agent ID; `wait` reaching idle does not itself prove task success. `send` waits by default: use `paseo send ID --no-wait ...` for a message to running work, then observe the original ID normally.

Planning preflight successfully dispatched two independent review agents, retrieved their reports, and verified `Archived: true` for both after cleanup. This verifies the local Paseo mechanism, not design readiness or product correctness.

Every child: `--provider pi --model openai-codex/gpt-6-astra --thinking high`. Confirm the effective selection in the new agent's inspection/result. An empty model-list response alone is not proof the provider cannot select the model; verify an explicit lightweight dispatch before the planning readiness gate closes. Do not silently change the model/reasoning tier.

Paseo may require a daemon password via `PASEO_PASSWORD`. Resolve existing local credentials without printing them or putting them in command arguments, repo files, prompts or GitHub. Keep daemon control credentials in the orchestrator's environment; do not explicitly forward them through child `--env`. Do not reset/restart the user's daemon or change its password as an authentication workaround.

For an isolated existing worktree, first register/select its **explicit workspace**. The installed agent-scoped CLI can ignore `--cwd` and inherit the caller's workspace; finalization reproduced this and verified explicit `--workspace` selects the intended Cwd. Inspect the returned workspace/agent Cwd before writing. Keep absolute-path/worktree ownership in the brief.

Illustrative lifecycle, with an already-authenticated CLI and a reviewed brief (fill placeholders; this is not a task runner to implement):

```sh
paseo workspace create --isolation local --path /absolute/task/worktree --json
# Use the returned workspaceId below; no guessed ID.
paseo run --background --json \
  --provider pi --model openai-codex/gpt-6-astra --thinking high \
  --workspace WORKSPACE_ID \
  --title 'CNPG: task name' \
  --label cnpg_effort=stable-effort-id --label role=review \
  'Read the task brief at /absolute/private/brief.md. Complete only that task. Do not spawn agents.'
paseo wait AGENT_ID --timeout 300 --json
paseo logs AGENT_ID --tail 100
paseo inspect AGENT_ID --json
# Capture report/results before archival; then always perform cleanup.
paseo archive AGENT_ID --json
paseo inspect AGENT_ID --json
```

Each independent review is a newly created Paseo agent with a clean context, not a fork/resume of the author. Run reviewers on a pinned snapshot/worktree; explicitly prohibit edits and delegation in the brief. CLI/prompt restrictions are not a security sandbox. Workers write only their assigned worktree. Avoid access to production credentials; keep test environments disposable.

## Collect → archive → verify, on every outcome

1. Wait/observe without spawning replacements beyond the limit. On completion, inspect the final report and actual task evidence; idle status can mean success, error or interruption.
2. Save a concise result and relevant diagnostics/commit/test references outside the child session. Publish only redacted evidence, not raw credential-bearing logs/transcripts.
3. **Immediately archive the child**, whether successful, failed, canceled, timed out or superseded. For a still-running owned child that must be abandoned, `paseo archive AGENT_ID --force --json` interrupts and archives it. Stop alone is insufficient.
4. Confirm archival in `inspect` (or the installed CLI's archived listing) and update the task ledger. Do not free/reuse its slot until verification succeeds.
5. If archival fails, repair cleanup before new spawns. On orchestrator interruption/resume, inspect and archive owned completed/orphaned agents first. Uncertain ownership is investigated, not solved by archiving everything.

A timeout in the caller does not prove the child stopped. Preserve its ID and explicitly clean it up. Cancellation, exceptions and partial task results must all follow the same cleanup path. Do not keep a completed author alive while reviews run; save its branch/report, archive it, and create a new scoped fix worker if needed.

At the end of planning, a PR cycle or the whole effort, verify **zero owned unarchived completed/abandoned children**. Active children remain only when intentionally working and tracked within the limit. Before handing off a dormant thread, leave no forgotten child sessions consuming memory.
