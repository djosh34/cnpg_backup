# Paseo subagent lifecycle

Use Paseo for every subagent, not raw Pi subprocesses. Unarchived children retain memory even after completion and can cause Paseo to run out of memory.

## Ownership and capacity

- Only the coordinator creates children. Workers and reviewers must not delegate recursively.
- Allow at most five unarchived children across the entire effort, including idle, failed, canceled, and waiting children. Enforce this by instruction and a task record, not a new limiter service.
- Record each child's ownership label, ID, role, task, worktree, result, and archival state. Reconcile owned sessions before dispatch or resume. Coordinators sharing an effort share the same limit.
- Use one writer per worktree. A successor coordinator takes exclusive dispatch ownership and counts toward the limit if it is itself a child.
- Archive only owned children. Never archive the parent, unrelated agents, or sessions whose ownership is uncertain.

## Workspace and model selection

Use only GPT-based models through `openai-codex`, with Paseo's `pi` provider.

| Role | Model | Thinking |
| --- | --- | --- |
| Coordinator, manager, or research coordinator | `openai-codex/gpt-6-astra` | `medium` |
| Implementer, researcher, reviewer, or adjudicator | `openai-codex/gpt-6-astra` | `high` |
| Separately authorized, non-delegating reference-cleanup worker | `openai-codex/gpt-5.6-luna` | `xhigh` |

Inspect the effective model, thinking level, workspace, and Cwd before work begins. Do not silently substitute settings. The agent-scoped CLI can ignore `--cwd` and inherit the caller's workspace. Register and select an explicit workspace for an existing worktree.

Keep `PASEO_PASSWORD` in the coordinator's environment. Do not print it, pass it in arguments or child `--env`, or place it in briefs or repository files. Use existing credentials without resetting the user's daemon or password.

Check installed CLI help when the interface changes. For a worker in an existing worktree:

```sh
paseo workspace create --isolation local --path /absolute/task/worktree --json
# Use the returned workspaceId.
paseo run --background --json \
  --provider pi --model openai-codex/gpt-6-astra --thinking high \
  --workspace WORKSPACE_ID \
  --title 'CNPG: task name' \
  --label cnpg_effort=EFFORT_ID --label role=worker \
  'Read /absolute/task/brief.md. Complete only that task. Do not spawn agents.'
paseo inspect AGENT_ID --json
```

## Wait for events, not short polling

Use `paseo wait AGENT_ID --timeout 1800 --json`. This is an event-driven wait, not a sleep. Completion, errors, and permission requests can return early. Set the calling tool's deadline above the wait plus transport overhead.

Inspect the returned status. `idle` does not prove success. `timeout` means observation expired, not that the child stopped. Inspect active work and repeat the wait when appropriate. Use `run --background` and `send AGENT_ID --no-wait` to avoid short default foreground waits.

For hosted CI, use `gh run watch RUN_ID --interval 1800 --exit-status` or `gh pr checks PR --watch --interval 1800`. These commands poll rather than wait for events. Local assertions, subprocess deadlines, and product readiness checks retain their own short bounds.

## Collect, archive, and verify

1. Inspect the report and actual task evidence. Save the result, commit, tests, and relevant diagnostics outside the child session. Redact credentials from published evidence.
2. Immediately archive every finished, failed, canceled, or abandoned child with `paseo archive AGENT_ID --json`. To abandon a still-running owned child, use `--force`. Stopping alone is not archival.
3. Run `paseo inspect AGENT_ID --json` and confirm archival before reusing the slot. If archival fails, repair cleanup before spawning more children.

A caller timeout or interruption does not remove this obligation. On resume, clean up owned completed or orphaned sessions before dispatch. Before a dormant handoff, collect and archive remaining owned work and leave a durable checkpoint.
