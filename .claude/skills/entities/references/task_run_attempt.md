# `task_run_attempt` (disk)

One execution of a [`task_run`](task_run.md)'s command — **the only level that runs a process.** Attempts count from 1; a task run gets up to `1 + max_retries`.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `task_run_id` | Foreign key to [`task_run`](task_run.md). |
| `job_run_id` | Foreign key to [`job_run`](job_run.md), carried so the stop check and log views need no join through `task_run`. |
| `job_id`, `task_id` | Denormalized for the same reason. |
| `created_at` | Bound from `Toolkit`. **`retry_delay` is measured from this** — when `TaskRunMonitor` decided to retry. |
| `started_at` | Nullable. Set at spawn, so `NULL` on a `Skipped` attempt says the command never ran. |
| `finished_at` | Nullable. Written with every terminal status. |
| `attempt` | 1 for the first, `last.attempt + 1` for each retry. |
| `status` | `TaskRunAttemptStatus` — same variants as `TaskRunStatus` but a separate enum; see the [orchestrator skill](../../orchestrator/references/task_run_attempt.md). |
| `output` | `TEXT NOT NULL`, empty for nothing. What the command wrote to `$FLOWLITE_TASK_OUTPUT`, recorded on success. Over `[orchestrator] max_task_output_bytes` fails the attempt, so it is never truncated. |
| `process_group_id` | Nullable. The spawned `sh`'s pid, which is its process group id (it is made group leader), so a stop or timeout signals the whole tree. `NULL` until spawn. |
| `waiting_since` | Nullable. When the attempt's process began waiting on another run in flowlite's own wait; a `Running` attempt with it set holds no concurrency slot. Not cleared on settle — every tally filters on `Running` first. |

`UNIQUE (task_run_id, attempt)`: the number is computed, so this index turns a racing second process into a failed insert rather than a double execution. It also defines "the last attempt" (`TaskRunMonitor::select_last_task_run_attempt` sorts by `attempt`).

## Written by

- **Inserted** by `TaskRunMonitor` alone ([src/orchestrator/task_run_monitor.rs](../../../../src/orchestrator/task_run_monitor.rs)), `Queued`: attempt 1 by `get_or_start_task_run_attempt` for a `Running` task run with none, retries by `insert_next_attempt` — one file owning the unique index.
- **Updated** by `TaskRunAttemptDispatcher` (`Queued` → `Running`/`Skipped`) and `TaskRunAttemptMonitor` (`Running` → terminal). Both can write `Invalid`, reachable from `Queued` and `Running` alike — see [Rows flowlite cannot read](../../orchestrator/SKILL.md#rows-flowlite-cannot-read). `Orchestrator::recover` settles an attempt left `Running` by an earlier process as `Invalid`. When the monitor or recovery settles one as anything but `Succeeded`, it calls `CRUD::stop_child_job_runs` *before* the status write, so a failure is retried next pass.
- **`waiting_since`** is written by the waiting process itself — `wait_for_job_run` ([src/shared/wait.rs](../../../../src/shared/wait.rs)), behind `job submit --wait` or an MCP tool's wait run inside the task, via `FLOWLITE_TASK_RUN_ATTEMPT_ID`. Stamped every poll, cleared when the wait returns, only on a `Running` attempt. Never by the orchestrator, which cannot see inside a command.

**Streams do not live here**; stdout/stderr go to [`task_run_attempt_output`](task_run_attempt_output.md). A stream arrives while the command works and is bounded by dropping its middle; `output` is a result, arriving once at the end, refused rather than trimmed. It is per attempt because its file is, so a retry never inherits old bytes; a task's result is the succeeding attempt's `output`, read by `CRUD::select_task_run_outputs` ([src/crud/multistatements/task_run_inputs.rs](../../../../src/crud/multistatements/task_run_inputs.rs)).

## Deleted by

`RetentionService`, with its [`job_run`](job_run.md) — see [job_run.md](job_run.md#deleted-by).

## Read by

- `TaskRunMonitor` — the last attempt decides what the task run does next.
- Both attempt services. `TaskRunAttemptDispatcher` also counts `Running` attempts without `waiting_since` for the global cap and named limits (`CRUD::count_running_attempts`, `claimed_limit_slots`), and reads `output` via `select_task_run_outputs` to point each `FLOWLITE_INPUT_<TASK_ID>` at a dependency's result.
- `Orchestrator::recover` — `process_group_id`, the only way back to a process an earlier run left behind; no kill unless `started_at` is after the last boot, since group ids are reused.
- `CRUD::resolve_parent_task_run_attempt` and `CRUD::stop_child_job_runs` — see [`job_run`](job_run.md).
- `job-run logs` ([src/cli/commands/job_run.rs](../../../../src/cli/commands/job_run.rs)) and the task-run web route.
