# `task_run_attempt` (disk)

One execution of a [`task_run`](task_run.md)'s command. **This is the only level that runs a process.** Attempts count from 1, so a task run gets `1 + max_retries` of them.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `task_run_id` | Foreign key to [`task_run`](task_run.md). |
| `job_run_id` | Foreign key to [`job_run`](job_run.md) — carried directly so the stop check and the log views don't have to join through `task_run`. |
| `job_id`, `task_id` | Denormalized for the same reason. |
| `created_at` | Bound from `Toolkit`. **`retry_delay` is measured from this**, since it is when `TaskRunMonitor` decided to retry. |
| `started_at` | Nullable. Set when the command is spawned — so a `Skipped` attempt has it NULL, which is what says the command never ran. |
| `finished_at` | Nullable. Written with every terminal status. |
| `attempt` | 1 for the first, `last.attempt + 1` for each retry. |
| `status` | `TaskRunAttemptStatus` — same eight variants as `TaskRunStatus`, but a separate enum; see the [orchestrator skill](../../orchestrator/references/task_run_attempt.md). |

`UNIQUE (task_run_id, attempt)`: the attempt number is computed rather than constrained (1 in `TaskRunDispatcher`, `last.attempt + 1` in `TaskRunMonitor`), so this index is what turns a second process racing the first into a failed insert instead of a task run quietly executed twice. It is also what makes "the last attempt" well defined — `get_last_task_run_attempt` orders by `attempt`.

## Written by

- **Inserted** by `TaskRunDispatcher` for attempt 1, before it writes the task run `Running`, and by `TaskRunMonitor` for every retry. Two services, but never the same row: the dispatcher only visits `Pending` task runs and the monitor only `Running` ones, and each insert is part of a transition that service already owns — attempt 1 *is* the task run starting, a retry *is* the task run not finishing.
- **Updated** by `TaskRunAttemptDispatcher` (`Pending` → `Running`/`Skipped`) and `TaskRunAttemptMonitor` (`Running` → terminal). Every transition after the insert belongs to the attempt services alone. Both can also write `Invalid`, which is the one status reached from `Pending` and `Running` alike — see [Rows flowlite cannot read](../../orchestrator/SKILL.md#rows-flowlite-cannot-read).

**The attempt's output does not live here.** It is appended to [`task_run_attempt_output`](task_run_attempt_output.md) in chunks, one row per stream per poll pass. It used to be two `TEXT NOT NULL` columns on this table, rewritten whole on every pass, which cost the square of the output size — see that reference for why the chunks replaced them and for the 1 MiB per-stream cap.

## Read by

`TaskRunMonitor` (the last attempt decides what the task run does next), both attempt services, `job-run logs` ([src/cli/commands/job_run.rs](../../../../src/cli/commands/job_run.rs)) and the task-run web route.
