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
| `output` | `TEXT NOT NULL`, empty for nothing at all — the convention [`task_run`](task_run.md)'s `stdin` uses for the other end of the same channel. What the command wrote to `$FLOWLITE_TASK_OUTPUT`, read off disk and recorded when the attempt succeeds. Bounded by `[orchestrator] max_task_output_bytes`: a result over it fails the attempt instead, so this never holds a truncated one. |
| `process_group_id` | Nullable. The spawned `sh`'s pid, which is also its process group id because the child is made a group leader — so a stop or a timeout signals the whole tree the command started, not only the shell flowlite spawned. NULL until the command is spawned, and for good on an attempt that never ran. |
| `waiting_since` | Nullable. When this attempt's process began waiting on another run; a Running attempt with it set holds no concurrency slot. |

`UNIQUE (task_run_id, attempt)`: the attempt number is computed rather than constrained (1 in `TaskRunDispatcher`, `last.attempt + 1` in `TaskRunMonitor`), so this index is what turns a second process racing the first into a failed insert instead of a task run quietly executed twice. It is also what makes "the last attempt" well defined — `get_last_task_run_attempt` orders by `attempt`.

## Written by

- **Inserted** by `TaskRunDispatcher` for attempt 1, before it writes the task run `Running`, and by `TaskRunMonitor` for every retry. Two services, but never the same row: the dispatcher only visits `Queued` task runs and the monitor only `Running` ones, and each insert is part of a transition that service already owns — attempt 1 *is* the task run starting, a retry *is* the task run not finishing.
- **Updated** by `TaskRunAttemptDispatcher` (`Queued` → `Running`/`Skipped`) and `TaskRunAttemptMonitor` (`Running` → terminal). Every transition after the insert belongs to the attempt services alone. Both can also write `Invalid`, which is the one status reached from `Queued` and `Running` alike — see [Rows flowlite cannot read](../../orchestrator/SKILL.md#rows-flowlite-cannot-read).

**The attempt's streams do not live here.** stdout and stderr are appended to [`task_run_attempt_output`](task_run_attempt_output.md) in chunks, one row per stream per poll pass. They used to be two `TEXT NOT NULL` columns on this table, rewritten whole on every pass, which cost the square of the output size — see that reference for why the chunks replaced them and for the 1 MiB per-stream cap.

**`output` is not one of them**, and the difference is what it is for. A stream is what the command said while working, arrives while it runs, and is bounded by dropping its middle; a result is what the command produced, arrives once at the end, and is refused rather than trimmed. It is on the attempt rather than on `task_run` because the file it is read from is per attempt: a retry must not inherit the bytes of the attempt it replaces, and the task's result is therefore *derived* — the output of the attempt that succeeded, which `CRUD::select_task_run_outputs` ([src/crud/multistatements/task_run_inputs.rs](../../../../src/crud/multistatements/task_run_inputs.rs)) is what reads.

## Deleted by

`RetentionService`, along with the [`job_run`](job_run.md) each row belongs to — see [job_run.md](job_run.md#deleted-by) for the policy.

## Read by

`Orchestrator::recover` reads `process_group_id` after a restart — it is the only way to reach a process an earlier run of the program left behind, and the kill is refused unless `started_at` is after the machine last booted, since a group id is a number the kernel hands out again.

`TaskRunAttemptDispatcher` reads `output` through `select_task_run_outputs` when it builds a command's environment, to point each `FLOWLITE_INPUT_<TASK_ID>` at the file a dependency's successful attempt wrote.

`TaskRunMonitor` (the last attempt decides what the task run does next), both attempt services, `job-run logs` ([src/cli/commands/job_run.rs](../../../../src/cli/commands/job_run.rs)) and the task-run web route.
