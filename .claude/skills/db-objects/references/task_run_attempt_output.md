# `task_run_attempt_output` (disk)

The output of one [`task_run_attempt`](task_run_attempt.md), in chunks. **Append-only** — there is no update method, and no chunk is ever rewritten or deleted.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. **Also the ordering** — see below. |
| `task_run_attempt_id` | Foreign key to [`task_run_attempt`](task_run_attempt.md). The only parent id carried; see below. |
| `stream` | `TaskRunAttemptOutputStream` — `stdout` or `stderr`. The streams stay apart because a task that failed usually explains itself on stderr while stdout still holds whatever it managed to produce. |
| `created_at` | Bound from `Toolkit`. |
| `content` | `TEXT NOT NULL`, already-validated UTF-8. Never empty: a stream with nothing new writes no row. |

## Why `id` is the ordering and there is no `seq`

Every chunk of one stream is inserted by one writer in write order, so `ORDER BY id` *is* write order. A `seq` column would be a second source of truth for the same fact, and `group_task_run_attempt_output` depends on the order rather than re-deriving it — which is why the sort is a contract with `select_task_run_attempt_outputs`, not an incidental choice.

## Why there are no denormalized parent ids

`task_run_attempt` carries `job_run_id`, `job_id` and `task_id` so the log views need no join through `task_run`. This table deliberately does not, because **both its readers already hold the attempt ids they want output for** and filter on `task_run_attempt_ids`, an `IN (...)` list. That avoids the join for the same reason the denormalization does, with less schema — and it avoids an over-fetch the denormalization would have caused: `job-run logs --task X` filtered by `job_run_id` would read every task's output to print one task's.

An empty id list becomes `AND 0 = 1` rather than `IN ()`, which is a syntax error. A task run with no attempts reaches that case.

## One row per stream per pass

`TaskRunAttemptMonitor` coalesces everything its reader tasks delivered on a pass into a single `content` per stream, so writes are at most two inserts per attempt per second and **total bytes written equal total bytes the task produced.** The `stdout`/`stderr` columns this replaced were rewritten whole on every pass, which cost the square of the output size — that is the defect this table exists to fix.

Output is capped at `MAX_STREAM_BYTES` (1 MiB) per stream, enforced in the reader, which bounds both this table and the memory in flight. Past the cap the reader keeps reading and stops recording, and appends one marker chunk saying so. **A cap that stopped reading would block the child on a full 64 KiB pipe and turn a noisy task into a hung one.**

Nothing prunes this table. The cap bounds one attempt; retention over time is not designed.

## Written by

`TaskRunAttemptMonitor` alone ([src/orchestrator/task_run_attempt_monitor.rs](../../../../src/orchestrator/task_run_attempt_monitor.rs)), from the chunks the two reader tasks per attempt deliver — see [src/orchestrator/task_run_attempt_reader.rs](../../../../src/orchestrator/task_run_attempt_reader.rs). Inserted on every pass while the process is alive, and once more after a final drain when it ends.

That final insert happens **before** the terminal status is written, so an attempt that reads as terminal has complete output. The per-pass insert is a data-only write and deliberately publishes no wake-up.

## Read by

The task-run web route ([src/router/app/routes/task_runs/task_run_id/route.rs](../../../../src/router/app/routes/task_runs/task_run_id/route.rs)) and `job-run logs` ([src/cli/commands/job_run.rs](../../../../src/cli/commands/job_run.rs)). Both group with `group_task_run_attempt_output`, and both resolve a missing attempt to `TaskRunAttemptOutputStreams::default()` — an attempt with no rows printed nothing, which is empty output rather than unknown output. That `unwrap_or_default` is what keeps output a `String` instead of an `Option<String>` now that the `NOT NULL` columns are gone.

No orchestrator service reads it. Output is not a channel between services; only statuses are.
