# `task_run_attempt_output` (disk)

stdout/stderr of one [`task_run_attempt`](task_run_attempt.md), in chunks. **Append-only**: no update method, no chunk rewritten.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. **Also the ordering** — see below. |
| `task_run_attempt_id` | Foreign key to [`task_run_attempt`](task_run_attempt.md). |
| `task_run_id` | Foreign key to [`task_run`](task_run.md). |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id`, `task_id` | Denormalized text, as on [`task_run_attempt`](task_run_attempt.md). |
| `stream` | `TaskRunAttemptOutputStream` — `stdout` or `stderr`, kept apart since a failing task explains itself on stderr. |
| `created_at` | Bound from `Toolkit`. |
| `content` | `TEXT NOT NULL`, already-validated UTF-8. Never empty: a stream with nothing new writes no row. |

**`id` is the ordering; no `seq`.** One writer inserts in write order, so `ORDER BY id` is write order — a contract `group_task_run_attempt_output` relies on from `select_task_run_attempt_outputs`.

**Every parent id is carried** so each view is one indexed query, not N+1 or an `IN (...)` list (and `IN ()` is a syntax error):

| Reader | Filter | Reads |
|---|---|---|
| task-run page | `task_run_id` | every attempt of that task run |
| `job-run logs` | `job_run_id` | every attempt of that job run |
| `job-run logs --task X` | `job_run_id` + `task_id` | only that task's attempts |

Retention's delete is then one `DELETE ... WHERE job_run_id = ?`.

**One row per stream per pass**: `TaskRunAttemptMonitor` coalesces a pass's chunks, so bytes written equal bytes produced (not the square, as rewriting a whole column each pass would be).

**Capped per stream by `[orchestrator] max_stream_bytes`** (default 1 MiB) in the reader, bounding the table and memory in flight: past it, a head and tail are kept, the middle dropped, with marker chunks saying so. **The reader keeps reading after the cap** — stopping would block the child on a full 64 KiB pipe and hang a noisy task.

## Written by

`TaskRunAttemptMonitor` alone ([src/orchestrator/task_run_attempt_monitor.rs](../../../../src/orchestrator/task_run_attempt_monitor.rs)), from its two reader tasks per attempt ([src/orchestrator/task_run_attempt_reader.rs](../../../../src/orchestrator/task_run_attempt_reader.rs)): each pass while the process lives (no wake-up published), then after a final drain **before** the terminal status — so a terminal attempt has complete output.

## Deleted by

`RetentionService`, with its [`job_run`](job_run.md) — see [job_run.md](job_run.md#deleted-by).

## Read by

The task-run web route ([src/router/app/routes/task_runs/task_run_id/route.rs](../../../../src/router/app/routes/task_runs/task_run_id/route.rs)), and `job-run logs` ([src/cli/commands/job_run.rs](../../../../src/cli/commands/job_run.rs)) and the MCP `get_job_run_logs` tool through `CRUD::select_task_run_attempt_logs`; also `NotificationService`, for the output a message quotes. They group with `group_task_run_attempt_output`; an attempt with no rows is `TaskRunAttemptOutputStreams::default()` — empty, not unknown, so output is a `String`, not `Option<String>`.

No orchestrator service reads it: only statuses pass between services.
