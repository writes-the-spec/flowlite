# `job_run_stop` (disk)

A stop signal for a [`job_run`](job_run.md). **Insert-only** — no status column, no update method; stopping a run is never a status write on `job_run`.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). The row's existence *is* the signal. |
| `created_at` | Bound from `Toolkit`. |

## Written by

- The job-run detail web route ([src/router/app/routes/job_runs/job_run_id/route.rs](../../../../src/router/app/routes/job_runs/job_run_id/route.rs)).
- `stop_job_run` ([src/shared/job_run.rs](../../../../src/shared/job_run.rs)), for `job-run stop` and the MCP `stop_job_run` tool.
- `CRUD::stop_child_job_runs` ([src/crud/multistatements/stop_child_job_runs.rs](../../../../src/crud/multistatements/stop_child_job_runs.rs)) — one row for each unfinished run an attempt submitted that has none yet, called by `TaskRunAttemptMonitor` and crash recovery when they settle that attempt as anything but `Succeeded`.

## Deleted by

`RetentionService`, with its [`job_run`](job_run.md) — see [job_run.md](job_run.md#deleted-by).

## Read by

- Five of the seven orchestrator services, every pass: `JobRunReleaser`, `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor`. The releaser skips a stopped `Scheduled` run and its task runs through `CRUD::skip_job_run`, as `JobRunDispatcher` does one status later; the others finish only what they own — a row that never started goes `Skipped`, an in-flight process is killed and its attempt goes `Aborted`.
- `CRUD::stop_child_job_runs` (to skip a child already stopped) and `CRUD::resolve_parent_task_run_attempt` (refuses a submission from a task of a stopped run).

`JobRunMonitor` and `TaskRunMonitor` never read it: **a stopped job run's status is derived like any other**, from its settled task runs. See the [orchestrator skill](../../orchestrator/SKILL.md).

## The pattern worth copying

Run control is an insert-only signal table the pollers check, not run state mutated from an unrelated code path. A new control (pause, resume, retry-all) should be a small table checked by the services owning the affected rows, each deciding what its own row's lifecycle makes correct.
