# `job_run_stop` (disk)

A stop signal for a [`job_run`](job_run.md). **Insert-only** — there is no status column, no update method, and stopping a run is never a status write on `job_run` itself.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). The row's existence *is* the signal. |
| `created_at` | Bound from `Toolkit`. |

## Written by

The job-run detail web route ([src/router/app/routes/job_runs/job_run_id/route.rs](../../../../src/router/app/routes/job_runs/job_run_id/route.rs)). There is no CLI stop command.

## Read by

Four of the six orchestrator services, each on every pass — a signal wake-up or the one-second interval, whichever came first: `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor`. Each finishes only what it owns: a row that never started goes `Skipped`, an in-flight process is killed and its attempt goes `Aborted`.

The two remaining monitors never read it. **A stopped job run's status is derived like any other** — from its task runs, once they have all settled — so the stop reaches `job_run` only as the statuses it produced. See the [orchestrator skill](../../orchestrator/SKILL.md).

## The pattern worth copying

An insert-only signal table that the pollers check is how run control is done here, rather than mutating run state from an unrelated code path. A new control feature — pause, resume, retry-all — should follow the same shape: a small table, checked by whichever services own the rows it affects, with each of them deciding for itself what its own row's lifecycle makes correct.
