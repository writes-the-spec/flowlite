# `job_run` (disk)

One execution of a [`job`](job.md). Created `Submitted`, released to `Queued` by `JobRunReleaser`, driven to a terminal status by `JobRunDispatcher` and `JobRunMonitor`, and kept forever — this is the history.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. Also the dispatcher's oldest-first order. |
| `job_id` | The job this run is of. **No foreign key** — the job lives in the attached `mem` database, so one is impossible. |
| `job_name`, `job_description` | **Snapshot copies** of `job.name`/`job.description` at submit time, so a finished run still displays as it was submitted however the YAML has moved. |
| `parameters` | `NOT NULL`. The **resolved** set — `job.parameters`' defaults with the caller's overrides applied by `resolve_job_parameters` — not the declaration itself. |
| `scheduled_at` | `NOT NULL DATETIME`. The instant this run is due — one of the cron occurrences the [Scheduler](../../scheduler/SKILL.md) computed for a scheduled run, or the submit instant (or an explicit `--schedule-at` / `schedule_at`) for a manual submission. Every run has one, so it is always injected onto the command as `FLOWLITE_SCHEDULED_AT` by [`build_task_run_attempt_env`](../../../../src/orchestrator/task_run_attempt_env.rs), overwriting anything a task's own `env:` (or the inherited process environment) tried to put there. |
| `schedule_id` | Nullable `TEXT`. The schedule that asked for this run, or `NULL` for a run nobody scheduled — a manual `job submit`, or a rerun of any run. **No foreign key** — the schedule lives in the attached `mem` database, same reason as `job_id`. |
| `created_at` | Bound from `Toolkit::get_current_ts()`, like every timestamp here. |
| `started_at` | Nullable. Written exactly once, by `JobRunDispatcher::settle_as_running`. Task run retries never touch it. |
| `finished_at` | Nullable. Written with every terminal status. |
| `status` | `JobRunStatus` — see the [orchestrator skill](../../orchestrator/references/job_run.md) for the nine variants and how task run statuses add up to one. `JobRunStatus::ALL` lists them in dashboard-filter order, and is what both that filter and the CLI's `--status` read. Every run is created `Submitted`; `JobRunReleaser` ([src/orchestrator/job_run_releaser.rs](../../../../src/orchestrator/job_run_releaser.rs)) is the only thing that moves a run out of it, and it always moves it to `Queued` — either because `scheduled_at` has arrived or because the run was stopped early (see [`job_run_stop`](job_run_stop.md)) and `JobRunDispatcher` needs to see it to skip it. |

## Written by

- **Inserted** by `CRUD::submit_job` and `CRUD::rerun_job` ([src/crud/multistatements/](../../../../src/crud/multistatements/)), always `Submitted`, together with one [`task_run`](task_run.md) per task (still `Queued` — there is no "submitted" status at that level) and one [`job_run_notification`](job_run_notification.md) per channel the job named, in the same call. `rerun_job` reads an existing run and its task runs and reproduces the snapshot rather than re-reading the YAML, so a rerun executes what the original did — including copying `parameters` and `scheduled_at` verbatim, not the job's current defaults or a fresh instant. A rerun of a scheduled run therefore still runs for the day it was originally scheduled for, and since that day is normally in the past it reaches `JobRunReleaser`'s notice almost immediately. `schedule_id` is the one field `rerun_job` deliberately does **not** copy: it always writes `NULL`, because the schedule asked for the original run, not for the rerun — copying it would make the rerun count as that schedule's own outstanding run.
- **Updated** by `JobRunReleaser` (`Submitted` → `Queued`), `JobRunDispatcher` (`Queued` → `Running`/`Skipped`) and `JobRunMonitor` (`Running` → terminal), and by nothing else. No request handler and no CLI command writes a run status.

## Deleted by

`RetentionService` ([src/retention/service.rs](../../../../src/retention/service.rs)), through `CRUD::delete_job_runs_with_children` ([src/crud/multistatements/](../../../../src/crud/multistatements/delete_job_runs_with_children.rs)) — never a run still `Submitted`, `Queued` or `Running`, and never one still owing a `Pending` row in [`job_run_notification`](job_run_notification.md). Deleting a run deletes this row and, in the same transaction, every row across the other five tables here that carries its `job_run_id`.

The [Scheduler](../../scheduler/SKILL.md) also deletes rows here, through the same `delete_job_runs_with_children` — a `Submitted` run still in the future that its schedule no longer wants, because `submit_ahead` fell or the cron changed, or because the schedule itself left the YAML. That is the one case a `Submitted` row is deleted rather than left to run: never one already due, which belongs to `JobRunReleaser` from that moment on.

`[job_defaults] keep_runs` (or a job's own `keep_runs` override on [`mem.job`](job.md)) bounds how many of a job's newest finished runs survive; `[retention] keep_runs_total` is the ceiling across every job, oldest first, enforced after each job's own number. See [Retention](../../../../README.md#retention).

Deleting a run also deletes the config snapshot a rerun would replay, so `job-run rerun` on a deleted run fails exactly as it would for an id that never existed — see [Reruns](../../../../README.md#reruns).

## Read by

`JobRunReleaser`, `JobRunDispatcher` and `JobRunMonitor` (the three that update it), `CRUD::is_job_at_max_parallel_runs` (counting this job's `Running` rows), the [Scheduler](../../scheduler/SKILL.md) (its own `Submitted` runs, to reconcile them against what a schedule now wants), the `job-run` CLI commands, and the home and job-run web routes.

There is no `job_id` foreign key, but no run is created for a job that isn't there either: `submit_job` selects `mem.job` first and bails with `Job '<id>' not found`.
