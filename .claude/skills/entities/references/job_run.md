# `job_run` (disk)

One execution of a [`job`](job.md). Created `Pending`, driven to a terminal status by `JobRunDispatcher` and `JobRunMonitor`, and kept forever — this is the history.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. Also the dispatcher's oldest-first order. |
| `job_id` | The job this run is of. **No foreign key** — the job lives in the attached `mem` database, so one is impossible. |
| `job_name`, `job_description` | **Snapshot copies** of `job.name`/`job.description` at submit time, so a finished run still displays as it was submitted however the YAML has moved. |
| `parameters` | `NOT NULL`. The **resolved** set — `job.parameters`' defaults with the caller's overrides applied by `resolve_job_parameters` — not the declaration itself. |
| `scheduled_at` | Nullable `DATETIME`. The instant a schedule fired for, `NULL` for a manual `job submit`. Injected onto the command as `FLOWLITE_SCHEDULED_AT` — omitted entirely, not empty, when it is `NULL` — by [`build_task_run_attempt_env`](../../../../src/orchestrator/task_run_attempt_env.rs). |
| `created_at` | Bound from `Toolkit::get_current_ts()`, like every timestamp here. |
| `started_at` | Nullable. Written exactly once, by `JobRunDispatcher::settle_as_running`. Task run retries never touch it. |
| `finished_at` | Nullable. Written with every terminal status. |
| `status` | `JobRunStatus` — see the [orchestrator skill](../../orchestrator/references/job_run.md) for the eight variants and how task run statuses add up to one. `JobRunStatus::ALL` lists them in dashboard-filter order, and is what both that filter and the CLI's `--status` read. |

## Written by

- **Inserted** by `CRUD::submit_job` and `CRUD::rerun_job` ([src/crud/multistatements/](../../../../src/crud/multistatements/)), always `Pending`, together with one [`task_run`](task_run.md) per task and one [`job_run_notification`](job_run_notification.md) per channel the job named, in the same call. `rerun_job` reads an existing run and its task runs and reproduces the snapshot rather than re-reading the YAML, so a rerun executes what the original did — including copying `parameters` and `scheduled_at` verbatim, not the job's current defaults or a fresh `NULL`. A rerun of a scheduled run therefore still runs for the day it was originally scheduled for.
- **Updated** by `JobRunDispatcher` (`Pending` → `Running`/`Skipped`) and `JobRunMonitor` (`Running` → terminal), and by nothing else. No request handler and no CLI command writes a run status.

## Read by

Both of those services, `CRUD::is_job_at_max_parallel_runs` (counting this job's `Running` rows), the `job-run` CLI commands, and the home and job-run web routes.

There is no `job_id` foreign key, but no run is created for a job that isn't there either: `submit_job` selects `mem.job` first and bails with `Job '<id>' not found`.
