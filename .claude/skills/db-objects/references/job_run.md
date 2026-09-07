# `job_run` (disk)

One execution of a [`job`](job.md). Created `Pending`, driven to a terminal status by `JobRunDispatcher` and `JobRunMonitor`, and kept forever — this is the history.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. Also the dispatcher's oldest-first order. |
| `job_id` | The job this run is of. **No foreign key** — the job lives in the attached `mem` database, so one is impossible. |
| `job_name`, `job_description` | **Snapshot copies** of `job.name`/`job.description` at submit time, so a finished run still displays as it was submitted however the YAML has moved. |
| `created_at` | Bound from `Toolkit::get_current_ts()`, like every timestamp here. |
| `started_at` | Nullable. Written exactly once, by `JobRunDispatcher::settle_as_running`. Task run retries never touch it. |
| `finished_at` | Nullable. Written with every terminal status. |
| `status` | `JobRunStatus` — see the [orchestrator skill](../../orchestrator/references/job_run.md) for the seven variants and how task run statuses add up to one. |

## Written by

- **Inserted** by `CRUD::submit_job` and `CRUD::rerun_job` ([src/crud/multistatements/misc.rs](../../../../src/crud/multistatements/misc.rs)), always `Pending`, together with one [`task_run`](task_run.md) per task in the same call. `rerun_job` reads an existing run and its task runs and reproduces the snapshot rather than re-reading the YAML, so a rerun executes what the original did.
- **Updated** by `JobRunDispatcher` (`Pending` → `Running`/`Skipped`) and `JobRunMonitor` (`Running` → terminal), and by nothing else. No request handler and no CLI command writes a run status.

## Read by

Both of those services, `CRUD::is_job_at_max_parallel_runs` (counting this job's `Running` rows), the `job-run` CLI commands, and the home and job-run web routes.

There is no `job_id` foreign key, but no run is created for a job that isn't there either: `submit_job` selects `mem.job` first and bails with `Job '<id>' not found`.
