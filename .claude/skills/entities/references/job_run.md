# `job_run` (disk)

One execution of a [`job`](job.md), and its history until retention prunes it.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. Also the dispatcher's oldest-first order. |
| `job_id` | **No foreign key** — the job lives in `mem`, so one is impossible. |
| `job_name`, `job_description` | **Snapshot copies** of `job.name`/`job.description` at submit, so a finished run displays as submitted. |
| `parameters` | `NOT NULL`. The **resolved** set — `job.parameters` defaults with the caller's overrides applied by `resolve_job_parameters`. |
| `scheduled_at` | `NOT NULL DATETIME`. When the run is due: a cron occurrence from the [Scheduler](../../scheduler/SKILL.md), else the submit instant or `--schedule-at` / `schedule_at`. Always injected as `FLOWLITE_SCHEDULED_AT` by [`build_task_run_attempt_env`](../../../../src/orchestrator/task_run_attempt_env.rs), overriding `env:` and the inherited environment. |
| `schedule_id` | Nullable `TEXT`. The schedule that asked for this run; `NULL` for a manual `job submit` or any rerun. **No foreign key** (in `mem`). |
| `parent_task_run_attempt_id` | Nullable FK to `task_run_attempt (id)`: the attempt whose command submitted this run (`job submit`, `job-run rerun` or MCP `submit_job`, via `FLOWLITE_TASK_RUN_ATTEMPT_ID`); `NULL` if no task did. The parent job run is that attempt's `job_run_id` (the `parent_job_run_id` select filter). `CRUD::resolve_parent_task_run_attempt` links nothing if the attempt row is gone, and refuses if the attempt ended without succeeding or its run has a stop row. Not copied by `rerun_job`. |
| `created_at` | Bound from `Toolkit::get_current_ts()`, like every timestamp here. |
| `started_at` | Nullable. Written once, by `JobRunDispatcher::set_to_running`; task run retries never touch it. |
| `finished_at` | Nullable. Written with every terminal status. |
| `status` | `JobRunStatus` — variants and how task run statuses add up to one are in the [orchestrator skill](../../orchestrator/references/job_run.md). `JobRunStatus::ALL` is the dashboard-filter order and what the CLI's `--status` parses. |

## Written by

- **Inserted** by `CRUD::submit_job` and `CRUD::rerun_job` ([src/crud/multistatements/](../../../../src/crud/multistatements/)), always `Scheduled`, with one `Planned` [`task_run`](task_run.md) per task and one [`job_run_notification`](job_run_notification.md) per channel per block. `rerun_job` replays the original's snapshot, `parameters` and `scheduled_at` verbatim (so a past occurrence is due at once), but writes `schedule_id` `NULL`, or the rerun would count as that schedule's outstanding run.
- **Updated** by:
  - `JobRunReleaser` ([src/orchestrator/job_run_releaser.rs](../../../../src/orchestrator/job_run_releaser.rs)) — `Scheduled` → `Queued` once `scheduled_at` arrives, `Skipped` if stopped first (see [`job_run_stop`](job_run_stop.md)), `Invalid` if nothing claims it.
  - `JobRunDispatcher` — `Queued` → `Running`/`Skipped`/`Invalid`.
  - `JobRunMonitor` — `Running` → terminal.
  - `CRUD::delete_job_run` (`job-run delete`, MCP `delete_job_run`, the job-run web route) — `Scheduled` → `Deleted`, a tombstone, not a row delete; any other status is refused, checked inside its transaction against the releaser.

## Deleted by

Only `RetentionService` ([src/retention/service.rs](../../../../src/retention/service.rs)), through `CRUD::delete_job_runs_with_children` ([src/crud/multistatements/](../../../../src/crud/multistatements/delete_job_runs_with_children.rs)). Never a run `Scheduled`, `Queued` or `Running`, nor one owing a `pending` [`job_run_notification`](job_run_notification.md). One transaction deletes the run, every row in the other five tables with its `job_run_id`, and every run its tasks submitted (`CRUD::select_job_run_descendants`) — deepest first, since each child's `parent_task_run_attempt_id` references an attempt of the run above. A run with an unfinished descendant waits for a later pass.

- `[job_defaults] keep_runs` (or [`mem.job`](job.md)'s `keep_runs`) keeps a job's newest finished runs; `[retention] keep_runs_total` then caps all jobs, oldest first. See [Retention](../../../../README.md#retention).
- `job-run rerun` of a deleted run fails as for an unknown id — see [Reruns](../../../../README.md#reruns).
- **The [Scheduler](../../scheduler/SKILL.md) only adds.** A `Scheduled` run its schedule no longer wants (`submit_ahead` fell, cron changed, job left `jobs:`, schedule disabled or deleted) still runs. A stop (→ `Skipped`) calls it off for good; `job-run delete` (→ `Deleted`) frees the occurrence, which the Scheduler resubmits if it is still ahead.

## Read by

- `JobRunReleaser`, `JobRunDispatcher` and `JobRunMonitor`.
- `CRUD::is_job_at_max_parallel_runs` — counts this job's `Running` rows.
- The [Scheduler](../../scheduler/SKILL.md) — one lookup per (`schedule_id`, `job_id`, `scheduled_at`) for whether that occurrence exists, in any status but `Deleted`.
- `RetentionService`, `CRUD::stop_child_job_runs`, the `job-run` CLI commands, MCP tools, and the home and job-run web routes.

No `job_id` foreign key, but `submit_job` selects `mem.job` first and bails with `Job '<id>' not found`.
