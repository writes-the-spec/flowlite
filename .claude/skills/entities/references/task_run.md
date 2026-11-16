# `task_run` (disk)

One [`task`](task.md) within one [`job_run`](job_run.md), and **the config it was submitted with**. One row per task of the job; ordering is enforced at dispatch.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id`, `task_id` | Which task. No foreign key — the task is in `Declared`, not a table. |
| `command`, `stdin`, `depends_on`, `timeout`, `idle_timeout`, `max_retries`, `retry_delay`, `working_dir` | **The snapshot**, copied off the declared [`Task`](task.md) by `submit_job`. `stdin` is snapshotted like `command`: a reworded prompt is not what this run asked. |
| `env`, `secret_env` | From `job_run_task_definition`: the job's layered under the task's, task winning. `secret_env` maps variable → secret name, never a value. A task's name in one block evicts the job's from the *other*, so the two never share a name (the YAML layer only rejects both blocks at the same level). |
| `limits` | From `job_run_task_definition`: job's and task's **unioned**, deduplicated, sorted — claims have no precedence. `'[]'` if none. |
| `created_at` | Bound from `Toolkit`. |
| `started_at` | Nullable. Written once, when the task run starts — covering every attempt, not the current one. |
| `finished_at` | Nullable. Written with every terminal status. |
| `status` | `TaskRunStatus` — see the [orchestrator skill](../../orchestrator/references/task_run.md). |

`UNIQUE (job_run_id, task_id, job_id)`: one run per task per job run.

## The snapshot is the point of this table

The orchestrator reads config only here; `TaskRunDispatcher::get_dependent_task_runs` resolves the copied `depends_on`. Sole exception: the `Job`'s `max_parallel_runs`, read live by `JobRunDispatcher` - the only orchestrator service handed `Declared` at all.

## Written by

- **Inserted** by `CRUD::submit_job` / `rerun_job`, all `Planned`, with their `job_run`.
- **Updated** by:
  - `JobRunDispatcher::set_to_running` — `Planned` → `Waiting`, all of the job run's in one update; the only way into `Waiting`, so what makes a task run visible to `TaskRunDispatcher`.
  - `TaskRunDispatcher` — `Waiting` → `Running`/`Skipped`.
  - `TaskRunMonitor` — `Running` → terminal.
  - Bulk settles of a never-started job run: `CRUD::skip_job_run` (→ `Skipped`; `JobRunReleaser`, `JobRunDispatcher`, for a stopped run), `CRUD::invalidate_job_run` (→ `Invalid`).

## Deleted by

`RetentionService`, with its [`job_run`](job_run.md) — see [job_run.md](job_run.md#deleted-by).

## Read by

- `TaskRunDispatcher` (its own rows and its dependencies'), `TaskRunMonitor`, `JobRunMonitor` (all of a job run's, to settle it).
- `TaskRunAttemptDispatcher` — `command`, `timeout` and `idle_timeout` (onto the in-memory child), `retry_delay`; `stdin` (written from its own task when non-empty, else `/dev/null`); `working_dir` as `current_dir`; `env`/`secret_env` for `build_task_run_attempt_env`; `limits` for the concurrency check.
- The job-run and task-run web routes. The task-run page shows `env` deliberately (already plaintext on disk; hiding it makes a wrong `env:` undebuggable) and `secret_env` too — only the reference, since the value is resolved at spawn into one `sh` environment and stored nowhere.
