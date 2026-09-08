# `task_run` (disk)

One [`task`](task.md) within one [`job_run`](job_run.md) — and **the config that task was submitted with**. One row per task of the job, inserted for *every* task, not just the ones without dependencies; ordering is enforced later, at dispatch time.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id`, `task_id` | Which task this is a run of. No foreign key — the task is in `mem`. |
| `command`, `depends_on`, `timeout`, `max_retries`, `retry_delay`, `env`, `working_dir` | **The snapshot.** Copied off `mem.task` by `submit_job`; see below. `env` is the exception to "copied": it is `mem.job.env` with `mem.task.env` layered over it, so the row holds the merged environment rather than either declaration. |
| `created_at` | Bound from `Toolkit`. |
| `started_at` | Nullable. Written once, when the task run starts — it means "when the task run started", covering every attempt, not "when the current attempt started". |
| `finished_at` | Nullable. Written with every terminal status. |
| `status` | `TaskRunStatus` — see the [orchestrator skill](../../orchestrator/references/task_run.md). |

`UNIQUE (job_run_id, task_id, job_id)`: one run per task per job run.

## The snapshot is the point of this table

Every config field the orchestrator acts on is read here, never from `mem.task`. **No orchestrator file imports `crate::crud::task` or `crate::crud::job`, and none touches [`task_dependent`](task_dependent.md).** A run therefore executes what it was submitted with however the YAML has moved since — and `TaskRunDispatcher::get_dependent_task_runs` resolves `task_run.depends_on`, the copy, not the definition.

The one deliberate exception in the whole orchestrator is `mem.job.max_parallel_runs`, which is read live because it is a question about the job now. Nothing enforces the rule: `mem` is attached to every pooled connection, so `mem.task` is one query away from any service that forgets.

## Written by

- **Inserted** by `CRUD::submit_job` / `rerun_job`, all `Pending`, in the same call as their `job_run`.
- **Updated** by `TaskRunDispatcher` (`Pending` → `Running`/`Skipped`), `TaskRunMonitor` (`Running` → terminal), and `JobRunDispatcher::settle_as_skipped`, which skips every task run of a stopped, never-started job run in one bulk update.

## Read by

`TaskRunDispatcher` (its own rows and its dependencies'), `TaskRunMonitor`, `JobRunMonitor` (all task runs of a job run, to settle it), `TaskRunAttemptDispatcher` (for `command`, `timeout` and `retry_delay` to spawn with, and `env`/`working_dir` to pass to `build_task_run_attempt_env`), and the job-run and task-run web routes — the latter renders `env` on the task-run page, deliberately: it is plaintext on disk already, and hiding it would make a wrong `env:` undebuggable from the run.
