# `task_run` (disk)

One [`task`](task.md) within one [`job_run`](job_run.md) — and **the config that task was submitted with**. One row per task of the job, inserted for *every* task, not just the ones without dependencies; ordering is enforced later, at dispatch time.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id`, `task_id` | Which task this is a run of. No foreign key — the task is in `mem`. |
| `command`, `depends_on`, `timeout`, `max_retries`, `retry_delay`, `env`, `secret_env`, `working_dir`, `limits` | **The snapshot.** Copied off `mem.task` by `submit_job`; see below. `env`, `secret_env` and `limits` are all exceptions to "copied": each is resolved from both `mem.job` and `mem.task` in `job_run_task_definition`, not `submit_job` directly, but by different rules. `env` and `secret_env` are `mem.job`'s own layered with `mem.task`'s own, the task's own winning a name both declare. `secret_env` is environment variable name to secret name, never a value. A name the task declares in one block also evicts the job's contribution to the *other* block, so a row's `env` and `secret_env` never share a name between them — the YAML layer only rejects a name declared in both blocks of the *same* level, not this cross-level case. `limits` is different again: it is the job's `limits:` **unioned** with the task's own, deduplicated and sorted — not layered, and nothing wins, because two lists of claimed resources have no sensible precedence between them the way a variable's value does. The result is a JSON array of named concurrency limits — `'[]'` for a run that claims none, which is also what the rebuild backfilled onto every row predating the column, since no run submitted before it could name a limit. |
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

`TaskRunDispatcher` (its own rows and its dependencies'), `TaskRunMonitor`, `JobRunMonitor` (all task runs of a job run, to settle it), `TaskRunAttemptDispatcher` (for `command`, `timeout` and `retry_delay` to spawn with, `working_dir` for the child's `current_dir`, and `env`/`secret_env` to pass to `build_task_run_attempt_env`), and the job-run and task-run web routes — the latter renders `env` on the task-run page, deliberately: it is plaintext on disk already, and hiding it would make a wrong `env:` undebuggable from the run.

`secret_env` is rendered on the same page for the inverse reason, and a stronger one: not because the value is harmless to show, but because there is no value here to show. This column is a reference — variable name to secret name — and the value it names is resolved only at spawn, into the environment of one `sh` process, and never written anywhere. The task-run page can therefore say which credential a run used without ever being in a position to reveal it; there is no plaintext-on-disk argument to make because there is no plaintext on disk, and hiding the reference would only make a run that used the wrong secret undebuggable in exactly the way rendering `env` avoids.
