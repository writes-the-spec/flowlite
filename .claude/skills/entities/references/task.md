# `task` (mem)

One row per task of a job, declared inline under the job YAML's `tasks:` list. Like [`job`](job.md) this is a *definition*; what executes and carries state is a [`task_run`](task_run.md), and each execution of its command is a [`task_run_attempt`](task_run_attempt.md).

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. |
| `task_id`, `job_id` | **Composite primary key** — a task id is unique per job, not globally. `job_id` is a foreign key to [`job`](job.md). |
| `description` | `NOT NULL`, `''` when the YAML declares none. What the task does, in words; never read by anything that runs. |
| `command` | The shell command, run as `sh -c <command>`. |
| `depends_on` | JSON array of task ids in the same job. `NOT NULL`; a task with no dependencies stores `'[]'`. |
| `timeout` | Seconds. Defaults to 3600 in the YAML, not in the DDL. |
| `max_retries` | Retries *after* the first attempt, so executions total `1 + max_retries`. Defaults to 0. |
| `retry_delay` | Seconds to wait before each retry. Defaults to 60. |
| `env` | `NOT NULL`, `'{}'` when the task declares none. Environment variables layered onto the command's, over whatever flowlite itself inherited. |
| `working_dir` | `NOT NULL`, `''` meaning inherit the server's own working directory. |

## Written by

`CRUD::init` only, from `JobYamlTask`. `CRUD::validate_job_tasks` rejects the job at startup if a task id is declared twice, if a `depends_on` id is not a task of the same job, or if the dependencies form a cycle — `TaskRunDispatcher` waits for every dependency to succeed, so any of those would leave the task runs pending and their job run running forever.

Each task's dependency list is written to **two** places from the same source: `depends_on` here, and one normalized row per edge in [`task_dependent`](task_dependent.md).

## Read by

- `CRUD::submit_job` — copies `command`, `depends_on`, `timeout`, `max_retries`, `retry_delay`, `env` and `working_dir` onto every `task_run` it inserts. This is the snapshot the orchestrator then runs on. `description` is not copied: a run snapshots what it executes, and prose is not that.
- The jobs and job-detail web routes, including the DAG ([src/router/app/routes/jobs/job_id/dag.rs](../../../../src/router/app/routes/jobs/job_id/dag.rs)), which describe the job **as defined now** rather than any run of it. The job page's task table lists `description`; the task page, `/jobs/{job_id}/tasks/{task_id}` ([src/router/app/routes/jobs/job_id/task_id/route.rs](../../../../src/router/app/routes/jobs/job_id/task_id/route.rs)), is the only place `command`, `env` and `working_dir` are shown as declared.

**No orchestrator file reads this table.** Every config field a service acts on comes off `task_run`. Editing the YAML changes what future runs are submitted with and nothing about the runs already in flight — and equally, a wrong value is frozen onto every run submitted after the edit rather than fixable by editing the YAML back.
