# `task` (mem)

One row per task of a job, declared inline under the job YAML's `tasks:` list. Like [`job`](job.md) this is a *definition*; what executes and carries state is a [`task_run`](task_run.md), and each execution of its command is a [`task_run_attempt`](task_run_attempt.md).

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. |
| `task_id`, `job_id` | **Composite primary key** — a task id is unique per job, not globally. `job_id` is a foreign key to [`job`](job.md). |
| `command` | The shell command, run as `sh -c <command>`. |
| `depends_on` | JSON array of task ids in the same job. `NOT NULL`; a task with no dependencies stores `'[]'`. |
| `timeout` | Seconds. Defaults to 3600 in the YAML, not in the DDL. |
| `max_retries` | Retries *after* the first attempt, so executions total `1 + max_retries`. Defaults to 0. |
| `retry_delay` | Seconds to wait before each retry. Defaults to 60. |

## Written by

`CRUD::init` only, from `JobYamlTask`. `CRUD::validate_job_tasks` rejects the job at startup if a task id is declared twice, if a `depends_on` id is not a task of the same job, or if the dependencies form a cycle — `TaskRunDispatcher` waits for every dependency to succeed, so any of those would leave the task runs pending and their job run running forever.

Each task's dependency list is written to **two** places from the same source: `depends_on` here, and one normalized row per edge in [`task_dependent`](task_dependent.md).

## Read by

- `CRUD::submit_job` — copies `command`, `depends_on`, `timeout`, `max_retries` and `retry_delay` onto every `task_run` it inserts. This is the snapshot the orchestrator then runs on.
- The jobs and job-detail web routes, including the DAG ([src/router/app/routes/jobs/job_id/dag.rs](../../../../src/router/app/routes/jobs/job_id/dag.rs)), which describe the job **as defined now** rather than any run of it.

**No orchestrator file reads this table.** Every config field a service acts on comes off `task_run`. Editing the YAML changes what future runs are submitted with and nothing about the runs already in flight — and equally, a wrong value is frozen onto every run submitted after the edit rather than fixable by editing the YAML back.
