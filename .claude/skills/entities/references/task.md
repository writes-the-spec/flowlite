# `task` (mem)

One row per task under a job YAML's `tasks:` — a *definition*, like [`job`](job.md). State lives on [`task_run`](task_run.md) and [`task_run_attempt`](task_run_attempt.md).

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. |
| `task_id`, `job_id` | **Composite primary key** — task ids are unique per job. `job_id` is a foreign key to [`job`](job.md). |
| `description` | `NOT NULL`, `''` if none. Prose; never read by anything that runs. |
| `command` | The shell command, run as `sh -c <command>`. |
| `stdin` | `NOT NULL`; `''` spawns against `/dev/null`. Otherwise written verbatim and the pipe closed — no shell expands it, so it suits a prompt or document. |
| `depends_on` | `NOT NULL` JSON array of task ids in the same job, `'[]'` if none. |
| `limits` | `NOT NULL` JSON array of named concurrency limits, added to the job's, `'[]'` if none; not validated here. |
| `timeout` | Seconds. Defaults to 3600 in the YAML, not the DDL. |
| `idle_timeout` | Seconds without output before an attempt is timed out, 0 for none. Defaults from `[job_defaults] idle_timeout_seconds`. |
| `max_retries` | Retries *after* the first attempt: `1 + max_retries` executions. Defaults to 0. |
| `retry_delay` | Seconds before each retry. Defaults to 60. |
| `env` | `NOT NULL`, `'{}'` if none. Layered over what flowlite inherited. |
| `secret_env` | `NOT NULL`, `'{}'` if none. Variable name → secret name, never a value; layered over the job's like `env`. |
| `working_dir` | `NOT NULL`. `''` means the run's own `.flowlite/runs/<job run id>` under the data dir, created at start and deleted with the run; name a path for a fixed place. |

## Written by

`CRUD::init` only, from `JobYamlTask`, also writing each edge to [`task_dependent`](task_dependent.md). `CRUD::validate_job_tasks` rejects at startup a duplicate task id, a `depends_on` id outside the job, or a cycle — each would leave the job run running forever.

## Read by

- `CRUD::submit_job` — copies `command`, `stdin`, `depends_on`, `timeout`, `idle_timeout`, `max_retries`, `retry_delay`, `env`, `secret_env`, `working_dir` and `limits` onto every `task_run` (`env`, `secret_env` and `limits` merged with the job's). `description` is not copied: a run snapshots what it executes.
- The jobs and job-detail web routes and DAG ([src/router/app/routes/jobs/job_id/dag.rs](../../../../src/router/app/routes/jobs/job_id/dag.rs)), showing the job **as defined now**; the job page lists `description`. Only the task page, `/jobs/{job_id}/tasks/{task_id}` ([src/router/app/routes/jobs/job_id/task_id/route.rs](../../../../src/router/app/routes/jobs/job_id/task_id/route.rs)), shows `command`, `stdin` (when declared), `env`, `secret_env` (the reference only; no secret is read) and `working_dir` as declared.

**No orchestrator file reads this table.** A YAML edit affects future runs only; a wrong value stays frozen on runs submitted while it was there.
