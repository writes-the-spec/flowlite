# `job` (mem)

One row per job YAML file under `<data_dir>/jobs/*.yml` — the *definition* of a job, never one of its executions. The execution is [`job_run`](job_run.md), and the two are not interchangeable: a `job` is the template, a `job_run` is one run of it.

In-memory config, re-seeded on every startup, so nothing here survives a restart independently of the YAML.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`, not the primary key. |
| `job_id` | **Primary key.** The `id:` from the YAML. |
| `name` | Display label. Deliberately **not** unique and **not** a key. |
| `description` | `NOT NULL`; the YAML `#[serde(default)]`s it to `""`, so the empty string arrives as a value. |
| `max_parallel_runs` | How many of this job's runs may be `Running` at once. Defaults to 1; **`0` means no limit** and short-circuits the count entirely. |
| `parameters` | `NOT NULL`. Declared name to default value, `'{}'` when the job declares none. This table only holds the declaration — resolving it against a caller's overrides happens in `submit_job`, not here. |
| `env` | `NOT NULL`. Environment variables for every task of the job, `'{}'` when it declares none. Merged with each task's own `env:` by `submit_job` — the task wins a shared name — and only the merged result is stored, on `task_run.env`. |

## Written by

`CRUD::init` ([src/crud/crud.rs](../../../../src/crud/crud.rs)) only, inside the config transaction, from `JobYaml` — see the [yaml skill](../../yaml/references/job_yaml.md). Never updated.

## Read by

- `CRUD::submit_job` ([src/crud/multistatements/misc.rs](../../../../src/crud/multistatements/misc.rs)) — copies `name` and `description` onto the `job_run` it inserts as `job_name`/`job_description`, and bails with `Job '<id>' not found` if the row is missing. It also passes `parameters` to `resolve_job_parameters`, which raises if a caller's override names a parameter this row does not declare, and merges `env` under each task's own `env:` via `merge_task_env`.
- `CRUD::is_job_at_max_parallel_runs` — reads `max_parallel_runs`. **This is the single definition field the orchestrator reads live**, everywhere else it reads the run's snapshot. Deliberate: "may I start another run?" is a question about the job now, so it is not frozen onto `job_run`. See [task_run.md](task_run.md).
- `job list` / `job submit` ([src/cli/commands/job.rs](../../../../src/cli/commands/job.rs)) and the home, jobs and job-detail web routes.

## Gotchas

- **`job submit <arg>` matches `job_id`, not `name`,** despite the argument's name. Don't copy the CLI arg naming as a model without checking which column it filters on.
- `max_parallel_runs` is enforced in exactly one place, `JobRunDispatcher::settle_as_pending`. Nothing rejects a submission for being over it — the run is created `Pending` and queues. See the [orchestrator skill](../../orchestrator/references/job_run.md).
- **A job run whose job is no longer in the config is never gated.** `is_job_at_max_parallel_runs` returns `false` when the row is missing, so a run left over from a deleted or renamed job starts on the next pass rather than queueing forever.
