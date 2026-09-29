# `job` (mem)

One row per job YAML file under `<data_dir>/jobs/*.yml` — the *definition*; an execution is a [`job_run`](job_run.md). Re-seeded every startup.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`, not the primary key. |
| `job_id` | **Primary key.** The YAML `id:`. |
| `name` | Display label. Deliberately **not** unique and **not** a key. |
| `description` | `NOT NULL`; serde defaults it to `""`. |
| `max_parallel_runs` | How many of this job's runs may be `Running` at once. Defaults to 1; **`0` means no limit** and skips the count. |
| `keep_runs` | How many of this job's newest finished runs retention keeps. Defaults to `[job_defaults] keep_runs`; **`0` keeps every run**, leaving only `[retention] keep_runs_total` to bound it. |
| `parameters` | `NOT NULL`. Declared name → default, `'{}'` if none. Only the declaration; `submit_job` resolves overrides. |
| `env` | `NOT NULL`, `'{}'` if none. Merged under each task's `env:` by `submit_job` (task wins); only the merge is stored, on `task_run.env`. |
| `secret_env` | `NOT NULL`, `'{}'` if none. Variable name → secret name, never a value. Merged like `env`, stored on `task_run.secret_env`. |
| `on_failure_recipients` | `NOT NULL`. `{"email": [...], "slack": [...]}` from `on_failure:`, `'{}'` if nobody; a channel naming nobody is **absent**, not `[]`. Built by `job_notify_recipients`. A recipient on a channel `config.toml` does not configure fails `CRUD::init`. |
| `on_success_recipients` | `NOT NULL`. Same, from `on_success:`. Separate because the two are read independently and often only one is declared. |
| `limits` | `NOT NULL` JSON array of named concurrency limits, `'[]'` if none; resolved against `[concurrency_limits]`, not validated here. Every task claims these on top of its own. |

## Written by

`CRUD::init` ([src/crud/crud.rs](../../../../src/crud/crud.rs)) only, inside the config transaction, from `JobYaml` — see the [yaml skill](../../yaml/references/job_yaml.md). Never updated.

## Read by

- `CRUD::submit_job` ([src/crud/multistatements/submit_job.rs](../../../../src/crud/multistatements/submit_job.rs)):
  - copies `name`/`description` onto the `job_run`; bails `Job '<id>' not found` if the row is missing;
  - `resolve_job_parameters` raises on an override `parameters` does not declare;
  - `merge_job_and_task_maps` layers `env`/`secret_env` under the task's, task winning; a task's name in either block also evicts the job's value from the other — see [task_run.md](task_run.md);
  - the recipient columns become [`job_run_notification`](job_run_notification.md) rows, **one per channel per block**. Frozen at submit, so a run stays notifiable after its YAML changes and a rerun tells whoever the original would have.
- `CRUD::is_job_at_max_parallel_runs` — `max_parallel_runs`, **the only field the orchestrator reads live** ("may I start another run?" is about the job now).
- `RetentionService` ([src/retention/service.rs](../../../../src/retention/service.rs)) — `keep_runs`; a job id with no row here (ad-hoc, or YAML deleted) falls back to `[job_defaults] keep_runs`.
- `job list` / `job submit` ([src/cli/commands/job.rs](../../../../src/cli/commands/job.rs)) and the home, jobs and job-detail web routes.

## Gotchas

- **`job submit <arg>` matches `job_id`, not `name`**, despite the argument's name.
- `max_parallel_runs` is enforced only in `JobRunDispatcher::is_job_at_max_parallel_runs`; nothing rejects a submission over it — the run waits in `Queued`. See the [orchestrator skill](../../orchestrator/references/job_run.md).
- **A run whose job left the config is never gated**: a missing row means no limit, so it starts rather than queueing forever.
