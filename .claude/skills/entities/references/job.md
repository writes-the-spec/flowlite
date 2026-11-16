# `Job` (Declared)

One per job YAML file under `<data_dir>/jobs/*.yml` — the *definition*; an execution is a [`job_run`](job_run.md). A struct in `Declared` ([src/declared/declared.rs](../../../../src/declared/declared.rs)), not a table: loaded from the YAML by every process that needs it.

| Field | Meaning |
|---|---|
| `job_id` | The YAML `id:`. Unique across files, refused at load otherwise. |
| `name` | Display label. Deliberately **not** unique and **not** a key. |
| `description` | serde defaults it to `""`. |
| `max_parallel_runs` | How many of this job's runs may be `Running` at once. Defaults to 1; **`0` means no limit** and skips the count. |
| `keep_runs` | How many of this job's newest finished runs retention keeps. Defaults to `[job_defaults] keep_runs`; **`0` keeps every run**, leaving only `[retention] keep_runs_total` to bound it. |
| `parameters` | Declared name → default, empty if none. Only the declaration; `submit_job` resolves overrides. |
| `env` | Empty if none. Merged under each task's `env:` by `submit_job` (task wins); only the merge is stored, on `task_run.env`. |
| `secret_env` | Empty if none. Variable name → secret name, never a value. Merged like `env`, stored on `task_run.secret_env`. |
| `on_failure_recipients` | Channel → recipients from `on_failure:`, empty if nobody; a channel naming nobody is **absent**, not `[]`. Built by `job_notify_recipients`. A recipient on a channel `flowlite.toml` does not configure fails `Declared::load`. |
| `on_success_recipients` | Same, from `on_success:`. Separate because the two are read independently and often only one is declared. |
| `limits` | Named concurrency limits, empty if none; `Declared::load` refuses a name `[concurrency_limits]` does not have. Every task claims these on top of its own. |

## Written by

`Declared::add_job` only, from `JobYaml` — see the [yaml skill](../../yaml/references/job_yaml.md). Never changed after.

## Read by

- `CRUD::submit_job` ([src/crud/multistatements/submit_job.rs](../../../../src/crud/multistatements/submit_job.rs)):
  - copies `name`/`description` onto the `job_run`; bails `Job '<id>' not found` if `Declared` has no such job;
  - `resolve_job_parameters` raises on an override `parameters` does not declare;
  - `merge_job_and_task_maps` layers `env`/`secret_env` under the task's, task winning; a task's name in either block also evicts the job's value from the other — see [task_run.md](task_run.md);
  - the recipient columns become [`job_run_notification`](job_run_notification.md) rows, **one per channel per block**. Frozen at submit, so a run stays notifiable after its YAML changes and a rerun tells whoever the original would have.
- `CRUD::is_job_at_max_parallel_runs` — `max_parallel_runs`, **the only field the orchestrator reads live** ("may I start another run?" is about the job now).
- `RetentionService` ([src/retention/service.rs](../../../../src/retention/service.rs)) — `keep_runs`; a job id `Declared` does not hold (ad-hoc, or YAML deleted) falls back to `[job_defaults] keep_runs`.
- `job list` / `job submit` ([src/cli/commands/job.rs](../../../../src/cli/commands/job.rs)) and the home, jobs and job-detail web routes.

## Gotchas

- **`job submit <arg>` matches `job_id`, not `name`**, despite the argument's name.
- `max_parallel_runs` is enforced only in `JobRunDispatcher::is_job_at_max_parallel_runs`; nothing rejects a submission over it — the run waits in `Queued`. See the [orchestrator skill](../../orchestrator/references/job_run.md).
- **A run whose job left the config is never gated**: a missing job means no limit, so it starts rather than queueing forever.
