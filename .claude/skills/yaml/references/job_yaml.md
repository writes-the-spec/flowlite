# JobYaml

[src/yaml_models/job_yaml.rs](../../../../src/yaml_models/job_yaml.rs) — one file per job under `<data_dir>/jobs/*.yml`, seeding `mem.job`, `mem.task`, `mem.task_dependent`.

```yaml
id: my-job
name: My Job
tasks:
  - id: task-a
    command: echo hello
  - id: task-b
    command: ./run.sh
    depends_on: [task-a]
    timeout: 600
    max_retries: 2
on_failure:
  email: [oncall@example.com]
  slack: ["#oncall"]
on_success:
  slack: ["#data"]
```

## `JobYaml`

"`[job_defaults]`, N": `Option` on the model, filled by `CRUD::init` from config.toml, else N.

| Field | Default | Notes |
|---|---|---|
| `id` | required | `mem.job` primary key; what lookups filter on — **not** `name`. |
| `name` | required | Display label, not unique. |
| `description` | `""` | |
| `max_parallel_runs` | `[job_defaults]`, `1` | **`0` = no limit.** Enforced only by `JobRunDispatcher::is_job_at_max_parallel_runs` holding the run `Queued`; submit never rejects. |
| `keep_runs` | `[job_defaults]`, `100` | Newest finished runs retention keeps; **`0` keeps all** (only `[retention] keep_runs_total` bounds it). Read by `RetentionService` — [entities](../../entities/references/job.md). |
| `parameters` | `{}` | Name → default. A schedule or `job submit --param` may override a declared name; an undeclared one is a submit error. [entities](../../entities/references/job.md). |
| `env` | `{}` | For **every** task; see [which env wins](#which-env-wins). |
| `secret_env` | `{}` | Variable name → `[secrets]` name. Only the name travels (row, dashboard, `--json`); the value is resolved at spawn. |
| `on_failure` | `{}` | `JobYamlNotify`: `email` (addresses), `slack` (`#channel`, channel id or user id). Sent on `Failed`/`TimedOut`, never `Aborted`. One notification per channel with recipients. **A recipient on a channel config.toml doesn't configure fails startup** (`CRUD::validate_job_notifications`) — a notification that never leaves is invisible afterwards. Fields per channel, not a map, as `NotificationChannel` is an enum ([notifications skill](../../notifications/SKILL.md)). |
| `on_success` | `{}` | Same shape and check, for `Succeeded`; addressed independently (failure wakes on-call, success tells whoever waits on the data). A run's ending delivers exactly one of the two. |
| `limits` | `[]` | `[concurrency_limits]` names, claimed by every task on top of its own. Unknown name fails startup (`CRUD::validate_job_limits`). |
| `tasks` | `[]` | No tasks is legal; the run finishes `Succeeded` at once. |

## `JobYamlTask`

| Field | Default | Notes |
|---|---|---|
| `id` | required | Unique within the job (`mem.task` key `(task_id, job_id)`); ASCII letters, digits, `-`, `_`. |
| `description` | `""` | Shown in the job page's task table instead of the command. |
| `command` | required | Run as `sh -c <command>`. |
| `stdin` | `""` | Fed to stdin verbatim, no shell expansion; empty means null stdin. |
| `depends_on` | `[]` | Task ids **of the same job**. |
| `limits` | `[]` | On top of the job's; same check. |
| `timeout` | `[job_defaults]`, `3600` | Seconds, per *attempt*. |
| `max_retries` | `[job_defaults]`, `0` | `1 + max_retries` executions; only `Failed` retries. |
| `retry_delay` | `[job_defaults]`, `60` | Seconds, enforced by `TaskRunAttemptDispatcher::should_stay_queued` from the retry row's `created_at`. |
| `env`, `secret_env` | `{}` | Over the job's. Task `env` is shown on `/jobs/{job_id}/tasks/{task_id}`; the job's `env` nowhere in the UI. |
| `working_dir` | `""` | Empty inherits the server's. |

## Which env wins

```
flowlite's own environment (FLOWLITE_* stripped)
  ← env:        (job's, overridden by task's)
  ← secret_env: resolved values (job's, overridden by task's)
  ← FLOWLITE_PARAM_* from resolved parameters
  ← injected FLOWLITE_* run metadata
```

Job and task maps merge at submit into `task_run.env` / `task_run.secret_env`, so a rerun replays them; a task's name wins across blocks too (task `env: X` drops the job's `secret_env: X`, and vice versa). `build_task_run_attempt_env` applies the rest at spawn — secrets after `env:` so a plain value can't shadow a credential, metadata last so a command can't be misled about its run. Redeclaring a name is the only way to override the job's.

## What a scalar becomes

`env`, `secret_env`, `parameters` and `ScheduleYamlJob.parameters` share `deserialize_string_map` ([src/yaml_models/string_map.rs](../../../../src/yaml_models/string_map.rs)):

| YAML value | Result |
|---|---|
| String | As written. |
| Number / bool | YAML 1.2 form, not source text: `1.10` → `"1.1"`, `1e3` → `"1000.0"`, `0x1F` → `"31"`; `yes`, `007` stay as written. Quote to keep literal. |
| Map / list | `'<key>' is a map, but only a string, a number or a boolean can reach a command` |
| Bare `key:` | `'<key>' has no value - write "" for an empty one` |

## What one task becomes

`CRUD::init` writes `depends_on` both as JSON on `task.depends_on` and as `task_dependent` rows. `submit_job` copies `task.depends_on` onto the run, and `TaskRunDispatcher::get_dependent_task_runs` resolves `task_run.depends_on`; **`task_dependent` is read by nothing** ([task](../../entities/references/task.md), [task_dependent](../../entities/references/task_dependent.md)).

Submit creates one `Queued` `task_run` per task, snapshotting `command`, `stdin`, `depends_on`, `timeout`, retry settings, `env`, `secret_env`, `working_dir` — later YAML edits don't affect it. Parameters resolve once per job run. Dependency order is enforced at dispatch ([orchestrator skill](../../orchestrator/references/task_run.md)).

## Validation

At parse, in `from_yaml_str` (file-only fields, so checked once here rather than on every read):

| Rule | Why |
|---|---|
| Task id: ASCII letters, digits, `-`, `_` | Path component (`.output/<id>.<attempt>`) and part of `FLOWLITE_INPUT_<ID>`. |
| No two ids map to one `FLOWLITE_INPUT_*` (`load-raw`/`load_raw`) | A dependent would get one path for two results. |
| `secret_env` variable: valid env name, not `FLOWLITE_*` | Metadata is applied last and would overwrite it. |
| `env` variable: not `FLOWLITE_*` (`validate_env_names`) | The prefix is flowlite's; the spawn drops any such name from `env:`, so it would never arrive. |
| Secret name: lowercase, digits, `_`, no `__` | `FLOWLITE_SECRETS__*` can't carry others; `__` splits into nested keys. |
| A name not in both `env` and `secret_env` at one level | Per level, so a task may still override a job's name across blocks. |

In `CRUD::seed_job`, before any insert, `validate_job_tasks` fails startup on:

| Case | Message |
|---|---|
| Duplicate task id | `Task 'a' is declared more than once` |
| Self-dependency | `Task 'a' depends on itself` |
| Unknown dependency | `Task 'a' depends on 'nope', which is not a task of this job` |
| Cycle | `Tasks depend on each other in a cycle: a -> c -> b -> a` |

Each would leave task runs `Queued` and the job run `Running` forever, since `TaskRunDispatcher` waits for every dependency to succeed.

Not checked: the same job `id` in two files fails only on the `mem.job` primary key (`UNIQUE constraint failed`, wrapped with the file name).
