# JobYaml

[src/yaml_models/job_yaml.rs](../../../../src/yaml_models/job_yaml.rs) — one file per job under `<config_dir>/jobs/*.yml`, seeding `mem.job`, `mem.task` and `mem.task_dependent`.

```yaml
id: my-job
name: My Job
description: what it does
tasks:
  - id: task-a
    command: echo hello
  - id: task-b
    command: ./run.sh
    depends_on: [task-a]
    timeout: 600
    max_retries: 2
    retry_delay: 30
```

## `JobYaml`

| Field | Required | Default | Notes |
|---|---|---|---|
| `id` | yes | — | Primary key of `mem.job`. The id CLI and API lookups filter on — **not** `name`. |
| `name` | yes | — | Display label only, not unique. |
| `description` | no | `""` | |
| `max_parallel_runs` | no | `1` | How many runs of this job may be `Running` at once. **`0` means no limit.** Enforced only in `JobRunDispatcher::settle_as_pending`; submitting is never rejected for exceeding it. |
| `parameters` | no | `{}` | Declared name to default value. A schedule's `jobs[].parameters` or `job submit --param` may override a declared name; naming one this job does not declare is a submit error, not a silent no-op. See the [entities skill](../../entities/references/job.md). |
| `tasks` | no | `[]` | A job with no tasks is legal; its job runs finish `Succeeded` immediately. |

## `JobYamlTask`

| Field | Required | Default | Notes |
|---|---|---|---|
| `id` | yes | — | Unique within the job — `mem.task`'s key is `(task_id, job_id)`. |
| `command` | yes | — | Run as `sh -c <command>`, so shell syntax works. |
| `depends_on` | no | `[]` | Task ids **of the same job**. |
| `timeout` | no | `3600` | Seconds. Applies per *attempt*, not to the task run as a whole. |
| `max_retries` | no | `0` | Total executions are `1 + max_retries`; only a `Failed` attempt is retried. |
| `retry_delay` | no | `60` | Seconds to wait after a failed attempt before the next one starts. Enforced in `TaskRunAttemptDispatcher::settle_as_pending`, measured from the retry row's `created_at`. |
| `env` | no | `{}` | Environment variables for the command, layered over whatever flowlite itself inherited. Shown in the UI on purpose — see the gotcha below. |
| `working_dir` | no | `""` | The command's working directory. Empty means inherit the server's own. |

## `parameters` and `env`: what a scalar becomes

Both fields, and `ScheduleYamlJob.parameters` in the [schedule YAML](schedule_yaml.md), share one deserializer, `deserialize_string_map` ([src/yaml_models/string_map.rs](../../../../src/yaml_models/string_map.rs)), because every one of them has to end up as a string an environment variable can carry:

| YAML value | Result |
|---|---|
| A string | Kept as written. |
| A number or boolean | Coerced to its string form — `retries: 3` becomes `"3"`. |
| A nested map or list | Rejected, naming the key: `'<key>' is a map, but only a string, a number or a boolean can reach a command`. |
| A bare `key:` (YAML null) | Rejected: `'<key>' has no value - write "" for an empty one`. A forgotten value and a deliberate empty one are not the same mistake, so the error says how to write the one that's legal. |

## What one task becomes

`CRUD::init` writes each task's `depends_on` to **two** places from the same list: `task.depends_on` as a JSON array on the task row, and one normalized `task_dependent` row per edge. `task.depends_on` is the list a run's dependency graph is built *from*, not the one the runtime resolves — `CRUD::submit_job` copies it onto the run's own rows, and `TaskRunDispatcher::get_dependent_task_runs` resolves that copy (`task_run.depends_on`). `task_dependent` is read by nothing. See [task](../../entities/references/task.md) and [task_dependent](../../entities/references/task_dependent.md) in the entities skill.

Submitting the job then creates one `task_run` per task, all `Pending`, each carrying a snapshot of its task's `command`, `depends_on`, `timeout`, retry settings, `env` and `working_dir` — so the run executes the definition it was submitted with, whatever the YAML says later. `job.parameters` goes through the same resolution the `job_run` above it does, not a per-task copy: there is one resolved set per run, not one per task. Dependency order is enforced at dispatch time, not at submission. See the [orchestrator skill](../../orchestrator/references/task_run.md).

## Validation

`CRUD::validate_job_tasks` runs right after `from_yaml`, before any insert, and rejects the whole startup on:

| | Message |
|---|---|
| A task id declared twice | `Task 'a' is declared more than once` |
| A task depending on itself | `Task 'a' depends on itself` |
| A `depends_on` id that is not a task of this job | `Task 'a' depends on 'nope', which is not a task of this job` |
| A cycle | `Tasks depend on each other in a cycle: a -> c -> b -> a` |

It lives in `CRUD::init` rather than on the model because each rule needs the whole task list, and it exists because `TaskRunDispatcher` waits for every dependency to succeed — any of the four would leave the task runs `Pending` and their job run `Running` forever.

Note what is *not* checked: a duplicate `id` across two job files is caught only by the `mem.job` primary key, as a `UNIQUE constraint failed` wrapped in the file name.
