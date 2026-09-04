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
```

## `JobYaml`

| Field | Required | Default | Notes |
|---|---|---|---|
| `id` | yes | — | Primary key of `mem.job`. The id CLI and API lookups filter on — **not** `name`. |
| `name` | yes | — | Display label only, not unique. |
| `description` | no | `""` | |
| `tasks` | no | `[]` | A job with no tasks is legal; its job runs finish `Succeeded` immediately. |

## `JobYamlTask`

| Field | Required | Default | Notes |
|---|---|---|---|
| `id` | yes | — | Unique within the job — `mem.task`'s key is `(task_id, job_id)`. |
| `command` | yes | — | Run as `sh -c <command>`, so shell syntax works. |
| `depends_on` | no | `[]` | Task ids **of the same job**. |
| `timeout` | no | `3600` | Seconds. Applies per *attempt*, not to the task run as a whole. |
| `max_retries` | no | `0` | Total executions are `1 + max_retries`; only a `Failed` attempt is retried. |

## What one task becomes

`CRUD::init` writes each task's `depends_on` to **two** places from the same list: `task.depends_on` as a JSON array on the task row, and one normalized `task_dependent` row per edge. `task.depends_on` is the list a run's dependency graph is built *from*, not the one the runtime resolves — `CRUD::submit_job` copies it onto the run's own rows, and `TaskRunDispatcher::get_dependent_task_runs` resolves that copy (`task_run.depends_on`). `task_dependent` is read by nothing. See the [task skill](../../task/SKILL.md).

Submitting the job then creates one `task_run` per task, all `Pending`, each carrying a snapshot of its task's `command`, `depends_on`, `timeout` and retry settings — so the run executes the definition it was submitted with, whatever the YAML says later. Dependency order is enforced at dispatch time, not at submission. See the [orchestrator skill](../../orchestrator/references/task_run.md).

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
