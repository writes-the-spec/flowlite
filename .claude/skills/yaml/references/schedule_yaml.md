# ScheduleYaml

[src/yaml_models/schedule_yaml.rs](../../../../src/yaml_models/schedule_yaml.rs) — one file per schedule under `<config_dir>/schedules/*.yml`, seeding `mem.schedule` and `mem.schedule_job`.

```yaml
id: nightly
name: Nightly
description: runs the pipeline every night
cron: "0 0 3 * * *"
timezone: Europe/Vienna
start_date: 2026-01-01
end_date: 2026-12-31
disabled: false
jobs:
  - id: my-job
```

## `ScheduleYaml`

| Field | Required | Default | Notes |
|---|---|---|---|
| `id` | yes | — | Primary key of `mem.schedule`. |
| `name` | yes | — | Display label only. |
| `description` | no | `""` | |
| `cron` | yes | — | Parsed into a `cron::Schedule` at load, so an invalid expression fails startup naming the file. |
| `timezone` | no | `UTC` | A `chrono_tz::Tz` name, e.g. `Europe/Vienna`. The cron expression is evaluated in it. |
| `start_date` | no | `None` | Date only. Occurrences before it are pulled forward to it. |
| `end_date` | no | `None` | Date only, inclusive to `23:59:59`. Past it the schedule stops firing for good. |
| `disabled` | no | `false` | A disabled schedule is filtered out of the scheduler's poll. |
| `jobs` | no | `[]` | The jobs submitted on each occurrence. |

`start_date` and `end_date` are `Option`, so omitting them is fine even without `#[serde(default)]`.

## `ScheduleYamlJob`

| Field | Required | Default | Notes |
|---|---|---|---|
| `id` | yes | — | A job id. A foreign key on `mem.schedule_job` checks it exists — see below. |
| `parameters` | no | `None` | Arbitrary JSON, stored as text in `mem.schedule_job` and **never read**. |

## Gotchas

- **`cron` takes six fields, seconds first** (`sec min hour dom month dow`), the `cron` crate's dialect. A five-field crontab expression means something else here — `"*/15 * * * * *"` is every 15 *seconds*.
- **`parameters` is inert.** `CRUD::submit_job` takes only a job id, so nothing declared per job on a schedule reaches the job run. `.config/schedules/example_schedule.yml` uses a `variables:` key that isn't even a field — unknown keys are dropped silently.
- **An unknown job id stops the server from starting.** `mem.schedule_job.job_id` is a foreign key to `mem.job` and `PRAGMA foreign_keys` is on (sqlx enables it by default), so `CRUD::init` — which inserts every job before any `schedule_job` — aborts the config transaction with `(code: 787) FOREIGN KEY constraint failed` and the process exits. There is no half-loaded config and no stuck job run: a typo in one schedule file stops `serve`, `job list` and `job submit` until it is fixed.

## What the scheduler does with it

`CRUD::init` computes the first `next_run` at load (`CronTrigger::get_next_run(None)` — the next occurrence after *now*, so restarts never backfill). From there `Scheduler` polls schedules with `next_run < now` and `disabled = false`, submits their jobs, and advances `next_run` from the schedule's own previous value. Past `end_date` that write is `NULL`, which retires the schedule for the rest of the process's life. See the [scheduler skill](../../scheduler/SKILL.md).
