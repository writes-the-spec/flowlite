# ScheduleYaml

[src/yaml_models/schedule_yaml.rs](../../../../src/yaml_models/schedule_yaml.rs) — one file per schedule under `<data_dir>/schedules/*.yml`, seeding `mem.schedule` and `mem.schedule_job`.

```yaml
id: nightly
name: Nightly
cron: "0 0 3 * * *"
timezone: Europe/Vienna
start_date: 2026-01-01
end_date: 2026-12-31
submit_ahead: 1
jobs:
  - id: my-job
```

## Fields

| Field | Default | Notes |
|---|---|---|
| `id` | required | `mem.schedule` primary key. |
| `name` | required | Display label. |
| `description` | `""` | |
| `cron` | required | Parsed to `cron::Schedule` at load; invalid fails startup naming the file. |
| `timezone` | `[schedule_defaults]`, `UTC` | `chrono_tz::Tz` name the cron is evaluated in; `Option`, filled by `CRUD::init`. |
| `start_date` | none | Date; earlier occurrences are pulled forward to it. |
| `end_date` | none | Date, inclusive to `23:59:59`; past it the schedule never fires. |
| `disabled` | `false` | Desires zero occurrences. |
| `submit_ahead` | `1` | Occurrences kept submitted ahead. `0` refused (`#[validate(range(min = 1))]`) — use `disabled: true`. Default on the model, not config: it is per schedule. |
| `jobs[].id` | required | Job id; a `mem.schedule_job` foreign key checks it. **Once per schedule** (`no_duplicate_job_ids`) — a second entry would submit a duplicate run every occurrence. |
| `jobs[].parameters` | `{}` | Overrides of the job's declared `parameters`, coerced as in [job_yaml.md](job_yaml.md#what-a-scalar-becomes). |

## Gotchas

- **`cron` has six fields, seconds first** (`sec min hour dom month dow`). `"*/15 * * * * *"` is every 15 *seconds*.
- **An undeclared parameter name fails at submit, not startup.** `Scheduler::submit_if_missing` calls `CRUD::submit_job`, which raises; the scheduler `eprintln!`s and moves on, so that job fails every occurrence until fixed. An unknown *key* is silently dropped.
- **An unknown job id stops startup.** `CRUD::init` inserts jobs first, then `schedule_job` hits `(code: 787) FOREIGN KEY constraint failed` (`PRAGMA foreign_keys` is on by sqlx default); the transaction aborts, so `serve`, `job list` and `job submit` all fail until fixed.

## What the scheduler does with it

The next run isn't stored (the dashboard derives it). Each pass, `Scheduler` takes every schedule, `disabled` or not, computes its next `submit_ahead` occurrences and submits a run for any that has none. **It only adds.** Disabled or past `end_date`, `CronTrigger::get_next_runs` yields nothing — and a run already written stays `Scheduled` and runs. To call it off: `flowlite job-run stop <id>` (the row keeps the occurrence, so it isn't rewritten) or `job-run delete <id>` (frees it, so a schedule still wanting it writes it again). Deciding a run is *due* belongs to [`JobRunReleaser`](../../orchestrator/SKILL.md). See the [scheduler skill](../../scheduler/SKILL.md).
