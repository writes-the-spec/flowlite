---
name: scheduler
description: The Scheduler service (src/scheduler/scheduler.rs) - how a schedule YAML becomes recurring job runs, how next_run advances, and why the scheduler is separate from the orchestrator. Use when working with Schedule, ScheduleJob or CronTrigger, or when a schedule fires too often, not at all, or at the wrong time.
---

# Scheduler

The **Scheduler** ([src/scheduler/scheduler.rs](../../../src/scheduler/scheduler.rs)) is the one service that creates job runs nobody asked for by hand. It polls the due schedules once a second and submits their jobs.

It is **not part of the [orchestrator](../orchestrator/SKILL.md)**. `ServeCmd::run` ([src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs)) starts the two side by side, and they share no state: the scheduler's entire output is a `CRUD::submit_job` call, and everything after that — dispatching, executing, retrying, finishing — is the orchestrator's business. Keep it that way: the scheduler must never read a job run's status or touch a task run.

## Definition (YAML → in-memory `mem.schedule` + `mem.schedule_job`)

A schedule is a YAML file under `<config_dir>/schedules/*.yml`, parsed by `ScheduleYaml` ([src/yaml_models/schedule_yaml.rs](../../../src/yaml_models/schedule_yaml.rs)):

```yaml
id: nightly
name: Nightly
cron: "0 0 3 * * *"     # 6 fields, seconds first
timezone: Europe/Vienna # defaults to UTC
start_date: 2026-01-01  # optional
end_date: 2026-12-31    # optional
disabled: false
jobs:
  - id: my-job
```

`CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)) inserts one `mem.schedule` row plus one `mem.schedule_job` row per listed job, and computes the first `next_run` right there with `CronTrigger::get_next_run(None)`. Both tables are in-memory config data — see the [db-storage skill](../db-storage/SKILL.md).

## The loop

`Scheduler::run` selects, once a second, every schedule with `next_run < now` and `disabled = false`, ordered by `row_id`, and hands each to `handle_due_schedule`, which:

1. selects the schedule's `schedule_job` rows,
2. calls `CRUD::submit_job` for each — one `job_run` and its `Pending` task runs, exactly as `job submit` does,
3. advances the schedule: `CronTrigger::from_schedule(schedule).get_next_run(schedule.next_run)`.

Step 3 measures from the schedule's **own** `next_run`, not from now, so a tick that arrives late still advances by one cron step rather than skipping ahead.

A schedule that fails to be handled is logged and left for the next tick; only a failure to select the schedules restarts the loop after 5s. Same rule as the orchestrator services — see [orchestrator](../orchestrator/SKILL.md).

## `next_run` and the end date

`next_run` lives in the in-memory schema, so it is **recomputed from scratch on every startup** (`get_next_run(None)` = the next occurrence after *now*). Two consequences:

- **There is no catch-up.** Runs whose time passed while the process was down are simply not submitted, and a restart never backfills them.
- `next_run` is not a record of anything — the job runs are. Don't reach for it as history.

`CronTrigger::get_next_run` ([src/cron_trigger.rs](../../../src/cron_trigger.rs)) returns `None` past `end_date`, which writes `next_run = NULL`. The filter is `next_run < now`, and SQL comparisons against NULL are never true, so **a NULL `next_run` retires the schedule for the rest of the process's life** — that is how an expired schedule stops firing, and it is also what a bug that nulls `next_run` would look like.

`from_schedule` unwraps the parsed cron expression and timezone. That is safe only because `ScheduleYaml` deserializes them into `cron::Schedule` and `Tz` at load time, so an invalid one fails startup, not the loop. Don't build a `Schedule` row from anything that hasn't been through that.

## Gotchas

- **`cron` takes six fields, seconds first** (the `cron` crate's dialect), so a five-field expression copied from crontab silently means something else.
- **`schedule_job.parameters` is stored and never used.** `CRUD::submit_job` takes only a `job_id`, so anything declared per job on a schedule is inert until submission learns to carry it.
- **A schedule may reference a job that doesn't exist.** Nothing validates it, and `job_run.job_id` has no foreign key (it can't — the job lives in the attached in-memory database). `submit_job` happily inserts a `job_run` with no task runs, but `select_job_runs` inner-joins `mem.job` for the job's name, so that job run is invisible to `JobRunDispatcher` and to the UI, and sits `Pending` for good. It is submitted again on every tick the schedule is due.
