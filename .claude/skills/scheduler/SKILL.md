---
name: scheduler
description: The Scheduler service (src/scheduler/scheduler.rs) - the add-only pass that keeps each schedule's next submit_ahead occurrences submitted as job runs, and why deciding a run is due belongs to JobRunReleaser, not this service. Use when working with Schedule, ScheduleJob or CronTrigger, or when a schedule submits too many, too few, or the wrong runs.
---

# Scheduler

The **Scheduler** ([src/scheduler/scheduler.rs](../../../src/scheduler/scheduler.rs)) is the only service that creates job runs nobody asked for by hand. It is a **reconciler, not a clock**: on each pass, for every schedule, it submits a [`job_run`](../entities/references/job_run.md) for each of the next `submit_ahead` cron occurrences that doesn't have one yet.

## Boundaries

- **It only adds.** It never deletes a run, whether surplus or orphaned by a schedule removed from the YAML. A run the schedule no longer wants stays `Scheduled` and is released and executed like any other.
- **It never decides a run is due.** Moving `Scheduled` → `Queued` at `scheduled_at` is [`JobRunReleaser`](../orchestrator/SKILL.md)'s job alone. The Scheduler asks "which runs, dated when, should exist?" (cron and YAML). The releaser asks "has this run's moment come, or was it stopped?" (the clock, per row). Merging them would make one service read both schedule and run to answer either.
- **It shares no state with the orchestrator.** Its only output is `CRUD::submit_job`, and everything from `Scheduled` on is the orchestrator's. It never reads a task run, and it asks `job_run` only the existence check below.
- **Submitting is unconditional**, even while an earlier occurrence runs. `max_parallel_runs` is enforced only in `JobRunDispatcher::is_job_at_max_parallel_runs`, so an over-limit run is submitted and queues once released. Checking here would mean counting runs by status, the coupling ruled out above.
- **Wiring:** an `impl Service` ([src/poller.rs](../../../src/poller.rs)), not part of the [orchestrator](../orchestrator/SKILL.md), started by `ServeCmd::run` ([src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs)). It *publishes* to `Signals` ([src/signals.rs](../../../src/signals.rs)) after each submit but doesn't subscribe. Its `Poller` gets a standalone `Arc<Notify>`, so only the poll interval wakes it, because the reconcile follows the clock, not status writes.

## Definition (YAML → `mem.schedule` + `mem.schedule_job`)

`<data_dir>/schedules/*.yml`, parsed by `ScheduleYaml` ([src/yaml_models/schedule_yaml.rs](../../../src/yaml_models/schedule_yaml.rs)):

```yaml
id: nightly
name: Nightly
cron: "0 0 3 * * *"     # 6 fields, seconds first
timezone: Europe/Vienna # defaults to [schedule_defaults] in config.toml, itself UTC
start_date: 2026-01-01  # optional
end_date: 2026-12-31    # optional
disabled: false
submit_ahead: 1         # occurrences to keep submitted ahead of time; refused at 0
jobs:
  - id: my-job
```

`CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)) inserts one `mem.schedule` row and one `mem.schedule_job` row per listed job. Both are in-memory config tables; see [schedule](../entities/references/schedule.md) and [schedule_job](../entities/references/schedule_job.md).

## The reconcile

`select` fetches **every** schedule (enabled or not, bounded or not) ordered by `row_id`. `handle` reconciles each one:

1. **`CronTrigger::get_next_runs`** returns the next `submit_ahead` occurrences strictly after now. It returns nothing for a disabled schedule or one past `end_date`; that comes from the trigger's own bounds, so the reconcile has no branch for it.
2. **`schedule_jobs`** is read once per schedule per pass.
3. **`submit_if_missing`** runs for each (occurrence, job) pair. It looks up a job run by `schedule_id` + `job_id` + `scheduled_at` and, if none exists, calls `CRUD::submit_job` exactly as `job submit` does, with the occurrence as `scheduled_at` and the schedule's id as `schedule_id`. **A failed submit is logged and skipped**, so only that job loses that occurrence, and the next pass retries it.

**The existence check counts every status except `Deleted`** (`occupying_statuses()`), because the question is "has this occurrence been dealt with?". Counting only `Scheduled` would break cancelling: stopping a future-dated run is allowed (`src/shared/job_run.rs` only refuses a run that `is_finished()`), `JobRunReleaser` skips it straight away, and the row leaves `Scheduled` before its instant. The next pass would then resubmit it and the cancelled job would run anyway.

| Status | Effect on the occurrence |
|---|---|
| `Skipped` (user stopped it) | stays claimed forever |
| `Deleted` (`flowlite job-run delete <id>` on a still-`Scheduled` run, via `CRUD::delete_job_run`: a tombstone, not an erase) | freed, so the next pass submits it again from the job as the running server read it |
| anything else | claimed |

Any new `JobRunStatus` must pick a side in `occupying_statuses()`.

**The instant is matched in SQL, as stored.** Both sides come from `get_next_runs` (whole seconds, UTC), so they encode identically. A caller passing an instant from anywhere else can't rely on that. If equality across the SQLite round trip ever broke, the result would be a duplicate run on every pass, noticed only by someone reading the table.

**Stateless and idempotent.** A re-run with nothing changed submits nothing. There's no cursor or next-run column: each pass re-derives occurrences from the cron and the clock and asks `job_run` what exists. So there's **no catch-up** for occurrences missed while the process was down. The `job_run` rows are the history. The dashboard's "next run" is derived per request the same way, with `CronTrigger::get_next_run(None)`.

**Nothing is undone.** Lowering `submit_ahead`, editing the cron, dropping a job from `jobs:`, disabling the schedule or deleting the YAML all leave already-submitted future runs in place, and they will execute.

## Gotchas

- **Cron fields are read in the schedule's zone, so DST moves the UTC instant, not the local time.** `0 30 3 * * *` in `Europe/Vienna` is 02:30Z in winter and 01:30Z in summer. An hour skipped in spring loses that day's run (an 02:30 daily fires 364 times in 2026). An hour repeated in autumn fires once, on the first pass through it. Both are pinned by tests in [src/cron_trigger.rs](../../../src/cron_trigger.rs). For anything that must run daily, avoid 02:00-03:00.
- **`cron` takes six fields, seconds first** (the `cron` crate's dialect). A five-field crontab expression silently means something else.
- **`schedule_job.parameters` go to the command.** They are passed as `CRUD::submit_job` overrides, which rejects a name not in the job's declared `parameters`. A typo fails that job on every occurrence (logged and skipped).
- **A schedule naming a nonexistent job stops startup.** `mem.schedule_job.job_id` is a foreign key to `mem.job`, and sqlx's `SqliteConnectOptions` enables `PRAGMA foreign_keys` by default. `CRUD::init` inserts jobs first, so the bad reference aborts the config transaction with `(code: 787) FOREIGN KEY constraint failed`, breaking `serve`, `job list` and `job submit` alike. The reconcile never sees an unknown job id. (`job_run.job_id` has no FK, since the job is in the attached in-memory database, but `submit_job` bails with `Job '<id>' not found` before inserting.)
- **A run carries the YAML as it was at *submission*.** `CRUD::submit_job` snapshots tasks, commands, env and parameters onto the run's rows. With `submit_ahead: 1`, tonight's 03:00 run was written just after 03:00 yesterday, so this morning's edit misses it.
- **A running server never re-reads YAML.** `ServeCmd::run` calls `CRUD::init` once, and nothing reloads it, so everything that server submits uses its startup YAML. An edit, including a schedule's per-job `parameters:` (not compared by the existence check), reaches only runs submitted by a server that has read it. To redo a submitted run, restart and `flowlite job-run delete <id>` it, and the next pass rewrites it. Stopping it gives `Skipped` instead, which holds the occurrence forever. Otherwise only retention deletes rows.
