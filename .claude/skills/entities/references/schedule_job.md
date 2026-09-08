# `schedule_job` (mem)

Which jobs a [`schedule`](schedule.md) submits — one row per job listed under the schedule YAML's `jobs:`. No primary key, only `UNIQUE (row_id)`.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. |
| `schedule_id` | Foreign key to [`schedule`](schedule.md). |
| `job_id` | Foreign key to [`job`](job.md). |
| `parameters` | `NOT NULL` (`'{}'` when the schedule declares none). Overrides of the job's declared `parameters`, read by `Scheduler::handle_due_schedule` and passed to `CRUD::submit_job`, where a name this job does not declare raises via `resolve_job_parameters`. |

## Written by

`CRUD::init`, after every `job` row, which is what makes the foreign key below fire at the right moment.

## Read by

`Scheduler::handle_due_schedule`, which selects a due schedule's rows and calls `CRUD::submit_job` for each, passing `parameters` as that call's overrides, and the schedule-detail web route, which renders `parameters` in its jobs table.

## The foreign key is load-bearing

`job_id` references `mem.job`, and sqlx turns `PRAGMA foreign_keys` on by default, so **a schedule naming a job that doesn't exist stops the server from starting.** The bad reference aborts `CRUD::init`'s whole config transaction with `(code: 787) FOREIGN KEY constraint failed`, and the process exits before the scheduler ever ticks — one typo in one schedule file takes down `serve`, `job list` and `job submit` alike. The upside is that the scheduler loop never sees an unresolvable job id and nothing there needs to handle one.
