# `schedule_job` (mem)

Which jobs a [`schedule`](schedule.md) submits — one row per job listed under the schedule YAML's `jobs:`. No primary key, only `UNIQUE (row_id)`.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. |
| `schedule_id` | Foreign key to [`schedule`](schedule.md). |
| `job_id` | Foreign key to [`job`](job.md). |
| `parameters` | Nullable `TEXT`. **Stored and never used** — `CRUD::submit_job` takes only a `job_id`, so anything declared here is inert until submission learns to carry it. |

## Written by

`CRUD::init`, after every `job` row, which is what makes the foreign key below fire at the right moment.

## Read by

`Scheduler::handle_due_schedule`, which selects a due schedule's rows and calls `CRUD::submit_job` for each, and the schedule-detail web route.

## The foreign key is load-bearing

`job_id` references `mem.job`, and sqlx turns `PRAGMA foreign_keys` on by default, so **a schedule naming a job that doesn't exist stops the server from starting.** The bad reference aborts `CRUD::init`'s whole config transaction with `(code: 787) FOREIGN KEY constraint failed`, and the process exits before the scheduler ever ticks — one typo in one schedule file takes down `serve`, `job list` and `job submit` alike. The upside is that the scheduler loop never sees an unresolvable job id and nothing there needs to handle one.
