# `schedule_job` (mem)

Which jobs a [`schedule`](schedule.md) submits — one row per job under the schedule YAML's `jobs:`. No primary key, only `UNIQUE (row_id)`.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. |
| `schedule_id` | Foreign key to [`schedule`](schedule.md). |
| `job_id` | Foreign key to [`job`](job.md). |
| `parameters` | `NOT NULL`, `'{}'` if none. Overrides of the job's `parameters`, passed to `CRUD::submit_job`, where an undeclared name raises via `resolve_job_parameters`. |

## Written by

`CRUD::init`, after every `job` row, so the foreign key below checks against a complete `job` table.

## Read by

- `Scheduler::schedule_jobs`, once per schedule per pass, so `submit_if_missing` can call `CRUD::submit_job` with `parameters` for each occurrence with no run yet.
- The schedule-detail web route, which renders `parameters`.

## The foreign key is load-bearing

sqlx turns `PRAGMA foreign_keys` on by default, so **a schedule naming a nonexistent job stops startup**: `CRUD::init`'s config transaction aborts with `(code: 787) FOREIGN KEY constraint failed`, taking down `serve`, `job list` and `job submit` alike. In exchange the Scheduler never sees an unresolvable job id.
