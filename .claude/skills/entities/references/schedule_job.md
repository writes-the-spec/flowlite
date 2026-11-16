# `ScheduleJob` (Declared)

Which jobs a [`Schedule`](schedule.md) submits — one per job under the schedule YAML's `jobs:`, a struct in `Declared` ([src/declared/declared.rs](../../../../src/declared/declared.rs)).

| Field | Meaning |
|---|---|
| `schedule_id` | The [`Schedule`](schedule.md) it belongs to. |
| `job_id` | A [`Job`](job.md) some file declares — checked at load. |
| `parameters` | Empty if none. Overrides of the job's `parameters`, passed to `CRUD::submit_job`, where an undeclared name raises via `resolve_job_parameters`. |

## Written by

`Declared::load`, after every job, so `job_id` is checked against the complete set.

## Read by

- `Scheduler::reconcile_schedule`, through `Declared::jobs_of_schedule`, once per schedule per pass, so `submit_if_missing` can call `CRUD::submit_job` with `parameters` for each occurrence with no run yet.
- The schedule-detail web route, which renders `parameters`.

## An unknown job stops startup

A schedule naming a job no file declares fails `Declared::load`, taking down `serve`, `job list` and `job submit` alike. In exchange the Scheduler never sees an unresolvable job id.
