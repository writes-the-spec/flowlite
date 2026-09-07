# `schedule` (mem)

One row per schedule YAML file under `<config_dir>/schedules/*.yml`. The [Scheduler](../../scheduler/SKILL.md) polls this table once a second and submits the jobs of every due schedule.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. Also the scheduler's select order. |
| `schedule_id` | **Primary key.** |
| `name`, `description` | Display only. `description` is `NOT NULL`, defaulted to `""` by serde. |
| `cron` | **Six fields, seconds first** (the `cron` crate's dialect) — a five-field crontab expression silently means something else. |
| `timezone` | IANA name; defaults to UTC in the YAML. |
| `start_date`, `end_date` | Nullable `DATE`. An open-ended schedule genuinely has no bound. |
| `disabled` | `INTEGER NOT NULL` — a boolean has no third state. |
| `next_run` | Nullable `TIMESTAMP`. Unset until computed; `NULL` past `end_date`. |

## Written by

`CRUD::init` inserts the row and computes the first `next_run` immediately with `CronTrigger::get_next_run(None)`.

**`Scheduler` then updates `next_run` on every fire** — this is the one `mem` column that carries live state rather than config, and the one exception to "config tables are insert-only". It is still in-memory, so it is recomputed from scratch at every startup: there is no catch-up for runs whose time passed while the process was down, and `next_run` is not a record of anything. The `job_run` rows are the history.

## Read by

`Scheduler::select` (`next_run < now AND disabled = false`, ordered by `row_id`) and the schedules web routes.

**A `NULL` `next_run` retires the schedule for the rest of the process's life** — SQL comparisons against NULL are never true, so it drops out of that filter. That is how an expired schedule stops firing, and also what a bug that nulls `next_run` looks like.
