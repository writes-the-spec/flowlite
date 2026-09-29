# `schedule` (mem)

One row per schedule YAML file under `<data_dir>/schedules/*.yml`. The [Scheduler](../../scheduler/SKILL.md) reconciles every row each pass, submitting a [`job_run`](job_run.md) for any of its next `submit_ahead` occurrences that has none. It decides which runs ought to exist, not when one is due, and never takes one back.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. Also the Scheduler's select order. |
| `schedule_id` | **Primary key.** |
| `name`, `description` | Display only. `description` is `NOT NULL`, serde-defaulted to `""`. |
| `cron` | **Six fields, seconds first** (the `cron` crate's dialect) — a five-field crontab silently means something else. |
| `timezone` | IANA name. Undeclared → `[schedule_defaults]` in config.toml, itself defaulting to UTC. |
| `start_date`, `end_date` | Nullable `DATE`; an open-ended schedule has no bound. |
| `disabled` | `INTEGER NOT NULL` — a boolean has no third state. |
| `submit_ahead` | `INTEGER NOT NULL`. Occurrences kept submitted ahead of time; defaults to `1` in the YAML, refused at `0` (spell that `disabled: true`). |

## Written by

`CRUD::init` only. Insert-only like every `mem` table: no `update_schedules`, no live-state column.

**A schedule's next run is stored nowhere.** It is derived on demand with `CronTrigger::from_schedule(schedule).get_next_run(None)` (next after *now*). The schedules routes compute it per request; `Scheduler` computes its desired occurrences with `get_next_runs` and keeps none of it.

## Read by

- `Scheduler::select` — every row, enabled or not, ordered by `row_id`; "is this occurrence submitted?" is asked of `job_run`, not here.
- The schedules web routes — `cron`, `timezone` and the bounds, to derive a next run for display.
