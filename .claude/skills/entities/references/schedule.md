# `schedule` (mem)

One row per schedule YAML file under `<data_dir>/schedules/*.yml`. The [Scheduler](../../scheduler/SKILL.md) reconciles every row once a second, submitting a [`job_run`](job_run.md) for whichever of its `submit_ahead` next occurrences has none yet — it does not decide when a run becomes due, only which runs ought to exist, and it never takes one back.

| Column | Meaning |
|---|---|
| `row_id` | YAML declaration order. `UNIQUE`. Also the scheduler's select order. |
| `schedule_id` | **Primary key.** |
| `name`, `description` | Display only. `description` is `NOT NULL`, defaulted to `""` by serde. |
| `cron` | **Six fields, seconds first** (the `cron` crate's dialect) — a five-field crontab expression silently means something else. |
| `timezone` | IANA name. A schedule that declares none takes `[schedule_defaults]` from the data dir's config.toml, which itself defaults to UTC. |
| `start_date`, `end_date` | Nullable `DATE`. An open-ended schedule genuinely has no bound. |
| `disabled` | `INTEGER NOT NULL` — a boolean has no third state. |
| `submit_ahead` | `INTEGER NOT NULL`. How many occurrences the reconcile keeps submitted ahead of their time; defaults to `1` in the YAML, refused at `0` because that state already has a clearer spelling: `disabled: true`. |

## Written by

`CRUD::init`, and nothing else. The table is insert-only config like every other `mem` table: there is no `update_schedules`, and there is no column on it that carries live state.

**A schedule's next run is not stored anywhere.** It is derived, wherever it is wanted, with `CronTrigger::from_schedule(schedule).get_next_run(None)` — the next occurrence after *now*, which is a different answer each time it is asked and which nothing in the reconcile reads. The schedules routes compute it per request for display; `Scheduler` computes the whole list of desired occurrences with `get_next_runs` and keeps none of it.

## Read by

`Scheduler::select`, which reads every row — enabled or not, due or not, ordered by `row_id` — because "is anything due?" is a question the reconcile puts to the [`job_run`](job_run.md) table, one lookup per occurrence and job, not to this table. And the schedules web routes, which read `cron`, `timezone` and the bounds to derive a next run for display.
