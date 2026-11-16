# `Schedule` (Declared)

One per schedule YAML file under `<data_dir>/schedules/*.yml`, a struct in `Declared` ([src/declared/declared.rs](../../../../src/declared/declared.rs)). The [Scheduler](../../scheduler/SKILL.md) reconciles every one each pass, submitting a [`job_run`](job_run.md) for any of its next `submit_ahead` occurrences that has none. It decides which runs ought to exist, not when one is due, and never takes one back.

| Field | Meaning |
|---|---|
| `schedule_id` | The YAML `id:`. Unique across files, refused at load otherwise. |
| `name`, `description` | Display only. `description` is serde-defaulted to `""`. |
| `cron` | **Six fields, seconds first** (the `cron` crate's dialect) — a five-field crontab silently means something else. |
| `timezone` | IANA name. Undeclared → `[schedule_defaults]` in flowlite.toml, itself defaulting to UTC. |
| `start_date`, `end_date` | `Option<NaiveDate>`; an open-ended schedule has no bound. |
| `disabled` | A plain `bool`. |
| `submit_ahead` | Occurrences kept submitted ahead of time; defaults to `1` in the YAML, refused at `0` (spell that `disabled: true`). |

## Written by

`Declared::load` only. Never changed after: no live state.

**A schedule's next run is stored nowhere.** It is derived on demand with `CronTrigger::from_schedule(schedule).get_next_run(None)` (next after *now*). The schedules routes compute it per request; `Scheduler` computes its desired occurrences with `get_next_runs` and keeps none of it.

## Read by

- `Scheduler::select` — every schedule, enabled or not, in reading order; "is this occurrence submitted?" is asked of `job_run`, not here.
- The schedules web routes — `cron`, `timezone` and the bounds, to derive a next run for display.
