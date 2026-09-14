# `schedule` (mem)

One row per schedule YAML file under `<data_dir>/schedules/*.yml`. The [Scheduler](../../scheduler/SKILL.md) reconciles every row once a second, keeping its `submit_ahead` next occurrences submitted as [`job_run`](job_run.md) rows — it does not decide when a run becomes due, only which runs ought to exist.

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
| `next_run` | Nullable `TIMESTAMP`. **Display only — not a cursor.** The earliest of the occurrences the last reconcile pass computed as desired; `NULL` when there is nothing left to submit (past `end_date`, or `disabled`). |

## Written by

`CRUD::init` inserts the row and computes the first `next_run` immediately with `CronTrigger::get_next_run(None)`, purely so the dashboard has something to show before the scheduler's first pass.

**`Scheduler` then rewrites `next_run` on every pass, unconditionally** — this is the one `mem` column that carries live state rather than config, and the one exception to "config tables are insert-only". Nothing advances it and nothing reads it back to decide anything: it is not a cursor the reconcile steps forward, just the first of the occurrences that pass's own cron math produced, written for a reader rather than for the code. It is still in-memory, so it is recomputed from scratch at every startup, from `CronTrigger::get_next_run(None)` on the same schedule row — never from an old `next_run` value, which no longer exists at that point anyway.

## Read by

`Scheduler::select`, which now reads every row — enabled or not, due or not, ordered by `row_id` — because "is anything due?" is a question the reconcile puts to the [`job_run`](job_run.md) rows it already holds, not to this table. And the schedules web routes, which still just display `next_run`.
