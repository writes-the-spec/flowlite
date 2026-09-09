# `job_run_notification` (disk)

One "tell someone this run broke" for a [`job_run`](job_run.md), and the record of what happened when we tried. It is the only table with a status column that is not a run status.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id` | Carried like every other parent id, so a row says which job it is about without a join. |
| `channel` | How it is delivered — `email` today. A column rather than something the sender infers from `recipients`, so one row says for itself what delivering it means. |
| `recipients` | JSON array, addressed however `channel` addresses people. For `email`, copied from `job_run.on_failure_emails` — the run's own snapshot, not the job's YAML now. |
| `status` | `pending`, `sent` or `failed`. |
| `error` | Why a send failed; **empty** until one does — a notification nobody has tried has no error, not an unknown one. |
| `created_at` | Bound from `Toolkit`. |
| `sent_at` | `NULL` until it leaves. A timestamp that has not happened yet is the nullable case. |

## Written by

`JobRunMonitor` ([src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs)) inserts it as `pending`, **in the same transaction as the job run's finishing status write**. One transaction because that monitor only ever visits `Running` rows: a status write that landed without its notification would leave a finished run nothing looks at again, and a failure nobody is told about.

It is written only for `Failed` and `TimedOut`, and only when `job_run.on_failure_emails` is non-empty. `Aborted` is deliberately not one of them — a stop is somebody at a keyboard, who already knows what they did.

`NotificationService` ([src/notifications/service.rs](../../../../src/notifications/service.rs)) then moves it to `sent` or `failed`. That service is **not** part of the orchestrator — see the [notifications skill](../../notifications/SKILL.md).

## Read by

`NotificationService` alone, which selects `pending` rows — the open ones — whatever channel and whatever run they are about.

## One attempt, recorded either way

Every path through `handle` writes the row. That is what stops a relay that is down from being mailed once a second forever: a send that fails is recorded as `failed`, with the error on the row and in the log, and is not tried again. There is deliberately no retry policy — one would need its own delay and attempt count, and an alert nobody can see failed is worse than one that failed loudly.

A channel with nothing configured is the same story: the row is closed as `failed`, naming what is missing, rather than being selected again every second forever.

A crash between the insert and the send leaves the row `pending`, so it is sent when the process comes back. That is the intended direction: at-least-once, on a table that survives a restart.
