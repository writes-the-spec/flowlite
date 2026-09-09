# `job_run_notification` (disk)

One "tell somebody how this run ended" for a [`job_run`](job_run.md), and the record of what happened when it came due. It is the only table with a status column that is not a run status.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id` | Carried like every other parent id, so a row says which job it is about without a join. |
| `channel` | How it is delivered — `email` today. A column rather than something the sender infers from `recipients`, so one row says for itself what delivering it means. |
| `recipients` | JSON array, addressed however `channel` addresses people. For `email`, the `on_failure.email` the job declared, resolved at submit. |
| `status` | `pending`, `sent`, `failed` or `skipped` — see below. |
| `error` | Why a delivery failed; **empty** until one does — a notification nobody has tried has no error, not an unknown one. |
| `created_at` | Bound from `Toolkit`. |
| `sent_at` | `NULL` until it leaves. A timestamp that has not happened yet is the nullable case. |

## `pending` means open, not ready

The row is written **when the run is submitted**, long before anyone knows whether it will be needed. So `pending` is "nobody has decided about this yet", and most passes over one are about a run that has not ended.

The other three are all closed, and only [`NotificationService`](../../notifications/SKILL.md) writes them:

- `skipped` — the run ended in a way nobody needs telling about (`Succeeded`, `Aborted`, `Skipped`). An ordinary outcome, not a failure: the row was written before that was knowable.
- `sent` — delivered, with `sent_at`.
- `failed` — a delivery was tried and did not work, with the reason in `error`.

## Written by

`CRUD::submit_job` and `CRUD::rerun_job` ([src/crud/multistatements/misc.rs](../../../../src/crud/multistatements/misc.rs)), through `insert_job_run_definition` — the same call that inserts the [`job_run`](job_run.md) and its [`task_run`](task_run.md)s, from the same snapshot. Who to tell is part of a run's definition, exactly like its commands and its parameters, so it is frozen at submit and a rerun replays it: `submit_job` builds it from `mem.job.on_failure_emails`, `rerun_job` from the earlier run's own rows.

**Nothing in the orchestrator writes this table.** `JobRunMonitor` finishes a run and publishes; it does not know notifications exist.

## Read by

`NotificationService` alone, which selects the `pending` rows — the open ones — whatever channel and whatever run they are about, and reads the run's status to decide what each deserves.

## One attempt, recorded either way

Every path that reaches a channel writes the row. That is what stops a relay that is down from being hammered once a second: a delivery that fails is recorded as `failed`, with the error on the row and in the log, and is not tried again. There is deliberately no retry policy — one would need its own delay and attempt count, and an alert nobody can see failed is worse than one that failed loudly.

A channel with nothing configured is the same story: closed as `failed`, naming what is missing.

## What writing it at submit buys

The row cannot be lost at the moment it matters. When the notification was written by the monitor as it finished a failed run, that insert had to share a transaction with the status write — because a monitor only ever visits `Running` rows, so a status write that committed alone would have left a finished run nothing ever looks at again. Writing it at submit removes the problem rather than guarding it: by the time a run can fail, the row is already there.

The cost is a row per run per channel even for runs that succeed, closed as `skipped`. That is the record of a decision made, which is worth more than the bytes.
