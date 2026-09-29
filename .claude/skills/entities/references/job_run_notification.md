# `job_run_notification` (disk)

One "tell somebody how this run ended" for a [`job_run`](job_run.md), and the record of what happened. Its status is the only one that is not a run status.

| Column | Meaning |
|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `job_run_id` | Foreign key to [`job_run`](job_run.md). |
| `job_id` | Carried like every parent id, so no join is needed. |
| `notify_on` | `failure` or `success` — the ending it waits for, named after the declaring block. A column because a job may have **one row per block**, which one ending settles differently. |
| `channel` | `email` or `slack`, as keyed in the block — stated, not inferred from `recipients`. |
| `recipients` | JSON array: addresses for `email`, conversations for `slack`. Resolved at submit from that channel **in that block**; blocks are independent. |
| `status` | `pending`, `sent`, `failed` or `skipped` — see below. |
| `error` | Why delivery failed; **empty** until then, not `NULL`. |
| `created_at` | Bound from `Toolkit`. |
| `sent_at` | `NULL` until it leaves. |

## Statuses

Written **at submit**, so **`pending` means open, not ready** — usually the run is still going. Only [`NotificationService`](../../notifications/SKILL.md) closes a row:

- `skipped` — the run ended another way (`NotifyOn::wants`): a `failure` row wants `Failed`, `TimedOut` or `Invalid`; a `success` row only `Succeeded`. Ordinary, not an error.
- `sent` — delivered, with `sent_at`.
- `failed` — the one delivery tried did not work, reason in `error`; also a channel with nothing configured, naming what is missing.

**One attempt, recorded either way**, so a down relay is not hammered every pass. No retry policy, deliberately: it would need its own delay and count, and a loud failure beats an unseen one.

## Written by

- **Inserted** by `CRUD::submit_job` / `CRUD::rerun_job` ([src/crud/multistatements/](../../../../src/crud/multistatements/)) via `insert_job_run_definition`, with the [`job_run`](job_run.md) and [`task_run`](task_run.md)s — frozen at submit like the rest of the definition: from `mem.job.on_failure_recipients`/`on_success_recipients`, or for a rerun the earlier run's rows. **One row per channel per block**, each delivered independently (a Slack outage does not swallow the mail); at most one of a run's `failure`/`success` rows is ever delivered.
- **Updated** by `NotificationService` only. **The orchestrator never writes it**; `JobRunMonitor` does not know notifications exist.

Writing at submit means the row exists before the run can fail; inserted at finish, it would have to share the monitor's status-write transaction or be lost, since a monitor never revisits a finished run. The cost is that most rows close `skipped`.

## Deleted by

`RetentionService`, with its [`job_run`](job_run.md) — see [job_run.md](job_run.md#deleted-by).

## Read by

- `NotificationService` — `pending` rows, deciding each from its run's status.
- `RetentionService`, in `select_deletable_job_runs` ([src/crud/multistatements/retention_candidates.rs](../../../../src/crud/multistatements/retention_candidates.rs)): a run with a `pending` row is never deleted, whatever `keep_runs`/`keep_runs_total` say; closed rows don't hold it.
