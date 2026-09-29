# Job run

`JobRunStatus` lives in [src/crud/job_run.rs](../../../../src/crud/job_run.rs). A `job_run` is created `Scheduled` by `CRUD::submit_job` or `CRUD::rerun_job`, with one `Planned` task run per task (`Planned` is the task-run spelling of `Scheduled`).

| Status | Meaning |
|---|---|
| `Scheduled` | Written, not yet due. Only `JobRunReleaser` moves it out: to `Queued`, or `Skipped` if stopped first. |
| `Queued` | Released, waiting for a slot. `started_at` is NULL. |
| `Running` | Started; its task runs are being dispatched, executed and retried. |
| `Succeeded` | Every task run succeeded — also a job with no tasks. |
| `Failed` | A task run failed with retries exhausted. |
| `TimedOut` | A task run exceeded `task_run.timeout` with retries exhausted. |
| `Aborted` | Stopped after it started — a task run killed mid-flight, or one skipped before its command began. |
| `Skipped` | Stopped before it started. Nothing ran. |
| `Invalid` | flowlite cannot account for the row — see [Rows flowlite cannot read](../SKILL.md#rows-flowlite-cannot-read). |
| `Deleted` | Tombstone from `flowlite job-run delete` on a `Scheduled` run. Unlike `Skipped`, frees the occurrence for the scheduler to submit again. |

**`Aborted` vs `Skipped` is decided by the row's own lifecycle**, not its task runs: started → `Aborted`, never started → `Skipped`. The releaser and dispatcher write only `Skipped`, the monitor only `Aborted`. There is deliberately no `Cancelled`.

## Releaser: Scheduled → Queued / Skipped

`JobRunReleaser` ([src/orchestrator/job_run_releaser.rs](../../../../src/orchestrator/job_run_releaser.rs)) polls `Scheduled` runs:

1. `Skipped` — **stopped?** (`job_run_stop` row) → `set_to_skipped`: run and all its task runs via `CRUD::skip_job_run`. First, so a run stopped on the pass it comes due is not released.
2. `Queued` — **`scheduled_at` arrived?** → `set_to_queued`. `started_at` stays NULL; the dispatcher writes it.
3. `Scheduled` — neither → nothing written (`Ok(())`); a not-yet-due run is the common case.
4. anything else → `set_to_invalid`, logged as a bug. Unreachable. A failed read goes back to `Poller`.

It is the one service that relies on the poll interval rather than a wake-up: nothing publishes when a future instant arrives. That split is also why the [Scheduler](../../scheduler/SKILL.md) is separate — it decides *which* runs exist, the releaser *when* one is due.

A `Scheduled` run is never withdrawn: the scheduler only inserts, so a run whose schedule has since changed (lower `submit_ahead`, edited cron, disabled or deleted schedule) is still released at its instant. Only a [`job_run_stop`](../../entities/references/job_run_stop.md) row (step 1) or `flowlite job-run delete` (`Deleted`, before the releaser selects it) prevents it.

## Dispatcher: Queued → Running / Skipped

`JobRunDispatcher` ([src/orchestrator/job_run_dispatcher.rs](../../../../src/orchestrator/job_run_dispatcher.rs)) polls `Queued` runs **oldest id first**:

1. `Skipped` — **stopped?** → `set_to_skipped`: run and all task runs in one update via `CRUD::skip_job_run`. First, so a stopped run never asks for a slot.
2. `Queued` — **job at `max_parallel_runs`?** (`CRUD::is_job_at_max_parallel_runs`, counting that job's `Running` runs) → nothing written; reconsidered next pass.
3. `Running` — otherwise `set_to_running`: create the run's directory (`create_job_run_dir`; on failure the run stays `Queued`), move every `Planned` task run to `Waiting`, then write `Running` and `started_at = now`. **This is the only door into `Waiting`**, so a run held at 2 has nothing `TaskRunDispatcher` will start. Task runs go first so a crash between the writes leaves the run `Queued` to be redone, not `Running` over task runs nothing releases.
4. anything else → `set_to_invalid`, logged as a bug. Unreachable. A failed read is returned, not settled.

The limit is asked once per row.

**This check is the only place `max_parallel_runs` is enforced.** Nothing rejects an over-limit submission (`job submit`, rerun, scheduler); the run queues here until a slot frees. Oldest-first makes the queue fair, and a job slower than its schedule accumulates queued runs rather than losing them. Only `Running` runs count — counting `Queued` would deadlock, since the row being considered is itself `Queued`.

`started_at` is written exactly once, here; task run retries never touch it.

## Monitor: Running → finished

`JobRunMonitor` ([src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs)) loads each `Running` run's task runs and passes them to `derive_next_status`, a pure function of their statuses:

1. not every task run finished → `Running`, nothing written
2. any `Invalid` → `Invalid`
3. every task run `Succeeded` → `Succeeded`
4. any `Failed` → `Failed`
5. any `TimedOut` → `TimedOut`
6. any `is_stopped` (`Aborted` or `Skipped`) → `Aborted`
7. none → `None` → `set_to_invalid`. Unreachable. A value, not `Err`: nothing here can fail.

**Step 1 is the only thing holding an unfinished run open**, and everything below assumes it. Finishing is irreversible (only `Running` rows are visited), so without it the first failed task run would finish the run while siblings still execute — held by `a_failure_does_not_finish_a_job_run_whose_work_is_still_going`.

**Precedence:** unknown outranks named (`Invalid` first); failure outranks stop (`Aborted` last); failed outranks timed out. `Succeeded` needs every task run succeeded, so its position is free.

**Step 6 needs only one skip, not all.** A skip means a stop or an unsucceeded dependency, and the latter implies a `Failed`/`TimedOut`/`Aborted` upstream — so once 4 and 5 miss, a skip means a stop. It covers a run stopped while **nothing was executing** (between tasks, or before the first): its task runs are `Succeeded` + `Skipped`, and without step 6 it would fall to `Invalid`.

The monitor never reads a stop row; a stop reaches the job run only through task run statuses.

`all` over an empty list is true, so a run with no task runs settles `Succeeded`.

## Invariants

- **A job run stays `Running` until every task run is terminal.** Finishing early is final; later task run outcomes are never read.
- **`TaskRunStatus::is_finished` and `is_stopped`** ([src/crud/task_run.rs](../../../../src/crud/task_run.rs)) match exhaustively, so a new status must declare its side. `is_stopped` means a stop only after failures are ruled out.
- **Verdict precedence is held by tests** in [src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs), which feed status lists through `settled_job_run_status`. Add a case when you add a verdict.
- **This monitor never writes `Skipped`**: a `Running` run has started, and its logs may show output.
- **Terminal statuses set `finished_at`** via `update_job_run_status`, which then publishes and does nothing else; `NotificationService` reads the status to act on the run's [`job_run_notification`](../../entities/references/job_run_notification.md) rows.
- **A new terminal `TaskRunStatus` needs a return here at the right rank.** The compiler won't catch it; the final `None` settles the run `Invalid` with a bug log, and it can't leak out as `Succeeded`.
- **A new `JobRunStatus` needs a badge** in `templates/routes/home/job_run_table/route.html` and `templates/routes/job_runs/job_run_id/route.html`, plus an entry in the home route's `all_statuses` filter list.
