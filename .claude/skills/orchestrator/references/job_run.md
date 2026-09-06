# Job run

`JobRunStatus` lives in [src/crud/job_run.rs](../../../../src/crud/job_run.rs). A `job_run` row is created `Pending` by `CRUD::submit_job`, together with one `Pending` task run per task of the job.

| Status | Meaning |
|---|---|
| `Pending` | Submitted, waiting. Nothing has run; `started_at` is NULL. |
| `Running` | Started. Its task runs are being dispatched, executed and retried. |
| `Succeeded` | Every task run succeeded — also the status of a job with no tasks. |
| `Failed` | A task run failed with retries exhausted. |
| `TimedOut` | A task run exceeded `task_run.timeout` with retries exhausted. |
| `Aborted` | A task run was killed mid-flight because the run was stopped. |
| `Skipped` | Stopped without anything being interrupted — before starting, or between tasks. |

`Aborted` vs `Skipped` is the "stopped" pair: `Aborted` means work was interrupted, `Skipped` means it never got going. There is deliberately no `Cancelled`.

## Dispatcher: Pending → Running / Skipped

`JobRunDispatcher` ([src/orchestrator/job_run_dispatcher.rs](../../../../src/orchestrator/job_run_dispatcher.rs)) polls `Pending` job runs — on a signal wake-up or its one-second interval, whichever comes first — and asks two questions per row, each of which owns its own guard and returns whether it transitioned:

1. `transition_to_skipped` — **stopped?** (a `job_run_stop` row exists) → the job run goes `Skipped`, and so do all of its task runs in one update. It never runs.
2. `transition_to_running` — otherwise, unconditionally: `status = Running`, `started_at = now`. No queue, no concurrency limit, no readiness check; whatever is `Pending` and not stopped starts on the next tick.

`started_at` is written exactly once, here; task run retries never touch it.

## Monitor: Running → finished

`JobRunMonitor` ([src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs)) polls `Running` job runs on the same wake-up-or-interval schedule, loads all task runs of each, and hands them to the pure `derive_next_job_run_status(&[TaskRun]) -> Option<JobRunStatus>`, first match winning:

0. Any task run `Pending` or `Running` → `None`, the job run stays `Running`
1. Any `Aborted` → `Aborted`
2. Any `TimedOut` → `TimedOut`
3. Any `Failed` → `Failed`
4. Any `Skipped` → `Skipped`
5. Otherwise → `Succeeded`

The status is derived **entirely from its task runs** — the monitor never reads a stop signal. A stop reaches the job run only as the task run statuses it produced, through rules 1 and 4.

Rule 4 does not require *all* task runs to be skipped: skips come either from a stop or from a dependency that didn't succeed, and in the second case that dependency is itself `Failed`/`TimedOut`/`Aborted` or skipped — so once rules 1–3 have not matched, a skip can only mean the run was stopped.

Every rule is an `any(...)`, so a job run with no task runs falls through to rule 5.

## Invariants

- **A job run must stay `Running` until every task run is terminal.** The monitor only visits `Running` rows, so finishing one early is final: its task runs keep executing but their outcome is never read again.
- **The decision stays pure.** `derive_next_job_run_status` takes `&[TaskRun]` and does no I/O. Add rules there, not around the update call.
- **Terminal statuses set `finished_at`**, via `update_job_run_status`.
- **A new terminal `TaskRunStatus` needs a rule here**, or it falls through to rule 5 and silently reports `Succeeded`.
- **A new `JobRunStatus` needs a badge** in `templates/routes/home/job_run_table/route.html` and `templates/routes/job_runs/job_run_id/route.html`, plus an entry in the home route's `all_statuses` filter list.
