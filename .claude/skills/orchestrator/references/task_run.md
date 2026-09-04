# Task run

`TaskRunStatus` lives in [src/crud/task_run.rs](../../../../src/crud/task_run.rs). One `task_run` row per task per job run, created `Pending` by `CRUD::submit_job` — for **every** task, not just the ones without dependencies. Ordering is enforced here, at dispatch time.

| Status | Meaning |
|---|---|
| `Pending` | Waiting. Its dependencies may or may not be resolved yet. |
| `Running` | Started, and owned by `TaskRunMonitor`, which decides which attempt runs next. Covers the gaps between attempts, not just the time a process is alive. |
| `Succeeded` | Its command exited 0. |
| `Failed` | Its command exited non-zero and no retries were left. |
| `TimedOut` | It ran past `task_run.timeout` and no retries were left. |
| `Aborted` | Its process was killed mid-flight because the job run was stopped. |
| `Skipped` | It never ran: a dependency didn't succeed, or the job run was stopped before it started. |

`Skipped` also covers the ordinary dependency case, so it is not by itself a sign of a stop. There is no `Cancelled`: a stop finds a task run either not yet started (`Skipped`) or executing (`Aborted`).

## Dispatcher: Pending → Running / Skipped

`TaskRunDispatcher` ([src/orchestrator/task_run_dispatcher.rs](../../../../src/orchestrator/task_run_dispatcher.rs)) polls **all** `Pending` task runs, whatever their job run's status. `derive_next_task_run_status`, first match winning:

1. **Job run stopped?** → `Skipped`
2. **Any dependency finished but didn't succeed** (`Failed`, `Skipped`, `Aborted`, `TimedOut`) → `Skipped`
3. **All dependencies `Succeeded`** → `Running`, via `handle_start_task_run`, which sets `started_at = now`
4. Otherwise → `None`, left `Pending` for the next tick

Dependencies come from `task_run.depends_on` — the list copied off `task.depends_on` when the run was submitted — resolved to the task runs of the same job run by `get_dependent_task_runs`. A task with no dependencies falls straight through to step 3, since `all()` over an empty list is true.

This transition runs **at most once per task run**: a retry keeps the row `Running`, so the dependency check happens once and `started_at` means "when the task run started", covering every attempt.

`JobRunDispatcher::handle_stopped_job_run` short-circuits step 1 for a job run stopped while still `Pending`, skipping all of its task runs in one update instead of one per tick.

## Monitor: Running → finished

`TaskRunMonitor` ([src/orchestrator/task_run_monitor.rs](../../../../src/orchestrator/task_run_monitor.rs)) polls `Running` task runs and looks only at their `task_run_attempt` rows — it never touches a process. No attempt yet → insert attempt 1 (`Pending`). Otherwise the **last** attempt (highest id) picks the handler:

| Last attempt | `handle_last_task_run_attempt_*` | Task run |
|---|---|---|
| `Pending` | `_pending` | left `Running` — the attempt dispatcher owns it |
| `Running` | `_running` | left `Running` — the attempt monitor owns it |
| `Succeeded` | `_succeeded` | `Succeeded` |
| `Failed` | `_failed` | next attempt while `attempt < task_run.max_retries + 1` and `task_run.retry_delay` has elapsed, otherwise `Failed` |
| `Skipped` | `_skipped` | `Skipped` |
| `Aborted` | `_aborted` | `Aborted` — terminal, never retried, so a stop can't be undone by a retry |
| `TimedOut` | `_timed_out` | `TimedOut` — terminal, not retried |

`Failed` is the only retried status. A retry inserts attempt `last.attempt + 1` and leaves the task run `Running`. Attempts count from 1, so total executions are `1 + max_retries` and `max_retries: 0` means one attempt. Both the count and the delay are read off the `task_run` row, so a run retries on the policy it was submitted with rather than on whatever the YAML says now.

Because the decision comes from the attempt rows alone, a stop landing *between* attempts isn't seen here: the monitor starts the next attempt, the attempt dispatcher skips or aborts it within a tick, and the task run finishes from that.

## Invariants

- **A `Pending` or `Running` task run keeps its job run `Running`.** A task run that is never visited again strands its whole job run — see [job_run.md](job_run.md).
- **A `Running` task run must always have either an unfinished attempt or a finished last attempt to decide on.** An attempt row that never finishes stalls the task run, and through it the job run.
- **A new `TaskRunAttemptStatus` needs an arm** in `handle_running_task_run`, whose match over the last attempt's status is exhaustive, plus a `handle_last_task_run_attempt_*` function.
- **A new terminal `TaskRunStatus` needs three edits**: the failure list in `did_any_dependent_task_run_finish_but_not_succeed` (or downstream task runs wait forever), a rule in `JobRunMonitor::derive_next_job_run_status`, and a badge arm in `templates/routes/job_runs/job_run_id/route.html`.
- **Terminal statuses set `finished_at`**, via `update_task_run_status`.
