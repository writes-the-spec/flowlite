# Task run

`TaskRunStatus` lives in [src/crud/task_run.rs](../../../../src/crud/task_run.rs). One `task_run` row per task per job run, created `Pending` by `CRUD::submit_job` — for **every** task, not just the ones without dependencies. Ordering is enforced here, at dispatch time.

| Status | Meaning |
|---|---|
| `Pending` | Waiting. Its dependencies may or may not be resolved yet. |
| `Running` | Started, and owned by `TaskRunMonitor`, which decides which attempt runs next. Covers the gaps between attempts, not just the time a process is alive. |
| `Succeeded` | Its command exited 0. |
| `Failed` | Its command exited non-zero and no retries were left. |
| `TimedOut` | It ran past `task_run.timeout` and no retries were left. |
| `Aborted` | The job run was stopped after this task run started — its process killed mid-flight, or its next attempt skipped before the command began. |
| `Skipped` | It never started: a dependency didn't succeed, or the job run was stopped while it was still `Pending`. |

`Skipped` also covers the ordinary dependency case, so it is not by itself a sign of a stop. There is no `Cancelled`: a stop finds a task run either not yet started (`Skipped`, written by `TaskRunDispatcher`) or already started (`Aborted`, written by `TaskRunMonitor`). **Which one it is depends on the task run's own lifecycle, not on what its last attempt says** — a task run that already burned an attempt and was waiting to retry is `Aborted`, even though the attempt that never spawned is `Skipped`.

## Dispatcher: Pending → Running / Skipped

`TaskRunDispatcher` ([src/orchestrator/task_run_dispatcher.rs](../../../../src/orchestrator/task_run_dispatcher.rs)) polls **all** `Pending` task runs, whatever their job run's status, on a signal wake-up or its one-second interval, whichever comes first. It asks two questions per row, each of which owns its own guard and returns whether it transitioned:

1. `transition_to_skipped` → `Skipped` if the **job run was stopped**, or if **any dependency finished but didn't succeed** (`Failed`, `Skipped`, `Aborted`, `TimedOut`). Either way the task run can never run.
2. `transition_to_running` → `Running` with `started_at = now`, once **all dependencies have `Succeeded`**. Otherwise it transitions nothing and the row stays `Pending` for the next tick.

The two guards short-circuit in that order, so a stopped job run costs one query and never loads the dependencies.

Dependencies come from `task_run.depends_on` — the list copied off `task.depends_on` when the run was submitted — resolved to the task runs of the same job run by `get_dependent_task_runs`. A task with no dependencies falls straight through to step 3, since `all()` over an empty list is true.

This transition runs **at most once per task run**: a retry keeps the row `Running`, so the dependency check happens once and `started_at` means "when the task run started", covering every attempt.

`JobRunDispatcher::transition_to_skipped` short-circuits the stop check for a job run stopped while still `Pending`, skipping all of its task runs in one update instead of one per pass.

## Monitor: Running → finished

`TaskRunMonitor` ([src/orchestrator/task_run_monitor.rs](../../../../src/orchestrator/task_run_monitor.rs)) polls `Running` task runs on the same wake-up-or-interval schedule and looks only at their `task_run_attempt` rows — it never touches a process. No attempt yet → insert attempt 1 (`Pending`). Otherwise the **last** attempt (highest id) picks the handler:

| Last attempt | Arm | Task run |
|---|---|---|
| `Pending` \| `Running` | `Ok(())` | left `Running` — the attempt services still own it |
| `Succeeded` | `transition_to_succeeded` | `Succeeded` |
| `Failed` | `retry_or_transition_to_failed` | next attempt while `attempt < task_run.max_retries + 1` and `task_run.retry_delay` has elapsed, otherwise `Failed` |
| `Skipped` | `transition_to_aborted` | `Aborted` — the task run had started, so a stop aborts it |
| `Aborted` | `_aborted` | `Aborted` — terminal, never retried, so a stop can't be undone by a retry |
| `TimedOut` | `_timed_out` | `TimedOut` — terminal, not retried |

`Failed` is the only retried status. A retry inserts attempt `last.attempt + 1` and leaves the task run `Running`. Attempts count from 1, so total executions are `1 + max_retries` and `max_retries: 0` means one attempt. Both the count and the delay are read off the `task_run` row, so a run retries on the policy it was submitted with rather than on whatever the YAML says now.

Because the decision comes from the attempt rows alone, a stop landing *between* attempts isn't seen here: the monitor starts the next attempt (publishing a wake-up as it inserts the row), the attempt dispatcher skips or aborts it on the next pass, and the task run finishes from that.

## Invariants

- **A `Pending` or `Running` task run keeps its job run `Running`.** A task run that is never visited again strands its whole job run — see [job_run.md](job_run.md).
- **A `Running` task run must always have either an unfinished attempt or a finished last attempt to decide on.** An attempt row that never finishes stalls the task run, and through it the job run.
- **A new `TaskRunAttemptStatus` needs an arm** in `handle_running_task_run`, whose match over the last attempt's status is exhaustive. That exhaustiveness is the reason this monitor keeps a `match` rather than the dispatchers' ordered `transition_to_*` chain: a chain of boolean guards would let a new status fall through every one of them and leave the task run `Running` forever, where the match refuses to compile.
- **A new terminal `TaskRunStatus` needs three edits**: the failure list in `did_any_dependent_task_run_finish_but_not_succeed` (or downstream task runs wait forever), a transition in `JobRunMonitor::handle_running_job_run`, at the right rank, and a badge arm in `templates/routes/job_runs/job_run_id/route.html`.
- **This monitor never writes `Skipped`.** It only ever visits `Running` task runs, which have started; a stop therefore aborts them. Only `TaskRunDispatcher` skips a task run, and only one that never started.
- **Terminal statuses set `finished_at`** — `TaskRunMonitor::update_task_run_status` for the ones it derives, `TaskRunDispatcher::transition_to_skipped` for a skip.
