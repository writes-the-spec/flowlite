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

`TaskRunDispatcher` ([src/orchestrator/task_run_dispatcher.rs](../../../../src/orchestrator/task_run_dispatcher.rs)) polls **all** `Pending` task runs, whatever their job run's status, on a signal wake-up or its one-second interval, whichever comes first. It settles each row as exactly one outcome, each owning its own guard and returning whether it is what happened:

1. `settle_as_skipped` → `Skipped` if the **job run was stopped**, or if **any dependency finished but didn't succeed** (`Failed`, `Skipped`, `Aborted`, `TimedOut`). Either way the task run can never run.
2. `settle_as_pending` → **any dependency still `Pending` or `Running`** → the row stays `Pending` for the next tick. **It writes nothing, and exists to say so.**
3. `settle_as_running` → `Running` with `started_at = now`, once **all dependencies have `Succeeded`**. It also inserts **attempt 1** (`Pending`), *before* the status write — `TaskRunMonitor` decides from the last attempt, so a `Running` task run without one is a state it cannot act on. Same ordering rule as the child-before-status hand-off one level down.
4. Past all three → `anyhow::bail!`.

Step 1's two guards short-circuit in order, so a stopped job run costs one query and never loads the dependencies.

Step 2 is what makes step 4 possible. By the time it is asked, step 1 has ruled out every dependency that finished without succeeding, so each one is succeeded, pending or running — and step 2 claims exactly the rows step 3 will not start. Without it, a task run nobody handled would sit at `Pending` looking exactly like one legitimately waiting on a dependency, which is the one failure this service cannot spot by watching it. Ordering it ahead of step 3 is what all three dispatchers do: skipped, pending, running.

**Unlike the two dispatchers either side of it, step 3 keeps a guard of its own** rather than starting whatever step 2 declined. It is not the same question asked twice: each outcome loads the dependencies for itself, so a dependency that fails between the two loads is caught by "have they all succeeded?" and the task run is not started.

**The price is a false-positive window on the bail.** Those separate snapshots mean a dependency that fails between the first load and the last leaves a set that is neither all-succeeded nor still-running, and the bail fires on an ordinary state. It clears on the next pass, where `settle_as_skipped` sees the failure and claims the row. Loading the dependencies once in `handle_pending_task_run` and passing them down would close the window and drop two queries per row, at the cost of the guards no longer standing on their own.

Dependencies come from `task_run.depends_on` — the list copied off `task.depends_on` when the run was submitted — resolved to the task runs of the same job run by `get_dependent_task_runs`. A task with no dependencies is turned down by step 2 (`any()` over an empty list is false) and started by step 3 (`all()` over one is true).

This transition runs **at most once per task run**: a retry keeps the row `Running`, so the dependency check happens once and `started_at` means "when the task run started", covering every attempt.

`JobRunDispatcher::settle_as_skipped` short-circuits the stop check for a job run stopped while still `Pending`, skipping all of its task runs in one update instead of one per pass.

## Monitor: Running → finished

`TaskRunMonitor` ([src/orchestrator/task_run_monitor.rs](../../../../src/orchestrator/task_run_monitor.rs)) polls `Running` task runs on the same wake-up-or-interval schedule and looks only at their `task_run_attempt` rows — it never touches a process. The **last** attempt (highest `attempt`, which a unique index makes unique per task run) picks the outcome; `get_last_task_run_attempt` returns it or raises, since a `Running` task run always has one:

| Last attempt | Outcome | Task run |
|---|---|---|
| `Succeeded` | `settle_for_succeeded` | `Succeeded` |
| `Pending`, `Running`, or `Failed` with a retry left | `settle_for_running` | left `Running`; inserts the retry row immediately — `TaskRunAttemptDispatcher` holds it `Pending` until `retry_delay` has passed |
| `Failed`, no retry left | `settle_for_failed` | `Failed` |
| `TimedOut` | `settle_for_timed_out` | `TimedOut` |
| `is_stopped` — `Aborted` or `Skipped` | `settle_for_aborted` | `Aborted` — the task run had started, so a stop aborts it |
| past all five | `anyhow::bail!` | — |

These guards are exclusive, since the last attempt has exactly one status, so **the order here carries nothing** and matches [job_run.md](job_run.md)'s ladder only so the two read alike. `TaskRunAttemptStatus::is_stopped` needs no failure ruled out first, unlike its `TaskRunStatus` namesake: `TaskRunAttemptDispatcher` skips an attempt for one reason only.
| `Aborted` | `_aborted` | `Aborted` — terminal, never retried, so a stop can't be undone by a retry |
| `TimedOut` | `_timed_out` | `TimedOut` — terminal, not retried |

`Failed` is the only retried status. A retry inserts attempt `last.attempt + 1` and leaves the task run `Running`. Attempts count from 1, so total executions are `1 + max_retries` and `max_retries: 0` means one attempt. Both the count and the delay are read off the `task_run` row, so a run retries on the policy it was submitted with rather than on whatever the YAML says now.

Because the decision comes from the attempt rows alone, a stop landing *between* attempts isn't seen here: the monitor starts the next attempt (publishing a wake-up as it inserts the row), the attempt dispatcher skips or aborts it on the next pass, and the task run finishes from that.

## Invariants

- **A `Pending` or `Running` task run keeps its job run `Running`.** A task run that is never visited again strands its whole job run — see [job_run.md](job_run.md).
- **A `Running` task run must always have either an unfinished attempt or a finished last attempt to decide on.** An attempt row that never finishes stalls the task run, and through it the job run.
- **A new `TaskRunAttemptStatus` needs an outcome** in `handle_running_task_run`. This used to be an exhaustive `match`, so the compiler caught the omission; the `settle_for_*` chain replaced that check with the bail, which catches it at runtime instead — the row is logged with its id on every pass rather than silently staying `Running` forever. `settle_for_failed` and `settle_for_running` split a `Failed` last attempt between them on `has_retry_left`, so the two cannot both claim it or both pass it by.
- **A new terminal `TaskRunStatus` needs three edits**: the failure list in `TaskRunDispatcher::settle_as_skipped` (or downstream task runs wait forever), a transition in `JobRunMonitor::handle_running_job_run`, at the right rank, and a badge arm in `templates/routes/job_runs/job_run_id/route.html`.
- **This monitor never writes `Skipped`.** It only ever visits `Running` task runs, which have started; a stop therefore aborts them. Only `TaskRunDispatcher` skips a task run, and only one that never started.
- **Terminal statuses set `finished_at`** — `TaskRunMonitor::update_task_run_status` for the ones it derives, `TaskRunDispatcher::settle_as_skipped` for a skip.
