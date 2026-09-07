# Task run attempt

`TaskRunAttemptStatus` lives in [src/crud/task_run_attempt.rs](../../../../src/crud/task_run_attempt.rs). One `task_run_attempt` row per execution of a task run's command, inserted `Pending` by `TaskRunDispatcher` for attempt 1 and by `TaskRunMonitor` for every retry — this is the only level that runs a process.

| Status | Meaning |
|---|---|
| `Pending` | Inserted, waiting for the dispatcher. |
| `Running` | Its command was spawned; the monitor owns the process. |
| `Succeeded` | The process exited 0. |
| `Failed` | The process exited non-zero. |
| `TimedOut` | The process ran past `task_run.timeout` and was killed. |
| `Aborted` | The process was killed because the job run was stopped — or there was no process left to wait for. |
| `Skipped` | The job run was stopped between the insert and the dispatch, so the command never started. |

Same seven variants as `TaskRunStatus`, because both levels have the same dispatcher/monitor shape — but a separate enum, and `TaskRunMonitor` maps one onto the other explicitly. The `Pending` state is what makes an attempt skippable: a stop arriving in the one tick before it is cleared to run finds nothing to kill.

## Dispatcher: Pending → Running / Skipped

`TaskRunAttemptDispatcher` ([src/orchestrator/task_run_attempt_dispatcher.rs](../../../../src/orchestrator/task_run_attempt_dispatcher.rs)) polls **all** `Pending` attempts, on a signal wake-up or its one-second interval, whichever comes first. It settles each row as exactly one outcome, each owning its own guard and returning whether it is what happened:

1. `settle_as_skipped` — **job run stopped?** → `Skipped`, `finished_at` set, `started_at` left NULL: the command never ran.
2. `settle_as_pending` — **a retry whose delay has not passed?** (`attempt > 1` and `now < created_at + task_run.retry_delay`) → writes nothing, the row waits for a later pass. This is where `retry_delay` is enforced, and it has to precede step 3, which spawns unconditionally.
3. `settle_as_running` — otherwise: load the attempt's `task_run` row by `task_run_id`, for the `command` and `timeout` the run was submitted with, spawn `sh -c <command>` with piped stdout/stderr, insert the child into `TaskRunAttemptChildren`, then write `Running` and `started_at = now`. This is the one outcome that does work outside the database, so a spawn failure propagates as the row's error and `Poller::run` logs it and moves to the next attempt.

Step 1 before step 2 is what makes a stop beat a waiting retry: a job run stopped mid-delay skips the pending retry rather than spawning it when the delay runs out. Attempt 1 never reaches step 2 — it is inserted by `TaskRunDispatcher` as it starts the task run and has nothing to wait for, so the `attempt > 1` check spares it the task run query as well.

**The child goes into the map before the status is written.** In the other order the monitor can see a `Running` attempt whose process isn't in the map yet and abort it.

## Monitor: Running → finished

`TaskRunAttemptMonitor` ([src/orchestrator/task_run_attempt_monitor.rs](../../../../src/orchestrator/task_run_attempt_monitor.rs)) polls `Running` attempts on the same wake-up-or-interval schedule and takes their child out of `TaskRunAttemptChildren`:

- **No child** → `settle_for_aborted_without_child`: `Aborted`, output left as last persisted. The map holds only processes *this* program spawned, so a `Running` row without one belongs to an earlier run of it. This is the restart path, and it is asked first because every transition below needs a process to act on.
- **Child present** → each outcome owns its guard, drains the output itself and returns whether it fired, tried in this order:
  1. `settle_for_exit_status` — **exited?** → `Succeeded`/`Failed` from the exit status.
  2. `settle_for_timed_out` — **past `task_run.timeout`?** → kill it, `TimedOut`. Measured from the in-memory spawn time (`times_out_at`), so neither the wait for dispatch nor the spawn counts against it.
  3. `settle_for_aborted` — **job run stopped?** → kill it, `Aborted`.
  4. `settle_for_running` — persist the output so far and put the child back for the next tick.
  5. Past all four → `anyhow::bail!`, unreachable while step 4 claims everything the others left.

Steps 1–3 borrow the child (`&mut TaskRunAttemptChild`) rather than taking it, so the caller still owns it when none of them fires and can hand it to step 4.

**Order decides precedence here**, on the same rule as [job_run.md](job_run.md): a real outcome outranks a stop, so step 3 is last. A process that already exited reports what it exited with rather than being recorded as killed, and one past its timeout reports the timeout. A process still running when its job run is stopped is still killed on the same pass, because steps 1 and 2 decline and step 3 is reached immediately.

It never reads or writes a task run row: retries and the task run status are `TaskRunMonitor`'s business.

## The shared children map

`TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../../src/orchestrator/task_run_attempt_children.rs)) is a `Mutex<HashMap<task_run_attempt_id, TaskRunAttemptChild>>` created by `Orchestrator::start` and shared by the two attempt services: the dispatcher inserts, the monitor removes. It is the orchestrator's only cross-service state outside the database, and it is in memory only.

## Stdout/stderr

`read_output` drains both pipes with a 10ms timeout so a chatty process can't block the loop. The accumulated bytes are written to `task_run_attempt.stdout`/`stderr` on every poll pass the process is still alive (so logs are visible while it runs) and once more when the attempt ends, after a final drain. This is a data-only write, so it deliberately never publishes — see the [orchestrator skill](../SKILL.md#how-they-coordinate).

## Invariants

- **A command is spawned exactly once per attempt row**, by the dispatcher. A `Running` attempt without a child is `Aborted`, never respawned — the retry comes from `TaskRunMonitor` inserting a *new* attempt row.

**Attempt 1 comes from `TaskRunDispatcher`**, which inserts it before writing the task run `Running`, so `TaskRunMonitor` never sees a `Running` task run with nothing to decide from. It raises if it ever does.
- **Terminal statuses set the attempt's `finished_at`**, via `finish_task_run_attempt`.
- **A new `TaskRunAttemptStatus` needs a handler in `TaskRunMonitor`** — see [task_run.md](task_run.md).
