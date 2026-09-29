---
name: orchestrator
description: High-level map of src/orchestrator/ - the background services that turn job runs into finished task runs, and the dispatcher/monitor pattern they all follow. Use when adding or changing an orchestrator service, or when tracing how a run moves through its statuses.
---

# Orchestrator

Seven background services, all spawned by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which `serve` calls. They take over a job run whatever created it — `job submit`, the web UI, or the [Scheduler](../scheduler/SKILL.md), a separate service that only ever inserts runs `Scheduled` and never updates or deletes one. From then on only these services touch it.

| Service | Polls | Does |
|---|---|---|
| `JobRunReleaser` | `Scheduled` job runs | `Queued` once `scheduled_at` arrives, or `Skipped` (with its task runs) if stopped first |
| `JobRunDispatcher` | `Queued` job runs | starts them while `max_parallel_runs` allows, releasing their task runs `Planned` → `Waiting`; holds or skips them |
| `JobRunMonitor` | `Running` job runs | finishes them from their task runs |
| `TaskRunDispatcher` | `Waiting` task runs | starts them once dependencies succeeded, holds them while one is unfinished, or skips them |
| `TaskRunMonitor` | `Running` task runs | makes every attempt (first and retries), finishes them |
| `TaskRunAttemptDispatcher` | `Queued` attempts | spawns the command, holds a retry until `retry_delay` passes or a cap frees, or skips |
| `TaskRunAttemptMonitor` | `Running` attempts | waits on the process and finishes the attempt |

Job run path: `Scheduled → Queued → Running → <terminal>`, plus `Scheduled → Skipped` for a run stopped before release. Only the releaser writes `Scheduled → Queued`. It is not a dispatcher: releasing only makes a run eligible. Its skip is the same outcome `JobRunDispatcher` owns one status later, both written through `CRUD::skip_job_run`; they select disjoint statuses, so a run is only ever skipped by one of them.

**Notifications are not the orchestrator's job.** [`job_run_notification`](../entities/references/job_run_notification.md) rows are written at submit, and the [NotificationService](../notifications/SKILL.md) (started by `serve` alongside) reads the status a monitor wrote. Nothing here calls it or knows the table exists — delivery is slow and can fail for hours, which a monitor must never wait on.

## The dispatcher/monitor pattern

Each level — [job run](references/job_run.md), [task run](references/task_run.md), [task run attempt](references/task_run_attempt.md) — has one **dispatcher** and one **monitor**:

- **Dispatcher: held → `Running` or `Skipped`.** Held is `Queued` for a job run and an attempt, `Waiting` for a task run.
- **Monitor: `Running` → a finished status.** Never touches a held row.

**Nothing else writes a run status**, with one exception: `CRUD::delete_job_run` (`job-run delete`, the MCP tool and the web route) tombstones a still-`Scheduled` run as `Deleted`, which no service ever selects.

Two levels start one step earlier, in a status nothing dispatches, so a row written ahead of time stays invisible until something decides its moment has come:

- `job_run` begins `Scheduled`; `JobRunReleaser` moves it on.
- `task_run` begins `Planned`; `JobRunDispatcher::set_to_running` moves it to `Waiting`.

Attempts are only inserted for a `Running` task run, so they start `Queued`, already eligible.

**Status words.** Terminal statuses are shared verbatim across levels; pre-running ones name what the row waits on. `Queued` means waiting for a slot (`max_parallel_runs`, `max_running_attempts`, a named limit); a task run waits on dependencies, so it says `Waiting`.

**`Invalid` cuts across the split deliberately.** It says flowlite cannot account for the row, not what the command did, so both dispatchers and monitors write it, from `Queued` or `Running`. See [Rows flowlite cannot read](#rows-flowlite-cannot-read).

## How they coordinate

No service calls another. Each is a `Service` ([src/poller.rs](../../../src/poller.rs): `name`, `row_context`, `select`, `handle`) driven by its own `Poller` ([poller skill](../poller/SKILL.md)), which owns the loop, error backoff (`[orchestrator] error_backoff_seconds`, default 5) and interval (`poll_interval_seconds`, default 1, read from the `AppConfig` each pass).

Adding a service to `Orchestrator::start` takes **two** lines besides constructing it: `let new_service_wakeup = self.signals.register();` in the registration block at the top, and `Poller::new(Arc::new(new_service), new_service_wakeup, self.app_config.clone()).start()` at the bottom. **Every wake-up is registered before any `Poller` spawns**, because a poller's first pass runs immediately and could publish to signals not yet registered. A service nothing publishes to takes a bare `Notify` instead, like the Scheduler in [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs).

A `Poller` wakes on its `Signals` ([src/signals.rs](../../../src/signals.rs)) wake-up or its interval, whichever comes first. A publish is advisory and carries no data; the interval is the safety net for missed wake-ups and for rows another process wrote (`flowlite job submit` cannot publish). The only real channel between services is the status column.

Consequences:

- **No service filters on the status of the row above it.** Anything that should hold a row back must be written onto that row — which is why `Planned` exists: `JobRunDispatcher` writes `Waiting` onto the task runs rather than `TaskRunDispatcher` asking about the job run.
- **New lifecycle stages belong in a new status-driven poller**, not a call from one service into another.
- **One row's error must not end the loop.** `Poller::run` logs a failing row with its id and moves on; only a failed *select* restarts the loop. New per-row work belongs in `Service::handle`, never in the loop.

**Publish rule:** call `self.signals.publish()` after writing a status, or inserting a row, that another service selects on — never after a data-only write. `TaskRunAttemptMonitor::insert_output` deliberately doesn't: it runs every pass per running attempt and would wake all seven pollers constantly.

**The orchestrator reads runs, never definitions.** `command`, `timeout`, `max_retries`, `retry_delay`, `depends_on`, `env`, `working_dir` come off the `task_run` row (snapshotted from `mem.task` by `CRUD::submit_job`), and `parameters`, `scheduled_at` off `job_run`, so a run executes what it was submitted with. No orchestrator file imports `crate::crud::task` or `crate::crud::job`, or touches `task_dependent`. The one exception is `JobRunDispatcher`'s `is_job_at_max_parallel_runs`, reading `mem.job.max_parallel_runs` — "may I start another?" is a question about the job now. Nothing enforces this: `mem` is attached to every pooled connection.

The only shared memory is `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)) — see [task_run_attempt.md](references/task_run_attempt.md).

## Deriving a status, and why the order matters

**Every service settles a row in two halves.** `derive_next_status` reads the row's world and returns the status it ought to hold; `handle_*` matches it and calls one `set_to_*`, which writes without re-deciding. That makes the decision testable alone (`JobRunMonitor`'s is a pure function of task run statuses).

- **The no-op is a named status**, matched to `Ok(())`: `Scheduled` (releaser), `Queued` (the two dispatchers that hold), `Waiting` (`TaskRunDispatcher`), `Running` (monitors). Otherwise a row left alone on purpose and one nobody decided about look the same.
- **A status nothing claims settles `Invalid`**, logged as a bug. Unreachable while each `derive_next_status` covers every case; it guards the day a status is added without a match arm. Leaving the row would stall the job; settling ends the run, notifies, and frees the slot.
- **A failed read never settles anything.** `Invalid` is terminal, so settling on a transient DB error would be permanent and lose the retries. Every `Err` goes back to `Poller`; `TaskRunAttemptMonitor` first puts its child back in `TaskRunAttemptChildren`.
- **So "nothing claims it" is a value, not an error.** `JobRunMonitor` returns `Option`; `TaskRunDispatcher` and `TaskRunMonitor` return `Result<Option<_>>`, where `None` is undecidable and `Err` is only a failed query or retry insert.

**Where an early return sits is its precedence.** Each check asks only about its own case (repetition over a shared guard). Two rules recur:

- **A real outcome outranks a stop.** `JobRunMonitor` reports a failure over an abort (`Aborted` is its last finished verdict). `TaskRunAttemptMonitor` reports an exit status, then a timeout, ahead of a stop on the same pass — tests `an_exit_status_outranks_a_stop`, `an_exit_status_outranks_a_timeout`, `a_timeout_outranks_a_stop`.
- **An unknown outranks every named verdict**, so `Invalid` is first in `JobRunMonitor` and `TaskRunMonitor`: naming a failure would present a partly unexplained run as explained.

**Dispatchers and monitors place the no-op differently.** A dispatcher checks stopped first, held next, start last, so starting is unconditional; `TaskRunAttemptDispatcher` puts its already-spawned check ahead of even the stop, so a stop cannot mask a command that may be running. A monitor returns `Running` from the "is the work over?" check: first in `JobRunMonitor` (`all_finished`), and in `TaskRunMonitor` for an unfinished attempt and for a failure with retries left. Moving it in a monitor changes which rows finish early; in a dispatcher, which rows start.

`TaskRunMonitor`'s is the one decision that writes: a failed attempt with retries left inserts the next attempt before returning `Running`.

## Stopping a run

A stop is an insert-only `job_run_stop` row, never a status update. `JobRunReleaser`, `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` check for it each pass and finish only what they own: unstarted rows go `Skipped`, an in-flight process is killed and its attempt goes `Aborted`. The job run's status is then derived from its task runs like any other.

The releaser skips a stopped `Scheduled` run itself (with its task runs, via [`CRUD::skip_job_run`](../../../src/crud/multistatements/skip_job_run.rs)) so the stop needn't wait for `scheduled_at`, and so a run that will never run never passes through `Queued`, which means "due".

`Invalid` is not a stop: `is_stopped()` returns false for it, so `JobRunMonitor` never reports a lost run as a stopped one.

**The row's own lifecycle decides `Skipped` vs `Aborted`, not its children.** Never started → `Skipped`; started, however far → `Aborted`. Dispatchers write only `Skipped`, monitors only `Aborted`. So a task run stopped while waiting to retry is `Aborted` even though the unspawned attempt is `Skipped`.

**Child runs live as long as the attempt that submitted them, unless it succeeded.** `TaskRunAttemptMonitor::finish_task_run_attempt` (where every attempt outcome is written) calls `CRUD::stop_child_job_runs` for any status but `Succeeded`, and crash recovery does the same for the attempts it settles `Invalid`. That inserts a stop row for each unfinished run the attempt submitted; their own services act on it, and grandchildren follow in turn. So a `--wait`ing parent's abort stops its child, while a child handed off by an attempt that exited 0 is left to finish. The stop is written before the status, so a failed insert leaves the attempt `Running` with no process and the next pass retries it via the `Invalid` path.

## Rows flowlite cannot read

`Invalid` settles a row whose outcome the program cannot determine (as opposed to failed, timed out or stopped). Writers:

- **A `Running` attempt with no process in `TaskRunAttemptChildren`.** The map holds only this process's children, so this is the restart path (shutdown kills process groups but leaves rows `Running`, since writing statuses would race the pollers). `Orchestrator::recover` handles these before any poller starts — see [Picking up after a crash](#picking-up-after-a-crash).
- **A queued attempt already spawned for** (`started_at` set but never recorded `Running`) — `TaskRunAttemptDispatcher` refuses to run the command twice.
- **The `Ok(_)` fall-throughs** above, one per service.

It propagates: an invalid attempt makes its task run invalid and is **never retried** (flowlite doesn't know what it did); an invalid task run makes its job run invalid and skips its dependents (`TaskRunStatus::Invalid` is in `TaskRunDispatcher::should_skip`'s failure list).

**Why settle rather than raise:** an unsettled row strands everything above it — the job run stays `Running` and holds a `max_parallel_runs` slot for ever; with `max_parallel_runs: 1` the job never runs again without hand-editing the DB.

**Where a raise is still right:** a missing `task_run`/`job_run` row for an attempt, or a dependency with no row. Enforced invariants keep those shut — foreign keys, a run's rows deleted only all together in one transaction (`CRUD::delete_job_runs_with_children`), and YAML refusing an unknown `depends_on`. Convert a raise only when its state can actually occur.

## Picking up after a crash

`Orchestrator::recover` ([src/orchestrator/recovery.rs](../../../src/orchestrator/recovery.rs)) runs once, **awaited before `start`**, and settles every `Running` attempt `Invalid` — at that moment each can only be left by an earlier process. Left to the pollers, `TaskRunAttemptMonitor` would settle them without reading the group id, and their commands would keep running.

It kills the recorded `process_group_id`, but **refuses rather than guesses**: pids are reused, so it kills only if the attempt started after the last boot, and kills nothing if boot time is unknown. Signalling a stranger's process tree is worse than leaking a command.

The status stays `Invalid` even after a successful kill: what the command had done is unknown, and `Aborted` would claim somebody stopped it.

## The three levels

- **[job_run.md](references/job_run.md)** — `JobRunReleaser` / `JobRunDispatcher` / `JobRunMonitor`, and how task run statuses add up to a job run status.
- **[task_run.md](references/task_run.md)** — `TaskRunDispatcher` / `TaskRunMonitor`, dependency gating and the retry loop.
- **[task_run_attempt.md](references/task_run_attempt.md)** — `TaskRunAttemptDispatcher` / `TaskRunAttemptMonitor`, spawning, output, timeout and kill.

Tables, columns and who writes them: [entities skill](../entities/SKILL.md).
