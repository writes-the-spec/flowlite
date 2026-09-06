---
name: orchestrator
description: High-level map of src/orchestrator/ - the background services that turn job runs into finished task runs, and the dispatcher/monitor pattern they all follow. Use when adding or changing an orchestrator service, or when tracing how a run moves through its statuses.
---

# Orchestrator

The orchestrator is the engine of flowlite: six background services, all spawned by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which `serve` calls.

It takes over from an existing job run, whatever created it — `job submit`, the web UI, or the [Scheduler](../scheduler/SKILL.md), which is a separate service `serve` starts alongside it.

| Service | Polls | Does |
|---|---|---|
| `JobRunDispatcher` | `Pending` job runs | starts them as `max_parallel_runs` allows, or skips them |
| `JobRunMonitor` | `Running` job runs | finishes them from their task runs |
| `TaskRunDispatcher` | `Pending` task runs | starts or skips them |
| `TaskRunMonitor` | `Running` task runs | drives their attempts, retries, finishes them |
| `TaskRunAttemptDispatcher` | `Pending` attempts | spawns their command, or skips them |
| `TaskRunAttemptMonitor` | `Running` attempts | waits on the processes and finishes the attempts |

## The dispatcher/monitor pattern

Every run level — [job run](references/job_run.md), [task run](references/task_run.md), [task run attempt](references/task_run_attempt.md) — has exactly one **dispatcher** and one **monitor**, and they own one status transition each:

- **Dispatcher: `Pending` → `Running` or `Skipped`.** Starts the row, or skips it because it must not run.
- **Monitor: `Running` → a finished status.** Never touches a `Pending` row.

Together they give every row the same shape: `Pending → Running → <finished>`, or `Pending → Skipped` if it never got going. **Nothing else writes a run status** — no request handler, no CLI command.

## How they coordinate

All six services are independent: none calls another. Each is a `Service` ([src/poller.rs](../../../src/poller.rs)) driven by its own `Poller`, which owns the loop — spawning it, restarting it after 5s on a select error, and running the per-row logic against whatever the service selects. A service holds nothing but its own logic: `name`, `row_context`, `select`, `handle`. The shape to copy for a new service is `impl Service for NewService { ... }` plus one `Poller::new(Arc::new(new_service), signals.register(), Duration::from_secs(1)).start()` line in `Orchestrator::start`.

A `Poller` wakes on whichever comes first: its `Signals` ([src/signals.rs](../../../src/signals.rs)) wake-up, or its one-second interval. The interval is the safety net — it is what notices a `job_run` written by a different process (`flowlite job submit`, which can't publish) and it is what keeps everything moving if a wake-up is ever missed, since a `Signals` publish is advisory and carries no data. The only channel of actual information between services is still the status column of the rows they read and write — a monitor sees the row a dispatcher started because its status is now `Running`, and nothing more; a publish just says "look again," it doesn't say at what.

Three consequences:

- **No service filters on the status of the row above it.** `TaskRunDispatcher` polls every `Pending` task run whatever its job run's status, so a status can only gate the row it is written on. Anything that should stop a task run must be written onto the task run itself.
- **New lifecycle stages belong in a new status-driven poller**, not in a call from an existing service into another.
- **One row's error must not end the loop.** This is `Poller::run`'s job, not any individual service's: it handles every selected row in a `for` loop that logs a failing row (with its id) and moves on; only a failure to *select* the rows propagates out of the loop, restarting it after 5s. A row the service can never handle would otherwise be re-selected by the restarted loop every 5s, so nothing else would ever be handled. New per-row work still belongs in `Service::handle`, never in the loop itself.

**The publish rule:** a service calls `self.signals.publish()` after writing a status another service selects on, or after inserting a row another service selects on — never after a data-only write. `TaskRunAttemptMonitor::update_task_run_attempt_output`, which rewrites an attempt's accumulated stdout/stderr on every poll pass it is still running, is the example worth remembering: it deliberately does not publish, because publishing there would wake all six pollers at least once a second, per running attempt, and turn the wake-up bus back into the polling loop it replaced.

The one shared piece of memory is `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)), which the two attempt services use to hand child processes over — see [task_run_attempt.md](references/task_run_attempt.md).

## Stopping a run

A stop is an insert-only `job_run_stop` row, never a status update. `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` each check for it on every pass — a signal wake-up or the one-second interval, whichever came first — and finish only what they own: rows that never started go `Skipped`, an in-flight process is killed and its attempt goes `Aborted`. A stopped job run's status is therefore derived like any other — from its task runs, once they have all settled.

**The row's own lifecycle decides `Skipped` vs `Aborted`, not what its children report.** A row that never started is `Skipped`; a row that had started is `Aborted`, however far it had got — so the dispatchers write `Skipped` and never `Aborted`, and the monitors write `Aborted` and never `Skipped`. That is why a task run stopped while waiting to retry is `Aborted` even though the attempt that never spawned is `Skipped`: the task run had already run once and left output, and `Skipped` would say nothing ran.

## The three levels

- **[job_run.md](references/job_run.md)** — `JobRunDispatcher` / `JobRunMonitor`, and how task run statuses add up to a job run status.
- **[task_run.md](references/task_run.md)** — `TaskRunDispatcher` / `TaskRunMonitor`, dependency gating and the retry loop.
- **[task_run_attempt.md](references/task_run_attempt.md)** — `TaskRunAttemptDispatcher` / `TaskRunAttemptMonitor`, process spawning, output, timeout and kill.

For the entities themselves see the [job](../job/SKILL.md) and [task](../task/SKILL.md) skills.
