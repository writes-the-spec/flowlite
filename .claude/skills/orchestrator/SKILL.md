---
name: orchestrator
description: High-level map of src/orchestrator/ - the background services that turn job runs into finished task runs, and the dispatcher/monitor pattern they all follow. Use when adding or changing an orchestrator service, or when tracing how a run moves through its statuses.
---

# Orchestrator

The orchestrator is the engine of flowlite: six background services, all spawned by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which `serve` calls.

It takes over from an existing job run, whatever created it — `job submit`, the web UI, or the [Scheduler](../scheduler/SKILL.md), which is a separate service `serve` starts alongside it.

| Service | Polls | Does |
|---|---|---|
| `JobRunDispatcher` | `Pending` job runs | starts them while `max_parallel_runs` allows, holds them when it doesn't, or skips them |
| `JobRunMonitor` | `Running` job runs | finishes them from their task runs |
| `TaskRunDispatcher` | `Pending` task runs | starts them once their dependencies have succeeded, holds them while one is unfinished, or skips them |
| `TaskRunMonitor` | `Running` task runs | drives their attempts, retries, finishes them |
| `TaskRunAttemptDispatcher` | `Pending` attempts | spawns their command, holds a retry until its `retry_delay` has passed, or skips them |
| `TaskRunAttemptMonitor` | `Running` attempts | waits on the processes and finishes the attempts |

## The dispatcher/monitor pattern

Every run level — [job run](references/job_run.md), [task run](references/task_run.md), [task run attempt](references/task_run_attempt.md) — has exactly one **dispatcher** and one **monitor**, and they own one status transition each:

- **Dispatcher: `Pending` → `Running` or `Skipped`.** Starts the row, or skips it because it must not run.
- **Monitor: `Running` → a finished status.** Never touches a `Pending` row.

Together they give every row the same shape: `Pending → Running → <finished>`, or `Pending → Skipped` if it never got going. **Nothing else writes a run status** — no request handler, no CLI command.

## How they coordinate

All six services are independent: none calls another. Each is a `Service` ([src/poller.rs](../../../src/poller.rs)) driven by its own `Poller`, which owns the loop — spawning it, restarting it after 5s on a select error, and running the per-row logic against whatever the service selects. A service holds nothing but its own logic: `name`, `row_context`, `select`, `handle`. The shape to copy for a new service is `impl Service for NewService { ... }` plus, in `Orchestrator::start`, a `let` that constructs it alongside the others and **two** lines around that: a `let new_service_wakeup = self.signals.register();` in the registration block at the top, and a `Poller::new(Arc::new(new_service), new_service_wakeup, POLL_INTERVAL).start()` at the bottom. They are two lines rather than one because **every wake-up is registered before any `Poller` is spawned** — a poller's first pass runs the moment it is spawned, so inlining `signals.register()` into the `Poller::new` call would let the first service publish to signals the later ones have yet to register. Registering at all is a choice: a service nothing publishes to takes a bare `Notify` instead, the way the [Scheduler](../scheduler/SKILL.md) does in [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs), rather than waking on every unrelated status change for nothing. `POLL_INTERVAL` is the one-second constant in [src/poller.rs](../../../src/poller.rs), which every call site uses.

A `Poller` wakes on whichever comes first: its `Signals` ([src/signals.rs](../../../src/signals.rs)) wake-up, or its one-second interval. The interval is the safety net — it is what notices a `job_run` written by a different process (`flowlite job submit`, which can't publish) and it is what keeps everything moving if a wake-up is ever missed, since a `Signals` publish is advisory and carries no data. The only channel of actual information between services is still the status column of the rows they read and write — a monitor sees the row a dispatcher started because its status is now `Running`, and nothing more; a publish just says "look again," it doesn't say at what.

Three consequences:

- **No service filters on the status of the row above it.** `TaskRunDispatcher` polls every `Pending` task run whatever its job run's status, so a status can only gate the row it is written on. Anything that should stop a task run must be written onto the task run itself.
- **New lifecycle stages belong in a new status-driven poller**, not in a call from an existing service into another.
- **One row's error must not end the loop.** This is `Poller::run`'s job, not any individual service's: it handles every selected row in a `for` loop that logs a failing row (with its id) and moves on; only a failure to *select* the rows propagates out of the loop, restarting it after 5s. A row the service can never handle would otherwise be re-selected by the restarted loop every 5s, so nothing else would ever be handled. New per-row work still belongs in `Service::handle`, never in the loop itself.

**The publish rule:** a service calls `self.signals.publish()` after writing a status another service selects on, or after inserting a row another service selects on — never after a data-only write. `TaskRunAttemptMonitor::update_task_run_attempt_output`, which rewrites an attempt's accumulated stdout/stderr on every poll pass it is still running, is the example worth remembering: it deliberately does not publish, because publishing there would wake all six pollers at least once a second, per running attempt, and turn the wake-up bus back into the polling loop it replaced.

**The orchestrator reads runs, never definitions.** Every config field a service acts on — `command`, `timeout`, `max_retries`, `retry_delay`, `depends_on` — is read off the `task_run` row, the copy `CRUD::submit_job` took out of `mem.task` when the run was created, so a run executes what it was submitted with however the YAML has moved since. No orchestrator file imports `crate::crud::task` or `crate::crud::job`, and none touches `task_dependent`. The single definition read in all six services is `JobRunDispatcher`'s `is_job_at_max_parallel_runs`, which reads `mem.job.max_parallel_runs` because "may I start another run?" is a question about the job now and is deliberately not snapshotted onto `job_run`. Nothing enforces the rule: `mem` is ATTACHed to every pooled connection, so `mem.task` is one query away from any service that forgets.

The one shared piece of memory is `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)), which the two attempt services use to hand child processes over — see [task_run_attempt.md](references/task_run_attempt.md).

## The settle chain, and why its order matters

Every service settles a row by asking its outcomes in order — `settle_as_*` in the dispatchers, `settle_for_*` in the monitors — each returning whether it is what happened, and bailing past the last one rather than returning quietly. Naming the outcome that writes nothing is the point of the shape: a row left alone on purpose and a row nobody handled are otherwise the same silence.

**Read the call order as part of the logic.** Each guard asks only about its own case, deliberately — a shared guard covering several outcomes reads worse at every call site than the repetition does — so where a call sits is what separates it from the others, and the compiler pins it in exactly one place. The test suite pins three of the six chains. Three rules recur:

- **Where the outcome that leaves a row alone belongs depends on how the ones around it are guarded, and it is load-bearing in exactly one of the three monitors.** In `JobRunMonitor`, `settle_for_failed`, `settle_for_timed_out` and `settle_for_aborted` each re-ask `all_finished` for themselves, so none of them can finish a job run whose work is still going; in `TaskRunMonitor` the guards are exclusive on the last attempt's single status. In both, `settle_for_running` claims what it should wherever it is placed — that redundancy is what makes finishing safe, not the position. In `TaskRunAttemptMonitor`, `settle_for_running` guards nothing at all: it drains the output, puts the child back and returns `true` unconditionally, so it **has to be asked last**. Move it up on the strength of a "leave the row alone first" rule and it claims every running attempt before exit status, timeout and abort are asked, and no attempt ever finishes. Read a chain's guards before moving a line in it.
- **All three dispatchers read skipped, pending, running.** The row that can never run is claimed first, the one that is only waiting next, and starting it comes last — so a dispatcher can start a row unconditionally, the guard that holds it back living in `settle_as_pending` where it is the whole of that outcome. `TaskRunDispatcher::settle_as_running` is the one that still asks a question of its own, because it reloads the dependencies and a failure between the two loads must not be started.
- **Outcomes that can match at once are ranked deliberately, and both monitors that rank them use one rule: a real outcome outranks a stop, so `settle_for_aborted` is asked last.** In `JobRunMonitor` that makes a job run with one aborted and one failed task run report the failure, the part worth acting on. In `TaskRunAttemptMonitor` it makes a process that had already exited report its exit status, and one past its timeout report the timeout, rather than either being recorded as killed.

What stops a reorder differs per chain, and three of the six are held by tests:

- **`JobRunMonitor`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` are pinned.** Every adjacent swap in those three fails at least one test. They are chain tests: they insert rows, run `Service::handle`, and read the settled status back, so what a chain wrote is read from the table the next poll pass would read it from. Build one with `TestDb` ([src/test_support.rs](../../../src/test_support.rs), `cfg(test)` only), which hands each test its own SQLite file in a temp directory, plus one `TaskRunAttemptChildren` so a test can give the attempt monitor a real process.
- **`JobRunDispatcher` and `TaskRunDispatcher` are not**, and both have the load-bearing skipped-pending-running order the bullet above describes. Reordering either compiles and passes the suite.
- **`TaskRunMonitor`'s order carries nothing**, so reordering it is harmless by design and its tests pass either way. That is a property of its exclusive guards, not a gap.

Two things the tests do not hold, for different reasons:

- **The compiler holds `settle_for_running` last in `TaskRunAttemptMonitor`**, not a test: it takes the child by value and the three outcomes above it borrow it, so moving it up fails to compile rather than silently claiming every running attempt. Restore that guarantee if you ever change it to borrow.
- **No test can catch the redundant `all_finished` in `JobRunMonitor`'s three failure outcomes on its own.** `settle_for_running` is asked first and claims the unfinished row either way, so only losing both mechanisms is detectable.

See [job_run.md](references/job_run.md) for the full ladder.

## Stopping a run

A stop is an insert-only `job_run_stop` row, never a status update. `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` each check for it on every pass — a signal wake-up or the one-second interval, whichever came first — and finish only what they own: rows that never started go `Skipped`, an in-flight process is killed and its attempt goes `Aborted`. A stopped job run's status is therefore derived like any other — from its task runs, once they have all settled.

**The row's own lifecycle decides `Skipped` vs `Aborted`, not what its children report.** A row that never started is `Skipped`; a row that had started is `Aborted`, however far it had got — so the dispatchers write `Skipped` and never `Aborted`, and the monitors write `Aborted` and never `Skipped`. That is why a task run stopped while waiting to retry is `Aborted` even though the attempt that never spawned is `Skipped`: the task run had already run once and left output, and `Skipped` would say nothing ran.

## The three levels

- **[job_run.md](references/job_run.md)** — `JobRunDispatcher` / `JobRunMonitor`, and how task run statuses add up to a job run status.
- **[task_run.md](references/task_run.md)** — `TaskRunDispatcher` / `TaskRunMonitor`, dependency gating and the retry loop.
- **[task_run_attempt.md](references/task_run_attempt.md)** — `TaskRunAttemptDispatcher` / `TaskRunAttemptMonitor`, process spawning, output, timeout and kill.

For the tables themselves — every column, who writes it and who reads it — see the [db-objects skill](../db-objects/SKILL.md).
