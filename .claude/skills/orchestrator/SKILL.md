---
name: orchestrator
description: High-level map of src/orchestrator/ - the background services that turn job runs into finished task runs, and the dispatcher/monitor pattern they all follow. Use when adding or changing an orchestrator service, or when tracing how a run moves through its statuses.
---

# Orchestrator

The orchestrator is the engine of flowlite: seven background services, all spawned by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which `serve` calls.

It takes over from an existing job run, whatever created it — `job submit`, the web UI, or the [Scheduler](../scheduler/SKILL.md), which is a separate service `serve` starts alongside it and which only ever inserts a run `Scheduled`. Nothing outside the orchestrator touches a run after that: the scheduler never updates one and never deletes one, so every row it writes is one of these services' to settle.

| Service | Polls | Does |
|---|---|---|
| `JobRunReleaser` | `Scheduled` job runs | moves one to `Queued` once its `scheduled_at` has arrived, or skips it — and its task runs — if it was stopped first |
| `JobRunDispatcher` | `Queued` job runs | starts them while `max_parallel_runs` allows — releasing their task runs `Planned` → `Waiting` as it does — holds them when it does not, or skips them |
| `JobRunMonitor` | `Running` job runs | finishes them from their task runs |
| `TaskRunDispatcher` | `Waiting` task runs | starts them once their dependencies have succeeded, holds them while one is unfinished, or skips them |
| `TaskRunMonitor` | `Running` task runs | drives their attempts, retries, finishes them |
| `TaskRunAttemptDispatcher` | `Queued` attempts | spawns their command, holds a retry until its `retry_delay` has passed, or skips them |
| `TaskRunAttemptMonitor` | `Running` attempts | waits on the processes and finishes the attempts |

Every job run is created `Scheduled`, so `JobRunReleaser` is the one service every run passes through before any of the others can see it: the status path is `Scheduled → Queued → Running → <terminal>`, with `Scheduled → Skipped` the one shortcut across it, for a run somebody stopped before it was ever released. `Scheduled → Queued` is owned solely by the releaser — nothing else ever writes it. It is still not a dispatcher or a monitor in the pattern below: releasing a run is not starting one, and a run it releases does not finish, it only becomes eligible to be picked up. Its skip is the exception that proves it, being the same outcome `JobRunDispatcher` owns one status later and written through the same `CRUD::skip_job_run` — the two select on disjoint statuses, so a run is only ever skipped by one of them.

Telling somebody a run broke is **not** one of them, and no service here writes a line about it. A run's [`job_run_notification`](../entities/references/job_run_notification.md) rows are written when it is submitted, and the [NotificationService](../notifications/SKILL.md) — started by `serve` alongside the orchestrator, the way the [Scheduler](../scheduler/SKILL.md) is — reads the status a monitor wrote and decides for itself. Nothing in the orchestrator calls that service, holds a handle to it, or knows the table exists. Delivery is slow and sometimes fails for hours, which is exactly the work a monitor must not be holding while it is meant to be finishing everyone else's runs.

## The dispatcher/monitor pattern

Every run level — [job run](references/job_run.md), [task run](references/task_run.md), [task run attempt](references/task_run_attempt.md) — has exactly one **dispatcher** and one **monitor**, and they own one status transition each:

- **Dispatcher: held → `Running` or `Skipped`.** Starts the row, or skips it because it must not run. The held status it polls is `Queued` for a job run and an attempt, `Waiting` for a task run.
- **Monitor: `Running` → a finished status.** Never touches a held row.

Together they give every row the same shape: held → `Running` → `<finished>`, or held → `Skipped` if it never got going. **Nothing else writes a run status** — no request handler, no CLI command.

Two of the three levels are created one step earlier than that shape starts, in a status nothing dispatches:

- Every `job_run` begins `Scheduled`, and `JobRunReleaser` (the table above) moves it to `Queued` once its `scheduled_at` arrives — or straight to `Skipped`, with its task runs, if it was stopped before that.
- Every `task_run` begins `Planned`, and `JobRunDispatcher::set_to_running` moves it to `Waiting` as it starts the job run it belongs to.

Both exist for the same reason: a row written ahead of time must be invisible to its dispatcher until something decides its moment has come. Attempts have no such stage — they are only ever inserted for a task run already `Running`, so they are created `Queued`, already eligible.

**Why the words differ per level.** Every terminal status is shared verbatim across all three levels, because "failed" means the same thing wherever you read it. The pre-running ones are not, because each level waits on something different, and a status that does not name its wait is how `Queued` came to mean "not due yet" on one row and "start me now" on another. `Queued` is kept only where a row really is waiting for a slot: a job run behind `max_parallel_runs`, an attempt behind `max_running_attempts` or a named limit. A task run waits for its dependencies, which is not a queue, so it says `Waiting`.

**`Invalid` is the exception to both halves of that rule**, and it is deliberate. It says flowlite cannot read the row — not what happened to the work — so it cross-cuts the split rather than fitting in it: a dispatcher writes it (the second terminal one writes, alongside `Skipped`) and so does a monitor, and it is reached from `Queued` and from `Running` alike. Every other status answers "what happened to the command?"; this one answers "can flowlite still account for this row?", and the answer is no. See [Rows flowlite cannot read](#rows-flowlite-cannot-read).

## How they coordinate

All seven services are independent: none calls another. Each is a `Service` ([src/poller.rs](../../../src/poller.rs)) driven by its own `Poller` ([poller skill](../poller/SKILL.md)), which owns the loop — spawning it, restarting it after `[orchestrator] error_backoff_seconds` (5 by default) on a select error, and running the per-row logic against whatever the service selects. A service holds nothing but its own logic: `name`, `row_context`, `select`, `handle`. The shape to copy for a new service is `impl Service for NewService { ... }` plus, in `Orchestrator::start`, a `let` that constructs it alongside the others and **two** lines around that: a `let new_service_wakeup = self.signals.register();` in the registration block at the top, and a `Poller::new(Arc::new(new_service), new_service_wakeup, self.app_config.clone()).start()` at the bottom. They are two lines rather than one because **every wake-up is registered before any `Poller` is spawned** — a poller's first pass runs the moment it is spawned, so inlining `signals.register()` into the `Poller::new` call would let the first service publish to signals the later ones have yet to register. Registering at all is a choice: a service nothing publishes to takes a bare `Notify` instead, the way the [Scheduler](../scheduler/SKILL.md) does in [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs), rather than waking on every unrelated status change for nothing. The third argument is the whole `AppConfig`, not an interval: the poller reads `app_config.orchestrator.poll_interval()` on each pass, which is `[orchestrator] poll_interval_seconds` in config.toml and one second by default.

A `Poller` wakes on whichever comes first: its `Signals` ([src/signals.rs](../../../src/signals.rs)) wake-up, or its interval — `[orchestrator] poll_interval_seconds`, one second by default. The interval is the safety net — it is what notices a `job_run` written by a different process (`flowlite job submit`, which can't publish) and it is what keeps everything moving if a wake-up is ever missed, since a `Signals` publish is advisory and carries no data. The only channel of actual information between services is still the status column of the rows they read and write — a monitor sees the row a dispatcher started because its status is now `Running`, and nothing more; a publish just says "look again," it doesn't say at what.

Three consequences:

- **No service filters on the status of the row above it.** A service selects on one status of its own rows and nothing else, so a status can only gate the row it is written on. Anything that should hold a task run back must be written onto the task run itself — which is precisely what `Planned` is: `JobRunDispatcher` writes `Waiting` onto every task run of the run it starts, rather than `TaskRunDispatcher` asking what each row's job run is doing. Before that status existed, this rule was broken in exactly the way it warns about: the dispatcher polled every `Queued` task run whatever its job run's status, and started the task runs of runs that were not due yet.
- **New lifecycle stages belong in a new status-driven poller**, not in a call from an existing service into another.
- **One row's error must not end the loop.** This is `Poller::run`'s job, not any individual service's: it handles every selected row in a `for` loop that logs a failing row (with its id) and moves on; only a failure to *select* the rows propagates out of the loop, restarting it after the error backoff. A row the service can never handle would otherwise be re-selected by every restarted loop, so nothing else would ever be handled. New per-row work still belongs in `Service::handle`, never in the loop itself.

**The publish rule:** a service calls `self.signals.publish()` after writing a status another service selects on, or after inserting a row another service selects on — never after a data-only write. `TaskRunAttemptMonitor::insert_output`, which appends whatever a running attempt's readers delivered on this pass, is the example worth remembering: it deliberately does not publish, because publishing there would wake all seven pollers at least once a second, per running attempt, and turn the wake-up bus back into the polling loop it replaced.

**The orchestrator reads runs, never definitions.** Every config field a service acts on — `command`, `timeout`, `max_retries`, `retry_delay`, `depends_on`, `env`, `working_dir` — is read off the `task_run` row, the copy `CRUD::submit_job` took out of `mem.task` when the run was created, plus `parameters` and `scheduled_at` off the `job_run` row above it, so a run executes what it was submitted with however the YAML has moved since. No orchestrator file imports `crate::crud::task` or `crate::crud::job`, and none touches `task_dependent`. The single definition read across all seven services is `JobRunDispatcher`'s `is_job_at_max_parallel_runs`, which reads `mem.job.max_parallel_runs` because "may I start another run?" is a question about the job now and is deliberately not snapshotted onto `job_run`. Nothing enforces the rule: `mem` is ATTACHed to every pooled connection, so `mem.task` is one query away from any service that forgets.

The one shared piece of memory is `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)), which the two attempt services use to hand child processes over — see [task_run_attempt.md](references/task_run_attempt.md).

## Deriving a status, and why the order matters

**Every service settles a row in two halves.** `derive_next_status` reads the row's world — a stop row, a dependency set, a child process, the last attempt — and returns the status the row ought to hold. `handle_*` matches on what came back and calls one `set_to_*`, which writes it and trusts the decision rather than re-deriving any part of it. Deciding reads; writing does not decide.

That split is what makes the order legible: `derive_next_status` is a run of early returns, and **where a check sits is its precedence**. It is also what makes the decision testable on its own — `JobRunMonitor`'s is a pure function of the task run statuses, and its tests hand it a list and read the verdict back without touching a table.

**The outcome that writes nothing is a returned status like any other.** `Scheduled` from the releaser, `Queued` from the two dispatchers that hold rows back, `Waiting` from `TaskRunDispatcher`, `Running` from the monitors — each is matched to `Ok(())`. Naming it is the point of the shape: a row left alone on purpose and a row nobody decided about are otherwise the same silence.

**Every match ends in `Ok(_) | Err(_) => set_to_invalid`**, which settles the row `Invalid` and logs that it is a bug. None is reachable while each `derive_next_status` covers every case — they exist for the day a status is added and a match arm is not, which is not hypothetical: it is what both monitors did while `Invalid` itself was being wired in. Leaving the row where it is would stop the job scheduling and say so only in a log; settling ends the run, tells whoever the job named, and lets the next one through.

**`TaskRunAttemptMonitor` is the exception, and the only one holding a live child.** Its `Err` arm puts the process back in `TaskRunAttemptChildren` and returns the error, rather than settling: a failure to derive may be transient, and the process is still there to ask about next pass. Its `Ok(_)` arm kills the process group before settling `Invalid`. Every other service folds `Err` into the invalid arm with the unhandled statuses.

**Read the order of the early returns as part of the logic.** Each check asks only about its own case — a shared guard covering several outcomes reads worse at every call site than the repetition does — so where it sits is what separates it from the others. Two rules recur:

- **A real outcome outranks a stop.** In `JobRunMonitor` a job run with one aborted and one failed task run reports the failure, the part worth acting on, so `Aborted` is the last of its finished verdicts. In `TaskRunAttemptMonitor` a process that had already exited reports its exit status and one past its timeout reports the timeout, ahead of a stop reaching it on the same pass — held by `an_exit_status_outranks_a_stop`, `an_exit_status_outranks_a_timeout` and `a_timeout_outranks_a_stop`.
- **An unknown outranks every named verdict**, so `Invalid` is returned first in `JobRunMonitor` and `TaskRunMonitor`. A job run with one invalid task run and one failed one reports invalid: naming the failure would present an explained result for a run that is partly unexplained.

**Where the no-op sits differs between the dispatchers and the monitors, and the difference is not cosmetic.** A dispatcher returns it before the status that starts work — stopped first, held next, start last — so starting is unconditional and the guard that holds a row back is the whole of the held case. `TaskRunAttemptDispatcher` puts the already-spawned check ahead of even the stop, so a stop cannot mask a command that may already be running. A monitor returns `Running` from the check that asks whether the work is over at all: `JobRunMonitor` returns it first, from the single `all_finished` check that every verdict below it may then assume, and `TaskRunMonitor` returns it for an attempt still owned by the attempt services and again for a failure with retries left. Moving that check in a monitor changes which rows finish early; moving it in a dispatcher changes which rows start.

`TaskRunMonitor`'s is the one decision that also writes: a failed attempt with retries left inserts the next attempt before returning `Running`, since starting one is not itself a status.

See [job_run.md](references/job_run.md) for the full set of returns per service.

## Stopping a run

A stop is an insert-only `job_run_stop` row, never a status update. `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` each check for it on every pass — a signal wake-up or the poll interval, whichever came first — and finish only what they own: rows that never started go `Skipped`, an in-flight process is killed and its attempt goes `Aborted`. A stopped job run's status is therefore derived like any other — from its task runs, once they have all settled.

`JobRunReleaser` checks for a stop too, and settles it itself: `Scheduled` straight to `Skipped`, the run's task runs with it. `JobRunDispatcher` only polls `Queued` rows, so a run stopped before its `scheduled_at` would otherwise sit `Scheduled`, unseen, until its due time. Releasing it to `Queued` early and letting the dispatcher skip it works too, and is what this used to do — but `Queued` says a run is due, so a run stopped in February read as due tonight for as long as the dispatcher's next pass took. Both services write the skip through `CRUD::skip_job_run` ([src/crud/multistatements/skip_job_run.rs](../../../src/crud/multistatements/skip_job_run.rs)), so neither writes half of the pair, and they cannot race for a row: `Scheduled` is the releaser's, `Queued` is the dispatcher's.

That split is about a stop, so `Invalid` sits outside it: it is not a stop at all, and `is_stopped()` returns false for it precisely so `JobRunMonitor` cannot report a run flowlite lost track of as one somebody stopped.

**The row's own lifecycle decides `Skipped` vs `Aborted`, not what its children report.** A row that never started is `Skipped`; a row that had started is `Aborted`, however far it had got — so the dispatchers write `Skipped` and never `Aborted`, and the monitors write `Aborted` and never `Skipped`. That is why a task run stopped while waiting to retry is `Aborted` even though the attempt that never spawned is `Skipped`: the task run had already run once and left output, and `Skipped` would say nothing ran.

## Rows flowlite cannot read

`Invalid` is what settles a row whose outcome the program cannot determine — as opposed to one whose command failed, timed out or was stopped. Two things write it:

- **A `Running` attempt with no process in `TaskRunAttemptChildren`.** The map holds only processes *this* run of the program spawned, so this is the restart path: shutdown kills the process groups and leaves the attempt rows `Running` on purpose, because writing statuses there would race the pollers. Every restart with work in flight produces one per running attempt, and a `SIGKILL`ed flowlite produces them with the process tree still alive. `Orchestrator::recover` deals with those before any poller starts — see [Picking up after a crash](#picking-up-after-a-crash).
- **The `Ok(_) | Err(_)` fall-throughs** described above, one per service.

It then propagates: an invalid attempt makes its task run invalid and is **never retried** — flowlite does not know what that attempt did, so another would be guessing it left nothing behind — and an invalid task run makes its job run invalid and skips everything downstream of it, `TaskRunStatus::Invalid` being in `TaskRunDispatcher::should_skip`'s failure list.

**Why settle at all rather than raise.** The status column is the only channel between the services, so a row that never settles strands the whole stack above it: the task run stays `Running`, so the job run does, so it holds one of its job's parallel slots for ever — with `max_parallel_runs: 1`, one crashed attempt stops that job running again, and the only repair is editing the database by hand.

**Where a raise is still right.** Two states look like this and are not: an attempt whose `task_run` or `job_run` row is missing, and a task run naming a dependency with no row. Both are held shut by an enforced invariant — foreign keys with no `DELETE` anywhere in the codebase, and the YAML layer refusing a job whose task depends on a name that is not a task of that job — so their raises never fire and strand nothing. Convert a raise only when the state it guards can actually occur.

## Picking up after a crash

`Orchestrator::recover` ([src/orchestrator/recovery.rs](../../../src/orchestrator/recovery.rs)) runs once, **awaited before `start`**, and settles every attempt still marked `Running` — which at that moment can only be one an earlier run of the program left, since `TaskRunAttemptChildren` is empty until a dispatcher fills it. Left to the pollers instead, `TaskRunAttemptMonitor` would settle those rows without ever reading the group id, and their commands would go on running.

Each attempt records the `process_group_id` it spawned, so the recovery pass can kill a command a crash left behind. **It refuses rather than guesses**: a group id is a number the kernel hands out again, so a kill happens only where the attempt started *after* the machine last booted — a pid from before that belongs to nothing this program spawned. An unknown boot time kills nothing. Signalling a stranger's process tree is a worse outcome than leaking a command, so the guard fails closed.

The status is still `Invalid` and not `Aborted` even when the kill succeeds: what the command had done before it died is exactly what nobody knows, and `Aborted` would claim somebody stopped the run.

## The three levels

- **[job_run.md](references/job_run.md)** — `JobRunDispatcher` / `JobRunMonitor`, and how task run statuses add up to a job run status.
- **[task_run.md](references/task_run.md)** — `TaskRunDispatcher` / `TaskRunMonitor`, dependency gating and the retry loop.
- **[task_run_attempt.md](references/task_run_attempt.md)** — `TaskRunAttemptDispatcher` / `TaskRunAttemptMonitor`, process spawning, output, timeout and kill.

For the tables themselves — every column, who writes it and who reads it — see the [entities skill](../entities/SKILL.md).
