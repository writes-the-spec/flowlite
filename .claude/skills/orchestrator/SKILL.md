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

Telling somebody a run broke is **not** one of them, and no service here writes a line about it. A run's [`job_run_notification`](../entities/references/job_run_notification.md) rows are written when it is submitted, and the [NotificationService](../notifications/SKILL.md) — started by `serve` alongside the orchestrator, the way the [Scheduler](../scheduler/SKILL.md) is — reads the status a monitor wrote and decides for itself. Nothing in the orchestrator calls that service, holds a handle to it, or knows the table exists. Delivery is slow and sometimes fails for hours, which is exactly the work a monitor must not be holding while it is meant to be finishing everyone else's runs.

## The dispatcher/monitor pattern

Every run level — [job run](references/job_run.md), [task run](references/task_run.md), [task run attempt](references/task_run_attempt.md) — has exactly one **dispatcher** and one **monitor**, and they own one status transition each:

- **Dispatcher: `Pending` → `Running` or `Skipped`.** Starts the row, or skips it because it must not run.
- **Monitor: `Running` → a finished status.** Never touches a `Pending` row.

Together they give every row the same shape: `Pending → Running → <finished>`, or `Pending → Skipped` if it never got going. **Nothing else writes a run status** — no request handler, no CLI command.

**`Invalid` is the exception to both halves of that rule**, and it is deliberate. It says flowlite cannot read the row — not what happened to the work — so it cross-cuts the split rather than fitting in it: a dispatcher writes it (the second terminal one writes, alongside `Skipped`) and so does a monitor, and it is reached from `Pending` and from `Running` alike. Every other status answers "what happened to the command?"; this one answers "can flowlite still account for this row?", and the answer is no. See [Rows flowlite cannot read](#rows-flowlite-cannot-read).

## How they coordinate

All six services are independent: none calls another. Each is a `Service` ([src/poller.rs](../../../src/poller.rs)) driven by its own `Poller` ([poller skill](../poller/SKILL.md)), which owns the loop — spawning it, restarting it after `[orchestrator] error_backoff_seconds` (5 by default) on a select error, and running the per-row logic against whatever the service selects. A service holds nothing but its own logic: `name`, `row_context`, `select`, `handle`. The shape to copy for a new service is `impl Service for NewService { ... }` plus, in `Orchestrator::start`, a `let` that constructs it alongside the others and **two** lines around that: a `let new_service_wakeup = self.signals.register();` in the registration block at the top, and a `Poller::new(Arc::new(new_service), new_service_wakeup, self.app_config.clone()).start()` at the bottom. They are two lines rather than one because **every wake-up is registered before any `Poller` is spawned** — a poller's first pass runs the moment it is spawned, so inlining `signals.register()` into the `Poller::new` call would let the first service publish to signals the later ones have yet to register. Registering at all is a choice: a service nothing publishes to takes a bare `Notify` instead, the way the [Scheduler](../scheduler/SKILL.md) does in [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs), rather than waking on every unrelated status change for nothing. The third argument is the whole `AppConfig`, not an interval: the poller reads `app_config.orchestrator.poll_interval()` on each pass, which is `[orchestrator] poll_interval_seconds` in config.toml and one second by default.

A `Poller` wakes on whichever comes first: its `Signals` ([src/signals.rs](../../../src/signals.rs)) wake-up, or its interval — `[orchestrator] poll_interval_seconds`, one second by default. The interval is the safety net — it is what notices a `job_run` written by a different process (`flowlite job submit`, which can't publish) and it is what keeps everything moving if a wake-up is ever missed, since a `Signals` publish is advisory and carries no data. The only channel of actual information between services is still the status column of the rows they read and write — a monitor sees the row a dispatcher started because its status is now `Running`, and nothing more; a publish just says "look again," it doesn't say at what.

Three consequences:

- **No service filters on the status of the row above it.** `TaskRunDispatcher` polls every `Pending` task run whatever its job run's status, so a status can only gate the row it is written on. Anything that should stop a task run must be written onto the task run itself.
- **New lifecycle stages belong in a new status-driven poller**, not in a call from an existing service into another.
- **One row's error must not end the loop.** This is `Poller::run`'s job, not any individual service's: it handles every selected row in a `for` loop that logs a failing row (with its id) and moves on; only a failure to *select* the rows propagates out of the loop, restarting it after the error backoff. A row the service can never handle would otherwise be re-selected by every restarted loop, so nothing else would ever be handled. New per-row work still belongs in `Service::handle`, never in the loop itself.

**The publish rule:** a service calls `self.signals.publish()` after writing a status another service selects on, or after inserting a row another service selects on — never after a data-only write. `TaskRunAttemptMonitor::insert_output`, which appends whatever a running attempt's readers delivered on this pass, is the example worth remembering: it deliberately does not publish, because publishing there would wake all six pollers at least once a second, per running attempt, and turn the wake-up bus back into the polling loop it replaced.

**The orchestrator reads runs, never definitions.** Every config field a service acts on — `command`, `timeout`, `max_retries`, `retry_delay`, `depends_on`, `env`, `working_dir` — is read off the `task_run` row, the copy `CRUD::submit_job` took out of `mem.task` when the run was created, plus `parameters` and `scheduled_at` off the `job_run` row above it, so a run executes what it was submitted with however the YAML has moved since. No orchestrator file imports `crate::crud::task` or `crate::crud::job`, and none touches `task_dependent`. The single definition read in all six services is `JobRunDispatcher`'s `is_job_at_max_parallel_runs`, which reads `mem.job.max_parallel_runs` because "may I start another run?" is a question about the job now and is deliberately not snapshotted onto `job_run`. Nothing enforces the rule: `mem` is ATTACHed to every pooled connection, so `mem.task` is one query away from any service that forgets.

The one shared piece of memory is `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)), which the two attempt services use to hand child processes over — see [task_run_attempt.md](references/task_run_attempt.md).

## The settle chain, and why its order matters

Every service settles a row by asking its outcomes in order — `settle_as_*` in the dispatchers, `settle_for_*` in the monitors — each returning whether it is what happened, and handling the row that fell past the last one rather than returning quietly. Naming the outcome that writes nothing is the point of the shape: a row left alone on purpose and a row nobody handled are otherwise the same silence.

**Five of the six chains end in `settle_unclaimed`, which settles the row `Invalid` and logs why.** None can be reached today — each chain's last rung claims unconditionally, and both monitor ladders cover every status — so they exist for the day a status is added and a rung is not. That day is not hypothetical: it is what both monitors did while `Invalid` itself was being wired in. A bail there would leave the row where it is, which stops the job scheduling and says so only in a log; settling ends the run, tells whoever the job named, and lets the next one through. `TaskRunAttemptMonitor`'s kills the process group first, being the only one holding a live child.

**`TaskRunDispatcher` is the sixth, and it still bails.** Its chain can fall through on an ordinary state — `settle_as_pending` and `settle_as_running` load the dependencies separately, so one failing between the two loads reaches the end — and the next pass settles it correctly. Making that terminal would turn a transient interleaving into a permanent `Invalid`, which is the same reason a SQLite error must stay an error.

**Read the call order as part of the logic.** Each guard asks only about its own case, deliberately — a shared guard covering several outcomes reads worse at every call site than the repetition does — so where a call sits is what separates it from the others, and the compiler pins it in exactly one place. The test suite pins three of the six chains. Three rules recur:

- **Every chain ends with the outcome that guards nothing of its own**, which is what gives all three dispatchers `skipped, pending, running` and all three monitors `succeeded, failed, timed out, aborted, running` — with `invalid` ahead of all of them in the two upper monitors. In a dispatcher that last outcome is `settle_as_running`: the row that can never run is claimed first, the one that is only waiting next, and starting it comes last — so a dispatcher can start a row unconditionally, the guard that holds it back living in `settle_as_pending` where it is the whole of that outcome. `TaskRunDispatcher::settle_as_running` is the one that still asks a question of its own, because it reloads the dependencies and a failure between the two loads must not be started. In a monitor it is `settle_for_running`, and in `TaskRunAttemptMonitor` the placement is forced: it drains the output, puts the child back and returns `true` unconditionally, so moving it up would claim every running attempt before succeeded, failed, timeout and abort are asked, and no attempt would ever finish.
- **The two upper monitors ask `settle_for_running` last to read like that one, and in `JobRunMonitor` that trades a redundancy for a tested guard.** Asked first, as it used to be, an unfinished job run was held open twice — by the position and by the `all_finished` that `settle_for_failed`, `settle_for_timed_out` and `settle_for_aborted` each re-ask. Asked last, those guards are the only mechanism, which is the point: the redundancy was untestable (dropping one guard alone still passed), and now dropping one fails `a_failure_does_not_finish_a_job_run_whose_work_is_still_going`. `TaskRunMonitor` gives up nothing, its guards being exclusive on the last attempt's single status. Read a chain's guards before moving a line in it.
- **An unknown outranks every named verdict, so `settle_for_invalid` is first** in `JobRunMonitor` and `TaskRunMonitor`. A job run with one invalid task run and one failed one reports invalid: naming the failure would present an explained result for a run that is partly unexplained. It still re-asks `all_finished` like the failure outcomes below it — outranking decides which *finished* verdict wins, not whether the run has finished.
- **Outcomes that can match at once are ranked deliberately, and both monitors that rank them use one rule: a real outcome outranks a stop, so `settle_for_aborted` is the last of the finished outcomes.** In `JobRunMonitor` that makes a job run with one aborted and one failed task run report the failure, the part worth acting on. In `TaskRunAttemptMonitor` it makes a process that had already exited report its exit status, and one past its timeout report the timeout, rather than either being recorded as killed. Its `settle_for_succeeded` and `settle_for_failed` each ask `try_wait` for themselves rather than sharing one exit-status call, so both statuses read as their own line of the ladder — `try_wait` caches the status it reaped, so the second ask is a repeated question, not a race.

What stops a reorder differs per chain, and three of the six are held by tests:

- **`JobRunMonitor`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` are pinned**, though not every line of each: in `JobRunMonitor` it is the relative order of the three failure outcomes, in `TaskRunAttemptMonitor` every adjacent swap but `settle_for_succeeded`/`settle_for_failed`, which are exclusive on the same exit status, and in `TaskRunAttemptDispatcher` every adjacent swap. They are chain tests: they insert rows, run `Service::handle`, and read the settled status back, so what a chain wrote is read from the table the next poll pass would read it from. Build one with `TestDb` ([src/test_support.rs](../../../src/test_support.rs), `cfg(test)` only), which hands each test its own SQLite file in a temp directory, plus one `TaskRunAttemptChildren` so a test can give the attempt monitor a real process.
- **`JobRunDispatcher` and `TaskRunDispatcher` are not**, and both have the load-bearing skipped-pending-running order the bullet above describes. Reordering either compiles and passes the suite.
- **`TaskRunMonitor`'s order carries nothing**, so reordering it is harmless by design and its tests pass either way. That is a property of its exclusive guards, not a gap.

Two things the tests do not hold, for different reasons:

- **The compiler holds `settle_for_running` last in `TaskRunAttemptMonitor`**, not a test: it takes the child by value and the four outcomes above it borrow it, so moving it up fails to compile rather than silently claiming every running attempt. Restore that guarantee if you ever change it to borrow.
- **`settle_for_running`'s position in `JobRunMonitor` is not pinned either way.** Asked anywhere below `settle_for_succeeded` it claims the same rows, and the suite still passes with it back at second. What the tests hold is the `all_finished` guards that asking it last leaves load-bearing.

See [job_run.md](references/job_run.md) for the full ladder.

## Stopping a run

A stop is an insert-only `job_run_stop` row, never a status update. `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` each check for it on every pass — a signal wake-up or the poll interval, whichever came first — and finish only what they own: rows that never started go `Skipped`, an in-flight process is killed and its attempt goes `Aborted`. A stopped job run's status is therefore derived like any other — from its task runs, once they have all settled.

That split is about a stop, so `Invalid` sits outside it: it is not a stop at all, and `is_stopped()` returns false for it precisely so `JobRunMonitor::settle_for_aborted` cannot report a run flowlite lost track of as one somebody stopped.

**The row's own lifecycle decides `Skipped` vs `Aborted`, not what its children report.** A row that never started is `Skipped`; a row that had started is `Aborted`, however far it had got — so the dispatchers write `Skipped` and never `Aborted`, and the monitors write `Aborted` and never `Skipped`. That is why a task run stopped while waiting to retry is `Aborted` even though the attempt that never spawned is `Skipped`: the task run had already run once and left output, and `Skipped` would say nothing ran.

## Rows flowlite cannot read

`Invalid` is what settles a row whose outcome the program cannot determine — as opposed to one whose command failed, timed out or was stopped. Two things write it:

- **A `Running` attempt with no process in `TaskRunAttemptChildren`.** The map holds only processes *this* run of the program spawned, so this is the restart path: shutdown kills the process groups and leaves the attempt rows `Running` on purpose, because writing statuses there would race the pollers. Every restart with work in flight produces one per running attempt, and a `SIGKILL`ed flowlite produces them with the process tree still alive. `Orchestrator::recover` deals with those before any poller starts — see [Picking up after a crash](#picking-up-after-a-crash).
- **The five `settle_unclaimed` fall-throughs** described above.

It then propagates: an invalid attempt makes its task run invalid and is **never retried** — flowlite does not know what that attempt did, so another would be guessing it left nothing behind — and an invalid task run makes its job run invalid and skips everything downstream of it, `TaskRunStatus::Invalid` being in `TaskRunDispatcher::settle_as_skipped`'s failure list.

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
