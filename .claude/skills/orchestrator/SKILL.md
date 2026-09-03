---
name: orchestrator
description: High-level map of src/orchestrator/ - the background services that turn job runs into finished task runs, and the dispatcher/monitor pattern they all follow. Use when adding or changing an orchestrator service, or when tracing how a run moves through its statuses.
---

# Orchestrator

The orchestrator is the engine of flowlite: six background services, all spawned by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which `serve` calls.

It takes over from an existing job run, whatever created it — `job submit`, the web UI, or the [Scheduler](../scheduler/SKILL.md), which is a separate service `serve` starts alongside it.

| Service | Polls | Does |
|---|---|---|
| `JobRunDispatcher` | `Pending` job runs | starts or skips them |
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

All six services are independent: each spawns its own once-a-second `tokio` loop, polls its own table, and **never calls another service**. The only channel between them is the status column of the rows they read and write — a monitor sees the row a dispatcher started because its status is now `Running`, and nothing more.

Two consequences:

- **No service filters on the status of the row above it.** `TaskRunDispatcher` polls every `Pending` task run whatever its job run's status, so a status can only gate the row it is written on. Anything that should stop a task run must be written onto the task run itself.
- **New lifecycle stages belong in a new status-driven poller**, not in a call from an existing service into another.
- **One row's error must not end the loop.** Every service handles its rows in a `for` loop that logs a failing row (with its id) and moves on; only a failure to *select* the rows propagates, restarting the loop after 5s. A row the service can never handle would otherwise be re-selected by the restarted loop every 5s, so nothing else would ever be handled. Keep new per-row work inside the `handle_*` function the loop calls, not in the loop body.

The one shared piece of memory is `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)), which the two attempt services use to hand child processes over — see [task_run_attempt.md](references/task_run_attempt.md).

## Stopping a run

A stop is an insert-only `job_run_stop` row, never a status update. `JobRunDispatcher`, `TaskRunDispatcher`, `TaskRunAttemptDispatcher` and `TaskRunAttemptMonitor` each check for it on every tick and finish only what they own: rows that never started go `Skipped`, an in-flight process is killed and its attempt goes `Aborted`. A stopped job run's status is therefore derived like any other — from its task runs, once they have all settled.

## The three levels

- **[job_run.md](references/job_run.md)** — `JobRunDispatcher` / `JobRunMonitor`, and how task run statuses add up to a job run status.
- **[task_run.md](references/task_run.md)** — `TaskRunDispatcher` / `TaskRunMonitor`, dependency gating and the retry loop.
- **[task_run_attempt.md](references/task_run_attempt.md)** — `TaskRunAttemptDispatcher` / `TaskRunAttemptMonitor`, process spawning, output, timeout and kill.

For the entities themselves see the [job](../job/SKILL.md) and [task](../task/SKILL.md) skills.
