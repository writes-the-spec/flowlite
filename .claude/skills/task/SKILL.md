---
name: task
description: Domain model for the Task entity - what a task is, how its dependencies work (task.depends_on vs the task_dependent table), and how the two dispatcher/monitor pairs (task run and task run attempt) execute, retry and skip it. Use when working with Task, TaskRun, or TaskRunAttempt (src/crud/task.rs, task_run.rs, task_run_attempt.rs, task_dependent.rs, src/orchestrator/task_run_dispatcher.rs, src/orchestrator/task_run_monitor.rs, src/orchestrator/task_run_attempt_dispatcher.rs, src/orchestrator/task_run_attempt_monitor.rs).
---

# Task

A **Task** is a single shell command inside a [Job](../job/SKILL.md), with an optional list of other tasks (in the same job) it depends on. Like a job, a task is a *definition* — the thing that actually executes and carries state is a **TaskRun** (one per task per job run), and each attempt at running it is a **TaskRunAttempt**.

## Definition (YAML → in-memory `mem.task` + `mem.task_dependent`)

Declared inline under a job's `tasks:` list ([src/yaml_models/job_yaml.rs](../../../src/yaml_models/job_yaml.rs) `JobYamlTask`):

```yaml
- id: task-b
  command: ./run.sh
  depends_on: [task-a]
  timeout: 600      # seconds, defaults to 3600
  max_retries: 2    # defaults to 0
```

`CRUD::init` writes each task's dependency list into **two** places, both from the same `depends_on` list:

- `task.depends_on` — the full list, stored as a JSON array on the task row itself.
- `task_dependent` — the same edges, normalized one-row-per-dependency (`job_id, task_id, dependent_task_id`).

`CRUD::validate_job_tasks` ([src/crud/crud.rs](../../../src/crud/crud.rs)) rejects the job at startup if a task id is declared twice, if a `depends_on` id is not a task of the same job, or if the dependencies form a cycle — `TaskRunDispatcher` waits for every dependency to succeed, so any of those would leave the task runs pending and their job run running forever.

These are genuinely redundant (same source data, same list, written together) rather than two different concepts. **`task.depends_on` is the list a run's dependency graph is built *from*, not the one the runtime resolves** — `CRUD::submit_job` copies it onto each `task_run` row at submit time, and both dependency checks go through `TaskRunDispatcher::get_dependent_task_runs`, which resolves that copy (`task_run.depends_on`). Outside the snapshot, `task.depends_on` is read only by the job page's task table and its DAG ([src/router/app/routes/jobs/job_id/dag.rs](../../../src/router/app/routes/jobs/job_id/dag.rs)), which describe the job as it is defined now rather than any run of it. `task_dependent` is written by `CRUD::init` and read by nothing at all; it's the normalized form, useful for reverse-edge queries ("who depends on me"). Keep them in sync if you change how dependencies are declared — and note that `task.depends_on` is now copied into every run submitted after an edit, so a wrong list is frozen onto those runs rather than fixable by editing the YAML.

## TaskRun lifecycle: two dispatcher/monitor pairs

Four **independent** background services, mirroring the job side (see the [job skill](../job/SKILL.md)) and coordinating only through row status — a dispatcher and a monitor for each level of the `task_run` → `task_run_attempt` hierarchy:

- **`TaskRunDispatcher`** ([src/orchestrator/task_run_dispatcher.rs](../../../src/orchestrator/task_run_dispatcher.rs)) — polls all `Pending` task runs, once a second or as soon as a wake-up arrives, and settles each row as exactly one outcome, each returning whether it is what happened:
  1. `settle_as_skipped` → `Skipped`, if the job run was stopped or any dependency task run finished but didn't succeed (`Failed`/`Skipped`/`Aborted`/`TimedOut`)
  2. `settle_as_running` → `Running`, once all dependency task runs have `Succeeded`
  3. `settle_as_pending` → nothing written, the row waits, because a dependency is still `Pending` or `Running`
  4. past all three → an error, rather than a row left sitting with nobody accountable for it
- **`TaskRunMonitor`** ([src/orchestrator/task_run_monitor.rs](../../../src/orchestrator/task_run_monitor.rs)) — polls `Running` task runs on the same schedule and drives them through their attempts: it inserts the attempt rows, decides retries, and moves the task run to a finished status. It reads attempt rows and writes task run rows; it never touches a process.
- **`TaskRunAttemptDispatcher`** ([src/orchestrator/task_run_attempt_dispatcher.rs](../../../src/orchestrator/task_run_attempt_dispatcher.rs)) — polls `Pending` attempt rows and settles each as `Skipped` (the job run was stopped before the command started) or `Running` (spawns its command). Two outcomes only, with no waiting case: the dependencies were already settled one level up.
- **`TaskRunAttemptMonitor`** ([src/orchestrator/task_run_attempt_monitor.rs](../../../src/orchestrator/task_run_attempt_monitor.rs)) — polls `Running` attempt rows, waits on the processes the dispatcher spawned and finishes them. It reads and writes attempt rows only; it knows nothing about task runs, retries or dependencies.

All four live in [src/orchestrator/](../../../src/orchestrator/), are each an `impl Service` ([src/poller.rs](../../../src/poller.rs)) driven by its own `Poller`, and are spawned by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which also creates the `TaskRunAttemptChildren` the two attempt services share. A `Poller` wakes on a `Signals` ([src/signals.rs](../../../src/signals.rs)) wake-up or its one-second interval, whichever comes first — the interval alone is what keeps things moving when nothing publishes.

The statuses themselves, and what each transition is allowed to write, are in the [orchestrator skill](../orchestrator/references/task_run.md).

## How an attempt executes

The two attempt services share one `TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../src/orchestrator/task_run_attempt_children.rs)): a `Mutex<HashMap<task_run_attempt_id, RunningTaskRunAttempt>>` that keeps the child processes alive between polls. The dispatcher puts a child in, the monitor takes it out.

`TaskRunAttemptDispatcher` spawns the command when it starts a `Pending` attempt: it loads the attempt's `task_run` row by `task_run_id`, for the `command` and `timeout` the run was submitted with, spawns `sh -c <command>` with piped stdout/stderr, inserts the child, and only then writes `Running` and the attempt's `started_at` — in the other order the monitor could see a `Running` attempt whose child is not in the map yet.

`TaskRunAttemptMonitor` then handles each `Running` attempt row per pass:

- **Not in the map** → attempt `Aborted`. The map only holds processes this program spawned, so the row is left over from an earlier run of it. This is the restart path.
- **In the map** → drain its output, then check in order: job run stopped → kill it, attempt `Aborted`; past `task_run.timeout` (measured from the in-memory spawn time) → kill it, attempt `TimedOut`; process exited → attempt `Succeeded`/`Failed` from the exit status; still running → persist the output so far and put the child back.

`TaskRunAttemptStatus` has the same seven variants as `TaskRunStatus`, since both levels have the same dispatcher/monitor shape. `Skipped` at this level means the job run was stopped in the pass between the row's insert and its dispatch, so there was never a process to kill.

## How the monitor retries a task run

`TaskRunMonitor` looks at the **last** attempt row of each `Running` task run: none yet → insert a `Pending` attempt; otherwise the attempt's status picks one `settle_for_*` outcome. `Pending` and `Running` wait, a `Failed` attempt is retried while `attempt < task_run.max_retries + 1` — once `task_run.retry_delay` seconds have passed since it finished — by inserting the next attempt row, and every other status finishes it without a retry (a stop is never undone by a retry) — `Succeeded` succeeds it, `TimedOut` times it out, and both `Aborted` and `Skipped` **abort** it, since a task run that reached `Running` had started and a stop can only interrupt it. Attempts count from 1, so total executions are `1 + max_retries`.

Both numbers come off the `task_run` row, not `mem.task`: a run retries on the policy it was submitted with, however the YAML has moved since.

The task run stays `Running` across the whole retry sequence — it does *not* go back to `Pending`, so the dispatcher's dependency check runs once per task run and `task_run.started_at` means "when the task run started", not "when the current attempt started".

**A restart loses the children, not the rows.** After a process restart the children map is empty while the DB still says `Running`, so `TaskRunAttemptMonitor` finishes those attempt rows `Aborted` — a command is never re-executed on an attempt row that already had one, and the retry, if the task has one left, comes from `TaskRunMonitor` inserting a new attempt row.

## Stdout/stderr

`read_output` (in `TaskRunAttemptMonitor`) drains both pipes with a 10ms timeout so a chatty process can't block the loop. The accumulated bytes are written to `task_run_attempt.stdout`/`stderr` on every poll pass that the process is still alive (so logs are visible while it runs) and once more when the attempt ends, including a final drain after exit so the last output isn't lost. This write never publishes — see the [orchestrator skill](../orchestrator/SKILL.md) for why.
