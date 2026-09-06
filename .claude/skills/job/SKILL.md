---
name: job
description: Domain model for the Job entity - what a job is, how it's defined in YAML, how submitting one creates a JobRun, and how JobRunDispatcher/JobRunMonitor drive it to completion. Use when working with Job, JobRun, or JobRunStop (src/crud/job.rs, job_run.rs, job_run_stop.rs, src/orchestrator/job_run_dispatcher.rs, src/orchestrator/job_run_monitor.rs, src/yaml_models/job_yaml.rs, src/cli/commands/job.rs).
---

# Job

A **Job** is a named, reusable unit of work: an ordered set of [Tasks](../task/SKILL.md) forming a dependency graph. A job is *defined* once (in YAML) and can be *run* many times — each run is tracked separately as a **JobRun**. Don't conflate the two: a `Job` is a template/definition, a `JobRun` is one execution of that template.

## Definition (YAML → in-memory `mem.job`)

A job is declared as a YAML file under `<config_dir>/jobs/*.yml`, parsed by `JobYaml` ([src/yaml_models/job_yaml.rs](../../../src/yaml_models/job_yaml.rs)):

```yaml
id: my-job
name: My Job
description: what it does
tasks:
  - id: task-a
    command: echo hello
```

On every startup, `CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)) reads every job YAML file and inserts one `job` row plus one `task` row per task into the **in-memory** `mem` schema (see the [db-storage skill](../db-storage/SKILL.md) — job/task definitions are config data, never persisted independently of the YAML). `job_id` is the primary key; `name` is a separate, non-unique display label — CLI/API lookups by id should filter on `job_id`, not `name`.

## Execution (`CRUD::submit_job` → `job_run` + `task_run`)

Submitting a job (`CRUD::submit_job` in [src/crud/multistatements/misc.rs](../../../src/crud/multistatements/misc.rs)) does two things inside one call:

1. Inserts a `job_run` row (disk-persisted, autoincrement `id`) with `status = Pending`.
2. Inserts a `task_run` row (also disk-persisted, see [task skill](../task/SKILL.md)) with `status = Pending` for **every** task belonging to the job — not just the ones with no dependencies. Dependency ordering is enforced later, at task-run time, not at submission time.

`JobRunStatus` (in [src/crud/job_run.rs](../../../src/crud/job_run.rs)) is: `Pending → Running → {Succeeded, Failed, Skipped, Aborted, TimedOut}`.

## Driving a JobRun to completion: Dispatcher + Monitor

Two **independent** background services split this work — neither calls the other; they hand off purely through the job run's `status` column:

- **`JobRunDispatcher`** ([src/orchestrator/job_run_dispatcher.rs](../../../src/orchestrator/job_run_dispatcher.rs)) — polls for `Pending` job runs, oldest first, once a second or as soon as a wake-up arrives, whichever comes first. Each one is either skipped (`transition_to_skipped`, if a stop signal exists — see below) or started (`transition_to_running`, which sets `status = Running` and `started_at`), and is left `Pending` for a later pass if its job is already at `max_parallel_runs`. That gate is the only place the limit is enforced — submitting is never rejected for being over it.
- **`JobRunMonitor`** ([src/orchestrator/job_run_monitor.rs](../../../src/orchestrator/job_run_monitor.rs)) — polls `Running` job runs on the same schedule. For each, once every task run has left `Pending`/`Running`, it decides the job run's terminal status.

Both are started independently by `Orchestrator::start` ([src/orchestrator/orchestrator.rs](../../../src/orchestrator/orchestrator.rs)), which `serve` calls, and both follow the same shape: `impl Service` ([src/poller.rs](../../../src/poller.rs)) for the type, then a `Poller::new(...).start()` line at the call site. The `Poller` owns the loop, the wake-ups and the error handling; the service holds nothing but its own logic: `name`, `row_context`, `select`, `handle`. Keep them decoupled — new job-run lifecycle stages should be added as another status-driven poller rather than by having one service call into another.

For exactly when a job run leaves `Running`, which finished status it gets, and the invariants that depend on it, see the [orchestrator skill](../orchestrator/references/job_run.md).

## Stopping a job run

A `job_run_stop` row (disk-persisted, [src/crud/job_run_stop.rs](../../../src/crud/job_run_stop.rs)) is an insert-only cancellation signal keyed by `job_run_id` — there's no "cancel" status update on `job_run` directly. `JobRunDispatcher`, `TaskRunDispatcher` and `TaskRunAttemptMonitor` all poll for a matching stop row on every pass — a wake-up or the one-second interval, whichever comes first — and, if present, finish what they own themselves — the job run `Skipped`, task runs that never started `Skipped` too, in-flight ones `Aborted`. When adding new run-control features (pause, retry-all, etc.), follow this same pattern: a small insert-only signal table that the pollers check, rather than mutating run state directly from an unrelated code path.

## CLI

`job list` and `job submit <job_name>` ([src/cli/commands/job.rs](../../../src/cli/commands/job.rs)). Despite the field name, `job submit`'s argument is matched against `job_id`, not the `name` column — the CLI arg name is misleading, don't copy it as a model for new commands without checking which column it actually filters on.
