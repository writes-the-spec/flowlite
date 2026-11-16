---
name: entities
description: Map of every entity in flowlite - the tables of flowlite.db and the structs Declared loads from YAML - what each holds, who writes it, who reads it and whether it is ever updated after insert. Use when working out what writes or reads a table, tracing which service owns a column, deciding whether data belongs in flowlite.db or in Declared, or adding an entity. For declaring the table itself - keys, nullability, defaults - and for landing the schema change, see the db-schema skill.
---

# Entities

**If the process restarts, should this still exist?**

- **No → `Declared`.** What the YAML declares, loaded by `Declared::load` ([src/declared/declared.rs](../../../src/declared/declared.rs)) in every process that needs it; rebuilding it on restart is correct. Plain structs, no database - see the [yaml skill](../yaml/SKILL.md).
- **Yes → a table in `flowlite.db`.** Data created while the app runs.

| Object | Lives in | Holds |
|---|---|---|
| [`Job`](references/job.md) | `Declared` | one per job YAML file — the definition |
| [`Task`](references/task.md) | `Declared` | one per task of a job — description, command, dependencies, retry policy |
| [`Schedule`](references/schedule.md) | `Declared` | one per schedule YAML file |
| [`ScheduleJob`](references/schedule_job.md) | `Declared` | which jobs a schedule submits |
| [`job_run`](references/job_run.md) | `flowlite.db` | one execution of a job |
| [`task_run`](references/task_run.md) | `flowlite.db` | one task within one job run, and the config it was submitted with |
| [`task_run_attempt`](references/task_run_attempt.md) | `flowlite.db` | one execution of a task run's command |
| [`task_run_attempt_output`](references/task_run_attempt_output.md) | `flowlite.db` | the output of one attempt, in append-only chunks |
| [`job_run_stop`](references/job_run_stop.md) | `flowlite.db` | an insert-only "stop this run" signal |
| [`job_run_notification`](references/job_run_notification.md) | `flowlite.db` | one notification (per channel, per `on_failure`/`on_success` block) to send, and what happened when it was tried |

**A run carries its own copy of its config**; the tables are not views onto `Declared`. `submit_job` snapshots each task's config onto its `task_run`, and resolved `parameters` and `scheduled_at` onto the `job_run`, so a run executes what it was submitted with whatever the YAML does later. The orchestrator never reads `Declared`, except a `Job`'s `max_parallel_runs` — a question about the job *now*. See [task_run.md](references/task_run.md).

## flowlite.db ([src/toolkit.rs](../../../src/toolkit.rs))

- `<data_dir>/flowlite.db`, opened only through `Toolkit::get_conn` / `get_conn_pool` (created if missing, WAL, `synchronous=NORMAL`, 5 s busy timeout, migrations run), so `flowlite.db-wal` and `flowlite.db-shm` sit beside it while anything has it open. See the [toolkit skill](../toolkit/SKILL.md).
- One migration history, `db/schemas/disk/migrations`. Editing an existing migration depends on what has applied it — see the [db-schema skill](../db-schema/SKILL.md).

## Conventions every entity obeys

- **Declaring a table** (keys, `NOT NULL`, no `DEFAULT`, foreign key reach) is the [db-schema skill](../db-schema/SKILL.md)'s; read [declaring-a-table.md](../db-schema/references/declaring-a-table.md) first. "Empty, not unknown" in the references is its nullability rule.
- **Updated after insert?** Tables are, via `update_*` methods. `Declared` never changes once loaded, apart from the one ad-hoc job `job submit -f` adds to its own copy.
- **Rows are deleted only by `RetentionService`** ([src/retention/service.rs](../../../src/retention/service.rs)) — no CLI command, route or other service, the Scheduler included. It deletes finished runs — never `Scheduled`/`Queued`/`Running`, never one owing a `pending` notification — with every row in the other five tables carrying the `job_run_id`, and every run its tasks submitted. Per-job survival is the `Job`'s `keep_runs` ([job.md](references/job.md)).

## Adding a new entity

1. Decide where it lives with the restart question.
2. **YAML-declared:** a struct and a `Vec` on `Declared`, filled in `Declared::load` ([yaml skill](../yaml/SKILL.md)). No migration, no CRUD.
3. **A table:** write the migration per the [db-schema skill](../db-schema/SKILL.md), build the CRUD file per the [crud skill](../crud/SKILL.md).
4. Add a reference file here.
