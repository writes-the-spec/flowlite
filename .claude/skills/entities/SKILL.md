---
name: entities
description: Map of every entity in flowlite's two SQLite databases - what each table holds, which database it lives in, who writes it, who reads it and whether it is ever updated after insert. Use when working out what writes or reads a table, tracing which service owns a column, deciding whether data belongs in the in-memory or the persisted database, or adding an entity. For declaring the table itself - keys, nullability, defaults - and for landing the schema change, see the db-schema skill.
---

# Entities

Every table lives in one of two SQLite databases. **If the process restarts, should this row still exist?**

- **No → in-memory `mem`.** YAML config, re-inserted every startup by `CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)); losing it on restart is correct.
- **Yes → the disk database.** Data created while the app runs.

| Object | Database | Holds |
|---|---|---|
| [`job`](references/job.md) | `mem` | one row per job YAML file — the definition |
| [`task`](references/task.md) | `mem` | one row per task of a job — description, command, dependencies, retry policy |
| [`task_dependent`](references/task_dependent.md) | `mem` | the dependency edges, normalized — **read by nothing** |
| [`schedule`](references/schedule.md) | `mem` | one row per schedule YAML file |
| [`schedule_job`](references/schedule_job.md) | `mem` | which jobs a schedule submits |
| [`job_run`](references/job_run.md) | disk | one execution of a job |
| [`task_run`](references/task_run.md) | disk | one task within one job run, and the config it was submitted with |
| [`task_run_attempt`](references/task_run_attempt.md) | disk | one execution of a task run's command |
| [`task_run_attempt_output`](references/task_run_attempt_output.md) | disk | the output of one attempt, in append-only chunks |
| [`job_run_stop`](references/job_run_stop.md) | disk | an insert-only "stop this run" signal |
| [`job_run_notification`](references/job_run_notification.md) | disk | one notification (per channel, per `on_failure`/`on_success` block) to send, and what happened when it was tried |

**A run carries its own copy of its config**; disk tables are not views onto `mem`. `submit_job` snapshots each task's config onto its `task_run`, and resolved `parameters` and `scheduled_at` onto the `job_run`, so a run executes what it was submitted with whatever the YAML does later. The orchestrator never reads `mem`, except `mem.job.max_parallel_runs` — a question about the job *now*. See [task_run.md](references/task_run.md).

## How the two databases are wired ([src/toolkit.rs](../../../src/toolkit.rs))

- Disk: `<data_dir>/flowlite.db`, opened `mode=rwc` by `Toolkit::create_db_if_not_exists`. Every connection runs `ATTACH DATABASE 'file:flowlite_mem?mode=memory&cache=shared' AS mem`; `cache=shared` makes every connection see the *same* `mem` for the life of the process.
- Use `Toolkit::get_conn_pool` / `get_conn` (disk opened, `mem` attached, disk migrations run). A connection opened otherwise has no `mem`, so config-table queries fail. `Toolkit::get_memory_conn` opens `mem` alone, for memory-schema setup only.
- Two migration histories with separate checksums: `Toolkit::update_disk_schema` (`db/schemas/disk/migrations`) and `update_memory_schema` (`db/schemas/memory/migrations`). Editing an existing disk migration depends on what has applied it — see the [db-schema skill](../db-schema/SKILL.md). **A `mem` table never needs a new migration**: `mem` starts empty each process, so edit its `CREATE TABLE` and restart.

## Conventions every entity obeys

- **Declaring a table** (keys, `NOT NULL`, no `DEFAULT`, foreign key reach, all following from the database) is the [db-schema skill](../db-schema/SKILL.md)'s; read [declaring-a-table.md](../db-schema/references/declaring-a-table.md) first. "Empty, not unknown" in the references is its nullability rule.
- **Updated after insert?** Disk tables are, via `update_*` methods. `mem` tables never: no live state, no `update_*` method.
- **Rows are deleted only from disk tables, only by `RetentionService`** ([src/retention/service.rs](../../../src/retention/service.rs)) — no CLI command, route or other service, the Scheduler included. (`job-run delete` only tombstones a run as `Deleted`; see [job_run.md](references/job_run.md).) It deletes finished runs — never `Scheduled`/`Queued`/`Running`, never one owing a `pending` notification — with every row in the other five disk tables carrying the `job_run_id`, and every run its tasks submitted. Per-job survival is `mem.job.keep_runs` ([job.md](references/job.md)).

## Adding a new table

1. Decide the database with the restart question.
2. Write the migration per the [db-schema skill](../db-schema/SKILL.md) (location, naming, edit vs. new — memory side: always edit, never a new `add_*` file — keys, columns). Step 1 decides the keys and most nullability. No schema prefix in a migration; `mem.` appears only in CRUD SQL.
3. If YAML-seeded, insert in `CRUD::init`'s existing transaction, incrementing the shared `row_id` counter.
4. Build the CRUD file per the [crud skill](../crud/SKILL.md) (including how a method takes its handle).
5. Add a reference file here.
