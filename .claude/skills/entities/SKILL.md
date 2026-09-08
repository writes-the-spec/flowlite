---
name: entities
description: Map of every entity in flowlite's two SQLite databases - what each table holds, which database it lives in, who writes it, who reads it and whether it is ever updated after insert. Use when working out what writes or reads a table, tracing which service owns a column, deciding whether data belongs in the in-memory or the persisted database, or adding an entity. For declaring the table itself - keys, nullability, defaults - and for landing the schema change, see the db-schema skill.
---

# Entities

flowlite runs against **two SQLite databases per connection**, and every table belongs to exactly one of them. Which one is decided by a single question: **if the process restarts, should this row still exist?**

- **No → the in-memory `mem` schema.** Config parsed from YAML, re-inserted on every startup by `CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)). Losing it on restart is correct, not a bug.
- **Yes → the persisted disk database.** Created while the app runs, and it has to survive.

| Object | Database | Holds |
|---|---|---|
| [`job`](references/job.md) | `mem` | one row per job YAML file — the definition |
| [`task`](references/task.md) | `mem` | one row per task of a job — description, command, dependencies, retry policy |
| [`task_dependent`](references/task_dependent.md) | `mem` | the dependency edges, normalized — **read by nothing** |
| [`schedule`](references/schedule.md) | `mem` | one row per schedule YAML file, plus its live `next_run` |
| [`schedule_job`](references/schedule_job.md) | `mem` | which jobs a schedule submits |
| [`job_run`](references/job_run.md) | disk | one execution of a job |
| [`task_run`](references/task_run.md) | disk | one task within one job run, and the config it was submitted with |
| [`task_run_attempt`](references/task_run_attempt.md) | disk | one execution of a task run's command |
| [`task_run_attempt_output`](references/task_run_attempt_output.md) | disk | the output of one attempt, in append-only chunks |
| [`job_run_stop`](references/job_run_stop.md) | disk | an insert-only "stop this run" signal |

The disk tables mirror the `mem` ones, but they are not views onto them: **a run carries its own copy of the config it was submitted with.** `submit_job` snapshots `command`, `depends_on`, `timeout`, `max_retries`, `retry_delay`, `env` and `working_dir` from `mem.task` onto each `task_run`, plus a job's resolved `parameters` and, for a scheduled run, `scheduled_at` onto the `job_run` itself, so a run executes what it was submitted with however the YAML has moved since. The orchestrator therefore never reads a `mem` table — with one deliberate exception, `mem.job.max_parallel_runs`, which is a question about the job *now*. See [task_run.md](references/task_run.md).

## How the two databases are wired ([src/toolkit.rs](../../../src/toolkit.rs))

- The disk database is a real file at `<data_dir>/flowlite.db`, opened `mode=rwc` by `Toolkit::create_db_if_not_exists`.
- Every connection to it immediately runs `ATTACH DATABASE 'file:flowlite_mem?mode=memory&cache=shared' AS mem`. Because it is `cache=shared`, every connection sees the *same* in-memory data for the life of the process — it is not per-connection.
- `Toolkit::get_conn_pool` / `get_conn` are the normal way to get a handle: disk file opened, `mem` attached, disk migrations run. Almost all app code uses these. A connection opened any other way has no `mem`, so every query reaching into a config table fails on the missing schema.
- `Toolkit::get_memory_conn` connects to `mem` *without* the disk file, for standalone memory-schema setup only.
- Two independent migration histories: `Toolkit::update_disk_schema` runs `db/schemas/disk/migrations`, `update_memory_schema` runs `db/schemas/memory/migrations`.

Each history is a separate `sqlx::migrate!` with its own checksums, which is why **whether an existing migration may be edited depends on what has already applied it** — the [db-schema skill](../db-schema/SKILL.md) has that call and the rest of the mechanics.

## Conventions every entity obeys

**How a table is declared is the [db-schema skill](../db-schema/SKILL.md)'s.** Four rules shape every column and every entity struct here — how the table is keyed, when a column is `NOT NULL`, that no column carries a `DEFAULT`, and where a foreign key can and cannot reach — and they follow from which of the two databases the table is in, which is the question this skill answers. Read [declaring-a-table.md](../db-schema/references/declaring-a-table.md) before declaring anything. One of them shows up all over the reference files below: "an attempt that printed nothing has empty output, not unknown output" is the nullability rule talking.

**Updated after insert?** Disk tables are; that is what `update_*` methods are for. `mem` tables are re-seeded fresh every startup and are mostly insert-only — **except `schedule.next_run`**, which the [Scheduler](../scheduler/SKILL.md) advances on every fire. It is the one config column that carries live state.

## Adding a new table

1. Decide the database with the restart question above.
2. Write the migration per the [db-schema skill](../db-schema/SKILL.md) — where the file goes, how it is named, whether an existing one may be edited instead, and how the table is keyed and its columns declared. Step 1's answer decides the keys and most of the nullability, so it is not a matter of preference. One thing worth repeating here: no schema prefix inside the migration, since each runs against its own database. The `mem.` prefix appears only later, in the SQL your CRUD methods write.
3. If it is YAML-seeded, wire the insert into `CRUD::init`, incrementing the shared `row_id` counter, inside the existing transaction.
4. Build the CRUD file per the [crud skill](../crud/SKILL.md), which also owns how a method takes its database handle.
5. Add a reference file here.
