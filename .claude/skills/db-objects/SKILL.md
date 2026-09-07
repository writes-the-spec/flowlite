---
name: db-objects
description: Map of every table in flowlite's two SQLite databases - what each one holds, which database it lives in, who writes it and who reads it - plus the conventions a new table follows. Use when adding a table, adding a migration, adding or changing a column, deciding whether data belongs in the in-memory or the persisted database, or working out what writes and reads an existing table.
---

# Database objects

flowlite runs against **two SQLite databases per connection**, and every table belongs to exactly one of them. Which one is decided by a single question: **if the process restarts, should this row still exist?**

- **No → the in-memory `mem` schema.** Config parsed from YAML, re-inserted on every startup by `CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)). Losing it on restart is correct, not a bug.
- **Yes → the persisted disk database.** Created while the app runs, and it has to survive.

| Object | Database | Holds |
|---|---|---|
| [`job`](references/job.md) | `mem` | one row per job YAML file — the definition |
| [`task`](references/task.md) | `mem` | one row per task of a job — command, dependencies, retry policy |
| [`task_dependent`](references/task_dependent.md) | `mem` | the dependency edges, normalized — **read by nothing** |
| [`schedule`](references/schedule.md) | `mem` | one row per schedule YAML file, plus its live `next_run` |
| [`schedule_job`](references/schedule_job.md) | `mem` | which jobs a schedule submits |
| [`job_run`](references/job_run.md) | disk | one execution of a job |
| [`task_run`](references/task_run.md) | disk | one task within one job run, and the config it was submitted with |
| [`task_run_attempt`](references/task_run_attempt.md) | disk | one execution of a task run's command |
| [`job_run_stop`](references/job_run_stop.md) | disk | an insert-only "stop this run" signal |

The disk tables mirror the `mem` ones, but they are not views onto them: **a run carries its own copy of the config it was submitted with.** `submit_job` snapshots `command`, `depends_on`, `timeout`, `max_retries` and `retry_delay` from `mem.task` onto each `task_run`, so a run executes what it was submitted with however the YAML has moved since. The orchestrator therefore never reads a `mem` table — with one deliberate exception, `mem.job.max_parallel_runs`, which is a question about the job *now*. See [task_run.md](references/task_run.md).

## How the two databases are wired ([src/toolkit.rs](../../../src/toolkit.rs))

- The disk database is a real file at `<data_dir>/flowlite.db`, opened `mode=rwc` by `Toolkit::create_db_if_not_exists`.
- Every connection to it immediately runs `ATTACH DATABASE 'file:flowlite_mem?mode=memory&cache=shared' AS mem`. Because it is `cache=shared`, every connection sees the *same* in-memory data for the life of the process — it is not per-connection.
- `Toolkit::get_conn_pool` / `get_conn` are the normal way to get a handle: disk file opened, `mem` attached, disk migrations run. Almost all app code uses these. A connection opened any other way has no `mem`, so every query reaching into a config table fails on the missing schema.
- `Toolkit::get_memory_conn` connects to `mem` *without* the disk file, for standalone memory-schema setup only.
- Two independent migration histories: `Toolkit::update_disk_schema` runs `db/schemas/disk/migrations`, `update_memory_schema` runs `db/schemas/memory/migrations`.

**Migrations are checksummed by `sqlx::migrate!`, so editing an applied migration file breaks startup** against an existing `flowlite.db` — *"migration ... was previously applied but has been modified"*. Pre-release the fix is to delete the dev database; after that, a column change needs a new migration file.

## Conventions every object obeys

**Keys split by database.** A `mem` table carries a caller-assigned `row_id INTEGER NOT NULL` with `UNIQUE (row_id)`, whose only job is to preserve YAML declaration order, alongside a natural primary key from the config (`job_id`, `(task_id, job_id)`, `schedule_id`) — or, for `task_dependent` and `schedule_job`, no primary key at all. A disk table has `id INTEGER PRIMARY KEY AUTOINCREMENT`, returned via `last_insert_rowid()`.

**`NOT NULL` whenever there is a logical null value.** If a column's type has a natural empty value — `''` for text, `0` for a counter, `'[]'` for a JSON list — *that value is the null*: declare `NOT NULL` and write the empty value explicitly on insert. A nullable column is only right when "no value" is a real, distinct state no ordinary value can express, which in this schema means timestamps that have not happened yet (`started_at`, `finished_at`, `next_run`) and genuinely open-ended bounds (`start_date`, `end_date`). The rule keeps two spellings of "nothing" from coexisting — once a text column is nullable, `NULL` and `''` both mean "no output" and no query can be written without an `OR ... IS NULL`. It pays off in Rust: a `NOT NULL` column is a plain `String`/`u32` instead of an `Option<...>`, so no call site invents a meaning for `None`.

**No `DEFAULT` clauses.** Every insert supplies every column it owns. A constant default goes in the SQL literal (`..., stdout, stderr) VALUES (..., '', '')`); a computed one is bound from Rust, and every timestamp binds `self.toolkit.get_current_ts()` — never `CURRENT_TIMESTAMP`, so `Toolkit` stays the single source of "now" and every timestamp in the schema shares one format, RFC3339 with subseconds, directly comparable in SQL. Two reasons: the value a row gets should be readable from the insert rather than from DDL written migrations ago, and `DEFAULT` plus `NOT NULL` quietly hides a forgotten column instead of failing.

**Updated after insert?** Disk tables are; that is what `update_*` methods are for. `mem` tables are re-seeded fresh every startup and are mostly insert-only — **except `schedule.next_run`**, which the [Scheduler](../scheduler/SKILL.md) advances on every fire. It is the one config column that carries live state.

**Foreign keys are enforced.** sqlx turns `PRAGMA foreign_keys` on by default, so a bad reference in config aborts `CRUD::init`'s transaction and the process exits — see [schedule_job.md](references/schedule_job.md). A disk table cannot have a foreign key to a config table, since the config lives in the attached database; `job_run.job_id` and `task_run.task_id` are therefore unconstrained, and the code checks instead.

## Adding a new table

1. Decide the database with the restart question above.
2. Add the migration under `db/schemas/memory/migrations/` or `db/schemas/disk/migrations/`, named `YYYYMMDDHHMMSS_create_<table>_table.sql`. No schema prefix in the migration itself — each runs against its own database. The `mem.` prefix is only needed later, in the SQL your CRUD methods write.
3. Declare each column `NOT NULL` unless "no value" is a state no ordinary value can express, and leave `DEFAULT` out entirely.
4. If it is YAML-seeded, wire the insert into `CRUD::init`, incrementing the shared `row_id` counter, inside the existing transaction.
5. Build the CRUD file per the [crud skill](../crud/SKILL.md), which also owns how a method takes its database handle.
6. Add a reference file here.
