---
name: toolkit
description: Toolkit (src/toolkit.rs) - the one thing that opens flowlite's two SQLite databases, attaches the in-memory `mem` schema, runs both migration histories and hands out connections. Use when a command or service needs a connection or a pool, when mem tables read as missing or empty, when "database schema is locked" or "no such table: mem.job" appears, when deciding whether something needs with_fresh_mem, or when reaching for the current time.
---

# Toolkit (src/toolkit.rs)

`Toolkit` is an `AppConfig` plus the name `mem` is attached under, and it is the only thing that opens a database. Everything else takes one and asks it for a connection.

Two databases, always both: `flowlite.db` in the data directory holds what happened (runs, attempts, output), and `mem` — a `mode=memory&cache=shared` SQLite database — holds what the YAML declared (jobs, tasks, schedules). They have separate migration histories under `db/schemas/`; see the [db-schema skill](../db-schema/SKILL.md) for which is which, and the [entities skill](../entities/SKILL.md) for what lives in each.

## Three ways in, and what each one migrates

| Call | Gives you | Migrates |
|---|---|---|
| `get_conn()` | one connection to `flowlite.db`, with `mem` attached | the **disk** schema |
| `get_conn_pool()` | a pool, every connection attaching `mem` on connect | the **disk** schema, once |
| `get_memory_conn()` | a connection to `mem` itself | the **memory** schema |

The split is the thing to get right. `get_conn` and `get_conn_pool` *attach* `mem` but never create its tables, so a caller that queries `mem.job` without a live `get_memory_conn` gets **`no such table: mem.job`** against an attached but unmigrated database. That is why a command reading config holds a `let _memory_conn = toolkit.get_memory_conn().await?;` binding for its whole life and one reading only run history does not — see the [cli skill](../cli/SKILL.md).

**A `mode=memory` database exists only while something has it open.** The binding is not decoration: drop it and the schema and every seeded row go with it, so the next attach is an empty database rather than the one just migrated. `serve` keeps its own alive in `AppState` for exactly this reason ([router skill](../router/SKILL.md)).

## with_fresh_mem

`Toolkit::new` names the memory database `flowlite_mem`, one per process. `with_fresh_mem()` returns the same toolkit with a name nothing else has.

Take a fresh one when a single process seeds `mem` **more than once**. The shared name makes the second seed a primary-key collision on the first's `mem.job` rows rather than a private view of its own. That is the MCP server's situation — every tool call takes a fresh name, so a job file written after startup still reaches `list_jobs`, and `submit_job` can seed the same inline id twice in one session ([mcp skill](../mcp/SKILL.md)). `serve` does the opposite on purpose: one shared `mem` for the whole process, which is what lets its pool and its `AppState` see the same seeded rows.

In tests this is a hazard rather than a choice: cargo runs a binary's tests as threads of one process, so a test that seeds the shared `flowlite_mem` races every other test's connection over that one name. Seed through a `with_fresh_mem` toolkit, or `CRUD::crud_with_private_mem`, or `TestDb`.

## Migrations

Both `update_disk_schema` and `update_memory_schema` hold `MIGRATION_LOCK`, a process-wide mutex, and both call sqlx's `Migrator::run_direct` rather than `.run()`.

- **The lock** exists because sqlx-sqlite implements `Migrate::lock`/`unlock` as no-ops: two connections migrating the same fresh database both read zero applied migrations and both run the same `CREATE TABLE`s. It is the in-process analogue of `serve`'s `ServeLock` and does **not** close the cross-process case — two flowlite processes first-migrating one fresh data directory can still collide, since only `serve` holds that lock.
- **`run_direct`** is `.run()`'s own body without its `E: Acquire<'e>` bound. That bound cannot be proven `Send` inside a caller that must itself be `Send` for a lifetime it cannot name — an rmcp `#[tool]` fn's boxed future — and fails with *"implementation of `sqlx::Acquire` is not general enough"*. It is the same failure the [crud skill](../crud/SKILL.md) warns about for multistatement methods, hit one layer down inside sqlx.

Read the doc comments on `MIGRATION_LOCK` and both methods before changing either; each records a failure that was observed rather than predicted.

## Rules

- **Nothing but `Toolkit` opens a database.** A connection string, an `ATTACH`, or a `SqlitePoolOptions` anywhere else is a second place the two schemas have to agree.
- **`toolkit.get_current_ts()` is how CRUD stamps a row.** Every `created_at` an insert writes goes through it, and the `Scheduler` reads "now" through it to find due schedules. It is *not* a single clock seam for the whole program: the orchestrator services, the router and the CLI call `Utc::now()` directly, so a test that needs to control time inserts a backdated row (`TestDb::backdate_task_run_attempt`) rather than replacing a clock.
- **`Toolkit` is cheap to clone and is passed by value**; services hold an `Arc<Toolkit>`.
- **Don't reach for `with_fresh_mem` by default.** A fresh name is a private, empty `mem` — right for a process that seeds repeatedly, wrong for anything that expects to see what `serve` seeded.
