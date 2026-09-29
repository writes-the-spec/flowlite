---
name: toolkit
description: Toolkit (src/toolkit.rs) - the one thing that opens flowlite's two SQLite databases, attaches the in-memory `mem` schema, runs both migration histories and hands out connections. Use when a command or service needs a connection or a pool, when mem tables read as missing or empty, when "database schema is locked" or "no such table: mem.job" appears, when deciding whether something needs with_fresh_mem, or when reaching for the current time.
---

# Toolkit (src/toolkit.rs)

`Toolkit` = an `AppConfig` + the name `mem` is attached under. It is the only thing that opens a database.

Two databases, always both: `flowlite.db` (data dir) holds what happened — runs, attempts, output; `mem` (`mode=memory&cache=shared`) holds what the YAML declared. Migrations: [db-schema skill](../db-schema/SKILL.md); contents: [entities skill](../entities/SKILL.md).

## Three ways in

| Call | Gives you | Migrates |
|---|---|---|
| `get_conn()` | a `flowlite.db` connection, `mem` attached | disk schema |
| `get_conn_pool()` | a pool, each connection attaching `mem` | disk schema, once |
| `get_memory_conn()` | a connection to `mem` itself | memory schema |

Attaching never creates `mem`'s tables: without a live `get_memory_conn`, `mem.job` gives **`no such table: mem.job`**. So a command reading config holds `let _memory_conn = toolkit.get_memory_conn().await?;` for its whole life ([cli skill](../cli/SKILL.md)).

**A `mode=memory` database dies when its last connection closes** — drop that binding and the schema and rows go. `serve` keeps its in `AppState` ([router skill](../router/SKILL.md)).

## with_fresh_mem

`Toolkit::new` uses `flowlite_mem`, one per process; `with_fresh_mem()` gives a unique name. Use it when one process seeds `mem` **more than once** — otherwise the second seed collides on `mem.job` primary keys. The MCP server takes one per tool call ([mcp skill](../mcp/SKILL.md)); `serve` deliberately shares one so its pool and `AppState` see the same rows. Don't use it by default: a fresh name is a private, empty `mem`, blind to what `serve` seeded.

In tests, a binary's tests share one process, so seeding `flowlite_mem` races every other test. Use `with_fresh_mem`, `TestDb::new_with_migrated_mem` or `crud_with_private_mem` ([test_support skill](../test_support/SKILL.md)).

## Migrations

`update_disk_schema` / `update_memory_schema` hold `MIGRATION_LOCK` and call `Migrator::run_direct`, not `.run()`:

- **The lock:** sqlx-sqlite's `Migrate::lock` is a no-op, so two connections migrating one fresh database both run the same `CREATE TABLE`s ("database schema is locked" under a shared cache). In-process only: two processes first-migrating one fresh data dir can still collide; only `serve` holds `ServeLock`.
- **`run_direct`** drops `.run()`'s `E: Acquire<'e>` bound, which can't be proven `Send` in an rmcp `#[tool]` future (*"implementation of `sqlx::Acquire` is not general enough"*; see the [crud skill](../crud/SKILL.md)).

Read the doc comments on `MIGRATION_LOCK` and both methods before changing either; each records an observed failure.

## Rules

- **Nothing but `Toolkit` opens a database** — no connection strings, `ATTACH` or `SqlitePoolOptions` elsewhere.
- **`toolkit.get_current_ts()` stamps every `created_at` CRUD writes**, and `Scheduler` and `JobRunReleaser` read "now" through it. It isn't a program-wide clock: other services, the router and the CLI call `Utc::now()`, so tests control time with backdated rows (`TestDb::backdate_task_run_attempt`).
- **`Toolkit` is cheap to clone**; services hold an `Arc<Toolkit>`.
