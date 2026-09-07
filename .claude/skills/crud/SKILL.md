---
name: crud
description: Add or modify CRUD entities in src/crud/ (insert/select query modules for a SQLite table). Use when adding a new table's data-access layer, adding a new filter/sort option to an existing select, or wiring a new entity into CRUD::init.
---

# CRUD module conventions (src/crud/)

Each entity gets its own file under `src/crud/`, exposed via `src/crud/mod.rs`, with methods implemented on the shared `CRUD` struct (defined in `src/crud/crud.rs`).

A method that runs **one** statement lives in its entity's file. A method that runs **several** statements to do one thing lives under `src/crud/multistatements/` (currently `misc.rs`) instead, because it spans more than one entity and so belongs to no single entity file. That split also decides how the method takes its database handle — see the two executor rules below.

Before writing a table's migration, figure out whether it's YAML-seeded config data or runtime-created data — see the [db-objects skill](../db-objects/SKILL.md). That decides whether it's `mem.<table>` (in-memory) or `<table>` (persisted disk), which in turn decides its primary-key style and whether it ever gets an `update_*` method.

For the conventions of each operation, see:

- [references/insert.md](references/insert.md) — input struct shape, `()` vs `i64` return, binding JSON/bool/enum columns, multi-row inserts inside a transaction.
- [references/select.md](references/select.md) — filter/sort/limit/offset struct shape, `QueryBuilder` with `WHERE 1=1`, filter kinds (equality, `LIKE`, comparison), joins, the plural→singular helper.
- [references/update.md](references/update.md) — input/filter struct shape, the double-`Option` pattern for nullable columns, the empty-`SET` guard.

## Rules

- One file per entity, named after the table (singular), under `src/crud/`. Add `pub mod widget;` to [src/crud/mod.rs](../../../src/crud/mod.rs). Methods that span several entities go in [src/crud/multistatements/misc.rs](../../../src/crud/multistatements/misc.rs) rather than being forced into one of them.
- Naming: `Insert<Entity>DataInput` wrapped by `Insert<Entity>Data { input }`; `Select<Entity>sDataFilter` / `Select<Entity>sDataSort` (enum — `job_run_stop`'s is `SelectJobRunStopsSort`, missing the `Data`, and is the one file not to copy) / `Select<Entity>sData { filter, sort, limit, offset }`; `Update<Entity>sDataInput` / `Update<Entity>sDataFilter` / `Update<Entity>sData { input, filter }`; plain entity struct (`Widget`) derives `sqlx::FromRow, Clone`.
- **Single-statement methods take a generic executor.** Every `insert_*` / `select_*` / `update_*` in an entity file is an `impl CRUD` method generic over `E: sqlx::Executor<'e, Database = sqlx::Sqlite>`, so the caller passes whatever it already holds — a pool, a connection, or a transaction. One statement doesn't care which.
- **Multistatement methods take `&mut SqliteConnection`, not a generic executor.** A method in `src/crud/multistatements/` (`submit_job`, `rerun_job`, `is_job_at_max_parallel_runs`) runs a sequence of statements that form one logical operation — insert the job run, then one task run per task — so it needs *one* connection rather than "any executor": it takes `conn: &mut SqliteConnection` and passes `&mut *conn` down to each single-statement method it calls. Don't instead make it generic over `E: sqlx::Executor + sqlx::Acquire` and acquire inside — that compiles on its own, but the method can then no longer be called with a `&mut SqliteConnection` from anywhere the future has to be `Send` (an axum handler, a `tokio::spawn`), where rustc rejects it with *"implementation of `sqlx::Acquire` is not general enough"*. A caller holding only a pool does `let mut conn = conn_pool.acquire().await?;` first.
- If the entity is seeded from YAML config on startup, wire it into `CRUD::init` in [src/crud/crud.rs](../../../src/crud/crud.rs), incrementing the shared `row_id` counter and inserting inside the existing transaction.
- Add the corresponding table migration under `db/schemas/memory/migrations/` or `db/schemas/disk/migrations/` if the table doesn't exist yet.
