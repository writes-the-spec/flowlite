---
name: crud
description: Add or modify CRUD entities in src/crud/ (insert/select query modules for a SQLite table). Use when adding a new table's data-access layer, adding a new filter/sort option to an existing select, or wiring a new entity into CRUD::init.
---

# CRUD module conventions (src/crud/)

Each entity gets its own file under `src/crud/`, exposed via `src/crud/mod.rs`, with methods implemented on the shared `CRUD` struct (defined in `src/crud/crud.rs`).

Before writing a table's migration, figure out whether it's YAML-seeded config data or runtime-created data — see the [db-storage skill](../db-storage/SKILL.md). That decides whether it's `mem.<table>` (in-memory) or `<table>` (persisted disk), which in turn decides its primary-key style and whether it ever gets an `update_*` method.

For the conventions of each operation, see:

- [references/insert.md](references/insert.md) — input struct shape, `()` vs `i64` return, binding JSON/bool/enum columns, multi-row inserts inside a transaction.
- [references/select.md](references/select.md) — filter/sort/limit/offset struct shape, `QueryBuilder` with `WHERE 1=1`, filter kinds (equality, `LIKE`, comparison), joins, the plural→singular helper.
- [references/update.md](references/update.md) — input/filter struct shape, the double-`Option` pattern for nullable columns, the empty-`SET` guard.

## Rules

- One file per entity, named after the table (singular), under `src/crud/`. Add `pub mod widget;` to [src/crud/mod.rs](../../../src/crud/mod.rs).
- Naming: `Insert<Entity>DataInput` wrapped by `Insert<Entity>Data { input }`; `Select<Entity>sDataFilter` / `Select<Entity>sDataSort` (enum) / `Select<Entity>sData { filter, sort, limit, offset }`; `Update<Entity>sDataInput` / `Update<Entity>sDataFilter` / `Update<Entity>sData { input, filter }`; plain entity struct (`Widget`) derives `sqlx::FromRow, Clone`.
- Methods are `impl CRUD { ... }` blocks, generic over `E: sqlx::Executor<'e, Database = sqlx::Sqlite>` so callers can pass a pool, connection, or transaction.
- If the entity is seeded from YAML config on startup, wire it into `CRUD::init` in [src/crud/crud.rs](../../../src/crud/crud.rs), incrementing the shared `row_id` counter and inserting inside the existing transaction.
- Add the corresponding table migration under `db/schemas/memory/migrations/` or `db/schemas/disk/migrations/` if the table doesn't exist yet.
