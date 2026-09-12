---
name: crud
description: Add or modify CRUD entities in src/crud/ (insert/select query modules for a SQLite table). Use when adding a new table's data-access layer, adding a new filter/sort option to an existing select, or wiring a new entity into CRUD::init.
---

# CRUD module conventions (src/crud/)

Each entity gets its own file under `src/crud/`, exposed via `src/crud/mod.rs`, with methods implemented on the shared `CRUD` struct (defined in `src/crud/crud.rs`).

A method that runs **one** statement lives in its entity's file. A method that runs **several** statements to do one thing lives under `src/crud/multistatements/` instead, because it spans more than one entity and so belongs to no single entity file. That split also decides how the method takes its database handle — see the two executor rules below.

`src/crud/multistatements/` is one file per operation, named after it:

| File | Holds |
|---|---|
| [submit_job.rs](../../../src/crud/multistatements/submit_job.rs) | `submit_job`, and the parameter resolution it checks a caller's overrides with |
| [rerun_job.rs](../../../src/crud/multistatements/rerun_job.rs) | `rerun_job` |
| [job_run_definition.rs](../../../src/crud/multistatements/job_run_definition.rs) | Not an operation: the `JobRunDefinition` vocabulary those two share, the merges that build one, and `insert_job_run_definition` — the only place a run's config is written |
| [limits.rs](../../../src/crud/multistatements/limits.rs) | `is_job_at_max_parallel_runs`, `count_running_attempts`, `claimed_limit_slots` |
| [secret_env.rs](../../../src/crud/multistatements/secret_env.rs) | `check_secret_env_is_satisfied` and the pure policy under it |
| [job_run_reads.rs](../../../src/crud/multistatements/job_run_reads.rs) | `select_job_run_with_task_runs`, `select_task_run_attempt_logs` |
| [ad_hoc_job.rs](../../../src/crud/multistatements/ad_hoc_job.rs) | `seed_ad_hoc_job` and `JobIdAlreadyInstalled` |

A new operation gets its own file and a `pub mod` line, the way a new MCP tool does. Items only the sibling operations use are `pub(super)`, not `pub`.

Before writing a table's migration, figure out whether it's YAML-seeded config data or runtime-created data — see the [entities skill](../entities/SKILL.md). That decides whether it's `mem.<table>` (in-memory) or `<table>` (persisted disk), which in turn decides its primary-key style and whether it ever gets an `update_*` method.

For the conventions of each operation, see:

- [references/insert.md](references/insert.md) — input struct shape, `()` vs `i64` return, binding JSON/bool/enum columns, multi-row inserts inside a transaction.
- [references/select.md](references/select.md) — filter/sort/limit/offset struct shape, `QueryBuilder` with `WHERE 1=1`, filter kinds (equality, `LIKE`, comparison), joins, the plural→singular helper.
- [references/update.md](references/update.md) — input/filter struct shape, the double-`Option` pattern for nullable columns, the empty-`SET` guard.

## Rules

- One file per entity, named after the table (singular), under `src/crud/`. Add `pub mod widget;` to [src/crud/mod.rs](../../../src/crud/mod.rs). A method that spans several entities goes in its own file under [src/crud/multistatements/](../../../src/crud/multistatements/) rather than being forced into one of them.
- **A refusal CRUD raises comes back as a typed value, not a finished sentence,** when its callers would word it differently. `seed_ad_hoc_job` raises `JobIdAlreadyInstalled` carrying the id and the jobs directory; `job submit -f` turns that into "drop -f" and the MCP tool into "submit it by name". Downcast it out of the `anyhow::Error` with `err.downcast::<T>()`.
- **Policy stays with whoever it belongs to, not in CRUD.** Ask "whose rule is this?" — if one caller would be wrong to skip it and another legitimately skips it, it is that caller's. A settled run is refused by `flowlite job-run stop`, while the dashboard's stop route redirects instead; two surfaces differing is accepted over one rule in CRUD. What earns a multistatement is several *statements* that must run together for anyone doing the operation at all (`submit_job`), or a query answering "may this happen" (`is_job_at_max_parallel_runs`) that each caller then acts on itself.
- Naming: `Insert<Entity>DataInput` wrapped by `Insert<Entity>Data { input }`; `Select<Entity>sDataFilter` / `Select<Entity>sDataSort` (enum — `job_run_stop`'s is `SelectJobRunStopsSort`, missing the `Data`, and is the one file not to copy) / `Select<Entity>sData { filter, sort, limit, offset }`; `Update<Entity>sDataInput` / `Update<Entity>sDataFilter` / `Update<Entity>sData { input, filter }`; plain entity struct (`Widget`) derives `sqlx::FromRow, Clone`.
- **Single-statement methods take a generic executor.** Every `insert_*` / `select_*` / `update_*` in an entity file is an `impl CRUD` method generic over `E: sqlx::Executor<'e, Database = sqlx::Sqlite>`, so the caller passes whatever it already holds — a pool, a connection, or a transaction. One statement doesn't care which.
- **Multistatement methods take `&mut SqliteConnection`, not a generic executor.** A method in `src/crud/multistatements/` (`submit_job`, `rerun_job`, `is_job_at_max_parallel_runs`) runs a sequence of statements that form one logical operation — insert the job run, then one task run per task — so it needs *one* connection rather than "any executor": it takes `conn: &mut SqliteConnection` and passes `&mut *conn` down to each single-statement method it calls. Don't instead make it generic over `E: sqlx::Executor + sqlx::Acquire` and acquire inside — that compiles on its own, but the method can then no longer be called with a `&mut SqliteConnection` from anywhere the future has to be `Send` (an axum handler, a `tokio::spawn`), where rustc rejects it with *"implementation of `sqlx::Acquire` is not general enough"*. A caller holding only a pool does `let mut conn = conn_pool.acquire().await?;` first.
- If the entity is seeded from YAML config on startup, wire it into `CRUD::init` in [src/crud/crud.rs](../../../src/crud/crud.rs), incrementing the shared `row_id` counter and inserting inside the existing transaction.
- Add the corresponding table migration if the table doesn't exist yet — the [db-schema skill](../db-schema/SKILL.md) owns where it goes, how it is named, and whether an existing migration may be edited instead.
