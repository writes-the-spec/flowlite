---
name: crud
description: Add or modify CRUD entities in src/crud/ (insert/select query modules for a SQLite table). Use when adding a new table's data-access layer, adding a new filter/sort option to an existing select, or wiring a new entity into CRUD::init.
---

# CRUD module conventions (src/crud/)

Each entity has its own file under `src/crud/`, with methods on the shared `CRUD` struct ([src/crud/crud.rs](../../../src/crud/crud.rs)). A method running **one** statement lives in its entity's file. One running **several** statements for one operation spans entities, so it lives under `src/crud/multistatements/`, one file per operation:

| File | Holds |
|---|---|
| [submit_job.rs](../../../src/crud/multistatements/submit_job.rs) | `submit_job`; `resolve_job_parameters`, which checks a caller's overrides |
| [rerun_job.rs](../../../src/crud/multistatements/rerun_job.rs) | `rerun_job` |
| [job_run_definition.rs](../../../src/crud/multistatements/job_run_definition.rs) | Not an operation: the `JobRunDefinition` vocabulary those two share, the merges that build one, and `insert_job_run_definition` — the only place a run's config is written |
| [limits.rs](../../../src/crud/multistatements/limits.rs) | `is_job_at_max_parallel_runs`, `count_running_attempts`, `claimed_limit_slots`, `count_waiting_attempts` |
| [secret_env.rs](../../../src/crud/multistatements/secret_env.rs) | `check_secret_env_is_satisfied` and its pure policy |
| [job_run_descendants.rs](../../../src/crud/multistatements/job_run_descendants.rs) | `select_job_run_descendants` — runs a run's tasks submitted, recursively |
| [job_run_reads.rs](../../../src/crud/multistatements/job_run_reads.rs) | `select_job_run_with_task_runs`, `select_task_run_attempt_logs` |
| [task_run_inputs.rs](../../../src/crud/multistatements/task_run_inputs.rs) | `select_task_run_outputs`, `TaskRunOutput` — what a run's tasks produced so far, for `FLOWLITE_TASK_OUTPUT` |
| [ad_hoc_job.rs](../../../src/crud/multistatements/ad_hoc_job.rs) | `seed_ad_hoc_job`, `JobIdAlreadyInstalled` |
| [skip_job_run.rs](../../../src/crud/multistatements/skip_job_run.rs) | `skip_job_run` — a job run plus its task runs, for a run nobody started |
| [invalidate_job_run.rs](../../../src/crud/multistatements/invalidate_job_run.rs) | `invalidate_job_run` — the same pair, for a run no outcome claimed (a bug path, from `JobRunReleaser::set_to_invalid`) |
| [delete_job_run.rs](../../../src/crud/multistatements/delete_job_run.rs) | `delete_job_run` — the same pair for a `Scheduled` run removed by hand, tombstoned as `Deleted` (not erased), which frees its occurrence for the Scheduler |
| [delete_job_runs_with_children.rs](../../../src/crud/multistatements/delete_job_runs_with_children.rs) | `delete_job_runs_with_children` — erases a run's rows from all six tables |
| [retention_candidates.rs](../../../src/crud/multistatements/retention_candidates.rs) | `count_finished_job_runs`, `select_job_ids_with_finished_job_runs`, `select_deletable_job_runs` — which finished runs retention may delete; how many is the retention service's call |
| [stop_child_job_runs.rs](../../../src/crud/multistatements/stop_child_job_runs.rs) | `stop_child_job_runs` — a stop row per unfinished run an attempt submitted, when it ends unsuccessfully |

A new operation gets its own file and a `pub mod` line in `multistatements/mod.rs`; items only siblings use are `pub(super)`.

Before writing a migration, decide whether the table is YAML-seeded config or runtime data ([entities skill](../entities/SKILL.md)). That decides `mem.<table>` vs disk `<table>`, the key style, and whether it gets an `update_*`.

- [references/insert.md](references/insert.md) — input struct, `()` vs `i64` return, binding JSON/bool/enum, multi-row inserts.
- [references/select.md](references/select.md) — filter/sort/limit/offset, `QueryBuilder` with `WHERE 1=1`, filter kinds, counts, joins, singular helper.
- [references/update.md](references/update.md) — double-`Option` for nullable columns, the empty-`SET` guard.

## Rules

- One file per entity, named after the table (singular); add `pub mod widget;` to [src/crud/mod.rs](../../../src/crud/mod.rs). Cross-entity methods go under [src/crud/multistatements/](../../../src/crud/multistatements/).
- **Refusals are typed values, not sentences,** when callers word them differently: `seed_ad_hoc_job` raises `JobIdAlreadyInstalled`; `job submit -f` says "drop -f", the MCP tool "submit it by name". Callers `err.downcast::<T>()`.
- **Policy belongs to its caller.** If one caller must apply a rule and another legitimately skips it, it's the caller's: `flowlite job-run stop` refuses a settled run, the dashboard's stop route redirects — surfaces differing beats one rule in CRUD. A multistatement is earned only by statements that must run together for anyone doing the operation (`submit_job`), or a "may this happen" query each caller acts on (`is_job_at_max_parallel_runs`).
- **A multistatement contains no SQL** — no `sqlx::query`, `QueryBuilder` or statement text. It composes entity `insert_*`/`select_*`/`update_*`/`delete_*`; if one is missing, add it to the entity. Counts and projections included ([select.md](references/select.md#counts-and-projections)).
- Naming: `Insert<Entity>DataInput` in `Insert<Entity>Data { input }`; `Select<Entity>sDataFilter`, `Select<Entity>sDataSort` (enum), `Select<Entity>sData { filter, sort, limit, offset }`; `Update<Entity>sDataInput`, `Update<Entity>sDataFilter`, `Update<Entity>sData { input, filter }`; row struct `Widget` derives `sqlx::FromRow, Clone`. Don't copy `job_run_stop`'s `SelectJobRunStopsSort` (missing `Data`).
- **Single-statement methods are generic over `E: sqlx::Executor<'e, Database = sqlx::Sqlite>`**, so callers pass a pool, connection or transaction.
- **Multistatement methods take `conn: &mut SqliteConnection`** (one operation, one connection) and pass `&mut *conn` down. Not generic `E: sqlx::Executor + sqlx::Acquire`: that compiles but can't be called from a future that must be `Send` (axum handler, `tokio::spawn`) — *"implementation of `sqlx::Acquire` is not general enough"*. A pool holder does `let mut conn = conn_pool.acquire().await?;`.
- YAML-seeded entities are wired into `CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)): increment the shared `row_id` counter, insert inside the existing transaction.
- The migration — where, its name, edit vs add — is the [db-schema skill](../db-schema/SKILL.md)'s.
