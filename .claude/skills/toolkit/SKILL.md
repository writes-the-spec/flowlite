---
name: toolkit
description: Toolkit (src/toolkit.rs) - the one thing that opens flowlite.db, runs its migrations and hands out connections. Use when a command or service needs a connection or a pool, when "database is locked" appears while two connections open a fresh data directory, when changing connection pragmas or OPEN_LOCK, or when reaching for the current time.
---

# Toolkit (src/toolkit.rs)

`Toolkit` = an `AppConfig`, and the only thing that opens `flowlite.db`. It holds what happened - runs, attempts, output ([entities skill](../entities/SKILL.md)). What the YAML declares is not in a database at all: it is a `Declared` struct, loaded by `Declared::load(&app_config)` ([yaml skill](../yaml/SKILL.md)).

## Two ways in

| Call | Gives you | Migrates |
|---|---|---|
| `get_conn()` | one `flowlite.db` connection | yes |
| `get_conn_pool()` | a pool | yes, once |

Every connection opens with WAL, `synchronous=NORMAL` and a 5 s busy timeout (`disk_connect_options`).

## OPEN_LOCK

Both calls hold `OPEN_LOCK` across connect **and** migrate. A fresh database needs it twice over:

- **WAL switch.** Turning a new file into WAL takes an exclusive lock that `busy_timeout` cannot wait on; a second connection opening mid-switch fails at once with **"database is locked"**. Two concurrent MCP calls on a new data directory hit this.
- **Migrations.** sqlx-sqlite's `Migrate::lock` is a no-op, so two connections migrating one fresh database both run the same `CREATE TABLE`s.

In-process only: two processes first-opening one fresh data dir can still collide; only `serve` holds `ServeLock` ([serve_state skill](../serve_state/SKILL.md)).

`update_disk_schema` calls `Migrator::run_direct`, not `.run()`: `.run()`'s `E: Acquire<'e>` bound can't be proven `Send` in an rmcp `#[tool]` future (*"implementation of `sqlx::Acquire` is not general enough"*; see the [crud skill](../crud/SKILL.md)). Migrations themselves: [db-schema skill](../db-schema/SKILL.md).

## Rules

- **Nothing but `Toolkit` opens a database** - no connection strings or `SqlitePoolOptions` elsewhere.
- **`toolkit.get_current_ts()` stamps every `created_at` CRUD writes**, and `Scheduler` and `JobRunReleaser` read "now" through it. It isn't a program-wide clock: other services, the router and the CLI call `Utc::now()`, so tests control time with backdated rows (`TestDb::backdate_task_run_attempt`).
- **`Toolkit` is cheap to clone**; services hold an `Arc<Toolkit>`.
