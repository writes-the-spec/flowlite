---
name: db-storage
description: Decide whether a new table's data belongs in the in-memory (YAML-seeded) SQLite schema or the persisted (disk) SQLite database, add its migration in the right place, and write its columns (NOT NULL vs nullable, no DEFAULT clauses). Use when adding a new table, adding a new migration, adding or changing a column, or deciding where a piece of data should live.
---

# Two SQLite databases: in-memory (config) vs disk (runtime)

flowlite runs against **two separate SQLite databases per connection**, and which one a table belongs in is decided by one question: **does this data come from YAML config, or does it get created while the app runs?**

- **YAML config → always the in-memory `mem` schema.** `job`, `task`, `task_dependent`, `schedule`, `schedule_job` are all parsed from files under the config dir (`jobs/*.yml`, `schedules/*.yml`) and re-inserted into `mem.*` tables on every startup by `CRUD::init` (see [src/crud/crud.rs](../../../src/crud/crud.rs)). This data is disposable by design — it is never hand-edited or persisted independently of the YAML files, so losing it on restart is correct, not a bug.
- **Everything else → the persisted disk database.** `job_run`, `task_run`, `task_run_attempt`, `job_run_stop` are created at runtime (a job gets submitted, a task starts, an attempt fails) and must survive process restarts, so they live as ordinary tables (no schema prefix) in the on-disk `flowlite.db` file.

If you're adding a new table and unsure which bucket it's in, ask: "if the process restarts, should this row still exist?" No → `mem`. Yes → disk.

## How the two databases are wired up ([src/toolkit.rs](../../../src/toolkit.rs))

- The disk database is a real file at `<data_dir>/flowlite.db`, opened with `mode=rwc` (`Toolkit::create_db_if_not_exists`).
- Every connection to it immediately runs `ATTACH DATABASE 'file:flowlite_mem?mode=memory&cache=shared' AS mem` — a SQLite shared-cache in-memory database, attached under the schema alias `mem`. Because it's `cache=shared`, every connection that attaches it sees the *same* in-memory data for the lifetime of the process — it isn't per-connection.
- `Toolkit::get_conn_pool` / `Toolkit::get_conn` are the normal way to get a connection: they open the disk file, attach `mem`, and run the disk migrations. This is what almost all app code should use.
- `Toolkit::get_memory_conn` connects to the `mem` database *without* the disk file at all, and runs only the memory migrations — used for standalone memory-schema setup/migration, not general query work.
- `Toolkit::update_disk_schema` runs `sqlx::migrate!("./db/schemas/disk/migrations")`; `Toolkit::update_memory_schema` runs `sqlx::migrate!("./db/schemas/memory/migrations")` — two independent migration histories, one per database.

## How a CRUD method takes its database handle

Every connection `Toolkit` hands out already has `mem` attached, so a method never picks a database — it picks how many statements it runs, and that decides its signature:

- **One statement → generic executor.** The `insert_*` / `select_*` / `update_*` method in an entity file is generic over `E: sqlx::Executor<'e, Database = sqlx::Sqlite>`, so the caller passes whatever it holds: `&*conn_pool`, `&mut conn`, or a transaction. A lone statement doesn't care which connection runs it.
- **Several statements → one connection, not a generic executor.** The methods in [src/crud/multistatements/misc.rs](../../../src/crud/multistatements/misc.rs) (`submit_job`, `rerun_job`, `is_job_at_max_active_runs`) take `conn: &mut SqliteConnection`. Two reasons: their statements form one logical operation and belong on the *same* connection rather than on whichever ones a pool hands out independently (which is also what wrapping them in a transaction would need); and making them generic over `E: sqlx::Executor + sqlx::Acquire` instead breaks every caller whose future has to be `Send` (an axum handler, a `tokio::spawn`) with *"implementation of `sqlx::Acquire` is not general enough"*. A caller holding only a pool does `let mut conn = conn_pool.acquire().await?;` first.

Whichever shape it is, the handle has to come from `Toolkit`. A connection opened any other way has no `mem` attached, so every query that reaches into a config table — `submit_job` selects `mem.job` and `mem.task` to snapshot the definition onto the run it inserts, and `is_job_at_max_active_runs` selects `mem.job` for its limit — fails on the missing schema.

## Where migrations and table names go

| | YAML-seeded (config) | Runtime (event/run) |
|---|---|---|
| Migration directory | `db/schemas/memory/migrations/` | `db/schemas/disk/migrations/` |
| Table name in SQL | schema-qualified: `mem.<table>` | bare: `<table>` (no prefix — it's already in the default/disk database) |
| Populated by | `CRUD::init`, parsing YAML under the config dir | `insert_*` calls made while handling a request/job run (e.g. `CRUD::submit_job`, in `src/crud/multistatements/misc.rs`) |
| Primary key | caller-assigned `row_id: u64`, used only to preserve YAML declaration order | DB-assigned autoincrement `id`, returned via `last_insert_rowid()` |
| Ever updated after insert? | no — insert-only, re-seeded fresh every startup | yes — these are exactly the entities that get `update_*` methods |

That primary-key and update-ability split isn't a coincidence: it falls directly out of which database the table lives in. See the [crud skill](../crud/SKILL.md) (and its [insert](../crud/references/insert.md)/[select](../crud/references/select.md)/[update](../crud/references/update.md) references) for the actual Rust struct/method conventions once you know which schema a new table belongs to.

## Column nullability: `NOT NULL` whenever there is a logical null value

If a column's type has a natural empty value — `''` for text, `0` for a counter, `'[]'` for a JSON list — **that value is the null**, so declare the column `NOT NULL` and write the empty value explicitly on insert. A nullable column is only correct when "no value" is a real, distinct state of the domain that no ordinary value can express.

The rule exists to keep two spellings of "nothing" from coexisting. Once a text column is nullable, `NULL` and `''` both mean "no output", every read has to handle both, and no query can be written without an `OR ... IS NULL`.

Reading the current schema, the split is consistent:

| Column | Declared | Why |
|---|---|---|
| `task_run_attempt.stdout` / `stderr` | `TEXT NOT NULL`, inserted as `''` | An attempt that printed nothing has empty output, not unknown output. |
| `task_run.num_attempts` | `INTEGER NOT NULL`, inserted as `0` | Zero attempts is a count, not a missing count. |
| `task.depends_on` | `TEXT NOT NULL` (JSON array) | A task with no dependencies has `'[]'`. |
| `job.description`, `schedule.description` | `TEXT NOT NULL` | The YAML defaults to `""` (`#[serde(default)]`), so the empty string arrives as a value. |
| `schedule.disabled` | `INTEGER NOT NULL` | A boolean has no third state. |
| `job_run` / `task_run` / `task_run_attempt`.`started_at`, `finished_at` | nullable | No timestamp can mean "not started" or "not finished". |
| `schedule.start_date`, `end_date`, `next_run` | nullable | An open-ended schedule genuinely has no bound, and `next_run` is unset until it is computed. |

The payoff lands in Rust: a `NOT NULL` column is a plain `String`/`u32` in the row struct instead of an `Option<...>`, so no call site has to invent a meaning for `None`. When you *do* declare a nullable column, the field is `Option<T>` and the update input is `Option<Option<T>>` — see the [crud skill](../crud/SKILL.md) for why the outer `Option` ("don't touch this column") and the inner one ("set it to NULL") are both needed.

## No `DEFAULT` clauses — defaults live in the query

Table definitions carry **no `DEFAULT`**, and every insert supplies every column it owns:

- A **constant** default goes in the SQL literal: `INSERT INTO task_run_attempt (..., stdout, stderr) VALUES (..., '', '')`.
- A **computed** default is bound from Rust: every `created_at` binds `self.toolkit.get_current_ts()`. Never `CURRENT_TIMESTAMP` — SQL functions are not used for values. `Toolkit` stays the single source of "now" for every timestamp in the schema (`started_at`, `finished_at` and `next_run` are bound the same way), so all of them share one format, RFC3339 with subseconds, and are directly comparable in SQL.

Two reasons. The value a row gets should be readable from the insert, not from the DDL of a table created migrations ago; and a `DEFAULT` combined with `NOT NULL` quietly hides a forgotten column instead of failing.

Note that migrations are checksummed by `sqlx::migrate!`, so **editing an applied migration file breaks startup** against an existing `flowlite.db` ("migration ... was previously applied but has been modified"). While the project is pre-release the fix is to delete the dev database; once it is not, a column change needs a new migration file instead.

## Adding a new table

1. Decide: YAML-seeded config data, or runtime-created data? (see the question above)
2. Add the migration file under the matching directory (`db/schemas/memory/migrations/` or `db/schemas/disk/migrations/`), following the existing timestamped filename pattern (`YYYYMMDDHHMMSS_create_<table>_table.sql`).
3. In the `CREATE TABLE` statement: no schema prefix needed in the migration itself (each migration runs against its own database, so the table is created in the right place by virtue of which migrations dir it's in) — the `mem.` prefix is only needed later, in the SQL your CRUD methods write, to tell the shared connection which attached database to query.
4. Declare each column `NOT NULL` unless "no value" is a state no ordinary value can express, and leave `DEFAULT` out entirely — the insert supplies the value (see the two sections above).
5. If it's YAML-seeded, wire the insert into `CRUD::init` (incrementing the shared `row_id` counter) so it gets populated on every startup — see [src/crud/crud.rs](../../../src/crud/crud.rs).
6. Build the CRUD file per the [crud skill](../crud/SKILL.md).
