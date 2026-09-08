# Migration files

Two databases, two independent migration histories. Which one a change belongs to follows from the restart question — *if the process restarts, should this row still exist?* — answered in the [entities skill](../../entities/SKILL.md).

| | Disk | Memory (`mem`) |
|---|---|---|
| Files | `db/schemas/disk/migrations/` | `db/schemas/memory/migrations/` |
| Run by | `Toolkit::update_disk_schema` | `Toolkit::update_memory_schema` |
| Holds | runs — data that must survive a restart | config parsed from YAML, re-seeded every startup |
| Lives | `<data_dir>/flowlite.db` | `file:flowlite_mem?mode=memory&cache=shared` |

Each is a separate `sqlx::migrate!` call in [src/toolkit.rs](../../../../src/toolkit.rs) with its own `_sqlx_migrations` table, so versions never collide across the two and a disk migration knows nothing about a memory one.

## Naming

`YYYYMMDDHHMMSS_create_<table>_table.sql`, or `..._<verb>_<table>_<what>.sql` for a change to an existing table — `20260908120100_drop_task_run_attempt_output_columns.sql`.

sqlx sorts by that leading version, so **the timestamp is the declared order** — and on a fresh database it is also the order things actually run in.

**Backdating a file does not skip it, which is worse.** `Migrator::run` decides per migration by asking whether that *version* is already in `_sqlx_migrations`, not whether it is newer than the last applied one. So a file numbered below migrations that have already run still gets applied — just last, out of its declared position. A database that already existed then has a different effective history from a fresh one built by `cargo test`, and anything order-dependent behaves differently between the two. Use a real current timestamp.

## Write no schema prefix inside a migration

Each migration runs against its own database, so it says `CREATE TABLE job`, never `CREATE TABLE mem.job`. The `mem.` prefix appears only later, in the SQL your CRUD methods write against the attached database — those run on a connection where `mem` is attached alongside the disk file, so there the prefix is what picks the database.

## How they get run

`Toolkit::get_conn_pool` and `get_conn` open the disk file, `ATTACH` the in-memory database as `mem`, and run the **disk** migrations. The memory migrations run separately, via `update_memory_schema`, because `mem` is created empty in every new process and rebuilt from YAML by `CRUD::init`.

A consequence worth knowing: **every test builds its own database from the migrations**, so a syntax error, a bad foreign key or a bad ordering fails in `cargo test` before it ever reaches a real instance.

## Never delete an applied migration file

sqlx validates in both directions. As well as checksumming files that were applied, it errors if a migration recorded in `_sqlx_migrations` has no file any more:

```
migration <version> is not present in the migration source
```

So a migration is append-only once anything has run it — the same condition that governs [editing one](editing-vs-adding.md). Undoing a change means a new migration that reverses it, not removing the file that made it.
