# Migration files

Disk migrations live in `db/schemas/disk/migrations/`, memory ones in `db/schemas/memory/migrations/` (which one: the restart question in the [entities skill](../../entities/SKILL.md)). Each is its own `sqlx::migrate!` call in [src/toolkit.rs](../../../../src/toolkit.rs) with its own `_sqlx_migrations` table, so versions never collide across the two.

**Only the disk side is a history.** `mem`'s `_sqlx_migrations` starts empty in every process, so a memory table is changed by editing its `CREATE TABLE`, never by adding a file — see [editing-vs-adding.md](editing-vs-adding.md).

## Naming

- New table: `YYYYMMDDHHMMSS_create_<table>_table.sql`.
- Change to an existing table: `YYYYMMDDHHMMSS_<verb>_<table>_<what>.sql` (e.g. `..._add_task_run_secret_env.sql`). Every disk migration is currently a `create_*`; this form is for once a create can no longer be edited.

sqlx sorts by the leading version, so **the timestamp is the declared order** and, on a fresh database, the run order.

**Use a real current timestamp; backdating doesn't skip a file, it's worse.** `Migrator::run` applies any version missing from `_sqlx_migrations`, regardless of order. A backdated file runs last on an existing database but in position on a fresh one (`cargo test`), so the two end up with different effective histories.

## No schema prefix inside a migration

Each migration runs against its own database: `CREATE TABLE job`, never `CREATE TABLE mem.job`. The `mem.` prefix belongs only in CRUD SQL, which runs on a connection with `mem` attached beside the disk file.

## How they get run

`Toolkit::get_conn_pool` and `get_conn` open the disk file, `ATTACH` the in-memory database as `mem`, and run the **disk** migrations. Memory migrations run separately via `update_memory_schema`; `CRUD::init` then re-seeds `mem` from YAML. Every test builds its own database from the migrations.

## Never delete an applied migration file

sqlx also errors when a recorded migration has no file:

```
migration <version> is not present in the migration source
```

Once anything has run it, a migration is append-only. Undo a change with a new migration that reverses it.
