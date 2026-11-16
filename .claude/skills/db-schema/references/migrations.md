# Migration files

Migrations live in `db/schemas/disk/migrations/`, applied by the one `sqlx::migrate!` call in [src/toolkit.rs](../../../../src/toolkit.rs) and recorded in `flowlite.db`'s `_sqlx_migrations`. They are a history: see [editing-vs-adding.md](editing-vs-adding.md) before changing one.

## Naming

- New table: `YYYYMMDDHHMMSS_create_<table>_table.sql`.
- Change to an existing table: `YYYYMMDDHHMMSS_<verb>_<table>_<what>.sql` (e.g. `..._add_task_run_secret_env.sql`). Every disk migration is currently a `create_*`; this form is for once a create can no longer be edited.

sqlx sorts by the leading version, so **the timestamp is the declared order** and, on a fresh database, the run order.

**Use a real current timestamp; backdating doesn't skip a file, it's worse.** `Migrator::run` applies any version missing from `_sqlx_migrations`, regardless of order. A backdated file runs last on an existing database but in position on a fresh one (`cargo test`), so the two end up with different effective histories.

## How they get run

`Toolkit::get_conn_pool` and `get_conn` open `flowlite.db` and run the migrations, under `OPEN_LOCK` ([toolkit skill](../../toolkit/SKILL.md)). Every test builds its own database from the migrations.

## Never delete an applied migration file

sqlx also errors when a recorded migration has no file:

```
migration <version> is not present in the migration source
```

Once anything has run it, a migration is append-only. Undo a change with a new migration that reverses it.
