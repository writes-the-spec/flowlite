# Edit an existing migration, or add a new one?

This decision is one-way: an edit that breaks a database is fixable only by resetting it.

## Why editing is dangerous

`sqlx::migrate!` stores a checksum of each applied migration in `_sqlx_migrations` and compares on startup. Change one byte of an *applied* file and the process refuses to start:

```
migration <version> was previously applied but has been modified
```

Only restoring the file byte-for-byte or resetting the database recovers.

## Check, don't assume

```bash
sqlite3 "<data_dir>/flowlite.db" \
  "SELECT version, description FROM _sqlx_migrations ORDER BY version;"
```

Also check the default location, where a dev instance lands without `-D`:

- macOS — `~/Library/Application Support/flowlite/flowlite.db`
- Linux — `~/.local/share/flowlite/flowlite.db`

**`mem` is exempt.** The memory schema is rebuilt from nothing every startup, so there is no checksum to break and no `_sqlx_migrations` to check — edit the `CREATE TABLE` and restart.

## The call

| State | Do | Why |
|---|---|---|
| **Nothing has applied it** — added minutes ago, pre-release, on a branch nobody ran | **Edit the original** | One migration describing the table beats a create plus an alter undoing half of it. |
| **Something has applied it** — a dev database, a deployed instance, CI with a persisted volume | **Add a new migration.** Never edit | The checksum stops their process and they can't fix it without losing data. |
| **Unsure** | **Add a new one** | An extra migration costs one file; a broken checksum costs a database. |
| **A `mem` table** | **Edit the `CREATE TABLE`.** Never add a migration | Nothing but the running process has applied it. |

When an edit is right and only a stale dev database is in the way, deleting that database is the fix — config comes back from YAML on startup, but it holds somebody's run history, so **say so rather than doing it silently**.

Example: four `NOT NULL` columns added to `task_run_attempt_output` the day it was created, when no database had it, went into `20260908120000_create_task_run_attempt_output_table.sql` itself. Had a database applied it, they'd need a new migration — a rebuild if it held rows ([altering-a-table.md](altering-a-table.md)).
