# Edit an existing migration, or add a new one?

This is the decision that matters most, because it is one-way: an edit that breaks a database is only fixable by resetting that database.

## Why editing is dangerous at all

**`sqlx::migrate!` checksums every migration it applies** and stores the hash in `_sqlx_migrations`. On startup it re-reads the files and compares. Change one byte of an *applied* file and the process refuses to start:

```
migration <version> was previously applied but has been modified
```

Nothing recovers from that except restoring the file byte-for-byte or resetting the database.

## So the question is not "is editing nicer?"

It is **"has any database applied this migration yet?"** Check, don't assume:

```bash
sqlite3 "<data_dir>/flowlite.db" \
  "SELECT version, description FROM _sqlx_migrations ORDER BY version;"
```

Check the default location too, which is where a dev instance lands if nobody passed `-D`:

- macOS — `~/Library/Application Support/flowlite/flowlite.db`
- Linux — `~/.local/share/flowlite/flowlite.db`

**None of this applies to `mem`.** The memory schema is built from nothing on every startup, so no memory migration is ever "already applied" anywhere but the running process. There is no checksum to break and no database to reset, which means **a `mem` table never needs a migration at all**: change its `CREATE TABLE` and restart. Do not check `_sqlx_migrations` for a memory migration, and do not add an `add_*` file beside a memory `create_*` file — the question this page answers only arises on the disk side.

## The call

| State | Do | Why |
|---|---|---|
| **Nothing has applied it** — a table added minutes ago, pre-release, on a branch nobody has run | **Edit the original** | One migration describing the table as it actually is beats a create plus an alter that undoes half of it. The history stays a description of the schema rather than a diary of your afternoon. |
| **Something has applied it** — someone's dev database, a deployed instance, CI with a persisted volume | **Add a new migration.** Never edit | The checksum will refuse to start their process, and they cannot fix it without losing data. |
| **Unsure** | **Add a new one** | An unnecessary migration costs one file. A broken checksum costs somebody's database. |
| **It is a `mem` table** | **Edit the `CREATE TABLE`.** Never add a migration | Nothing has applied it but the running process, and the next startup rebuilds the schema from scratch. A memory `add_*` file is always the wrong answer. |

When an edit is the right call and a stale dev database is the only thing in the way, deleting that database is the fix — it holds runs, not config, and config comes back from YAML on the next startup. **Say so out loud rather than doing it silently:** it is somebody's run history, and it is theirs to spend.

## Worked example

Adding `task_run_id`, `job_run_id`, `job_id` and `task_id` to `task_run_attempt_output`, a table created earlier the same day:

1. Queried `_sqlx_migrations` in both default data directories. No database had the table at all — the migration existed only on the branch.
2. **Edited `20260908120000_create_task_run_attempt_output_table.sql`** to include the four columns, their foreign keys and their indexes, rather than adding an alter that would have amended a table nobody had ever created.
3. Ran `cargo test`, which builds each test's database from the migrations, so a broken file fails there first.

Had one real database applied it, step 2 would have been a second migration — and since the four columns are `NOT NULL`, that migration would have had to be a rebuild if that database held any output rows, or a plain `ADD COLUMN` if it did not. See [altering-a-table.md](altering-a-table.md).
