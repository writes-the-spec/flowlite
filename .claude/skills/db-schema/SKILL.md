---
name: db-schema
description: How a column is declared and how a schema change lands in flowlite - NOT NULL vs nullable, why no column carries a DEFAULT, the two migration histories, sqlx checksums, and the call between editing an existing migration and adding a new one. Use this whenever you write, edit or name a migration file, add/drop/rename a column, choose a column's nullability or type, wonder whether something should be Option<T>, hit "migration was previously applied but has been modified", need to reset a dev database, or are about to reach for ALTER TABLE. Read it BEFORE writing the migration, not after it fails - which file you change is hard to undo. For what each table holds and who reads it, see the entities skill.
---

# Landing a schema change

Two databases, two independent migration histories, and no `DEFAULT` clauses anywhere. Those three facts decide almost every schema change in this repo, and they interact in a way that is not obvious until it bites.

| | Disk | Memory (`mem`) |
|---|---|---|
| Files | `db/schemas/disk/migrations/` | `db/schemas/memory/migrations/` |
| Run by | `Toolkit::update_disk_schema` | `Toolkit::update_memory_schema` |
| Holds | runs — data that must survive a restart | config parsed from YAML, re-seeded every startup |
| Lives | `<data_dir>/flowlite.db` | `file:flowlite_mem?mode=memory&cache=shared` |

Each history is a separate `sqlx::migrate!` with its own `_sqlx_migrations` table, so versions never collide across the two and a disk migration knows nothing about a memory one. Pick the database with the restart question — *if the process restarts, should this row still exist?* — see the [entities skill](../entities/SKILL.md).

**Write no schema prefix inside a migration.** Each one runs against its own database, so it says `CREATE TABLE job`, not `CREATE TABLE mem.job`. The `mem.` prefix appears only later, in the SQL your CRUD methods write against the attached database.

**Name it `YYYYMMDDHHMMSS_create_<table>_table.sql`**, or `..._<verb>_<table>_<what>.sql` for a change to an existing table. sqlx orders by that leading version, so the timestamp is the ordering — a file whose timestamp sorts before an already-applied one will not run.

## Edit the migration, or add a new one?

This is the decision that matters, and it is one-way: an edit that breaks a database is only fixable by resetting it.

**`sqlx::migrate!` checksums every migration it applies.** On startup it re-reads the files and compares. Change one byte of an *applied* file and the process refuses to start:

```
migration <version> was previously applied but has been modified
```

So the question is never "is editing nicer?" — it is **"has any database applied this migration yet?"** Check, don't assume:

```bash
sqlite3 "<data_dir>/flowlite.db" \
  "SELECT version, description FROM _sqlx_migrations ORDER BY version;"
```

Also check the default location, which is where a dev instance lands if nobody passed `-D`: `~/Library/Application Support/flowlite/flowlite.db` on macOS, `~/.local/share/flowlite/flowlite.db` on Linux. A memory migration is a different case entirely — `mem` is rebuilt from nothing on every startup, so **no memory migration is ever "already applied"** anywhere but the running process, and editing one is always safe.

- **Nothing has applied it** — typically a table you added minutes ago, pre-release, on a branch nobody has run. **Edit the original.** One migration that describes the table as it actually is beats a create plus an alter that undoes half of it, and the history stays readable as a description of the schema rather than a diary.
- **Something has applied it** — someone's dev database, a deployed instance, CI with a persisted volume. **Add a new migration.** Never edit.
- **Unsure** — add a new one. The cost of an unnecessary migration is one extra file; the cost of a broken checksum is somebody's database.

When an edit is the right call and a stale dev database is the only thing in the way, deleting that database is the fix (it holds runs, not config — config comes back from YAML). Say so out loud rather than doing it silently: it is somebody's run history.

## What a column may say

Two rules constrain every column this repo declares. They are DDL rules, so they belong here — but they are also why the entity structs in `src/crud/` look the way they do, and, as the next section shows, why `ALTER TABLE` gets you less far in this repo than it would elsewhere.

**`NOT NULL` whenever there is a logical null value.** If a column's type has a natural empty value — `''` for text, `0` for a counter, `'[]'` for a JSON list — *that value is the null*: declare `NOT NULL` and write the empty value explicitly on insert. A nullable column is only right when "no value" is a real, distinct state no ordinary value can express, which in this schema means timestamps that have not happened yet (`started_at`, `finished_at`, `next_run`) and genuinely open-ended bounds (`start_date`, `end_date`).

The rule keeps two spellings of "nothing" from coexisting. Once a text column is nullable, `NULL` and `''` both mean "no output" and no query can be written without an `OR ... IS NULL`. It pays off in Rust too: a `NOT NULL` column is a plain `String`/`u32` rather than an `Option<...>`, so no call site has to invent a meaning for `None`.

**No `DEFAULT` clauses.** Every insert supplies every column it owns, and every value is bound from Rust — there is currently no constant left in any `INSERT` literal in `src/crud/`. An empty value is bound as the empty value (`job_description: String::new()` for a job with no description; `depends_on: Vec::new()`, which `sqlx::types::Json` writes as `'[]'`), and every timestamp binds `self.toolkit.get_current_ts()`, never `CURRENT_TIMESTAMP` — so `Toolkit` stays the single source of "now" and every timestamp in the schema shares one format, RFC3339 with subseconds, directly comparable in SQL.

Two reasons for it. The value a row gets should be readable from the insert rather than from DDL written migrations ago; and `DEFAULT` combined with `NOT NULL` makes a forgotten column succeed quietly instead of failing, which is the one outcome you cannot debug from the row afterwards.

**Together they are what makes `ADD COLUMN` awkward here**, which the next section works through. The awkwardness is doing its job: it means a new column on a populated table forces a decision about what the existing rows should say, instead of letting `DEFAULT ''` answer that question silently for you.

## Why this repo edits more often than most

The two conventions from the [entities skill](../entities/SKILL.md) — **every column `NOT NULL` unless "no value" is a genuinely distinct state**, and **no `DEFAULT` clauses** — collide with what SQLite's `ALTER TABLE` can do. So "just add an ALTER" is not always available:

| Change | SQLite | In this repo |
|---|---|---|
| `ADD COLUMN` nullable | fine | fine |
| `ADD COLUMN ... NOT NULL` to an **empty** table | fine, no default needed | fine |
| `ADD COLUMN ... NOT NULL` to a table **with rows** | *"Cannot add a NOT NULL column with default value NULL"* — needs a non-null `DEFAULT` | blocked: the schema does not use `DEFAULT`, so this needs a rebuild |
| `DROP COLUMN` | 3.35+ | fine |
| `RENAME COLUMN` | 3.25+ | fine |
| Change a type, add a constraint, reorder | unsupported | rebuild: create the new table, `INSERT INTO ... SELECT`, drop, rename |

Note the middle two rows: the same `ALTER` that works on an empty table fails once a single row exists. That is why "does anything have this data?" is the question, not "is the table new?"

**Adding a `NOT NULL` column to a table that already has rows is the case to think hardest about.** The honest options are a rebuild migration, or a nullable column (which then needs a reason why `NULL` means something no ordinary value can express — usually it does not, and that is the convention telling you something). Reaching for `DEFAULT ''` to get past it puts two spellings of "nothing" in the column and is what the convention exists to prevent.

## Worked example

Adding `task_run_id`, `job_run_id`, `job_id` and `task_id` to `task_run_attempt_output`, a table created earlier the same day:

1. Checked `_sqlx_migrations` in both default data directories. No database had the table at all — the migration existed only on the branch.
2. **Edited `20260908120000_create_task_run_attempt_output_table.sql`** to include the four columns, their foreign keys and their indexes, rather than adding an alter.
3. Ran `cargo test`, which builds each test's database from the migrations, so a broken file fails there first.

Had one real database applied it, step 2 would have been a second migration instead — and since the four columns are `NOT NULL`, that migration would have had to be a rebuild if that database had any output rows, or a plain `ADD COLUMN` if it did not.

## Before you finish

- `cargo test` — every test builds a fresh database from the migrations, so it catches a syntax error, a bad foreign key, or an ordering problem.
- Run the app once against a real data directory if you edited an applied file, since the checksum check only happens on startup: `cargo run -- -D /tmp/probe serve`.
- Update the table's reference file under `.claude/skills/entities/references/`, and the object table in that skill's `SKILL.md` for a new table. A schema the map does not describe is worse than one nobody documented, because the map is what the next reader trusts.
