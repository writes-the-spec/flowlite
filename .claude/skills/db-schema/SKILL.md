---
name: db-schema
description: How a table is declared in flowlite and how a schema change lands - how it is keyed, NOT NULL vs nullable, why no column carries a DEFAULT, how foreign keys work across the two databases, the two migration histories, sqlx checksums, and the call between editing an existing migration and adding a new one. Use this whenever you write, edit or name a migration file, add/drop/rename a column, choose a primary key or a foreign key, choose a column's nullability or type, wonder whether something should be Option<T>, hit "migration was previously applied but has been modified", need to reset a dev database, or are about to reach for ALTER TABLE. Read it BEFORE writing the migration, not after it fails - which file you change is hard to undo. For what each table holds and who reads it, see the entities skill.
---

# Landing a schema change

| | Disk | Memory (`mem`) |
|---|---|---|
| Files | `db/schemas/disk/migrations/` | `db/schemas/memory/migrations/` |
| Run by | `Toolkit::update_disk_schema` | `Toolkit::update_memory_schema` |
| Holds | runs — must survive a restart | config parsed from YAML, re-seeded every startup |
| Lives | `<data_dir>/flowlite.db` | `file:flowlite_mem?mode=memory&cache=shared` |
| Changing a table | a real history: a file any database has applied is never edited | **no migration — edit the initial `CREATE TABLE`** |

**Decide first:** *if the process restarts, should this row still exist?* Yes → disk, no → `mem`. That settles the directory, the key style and whether you write a migration at all. The [entities skill](../entities/SKILL.md) maps what exists.

## `mem` never needs a migration

The memory schema is built from nothing every startup, so nothing has "already applied" a memory migration. A `mem` change is always an edit to its `CREATE TABLE` — never an `add_*` file beside `20260705153000_create_task_table.sql`.

## The one rule: sqlx checksums

**`sqlx::migrate!` checksums every migration it applies.** Change one byte of a file a database has run and that process refuses to start until the file is restored or the database reset. So ask **"has any database applied this migration?"** — by looking:

```bash
sqlite3 "<data_dir>/flowlite.db" \
  "SELECT version, description FROM _sqlx_migrations ORDER BY version;"
```

Nothing has → edit the original. Something has, or unsure → add a new migration.

## Read the topic you need

| Read | When |
|---|---|
| [migrations.md](references/migrations.md) | Writing or naming a migration; timestamp ordering; why `mem.` must not appear inside one; deleting one. |
| [editing-vs-adding.md](references/editing-vs-adding.md) | **Changing anything about an existing migration**; *"migration was previously applied but has been modified"*; whether to reset a dev database. |
| [declaring-a-table.md](references/declaring-a-table.md) | Choosing a primary key, nullability or a foreign key; whether a field is `Option<T>`; tempted to write a `DEFAULT`. |
| [altering-a-table.md](references/altering-a-table.md) | Adding, dropping, renaming or retyping a column on an existing table — especially `NOT NULL` on a table with rows, which needs a rebuild. |

## Before you finish

- **`cargo test`** — every test builds its database from the migrations, catching syntax errors, bad foreign keys and bad ordering first.
- **If you edited a file that had been applied, start the app once against a real data directory** (checksums are only compared on startup): `cargo run -- -D /tmp/probe serve`.
- **Update the table's file** under `.claude/skills/entities/references/`, and the entities `SKILL.md` table if the table is new — readers trust the map instead of the DDL.
