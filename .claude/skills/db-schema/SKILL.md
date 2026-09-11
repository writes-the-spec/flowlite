---
name: db-schema
description: How a table is declared in flowlite and how a schema change lands - how it is keyed, NOT NULL vs nullable, why no column carries a DEFAULT, how foreign keys work across the two databases, the two migration histories, sqlx checksums, and the call between editing an existing migration and adding a new one. Use this whenever you write, edit or name a migration file, add/drop/rename a column, choose a primary key or a foreign key, choose a column's nullability or type, wonder whether something should be Option<T>, hit "migration was previously applied but has been modified", need to reset a dev database, or are about to reach for ALTER TABLE. Read it BEFORE writing the migration, not after it fails - which file you change is hard to undo. For what each table holds and who reads it, see the entities skill.
---

# Landing a schema change

Two databases, two independent migration histories, and no `DEFAULT` clauses anywhere. Those three facts decide almost every schema change here, and they interact in ways that are not obvious until they bite — a `NOT NULL` column you cannot add, a migration you cannot edit, a rebuild you did not expect.

| | Disk | Memory (`mem`) |
|---|---|---|
| Files | `db/schemas/disk/migrations/` | `db/schemas/memory/migrations/` |
| Holds | runs — data that must survive a restart | config parsed from YAML, re-seeded every startup |
| Lives | `<data_dir>/flowlite.db` | `file:flowlite_mem?mode=memory&cache=shared` |
| Changing a table | a real history: a file some database has applied can never be edited | **never needs a migration — edit the initial `CREATE TABLE`** |

**Answer this first, because it decides the rest:** *if the process restarts, should this row still exist?* Yes means disk, no means `mem`. That one answer settles which migration directory you write in, how the table is keyed, and whether you are writing a migration at all. The [entities skill](../entities/SKILL.md) has the longer version and the map of what already exists.

## `mem` never needs a migration

The memory schema is created from nothing on every startup and thrown away on every shutdown. There is no `_sqlx_migrations` row anywhere but in the running process, so nothing has "already applied" a memory migration and nothing can be broken by changing one.

So **a change to a `mem` table is an edit to its `CREATE TABLE`, always** — add the column where it belongs, drop the one nobody reads, change a type. Never add `20260911..._add_task_limits.sql` beside `20260705153000_create_task_table.sql`; put the column in the create. The files under `db/schemas/memory/migrations/` are not a history to preserve, they are the schema written once, and they should read like a description of the tables as they are today.

The "two independent migration histories" framing below is about the disk side. On the memory side there is really only a schema.

## Read the topic you need

| Read | When |
|---|---|
| [migrations.md](references/migrations.md) | Writing or naming a migration file; working out which directory, why the timestamp is the ordering, or why `mem.` must not appear inside a migration. |
| [editing-vs-adding.md](references/editing-vs-adding.md) | **Changing anything about an existing migration.** Also for *"migration was previously applied but has been modified"*, and for deciding whether a dev database has to be reset. |
| [declaring-a-table.md](references/declaring-a-table.md) | Choosing a primary key, a column's nullability or a foreign key; wondering whether a field should be `Option<T>`; tempted to write a `DEFAULT`. |
| [altering-a-table.md](references/altering-a-table.md) | Adding, dropping, renaming or retyping a column on a table that already exists — especially adding a `NOT NULL` one to a table with rows, which needs a rebuild. |

## The one rule to carry in your head

**`sqlx::migrate!` checksums every migration it applies.** Change one byte of a file some database has already run and that database's process refuses to start, recoverable only by restoring the file exactly or resetting the database.

So the question is never "is editing nicer?" — it is **"has any database applied this migration yet?"**, and it is answered by looking, not by assuming:

```bash
sqlite3 "<data_dir>/flowlite.db" \
  "SELECT version, description FROM _sqlx_migrations ORDER BY version;"
```

Nothing has applied it — edit the original, and the history stays a description of the schema rather than a diary. Something has, or you are not sure — add a new migration. [editing-vs-adding.md](references/editing-vs-adding.md) has the full call, the default database locations to check, and why `mem` migrations are exempt.

## Before you finish

- **`cargo test`** — every test builds its own database from the migrations, so this catches a syntax error, a bad foreign key or a bad ordering before any real instance sees it.
- **Start the app once against a real data directory if you edited a file that had been applied**, since the checksum comparison only happens on startup: `cargo run -- -D /tmp/probe serve`.
- **Update the table's reference file** under `.claude/skills/entities/references/`, and the entity table in that skill's `SKILL.md` if the table is new. A schema the map does not describe is worse than one nobody documented, because the map is what the next reader trusts instead of reading the DDL.
