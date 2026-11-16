---
name: db-schema
description: How a table is declared in flowlite and how a schema change lands - how it is keyed, NOT NULL vs nullable, why no column carries a DEFAULT, how foreign keys work, the migration history, sqlx checksums, and the call between editing an existing migration and adding a new one. Use this whenever you write, edit or name a migration file, add/drop/rename a column, choose a primary key or a foreign key, choose a column's nullability or type, wonder whether something should be Option<T>, hit "migration was previously applied but has been modified", need to reset a dev database, or are about to reach for ALTER TABLE. Read it BEFORE writing the migration, not after it fails - which file you change is hard to undo. For what each table holds and who reads it, see the entities skill.
---

# Landing a schema change

One database, `<project_dir>/.flowlite/flowlite.db`, with one migration history in `db/schemas/disk/migrations/`, run by `Toolkit::update_disk_schema`. It holds runs — what must survive a restart.

**Decide first:** *if the process restarts, should this still exist?* No → it is config, a field on a `Declared` struct loaded from YAML, and needs no table or migration at all ([yaml skill](../yaml/SKILL.md)). Yes → a table here. The [entities skill](../entities/SKILL.md) maps what exists.

## The one rule: sqlx checksums

**`sqlx::migrate!` checksums every migration it applies.** Change one byte of a file a database has run and that process refuses to start until the file is restored or the database reset. So ask **"has any database applied this migration?"** — by looking:

```bash
sqlite3 "<project_dir>/.flowlite/flowlite.db" \
  "SELECT version, description FROM _sqlx_migrations ORDER BY version;"
```

Nothing has → edit the original. Something has, or unsure → add a new migration.

## Read the topic you need

| Read | When |
|---|---|
| [migrations.md](references/migrations.md) | Writing or naming a migration; timestamp ordering; deleting one. |
| [editing-vs-adding.md](references/editing-vs-adding.md) | **Changing anything about an existing migration**; *"migration was previously applied but has been modified"*; whether to reset a dev database. |
| [declaring-a-table.md](references/declaring-a-table.md) | Choosing a primary key, nullability or a foreign key; whether a field is `Option<T>`; tempted to write a `DEFAULT`. |
| [altering-a-table.md](references/altering-a-table.md) | Adding, dropping, renaming or retyping a column on an existing table — especially `NOT NULL` on a table with rows, which needs a rebuild. |

## Before you finish

- **`cargo test`** — every test builds its database from the migrations, catching syntax errors, bad foreign keys and bad ordering first.
- **If you edited a file that had been applied, start the app once against a real project directory** (checksums are only compared on startup): `cargo run -- -p /tmp/probe serve`.
- **Update the table's file** under `.claude/skills/entities/references/`, and the entities `SKILL.md` table if the table is new — readers trust the map instead of the DDL.
