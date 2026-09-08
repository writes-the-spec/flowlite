# Altering a table

Two of the rules in [declaring-a-table.md](declaring-a-table.md) — **`NOT NULL` unless "no value" is a genuinely distinct state**, and **no `DEFAULT` clauses** — collide with what SQLite's `ALTER TABLE` can actually do. So "just add an ALTER" is not always available.

| Change | SQLite | In this repo |
|---|---|---|
| `ADD COLUMN` nullable | fine | fine |
| `ADD COLUMN ... NOT NULL` to an **empty** table | fine, no default needed | fine |
| `ADD COLUMN ... NOT NULL` to a table **with rows** | *"Cannot add a NOT NULL column with default value NULL"* — needs a non-null `DEFAULT` | blocked: the schema does not use `DEFAULT`, so this needs a rebuild |
| `DROP COLUMN` | 3.35+ | fine |
| `RENAME COLUMN` | 3.25+ | fine |
| `RENAME TO` | 3.25+ | fine |
| Change a type, add or drop a constraint, reorder columns | unsupported | rebuild |

**Note the middle two rows.** The same `ALTER` that works on an empty table fails once a single row exists — the error is about the *default value being NULL*, not about the table being new. That is why the question is "does anything have rows in this table?", not "is the table new?".

## Adding a `NOT NULL` column to a populated table

This is the case to think hardest about, because the two easy ways out are both wrong here.

- **Reaching for `DEFAULT ''`** puts two spellings of "nothing" in the column, which is exactly what the no-`DEFAULT` and `NOT NULL` rules exist to prevent.
- **Making it nullable to dodge the problem** needs a reason why `NULL` means something no ordinary value can express. Usually it does not — and that difficulty is the convention telling you the column has not been thought through yet.

The honest options are a rebuild, or an answer to "what should the existing rows say?" that you can defend. If the answer is a real empty value, a rebuild writes it explicitly. If the answer is "we do not know for old rows", that is a genuine distinct state and nullable is right.

## The rebuild pattern

SQLite's own recommended sequence, for anything the table above calls a rebuild:

```sql
CREATE TABLE task_run_new (
    -- the full new definition, columns and constraints as you want them
);

INSERT INTO task_run_new (id, job_run_id, ...)
SELECT id, job_run_id, ... FROM task_run;

DROP TABLE task_run;

ALTER TABLE task_run_new RENAME TO task_run;

-- Indexes go with the old table, so recreate every one of them here.
CREATE INDEX idx_task_run_job_run_id ON task_run (job_run_id);
```

Three things that bite:

- **`DROP TABLE` drops the indexes with it.** Recreate every index the old table had, or reads quietly get slower and nothing fails.
- **Foreign keys pointing *at* this table** reference it by name. `PRAGMA foreign_keys` is on, so do the rebuild inside the migration's transaction and check the references still resolve; `PRAGMA foreign_key_check` after the rename is the cheap way to be sure.
- **Column order in the `INSERT ... SELECT` matters.** Name both lists explicitly rather than relying on `SELECT *`, which silently pairs the wrong columns once the definitions differ.
