# Altering a table

Two rules from [declaring-a-table.md](declaring-a-table.md) — `NOT NULL` unless "no value" is a distinct state, and no `DEFAULT` — limit what SQLite's `ALTER TABLE` can do here.

| Change | SQLite | In this repo |
|---|---|---|
| `ADD COLUMN` nullable | fine | fine |
| `ADD COLUMN ... NOT NULL`, **empty** table | fine | fine |
| `ADD COLUMN ... NOT NULL`, table **with rows** | *"Cannot add a NOT NULL column with default value NULL"* — needs a non-null `DEFAULT` | rebuild (no `DEFAULT` allowed) |
| `DROP COLUMN` | 3.35+ | fine |
| `RENAME COLUMN` / `RENAME TO` | 3.25+ | fine |
| Change a type, add/drop a constraint, reorder columns | unsupported | rebuild |

The same `ALTER` that works on an empty table fails once one row exists, so ask "does anything have rows in this table?", not "is it new?".

## `NOT NULL` column on a populated table

Both easy outs are wrong:

- **`DEFAULT ''`** creates two spellings of "nothing" — what the rules exist to prevent.
- **Making it nullable** needs `NULL` to mean something no ordinary value can. If you can't say what, the column isn't thought through yet.

Decide what existing rows should say. A real empty value → rebuild and write it explicitly. "Unknown for old rows" → a genuine distinct state, so nullable is right.

## The rebuild pattern

```sql
CREATE TABLE task_run_new (
    -- the full new definition
);

INSERT INTO task_run_new (id, job_run_id, ...)
SELECT id, job_run_id, ... FROM task_run;

DROP TABLE task_run;

ALTER TABLE task_run_new RENAME TO task_run;

-- DROP TABLE took the indexes with it.
CREATE INDEX idx_task_run_job_run_id ON task_run (job_run_id);
```

- **Recreate every index** the old table had, or reads silently slow down.
- **Foreign keys pointing at the table** reference it by name and `PRAGMA foreign_keys` is on: rebuild inside the migration's transaction and run `PRAGMA foreign_key_check` after the rename.
- **Name both column lists** in `INSERT ... SELECT`; `SELECT *` silently mispairs columns once the definitions differ.
