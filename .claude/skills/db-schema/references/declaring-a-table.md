# Declaring a table

Four rules constrain this repo's DDL and shape the entity structs in `src/crud/`. The middle two are why [`ALTER TABLE` gets you less far here](altering-a-table.md): a new column on a populated table must force a decision about what existing rows say, not let `DEFAULT ''` decide silently. Config is not a table at all — the restart question in the [entities skill](../../entities/SKILL.md).

## Keys

`id INTEGER PRIMARY KEY AUTOINCREMENT`, returned via `last_insert_rowid()`: a row is created once by something that then needs to refer to it. See the [crud skill](../../crud/SKILL.md).

## `NOT NULL` whenever there is a logical null value

If the type has a natural empty value — `''` text, `0` counter, `'[]'` JSON list — that is the null: declare `NOT NULL` and write the empty value on insert. Nullable only when "no value" is a distinct state no ordinary value expresses: timestamps not yet reached (`started_at`, `finished_at`) and open-ended bounds (`start_date`, `end_date`).

Why: otherwise `NULL` and `''` both mean "nothing" and every query needs `OR ... IS NULL`. In Rust, a `NOT NULL` column is a plain `String`/`u32`, so no call site has to invent a meaning for `None`.

## No `DEFAULT` clauses

Every insert supplies every column it owns, bound from Rust: empty values as empty values (`job_description: String::new()`; `depends_on: Vec::new()`, which `sqlx::types::Json` writes as `'[]'`). The one literal left in an `INSERT` in `src/crud/` is `task_run_attempt`'s initial `output` of `''`.

Timestamps bind `self.toolkit.get_current_ts()`, never `CURRENT_TIMESTAMP`, so `Toolkit` is the single source of "now" and every timestamp shares one format (RFC3339 with subseconds, comparable in SQL).

Why: a row's value should be readable from the insert, not from old DDL; and `DEFAULT` with `NOT NULL` makes a **forgotten column succeed quietly**, which you can't debug from the row afterwards.

## Foreign keys are enforced

sqlx turns `PRAGMA foreign_keys` on by default, so a bad reference between tables is an error.

**Nothing can reference config**: jobs and tasks are YAML in `Declared`, not tables. So `job_run.job_id` and `task_run.task_id` are unconstrained and the code checks instead. That's intended: a run carries a snapshot of what it was submitted with and must stay valid after its YAML is edited or deleted, which a foreign key would forbid.
