# Declaring a table

Four rules constrain what this repo's DDL may say. They are also why the entity structs in `src/crud/` look the way they do, and — for the middle two — why [`ALTER TABLE` gets you less far here](altering-a-table.md) than it would elsewhere.

**Which database the table is in decides most of this**, so answer that first, with the restart question from the [entities skill](../../entities/SKILL.md): *if the process restarts, should this row still exist?*

## Keys split by database

A **`mem`** table carries a caller-assigned `row_id INTEGER NOT NULL` with `UNIQUE (row_id)`, whose only job is to preserve YAML declaration order, alongside a natural primary key from the config (`job_id`, `(task_id, job_id)`, `schedule_id`) — or, for `task_dependent` and `schedule_job`, no primary key at all.

A **disk** table has `id INTEGER PRIMARY KEY AUTOINCREMENT`, returned via `last_insert_rowid()`.

The split follows from where the rows come from. Config is re-seeded from YAML on every startup, so a generated id would differ run to run and nothing could reference it — the natural key from the config is the only stable one, and `row_id` exists purely so a later `RowId` sort can replay declaration order. A disk row is created once by something that then needs to refer to it, so it wants an id the database hands back.

This is also what decides an insert's return type — `()` for a caller-assigned `row_id`, `i64` for an autoincrement id. See the [crud skill](../../crud/SKILL.md).

## `NOT NULL` whenever there is a logical null value

If a column's type has a natural empty value — `''` for text, `0` for a counter, `'[]'` for a JSON list — *that value is the null*: declare `NOT NULL` and write the empty value explicitly on insert.

A nullable column is only right when "no value" is a real, distinct state that no ordinary value can express. In this schema that means timestamps which have not happened yet (`started_at`, `finished_at`, `next_run`) and genuinely open-ended bounds (`start_date`, `end_date`).

The rule keeps two spellings of "nothing" from coexisting. Once a text column is nullable, `NULL` and `''` both mean "no output", and no query can be written without an `OR ... IS NULL`. It pays off in Rust too: a `NOT NULL` column is a plain `String`/`u32` rather than an `Option<...>`, so no call site has to invent a meaning for `None`.

This rule is the one the per-table reference files lean on constantly — "an attempt that printed nothing has empty output, not unknown output" is it talking.

## No `DEFAULT` clauses

Every insert supplies every column it owns, and every value is bound from Rust — there is currently no constant left in any `INSERT` literal in `src/crud/`. An empty value is bound as the empty value (`job_description: String::new()` for a job with no description; `depends_on: Vec::new()`, which `sqlx::types::Json` writes as `'[]'`).

Every timestamp binds `self.toolkit.get_current_ts()`, never `CURRENT_TIMESTAMP`, so `Toolkit` stays the single source of "now" and every timestamp in the schema shares one format — RFC3339 with subseconds, directly comparable in SQL.

Two reasons for the rule. The value a row gets should be readable from the insert rather than from DDL written migrations ago; and `DEFAULT` combined with `NOT NULL` makes a **forgotten column succeed quietly** instead of failing, which is the one outcome you cannot debug from the row afterwards.

## Foreign keys are enforced

sqlx turns `PRAGMA foreign_keys` on by default, so a bad reference is a real error rather than a dangling row: a bad reference in config aborts `CRUD::init`'s transaction and the process exits — see [schedule_job.md](../../entities/references/schedule_job.md).

**A disk table cannot have a foreign key to a config table.** Config lives in the attached `mem` database, and SQLite does not support foreign keys across attached databases. So `job_run.job_id` and `task_run.task_id` are unconstrained, and the code checks instead.

That is not a gap to be closed — it falls out of the same snapshot design that makes runs independent of config. A run carries its own copy of what it was submitted with, so it must stay valid after the YAML that produced it is edited or deleted, which a foreign key would forbid. Declare the key where both tables are in the same database, and check in Rust where they are not.

## Together

The middle two rules are what make `ADD COLUMN` awkward here, which [altering-a-table.md](altering-a-table.md) works through. The awkwardness is doing its job: a new column on a populated table has to force a decision about what the existing rows should say, instead of letting `DEFAULT ''` answer that question silently on your behalf.
