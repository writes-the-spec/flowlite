# Insert methods

## Struct shape

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct Insert<Entity>DataInput {
    pub row_id: u64,       // only if the table is seeded/ordered via CRUD::init, see below
    pub <entity>_id: String,
    // ...other columns, same order as the CREATE TABLE
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Insert<Entity>Data {
    pub input: Insert<Entity>DataInput,
}
```

The wrapper (`Insert<Entity>Data { input }`) is boilerplate — always add it, even though it only ever holds `input`. It keeps insert/select/update call sites symmetric (`&SelectXData { filter, sort, .. }`, `&UpdateXData { input, filter }`).

## Return type: `()` vs `i64`

Two id strategies exist in this codebase, and the insert's return type follows which one the table uses:

- **Caller-assigned `row_id`** (config-seeded tables like `job`, `task`, `schedule`, `schedule_job`, `task_dependent`): the input carries `row_id: u64` bound positionally, and the method returns `anyhow::Result<()>`. `row_id` is a monotonic counter incremented by the caller across every insert in `CRUD::init` (see [src/crud/crud.rs](../../../../src/crud/crud.rs) `init`) — it exists purely to preserve YAML declaration order for later `RowId` sorts, not as a primary key.
- **DB-assigned autoincrement id** (event/run tables like `job_run`, `task_run`, `task_run_attempt`, `job_run_stop`): no `row_id` in the input; the method returns `anyhow::Result<i64>` via `res.last_insert_rowid()`:

```rust
pub async fn insert_job_run<'e, E>(&self, executor: E, data: &InsertJobRunData) -> anyhow::Result<i64>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let res = sqlx::query("INSERT INTO job_run (job_id, status) VALUES (?, ?)")
        .bind(&data.input.job_id)
        .bind(&data.input.status)
        .execute(executor)
        .await?;

    Ok(res.last_insert_rowid())
}
```

Pick whichever matches the table's primary key column in its migration — don't invent a third convention. Which one a table gets is not a choice made here: it follows from which database the table is in, and the [db-schema skill](../../db-schema/SKILL.md) has the rule and the reason.

## Binding values

- Plain columns: `.bind(&data.input.field)` (or by value for `Copy` types like `i64`/`u32`/enums deriving `sqlx::Type`).
- `row_id: u64` binds as `data.input.row_id as i64` (SQLite has no unsigned type).
- `Vec<String>`-like columns: wrap in `sqlx::types::Json` — `.bind(sqlx::types::Json(&data.input.depends_on))` (see [src/crud/task.rs](../../../../src/crud/task.rs) `depends_on`).
- `bool` columns: SQLite has no bool type either — bind `if data.input.disabled { 1 } else { 0 }` (see [src/crud/schedule.rs](../../../../src/crud/schedule.rs) `disabled`).
- Status/kind columns: model as a Rust enum deriving `sqlx::Type` with `#[sqlx(rename_all = "lowercase")]`, bound directly (`.bind(&data.input.status)`). Pair it with a `Display` impl writing the same lowercase strings if the value needs to appear in log/error messages — see `JobRunStatus` and `TaskRunStatus`.
- Types with a custom domain representation (`cron::Schedule`, `chrono_tz::Tz`) are stored as their `.to_string()` — the column is `TEXT` — while the struct field keeps the rich type for callers.

## Multi-row inserts and transactions

When one YAML/API input fans out into several related inserts (e.g. a job's tasks, a task's `depends_on` edges, a schedule's jobs), don't add a single "insert everything" method to the child entity's file. Instead, do the fan-out at the call site inside an existing transaction, incrementing the shared `row_id` counter as you go — see `CRUD::init` in [src/crud/crud.rs](../../../../src/crud/crud.rs) and `CRUD::submit_job` in [src/crud/multistatements/submit_job.rs](../../../../src/crud/multistatements/submit_job.rs) for the pattern. Each entity's `insert_*` stays a single-row insert; composition happens one layer up.

## Immutable vs mutable entities

Not every entity needs update support. `job`, `task`, `task_dependent`, and `schedule_job` are insert-only (config-seeded, never mutated after `CRUD::init`) — they have no `update_*` method and no `Update*Data` structs. Only add those if the entity is genuinely a mutable event/run record (see [update.md](update.md)).
