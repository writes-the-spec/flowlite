# Insert methods

## Struct shape

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct Insert<Entity>DataInput {
    pub row_id: u64,       // only for tables seeded via CRUD::init, see below
    pub <entity>_id: String,
    // ...other columns, same order as the CREATE TABLE
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Insert<Entity>Data {
    pub input: Insert<Entity>DataInput,
}
```

Always add the `Insert<Entity>Data { input }` wrapper, though it only holds `input` — it keeps call sites symmetric with `SelectXData { filter, .. }` and `UpdateXData { input, filter }`.

## Return type: `()` vs `i64`

Follows the table's primary key; the [db-schema skill](../../db-schema/SKILL.md) says which a table gets and why. Don't invent a third.

- **Caller-assigned `row_id`** (config tables: `job`, `task`, `schedule`, `schedule_job`, `task_dependent`): input carries `row_id: u64`; returns `anyhow::Result<()>`. `row_id` is a counter the caller increments across every insert in `CRUD::init` ([src/crud/crud.rs](../../../../src/crud/crud.rs)), existing only to preserve YAML order for `RowId` sorts — not a primary key.
- **Autoincrement id** (run tables: `job_run`, `task_run`, `task_run_attempt`, `job_run_stop`): no `row_id`; returns `anyhow::Result<i64>` via `last_insert_rowid()`:

```rust
// Simplified from insert_job_run in src/crud/job_run.rs.
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

## Binding values

| Column | Bind |
|---|---|
| Plain | `.bind(&data.input.field)`, or by value for `Copy` types |
| `row_id: u64` | `data.input.row_id as i64` (SQLite has no unsigned type) |
| `Vec<String>`-like | `.bind(sqlx::types::Json(&data.input.depends_on))` — [src/crud/task.rs](../../../../src/crud/task.rs) |
| `bool` | `if data.input.disabled { 1 } else { 0 }` (no bool type) — [src/crud/schedule.rs](../../../../src/crud/schedule.rs) |
| Status/kind | Rust enum deriving `sqlx::Type` with `#[sqlx(rename_all = "lowercase")]`, bound directly. Add a `Display` writing the same lowercase strings if it appears in logs/errors (`JobRunStatus`, `TaskRunStatus`). |
| Rich domain types (`cron::Schedule`, `chrono_tz::Tz`) | stored as `.to_string()` in a `TEXT` column; the struct keeps the rich type |

## Multi-row inserts

When one input fans out into related rows (a job's tasks, a task's `depends_on` edges, a schedule's jobs), keep each `insert_*` single-row and do the fan-out at the call site inside the existing transaction, incrementing the shared `row_id` — see `CRUD::init` in [src/crud/crud.rs](../../../../src/crud/crud.rs) and `CRUD::submit_job` in [src/crud/multistatements/submit_job.rs](../../../../src/crud/multistatements/submit_job.rs). No "insert everything" method on the child entity.

## Immutable entities

`job`, `task`, `task_dependent` and `schedule_job` are insert-only (never mutated after `CRUD::init`): no `update_*`, no `Update*Data`. Add those only for a genuinely mutable run record — see [update.md](update.md).
