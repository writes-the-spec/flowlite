# Insert methods

## Struct shape

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct Insert<Entity>DataInput {
    pub <entity>_id: String,
    // ...other columns, same order as the CREATE TABLE
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Insert<Entity>Data {
    pub input: Insert<Entity>DataInput,
}
```

Always add the `Insert<Entity>Data { input }` wrapper, though it only holds `input` — it keeps call sites symmetric with `SelectXData { filter, .. }` and `UpdateXData { input, filter }`.

## Return type: `i64`

Every table has an autoincrement `id` ([db-schema skill](../../db-schema/SKILL.md)), so an insert returns `anyhow::Result<i64>` via `last_insert_rowid()`:

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
| `Vec<String>`-like | `.bind(sqlx::types::Json(&data.input.depends_on))` — [src/crud/task_run.rs](../../../../src/crud/task_run.rs) |
| `bool` | `if value { 1 } else { 0 }` (no bool type) |
| Status/kind | Rust enum deriving `sqlx::Type` with `#[sqlx(rename_all = "lowercase")]`, bound directly. Add a `Display` writing the same lowercase strings if it appears in logs/errors (`JobRunStatus`, `TaskRunStatus`). |
| Rich domain types (`cron::Schedule`, `chrono_tz::Tz`) | stored as `.to_string()` in a `TEXT` column; the struct keeps the rich type |

## Multi-row inserts

When one input fans out into related rows (a run's task runs and notifications), keep each `insert_*` single-row and do the fan-out at the call site inside one transaction — see `insert_job_run_definition` in [src/crud/multistatements/job_run_definition.rs](../../../../src/crud/multistatements/job_run_definition.rs). No "insert everything" method on the child entity.
