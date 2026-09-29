# Update methods

Only entities that change state after creation get an `update_*`: `job_run`, `task_run`, `task_run_attempt`, `job_run_notification`. Config-seeded entities (`job`, `task`, `task_dependent`, `schedule`, `schedule_job`) don't.

## Struct shape

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct Update<Entity>sDataInput {
    pub status: Option<SomeStatusEnum>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub finished_at: Option<Option<DateTime<Utc>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Update<Entity>sDataFilter {
    pub id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Update<Entity>sData {
    pub input: Update<Entity>sDataInput,
    pub filter: Update<Entity>sDataFilter,
}
```

Declare `input` before `filter`, as the existing files do.

### Double `Option` for nullable columns

A nullable column's input field is `Option<Option<T>>`:

- `None` — leave the column untouched (not in `SET`).
- `Some(None)` — set it to `NULL`.
- `Some(Some(v))` — set it to `v`.

See `started_at`/`finished_at` in `UpdateJobRunsDataInput` ([src/crud/job_run.rs](../../../../src/crud/job_run.rs)). A `NOT NULL` column (`status`, `output`, `error`) uses a single `Option<T>`.

## Query building

```rust
pub async fn update_widgets<'e, E>(&self, executor: E, data: &UpdateWidgetsData) -> anyhow::Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("UPDATE mem.widget SET ");
    let mut separated = query_builder.separated(", ");

    if let Some(status) = &data.input.status {
        separated.push("status = ");
        separated.push_bind_unseparated(status);
    }

    if let Some(finished_at) = &data.input.finished_at {
        separated.push("finished_at = ");
        separated.push_bind_unseparated(finished_at);
    }

    if data.input.status.is_none() && data.input.finished_at.is_none() {
        return Ok(());
    }

    query_builder.push(" WHERE 1=1");

    if let Some(id) = data.filter.id {
        query_builder.push(" AND id = ");
        query_builder.push_bind(id);
    }

    query_builder.build().execute(executor).await?;

    Ok(())
}
```

- Push each bind with `separated.push_bind_unseparated(...)`; plain `push_bind` would insert an extra `", "` before the value.
- **Empty-`SET` guard:** `UPDATE ... SET` with no assignments is invalid SQL, so return `Ok(())` early when every input field is `None` — one `&&`-chained `is_none()` check over all of them, before appending `WHERE`. Write it even if callers "always" pass a field; every existing `update_*` has it.
- Then `WHERE 1=1` plus `AND col = <bind>` per `Some` filter field, as in selects. Filters are independent of inputs (`update_task_runs` filters on `status`).
- `.build().execute(executor)`, not `build_query_as()`; return `anyhow::Result<()>`.
- One `update_<entity>s` with an all-optional input — no per-column `set_status`/`set_finished_at` methods.
