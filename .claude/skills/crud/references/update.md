# Update methods

Only mutable event/run entities get an `update_*` method — `job_run`, `task_run`, `task_run_attempt`, `schedule` all have one; config-seeded, insert-only entities (`job`, `task`, `task_dependent`, `schedule_job`) don't. Add one only when the entity actually changes state after creation.

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

Field order in the struct definition is `input` then `filter` (the reverse of `Select<Entity>sData`, which is `filter` then `sort`/`limit`/`offset`) — follow the existing files' order rather than alphabetizing.

### The double-`Option` pattern

For a nullable column, the input field is `Option<Option<T>>`, not `Option<T>`:

- Outer `None` → "leave this column untouched" (field omitted from the `SET` clause).
- Outer `Some(None)` → "set this column to `NULL`".
- Outer `Some(Some(v))` → "set this column to `v`".

This is how `started_at`/`finished_at`/`next_run` are modeled in `job_run`, `task_run`, `task_run_attempt`, and `schedule` — see [src/crud/job_run.rs](../../../../src/crud/job_run.rs) `UpdateJobRunsDataInput`. A non-nullable column (`status`, `stdout`, `stderr`) just uses a single `Option<T>` since there's no NULL case to distinguish.

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

Key points:

- Build the `SET` clause with `query_builder.separated(", ")`, and push each bind with `separated.push_bind_unseparated(...)` (plain `push_bind` would insert an extra `", "` before the value).
- **Guard against an empty `SET`**: after building the assignment list, check whether every `input` field was `None` and `return Ok(())` early. `UPDATE ... SET` with no assignments is invalid SQL — every existing `update_*` method has this guard, listing all its input fields in one `&&`-chained `is_none()` check. Do this check *before* appending `WHERE`.
- After the guard, `query_builder.push(" WHERE 1=1")` and append filter clauses the same way selects do — `AND col = <bind>` per `Some` filter field.
- Filters can reference columns not present in the input (e.g. `update_task_runs` filters on `status` — a value the update isn't necessarily changing) — treat the filter list independently from the input list.
- Use `.build().execute(executor)` (a plain execute), not `.build_query_as()` — updates don't return rows.
- Return type is always `anyhow::Result<()>`.

## What NOT to do

- Don't write separate single-column update methods (`set_status`, `set_finished_at`, ...) — one `update_<entity>s` method with an all-optional input covers every partial-update shape a caller needs.
- Don't skip the empty-`SET` guard "because callers will always pass at least one field" — write it anyway; it's one line and it's in every existing update method.
