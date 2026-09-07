# Select methods

## Struct shape

```rust
#[derive(Debug, Serialize, Deserialize)]
pub enum Select<Entity>sDataSort {
    Alphabetical,
    RowId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Select<Entity>sDataFilter {
    pub <entity>_id: Option<String>,
    // ...other optional filters
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Select<Entity>sData {
    pub filter: Select<Entity>sDataFilter,
    pub sort: Option<Select<Entity>sDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}
```

Every field in `Select<Entity>sDataFilter` is optional and additive — an empty filter returns everything. `sort`, `limit`, and `offset` are top-level optionals on `Select<Entity>sData`, not inside the filter.

`limit`/`offset` are `Option<u32>` on the config tables and `Option<i64>` on the disk ones (`job_run`, `job_run_stop`) — both are live, so match the neighbours of whichever table you are adding rather than picking one. Either way the bind casts to `i64`.

They are omitted entirely from the struct for tables that are always fetched in full for a given key (e.g. `task_run`, `task_run_attempt`, `task_dependent` — always scoped to one `job_run_id`/`task_run_id`). Only add them when callers actually need pagination.

## Query building

Always build with `sqlx::QueryBuilder`, always start from `WHERE 1=1` so every filter clause can unconditionally `AND`:

```rust
pub async fn select_widgets<'e, E>(&self, executor: E, data: &SelectWidgetsData) -> anyhow::Result<Vec<Widget>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> =
        sqlx::QueryBuilder::new("SELECT widget_id, ... FROM mem.widget WHERE 1=1");

    if let Some(widget_id) = &data.filter.widget_id {
        query_builder.push(" AND widget_id = ");
        query_builder.push_bind(widget_id);
    }

    if let Some(sort) = &data.sort {
        match sort {
            SelectWidgetsDataSort::Alphabetical => query_builder.push(" ORDER BY widget_id ASC"),
            SelectWidgetsDataSort::RowId => query_builder.push(" ORDER BY row_id ASC"),
        };
    }

    if let Some(limit) = data.limit {
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit as i64);
    }

    if let Some(offset) = data.offset {
        query_builder.push(" OFFSET ");
        query_builder.push_bind(offset as i64);
    }

    Ok(query_builder.build_query_as::<Widget>().fetch_all(executor).await?)
}
```

Column list is explicit (`SELECT a, b, c FROM ...`), never `SELECT *` — the column order must line up with the `Widget` struct's `sqlx::FromRow` field order/names.

### Filter kinds seen in this codebase

- **Equality**: `AND col = <bind>`, guarded by `if let Some(v) = &data.filter.col`.
- **Substring match**: `name_like: Option<String>` binds `format!("%{}%", name)` against `LIKE` — see `SelectJobsDataFilter::name_like` in [src/crud/job.rs](../../../../src/crud/job.rs) and `SelectSchedulesDataFilter::name_like`.
- **Comparison**: name the field after the operator, e.g. `next_run_lt: Option<DateTime<Utc>>` → `AND next_run < <bind>` (see [src/crud/schedule.rs](../../../../src/crud/schedule.rs)).
- **Bool**: bind `if v { 1 } else { 0 }`, same as insert.
- **Enum**: bind directly, same as insert (`status: Option<JobRunStatus>`).

### Joins

**No `select_*` in this codebase joins today.** `select_job_runs` used to join `mem.job` for the job's name, and that name is now written onto the `job_run` row at submit time instead, by `CRUD::submit_job`. Before adding a join, ask whether the column belongs on the row: joining a disk table to a `mem` one ties durable rows to config that is re-seeded from the YAML on every start, which is exactly why that one was removed.

If a select does need a column from another table, join it directly in the base query string rather than doing a second round-trip. The query that was there is still the shape to copy:

```rust
// Illustrative — this query no longer exists.
sqlx::QueryBuilder::new(
    "SELECT jr.id, jr.job_id, j.name AS job_name, jr.created_at, jr.started_at, jr.finished_at, jr.status \
     FROM job_run jr JOIN mem.job j ON j.job_id = jr.job_id WHERE 1=1"
)
```

Alias every table when a join is involved, and prefix every column in the `SELECT` list with its table alias to avoid ambiguity.

### Sort

`Select<Entity>sDataSort` is a plain enum matched with `match sort { ... => query_builder.push(" ORDER BY ...") }`. Every entity should support at least sorting by its natural/creation order (`RowId` for config-seeded tables, `Id`/`IdDesc` for autoincrement run tables).

## Singular helper

Where a caller needs one row, add a `select_<entity>` (singular) beside the plural that just takes the first result — never hand-write a second query for the "get one" case:

```rust
pub async fn select_widget<'e, E>(&self, executor: E, data: &SelectWidgetsData) -> anyhow::Result<Option<Widget>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let widgets = self.select_widgets(executor, data).await?;
    Ok(widgets.into_iter().next())
}
```

Callers pass `limit: Some(1)` if they want the database to stop early, but that's optional — the helper doesn't add it implicitly.

Not every entity has one, and that is fine: `task_dependent` and `task_run_attempt` are only ever read in full for a key, so neither has a singular. One entity is simply misnamed — `schedule_job`'s is `select_schedule_job_internal` ([src/crud/schedule_job.rs](../../../../src/crud/schedule_job.rs)), whose body is exactly the two lines above. Copy the shape, not that name.

## Row struct

```rust
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Widget {
    pub widget_id: String,
    // ...other columns
}
```

- Always derives `sqlx::FromRow, Clone` (plus `Debug, Serialize, Deserialize`).
- JSON columns come back as `sqlx::types::Json<Vec<String>>` (see `Task::depends_on`), not `Vec<String>` directly.
- `bool` columns come back as `bool` even though they're stored as `0`/`1` — `sqlx::FromRow` handles the coercion for SQLite.
- Field names and order must match the `SELECT` list exactly.
