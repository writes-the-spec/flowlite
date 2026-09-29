# Select methods

## Struct shape

```rust
#[derive(Debug, Serialize, Deserialize)]
pub enum Select<Entity>sDataSort { Alphabetical, RowId }

#[derive(Debug, Serialize, Deserialize)]
pub struct Select<Entity>sDataFilter {
    pub <entity>_id: Option<String>, // every field optional
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Select<Entity>sData {
    pub filter: Select<Entity>sDataFilter,
    pub sort: Option<Select<Entity>sDataSort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}
```

- Filter fields are additive; an empty filter returns everything.
- `limit`/`offset` are `Option<u32>` on config tables, `Option<i64>` on disk ones (`job_run`, `job_run_stop`, `job_run_notification`) — match the neighbours; the bind casts to `i64` either way. Omit them for tables always read in full for a key (`task_run`, `task_run_attempt`, `task_dependent`) until a caller paginates.

## Query building

Always `sqlx::QueryBuilder` from `WHERE 1=1`, so every clause can `AND`:

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
    // OFFSET: same shape.

    Ok(query_builder.build_query_as::<Widget>().fetch_all(executor).await?)
}
```

- Explicit column list, never `SELECT *`: it must match the row struct's `FromRow` field names and order.
- Sort: support at least creation order — `RowId` for config tables, `Id`/`IdDesc` for autoincrement run tables.

### Filter kinds

| Kind | Shape |
|---|---|
| Equality | `AND col = <bind>` when `Some` |
| Substring | `name_like` binds `format!("%{}%", name)` to `LIKE` (`SelectJobsDataFilter` in [src/crud/job.rs](../../../../src/crud/job.rs), `SelectSchedulesDataFilter`) |
| Comparison | name the operator: `scheduled_at_gt` → `AND scheduled_at > <bind>` (`DeleteJobRunsDataFilter` in [src/crud/job_run.rs](../../../../src/crud/job_run.rs)); an unsuffixed name is equality |
| Bool | bind `if v { 1 } else { 0 }` |
| Enum | bind directly |
| Any of several | `statuses: Option<Vec<JobRunStatus>>` → `AND status IN (?, ...)` via `separated(", ")`, beside the singular `status` (a different question). Pass a non-empty list: `IN ()` is not valid SQLite. |

**Equality on a `DATETIME`** matches the stored value exactly, so it only serves a caller passing back the instant it inserted: `SelectJobRunsDataFilter::scheduled_at` is the Scheduler's (one cron occurrence, as `CRUD::submit_job` wrote it), and its doc comment says so. Any other caller wants a `_gt`/`_lt` window.

### Adding a field to an existing filter

`SelectJobRunsDataFilter` doesn't derive `Default`, so a new field breaks every literal building it (about 50). Intended: each site says `None` on purpose — let `cargo build` list them. (`SelectSchedulesDataFilter` and `SelectScheduleJobsDataFilter` do derive `Default`, and the router's schedule routes use `..Default::default()`.)

With a shared clause-pusher, add the clause there only. `push_job_run_filter` in [src/crud/job_run.rs](../../../../src/crud/job_run.rs) serves `select_job_runs`, `count_job_runs` and `select_job_run_job_ids` — the one place redundancy is overruled, since a clause missing from the count widens retention's deletion window. `delete_job_runs` has its own filter type; a select-side field doesn't reach it.

### Counts and projections

A count (`count_job_runs`) or distinct projection (`select_job_run_job_ids`) is entity work beside `select_*` — not a `QueryBuilder` in a multistatement, nor a `select_*` counted or deduped in Rust. `count_running_attempts` in [limits.rs](../../../../src/crud/multistatements/limits.rs) stays as is: its set is small and bounded. Both take the select's own filter so count and select can't drift:

```rust
pub struct CountJobRunsData {
    pub filter: SelectJobRunsDataFilter,
}
```

### Joins

**No `select_*` joins today.** First ask whether the column belongs on the row: joining disk to `mem` ties durable rows to config re-seeded every start (why `job_run` carries `job_name`, written by `CRUD::submit_job`, instead of joining `mem.job`). A justified join goes in the base query, with every table aliased and every column prefixed (`FROM job_run jr JOIN mem.job j ON j.job_id = jr.job_id`), not a second round-trip.

## Singular helper

For one row, add `select_<entity>` taking the plural's first result — never a second query:

```rust
pub async fn select_widget<'e, E>(&self, executor: E, data: &SelectWidgetsData) -> anyhow::Result<Option<Widget>>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    Ok(self.select_widgets(executor, data).await?.into_iter().next())
}
```

It doesn't add `LIMIT 1`; callers pass `limit: Some(1)` if they want. `task_dependent` and `task_run_attempt` (read in full) have none. `schedule_job`'s is misnamed `select_schedule_job_internal` ([src/crud/schedule_job.rs](../../../../src/crud/schedule_job.rs)) — copy the shape, not the name.

## Row struct

Derives `Debug, Serialize, Deserialize, sqlx::FromRow, Clone`; fields match the `SELECT` list exactly. JSON columns read as `sqlx::types::Json<Vec<String>>` (`Task::depends_on`); `bool` columns read as `bool` (stored `0`/`1`).
