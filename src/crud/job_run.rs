use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Aborted,
    TimedOut,
    Invalid,
}

impl JobRunStatus {

    /// Every status a run can hold, in the order the dashboard offers them as filters.
    ///
    /// It lives beside the enum so the surfaces that need to enumerate statuses - the
    /// filter chips and the CLI's `--status` parser - read one list rather than each
    /// keeping its own copy to forget to update.
    pub const ALL: [JobRunStatus; 8] = [
        JobRunStatus::Pending,
        JobRunStatus::Running,
        JobRunStatus::Succeeded,
        JobRunStatus::Failed,
        JobRunStatus::Skipped,
        JobRunStatus::Aborted,
        JobRunStatus::TimedOut,
        JobRunStatus::Invalid,
    ];

    /// Whether the run has settled and will not change again. Matched exhaustively on
    /// purpose: a new status has to say which side of this line it falls on, or it stops
    /// compiling.
    pub fn is_finished(&self) -> bool {
        match self {
            JobRunStatus::Pending
            | JobRunStatus::Running => false,
            JobRunStatus::Succeeded
            | JobRunStatus::Failed
            | JobRunStatus::Skipped
            | JobRunStatus::Aborted
            | JobRunStatus::TimedOut
            | JobRunStatus::Invalid => true,
        }
    }

}

impl std::fmt::Display for JobRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobRunStatus::Pending => write!(f, "pending"),
            JobRunStatus::Running => write!(f, "running"),
            JobRunStatus::Succeeded => write!(f, "succeeded"),
            JobRunStatus::Failed => write!(f, "failed"),
            JobRunStatus::Skipped => write!(f, "skipped"),
            JobRunStatus::Aborted => write!(f, "aborted"),
            JobRunStatus::TimedOut => write!(f, "timedout"),
            JobRunStatus::Invalid => write!(f, "invalid"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunDataInput {
    pub job_id: String,
    pub job_name: String,
    pub job_description: String,
    pub parameters: BTreeMap<String, String>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub status: JobRunStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunData {
    pub input: InsertJobRunDataInput,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum SelectJobRunsDataSort {
    Id,
    IdDesc,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunsDataFilter {
    pub id: Option<i64>,
    pub job_id: Option<String>,
    /// Exactly this one status.
    pub status: Option<JobRunStatus>,
    /// Any of these statuses. It sits beside `status` rather than replacing it — the two
    /// ask different questions, and both apply when both are set. Pass a non-empty list:
    /// `IN ()` is not valid SQLite.
    pub statuses: Option<Vec<JobRunStatus>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunsData {
    pub filter: SelectJobRunsDataFilter,
    pub sort: Option<SelectJobRunsDataSort>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Counting is asking how many rows a select would return, so it takes the select's own
/// filter rather than a copy of it: a caller that counts and then selects over "the same
/// rows" builds one filter and passes it to both, and the two cannot drift.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CountJobRunsData {
    pub filter: SelectJobRunsDataFilter,
}

/// Same reasoning as [`CountJobRunsData`]: a projection asks the select's question and
/// answers it with one column, so it takes the select's filter.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunJobIdsData {
    pub filter: SelectJobRunsDataFilter,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteJobRunsDataFilter {
    pub id: Option<i64>,
    pub job_id: Option<String>,
    pub status: Option<JobRunStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteJobRunsData {
    pub filter: DeleteJobRunsDataFilter,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRunsDataInput {
    pub status: Option<JobRunStatus>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub finished_at: Option<Option<DateTime<Utc>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRunsDataFilter {
    pub id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRunsData {
    pub input: UpdateJobRunsDataInput,
    pub filter: UpdateJobRunsDataFilter,
}


#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct JobRun {
    pub id: i64,
    pub job_id: String,
    pub job_name: String,
    pub job_description: String,
    pub parameters: sqlx::types::Json<BTreeMap<String, String>>,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: JobRunStatus,
}

impl CRUD {
    pub async fn insert_job_run<'e, E>(&self, executor: E, data: &InsertJobRunData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO job_run (job_id, job_name, job_description, parameters, created_at, scheduled_at, status) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
            .bind(&data.input.job_id)
            .bind(&data.input.job_name)
            .bind(&data.input.job_description)
            .bind(sqlx::types::Json(&data.input.parameters))
            .bind(self.toolkit.get_current_ts())
            .bind(&data.input.scheduled_at)
            .bind(&data.input.status)
            .execute(executor)
            .await?;

        Ok(res.last_insert_rowid())
    }

    pub async fn select_job_run<'e, E>(&self, executor: E, data: &SelectJobRunsData) -> anyhow::Result<Option<JobRun>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let runs = self.select_job_runs(executor, data).await?;
        Ok(runs.into_iter().next())
    }

    pub async fn select_job_runs<'e, E>(&self, executor: E, data: &SelectJobRunsData) -> anyhow::Result<Vec<JobRun>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, job_id, job_name, job_description, parameters, created_at, scheduled_at, started_at, finished_at, status FROM job_run WHERE 1=1"
        );

        push_job_run_filter(&mut query_builder, &data.filter);

        if let Some(sort) = &data.sort {
            match sort {
                SelectJobRunsDataSort::Id => {
                    query_builder.push(" ORDER BY id ASC");
                }
                SelectJobRunsDataSort::IdDesc => {
                    query_builder.push(" ORDER BY id DESC");
                }
            }
        }

        if let Some(limit) = data.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit);
        } else if data.offset.is_some() {
            // SQLite has no `OFFSET` without a `LIMIT` before it, and `-1` is how it spells
            // "no limit" — so an offset on its own still means "every row past the first n".
            query_builder.push(" LIMIT -1");
        }

        if let Some(offset) = data.offset {
            query_builder.push(" OFFSET ");
            query_builder.push_bind(offset);
        }

        let runs = query_builder
            .build_query_as::<JobRun>()
            .fetch_all(executor)
            .await?;

        Ok(runs)
    }

    /// How many rows `data.filter` matches — counted by the database rather than by
    /// selecting the rows and taking their length, because the sets this is asked about are
    /// unbounded: "every finished run in the database" is 50,000 rows on a long-lived data
    /// directory, and the answer is one integer.
    pub async fn count_job_runs<'e, E>(&self, executor: E, data: &CountJobRunsData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT COUNT(*) FROM job_run WHERE 1=1"
        );

        push_job_run_filter(&mut query_builder, &data.filter);

        let count = query_builder
            .build_query_scalar::<i64>()
            .fetch_one(executor)
            .await?;

        Ok(count)
    }

    /// The distinct `job_id`s among the rows `data.filter` matches — deduplicated by the
    /// database for the same reason `count_job_runs` counts there: the rows behind a
    /// handful of job ids can be the whole table.
    ///
    /// Nothing is joined against `mem.job`, so a job id whose YAML has since been deleted,
    /// or which never had a row at all, still comes back.
    pub async fn select_job_run_job_ids<'e, E>(&self, executor: E, data: &SelectJobRunJobIdsData) -> anyhow::Result<Vec<String>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT DISTINCT job_id FROM job_run WHERE 1=1"
        );

        push_job_run_filter(&mut query_builder, &data.filter);

        let job_ids = query_builder
            .build_query_scalar::<String>()
            .fetch_all(executor)
            .await?;

        Ok(job_ids)
    }

    /// Deletes every row in `job_run` matching `data.filter`. An entirely empty filter
    /// matches every row and so deletes the whole table — exact parity with an empty
    /// select filter, and the caller's business, not this method's.
    pub async fn delete_job_runs<'e, E>(&self, executor: E, data: &DeleteJobRunsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM job_run WHERE 1=1"
        );

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(status) = &data.filter.status {
            query_builder.push(" AND status = ");
            query_builder.push_bind(status);
        }

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        query_builder.build().execute(executor).await?;

        Ok(())
    }

    pub async fn update_job_runs<'e, E>(&self, executor: E, data: &UpdateJobRunsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("UPDATE job_run SET ");

        let mut separated = query_builder.separated(", ");

        if let Some(status) = &data.input.status {
            separated.push("status = ");
            separated.push_bind_unseparated(status);
        }

        if let Some(started_at) = &data.input.started_at {
            separated.push("started_at = ");
            separated.push_bind_unseparated(started_at);
        }

        if let Some(finished_at) = &data.input.finished_at {
            separated.push("finished_at = ");
            separated.push_bind_unseparated(finished_at);
        }

        if data.input.status.is_none()
            && data.input.started_at.is_none()
            && data.input.finished_at.is_none()
        {
            return Ok(());
        }

        query_builder.push(" WHERE 1=1");

        if let Some(id) = data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        let query = query_builder.build();

        query.execute(executor).await?;

        Ok(())
    }

}

/// Pushes every clause of a [`SelectJobRunsDataFilter`] onto a query already ending in
/// `WHERE 1=1`. Shared by `select_job_runs`, `count_job_runs` and
/// `select_job_run_job_ids` — the one place in this codebase where the usual preference
/// for redundancy over abstraction is overruled, because here the three renderings
/// drifting apart is not a cosmetic inconsistency but an over-delete.
///
/// `select_deletable_job_runs` derives its deletion window from `count_job_runs` minus
/// the job's `keep_runs`, over the filter `select_job_runs` then reads with. A clause
/// added to the select and forgotten in the count makes the count too large, which makes
/// the window too wide, which deletes runs the job asked to keep. Sharing the filter
/// *type* stops the fields drifting; only sharing this function stops the clauses.
fn push_job_run_filter(query_builder: &mut sqlx::QueryBuilder<sqlx::Sqlite>, filter: &SelectJobRunsDataFilter) {

    if let Some(job_id) = &filter.job_id {
        query_builder.push(" AND job_id = ");
        query_builder.push_bind(job_id);
    }

    if let Some(status) = &filter.status {
        query_builder.push(" AND status = ");
        query_builder.push_bind(status);
    }

    if let Some(statuses) = &filter.statuses {
        push_statuses_in(query_builder, statuses);
    }

    if let Some(id) = &filter.id {
        query_builder.push(" AND id = ");
        query_builder.push_bind(id);
    }
}

/// Pushes `AND status IN (?, ?, ...)`, one bind per status. It stays its own function
/// beside [`push_job_run_filter`] because it is the one clause whose bind count varies
/// with the filter's contents rather than being a single `push_bind`.
fn push_statuses_in(query_builder: &mut sqlx::QueryBuilder<sqlx::Sqlite>, statuses: &[JobRunStatus]) {

    query_builder.push(" AND status IN (");

    let mut separated = query_builder.separated(", ");
    for status in statuses {
        separated.push_bind(*status);
    }

    query_builder.push(")");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing will ever wait on it again, so it must count as settled - a run left
    /// unfinished is the exact condition Invalid exists to end.
    #[test]
    fn an_invalid_run_is_finished() {
        assert!(JobRunStatus::Invalid.is_finished());
    }

    async fn select(db: &crate::test_support::TestDb, filter: SelectJobRunsDataFilter) -> Vec<JobRun> {
        db.crud.select_job_runs(&*db.conn_pool, &SelectJobRunsData {
            filter,
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap()
    }

    fn empty_filter() -> SelectJobRunsDataFilter {
        SelectJobRunsDataFilter { id: None, job_id: None, status: None, statuses: None }
    }

    /// `status` really filters: deleting by status only removes the matching run, and its
    /// neighbour of a different status survives.
    #[tokio::test]
    async fn delete_job_runs_filters_by_status() {

        let db = crate::test_support::TestDb::new().await;

        let failed = db.insert_job_run(JobRunStatus::Failed).await;
        let succeeded = db.insert_job_run(JobRunStatus::Succeeded).await;

        db.crud.delete_job_runs(&*db.conn_pool, &DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter { id: None, job_id: None, status: Some(JobRunStatus::Failed) },
        }).await.unwrap();

        assert!(select(&db, SelectJobRunsDataFilter { id: Some(failed.id), ..empty_filter() }).await.is_empty());
        assert!(!select(&db, SelectJobRunsDataFilter { id: Some(succeeded.id), ..empty_filter() }).await.is_empty());
    }

    /// The decided behaviour, pinned so a future guard cannot be added silently: an
    /// entirely empty filter matches every row and so deletes the whole table.
    #[tokio::test]
    async fn an_empty_filter_deletes_every_job_run() {

        let db = crate::test_support::TestDb::new().await;

        db.insert_job_run(JobRunStatus::Failed).await;
        db.insert_job_run(JobRunStatus::Succeeded).await;

        db.crud.delete_job_runs(&*db.conn_pool, &DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter { id: None, job_id: None, status: None },
        }).await.unwrap();

        assert!(select(&db, empty_filter()).await.is_empty());
    }
}
