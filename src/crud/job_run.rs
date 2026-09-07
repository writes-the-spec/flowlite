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
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunDataInput {
    pub job_id: String,
    pub job_name: String,
    pub job_description: String,
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
    pub status: Option<JobRunStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunsData {
    pub filter: SelectJobRunsDataFilter,
    pub sort: Option<SelectJobRunsDataSort>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
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
    pub created_at: DateTime<Utc>,
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
            "INSERT INTO job_run (job_id, job_name, job_description, created_at, status) VALUES (?, ?, ?, ?, ?)"
        )
            .bind(&data.input.job_id)
            .bind(&data.input.job_name)
            .bind(&data.input.job_description)
            .bind(self.toolkit.get_current_ts())
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
            "SELECT id, job_id, job_name, job_description, created_at, started_at, finished_at, status FROM job_run WHERE 1=1"
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
