use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum TaskRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Aborted,
    TimedOut,
}

impl std::fmt::Display for TaskRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskRunStatus::Pending => write!(f, "pending"),
            TaskRunStatus::Running => write!(f, "running"),
            TaskRunStatus::Succeeded => write!(f, "succeeded"),
            TaskRunStatus::Failed => write!(f, "failed"),
            TaskRunStatus::Skipped => write!(f, "skipped"),
            TaskRunStatus::Aborted => write!(f, "aborted"),
            TaskRunStatus::TimedOut => write!(f, "timedout"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunDataInput {
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub status: TaskRunStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunData {
    pub input: InsertTaskRunDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskRunsDataFilter {
    pub id: Option<i64>,
    pub job_run_id: Option<i64>,
    pub job_id: Option<String>,
    pub task_id: Option<String>,
    pub status: Option<TaskRunStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectTaskRunsDataSort {
    Id,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskRunsData {
    pub filter: SelectTaskRunsDataFilter,
    pub sort: Option<SelectTaskRunsDataSort>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunsDataInput {
    pub status: Option<TaskRunStatus>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub finished_at: Option<Option<DateTime<Utc>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunsDataFilter {
    pub id: Option<i64>,
    pub job_run_id: Option<i64>,
    pub status: Option<TaskRunStatus>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunsData {
    pub input: UpdateTaskRunsDataInput,
    pub filter: UpdateTaskRunsDataFilter,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TaskRun {
    pub id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: TaskRunStatus,
}

impl CRUD {
    pub async fn insert_task_run<'e, E>(&self, executor: E, data: &InsertTaskRunData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO task_run (job_run_id, job_id, task_id, created_at, status) VALUES (?, ?, ?, ?, ?)"
        )
            .bind(data.input.job_run_id)
            .bind(&data.input.job_id)
            .bind(&data.input.task_id)
            .bind(self.toolkit.get_current_ts())
            .bind(&data.input.status)
            .execute(executor)
            .await?;

        Ok(res.last_insert_rowid())
    }

    pub async fn select_task_run<'e, E>(&self, executor: E, data: &SelectTaskRunsData) -> anyhow::Result<Option<TaskRun>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let runs = self.select_task_runs(executor, data).await?;
        Ok(runs.into_iter().next())
    }

    pub async fn select_task_runs<'e, E>(&self, executor: E, data: &SelectTaskRunsData) -> anyhow::Result<Vec<TaskRun>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, job_run_id, job_id, task_id, created_at, started_at, finished_at, status FROM task_run WHERE 1=1"
        );

        if let Some(id) = data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(job_run_id) = data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
        }

        if let Some(job_id) = &data.filter.job_id {
            query_builder.push(" AND job_id = ");
            query_builder.push_bind(job_id);
        }

        if let Some(task_id) = &data.filter.task_id {
            query_builder.push(" AND task_id = ");
            query_builder.push_bind(task_id);
        }

        if let Some(status) = &data.filter.status {
            query_builder.push(" AND status = ");
            query_builder.push_bind(status);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectTaskRunsDataSort::Id => {
                    query_builder.push(" ORDER BY id ASC");
                }
            }
        }

        let runs = query_builder
            .build_query_as::<TaskRun>()
            .fetch_all(executor)
            .await?;

        Ok(runs)
    }

    pub async fn update_task_runs<'e, E>(&self, executor: E, data: &UpdateTaskRunsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("UPDATE task_run SET ");
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

        if data.input.status.is_none() && data.input.started_at.is_none() && data.input.finished_at.is_none() {
            return Ok(());
        }

        query_builder.push(" WHERE 1=1");

        if let Some(id) = data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(job_run_id) = data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
        }

        if let Some(status) = &data.filter.status {
            query_builder.push(" AND status = ");
            query_builder.push_bind(status);
        }

        let query = query_builder.build();
        query.execute(executor).await?;

        Ok(())
    }
}
