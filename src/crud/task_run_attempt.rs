use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum TaskRunAttemptStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Aborted,
    TimedOut,
}

impl std::fmt::Display for TaskRunAttemptStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskRunAttemptStatus::Pending => write!(f, "pending"),
            TaskRunAttemptStatus::Running => write!(f, "running"),
            TaskRunAttemptStatus::Succeeded => write!(f, "succeeded"),
            TaskRunAttemptStatus::Failed => write!(f, "failed"),
            TaskRunAttemptStatus::Skipped => write!(f, "skipped"),
            TaskRunAttemptStatus::Aborted => write!(f, "aborted"),
            TaskRunAttemptStatus::TimedOut => write!(f, "timedout"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunAttemptDataInput {
    pub task_run_id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub attempt: u32,
    pub status: TaskRunAttemptStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunAttemptData {
    pub input: InsertTaskRunAttemptDataInput,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskRunAttemptsDataFilter {
    pub task_run_id: Option<i64>,
    pub job_run_id: Option<i64>,
    pub task_id: Option<String>,
    pub status: Option<TaskRunAttemptStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SelectTaskRunAttemptsDataSort {
    Id,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskRunAttemptsData {
    pub filter: SelectTaskRunAttemptsDataFilter,
    pub sort: Option<SelectTaskRunAttemptsDataSort>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunAttemptsDataInput {
    pub status: Option<TaskRunAttemptStatus>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub finished_at: Option<Option<DateTime<Utc>>>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunAttemptsDataFilter {
    pub id: Option<i64>,
    pub task_run_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunAttemptsData {
    pub input: UpdateTaskRunAttemptsDataInput,
    pub filter: UpdateTaskRunAttemptsDataFilter,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TaskRunAttempt {
    pub id: i64,
    pub task_run_id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub attempt: u32,
    pub status: TaskRunAttemptStatus,
    pub stdout: String,
    pub stderr: String,
}

impl CRUD {
    pub async fn insert_task_run_attempt<'e, E>(&self, executor: E, data: &InsertTaskRunAttemptData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO task_run_attempt (task_run_id, job_run_id, job_id, task_id, created_at, attempt, status, stdout, stderr) VALUES (?, ?, ?, ?, ?, ?, ?, '', '')"
        )
            .bind(data.input.task_run_id)
            .bind(data.input.job_run_id)
            .bind(&data.input.job_id)
            .bind(&data.input.task_id)
            .bind(self.toolkit.get_current_ts())
            .bind(data.input.attempt)
            .bind(&data.input.status)
            .execute(executor)
            .await?;

        Ok(res.last_insert_rowid())
    }

    pub async fn select_task_run_attempts<'e, E>(&self, executor: E, data: &SelectTaskRunAttemptsData) -> anyhow::Result<Vec<TaskRunAttempt>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, task_run_id, job_run_id, job_id, task_id, created_at, started_at, finished_at, attempt, status, stdout, stderr FROM task_run_attempt WHERE 1=1"
        );

        if let Some(task_run_id) = data.filter.task_run_id {
            query_builder.push(" AND task_run_id = ");
            query_builder.push_bind(task_run_id);
        }

        if let Some(job_run_id) = data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
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
                SelectTaskRunAttemptsDataSort::Id => {
                    query_builder.push(" ORDER BY id ASC");
                }
            }
        }

        let attempts = query_builder
            .build_query_as::<TaskRunAttempt>()
            .fetch_all(executor)
            .await?;

        Ok(attempts)
    }


    pub async fn update_task_run_attempts<'e, E>(&self, executor: E, data: &UpdateTaskRunAttemptsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("UPDATE task_run_attempt SET ");
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

        if let Some(stdout) = &data.input.stdout {
            separated.push("stdout = ");
            separated.push_bind_unseparated(stdout);
        }

        if let Some(stderr) = &data.input.stderr {
            separated.push("stderr = ");
            separated.push_bind_unseparated(stderr);
        }

        if data.input.status.is_none() && data.input.started_at.is_none() && data.input.finished_at.is_none() && data.input.stdout.is_none() && data.input.stderr.is_none() {
            return Ok(());
        }

        query_builder.push(" WHERE 1=1");

        if let Some(id) = data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(task_run_id) = data.filter.task_run_id {
            query_builder.push(" AND task_run_id = ");
            query_builder.push_bind(task_run_id);
        }

        let query = query_builder.build();
        query.execute(executor).await?;

        Ok(())
    }
}
