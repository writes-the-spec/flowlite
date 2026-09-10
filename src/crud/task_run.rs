use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum TaskRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Aborted,
    TimedOut,
    Invalid,
}

impl TaskRunStatus {

    /// Whether the task run has settled and will not change again. Matched exhaustively
    /// on purpose: a new status has to say which side of this line it falls on, or it
    /// stops compiling.
    pub fn is_finished(&self) -> bool {
        match self {
            TaskRunStatus::Pending
            | TaskRunStatus::Running => false,
            TaskRunStatus::Succeeded
            | TaskRunStatus::Failed
            | TaskRunStatus::Skipped
            | TaskRunStatus::Aborted
            | TaskRunStatus::TimedOut
            | TaskRunStatus::Invalid => true,
        }
    }


    /// Whether the task run reports a stop: killed mid-flight, or never started. Matched
    /// exhaustively for the same reason as `is_finished`.
    ///
    /// `Aborted` always means a stop. `Skipped` means one only once a failure has been
    /// ruled out, since a dependency that did not succeed skips its dependents too — so
    /// ask this after the failure cases, not before them.
    ///
    /// `Invalid` is deliberately not a stop. `JobRunMonitor::settle_for_aborted` reads
    /// this as its abort signal, and nobody stopped a run flowlite merely lost track of —
    /// reporting it as `Aborted` is the exact conflation `Invalid` exists to end.
    pub fn is_stopped(&self) -> bool {
        match self {
            TaskRunStatus::Aborted
            | TaskRunStatus::Skipped => true,
            TaskRunStatus::Pending
            | TaskRunStatus::Running
            | TaskRunStatus::Succeeded
            | TaskRunStatus::Failed
            | TaskRunStatus::TimedOut
            | TaskRunStatus::Invalid => false,
        }
    }

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
            TaskRunStatus::Invalid => write!(f, "invalid"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertTaskRunDataInput {
    pub job_run_id: i64,
    pub job_id: String,
    pub task_id: String,
    pub command: String,
    pub depends_on: Vec<String>,
    pub timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: BTreeMap<String, String>,
    pub secret_env: BTreeMap<String, String>,
    pub working_dir: String,
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
    pub command: String,
    pub depends_on: sqlx::types::Json<Vec<String>>,
    pub timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: sqlx::types::Json<BTreeMap<String, String>>,
    /// Environment variable name to secret name - never a value.
    pub secret_env: sqlx::types::Json<BTreeMap<String, String>>,
    pub working_dir: String,
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
            "INSERT INTO task_run (job_run_id, job_id, task_id, command, depends_on, timeout, max_retries, retry_delay, env, secret_env, working_dir, created_at, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
            .bind(data.input.job_run_id)
            .bind(&data.input.job_id)
            .bind(&data.input.task_id)
            .bind(&data.input.command)
            .bind(sqlx::types::Json(&data.input.depends_on))
            .bind(data.input.timeout)
            .bind(data.input.max_retries)
            .bind(data.input.retry_delay)
            .bind(sqlx::types::Json(&data.input.env))
            .bind(sqlx::types::Json(&data.input.secret_env))
            .bind(&data.input.working_dir)
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
            "SELECT id, job_run_id, job_id, task_id, command, depends_on, timeout, max_retries, retry_delay, env, secret_env, working_dir, created_at, started_at, finished_at, status FROM task_run WHERE 1=1"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_task_run_is_finished() {
        assert!(TaskRunStatus::Invalid.is_finished());
    }

    /// Not a stop: JobRunMonitor reads is_stopped as its abort signal, and reporting the
    /// job run as Aborted is exactly the conflation Invalid exists to end. Nobody stopped
    /// this - flowlite lost track of it.
    #[test]
    fn an_invalid_task_run_does_not_report_a_stop() {
        assert!(!TaskRunStatus::Invalid.is_stopped());
    }

    /// `--json` (job-run get/list, job-run logs) prints this row straight through
    /// `serde_json`. Pin the wire shape: `secret_env` carries variable name to secret
    /// name, at the same level as `env`, never a resolved value - there is no value in
    /// this row to serialize.
    #[test]
    fn task_run_serializes_secret_env_as_variable_to_secret_name() {
        let task_run = TaskRun {
            id: 1,
            job_run_id: 2,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            command: "sh -c true".to_string(),
            depends_on: sqlx::types::Json(Vec::new()),
            timeout: 60,
            max_retries: 0,
            retry_delay: 0,
            env: sqlx::types::Json(BTreeMap::new()),
            secret_env: sqlx::types::Json(BTreeMap::from([
                ("PGPASSWORD".to_string(), "warehouse_pw".to_string()),
            ])),
            working_dir: "".to_string(),
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            status: TaskRunStatus::Pending,
        };

        let value = serde_json::to_value(&task_run).unwrap();

        assert_eq!(value["secret_env"], serde_json::json!({ "PGPASSWORD": "warehouse_pw" }));
    }
}
