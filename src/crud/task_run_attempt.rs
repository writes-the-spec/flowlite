use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum TaskRunAttemptStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Skipped,
    Aborted,
    TimedOut,
    Invalid,
}

impl TaskRunAttemptStatus {

    /// Whether the attempt has settled and will not change again. Matched exhaustively so
    /// a new status has to declare which side of this line it falls on.
    pub fn is_finished(&self) -> bool {
        match self {
            TaskRunAttemptStatus::Queued
            | TaskRunAttemptStatus::Running => false,
            TaskRunAttemptStatus::Succeeded
            | TaskRunAttemptStatus::Failed
            | TaskRunAttemptStatus::Skipped
            | TaskRunAttemptStatus::Aborted
            | TaskRunAttemptStatus::TimedOut
            | TaskRunAttemptStatus::Invalid => true,
        }
    }

    /// Whether the attempt reports a stop: its process was killed mid-flight, or its
    /// command never started. Matched exhaustively so a new status has to declare its side.
    ///
    /// Both are only ever written for a stopped job run — TaskRunAttemptDispatcher skips an
    /// attempt for no other reason — so unlike `TaskRunStatus::is_stopped` this needs no
    /// failure ruled out first.
    ///
    /// `Invalid` is deliberately not a stop: nobody stopped an attempt flowlite merely
    /// lost track of, and its process may well still be running.
    pub fn is_stopped(&self) -> bool {
        match self {
            TaskRunAttemptStatus::Aborted
            | TaskRunAttemptStatus::Skipped => true,
            TaskRunAttemptStatus::Queued
            | TaskRunAttemptStatus::Running
            | TaskRunAttemptStatus::Succeeded
            | TaskRunAttemptStatus::Failed
            | TaskRunAttemptStatus::TimedOut
            | TaskRunAttemptStatus::Invalid => false,
        }
    }

}

impl std::fmt::Display for TaskRunAttemptStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskRunAttemptStatus::Queued => write!(f, "queued"),
            TaskRunAttemptStatus::Running => write!(f, "running"),
            TaskRunAttemptStatus::Succeeded => write!(f, "succeeded"),
            TaskRunAttemptStatus::Failed => write!(f, "failed"),
            TaskRunAttemptStatus::Skipped => write!(f, "skipped"),
            TaskRunAttemptStatus::Aborted => write!(f, "aborted"),
            TaskRunAttemptStatus::TimedOut => write!(f, "timedout"),
            TaskRunAttemptStatus::Invalid => write!(f, "invalid"),
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
    Attempt,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SelectTaskRunAttemptsData {
    pub filter: SelectTaskRunAttemptsDataFilter,
    pub sort: Option<SelectTaskRunAttemptsDataSort>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteTaskRunAttemptsDataFilter {
    pub task_run_id: Option<i64>,
    pub job_run_id: Option<i64>,
    pub task_id: Option<String>,
    pub status: Option<TaskRunAttemptStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteTaskRunAttemptsData {
    pub filter: DeleteTaskRunAttemptsDataFilter,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTaskRunAttemptsDataInput {
    pub status: Option<TaskRunAttemptStatus>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub finished_at: Option<Option<DateTime<Utc>>>,
    pub process_group_id: Option<Option<i64>>,
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
    /// The spawned child's pid, which `process_group(0)` makes its group id too. Kept on
    /// the row because `TaskRunAttemptChildren` is memory: after a restart this is the only
    /// way back to a process that may still be running.
    pub process_group_id: Option<i64>,
}

impl CRUD {
    pub async fn insert_task_run_attempt<'e, E>(&self, executor: E, data: &InsertTaskRunAttemptData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO task_run_attempt (task_run_id, job_run_id, job_id, task_id, created_at, attempt, status) VALUES (?, ?, ?, ?, ?, ?, ?)"
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
            "SELECT id, task_run_id, job_run_id, job_id, task_id, created_at, started_at, finished_at, attempt, status, process_group_id FROM task_run_attempt WHERE 1=1"
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
                SelectTaskRunAttemptsDataSort::Attempt => {
                    query_builder.push(" ORDER BY attempt ASC");
                }
            }
        }

        let attempts = query_builder
            .build_query_as::<TaskRunAttempt>()
            .fetch_all(executor)
            .await?;

        Ok(attempts)
    }


    /// Deletes every row in `task_run_attempt` matching `data.filter`. An entirely empty
    /// filter matches every row and so deletes the whole table — exact parity with an
    /// empty select filter, and the caller's business, not this method's.
    pub async fn delete_task_run_attempts<'e, E>(&self, executor: E, data: &DeleteTaskRunAttemptsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "DELETE FROM task_run_attempt WHERE 1=1"
        );

        if let Some(task_run_id) = &data.filter.task_run_id {
            query_builder.push(" AND task_run_id = ");
            query_builder.push_bind(task_run_id);
        }

        if let Some(job_run_id) = &data.filter.job_run_id {
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

        query_builder.build().execute(executor).await?;

        Ok(())
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

        if let Some(process_group_id) = &data.input.process_group_id {
            separated.push("process_group_id = ");
            separated.push_bind_unseparated(process_group_id);
        }

        if data.input.status.is_none()
            && data.input.started_at.is_none()
            && data.input.finished_at.is_none()
            && data.input.process_group_id.is_none()
        {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_attempt_is_finished() {
        assert!(TaskRunAttemptStatus::Invalid.is_finished());
    }

    #[test]
    fn an_invalid_attempt_does_not_report_a_stop() {
        assert!(!TaskRunAttemptStatus::Invalid.is_stopped());
    }

    /// The group id is how a restart reaches a process the map no longer holds, so it has
    /// to survive on the row rather than in memory.
    #[tokio::test]
    async fn a_process_group_id_is_written_and_read_back() {

        let db = crate::test_support::TestDb::new().await;

        let job_run = db.insert_job_run(crate::crud::job_run::JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, crate::crud::task_run::TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;

        assert_eq!(task_run_attempt.process_group_id, None);

        db.crud.update_task_run_attempts(
            &*db.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: None,
                    started_at: None,
                    finished_at: None,
                    process_group_id: Some(Some(4242)),
                },
            },
        ).await.unwrap();

        assert_eq!(db.task_run_attempt(task_run_attempt.id).await.process_group_id, Some(4242));
    }

    async fn select(db: &crate::test_support::TestDb, filter: SelectTaskRunAttemptsDataFilter) -> Vec<TaskRunAttempt> {
        db.crud.select_task_run_attempts(&*db.conn_pool, &SelectTaskRunAttemptsData { filter, sort: None }).await.unwrap()
    }

    fn empty_filter() -> SelectTaskRunAttemptsDataFilter {
        SelectTaskRunAttemptsDataFilter { task_run_id: None, job_run_id: None, task_id: None, status: None }
    }

    /// `job_run_id` really filters: deleting by one job run's id only removes its attempt,
    /// leaving a neighbouring job run's attempt untouched.
    ///
    /// A throwaway job run is inserted first so `job_run.id` diverges from `task_run.id`
    /// and `task_run_attempt.id` (2, then 1 and 1 - not all three coinciding) - otherwise a
    /// delete that filtered on the attempt's own `id` or on `task_run_id` instead of
    /// `job_run_id` would still happen to hit the right row and this test would not notice.
    #[tokio::test]
    async fn delete_task_run_attempts_filters_by_job_run_id() {

        let db = crate::test_support::TestDb::new().await;

        db.insert_job_run(crate::crud::job_run::JobRunStatus::Failed).await;

        let job_run = db.insert_job_run(crate::crud::job_run::JobRunStatus::Failed).await;
        let task_run = db.insert_task_run(job_run.id, crate::crud::task_run::TaskRunStatus::Failed).await;
        let attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Failed).await;

        let other_job_run = db.insert_job_run(crate::crud::job_run::JobRunStatus::Failed).await;
        let other_task_run = db.insert_task_run(other_job_run.id, crate::crud::task_run::TaskRunStatus::Failed).await;
        let other_attempt = db.insert_task_run_attempt(&other_task_run, 1, TaskRunAttemptStatus::Failed).await;

        db.crud.delete_task_run_attempts(&*db.conn_pool, &DeleteTaskRunAttemptsData {
            filter: DeleteTaskRunAttemptsDataFilter { task_run_id: None, job_run_id: Some(job_run.id), task_id: None, status: None },
        }).await.unwrap();

        assert!(select(&db, SelectTaskRunAttemptsDataFilter { task_run_id: Some(attempt.task_run_id), ..empty_filter() }).await.is_empty());
        assert!(!select(&db, SelectTaskRunAttemptsDataFilter { task_run_id: Some(other_attempt.task_run_id), ..empty_filter() }).await.is_empty());
    }

    /// The decided behaviour, pinned so a future guard cannot be added silently: an
    /// entirely empty filter matches every row and so deletes the whole table.
    #[tokio::test]
    async fn an_empty_filter_deletes_every_task_run_attempt() {

        let db = crate::test_support::TestDb::new().await;

        let job_run = db.insert_job_run(crate::crud::job_run::JobRunStatus::Failed).await;
        let task_run = db.insert_task_run(job_run.id, crate::crud::task_run::TaskRunStatus::Failed).await;
        db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Failed).await;

        db.crud.delete_task_run_attempts(&*db.conn_pool, &DeleteTaskRunAttemptsData {
            filter: DeleteTaskRunAttemptsDataFilter { task_run_id: None, job_run_id: None, task_id: None, status: None },
        }).await.unwrap();

        assert!(select(&db, empty_filter()).await.is_empty());
    }
}
