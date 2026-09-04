use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, Task};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use chrono::{TimeDelta, Utc};
use tokio::time::interval;


/// Watches running task runs and drives them through their attempts: it starts the
/// first one, retries a failed one while the task has retries left, and finishes the
/// task run with the status of its last attempt otherwise. Hands off to the attempt
/// services through the attempt row only, never by calling them, and never touches a
/// child process itself.
pub struct TaskRunMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
}


impl TaskRunMonitor {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
        }
    }

    /// Spawns the polling loop and returns immediately, restarting it on error.
    pub fn start(self: &Self) {

        let crud = self.crud.clone();
        let conn_pool = self.conn_pool.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::run(crud.clone(), conn_pool.clone()).await {
                    eprintln!("Task Run Monitor error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every running task run, once per second, until selecting them fails.
    async fn run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {

            timer.tick().await;

            let task_runs = Self::get_running_task_runs(crud.clone(), conn_pool.clone()).await?;

            // A row the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other row from being
            // handled, since the restarted loop would select the same row again.
            for task_run in &task_runs {
                if let Err(e) = Self::handle_running_task_run(
                    crud.clone(),
                    conn_pool.clone(),
                    task_run,
                ).await {
                    eprintln!("Task Run Monitor error on task run {}: {e:?}", task_run.id);
                }
            }

        }

    }

    /// Starts the first attempt of a task run that has none, and otherwise hands its
    /// last attempt to the handler for that attempt's status.
    async fn handle_running_task_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        let task_run_attempts = Self::get_task_run_attempts(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        let Some(last_task_run_attempt) = task_run_attempts.last() else {
            return Self::handle_start_task_run_attempt(
                crud.clone(),
                conn_pool.clone(),
                task_run,
                1,
            ).await;
        };

        match last_task_run_attempt.status {
            TaskRunAttemptStatus::Pending => Self::handle_last_task_run_attempt_pending(),
            TaskRunAttemptStatus::Running => Self::handle_last_task_run_attempt_running(),
            TaskRunAttemptStatus::Succeeded => Self::handle_last_task_run_attempt_succeeded(
                crud.clone(),
                conn_pool.clone(),
                task_run,
            ).await,
            TaskRunAttemptStatus::Failed => Self::handle_last_task_run_attempt_failed(
                crud.clone(),
                conn_pool.clone(),
                task_run,
                last_task_run_attempt,
            ).await,
            TaskRunAttemptStatus::Skipped => Self::handle_last_task_run_attempt_skipped(
                crud.clone(),
                conn_pool.clone(),
                task_run,
            ).await,
            TaskRunAttemptStatus::Aborted => Self::handle_last_task_run_attempt_aborted(
                crud.clone(),
                conn_pool.clone(),
                task_run,
            ).await,
            TaskRunAttemptStatus::TimedOut => Self::handle_last_task_run_attempt_timed_out(
                crud.clone(),
                conn_pool.clone(),
                task_run,
            ).await,
        }
    }

    /// TaskRunAttemptDispatcher still owns the attempt, so the task run stays Running.
    fn handle_last_task_run_attempt_pending() -> anyhow::Result<()> {
        Ok(())
    }

    /// TaskRunAttemptMonitor still owns the attempt, so the task run stays Running.
    fn handle_last_task_run_attempt_running() -> anyhow::Result<()> {
        Ok(())
    }

    async fn handle_last_task_run_attempt_succeeded(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        Self::update_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
            TaskRunStatus::Succeeded,
        ).await
    }

    /// Starts the next attempt while the task has a retry left, and fails the task run
    /// once they are used up. Attempts count from 1, so the task run gets
    /// `max_retries + 1` of them.
    async fn handle_last_task_run_attempt_failed(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        let task = Self::get_task(crud.clone(), conn_pool.clone(), task_run).await?;

        if last_task_run_attempt.attempt < task.max_retries + 1 {

            // Nothing is written while the delay runs down: the task run stays Running
            // and the next tick asks the same question again, until the wait is over.
            if Self::is_waiting_to_retry(&task, last_task_run_attempt) {
                return Ok(());
            }

            return Self::handle_start_task_run_attempt(
                crud.clone(),
                conn_pool.clone(),
                task_run,
                last_task_run_attempt.attempt + 1,
            ).await;
        }

        Self::update_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
            TaskRunStatus::Failed,
        ).await
    }

    /// Whether the retry_delay of the task has yet to pass since its last attempt
    /// finished. An attempt with no finish time is not made to wait, since there is no
    /// moment to count the delay from.
    fn is_waiting_to_retry(task: &Task, last_task_run_attempt: &TaskRunAttempt) -> bool {

        let Some(finished_at) = last_task_run_attempt.finished_at else {
            return false;
        };

        Utc::now() < finished_at + TimeDelta::seconds(task.retry_delay as i64)
    }

    async fn handle_last_task_run_attempt_skipped(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        Self::update_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
            TaskRunStatus::Skipped,
        ).await
    }

    async fn handle_last_task_run_attempt_aborted(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        Self::update_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
            TaskRunStatus::Aborted,
        ).await
    }

    async fn handle_last_task_run_attempt_timed_out(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        Self::update_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
            TaskRunStatus::TimedOut,
        ).await
    }

    /// Inserts the pending attempt TaskRunAttemptDispatcher clears to run.
    async fn handle_start_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
        attempt: u32,
    ) -> anyhow::Result<()> {

        crud.insert_task_run_attempt(
            &*conn_pool,
            &InsertTaskRunAttemptData {
                input: InsertTaskRunAttemptDataInput {
                    task_run_id: task_run.id,
                    job_run_id: task_run.job_run_id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    attempt,
                    status: TaskRunAttemptStatus::Pending,
                },
            },
        ).await?;

        Ok(())
    }

    async fn get_running_task_runs(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<Vec<TaskRun>> {

        crud.select_task_runs(
            &*conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: Some(TaskRunStatus::Running),
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await

    }

    async fn get_task_run_attempts(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<Vec<TaskRunAttempt>> {

        crud.select_task_run_attempts(
            &*conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: Some(task_run.id),
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await

    }

    async fn get_task(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<Task> {

        crud.select_task(
            &*conn_pool,
            &SelectTasksData {
                filter: SelectTasksDataFilter {
                    job_id: Some(task_run.job_id.clone()),
                    task_id: Some(task_run.task_id.clone()),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        )
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found: {}", task_run.task_id))
    }

    async fn update_task_run_status(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
        status: TaskRunStatus,
    ) -> anyhow::Result<()> {

        crud.update_task_runs(
            &*conn_pool,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: Some(task_run.id),
                    job_run_id: None,
                    status: None,
                },
                input: UpdateTaskRunsDataInput {
                    status: Some(status),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                },
            }
        ).await?;

        Ok(())
    }

}
