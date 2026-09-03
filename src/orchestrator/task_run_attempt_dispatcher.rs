use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, Task};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use chrono::{TimeDelta, Utc};
use tokio::time::interval;


/// Picks up pending task run attempts and either skips them or spawns their command and
/// sets them to running. Hands the child process to TaskRunAttemptMonitor through
/// TaskRunAttemptChildren, and the attempt itself through its status, never by calling
/// it.
pub struct TaskRunAttemptDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub children: Arc<TaskRunAttemptChildren>,
}


impl TaskRunAttemptDispatcher {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            children,
        }
    }

    /// Spawns the polling loop and returns immediately, restarting it on error.
    pub fn start(self: &Self) {

        let crud = self.crud.clone();
        let conn_pool = self.conn_pool.clone();
        let children = self.children.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::run(crud.clone(), conn_pool.clone(), children.clone()).await {
                    eprintln!("Task Run Attempt Dispatcher error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every pending task run attempt, once per second, until selecting them fails.
    async fn run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {

            timer.tick().await;

            let task_run_attempts = Self::get_pending_task_run_attempts(crud.clone(), conn_pool.clone()).await?;

            // A row the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other row from being
            // handled, since the restarted loop would select the same row again.
            for task_run_attempt in &task_run_attempts {
                if let Err(e) = Self::handle_pending_task_run_attempt(
                    crud.clone(),
                    conn_pool.clone(),
                    children.clone(),
                    task_run_attempt,
                ).await {
                    eprintln!(
                        "Task Run Attempt Dispatcher error on task run attempt {} of task run {}: {e:?}",
                        task_run_attempt.id,
                        task_run_attempt.task_run_id,
                    );
                }
            }

        }

    }

    /// Skips the attempt if its job run was stopped, otherwise starts it.
    async fn handle_pending_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        let status = Self::derive_next_task_run_attempt_status(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
        ).await?;

        if status == TaskRunAttemptStatus::Skipped {
            return Self::handle_stopped_job_run(crud.clone(), conn_pool.clone(), task_run_attempt).await;
        }

        Self::handle_start_task_run_attempt(
            crud.clone(),
            conn_pool.clone(),
            children.clone(),
            task_run_attempt,
        ).await
    }

    /// Derives the status a pending attempt moves to: skipped if its job run was
    /// stopped before it could run, running otherwise. There is nothing else to wait
    /// for, the task run has already resolved its dependencies.
    async fn derive_next_task_run_attempt_status(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<TaskRunAttemptStatus> {

        let job_run_stopped = Self::is_job_run_stopped(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
        ).await?;

        if job_run_stopped {
            return Ok(TaskRunAttemptStatus::Skipped);
        }

        Ok(TaskRunAttemptStatus::Running)
    }

    /// Spawns the command of the attempt, hands the child process over and sets the
    /// attempt to running, which is what makes TaskRunAttemptMonitor pick it up.
    /// The child has to be in TaskRunAttemptChildren before the status is written, or
    /// the monitor sees a running attempt with no process and aborts it.
    async fn handle_start_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        let task = Self::get_task(crud.clone(), conn_pool.clone(), task_run_attempt).await?;

        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&task.command)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stdout of task: {}", task_run_attempt.task_id))?;
        let stderr = child.stderr.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stderr of task: {}", task_run_attempt.task_id))?;

        let started_at = Utc::now();
        let times_out_at = started_at + TimeDelta::seconds(task.timeout as i64);

        let running_task_run_attempt = TaskRunAttemptChild {
            child,
            stdout,
            stderr,
            stdout_accumulated: Vec::new(),
            stderr_accumulated: Vec::new(),
            times_out_at,
        };

        children.insert(task_run_attempt.id, running_task_run_attempt).await;

        crud.update_task_run_attempts(
            &*conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Running),
                    started_at: Some(Some(started_at)),
                    finished_at: None,
                    stdout: None,
                    stderr: None,
                },
            },
        ).await?;

        Ok(())
    }

    /// Skips the attempt of a stopped job run, whose command never started.
    async fn handle_stopped_job_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        crud.update_task_run_attempts(
            &*conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    stdout: None,
                    stderr: None,
                },
            },
        ).await?;

        Ok(())
    }

    async fn get_pending_task_run_attempts(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<Vec<TaskRunAttempt>> {

        crud.select_task_run_attempts(
            &*conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    task_id: None,
                    status: Some(TaskRunAttemptStatus::Pending),
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await

    }

    async fn get_task(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<Task> {

        crud.select_task(
            &*conn_pool,
            &SelectTasksData {
                filter: SelectTasksDataFilter {
                    job_id: Some(task_run_attempt.job_id.clone()),
                    task_id: Some(task_run_attempt.task_id.clone()),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        )
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found: {}", task_run_attempt.task_id))
    }

    async fn is_job_run_stopped(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<bool> {

        let job_run_stop = crud.select_job_run_stop(
            &*conn_pool,
            &SelectJobRunStopsData {
                filter: SelectJobRunStopsDataFilter {
                    id: None,
                    job_run_id: Some(task_run_attempt.job_run_id),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        ).await?;

        Ok(job_run_stop.is_some())

    }

}
