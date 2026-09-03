use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use chrono::Utc;
use tokio::io::AsyncReadExt;
use tokio::time::interval;


/// Waits on the child process TaskRunAttemptDispatcher spawned for every running task
/// run attempt and finishes the attempt once its process exits, runs past its timeout
/// or gets aborted. Takes the process out of TaskRunAttemptChildren and owns it from
/// there on, and aborts an attempt whose process is gone.
/// Runs independently of TaskRunAttemptDispatcher, picking up whatever it set to
/// running. Knows nothing about task runs or retries: it never writes a task run
/// status, and TaskRunMonitor reads the attempt statuses written here to decide what
/// the task run does next.
pub struct TaskRunAttemptMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub children: Arc<TaskRunAttemptChildren>,
}


impl TaskRunAttemptMonitor {

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
                    eprintln!("Task Run Attempt Monitor error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every running task run attempt, once per second, until selecting them fails.
    async fn run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {

            timer.tick().await;

            let running_task_run_attempts = Self::get_task_run_attempts(
                crud.clone(),
                conn_pool.clone(),
                TaskRunAttemptStatus::Running,
            ).await?;

            // A row the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other row from being
            // handled, since the restarted loop would select the same row again.
            for task_run_attempt in &running_task_run_attempts {
                if let Err(e) = Self::handle_running_task_run_attempt(
                    crud.clone(),
                    conn_pool.clone(),
                    children.clone(),
                    task_run_attempt,
                ).await {
                    eprintln!(
                        "Task Run Attempt Monitor error on task run attempt {} of task run {}: {e:?}",
                        task_run_attempt.id,
                        task_run_attempt.task_run_id,
                    );
                }
            }

        }

    }

    /// Aborts, times out or finishes the attempt, keeps its process otherwise, and
    /// aborts the attempt that has no process at all.
    async fn handle_running_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        let task_run_attempt_child = children.remove(task_run_attempt.id).await;

        let Some(mut task_run_attempt_child) = task_run_attempt_child else {
            return Self::handle_missing_task_run_attempt_child(
                crud.clone(),
                conn_pool.clone(),
                task_run_attempt,
            ).await;
        };

        let job_run_stopped = Self::is_job_run_stopped(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
        ).await?;

        if job_run_stopped {
            return Self::handle_job_run_stopped(
                crud.clone(),
                conn_pool.clone(),
                task_run_attempt,
                task_run_attempt_child,
            ).await;
        }

        let task_run_attempt_timed_out = Self::is_task_run_attempt_timed_out(&task_run_attempt_child);

        if task_run_attempt_timed_out {
            return Self::handle_timed_out_task_run_attempt(
                crud.clone(),
                conn_pool.clone(),
                task_run_attempt,
                task_run_attempt_child,
            ).await;
        }

        if let Some(exit_status) = task_run_attempt_child.child.try_wait()? {
            return Self::handle_exited_task_run_attempt(
                crud.clone(),
                conn_pool.clone(),
                task_run_attempt,
                task_run_attempt_child,
                exit_status,
            ).await;
        }

        Self::handle_unfinished_task_run_attempt(
            crud.clone(),
            conn_pool.clone(),
            children.clone(),
            task_run_attempt,
            task_run_attempt_child,
        ).await
    }

    /// Persists the output of an attempt whose process is still running and puts the
    /// process back for the next tick.
    async fn handle_unfinished_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        task_run_attempt: &TaskRunAttempt,
        mut task_run_attempt_child: TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        Self::read_output(&mut task_run_attempt_child).await;

        Self::update_task_run_attempt_output(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
            &task_run_attempt_child,
        ).await?;

        children.insert(task_run_attempt.id, task_run_attempt_child).await;

        Ok(())
    }

    /// Aborts an attempt whose process is not in TaskRunAttemptChildren: it was spawned
    /// by an earlier run of this program, so there is nothing left to wait for and its
    /// output is whatever was persisted before.
    async fn handle_missing_task_run_attempt_child(
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
                    status: Some(TaskRunAttemptStatus::Aborted),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    stdout: None,
                    stderr: None,
                },
            },
        ).await?;

        Ok(())
    }

    /// Kills the process of a stopped job run and aborts the attempt.
    async fn handle_job_run_stopped(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
        mut running_task_run_attempt: TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        Self::read_output(&mut running_task_run_attempt).await;

        let _ = running_task_run_attempt.child.kill().await;

        Self::finish_task_run_attempt(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
            &running_task_run_attempt,
            TaskRunAttemptStatus::Aborted,
        ).await
    }

    /// Kills the process that ran past its timeout and times the attempt out.
    async fn handle_timed_out_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
        mut running_task_run_attempt: TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        Self::read_output(&mut running_task_run_attempt).await;

        let _ = running_task_run_attempt.child.kill().await;

        Self::finish_task_run_attempt(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
            &running_task_run_attempt,
            TaskRunAttemptStatus::TimedOut,
        ).await
    }

    /// Finishes the attempt with the status its process exited with.
    async fn handle_exited_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
        mut running_task_run_attempt: TaskRunAttemptChild,
        exit_status: ExitStatus,
    ) -> anyhow::Result<()> {

        Self::read_output(&mut running_task_run_attempt).await;

        let status = match exit_status.success() {
            true => TaskRunAttemptStatus::Succeeded,
            false => TaskRunAttemptStatus::Failed,
        };

        Self::finish_task_run_attempt(
            crud.clone(),
            conn_pool.clone(),
            task_run_attempt,
            &running_task_run_attempt,
            status,
        ).await
    }

    fn is_task_run_attempt_timed_out(task_run_attempt_child: &TaskRunAttemptChild) -> bool {
        Utc::now() > task_run_attempt_child.times_out_at
    }

    /// Drains whatever the process has written so far without blocking on it.
    async fn read_output(running_task_run_attempt: &mut TaskRunAttemptChild) {

        let mut buf = [0; 1024];

        loop {
            match tokio::time::timeout(Duration::from_millis(10), running_task_run_attempt.stdout.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => running_task_run_attempt.stdout_accumulated.extend_from_slice(&buf[..n]),
                _ => break,
            }
        }

        loop {
            match tokio::time::timeout(Duration::from_millis(10), running_task_run_attempt.stderr.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => running_task_run_attempt.stderr_accumulated.extend_from_slice(&buf[..n]),
                _ => break,
            }
        }

    }

    async fn get_task_run_attempts(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        status: TaskRunAttemptStatus,
    ) -> anyhow::Result<Vec<TaskRunAttempt>> {

        crud.select_task_run_attempts(
            &*conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    status: Some(status),
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await

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

    async fn finish_task_run_attempt(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
        running_task_run_attempt: &TaskRunAttemptChild,
        status: TaskRunAttemptStatus,
    ) -> anyhow::Result<()> {

        crud.update_task_run_attempts(
            &*conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(status),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    stdout: Some(String::from_utf8_lossy(&running_task_run_attempt.stdout_accumulated).to_string()),
                    stderr: Some(String::from_utf8_lossy(&running_task_run_attempt.stderr_accumulated).to_string()),
                },
            },
        ).await?;

        Ok(())
    }

    async fn update_task_run_attempt_output(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run_attempt: &TaskRunAttempt,
        running_task_run_attempt: &TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        crud.update_task_run_attempts(
            &*conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: None,
                    started_at: None,
                    finished_at: None,
                    stdout: Some(String::from_utf8_lossy(&running_task_run_attempt.stdout_accumulated).to_string()),
                    stderr: Some(String::from_utf8_lossy(&running_task_run_attempt.stderr_accumulated).to_string()),
                },
            },
        ).await?;

        Ok(())
    }

}
