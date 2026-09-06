use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;
use tokio::io::AsyncReadExt;


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
    pub signals: Arc<Signals>,
}


impl TaskRunAttemptMonitor {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        signals: Arc<Signals>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            children,
            signals,
        }
    }

    /// Tries each way a running attempt can finish, keeping its process for the next pass
    /// if none fired. A missing process is handled first, since the rest need one.
    async fn handle_running_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        let task_run_attempt_child = self.children.remove(task_run_attempt.id).await;

        let Some(mut task_run_attempt_child) = task_run_attempt_child else {
            self.transition_to_aborted_without_child(task_run_attempt).await?;
            return Ok(());
        };

        if self.transition_to_aborted(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        if self.transition_to_timed_out(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        if self.transition_to_exit_status(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        self.keep_task_run_attempt_running(task_run_attempt, task_run_attempt_child).await
    }

    /// Aborts an attempt whose process is not in TaskRunAttemptChildren: it was spawned
    /// by an earlier run of this program, so there is nothing left to wait for and its
    /// output is whatever was persisted before. This is the restart path.
    async fn transition_to_aborted_without_child(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

    /// Kills the process of a stopped job run and aborts the attempt.
    async fn transition_to_aborted(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let job_run_stopped = self.is_job_run_stopped(task_run_attempt).await?;

        if !job_run_stopped {
            return Ok(false);
        }

        Self::read_output(task_run_attempt_child).await;

        let _ = task_run_attempt_child.child.kill().await;

        self.finish_task_run_attempt(
            task_run_attempt,
            task_run_attempt_child,
            TaskRunAttemptStatus::Aborted,
        ).await?;

        Ok(true)
    }

    /// Kills the process that ran past its timeout and times the attempt out.
    async fn transition_to_timed_out(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let timed_out = Self::is_task_run_attempt_timed_out(task_run_attempt_child);

        if !timed_out {
            return Ok(false);
        }

        Self::read_output(task_run_attempt_child).await;

        let _ = task_run_attempt_child.child.kill().await;

        self.finish_task_run_attempt(
            task_run_attempt,
            task_run_attempt_child,
            TaskRunAttemptStatus::TimedOut,
        ).await?;

        Ok(true)
    }

    /// Finishes the attempt with the status its process exited with, once it has exited.
    async fn transition_to_exit_status(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let Some(exit_status) = task_run_attempt_child.child.try_wait()? else {
            return Ok(false);
        };

        Self::read_output(task_run_attempt_child).await;

        let status = match exit_status.success() {
            true => TaskRunAttemptStatus::Succeeded,
            false => TaskRunAttemptStatus::Failed,
        };

        self.finish_task_run_attempt(task_run_attempt, task_run_attempt_child, status).await?;

        Ok(true)
    }

    /// Persists the output of an unfinished attempt and puts its process back.
    async fn keep_task_run_attempt_running(
        &self,
        task_run_attempt: &TaskRunAttempt,
        mut task_run_attempt_child: TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        Self::read_output(&mut task_run_attempt_child).await;

        self.update_task_run_attempt_output(task_run_attempt, &task_run_attempt_child).await?;

        self.children.insert(task_run_attempt.id, task_run_attempt_child).await;

        Ok(())
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

    async fn get_task_run_attempts(&self, status: TaskRunAttemptStatus) -> anyhow::Result<Vec<TaskRunAttempt>> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    task_id: None,
                    status: Some(status),
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await

    }

    async fn is_job_run_stopped(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let job_run_stop = self.crud.select_job_run_stop(
            &*self.conn_pool,
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
        &self,
        task_run_attempt: &TaskRunAttempt,
        running_task_run_attempt: &TaskRunAttemptChild,
        status: TaskRunAttemptStatus,
    ) -> anyhow::Result<()> {

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

    async fn update_task_run_attempt_output(
        &self,
        task_run_attempt: &TaskRunAttempt,
        running_task_run_attempt: &TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
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

        // No publish: this runs on every pass for every running attempt and changes no
        // status, so waking all six pollers here would put the bus back into a loop.
        Ok(())
    }

}


impl Service for TaskRunAttemptMonitor {
    type Row = TaskRunAttempt;

    fn name(&self) -> &'static str {
        "Task Run Attempt Monitor"
    }

    fn row_context(&self, task_run_attempt: &TaskRunAttempt) -> String {
        format!(
            "task run attempt {} of task run {}",
            task_run_attempt.id,
            task_run_attempt.task_run_id,
        )
    }

    async fn select(&self) -> anyhow::Result<Vec<TaskRunAttempt>> {
        self.get_task_run_attempts(TaskRunAttemptStatus::Running).await
    }

    async fn handle(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {
        self.handle_running_task_run_attempt(task_run_attempt).await
    }
}
