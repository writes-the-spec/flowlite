use std::process::Stdio;
use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::{TimeDelta, Utc};


/// Picks up pending task run attempts and settles each one as skipped, or as running by
/// spawning its command. Hands the child process to TaskRunAttemptMonitor through
/// TaskRunAttemptChildren, and the attempt itself through its status, never by calling
/// it.
pub struct TaskRunAttemptDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub children: Arc<TaskRunAttemptChildren>,
    pub signals: Arc<Signals>,
}


impl TaskRunAttemptDispatcher {

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

    /// Settles a pending attempt as exactly one outcome. `settle_as_pending` has to precede
    /// `settle_as_running`, which spawns unconditionally and would start a retry the moment
    /// it was inserted.
    async fn handle_pending_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        if self.settle_as_skipped(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_pending(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_running(task_run_attempt).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Task run attempt {} settled as nothing: its job run was not stopped and it \
             was not started",
            task_run_attempt.id,
        )
    }

    /// Skips the attempt if its job run was stopped, so its command never started.
    async fn settle_as_skipped(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let job_run_stopped = self.is_job_run_stopped(task_run_attempt).await?;

        if !job_run_stopped {
            return Ok(false);
        }

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(true)
    }

    /// Leaves the attempt pending, writing nothing, while the retry delay of the run it
    /// belongs to has yet to pass. Returns whether this is what happened.
    ///
    /// Only a retry waits. Attempt 1 is inserted by TaskRunDispatcher as it starts the task
    /// run and has nothing to wait for, so it never reaches the task run query below.
    async fn settle_as_pending(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        if task_run_attempt.attempt <= 1 {
            return Ok(false);
        }

        let task_run = self.get_task_run(task_run_attempt).await?;

        Ok(Self::is_waiting_to_retry(&task_run, task_run_attempt))
    }

    /// Whether the retry_delay the run was submitted with has yet to pass since this
    /// attempt was created, which is when TaskRunMonitor decided to retry.
    fn is_waiting_to_retry(task_run: &TaskRun, task_run_attempt: &TaskRunAttempt) -> bool {
        Utc::now() < task_run_attempt.created_at + TimeDelta::seconds(task_run.retry_delay as i64)
    }

    /// Spawns the command of the attempt, hands the child process over and sets the
    /// attempt to running, which is what makes TaskRunAttemptMonitor pick it up.
    ///
    /// The child has to be in TaskRunAttemptChildren before the status is written, or
    /// the monitor sees a running attempt with no process and aborts it.
    async fn settle_as_running(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let task_run = self.get_task_run(task_run_attempt).await?;

        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&task_run.command)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stdout of task: {}", task_run_attempt.task_id))?;
        let stderr = child.stderr.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stderr of task: {}", task_run_attempt.task_id))?;

        let started_at = Utc::now();
        let times_out_at = started_at + TimeDelta::seconds(task_run.timeout as i64);

        let running_task_run_attempt = TaskRunAttemptChild {
            child,
            stdout,
            stderr,
            stdout_accumulated: Vec::new(),
            stderr_accumulated: Vec::new(),
            times_out_at,
        };

        self.children.insert(task_run_attempt.id, running_task_run_attempt).await;

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(true)
    }

    async fn get_pending_task_run_attempts(&self) -> anyhow::Result<Vec<TaskRunAttempt>> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
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

    /// Loads the task run the attempt belongs to, for the command and timeout it was
    /// submitted with. The config is read off the run rather than out of mem.task, so an
    /// attempt spawns what its run was submitted with however the YAML has moved since.
    async fn get_task_run(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<TaskRun> {

        self.crud.select_task_run(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: Some(task_run_attempt.task_run_id),
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            }
        )
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task run not found: {}", task_run_attempt.task_run_id))
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

}


impl Service for TaskRunAttemptDispatcher {
    type Row = TaskRunAttempt;

    fn name(&self) -> &'static str {
        "Task Run Attempt Dispatcher"
    }

    fn row_context(&self, task_run_attempt: &TaskRunAttempt) -> String {
        format!(
            "task run attempt {} of task run {}",
            task_run_attempt.id,
            task_run_attempt.task_run_id,
        )
    }

    async fn select(&self) -> anyhow::Result<Vec<TaskRunAttempt>> {
        self.get_pending_task_run_attempts().await
    }

    async fn handle(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {
        self.handle_pending_task_run_attempt(task_run_attempt).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run::TaskRunStatus;
    use chrono::DateTime;

    fn task_run(retry_delay: u32) -> TaskRun {
        TaskRun {
            id: 1,
            job_run_id: 1,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            command: "false".to_string(),
            depends_on: sqlx::types::Json(Vec::new()),
            timeout: 3600,
            max_retries: 2,
            retry_delay,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            status: TaskRunStatus::Running,
        }
    }

    fn retry_attempt(created_at: DateTime<Utc>) -> TaskRunAttempt {
        TaskRunAttempt {
            id: 2,
            task_run_id: 1,
            job_run_id: 1,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            created_at,
            started_at: None,
            finished_at: None,
            attempt: 2,
            status: TaskRunAttemptStatus::Pending,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn the_retry_waits_while_the_delay_has_not_passed() {
        let created_at = Utc::now() - TimeDelta::seconds(10);

        let waiting = TaskRunAttemptDispatcher::is_waiting_to_retry(
            &task_run(60),
            &retry_attempt(created_at),
        );

        assert!(waiting);
    }

    #[test]
    fn the_retry_starts_once_the_delay_has_passed() {
        let created_at = Utc::now() - TimeDelta::seconds(61);

        let waiting = TaskRunAttemptDispatcher::is_waiting_to_retry(
            &task_run(60),
            &retry_attempt(created_at),
        );

        assert!(!waiting);
    }

    #[test]
    fn a_retry_delay_of_zero_never_waits() {
        let waiting = TaskRunAttemptDispatcher::is_waiting_to_retry(
            &task_run(0),
            &retry_attempt(Utc::now()),
        );

        assert!(!waiting);
    }
}
