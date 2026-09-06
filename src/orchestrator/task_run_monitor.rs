use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::{TimeDelta, Utc};


/// Watches running task runs and drives them through their attempts: it starts the
/// first one, retries a failed one while the task has retries left, and finishes the
/// task run with the status of its last attempt otherwise. Hands off to the attempt
/// services through the attempt row only, never by calling them, and never touches a
/// child process itself.
pub struct TaskRunMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl TaskRunMonitor {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        signals: Arc<Signals>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            signals,
        }
    }

    /// Settles a running task run as exactly one outcome, from its last attempt.
    ///
    /// The guards are exclusive — the last attempt has one status, and `settle_for_failed`
    /// and `settle_for_running` split a failed one by whether a retry is left — so unlike
    /// JobRunMonitor the order here carries nothing, and matches that ladder only so the
    /// two read alike. The bail replaces the exhaustive match this used to be: a new
    /// TaskRunAttemptStatus no longer fails to compile, it reaches the bail at runtime and
    /// `Poller::run` logs it with the row id.
    async fn handle_running_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        let task_run_attempts = self.get_task_run_attempts(task_run).await?;

        let last_task_run_attempt = task_run_attempts.last();

        if self.settle_for_succeeded(task_run, last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_running(task_run, last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_failed(task_run, last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_timed_out(task_run, last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_aborted(task_run, last_task_run_attempt).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Task run {} settled as nothing: no outcome claimed the status of its last \
             attempt",
            task_run.id,
        )
    }

    async fn settle_for_succeeded(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: Option<&TaskRunAttempt>,
    ) -> anyhow::Result<bool> {

        if !Self::last_attempt_has_status(last_task_run_attempt, TaskRunAttemptStatus::Succeeded) {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::Succeeded).await?;

        Ok(true)
    }

    /// Keeps the task run running and makes sure an attempt is in flight: starts the first
    /// one, waits out a retry delay, or starts the retry. Writes no task run status — the
    /// task run stays Running for the whole retry loop.
    async fn settle_for_running(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: Option<&TaskRunAttempt>,
    ) -> anyhow::Result<bool> {

        let Some(last_task_run_attempt) = last_task_run_attempt else {
            self.start_task_run_attempt(task_run, 1).await?;
            return Ok(true);
        };

        match last_task_run_attempt.status {

            // The attempt services still own the attempt.
            TaskRunAttemptStatus::Pending | TaskRunAttemptStatus::Running => Ok(true),

            TaskRunAttemptStatus::Failed if Self::has_retry_left(task_run, last_task_run_attempt) => {

                // Nothing is written while the delay runs down: the next pass asks the
                // same question again, until the wait is over.
                if Self::is_waiting_to_retry(task_run, last_task_run_attempt) {
                    return Ok(true);
                }

                self.start_task_run_attempt(
                    task_run,
                    last_task_run_attempt.attempt + 1,
                ).await?;

                Ok(true)
            }

            _ => Ok(false),
        }
    }

    /// Fails the task run once its attempts are used up.
    async fn settle_for_failed(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: Option<&TaskRunAttempt>,
    ) -> anyhow::Result<bool> {

        if !Self::last_attempt_has_status(last_task_run_attempt, TaskRunAttemptStatus::Failed) {
            return Ok(false);
        }

        let Some(last_task_run_attempt) = last_task_run_attempt else {
            return Ok(false);
        };

        if Self::has_retry_left(task_run, last_task_run_attempt) {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::Failed).await?;

        Ok(true)
    }

    async fn settle_for_timed_out(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: Option<&TaskRunAttempt>,
    ) -> anyhow::Result<bool> {

        if !Self::last_attempt_has_status(last_task_run_attempt, TaskRunAttemptStatus::TimedOut) {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::TimedOut).await?;

        Ok(true)
    }

    /// Aborts the task run, where both stop outcomes land: the attempt was killed
    /// mid-flight, or skipped before its command started.
    ///
    /// A skipped attempt does **not** make the task run Skipped. It only ever sees Running
    /// task runs, which had started and may already have left output, so Skipped would
    /// claim nothing ran.
    async fn settle_for_aborted(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: Option<&TaskRunAttempt>,
    ) -> anyhow::Result<bool> {

        let stopped = matches!(last_task_run_attempt, Some(attempt) if attempt.status.is_stopped());

        if !stopped {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::Aborted).await?;

        Ok(true)
    }

    fn last_attempt_has_status(
        last_task_run_attempt: Option<&TaskRunAttempt>,
        status: TaskRunAttemptStatus,
    ) -> bool {
        matches!(last_task_run_attempt, Some(attempt) if attempt.status == status)
    }

    /// Attempts count from 1, so the task run gets `max_retries + 1` of them.
    fn has_retry_left(task_run: &TaskRun, last_task_run_attempt: &TaskRunAttempt) -> bool {
        last_task_run_attempt.attempt < task_run.max_retries + 1
    }

    /// Whether the retry_delay the run was submitted with has yet to pass since its last
    /// attempt finished. An attempt with no finish time is not made to wait, since there
    /// is no moment to count the delay from.
    fn is_waiting_to_retry(task_run: &TaskRun, last_task_run_attempt: &TaskRunAttempt) -> bool {

        let Some(finished_at) = last_task_run_attempt.finished_at else {
            return false;
        };

        Utc::now() < finished_at + TimeDelta::seconds(task_run.retry_delay as i64)
    }

    /// Aborts the task run, where both stop outcomes land: the attempt was killed
    /// mid-flight, or skipped before its command started.
    ///
    /// A skipped attempt does **not** make the task run Skipped. It only ever sees Running
    /// task runs, which had started and may already have left output, so Skipped would
    /// claim nothing ran.
    /// Inserts the pending attempt TaskRunAttemptDispatcher clears to run. Writes no task
    /// run status: the task run stays Running for the whole retry loop.
    async fn start_task_run_attempt(&self, task_run: &TaskRun, attempt: u32) -> anyhow::Result<()> {

        self.crud.insert_task_run_attempt(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

    async fn get_running_task_runs(&self) -> anyhow::Result<Vec<TaskRun>> {

        self.crud.select_task_runs(
            &*self.conn_pool,
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

    async fn get_task_run_attempts(&self, task_run: &TaskRun) -> anyhow::Result<Vec<TaskRunAttempt>> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
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

    async fn update_task_run_status(&self, task_run: &TaskRun, status: TaskRunStatus) -> anyhow::Result<()> {

        self.crud.update_task_runs(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

}


impl Service for TaskRunMonitor {
    type Row = TaskRun;

    fn name(&self) -> &'static str {
        "Task Run Monitor"
    }

    fn row_context(&self, task_run: &TaskRun) -> String {
        format!("task run {}", task_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<TaskRun>> {
        self.get_running_task_runs().await
    }

    async fn handle(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.handle_running_task_run(task_run).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
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

    fn failed_attempt(finished_at: Option<DateTime<Utc>>) -> TaskRunAttempt {
        TaskRunAttempt {
            id: 1,
            task_run_id: 1,
            job_run_id: 1,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            created_at: Utc::now(),
            started_at: None,
            finished_at,
            attempt: 1,
            status: TaskRunAttemptStatus::Failed,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn an_attempt_that_never_finished_is_not_made_to_wait() {
        let waiting = TaskRunMonitor::is_waiting_to_retry(&task_run(60), &failed_attempt(None));

        assert!(!waiting);
    }

    #[test]
    fn the_retry_waits_while_the_delay_has_not_passed() {
        let finished_at = Utc::now() - TimeDelta::seconds(10);

        let waiting = TaskRunMonitor::is_waiting_to_retry(
            &task_run(60),
            &failed_attempt(Some(finished_at)),
        );

        assert!(waiting);
    }

    #[test]
    fn the_retry_starts_once_the_delay_has_passed() {
        let finished_at = Utc::now() - TimeDelta::seconds(61);

        let waiting = TaskRunMonitor::is_waiting_to_retry(
            &task_run(60),
            &failed_attempt(Some(finished_at)),
        );

        assert!(!waiting);
    }
}
