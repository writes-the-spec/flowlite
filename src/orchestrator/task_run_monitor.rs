use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


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
    /// and `settle_for_running` split a failed one on whether an attempt is left — so unlike
    /// JobRunMonitor the order here carries nothing, and matches that ladder only so the
    /// two read alike. The bail replaces the exhaustive match this used to be: a new
    /// TaskRunAttemptStatus no longer fails to compile, it reaches the bail at runtime and
    /// `Poller::run` logs it with the row id.
    async fn handle_running_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        let last_task_run_attempt = self.get_last_task_run_attempt(task_run).await?;

        if self.settle_for_succeeded(task_run, &last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_running(task_run, &last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_failed(task_run, &last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_timed_out(task_run, &last_task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_for_aborted(task_run, &last_task_run_attempt).await? {
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
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<bool> {

        if last_task_run_attempt.status != TaskRunAttemptStatus::Succeeded {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::Succeeded).await?;

        Ok(true)
    }

    /// Keeps the task run running: waits while an attempt is in flight, or inserts the next
    /// one. Writes no task run status — the task run stays Running for the whole retry loop.
    ///
    /// The retry row goes in immediately; `TaskRunAttemptDispatcher` is what holds it
    /// pending until `retry_delay` has passed.
    async fn settle_for_running(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<bool> {

        // The attempt services still own the attempt.
        if !last_task_run_attempt.status.is_finished() {
            return Ok(true);
        }

        if last_task_run_attempt.status != TaskRunAttemptStatus::Failed {
            return Ok(false);
        }

        // Attempts count from 1, so the task run gets max_retries + 1 of them.
        if last_task_run_attempt.attempt >= task_run.max_retries + 1 {
            return Ok(false);
        }

        self.start_task_run_attempt(
            task_run,
            last_task_run_attempt.attempt + 1,
        ).await?;

        Ok(true)
    }

    /// Fails the task run once its attempts are used up. Attempts count from 1, so the
    /// task run gets `max_retries + 1` of them.
    async fn settle_for_failed(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<bool> {

        if last_task_run_attempt.status != TaskRunAttemptStatus::Failed {
            return Ok(false);
        }

        if last_task_run_attempt.attempt < task_run.max_retries + 1 {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::Failed).await?;

        Ok(true)
    }

    async fn settle_for_timed_out(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<bool> {

        if last_task_run_attempt.status != TaskRunAttemptStatus::TimedOut {
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
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<bool> {

        if !last_task_run_attempt.status.is_stopped() {
            return Ok(false);
        }

        self.update_task_run_status(task_run, TaskRunStatus::Aborted).await?;

        Ok(true)
    }

    /// Inserts the retry TaskRunAttemptDispatcher clears to run, once its retry_delay has
    /// passed. Attempt 1 is not inserted here — TaskRunDispatcher creates it as it starts
    /// the task run.
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

    /// The attempt that decides what the task run does next, which is the highest id: the
    /// earlier ones are the retries already accounted for.
    ///
    /// A Running task run always has one — TaskRunDispatcher inserts attempt 1 before it
    /// writes Running — so none at all is a broken invariant rather than a state to handle,
    /// and this says so instead of leaving the caller an Option to interpret.
    async fn get_last_task_run_attempt(&self, task_run: &TaskRun) -> anyhow::Result<TaskRunAttempt> {

        let task_run_attempts = self.crud.select_task_run_attempts(
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
        ).await?;

        task_run_attempts
            .into_iter()
            .last()
            .ok_or_else(|| anyhow::anyhow!(
                "Task run {} is running with no attempt to decide from",
                task_run.id,
            ))

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
