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

    /// Dispatches the write for whatever `derive_next_status` decides. Deciding only reads.
    async fn handle_running_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        let last_task_run_attempt = self.get_or_start_task_run_attempt(task_run).await?;

        match self.derive_next_status(task_run, &last_task_run_attempt).await {
            Ok(TaskRunStatus::Succeeded) => self.set_to_succeeded(task_run).await,
            Ok(TaskRunStatus::Failed) => self.set_to_failed(task_run).await,
            Ok(TaskRunStatus::TimedOut) => self.set_to_timed_out(task_run).await,
            Ok(TaskRunStatus::Aborted) => self.set_to_aborted(task_run).await,
            Ok(TaskRunStatus::Running) => Ok(()),
            Ok(_) | Err(_) => self.set_to_invalid(task_run).await,
        }
    }

    /// Derives a running task run's next status from its last attempt alone: the guards
    /// are exclusive — the last attempt has one status — so the order carries nothing and
    /// follows the other two monitors only so all three read alike.
    ///
    /// A failed attempt with retries left inserts its next attempt here rather than
    /// reporting a status the caller would have to insert it for: starting the next
    /// attempt is not a task run status, so `Running` alone would lose the distinction
    /// from an attempt already in flight. An attempt none of these claims errs, unreachable
    /// while this covers every attempt status.
    async fn derive_next_status(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<TaskRunStatus> {

        // An unknown outranks every named outcome: the task run cannot claim an ending it
        // does not know. Never retried — flowlite has no idea what that attempt did.
        if last_task_run_attempt.status == TaskRunAttemptStatus::Invalid {
            return Ok(TaskRunStatus::Invalid);
        }

        if last_task_run_attempt.status == TaskRunAttemptStatus::Succeeded {
            return Ok(TaskRunStatus::Succeeded);
        }

        // The attempt services still own the attempt.
        if !last_task_run_attempt.status.is_finished() {
            return Ok(TaskRunStatus::Running);
        }

        if last_task_run_attempt.status == TaskRunAttemptStatus::Failed {
            // Attempts count from 1, so the task run gets max_retries + 1 of them.
            if last_task_run_attempt.attempt < task_run.max_retries + 1 {
                self.insert_next_attempt(task_run, last_task_run_attempt).await?;
                return Ok(TaskRunStatus::Running);
            }
            return Ok(TaskRunStatus::Failed);
        }

        if last_task_run_attempt.status == TaskRunAttemptStatus::TimedOut {
            return Ok(TaskRunStatus::TimedOut);
        }

        // Both stop outcomes land here: killed mid-flight, or skipped before the command
        // started. A skipped attempt does **not** make the task run Skipped — it had
        // started and may have left output, so Skipped would claim nothing ran.
        if last_task_run_attempt.status.is_stopped() {
            return Ok(TaskRunStatus::Aborted);
        }

        Err(anyhow::anyhow!(
            "Task run {}'s last attempt has status {:?}, which no outcome claims",
            task_run.id,
            last_task_run_attempt.status,
        ))
    }

    /// Starts the next attempt for a failed task run that still has retries left. Writes no
    /// task run status — it stays Running for the whole retry loop — and the row goes in
    /// immediately for `TaskRunAttemptDispatcher` to hold until `retry_delay` passes.
    async fn insert_next_attempt(
        &self,
        task_run: &TaskRun,
        last_task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<()> {

        self.crud.insert_task_run_attempt(
            &*self.conn_pool,
            &InsertTaskRunAttemptData {
                input: InsertTaskRunAttemptDataInput {
                    task_run_id: task_run.id,
                    job_run_id: task_run.job_run_id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    attempt: last_task_run_attempt.attempt + 1,
                    status: TaskRunAttemptStatus::Queued,
                },
            },
        ).await?;

        self.signals.publish();

        Ok(())
    }

    /// Settles the task run invalid: either its last attempt reported Invalid, or
    /// `derive_next_status` could not read it at all — unreachable while it covers every
    /// attempt status. See `JobRunDispatcher::settle_unclaimed` for why the latter settles
    /// rather than raises.
    async fn set_to_invalid(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.update_task_run_status(task_run, TaskRunStatus::Invalid).await
    }

    async fn set_to_succeeded(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.update_task_run_status(task_run, TaskRunStatus::Succeeded).await
    }

    /// Fails the task run once its attempts are used up.
    async fn set_to_failed(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.update_task_run_status(task_run, TaskRunStatus::Failed).await
    }

    async fn set_to_timed_out(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.update_task_run_status(task_run, TaskRunStatus::TimedOut).await
    }

    async fn set_to_aborted(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.update_task_run_status(task_run, TaskRunStatus::Aborted).await
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

    /// The attempt the ladder decides from: the task run's last, or a fresh attempt 1 when
    /// it has none.
    ///
    /// Every attempt a task run ever gets is made in this monitor, which is what keeps the
    /// unique index on (task_run_id, attempt) the concern of one file.
    async fn get_or_start_task_run_attempt(&self, task_run: &TaskRun) -> anyhow::Result<TaskRunAttempt> {

        if let Some(last_task_run_attempt) = self.select_last_task_run_attempt(task_run).await? {
            return Ok(last_task_run_attempt);
        }

        self.crud.insert_task_run_attempt(
            &*self.conn_pool,
            &InsertTaskRunAttemptData {
                input: InsertTaskRunAttemptDataInput {
                    task_run_id: task_run.id,
                    job_run_id: task_run.job_run_id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    attempt: 1,
                    status: TaskRunAttemptStatus::Queued,
                },
            },
        ).await?;

        self.signals.publish();

        // Read back rather than built here: `created_at` is what a retry_delay is measured
        // from, so the ladder must decide from the row as stored.
        self.select_last_task_run_attempt(task_run).await?
            .ok_or_else(|| anyhow::anyhow!(
                "Task run {} still has no attempt after one was inserted for it",
                task_run.id,
            ))
    }

    async fn select_last_task_run_attempt(&self, task_run: &TaskRun) -> anyhow::Result<Option<TaskRunAttempt>> {

        let task_run_attempts = self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: Some(task_run.id),
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Attempt),
            }
        ).await?;

        Ok(task_run_attempts.into_iter().last())
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
    use crate::crud::job_run::JobRunStatus;
    use crate::test_support::TestDb;

    /// Runs the monitor over a running task run whose last attempt has the given status,
    /// and reports what it settled the task run as, with the attempts it left behind.
    async fn settle(
        max_retries: u32,
        attempt: u32,
        attempt_status: TaskRunAttemptStatus,
    ) -> (TaskRunStatus, Vec<u32>) {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, max_retries, 60).await;

        db.insert_task_run_attempt(&task_run, attempt, attempt_status).await;

        db.task_run_monitor().handle(&task_run).await.unwrap();

        let attempts = db.task_run_attempts(task_run.id).await
            .iter()
            .map(|task_run_attempt| task_run_attempt.attempt)
            .collect();

        (db.task_run(task_run.id).await.status, attempts)
    }

    #[tokio::test]
    async fn a_succeeded_attempt_succeeds_the_task_run() {
        let (status, attempts) = settle(2, 1, TaskRunAttemptStatus::Succeeded).await;

        assert_eq!(status, TaskRunStatus::Succeeded);
        assert_eq!(attempts, vec![1]);
    }

    #[tokio::test]
    async fn an_unfinished_attempt_is_left_to_the_attempt_services() {
        let (status, attempts) = settle(2, 1, TaskRunAttemptStatus::Running).await;

        assert_eq!(status, TaskRunStatus::Running);
        assert_eq!(attempts, vec![1]);
    }

    /// Attempts count from 1, so `max_retries: 2` is three attempts in all.
    #[tokio::test]
    async fn a_failed_attempt_with_retries_left_starts_the_next_one() {
        let (status, attempts) = settle(2, 1, TaskRunAttemptStatus::Failed).await;

        assert_eq!(status, TaskRunStatus::Running);
        assert_eq!(attempts, vec![1, 2]);
    }

    #[tokio::test]
    async fn a_failed_last_attempt_fails_the_task_run() {
        let (status, attempts) = settle(2, 3, TaskRunAttemptStatus::Failed).await;

        assert_eq!(status, TaskRunStatus::Failed);
        assert_eq!(attempts, vec![3]);
    }

    #[tokio::test]
    async fn a_task_run_with_no_retries_fails_on_its_first_attempt() {
        let (status, attempts) = settle(0, 1, TaskRunAttemptStatus::Failed).await;

        assert_eq!(status, TaskRunStatus::Failed);
        assert_eq!(attempts, vec![1]);
    }

    /// A timeout is not retried: only a Failed attempt reaches `settle_for_running`.
    #[tokio::test]
    async fn a_timed_out_attempt_times_out_the_task_run() {
        let (status, attempts) = settle(2, 1, TaskRunAttemptStatus::TimedOut).await;

        assert_eq!(status, TaskRunStatus::TimedOut);
        assert_eq!(attempts, vec![1]);
    }

    #[tokio::test]
    async fn an_aborted_attempt_aborts_the_task_run() {
        let (status, _attempts) = settle(2, 1, TaskRunAttemptStatus::Aborted).await;

        assert_eq!(status, TaskRunStatus::Aborted);
    }

    /// A skipped attempt aborts the task run rather than skipping it: the task run had
    /// started and may already have left output, so Skipped would claim nothing ran.
    #[tokio::test]
    async fn a_skipped_attempt_aborts_the_task_run_rather_than_skipping_it() {
        let (status, _attempts) = settle(2, 1, TaskRunAttemptStatus::Skipped).await;

        assert_eq!(status, TaskRunStatus::Aborted);
    }

    /// A Running task run with no attempt is one the dispatcher has just started, which is
    /// an ordinary first pass rather than a broken invariant.
    #[tokio::test]
    async fn a_running_task_run_with_no_attempt_gets_its_first_one() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, 0, 60).await;

        db.task_run_monitor().handle(&task_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Running);

        let attempts = db.task_run_attempts(task_run.id).await;

        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].attempt, 1);
        assert_eq!(attempts[0].status, TaskRunAttemptStatus::Queued);
    }

    /// And only one: a second pass reads the attempt it made rather than colliding with
    /// the unique index.
    #[tokio::test]
    async fn a_second_pass_does_not_insert_a_second_first_attempt() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, 0, 60).await;

        db.task_run_monitor().handle(&task_run).await.unwrap();
        db.task_run_monitor().handle(&task_run).await.unwrap();

        assert_eq!(db.task_run_attempts(task_run.id).await.len(), 1);
    }

    #[tokio::test]
    async fn an_invalid_attempt_makes_the_task_run_invalid() {
        let (status, _) = settle(0, 1, TaskRunAttemptStatus::Invalid).await;

        assert_eq!(status, TaskRunStatus::Invalid);
    }

    /// Never retried, even with retries to spare: flowlite does not know what that attempt
    /// did, and after a crash its command may still be running.
    #[tokio::test]
    async fn an_invalid_attempt_is_not_retried_even_with_retries_left() {
        let (status, attempts) = settle(2, 1, TaskRunAttemptStatus::Invalid).await;

        assert_eq!(status, TaskRunStatus::Invalid);
        assert_eq!(attempts, vec![1]);
    }

    /// Unreachable while the ladder claims every attempt status, so it is called directly.
    #[tokio::test]
    async fn an_unclaimed_task_run_is_settled_invalid() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        db.task_run_monitor().set_to_invalid(&task_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Invalid);
    }
}
