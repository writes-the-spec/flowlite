use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Watches running job runs and finishes them once all their task runs are done.
/// Runs independently of JobRunDispatcher, picking up whatever it set to running.
pub struct JobRunMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl JobRunMonitor {

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

    /// Finishes the job run once its task runs say it is done.
    ///
    /// **The order of these transitions is the job run's status precedence, and changing
    /// it changes what a mixed set of task runs reports.** A task run that is still going
    /// holds every one of them off; after that the worst outcome wins, so that a job run
    /// with one aborted and one failed task run reports the abort rather than the failure.
    ///
    /// None of these transitions writes Skipped. A Running job run has started, so a stop
    /// aborts it; only JobRunDispatcher skips a job run, and only one that never started.
    async fn handle_running_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let task_runs = self.get_task_runs(job_run).await?;

        if Self::has_unfinished_task_run(&task_runs) {
            return Ok(());
        }

        if self.transition_to_aborted(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.transition_to_timed_out(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.transition_to_failed(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.transition_to_aborted_after_stop(job_run, &task_runs).await? {
            return Ok(());
        }

        self.transition_to_succeeded(job_run).await?;

        Ok(())
    }

    /// Aborts the job run if any of its task runs was killed mid-flight. Returns whether
    /// it transitioned. This outranks every failure; `transition_to_aborted_after_stop`
    /// writes the same status from the other stop outcome, but below them.
    async fn transition_to_aborted(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        if !Self::has_task_run_with_status(task_runs, TaskRunStatus::Aborted) {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Aborted).await?;

        Ok(true)
    }

    /// Times the job run out if any of its task runs ran past its timeout with no retry
    /// left. Returns whether it transitioned.
    async fn transition_to_timed_out(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        if !Self::has_task_run_with_status(task_runs, TaskRunStatus::TimedOut) {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::TimedOut).await?;

        Ok(true)
    }

    /// Fails the job run if any of its task runs failed with no retry left. Returns
    /// whether it transitioned.
    async fn transition_to_failed(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        if !Self::has_task_run_with_status(task_runs, TaskRunStatus::Failed) {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Failed).await?;

        Ok(true)
    }

    /// Aborts the job run whose task runs were skipped out from under it by a stop.
    /// Returns whether it transitioned.
    ///
    /// This ranks below the three failure transitions for a reason: a skipped task run
    /// means either a stop or a dependency that did not succeed, and in the second case
    /// that dependency is itself failed, timed out or aborted — so by the time this is
    /// asked, nothing has failed, and a skip can only mean the run was stopped. Keeping it
    /// here rather than folding it into `transition_to_aborted` is what makes a real
    /// failure outrank a stop.
    ///
    /// It writes Aborted, not Skipped: this monitor only ever sees Running job runs, so
    /// the job run had already started — its earlier task runs may well have executed —
    /// and Skipped would claim nothing ever ran. A job run stopped before it started is
    /// Skipped by JobRunDispatcher instead.
    async fn transition_to_aborted_after_stop(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        if !Self::has_task_run_with_status(task_runs, TaskRunStatus::Skipped) {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Aborted).await?;

        Ok(true)
    }

    /// Succeeds the job run, which is what is left once no task run reports anything
    /// worse. Returns whether it transitioned; reaching here it always does, and a job
    /// run with no task runs at all reaches it immediately.
    async fn transition_to_succeeded(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        self.update_job_run_status(job_run, JobRunStatus::Succeeded).await?;

        Ok(true)
    }

    /// Whether a task run of the job run has yet to reach a terminal status, which is
    /// what keeps the job run Running however bad the statuses of the others already are.
    fn has_unfinished_task_run(task_runs: &[TaskRun]) -> bool {

        task_runs.iter().any(|task_run| matches!(
            task_run.status,
            TaskRunStatus::Pending | TaskRunStatus::Running,
        ))
    }

    fn has_task_run_with_status(task_runs: &[TaskRun], status: TaskRunStatus) -> bool {
        task_runs.iter().any(|task_run| task_run.status == status)
    }

    async fn get_task_runs(&self, job_run: &JobRun) -> anyhow::Result<Vec<TaskRun>> {

        self.crud.select_task_runs(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run.id),
                    task_id: None,
                    job_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await

    }

    async fn get_running_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Running)
                },
                sort: None,
                limit: None,
                offset: None,
            }
        ).await

    }

    async fn update_job_run_status(&self, job_run: &JobRun, status: JobRunStatus) -> anyhow::Result<()> {
        self.crud
            .update_job_runs(
                &*self.conn_pool,
                &UpdateJobRunsData {
                    filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                    input: UpdateJobRunsDataInput {
                        status: Some(status),
                        started_at: None,
                        finished_at: Some(Some(Utc::now())),
                    },
                },
            )
            .await?;

        self.signals.publish();

        Ok(())
    }

}


impl Service for JobRunMonitor {
    type Row = JobRun;

    fn name(&self) -> &'static str {
        "Job Run Monitor"
    }

    fn row_context(&self, job_run: &JobRun) -> String {
        format!("job run {}", job_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<JobRun>> {
        self.get_running_job_runs().await
    }

    async fn handle(&self, job_run: &JobRun) -> anyhow::Result<()> {
        self.handle_running_job_run(job_run).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run::TaskRunStatus;

    fn task_run(status: TaskRunStatus) -> TaskRun {
        TaskRun {
            id: 1,
            job_run_id: 1,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            command: "true".to_string(),
            depends_on: sqlx::types::Json(Vec::new()),
            timeout: 3600,
            max_retries: 0,
            retry_delay: 60,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            status,
        }
    }

    #[test]
    fn a_running_task_run_keeps_the_job_run_running() {
        let task_runs = vec![
            task_run(TaskRunStatus::Succeeded),
            task_run(TaskRunStatus::Running),
        ];

        assert!(JobRunMonitor::has_unfinished_task_run(&task_runs));
    }

    #[test]
    fn a_pending_task_run_keeps_the_job_run_running() {
        let task_runs = vec![
            task_run(TaskRunStatus::Failed),
            task_run(TaskRunStatus::Pending),
        ];

        assert!(JobRunMonitor::has_unfinished_task_run(&task_runs));
    }

    #[test]
    fn task_runs_that_all_finished_hold_nothing_off() {
        let task_runs = vec![
            task_run(TaskRunStatus::Succeeded),
            task_run(TaskRunStatus::Skipped),
        ];

        assert!(!JobRunMonitor::has_unfinished_task_run(&task_runs));
    }

    #[test]
    fn a_job_run_with_no_task_runs_holds_nothing_off() {
        assert!(!JobRunMonitor::has_unfinished_task_run(&[]));
    }

    /// The four failure guards are asked in the order Aborted, TimedOut, Failed, Skipped
    /// by handle_running_job_run, so a set matching more than one of them reports the
    /// first. These assert the guards themselves match; the order they are asked in lives
    /// in handle_running_job_run.
    #[test]
    fn an_aborted_task_run_is_seen_beside_a_failed_one() {
        let task_runs = vec![
            task_run(TaskRunStatus::Failed),
            task_run(TaskRunStatus::Aborted),
        ];

        assert!(JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::Aborted));
        assert!(JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::Failed));
    }

    #[test]
    fn a_timed_out_task_run_is_seen_beside_a_failed_one() {
        let task_runs = vec![
            task_run(TaskRunStatus::Failed),
            task_run(TaskRunStatus::TimedOut),
        ];

        assert!(JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::TimedOut));
        assert!(JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::Failed));
    }

    #[test]
    fn a_set_with_no_failure_matches_no_failure_guard() {
        let task_runs = vec![
            task_run(TaskRunStatus::Succeeded),
            task_run(TaskRunStatus::Skipped),
        ];

        assert!(!JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::Aborted));
        assert!(!JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::TimedOut));
        assert!(!JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::Failed));
        assert!(JobRunMonitor::has_task_run_with_status(&task_runs, TaskRunStatus::Skipped));
    }

    #[test]
    fn task_runs_that_all_succeeded_match_no_guard_at_all() {
        let task_runs = vec![
            task_run(TaskRunStatus::Succeeded),
            task_run(TaskRunStatus::Succeeded),
        ];

        for status in [
            TaskRunStatus::Aborted,
            TaskRunStatus::TimedOut,
            TaskRunStatus::Failed,
            TaskRunStatus::Skipped,
        ] {
            assert!(!JobRunMonitor::has_task_run_with_status(&task_runs, status));
        }
    }
}
