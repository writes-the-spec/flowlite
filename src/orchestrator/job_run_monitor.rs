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
    async fn handle_running_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let task_runs = self.crud.select_task_runs(
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
        ).await?;

        let Some(status) = Self::derive_next_job_run_status(&task_runs) else {
            return Ok(());
        };

        self.handle_job_run_finish(job_run, status).await
    }

    /// Writes the finished status of the job run.
    async fn handle_job_run_finish(&self, job_run: &JobRun, status: JobRunStatus) -> anyhow::Result<()> {

        self.update_job_run_status(job_run, status).await
    }

    /// Derives the status a running job run moves to from its task runs, the first
    /// matching rule winning, or None while it has to keep running.
    fn derive_next_job_run_status(
        task_runs: &[TaskRun],
    ) -> Option<JobRunStatus> {

        let has_status = |status: TaskRunStatus| task_runs.iter().any(|tr| tr.status == status);

        if has_status(TaskRunStatus::Pending) || has_status(TaskRunStatus::Running) {
            return None;
        }

        if has_status(TaskRunStatus::Aborted) {
            return Some(JobRunStatus::Aborted);
        }

        if has_status(TaskRunStatus::TimedOut) {
            return Some(JobRunStatus::TimedOut);
        }

        if has_status(TaskRunStatus::Failed) {
            return Some(JobRunStatus::Failed);
        }

        // Nothing failed, so a skipped task run can only come from a stop.
        if has_status(TaskRunStatus::Skipped) {
            return Some(JobRunStatus::Skipped);
        }

        Some(JobRunStatus::Succeeded)
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
    fn an_unfinished_task_run_keeps_the_job_run_running() {
        let task_runs = vec![
            task_run(TaskRunStatus::Succeeded),
            task_run(TaskRunStatus::Running),
        ];

        assert_eq!(JobRunMonitor::derive_next_job_run_status(&task_runs), None);
    }

    #[test]
    fn every_task_run_succeeding_succeeds_the_job_run() {
        let task_runs = vec![task_run(TaskRunStatus::Succeeded)];

        assert_eq!(
            JobRunMonitor::derive_next_job_run_status(&task_runs),
            Some(JobRunStatus::Succeeded),
        );
    }

    #[test]
    fn an_aborted_task_run_outranks_a_failed_one() {
        let task_runs = vec![
            task_run(TaskRunStatus::Failed),
            task_run(TaskRunStatus::Aborted),
        ];

        assert_eq!(
            JobRunMonitor::derive_next_job_run_status(&task_runs),
            Some(JobRunStatus::Aborted),
        );
    }

    #[test]
    fn a_skipped_task_run_with_no_failure_skips_the_job_run() {
        let task_runs = vec![
            task_run(TaskRunStatus::Succeeded),
            task_run(TaskRunStatus::Skipped),
        ];

        assert_eq!(
            JobRunMonitor::derive_next_job_run_status(&task_runs),
            Some(JobRunStatus::Skipped),
        );
    }

    #[test]
    fn a_timed_out_task_run_outranks_a_failed_one() {
        let task_runs = vec![
            task_run(TaskRunStatus::Failed),
            task_run(TaskRunStatus::TimedOut),
        ];

        assert_eq!(
            JobRunMonitor::derive_next_job_run_status(&task_runs),
            Some(JobRunStatus::TimedOut),
        );
    }

    #[test]
    fn an_aborted_task_run_outranks_a_timed_out_one() {
        let task_runs = vec![
            task_run(TaskRunStatus::TimedOut),
            task_run(TaskRunStatus::Aborted),
        ];

        assert_eq!(
            JobRunMonitor::derive_next_job_run_status(&task_runs),
            Some(JobRunStatus::Aborted),
        );
    }
}
