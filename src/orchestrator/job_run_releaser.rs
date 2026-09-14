use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::poller::Service;
use crate::signals::Signals;


/// Moves a job run from Submitted to Queued once its time has come. That one update is
/// the only thing this service writes, and it is the only place in the program that writes
/// it — which is what makes "a run is not due yet" a fact you can read off the table
/// rather than infer from a service's control flow.
///
/// It is the one service that genuinely depends on the poll interval. Nothing publishes a
/// signal when a future instant arrives, so the timer is what notices.
pub struct JobRunReleaser {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl JobRunReleaser {

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

    /// A submitted run is released when its instant has arrived, or when somebody has
    /// stopped it — a stop must not have to wait for a run's time to take effect, and
    /// JobRunDispatcher is what turns it into a skip, along with the run's task runs.
    async fn handle_submitted_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let due = job_run.scheduled_at <= self.crud.toolkit.get_current_ts();

        if !due && !self.is_job_run_stopped(job_run).await? {
            return Ok(());
        }

        self.release(job_run).await
    }

    /// Sets the run queued, which is what makes JobRunDispatcher pick it up. `started_at`
    /// is deliberately untouched: nothing has started, and the dispatcher sets it when
    /// something does.
    async fn release(&self, job_run: &JobRun) -> anyhow::Result<()> {

        self.crud.update_job_runs(
            &*self.conn_pool,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Queued),
                    started_at: None,
                    finished_at: None,
                },
            }
        ).await?;

        self.signals.publish();

        Ok(())
    }

    /// Every submitted run, not only the due ones: `handle` needs to see a stopped run
    /// whose time has not come, and that is not a question the due filter can ask.
    async fn get_submitted_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Submitted),
                    statuses: None,
                    scheduled_at_lte: None,
                    schedule_id: None,
                },
                sort: Some(SelectJobRunsDataSort::Id),
                limit: None,
                offset: None,
            }
        ).await
    }

    async fn is_job_run_stopped(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        let job_run_stop = self.crud.select_job_run_stop(
            &*self.conn_pool,
            &SelectJobRunStopsData {
                filter: SelectJobRunStopsDataFilter {
                    id: None,
                    job_run_id: Some(job_run.id),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        ).await?;

        Ok(job_run_stop.is_some())
    }

}


impl Service for JobRunReleaser {
    type Row = JobRun;

    fn name(&self) -> &'static str {
        "Job Run Releaser"
    }

    fn row_context(&self, job_run: &JobRun) -> String {
        format!("job run {}", job_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<JobRun>> {
        self.get_submitted_job_runs().await
    }

    async fn handle(&self, job_run: &JobRun) -> anyhow::Result<()> {
        self.handle_submitted_job_run(job_run).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDb;

    async fn released_status(scheduled_at: DateTime<Utc>) -> JobRunStatus {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_at(JobRunStatus::Submitted, scheduled_at, None).await;

        db.job_run_releaser().handle(&job_run).await.unwrap();

        db.job_run(job_run.id).await.status
    }

    #[tokio::test]
    async fn a_run_whose_time_has_come_is_released() {
        let status = released_status(Utc::now() - chrono::TimeDelta::minutes(1)).await;

        assert_eq!(status, JobRunStatus::Queued);
    }

    #[tokio::test]
    async fn a_run_that_is_not_due_is_left_alone() {
        let status = released_status(Utc::now() + chrono::TimeDelta::hours(1)).await;

        assert_eq!(status, JobRunStatus::Submitted);
    }

    /// Stopping a run that has not come due yet is a thing a person can do, and it must
    /// not have to wait until the run's time to take effect. The releaser hands it to
    /// JobRunDispatcher, which owns skipping and also skips the run's task runs.
    #[tokio::test]
    async fn a_stopped_run_is_released_early_so_it_can_be_skipped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_at(
            JobRunStatus::Submitted,
            Utc::now() + chrono::TimeDelta::days(7),
            None,
        ).await;

        db.insert_job_run_stop(job_run.id).await;

        db.job_run_releaser().handle(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Queued);
    }

    /// The select is the other half of the contract: a run already released must not come
    /// back round, or the releaser would fight the dispatcher for it.
    #[tokio::test]
    async fn only_submitted_runs_are_selected() {

        let db = TestDb::new().await;

        let submitted = db.insert_job_run_at(JobRunStatus::Submitted, Utc::now(), None).await;
        let _pending = db.insert_job_run(JobRunStatus::Queued).await;
        let _running = db.insert_job_run(JobRunStatus::Running).await;

        let selected = db.job_run_releaser().select().await.unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, submitted.id);
    }
}
