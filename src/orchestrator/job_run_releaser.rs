use std::sync::Arc;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::poller::Service;
use crate::signals::Signals;


/// Moves a job run from Submitted to Queued once its time has come, skipping it instead if
/// it was stopped first. The only place that writes that transition, so "not due yet" is a
/// fact readable off the table rather than inferred from control flow.
///
/// The one service that depends on the poll interval: nothing signals when a future instant
/// arrives, so the timer is what notices.
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

    /// Settles a submitted run as whatever `derive_next_status` decides. Decision and write
    /// are split on purpose: deriving only reads, and each `set_to_*` just trusts the result
    /// rather than re-deriving any part of it.
    async fn handle_submitted_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        match self.derive_next_status(job_run).await {
            Ok(JobRunStatus::Skipped) => self.set_to_skipped(job_run).await,
            Ok(JobRunStatus::Queued) => self.set_to_queued(job_run).await,
            Ok(JobRunStatus::Submitted) => self.set_to_submitted(),
            Ok(_) | Err(_) => self.set_to_invalid(job_run).await,
        }
    }

    /// Derives a submitted run's next status without writing anything: skipped if stopped,
    /// queued if due, else left submitted.
    async fn derive_next_status(&self, job_run: &JobRun) -> anyhow::Result<JobRunStatus> {

        let is_stopped = self.is_job_run_stopped(job_run).await?;
        let is_due = job_run.scheduled_at <= self.crud.toolkit.get_current_ts();

        Ok(match (is_stopped, is_due) {
            (true, _) => JobRunStatus::Skipped,
            (false, true) => JobRunStatus::Queued,
            (false, false) => JobRunStatus::Submitted,
        })
    }

    /// Skips the run, and with it every task run it owns — a stop must not wait for the
    /// run's time to take effect, and a run that will never run must not pass through
    /// Queued on its way to Skipped.
    ///
    /// JobRunDispatcher writes the same pair for a Queued run, through this same call. They
    /// select on disjoint statuses, so the two can't collide.
    async fn set_to_skipped(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let mut conn = self.conn_pool.acquire().await?;

        self.crud.skip_job_run(&mut conn, job_run.id).await?;

        self.signals.publish();

        Ok(())
    }

    /// Queues the run, which is what makes JobRunDispatcher pick it up. `started_at` stays
    /// untouched — nothing has started, and the dispatcher sets it when something does.
    async fn set_to_queued(&self, job_run: &JobRun) -> anyhow::Result<()> {

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

    /// Leaves the run submitted, writing nothing.
    fn set_to_submitted(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Settles a run `derive_next_status` failed to decide, invalidating its task runs too.
    /// Unreachable — `is_stopped`/`is_due` cover every case above. See
    /// `JobRunMonitor::settle_unclaimed` for why it settles rather than raises.
    async fn set_to_invalid(&self, job_run: &JobRun) -> anyhow::Result<()> {

        eprintln!(
            "Job run {} was submitted but its next status could not be derived, or was \
             derived as something the submitted-run dispatch does not handle. Settling it \
             invalid. This is a bug.",
            job_run.id,
        );

        let mut conn = self.conn_pool.acquire().await?;

        self.crud.invalidate_job_run(&mut conn, job_run.id).await?;

        self.signals.publish();

        Ok(())
    }

    /// Every submitted run, not only the due ones — a stopped run whose time has not come
    /// still needs settling.
    async fn get_submitted_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Submitted),
                    statuses: None,
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
    use chrono::{DateTime, Utc};
    use crate::crud::task_run::TaskRunStatus;
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

    /// A stop must not have to wait until the run's time to take effect.
    #[tokio::test]
    async fn a_stopped_run_that_is_not_due_yet_is_skipped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_at(
            JobRunStatus::Submitted,
            Utc::now() + chrono::TimeDelta::days(7),
            None,
        ).await;

        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        db.insert_job_run_stop(job_run.id).await;

        db.job_run_releaser().handle(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Skipped);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Skipped);
    }

    /// `is_stopped` outranks `is_due`, so a run stopped the moment it comes due is skipped
    /// rather than released.
    #[tokio::test]
    async fn a_stopped_run_that_is_due_is_skipped_rather_than_released() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_at(
            JobRunStatus::Submitted,
            Utc::now() - chrono::TimeDelta::minutes(1),
            None,
        ).await;

        db.insert_job_run_stop(job_run.id).await;

        db.job_run_releaser().handle(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Skipped);
    }

    /// A run already released must not come back round, or the releaser would fight the
    /// dispatcher for it.
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

    /// Unreachable through `handle`, so called directly. See `JobRunMonitor`'s equivalent
    /// for why it is settled at all.
    #[tokio::test]
    async fn an_unclaimed_submitted_job_run_is_settled_invalid() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_at(JobRunStatus::Submitted, Utc::now(), None).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        db.job_run_releaser().set_to_invalid(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Invalid);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Invalid);
    }
}
