use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Picks up queued job runs, oldest first, and settles each one skipped, still queued or
/// running. Hands off to JobRunMonitor through the job run status only.
pub struct JobRunDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl JobRunDispatcher {

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
    async fn handle_queued_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        match self.derive_next_status(job_run).await {
            Ok(JobRunStatus::Skipped) => self.set_to_skipped(job_run).await,
            Ok(JobRunStatus::Queued) => Ok(()),
            Ok(JobRunStatus::Running) => self.set_to_running(job_run).await,
            Ok(_) | Err(_) => self.set_to_invalid(job_run).await,
        }
    }

    /// Derives a queued run's next status: skipped if stopped, running if a slot is free,
    /// else still queued behind max_parallel_runs.
    ///
    /// Stopped is checked first and returned on early rather than gathered into a tuple: it
    /// is the one of the two that is a read, and a stopped run's job is never asked about a
    /// slot it will not take.
    async fn derive_next_status(&self, job_run: &JobRun) -> anyhow::Result<JobRunStatus> {

        if self.is_job_run_stopped(job_run).await? {
            return Ok(JobRunStatus::Skipped);
        }

        if self.is_job_at_max_parallel_runs(job_run).await? {
            return Ok(JobRunStatus::Queued);
        }

        Ok(JobRunStatus::Running)
    }

    /// Settles a run `derive_next_status` failed to decide. Unreachable — its two checks
    /// cover every case. See `JobRunMonitor::settle_unclaimed` for why it settles rather
    /// than raises.
    async fn set_to_invalid(&self, job_run: &JobRun) -> anyhow::Result<()> {

        eprintln!(
            "Job run {} was queued but its next status could not be derived, or was \
             derived as something the queued-run dispatch does not handle. Settling it \
             invalid. This is a bug.",
            job_run.id,
        );

        let mut conn = self.conn_pool.acquire().await?;

        self.crud.invalidate_job_run(&mut conn, job_run.id).await?;

        self.signals.publish();

        Ok(())
    }

    /// Skips the job run and all of its task runs, none of which ever started.
    ///
    /// `JobRunReleaser::set_to_skipped` is the same outcome one status earlier, for a run
    /// stopped before it was released. Both go through `skip_job_run`, and select on
    /// disjoint statuses, so a run is only ever skipped by one of them.
    async fn set_to_skipped(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let mut conn = self.conn_pool.acquire().await?;

        self.crud.skip_job_run(&mut conn, job_run.id).await?;

        self.signals.publish();

        Ok(())
    }

    /// Sets the job run running, which is what makes JobRunMonitor pick it up, and releases
    /// its task runs to TaskRunDispatcher in the same pass — the only door into `Waiting`.
    ///
    /// The task runs go first: a crash between the two writes then leaves the job run still
    /// Queued, so the next pass settles it again, rather than Running with task runs stuck
    /// Planned for ever.
    async fn set_to_running(&self, job_run: &JobRun) -> anyhow::Result<()> {

        self.crud.update_task_runs(
            &*self.conn_pool,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run.id),
                    status: Some(TaskRunStatus::Planned),
                },
                input: UpdateTaskRunsDataInput {
                    status: Some(TaskRunStatus::Waiting),
                    started_at: None,
                    finished_at: None,
                },
            }
        ).await?;

        self.crud.update_job_runs(
            &*self.conn_pool,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Running),
                    started_at: Some(Some(Utc::now())),
                    finished_at: None,
                },
            }
        ).await?;

        self.signals.publish();

        Ok(())
    }

    async fn get_queued_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Queued),
                    statuses: None,
                    schedule_id: None,
                },
                sort: Some(SelectJobRunsDataSort::Id),
                limit: None,
                offset: None,
            }
        ).await

    }

    async fn is_job_at_max_parallel_runs(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        let mut conn = self.conn_pool.acquire().await?;

        self.crud.is_job_at_max_parallel_runs(&mut conn, &job_run.job_id).await

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


impl Service for JobRunDispatcher {
    type Row = JobRun;

    fn name(&self) -> &'static str {
        "Job Run Dispatcher"
    }

    fn row_context(&self, job_run: &JobRun) -> String {
        format!("job run {}", job_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<JobRun>> {
        self.get_queued_job_runs().await
    }

    async fn handle(&self, job_run: &JobRun) -> anyhow::Result<()> {
        self.handle_queued_job_run(job_run).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDb;

    /// Unreachable through `handle`, so called directly. See `JobRunMonitor`'s equivalent
    /// for why it is settled at all.
    #[tokio::test]
    async fn an_unclaimed_job_run_is_settled_invalid() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Queued).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        db.job_run_dispatcher().set_to_invalid(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Invalid);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Invalid);
    }

    /// The other half of the skip JobRunReleaser owns for a Submitted run: a Queued one
    /// that was stopped ends here, and its task runs end with it - left behind they would
    /// hold the run open for ever.
    #[tokio::test]
    async fn a_stopped_job_run_is_skipped_along_with_its_task_runs() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Queued).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        db.insert_job_run_stop(job_run.id).await;

        db.job_run_dispatcher().handle(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Skipped);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Skipped);
    }

    /// Starting the job run is what releases its task runs, and the only thing that does:
    /// left Planned they would never be dispatched, and the run would hold open for ever.
    #[tokio::test]
    async fn starting_a_job_run_releases_its_task_runs() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Queued).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        db.job_run_dispatcher().set_to_running(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Running);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Waiting);
    }

    /// A task run that already finished is not dragged back to Waiting by a later start,
    /// which is why the update filters on Planned rather than on the job run alone.
    #[tokio::test]
    async fn starting_a_job_run_leaves_a_finished_task_run_alone() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Queued).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Succeeded).await;

        db.job_run_dispatcher().set_to_running(&job_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Succeeded);
    }
}
