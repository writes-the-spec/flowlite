use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Picks up pending job runs, oldest first, and settles each one as skipped, still
/// pending or running. Hands off to JobRunMonitor through the job run status only.
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

    /// Settles a pending job run as exactly one outcome. Falling past all three bails
    /// rather than returning quietly: a row nobody handled looks exactly like one
    /// legitimately queued, so silence is the one failure this service cannot spot.
    ///
    /// `settle_as_pending` has to precede `settle_as_running`, which starts the run
    /// unconditionally and would take a slot the job does not have.
    async fn handle_pending_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        if self.settle_as_skipped(job_run).await? {
            return Ok(());
        }

        if self.settle_as_pending(job_run).await? {
            return Ok(());
        }

        if self.settle_as_running(job_run).await? {
            return Ok(());
        }

        self.settle_unclaimed(job_run).await
    }

    /// Unreachable while `settle_as_running` claims unconditionally: this is the day a
    /// status is added and a rung is not.
    ///
    /// Settled rather than raised on — unlike the raises kept for states an invariant makes
    /// impossible, this one would fire every pass and stop the job scheduling for ever.
    async fn settle_unclaimed(&self, job_run: &JobRun) -> anyhow::Result<()> {

        eprintln!(
            "Job run {} was claimed by no outcome: it was not stopped, is not waiting on \
             max_parallel_runs, and was not started. Settling it invalid. This is a bug.",
            job_run.id,
        );

        self.crud.update_job_runs(
            &*self.conn_pool,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Invalid),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                },
            }
        ).await?;

        self.signals.publish();

        Ok(())
    }

    /// Skips the job run, and with it all of its task runs, none of which ever started.
    async fn settle_as_skipped(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        let job_run_stopped = self.is_job_run_stopped(job_run).await?;

        if !job_run_stopped {
            return Ok(false);
        }

        self.crud.update_job_runs(
            &*self.conn_pool,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                },
            }
        ).await?;

        self.crud.update_task_runs(
            &*self.conn_pool,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run.id),
                    status: None,
                },
                input: UpdateTaskRunsDataInput {
                    status: Some(TaskRunStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                },
            }
        ).await?;

        self.signals.publish();

        Ok(true)
    }

    /// Leaves the job run pending, writing nothing, while its job is at max_parallel_runs.
    ///
    /// The only place max_parallel_runs is enforced: submitting never rejects a job, so
    /// every path that creates a job run queues behind this gate without knowing about it.
    async fn settle_as_pending(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        self.is_job_at_max_parallel_runs(job_run).await
    }

    /// Sets the job run to running, which is what makes JobRunMonitor pick it up.
    async fn settle_as_running(&self, job_run: &JobRun) -> anyhow::Result<bool> {

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

        Ok(true)
    }

    async fn get_pending_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Pending)
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
        self.get_pending_job_runs().await
    }

    async fn handle(&self, job_run: &JobRun) -> anyhow::Result<()> {
        self.handle_pending_job_run(job_run).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDb;

    /// Unreachable while every rung of the chain is complete - `settle_as_running` claims
    /// unconditionally - so it is called directly. It exists for the day a status is added
    /// and a rung is not, which is exactly what happened to the monitors while `Invalid`
    /// was being wired up.
    #[tokio::test]
    async fn an_unclaimed_job_run_is_settled_invalid() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Pending).await;

        db.job_run_dispatcher().settle_unclaimed(&job_run).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Invalid);
    }
}
