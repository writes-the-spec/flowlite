use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Picks up pending job runs, oldest first, and settles each one as skipped, running,
/// or still pending as their job's max_parallel_runs allows.
/// Hands off to JobRunMonitor through the job run status only, never by calling it.
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

    /// Settles a pending job run into exactly one outcome: skipped if it was stopped
    /// before it could run, running if its job is under its max_parallel_runs, and left
    /// pending if it is not.
    ///
    /// The three are meant to be exhaustive over the reasons a pending job run can have,
    /// and falling past all of them is treated as a bug rather than a quiet no-op. A guard
    /// tightened on one outcome without a matching one added here would otherwise leave
    /// the row pending forever, looking exactly like a job that is legitimately queued —
    /// the one failure this service cannot distinguish by watching it. `Poller::run` logs
    /// the error with the row id and moves on, so one such row cannot stop the loop.
    async fn handle_pending_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        if self.settle_as_skipped(job_run).await? {
            return Ok(());
        }

        if self.settle_as_running(job_run).await? {
            return Ok(());
        }

        if self.settle_as_pending(job_run).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Job run {} settled as nothing: it was not stopped, was not started, and is \
             not waiting on max_parallel_runs",
            job_run.id,
        )
    }

    /// Skips the job run, and with it all of its task runs, none of which ever started,
    /// if it was stopped before it could run. Returns whether this is what happened.
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

    /// Sets the job run to running, which is what makes JobRunMonitor pick it up, unless
    /// its job is already at its max_parallel_runs. Returns whether this is what happened;
    /// a job run held back here is left to `settle_as_pending`.
    ///
    /// This is the only place the limit is enforced. Submitting a job never rejects it,
    /// so every path that creates a job run — the CLI, the scheduler, a rerun — queues
    /// behind the same gate without having to know about it.
    async fn settle_as_running(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        let at_max_parallel_runs = self.is_job_at_max_parallel_runs(job_run).await?;

        if at_max_parallel_runs {
            return Ok(false);
        }

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

    /// Leaves the job run pending, writing nothing, because its job is already at its
    /// max_parallel_runs. Returns whether this is what happened.
    ///
    /// Having nothing to write is not the same as having nothing to say. This asks the
    /// question `settle_as_running` just asked and claims the rows it turned down, so that
    /// between them they account for every job run that was not stopped. That is what lets
    /// `handle_pending_job_run` treat falling past all three as an error: without this
    /// outcome, "left pending on purpose" and "left pending because nobody handled it"
    /// would be the same silence.
    ///
    /// The repeated question costs a second `is_job_at_max_parallel_runs` for a held row,
    /// which is a count over that job's running job runs. Passing the answer down from the
    /// caller instead would save it, at the price of the guard no longer living with the
    /// outcome it decides.
    async fn settle_as_pending(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        self.is_job_at_max_parallel_runs(job_run).await
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
