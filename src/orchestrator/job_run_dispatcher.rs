use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use chrono::Utc;
use tokio::time::interval;


/// Picks up pending job runs and either cancels them or sets them to running.
/// Hands off to JobRunMonitor through the job run status only, never by calling it.
pub struct JobRunDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
}


impl JobRunDispatcher {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
        }
    }

    /// Spawns the polling loop and returns immediately, restarting it on error.
    pub fn start(self: &Self) {

        let crud = self.crud.clone();
        let conn_pool = self.conn_pool.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::run(crud.clone(), conn_pool.clone()).await {
                    eprintln!("Job Run Dispatcher error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every pending job run, once per second, until selecting them fails.
    async fn run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {

            timer.tick().await;

            let job_runs = Self::get_pending_job_runs(crud.clone(), conn_pool.clone()).await?;

            // A row the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other row from being
            // handled, since the restarted loop would select the same row again.
            for job_run in &job_runs {
                if let Err(e) = Self::handle_pending_job_run(
                    crud.clone(),
                    conn_pool.clone(),
                    job_run,
                ).await {
                    eprintln!("Job Run Dispatcher error on job run {}: {e:?}", job_run.id);
                }
            }

        }


    }

    /// Skips the job run if it was stopped, otherwise starts it.
    async fn handle_pending_job_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        job_run: &JobRun,
    ) -> anyhow::Result<()> {

        let status = Self::derive_next_job_run_status(
            crud.clone(),
            conn_pool.clone(),
            job_run,
        ).await?;

        if status == JobRunStatus::Skipped {
            return Self::handle_stopped_job_run(crud.clone(), conn_pool.clone(), job_run).await;
        }

        Self::handle_start_job_run(crud.clone(), conn_pool.clone(), job_run).await
    }

    /// Derives the status a pending job run moves to: skipped if it was stopped
    /// before it could run, running otherwise. There is nothing else to wait for.
    async fn derive_next_job_run_status(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        job_run: &JobRun,
    ) -> anyhow::Result<JobRunStatus> {

        let job_run_stopped = Self::is_job_run_stopped(
            crud.clone(),
            conn_pool.clone(),
            job_run,
        ).await?;

        if job_run_stopped {
            return Ok(JobRunStatus::Skipped);
        }

        Ok(JobRunStatus::Running)
    }

    /// Sets the job run to running, which is what makes JobRunMonitor pick it up.
    async fn handle_start_job_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        job_run: &JobRun,
    ) -> anyhow::Result<()> {

        crud.update_job_runs(
            &*conn_pool,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Running),
                    started_at: Some(Some(Utc::now())),
                    finished_at: None,
                },
            }
        ).await?;

        Ok(())
    }

    /// Skips the job run and all of its task runs, none of which ever started.
    async fn handle_stopped_job_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        job_run: &JobRun,
    ) -> anyhow::Result<()> {

        crud.update_job_runs(
            &*conn_pool,
            &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run.id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                },
            }
        ).await?;

        crud.update_task_runs(
            &*conn_pool,
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

        Ok(())
    }

    async fn get_pending_job_runs(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<Vec<JobRun>> {

        crud.select_job_runs(
            &*conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Pending)
                },
                sort: None,
                limit: None,
                offset: None,
            }
        ).await

    }

    async fn is_job_run_stopped(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        job_run: &JobRun,
    ) -> anyhow::Result<bool> {

        let job_run_stop = crud.select_job_run_stop(
            &*conn_pool,
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
