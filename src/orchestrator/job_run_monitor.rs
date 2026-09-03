use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus};
use chrono::Utc;
use tokio::time::interval;


/// Watches running job runs and finishes them once all their task runs are done.
/// Runs independently of JobRunDispatcher, picking up whatever it set to running.
pub struct JobRunMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
}


impl JobRunMonitor {

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
                    eprintln!("Job Run Monitor error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every running job run, once per second, until selecting them fails.
    async fn run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {

            timer.tick().await;

            let job_runs = Self::get_running_job_runs(crud.clone(), conn_pool.clone()).await?;

            // A row the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other row from being
            // handled, since the restarted loop would select the same row again.
            for job_run in &job_runs {
                if let Err(e) = Self::handle_running_job_run(
                    crud.clone(),
                    conn_pool.clone(),
                    job_run,
                ).await {
                    eprintln!("Job Run Monitor error on job run {}: {e:?}", job_run.id);
                }
            }

        }

    }

    /// Finishes the job run once its task runs say it is done.
    async fn handle_running_job_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        job_run: &JobRun,
    ) -> anyhow::Result<()> {

        let task_runs = crud.select_task_runs(
            &*conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
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

        Self::handle_job_run_finish(
            job_run,
            crud.clone(),
            conn_pool.clone(),
            status,
        ).await
    }

    /// Writes the finished status of the job run.
    async fn handle_job_run_finish(
        job_run: &JobRun,
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        status: JobRunStatus,
    ) -> anyhow::Result<()> {

        Self::update_job_run_status(
            job_run,
            crud.clone(),
            conn_pool.clone(),
            status,
        ).await
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

    async fn get_running_job_runs(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<Vec<JobRun>> {

        crud.select_job_runs(
            &*conn_pool,
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

    async fn update_job_run_status(
        job_run: &JobRun,
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        status: JobRunStatus,
    ) -> anyhow::Result<()> {
        crud
            .update_job_runs(
                &*conn_pool,
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

        Ok(())
    }

}
