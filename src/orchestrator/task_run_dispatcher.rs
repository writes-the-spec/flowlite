use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use chrono::Utc;
use tokio::time::interval;


/// Picks up pending task runs and either skips them or sets them to running.
/// Hands off to TaskRunMonitor through the task run status only, never by calling it.
pub struct TaskRunDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
}


impl TaskRunDispatcher {

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
                    eprintln!("Task Run Dispatcher error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Handles every pending task run, once per second, until selecting them fails.
    async fn run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {

            timer.tick().await;

            let task_runs = Self::get_pending_task_runs(crud.clone(), conn_pool.clone()).await?;

            // A row the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other row from being
            // handled, since the restarted loop would select the same row again.
            for task_run in &task_runs {
                if let Err(e) = Self::handle_pending_task_run(
                    crud.clone(),
                    conn_pool.clone(),
                    task_run,
                ).await {
                    eprintln!("Task Run Dispatcher error on task run {}: {e:?}", task_run.id);
                }
            }

        }

    }

    /// Skips the task run if its job run was stopped or if a task run it depends on
    /// did not succeed, and starts it once all of them have succeeded.
    async fn handle_pending_task_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        let status = Self::derive_next_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        let Some(status) = status else {
            return Ok(());
        };

        if status == TaskRunStatus::Running {
            return Self::handle_start_task_run(crud.clone(), conn_pool.clone(), task_run).await;
        }

        Self::update_task_run_status(
            crud.clone(),
            conn_pool.clone(),
            task_run,
            status,
        ).await
    }

    /// Derives the status a pending task run moves to: skipped if its job run was
    /// stopped or a task run it depends on did not succeed, running once all of them
    /// have succeeded, and none while any of them is still on its way there.
    async fn derive_next_task_run_status(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<Option<TaskRunStatus>> {

        let job_run_stopped = Self::is_job_run_stopped(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        if job_run_stopped {
            return Ok(Some(TaskRunStatus::Skipped));
        }

        let any_dependent_task_run_failed = Self::did_any_dependent_task_run_finish_but_not_succeed(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        if any_dependent_task_run_failed {
            return Ok(Some(TaskRunStatus::Skipped));
        }

        let all_dependent_task_runs_succeeded = Self::have_all_dependent_task_runs_succeeded(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        if all_dependent_task_runs_succeeded {
            return Ok(Some(TaskRunStatus::Running));
        }

        Ok(None)
    }

    /// Sets the task run to running, which is what makes TaskRunMonitor pick it up.
    async fn handle_start_task_run(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<()> {

        crud.update_task_runs(
            &*conn_pool,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: Some(task_run.id),
                    job_run_id: None,
                    status: None,
                },
                input: UpdateTaskRunsDataInput {
                    status: Some(TaskRunStatus::Running),
                    started_at: Some(Some(Utc::now())),
                    finished_at: None,
                },
            }
        ).await?;

        Ok(())
    }

    async fn get_pending_task_runs(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<Vec<TaskRun>> {

        crud.select_task_runs(
            &*conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: Some(TaskRunStatus::Pending),
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await

    }

    async fn is_job_run_stopped(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<bool> {

        let job_run_stop = crud.select_job_run_stop(
            &*conn_pool,
            &SelectJobRunStopsData {
                filter: SelectJobRunStopsDataFilter {
                    id: None,
                    job_run_id: Some(task_run.job_run_id),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        ).await?;

        Ok(job_run_stop.is_some())

    }

    async fn did_any_dependent_task_run_finish_but_not_succeed(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<bool> {

        let dependent_task_runs = Self::get_dependent_task_runs(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        let any_failed = dependent_task_runs.iter().any(|tr| matches!(
            tr.status,
            TaskRunStatus::Failed
                | TaskRunStatus::Skipped
                | TaskRunStatus::Aborted
                | TaskRunStatus::TimedOut
        ));

        Ok(any_failed)

    }

    async fn have_all_dependent_task_runs_succeeded(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<bool> {

        let dependent_task_runs = Self::get_dependent_task_runs(
            crud.clone(),
            conn_pool.clone(),
            task_run,
        ).await?;

        let all_succeeded = dependent_task_runs.iter().all(|tr| tr.status == TaskRunStatus::Succeeded);

        Ok(all_succeeded)

    }

    /// Loads the task runs of the same job run that this task run depends on.
    async fn get_dependent_task_runs(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
    ) -> anyhow::Result<Vec<TaskRun>> {

        let task = crud.select_task(
            &*conn_pool,
            &SelectTasksData {
                filter: SelectTasksDataFilter {
                    job_id: Some(task_run.job_id.clone()),
                    task_id: Some(task_run.task_id.clone()),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        )
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task not found: {}", task_run.task_id))?;

        let mut dependent_task_runs = Vec::new();

        for dependent_task_id in task.depends_on.iter() {

            let dependent_task_run = crud.select_task_run(
                &*conn_pool,
                &SelectTaskRunsData {
                    filter: SelectTaskRunsDataFilter {
                        id: None,
                        job_run_id: Some(task_run.job_run_id),
                        job_id: Some(task_run.job_id.clone()),
                        task_id: Some(dependent_task_id.clone()),
                        status: None,
                    },
                    sort: None,
                }
            )
                .await?
                .ok_or_else(|| anyhow::anyhow!(
                    "Task run not found for task_id: {}, job_run_id: {}",
                    dependent_task_id,
                    task_run.job_run_id,
                ))?;

            dependent_task_runs.push(dependent_task_run);

        }

        Ok(dependent_task_runs)

    }

    async fn update_task_run_status(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        task_run: &TaskRun,
        status: TaskRunStatus,
    ) -> anyhow::Result<()> {

        crud.update_task_runs(
            &*conn_pool,
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

        Ok(())
    }

}
