use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Picks up pending task runs and either skips them or sets them to running.
/// Hands off to TaskRunMonitor through the task run status only, never by calling it.
pub struct TaskRunDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl TaskRunDispatcher {

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

    /// Skips the task run if its job run was stopped or if a task run it depends on
    /// did not succeed, and starts it once all of them have succeeded.
    async fn handle_pending_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        let status = self.derive_next_task_run_status(task_run).await?;

        let Some(status) = status else {
            return Ok(());
        };

        if status == TaskRunStatus::Running {
            return self.handle_start_task_run(task_run).await;
        }

        self.update_task_run_status(task_run, status).await
    }

    /// Derives the status a pending task run moves to: skipped if its job run was
    /// stopped or a task run it depends on did not succeed, running once all of them
    /// have succeeded, and none while any of them is still on its way there.
    async fn derive_next_task_run_status(&self, task_run: &TaskRun) -> anyhow::Result<Option<TaskRunStatus>> {

        let job_run_stopped = self.is_job_run_stopped(task_run).await?;

        if job_run_stopped {
            return Ok(Some(TaskRunStatus::Skipped));
        }

        let any_dependent_task_run_failed = self.did_any_dependent_task_run_finish_but_not_succeed(task_run).await?;

        if any_dependent_task_run_failed {
            return Ok(Some(TaskRunStatus::Skipped));
        }

        let all_dependent_task_runs_succeeded = self.have_all_dependent_task_runs_succeeded(task_run).await?;

        if all_dependent_task_runs_succeeded {
            return Ok(Some(TaskRunStatus::Running));
        }

        Ok(None)
    }

    /// Sets the task run to running, which is what makes TaskRunMonitor pick it up.
    async fn handle_start_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        self.crud.update_task_runs(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

    async fn get_pending_task_runs(&self) -> anyhow::Result<Vec<TaskRun>> {

        self.crud.select_task_runs(
            &*self.conn_pool,
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

    async fn is_job_run_stopped(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let job_run_stop = self.crud.select_job_run_stop(
            &*self.conn_pool,
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

    async fn did_any_dependent_task_run_finish_but_not_succeed(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        let any_failed = dependent_task_runs.iter().any(|tr| matches!(
            tr.status,
            TaskRunStatus::Failed
                | TaskRunStatus::Skipped
                | TaskRunStatus::Aborted
                | TaskRunStatus::TimedOut
        ));

        Ok(any_failed)

    }

    async fn have_all_dependent_task_runs_succeeded(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        let all_succeeded = dependent_task_runs.iter().all(|tr| tr.status == TaskRunStatus::Succeeded);

        Ok(all_succeeded)

    }

    /// Loads the task runs of the same job run that this task run depends on.
    async fn get_dependent_task_runs(&self, task_run: &TaskRun) -> anyhow::Result<Vec<TaskRun>> {

        let mut dependent_task_runs = Vec::new();

        for dependent_task_id in task_run.depends_on.0.iter() {

            let dependent_task_run = self.crud.select_task_run(
                &*self.conn_pool,
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

    async fn update_task_run_status(&self, task_run: &TaskRun, status: TaskRunStatus) -> anyhow::Result<()> {

        self.crud.update_task_runs(
            &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

}


impl Service for TaskRunDispatcher {
    type Row = TaskRun;

    fn name(&self) -> &'static str {
        "Task Run Dispatcher"
    }

    fn row_context(&self, task_run: &TaskRun) -> String {
        format!("task run {}", task_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<TaskRun>> {
        self.get_pending_task_runs().await
    }

    async fn handle(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.handle_pending_task_run(task_run).await
    }
}
