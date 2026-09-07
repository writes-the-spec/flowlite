use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, TaskRunAttemptStatus};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Picks up pending task runs and settles each one as skipped, still pending on a
/// dependency or running. Hands off to TaskRunMonitor through the task run status only.
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

    /// Settles a pending task run as exactly one outcome. Falling past all three bails
    /// rather than returning quietly: a row nobody handled looks exactly like one
    /// legitimately waiting on a dependency.
    ///
    /// Each outcome loads the dependencies itself, so a dependency failing mid-pass can
    /// leave a set that is neither all-succeeded nor still-running and bail on an ordinary
    /// state. It clears next pass, when `settle_as_skipped` claims the row.
    async fn handle_pending_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        if self.settle_as_skipped(task_run).await? {
            return Ok(());
        }

        if self.settle_as_pending(task_run).await? {
            return Ok(());
        }

        if self.settle_as_running(task_run).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Task run {} settled as nothing: its job run was not stopped, no task run it \
             depends on failed, none of them is still running, and they have not all \
             succeeded",
            task_run.id,
        )
    }

    /// Skips the task run if its job run was stopped or a dependency did not succeed,
    /// either of which means it can never run.
    async fn settle_as_skipped(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let must_skip = self.is_job_run_stopped(task_run).await?
            || self.get_dependent_task_runs(task_run).await?
                .iter()
                .any(|tr| matches!(
                    tr.status,
                    TaskRunStatus::Failed
                        | TaskRunStatus::Skipped
                        | TaskRunStatus::Aborted
                        | TaskRunStatus::TimedOut
                ));

        if !must_skip {
            return Ok(false);
        }

        self.crud.update_task_runs(
            &*self.conn_pool,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: Some(task_run.id),
                    job_run_id: None,
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

    /// Leaves the task run pending, writing nothing, while a task run it depends on has yet
    /// to finish. `settle_as_skipped` has already ruled out every dependency that finished
    /// without succeeding, so an unfinished one here is still one this run is waiting for.
    async fn settle_as_pending(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        let any_unfinished = dependent_task_runs.iter().any(|tr| matches!(
            tr.status,
            TaskRunStatus::Pending | TaskRunStatus::Running,
        ));

        Ok(any_unfinished)
    }

    /// Sets the task run to running, which is what makes TaskRunMonitor pick it up, once
    /// every task run it depends on has succeeded, and gives it the first attempt to run.
    ///
    /// Asking whether they all succeeded, rather than starting whatever `settle_as_pending`
    /// turned down, is what stops a dependency that failed since that guard ran: the two
    /// load the dependencies separately.
    ///
    /// The attempt row is inserted **before** the status, for the same reason the attempt
    /// dispatcher hands its child over before writing Running: TaskRunMonitor decides from
    /// the last attempt, so a Running task run without one is a state it cannot act on.
    /// Every later attempt is a retry, and those are TaskRunMonitor's.
    async fn settle_as_running(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        let all_succeeded = dependent_task_runs.iter().all(|tr| tr.status == TaskRunStatus::Succeeded);

        if !all_succeeded {
            return Ok(false);
        }

        self.crud.insert_task_run_attempt(
            &*self.conn_pool,
            &InsertTaskRunAttemptData {
                input: InsertTaskRunAttemptDataInput {
                    task_run_id: task_run.id,
                    job_run_id: task_run.job_run_id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    attempt: 1,
                    status: TaskRunAttemptStatus::Pending,
                },
            },
        ).await?;

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

        Ok(true)
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
