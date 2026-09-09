use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
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

    /// Settles a pending task run as exactly one outcome, bailing past the last rather than
    /// returning quietly: a row nobody handled looks like one legitimately waiting.
    ///
    /// Each outcome loads the dependencies itself, so one failing mid-pass can leave a set
    /// that is neither all-succeeded nor still-running and reach that bail on an ordinary
    /// state. The next pass settles it.
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
    ///
    /// `Invalid` counts as not succeeding: left out, its dependents are neither held nor
    /// started, and one unreadable row becomes an unreadable subtree.
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
                        | TaskRunStatus::Invalid
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
    /// every task run it depends on has succeeded.
    ///
    /// Asking whether they all succeeded, rather than starting whatever `settle_as_pending`
    /// turned down, is what stops a dependency that failed since that guard ran.
    ///
    /// It writes one status and nothing else — attempts are TaskRunMonitor's — so no crash
    /// can strand a half-started row here.
    async fn settle_as_running(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        let all_succeeded = dependent_task_runs.iter().all(|tr| tr.status == TaskRunStatus::Succeeded);

        if !all_succeeded {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::crud::job_run::JobRunStatus;
    use crate::test_support::TestDb;

    /// Without Invalid in the skip list a dependent is neither skipped, nor pending (Invalid
    /// is finished), nor started (nothing succeeded) — so it hits the bail on every pass and
    /// one stranded row becomes a stranded subtree.
    #[tokio::test]
    async fn a_dependent_of_an_invalid_task_run_is_skipped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        db.insert_named_task_run(job_run.id, "upstream", TaskRunStatus::Invalid).await;

        let dependent = db.insert_task_run_depending_on(job_run.id, &["upstream"]).await;

        db.task_run_dispatcher().handle(&dependent).await.unwrap();

        assert_eq!(db.task_run(dependent.id).await.status, TaskRunStatus::Skipped);
    }

    /// Inserting attempt 1 here too would leave a window a crash could stop inside, after
    /// which every pass hit the unique index on (task_run_id, attempt) instead of starting.
    #[tokio::test]
    async fn starting_a_task_run_inserts_no_attempt() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Pending).await;

        db.task_run_dispatcher().handle(&task_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Running);
        assert!(db.task_run_attempts(task_run.id).await.is_empty());
    }

    /// The state that window left behind, which a database written by the old order can
    /// still hold: a pending task run that already has attempt 1. Starting it must not
    /// collide with that row.
    #[tokio::test]
    async fn a_pending_task_run_that_already_has_an_attempt_still_starts() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Pending).await;

        db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.task_run_dispatcher().handle(&task_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Running);
        assert_eq!(db.task_run_attempts(task_run.id).await.len(), 1);
    }

    /// The ordinary path is unchanged: a dependency that succeeded still starts the run.
    #[tokio::test]
    async fn a_task_run_whose_dependency_succeeded_still_starts() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        db.insert_named_task_run(job_run.id, "upstream", TaskRunStatus::Succeeded).await;

        let dependent = db.insert_task_run_depending_on(job_run.id, &["upstream"]).await;

        db.task_run_dispatcher().handle(&dependent).await.unwrap();

        assert_eq!(db.task_run(dependent.id).await.status, TaskRunStatus::Running);
    }
}
