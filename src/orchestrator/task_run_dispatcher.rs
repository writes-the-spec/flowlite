use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus, UpdateTaskRunsData, UpdateTaskRunsDataFilter, UpdateTaskRunsDataInput};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Picks up waiting task runs and settles each one as skipped, still waiting on a
/// dependency or running. Hands off to TaskRunMonitor through the task run status only.
///
/// Only `Waiting` is picked up, and `JobRunDispatcher` is the only thing that writes it —
/// so a task run whose job run has yet to start is `Planned` and invisible here, rather
/// than started hours before its run is due.
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

    /// Dispatches the write for whatever `derive_next_status` decides. Deciding only reads.
    async fn handle_waiting_task_run(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        match self.derive_next_status(task_run).await {
            Ok(Some(TaskRunStatus::Skipped)) => self.set_to_skipped(task_run).await,
            Ok(Some(TaskRunStatus::Waiting)) => Ok(()),
            Ok(Some(TaskRunStatus::Running)) => self.set_to_running(task_run).await,
            Ok(_) | Err(_) => self.set_to_invalid(task_run).await,
        }
    }

    /// Derives a waiting run's next status: skipped if stopped or a dependency did not
    /// succeed, running once every dependency has, else still waiting on one unfinished.
    ///
    /// Each check reloads the dependencies fresh rather than sharing one snapshot, so a
    /// dependency that moves between checks is read at its later status. A dependency set
    /// none of the three matches reaches `None`, settled invalid like any other undecided
    /// or unreadable case.
    async fn derive_next_status(&self, task_run: &TaskRun) -> anyhow::Result<Option<TaskRunStatus>> {

        if self.should_skip(task_run).await? {
            return Ok(Some(TaskRunStatus::Skipped));
        }

        if self.is_still_waiting(task_run).await? {
            return Ok(Some(TaskRunStatus::Waiting));
        }

        if self.all_dependencies_succeeded(task_run).await? {
            return Ok(Some(TaskRunStatus::Running));
        }

        Ok(None)
    }

    /// True if the job run was stopped or a dependency did not succeed, either of which
    /// means this run can never start.
    ///
    /// `Invalid` counts as not succeeding: left out, one unreadable row becomes an
    /// unreadable subtree.
    async fn should_skip(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

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

        Ok(must_skip)
    }

    /// True while a dependency has yet to finish. `should_skip` already ruled out every one
    /// that finished without succeeding.
    async fn is_still_waiting(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        Ok(dependent_task_runs.iter().any(|tr| !tr.status.is_finished()))
    }

    /// True once every dependency has succeeded. Asked rather than starting whatever
    /// `is_still_waiting` turned down, so one that failed since stops the run here.
    async fn all_dependencies_succeeded(&self, task_run: &TaskRun) -> anyhow::Result<bool> {

        let dependent_task_runs = self.get_dependent_task_runs(task_run).await?;

        Ok(dependent_task_runs.iter().all(|tr| tr.status == TaskRunStatus::Succeeded))
    }

    /// Settles a run `derive_next_status` could not decide or read. See
    /// `JobRunMonitor::settle_unclaimed` for why it settles rather than raises.
    async fn set_to_invalid(&self, task_run: &TaskRun) -> anyhow::Result<()> {

        eprintln!(
            "Task run {} was waiting but its next status could not be derived. Settling it \
             invalid. This is a bug.",
            task_run.id,
        );

        self.crud.update_task_runs(
            &*self.conn_pool,
            &UpdateTaskRunsData {
                filter: UpdateTaskRunsDataFilter {
                    id: Some(task_run.id),
                    job_run_id: None,
                    status: None,
                },
                input: UpdateTaskRunsDataInput {
                    status: Some(TaskRunStatus::Invalid),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                },
            }
        ).await?;

        self.signals.publish();

        Ok(())
    }

    /// Skips the task run, none of whose dependencies can still let it run.
    async fn set_to_skipped(&self, task_run: &TaskRun) -> anyhow::Result<()> {

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

        Ok(())
    }

    /// Sets the task run running, which is what makes TaskRunMonitor pick it up. It writes
    /// one status and nothing else — attempts are TaskRunMonitor's — so no crash can strand
    /// a half-started row here.
    async fn set_to_running(&self, task_run: &TaskRun) -> anyhow::Result<()> {

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

    async fn get_waiting_task_runs(&self) -> anyhow::Result<Vec<TaskRun>> {

        self.crud.select_task_runs(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: Some(TaskRunStatus::Waiting),
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
        self.get_waiting_task_runs().await
    }

    async fn handle(&self, task_run: &TaskRun) -> anyhow::Result<()> {
        self.handle_waiting_task_run(task_run).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::crud::job_run::JobRunStatus;
    use crate::test_support::TestDb;

    /// Unreachable through `handle` while the three checks cover every status a dependency
    /// can hold, so called directly. See `JobRunMonitor::settle_unclaimed` for why it is
    /// settled at all.
    #[tokio::test]
    async fn an_unclaimed_task_run_is_settled_invalid() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Waiting).await;

        db.task_run_dispatcher().set_to_invalid(&task_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Invalid);
    }

    /// Without Invalid in the skip list a dependent is neither skipped, waiting, nor started
    /// — settled invalid instead — and one stranded row becomes a stranded subtree.
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
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Waiting).await;

        db.task_run_dispatcher().handle(&task_run).await.unwrap();

        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Running);
        assert!(db.task_run_attempts(task_run.id).await.is_empty());
    }

    /// The state that window left behind, which a database written by the old order can
    /// still hold: a waiting task run that already has attempt 1. Starting it must not
    /// collide with that row.
    #[tokio::test]
    async fn a_waiting_task_run_that_already_has_an_attempt_still_starts() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Waiting).await;

        db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Queued).await;

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

    /// The whole point of Planned: a run submitted for tonight is written with its task
    /// runs, and this service must not see one of them until JobRunDispatcher releases it.
    /// Selecting on Queued, as this once did, started tonight's work on the next pass.
    #[tokio::test]
    async fn a_task_run_of_a_job_run_that_has_not_started_is_not_selected() {

        let db = TestDb::new().await;

        let due = Utc::now() + chrono::TimeDelta::hours(3);
        let job_run = db.insert_job_run_at(JobRunStatus::Submitted, due, None).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        assert!(db.task_run_dispatcher().select().await.unwrap().is_empty());
    }

    /// A dependency its job run has yet to release is unfinished, so the dependent holds
    /// rather than being settled invalid.
    #[tokio::test]
    async fn a_dependent_of_a_planned_task_run_keeps_waiting() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        db.insert_named_task_run(job_run.id, "upstream", TaskRunStatus::Planned).await;

        let dependent = db.insert_task_run_depending_on(job_run.id, &["upstream"]).await;

        db.task_run_dispatcher().handle(&dependent).await.unwrap();

        assert_eq!(db.task_run(dependent.id).await.status, TaskRunStatus::Waiting);
    }
}
