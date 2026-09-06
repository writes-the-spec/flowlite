use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Watches running job runs and finishes them once all their task runs are done.
/// Runs independently of JobRunDispatcher, picking up whatever it set to running.
pub struct JobRunMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl JobRunMonitor {

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

    /// Settles a running job run as exactly one outcome, from its task runs alone.
    ///
    /// **The order of these calls is the logic, not a formatting choice**, since each guard
    /// asks only whether its own status is present:
    ///
    /// - `settle_for_running` has to precede the failure outcomes, or a job run holding one
    ///   failed task run and one still executing gets finished while its work carries on,
    ///   and finishing is irreversible — this monitor only ever visits Running rows.
    /// - A real failure outranks a stop, so `settle_for_aborted` is asked last: a job run
    ///   with one aborted and one failed task run reports the failure, which is the part
    ///   worth acting on. Between the two failures, timed out outranks failed.
    ///
    /// Nothing here writes Skipped: a Running job run has started, so a stop aborts it.
    async fn handle_running_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let task_runs = self.get_task_runs(job_run).await?;

        if self.settle_for_succeeded(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_running(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_timed_out(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_failed(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_aborted(job_run, &task_runs).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Job run {} settled as nothing: all of its task runs finished, none of them \
             timed out, failed, was aborted or was skipped, and they did not all succeed",
            job_run.id,
        )
    }

    /// Succeeds the job run once every task run has succeeded — including a job run with
    /// no task runs at all, which `all` over an empty list settles here immediately.
    async fn settle_for_succeeded(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let all_succeeded = task_runs.iter().all(|task_run| task_run.status == TaskRunStatus::Succeeded);

        if !all_succeeded {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Succeeded).await?;

        Ok(true)
    }

    /// Leaves the job run running, writing nothing, while any task run of it is still
    /// pending or running.
    async fn settle_for_running(&self, _job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        Ok(task_runs.iter().any(|task_run| !task_run.status.is_finished()))
    }

    /// Times the job run out if a task run of it ran past its timeout with no retry left.
    async fn settle_for_timed_out(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let any_timed_out = task_runs.iter().any(|task_run| task_run.status == TaskRunStatus::TimedOut);

        if !any_timed_out {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::TimedOut).await?;

        Ok(true)
    }

    /// Fails the job run if a task run of it failed with no retry left.
    async fn settle_for_failed(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let any_failed = task_runs.iter().any(|task_run| task_run.status == TaskRunStatus::Failed);

        if !any_failed {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Failed).await?;

        Ok(true)
    }

    /// Aborts the job run that was stopped, which its task runs report in either of two
    /// ways: one was killed mid-flight, or one was skipped before it could start.
    ///
    /// Asked last, so a real failure outranks a stop. A skipped task run means a stop or a
    /// dependency that did not succeed, and such a dependency would itself be failed or
    /// timed out — so by the time this is asked, nothing has failed and only a stop is left.
    async fn settle_for_aborted(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let any_stopped = task_runs.iter().any(|task_run| matches!(
            task_run.status,
            TaskRunStatus::Aborted | TaskRunStatus::Skipped,
        ));

        if !any_stopped {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Aborted).await?;

        Ok(true)
    }

    async fn get_task_runs(&self, job_run: &JobRun) -> anyhow::Result<Vec<TaskRun>> {

        self.crud.select_task_runs(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run.id),
                    task_id: None,
                    job_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await

    }

    async fn get_running_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
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

    async fn update_job_run_status(&self, job_run: &JobRun, status: JobRunStatus) -> anyhow::Result<()> {
        self.crud
            .update_job_runs(
                &*self.conn_pool,
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

        self.signals.publish();

        Ok(())
    }

}


impl Service for JobRunMonitor {
    type Row = JobRun;

    fn name(&self) -> &'static str {
        "Job Run Monitor"
    }

    fn row_context(&self, job_run: &JobRun) -> String {
        format!("job run {}", job_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<JobRun>> {
        self.get_running_job_runs().await
    }

    async fn handle(&self, job_run: &JobRun) -> anyhow::Result<()> {
        self.handle_running_job_run(job_run).await
    }
}


