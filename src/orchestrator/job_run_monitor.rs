use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
use crate::crud::job_run_notification::{InsertJobRunNotificationData, InsertJobRunNotificationDataInput, JobRunNotificationStatus, NotificationChannel};
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
    /// **Order decides precedence**: a real failure outranks a stop, so `settle_for_aborted`
    /// is the last of the finished outcomes — a job run with one aborted and one failed task
    /// run reports the failure, which is the part worth acting on. Failed outranks timed out.
    /// That ordering is also what makes a skipped task run readable as a stop; see
    /// `TaskRunStatus::is_stopped`.
    ///
    /// `settle_for_running` is asked last, the succeeded, failed, timed out, aborted, running
    /// ladder all three monitors read in: it is the outcome that guards nothing of its own,
    /// so the `all_finished` each failure outcome re-asks is the one thing holding open a job
    /// run whose work is still going — finishing is irreversible, since this monitor only
    /// visits Running rows.
    ///
    /// Nothing here writes Skipped: a Running job run has started, so a stop aborts it.
    async fn handle_running_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        let task_runs = self.get_task_runs(job_run).await?;

        if self.settle_for_succeeded(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_failed(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_timed_out(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_aborted(job_run, &task_runs).await? {
            return Ok(());
        }

        if self.settle_for_running(job_run, &task_runs).await? {
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
    /// pending or running. Asked last, so it claims every job run the outcomes above
    /// declined; the bail below it means a task run status none of them knows.
    async fn settle_for_running(&self, _job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        Ok(task_runs.iter().any(|task_run| !task_run.status.is_finished()))
    }

    /// Fails the job run if a task run of it failed with no retry left, once the rest have
    /// finished too.
    async fn settle_for_failed(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let all_finished = task_runs.iter().all(|task_run| task_run.status.is_finished());
        let any_failed = task_runs.iter().any(|task_run| task_run.status == TaskRunStatus::Failed);

        if !all_finished || !any_failed {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::Failed).await?;

        Ok(true)
    }

    /// Times the job run out if a task run of it ran past its timeout with no retry left,
    /// once the rest have finished too.
    async fn settle_for_timed_out(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let all_finished = task_runs.iter().all(|task_run| task_run.status.is_finished());
        let any_timed_out = task_runs.iter().any(|task_run| task_run.status == TaskRunStatus::TimedOut);

        if !all_finished || !any_timed_out {
            return Ok(false);
        }

        self.update_job_run_status(job_run, JobRunStatus::TimedOut).await?;

        Ok(true)
    }

    /// Aborts the job run that was stopped, which its task runs report in either of two
    /// ways: one was killed mid-flight, or one was skipped before it could start.
    ///
    /// Asked last, so a real failure outranks a stop. A skipped task run means a stop or a
    /// dependency that did not succeed, and such a dependency would itself be failed or
    /// timed out — so by the time this is asked, nothing has failed and only a stop is left.
    async fn settle_for_aborted(&self, job_run: &JobRun, task_runs: &[TaskRun]) -> anyhow::Result<bool> {

        let all_finished = task_runs.iter().all(|task_run| task_run.status.is_finished());
        let any_stopped = task_runs.iter().any(|task_run| task_run.status.is_stopped());

        if !all_finished || !any_stopped {
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

    /// Finishes the job run, and queues its failure notification in the same transaction.
    ///
    /// One transaction because this monitor only ever visits Running rows: a status write
    /// that landed without its notification would leave a finished run nothing ever looks
    /// at again, which is a failure nobody is told about.
    async fn update_job_run_status(&self, job_run: &JobRun, status: JobRunStatus) -> anyhow::Result<()> {

        let mut tx = self.conn_pool.begin().await?;

        self.crud
            .update_job_runs(
                &mut *tx,
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

        // Left open for the NotificationService to deliver. This monitor writes the row
        // and nothing else - it never talks to that service, and never waits on a channel.
        if Self::is_worth_notifying(status) && !job_run.on_failure_emails.0.is_empty() {
            self.crud
                .insert_job_run_notification(
                    &mut *tx,
                    &InsertJobRunNotificationData {
                        input: InsertJobRunNotificationDataInput {
                            job_run_id: job_run.id,
                            job_id: job_run.job_id.clone(),
                            channel: NotificationChannel::Email,
                            recipients: job_run.on_failure_emails.0.clone(),
                            status: JobRunNotificationStatus::Pending,
                            error: String::new(),
                        }
                    },
                )
                .await?;
        }

        tx.commit().await?;

        self.signals.publish();

        Ok(())
    }

    /// Which outcomes are worth an email. Aborted is not one of them: a stop is somebody
    /// at a keyboard, who already knows what they did.
    fn is_worth_notifying(status: JobRunStatus) -> bool {
        matches!(status, JobRunStatus::Failed | JobRunStatus::TimedOut)
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




#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run_notification::JobRunNotification;
    use crate::test_support::TestDb;

    /// Runs the monitor over a running job run whose task runs have the given statuses,
    /// and reports the status it settled the job run as.
    async fn settled_job_run_status(task_run_statuses: &[TaskRunStatus]) -> JobRunStatus {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        for status in task_run_statuses {
            db.insert_task_run(job_run.id, *status).await;
        }

        db.job_run_monitor().handle(&job_run).await.unwrap();

        db.job_run(job_run.id).await.status
    }

    #[tokio::test]
    async fn every_task_run_succeeding_succeeds_the_job_run() {
        let status = settled_job_run_status(&[
            TaskRunStatus::Succeeded,
            TaskRunStatus::Succeeded,
        ]).await;

        assert_eq!(status, JobRunStatus::Succeeded);
    }

    #[tokio::test]
    async fn a_job_run_with_no_task_runs_succeeds() {
        let status = settled_job_run_status(&[]).await;

        assert_eq!(status, JobRunStatus::Succeeded);
    }

    #[tokio::test]
    async fn an_unfinished_task_run_keeps_the_job_run_running() {
        let status = settled_job_run_status(&[
            TaskRunStatus::Succeeded,
            TaskRunStatus::Running,
        ]).await;

        assert_eq!(status, JobRunStatus::Running);
    }

    /// The `all_finished` each failure outcome re-asks is what holds this job run open:
    /// `settle_for_running` is asked after them, so dropping one of those guards finishes
    /// the job run here instead and fails this test.
    #[tokio::test]
    async fn a_failure_does_not_finish_a_job_run_whose_work_is_still_going() {
        let status = settled_job_run_status(&[
            TaskRunStatus::Failed,
            TaskRunStatus::Running,
        ]).await;

        assert_eq!(status, JobRunStatus::Running);
    }

    /// A real failure outranks a stop: the failure is the part worth acting on.
    #[tokio::test]
    async fn a_failed_task_run_outranks_an_aborted_one() {
        let status = settled_job_run_status(&[
            TaskRunStatus::Aborted,
            TaskRunStatus::Failed,
        ]).await;

        assert_eq!(status, JobRunStatus::Failed);
    }

    #[tokio::test]
    async fn a_failed_task_run_outranks_a_timed_out_one() {
        let status = settled_job_run_status(&[
            TaskRunStatus::TimedOut,
            TaskRunStatus::Failed,
        ]).await;

        assert_eq!(status, JobRunStatus::Failed);
    }

    #[tokio::test]
    async fn a_timed_out_task_run_outranks_an_aborted_one() {
        let status = settled_job_run_status(&[
            TaskRunStatus::Aborted,
            TaskRunStatus::TimedOut,
        ]).await;

        assert_eq!(status, JobRunStatus::TimedOut);
    }

    /// A skipped task run reports a stop, and a job run that had started is aborted by one
    /// rather than skipped — nothing in this monitor writes Skipped.
    #[tokio::test]
    async fn a_skipped_task_run_with_no_failure_aborts_the_job_run() {
        let status = settled_job_run_status(&[
            TaskRunStatus::Succeeded,
            TaskRunStatus::Skipped,
        ]).await;

        assert_eq!(status, JobRunStatus::Aborted);
    }

    /// Runs the monitor over a job run that asked to be emailed, and reports the
    /// notifications it queued alongside the status it settled.
    async fn settled_with_notifications(
        task_run_statuses: &[TaskRunStatus],
        on_failure_emails: &[&str],
    ) -> (JobRunStatus, Vec<JobRunNotification>) {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_with_on_failure_emails(
            JobRunStatus::Running,
            on_failure_emails,
        ).await;

        for status in task_run_statuses {
            db.insert_task_run(job_run.id, *status).await;
        }

        db.job_run_monitor().handle(&job_run).await.unwrap();

        (
            db.job_run(job_run.id).await.status,
            db.job_run_notifications(job_run.id).await,
        )
    }

    #[tokio::test]
    async fn a_failed_job_run_queues_a_notification_to_everyone_it_names() {
        let (status, notifications) = settled_with_notifications(
            &[TaskRunStatus::Failed],
            &["oncall@example.com", "data@example.com"],
        ).await;

        assert_eq!(status, JobRunStatus::Failed);
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].status, JobRunNotificationStatus::Pending);
        assert_eq!(notifications[0].recipients.0, vec!["oncall@example.com", "data@example.com"]);
        assert_eq!(notifications[0].channel, NotificationChannel::Email);
        assert_eq!(notifications[0].sent_at, None);
    }

    #[tokio::test]
    async fn a_timed_out_job_run_queues_a_notification_too() {
        let (status, notifications) = settled_with_notifications(
            &[TaskRunStatus::TimedOut],
            &["oncall@example.com"],
        ).await;

        assert_eq!(status, JobRunStatus::TimedOut);
        assert_eq!(notifications.len(), 1);
    }

    #[tokio::test]
    async fn a_succeeded_job_run_queues_nothing() {
        let (status, notifications) = settled_with_notifications(
            &[TaskRunStatus::Succeeded],
            &["oncall@example.com"],
        ).await;

        assert_eq!(status, JobRunStatus::Succeeded);
        assert!(notifications.is_empty());
    }

    /// A stop is somebody at a keyboard, who already knows what they did.
    #[tokio::test]
    async fn an_aborted_job_run_queues_nothing() {
        let (status, notifications) = settled_with_notifications(
            &[TaskRunStatus::Aborted],
            &["oncall@example.com"],
        ).await;

        assert_eq!(status, JobRunStatus::Aborted);
        assert!(notifications.is_empty());
    }

    #[tokio::test]
    async fn a_failed_job_run_naming_nobody_queues_nothing() {
        let (status, notifications) = settled_with_notifications(
            &[TaskRunStatus::Failed],
            &[],
        ).await;

        assert_eq!(status, JobRunStatus::Failed);
        assert!(notifications.is_empty());
    }

    /// The monitor only ever visits Running rows, so a job run it has already finished is
    /// never settled twice — which is what stops one failure becoming two emails.
    #[tokio::test]
    async fn a_finished_job_run_is_no_longer_selected() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_with_on_failure_emails(
            JobRunStatus::Running,
            &["oncall@example.com"],
        ).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;

        let monitor = db.job_run_monitor();

        monitor.handle(&job_run).await.unwrap();

        assert!(monitor.select().await.unwrap().is_empty());
        assert_eq!(db.job_run_notifications(job_run.id).await.len(), 1);
    }
}
