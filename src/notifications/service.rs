use std::sync::Arc;
use chrono::Utc;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_notification::{
    JobRunNotification, JobRunNotificationStatus, SelectJobRunNotificationsData,
    SelectJobRunNotificationsDataFilter, SelectJobRunNotificationsDataSort,
    UpdateJobRunNotificationsData, UpdateJobRunNotificationsDataFilter,
    UpdateJobRunNotificationsDataInput,
};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt};
use crate::crud::task_run_attempt_output::{
    group_task_run_attempt_output, SelectTaskRunAttemptOutputsData,
    SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort,
};
use crate::notifications::channel::NotificationChannels;
use crate::notifications::message::{job_run_failure_message, JobRunFailureTask};
use crate::poller::Service;


/// Delivers the notifications something else has left open, over whichever channel each
/// one asks for.
///
/// It is not part of the orchestrator, and starts alongside it the way the Scheduler
/// does. Nothing in the orchestrator calls it and it calls nothing back: a run that fails
/// leaves a `job_run_notification` row, and this loop picks up every open one on its own
/// pass. Delivery is slow and sometimes fails for hours, which is exactly the work a
/// monitor must not be holding when it is meant to be finishing everyone else's runs.
pub struct NotificationService {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub channels: Arc<NotificationChannels>,
}


impl NotificationService {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        channels: Arc<NotificationChannels>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            channels,
        }
    }

    /// Delivers one open notification and records what happened to it.
    ///
    /// **Every path writes the row**, which is what stops a channel that is down from
    /// being hammered every second: a delivery that fails is recorded as failed, with the
    /// error on the row, and is not tried again. There is deliberately no retry policy
    /// here — one would need its own delay and attempt count, and an alert nobody can see
    /// failed is worse than one that failed loudly.
    async fn handle_open_notification(&self, notification: &JobRunNotification) -> anyhow::Result<()> {

        let job_run = self.get_job_run(notification.job_run_id).await?;

        let task_runs = self.get_task_runs(job_run.id).await?;

        let failures = self.get_failures(&task_runs).await?;

        let message = job_run_failure_message(
            &job_run,
            &task_runs,
            &failures,
            self.channels.max_output_bytes(notification.channel),
        );

        let delivered = self.channels.send(
            notification.channel,
            &notification.recipients.0,
            &message,
        ).await;

        if let Err(e) = delivered {
            self.record_failed(notification, &format!("{:#}", e)).await?;

            return Err(e.context(format!(
                "Failed to deliver job run {} by {} to {}",
                job_run.id,
                notification.channel,
                notification.recipients.0.join(", "),
            )));
        }

        self.record_sent(notification).await
    }

    /// The task runs worth quoting: the ones that did not succeed on their own account.
    /// A skipped task run is left out — it says only that something above it broke, and
    /// the task list in the message already carries that.
    async fn get_failures(&self, task_runs: &[TaskRun]) -> anyhow::Result<Vec<JobRunFailureTask>> {

        let mut failures = Vec::new();

        for task_run in task_runs {

            let is_failure = matches!(
                task_run.status,
                TaskRunStatus::Failed | TaskRunStatus::TimedOut,
            );

            if !is_failure {
                continue;
            }

            let attempt = self.get_last_attempt(task_run).await?;

            let streams = match &attempt {
                Some(attempt) => self.get_attempt_streams(attempt).await?,
                None => Default::default(),
            };

            failures.push(JobRunFailureTask {
                task_run: task_run.clone(),
                attempt,
                streams,
            });
        }

        Ok(failures)
    }

    async fn get_last_attempt(&self, task_run: &TaskRun) -> anyhow::Result<Option<TaskRunAttempt>> {

        let attempts = self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: Some(task_run.id),
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await?;

        Ok(attempts.into_iter().last())
    }

    async fn get_attempt_streams(
        &self,
        attempt: &TaskRunAttempt,
    ) -> anyhow::Result<crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams> {

        let output = self.crud.select_task_run_attempt_outputs(
            &*self.conn_pool,
            &SelectTaskRunAttemptOutputsData {
                filter: SelectTaskRunAttemptOutputsDataFilter {
                    id: None,
                    task_run_attempt_id: Some(attempt.id),
                    task_run_id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    stream: None,
                },
                sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
            }
        ).await?;

        // An attempt with no rows printed nothing, which is empty output rather than
        // unknown output.
        Ok(group_task_run_attempt_output(output)
            .remove(&attempt.id)
            .unwrap_or_default())
    }

    async fn get_job_run(&self, job_run_id: i64) -> anyhow::Result<JobRun> {

        let job_run = self.crud.select_job_run(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: Some(job_run_id),
                    job_id: None,
                    status: None,
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        ).await?;

        job_run.ok_or_else(|| anyhow::anyhow!("Job run {} not found", job_run_id))
    }

    async fn get_task_runs(&self, job_run_id: i64) -> anyhow::Result<Vec<TaskRun>> {

        self.crud.select_task_runs(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run_id),
                    task_id: None,
                    job_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await
    }

    /// Every notification still open, whatever channel it wants and whatever run it is
    /// about — oldest first, so a backlog after a restart goes out in the order it built
    /// up in.
    async fn get_open_notifications(&self) -> anyhow::Result<Vec<JobRunNotification>> {

        self.crud.select_job_run_notifications(
            &*self.conn_pool,
            &SelectJobRunNotificationsData {
                filter: SelectJobRunNotificationsDataFilter {
                    id: None,
                    job_run_id: None,
                    channel: None,
                    status: Some(JobRunNotificationStatus::Pending),
                },
                sort: Some(SelectJobRunNotificationsDataSort::Id),
                limit: None,
                offset: None,
            }
        ).await
    }

    async fn record_sent(&self, notification: &JobRunNotification) -> anyhow::Result<()> {

        self.crud.update_job_run_notifications(
            &*self.conn_pool,
            &UpdateJobRunNotificationsData {
                filter: UpdateJobRunNotificationsDataFilter { id: Some(notification.id) },
                input: UpdateJobRunNotificationsDataInput {
                    status: Some(JobRunNotificationStatus::Sent),
                    error: None,
                    sent_at: Some(Some(Utc::now())),
                },
            },
        ).await
    }

    async fn record_failed(&self, notification: &JobRunNotification, error: &str) -> anyhow::Result<()> {

        self.crud.update_job_run_notifications(
            &*self.conn_pool,
            &UpdateJobRunNotificationsData {
                filter: UpdateJobRunNotificationsDataFilter { id: Some(notification.id) },
                input: UpdateJobRunNotificationsDataInput {
                    status: Some(JobRunNotificationStatus::Failed),
                    error: Some(error.to_string()),
                    sent_at: None,
                },
            },
        ).await
    }

}


impl Service for NotificationService {
    type Row = JobRunNotification;

    fn name(&self) -> &'static str {
        "Notification Service"
    }

    fn row_context(&self, notification: &JobRunNotification) -> String {
        format!(
            "job run notification {} of job run {}, by {}",
            notification.id,
            notification.job_run_id,
            notification.channel,
        )
    }

    async fn select(&self) -> anyhow::Result<Vec<JobRunNotification>> {
        self.get_open_notifications().await
    }

    async fn handle(&self, notification: &JobRunNotification) -> anyhow::Result<()> {
        self.handle_open_notification(notification).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::poller::Service;
    use crate::test_support::TestDb;

    /// The two services meet through the row and nowhere else: the monitor leaves one
    /// open, this one picks it up on its own pass.
    async fn open_notification(db: &TestDb) -> JobRunNotification {

        let job_run = db.insert_job_run_with_on_failure_emails(
            JobRunStatus::Running,
            &["oncall@example.com"],
        ).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;

        db.job_run_monitor().handle(&job_run).await.unwrap();

        db.job_run_notifications(job_run.id).await.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn a_failed_run_leaves_exactly_one_open_notification_to_pick_up() {

        let db = TestDb::new().await;

        let notification = open_notification(&db).await;

        assert_eq!(notification.status, JobRunNotificationStatus::Pending);
        assert_eq!(db.notification_service().select().await.unwrap().len(), 1);
    }

    /// The whole point of recording every outcome: a notification whose channel this box
    /// cannot deliver over is closed as failed, with the reason on the row, rather than
    /// being selected again on every pass forever.
    #[tokio::test]
    async fn a_channel_with_nothing_configured_closes_the_notification_as_failed() {

        let db = TestDb::new().await;

        let notification = open_notification(&db).await;

        let service = db.notification_service();

        assert!(service.handle(&notification).await.is_err());

        let settled = db.job_run_notifications(notification.job_run_id).await
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(settled.status, JobRunNotificationStatus::Failed);
        assert!(settled.error.contains("[smtp]"), "{}", settled.error);
        assert_eq!(settled.sent_at, None);

        // And so it is no longer open.
        assert!(service.select().await.unwrap().is_empty());
    }
}
