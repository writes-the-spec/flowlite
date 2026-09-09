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
use crate::notifications::message::{job_run_message, JobRunFailureTask};
use crate::poller::Service;


/// Delivers the notifications something else has left open, over whichever channel each
/// one asks for.
///
/// It is not part of the orchestrator, and starts alongside it the way the Scheduler
/// does. Nothing in the orchestrator calls it and it calls nothing back: a run leaves
/// `job_run_notification` rows when it is submitted, and this loop picks up every open one
/// on its own pass. Delivery is slow and sometimes fails for hours, which is exactly the work a
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

    /// Decides what one open notification deserves, and does it.
    ///
    /// **Open does not mean ready.** A notification is written when its run is submitted,
    /// long before anyone knows whether it will be needed, so the first question is how
    /// the run ended:
    ///
    /// - still going — leave it open, and ask again on the next pass;
    /// - ended some way other than the one this row is waiting for — close it as skipped;
    /// - ended the way it was written for — build the message and deliver it.
    ///
    /// Which ending that is belongs to the row, not to this loop: a run a job wants to
    /// hear about either way carries a notification for each, and one run ending settles
    /// them differently.
    ///
    /// **Every path that reaches a channel writes the row**, which is what stops a
    /// channel that is down from being hammered every second: a delivery that fails is
    /// recorded as failed, with the error on the row, and is not tried again. There is
    /// deliberately no retry policy here — one would need its own delay and attempt
    /// count, and an alert nobody can see failed is worse than one that failed loudly.
    async fn handle_open_notification(&self, notification: &JobRunNotification) -> anyhow::Result<()> {

        let job_run = self.get_job_run(notification.job_run_id).await?;

        if !job_run.status.is_finished() {
            return Ok(());
        }

        if !notification.notify_on.wants(job_run.status) {
            return self.record_skipped(notification).await;
        }

        let task_runs = self.get_task_runs(job_run.id).await?;

        // Empty for a run that succeeded, which is what makes one message shape enough
        // for both endings.
        let failures = self.get_failures(&task_runs).await?;

        let message = job_run_message(
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
                "Failed to deliver job run {} on {} by {} to {}",
                job_run.id,
                notification.notify_on,
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
    ///
    /// These include the runs still going: a notification is open from the moment its run
    /// is submitted, and `handle` is what decides whether its run has ended and how.
    async fn get_open_notifications(&self) -> anyhow::Result<Vec<JobRunNotification>> {

        self.crud.select_job_run_notifications(
            &*self.conn_pool,
            &SelectJobRunNotificationsData {
                filter: SelectJobRunNotificationsDataFilter {
                    id: None,
                    job_run_id: None,
                    notify_on: None,
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

    /// Closes a notification whose run ended in a way nobody needs telling about. It was
    /// written before that was knowable, so this is an ordinary outcome rather than a
    /// failure — and closing it is what keeps it out of the next pass.
    async fn record_skipped(&self, notification: &JobRunNotification) -> anyhow::Result<()> {

        self.crud.update_job_run_notifications(
            &*self.conn_pool,
            &UpdateJobRunNotificationsData {
                filter: UpdateJobRunNotificationsDataFilter { id: Some(notification.id) },
                input: UpdateJobRunNotificationsDataInput {
                    status: Some(JobRunNotificationStatus::Skipped),
                    error: None,
                    sent_at: None,
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
            "job run notification {} of job run {}, on {} by {}",
            notification.id,
            notification.job_run_id,
            notification.notify_on,
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
    use crate::crud::job_run_notification::{NotificationChannel, NotifyOn};
    use crate::crud::task_run::TaskRunStatus;
    use crate::poller::Service;
    use crate::test_support::TestDb;

    /// A run in the given state, with the open `on_failure:` notification `submit_job`
    /// would have written for it when it was submitted.
    async fn notification_for_run(db: &TestDb, status: JobRunStatus) -> JobRunNotification {

        let job_run = db.insert_job_run(status).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;

        db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Failure,
            NotificationChannel::Email,
            &["oncall@example.com"],
        ).await
    }

    /// The same, for a run whose job asked to hear about a success instead.
    async fn success_notification_for_run(db: &TestDb, status: JobRunStatus) -> JobRunNotification {

        let job_run = db.insert_job_run(status).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Succeeded).await;

        db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Success,
            NotificationChannel::Email,
            &["data-team@example.com"],
        ).await
    }

    async fn settled_status(db: &TestDb, notification: &JobRunNotification) -> JobRunNotificationStatus {
        db.job_run_notifications(notification.job_run_id).await
            .into_iter()
            .find(|settled| settled.id == notification.id)
            .unwrap()
            .status
    }

    /// Open does not mean ready: the notification exists from submit, so most passes over
    /// it are about a run that has not ended yet.
    #[tokio::test]
    async fn a_run_still_going_leaves_its_notification_open() {

        let db = TestDb::new().await;

        let notification = notification_for_run(&db, JobRunStatus::Running).await;

        let service = db.notification_service();

        service.handle(&notification).await.unwrap();

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Pending);
        assert_eq!(service.select().await.unwrap().len(), 1);
    }

    /// A success notification is left open by a run still going for the same reason a
    /// failure one is: nothing is decidable until the run has ended.
    #[tokio::test]
    async fn a_run_still_going_leaves_its_success_notification_open_too() {

        let db = TestDb::new().await;

        let notification = success_notification_for_run(&db, JobRunStatus::Running).await;

        db.notification_service().handle(&notification).await.unwrap();

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Pending);
    }

    /// The mirror of the failure path: what one row calls news the other calls nothing to
    /// report, and the row is what says which.
    #[tokio::test]
    async fn a_succeeded_run_delivers_the_notification_that_asked_for_it() {

        let db = TestDb::new().await;

        let notification = success_notification_for_run(&db, JobRunStatus::Succeeded).await;

        let service = db.notification_service();

        // No channel is configured here, so reaching one at all is what this asserts.
        assert!(service.handle(&notification).await.is_err());

        let settled = db.job_run_notifications(notification.job_run_id).await
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(settled.status, JobRunNotificationStatus::Failed);
        assert!(settled.error.contains("[smtp]"), "{}", settled.error);

        assert!(service.select().await.unwrap().is_empty());
    }

    /// A failure is not what a success notification was written for, so it closes as
    /// skipped — the run's `on_failure:` row is what tells anybody about that.
    #[tokio::test]
    async fn a_failed_run_closes_its_success_notification_as_skipped() {

        let db = TestDb::new().await;

        let notification = success_notification_for_run(&db, JobRunStatus::Failed).await;

        db.notification_service().handle(&notification).await.unwrap();

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Skipped);
    }

    /// Nobody asked to be told that a run they stopped did not finish, either way round.
    #[tokio::test]
    async fn an_aborted_run_closes_its_success_notification_as_skipped() {

        let db = TestDb::new().await;

        let notification = success_notification_for_run(&db, JobRunStatus::Aborted).await;

        db.notification_service().handle(&notification).await.unwrap();

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Skipped);
    }

    /// One run ending settles a job's two rows in opposite directions, which is the whole
    /// reason the ending each waits for lives on the row.
    #[tokio::test]
    async fn a_run_asking_to_be_told_either_way_settles_its_two_rows_differently() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Succeeded).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Succeeded).await;

        let on_failure = db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Failure,
            NotificationChannel::Email,
            &["oncall@example.com"],
        ).await;

        let on_success = db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Success,
            NotificationChannel::Email,
            &["data-team@example.com"],
        ).await;

        let service = db.notification_service();

        service.handle(&on_failure).await.unwrap();

        // Delivery is what this one is for, and no channel is configured in a test.
        assert!(service.handle(&on_success).await.is_err());

        assert_eq!(settled_status(&db, &on_failure).await, JobRunNotificationStatus::Skipped);
        assert_eq!(settled_status(&db, &on_success).await, JobRunNotificationStatus::Failed);
    }

    #[tokio::test]
    async fn a_succeeded_run_closes_its_notification_as_skipped() {

        let db = TestDb::new().await;

        let notification = notification_for_run(&db, JobRunStatus::Succeeded).await;

        let service = db.notification_service();

        service.handle(&notification).await.unwrap();

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Skipped);
        assert!(service.select().await.unwrap().is_empty());
    }

    /// A stop is somebody at a keyboard, who already knows what they did.
    #[tokio::test]
    async fn an_aborted_run_closes_its_notification_as_skipped() {

        let db = TestDb::new().await;

        let notification = notification_for_run(&db, JobRunStatus::Aborted).await;

        db.notification_service().handle(&notification).await.unwrap();

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Skipped);
    }

    /// The whole point of recording every outcome: a notification whose channel this box
    /// cannot deliver over is closed as failed, with the reason on the row, rather than
    /// being selected again on every pass forever.
    #[tokio::test]
    async fn a_failed_run_with_no_channel_configured_closes_it_as_failed() {

        let db = TestDb::new().await;

        let notification = notification_for_run(&db, JobRunStatus::Failed).await;

        let service = db.notification_service();

        assert!(service.handle(&notification).await.is_err());

        let settled = db.job_run_notifications(notification.job_run_id).await
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(settled.status, JobRunNotificationStatus::Failed);
        assert!(settled.error.contains("[smtp]"), "{}", settled.error);
        assert_eq!(settled.sent_at, None);

        assert!(service.select().await.unwrap().is_empty());
    }

    /// The same for every channel, which is what the `None`-rather-than-absent shape in
    /// `NotificationChannels` buys: a job that asked for Slack on a box with no `[slack]`
    /// is told so on the row, naming the section that is missing.
    #[tokio::test]
    async fn a_slack_notification_with_no_slack_configured_names_that_section() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;

        let notification = db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Failure,
            NotificationChannel::Slack,
            &["#oncall"],
        ).await;

        assert!(db.notification_service().handle(&notification).await.is_err());

        let settled = db.job_run_notifications(job_run.id).await
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(settled.status, JobRunNotificationStatus::Failed);
        assert!(settled.error.contains("[slack]"), "{}", settled.error);
    }

    #[tokio::test]
    async fn a_timed_out_run_is_worth_telling_somebody_about_too() {

        let db = TestDb::new().await;

        let notification = notification_for_run(&db, JobRunStatus::TimedOut).await;

        // No channel is configured here, so reaching one at all is what this asserts.
        assert!(db.notification_service().handle(&notification).await.is_err());

        assert_eq!(settled_status(&db, &notification).await, JobRunNotificationStatus::Failed);
    }
}
