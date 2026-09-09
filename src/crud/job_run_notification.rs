use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;
use crate::crud::job_run::JobRunStatus;

/// Where one notification has got to.
///
/// It is written Pending when the run is submitted, long before anyone knows whether it
/// will be needed — so Pending means "open", not "ready to send". `NotificationService`
/// is what decides: Skipped once the run ends in a way not worth telling anyone about,
/// otherwise Sent or Failed once it has tried.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobRunNotificationStatus {
    Pending,
    Sent,
    Failed,
    Skipped,
}

impl std::fmt::Display for JobRunNotificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobRunNotificationStatus::Pending => write!(f, "pending"),
            JobRunNotificationStatus::Sent => write!(f, "sent"),
            JobRunNotificationStatus::Failed => write!(f, "failed"),
            JobRunNotificationStatus::Skipped => write!(f, "skipped"),
        }
    }
}

/// What a notification is waiting for its run to do. Spelled exactly as the suffix of the
/// key a job declares it under — `on_failure:` writes `failure`, `on_success:` writes
/// `success` — so a row says for itself which block asked for it.
///
/// A column rather than something inferred from the run afterwards: a job may ask for
/// both, and then the same run ending once has to settle two rows differently.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum NotifyOn {
    Failure,
    Success,
}

impl NotifyOn {

    /// Whether a run that ended this way is the thing this notification was written for.
    /// Asked only of a finished run — `JobRunStatus::is_finished` is the other half — and
    /// a `false` here closes the row as skipped rather than delivering it.
    ///
    /// Both arms are matched exhaustively on purpose: a new run status has to say what it
    /// means for a failure notification *and* for a success one, or it stops compiling.
    ///
    /// `Aborted` and `Skipped` are news to nobody either way: both mean somebody stopped
    /// the run, and they already know what they did. `Invalid` is the opposite case and
    /// counts as a failure: nobody chose it, it may have left a command running, and it is
    /// the ending least likely to be noticed by anyone watching.
    pub fn wants(&self, status: JobRunStatus) -> bool {
        match self {
            NotifyOn::Failure => match status {
                JobRunStatus::Failed
                | JobRunStatus::TimedOut
                | JobRunStatus::Invalid => true,
                JobRunStatus::Pending
                | JobRunStatus::Running
                | JobRunStatus::Succeeded
                | JobRunStatus::Skipped
                | JobRunStatus::Aborted => false,
            },
            NotifyOn::Success => match status {
                JobRunStatus::Succeeded => true,
                JobRunStatus::Pending
                | JobRunStatus::Running
                | JobRunStatus::Failed
                | JobRunStatus::Skipped
                | JobRunStatus::Aborted
                | JobRunStatus::TimedOut
                | JobRunStatus::Invalid => false,
            },
        }
    }

}

impl std::fmt::Display for NotifyOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotifyOn::Failure => write!(f, "failure"),
            NotifyOn::Success => write!(f, "success"),
        }
    }
}

/// How a notification reaches somebody. A column rather than something the sender infers
/// from the recipients, so one row says for itself what delivering it means — and a third
/// channel is a variant here plus an arm the compiler then demands.
///
/// Each variant is spelled exactly as the key a job declares it under inside `on_failure:`
/// or `on_success:`, so an error about a channel can name the YAML the reader has to go and
/// edit.
///
/// Ordered because a job's recipients are keyed by it, which is also what fixes the order
/// a run's notification rows are written in.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum NotificationChannel {
    Email,
    Slack,
}

impl std::fmt::Display for NotificationChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotificationChannel::Email => write!(f, "email"),
            NotificationChannel::Slack => write!(f, "slack"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunNotificationDataInput {
    pub job_run_id: i64,
    pub job_id: String,
    pub notify_on: NotifyOn,
    pub channel: NotificationChannel,
    pub recipients: Vec<String>,
    pub status: JobRunNotificationStatus,
    /// Empty until a send fails: a notification that has not been tried has no error,
    /// rather than an unknown one.
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunNotificationData {
    pub input: InsertJobRunNotificationDataInput,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunNotificationsDataFilter {
    pub id: Option<i64>,
    pub job_run_id: Option<i64>,
    pub notify_on: Option<NotifyOn>,
    pub channel: Option<NotificationChannel>,
    pub status: Option<JobRunNotificationStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SelectJobRunNotificationsDataSort {
    Id,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SelectJobRunNotificationsData {
    pub filter: SelectJobRunNotificationsDataFilter,
    pub sort: Option<SelectJobRunNotificationsDataSort>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRunNotificationsDataInput {
    pub status: Option<JobRunNotificationStatus>,
    pub error: Option<String>,
    pub sent_at: Option<Option<DateTime<Utc>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRunNotificationsDataFilter {
    pub id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRunNotificationsData {
    pub input: UpdateJobRunNotificationsDataInput,
    pub filter: UpdateJobRunNotificationsDataFilter,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct JobRunNotification {
    pub id: i64,
    pub job_run_id: i64,
    pub job_id: String,
    pub notify_on: NotifyOn,
    pub channel: NotificationChannel,
    pub recipients: sqlx::types::Json<Vec<String>>,
    pub status: JobRunNotificationStatus,
    pub error: String,
    pub created_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
}


impl CRUD {
    pub async fn insert_job_run_notification<'e, E>(&self, executor: E, data: &InsertJobRunNotificationData) -> anyhow::Result<i64>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let res = sqlx::query(
            "INSERT INTO job_run_notification (job_run_id, job_id, notify_on, channel, recipients, status, error, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
            .bind(data.input.job_run_id)
            .bind(&data.input.job_id)
            .bind(&data.input.notify_on)
            .bind(&data.input.channel)
            .bind(sqlx::types::Json(&data.input.recipients))
            .bind(&data.input.status)
            .bind(&data.input.error)
            .bind(self.toolkit.get_current_ts())
            .execute(executor)
            .await?;

        Ok(res.last_insert_rowid())
    }

    pub async fn select_job_run_notification<'e, E>(&self, executor: E, data: &SelectJobRunNotificationsData) -> anyhow::Result<Option<JobRunNotification>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let notifications = self.select_job_run_notifications(executor, data).await?;

        Ok(notifications.into_iter().next())
    }

    pub async fn select_job_run_notifications<'e, E>(&self, executor: E, data: &SelectJobRunNotificationsData) -> anyhow::Result<Vec<JobRunNotification>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "SELECT id, job_run_id, job_id, notify_on, channel, recipients, status, error, created_at, sent_at FROM job_run_notification WHERE 1=1"
        );

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(job_run_id) = &data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
        }

        if let Some(notify_on) = &data.filter.notify_on {
            query_builder.push(" AND notify_on = ");
            query_builder.push_bind(notify_on);
        }

        if let Some(channel) = &data.filter.channel {
            query_builder.push(" AND channel = ");
            query_builder.push_bind(channel);
        }

        if let Some(status) = &data.filter.status {
            query_builder.push(" AND status = ");
            query_builder.push_bind(status);
        }

        if let Some(sort) = &data.sort {
            match sort {
                SelectJobRunNotificationsDataSort::Id => {
                    query_builder.push(" ORDER BY id ASC");
                }
            }
        }

        if let Some(limit) = data.limit {
            query_builder.push(" LIMIT ");
            query_builder.push_bind(limit);
        }

        if let Some(offset) = data.offset {
            query_builder.push(" OFFSET ");
            query_builder.push_bind(offset);
        }

        let notifications = query_builder
            .build_query_as::<JobRunNotification>()
            .fetch_all(executor)
            .await?;

        Ok(notifications)
    }

    pub async fn update_job_run_notifications<'e, E>(&self, executor: E, data: &UpdateJobRunNotificationsData) -> anyhow::Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        if data.input.status.is_none()
            && data.input.error.is_none()
            && data.input.sent_at.is_none()
        {
            return Ok(());
        }

        let mut query_builder: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new("UPDATE job_run_notification SET ");

        let mut separated = query_builder.separated(", ");

        if let Some(status) = &data.input.status {
            separated.push("status = ");
            separated.push_bind_unseparated(status);
        }

        if let Some(error) = &data.input.error {
            separated.push("error = ");
            separated.push_bind_unseparated(error);
        }

        if let Some(sent_at) = &data.input.sent_at {
            separated.push("sent_at = ");
            separated.push_bind_unseparated(sent_at);
        }

        query_builder.push(" WHERE 1=1");

        if let Some(id) = data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        query_builder.build().execute(executor).await?;

        Ok(())
    }

}
