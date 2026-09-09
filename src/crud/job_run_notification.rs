use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::crud::CRUD;

/// Where one notification has got to. `JobRunMonitor` writes Pending as it finishes a run
/// that asked to be told about a failure, and `NotificationService` moves it to Sent or Failed
/// once it has tried to send it.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum JobRunNotificationStatus {
    Pending,
    Sent,
    Failed,
}

impl std::fmt::Display for JobRunNotificationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobRunNotificationStatus::Pending => write!(f, "pending"),
            JobRunNotificationStatus::Sent => write!(f, "sent"),
            JobRunNotificationStatus::Failed => write!(f, "failed"),
        }
    }
}

/// How a notification reaches somebody. A column rather than something the sender infers
/// from the recipients, so one row says for itself what delivering it means — and a second
/// channel is a variant here plus an arm the compiler then demands.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum NotificationChannel {
    Email,
}

impl std::fmt::Display for NotificationChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NotificationChannel::Email => write!(f, "email"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertJobRunNotificationDataInput {
    pub job_run_id: i64,
    pub job_id: String,
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
            "INSERT INTO job_run_notification (job_run_id, job_id, channel, recipients, status, error, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
            .bind(data.input.job_run_id)
            .bind(&data.input.job_id)
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
            "SELECT id, job_run_id, job_id, channel, recipients, status, error, created_at, sent_at FROM job_run_notification WHERE 1=1"
        );

        if let Some(id) = &data.filter.id {
            query_builder.push(" AND id = ");
            query_builder.push_bind(id);
        }

        if let Some(job_run_id) = &data.filter.job_run_id {
            query_builder.push(" AND job_run_id = ");
            query_builder.push_bind(job_run_id);
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
