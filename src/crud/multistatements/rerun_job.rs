//! `rerun_job`: a fresh run of the definition an earlier run executed, not of whatever
//! the YAML says now - which is why a run whose job has since been deleted is rerunnable.

use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job_run::{SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_notification::{SelectJobRunNotificationsData, SelectJobRunNotificationsDataFilter, SelectJobRunNotificationsDataSort};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort};

use super::job_run_definition::{JobRunDefinition, JobRunNotificationDefinition, JobRunTaskDefinition};

impl CRUD {

    /// Submits a fresh run of the job the given run belongs to, whatever state that run
    /// is in. What gets run is the definition that run executed, not whatever the YAML
    /// says now - so a rerun of an old run is a rerun of the old config. Nothing here
    /// reads the config at all, which is why a run whose job YAML has since been deleted
    /// is still rerunnable.
    pub async fn rerun_job(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
    ) -> anyhow::Result<i64> {

        let job_run = self.select_job_run(
            &mut *conn,
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

        let Some(job_run) = job_run else {
            anyhow::bail!("Job run {} not found", job_run_id);
        };

        let task_runs = self.select_task_runs(
            &mut *conn,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(job_run_id),
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunsDataSort::Id),
            }
        ).await?;

        let notifications = self.select_job_run_notifications(
            &mut *conn,
            &SelectJobRunNotificationsData {
                filter: SelectJobRunNotificationsDataFilter {
                    id: None,
                    job_run_id: Some(job_run_id),
                    notify_on: None,
                    channel: None,
                    status: None,
                },
                sort: Some(SelectJobRunNotificationsDataSort::Id),
                limit: None,
                offset: None,
            }
        ).await?;

        let definition = JobRunDefinition {
            job_id: job_run.job_id,
            job_name: job_run.job_name,
            job_description: job_run.job_description,
            parameters: job_run.parameters.0.clone(),
            scheduled_at: job_run.scheduled_at,
            tasks: task_runs
                .into_iter()
                .map(|task_run| JobRunTaskDefinition {
                    task_id: task_run.task_id,
                    command: task_run.command,
                    depends_on: task_run.depends_on.0,
                    limits: task_run.limits.0,
                    timeout: task_run.timeout,
                    max_retries: task_run.max_retries,
                    retry_delay: task_run.retry_delay,
                    env: task_run.env.0.clone(),
                    secret_env: task_run.secret_env.0.clone(),
                    working_dir: task_run.working_dir.clone(),
                })
                .collect(),
            notifications: notifications
                .into_iter()
                .map(|notification| JobRunNotificationDefinition {
                    notify_on: notification.notify_on,
                    channel: notification.channel,
                    recipients: notification.recipients.0.clone(),
                })
                .collect(),
        };

        self.insert_job_run_definition(&mut *conn, &definition).await
    }
}

#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::job_run_notification::{JobRunNotificationStatus, NotificationChannel, NotifyOn};
    use crate::test_support::{map, TestDb};


    /// A rerun replays the run's own inputs. Nothing here reads config, so the rerun of a
    /// scheduled run stays a run for the same slice and the same instant - rerunning
    /// yesterday's failed daily job reruns it for yesterday.
    #[tokio::test]
    async fn a_rerun_replays_the_original_parameters_env_and_scheduled_at() {

        let db = TestDb::new().await;

        let scheduled_at = chrono::Utc::now() - chrono::TimeDelta::days(1);

        let job_run = db.insert_job_run_with_parameters(
            JobRunStatus::Failed,
            map(&[("region", "us")]),
            Some(scheduled_at),
        ).await;

        db.insert_task_run_for_command_with_env(
            job_run.id,
            "echo hi",
            map(&[("PYTHONUNBUFFERED", "1")]),
            "/tmp",
        ).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let rerun_id = db.crud.rerun_job(&mut conn, job_run.id).await.unwrap();

        let rerun = db.job_run(rerun_id).await;

        assert_eq!(rerun.parameters.0.get("region").unwrap(), "us");
        assert_eq!(
            rerun.scheduled_at.unwrap().timestamp_millis(),
            scheduled_at.timestamp_millis(),
        );

        let task_runs = db.crud.select_task_runs(
            &*db.conn_pool,
            &crate::crud::task_run::SelectTaskRunsData {
                filter: crate::crud::task_run::SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(rerun_id),
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            },
        ).await.unwrap();

        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].env.0.get("PYTHONUNBUFFERED").unwrap(), "1");
        assert_eq!(task_runs[0].working_dir, "/tmp");
        assert_eq!(task_runs[0].status, TaskRunStatus::Pending);
    }

    /// A rerun replays the concurrency limits the original run was submitted with, so it
    /// queues behind the same resources the first attempt did.
    ///
    /// This is the one field here that has already been wrong once: every insert path
    /// bound an empty vec when `task_run.limits` was introduced, and a rerun would have
    /// been admitted claiming nothing — running outside the limit, invisibly, on the path
    /// a human reaches by clicking rerun on a failed nightly job. Nothing failed when that
    /// line was reverted, which is why the assertion exists rather than being left to the
    /// fields around it.
    #[tokio::test]
    async fn a_rerun_replays_the_original_limits() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;

        db.insert_task_run_with_limits(
            job_run.id,
            vec!["openai_api".to_string(), "warehouse".to_string()],
        ).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let rerun_id = db.crud.rerun_job(&mut conn, job_run.id).await.unwrap();

        let task_runs = db.crud.select_task_runs(
            &*db.conn_pool,
            &crate::crud::task_run::SelectTaskRunsData {
                filter: crate::crud::task_run::SelectTaskRunsDataFilter {
                    id: None,
                    job_run_id: Some(rerun_id),
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            },
        ).await.unwrap();

        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].limits.0, vec!["openai_api".to_string(), "warehouse".to_string()]);
    }

    /// A rerun replays who to tell along with everything else it replays: the original
    /// run's own notifications, not whatever the job's YAML says now — which is what
    /// keeps a rerun of a deleted job notifiable at all.
    #[tokio::test]
    async fn a_rerun_replays_the_original_notifications() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;

        db.insert_task_run(job_run.id, TaskRunStatus::Failed).await;

        db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Failure,
            NotificationChannel::Email,
            &["oncall@example.com"],
        ).await;

        db.insert_job_run_notification(
            job_run.id,
            NotifyOn::Success,
            NotificationChannel::Slack,
            &["#oncall"],
        ).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let rerun_id = db.crud.rerun_job(&mut conn, job_run.id).await.unwrap();

        let notifications = db.job_run_notifications(rerun_id).await;

        assert_eq!(notifications.len(), 2);
        assert_eq!(notifications[0].notify_on, NotifyOn::Failure);
        assert_eq!(notifications[0].channel, NotificationChannel::Email);
        assert_eq!(notifications[0].recipients.0, vec!["oncall@example.com"]);
        assert_eq!(notifications[1].notify_on, NotifyOn::Success);
        assert_eq!(notifications[1].channel, NotificationChannel::Slack);
        assert_eq!(notifications[1].recipients.0, vec!["#oncall"]);

        // Open again, so the rerun is judged on its own outcome rather than inheriting one.
        assert_eq!(notifications[0].status, JobRunNotificationStatus::Pending);
        assert_eq!(notifications[0].sent_at, None);
    }
}
