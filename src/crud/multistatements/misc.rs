use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_notification::{InsertJobRunNotificationData, InsertJobRunNotificationDataInput, JobRunNotificationStatus, NotificationChannel, NotifyOn, SelectJobRunNotificationsData, SelectJobRunNotificationsDataFilter, SelectJobRunNotificationsDataSort};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRunStatus};


/// A job's definition, as one job run will execute it. `submit_job` builds it from the
/// config the YAML declares now and `rerun_job` from an earlier run's snapshot, and the
/// insert treats both alike: where a definition came from is the caller's business.
struct JobRunDefinition {
    job_id: String,
    job_name: String,
    job_description: String,
    parameters: BTreeMap<String, String>,
    scheduled_at: Option<DateTime<Utc>>,
    tasks: Vec<JobRunTaskDefinition>,
    notifications: Vec<JobRunNotificationDefinition>,
}

/// Somebody to tell about this run, written when the run is created rather than when it
/// ends. Nothing here knows yet whether it will be needed — whether the run ends the way
/// `notify_on` is waiting for is the notification service's question, once it has ended.
struct JobRunNotificationDefinition {
    notify_on: NotifyOn,
    channel: NotificationChannel,
    recipients: Vec<String>,
}

struct JobRunTaskDefinition {
    task_id: String,
    command: String,
    depends_on: Vec<String>,
    timeout: u32,
    max_retries: u32,
    retry_delay: u32,
    env: BTreeMap<String, String>,
    secret_env: BTreeMap<String, String>,
    working_dir: String,
}

/// The parameters one run will carry: the job's declared defaults, with the caller's
/// overrides applied.
///
/// An override the job does not declare raises rather than being passed through. A
/// schedule or a --param naming a parameter that isn't there is a typo, and a typo that
/// delivers an unset variable to a command is the one outcome you cannot debug from the
/// row afterwards.
pub fn resolve_job_parameters(
    job_id: &str,
    declared: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {

    let mut parameters = declared.clone();

    for (name, value) in overrides {

        if !declared.contains_key(name) {

            let declared_names = if declared.is_empty() {
                "none".to_string()
            } else {
                declared.keys().cloned().collect::<Vec<String>>().join(", ")
            };

            anyhow::bail!(
                "Job '{}' does not declare a parameter '{}'. Declared: {}",
                job_id,
                name,
                declared_names,
            );
        }

        parameters.insert(name.clone(), value.clone());
    }

    for name in parameters.keys() {
        if !is_valid_parameter_name(name) {
            anyhow::bail!(
                "Job '{}' has a parameter named '{}', which is not a valid environment \
                 variable name. A parameter name may contain only ASCII letters, digits \
                 and underscores, and may not start with a digit.",
                job_id,
                name,
            );
        }
    }

    // Two names that only differ in case become the same FLOWLITE_PARAM_ env var, and a
    // BTreeMap can only hold one of them - so this has to be caught here rather than left
    // to silently drop one value at spawn time.
    let mut seen_env_names: BTreeMap<String, &String> = BTreeMap::new();

    for name in parameters.keys() {
        let env_name = name.to_ascii_uppercase();

        if let Some(other_name) = seen_env_names.insert(env_name.clone(), name) {
            anyhow::bail!(
                "Job '{}' declares parameters '{}' and '{}', which both become the \
                 environment variable FLOWLITE_PARAM_{}. Parameter names must be distinct \
                 once uppercased.",
                job_id,
                other_name,
                name,
                env_name,
            );
        }
    }

    Ok(parameters)
}

/// Merges a job-level declaration with a task's own, the task's own winning any name both
/// set. Used for both `env` and `secret_env`: each is declared at the job level and may be
/// overridden per task, and the merge rule is identical either way.
///
/// Merged here rather than at spawn so the run snapshots what it will actually run with,
/// and so the ordering between the two declarations is decided once, in the place that
/// builds the definition, instead of becoming a fourth layer the spawn site has to keep
/// in the right order forever.
fn merge_job_and_task_maps(
    job_map: &BTreeMap<String, String>,
    task_map: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {

    let mut merged = job_map.clone();

    for (name, value) in task_map {
        merged.insert(name.clone(), value.clone());
    }

    merged
}


fn is_valid_parameter_name(name: &str) -> bool {
    let mut chars = name.chars();

    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The notifications one run is submitted with: one per channel each block named somebody
/// under. A job that names nobody at all gets none.
///
/// A job asking to be told both ways over the same channel gets two rows, settled
/// separately — the run can only end one way, so exactly one of them is ever delivered and
/// the other closes as skipped.
fn job_run_notification_definitions(
    on_failure_recipients: &BTreeMap<NotificationChannel, Vec<String>>,
    on_success_recipients: &BTreeMap<NotificationChannel, Vec<String>>,
) -> Vec<JobRunNotificationDefinition> {

    let blocks = [
        (NotifyOn::Failure, on_failure_recipients),
        (NotifyOn::Success, on_success_recipients),
    ];

    blocks
        .into_iter()
        .flat_map(|(notify_on, block)| {
            block
                .iter()
                .map(move |(channel, recipients)| JobRunNotificationDefinition {
                    notify_on,
                    channel: *channel,
                    recipients: recipients.clone(),
                })
        })
        .collect()
}


/// Operations that span more than one entity, and so belong to no single entity file.
impl CRUD {

    /// Submits a run of the job's current definition. The definition is snapshotted onto
    /// the run's own rows, so what the run executes can no longer change under it -
    /// not when the YAML is edited, and not when the process restarts mid-run.
    ///
    /// `overrides` are the caller's parameter values, checked against what the job
    /// declares. `scheduled_at` is the instant a schedule fired for, and None for a
    /// manual submission - a manual run has no scheduled instant, and the spawned command
    /// gets no FLOWLITE_SCHEDULED_AT rather than a misleading copy of created_at.
    ///
    /// A job with no config is an error rather than an empty run: the caller asked for a
    /// job that isn't there.
    pub async fn submit_job(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
        overrides: &BTreeMap<String, String>,
        scheduled_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<i64> {

        let job = self.select_job(&mut *conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: Some(job_id.to_string()),
                name_like: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        let Some(job) = job else {
            anyhow::bail!("Job '{}' not found", job_id);
        };

        let parameters = resolve_job_parameters(job_id, &job.parameters.0, overrides)?;

        let job_env = job.env.0.clone();
        let job_secret_env = job.secret_env.0.clone();

        let tasks = self.select_tasks(&mut *conn, &SelectTasksData {
            filter: SelectTasksDataFilter {
                task_id: None,
                job_id: Some(job_id.to_string()),
            },
            sort: Some(SelectTasksDataSort::RowId),
            limit: None,
            offset: None,
        }).await?;

        let definition = JobRunDefinition {
            job_id: job.job_id,
            job_name: job.name,
            job_description: job.description,
            parameters,
            scheduled_at,
            tasks: tasks
                .into_iter()
                .map(|task| JobRunTaskDefinition {
                    task_id: task.task_id,
                    command: task.command,
                    depends_on: task.depends_on.0,
                    timeout: task.timeout,
                    max_retries: task.max_retries,
                    retry_delay: task.retry_delay,
                    env: merge_job_and_task_maps(&job_env, &task.env.0),
                    secret_env: merge_job_and_task_maps(&job_secret_env, &task.secret_env.0),
                    working_dir: task.working_dir.clone(),
                })
                .collect(),
            notifications: job_run_notification_definitions(
                &job.on_failure_recipients.0,
                &job.on_success_recipients.0,
            ),
        };

        self.insert_job_run_definition(&mut *conn, &definition).await
    }

    /// Inserts a pending job run, one pending task run per task, and one open notification
    /// per channel each of the job's notify blocks named. This is the only place a run's
    /// config is written.
    async fn insert_job_run_definition(
        &self,
        conn: &mut SqliteConnection,
        definition: &JobRunDefinition,
    ) -> anyhow::Result<i64> {

        let job_run_id = self.insert_job_run(
            &mut *conn,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: definition.job_id.clone(),
                    job_name: definition.job_name.clone(),
                    job_description: definition.job_description.clone(),
                    parameters: definition.parameters.clone(),
                    scheduled_at: definition.scheduled_at,
                    status: JobRunStatus::Pending,
                }
            }
        ).await?;

        for task in definition.tasks.iter() {
            self.insert_task_run(
                &mut *conn,
                &InsertTaskRunData {
                    input: InsertTaskRunDataInput {
                        job_run_id,
                        job_id: definition.job_id.clone(),
                        task_id: task.task_id.clone(),
                        command: task.command.clone(),
                        depends_on: task.depends_on.clone(),
                        timeout: task.timeout,
                        max_retries: task.max_retries,
                        retry_delay: task.retry_delay,
                        env: task.env.clone(),
                        secret_env: task.secret_env.clone(),
                        working_dir: task.working_dir.clone(),
                        status: TaskRunStatus::Pending,
                    }
                }
            ).await?;
        }

        for notification in definition.notifications.iter() {
            self.insert_job_run_notification(
                &mut *conn,
                &InsertJobRunNotificationData {
                    input: InsertJobRunNotificationDataInput {
                        job_run_id,
                        job_id: definition.job_id.clone(),
                        notify_on: notification.notify_on,
                        channel: notification.channel,
                        recipients: notification.recipients.clone(),
                        status: JobRunNotificationStatus::Pending,
                        error: String::new(),
                    }
                }
            ).await?;
        }

        Ok(job_run_id)
    }

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
                    timeout: task_run.timeout,
                    max_retries: task_run.max_retries,
                    retry_delay: task_run.retry_delay,
                    env: task_run.env.0.clone(),
                    secret_env: task_run.secret_env.clone().map(|json| json.0).unwrap_or_default(),
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

    /// Whether the job already has as many runs in flight as it allows. Only a running
    /// job run holds a slot — a pending one is waiting for exactly this answer — and a
    /// max_parallel_runs of 0 means the job has no limit at all.
    pub async fn is_job_at_max_parallel_runs(
        &self,
        conn: &mut SqliteConnection,
        job_id: &str,
    ) -> anyhow::Result<bool> {

        let job = self.select_job(&mut *conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: Some(job_id.to_string()),
                name_like: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        let Some(job) = job else {
            return Ok(false);
        };

        if job.max_parallel_runs == 0 {
            return Ok(false);
        }

        let running_job_runs = self.select_job_runs(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: Some(job_id.to_string()),
                status: Some(JobRunStatus::Running),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        Ok(running_job_runs.len() >= job.max_parallel_runs as usize)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn a_declared_default_is_carried_when_nothing_overrides_it() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region", "eu")]),
            &map(&[]),
        ).unwrap();

        assert_eq!(resolved.get("region").unwrap(), "eu");
    }

    #[test]
    fn an_override_replaces_the_default() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region", "eu")]),
            &map(&[("region", "us")]),
        ).unwrap();

        assert_eq!(resolved.get("region").unwrap(), "us");
    }

    #[test]
    fn the_parameters_the_override_does_not_mention_keep_their_defaults() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region", "eu"), ("slice", "")]),
            &map(&[("region", "us")]),
        ).unwrap();

        assert_eq!(resolved.get("slice").unwrap(), "");
        assert_eq!(resolved.len(), 2);
    }

    /// The whole reason declaration is worth having: a typo in a schedule is reported
    /// rather than silently delivering nothing to the command.
    #[test]
    fn an_undeclared_override_raises_naming_the_job_the_key_and_the_declared_names() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("region", "eu"), ("slice", "")]),
            &map(&[("regoin", "us")]),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("regoin"), "{error}");
        assert!(error.contains("region"), "{error}");
        assert!(error.contains("slice"), "{error}");
    }

    #[test]
    fn an_override_of_a_job_declaring_nothing_says_so() {
        let error = resolve_job_parameters(
            "job",
            &map(&[]),
            &map(&[("region", "us")]),
        ).unwrap_err().to_string();

        assert!(error.contains("none"), "{error}");
    }

    #[test]
    fn a_job_declaring_nothing_resolves_to_nothing() {
        assert!(resolve_job_parameters("job", &map(&[]), &map(&[])).unwrap().is_empty());
    }

    /// Hyphenated identifiers are this repo's own naming convention, so `my-param` is the
    /// natural thing for a user to declare - and it is exactly the name `sh` cannot read
    /// out of an env var.
    #[test]
    fn a_hyphenated_parameter_name_is_rejected() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("my-param", "v")]),
            &map(&[]),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("my-param"), "{error}");
    }

    #[test]
    fn a_parameter_name_starting_with_a_digit_is_rejected() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("1region", "v")]),
            &map(&[]),
        ).unwrap_err().to_string();

        assert!(error.contains("1region"), "{error}");
    }

    #[test]
    fn a_parameter_name_with_underscores_and_digits_is_accepted() {
        let resolved = resolve_job_parameters(
            "job",
            &map(&[("region_2", "eu")]),
            &map(&[]),
        ).unwrap();

        assert_eq!(resolved.get("region_2").unwrap(), "eu");
    }

    /// `region` and `REGION` are distinct declared parameters but the same env var once
    /// uppercased, and one of them would otherwise silently lose its value.
    #[test]
    fn two_names_differing_only_in_case_are_rejected_as_a_collision() {
        let error = resolve_job_parameters(
            "daily-etl",
            &map(&[("region", "eu"), ("REGION", "us")]),
            &map(&[]),
        ).unwrap_err().to_string();

        assert!(error.contains("daily-etl"), "{error}");
        assert!(error.contains("region"), "{error}");
        assert!(error.contains("REGION"), "{error}");
        assert!(error.contains("FLOWLITE_PARAM_REGION"), "{error}");
    }

    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::TestDb;

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

    fn recipients(pairs: &[(NotificationChannel, &[&str])]) -> BTreeMap<NotificationChannel, Vec<String>> {
        pairs
            .iter()
            .map(|(channel, recipients)| (
                *channel,
                recipients.iter().map(|r| r.to_string()).collect(),
            ))
            .collect()
    }

    /// A job that names nobody gets no notification at all, rather than an empty one
    /// every pass has to look at and decide about.
    #[test]
    fn a_job_naming_nobody_is_submitted_with_no_notifications() {
        assert!(job_run_notification_definitions(&recipients(&[]), &recipients(&[])).is_empty());
    }

    #[test]
    fn the_addresses_a_job_names_become_one_email_notification() {

        let definitions = job_run_notification_definitions(
            &recipients(&[
                (NotificationChannel::Email, &["oncall@example.com", "data@example.com"]),
            ]),
            &recipients(&[]),
        );

        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].notify_on, NotifyOn::Failure);
        assert_eq!(definitions[0].channel, NotificationChannel::Email);
        assert_eq!(definitions[0].recipients.len(), 2);
    }

    /// The success block produces the same rows as the failure one, marked for the ending
    /// it is waiting for.
    #[test]
    fn the_addresses_a_success_block_names_become_a_success_notification() {

        let definitions = job_run_notification_definitions(
            &recipients(&[]),
            &recipients(&[(NotificationChannel::Slack, &["#data"])]),
        );

        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].notify_on, NotifyOn::Success);
        assert_eq!(definitions[0].channel, NotificationChannel::Slack);
        assert_eq!(definitions[0].recipients, vec!["#data"]);
    }

    /// A job wanting to hear either way over the same channel gets a row for each. Only
    /// one of them can ever be delivered — the run ends once — and the other closes as
    /// skipped, which is what keeps the decision on the row rather than in the sender.
    #[test]
    fn a_job_naming_both_endings_is_submitted_with_a_row_for_each() {

        let definitions = job_run_notification_definitions(
            &recipients(&[(NotificationChannel::Email, &["oncall@example.com"])]),
            &recipients(&[(NotificationChannel::Email, &["oncall@example.com"])]),
        );

        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].notify_on, NotifyOn::Failure);
        assert_eq!(definitions[1].notify_on, NotifyOn::Success);
    }

    /// A run is told over every channel its job named, and each channel gets its own row —
    /// so a Slack post that fails does not take the mail down with it, and each is
    /// recorded separately.
    #[test]
    fn a_job_naming_two_channels_is_submitted_with_one_notification_each() {

        let definitions = job_run_notification_definitions(
            &recipients(&[
                (NotificationChannel::Email, &["oncall@example.com"]),
                (NotificationChannel::Slack, &["#oncall"]),
            ]),
            &recipients(&[]),
        );

        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].channel, NotificationChannel::Email);
        assert_eq!(definitions[1].channel, NotificationChannel::Slack);
        assert_eq!(definitions[1].recipients, vec!["#oncall"]);
    }

    #[test]
    fn a_job_value_reaches_a_task_that_declares_none() {
        let merged = merge_job_and_task_maps(&map(&[("TZ", "UTC")]), &map(&[]));

        assert_eq!(merged.get("TZ").unwrap(), "UTC");
    }

    #[test]
    fn a_task_value_is_kept_when_the_job_declares_none() {
        let merged = merge_job_and_task_maps(&map(&[]), &map(&[("LC_ALL", "C")]));

        assert_eq!(merged.get("LC_ALL").unwrap(), "C");
    }

    /// The point of the whole merge: the task is the more specific declaration, so it
    /// wins the name both of them set. True of `env` and of `secret_env` alike.
    #[test]
    fn a_task_value_overrides_the_job_on_the_same_name() {
        let merged = merge_job_and_task_maps(
            &map(&[("TZ", "UTC")]),
            &map(&[("TZ", "Europe/Vienna")]),
        );

        assert_eq!(merged.get("TZ").unwrap(), "Europe/Vienna");
    }

    #[test]
    fn the_names_only_one_of_them_sets_all_survive_the_merge() {
        let merged = merge_job_and_task_maps(
            &map(&[("TZ", "UTC"), ("SHARED", "job")]),
            &map(&[("LC_ALL", "C"), ("SHARED", "task")]),
        );

        assert_eq!(merged.get("TZ").unwrap(), "UTC");
        assert_eq!(merged.get("LC_ALL").unwrap(), "C");
        assert_eq!(merged.get("SHARED").unwrap(), "task");
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn a_job_and_task_both_declaring_nothing_merge_to_nothing() {
        assert!(merge_job_and_task_maps(&map(&[]), &map(&[])).is_empty());
    }

    /// One task's definition, with `secret_env` the only field a case varies - everything
    /// else is filler a `task_run` row needs to exist at all.
    fn task_definition(secret_env: BTreeMap<String, String>) -> JobRunTaskDefinition {
        JobRunTaskDefinition {
            task_id: "task".to_string(),
            command: "true".to_string(),
            depends_on: Vec::new(),
            timeout: 60,
            max_retries: 0,
            retry_delay: 60,
            env: BTreeMap::new(),
            secret_env,
            working_dir: String::new(),
        }
    }

    /// Inserts one job run definition carrying a single task, and reads back that task's
    /// `secret_env` off the `task_run` row `insert_job_run_definition` wrote.
    ///
    /// Goes through `insert_job_run_definition` rather than `submit_job` itself, so the
    /// test needs no `mem.job`/`mem.task` rows: `mem` is one shared-cache database for the
    /// whole test binary, and seeding it from a test races any other test's pooled
    /// connection over its schema lock - confirmed by running exactly that here, which
    /// broke two unrelated tests with "database schema is locked: mem". `submit_job`'s own
    /// contribution once `mem` is read is one line, `merge_job_and_task_maps(&job_secret_env,
    /// &task.secret_env.0)`, identical in shape to the `env` line beside it.
    async fn submitted_secret_env(db: &TestDb, secret_env: BTreeMap<String, String>) -> BTreeMap<String, String> {

        let definition = JobRunDefinition {
            job_id: "job".to_string(),
            job_name: "Job".to_string(),
            job_description: String::new(),
            parameters: BTreeMap::new(),
            scheduled_at: None,
            tasks: vec![task_definition(secret_env)],
            notifications: Vec::new(),
        };

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let job_run_id = db.crud.insert_job_run_definition(&mut conn, &definition).await.unwrap();

        let task_runs = db.crud.select_task_runs(&*db.conn_pool, &SelectTaskRunsData {
            filter: SelectTaskRunsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: None,
                status: None,
            },
            sort: None,
        }).await.unwrap();

        assert_eq!(task_runs.len(), 1);
        assert!(task_runs[0].secret_env.is_some(), "an inserted task_run's secret_env is never NULL");

        task_runs[0].secret_env.clone().unwrap().0
    }

    /// The reference travels all the way from the two levels' `secret_env:` onto the
    /// submitted `task_run`, and a name both levels declare is settled the same way `env`
    /// settles it: the task's own wins.
    #[tokio::test]
    async fn a_submitted_run_carries_the_merged_secret_env_with_the_task_winning_a_shared_name() {

        let db = TestDb::new().await;

        let job_secret_env = map(&[("DB_PASSWORD", "job_db_password"), ("SHARED", "job_shared_secret")]);
        let task_secret_env = map(&[("API_KEY", "task_api_key"), ("SHARED", "task_shared_secret")]);

        let secret_env = submitted_secret_env(
            &db,
            merge_job_and_task_maps(&job_secret_env, &task_secret_env),
        ).await;

        assert_eq!(secret_env.get("DB_PASSWORD").unwrap(), "job_db_password");
        assert_eq!(secret_env.get("API_KEY").unwrap(), "task_api_key");
        assert_eq!(secret_env.get("SHARED").unwrap(), "task_shared_secret");
        assert_eq!(secret_env.len(), 3);
    }

    /// A job naming no secret at either level submits a run with an empty map, not a null
    /// one - the insert always writes a map, so `NULL` stays a state only a pre-migration
    /// row can be in.
    #[tokio::test]
    async fn a_job_with_no_secret_env_submits_a_run_whose_map_is_empty() {

        let db = TestDb::new().await;

        let secret_env = submitted_secret_env(&db, merge_job_and_task_maps(&map(&[]), &map(&[]))).await;

        assert!(secret_env.is_empty());
    }
}
