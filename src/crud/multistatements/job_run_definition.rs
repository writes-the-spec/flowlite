//! The definition one job run will execute, and the insert that writes it.
//!
//! `submit_job` builds one from the config the YAML declares now and `rerun_job` from an
//! earlier run's snapshot; `insert_job_run_definition` treats both alike, because where a
//! definition came from is the caller's business. Everything here is `pub(super)` for that
//! reason - it is the vocabulary those two operations share, and nothing outside this
//! module constructs it.

use std::collections::{BTreeMap, BTreeSet};
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::Job;
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRunStatus};
use crate::crud::job_run_notification::{InsertJobRunNotificationData, InsertJobRunNotificationDataInput, JobRunNotificationStatus, NotificationChannel, NotifyOn};
use crate::crud::task::Task;
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, TaskRunStatus};

/// A job's definition, as one job run will execute it. `submit_job` builds it from the
/// config the YAML declares now and `rerun_job` from an earlier run's snapshot, and the
/// insert treats both alike: where a definition came from is the caller's business.
pub(super) struct JobRunDefinition {
    pub(super) job_id: String,
    pub(super) job_name: String,
    pub(super) job_description: String,
    pub(super) parameters: BTreeMap<String, String>,
    pub(super) scheduled_at: Option<DateTime<Utc>>,
    pub(super) tasks: Vec<JobRunTaskDefinition>,
    pub(super) notifications: Vec<JobRunNotificationDefinition>,
}

/// Somebody to tell about this run, written when the run is created rather than when it
/// ends. Nothing here knows yet whether it will be needed — whether the run ends the way
/// `notify_on` is waiting for is the notification service's question, once it has ended.
pub(super) struct JobRunNotificationDefinition {
    pub(super) notify_on: NotifyOn,
    pub(super) channel: NotificationChannel,
    pub(super) recipients: Vec<String>,
}

pub(super) struct JobRunTaskDefinition {
    pub(super) task_id: String,
    pub(super) command: String,
    pub(super) depends_on: Vec<String>,
    pub(super) limits: Vec<String>,
    pub(super) timeout: u32,
    pub(super) max_retries: u32,
    pub(super) retry_delay: u32,
    pub(super) env: BTreeMap<String, String>,
    pub(super) secret_env: BTreeMap<String, String>,
    pub(super) working_dir: String,
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

/// Claims a task run stores at submit time: the job's own `limits` plus the task's own,
/// as one set. Unlike `merge_job_and_task_maps`, there is no name to win here - claiming
/// `warehouse` and `openai_api` is strictly more constrained than claiming either alone, so
/// the two lists union rather than one overriding the other. A `BTreeSet` gives the dedup
/// and the sort the stored snapshot needs in one step: a claim list that reordered between
/// runs would be a diff nobody could read.
fn union_job_and_task_limits(job_limits: &[String], task_limits: &[String]) -> Vec<String> {

    job_limits
        .iter()
        .chain(task_limits)
        .cloned()
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect()
}

/// One task's definition as `submit_job` builds it: `env` and `secret_env` each merged
/// from the job's own and the task's own, with a guard `merge_job_and_task_maps` alone
/// cannot provide.
///
/// `validate_secret_env_block` promises a name never appears in both `env` and
/// `secret_env` of one declaration - but it only checks each level in isolation, so a task
/// overriding a job's `env` name in its own `secret_env` (or vice versa) is a cross-level
/// case that check cannot see. Merging the two blocks independently would store both
/// values under the same name with no record of which level either came from, leaving
/// nothing downstream able to tell which one the task actually meant. So the task's own
/// declaration wins the name outright, in whichever block it appears: a name the task
/// declares in its `env` is dropped from the merged `secret_env`, and a name it declares in
/// its `secret_env` is dropped from the merged `env`. A name neither block of the task
/// declares cannot collide, because the same-level check already rejects a job or task
/// declaring one name in both of its own blocks.
///
/// The job side is the whole `Job` rather than its two maps: `env` and `secret_env` are
/// both `BTreeMap<String, String>`, so passing them positionally made transposing them a
/// change that compiled and left every test passing, while silently swapping which block
/// each name landed in - and a plain value moved into `secret_env` is a name resolved
/// against the secrets map. Taking the row makes that swap a type error.
pub(super) fn job_run_task_definition(
    task: &Task,
    job: &Job,
) -> JobRunTaskDefinition {

    let mut env = merge_job_and_task_maps(&job.env.0, &task.env.0);
    let mut secret_env = merge_job_and_task_maps(&job.secret_env.0, &task.secret_env.0);

    for name in task.env.0.keys() {
        secret_env.remove(name);
    }

    for name in task.secret_env.0.keys() {
        env.remove(name);
    }

    JobRunTaskDefinition {
        task_id: task.task_id.clone(),
        command: task.command.clone(),
        depends_on: task.depends_on.0.clone(),
        limits: union_job_and_task_limits(&job.limits.0, &task.limits.0),
        timeout: task.timeout,
        max_retries: task.max_retries,
        retry_delay: task.retry_delay,
        env,
        secret_env,
        working_dir: task.working_dir.clone(),
    }
}

/// The notifications one run is submitted with: one per channel each block named somebody
/// under. A job that names nobody at all gets none.
///
/// A job asking to be told both ways over the same channel gets two rows, settled
/// separately — the run can only end one way, so exactly one of them is ever delivered and
/// the other closes as skipped.
pub(super) fn job_run_notification_definitions(
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

impl CRUD {

    /// Inserts a pending job run, one pending task run per task, and one open notification
    /// per channel each of the job's notify blocks named. This is the only place a run's
    /// config is written.
    pub(super) async fn insert_job_run_definition(
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
                        limits: task.limits.clone(),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter};
    use crate::test_support::{map, TestDb};


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

    /// A `mem.task` row with the given `env`/`secret_env`/`limits` - everything else is
    /// filler a `Task` needs to exist at all.
    fn task_row(env: BTreeMap<String, String>, secret_env: BTreeMap<String, String>, limits: Vec<String>) -> Task {
        Task {
            task_id: "task".to_string(),
            job_id: "job".to_string(),
            description: String::new(),
            command: "true".to_string(),
            depends_on: sqlx::types::Json(Vec::new()),
            limits: sqlx::types::Json(limits),
            timeout: 60,
            max_retries: 0,
            retry_delay: 60,
            env: sqlx::types::Json(env),
            secret_env: sqlx::types::Json(secret_env),
            working_dir: String::new(),
        }
    }

    /// The `mem.job` row a task's definition is merged against - the same filler idea as
    /// `task_row`, for the other side of the merge.
    fn job_row(env: BTreeMap<String, String>, secret_env: BTreeMap<String, String>, limits: Vec<String>) -> Job {
        Job {
            job_id: "job".to_string(),
            name: "Job".to_string(),
            description: String::new(),
            max_parallel_runs: 0,
            parameters: sqlx::types::Json(BTreeMap::new()),
            env: sqlx::types::Json(env),
            secret_env: sqlx::types::Json(secret_env),
            on_failure_recipients: sqlx::types::Json(BTreeMap::new()),
            on_success_recipients: sqlx::types::Json(BTreeMap::new()),
            limits: sqlx::types::Json(limits),
        }
    }

    /// `job_run_task_definition` is what `submit_job` itself calls to build each task's
    /// definition, so exercising it directly - rather than through `submit_job` - covers
    /// that line for real, with no `mem.job`/`mem.task` rows needed at all: `mem` is one
    /// shared-cache database for the whole test binary, and seeding it from a test races
    /// any other test's pooled connection over its schema lock, confirmed by running
    /// exactly that here, which broke two unrelated tests with "database schema is locked:
    /// mem".
    #[test]
    fn a_tasks_secret_env_is_merged_with_the_jobs_the_task_winning_a_shared_name() {

        let task = task_row(BTreeMap::new(), map(&[("API_KEY", "task_api_key"), ("SHARED", "task_shared_secret")]), Vec::new());

        let definition = job_run_task_definition(
            &task,
            &job_row(BTreeMap::new(), map(&[("DB_PASSWORD", "job_db_password"), ("SHARED", "job_shared_secret")]), Vec::new()),
        );

        assert_eq!(definition.secret_env.get("DB_PASSWORD").unwrap(), "job_db_password");
        assert_eq!(definition.secret_env.get("API_KEY").unwrap(), "task_api_key");
        assert_eq!(definition.secret_env.get("SHARED").unwrap(), "task_shared_secret");
        assert_eq!(definition.secret_env.len(), 3);
    }

    /// A job with no `secret_env:` at either level submits a task carrying an empty map,
    /// not a null one - the insert always writes a map.
    #[test]
    fn a_task_declaring_no_secret_env_and_a_job_declaring_none_either_has_an_empty_map() {

        let task = task_row(BTreeMap::new(), BTreeMap::new(), Vec::new());

        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), Vec::new()));

        assert!(definition.secret_env.is_empty());
    }

    /// The cross-level case `validate_secret_env_block` cannot see: it only rejects a name
    /// declared in both blocks of the *same* level, so a task overriding a job's `env` name
    /// in its own `secret_env` is legal YAML that would otherwise land the job's value in
    /// `env` and the task's in `secret_env` at once. The task's own `secret_env` wins the
    /// name outright, evicting the job's `env` contribution.
    #[test]
    fn a_tasks_secret_env_evicts_the_jobs_env_value_of_the_same_name() {

        let task = task_row(BTreeMap::new(), map(&[("FOO", "task_secret")]), Vec::new());

        let definition = job_run_task_definition(&task, &job_row(map(&[("FOO", "job_env_value")]), BTreeMap::new(), Vec::new()));

        assert_eq!(definition.secret_env.get("FOO").unwrap(), "task_secret");
        assert!(!definition.env.contains_key("FOO"));
    }

    /// The other direction of the same cross-level case: a task overriding a job's
    /// `secret_env` name in its own `env` evicts the job's `secret_env` contribution.
    #[test]
    fn a_tasks_env_evicts_the_jobs_secret_env_value_of_the_same_name() {

        let task = task_row(map(&[("FOO", "task_env_value")]), BTreeMap::new(), Vec::new());

        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), map(&[("FOO", "job_secret")]), Vec::new()));

        assert_eq!(definition.env.get("FOO").unwrap(), "task_env_value");
        assert!(!definition.secret_env.contains_key("FOO"));
    }

    /// The invariant Task 4's layering depends on: whatever combination of job- and
    /// task-level declarations produced it, a definition's `env` and `secret_env` never
    /// share a name.
    #[test]
    fn a_definitions_env_and_secret_env_never_share_a_name() {

        let task = task_row(
            map(&[("FOO", "task_env"), ("BAR", "task_env_2")]),
            map(&[("BAZ", "task_secret")]),
            Vec::new(),
        );

        let definition = job_run_task_definition(
            &task,
            &job_row(map(&[("BAR", "job_env")]), map(&[("FOO", "job_secret"), ("QUX", "job_secret_2")]), Vec::new()),
        );

        let shared_names: Vec<&String> = definition.env.keys()
            .filter(|name| definition.secret_env.contains_key(*name))
            .collect();

        assert!(shared_names.is_empty(), "names in both blocks: {:?}", shared_names);
    }

    /// `limits` has no override semantics: a task's claim set is the job's plus its own,
    /// never one replacing the other. Covers the job-only, task-only and both-declare
    /// cases, a name declared at both levels, and the case where neither declares any.
    #[test]
    fn a_jobs_limits_reach_the_task_when_the_task_declares_none() {

        let task = task_row(BTreeMap::new(), BTreeMap::new(), Vec::new());
        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), vec!["warehouse".to_string()]));

        assert_eq!(definition.limits, vec!["warehouse".to_string()]);
    }

    #[test]
    fn a_tasks_limits_reach_the_definition_when_the_job_declares_none() {

        let task = task_row(BTreeMap::new(), BTreeMap::new(), vec!["openai_api".to_string()]);
        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), Vec::new()));

        assert_eq!(definition.limits, vec!["openai_api".to_string()]);
    }

    #[test]
    fn a_job_and_a_task_declaring_different_limits_union() {

        let task = task_row(BTreeMap::new(), BTreeMap::new(), vec!["openai_api".to_string()]);
        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), vec!["warehouse".to_string()]));

        assert_eq!(definition.limits, vec!["openai_api".to_string(), "warehouse".to_string()]);
    }

    /// A name declared at both levels is one claim, not two - the union is a set, not a
    /// concatenation.
    #[test]
    fn a_limit_declared_at_both_levels_appears_once() {

        let task = task_row(BTreeMap::new(), BTreeMap::new(), vec!["warehouse".to_string()]);
        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), vec!["warehouse".to_string()]));

        assert_eq!(definition.limits, vec!["warehouse".to_string()]);
    }

    #[test]
    fn a_job_and_task_declaring_no_limits_has_an_empty_list() {

        let task = task_row(BTreeMap::new(), BTreeMap::new(), Vec::new());
        let definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), Vec::new()));

        assert!(definition.limits.is_empty());
    }

    /// The DB round trip above the pure merge: `insert_job_run_definition` writes whatever
    /// `job_run_task_definition` computed onto the `task_run` row, and JSON
    /// (de)serialization is a place a field could silently get lost.
    #[tokio::test]
    async fn a_task_definitions_secret_env_reaches_the_inserted_task_run_row() {

        let db = TestDb::new().await;

        let task = task_row(BTreeMap::new(), map(&[("API_KEY", "task_api_key")]), Vec::new());
        let task_definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), map(&[("DB_PASSWORD", "job_db_password")]), Vec::new()));

        let definition = JobRunDefinition {
            job_id: "job".to_string(),
            job_name: "Job".to_string(),
            job_description: String::new(),
            parameters: BTreeMap::new(),
            scheduled_at: None,
            tasks: vec![task_definition],
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
        assert_eq!(task_runs[0].secret_env.0.get("DB_PASSWORD").unwrap(), "job_db_password");
        assert_eq!(task_runs[0].secret_env.0.get("API_KEY").unwrap(), "task_api_key");
    }

    /// `insert_job_run_definition` writes the union `job_run_task_definition` computed, not
    /// an empty placeholder - the same round-trip guard as `secret_env` above, for the
    /// column `insert_task_run` had to carry empty until this task filled it in.
    #[tokio::test]
    async fn a_task_definitions_limits_reach_the_inserted_task_run_row() {

        let db = TestDb::new().await;

        let task = task_row(BTreeMap::new(), BTreeMap::new(), vec!["openai_api".to_string()]);
        let task_definition = job_run_task_definition(&task, &job_row(BTreeMap::new(), BTreeMap::new(), vec!["warehouse".to_string()]));

        let definition = JobRunDefinition {
            job_id: "job".to_string(),
            job_name: "Job".to_string(),
            job_description: String::new(),
            parameters: BTreeMap::new(),
            scheduled_at: None,
            tasks: vec![task_definition],
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
        assert_eq!(task_runs[0].limits.0, vec!["openai_api".to_string(), "warehouse".to_string()]);
    }
}
