use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use chrono::{DateTime, Utc};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter, SelectJobsDataSort};
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_notification::{InsertJobRunNotificationData, InsertJobRunNotificationDataInput, JobRunNotificationStatus, NotificationChannel, NotifyOn, SelectJobRunNotificationsData, SelectJobRunNotificationsDataFilter, SelectJobRunNotificationsDataSort};
use crate::crud::task::{SelectTasksData, SelectTasksDataFilter, SelectTasksDataSort, Task};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::crud::task_run_attempt_output::{group_task_run_attempt_output, SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort, TaskRunAttemptOutputStreams};
use crate::yaml_models::job_yaml::JobYaml;


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
    limits: Vec<String>,
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
fn job_run_task_definition(
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


/// One `secret_env:` entry a job or a task declared — the unit
/// `first_unsatisfied_secret_reference` checks against the secrets `serve` was actually
/// given. `task_id` is `None` for a job-level declaration, which names no task of its own;
/// a task-level one always carries its own `task_id`, whether or not the name is shared
/// with the job's own block.
struct SecretEnvReference<'a> {
    job_id: &'a str,
    task_id: Option<&'a str>,
    variable_name: &'a str,
    secret_name: &'a str,
}

/// The first reference the given secrets do not satisfy, in the order the caller collected
/// them - `check_secret_env_is_satisfied` collects every job's declaration before any
/// task's, and each in the row_id order `CRUD::init` assigned, so "first" means the first
/// one a person reading the YAML top to bottom would reach.
///
/// Pure and database-free on purpose: `check_secret_env_is_satisfied` reads real
/// `mem.job`/`mem.task` rows, and seeding `mem` inside a test to exercise this logic would
/// race every other test's pooled connection over `mem`'s shared-cache schema lock - the
/// "database schema is locked: mem" hazard `src/test_support.rs:70-72` documents and Task 3
/// already hit. So every case of the policy belongs here, where it needs no seeding at all.
fn first_unsatisfied_secret_reference<'a, 'b>(
    references: &'b [SecretEnvReference<'a>],
    secrets: &BTreeMap<String, String>,
) -> Option<&'b SecretEnvReference<'a>> {

    references.iter().find(|reference| !secrets.contains_key(reference.secret_name))
}

/// Turns the first unsatisfied reference into the error `serve` reports. The job is the
/// outer context - so the message names the job even before the cause - and the reference
/// itself is the cause: a task's own declaration reads as "task '<id>'", and a job-level one
/// with no task of its own reads as "job '<id>'" rather than naming a task that isn't there.
///
/// The job's YAML path is not on the `mem.job` row `check_secret_env_is_satisfied` reads
/// this from, so unlike the design's own example this message names only the job and the
/// task, not the file - carrying the path through would mean putting it onto that row for a
/// check that only ever runs once, at startup.
fn unsatisfied_secret_reference_error(reference: &SecretEnvReference) -> anyhow::Error {

    let declarer = match reference.task_id {
        Some(task_id) => format!("task '{}'", task_id),
        None => format!("job '{}'", reference.job_id),
    };

    anyhow::anyhow!(
        "{} needs secret '{}' for {}, but nothing defines it. Add it under [secrets] in \
         config.toml, or set FLOWLITE_SECRETS__{}.",
        declarer,
        reference.secret_name,
        reference.variable_name,
        reference.secret_name.to_ascii_uppercase(),
    ).context(format!("Job '{}'", reference.job_id))
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
            // Cloned rather than moved so the whole row is still borrowable below: each
            // task's definition is built against the job's own `env` and `secret_env`.
            job_id: job.job_id.clone(),
            job_name: job.name.clone(),
            job_description: job.description.clone(),
            parameters,
            scheduled_at,
            tasks: tasks
                .iter()
                .map(|task| job_run_task_definition(task, &job))
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

    /// How many task run attempts are Running right now, across every job - what
    /// `settle_as_pending`'s global cap counts against. Running is the only status that
    /// holds a slot, the same way `is_job_at_max_parallel_runs` counts only running job
    /// runs.
    pub async fn count_running_attempts(&self, conn: &mut SqliteConnection) -> anyhow::Result<u32> {

        let running_attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: Some(TaskRunAttemptStatus::Running),
            },
            sort: None,
        }).await?;

        Ok(running_attempts.len() as u32)
    }

    /// How many running task run attempts currently claim each named limit - what
    /// `settle_as_pending`'s named-limit gate counts against. A name claimed by three
    /// Running attempts' task runs maps to `3`; a name nothing running claims is absent
    /// rather than `0`. Tallied from Running attempts only, the same as
    /// `count_running_attempts`.
    pub async fn claimed_limit_slots(&self, conn: &mut SqliteConnection) -> anyhow::Result<BTreeMap<String, u32>> {

        let running_attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: Some(TaskRunAttemptStatus::Running),
            },
            sort: None,
        }).await?;

        let mut claimed_limit_slots = BTreeMap::new();

        for running_attempt in &running_attempts {

            let task_run = self.select_task_run(&mut *conn, &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: Some(running_attempt.task_run_id),
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            }).await?
                .ok_or_else(|| anyhow::anyhow!("Task run not found: {}", running_attempt.task_run_id))?;

            for limit in &task_run.limits.0 {
                *claimed_limit_slots.entry(limit.clone()).or_insert(0) += 1;
            }
        }

        Ok(claimed_limit_slots)
    }

    /// Refuses when a job's or a task's `secret_env:` names a secret `secrets` does not
    /// define - the check that keeps the spawn-time bail in `build_task_run_attempt_env`
    /// off the path of anything this process serves. Not off it entirely: this reads the
    /// declarations `init` seeded into `mem` at startup, so a rerun replaying a row whose
    /// YAML has since changed, or a run `job submit` created in another process from a
    /// YAML this server never read, still reaches that bail - which is why it names the
    /// job and the task and carries this message's remedy rather than deferring to it.
    /// Called from `serve.rs` alone, after `init` and before the bind: see the comment at
    /// that call site for why it cannot live in `init` itself.
    ///
    /// Deliberately conservative about the other thing `submit_job` knows and this does
    /// not: `job_run_task_definition` evicts a job-level `secret_env` name from the merged
    /// map when a task declares that same name in its own `env`, so a job whose every task
    /// overrides `secret_env: {PW: x}` that way submits no row referencing `x` at all -
    /// and this still refuses to start over it. That asymmetry is chosen, not overlooked.
    /// Modelling the eviction would mean duplicating the merge here, so the two places
    /// that reason about these declarations could then disagree about which references are
    /// live - and the failure mode of disagreeing is a job that starts and bails at 03:00,
    /// which is the outcome this check exists to prevent. Refusing a declaration nothing
    /// would have read costs one YAML edit, at startup, with the job and the name in the
    /// message; the remedy is to drop the `secret_env` entry no task uses.
    ///
    /// Its own coverage is the integration test in `tests/serve_secret_check.rs`, not a
    /// unit test: collecting real declarations here means reading real `mem.job`/
    /// `mem.task` rows, which needs `init` to have seeded `mem` first, and `mem` is one
    /// shared-cache name for the whole test binary (`src/test_support.rs:70-72`) - seeding
    /// it from a test races every other test's pooled connection over its schema lock. The
    /// policy this delegates to, `first_unsatisfied_secret_reference`, carries the
    /// exhaustive unit coverage instead; this function does only a query each, a collect,
    /// and the delegate.
    /// `job_id` scopes the check. `serve` passes `None` and asks about the whole data
    /// directory, which is what it is about to run. `job submit -f` passes its own id and
    /// must not ask about anything else: by then this same `mem` also holds every installed
    /// job, and an installed job's unsatisfied secret is no reason to refuse a submit that
    /// has nothing to do with it.
    pub async fn check_secret_env_is_satisfied(
        &self,
        conn: &mut SqliteConnection,
        secrets: &BTreeMap<String, String>,
        job_id: Option<&str>,
    ) -> anyhow::Result<()> {

        let jobs = self.select_jobs(&mut *conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: job_id.map(str::to_string),
                name_like: None,
            },
            sort: Some(SelectJobsDataSort::RowId),
            limit: None,
            offset: None,
        }).await?;

        let tasks = self.select_tasks(&mut *conn, &SelectTasksData {
            filter: SelectTasksDataFilter {
                task_id: None,
                job_id: job_id.map(str::to_string),
            },
            sort: Some(SelectTasksDataSort::RowId),
            limit: None,
            offset: None,
        }).await?;

        let mut references: Vec<SecretEnvReference> = Vec::new();

        for job in jobs.iter() {
            for (variable_name, secret_name) in job.secret_env.0.iter() {
                references.push(SecretEnvReference {
                    job_id: &job.job_id,
                    task_id: None,
                    variable_name,
                    secret_name,
                });
            }
        }

        for task in tasks.iter() {
            for (variable_name, secret_name) in task.secret_env.0.iter() {
                references.push(SecretEnvReference {
                    job_id: &task.job_id,
                    task_id: Some(&task.task_id),
                    variable_name,
                    secret_name,
                });
            }
        }

        match first_unsatisfied_secret_reference(&references, secrets) {
            Some(reference) => Err(unsatisfied_secret_reference_error(reference)),
            None => Ok(()),
        }
    }

    /// The run and every task run under it — what `job-run get` and the MCP `get_job_run`
    /// tool both assemble to answer "what happened to this run": one row from `job_run`
    /// and every row from `task_run` naming it.
    pub async fn select_job_run_with_task_runs(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
    ) -> anyhow::Result<(JobRun, Vec<TaskRun>)> {

        let job_run = self.select_job_run(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter { id: Some(job_run_id), job_id: None, status: None },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        let Some(job_run) = job_run else {
            anyhow::bail!("Job run {} not found", job_run_id);
        };

        let task_runs = self.select_task_runs(&mut *conn, &SelectTaskRunsData {
            filter: SelectTaskRunsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: None,
                status: None,
            },
            sort: Some(SelectTaskRunsDataSort::Id),
        }).await?;

        Ok((job_run, task_runs))
    }

    /// Every task run attempt of a run, grouped with what each wrote to stdout and
    /// stderr — what `job-run logs` and the MCP `get_task_output` tool both assemble.
    /// `task_id` narrows to one task's attempts, exactly as `job-run logs --task` does.
    ///
    /// Checked against `job_run` first so an id nothing matches is reported as that,
    /// rather than as an empty run indistinguishable from one with no attempts yet.
    pub async fn select_task_run_attempt_logs(
        &self,
        conn: &mut SqliteConnection,
        job_run_id: i64,
        task_id: Option<&str>,
    ) -> anyhow::Result<(Vec<TaskRunAttempt>, HashMap<i64, TaskRunAttemptOutputStreams>)> {

        let job_run = self.select_job_run(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter { id: Some(job_run_id), job_id: None, status: None },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        if job_run.is_none() {
            anyhow::bail!("Job run {} not found", job_run_id);
        }

        let task_run_attempts = self.select_task_run_attempts(&mut *conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: Some(job_run_id),
                task_id: task_id.map(str::to_string),
                status: None,
            },
            sort: Some(SelectTaskRunAttemptsDataSort::Id),
        }).await?;

        let task_run_attempt_output = self.select_task_run_attempt_outputs(&mut *conn, &SelectTaskRunAttemptOutputsData {
            filter: SelectTaskRunAttemptOutputsDataFilter {
                id: None,
                task_run_attempt_id: None,
                task_run_id: None,
                job_run_id: Some(job_run_id),
                job_id: None,
                task_id: task_id.map(str::to_string),
                stream: None,
            },
            sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
        }).await?;

        Ok((task_run_attempts, group_task_run_attempt_output(task_run_attempt_output)))
    }

    /// Seeds a job definition that is not installed in the data directory - the sequence
    /// `job submit -f` and the MCP `submit_job` tool's `file` and `yaml` arguments all run
    /// before they can submit it, kept in one place so the two callers cannot drift apart:
    /// refuse a collision with an installed job before anything is written, seed the
    /// parsed definition, then check its own `secret_env` is satisfied, scoped to just this
    /// job rather than the whole data directory `mem` holds by now.
    ///
    /// A collision comes back as `JobIdAlreadyInstalled`, downcastable out of the
    /// `anyhow::Error` this returns, rather than a finished sentence: the CLI's `-f` and the
    /// MCP tool's `job` argument each name an installed job a different way, and wording
    /// that remedy is each caller's own job, not this method's.
    pub async fn seed_ad_hoc_job(
        &self,
        conn: &mut SqliteConnection,
        job_yaml: JobYaml,
        job_path: &Path,
        row_id: u64,
    ) -> anyhow::Result<String> {

        let job_id = job_yaml.id.clone();

        let installed = self.select_job(&mut *conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: Some(job_id.clone()),
                name_like: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        if installed.is_some() {
            return Err(JobIdAlreadyInstalled {
                job_id,
                jobs_dir: Path::new(&self.toolkit.app_config.data_dir).join("jobs"),
            }.into());
        }

        self.seed_job(&mut *conn, job_yaml, job_path, row_id).await?;

        self.check_secret_env_is_satisfied(
            &mut *conn,
            &self.toolkit.app_config.secrets,
            Some(&job_id),
        ).await?;

        Ok(job_id)
    }

}

/// The one fact `seed_ad_hoc_job` refuses on: an installed job already holds this id.
/// `job_id` is `mem.job`'s primary key, so the alternative to refusing is a raw constraint
/// error instead of something a caller can act on.
///
/// A typed value rather than a finished sentence, so each caller can word its own remedy
/// without CRUD phrasing half of it: `job submit -f`'s "drop -f" and the MCP tool's "submit
/// it by name" are two different sentences about the same fact, and neither belongs here.
#[derive(Debug)]
pub struct JobIdAlreadyInstalled {
    pub job_id: String,
    pub jobs_dir: PathBuf,
}

impl std::fmt::Display for JobIdAlreadyInstalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}' is already a job in {}", self.job_id, self.jobs_dir.display())
    }
}

impl std::error::Error for JobIdAlreadyInstalled {}

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

    fn task_reference<'a>(job_id: &'a str, task_id: &'a str, variable_name: &'a str, secret_name: &'a str) -> SecretEnvReference<'a> {
        SecretEnvReference { job_id, task_id: Some(task_id), variable_name, secret_name }
    }

    fn job_reference<'a>(job_id: &'a str, variable_name: &'a str, secret_name: &'a str) -> SecretEnvReference<'a> {
        SecretEnvReference { job_id, task_id: None, variable_name, secret_name }
    }

    #[test]
    fn no_references_at_all_is_satisfied() {
        assert!(first_unsatisfied_secret_reference(&[], &map(&[])).is_none());
    }

    #[test]
    fn every_reference_defined_is_satisfied() {
        let references = [
            task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw"),
            job_reference("nightly-sync", "API_KEY", "api_key"),
        ];

        let available = map(&[("warehouse_pw", "hunter2"), ("api_key", "abc")]);

        assert!(first_unsatisfied_secret_reference(&references, &available).is_none());
    }

    /// A task-level reference nothing defines is reported, naming its own job, task,
    /// variable and secret name - not just "something is wrong".
    #[test]
    fn an_undefined_task_level_reference_is_reported() {
        let references = [task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw")];

        let reference = first_unsatisfied_secret_reference(&references, &map(&[])).unwrap();

        assert_eq!(reference.job_id, "nightly-sync");
        assert_eq!(reference.task_id, Some("load"));
        assert_eq!(reference.variable_name, "PGPASSWORD");
        assert_eq!(reference.secret_name, "warehouse_pw");
    }

    /// The job-level case carries no task - `task_id` stays `None` rather than being
    /// attributed to a task that never declared it.
    #[test]
    fn an_undefined_job_level_reference_is_reported_with_no_task() {
        let references = [job_reference("nightly-sync", "API_KEY", "api_key")];

        let reference = first_unsatisfied_secret_reference(&references, &map(&[])).unwrap();

        assert_eq!(reference.job_id, "nightly-sync");
        assert!(reference.task_id.is_none());
        assert_eq!(reference.secret_name, "api_key");
    }

    /// "First" means first in the caller's order, not sorted some other way - a defined
    /// reference ahead of an undefined one must not hide behind it, and two undefined ones
    /// report the earlier.
    #[test]
    fn the_first_unsatisfied_reference_in_order_wins_over_a_later_one() {
        let references = [
            task_reference("nightly-sync", "extract", "API_KEY", "api_key"),
            task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw"),
        ];

        let available = map(&[("api_key", "abc")]);

        let reference = first_unsatisfied_secret_reference(&references, &available).unwrap();

        assert_eq!(reference.task_id, Some("load"));
        assert_eq!(reference.secret_name, "warehouse_pw");
    }

    #[test]
    fn a_reference_satisfied_by_an_empty_secret_value_still_counts_as_defined() {
        let references = [task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw")];

        // An empty string is still a defined secret - "nothing defines it" is about the
        // name being absent from the map, not about the value being non-empty.
        let available = map(&[("warehouse_pw", "")]);

        assert!(first_unsatisfied_secret_reference(&references, &available).is_none());
    }

    /// The error the design fixes, for the task-level case: the job as the outer context
    /// and "task '<id>' needs secret '<name>' for <variable>, but nothing defines it",
    /// naming the env var to set.
    #[test]
    fn a_task_level_error_names_the_job_the_task_the_variable_and_the_secret() {
        let reference = task_reference("nightly-sync", "load", "PGPASSWORD", "warehouse_pw");

        let error = format!("{:?}", unsatisfied_secret_reference_error(&reference));

        assert!(error.contains("Job 'nightly-sync'"), "{error}");
        assert!(error.contains("task 'load'"), "{error}");
        assert!(error.contains("secret 'warehouse_pw'"), "{error}");
        assert!(error.contains("PGPASSWORD"), "{error}");
        assert!(error.contains("but nothing defines it"), "{error}");
        assert!(error.contains("[secrets] in config.toml"), "{error}");
        assert!(error.contains("FLOWLITE_SECRETS__WAREHOUSE_PW"), "{error}");
    }

    /// The job-level case names the job as the declarer too, rather than inventing a task
    /// that was never declared.
    #[test]
    fn a_job_level_error_names_the_job_rather_than_a_task() {
        let reference = job_reference("nightly-sync", "API_KEY", "api_key");

        let error = format!("{:?}", unsatisfied_secret_reference_error(&reference));

        assert!(error.contains("job 'nightly-sync' needs secret 'api_key'"), "{error}");
        assert!(!error.contains("task '"), "{error}");
        assert!(error.contains("FLOWLITE_SECRETS__API_KEY"), "{error}");
    }

    /// The env var name the message tells someone to set is the secret name uppercased,
    /// whatever case and punctuation the name itself carries within what Task 2 allows.
    #[test]
    fn the_suggested_environment_variable_is_the_secret_name_uppercased() {
        let reference = task_reference("job", "task", "VAR", "warehouse_read_only_pw");

        let error = format!("{:?}", unsatisfied_secret_reference_error(&reference));

        assert!(error.contains("FLOWLITE_SECRETS__WAREHOUSE_READ_ONLY_PW"), "{error}");
    }
}
