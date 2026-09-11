use std::sync::Arc;
use anyhow::Context;
use sqlx::Acquire;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::fs;
use crate::crud::job::{InsertJobData, InsertJobDataInput};
use crate::crud::job_run_notification::NotificationChannel;
use crate::crud::schedule::{InsertScheduleData, InsertScheduleDataInput};
use crate::crud::schedule_job::{InsertScheduleJobData, InsertScheduleJobDataInput};
use crate::crud::task::{InsertTaskData, InsertTaskDataInput};
use crate::crud::task_dependent::{InsertTaskDependentData, InsertTaskDependentDataInput};
use crate::toolkit::Toolkit;
use crate::yaml_models::job_yaml::{JobYaml, JobYamlNotify};
use crate::yaml_models::schedule_yaml::ScheduleYaml;
use crate::cron_trigger::CronTrigger;


#[derive(Clone)]
pub struct CRUD {
    pub toolkit: Arc<Toolkit>,
}


/// Who one of a job's notify blocks tells, keyed by the channel that will tell them.
///
/// The one place the YAML's per-channel fields become the shape everything downstream
/// works in: the job row stores one of these per block, `submit_job` turns each entry into
/// a notification, and the startup check walks them to ask whether each channel is
/// configured. A channel naming nobody is left out entirely rather than carried as an
/// empty list — there is nothing to decide about later.
fn job_notify_recipients(notify: &JobYamlNotify) -> BTreeMap<NotificationChannel, Vec<String>> {

    let declared = [
        (NotificationChannel::Email, &notify.email),
        (NotificationChannel::Slack, &notify.slack),
    ];

    declared
        .into_iter()
        .filter(|(_, recipients)| !recipients.is_empty())
        .map(|(channel, recipients)| (channel, recipients.clone()))
        .collect()
}


impl CRUD {

    pub fn new(
        toolkit: Arc<Toolkit>,
    ) -> Self {
        Self { toolkit }
    }

    pub async fn init<'e, E>(&self, executor: E) -> anyhow::Result<()>
    where
        E: Acquire<'e, Database = sqlx::Sqlite>,
    {
        let data_dir = PathBuf::from(&self.toolkit.app_config.data_dir);

        // What a job, task or schedule gets for a field its YAML leaves out.
        let job_defaults = &self.toolkit.app_config.job_defaults;
        let schedule_defaults = &self.toolkit.app_config.schedule_defaults;

        let mut conn = executor.acquire().await
            .context("Failed to acquire a database connection to read the configuration into")?;

        let mut tx = conn.begin().await
            .context("Failed to begin the configuration transaction")?;

        let mut row_id = 0;

        let jobs_dir = data_dir.join("jobs");
        if jobs_dir.exists() {
            for job_path in CRUD::read_dir_sorted(&jobs_dir)? {
                if Self::is_yaml_file(&job_path) {
                    let job_yaml = JobYaml::from_yaml(&job_path)?;

                    Self::validate_job_tasks(&job_yaml)
                        .with_context(|| format!(
                            "Invalid tasks of job '{}' at {}",
                            job_yaml.id,
                            job_path.display(),
                        ))?;

                    self.validate_job_notifications(&job_yaml)
                        .with_context(|| format!(
                            "Invalid notifications of job '{}' at {}",
                            job_yaml.id,
                            job_path.display(),
                        ))?;

                    row_id += 1;
                    self.insert_job(&mut *tx, &InsertJobData {
                        input: InsertJobDataInput {
                            row_id,
                            job_id: job_yaml.id.clone(),
                            name: job_yaml.name,
                            description: job_yaml.description,
                            max_parallel_runs: job_yaml.max_parallel_runs
                                .unwrap_or(job_defaults.max_parallel_runs),
                            parameters: job_yaml.parameters.clone(),
                            env: job_yaml.env.clone(),
                            secret_env: job_yaml.secret_env.clone(),
                            on_failure_recipients: job_notify_recipients(&job_yaml.on_failure),
                            on_success_recipients: job_notify_recipients(&job_yaml.on_success),
                            limits: job_yaml.limits.clone(),
                        }
                    })
                        .await
                        .with_context(|| format!(
                            "Failed to insert job '{}' from {}",
                            job_yaml.id,
                            job_path.display(),
                        ))?;

                    for task_yaml in job_yaml.tasks {
                        row_id += 1;
                        self.insert_task(&mut *tx, &InsertTaskData {
                            input: InsertTaskDataInput {
                                row_id,
                                task_id: task_yaml.id.clone(),
                                job_id: job_yaml.id.clone(),
                                description: task_yaml.description,
                                command: task_yaml.command,
                                depends_on: task_yaml.depends_on.clone(),
                                limits: task_yaml.limits.clone(),
                                timeout: task_yaml.timeout
                                    .unwrap_or(job_defaults.timeout_seconds),
                                max_retries: task_yaml.max_retries
                                    .unwrap_or(job_defaults.max_retries),
                                retry_delay: task_yaml.retry_delay
                                    .unwrap_or(job_defaults.retry_delay_seconds),
                                env: task_yaml.env.clone(),
                                secret_env: task_yaml.secret_env.clone(),
                                working_dir: task_yaml.working_dir.clone(),
                            }
                        })
                            .await
                            .with_context(|| format!(
                                "Failed to insert task '{}' of job '{}' from {}",
                                task_yaml.id,
                                job_yaml.id,
                                job_path.display(),
                            ))?;

                        for dependent_task_id in task_yaml.depends_on {
                            row_id += 1;
                            self.insert_task_dependent(&mut *tx, &InsertTaskDependentData {
                                input: InsertTaskDependentDataInput {
                                    row_id,
                                    job_id: job_yaml.id.clone(),
                                    task_id: task_yaml.id.clone(),
                                    dependent_task_id: dependent_task_id.clone(),
                                }
                            })
                                .await
                                .with_context(|| format!(
                                    "Failed to insert dependency '{}' of task '{}' of job '{}' from {}",
                                    dependent_task_id,
                                    task_yaml.id,
                                    job_yaml.id,
                                    job_path.display(),
                                ))?;
                        }

                    }
                }
            }
        }

        let schedules_dir = data_dir.join("schedules");
        if schedules_dir.exists() {
            for schedule_path in CRUD::read_dir_sorted(&schedules_dir)? {
                if Self::is_yaml_file(&schedule_path) {
                    let schedule_yaml = ScheduleYaml::from_yaml(&schedule_path)?;

                    // Resolved once: the trigger that computes the first next_run and the
                    // row it is stored on must read the cron in the same zone.
                    let timezone = schedule_yaml.timezone
                        .unwrap_or(schedule_defaults.timezone);

                    let cron_trigger = CronTrigger::new(
                        schedule_yaml.cron.clone(),
                        timezone,
                        schedule_yaml.start_date,
                        schedule_yaml.end_date,
                    );

                    let next_run = cron_trigger.get_next_run(None);

                    row_id += 1;
                    self.insert_schedule(&mut *tx, &InsertScheduleData {
                        input: InsertScheduleDataInput {
                            row_id,
                            schedule_id: schedule_yaml.id.clone(),
                            name: schedule_yaml.name,
                            description: schedule_yaml.description,
                            cron: schedule_yaml.cron,
                            timezone,
                            start_date: schedule_yaml.start_date,
                            end_date: schedule_yaml.end_date,
                            disabled: schedule_yaml.disabled,
                            next_run,
                        }
                    })
                        .await
                        .with_context(|| format!(
                            "Failed to insert schedule '{}' from {}",
                            schedule_yaml.id,
                            schedule_path.display(),
                        ))?;

                    for schedule_job_yaml in schedule_yaml.jobs {
                        row_id += 1;
                        self.insert_schedule_job(&mut *tx, &InsertScheduleJobData {
                            input: InsertScheduleJobDataInput {
                                row_id,
                                schedule_id: schedule_yaml.id.clone(),
                                job_id: schedule_job_yaml.id.clone(),
                                parameters: schedule_job_yaml.parameters,
                            }
                        })
                            .await
                            .with_context(|| format!(
                                "Failed to insert job '{}' of schedule '{}' from {}",
                                schedule_job_yaml.id,
                                schedule_yaml.id,
                                schedule_path.display(),
                            ))?;
                    }

                }
            }
        }

        tx.commit().await
            .with_context(|| format!(
                "Failed to commit the configuration read from {}",
                data_dir.display(),
            ))?;

        Ok(())
    }

    /// Rejects a job that asks to be told over a channel this box cannot deliver on. Read
    /// here rather than at send time because a notification that silently never leaves is
    /// the one failure you cannot see from the run afterwards - and by then it is 03:00
    /// and the run everyone wanted to hear about has already finished.
    ///
    /// Both blocks are checked the same way and the error names the one at fault, since a
    /// job may well ask for Slack on a success and only mail on a failure.
    ///
    /// The match is over the same enum the sender matches on, so a new channel cannot be
    /// added without saying what config.toml section it needs to work at all.
    fn validate_job_notifications(&self, job_yaml: &JobYaml) -> anyhow::Result<()> {

        let blocks = [
            ("on_failure", &job_yaml.on_failure),
            ("on_success", &job_yaml.on_success),
        ];

        for (block, notify) in blocks {
            for (channel, recipients) in job_notify_recipients(notify) {

                let missing_section = match channel {
                    NotificationChannel::Email => self.toolkit.app_config.smtp
                        .is_none()
                        .then_some("[smtp]"),
                    NotificationChannel::Slack => self.toolkit.app_config.slack
                        .is_none()
                        .then_some("[slack]"),
                };

                if let Some(section) = missing_section {
                    anyhow::bail!(
                        "{}.{} names {} but config.toml has no {} section, so nothing \
                         can be sent by {}. Add one, or remove the recipients.",
                        block,
                        channel,
                        recipients.join(", "),
                        section,
                        channel,
                    );
                }
            }
        }

        Ok(())
    }

    /// Rejects a task graph the orchestrator could never finish. TaskRunDispatcher only
    /// starts a task run once every task run it depends on has succeeded, so a dependency
    /// on a task that isn't part of the job, or a cycle between tasks, would leave the
    /// task runs pending - and their job run running - forever.
    fn validate_job_tasks(job_yaml: &JobYaml) -> anyhow::Result<()> {

        let mut task_ids: HashSet<&str> = HashSet::new();

        for task_yaml in job_yaml.tasks.iter() {
            if !task_ids.insert(task_yaml.id.as_str()) {
                anyhow::bail!("Task '{}' is declared more than once", task_yaml.id);
            }
        }

        for task_yaml in job_yaml.tasks.iter() {
            for dependent_task_id in task_yaml.depends_on.iter() {

                if dependent_task_id == &task_yaml.id {
                    anyhow::bail!("Task '{}' depends on itself", task_yaml.id);
                }

                if !task_ids.contains(dependent_task_id.as_str()) {
                    anyhow::bail!(
                        "Task '{}' depends on '{}', which is not a task of this job",
                        task_yaml.id,
                        dependent_task_id,
                    );
                }
            }
        }

        if let Some(cycle) = Self::find_task_dependency_cycle(job_yaml) {
            anyhow::bail!("Tasks depend on each other in a cycle: {}", cycle.join(" -> "));
        }

        Ok(())
    }

    /// Returns the tasks of the first dependency cycle, in the order they depend on each
    /// other, or None if the tasks form a directed acyclic graph.
    fn find_task_dependency_cycle(job_yaml: &JobYaml) -> Option<Vec<String>> {

        let depends_on_by_task_id: HashMap<&str, &[String]> = job_yaml.tasks
            .iter()
            .map(|task_yaml| (task_yaml.id.as_str(), task_yaml.depends_on.as_slice()))
            .collect();

        let mut acyclic_task_ids: HashSet<&str> = HashSet::new();
        let mut path: Vec<&str> = Vec::new();

        for task_yaml in job_yaml.tasks.iter() {

            let cycle = Self::walk_task_dependencies(
                task_yaml.id.as_str(),
                &depends_on_by_task_id,
                &mut acyclic_task_ids,
                &mut path,
            );

            if cycle.is_some() {
                return cycle;
            }
        }

        None
    }

    /// Walks the dependencies of one task depth first, reporting a cycle as soon as the
    /// path leads back to a task already on it. Tasks that turned out to be free of cycles
    /// are remembered, so no task is walked twice.
    fn walk_task_dependencies<'a>(
        task_id: &'a str,
        depends_on_by_task_id: &HashMap<&'a str, &'a [String]>,
        acyclic_task_ids: &mut HashSet<&'a str>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<String>> {

        if let Some(position) = path.iter().position(|id| *id == task_id) {

            let mut cycle: Vec<String> = path[position..]
                .iter()
                .map(|id| id.to_string())
                .collect();

            cycle.push(task_id.to_string());

            return Some(cycle);
        }

        if acyclic_task_ids.contains(task_id) {
            return None;
        }

        path.push(task_id);

        let depends_on = depends_on_by_task_id
            .get(task_id)
            .copied()
            .unwrap_or(&[]);

        for dependent_task_id in depends_on.iter() {

            let cycle = Self::walk_task_dependencies(
                dependent_task_id.as_str(),
                depends_on_by_task_id,
                acyclic_task_ids,
                path,
            );

            if cycle.is_some() {
                return cycle;
            }
        }

        path.pop();
        acyclic_task_ids.insert(task_id);

        None
    }

    /// Both spellings of the extension are accepted: either is a reasonable thing to
    /// call a YAML file, and a config silently ignored for being named the other way is
    /// a bad way to find that out.
    fn is_yaml_file(path: &std::path::Path) -> bool {

        if !path.is_file() {
            return false;
        }

        matches!(path.extension().and_then(|s| s.to_str()), Some("yml") | Some("yaml"))
    }

    fn read_dir_sorted(path: impl AsRef<std::path::Path>) -> anyhow::Result<Vec<PathBuf>> {

        let path = path.as_ref();

        let entries = fs::read_dir(path)
            .with_context(|| format!("Failed to read directory {}", path.display()))?;

        let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        paths.sort();

        Ok(paths)
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::{AppConfig, AppConfigSlack, AppConfigSmtp, AppConfigSmtpEncryption};

    fn smtp() -> AppConfigSmtp {
        AppConfigSmtp {
            host: "smtp.example.com".to_string(),
            port: 587,
            username: String::new(),
            password: String::new(),
            from: "flowlite@example.com".to_string(),
            encryption: AppConfigSmtpEncryption::StartTls,
            max_output_bytes: 4096,
        }
    }

    fn slack() -> AppConfigSlack {
        AppConfigSlack {
            token: "xoxb-test".to_string(),
            api_url: "https://slack.example.com/api/chat.postMessage".to_string(),
            timeout_seconds: 10,
            max_output_bytes: 2048,
        }
    }

    fn crud_with(smtp: Option<AppConfigSmtp>, slack: Option<AppConfigSlack>) -> CRUD {
        CRUD::new(Arc::new(Toolkit::new(AppConfig { smtp, slack, ..AppConfig::default() })))
    }

    fn job_yaml(notify: &str) -> JobYaml {
        serde_yaml::from_str(&format!(
            "id: nightly\nname: Nightly\n{}tasks:\n  - id: sync\n    command: ./sync.sh\n",
            notify,
        )).unwrap()
    }

    #[test]
    fn a_job_naming_an_address_with_no_smtp_section_is_refused() {

        let crud = crud_with(None, None);

        let error = crud
            .validate_job_notifications(&job_yaml("on_failure:\n  email: [oncall@example.com]\n"))
            .unwrap_err()
            .to_string();

        assert!(error.contains("oncall@example.com"), "{}", error);
        assert!(error.contains("[smtp]"), "{}", error);
    }

    #[test]
    fn a_job_naming_an_address_is_accepted_once_smtp_is_configured() {

        let crud = crud_with(Some(smtp()), None);

        assert!(crud
            .validate_job_notifications(&job_yaml("on_failure:\n  email: [oncall@example.com]\n"))
            .is_ok());
    }

    /// The check is about a job that asked for something it cannot have — a job that asks
    /// for nothing is fine on a box with no mail at all.
    #[test]
    fn a_job_naming_nobody_needs_no_smtp_section() {

        let crud = crud_with(None, None);

        assert!(crud.validate_job_notifications(&job_yaml("")).is_ok());
    }

    /// The same refusal for the second channel, and it names `[slack]` rather than the
    /// section the first channel happened to need.
    #[test]
    fn a_job_naming_a_conversation_with_no_slack_section_is_refused() {

        let crud = crud_with(Some(smtp()), None);

        let error = crud
            .validate_job_notifications(&job_yaml("on_failure:\n  slack: ['#oncall']\n"))
            .unwrap_err()
            .to_string();

        assert!(error.contains("#oncall"), "{}", error);
        assert!(error.contains("[slack]"), "{}", error);
        assert!(!error.contains("[smtp]"), "{}", error);
    }

    /// A job may name both, and each is checked against its own section — so mail
    /// configured while Slack is not still refuses, rather than passing on the first
    /// channel that happened to be fine.
    #[test]
    fn a_job_naming_both_channels_needs_both_sections() {

        let both = "on_failure:\n  email: [oncall@example.com]\n  slack: ['#oncall']\n";

        assert!(crud_with(Some(smtp()), None)
            .validate_job_notifications(&job_yaml(both))
            .is_err());

        assert!(crud_with(None, Some(slack()))
            .validate_job_notifications(&job_yaml(both))
            .is_err());

        assert!(crud_with(Some(smtp()), Some(slack()))
            .validate_job_notifications(&job_yaml(both))
            .is_ok());
    }

    /// A success block is not a second-class one: it is checked against the same
    /// sections, so a job that would have gone quiet on every good run refuses to start.
    #[test]
    fn a_job_naming_a_success_recipient_with_no_smtp_section_is_refused() {

        let crud = crud_with(None, None);

        let error = crud
            .validate_job_notifications(&job_yaml("on_success:\n  email: [data-team@example.com]\n"))
            .unwrap_err()
            .to_string();

        assert!(error.contains("on_success.email"), "{}", error);
        assert!(error.contains("data-team@example.com"), "{}", error);
        assert!(error.contains("[smtp]"), "{}", error);
    }

    /// The two blocks are checked separately, so a box that can mail but not post refuses
    /// a job asking for Slack on success even though its failure block is deliverable.
    #[test]
    fn a_deliverable_failure_block_does_not_excuse_an_undeliverable_success_one() {

        let crud = crud_with(Some(smtp()), None);

        let error = crud
            .validate_job_notifications(&job_yaml(
                "on_failure:\n  email: [oncall@example.com]\non_success:\n  slack: ['#data']\n"
            ))
            .unwrap_err()
            .to_string();

        assert!(error.contains("on_success.slack"), "{}", error);
        assert!(error.contains("[slack]"), "{}", error);
    }

    #[test]
    fn a_job_declaring_both_channels_becomes_one_map_entry_each() {

        let on_failure = &job_yaml(
            "on_failure:\n  email: [oncall@example.com]\n  slack: ['#oncall', '#data']\n"
        ).on_failure;

        let recipients = job_notify_recipients(on_failure);

        assert_eq!(recipients.len(), 2);
        assert_eq!(recipients[&NotificationChannel::Email], vec!["oncall@example.com"]);
        assert_eq!(recipients[&NotificationChannel::Slack], vec!["#oncall", "#data"]);
    }

    /// A channel the YAML mentions nobody under is left out rather than carried as an
    /// empty list, so a run is never submitted with a notification addressed to nobody.
    #[test]
    fn a_channel_naming_nobody_is_left_out_of_the_map() {

        let on_failure = &job_yaml("on_failure:\n  email: [oncall@example.com]\n").on_failure;

        let recipients = job_notify_recipients(on_failure);

        assert_eq!(recipients.len(), 1);
        assert!(!recipients.contains_key(&NotificationChannel::Slack));
    }

    /// The map is what the job row stores, so it has to survive the round trip through
    /// JSON with the channel as the key.
    #[test]
    fn the_map_survives_being_written_as_json_and_read_back() {

        let recipients = job_notify_recipients(
            &job_yaml("on_failure:\n  slack: ['#oncall']\n").on_failure
        );

        let json = serde_json::to_string(&recipients).unwrap();

        assert_eq!(json, r##"{"slack":["#oncall"]}"##);

        let read_back: BTreeMap<NotificationChannel, Vec<String>> =
            serde_json::from_str(&json).unwrap();

        assert_eq!(read_back, recipients);
    }

    /// A private `mem` schema, migrated onto its own uniquely-named in-memory database
    /// rather than the shared-cache `flowlite_mem` name `TestDb` leaves unmigrated on
    /// purpose (see its doc comment) - every test in this binary would otherwise share that
    /// one name, racing each other's schema and rows. Two connections is what migrating one
    /// actually takes: `mem_conn` connects directly to the private database, so the
    /// migrator builds its tables as *that connection's* main schema, and the returned
    /// connection attaches the same database under the `mem` alias `CRUD::init`'s queries
    /// expect - mirroring `Toolkit::get_memory_conn` and `Toolkit::get_conn_pool` exactly,
    /// just under a name nothing else in the suite can collide with. `mem_conn` is only
    /// ever kept alive: a `mode=memory` database is dropped the instant nothing has it open.
    async fn crud_with_private_mem(data_dir: &std::path::Path) -> (CRUD, sqlx::SqliteConnection, sqlx::SqliteConnection) {
        use sqlx::Connection;

        let mem_uri = format!("file:flowlite-mem-test-{}?mode=memory&cache=shared", uuid::Uuid::new_v4());

        let mut mem_conn = sqlx::SqliteConnection::connect(&mem_uri).await.unwrap();
        sqlx::migrate!("./db/schemas/memory/migrations").run(&mut mem_conn).await.unwrap();

        let mut main_conn = sqlx::SqliteConnection::connect("sqlite::memory:").await.unwrap();
        // The uri is generated here, not attacker input, so this mirrors the
        // AssertSqlSafe uses inside sqlx itself for its own dynamic SAVEPOINT names.
        sqlx::query(sqlx::AssertSqlSafe(format!("ATTACH DATABASE '{}' AS mem", mem_uri)))
            .execute(&mut main_conn)
            .await
            .unwrap();

        let crud = CRUD::new(Arc::new(Toolkit::new(AppConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            ..AppConfig::default()
        })));

        (crud, main_conn, mem_conn)
    }

    fn write_job_yaml(data_dir: &std::path::Path, file_name: &str, content: &str) {
        std::fs::create_dir_all(data_dir.join("jobs")).unwrap();
        std::fs::write(data_dir.join("jobs").join(file_name), content).unwrap();
    }

    /// `limits:` at both levels seeds two independent claims - the job's own and the
    /// task's own - onto their respective rows, unmerged: combining a job's claim with its
    /// tasks' is enforcement's job, in a later task, not `CRUD::init`'s.
    #[tokio::test]
    async fn a_job_naming_limits_at_both_levels_seeds_them_onto_the_job_and_task_rows() {
        use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
        use crate::crud::task::{SelectTasksData, SelectTasksDataFilter};

        let data_dir = std::env::temp_dir().join(format!("flowlite-limits-seed-{}", uuid::Uuid::new_v4()));
        write_job_yaml(&data_dir, "nightly.yaml", "
id: nightly-sync
name: Nightly Sync
limits: [warehouse]
tasks:
  - id: ingest
    command: ./run.sh
    limits: [warehouse, api]
");

        let (crud, mut main_conn, _mem_conn) = crud_with_private_mem(&data_dir).await;

        crud.init(&mut main_conn).await.unwrap();

        let job = crud.select_job(&mut main_conn, &SelectJobsData {
            filter: SelectJobsDataFilter { job_id: Some("nightly-sync".to_string()), name_like: None },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap().unwrap();

        assert_eq!(job.limits.0, vec!["warehouse".to_string()]);

        let task = crud.select_task(&mut main_conn, &SelectTasksData {
            filter: SelectTasksDataFilter { task_id: Some("ingest".to_string()), job_id: Some("nightly-sync".to_string()) },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap().unwrap();

        assert_eq!(task.limits.0, vec!["warehouse".to_string(), "api".to_string()]);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// A job naming no limits at either level still seeds fine, storing `[]` rather than
    /// failing or leaving the column unset - "claims nothing" is an ordinary value, not a
    /// distinct unknown state.
    #[tokio::test]
    async fn a_job_naming_no_limits_seeds_an_empty_claim_at_both_levels() {
        use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
        use crate::crud::task::{SelectTasksData, SelectTasksDataFilter};

        let data_dir = std::env::temp_dir().join(format!("flowlite-limits-seed-{}", uuid::Uuid::new_v4()));
        write_job_yaml(&data_dir, "nightly.yaml", "
id: nightly-sync
name: Nightly Sync
tasks:
  - id: ingest
    command: ./run.sh
");

        let (crud, mut main_conn, _mem_conn) = crud_with_private_mem(&data_dir).await;

        crud.init(&mut main_conn).await.unwrap();

        let job = crud.select_job(&mut main_conn, &SelectJobsData {
            filter: SelectJobsDataFilter { job_id: Some("nightly-sync".to_string()), name_like: None },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap().unwrap();

        assert!(job.limits.0.is_empty());

        let task = crud.select_task(&mut main_conn, &SelectTasksData {
            filter: SelectTasksDataFilter { task_id: Some("ingest".to_string()), job_id: Some("nightly-sync".to_string()) },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap().unwrap();

        assert!(task.limits.0.is_empty());

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
