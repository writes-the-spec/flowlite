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
use crate::yaml_models::job_yaml::{JobYaml, JobYamlOnFailure};
use crate::yaml_models::schedule_yaml::ScheduleYaml;
use crate::cron_trigger::CronTrigger;


#[derive(Clone)]
pub struct CRUD {
    pub toolkit: Arc<Toolkit>,
}


/// Who a job's `on_failure:` tells, keyed by the channel that will tell them.
///
/// The one place the YAML's per-channel fields become the shape everything downstream
/// works in: the job row stores this map, `submit_job` turns it into one notification per
/// entry, and the startup check walks it to ask whether each channel is configured. A
/// channel naming nobody is left out entirely rather than carried as an empty list —
/// there is nothing to decide about later.
fn job_on_failure_recipients(on_failure: &JobYamlOnFailure) -> BTreeMap<NotificationChannel, Vec<String>> {

    let declared = [
        (NotificationChannel::Email, &on_failure.email),
        (NotificationChannel::Slack, &on_failure.slack),
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
                            on_failure_recipients: job_on_failure_recipients(&job_yaml.on_failure),
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
                                timeout: task_yaml.timeout
                                    .unwrap_or(job_defaults.timeout_seconds),
                                max_retries: task_yaml.max_retries
                                    .unwrap_or(job_defaults.max_retries),
                                retry_delay: task_yaml.retry_delay
                                    .unwrap_or(job_defaults.retry_delay_seconds),
                                env: task_yaml.env.clone(),
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

    /// Rejects a job that asks to be told on a failure over a channel this box cannot
    /// deliver on. Read here rather than at send time because a notification that silently
    /// never leaves is the one failure you cannot see from the run afterwards - and by
    /// then it is 03:00 and the run everyone wanted to hear about has already finished.
    ///
    /// The match is over the same enum the sender matches on, so a new channel cannot be
    /// added without saying what config.toml section it needs to work at all.
    fn validate_job_notifications(&self, job_yaml: &JobYaml) -> anyhow::Result<()> {

        for (channel, recipients) in job_on_failure_recipients(&job_yaml.on_failure) {

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
                    "on_failure.{} names {} but config.toml has no {} section, so nothing \
                     can be sent by {}. Add one, or remove the recipients.",
                    channel,
                    recipients.join(", "),
                    section,
                    channel,
                );
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

    fn job_yaml(on_failure: &str) -> JobYaml {
        serde_yaml::from_str(&format!(
            "id: nightly\nname: Nightly\n{}tasks:\n  - id: sync\n    command: ./sync.sh\n",
            on_failure,
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

    #[test]
    fn a_job_declaring_both_channels_becomes_one_map_entry_each() {

        let on_failure = &job_yaml(
            "on_failure:\n  email: [oncall@example.com]\n  slack: ['#oncall', '#data']\n"
        ).on_failure;

        let recipients = job_on_failure_recipients(on_failure);

        assert_eq!(recipients.len(), 2);
        assert_eq!(recipients[&NotificationChannel::Email], vec!["oncall@example.com"]);
        assert_eq!(recipients[&NotificationChannel::Slack], vec!["#oncall", "#data"]);
    }

    /// A channel the YAML mentions nobody under is left out rather than carried as an
    /// empty list, so a run is never submitted with a notification addressed to nobody.
    #[test]
    fn a_channel_naming_nobody_is_left_out_of_the_map() {

        let on_failure = &job_yaml("on_failure:\n  email: [oncall@example.com]\n").on_failure;

        let recipients = job_on_failure_recipients(on_failure);

        assert_eq!(recipients.len(), 1);
        assert!(!recipients.contains_key(&NotificationChannel::Slack));
    }

    /// The map is what the job row stores, so it has to survive the round trip through
    /// JSON with the channel as the key.
    #[test]
    fn the_map_survives_being_written_as_json_and_read_back() {

        let recipients = job_on_failure_recipients(
            &job_yaml("on_failure:\n  slack: ['#oncall']\n").on_failure
        );

        let json = serde_json::to_string(&recipients).unwrap();

        assert_eq!(json, r##"{"slack":["#oncall"]}"##);

        let read_back: BTreeMap<NotificationChannel, Vec<String>> =
            serde_json::from_str(&json).unwrap();

        assert_eq!(read_back, recipients);
    }
}
