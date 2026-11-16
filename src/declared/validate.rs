//! The checks a job file must pass before it is declared: a task graph the orchestrator
//! can finish, notifications this box can deliver, and limits flowlite.toml names.

use std::collections::{HashMap, HashSet};
use crate::app_config::AppConfig;
use crate::crud::job_run_notification::NotificationChannel;
use crate::yaml_models::job_yaml::JobYaml;
use super::declared::job_notify_recipients;


/// Rejects a job that asks to be told over a channel this box cannot deliver on. Read
/// here rather than at send time because a notification that silently never leaves is
/// the one failure you cannot see from the run afterwards - and by then it is 03:00
/// and the run everyone wanted to hear about has already finished.
///
/// Both blocks are checked the same way and the error names the one at fault, since a
/// job may well ask for Slack on a success and only mail on a failure.
///
/// The match is over the same enum the sender matches on, so a new channel cannot be
/// added without saying what flowlite.toml section it needs to work at all.
pub(super) fn validate_job_notifications(app_config: &AppConfig, job_yaml: &JobYaml) -> anyhow::Result<()> {

    let blocks = [
        ("on_failure", &job_yaml.on_failure),
        ("on_success", &job_yaml.on_success),
    ];

    for (block, notify) in blocks {
        for (channel, recipients) in job_notify_recipients(notify) {

            let missing_section = match channel {
                NotificationChannel::Email => app_config.smtp
                    .is_none()
                    .then_some("[smtp]"),
                NotificationChannel::Slack => app_config.slack
                    .is_none()
                    .then_some("[slack]"),
            };

            if let Some(section) = missing_section {
                anyhow::bail!(
                    "{}.{} names {} but flowlite.toml has no {} section, so nothing \
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

/// Rejects a job that claims a concurrency limit `[concurrency_limits]` in flowlite.toml
/// never named. A name is valid by being a key of that map - the number behind it is
/// irrelevant here, so a limit configured `0` is still a name a job may claim. Checking
/// this at startup rather than at enforcement is what makes a typo'd name fail loudly
/// instead of quietly running unbounded, indistinguishable from a job that never named
/// a limit at all. `global` can never be a key (see `AppConfig::load`), so a job that
/// claims it fails here with no special case.
pub(super) fn validate_job_limits(app_config: &AppConfig, job_yaml: &JobYaml) -> anyhow::Result<()> {

    let concurrency_limits = &app_config.concurrency_limits;

    for limit in &job_yaml.limits {
        if !concurrency_limits.contains_key(limit) {
            anyhow::bail!(
                "Job '{}' claims limit '{}', which is not a key of \
                 [concurrency_limits] in flowlite.toml",
                job_yaml.id,
                limit,
            );
        }
    }

    for task_yaml in &job_yaml.tasks {
        for limit in &task_yaml.limits {
            if !concurrency_limits.contains_key(limit) {
                anyhow::bail!(
                    "Job '{}' task '{}' claims limit '{}', which is not a key of \
                     [concurrency_limits] in flowlite.toml",
                    job_yaml.id,
                    task_yaml.id,
                    limit,
                );
            }
        }
    }

    Ok(())
}

/// Rejects a task graph the orchestrator could never finish. TaskRunDispatcher only
/// starts a task run once every task run it depends on has succeeded, so a dependency
/// on a task that isn't part of the job, or a cycle between tasks, would leave the
/// task runs queued - and their job run running - forever.
pub(super) fn validate_job_tasks(job_yaml: &JobYaml) -> anyhow::Result<()> {

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

    if let Some(cycle) = find_task_dependency_cycle(job_yaml) {
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

        let cycle = walk_task_dependencies(
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

        let cycle = walk_task_dependencies(
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::app_config::{AppConfigSlack, AppConfigSmtp, AppConfigSmtpEncryption};

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

    fn config_with(smtp: Option<AppConfigSmtp>, slack: Option<AppConfigSlack>) -> AppConfig {
        AppConfig { smtp, slack, ..AppConfig::default() }
    }

    fn job_yaml(notify: &str) -> JobYaml {
        serde_yaml::from_str(&format!(
            "id: nightly\nname: Nightly\n{}tasks:\n  - id: sync\n    command: ./sync.sh\n",
            notify,
        )).unwrap()
    }

    #[test]
    fn a_job_naming_an_address_with_no_smtp_section_is_refused() {

        let app_config = config_with(None, None);

        let error = validate_job_notifications(&app_config, &job_yaml("on_failure:\n  email: [oncall@example.com]\n"))
            .unwrap_err()
            .to_string();

        assert!(error.contains("oncall@example.com"), "{}", error);
        assert!(error.contains("[smtp]"), "{}", error);
    }

    #[test]
    fn a_job_naming_an_address_is_accepted_once_smtp_is_configured() {

        let app_config = config_with(Some(smtp()), None);

        assert!(validate_job_notifications(&app_config, &job_yaml("on_failure:\n  email: [oncall@example.com]\n"))
            .is_ok());
    }

    /// The check is about a job that asked for something it cannot have — a job that asks
    /// for nothing is fine on a box with no mail at all.
    #[test]
    fn a_job_naming_nobody_needs_no_smtp_section() {

        let app_config = config_with(None, None);

        assert!(validate_job_notifications(&app_config, &job_yaml("")).is_ok());
    }

    /// The same refusal for the second channel, and it names `[slack]` rather than the
    /// section the first channel happened to need.
    #[test]
    fn a_job_naming_a_conversation_with_no_slack_section_is_refused() {

        let app_config = config_with(Some(smtp()), None);

        let error = validate_job_notifications(&app_config, &job_yaml("on_failure:\n  slack: ['#oncall']\n"))
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

        assert!(validate_job_notifications(&config_with(Some(smtp()), None), &job_yaml(both)).is_err());

        assert!(validate_job_notifications(&config_with(None, Some(slack())), &job_yaml(both)).is_err());

        assert!(validate_job_notifications(&config_with(Some(smtp()), Some(slack())), &job_yaml(both)).is_ok());
    }

    /// A success block is not a second-class one: it is checked against the same
    /// sections, so a job that would have gone quiet on every good run refuses to start.
    #[test]
    fn a_job_naming_a_success_recipient_with_no_smtp_section_is_refused() {

        let app_config = config_with(None, None);

        let error = validate_job_notifications(&app_config, &job_yaml("on_success:\n  email: [data-team@example.com]\n"))
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

        let app_config = config_with(Some(smtp()), None);

        let error = validate_job_notifications(&app_config, &job_yaml(
                "on_failure:\n  email: [oncall@example.com]\non_success:\n  slack: ['#data']\n"
            ))
            .unwrap_err()
            .to_string();

        assert!(error.contains("on_success.slack"), "{}", error);
        assert!(error.contains("[slack]"), "{}", error);
    }

    fn config_with_limits(concurrency_limits: BTreeMap<String, u32>) -> AppConfig {
        AppConfig { concurrency_limits, ..AppConfig::default() }
    }

    /// A job's own claim is checked the same way a task's is, and the error names the job
    /// and the limit - a typo'd name must never be mistaken for an unbounded job.
    #[test]
    fn a_job_naming_an_unconfigured_limit_is_refused() {

        let app_config = config_with_limits(BTreeMap::new());

        let job: JobYaml = serde_yaml::from_str("
id: nightly-sync
name: Nightly Sync
limits: [warehouse]
tasks:
  - id: ingest
    command: ./run.sh
").unwrap();

        let error = validate_job_limits(&app_config, &job).unwrap_err().to_string();

        assert!(error.contains("nightly-sync"), "{}", error);
        assert!(error.contains("warehouse"), "{}", error);
    }

    /// Same check at task level, and the error names the task too, since a job may have
    /// several and only one of them claimed the bad name.
    #[test]
    fn a_task_naming_an_unconfigured_limit_is_refused_naming_the_task() {

        let app_config = config_with_limits(BTreeMap::from([("warehouse".to_string(), 3)]));

        let job: JobYaml = serde_yaml::from_str("
id: nightly-sync
name: Nightly Sync
tasks:
  - id: ingest
    command: ./run.sh
    limits: [warehouse, api]
").unwrap();

        let error = validate_job_limits(&app_config, &job).unwrap_err().to_string();

        assert!(error.contains("nightly-sync"), "{}", error);
        assert!(error.contains("ingest"), "{}", error);
        assert!(error.contains("api"), "{}", error);
    }

    /// A limit configured `0` is still a real name - `0` means no limit, not missing - so
    /// claiming it at either level is accepted rather than treated as unconfigured.
    #[test]
    fn a_job_and_task_naming_only_configured_limits_are_accepted() {

        let app_config = config_with_limits(BTreeMap::from([
            ("warehouse".to_string(), 3),
            ("api".to_string(), 0),
        ]));

        let job: JobYaml = serde_yaml::from_str("
id: nightly-sync
name: Nightly Sync
limits: [warehouse]
tasks:
  - id: ingest
    command: ./run.sh
    limits: [warehouse, api]
").unwrap();

        assert!(validate_job_limits(&app_config, &job).is_ok());
    }
}
