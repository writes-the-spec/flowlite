use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use anyhow::Context;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use crate::app_config::AppConfig;
use crate::crud::job_run_notification::NotificationChannel;
use crate::yaml_models::job_yaml::{JobYaml, JobYamlNotify};
use crate::yaml_models::schedule_yaml::ScheduleYaml;
use super::validate::{validate_job_limits, validate_job_notifications, validate_job_tasks};


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    pub job_id: String,
    pub name: String,
    pub description: String,
    pub max_parallel_runs: u32,
    /// The newest finished runs of this job retention keeps, 0 for all of them. See
    /// `JobYaml::keep_runs`.
    pub keep_runs: u32,
    pub parameters: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    /// Environment variable name to secret name - never a value. See `JobYaml::secret_env`.
    pub secret_env: BTreeMap<String, String>,
    /// Who to tell when a run of this job fails, keyed by the channel that tells them. A
    /// channel the YAML named nobody under is absent rather than empty.
    pub on_failure_recipients: BTreeMap<NotificationChannel, Vec<String>>,
    /// The same, for a run that succeeds.
    pub on_success_recipients: BTreeMap<NotificationChannel, Vec<String>>,
    /// Named concurrency limits this job claims, on top of whatever each of its tasks
    /// claims for itself. See `JobYaml::limits`.
    pub limits: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Task {
    pub task_id: String,
    pub job_id: String,
    pub description: String,
    pub command: String,
    /// What the command reads on stdin, empty for nothing at all. See `JobYamlTask::stdin`.
    pub stdin: String,
    pub depends_on: Vec<String>,
    /// Named concurrency limits this task claims, on top of whatever its job claims. See
    /// `JobYamlTask::limits`.
    pub limits: Vec<String>,
    pub timeout: u32,
    pub idle_timeout: u32,
    pub max_retries: u32,
    pub retry_delay: u32,
    pub env: BTreeMap<String, String>,
    /// Environment variable name to secret name - never a value. See `JobYamlTask::secret_env`.
    pub secret_env: BTreeMap<String, String>,
    pub working_dir: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schedule {
    pub schedule_id: String,
    pub name: String,
    pub description: String,
    pub cron: String,
    pub timezone: String,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub disabled: bool,
    pub submit_ahead: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScheduleJob {
    pub schedule_id: String,
    pub job_id: String,
    pub parameters: BTreeMap<String, String>,
}


/// Every job, task and schedule the data directory declares, with `[job_defaults]` and
/// `[schedule_defaults]` already applied. Each list is in reading order: files sorted by
/// path, and the entries of one file as written - so a job's tasks come back in the order
/// its YAML lists them.
///
/// Loaded once per process (once per tool call in the MCP server, so a file written after
/// it started is seen) and never changed after, apart from the one ad-hoc job `job submit
/// -f` adds to its own copy.
#[derive(Debug, Clone, Default)]
pub struct Declared {
    pub jobs: Vec<Job>,
    pub tasks: Vec<Task>,
    pub schedules: Vec<Schedule>,
    pub schedule_jobs: Vec<ScheduleJob>,
}


impl Declared {

    pub fn load(app_config: &AppConfig) -> anyhow::Result<Declared> {

        let data_dir = PathBuf::from(&app_config.data_dir);

        let mut declared = Declared::default();

        let jobs_dir = data_dir.join("jobs");
        if jobs_dir.exists() {
            for job_path in read_dir_sorted(&jobs_dir)? {
                if is_yaml_file(&job_path) {
                    let job_yaml = JobYaml::from_yaml(&job_path)?;

                    declared.add_job(app_config, job_yaml, &job_path)?;
                }
            }
        }

        // After every job, since a schedule may only name a job some file declared.
        let schedules_dir = data_dir.join("schedules");
        if schedules_dir.exists() {
            for schedule_path in read_dir_sorted(&schedules_dir)? {
                if is_yaml_file(&schedule_path) {
                    let schedule_yaml = ScheduleYaml::from_yaml(&schedule_path)?;

                    declared.add_schedule(app_config, schedule_yaml, &schedule_path)?;
                }
            }
        }

        Ok(declared)
    }

    /// The only place a JobYaml becomes a `Job` and its `Task`s. `load` calls it once per
    /// file in the data directory; `add_ad_hoc_job` calls it for a file that is never
    /// installed there. A run's definition is built from what this produces either way, so
    /// the two must not drift.
    pub(super) fn add_job(
        &mut self,
        app_config: &AppConfig,
        job_yaml: JobYaml,
        job_path: &Path,
    ) -> anyhow::Result<()> {

        let job_defaults = &app_config.job_defaults;

        if self.job(&job_yaml.id).is_some() {
            anyhow::bail!(
                "Job '{}' at {} is declared by another file too. A job id must be unique.",
                job_yaml.id,
                job_path.display(),
            );
        }

        validate_job_tasks(&job_yaml)
            .with_context(|| format!(
                "Invalid tasks of job '{}' at {}",
                job_yaml.id,
                job_path.display(),
            ))?;

        validate_job_notifications(app_config, &job_yaml)
            .with_context(|| format!(
                "Invalid notifications of job '{}' at {}",
                job_yaml.id,
                job_path.display(),
            ))?;

        validate_job_limits(app_config, &job_yaml)
            .with_context(|| format!(
                "Invalid limits of job '{}' at {}",
                job_yaml.id,
                job_path.display(),
            ))?;

        self.jobs.push(Job {
            job_id: job_yaml.id.clone(),
            name: job_yaml.name,
            description: job_yaml.description,
            max_parallel_runs: job_yaml.max_parallel_runs
                .unwrap_or(job_defaults.max_parallel_runs),
            keep_runs: job_yaml.keep_runs
                .unwrap_or(job_defaults.keep_runs),
            parameters: job_yaml.parameters,
            env: job_yaml.env,
            secret_env: job_yaml.secret_env,
            on_failure_recipients: job_notify_recipients(&job_yaml.on_failure),
            on_success_recipients: job_notify_recipients(&job_yaml.on_success),
            limits: job_yaml.limits,
        });

        for task_yaml in job_yaml.tasks {
            self.tasks.push(Task {
                task_id: task_yaml.id,
                job_id: job_yaml.id.clone(),
                description: task_yaml.description,
                command: task_yaml.command,
                stdin: task_yaml.stdin,
                depends_on: task_yaml.depends_on,
                limits: task_yaml.limits,
                timeout: task_yaml.timeout
                    .unwrap_or(job_defaults.timeout_seconds),
                idle_timeout: task_yaml.idle_timeout
                    .unwrap_or(job_defaults.idle_timeout_seconds),
                max_retries: task_yaml.max_retries
                    .unwrap_or(job_defaults.max_retries),
                retry_delay: task_yaml.retry_delay
                    .unwrap_or(job_defaults.retry_delay_seconds),
                env: task_yaml.env,
                secret_env: task_yaml.secret_env,
                working_dir: task_yaml.working_dir,
            });
        }

        Ok(())
    }

    fn add_schedule(
        &mut self,
        app_config: &AppConfig,
        schedule_yaml: ScheduleYaml,
        schedule_path: &Path,
    ) -> anyhow::Result<()> {

        if self.schedule(&schedule_yaml.id).is_some() {
            anyhow::bail!(
                "Schedule '{}' at {} is declared by another file too. A schedule id must be \
                 unique.",
                schedule_yaml.id,
                schedule_path.display(),
            );
        }

        // Refused here rather than at submit time, where the scheduler would only log it
        // once a pass for as long as the server ran.
        for schedule_job_yaml in &schedule_yaml.jobs {
            if self.job(&schedule_job_yaml.id).is_none() {
                anyhow::bail!(
                    "Schedule '{}' at {} names job '{}', which no file under jobs/ declares",
                    schedule_yaml.id,
                    schedule_path.display(),
                    schedule_job_yaml.id,
                );
            }
        }

        self.schedules.push(Schedule {
            schedule_id: schedule_yaml.id.clone(),
            name: schedule_yaml.name,
            description: schedule_yaml.description,
            cron: schedule_yaml.cron.to_string(),
            timezone: schedule_yaml.timezone
                .unwrap_or(app_config.schedule_defaults.timezone)
                .to_string(),
            start_date: schedule_yaml.start_date,
            end_date: schedule_yaml.end_date,
            disabled: schedule_yaml.disabled,
            submit_ahead: schedule_yaml.submit_ahead,
        });

        for schedule_job_yaml in schedule_yaml.jobs {
            self.schedule_jobs.push(ScheduleJob {
                schedule_id: schedule_yaml.id.clone(),
                job_id: schedule_job_yaml.id,
                parameters: schedule_job_yaml.parameters,
            });
        }

        Ok(())
    }

    pub fn job(&self, job_id: &str) -> Option<&Job> {
        self.jobs.iter().find(|job| job.job_id == job_id)
    }

    /// The job's tasks, in the order its YAML lists them.
    pub fn tasks_of(&self, job_id: &str) -> Vec<&Task> {
        self.tasks.iter().filter(|task| task.job_id == job_id).collect()
    }

    pub fn task(&self, job_id: &str, task_id: &str) -> Option<&Task> {
        self.tasks
            .iter()
            .find(|task| task.job_id == job_id && task.task_id == task_id)
    }

    pub fn schedule(&self, schedule_id: &str) -> Option<&Schedule> {
        self.schedules.iter().find(|schedule| schedule.schedule_id == schedule_id)
    }

    /// The jobs a schedule names, in the order its YAML lists them.
    pub fn jobs_of_schedule(&self, schedule_id: &str) -> Vec<&ScheduleJob> {
        self.schedule_jobs
            .iter()
            .filter(|schedule_job| schedule_job.schedule_id == schedule_id)
            .collect()
    }
}


/// Who one of a job's notify blocks tells, keyed by the channel that will tell them.
///
/// The one place the YAML's per-channel fields become the shape everything downstream
/// works in: `Job` holds one of these per block, `submit_job` turns each entry into a
/// notification, and the notification check walks them to ask whether each channel is
/// configured. A channel naming nobody is left out entirely rather than carried as an
/// empty list - there is nothing to decide about later.
pub(super) fn job_notify_recipients(notify: &JobYamlNotify) -> BTreeMap<NotificationChannel, Vec<String>> {

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

/// Both spellings of the extension are accepted: either is a reasonable thing to call a
/// YAML file, and a config silently ignored for being named the other way is a bad way to
/// find that out.
fn is_yaml_file(path: &Path) -> bool {

    if !path.is_file() {
        return false;
    }

    matches!(path.extension().and_then(|s| s.to_str()), Some("yml") | Some("yaml"))
}

fn read_dir_sorted(path: &Path) -> anyhow::Result<Vec<PathBuf>> {

    let entries = fs::read_dir(path)
        .with_context(|| format!("Failed to read directory {}", path.display()))?;

    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();

    Ok(paths)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn data_dir() -> PathBuf {
        std::env::temp_dir().join(format!("flowlite-declared-{}", uuid::Uuid::new_v4()))
    }

    fn write_yaml(data_dir: &Path, directory: &str, file_name: &str, content: &str) {
        std::fs::create_dir_all(data_dir.join(directory)).unwrap();
        std::fs::write(data_dir.join(directory).join(file_name), content).unwrap();
    }

    fn load(data_dir: &Path, concurrency_limits: BTreeMap<String, u32>) -> anyhow::Result<Declared> {
        Declared::load(&AppConfig {
            data_dir: data_dir.to_string_lossy().into_owned(),
            concurrency_limits,
            ..AppConfig::default()
        })
    }

    const NIGHTLY_SYNC: &str = "
id: nightly-sync
name: Nightly Sync
tasks:
  - id: ingest
    command: ./run.sh
";

    /// `limits:` at both levels are two independent claims - the job's own and the task's
    /// own - kept unmerged: combining them is enforcement's job, not loading's.
    #[test]
    fn a_job_naming_limits_at_both_levels_keeps_them_on_the_job_and_the_task() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "nightly.yaml", "
id: nightly-sync
name: Nightly Sync
limits: [warehouse]
tasks:
  - id: ingest
    command: ./run.sh
    limits: [warehouse, api]
");

        let declared = load(&data_dir, BTreeMap::from([
            ("warehouse".to_string(), 3),
            ("api".to_string(), 1),
        ])).unwrap();

        assert_eq!(declared.job("nightly-sync").unwrap().limits, vec!["warehouse"]);
        assert_eq!(declared.task("nightly-sync", "ingest").unwrap().limits, vec!["warehouse", "api"]);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// A job that never declares `keep_runs:` gets `[job_defaults]`'s value, not 0.
    #[test]
    fn a_job_omitting_keep_runs_gets_the_job_defaults_value() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "nightly.yaml", NIGHTLY_SYNC);

        let declared = load(&data_dir, BTreeMap::new()).unwrap();

        assert_eq!(
            declared.job("nightly-sync").unwrap().keep_runs,
            AppConfig::default().job_defaults.keep_runs,
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn a_job_declaring_keep_runs_keeps_its_own_value() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "nightly.yaml", "
id: nightly-sync
name: Nightly Sync
keep_runs: 400
tasks:
  - id: ingest
    command: ./run.sh
");

        let declared = load(&data_dir, BTreeMap::new()).unwrap();

        assert_eq!(declared.job("nightly-sync").unwrap().keep_runs, 400);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The order `submit_job` creates a run's task runs in.
    #[test]
    fn a_jobs_tasks_come_back_in_the_order_its_yaml_lists_them() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "nightly.yaml", "
id: nightly-sync
name: Nightly Sync
tasks:
  - id: load
    command: ./load.sh
  - id: extract
    command: ./extract.sh
");

        let declared = load(&data_dir, BTreeMap::new()).unwrap();

        let task_ids: Vec<&str> = declared.tasks_of("nightly-sync")
            .iter()
            .map(|task| task.task_id.as_str())
            .collect();

        assert_eq!(task_ids, vec!["load", "extract"]);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn two_files_declaring_one_job_id_are_refused() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "a.yaml", NIGHTLY_SYNC);
        write_yaml(&data_dir, "jobs", "b.yaml", NIGHTLY_SYNC);

        let error = load(&data_dir, BTreeMap::new()).unwrap_err().to_string();

        assert!(error.contains("nightly-sync"), "{error}");
        assert!(error.contains("b.yaml"), "{error}");

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn a_schedule_naming_a_job_nothing_declares_is_refused() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "nightly.yaml", NIGHTLY_SYNC);
        write_yaml(&data_dir, "schedules", "nightly.yaml", "
id: nightly
name: Nightly
cron: '0 0 3 * * *'
jobs:
  - id: nightly-snyc
");

        let error = load(&data_dir, BTreeMap::new()).unwrap_err().to_string();

        assert!(error.contains("'nightly'"), "{error}");
        assert!(error.contains("nightly-snyc"), "{error}");

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn a_schedule_gets_the_schedule_defaults_timezone_and_lists_its_jobs() {

        let data_dir = data_dir();
        write_yaml(&data_dir, "jobs", "nightly.yaml", NIGHTLY_SYNC);
        write_yaml(&data_dir, "schedules", "nightly.yaml", "
id: nightly
name: Nightly
cron: '0 0 3 * * *'
jobs:
  - id: nightly-sync
    parameters:
      region: eu
");

        let declared = load(&data_dir, BTreeMap::new()).unwrap();

        let schedule = declared.schedule("nightly").unwrap();

        assert_eq!(
            schedule.timezone,
            AppConfig::default().schedule_defaults.timezone.to_string(),
        );

        let schedule_jobs = declared.jobs_of_schedule("nightly");

        assert_eq!(schedule_jobs.len(), 1);
        assert_eq!(schedule_jobs[0].job_id, "nightly-sync");
        assert_eq!(schedule_jobs[0].parameters["region"], "eu");

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn a_data_dir_with_no_yaml_declares_nothing() {

        let declared = load(&data_dir(), BTreeMap::new()).unwrap();

        assert!(declared.jobs.is_empty());
        assert!(declared.schedules.is_empty());
    }

    #[test]
    fn a_job_declaring_both_channels_becomes_one_map_entry_each() {

        let job_yaml: JobYaml = serde_yaml::from_str(
            "id: nightly\nname: Nightly\non_failure:\n  email: [oncall@example.com]\n  slack: ['#oncall', '#data']\ntasks:\n  - id: sync\n    command: ./sync.sh\n",
        ).unwrap();

        let recipients = job_notify_recipients(&job_yaml.on_failure);

        assert_eq!(recipients.len(), 2);
        assert_eq!(recipients[&NotificationChannel::Email], vec!["oncall@example.com"]);
        assert_eq!(recipients[&NotificationChannel::Slack], vec!["#oncall", "#data"]);
    }

    /// A channel the YAML mentions nobody under is left out rather than carried as an
    /// empty list, so a run is never submitted with a notification addressed to nobody.
    #[test]
    fn a_channel_naming_nobody_is_left_out_of_the_map() {

        let job_yaml: JobYaml = serde_yaml::from_str(
            "id: nightly\nname: Nightly\non_failure:\n  email: [oncall@example.com]\ntasks:\n  - id: sync\n    command: ./sync.sh\n",
        ).unwrap();

        let recipients = job_notify_recipients(&job_yaml.on_failure);

        assert_eq!(recipients.len(), 1);
        assert!(!recipients.contains_key(&NotificationChannel::Slack));
    }
}
