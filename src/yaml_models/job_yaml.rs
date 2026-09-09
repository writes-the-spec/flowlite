use serde::Deserialize;
use anyhow::Context;
use validator::Validate;
use std::collections::BTreeMap;
use crate::yaml_models::string_map::deserialize_string_map;


#[derive(Deserialize, Validate, Debug)]
pub struct JobYamlTask {
    pub id: String,
    #[serde(default)]
    pub description: String,
    pub command: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Seconds one attempt may run for. `None` is "not declared", which `CRUD::init`
    /// resolves against `[job_defaults]` in config.toml rather than a number written here.
    #[serde(default)]
    pub timeout: Option<u32>,
    #[serde(default)]
    pub max_retries: Option<u32>,
    /// Seconds to wait after a failed attempt before the next one starts.
    #[serde(default)]
    pub retry_delay: Option<u32>,
    /// Environment variables for the command, over the environment flowlite inherited.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub env: BTreeMap<String, String>,
    /// The command's working directory, empty to inherit the server's.
    #[serde(default)]
    pub working_dir: String,
}

/// What happens when a run of this job does not succeed. Each key is a channel and holds
/// who that channel tells — addresses under `email`, conversations under `slack`. A job
/// may name both, and gets one notification per channel it names.
///
/// One field per channel rather than a map keyed by channel name, for the reason
/// `NotificationChannel` is an enum and not a registry: the channels are known at compile
/// time, and adding one should be a field the compiler carries through rather than a
/// string somebody has to spell right. `CRUD::job_on_failure_recipients` is what turns
/// this into the channel-to-recipients map the job row carries.
///
/// The block is a block rather than a bare `notify_email:` so the action to run on a
/// failure can join it later without the two ways of reacting reading as unrelated keys.
#[derive(Deserialize, Validate, Debug, Default)]
pub struct JobYamlOnFailure {
    #[serde(default)]
    pub email: Vec<String>,
    /// Conversations, as `#channel`, a channel id, or a user id for a direct message —
    /// whatever the bot can post in.
    #[serde(default)]
    pub slack: Vec<String>,
}

#[derive(Deserialize, Validate, Debug)]
pub struct JobYaml {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// How many runs of this job may run in parallel, 0 for no limit. `None` is "not
    /// declared" — see `JobYamlTask::timeout`.
    #[serde(default)]
    pub max_parallel_runs: Option<u32>,
    /// The parameters this job accepts, name to default value. A schedule or the CLI may
    /// override a declared name; an undeclared one is a submit error.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub parameters: BTreeMap<String, String>,
    /// Environment variables for every task of this job. A task's own `env:` wins the
    /// names both of them set.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub env: BTreeMap<String, String>,
    /// Who to tell when a run of this job fails or times out. Naming a recipient of a
    /// channel config.toml does not configure is a startup error — see
    /// `CRUD::validate_job_notifications`.
    #[serde(default)]
    pub on_failure: JobYamlOnFailure,
    #[serde(default)]
    pub tasks: Vec<JobYamlTask>,
}

impl JobYaml {
    pub fn from_yaml(path: &std::path::Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read Job YAML from {}", path.display()))?;

        let job: JobYaml = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse Job YAML from {}", path.display()))?;

        job.validate().with_context(|| format!("Invalid Job YAML at {}", path.display()))?;

        Ok(job)
    }
}
