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
    /// Environment variables whose values are secrets, as variable name to secret name.
    /// The name is what travels - through the row, the dashboard and `--json`; the value is
    /// resolved out of config at spawn and exists only in the command's environment.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub secret_env: BTreeMap<String, String>,
    /// The command's working directory, empty to inherit the server's.
    #[serde(default)]
    pub working_dir: String,
}

/// Who to tell about one way a run can end — the shape of both `on_failure:` and
/// `on_success:`. Each key is a channel and holds who that channel tells — addresses
/// under `email`, conversations under `slack`. A job may name both, and gets one
/// notification per channel it names.
///
/// One struct for the two blocks because they are one concept asked twice: the channels a
/// failure can be announced over are exactly the channels a success can. A job wanting to
/// be told about both names the same channels under each.
///
/// One field per channel rather than a map keyed by channel name, for the reason
/// `NotificationChannel` is an enum and not a registry: the channels are known at compile
/// time, and adding one should be a field the compiler carries through rather than a
/// string somebody has to spell right. `CRUD::job_notify_recipients` is what turns this
/// into the channel-to-recipients map the job row carries.
///
/// Each is a block rather than a bare `notify_email:` so the action to run on a failure
/// can join it later without the two ways of reacting reading as unrelated keys.
#[derive(Deserialize, Validate, Debug, Default)]
pub struct JobYamlNotify {
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
    /// Environment variables whose values are secrets, as variable name to secret name.
    /// The name is what travels - through the row, the dashboard and `--json`; the value is
    /// resolved out of config at spawn and exists only in the command's environment.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub secret_env: BTreeMap<String, String>,
    /// Who to tell when a run of this job fails or times out. Naming a recipient of a
    /// channel config.toml does not configure is a startup error — see
    /// `CRUD::validate_job_notifications`.
    #[serde(default)]
    pub on_failure: JobYamlNotify,
    /// Who to tell when a run of this job succeeds. Separate from `on_failure` rather
    /// than a flag on it, because the two are addressed to different people as often as
    /// not: a failure wakes whoever is on call, a success reassures whoever is waiting on
    /// the data.
    #[serde(default)]
    pub on_success: JobYamlNotify,
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

        job.validate_secret_env().with_context(|| format!("Invalid Job YAML at {}", path.display()))?;

        Ok(job)
    }

    /// `secret_env` can only ever come from a file - unlike a parameter, nothing at
    /// submit time can add or override one - so its self-consistency is checked here,
    /// once, rather than every time `CRUD` reads the job back out of the row.
    fn validate_secret_env(&self) -> anyhow::Result<()> {
        validate_secret_env_block(&self.id, None, &self.env, &self.secret_env)?;

        for task in &self.tasks {
            validate_secret_env_block(&self.id, Some(task.id.as_str()), &task.env, &task.secret_env)?;
        }

        Ok(())
    }
}

/// One `env:`/`secret_env:` pair - the job's own, or one task's - checked in isolation.
/// Checking level by level rather than across the whole job is what makes a job-level
/// default and a task-level secret_env override of the same name legitimate: they are
/// never compared against each other, only each block against its own level's `env:`.
fn validate_secret_env_block(
    job_id: &str,
    task_id: Option<&str>,
    env: &BTreeMap<String, String>,
    secret_env: &BTreeMap<String, String>,
) -> anyhow::Result<()> {

    let subject = match task_id {
        Some(task_id) => format!("Job '{}' task '{}'", job_id, task_id),
        None => format!("Job '{}'", job_id),
    };

    for (variable_name, secret_name) in secret_env {

        if !is_valid_env_var_name(variable_name) {
            anyhow::bail!(
                "{} has a secret_env variable named '{}', which is not a valid \
                 environment variable name. A variable name may contain only ASCII \
                 letters, digits and underscores, and may not start with a digit.",
                subject,
                variable_name,
            );
        }

        if variable_name.starts_with("FLOWLITE_") {
            anyhow::bail!(
                "{} has a secret_env variable named '{}', which starts with FLOWLITE_. \
                 Run metadata is applied last under that prefix and would silently \
                 overwrite it, leaving the task without its credential.",
                subject,
                variable_name,
            );
        }

        if !is_valid_secret_name(secret_name) {
            anyhow::bail!(
                "{} names secret '{}' for variable '{}', which is not a valid secret \
                 name. A secret name may contain only lowercase ASCII letters, digits \
                 and underscores: config.toml can hold other characters, but \
                 FLOWLITE_SECRETS__* cannot reach them, so the name would work on a \
                 development box and be unreachable in production.",
                subject,
                secret_name,
                variable_name,
            );
        }

        if env.contains_key(variable_name) {
            anyhow::bail!(
                "{} declares '{}' in both env and secret_env. A name may appear in \
                 only one of the two blocks at the same level; a job-level env or \
                 secret_env may still be overridden by the other block on a task.",
                subject,
                variable_name,
            );
        }
    }

    Ok(())
}

/// The same rule `is_valid_parameter_name` in
/// `src/crud/multistatements/misc.rs` states for a parameter name, duplicated rather than
/// shared: a parameter is validated at submit time in CRUD and a `secret_env` variable at
/// parse time in the YAML layer, and the two are kept in their own layers on purpose - a
/// job's `secret_env` can only ever come from its file, so its own parser is where its
/// self-consistency belongs.
fn is_valid_env_var_name(name: &str) -> bool {
    let mut chars = name.chars();

    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `FLOWLITE_SECRETS__*` is how a secret's value reaches `AppConfig` in production, and it
/// can only carry the characters an environment variable name can - so a secret name
/// outside that set would parse from `[secrets]` in `config.toml` on a development box
/// and never be reachable through the env var form at all.
fn is_valid_secret_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_yaml` only takes a path, so each test writes its YAML to a throwaway file
    /// under a unique name - the same isolation `TestDb` gives a database.
    fn parse(content: &str) -> anyhow::Result<JobYaml> {
        let path = std::env::temp_dir().join(format!("flowlite-job-yaml-test-{}.yml", uuid::Uuid::new_v4()));
        std::fs::write(&path, content).unwrap();
        let result = JobYaml::from_yaml(&path);
        let _ = std::fs::remove_file(&path);
        result
    }

    /// `{:?}` rather than `to_string()`, because `anyhow::Error`'s `Display` shows only
    /// the outermost context - the one `from_yaml` adds - and drops the specific cause
    /// these tests assert on. `main.rs` prints errors the same way.
    fn parse_error(content: &str) -> String {
        format!("{:?}", parse(content).unwrap_err())
    }

    #[test]
    fn a_secret_env_variable_name_that_is_not_a_valid_env_var_name_is_rejected() {
        let error = parse_error("
id: nightly-sync
name: Nightly Sync
tasks:
  - id: ingest
    command: ./run.sh
    secret_env:
      1secret: warehouse_pw
");

        assert!(error.contains("nightly-sync"), "{error}");
        assert!(error.contains("ingest"), "{error}");
        assert!(error.contains("1secret"), "{error}");
    }

    #[test]
    fn a_secret_name_outside_lowercase_letters_digits_and_underscores_is_rejected() {
        let error = parse_error("
id: nightly-sync
name: Nightly Sync
secret_env:
  DB_PASSWORD: Warehouse-PW
");

        assert!(error.contains("Warehouse-PW"), "{error}");
        assert!(error.contains("production"), "{error}");
    }

    #[test]
    fn a_secret_env_variable_named_with_the_flowlite_prefix_is_rejected() {
        let error = parse_error("
id: nightly-sync
name: Nightly Sync
secret_env:
  FLOWLITE_TOKEN: warehouse_pw
");

        assert!(error.contains("nightly-sync"), "{error}");
        assert!(error.contains("FLOWLITE_TOKEN"), "{error}");
    }

    #[test]
    fn a_name_in_both_env_and_secret_env_of_the_same_task_is_rejected() {
        let error = parse_error("
id: nightly-sync
name: Nightly Sync
tasks:
  - id: ingest
    command: ./run.sh
    env:
      DB_PASSWORD: plain
    secret_env:
      DB_PASSWORD: warehouse_pw
");

        assert!(error.contains("ingest"), "{error}");
        assert!(error.contains("DB_PASSWORD"), "{error}");
    }

    /// The same name may still appear once on the job and once on a task - a job
    /// declaring a default that a task replaces with a secret is legitimate layering,
    /// not the same-level collision the previous test rejects.
    #[test]
    fn a_valid_job_with_both_blocks_at_both_levels_parses() {
        let job = parse("
id: nightly-sync
name: Nightly Sync
env:
  REGION: eu
secret_env:
  DB_PASSWORD: warehouse_pw
tasks:
  - id: ingest
    command: ./run.sh
    env:
      DB_PASSWORD: plain
    secret_env:
      API_TOKEN: ingest_api_token
").unwrap();

        assert_eq!(job.secret_env.get("DB_PASSWORD").unwrap(), "warehouse_pw");
        let task = &job.tasks[0];
        assert_eq!(task.env.get("DB_PASSWORD").unwrap(), "plain");
        assert_eq!(task.secret_env.get("API_TOKEN").unwrap(), "ingest_api_token");
    }
}
