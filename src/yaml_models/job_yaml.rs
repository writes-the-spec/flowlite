use serde::Deserialize;
use crate::yaml_models::defaults::default_u32;
use anyhow::Context;
use validator::Validate;


#[derive(Deserialize, Validate, Debug)]
pub struct JobYamlTask {
    pub id: String,
    pub command: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default = "default_u32::<3600>")]
    pub timeout: u32,
    #[serde(default)]
    pub max_retries: u32,
    /// Seconds to wait after a failed attempt before the next one starts.
    #[serde(default = "default_u32::<60>")]
    pub retry_delay: u32,
}

#[derive(Deserialize, Validate, Debug)]
pub struct JobYaml {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// How many runs of this job may be active at once, 0 for no limit.
    #[serde(default = "default_u32::<1>")]
    pub max_active_runs: u32,
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
