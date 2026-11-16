//! `add_ad_hoc_job`: what `job submit -f` and the MCP `submit_job` tool both do with a
//! definition that is not installed in the data directory.

use std::path::{Path, PathBuf};

use crate::app_config::AppConfig;
use crate::yaml_models::job_yaml::JobYaml;
use super::declared::Declared;

impl Declared {

    /// Adds a job definition that is not installed in the data directory, kept in one place
    /// so `job submit -f` and the MCP tool's `file` and `yaml` arguments cannot drift apart:
    /// refuse a collision with an installed job, add the parsed definition, then check its
    /// own `secret_env` is satisfied - scoped to just this job, not every installed one.
    ///
    /// A collision comes back as `JobIdAlreadyInstalled`, downcastable out of the
    /// `anyhow::Error`, rather than a finished sentence: the CLI's `-f` and the MCP tool's
    /// `job` argument each name an installed job a different way, and wording that remedy
    /// is each caller's own job.
    pub fn add_ad_hoc_job(
        &mut self,
        app_config: &AppConfig,
        job_yaml: JobYaml,
        job_path: &Path,
    ) -> anyhow::Result<String> {

        let job_id = job_yaml.id.clone();

        if self.job(&job_id).is_some() {
            return Err(JobIdAlreadyInstalled {
                job_id,
                jobs_dir: Path::new(&app_config.data_dir).join("jobs"),
            }.into());
        }

        self.add_job(app_config, job_yaml, job_path)?;

        self.check_secret_env_is_satisfied(&app_config.secrets, Some(&job_id))?;

        Ok(job_id)
    }
}

/// The one fact `add_ad_hoc_job` refuses on: an installed job already holds this id.
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
