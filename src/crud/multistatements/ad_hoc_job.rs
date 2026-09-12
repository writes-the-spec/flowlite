//! `seed_ad_hoc_job`: the sequence `job submit -f` and the MCP `submit_job` tool both run
//! over a definition that is not installed in the data directory.

use std::path::{Path, PathBuf};
use sqlx::SqliteConnection;

use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::yaml_models::job_yaml::JobYaml;

impl CRUD {

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
