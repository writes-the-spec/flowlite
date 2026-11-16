use crate::declared::Declared;

/// The id of an installed job, or an error naming the one nothing matched.
pub(crate) fn installed_job_id(declared: &Declared, job_name: &str) -> anyhow::Result<String> {

    match declared.job(job_name) {
        Some(job) => Ok(job.job_id.clone()),
        None => anyhow::bail!("Job {} not found", job_name),
    }
}
