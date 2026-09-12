use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::CRUD;

/// The id of an installed job, or an error naming the one nothing matched.
pub(crate) async fn installed_job_id(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_name: &str,
) -> anyhow::Result<String> {

    let job = crud.select_job(&mut *conn, &SelectJobsData {
        filter: SelectJobsDataFilter {
            job_id: Some(job_name.to_string()),
            name_like: None,
        },
        sort: None,
        limit: None,
        offset: None,
    }).await?;

    match job {
        Some(job) => Ok(job.job_id),
        None => anyhow::bail!("Job {} not found", job_name),
    }
}
