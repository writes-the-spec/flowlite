use clap::{Args, Subcommand};
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::router::app::format;

#[derive(Args)]
pub struct JobCmd {
    #[command(subcommand)]
    pub command: JobSubcommand,

    /// Print the result as JSON instead of as text.
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand)]
pub enum JobSubcommand {
    List(JobListCmd),
    Submit(JobSubmitCmd),
}

/// List the jobs declared in the data directory.
#[derive(Args)]
pub struct JobListCmd {
}

/// Submit a run of a job.
#[derive(Args)]
pub struct JobSubmitCmd {
    pub job_name: String,

    /// A parameter for this run, as name=value. Repeat for more than one.
    #[arg(long = "param", value_name = "NAME=VALUE", value_parser = parse_param)]
    pub params: Vec<(String, String)>,

    /// Wait for the run to finish, and exit non-zero unless it succeeded.
    #[arg(long)]
    pub wait: bool,
}

impl JobCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let _memory_conn = toolkit.get_memory_conn().await?;

        match &self.command {
            JobSubcommand::List(cmd) => cmd.run(toolkit, self.json).await,
            JobSubcommand::Submit(cmd) => cmd.run(toolkit, self.json).await,
        }
    }
}

impl JobListCmd {
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        crud.init(&mut conn).await?;

        let jobs = crud.select_jobs(&mut conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: None,
                name_like: None,
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        if json {
            println!("{}", serde_json::to_string_pretty(&jobs)?);
            return Ok(());
        }

        if jobs.is_empty() {
            println!("No jobs found");
        } else {
            println!("{:<20} {:<20}", "Job ID", "Name");
            println!("{}", "-".repeat(40));
            for job in jobs {
                println!("{:<20} {:<20}", job.job_id, job.name);
            }
        }

        Ok(())
    }
}

impl JobSubmitCmd {
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let poll_interval = toolkit.app_config.orchestrator.poll_interval();

        let mut conn = toolkit.get_conn().await?;
        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        crud.init(&mut conn).await?;

        let job = crud.select_job(&mut conn, &SelectJobsData {
            filter: SelectJobsDataFilter {
                job_id: Some(self.job_name.clone()),
                name_like: None,
            },
            sort: None,
            limit: None,
            offset: None,
        }).await?;

        let Some(job) = job else {
            anyhow::bail!("Job {} not found", self.job_name);
        };

        let overrides: std::collections::BTreeMap<String, String> =
            self.params.iter().cloned().collect();

        let job_run_id = crud.submit_job(
            &mut conn,
            &job.job_id,
            &overrides,
            None,
        ).await?;

        // The run is read back even without --wait, so that --json prints one shape either
        // way and a caller can read .status off both.
        let job_run = match self.wait {
            true => wait_for_job_run(&crud, &mut conn, job_run_id, poll_interval).await?,
            false => select_job_run(&crud, &mut conn, job_run_id).await?,
        };

        // Only a run that was waited for has an outcome worth an exit code: without --wait
        // it is pending by construction, which is not a failure.
        let outcome_error = match self.wait {
            true => run_outcome_error(job_run.id, job_run.status),
            false => None,
        };

        // A failure is reported once, by main, on stderr. Saying it here too would print
        // the same sentence twice for the ending a caller is most likely to be reading.
        if json {
            println!("{}", serde_json::to_string_pretty(&job_run)?);
        } else if !self.wait {
            println!("Job {} submitted successfully. Job Run ID: {}", self.job_name, job_run.id);
        } else if outcome_error.is_none() {
            println!("Job run {} {}", job_run.id, format::job_run_word(job_run.status));
        }

        if let Some(error) = outcome_error {
            anyhow::bail!(error);
        }

        Ok(())
    }
}

/// The run, or an error naming the id nothing matched.
async fn select_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
) -> anyhow::Result<JobRun> {

    let job_run = crud.select_job_run(&mut *conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await?;

    match job_run {
        Some(job_run) => Ok(job_run),
        None => anyhow::bail!("Job run {} not found", job_run_id),
    }
}

/// Blocks until the run has settled, and reports it as it settled.
///
/// Signals never leave the process that publishes them, and the run is executed by the
/// serve process rather than this one, so there is nothing to subscribe to here - the
/// table is the only channel between the two, and polling it is the whole mechanism.
/// The interval is the orchestrator's own, since a run cannot settle any sooner than the
/// pass that settles it.
async fn wait_for_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    poll_interval: std::time::Duration,
) -> anyhow::Result<JobRun> {

    loop {
        let job_run = select_job_run(crud, &mut *conn, job_run_id).await?;

        if job_run.status.is_finished() {
            return Ok(job_run);
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// What `--wait` leaves the command with once the run has settled: None for a success,
/// so the process exits 0, and a message otherwise, which main prints before exiting 1.
///
/// Matched exhaustively so a new status has to say which side of that line it falls on.
/// Only a finished run reaches this, so the two unfinished statuses cannot arrive here -
/// they are grouped with the failures because a run that somehow did is not a success.
fn run_outcome_error(job_run_id: i64, status: JobRunStatus) -> Option<String> {
    match status {
        JobRunStatus::Succeeded => None,
        JobRunStatus::Pending
        | JobRunStatus::Running
        | JobRunStatus::Failed
        | JobRunStatus::Skipped
        | JobRunStatus::Aborted
        | JobRunStatus::TimedOut
        | JobRunStatus::Invalid => Some(format!(
            "Job run {} {}",
            job_run_id,
            format::job_run_word(status),
        )),
    }
}

fn parse_param(raw: &str) -> Result<(String, String), String> {

    let Some((name, value)) = raw.split_once('=') else {
        return Err(format!("expected name=value, got '{}'", raw));
    };

    if name.is_empty() {
        return Err(format!("the parameter name is empty in '{}'", raw));
    }

    Ok((name.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::crud::job_run::{UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
    use crate::test_support::TestDb;

    #[test]
    fn a_pair_splits_into_name_and_value() {
        assert_eq!(
            parse_param("region=us").unwrap(),
            ("region".to_string(), "us".to_string()),
        );
    }

    /// Splitting on the first = only, so a value may contain one.
    #[test]
    fn a_value_may_contain_an_equals_sign() {
        assert_eq!(
            parse_param("filter=a=b").unwrap(),
            ("filter".to_string(), "a=b".to_string()),
        );
    }

    #[test]
    fn an_empty_value_is_allowed() {
        assert_eq!(
            parse_param("slice=").unwrap(),
            ("slice".to_string(), String::new()),
        );
    }

    #[test]
    fn a_pair_without_an_equals_sign_is_refused() {
        assert!(parse_param("region").unwrap_err().contains("name=value"));
    }

    #[test]
    fn an_empty_name_is_refused() {
        assert!(parse_param("=us").unwrap_err().contains("name"));
    }

    #[test]
    fn a_succeeded_run_leaves_the_command_with_nothing_to_report() {
        assert_eq!(run_outcome_error(7, JobRunStatus::Succeeded), None);
    }

    #[test]
    fn a_failed_run_is_an_error_naming_the_run_and_what_happened() {
        assert_eq!(
            run_outcome_error(7, JobRunStatus::Failed),
            Some("Job run 7 failed".to_string()),
        );
    }

    /// Every ending that is not a success exits non-zero, not only the one called Failed -
    /// a caller waiting on a run wants to hear about a timeout just as much.
    #[test]
    fn a_timed_out_run_is_an_error_too() {
        assert_eq!(
            run_outcome_error(7, JobRunStatus::TimedOut),
            Some("Job run 7 timed out".to_string()),
        );
    }

    #[test]
    fn an_aborted_run_is_an_error_too() {
        assert_eq!(
            run_outcome_error(7, JobRunStatus::Aborted),
            Some("Job run 7 aborted".to_string()),
        );
    }

    #[tokio::test]
    async fn waiting_on_a_run_that_has_already_finished_returns_it_at_once() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Failed).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let finished = wait_for_job_run(
            &db.crud,
            &mut conn,
            job_run.id,
            Duration::from_secs(30),
        ).await.unwrap();

        assert_eq!(finished.status, JobRunStatus::Failed);
    }

    /// The wait is a poll of the table, not a signal: the run is settled over another
    /// connection here the way the serve process settles it from another process.
    #[tokio::test]
    async fn waiting_returns_once_somebody_else_finishes_the_run() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();
        let job_run_id = job_run.id;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;

            crud.update_job_runs(&*conn_pool, &UpdateJobRunsData {
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Succeeded),
                    started_at: None,
                    finished_at: None,
                },
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
            }).await.unwrap();
        });

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let finished = wait_for_job_run(
            &db.crud,
            &mut conn,
            job_run.id,
            Duration::from_millis(1),
        ).await.unwrap();

        assert_eq!(finished.status, JobRunStatus::Succeeded);
    }

    #[tokio::test]
    async fn waiting_on_a_run_that_does_not_exist_raises() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = wait_for_job_run(
            &db.crud,
            &mut conn,
            404,
            Duration::from_millis(1),
        ).await.unwrap_err().to_string();

        assert!(error.contains("404"), "{error}");
    }
}
