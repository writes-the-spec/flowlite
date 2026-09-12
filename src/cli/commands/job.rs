use std::path::{Path, PathBuf};
use clap::{Args, Subcommand};
use crate::serve_state::{status, ServeStatus};
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::multistatements::misc::JobIdAlreadyInstalled;
use crate::router::app::format;
use crate::yaml_models::job_yaml::JobYaml;

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

/// Submit a run of a job: one installed in the data directory, by id, or a definition read
/// from a file that is never installed there at all.
#[derive(Args)]
#[command(group(
    clap::ArgGroup::new("definition").required(true).args(["job_name", "file"]),
))]
pub struct JobSubmitCmd {
    pub job_name: Option<String>,

    /// Submit the job defined by this file rather than one installed in the data
    /// directory. The file is read where it is and never copied there.
    #[arg(short = 'f', long, value_name = "FILE")]
    pub file: Option<PathBuf>,

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

        // Before the run is written, so a wait that cannot be serviced leaves no queued
        // run behind for a server that is not there to run it.
        if self.wait {
            ensure_data_dir_is_served(&toolkit.app_config.data_dir)
                .map_err(describe_unserved_data_dir)?;
        }

        let mut conn = toolkit.get_conn().await?;
        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let row_id = crud.init(&mut conn).await?;

        let job_id = match (&self.job_name, &self.file) {
            (Some(job_name), None) => installed_job_id(&crud, &mut conn, job_name).await?,
            (None, Some(file)) => seed_job_file(&crud, &mut conn, file, row_id).await?,
            // clap's `definition` group requires exactly one of the two.
            _ => anyhow::bail!("Name a job to submit, or pass -f to submit a file"),
        };

        let overrides: std::collections::BTreeMap<String, String> =
            self.params.iter().cloned().collect();

        let job_run_id = crud.submit_job(
            &mut conn,
            &job_id,
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
            println!("Job {} submitted successfully. Job Run ID: {}", job_id, job_run.id);
        } else if outcome_error.is_none() {
            println!("Job run {} {}", job_run.id, format::job_run_word(job_run.status));
        }

        if let Some(error) = outcome_error {
            anyhow::bail!(error);
        }

        Ok(())
    }
}

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

/// Seeds a job file that is not installed in the data directory, so its run is built from
/// the same rows every other run is built from.
///
/// The seeded job lives only in this process: `mem` is a shared-cache in-memory database,
/// which SQLite scopes to the process that opened it, and it is gone when this command
/// exits. `serve` and the dashboard never see it - only the run it produced, which carries
/// its own definition and so needs nothing to look back at.
///
/// The collision check, the seed and the secret check are `CRUD::seed_ad_hoc_job` - shared
/// with the MCP `submit_job` tool's `file` and `yaml` arguments, which run this same
/// sequence over a definition of their own. Only the remedy sentence is this command's own:
/// a collision comes back as a typed `JobIdAlreadyInstalled`, and this is where it becomes
/// the CLI's own words for "submit it by name instead."
async fn seed_job_file(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    file: &Path,
    row_id: u64,
) -> anyhow::Result<String> {

    let job_yaml = JobYaml::from_yaml(file)?;

    match crud.seed_ad_hoc_job(&mut *conn, job_yaml, file, row_id).await {
        Ok(job_id) => Ok(job_id),
        Err(err) => match err.downcast::<JobIdAlreadyInstalled>() {
            Ok(collision) => anyhow::bail!(
                "{}. Drop -f to submit it: flowlite job submit {}",
                collision,
                collision.job_id,
            ),
            Err(err) => Err(err),
        },
    }
}

/// The run, or an error naming the id nothing matched.
pub(crate) async fn select_job_run(
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

/// Refuses a wait that nothing would ever end.
///
/// The waits below poll a row that only the serve process writes, so waiting on a data
/// directory nothing is serving blocks for ever on a row that cannot change - silently,
/// which is worse than failing. Read once, before the wait rather than on every pass: a
/// wait is allowed to span a deliberate restart of serve, which is a reasonable thing to
/// do to a server while a long run is in flight.
///
/// `Starting` counts as served. That server has the lock and will reach the row.
///
/// The refusal comes back as a typed `DataDirNotServed`, not a finished sentence - see the
/// type. Each caller words its own remedy: the CLI's is `describe_unserved_data_dir` below.
pub(crate) fn ensure_data_dir_is_served(data_dir: &str) -> anyhow::Result<()> {

    if matches!(status(Path::new(data_dir))?, ServeStatus::Down) {
        return Err(DataDirNotServed { data_dir: data_dir.to_string() }.into());
    }

    Ok(())
}

/// The one fact `ensure_data_dir_is_served` refuses on: nothing is serving this directory,
/// so the row a wait polls has no writer.
///
/// A typed value rather than a finished sentence, for the same reason `JobIdAlreadyInstalled`
/// is one: the remedy names what the caller would have to drop, and the callers do not agree
/// on what that is. `--wait` is a CLI flag; the MCP tools take a `wait_seconds` argument and
/// have no flags at all, so telling an agent to "drop --wait" names something it never sent.
#[derive(Debug)]
pub struct DataDirNotServed {
    pub data_dir: String,
}

impl std::fmt::Display for DataDirNotServed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Data directory {} is not being served", self.data_dir)
    }
}

impl std::error::Error for DataDirNotServed {}

/// The CLI's own words for `DataDirNotServed`: the caller passed `--wait`, so dropping it is
/// the remedy. Shared by `job submit` and `job-run stop`, which pass the same flag and so say
/// the same sentence. Any other error passes through unchanged.
pub(crate) fn describe_unserved_data_dir(err: anyhow::Error) -> anyhow::Error {
    match err.downcast::<DataDirNotServed>() {
        Ok(unserved) => anyhow::anyhow!(
            "{}, so --wait would never return. Start flowlite serve against it, or drop \
             --wait.",
            unserved,
        ),
        Err(err) => err,
    }
}

/// Blocks until the run has settled, and reports it as it settled.
///
/// Signals never leave the process that publishes them, and the run is executed by the
/// serve process rather than this one, so there is nothing to subscribe to here - the
/// table is the only channel between the two, and polling it is the whole mechanism.
/// The interval is the orchestrator's own, since a run cannot settle any sooner than the
/// pass that settles it.
pub(crate) async fn wait_for_job_run(
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
    use crate::serve_state::ServeLock;
    use crate::test_support::TestDb;

    /// The hang this guards against: nothing is serving the directory, so the row the
    /// wait polls has no writer and the command would sit there for ever. The check itself
    /// raises the typed fact and nothing more - the remedy sentence belongs to the caller.
    #[test]
    fn waiting_on_an_unserved_data_dir_is_refused_naming_it() {

        let data_dir = std::env::temp_dir().join(format!("flowlite-unserved-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();

        let error = ensure_data_dir_is_served(&data_dir.to_string_lossy()).unwrap_err();

        assert!(error.downcast_ref::<DataDirNotServed>().is_some(), "{error:#}");
        assert!(error.to_string().contains(&data_dir.to_string_lossy().to_string()), "{error}");
    }

    /// The CLI's wording of that fact, word for word what the check itself used to raise:
    /// a person at a terminal really did pass `--wait`, so dropping it is their remedy.
    #[test]
    fn the_cli_wording_of_an_unserved_data_dir_names_the_flag_to_drop() {

        let unserved = DataDirNotServed { data_dir: "./d1".to_string() };
        let error = describe_unserved_data_dir(unserved.into());

        assert_eq!(
            error.to_string(),
            "Data directory ./d1 is not being served, so --wait would never return. Start \
             flowlite serve against it, or drop --wait.",
        );
    }

    /// One typed fact reworded, not a catch-all: anything else comes back untouched, so a
    /// `status` read failure inside the check is not relabelled as an unserved directory.
    #[test]
    fn the_cli_wording_leaves_any_other_error_alone() {

        let error = describe_unserved_data_dir(anyhow::anyhow!("Job run 7 not found"));

        assert_eq!(error.to_string(), "Job run 7 not found");
    }

    /// The lock is what `serve` holds for as long as it runs, so holding it here is the
    /// directory looking served to anything that asks.
    #[test]
    fn waiting_on_a_served_data_dir_is_allowed() {

        let data_dir = std::env::temp_dir().join(format!("flowlite-served-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();

        let _lock = ServeLock::acquire(&data_dir).unwrap();

        assert!(ensure_data_dir_is_served(&data_dir.to_string_lossy()).is_ok());
    }

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

    /// Parsed through the real root command rather than `JobSubmitCmd` alone, because the
    /// exclusivity being asserted is clap's, declared on the struct, not the command's own.
    fn submit_from(args: &[&str]) -> Result<JobSubmitCmd, clap::Error> {

        let cli = <crate::cli::cli::Cli as clap::Parser>::try_parse_from(args)?;

        match cli.command {
            crate::cli::cli::Command::Job(job) => match job.command {
                JobSubcommand::Submit(cmd) => Ok(cmd),
                _ => panic!("parsed as some other job subcommand"),
            },
            _ => panic!("parsed as some other command"),
        }
    }

    #[test]
    fn a_job_name_alone_submits_an_installed_job() {

        let cmd = submit_from(&["flowlite", "job", "submit", "etl"]).unwrap();

        assert_eq!(cmd.job_name.as_deref(), Some("etl"));
        assert_eq!(cmd.file, None);
    }

    #[test]
    fn a_file_alone_submits_a_definition_that_is_not_installed() {

        let cmd = submit_from(&["flowlite", "job", "submit", "-f", "pipeline.yaml"]).unwrap();

        assert_eq!(cmd.file.as_deref(), Some(Path::new("pipeline.yaml")));
        assert_eq!(cmd.job_name, None);
    }

    /// The two name the same thing two ways, so there is no reading of both at once that
    /// is not a mistake.
    #[test]
    fn a_name_and_a_file_together_are_refused() {
        assert!(submit_from(&["flowlite", "job", "submit", "etl", "-f", "etl.yaml"]).is_err());
    }

    #[test]
    fn neither_a_name_nor_a_file_is_refused() {
        assert!(submit_from(&["flowlite", "job", "submit"]).is_err());
    }

    #[test]
    fn a_file_submit_still_takes_params_and_wait() {

        let cmd = submit_from(
            &["flowlite", "job", "submit", "-f", "p.yaml", "--param", "region=eu", "--wait"],
        ).unwrap();

        assert_eq!(cmd.params, vec![("region".to_string(), "eu".to_string())]);
        assert!(cmd.wait);
    }
}
