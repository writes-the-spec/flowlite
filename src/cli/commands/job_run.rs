use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::task_run::TaskRun;
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::shared::format;
use crate::shared::job_run::{parse_job_run_status, stop_job_run, JobRunDetail, TaskRunAttemptLog};
use crate::shared::wait::{ensure_data_dir_is_served, wait_for_job_run};
use super::job::describe_unserved_data_dir;

#[derive(Args)]
pub struct JobRunCmd {
    #[command(subcommand)]
    pub command: JobRunSubcommand,

    /// Print the result as JSON instead of as text.
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand)]
pub enum JobRunSubcommand {
    List(JobRunListCmd),
    Get(JobRunGetCmd),
    Logs(JobRunLogsCmd),
    Stop(JobRunStopCmd),
    Delete(JobRunDeleteCmd),
    Rerun(JobRunRerunCmd),
}

/// List job runs, newest first.
#[derive(Args)]
pub struct JobRunListCmd {
    /// Only runs of this job.
    #[arg(long)]
    pub job: Option<String>,

    /// Only runs with this status.
    #[arg(long, value_parser = parse_job_run_status)]
    pub status: Option<JobRunStatus>,

    /// How many runs to show, newest first.
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

/// Show one job run and the task runs under it.
#[derive(Args)]
pub struct JobRunGetCmd {
    pub job_run_id: i64,
}

/// Show what each attempt of a job run wrote to stdout and stderr.
#[derive(Args)]
pub struct JobRunLogsCmd {
    pub job_run_id: i64,

    #[arg(long)]
    pub task: Option<String>,
}

/// Ask for a running job run to be stopped.
#[derive(Args)]
pub struct JobRunStopCmd {
    pub job_run_id: i64,

    /// Wait for the run to settle, instead of returning once the stop has been queued.
    #[arg(long)]
    pub wait: bool,
}

/// Remove a run that is still waiting for its time, so its schedule submits the occurrence
/// again under the job as it now stands.
#[derive(Args)]
pub struct JobRunDeleteCmd {
    pub job_run_id: i64,
}

/// Submit a fresh run of the definition this run executed.
#[derive(Args)]
pub struct JobRunRerunCmd {
    pub job_run_id: i64,
}

impl JobRunCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        match &self.command {
            JobRunSubcommand::List(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Get(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Logs(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Stop(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Delete(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Rerun(cmd) => cmd.run(toolkit, self.json).await,
        }
    }
}

impl JobRunListCmd {
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let job_runs = crud.select_job_runs(&mut conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: self.job.clone(),
                status: self.status,
                statuses: None,
                schedule_id: None,
                scheduled_at: None,
            },
            sort: Some(SelectJobRunsDataSort::IdDesc),
            limit: Some(self.limit),
            offset: None,
        }).await?;

        if json {
            println!("{}", serde_json::to_string_pretty(&job_runs)?);
            return Ok(());
        }

        if job_runs.is_empty() {
            println!("No job runs found");
            return Ok(());
        }

        println!("{:<12} {:<20} {:<12} {:<22} {:<10}", "Job Run ID", "Job ID", "Status", "Created", "Duration");
        println!("{}", "-".repeat(80));

        for job_run in job_runs {
            println!(
                "{:<12} {:<20} {:<12} {:<22} {:<10}",
                job_run.id,
                job_run.job_id,
                format::job_run_word(job_run.status),
                format::timestamp(job_run.created_at),
                run_duration(&job_run),
            );
        }

        Ok(())
    }
}

impl JobRunGetCmd {
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let (job_run, task_runs) = crud
            .select_job_run_with_task_runs(&mut conn, self.job_run_id)
            .await?;

        if json {
            let detail = JobRunDetail { job_run, task_runs };
            println!("{}", serde_json::to_string_pretty(&detail)?);
            return Ok(());
        }

        print_job_run(&job_run, &task_runs);

        Ok(())
    }
}

impl JobRunLogsCmd {
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let (task_run_attempts, mut task_run_attempt_output) = crud
            .select_task_run_attempt_logs(&mut conn, self.job_run_id, self.task.as_deref())
            .await?;

        // An empty run still answers in JSON, as an empty array - a caller parsing the
        // output should not have to read a sentence to learn there was nothing.
        if task_run_attempts.is_empty() && !json {
            match &self.task {
                Some(task_id) => println!("No attempts of task {} in job run {}", task_id, self.job_run_id),
                None => println!("No task attempts in job run {}", self.job_run_id),
            }
            return Ok(());
        }

        if json {
            let logs: Vec<TaskRunAttemptLog> = task_run_attempts
                .into_iter()
                .map(|task_run_attempt| {
                    let streams = task_run_attempt_output
                        .remove(&task_run_attempt.id)
                        .unwrap_or_default();

                    TaskRunAttemptLog {
                        task_run_attempt,
                        stdout: streams.stdout,
                        stderr: streams.stderr,
                    }
                })
                .collect();

            println!("{}", serde_json::to_string_pretty(&logs)?);
            return Ok(());
        }

        for task_run_attempt in task_run_attempts {

            // An attempt with no rows printed nothing, which is empty output rather than
            // unknown output.
            let streams = task_run_attempt_output
                .remove(&task_run_attempt.id)
                .unwrap_or_default();

            print_task_run_attempt(&task_run_attempt, &streams);
        }

        Ok(())
    }
}

impl JobRunStopCmd {
    /// Seeds no config: a stop is a row against a run, whose definition is already
    /// snapshotted onto it. `--wait` polls at the orchestrator's interval, which comes
    /// from config.toml - read, not seeded.
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let poll_interval = toolkit.app_config.orchestrator.poll_interval();

        // Before the row is written, for the reason `job submit --wait` checks before
        // submitting: a wait nothing can service should change nothing.
        if self.wait {
            ensure_data_dir_is_served(&toolkit.app_config.data_dir)
                .map_err(describe_unserved_data_dir)?;
        }

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        stop_job_run(&crud, &mut conn, self.job_run_id).await?;

        if !self.wait {
            match json {
                true => println!("{}", serde_json::json!({
                    "job_run_id": self.job_run_id,
                    "stop_requested": true,
                })),
                // "Requested" rather than "stopped": the serve process acts on the row on
                // a later pass, and this process never sees it happen.
                false => println!("Stop requested for job run {}", self.job_run_id),
            }

            return Ok(());
        }

        let job_run = wait_for_job_run(&crud, &mut conn, self.job_run_id, poll_interval).await?;

        // No settled status is a failure here, unlike `job submit --wait`: this command
        // asked for the run to settle and it settled. Which status it settled to is the
        // run's outcome, not this command's, so it is reported rather than exited on.
        match json {
            true => println!("{}", serde_json::to_string_pretty(&job_run)?),
            false => println!("Job run {} {}", job_run.id, format::job_run_word(job_run.status)),
        }

        Ok(())
    }
}

/// Refuses anything but a `Submitted` run, and says which status it found instead. The
/// status is what separates a run nothing has touched from one already on its way to
/// executing, and only the first can be removed without stranding work.
///
/// `delete_job_run` re-checks the status inside its own transaction, so the `false` here is
/// the run having moved between the two - a `job-run delete` racing the releaser at the
/// instant the run came due.
async fn delete_submitted_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
) -> anyhow::Result<()> {

    let job_run = crud.select_job_run(&mut *conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter {
            id: Some(job_run_id),
            job_id: None,
            status: None,
            statuses: None,
            schedule_id: None,
            scheduled_at: None,
        },
        sort: None,
        limit: Some(1),
        offset: None,
    }).await?;

    let Some(job_run) = job_run else {
        anyhow::bail!("Job run {} not found", job_run_id);
    };

    if job_run.status != JobRunStatus::Submitted {

        // A run already under way is the one case with somewhere else to go. A settled one
        // cannot be stopped either, so naming `stop` there would only buy a second refusal.
        let remedy = match job_run.status.is_finished() {
            true => String::new(),
            false => format!(
                " `flowlite job-run stop {}` is what calls off one already under way.",
                job_run_id,
            ),
        };

        anyhow::bail!(
            "Job run {} is {} and can no longer be deleted. Only a run still waiting for \
             its time can be.{}",
            job_run_id,
            format::job_run_word(job_run.status),
            remedy,
        );
    }

    if !crud.delete_job_run(&mut *conn, job_run_id).await? {
        anyhow::bail!(
            "Job run {} came due while it was being deleted, and was left alone",
            job_run_id,
        );
    }

    Ok(())
}

impl JobRunDeleteCmd {
    /// Seeds no config, like `stop`: which runs exist and what status they hold is run
    /// history, and the schedule that writes the occurrence again reads its own YAML in the
    /// serve process.
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        delete_submitted_job_run(&crud, &mut conn, self.job_run_id).await?;

        match json {
            true => println!("{}", serde_json::json!({
                "job_run_id": self.job_run_id,
                "deleted": true,
            })),
            // Said in terms of the occurrence rather than the row: a scheduled run coming
            // back on the next pass is the point of the command, not a surprise.
            false => println!(
                "Job run {} deleted. Its schedule will submit the occurrence again on its \
                 next pass, under the job as the running server read it.",
                self.job_run_id,
            ),
        }

        Ok(())
    }
}

impl JobRunRerunCmd {
    /// Reads no config on purpose - a rerun replays the original run's own snapshot, so
    /// this process never seeds mem. Anything added here that reads a config table would
    /// see it empty, not merely stale.
    pub async fn run(&self, toolkit: Toolkit, json: bool) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let job_run_id = crud.rerun_job(&mut conn, self.job_run_id).await?;

        match json {
            true => println!("{}", serde_json::json!({
                "job_run_id": job_run_id,
                "rerun_of": self.job_run_id,
            })),
            false => println!("Job run {} rerun successfully. Job Run ID: {}", self.job_run_id, job_run_id),
        }

        Ok(())
    }
}

/// How long the run took, or how long it has been going - a run that has not finished is
/// measured against now, the way the UI's own table measures it.
fn run_duration(job_run: &JobRun) -> String {

    let Some(started_at) = job_run.started_at else {
        return "—".to_string();
    };

    let finished_at = job_run.finished_at.unwrap_or_else(chrono::Utc::now);

    format::duration(finished_at.signed_duration_since(started_at).num_seconds())
}

fn task_run_duration(task_run: &TaskRun) -> String {

    let Some(started_at) = task_run.started_at else {
        return "—".to_string();
    };

    let finished_at = task_run.finished_at.unwrap_or_else(chrono::Utc::now);

    format::duration(finished_at.signed_duration_since(started_at).num_seconds())
}

/// A timestamp, or the dash the tables use for something that has not happened yet.
fn optional_timestamp(at: Option<DateTime<Utc>>) -> String {
    match at {
        Some(at) => format::timestamp(at),
        None => "—".to_string(),
    }
}

fn print_job_run(job_run: &JobRun, task_runs: &[TaskRun]) {

    println!(
        "Job run {} · {} · {}",
        job_run.id,
        job_run.job_id,
        format::job_run_word(job_run.status),
    );
    println!("{}", "-".repeat(40));

    println!("{:<10} {}", "created", format::timestamp(job_run.created_at));

    println!("{:<10} {}", "scheduled", format::timestamp(job_run.scheduled_at));

    println!("{:<10} {}", "started", optional_timestamp(job_run.started_at));
    println!("{:<10} {}", "finished", optional_timestamp(job_run.finished_at));

    println!("{:<10} {}", "duration", run_duration(job_run));

    if !job_run.parameters.0.is_empty() {
        println!();
        println!("Parameters");

        for (name, value) in job_run.parameters.0.iter() {
            println!("{:<20} {}", name, value);
        }
    }

    println!();

    if task_runs.is_empty() {
        println!("No task runs");
        return;
    }

    println!("{:<24} {:<12} {:<10}", "Task ID", "Status", "Duration");
    println!("{}", "-".repeat(48));

    for task_run in task_runs {
        println!(
            "{:<24} {:<12} {:<10}",
            task_run.task_id,
            format::task_run_word(task_run.status),
            task_run_duration(task_run),
        );
    }
}

/// Both streams are printed unindented so that a copied line is the line the task wrote.
fn print_task_run_attempt(
    task_run_attempt: &TaskRunAttempt,
    streams: &TaskRunAttemptOutputStreams,
) {

    let duration = match task_run_attempt.started_at {
        Some(started_at) => {
            let finished_at = task_run_attempt.finished_at.unwrap_or_else(chrono::Utc::now);
            format::duration(finished_at.signed_duration_since(started_at).num_seconds())
        },
        None => "—".to_string(),
    };

    println!();
    println!(
        "{} · attempt {} · {} · {}",
        task_run_attempt.task_id,
        task_run_attempt.attempt,
        format::task_run_attempt_word(task_run_attempt.status),
        duration,
    );
    println!("{}", "-".repeat(40));

    println!("stdout:");
    match streams.stdout.is_empty() {
        true => println!("(nothing written)"),
        false => println!("{}", streams.stdout.trim_end()),
    }

    println!("stderr:");
    match streams.stderr.is_empty() {
        true => println!("(nothing written)"),
        false => println!("{}", streams.stderr.trim_end()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::TestDb;

    #[tokio::test]
    async fn a_submitted_job_run_is_deleted() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Submitted).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        delete_submitted_job_run(&db.crud, &mut conn, job_run.id).await.unwrap();

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Deleted);
    }

    /// The guard the whole command rests on. A queued run is already the dispatcher's, and
    /// the message has to send the user to `stop`, which is what deals with one under way.
    #[tokio::test]
    async fn a_run_that_has_left_submitted_is_refused_and_told_to_stop_it_instead() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Queued).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Planned).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = delete_submitted_job_run(&db.crud, &mut conn, job_run.id)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains(&job_run.id.to_string()), "{error}");
        assert!(error.contains("queued"), "{error}");
        assert!(error.contains("stop"), "{error}");

        assert_eq!(db.job_run(job_run.id).await.status, JobRunStatus::Queued);
        assert_eq!(db.task_run(task_run.id).await.status, TaskRunStatus::Planned);
    }

    /// A run that has settled cannot be stopped either, so pointing at `stop` would send
    /// the reader to a second refusal. Only a run still under way gets that suggestion.
    #[tokio::test]
    async fn a_run_that_has_already_settled_is_not_pointed_at_stop() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = delete_submitted_job_run(&db.crud, &mut conn, job_run.id)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("succeeded"), "{error}");
        assert!(!error.contains("stop"), "a settled run cannot be stopped either: {error}");
    }

    #[tokio::test]
    async fn deleting_a_job_run_that_is_not_there_says_so() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = delete_submitted_job_run(&db.crud, &mut conn, 404)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("404"), "{error}");
        assert!(error.contains("not found"), "{error}");
    }
}
