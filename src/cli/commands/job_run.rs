use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use serde::Serialize;
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, SelectTaskRunsDataSort, TaskRun};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt};
use crate::crud::task_run_attempt_output::{group_task_run_attempt_output, SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort, TaskRunAttemptOutputStreams};
use crate::router::app::format;
use super::job::{ensure_data_dir_is_served, wait_for_job_run};

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

/// Submit a fresh run of the definition this run executed.
#[derive(Args)]
pub struct JobRunRerunCmd {
    pub job_run_id: i64,
}

/// A run and the task runs under it, flattened so that `.status` reads off the run itself
/// - the same field name `job submit --json` prints, rather than one nested a level down.
#[derive(Serialize)]
struct JobRunDetail {
    #[serde(flatten)]
    job_run: JobRun,
    task_runs: Vec<TaskRun>,
}

/// One attempt with what it wrote, which is the shape the whole command exists to report.
#[derive(Serialize)]
struct TaskRunAttemptLog {
    #[serde(flatten)]
    task_run_attempt: TaskRunAttempt,
    stdout: String,
    stderr: String,
}

impl JobRunCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        match &self.command {
            JobRunSubcommand::List(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Get(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Logs(cmd) => cmd.run(toolkit, self.json).await,
            JobRunSubcommand::Stop(cmd) => cmd.run(toolkit, self.json).await,
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

        let job_run = crud.select_job_run(&mut conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: Some(self.job_run_id),
                job_id: None,
                status: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        let Some(job_run) = job_run else {
            anyhow::bail!("Job run {} not found", self.job_run_id);
        };

        let task_runs = crud.select_task_runs(&mut conn, &SelectTaskRunsData {
            filter: SelectTaskRunsDataFilter {
                id: None,
                job_run_id: Some(self.job_run_id),
                job_id: None,
                task_id: None,
                status: None,
            },
            sort: Some(SelectTaskRunsDataSort::Id),
        }).await?;

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

        let job_run = crud.select_job_run(&mut conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: Some(self.job_run_id),
                job_id: None,
                status: None,
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        if job_run.is_none() {
            anyhow::bail!("Job run {} not found", self.job_run_id);
        }

        let task_run_attempts = crud.select_task_run_attempts(&mut conn, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                task_run_id: None,
                job_run_id: Some(self.job_run_id),
                task_id: self.task.clone(),
                status: None,
            },
            sort: Some(SelectTaskRunAttemptsDataSort::Id),
        }).await?;

        // An empty run still answers in JSON, as an empty array - a caller parsing the
        // output should not have to read a sentence to learn there was nothing.
        if task_run_attempts.is_empty() && !json {
            match &self.task {
                Some(task_id) => println!("No attempts of task {} in job run {}", task_id, self.job_run_id),
                None => println!("No task attempts in job run {}", self.job_run_id),
            }
            return Ok(());
        }

        // Filtered exactly as the attempts above were - the same job run, narrowed by the
        // same `--task` - so this reads only the output it is about to print rather than
        // every task's to print one task's.
        let task_run_attempt_output = crud.select_task_run_attempt_outputs(&mut conn, &SelectTaskRunAttemptOutputsData {
            filter: SelectTaskRunAttemptOutputsDataFilter {
                id: None,
                task_run_attempt_id: None,
                task_run_id: None,
                job_run_id: Some(self.job_run_id),
                job_id: None,
                task_id: self.task.clone(),
                stream: None,
            },
            sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
        }).await?;

        let mut task_run_attempt_output = group_task_run_attempt_output(task_run_attempt_output);

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
            ensure_data_dir_is_served(&toolkit.app_config.data_dir)?;
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

/// Asks for a run to be stopped by writing the row the orchestrator reads. Nothing is
/// killed here - this process owns no child, and the serve process acts on the row on a
/// later pass of its own.
///
/// A settled run is refused rather than accepted: its row would never be read, and a
/// caller told the stop succeeded would have been told something untrue.
async fn stop_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
) -> anyhow::Result<()> {

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

    let Some(job_run) = job_run else {
        anyhow::bail!("Job run {} not found", job_run_id);
    };

    if job_run.status.is_finished() {
        anyhow::bail!(
            "Job run {} has already {}",
            job_run_id,
            format::job_run_word(job_run.status),
        );
    }

    crud.insert_job_run_stop(&mut *conn, &InsertJobRunStopData {
        input: InsertJobRunStopDataInput { job_run_id },
    }).await?;

    Ok(())
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

    if let Some(scheduled_at) = job_run.scheduled_at {
        println!("{:<10} {}", "scheduled", format::timestamp(scheduled_at));
    }

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

/// The words a run's status is stored and filtered by, so `--status` accepts exactly what
/// the UI's own filter does rather than a second spelling of the same set.
///
/// Derived from `JobRunStatus::ALL` rather than matched by hand: a status added to the
/// enum reaches this parser and its error message without anyone remembering to come here.
fn parse_job_run_status(raw: &str) -> Result<JobRunStatus, String> {

    let status = JobRunStatus::ALL
        .into_iter()
        .find(|status| status.to_string() == raw);

    match status {
        Some(status) => Ok(status),
        None => Err(format!("unknown status '{}', expected one of {}", raw, accepted_statuses())),
    }
}

fn accepted_statuses() -> String {
    JobRunStatus::ALL
        .iter()
        .map(|status| status.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::{UpdateJobRunsData, UpdateJobRunsDataFilter, UpdateJobRunsDataInput};
    use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
    use crate::test_support::TestDb;

    #[test]
    fn a_status_word_parses_to_its_status() {
        assert_eq!(parse_job_run_status("running").unwrap(), JobRunStatus::Running);
        assert_eq!(parse_job_run_status("succeeded").unwrap(), JobRunStatus::Succeeded);
    }

    /// The same spelling the UI filters by and the same one serde writes, so the two
    /// surfaces name a status alike.
    #[test]
    fn a_timed_out_run_is_spelled_the_way_it_is_stored() {
        assert_eq!(parse_job_run_status("timedout").unwrap(), JobRunStatus::TimedOut);
    }

    /// The parser reads the same list the UI's filter chips are built from, so a status
    /// added to the enum cannot reach one surface and not the other - which it did when
    /// `Invalid` was added and this parser still refused the word.
    #[test]
    fn every_status_the_ui_can_filter_by_parses() {
        for status in JobRunStatus::ALL {
            assert_eq!(parse_job_run_status(&status.to_string()).unwrap(), status);
        }
    }

    #[test]
    fn an_invalid_status_parses() {
        assert_eq!(parse_job_run_status("invalid").unwrap(), JobRunStatus::Invalid);
    }

    #[test]
    fn an_unknown_status_is_refused_naming_what_is_accepted() {
        let error = parse_job_run_status("exploded").unwrap_err();

        assert!(error.contains("exploded"), "{error}");
        assert!(error.contains("succeeded"), "{error}");
        assert!(error.contains("timedout"), "{error}");
    }

    /// The stop is a row, not a signal: this command writes it from one process and the
    /// orchestrator reads it from another, so what this asserts is the row being there.
    #[tokio::test]
    async fn a_running_job_run_is_stopped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        stop_job_run(&db.crud, &mut conn, job_run.id).await.unwrap();

        assert!(job_run_stop(&db, job_run.id).await.is_some());
    }

    /// A settled run has nothing left to stop, and the row would sit in the table for ever
    /// unread. Saying so beats reporting a success that did nothing.
    #[tokio::test]
    async fn a_finished_job_run_cannot_be_stopped() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Succeeded).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = stop_job_run(&db.crud, &mut conn, job_run.id).await.unwrap_err().to_string();

        assert!(error.contains(&job_run.id.to_string()), "{error}");
        assert!(error.contains("succeeded"), "{error}");

        assert!(job_run_stop(&db, job_run.id).await.is_none(), "a refused stop still wrote a row");
    }

    /// A run id nothing matches is the caller's mistake, not a silent no-op.
    #[tokio::test]
    async fn stopping_a_job_run_that_does_not_exist_raises() {

        let db = TestDb::new().await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        let error = stop_job_run(&db.crud, &mut conn, 404).await.unwrap_err().to_string();

        assert!(error.contains("404"), "{error}");
    }

    /// What `--wait` runs end to end: the row goes in, and the wait returns only once
    /// somebody else settles the run - the serve process, here another connection. The
    /// stop is still on the table while the wait is in progress, which is what lets the
    /// serve process see it at all.
    #[tokio::test]
    async fn stopping_with_wait_returns_once_the_run_settles() {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        stop_job_run(&db.crud, &mut conn, job_run.id).await.unwrap();

        let crud = db.crud.clone();
        let conn_pool = db.conn_pool.clone();
        let job_run_id = job_run.id;

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;

            crud.update_job_runs(&*conn_pool, &UpdateJobRunsData {
                filter: UpdateJobRunsDataFilter { id: Some(job_run_id) },
                input: UpdateJobRunsDataInput {
                    status: Some(JobRunStatus::Aborted),
                    started_at: None,
                    finished_at: None,
                },
            }).await.unwrap();
        });

        let settled = wait_for_job_run(
            &db.crud,
            &mut conn,
            job_run.id,
            std::time::Duration::from_millis(1),
        ).await.unwrap();

        assert_eq!(settled.status, JobRunStatus::Aborted);
        assert!(job_run_stop(&db, job_run.id).await.is_some());
    }

    async fn job_run_stop(db: &TestDb, job_run_id: i64) -> Option<crate::crud::job_run_stop::JobRunStop> {
        db.crud.select_job_run_stop(&*db.conn_pool, &SelectJobRunStopsData {
            filter: SelectJobRunStopsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap()
    }
}
