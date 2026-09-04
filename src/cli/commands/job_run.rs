use clap::{Args, Subcommand};
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::job_run::{SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt};
use crate::router::app::format;

#[derive(Args)]
pub struct JobRunCmd {
    #[command(subcommand)]
    pub command: JobRunSubcommand,
}

#[derive(Subcommand)]
pub enum JobRunSubcommand {
    Logs(JobRunLogsCmd),
    Rerun(JobRunRerunCmd),
}

#[derive(Args)]
pub struct JobRunLogsCmd {
    pub job_run_id: i64,

    #[arg(long)]
    pub task: Option<String>,
}

#[derive(Args)]
pub struct JobRunRerunCmd {
    pub job_run_id: i64,
}

impl JobRunCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let _memory_conn = toolkit.get_memory_conn().await?;

        match &self.command {
            JobRunSubcommand::Logs(cmd) => cmd.run(toolkit).await,
            JobRunSubcommand::Rerun(cmd) => cmd.run(toolkit).await,
        }
    }
}

impl JobRunLogsCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

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

        if task_run_attempts.is_empty() {
            match &self.task {
                Some(task_id) => println!("No attempts of task {} in job run {}", task_id, self.job_run_id),
                None => println!("No task attempts in job run {}", self.job_run_id),
            }
            return Ok(());
        }

        for task_run_attempt in task_run_attempts {
            print_task_run_attempt(&task_run_attempt);
        }

        Ok(())
    }
}

impl JobRunRerunCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let mut conn = toolkit.get_conn().await?;

        let crud = CRUD::new(std::sync::Arc::new(toolkit));

        let job_run_id = crud.rerun_job(&mut conn, self.job_run_id).await?;

        println!("Job run {} rerun successfully. Job Run ID: {}", self.job_run_id, job_run_id);

        Ok(())
    }
}

/// Both streams are printed unindented so that a copied line is the line the task wrote.
fn print_task_run_attempt(task_run_attempt: &TaskRunAttempt) {

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
    match task_run_attempt.stdout.is_empty() {
        true => println!("(nothing written)"),
        false => println!("{}", task_run_attempt.stdout.trim_end()),
    }

    println!("stderr:");
    match task_run_attempt.stderr.is_empty() {
        true => println!("(nothing written)"),
        false => println!("{}", task_run_attempt.stderr.trim_end()),
    }
}
