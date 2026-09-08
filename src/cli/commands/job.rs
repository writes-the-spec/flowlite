use clap::{Args, Subcommand};
use crate::toolkit::Toolkit;
use crate::crud::CRUD;
use crate::crud::job::{SelectJobsData, SelectJobsDataFilter};

#[derive(Args)]
pub struct JobCmd {
    #[command(subcommand)]
    pub command: JobSubcommand,
}

#[derive(Subcommand)]
pub enum JobSubcommand {
    List(JobListCmd),
    Submit(JobSubmitCmd),
}

#[derive(Args)]
pub struct JobListCmd {
}

#[derive(Args)]
pub struct JobSubmitCmd {
    pub job_name: String,
}

impl JobCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let _memory_conn = toolkit.get_memory_conn().await?;

        match &self.command {
            JobSubcommand::List(cmd) => cmd.run(toolkit).await,
            JobSubcommand::Submit(cmd) => cmd.run(toolkit).await,
        }
    }
}

impl JobListCmd {
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

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
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

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

        let job_run_id = crud.submit_job(
            &mut conn,
            &job.job_id,
            &std::collections::BTreeMap::new(),
            None,
        ).await?;

        println!("Job {} submitted successfully. Job Run ID: {}", self.job_name, job_run_id);

        Ok(())
    }
}
