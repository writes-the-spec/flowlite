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

    /// A parameter for this run, as name=value. Repeat for more than one.
    #[arg(long = "param", value_name = "NAME=VALUE", value_parser = parse_param)]
    pub params: Vec<(String, String)>,
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

        let overrides: std::collections::BTreeMap<String, String> =
            self.params.iter().cloned().collect();

        let job_run_id = crud.submit_job(
            &mut conn,
            &job.job_id,
            &overrides,
            None,
        ).await?;

        println!("Job {} submitted successfully. Job Run ID: {}", self.job_name, job_run_id);

        Ok(())
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
}
