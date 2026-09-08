use std::path::PathBuf;
use clap::{Parser, Subcommand};
use crate::app_config::AppConfig;
use crate::cli::commands::serve::ServeCmd;
use crate::cli::commands::job::JobCmd;
use crate::cli::commands::job_run::JobRunCmd;
use crate::toolkit::Toolkit;


#[derive(Parser)]
#[command(name = "FlowLite")]
pub struct Cli {
    #[arg(short = 'D', long, env = "FLOWLITE_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}


#[derive(Subcommand)]
pub enum Command {
    Serve(ServeCmd),
    Job(JobCmd),
    JobRun(JobRunCmd),
}


impl Cli {

    pub async fn run(&self) -> anyhow::Result<()> {

        let app_config = AppConfig::load(self.data_dir.clone())?;
        let toolkit = Toolkit::new(app_config);

        match &self.command {
            Command::Serve(cmd) => cmd.run(toolkit).await?,
            Command::Job(cmd) => cmd.run(toolkit).await?,
            Command::JobRun(cmd) => cmd.run(toolkit).await?,
        }

        Ok(())
    }
}
