use std::path::PathBuf;
use clap::{Parser, Subcommand};
use crate::app_config::AppConfig;
use crate::cli::commands::serve::ServeCmd;
use crate::cli::commands::job::JobCmd;
use crate::cli::commands::job_run::JobRunCmd;
use crate::cli::commands::status::StatusCmd;
use crate::cli::commands::limits::LimitsCmd;
use crate::cli::commands::mcp::McpCmd;
use crate::cli::commands::init::InitCmd;
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
    /// Lay an example job, an example schedule and a commented config.toml into this data
    /// directory, leaving any of the three that are already there untouched.
    Init(InitCmd),
    Serve(ServeCmd),
    Job(JobCmd),
    JobRun(JobRunCmd),
    /// Whether this data directory is being served, and by what.
    Status(StatusCmd),
    /// Why nothing is running: the global cap and every named limit, each with its
    /// current use and its maximum.
    Limits(LimitsCmd),
    /// Speak the Model Context Protocol on stdin and stdout, so an agent can submit and
    /// inspect runs in this data directory as tool calls.
    Mcp(McpCmd),
}


impl Cli {

    pub async fn run(&self) -> anyhow::Result<()> {

        let app_config = AppConfig::load(self.data_dir.clone())?;
        let toolkit = Toolkit::new(app_config);

        match &self.command {
            Command::Init(cmd) => cmd.run(toolkit).await?,
            Command::Serve(cmd) => cmd.run(toolkit).await?,
            Command::Job(cmd) => cmd.run(toolkit).await?,
            Command::JobRun(cmd) => cmd.run(toolkit).await?,
            Command::Status(cmd) => cmd.run(toolkit).await?,
            Command::Limits(cmd) => cmd.run(toolkit).await?,
            Command::Mcp(cmd) => cmd.run(toolkit).await?,
        }

        Ok(())
    }
}
