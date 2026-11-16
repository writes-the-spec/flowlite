use clap::Args;
use crate::shared::validate::validate;
use crate::toolkit::Toolkit;


#[derive(Args)]
pub struct ValidateCmd {
    /// Print the counts as JSON, for a script or a CI step.
    #[arg(long)]
    pub json: bool,
}


impl ValidateCmd {

    /// Opens no database and binds nothing, so it checks a directory whether or not it is
    /// served, and before it has a flowlite.db at all.
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let result = validate(&toolkit.app_config)?;

        if self.json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!(
                "{} {}, {} {}: OK",
                result.jobs,
                if result.jobs == 1 { "job" } else { "jobs" },
                result.schedules,
                if result.schedules == 1 { "schedule" } else { "schedules" },
            );
        }

        Ok(())
    }
}
