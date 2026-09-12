use std::path::Path;
use clap::Args;
use crate::shared::format;
use crate::shared::serve_status::{status_json, uptime_seconds};
use crate::serve_state::{status, ServeState, ServeStatus};
use crate::toolkit::Toolkit;


#[derive(Args)]
pub struct StatusCmd {
    /// Print the answer as JSON, for a script or a supervisor.
    #[arg(long)]
    pub json: bool,
}


impl StatusCmd {

    /// Reads the lock and the state file rather than the database, so it answers for a
    /// data directory whose server is down as readily as one whose server is up - which
    /// is the whole reason it exists rather than a page in the UI.
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {

        let data_dir = Path::new(&toolkit.app_config.data_dir);

        let state = status(data_dir)?;

        if self.json {
            println!("{}", status_json(&state));
        } else {
            println!("{}", status_line(&state));
        }

        Ok(())
    }
}


/// One line for a person.
pub fn status_line(state: &ServeStatus) -> String {
    match state {
        ServeStatus::Down => "not being served".to_string(),
        ServeStatus::Starting => "starting up".to_string(),
        ServeStatus::Up(state) => format!(
            "serving on http://{}:{} (pid {}, up {}, flowlite {})",
            state.address,
            state.port,
            state.pid,
            uptime(state),
            state.version,
        ),
    }
}


/// Spelled the way the UI spells a duration, so the two surfaces do not disagree about
/// what 90 seconds is called.
fn uptime(state: &ServeState) -> String {
    format::duration(uptime_seconds(state))
}


#[cfg(test)]
mod tests {
    use super::*;

    fn a_state() -> ServeState {
        ServeState {
            pid: 4242,
            address: "127.0.0.1".to_string(),
            port: 8001,
            started_at: chrono::Utc::now() - chrono::Duration::seconds(90),
            version: "0.1.0".to_string(),
        }
    }

    #[test]
    fn a_served_directory_reports_where_and_since_when() {
        let line = status_line(&ServeStatus::Up(a_state()));

        assert!(line.contains("http://127.0.0.1:8001"), "{line}");
        assert!(line.contains("4242"), "{line}");
        assert!(line.contains("0.1.0"), "{line}");
        // Not the exact string: num_seconds() truncates, so a slow test crossing into 91
        // seconds would read "1m 31s". The assertion is that uptime comes from
        // started_at at all.
        assert!(line.contains("1m"), "{line}");
    }

    #[test]
    fn an_unserved_directory_says_so_in_words() {
        assert_eq!(status_line(&ServeStatus::Down), "not being served");
    }

    #[test]
    fn a_directory_between_the_lock_and_the_bind_says_it_is_starting() {
        assert_eq!(status_line(&ServeStatus::Starting), "starting up");
    }
}
