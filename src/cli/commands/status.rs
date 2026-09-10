use std::path::Path;
use clap::Args;
use crate::router::app::format;
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


/// The same answer for a script. The `status` word is on every branch and never moves, so
/// a wrapper can read one field without knowing which shape it got.
pub fn status_json(state: &ServeStatus) -> serde_json::Value {
    match state {
        ServeStatus::Down => serde_json::json!({ "status": "down" }),
        ServeStatus::Starting => serde_json::json!({ "status": "starting" }),
        ServeStatus::Up(state) => serde_json::json!({
            "status": "up",
            "pid": state.pid,
            "address": state.address,
            "port": state.port,
            "started_at": state.started_at,
            "uptime_seconds": uptime_seconds(state),
            "version": state.version,
        }),
    }
}


/// Clamped for the same reason `format::duration` clamps: clock skew between this
/// process and the one that wrote `started_at` must not surface as a negative uptime.
fn uptime_seconds(state: &ServeState) -> i64 {
    chrono::Utc::now().signed_duration_since(state.started_at).num_seconds().max(0)
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

    /// The shape a wrapper parses, so the status word is present on every branch and is
    /// the one field that never moves.
    #[test]
    fn the_json_always_carries_a_status_word() {
        assert_eq!(status_json(&ServeStatus::Down)["status"], "down");
        assert_eq!(status_json(&ServeStatus::Starting)["status"], "starting");
        assert_eq!(status_json(&ServeStatus::Up(a_state()))["status"], "up");
    }

    #[test]
    fn the_json_for_a_served_directory_carries_the_pid_and_port() {
        let json = status_json(&ServeStatus::Up(a_state()));

        assert_eq!(json["pid"], 4242);
        assert_eq!(json["port"], 8001);
        assert_eq!(json["address"], "127.0.0.1");
        assert_eq!(json["version"], "0.1.0");
    }

    /// A down directory has no pid or port, and must not invent a zero for them: absent
    /// is the honest answer and null is what a parser can test for.
    #[test]
    fn the_json_for_an_unserved_directory_carries_nothing_else() {
        let json = status_json(&ServeStatus::Down);

        assert!(json.get("pid").is_none(), "{json}");
        assert!(json.get("port").is_none(), "{json}");
    }
}
