//! The serve status as JSON - what `flowlite status --json` prints and what the
//! `get_serve_status` tool returns, so a supervisor and an agent read the same answer.
//!
//! The prose spelling stays in the CLI: "serving on http://..." is a sentence for a
//! terminal, and shared stops at the fact.

use chrono::Utc;

use crate::serve_state::{ServeState, ServeStatus};


/// The answer for a script. The `status` word is on every branch and never moves, so a
/// wrapper can read one field without knowing which shape it got.
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


/// Clamped for the same reason `format::duration` clamps: clock skew between this process
/// and the one that wrote `started_at` must not surface as a negative uptime.
pub fn uptime_seconds(state: &ServeState) -> i64 {
    Utc::now().signed_duration_since(state.started_at).num_seconds().max(0)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn a_state() -> ServeState {
        ServeState {
            pid: 4242,
            address: "127.0.0.1".to_string(),
            port: 8001,
            started_at: Utc::now() - chrono::Duration::seconds(90),
            version: "0.1.0".to_string(),
        }
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
