use serde::{Deserialize, Serialize};


/// How many finished job runs the retention service keeps, and how much of one pass it
/// may spend deleting them.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigRetention {
    /// The most finished job runs to keep across every job, whatever each job allows
    /// itself. Enforced oldest-first, after each job's own number. 0 for no ceiling.
    pub keep_runs_total: u32,
    /// The most runs one pass deletes, so the first pass after enabling this cannot hold
    /// the single SQLite writer for minutes. 0 for no cap.
    pub max_deletes_per_pass: u32,
}

impl Default for AppConfigRetention {
    fn default() -> Self {
        Self {
            keep_runs_total: 10000,
            max_deletes_per_pass: 100,
        }
    }
}
