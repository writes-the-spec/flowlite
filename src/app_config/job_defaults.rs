use serde::{Deserialize, Serialize};


/// What a job or task gets for a field its YAML leaves out.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigJobDefaults {
    /// Seconds one attempt may run for.
    pub timeout_seconds: u32,
    /// Retries *after* the first attempt, so executions total `1 + max_retries`.
    pub max_retries: u32,
    /// Seconds to wait after a failed attempt before the next one starts.
    pub retry_delay_seconds: u32,
    /// How many runs of one job may run at once, 0 for no limit.
    pub max_parallel_runs: u32,
}

impl Default for AppConfigJobDefaults {
    fn default() -> Self {
        Self {
            timeout_seconds: 3600,
            max_retries: 0,
            retry_delay_seconds: 60,
            max_parallel_runs: 1,
        }
    }
}
