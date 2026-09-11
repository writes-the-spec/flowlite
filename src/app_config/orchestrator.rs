use std::time::Duration;
use serde::{Deserialize, Serialize};


/// What the orchestrator's loops and readers are timed and sized by.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigOrchestrator {
    /// The safety net rather than the driver — signals do the waking — but it cannot be
    /// turned off: `job submit` writes from another process and so cannot publish, and
    /// this interval is the only thing that notices.
    pub poll_interval_seconds: u64,
    /// How long a poller waits before restarting after an error.
    pub error_backoff_seconds: u64,
    /// How long the attempt monitor waits for the readers to reach EOF after the process
    /// has gone, per timeout and per stop.
    pub reader_eof_timeout_seconds: u64,
    /// The most output one stream of one attempt records. Past it the reader keeps
    /// reading and stops recording, which bounds both the table and the memory in flight.
    pub max_stream_bytes: usize,
    /// One read from a child's pipe.
    pub read_buffer_bytes: usize,
    /// The most task run attempts that may be running at once, across every job. 0 for no limit.
    pub max_running_attempts: u32,
}

impl Default for AppConfigOrchestrator {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 1,
            error_backoff_seconds: 5,
            reader_eof_timeout_seconds: 2,
            max_stream_bytes: 1024 * 1024,
            read_buffer_bytes: 8192,
            max_running_attempts: 32,
        }
    }
}

impl AppConfigOrchestrator {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds)
    }

    pub fn error_backoff(&self) -> Duration {
        Duration::from_secs(self.error_backoff_seconds)
    }

    pub fn reader_eof_timeout(&self) -> Duration {
        Duration::from_secs(self.reader_eof_timeout_seconds)
    }
}
