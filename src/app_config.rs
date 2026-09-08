use std::path::PathBuf;
use std::time::Duration;
use chrono_tz::Tz;
use anyhow::{Result};
use figment::Figment;
use figment::providers::{Toml, Env, Format, Serialized};
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
}

impl Default for AppConfigOrchestrator {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 1,
            error_backoff_seconds: 5,
            reader_eof_timeout_seconds: 2,
            max_stream_bytes: 1024 * 1024,
            read_buffer_bytes: 8192,
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


/// What the web UI pages and refreshes by.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigUi {
    /// Rows per page on the run, job and schedule lists.
    pub page_size: u32,
    /// The largest `page_size` the run list accepts from its query string.
    pub max_page_size: u32,
    /// How often a page that is watching a live run asks for itself again.
    pub refresh_interval_seconds: u32,
}

impl Default for AppConfigUi {
    fn default() -> Self {
        Self {
            page_size: 25,
            max_page_size: 100,
            refresh_interval_seconds: 3,
        }
    }
}


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


/// What a schedule gets for a field its YAML leaves out.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigScheduleDefaults {
    /// The zone a schedule's cron expression is read in. An IANA name, so a schedule that
    /// says nothing follows this zone's daylight saving rather than a fixed offset.
    pub timezone: Tz,
}

impl Default for AppConfigScheduleDefaults {
    fn default() -> Self {
        Self {
            timezone: chrono_tz::UTC,
        }
    }
}


#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfig {
    pub data_dir: String,
    #[serde(default)]
    pub orchestrator: AppConfigOrchestrator,
    #[serde(default)]
    pub ui: AppConfigUi,
    #[serde(default)]
    pub job_defaults: AppConfigJobDefaults,
    #[serde(default)]
    pub schedule_defaults: AppConfigScheduleDefaults,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            data_dir: ".".to_string(),
            orchestrator: AppConfigOrchestrator::default(),
            ui: AppConfigUi::default(),
            job_defaults: AppConfigJobDefaults::default(),
            schedule_defaults: AppConfigScheduleDefaults::default(),
        }
    }
}


impl AppConfig {

    /// Every value has a default, so a data directory with no `config.toml` — or one
    /// naming a single key — loads exactly as well as a complete file.
    pub fn load(data_dir: Option<PathBuf>) -> Result<AppConfig> {

        // The current directory, so `flowlite serve` in a directory holding jobs/ and
        // schedules/ works with no flags and writes its database beside them.
        let data_dir_fin = data_dir.unwrap_or_else(|| PathBuf::from("."));

        let app_config: AppConfig = Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Toml::file(data_dir_fin.join("config.toml")))
            .merge(Env::prefixed("FLOWLITE_").split("__"))
            // Last, because the directory flowlite was pointed at is not up for debate by
            // a file inside it.
            .merge(Serialized::default("data_dir", data_dir_fin.to_string_lossy()))
            .extract()?;

        Ok(app_config)
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flowlite-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_directory_with_no_config_file_loads_every_default() {
        let dir = temp_dir();

        let config = AppConfig::load(Some(dir.clone())).unwrap();

        assert_eq!(config.data_dir, dir.to_string_lossy());
        assert_eq!(config.orchestrator.poll_interval_seconds, 1);
        assert_eq!(config.orchestrator.max_stream_bytes, 1024 * 1024);
        assert_eq!(config.ui.page_size, 25);
        assert_eq!(config.job_defaults.timeout_seconds, 3600);
        assert_eq!(config.schedule_defaults.timezone, chrono_tz::UTC);
    }

    #[test]
    fn a_file_naming_one_key_leaves_every_other_default_alone() {
        let dir = temp_dir();
        std::fs::write(dir.join("config.toml"), "[ui]\npage_size = 10\n").unwrap();

        let config = AppConfig::load(Some(dir)).unwrap();

        assert_eq!(config.ui.page_size, 10);
        // The rest of [ui], and the sections the file never mentions.
        assert_eq!(config.ui.max_page_size, 100);
        assert_eq!(config.ui.refresh_interval_seconds, 3);
        assert_eq!(config.orchestrator.poll_interval_seconds, 1);
        assert_eq!(config.job_defaults.retry_delay_seconds, 60);
    }

    #[test]
    fn a_file_may_set_keys_across_several_sections() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[orchestrator]\npoll_interval_seconds = 5\nmax_stream_bytes = 2048\n\n\
             [job_defaults]\ntimeout_seconds = 60\nmax_parallel_runs = 0\n",
        ).unwrap();

        let config = AppConfig::load(Some(dir)).unwrap();

        assert_eq!(config.orchestrator.poll_interval(), Duration::from_secs(5));
        assert_eq!(config.orchestrator.max_stream_bytes, 2048);
        assert_eq!(config.orchestrator.error_backoff(), Duration::from_secs(5));
        assert_eq!(config.job_defaults.timeout_seconds, 60);
        assert_eq!(config.job_defaults.max_parallel_runs, 0);
        assert_eq!(config.job_defaults.max_retries, 0);
    }

    #[test]
    fn a_schedule_timezone_is_read_as_a_zone_rather_than_a_string() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[schedule_defaults]\ntimezone = \"Europe/Vienna\"\n",
        ).unwrap();

        let config = AppConfig::load(Some(dir)).unwrap();

        assert_eq!(config.schedule_defaults.timezone, chrono_tz::Europe::Vienna);
    }

    #[test]
    fn an_unknown_timezone_is_an_error_rather_than_a_silent_fallback() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[schedule_defaults]\ntimezone = \"Mars/Olympus\"\n",
        ).unwrap();

        assert!(AppConfig::load(Some(dir)).is_err());
    }

    #[test]
    fn the_directory_flowlite_was_pointed_at_wins_over_the_file_in_it() {
        let dir = temp_dir();
        std::fs::write(dir.join("config.toml"), "data_dir = \"/somewhere/else\"\n").unwrap();

        let config = AppConfig::load(Some(dir.clone())).unwrap();

        assert_eq!(config.data_dir, dir.to_string_lossy());
    }
}
