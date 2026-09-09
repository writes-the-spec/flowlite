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


/// How a failure email leaves the box.
///
/// The one section with no default, because there is no default mail server: no `[smtp]`
/// means notifications are off entirely, and a job asking for one is then a startup error
/// rather than a message nobody gets at 03:00. It is deployment config rather than job
/// config for the same reason it is not in the YAML — the relay differs per machine, and
/// `password` belongs in `FLOWLITE_SMTP__PASSWORD`, not in a file that is read out to the
/// dashboard and committed alongside the jobs.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigSmtp {
    pub host: String,
    #[serde(default = "default_smtp_port")]
    pub port: u16,
    /// Empty for a relay that authenticates nobody, which a local MTA usually doesn't.
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    /// The From: address, which most relays insist on owning.
    pub from: String,
    #[serde(default)]
    pub encryption: AppConfigSmtpEncryption,
    /// The most of one stream of one failed task a message carries. Past it the end is
    /// kept and the front cut, since a relay's own size limit is the reason to have one.
    #[serde(default = "default_smtp_max_output_bytes")]
    pub max_output_bytes: usize,
}

fn default_smtp_port() -> u16 {
    587
}

fn default_smtp_max_output_bytes() -> usize {
    4096
}

/// How a failure message reaches Slack.
///
/// No default, for the reason `[smtp]` has none: there is no default workspace. Absent
/// means the channel is unconfigured, and a job asking for it is a startup error rather
/// than a message nobody gets at 03:00.
///
/// It is a bot token and `chat.postMessage` rather than an incoming webhook on purpose.
/// A webhook URL *is* its destination, so a job naming a second channel would have to
/// carry a second secret URL in its YAML — which is the one thing the split between
/// config.toml and the job files exists to prevent. A token here addresses any
/// conversation the bot is in, so the YAML names `#oncall` and nothing else.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigSlack {
    /// A bot token, `xoxb-...`, with `chat:write`. Belongs in `FLOWLITE_SLACK__TOKEN`
    /// rather than in the file, like the SMTP password.
    pub token: String,
    /// Overridable so a test — or a Slack-compatible relay — can be pointed at instead.
    #[serde(default = "default_slack_api_url")]
    pub api_url: String,
    /// How long one post may take before it is recorded as failed. A hung request would
    /// otherwise hold the notification service's whole pass.
    #[serde(default = "default_slack_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Smaller than the mail cap by default: `chat.postMessage` takes far more than this,
    /// but a chat message is read in a scroll rather than opened, and the mail is where
    /// the long tail belongs.
    #[serde(default = "default_slack_max_output_bytes")]
    pub max_output_bytes: usize,
}

fn default_slack_api_url() -> String {
    "https://slack.com/api/chat.postMessage".to_string()
}

fn default_slack_timeout_seconds() -> u64 {
    10
}

fn default_slack_max_output_bytes() -> usize {
    2048
}

impl AppConfigSlack {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds)
    }
}


/// What the connection to the relay is wrapped in: STARTTLS on the submission port,
/// implicit TLS on 465, or nothing at all for an MTA on localhost.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AppConfigSmtpEncryption {
    #[default]
    StartTls,
    Tls,
    None,
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
    /// None is "no `[smtp]` section", so it is skipped when the defaults are serialized
    /// into figment: a null default provider would otherwise be the thing a real `[smtp]`
    /// table has to merge over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp: Option<AppConfigSmtp>,
    /// None is "no `[slack]` section", skipped for the reason `smtp` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack: Option<AppConfigSlack>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            data_dir: ".".to_string(),
            orchestrator: AppConfigOrchestrator::default(),
            ui: AppConfigUi::default(),
            job_defaults: AppConfigJobDefaults::default(),
            schedule_defaults: AppConfigScheduleDefaults::default(),
            smtp: None,
            slack: None,
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
    fn a_directory_with_no_config_file_has_no_channel_and_so_no_notifications() {
        let dir = temp_dir();

        let config = AppConfig::load(Some(dir)).unwrap();

        assert!(config.smtp.is_none());
        assert!(config.slack.is_none());
    }

    #[test]
    fn an_smtp_section_naming_only_what_it_must_takes_the_rest_by_default() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[smtp]\nhost = \"smtp.example.com\"\nfrom = \"flowlite@example.com\"\n",
        ).unwrap();

        let smtp = AppConfig::load(Some(dir)).unwrap().smtp.unwrap();

        assert_eq!(smtp.host, "smtp.example.com");
        assert_eq!(smtp.from, "flowlite@example.com");
        assert_eq!(smtp.port, 587);
        assert_eq!(smtp.encryption, AppConfigSmtpEncryption::StartTls);
        assert_eq!(smtp.username, "");
        assert_eq!(smtp.password, "");
    }

    #[test]
    fn an_smtp_section_missing_a_host_is_an_error_rather_than_a_silent_default() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[smtp]\nfrom = \"flowlite@example.com\"\n",
        ).unwrap();

        assert!(AppConfig::load(Some(dir)).is_err());
    }

    #[test]
    fn a_slack_section_naming_only_a_token_takes_the_rest_by_default() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[slack]\ntoken = \"xoxb-abc\"\n",
        ).unwrap();

        let slack = AppConfig::load(Some(dir)).unwrap().slack.unwrap();

        assert_eq!(slack.token, "xoxb-abc");
        assert_eq!(slack.api_url, "https://slack.com/api/chat.postMessage");
        assert_eq!(slack.timeout(), Duration::from_secs(10));
        assert_eq!(slack.max_output_bytes, 2048);
    }

    /// The token is the one key you should not write in the file, so the section has to
    /// come into existence from the environment alone.
    #[test]
    fn a_token_set_only_in_the_environment_configures_slack_with_no_file_at_all() {
        let dir = temp_dir();

        // SAFETY: figment reads the environment, and this is the only test that sets this
        // variable. Cargo runs tests of one binary in threads, so it is removed again
        // before anything else can observe it.
        unsafe { std::env::set_var("FLOWLITE_SLACK__TOKEN", "xoxb-from-env") };

        let slack = AppConfig::load(Some(dir)).unwrap().slack;

        unsafe { std::env::remove_var("FLOWLITE_SLACK__TOKEN") };

        assert_eq!(slack.unwrap().token, "xoxb-from-env");
    }

    #[test]
    fn a_slack_section_missing_a_token_is_an_error_rather_than_a_silent_default() {
        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[slack]\napi_url = \"https://slack.example.com/api/chat.postMessage\"\n",
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
