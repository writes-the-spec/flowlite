use std::path::PathBuf;
use anyhow::Result;
use figment::Figment;
use figment::providers::{Toml, Env, Format, Serialized};
use serde::{Deserialize, Serialize};

use crate::app_config::job_defaults::AppConfigJobDefaults;
use crate::app_config::orchestrator::AppConfigOrchestrator;
use crate::app_config::schedule_defaults::AppConfigScheduleDefaults;
use crate::app_config::slack::AppConfigSlack;
use crate::app_config::smtp::AppConfigSmtp;
use crate::app_config::ui::AppConfigUi;


/// Everything `config.toml` can say, and the only thing that reads it. One field per
/// section, each declared in a file of its own beside this one.
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
    use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::time::Duration;

    use super::*;
    use crate::app_config::smtp::AppConfigSmtpEncryption;

    /// `load` reads the process environment, and cargo runs the tests of one binary as
    /// threads of one process — so a test that sets a `FLOWLITE_` variable is setting it
    /// for every `load` running beside it, not only its own.
    ///
    /// A lock rather than a convention, because the failure it prevents is a test that
    /// passes alone and fails in a full run, blaming whichever load happened to overlap.
    /// One writer and many readers is the shape of the problem exactly: only the test that
    /// mutates the environment needs the binary to itself.
    static ENVIRONMENT: RwLock<()> = RwLock::new(());

    /// Taken by every test that loads a config. Bind it to a name — a `let _` drops the
    /// guard on the spot and holds nothing.
    fn reading_the_environment() -> RwLockReadGuard<'static, ()> {
        ENVIRONMENT.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Taken by the one test that sets a variable, for as long as it is set.
    fn writing_the_environment() -> RwLockWriteGuard<'static, ()> {
        ENVIRONMENT.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flowlite-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_directory_with_no_config_file_loads_every_default() {
        let _environment = reading_the_environment();

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
        let _environment = reading_the_environment();

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
        let _environment = reading_the_environment();

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
        let _environment = reading_the_environment();

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
        let _environment = reading_the_environment();

        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[schedule_defaults]\ntimezone = \"Mars/Olympus\"\n",
        ).unwrap();

        assert!(AppConfig::load(Some(dir)).is_err());
    }

    #[test]
    fn a_directory_with_no_config_file_has_no_channel_and_so_no_notifications() {
        let _environment = reading_the_environment();

        let dir = temp_dir();

        let config = AppConfig::load(Some(dir)).unwrap();

        assert!(config.smtp.is_none());
        assert!(config.slack.is_none());
    }

    #[test]
    fn an_smtp_section_naming_only_what_it_must_takes_the_rest_by_default() {
        let _environment = reading_the_environment();

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
        let _environment = reading_the_environment();

        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[smtp]\nfrom = \"flowlite@example.com\"\n",
        ).unwrap();

        assert!(AppConfig::load(Some(dir)).is_err());
    }

    #[test]
    fn a_slack_section_naming_only_a_token_takes_the_rest_by_default() {
        let _environment = reading_the_environment();

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
        let _environment = writing_the_environment();

        let dir = temp_dir();

        // SAFETY: the environment is process-wide, and the write guard above is what makes
        // this the only thread reading it until the variable is gone again.
        unsafe { std::env::set_var("FLOWLITE_SLACK__TOKEN", "xoxb-from-env") };

        let config = AppConfig::load(Some(dir));

        unsafe { std::env::remove_var("FLOWLITE_SLACK__TOKEN") };

        // Asserted after the removal, so a load that fails cannot leave the variable set
        // for whatever runs next.
        assert_eq!(config.unwrap().slack.unwrap().token, "xoxb-from-env");
    }

    #[test]
    fn a_slack_section_missing_a_token_is_an_error_rather_than_a_silent_default() {
        let _environment = reading_the_environment();

        let dir = temp_dir();
        std::fs::write(
            dir.join("config.toml"),
            "[slack]\napi_url = \"https://slack.example.com/api/chat.postMessage\"\n",
        ).unwrap();

        assert!(AppConfig::load(Some(dir)).is_err());
    }

    #[test]
    fn the_directory_flowlite_was_pointed_at_wins_over_the_file_in_it() {
        let _environment = reading_the_environment();

        let dir = temp_dir();
        std::fs::write(dir.join("config.toml"), "data_dir = \"/somewhere/else\"\n").unwrap();

        let config = AppConfig::load(Some(dir.clone())).unwrap();

        assert_eq!(config.data_dir, dir.to_string_lossy());
    }
}
