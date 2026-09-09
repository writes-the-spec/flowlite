use std::time::Duration;
use serde::{Deserialize, Serialize};


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
