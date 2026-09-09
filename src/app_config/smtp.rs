use serde::{Deserialize, Serialize};


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
