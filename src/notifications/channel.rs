use crate::app_config::AppConfig;
use crate::crud::job_run_notification::NotificationChannel;
use crate::notifications::email::EmailChannel;
use crate::notifications::message::NotificationMessage;
use crate::notifications::slack::SlackChannel;


/// Every channel this process can actually deliver over, built once from config.toml.
///
/// A channel nothing configured is `None` here rather than absent: a notification asking
/// for it is then recorded as failed, naming what is missing, instead of sitting open
/// forever waiting for a sender that will never exist.
pub struct NotificationChannels {
    email: Option<EmailChannel>,
    slack: Option<SlackChannel>,
}


impl NotificationChannels {

    pub fn from_config(app_config: &AppConfig) -> Self {
        Self {
            email: app_config.smtp.clone().map(EmailChannel::new),
            slack: app_config.slack.clone().map(SlackChannel::new),
        }
    }

    /// Delivers one message over the channel the notification asked for.
    ///
    /// A `match` rather than a registry of trait objects: the channels are known at
    /// compile time, so adding one is a variant the compiler then demands an arm for
    /// here — which is a better reminder than a `Vec` nobody was told to register in.
    pub async fn send(
        &self,
        channel: NotificationChannel,
        recipients: &[String],
        message: &NotificationMessage,
    ) -> anyhow::Result<()> {

        match channel {
            NotificationChannel::Email => match &self.email {
                Some(email) => email.send(recipients, message).await,
                None => anyhow::bail!(
                    "config.toml has no [smtp] section, so nothing can be delivered by email",
                ),
            },
            NotificationChannel::Slack => match &self.slack {
                Some(slack) => slack.send(recipients, message).await,
                None => anyhow::bail!(
                    "config.toml has no [slack] section, so nothing can be delivered by slack",
                ),
            },
        }
    }

    /// The most of one stream of one failed task a message for this channel may carry.
    ///
    /// Asked of the channel rather than fixed on the message, because the reason for a cap
    /// is the transport's own limit — a relay's maximum message size, or how much of a
    /// chat message anybody scrolls through.
    pub fn max_output_bytes(&self, channel: NotificationChannel) -> usize {

        match channel {
            NotificationChannel::Email => self.email
                .as_ref()
                .map(|email| email.max_output_bytes())
                .unwrap_or(0),
            NotificationChannel::Slack => self.slack
                .as_ref()
                .map(|slack| slack.max_output_bytes())
                .unwrap_or(0),
        }
    }

}
