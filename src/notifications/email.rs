use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::app_config::{AppConfigSmtp, AppConfigSmtpEncryption};
use crate::notifications::message::NotificationMessage;


/// Delivery by email, and the one place that talks SMTP. Built from `[smtp]` in
/// config.toml, so a caller only has to know who to tell and what to say.
pub struct EmailChannel {
    smtp: AppConfigSmtp,
}


impl EmailChannel {

    pub fn new(smtp: AppConfigSmtp) -> Self {
        Self { smtp }
    }

    pub fn max_output_bytes(&self) -> usize {
        self.smtp.max_output_bytes
    }

    /// Sends one plain-text message to every recipient at once.
    ///
    /// Plain text because the one thing every mail client renders the same is a
    /// monospaced block of a command's own output.
    ///
    /// The transport is built per send rather than kept: a notification is rare enough
    /// that a connection held open between failures would be idle for hours, and a
    /// relay's own idea of how long that may last is not worth tracking.
    pub async fn send(&self, recipients: &[String], message: &NotificationMessage) -> anyhow::Result<()> {

        let from: Mailbox = self.smtp.from.parse()
            .map_err(|e| anyhow::anyhow!("[smtp] from '{}' is not an address: {}", self.smtp.from, e))?;

        let mut builder = Message::builder()
            .from(from)
            .subject(&message.subject);

        for recipient in recipients {
            let mailbox: Mailbox = recipient.parse()
                .map_err(|e| anyhow::anyhow!("'{}' is not an address: {}", recipient, e))?;

            builder = builder.to(mailbox);
        }

        let email = builder.body(message.body.clone())?;

        self.transport()?.send(email).await?;

        Ok(())
    }

    fn transport(&self) -> anyhow::Result<AsyncSmtpTransport<Tokio1Executor>> {

        let builder = match self.smtp.encryption {
            AppConfigSmtpEncryption::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.smtp.host)?,
            AppConfigSmtpEncryption::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(&self.smtp.host)?,
            AppConfigSmtpEncryption::None => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&self.smtp.host),
        };

        let builder = builder.port(self.smtp.port);

        // An empty username is a relay that authenticates nobody - offering it empty
        // credentials would fail the handshake rather than skip it.
        let builder = match self.smtp.username.is_empty() {
            true => builder,
            false => builder.credentials(Credentials::new(
                self.smtp.username.clone(),
                self.smtp.password.clone(),
            )),
        };

        Ok(builder.build())
    }

}
