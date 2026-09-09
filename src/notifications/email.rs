use lettre::message::{Mailbox, MultiPart};
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

    /// Sends one message to every recipient at once.
    ///
    /// The transport is built per send rather than kept: a notification is rare enough
    /// that a connection held open between failures would be idle for hours, and a
    /// relay's own idea of how long that may last is not worth tracking.
    pub async fn send(&self, recipients: &[String], message: &NotificationMessage) -> anyhow::Result<()> {

        let email = self.build(recipients, message)?;

        self.transport()?.send(email).await?;

        Ok(())
    }

    /// A message carrying its HTML rendering goes out as `multipart/alternative`, so a
    /// client that renders HTML shows the formatted message and one that does not - or a
    /// reader who prefers plain text - still gets everything it says. A message with no
    /// HTML rendering is plain text alone rather than an empty alternative part.
    ///
    /// Separate from `send` so that what is built can be asserted on without a relay.
    fn build(&self, recipients: &[String], message: &NotificationMessage) -> anyhow::Result<Message> {

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

        let email = match &message.html {
            Some(html) => builder.multipart(MultiPart::alternative_plain_html(
                message.body.clone(),
                html.clone(),
            ))?,
            None => builder.body(message.body.clone())?,
        };

        Ok(email)
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


#[cfg(test)]
mod tests {
    use super::*;

    fn channel() -> EmailChannel {
        EmailChannel::new(AppConfigSmtp {
            host: "localhost".to_string(),
            port: 25,
            username: String::new(),
            password: String::new(),
            from: "flowlite@example.com".to_string(),
            encryption: AppConfigSmtpEncryption::None,
            max_output_bytes: 4096,
        })
    }

    fn message(html: Option<&str>) -> NotificationMessage {
        NotificationMessage {
            subject: "[flowlite] Nightly Sync run 42 failed".to_string(),
            body: "Job run 42 of 'Nightly Sync' (nightly-sync) failed.".to_string(),
            html: html.map(|html| html.to_string()),
            // Email ignores the Slack rendering, so what a message carries there cannot
            // change what is built here.
            blocks: None,
        }
    }

    fn formatted(email: &EmailChannel, message: &NotificationMessage) -> String {
        let built = email
            .build(&["ops@example.com".to_string()], message)
            .expect("the message should build");

        String::from_utf8(built.formatted()).expect("a formatted message should be text")
    }

    /// Both parts, so a client that renders HTML gets the formatted message and one that
    /// does not still gets everything it says.
    #[test]
    fn a_message_with_an_html_rendering_is_sent_as_both_parts() {

        let raw = formatted(&channel(), &message(Some("<p>it broke</p>")));

        assert!(raw.contains("multipart/alternative"), "{}", raw);
        assert!(raw.contains("text/plain"), "{}", raw);
        assert!(raw.contains("text/html"), "{}", raw);
    }

    /// A message kind with no HTML rendering is still an email: the body alone, exactly
    /// as it went out before there was an HTML part, rather than an empty alternative.
    #[test]
    fn a_message_with_no_html_rendering_is_sent_as_plain_text_alone() {

        let raw = formatted(&channel(), &message(None));

        assert!(!raw.contains("multipart"), "{}", raw);
        assert!(raw.ends_with("Job run 42 of 'Nightly Sync' (nightly-sync) failed."), "{}", raw);
    }

    #[test]
    fn every_recipient_is_on_the_one_message() {

        let email = channel();

        let built = email.build(
            &["ops@example.com".to_string(), "oncall@example.com".to_string()],
            &message(None),
        ).expect("the message should build");

        let raw = String::from_utf8(built.formatted()).expect("a formatted message should be text");

        assert!(raw.contains("ops@example.com"), "{}", raw);
        assert!(raw.contains("oncall@example.com"), "{}", raw);
    }

    #[test]
    fn a_recipient_that_is_not_an_address_is_named_in_the_error() {

        let error = channel()
            .build(&["not an address".to_string()], &message(None))
            .expect_err("an unparseable recipient should not build");

        assert!(format!("{}", error).contains("not an address"), "{}", error);
    }
}
