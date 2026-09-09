use serde::Deserialize;

use crate::app_config::AppConfigSlack;
use crate::notifications::message::NotificationMessage;


/// Delivery by Slack, and the one place that talks to its API. Built from `[slack]` in
/// config.toml, so a caller only has to know which conversations to post in and what to
/// say.
pub struct SlackChannel {
    slack: AppConfigSlack,
}

/// What `chat.postMessage` answers with. Slack reports a refusal — an unknown channel, a
/// bot that was never invited, a revoked token — as `ok: false` in a **200** body, so a
/// send that only checked the status code would record every one of them as delivered.
#[derive(Deserialize)]
struct SlackResponse {
    ok: bool,
    #[serde(default)]
    error: String,
}


impl SlackChannel {

    pub fn new(slack: AppConfigSlack) -> Self {

        // reqwest reads rustls' process-wide provider when it builds a client, and this
        // is the only thing in flowlite that asks for one — lettre carries its own. Ring
        // rather than aws-lc-rs so the binary holds one implementation and the build
        // needs no C toolchain. Already-installed is not an error: `from_config` runs
        // once per process, but a test may build several channels.
        let _ = rustls::crypto::ring::default_provider().install_default();

        Self { slack }
    }

    pub fn max_output_bytes(&self) -> usize {
        self.slack.max_output_bytes
    }

    /// Posts the message into every conversation named, one call each — `chat.postMessage`
    /// addresses one conversation, unlike a mail header that takes a list.
    ///
    /// A conversation that refuses does not stop the others: an unknown `#typo` alongside
    /// a real `#oncall` should still reach `#oncall`, since the whole point of the alert
    /// is that somebody sees it. The failures are collected and reported together, so the
    /// row still records that the notification did not fully land.
    pub async fn send(&self, recipients: &[String], message: &NotificationMessage) -> anyhow::Result<()> {

        let client = reqwest::Client::builder()
            .timeout(self.slack.timeout())
            .build()?;

        let mut errors: Vec<String> = Vec::new();

        for recipient in recipients {
            if let Err(e) = self.post(&client, &post_payload(recipient, message)).await {
                errors.push(format!("{}: {:#}", recipient, e));
            }
        }

        if !errors.is_empty() {
            anyhow::bail!("{}", errors.join("; "));
        }

        Ok(())
    }

    async fn post(&self, client: &reqwest::Client, payload: &serde_json::Value) -> anyhow::Result<()> {

        let response = client
            .post(&self.slack.api_url)
            .bearer_auth(&self.slack.token)
            .json(payload)
            .send()
            .await?;

        let status = response.status();

        // Read as text first: a token so wrong that Slack answers with HTML, or a proxy
        // in the way, would otherwise be reported as a JSON parse error rather than as
        // whatever it actually said.
        let body = response.text().await?;

        let Ok(parsed) = serde_json::from_str::<SlackResponse>(&body) else {
            anyhow::bail!("Slack answered {} with {}", status, body.trim());
        };

        if !parsed.ok {
            anyhow::bail!("Slack refused the message: {}", parsed.error);
        }

        Ok(())
    }

}

/// What one `chat.postMessage` call sends.
///
/// `text` is what Slack shows in a notification preview and reads out where it cannot
/// render blocks, so a message that has blocks puts its subject there and lets them carry
/// the rest. A message kind with no blocks has nowhere else to say what it says, so it
/// falls back to the whole thing in one fence.
fn post_payload(channel: &str, message: &NotificationMessage) -> serde_json::Value {

    match &message.blocks {
        Some(blocks) => serde_json::json!({
            "channel": channel,
            "text": message.subject,
            "blocks": blocks,
        }),
        None => serde_json::json!({
            "channel": channel,
            "text": post_text(message),
        }),
    }
}

/// The message as one Slack post, for a message kind that has no blocks: the subject as
/// the line you read in a notification preview, and the body fenced, because it is aligned
/// plain text that only survives in a monospaced block.
fn post_text(message: &NotificationMessage) -> String {
    format!(
        "*{}*\n```\n{}\n```",
        message.subject,
        fence_safe(message.body.trim_end()),
    )
}

/// A fence inside the body would close the block early and garble everything after it,
/// and the body quotes output from a command that may print anything at all.
pub fn fence_safe(body: &str) -> String {
    body.replace("```", "` ` `")
}

/// The three characters Slack reads as its own before it reads anything else. A task is
/// free to print `a < b`, and unescaped it arrives as the start of a tag Slack then eats
/// along with whatever follows it.
///
/// `&` first, or the ampersands written by the other two are escaped a second time.
pub fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FakeSlack;

    /// A message with no Slack rendering of its own, which is what the fallback path
    /// exists for.
    fn message() -> NotificationMessage {
        NotificationMessage {
            subject: "[flowlite] Nightly Sync run 42 failed".to_string(),
            body: "Job run 42 of 'Nightly Sync' (nightly-sync) failed.".to_string(),
            // Slack shapes its post from the subject and the body, so a message with no
            // HTML rendering has to post exactly the same as one that has one.
            html: None,
            blocks: None,
        }
    }

    fn message_with_blocks() -> NotificationMessage {
        NotificationMessage {
            blocks: Some(serde_json::json!([
                {
                    "type": "header",
                    "text": { "type": "plain_text", "text": ":x: Nightly Sync run 42 failed" },
                },
            ])),
            ..message()
        }
    }

    #[tokio::test]
    async fn a_post_carries_the_token_the_conversation_and_the_message() {

        let slack = FakeSlack::start().await;

        SlackChannel::new(slack.config()).send(&["#oncall".to_string()], &message()).await.unwrap();

        let posts = slack.posts();

        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].authorization, "Bearer xoxb-test");
        assert_eq!(posts[0].channel, "#oncall");
        assert!(posts[0].text.contains("[flowlite] Nightly Sync run 42 failed"), "{}", posts[0].text);
        assert!(posts[0].text.contains("(nightly-sync) failed."), "{}", posts[0].text);
    }

    /// What the blocks are for: the post Slack lays out itself, with the subject left in
    /// `text` because that is what a notification preview and a screen reader read.
    #[tokio::test]
    async fn a_message_with_blocks_posts_them_rather_than_one_fenced_wall() {

        let slack = FakeSlack::start().await;

        SlackChannel::new(slack.config())
            .send(&["#oncall".to_string()], &message_with_blocks())
            .await
            .unwrap();

        let posts = slack.posts();

        assert_eq!(posts[0].text, "[flowlite] Nightly Sync run 42 failed");
        assert_eq!(posts[0].blocks[0]["type"], "header");
        assert!(!posts[0].text.contains("```"), "{}", posts[0].text);
    }

    /// A message kind with no Slack rendering still has to reach Slack, so the post falls
    /// back to the whole thing fenced rather than to a subject line on its own.
    #[tokio::test]
    async fn a_message_with_no_blocks_still_posts_everything_it_says() {

        let slack = FakeSlack::start().await;

        SlackChannel::new(slack.config()).send(&["#oncall".to_string()], &message()).await.unwrap();

        let posts = slack.posts();

        assert!(posts[0].blocks.is_null(), "{}", posts[0].blocks);
        assert!(posts[0].text.contains("(nightly-sync) failed."), "{}", posts[0].text);
    }

    #[tokio::test]
    async fn every_conversation_named_gets_its_own_call() {

        let slack = FakeSlack::start().await;

        SlackChannel::new(slack.config())
            .send(&["#oncall".to_string(), "#data".to_string()], &message())
            .await
            .unwrap();

        let channels: Vec<String> = slack.posts()
            .iter()
            .map(|post| post.channel.clone())
            .collect();

        assert_eq!(channels, vec!["#oncall", "#data"]);
    }

    /// The failure this channel exists to catch: Slack says no in a 200, and a send that
    /// trusted the status code would have recorded it as delivered.
    #[tokio::test]
    async fn a_refusal_in_a_200_is_a_failure_naming_what_slack_said() {

        let slack = FakeSlack::refusing(&[("#typo", "channel_not_found")]).await;

        let error = SlackChannel::new(slack.config())
            .send(&["#typo".to_string()], &message())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("#typo"), "{}", error);
        assert!(error.contains("channel_not_found"), "{}", error);
    }

    /// An alert is worth more delivered somewhere than nowhere, so one bad name does not
    /// take the good one down with it — but the notification is still recorded as failed.
    #[tokio::test]
    async fn one_refused_conversation_does_not_stop_the_others() {

        let slack = FakeSlack::refusing(&[("#typo", "channel_not_found")]).await;

        let error = SlackChannel::new(slack.config())
            .send(&["#typo".to_string(), "#oncall".to_string()], &message())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("#typo"), "{}", error);
        assert!(!error.contains("#oncall"), "{}", error);

        assert_eq!(slack.posts().len(), 2);
    }

    #[tokio::test]
    async fn an_answer_that_is_not_json_is_reported_as_what_it_said() {

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let router = axum::Router::new().route(
                "/chat.postMessage",
                axum::routing::post(|| async { "<html>gateway error</html>" }),
            );

            axum::serve(listener, router).await.unwrap();
        });

        let error = SlackChannel::new(AppConfigSlack {
            token: "xoxb-test".to_string(),
            api_url: format!("http://{}/chat.postMessage", address),
            timeout_seconds: 5,
            max_output_bytes: 2048,
        })
            .send(&["#oncall".to_string()], &message())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("gateway error"), "{}", error);
    }

    #[test]
    fn the_fallback_body_is_fenced_so_the_alignment_survives() {
        let text = post_text(&message());

        assert!(text.starts_with("*[flowlite] Nightly Sync run 42 failed*\n```\n"), "{}", text);
        assert!(text.ends_with("\n```"), "{}", text);
    }

    /// A command is free to print a fence of its own, and it must not be able to end the
    /// block the rest of the message is quoted in.
    #[test]
    fn a_fence_in_the_quoted_output_cannot_close_the_block() {
        let broken = fence_safe("stderr:\n```\nboom\n");

        assert!(!broken.contains("```"), "{}", broken);
        assert!(broken.contains("boom"), "{}", broken);
    }
}
