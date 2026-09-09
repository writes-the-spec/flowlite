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

        let text = post_text(message);

        let mut errors: Vec<String> = Vec::new();

        for recipient in recipients {
            if let Err(e) = self.post(&client, recipient, &text).await {
                errors.push(format!("{}: {:#}", recipient, e));
            }
        }

        if !errors.is_empty() {
            anyhow::bail!("{}", errors.join("; "));
        }

        Ok(())
    }

    async fn post(&self, client: &reqwest::Client, channel: &str, text: &str) -> anyhow::Result<()> {

        let response = client
            .post(&self.slack.api_url)
            .bearer_auth(&self.slack.token)
            .json(&serde_json::json!({
                "channel": channel,
                "text": text,
            }))
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

/// The message as one Slack post: the subject as the line you read in a notification
/// preview, and the body fenced, because it is aligned plain text that only survives in a
/// monospaced block.
fn post_text(message: &NotificationMessage) -> String {
    format!(
        "*{}*\n```\n{}\n```",
        message.subject,
        fence_safe(message.body.trim_end()),
    )
}

/// A fence inside the body would close the block early and garble everything after it,
/// and the body quotes output from a command that may print anything at all.
fn fence_safe(body: &str) -> String {
    body.replace("```", "` ` `")
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;
    use axum::Json;
    use axum::extract::State;
    use axum::routing::post;

    /// One request the fake Slack recorded, in the parts a test asks about.
    #[derive(Clone)]
    struct Post {
        authorization: String,
        channel: String,
        text: String,
    }

    #[derive(Clone)]
    struct FakeSlack {
        posts: Arc<Mutex<Vec<Post>>>,
        /// The `error` to refuse with, keyed by channel — everything else is accepted.
        refusals: Arc<Vec<(String, String)>>,
    }

    /// A Slack that answers on localhost, so the whole path — the header, the JSON, and
    /// the `ok: false` in a 200 — is exercised rather than mocked away.
    async fn fake_slack(refusals: &[(&str, &str)]) -> (String, Arc<Mutex<Vec<Post>>>) {

        let state = FakeSlack {
            posts: Arc::new(Mutex::new(Vec::new())),
            refusals: Arc::new(
                refusals
                    .iter()
                    .map(|(channel, error)| (channel.to_string(), error.to_string()))
                    .collect(),
            ),
        };

        let posts = state.posts.clone();

        let router = axum::Router::new()
            .route("/chat.postMessage", post(post_message))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        (format!("http://{}/chat.postMessage", address), posts)
    }

    async fn post_message(
        State(state): State<FakeSlack>,
        headers: axum::http::HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {

        let channel = body["channel"].as_str().unwrap().to_string();

        state.posts.lock().unwrap().push(Post {
            authorization: headers
                .get("authorization")
                .map(|value| value.to_str().unwrap().to_string())
                .unwrap_or_default(),
            channel: channel.clone(),
            text: body["text"].as_str().unwrap().to_string(),
        });

        let refusal = state.refusals
            .iter()
            .find(|(refused, _)| refused == &channel);

        match refusal {
            Some((_, error)) => Json(serde_json::json!({ "ok": false, "error": error })),
            None => Json(serde_json::json!({ "ok": true })),
        }
    }

    fn channel(api_url: String) -> SlackChannel {
        SlackChannel::new(AppConfigSlack {
            token: "xoxb-test".to_string(),
            api_url,
            timeout_seconds: 5,
            max_output_bytes: 2048,
        })
    }

    fn message() -> NotificationMessage {
        NotificationMessage {
            subject: "[flowlite] Nightly Sync run 42 failed".to_string(),
            body: "Job run 42 of 'Nightly Sync' (nightly-sync) failed.".to_string(),
            // Slack shapes its post from the subject and the body, so a message with no
            // HTML rendering has to post exactly the same as one that has one.
            html: None,
        }
    }

    #[tokio::test]
    async fn a_post_carries_the_token_the_conversation_and_the_message() {

        let (api_url, posts) = fake_slack(&[]).await;

        channel(api_url).send(&["#oncall".to_string()], &message()).await.unwrap();

        let posts = posts.lock().unwrap();

        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].authorization, "Bearer xoxb-test");
        assert_eq!(posts[0].channel, "#oncall");
        assert!(posts[0].text.contains("[flowlite] Nightly Sync run 42 failed"), "{}", posts[0].text);
        assert!(posts[0].text.contains("(nightly-sync) failed."), "{}", posts[0].text);
    }

    #[tokio::test]
    async fn every_conversation_named_gets_its_own_call() {

        let (api_url, posts) = fake_slack(&[]).await;

        channel(api_url)
            .send(&["#oncall".to_string(), "#data".to_string()], &message())
            .await
            .unwrap();

        let channels: Vec<String> = posts.lock().unwrap()
            .iter()
            .map(|post| post.channel.clone())
            .collect();

        assert_eq!(channels, vec!["#oncall", "#data"]);
    }

    /// The failure this channel exists to catch: Slack says no in a 200, and a send that
    /// trusted the status code would have recorded it as delivered.
    #[tokio::test]
    async fn a_refusal_in_a_200_is_a_failure_naming_what_slack_said() {

        let (api_url, _) = fake_slack(&[("#typo", "channel_not_found")]).await;

        let error = channel(api_url)
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

        let (api_url, posts) = fake_slack(&[("#typo", "channel_not_found")]).await;

        let error = channel(api_url)
            .send(&["#typo".to_string(), "#oncall".to_string()], &message())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("#typo"), "{}", error);
        assert!(!error.contains("#oncall"), "{}", error);

        assert_eq!(posts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn an_answer_that_is_not_json_is_reported_as_what_it_said() {

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let router = axum::Router::new()
                .route("/chat.postMessage", post(|| async { "<html>gateway error</html>" }));

            axum::serve(listener, router).await.unwrap();
        });

        let error = channel(format!("http://{}/chat.postMessage", address))
            .send(&["#oncall".to_string()], &message())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("gateway error"), "{}", error);
    }

    #[test]
    fn the_body_is_fenced_so_the_alignment_survives() {
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
