use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;

use crate::app_config::AppConfigAuth;


/// What a token endpoint granted, kept to the part flowlite uses.
///
/// The access token itself is dropped. flowlite never sends it anywhere: it got it straight
/// from the provider, so the grant already says the person is allowed in, and its expiry is
/// only when to ask the provider again.
#[derive(Debug, Clone)]
pub struct TokenGrant {
    pub refresh_token: Option<String>,
    pub expires_at: DateTime<Utc>,
}

/// Why a token endpoint did not grant, split by what the dashboard does about it.
#[derive(Debug)]
pub enum IdpError {
    /// The provider answered and said no - a wrong password, a revoked refresh token, a
    /// misconfigured client. The person is not let in.
    Rejected(String),
    /// The provider did not answer in a form that says anything about the person: it was
    /// unreachable, slow, failing, or answered something that is not a token response.
    Unavailable(String),
}

impl std::fmt::Display for IdpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdpError::Rejected(reason) => write!(f, "the identity provider refused: {reason}"),
            IdpError::Unavailable(reason) => write!(f, "the identity provider is unavailable: {reason}"),
        }
    }
}

/// The spec only recommends `expires_in`, and a grant has to run out some time.
const EXPIRES_IN_SECONDS_WHEN_UNSAID: i64 = 300;

#[derive(Deserialize)]
struct TokenResponse {
    /// Never read, and required for exactly that reason: a `token_url` pointed at something
    /// that answers 200 to anything would otherwise turn every password into a login.
    #[allow(dead_code)]
    access_token: String,
    expires_in: Option<i64>,
    refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct OAuthErrorResponse {
    error: String,
}

/// The one place that talks to the identity provider's token endpoint. Each login flow is
/// one method here, and every one of them ends in the same `TokenGrant`.
pub struct IdpClient {
    auth: AppConfigAuth,
}


impl IdpClient {

    pub fn new(auth: AppConfigAuth) -> Self {

        // See `SlackChannel::new`: reqwest reads rustls' process-wide provider when it
        // builds a client, and already-installed is not an error.
        let _ = rustls::crypto::ring::default_provider().install_default();

        Self { auth }
    }

    /// The OAuth2 password grant.
    pub async fn password(&self, username: &str, password: &str, now: DateTime<Utc>) -> Result<TokenGrant, IdpError> {
        self.request_token(
            &[
                ("grant_type", "password"),
                ("username", username),
                ("password", password),
                ("scope", &self.auth.scope),
            ],
            now,
        ).await
    }

    pub async fn refresh(&self, refresh_token: &str, now: DateTime<Utc>) -> Result<TokenGrant, IdpError> {
        self.request_token(
            &[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
            ],
            now,
        ).await
    }

    async fn request_token(&self, grant: &[(&str, &str)], now: DateTime<Utc>) -> Result<TokenGrant, IdpError> {

        let mut form = grant.to_vec();
        form.push(("client_id", &self.auth.client_id));

        if let Some(client_secret) = &self.auth.client_secret {
            form.push(("client_secret", client_secret));
        }

        let client = reqwest::Client::builder()
            .timeout(self.auth.timeout())
            .build()
            .map_err(|e| IdpError::Unavailable(e.to_string()))?;

        let response = client
            .post(&self.auth.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| IdpError::Unavailable(e.to_string()))?;

        let status = response.status();

        // RFC 6749 5.2: a refused grant or client is a 400, or a 401 for client auth.
        if status == reqwest::StatusCode::BAD_REQUEST || status == reqwest::StatusCode::UNAUTHORIZED {
            let reason = match response.json::<OAuthErrorResponse>().await {
                Ok(body) => body.error,
                Err(_) => status.to_string(),
            };

            return Err(IdpError::Rejected(reason));
        }

        if !status.is_success() {
            return Err(IdpError::Unavailable(format!("the token endpoint answered {status}")));
        }

        let token: TokenResponse = response
            .json()
            .await
            .map_err(|e| IdpError::Unavailable(format!("the token endpoint did not answer with a token: {e}")))?;

        let expires_in = token.expires_in.unwrap_or(EXPIRES_IN_SECONDS_WHEN_UNSAID);

        Ok(TokenGrant {
            refresh_token: token.refresh_token,
            expires_at: now + TimeDelta::seconds(expires_in),
        })
    }

}
