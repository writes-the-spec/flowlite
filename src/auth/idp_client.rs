use serde::Deserialize;

use crate::app_config::AppConfigAuth;


/// Why a token endpoint did not grant, split by what the dashboard does about it.
#[derive(Debug)]
pub enum IdpError {
    /// The provider answered and said no - a wrong password, a disabled user, a
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

/// Only checked for, never kept. flowlite sends the token nowhere: a grant straight from
/// the provider already says the person is allowed in, and the session's length is
/// flowlite's own.
#[derive(Deserialize)]
struct TokenResponse {
    /// Required so that a `token_url` pointed at something that answers 200 to anything
    /// does not turn every password into a login.
    #[allow(dead_code)]
    access_token: String,
}

#[derive(Deserialize)]
struct OAuthErrorResponse {
    error: String,
}

/// The one place that talks to the identity provider. Each login flow is one method here.
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

    /// The OAuth2 password grant: Ok when the provider grants a token for this username
    /// and password.
    pub async fn password(&self, username: &str, password: &str) -> Result<(), IdpError> {

        let mut form = vec![
            ("grant_type", "password"),
            ("username", username),
            ("password", password),
            ("scope", self.auth.scope.as_str()),
            ("client_id", self.auth.client_id.as_str()),
        ];

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

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| IdpError::Unavailable(format!("the token endpoint did not answer with a token: {e}")))?;

        Ok(())
    }

}
