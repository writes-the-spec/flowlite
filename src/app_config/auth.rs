use std::time::Duration;
use serde::{Deserialize, Serialize};


/// Who may open the dashboard, decided by an OAuth2 identity provider.
///
/// No default, for the reason `[smtp]` has none: there is no default identity provider.
/// Absent means the dashboard has no login, which is what it has always been on localhost.
/// A section missing a required key is a startup error rather than a dashboard left open.
#[derive(Deserialize, Serialize, Clone)]
pub struct AppConfigAuth {
    /// Required rather than defaulted, so a config file keeps its meaning once a second
    /// flow exists.
    pub flow: AuthFlow,
    /// The provider's token endpoint - Keycloak's is
    /// `<base>/realms/<realm>/protocol/openid-connect/token`.
    pub token_url: String,
    pub client_id: String,
    /// None for a public client. Sent in the form body, which Keycloak, Auth0 and Entra
    /// all accept.
    #[serde(default, skip_serializing)]
    pub client_secret: Option<String>,
    #[serde(default = "default_auth_scope")]
    pub scope: String,
    /// How long one call to the token endpoint may take. A hung provider would otherwise
    /// hold every page that needs a session refreshed.
    #[serde(default = "default_auth_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Whether the session cookie is sent over HTTPS only. Turning auth on is what
    /// exposing the dashboard asks for, and that belongs behind TLS - off is for trying it
    /// out over plain HTTP.
    #[serde(default = "default_auth_cookie_secure")]
    pub cookie_secure: bool,
}

/// How a person proves who they are to the identity provider.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthFlow {
    /// The OAuth2 password grant: the dashboard's own form takes a username and password
    /// and hands them to the token endpoint. Most providers ship it switched off -
    /// Keycloak calls it "Direct access grants" - and it cannot do MFA.
    Password,
}

fn default_auth_scope() -> String {
    "openid".to_string()
}

fn default_auth_timeout_seconds() -> u64 {
    10
}

fn default_auth_cookie_secure() -> bool {
    true
}

impl AppConfigAuth {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds)
    }
}

/// Manual so the client secret is never printed, the way `AppConfig`'s own `Debug` keeps
/// `[secrets]` out.
impl std::fmt::Debug for AppConfigAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppConfigAuth")
            .field("flow", &self.flow)
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("client_secret", &self.client_secret.as_ref().map(|_| "<redacted>"))
            .field("scope", &self.scope)
            .field("timeout_seconds", &self.timeout_seconds)
            .field("cookie_secure", &self.cookie_secure)
            .finish()
    }
}
