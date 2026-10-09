use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Form, Json, Router};
use serde::Deserialize;

pub const USERNAME: &str = "ada";
pub const PASSWORD: &str = "correct horse";
pub const CLIENT_ID: &str = "flowlite";
pub const CLIENT_SECRET: &str = "s3cret";

/// How the fake answers every call from now on - switched mid-test to take the provider
/// down after a login has already succeeded.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Normally,
    WithAServerError,
    WithABodyThatIsNotATokenResponse,
    NeverInTime,
}

#[derive(Clone)]
pub struct FakeIdpOptions {
    /// None leaves `expires_in` out of the response, which the spec allows.
    pub expires_in: Option<i64>,
    /// Every refresh retires the token it was given and issues a new one, as Keycloak's
    /// "Revoke Refresh Token" does. Off, a refresh answers with no refresh token at all.
    pub rotate_refresh_tokens: bool,
    /// How long a refresh takes to answer, so two concurrent ones overlap.
    pub refresh_delay: Duration,
}

impl Default for FakeIdpOptions {
    fn default() -> Self {
        Self {
            expires_in: Some(300),
            rotate_refresh_tokens: false,
            refresh_delay: Duration::ZERO,
        }
    }
}

#[derive(Default)]
struct Ledger {
    issued: u32,
    valid_refresh_tokens: HashSet<String>,
    refreshes: u32,
    refreshes_with_a_retired_token: u32,
}

#[derive(Clone)]
struct FakeIdpState {
    options: FakeIdpOptions,
    answer: Arc<Mutex<Answer>>,
    ledger: Arc<Mutex<Ledger>>,
}

/// An OAuth2 token endpoint on localhost, answering the password and refresh-token grants
/// for one user of one confidential client - so the dashboard's login is exercised over
/// the form body, status codes and JSON it really sends and reads, rather than mocked.
///
/// Served from its own thread and runtime, so a plain `#[test]` driving the binary and a
/// `#[tokio::test]` calling the library can both use it.
pub struct FakeIdp {
    pub token_url: String,
    answer: Arc<Mutex<Answer>>,
    ledger: Arc<Mutex<Ledger>>,
}

impl FakeIdp {

    pub fn start() -> Self {
        Self::with(FakeIdpOptions::default())
    }

    pub fn with(options: FakeIdpOptions) -> Self {

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let token_url = format!("http://{}/token", listener.local_addr().unwrap());

        let state = FakeIdpState {
            options,
            answer: Arc::new(Mutex::new(Answer::Normally)),
            ledger: Arc::new(Mutex::new(Ledger::default())),
        };

        let fake = FakeIdp {
            token_url,
            answer: state.answer.clone(),
            ledger: state.ledger.clone(),
        };

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();

            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let router = Router::new().route("/token", post(token)).with_state(state);
                axum::serve(listener, router).await.unwrap();
            });
        });

        fake
    }

    pub fn answer(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }

    /// Retires every refresh token issued so far - an administrator disabling the user, or
    /// ending their sessions, at the provider.
    pub fn revoke_every_refresh_token(&self) {
        self.ledger.lock().unwrap().valid_refresh_tokens.clear();
    }

    pub fn refreshes(&self) -> u32 {
        self.ledger.lock().unwrap().refreshes
    }

    /// Refreshes that presented a token an earlier refresh had already retired - what two
    /// racing refreshes of one session look like to a provider that rotates.
    pub fn refreshes_with_a_retired_token(&self) -> u32 {
        self.ledger.lock().unwrap().refreshes_with_a_retired_token
    }
}


#[derive(Deserialize)]
struct TokenRequest {
    grant_type: String,
    client_id: String,
    client_secret: Option<String>,
    username: Option<String>,
    password: Option<String>,
    refresh_token: Option<String>,
}

async fn token(State(state): State<FakeIdpState>, Form(request): Form<TokenRequest>) -> Response {

    let answer = *state.answer.lock().unwrap();

    match answer {
        Answer::Normally => {}
        Answer::WithAServerError => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "the provider fell over").into_response();
        }
        Answer::WithABodyThatIsNotATokenResponse => {
            return Json(serde_json::json!({ "hello": "world" })).into_response();
        }
        Answer::NeverInTime => {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    if request.client_id != CLIENT_ID || request.client_secret.as_deref() != Some(CLIENT_SECRET) {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client");
    }

    match request.grant_type.as_str() {
        "password" => {
            let correct = request.username.as_deref() == Some(USERNAME)
                && request.password.as_deref() == Some(PASSWORD);

            if !correct {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
            }

            let refresh_token = issue_refresh_token(&state);
            token_response(&state, Some(refresh_token))
        }
        "refresh_token" => {
            tokio::time::sleep(state.options.refresh_delay).await;

            let presented = request.refresh_token.unwrap_or_default();

            let still_valid = {
                let mut ledger = state.ledger.lock().unwrap();
                ledger.refreshes += 1;

                let still_valid = ledger.valid_refresh_tokens.contains(&presented);
                if !still_valid {
                    ledger.refreshes_with_a_retired_token += 1;
                }

                still_valid
            };

            if !still_valid {
                return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
            }

            if !state.options.rotate_refresh_tokens {
                return token_response(&state, None);
            }

            state.ledger.lock().unwrap().valid_refresh_tokens.remove(&presented);

            let refresh_token = issue_refresh_token(&state);
            token_response(&state, Some(refresh_token))
        }
        _ => oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type"),
    }
}

fn issue_refresh_token(state: &FakeIdpState) -> String {
    let mut ledger = state.ledger.lock().unwrap();

    ledger.issued += 1;
    let refresh_token = format!("refresh-{}", ledger.issued);
    ledger.valid_refresh_tokens.insert(refresh_token.clone());

    refresh_token
}

fn token_response(state: &FakeIdpState, refresh_token: Option<String>) -> Response {
    let mut body = serde_json::json!({
        "access_token": "an-access-token",
        "token_type": "Bearer",
    });

    if let Some(expires_in) = state.options.expires_in {
        body["expires_in"] = serde_json::json!(expires_in);
    }

    if let Some(refresh_token) = refresh_token {
        body["refresh_token"] = serde_json::json!(refresh_token);
    }

    Json(body).into_response()
}

fn oauth_error(status: StatusCode, error: &str) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}
