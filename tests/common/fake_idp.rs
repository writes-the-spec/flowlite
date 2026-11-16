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

/// How the fake answers every call from now on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Normally,
    WithAServerError,
    WithABodyThatIsNotATokenResponse,
    NeverInTime,
}

#[derive(Clone)]
struct FakeIdpState {
    answer: Arc<Mutex<Answer>>,
}

/// An OAuth2 token endpoint on localhost, answering the password grant for one user of one
/// confidential client - so the dashboard's login is exercised over the form body, status
/// codes and JSON it really sends and reads, rather than mocked.
///
/// Served from its own thread and runtime, so a plain `#[test]` driving the binary and a
/// `#[tokio::test]` calling the library can both use it.
pub struct FakeIdp {
    pub token_url: String,
    answer: Arc<Mutex<Answer>>,
}

impl FakeIdp {

    pub fn start() -> Self {

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let token_url = format!("http://{}/token", listener.local_addr().unwrap());

        let state = FakeIdpState {
            answer: Arc::new(Mutex::new(Answer::Normally)),
        };

        let fake = FakeIdp {
            token_url,
            answer: state.answer.clone(),
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
}


#[derive(Deserialize)]
struct TokenRequest {
    grant_type: String,
    client_id: String,
    client_secret: Option<String>,
    username: Option<String>,
    password: Option<String>,
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

    if request.grant_type != "password" {
        return oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type");
    }

    let correct = request.username.as_deref() == Some(USERNAME)
        && request.password.as_deref() == Some(PASSWORD);

    if !correct {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }

    Json(serde_json::json!({
        "access_token": "an-access-token",
        "token_type": "Bearer",
        "expires_in": 300,
    })).into_response()
}

fn oauth_error(status: StatusCode, error: &str) -> Response {
    (status, Json(serde_json::json!({ "error": error }))).into_response()
}
