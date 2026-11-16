//! The dashboard's login against an identity provider: what `IdpClient` makes of each way a
//! token endpoint can answer.
//!
//! Here rather than inline because the provider is `FakeIdp`, a real HTTP server shared
//! with `dashboard_auth`, which drives the same provider through the built binary - one
//! fake for both, so the two cannot drift apart about what a token endpoint says.

use std::time::Duration;

use flowlite::app_config::{AppConfigAuth, AuthFlow};
use flowlite::auth::{IdpClient, IdpError};

mod common;
use common::fake_idp::{Answer, FakeIdp, CLIENT_ID, CLIENT_SECRET, PASSWORD, USERNAME};

fn auth_config(fake: &FakeIdp) -> AppConfigAuth {
    AppConfigAuth {
        flow: AuthFlow::Password,
        token_url: fake.token_url.clone(),
        client_id: CLIENT_ID.to_string(),
        client_secret: Some(CLIENT_SECRET.to_string()),
        scope: "openid".to_string(),
        timeout_seconds: 1,
        session_seconds: 3600,
        cookie_secure: false,
    }
}


#[tokio::test]
async fn a_correct_password_is_granted() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));

    let result = idp.password(USERNAME, PASSWORD).await;

    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn a_wrong_password_is_rejected() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));

    let result = idp.password(USERNAME, "wrong").await;

    assert!(matches!(result, Err(IdpError::Rejected(_))), "{result:?}");
}

#[tokio::test]
async fn a_wrong_client_secret_is_rejected() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(AppConfigAuth {
        client_secret: Some("not-the-secret".to_string()),
        ..auth_config(&fake)
    });

    let result = idp.password(USERNAME, PASSWORD).await;

    assert!(matches!(result, Err(IdpError::Rejected(_))), "{result:?}");
}

/// Not `Rejected`: a provider that fell over has not said the password is wrong, and the
/// login page should not tell the person it was.
#[tokio::test]
async fn a_server_error_is_unavailable_rather_than_rejected() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    fake.answer(Answer::WithAServerError);

    let result = idp.password(USERNAME, PASSWORD).await;

    assert!(matches!(result, Err(IdpError::Unavailable(_))), "{result:?}");
}

/// A `token_url` pointed at something that answers 200 to anything must not turn every
/// password into a login.
#[tokio::test]
async fn a_200_that_is_not_a_token_response_is_never_a_login() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    fake.answer(Answer::WithABodyThatIsNotATokenResponse);

    let result = idp.password(USERNAME, PASSWORD).await;

    assert!(matches!(result, Err(IdpError::Unavailable(_))), "{result:?}");
}

#[tokio::test]
async fn a_provider_that_does_not_answer_in_time_is_unavailable() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    fake.answer(Answer::NeverInTime);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        idp.password(USERNAME, PASSWORD),
    ).await.expect("the client's own timeout should have ended the call");

    assert!(matches!(result, Err(IdpError::Unavailable(_))), "{result:?}");
}
