//! The dashboard's login against an identity provider: what `IdpClient` makes of each way a
//! token endpoint can answer, and what `SessionStore` does with a session once its grant
//! runs out.
//!
//! Here rather than inline because the provider is `FakeIdp`, a real HTTP server shared
//! with `dashboard_auth`, which drives the same provider through the built binary - one
//! fake for both, so the two cannot drift apart about what a token endpoint says.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};

use flowlite::app_config::{AppConfigAuth, AuthFlow};
use flowlite::auth::{IdpClient, IdpError, SessionCheck, SessionStore, TokenGrant};

mod common;
use common::fake_idp::{Answer, FakeIdp, FakeIdpOptions, CLIENT_ID, CLIENT_SECRET, PASSWORD, USERNAME};

fn auth_config(fake: &FakeIdp) -> AppConfigAuth {
    AppConfigAuth {
        flow: AuthFlow::Password,
        token_url: fake.token_url.clone(),
        client_id: CLIENT_ID.to_string(),
        client_secret: Some(CLIENT_SECRET.to_string()),
        scope: "openid".to_string(),
        timeout_seconds: 1,
        cookie_secure: false,
    }
}

fn now() -> DateTime<Utc> {
    "2026-10-09T12:00:00Z".parse().unwrap()
}

fn an_hour_later() -> DateTime<Utc> {
    now() + TimeDelta::hours(1)
}

/// A session begun the way the login route begins one.
async fn signed_in(sessions: &SessionStore, idp: &IdpClient) -> String {
    let grant = idp.password(USERNAME, PASSWORD, now()).await.unwrap();
    sessions.start(USERNAME.to_string(), grant, now())
}


#[tokio::test]
async fn a_correct_password_is_granted_until_expires_in_runs_out() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));

    let grant = idp.password(USERNAME, PASSWORD, now()).await.unwrap();

    assert_eq!(grant.expires_at, now() + TimeDelta::seconds(300));
    assert!(grant.refresh_token.is_some());
}

#[tokio::test]
async fn a_wrong_password_is_rejected() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));

    let result = idp.password(USERNAME, "wrong", now()).await;

    assert!(matches!(result, Err(IdpError::Rejected(_))), "{result:?}");
}

#[tokio::test]
async fn a_wrong_client_secret_is_rejected() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(AppConfigAuth {
        client_secret: Some("not-the-secret".to_string()),
        ..auth_config(&fake)
    });

    let result = idp.password(USERNAME, PASSWORD, now()).await;

    assert!(matches!(result, Err(IdpError::Rejected(_))), "{result:?}");
}

/// Not `Rejected`: a provider that fell over has not said the password is wrong, and the
/// login page should not tell the person it was.
#[tokio::test]
async fn a_server_error_is_unavailable_rather_than_rejected() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    fake.answer(Answer::WithAServerError);

    let result = idp.password(USERNAME, PASSWORD, now()).await;

    assert!(matches!(result, Err(IdpError::Unavailable(_))), "{result:?}");
}

/// A `token_url` pointed at something that answers 200 to anything must not turn every
/// password into a login.
#[tokio::test]
async fn a_200_that_is_not_a_token_response_is_never_a_login() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    fake.answer(Answer::WithABodyThatIsNotATokenResponse);

    let result = idp.password(USERNAME, PASSWORD, now()).await;

    assert!(matches!(result, Err(IdpError::Unavailable(_))), "{result:?}");
}

#[tokio::test]
async fn a_provider_that_does_not_answer_in_time_is_unavailable() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    fake.answer(Answer::NeverInTime);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        idp.password(USERNAME, PASSWORD, now()),
    ).await.expect("the client's own timeout should have ended the call");

    assert!(matches!(result, Err(IdpError::Unavailable(_))), "{result:?}");
}

#[tokio::test]
async fn a_grant_without_expires_in_lasts_five_minutes() {
    let fake = FakeIdp::with(FakeIdpOptions { expires_in: None, ..FakeIdpOptions::default() });
    let idp = IdpClient::new(auth_config(&fake));

    let grant = idp.password(USERNAME, PASSWORD, now()).await.unwrap();

    assert_eq!(grant.expires_at, now() + TimeDelta::seconds(300));
}


#[tokio::test]
async fn a_session_is_signed_in_until_it_expires_without_asking_the_provider() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;

    let check = sessions.check(&id, &idp, now() + TimeDelta::seconds(299)).await;

    assert_eq!(check, SessionCheck::SignedIn(USERNAME.to_string()));
    assert_eq!(fake.refreshes(), 0);
}

#[tokio::test]
async fn an_expired_session_is_refreshed_and_stays_signed_in() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;

    let check = sessions.check(&id, &idp, an_hour_later()).await;

    assert_eq!(check, SessionCheck::SignedIn(USERNAME.to_string()));
    assert_eq!(fake.refreshes(), 1);
}

/// The spec lets a provider answer a refresh without a new refresh token, and the one the
/// session already holds is then still the one to use next time.
#[tokio::test]
async fn a_refresh_that_issues_no_new_refresh_token_keeps_the_old_one() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;

    let first = sessions.check(&id, &idp, an_hour_later()).await;
    let second = sessions.check(&id, &idp, an_hour_later() + TimeDelta::hours(1)).await;

    assert_eq!(first, SessionCheck::SignedIn(USERNAME.to_string()));
    assert_eq!(second, SessionCheck::SignedIn(USERNAME.to_string()));
    assert_eq!(fake.refreshes(), 2);
}

#[tokio::test]
async fn an_expired_session_the_provider_refuses_to_refresh_is_signed_out_for_good() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;
    fake.revoke_every_refresh_token();

    let refused = sessions.check(&id, &idp, an_hour_later()).await;
    let afterwards = sessions.check(&id, &idp, an_hour_later()).await;

    assert_eq!(refused, SessionCheck::SignedOut);
    assert_eq!(afterwards, SessionCheck::SignedOut);
    // Gone from the store, so the second check never reached the provider.
    assert_eq!(fake.refreshes(), 1);
}

/// An outage is not a verdict on the person: they are neither let in nor logged out, and
/// the session is usable again the moment the provider is.
#[tokio::test]
async fn an_expired_session_is_kept_but_not_served_while_the_provider_is_down() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;

    fake.answer(Answer::WithAServerError);
    let during = sessions.check(&id, &idp, an_hour_later()).await;

    fake.answer(Answer::Normally);
    let after = sessions.check(&id, &idp, an_hour_later()).await;

    assert_eq!(during, SessionCheck::IdpUnavailable);
    assert_eq!(after, SessionCheck::SignedIn(USERNAME.to_string()));
}

/// The dashboard's partials refresh every few seconds beside the page, so two requests on
/// one expired session are routine. Against a provider that rotates, the second refresh
/// would present a token the first had just retired, and be told the session is over.
#[tokio::test]
async fn two_concurrent_checks_of_an_expired_session_refresh_it_once() {
    let fake = FakeIdp::with(FakeIdpOptions {
        rotate_refresh_tokens: true,
        refresh_delay: Duration::from_millis(300),
        ..FakeIdpOptions::default()
    });
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;

    let (first, second) = tokio::join!(
        sessions.check(&id, &idp, an_hour_later()),
        sessions.check(&id, &idp, an_hour_later()),
    );

    assert_eq!(first, SessionCheck::SignedIn(USERNAME.to_string()));
    assert_eq!(second, SessionCheck::SignedIn(USERNAME.to_string()));
    assert_eq!(fake.refreshes(), 1);
    assert_eq!(fake.refreshes_with_a_retired_token(), 0);
}

#[tokio::test]
async fn an_expired_session_with_no_refresh_token_is_signed_out() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let grant = TokenGrant {
        refresh_token: None,
        expires_at: now() + TimeDelta::seconds(300),
    };
    let id = sessions.start(USERNAME.to_string(), grant, now());

    let check = sessions.check(&id, &idp, an_hour_later()).await;

    assert_eq!(check, SessionCheck::SignedOut);
    assert_eq!(fake.refreshes(), 0);
}

#[tokio::test]
async fn an_ended_session_is_signed_out() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let id = signed_in(&sessions, &idp).await;
    sessions.end(&id);

    assert_eq!(sessions.check(&id, &idp, now()).await, SessionCheck::SignedOut);
}

#[tokio::test]
async fn a_session_id_nobody_was_given_is_signed_out() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let check = sessions.check("made-up", &idp, now()).await;

    assert_eq!(check, SessionCheck::SignedOut);
}

#[tokio::test]
async fn every_login_gets_a_session_id_of_its_own() {
    let fake = FakeIdp::start();
    let idp = IdpClient::new(auth_config(&fake));
    let sessions = SessionStore::new();

    let first = signed_in(&sessions, &idp).await;
    let second = signed_in(&sessions, &idp).await;

    assert_ne!(first, second);
}
