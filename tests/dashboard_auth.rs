//! The dashboard behind an identity provider's login: what a request without a session
//! gets, how a login and a logout land, and that a data directory with no `[auth]` serves
//! the dashboard exactly as before.
//!
//! Driven through the built binary, like `serve_lock`, because what is under test is the
//! order the server stacks its layers in - which requests reach the session check, and
//! which are refused before it. The provider is `FakeIdp`, shared with `idp_client`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::header::{HeaderMap, LOCATION, SET_COOKIE};
use reqwest::StatusCode;

mod common;
use common::fake_idp::{Answer, FakeIdp, CLIENT_ID, CLIENT_SECRET, PASSWORD, USERNAME};
use common::{is_up, serve, until, ServerGuard};

fn data_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-auth-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

    std::fs::write(
        dir.join("jobs").join("hello.yaml"),
        "id: hello\nname: Hello\ntasks:\n  - id: say\n    \
         description: Says hello\n    command: echo hello\n",
    ).unwrap();

    dir
}

fn with_auth(dir: &Path, fake: &FakeIdp) {
    std::fs::write(
        dir.join("flowlite.toml"),
        format!(
            "[auth]\nflow = \"password\"\ntoken_url = \"{}\"\n\
             client_id = \"{CLIENT_ID}\"\nclient_secret = \"{CLIENT_SECRET}\"\n\
             timeout_seconds = 2\ncookie_secure = false\n",
            fake.token_url,
        ),
    ).unwrap();
}

fn started(dir: &Path, port: u16) -> ServerGuard {
    let server = ServerGuard::new(serve(dir, port), libc::SIGTERM);
    assert!(until(Duration::from_secs(30), || is_up(dir)), "the server never came up");
    server
}

/// Never follows a redirect, so every test sees the 303 it is asserting on.
fn client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();

    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

fn location(headers: &HeaderMap) -> &str {
    headers.get(LOCATION).unwrap().to_str().unwrap()
}

/// The `name=value` part of the session cookie a response set.
fn session_cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get(SET_COOKIE)
        .map(|value| value.to_str().unwrap().split(';').next().unwrap().to_string())
}

async fn log_in(port: u16, password: &str, next: &str) -> reqwest::Response {
    client()
        .post(format!("http://127.0.0.1:{port}/login"))
        .form(&[("username", USERNAME), ("password", password), ("next", next)])
        .send()
        .await
        .unwrap()
}


#[tokio::test]
async fn a_page_without_a_session_is_sent_to_login_and_back() {
    let fake = FakeIdp::start();
    let dir = data_dir("no-session");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18301);

    let response = client().get("http://127.0.0.1:18301/jobs").send().await.unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(response.headers()), "/login?next=%2Fjobs");
}

/// The login page needs its stylesheet before anyone is signed in.
#[tokio::test]
async fn assets_are_served_without_a_session() {
    let fake = FakeIdp::start();
    let dir = data_dir("assets");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18302);

    let response = client().get("http://127.0.0.1:18302/assets/css/app.css").send().await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_wrong_password_is_refused_without_a_session() {
    let fake = FakeIdp::start();
    let dir = data_dir("wrong-password");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18303);

    let response = log_in(18303, "wrong", "/").await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(session_cookie(response.headers()), None);
    assert!(response.text().await.unwrap().contains("Wrong username or password"));
}

#[tokio::test]
async fn the_right_password_signs_in_and_returns_to_next() {
    let fake = FakeIdp::start();
    let dir = data_dir("signs-in");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18304);

    let response = log_in(18304, PASSWORD, "/jobs").await;

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(response.headers()), "/jobs");

    let cookie = session_cookie(response.headers()).expect("a session cookie");
    assert!(cookie.starts_with("flowlite_session_18304="), "{cookie}");

    let page = client()
        .get("http://127.0.0.1:18304/jobs")
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(page.status(), StatusCode::OK);
    assert!(page.text().await.unwrap().contains(USERNAME));
}

#[tokio::test]
async fn a_login_never_sends_anyone_off_this_server() {
    let fake = FakeIdp::start();
    let dir = data_dir("open-redirect");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18305);

    let response = log_in(18305, PASSWORD, "//evil.example/").await;

    assert_eq!(location(response.headers()), "/");
}

#[tokio::test]
async fn a_signed_out_htmx_partial_moves_the_whole_page_to_login() {
    let fake = FakeIdp::start();
    let dir = data_dir("htmx");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18306);

    let response = client()
        .get("http://127.0.0.1:18306/job-run-table")
        .header("hx-request", "true")
        .header("hx-current-url", "http://127.0.0.1:18306/?page=2")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers().get("hx-redirect").unwrap(), "/login?next=%2F%3Fpage%3D2");
}

/// The same-origin check sits outside the login route, so another site cannot sign the
/// operator's browser in as an account of its own choosing.
#[tokio::test]
async fn a_cross_site_login_is_refused() {
    let fake = FakeIdp::start();
    let dir = data_dir("cross-site");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18307);

    let response = client()
        .post("http://127.0.0.1:18307/login")
        .header("sec-fetch-site", "cross-site")
        .form(&[("username", USERNAME), ("password", PASSWORD), ("next", "/")])
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn logging_out_ends_the_session() {
    let fake = FakeIdp::start();
    let dir = data_dir("logout");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18308);

    let cookie = session_cookie(log_in(18308, PASSWORD, "/").await.headers()).unwrap();

    let logout = client()
        .post("http://127.0.0.1:18308/logout")
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(logout.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(logout.headers()), "/login");

    let afterwards = client()
        .get("http://127.0.0.1:18308/")
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(afterwards.status(), StatusCode::SEE_OTHER);
}

/// An outage is not "wrong password": the person should not go and reset a password that
/// was right.
#[tokio::test]
async fn a_login_while_the_provider_is_down_says_so() {
    let fake = FakeIdp::start();
    let dir = data_dir("provider-down");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18309);

    fake.answer(Answer::WithAServerError);
    let response = log_in(18309, PASSWORD, "/").await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(session_cookie(response.headers()), None);
    assert!(response.text().await.unwrap().contains("identity provider"));
}

#[tokio::test]
async fn without_an_auth_section_the_dashboard_has_no_login() {
    let dir = data_dir("no-auth");
    let _server = started(&dir, 18310);

    let home = client().get("http://127.0.0.1:18310/").send().await.unwrap();
    let login = client().get("http://127.0.0.1:18310/login").send().await.unwrap();

    assert_eq!(home.status(), StatusCode::OK);
    assert_eq!(login.status(), StatusCode::NOT_FOUND);
}
