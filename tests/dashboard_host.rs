//! Who the dashboard answers to when nothing asks who is there: on loopback with no
//! `[auth]`, only requests naming this machine, so a page that rebinds its own name to
//! 127.0.0.1 cannot read it; and a wider bind with no `[auth]` says so as it starts.
//!
//! Driven through the built binary, because what is under test is the bind and the layer
//! order together.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use reqwest::StatusCode;

mod common;
use common::fake_idp::{FakeIdp, CLIENT_ID, CLIENT_SECRET};
use common::{is_up, serve, until, ServerGuard, BINARY};

fn project_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("flowlite-host-{label}-{}", uuid::Uuid::new_v4()));

    std::fs::create_dir_all(dir.join("jobs")).unwrap();

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

/// The home page, sent to 127.0.0.1 but naming `host` - what a browser sends once a
/// rebinding page's name resolves here.
async fn home_status(port: u16, host: &str) -> StatusCode {
    let _ = rustls::crypto::ring::default_provider().install_default();

    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .get(format!("http://127.0.0.1:{port}/"))
        .header("host", host)
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn on_loopback_without_auth_a_request_naming_another_host_is_refused() {
    let dir = project_dir("rebind");
    let _server = started(&dir, 18401);

    assert_eq!(home_status(18401, "evil.example:18401").await, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn on_loopback_without_auth_the_names_of_this_machine_are_served() {
    let dir = project_dir("local");
    let _server = started(&dir, 18402);

    assert_eq!(home_status(18402, "127.0.0.1:18402").await, StatusCode::OK);
    assert_eq!(home_status(18402, "localhost:18402").await, StatusCode::OK);
}

/// With `[auth]` the login is the defence - a rebinding page has no session cookie - and a
/// proxy in front may send any name, so the name is not checked.
#[tokio::test]
async fn with_auth_another_host_reaches_the_login() {
    let fake = FakeIdp::start();
    let dir = project_dir("auth");
    with_auth(&dir, &fake);
    let _server = started(&dir, 18403);

    assert_eq!(home_status(18403, "flowlite.example").await, StatusCode::SEE_OTHER);
}

#[test]
fn a_wider_bind_without_auth_warns_as_it_starts() {
    let dir = project_dir("wide");

    let mut server = ServerGuard::new(
        Command::new(BINARY)
            .args(["--project-dir", &dir.to_string_lossy(), "serve", "--address", "0.0.0.0", "--port", "18404"])
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        libc::SIGTERM,
    );

    assert!(until(Duration::from_secs(30), || is_up(&dir)), "the server never came up");

    server.stop();

    let stderr = server.take_stderr();

    assert!(stderr.contains("no [auth] section"), "{stderr}");
}
