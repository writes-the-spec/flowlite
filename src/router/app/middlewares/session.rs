use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};

use crate::auth::SessionCheck;
use crate::router::app::app_state::AppState;


/// Who a request is signed in as, for a page to show in its masthead. Only present when
/// `[auth]` is configured.
#[derive(Clone)]
pub struct SignedIn {
    pub subject: String,
}

/// Lets a request through only on a signed-in session, when `[auth]` is configured; with no
/// `[auth]` every request passes, as it always has.
pub async fn session_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {

    let Some(auth) = state.auth.clone() else {
        return next.run(req).await;
    };

    let check = match auth.cookie.read(req.headers()) {
        Some(session_id) => {
            auth.sessions.check(&session_id, &auth.idp, state.toolkit.get_current_ts()).await
        }
        None => SessionCheck::SignedOut,
    };

    let htmx_current_url = req.headers().get("hx-request").map(|_| {
        req.headers()
            .get("hx-current-url")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string()
    });

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|path_and_query| path_and_query.as_str())
        .unwrap_or("/")
        .to_string();

    match outcome(check, req.method(), &path_and_query, htmx_current_url.as_deref()) {
        Outcome::Pass(subject) => {
            req.extensions_mut().insert(SignedIn { subject });
            next.run(req).await
        }
        Outcome::RedirectToLogin(location) => Redirect::to(&location).into_response(),
        Outcome::HtmxRedirectToLogin(location) => (
            StatusCode::UNAUTHORIZED,
            [("hx-redirect", location)],
        ).into_response(),
        Outcome::IdpUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "The identity provider cannot be reached, so your session cannot be checked. \
             Try again in a moment.",
        ).into_response(),
    }
}


#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Pass(String),
    RedirectToLogin(String),
    /// htmx follows `HX-Redirect` before it looks at the status, so a partial's request
    /// moves the whole page to the login form rather than swapping the form into a table.
    HtmxRedirectToLogin(String),
    IdpUnavailable,
}

fn outcome(check: SessionCheck, method: &Method, path_and_query: &str, htmx_current_url: Option<&str>) -> Outcome {

    match check {
        SessionCheck::SignedIn(subject) => Outcome::Pass(subject),
        SessionCheck::IdpUnavailable => Outcome::IdpUnavailable,
        SessionCheck::SignedOut => match htmx_current_url {
            // Back to the page the partial sits on, not to the partial.
            Some(current_url) => Outcome::HtmxRedirectToLogin(login_location(path_of(current_url))),
            None if method == Method::GET => Outcome::RedirectToLogin(login_location(path_and_query)),
            // A form post cannot be replayed by a redirect.
            None => Outcome::RedirectToLogin(login_location("/")),
        },
    }
}

/// `next` if it is a path on this server, and the home page otherwise - so the login page
/// cannot send someone who just typed their password to a site of somebody else's choosing.
/// `//host` and `/\host` are paths only to this function; a browser reads both as a host.
pub fn safe_next(next: &str) -> &str {

    let is_local = next.starts_with('/')
        && !next.starts_with("//")
        && !next.starts_with("/\\");

    if is_local { next } else { "/" }
}

/// Where to send someone who has to log in first, coming back to `next` afterwards.
pub fn login_location(next: &str) -> String {

    let next = safe_next(next);

    if next == "/" {
        return "/login".to_string();
    }

    format!("/login?next={}", percent_encode(next))
}

/// The path and query of an absolute URL - `HX-Current-URL` is the full address bar.
fn path_of(url: &str) -> &str {

    let Some((_, after_scheme)) = url.split_once("://") else {
        return url;
    };

    match after_scheme.find('/') {
        Some(start) => &after_scheme[start..],
        None => "/",
    }
}

fn percent_encode(value: &str) -> String {

    let mut encoded = String::new();

    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }

    encoded
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_local_path_with_a_query_is_followed() {
        assert_eq!(safe_next("/job-runs/7?tab=tasks"), "/job-runs/7?tab=tasks");
    }

    /// The login page must not be a way to send someone who just typed their password to
    /// a site of an attacker's choosing.
    #[test]
    fn anything_that_leaves_this_server_falls_back_to_the_home_page() {
        assert_eq!(safe_next("https://evil.example"), "/");
        assert_eq!(safe_next("//evil.example"), "/");
        assert_eq!(safe_next("/\\evil.example"), "/");
        assert_eq!(safe_next("evil.example"), "/");
        assert_eq!(safe_next(""), "/");
    }

    #[test]
    fn the_login_location_carries_next_encoded() {
        assert_eq!(login_location("/jobs?q=a b&x=1"), "/login?next=%2Fjobs%3Fq%3Da%20b%26x%3D1");
    }

    #[test]
    fn the_login_location_for_the_home_page_carries_no_next() {
        assert_eq!(login_location("/"), "/login");
    }

    #[test]
    fn a_signed_in_request_passes_as_its_subject() {
        let outcome = outcome(SessionCheck::SignedIn("ada".to_string()), &Method::GET, "/jobs", None);

        assert_eq!(outcome, Outcome::Pass("ada".to_string()));
    }

    #[test]
    fn a_signed_out_page_load_is_sent_to_login_and_back() {
        let outcome = outcome(SessionCheck::SignedOut, &Method::GET, "/jobs?page=2", None);

        assert_eq!(outcome, Outcome::RedirectToLogin("/login?next=%2Fjobs%3Fpage%3D2".to_string()));
    }

    /// A form post cannot be replayed by a redirect, so it is not offered as `next`.
    #[test]
    fn a_signed_out_form_post_is_sent_to_login_without_next() {
        let outcome = outcome(SessionCheck::SignedOut, &Method::POST, "/job-runs/7/stop", None);

        assert_eq!(outcome, Outcome::RedirectToLogin("/login".to_string()));
    }

    /// A plain redirect would have htmx swap the login form into the run table, so the
    /// whole page is sent to log in - and back to the page, not to the partial.
    #[test]
    fn a_signed_out_htmx_partial_sends_the_whole_page_to_login() {
        let outcome = outcome(
            SessionCheck::SignedOut,
            &Method::GET,
            "/job-run-table?page=2",
            Some("https://flowlite.example/?page=2"),
        );

        assert_eq!(outcome, Outcome::HtmxRedirectToLogin("/login?next=%2F%3Fpage%3D2".to_string()));
    }

    #[test]
    fn a_signed_out_htmx_partial_with_no_current_url_goes_back_home() {
        let outcome = outcome(SessionCheck::SignedOut, &Method::GET, "/limits-panel", Some(""));

        assert_eq!(outcome, Outcome::HtmxRedirectToLogin("/login".to_string()));
    }

    #[test]
    fn an_unreachable_provider_is_neither_a_pass_nor_a_login() {
        let page = outcome(SessionCheck::IdpUnavailable, &Method::GET, "/jobs", None);
        let partial = outcome(SessionCheck::IdpUnavailable, &Method::GET, "/limits-panel", Some("http://h/"));

        assert_eq!(page, Outcome::IdpUnavailable);
        assert_eq!(partial, Outcome::IdpUnavailable);
    }
}
