use axum::{
    extract::Request,
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Refuses a state-changing request that a different site caused the browser to make.
///
/// The dashboard has no auth and binds to localhost, which is the product's position, not
/// an oversight. That is fine while every route is a read: a page on another origin can
/// cause a request but cannot read the response. It stops being fine once a POST starts
/// work - a form post is a *simple* request, so it is sent with no preflight, and any page
/// the operator happens to be visiting could otherwise submit a job run on this machine
/// with parameter values of its choosing.
///
/// The check is the cheap one that fits a server with no session to protect:
///
/// - `Sec-Fetch-Site` decides it when present. Browsers set it on every request and script
///   cannot forge it. `same-origin` is the page's own form; `none` is a directly entered
///   URL. Anything else - `cross-site`, `same-site` - is another origin driving us.
/// - Otherwise `Origin` is compared against `Host`, for browsers too old to send the first.
/// - A request with neither is allowed through: that is `curl`, a script, or a health
///   check, none of which is a confused deputy. This guards against a browser being used
///   as one, not against someone who can already reach the port.
fn is_allowed(method: &Method, headers: &HeaderMap) -> bool {

    if !matches!(method, &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE) {
        return true;
    }

    match headers.get("sec-fetch-site").and_then(|value| value.to_str().ok()) {
        Some(site) => site == "same-origin" || site == "none",
        None => match headers.get("origin").and_then(|value| value.to_str().ok()) {
            // Origin is a full URL and Host is bare authority, so compare what they share.
            Some(origin) => match headers.get("host").and_then(|value| value.to_str().ok()) {
                Some(host) => origin
                    .split_once("://")
                    .is_some_and(|(_, authority)| authority == host),
                None => false,
            },
            None => true,
        },
    }
}

pub async fn same_origin_middleware(req: Request, next: Next) -> Response {

    if !is_allowed(req.method(), req.headers()) {
        eprintln!(
            "Refused a cross-origin {} {} - a page on another site tried to make this \
             server do something.",
            req.method(),
            req.uri().path(),
        );

        return (
            StatusCode::FORBIDDEN,
            "Refused: this request came from another site.",
        ).into_response();
    }

    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        headers
    }

    #[test]
    fn a_cross_site_post_is_refused() {
        assert!(!is_allowed(&Method::POST, &headers(&[("sec-fetch-site", "cross-site")])));
    }

    #[test]
    fn a_same_site_post_from_another_subdomain_is_refused() {
        assert!(!is_allowed(&Method::POST, &headers(&[("sec-fetch-site", "same-site")])));
    }

    #[test]
    fn the_pages_own_post_is_allowed() {
        assert!(is_allowed(&Method::POST, &headers(&[("sec-fetch-site", "same-origin")])));
    }

    /// `none` is a directly entered URL or a bookmark - the operator themselves.
    #[test]
    fn a_post_the_user_initiated_directly_is_allowed() {
        assert!(is_allowed(&Method::POST, &headers(&[("sec-fetch-site", "none")])));
    }

    /// A GET is a read: another origin can cause one but cannot see the answer, and
    /// refusing it would break an ordinary link into the dashboard.
    #[test]
    fn a_cross_site_get_is_allowed() {
        assert!(is_allowed(&Method::GET, &headers(&[("sec-fetch-site", "cross-site")])));
    }

    /// curl and scripts send neither header. They are not a browser being used as a
    /// confused deputy, and the CLI is a supported way to drive this server.
    #[test]
    fn a_post_with_no_browser_headers_is_allowed() {
        assert!(is_allowed(&Method::POST, &HeaderMap::new()));
    }

    #[test]
    fn an_origin_matching_host_is_allowed_without_sec_fetch_site() {
        assert!(is_allowed(&Method::POST, &headers(&[
            ("origin", "http://127.0.0.1:8000"),
            ("host", "127.0.0.1:8000"),
        ])));
    }

    #[test]
    fn an_origin_from_elsewhere_is_refused() {
        assert!(!is_allowed(&Method::POST, &headers(&[
            ("origin", "http://evil.example"),
            ("host", "127.0.0.1:8000"),
        ])));
    }

    /// A port is part of an origin: another server on this same machine is not us.
    #[test]
    fn an_origin_on_a_different_port_of_this_host_is_refused() {
        assert!(!is_allowed(&Method::POST, &headers(&[
            ("origin", "http://127.0.0.1:9999"),
            ("host", "127.0.0.1:8000"),
        ])));
    }
}
