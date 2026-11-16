use std::net::{Ipv4Addr, Ipv6Addr};
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use crate::router::app::app_state::AppState;

/// Refuses a request that names some other host, while the dashboard is on loopback with no
/// `[auth]`.
///
/// This is the defence against DNS rebinding. A page on `evil.example` can re-point that
/// name at `127.0.0.1` once it has loaded, and its script then reads this dashboard as its
/// own origin: logs, environments, parameters, and the POSTs that submit and stop runs.
/// `same_origin_middleware` cannot tell, since `Origin` and `Host` both say `evil.example`
/// and agree. But that is also the giveaway: on loopback, every request a person makes
/// names this machine, so a `Host` naming anything else was not typed by anyone here.
///
/// Off loopback, or with `[auth]`, it lets everything through. A wider bind is reached by
/// names this server cannot list, and a rebinding page has no session cookie to show.
pub async fn loopback_host_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {

    if !state.host_must_be_loopback {
        return next.run(req).await;
    }

    let host = req.headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| req.uri().authority().map(|authority| authority.to_string()));

    if !is_loopback_host(host.as_deref()) {
        eprintln!(
            "Refused {} {} addressed to host '{}': this dashboard is on loopback with no \
             [auth], and only answers to localhost, 127.0.0.1 or [::1].",
            req.method(),
            req.uri().path(),
            host.as_deref().unwrap_or_default(),
        );

        return (
            StatusCode::FORBIDDEN,
            "Refused: this dashboard only answers to localhost, 127.0.0.1 or [::1]. A proxy in \
             front of it has to send one of those as Host.",
        ).into_response();
    }

    next.run(req).await
}

/// Whether a `Host` value names this machine: `localhost`, a 127.x address or `[::1]`, on any
/// port. Any port, because an SSH tunnel moves the port and keeps the name; rebinding is the
/// other way round.
///
/// No `Host` at all is allowed: a browser always sends one, so a request without is `curl`
/// over HTTP/1.0 or something like it, not a page driving a browser.
fn is_loopback_host(host: Option<&str>) -> bool {

    let Some(host) = host else {
        return true;
    };

    if let Some(bracketed) = host.strip_prefix('[') {
        return bracketed
            .split_once(']')
            .and_then(|(address, _port)| address.parse::<Ipv6Addr>().ok())
            .is_some_and(|address| address.is_loopback());
    }

    let name = host.split_once(':').map_or(host, |(name, _port)| name);

    name.eq_ignore_ascii_case("localhost")
        || name.parse::<Ipv4Addr>().is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_names_of_this_machine_are_allowed() {
        assert!(is_loopback_host(Some("127.0.0.1:8000")));
        assert!(is_loopback_host(Some("localhost:8000")));
        assert!(is_loopback_host(Some("LOCALHOST:8000")));
        assert!(is_loopback_host(Some("[::1]:8000")));
        assert!(is_loopback_host(Some("127.0.0.1")));
        assert!(is_loopback_host(Some("[::1]")));
    }

    /// The whole of 127.0.0.0/8 is loopback, not only 127.0.0.1.
    #[test]
    fn any_ipv4_loopback_address_is_allowed() {
        assert!(is_loopback_host(Some("127.1.2.3:8000")));
    }

    /// An SSH tunnel keeps the name and moves the port.
    #[test]
    fn a_tunnelled_port_is_allowed() {
        assert!(is_loopback_host(Some("localhost:9999")));
    }

    /// What a rebinding page sends: its own name, now resolving here.
    #[test]
    fn another_name_is_refused() {
        assert!(!is_loopback_host(Some("evil.example:8000")));
        assert!(!is_loopback_host(Some("evil.example")));
    }

    /// A name that merely starts like this machine's is somebody else's.
    #[test]
    fn a_name_that_only_begins_like_localhost_is_refused() {
        assert!(!is_loopback_host(Some("localhost.evil.example:8000")));
        assert!(!is_loopback_host(Some("127.0.0.1.evil.example")));
    }

    #[test]
    fn an_address_that_is_not_loopback_is_refused() {
        assert!(!is_loopback_host(Some("192.168.1.10:8000")));
        assert!(!is_loopback_host(Some("[2001:db8::1]:8000")));
        assert!(!is_loopback_host(Some("[::1")));
    }

    #[test]
    fn a_request_with_no_host_is_allowed() {
        assert!(is_loopback_host(None));
    }
}
