use axum::http::HeaderMap;


/// The cookie a dashboard session travels in.
pub struct SessionCookie {
    name: String,
    secure: bool,
}


impl SessionCookie {

    /// Named after the port, because a browser keeps cookies by host and not by port: two
    /// `serve`s on one machine would otherwise each sign the other's sessions out.
    pub fn new(port: u16, secure: bool) -> Self {
        Self {
            name: format!("flowlite_session_{port}"),
            secure,
        }
    }

    /// The session id this request carries, if any.
    pub fn read(&self, headers: &HeaderMap) -> Option<String> {
        headers
            .get_all("cookie")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(';'))
            .filter_map(|pair| pair.trim().split_once('='))
            .find(|(name, _)| *name == self.name)
            .map(|(_, value)| value.to_string())
            .filter(|value| !value.is_empty())
    }

    /// The `Set-Cookie` value that hands the browser a session. No `Max-Age`, so it lasts
    /// as long as the browser does - the session's own lifetime is the provider's call.
    pub fn set(&self, session_id: &str) -> String {
        format!("{}={}; {}", self.name, session_id, self.attributes())
    }

    pub fn clear(&self) -> String {
        format!("{}=; Max-Age=0; {}", self.name, self.attributes())
    }

    /// `HttpOnly` so no script on the page can read the session, `SameSite=Lax` so another
    /// site's form post arrives without it.
    fn attributes(&self) -> &'static str {
        if self.secure {
            "Path=/; HttpOnly; SameSite=Lax; Secure"
        } else {
            "Path=/; HttpOnly; SameSite=Lax"
        }
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    fn with_cookie(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("cookie", value.parse().unwrap());
        headers
    }

    #[test]
    fn the_session_id_is_found_among_other_cookies() {
        let cookie = SessionCookie::new(8000, true);

        let headers = with_cookie("theme=dark; flowlite_session_8000=abc123; other=1");

        assert_eq!(cookie.read(&headers), Some("abc123".to_string()));
    }

    #[test]
    fn another_ports_session_is_not_this_ones() {
        let cookie = SessionCookie::new(8000, true);

        let headers = with_cookie("flowlite_session_8001=abc123");

        assert_eq!(cookie.read(&headers), None);
    }

    #[test]
    fn a_request_with_no_cookie_has_no_session() {
        let cookie = SessionCookie::new(8000, true);

        assert_eq!(cookie.read(&HeaderMap::new()), None);
    }

    /// What a browser sends back after `clear` until it drops the cookie.
    #[test]
    fn an_empty_session_cookie_is_no_session() {
        let cookie = SessionCookie::new(8000, true);

        assert_eq!(cookie.read(&with_cookie("flowlite_session_8000=")), None);
    }

    #[test]
    fn a_secure_cookie_is_http_only_lax_and_secure() {
        let cookie = SessionCookie::new(8000, true);

        assert_eq!(
            cookie.set("abc123"),
            "flowlite_session_8000=abc123; Path=/; HttpOnly; SameSite=Lax; Secure",
        );
    }

    #[test]
    fn cookie_secure_off_leaves_secure_out() {
        let cookie = SessionCookie::new(8000, false);

        assert_eq!(
            cookie.set("abc123"),
            "flowlite_session_8000=abc123; Path=/; HttpOnly; SameSite=Lax",
        );
    }

    #[test]
    fn clearing_the_cookie_expires_it_on_the_same_path() {
        let cookie = SessionCookie::new(8000, true);

        assert_eq!(
            cookie.clear(),
            "flowlite_session_8000=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax; Secure",
        );
    }
}
