use std::net::SocketAddr;
use std::sync::Arc;
use sqlx::SqlitePool;
use crate::declared::Declared;
use crate::signals::Signals;
use crate::toolkit::Toolkit;
use crate::auth::{IdpClient, SessionStore};
use crate::router::app::session_cookie::SessionCookie;


#[derive(Clone)]
pub struct AppState {
    pub toolkit: Arc<Toolkit>,
    pub conn_pool: Arc<SqlitePool>,
    /// The jobs and schedules `serve` loaded at startup. The dashboard shows these, not
    /// whatever the YAML says now.
    pub declared: Arc<Declared>,
    pub signals: Arc<Signals>,
    /// None when flowlite.toml has no `[auth]`, and the dashboard has no login.
    pub auth: Option<Arc<DashboardAuth>>,
    /// Bound to loopback with no `[auth]`, where `loopback_host_middleware` refuses a
    /// request naming any other host.
    pub host_must_be_loopback: bool,
}


impl AppState {

    /// `bound` is the address actually bound: its port names the session cookie, and its
    /// address decides whether the `Host` check applies.
    pub fn new(
        toolkit: Arc<Toolkit>,
        conn_pool: Arc<SqlitePool>,
        declared: Arc<Declared>,
        signals: Arc<Signals>,
        bound: SocketAddr,
    ) -> Self {

        let auth = toolkit.app_config.auth.clone().map(|auth| {
            Arc::new(DashboardAuth {
                cookie: SessionCookie::new(bound.port(), auth.cookie_secure),
                sessions: SessionStore::new(auth.session_length()),
                idp: IdpClient::new(auth),
            })
        });

        let host_must_be_loopback = bound.ip().is_loopback() && auth.is_none();

        Self {
            toolkit,
            conn_pool,
            declared,
            signals,
            auth,
            host_must_be_loopback,
        }
    }

}

/// What the dashboard's login needs: who to ask, who is signed in, and where the session
/// travels.
pub struct DashboardAuth {
    pub idp: IdpClient,
    pub sessions: SessionStore,
    pub cookie: SessionCookie,
}
