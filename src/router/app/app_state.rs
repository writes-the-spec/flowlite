use std::sync::Arc;
use sqlx::{SqlitePool, SqliteConnection};
use crate::signals::Signals;
use crate::toolkit::Toolkit;
use tokio::sync::Mutex;
use crate::auth::{IdpClient, SessionStore};
use crate::router::app::session_cookie::SessionCookie;


#[derive(Clone)]
pub struct AppState {
    pub toolkit: Arc<Toolkit>,
    pub conn_pool: Arc<SqlitePool>,
    pub signals: Arc<Signals>,
    /// None when config.toml has no `[auth]`, and the dashboard has no login.
    pub auth: Option<Arc<DashboardAuth>>,
    /// Never read, and held for exactly that reason: a `mode=memory` SQLite database
    /// exists only while something has it open, so dropping this would take `mem`'s schema
    /// and every seeded job with it while the pool went on attaching an empty one.
    #[allow(dead_code)]
    memory_conn: Arc<Mutex<SqliteConnection>>,
}


impl AppState {

    /// `port` is the one actually bound, which names the session cookie.
    pub fn new(
        toolkit: Arc<Toolkit>,
        conn_pool: Arc<SqlitePool>,
        memory_conn: Arc<Mutex<SqliteConnection>>,
        signals: Arc<Signals>,
        port: u16,
    ) -> Self {

        let auth = toolkit.app_config.auth.clone().map(|auth| {
            Arc::new(DashboardAuth {
                cookie: SessionCookie::new(port, auth.cookie_secure),
                idp: IdpClient::new(auth),
                sessions: SessionStore::new(),
            })
        });

        Self {
            toolkit,
            conn_pool,
            signals,
            auth,
            memory_conn,
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
