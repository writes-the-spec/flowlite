use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};

use crate::auth::idp_client::{IdpClient, IdpError, TokenGrant};


struct Session {
    subject: String,
    refresh_token: Option<String>,
    expires_at: DateTime<Utc>,
}

/// What a request's session says about the person making it.
#[derive(Debug, PartialEq, Eq)]
pub enum SessionCheck {
    /// Signed in, as this subject.
    SignedIn(String),
    SignedOut,
    /// The session ran out and the provider could not be asked whether it may go on. It is
    /// kept, so the person is neither let in nor logged out by an outage.
    IdpUnavailable,
}

/// A session expired this long ago is dropped at the next login, rather than kept until a
/// restart. Sessions are small and one per login, so this only has to stop them piling up.
const PRUNE_AFTER_HOURS: i64 = 24;

/// The dashboard's signed-in sessions, by the id their cookie carries. In memory only: a
/// restart signs everyone out, which costs a login and needs no table.
pub struct SessionStore {
    /// Each session has a lock of its own, held across a refresh. The dashboard's partials
    /// poll beside the page, so two requests on one expired session are routine, and a
    /// provider that rotates refresh tokens answers the second of two racing refreshes by
    /// ending the session.
    sessions: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Session>>>>,
}


impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {

    pub fn new() -> Self {
        Self { sessions: std::sync::Mutex::new(HashMap::new()) }
    }

    /// Begins a session for what a login was granted, and returns its id. Always a new id,
    /// never one the browser already had, so a session id planted before the login is not
    /// the one that ends up signed in.
    pub fn start(&self, subject: String, grant: TokenGrant, now: DateTime<Utc>) -> String {

        let id = uuid::Uuid::new_v4().simple().to_string();

        let session = Session {
            subject,
            refresh_token: grant.refresh_token,
            expires_at: grant.expires_at,
        };

        let mut sessions = self.sessions.lock().unwrap();

        let prune_before = now - TimeDelta::hours(PRUNE_AFTER_HOURS);

        // A session locked right now is one a request is using, so it is not stale.
        sessions.retain(|_, session| match session.try_lock() {
            Ok(session) => session.expires_at > prune_before,
            Err(_) => true,
        });

        sessions.insert(id.clone(), Arc::new(tokio::sync::Mutex::new(session)));

        id
    }

    pub fn end(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
    }

    /// Whether `id` is a signed-in session at `now`, refreshing it with the provider first
    /// if its grant has run out.
    pub async fn check(&self, id: &str, idp: &IdpClient, now: DateTime<Utc>) -> SessionCheck {

        let Some(session) = self.sessions.lock().unwrap().get(id).cloned() else {
            return SessionCheck::SignedOut;
        };

        let mut session = session.lock().await;

        if now < session.expires_at {
            return SessionCheck::SignedIn(session.subject.clone());
        }

        let Some(refresh_token) = session.refresh_token.clone() else {
            self.end(id);
            return SessionCheck::SignedOut;
        };

        match idp.refresh(&refresh_token, now).await {
            Ok(grant) => {
                session.expires_at = grant.expires_at;

                // A provider may answer a refresh without a new refresh token, and the
                // one already held is then still the one to use.
                if grant.refresh_token.is_some() {
                    session.refresh_token = grant.refresh_token;
                }

                SessionCheck::SignedIn(session.subject.clone())
            }
            Err(IdpError::Rejected(reason)) => {
                eprintln!("Signed {} out: the identity provider refused to refresh the session ({reason}).", session.subject);

                // Cleared as well as removed, so a request already waiting on this lock
                // finds nothing to refresh rather than asking the provider a second time.
                session.refresh_token = None;
                self.end(id);

                SessionCheck::SignedOut
            }
            Err(error @ IdpError::Unavailable(_)) => {
                eprintln!("Could not refresh {}'s session: {error}.", session.subject);

                SessionCheck::IdpUnavailable
            }
        }
    }

}
