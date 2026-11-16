use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};


struct Session {
    subject: String,
    expires_at: DateTime<Utc>,
}

/// The dashboard's signed-in sessions, by the id their cookie carries. In memory only: a
/// restart signs everyone out, which costs a login and needs no table.
pub struct SessionStore {
    length: TimeDelta,
    sessions: Mutex<HashMap<String, Session>>,
}


impl SessionStore {

    /// Every session lasts `length` from its login.
    pub fn new(length: Duration) -> Self {
        Self {
            length: TimeDelta::from_std(length).expect("a session length that fits in a TimeDelta"),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Begins a session for `subject`, and returns its id. Always a new id, never one the
    /// browser already had, so a session id planted before the login is not the one that
    /// ends up signed in.
    pub fn start(&self, subject: String, now: DateTime<Utc>) -> String {

        let id = uuid::Uuid::new_v4().simple().to_string();

        let session = Session {
            subject,
            expires_at: now + self.length,
        };

        let mut sessions = self.sessions.lock().unwrap();

        // Dropped here rather than on a timer: logins are what add sessions, so this is
        // enough to stop expired ones piling up.
        sessions.retain(|_, session| session.expires_at > now);

        sessions.insert(id.clone(), session);

        id
    }

    pub fn end(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
    }

    /// Who `id` is signed in as at `now`, or None if it is not a session or has run out.
    pub fn check(&self, id: &str, now: DateTime<Utc>) -> Option<String> {

        let mut sessions = self.sessions.lock().unwrap();

        let session = sessions.get(id)?;

        if now >= session.expires_at {
            sessions.remove(id);
            return None;
        }

        Some(session.subject.clone())
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-10-09T12:00:00Z".parse().unwrap()
    }

    fn an_hour_long() -> SessionStore {
        SessionStore::new(Duration::from_secs(3600))
    }

    #[test]
    fn a_session_is_signed_in_until_its_length_runs_out() {
        let sessions = an_hour_long();

        let id = sessions.start("ada".to_string(), now());

        let before = sessions.check(&id, now() + TimeDelta::seconds(3599));
        let after = sessions.check(&id, now() + TimeDelta::seconds(3600));

        assert_eq!(before, Some("ada".to_string()));
        assert_eq!(after, None);
    }

    #[test]
    fn an_ended_session_is_signed_out() {
        let sessions = an_hour_long();

        let id = sessions.start("ada".to_string(), now());
        sessions.end(&id);

        assert_eq!(sessions.check(&id, now()), None);
    }

    #[test]
    fn a_session_id_nobody_was_given_is_signed_out() {
        let sessions = an_hour_long();

        assert_eq!(sessions.check("made-up", now()), None);
    }

    #[test]
    fn every_login_gets_a_session_id_of_its_own() {
        let sessions = an_hour_long();

        let first = sessions.start("ada".to_string(), now());
        let second = sessions.start("ada".to_string(), now());

        assert_ne!(first, second);
    }

    #[test]
    fn a_login_drops_the_sessions_that_have_run_out() {
        let sessions = an_hour_long();

        let expired = sessions.start("ada".to_string(), now());
        let current = sessions.start("bob".to_string(), now() + TimeDelta::minutes(30));

        sessions.start("eve".to_string(), now() + TimeDelta::hours(1));

        let remaining = sessions.sessions.lock().unwrap();

        assert!(!remaining.contains_key(&expired));
        assert!(remaining.contains_key(&current));
    }
}
