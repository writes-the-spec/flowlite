//! `--schedule-at` / `schedule_at`: reading a due time a caller asked for, and refusing the
//! one combination that cannot mean anything.

use chrono::{DateTime, Utc};

/// RFC3339, which is the one widely written timestamp format that carries its own offset —
/// so an instant means the same thing to the person typing it and to the row it lands on.
/// A bare "2026-09-15 09:00" is refused rather than guessed at.
pub fn parse_schedule_at(raw: &str) -> Result<DateTime<Utc>, String> {

    match DateTime::parse_from_rfc3339(raw) {
        Ok(parsed) => Ok(parsed.with_timezone(&Utc)),
        Err(e) => Err(format!(
            "'{}' is not an RFC3339 instant ({}). It needs a date, a time and an offset, \
             for example 2026-09-15T09:00:00Z or 2026-09-15T09:00:00+02:00.",
            raw,
            e,
        )),
    }
}

/// Waiting on a run that is not due yet can only time out: both waits poll for a finished
/// status, and the run will not even be released until its instant arrives. Refused with
/// the numbers in it, so the caller can see whether they meant the time or meant the wait.
pub fn refuse_waiting_for_a_future_run(
    scheduled_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {

    if scheduled_at <= now {
        return Ok(());
    }

    anyhow::bail!(
        "This run is not due until {}, so waiting for it would only time out. Submit it \
         without waiting, and read it back later.",
        scheduled_at.to_rfc3339(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_rfc3339_instant_parses_to_utc() {
        let parsed = parse_schedule_at("2026-09-15T09:00:00+02:00").unwrap();

        assert_eq!(parsed.to_rfc3339(), "2026-09-15T07:00:00+00:00");
    }

    /// The offset is the whole point of requiring RFC3339: "2026-09-15 09:00" names two
    /// different instants depending on who reads it, and a run submitted for the wrong one
    /// is not a failure anybody would spot.
    #[test]
    fn an_instant_with_no_offset_is_refused_and_says_what_is_wanted() {
        let error = parse_schedule_at("2026-09-15 09:00").unwrap_err();

        assert!(error.contains("2026-09-15T09:00:00Z"), "the error should show a usable example, got: {error}");
    }

    #[test]
    fn waiting_for_a_run_that_is_already_due_is_allowed() {
        let now = chrono::Utc::now();

        assert!(refuse_waiting_for_a_future_run(now - chrono::TimeDelta::seconds(1), now).is_ok());
    }

    /// Both waits poll for a run to reach a finished status, so waiting on one that has not
    /// even been released can only ever burn the whole timeout and then report a run that
    /// is perfectly healthy.
    #[test]
    fn waiting_for_a_run_that_is_not_due_yet_is_refused() {
        let now = chrono::Utc::now();

        let error = refuse_waiting_for_a_future_run(now + chrono::TimeDelta::hours(1), now)
            .unwrap_err()
            .to_string();

        assert!(error.contains("not due"), "got: {error}");
    }
}
