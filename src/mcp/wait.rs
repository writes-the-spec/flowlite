//! The bounded wait shared by `submit_job`, `get_job_run` and `stop_job_run`.
//!
//! `wait_for_job_run` (`src/cli/commands/job.rs`) already polls a run at the orchestrator's
//! own interval until it finishes. This wraps that same loop in `tokio::time::timeout`
//! rather than rewriting it, so the polling loop itself gains no notion of a deadline. On
//! elapse the run is read once more and returned unfinished, never as an error: "still
//! running, here is the id" is an answer, and a timeout error would throw away the id the
//! agent needs to ask again.
//!
//! No transaction wraps the poll, on purpose: `src/toolkit.rs`'s `MIGRATION_LOCK` is held
//! only while a connection's own schema migration runs, never across a sleep, so a
//! five-minute wait here holds neither that mutex nor a SQLite write lock. Wrapping the poll
//! in a transaction would hold a write lock for the same five minutes, stalling every other
//! connection this process opens behind a busy timeout - not only this call's own.

use std::time::Duration;

use crate::cli::commands::job::{select_job_run, wait_for_job_run};
use crate::crud::job_run::JobRun;
use crate::crud::CRUD;

/// The most a caller may ask to wait. The client's own call timeout, not this server's, is
/// what a longer wait would actually run into - see the design's "Waiting, bounded".
const MAX_WAIT_SECONDS: u64 = 300;

/// Absent or `0` both mean "return at once". A value above `MAX_WAIT_SECONDS` clamps down
/// to it rather than being refused: a clamped wait still returns a run in the one shape a
/// caller already handles - merely unfinished - so refusing would cost a turn to say
/// nothing the answer does not already say.
pub(crate) fn clamp_wait_seconds(wait_seconds: Option<u64>) -> u64 {
    wait_seconds.unwrap_or(0).min(MAX_WAIT_SECONDS)
}

/// Waits for `job_run_id` to settle, bounded by `wait_seconds`, and returns it either way:
/// settled if the bound was long enough, merely unfinished if it elapsed first. `0` skips
/// polling and reads the run once, exactly as a call with no wait at all would.
///
/// If the client cancels the call mid-wait, rmcp drops this future and the run carries on
/// underneath it, exactly as Ctrl-C under `--wait` does - there is nothing to clean up here.
pub(crate) async fn wait_for_settled_job_run(
    crud: &CRUD,
    conn: &mut sqlx::SqliteConnection,
    job_run_id: i64,
    wait_seconds: u64,
) -> anyhow::Result<JobRun> {

    if wait_seconds == 0 {
        return select_job_run(crud, conn, job_run_id).await;
    }

    let poll_interval = crud.toolkit.app_config.orchestrator.poll_interval();
    let bound = Duration::from_secs(wait_seconds);

    match tokio::time::timeout(bound, wait_for_job_run(crud, conn, job_run_id, poll_interval)).await {
        Ok(result) => result,
        Err(_elapsed) => select_job_run(crud, conn, job_run_id).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_wait_seconds_means_no_wait() {
        assert_eq!(clamp_wait_seconds(None), 0);
    }

    #[test]
    fn a_zero_wait_seconds_means_no_wait() {
        assert_eq!(clamp_wait_seconds(Some(0)), 0);
    }

    #[test]
    fn a_value_above_the_cap_clamps_to_it() {
        assert_eq!(clamp_wait_seconds(Some(301)), MAX_WAIT_SECONDS);
        assert_eq!(clamp_wait_seconds(Some(10_000)), MAX_WAIT_SECONDS);
    }

    #[test]
    fn a_value_at_or_below_the_cap_is_used_as_given() {
        assert_eq!(clamp_wait_seconds(Some(MAX_WAIT_SECONDS)), MAX_WAIT_SECONDS);
        assert_eq!(clamp_wait_seconds(Some(5)), 5);
    }
}
