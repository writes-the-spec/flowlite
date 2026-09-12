//! `get_task_output`: what each attempt of a job run wrote to stdout and stderr, as
//! `job-run logs --json` prints it, with each stream cut to a budget from the tail.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::cli::commands::job_run::TaskRunAttemptLog;
use crate::crud::task_run_attempt::TaskRunAttempt;
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::crud::CRUD;
use crate::mcp::McpServer;
use crate::toolkit::Toolkit;

use super::result::{error_result, success_json};

/// The default, applied per stream when the caller does not name one.
const DEFAULT_MAX_BYTES: usize = 20_000;

/// The most this tool will keep of one stream, whatever the caller asks for. Ten times the
/// default, the ratio `MAX_JOB_RUN_LIMIT` keeps to its own: a caller that wants more than
/// the default gets meaningfully more, and a command that spent an hour looping on a
/// warning still cannot spend a whole context window on it.
const MAX_STREAM_BYTES: usize = 200_000;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetTaskOutput {
    /// The id of the job run whose task output to read.
    pub job_run_id: i64,
    /// Only this task's attempts, by task id. Every task in the run otherwise.
    pub task: Option<String>,
    /// The most bytes to keep of each stream, counted from the end. Applied to stdout and
    /// stderr independently, on every attempt. Defaults to 20000, and 200000 is the most
    /// that will be kept however large a number is named.
    pub max_bytes: Option<usize>,
}

#[tool_router(router = get_task_output_router, vis = "pub(super)")]
impl McpServer {

    /// Show what each attempt of a job run wrote to stdout and stderr.
    #[tool]
    async fn get_task_output(&self, Parameters(args): Parameters<GetTaskOutput>) -> CallToolResult {
        let max_bytes = clamp_stream_bytes(args.max_bytes);

        match get_task_output_logs(&self.toolkit, args.job_run_id, args.task.as_deref(), max_bytes).await {
            Ok(logs) => success_json(logs),
            Err(err) => error_result(&err),
        }
    }
}

/// This tool's bound, and the one this pair was missing. Truncation is tail-biased and
/// defaults to 20000 a stream, so the tool cannot flood a context window on its own - but
/// the argument that sets that had no ceiling, and a caller naming a large enough number
/// undid the default by asking. The whole of a log some command looped on all night is the
/// case this exists for.
///
/// Anything below 1 means the default rather than "nothing". A stream cut to zero bytes is a
/// marker line and no output, which answers nothing an agent asked - and unlike a negative
/// `limit`, which SQLite read as no limit at all, it fails towards silence rather than
/// towards everything.
fn clamp_stream_bytes(max_bytes: Option<usize>) -> usize {
    match max_bytes {
        Some(max_bytes) if max_bytes > 0 => max_bytes.min(MAX_STREAM_BYTES),
        _ => DEFAULT_MAX_BYTES,
    }
}

/// Keeps at most `max_bytes` of `stream`, from the tail - the end of a log is where the
/// error is. A stream already within the limit passes through byte-identical, which is
/// what keeps an untruncated `get_task_output` stream identical to `job-run logs --json`.
///
/// The cut point is rounded up to the next char boundary, so a multi-byte UTF-8 character
/// is never split - which would otherwise panic when slicing.
fn truncate_tail(stream: &str, max_bytes: usize) -> String {
    if stream.len() <= max_bytes {
        return stream.to_string();
    }

    let cut_at = stream.len() - max_bytes;
    let tail_start = (cut_at..=stream.len())
        .find(|&i| stream.is_char_boundary(i))
        .unwrap_or(stream.len());

    format!("[truncated: {tail_start} earlier bytes dropped]\n{}", &stream[tail_start..])
}

/// `get_task_output`'s own connection, and the same cross-entity assembly `job-run logs`
/// reads through `CRUD::select_task_run_attempt_logs` - narrowed by `task_id` exactly as
/// `--task` narrows it, and with each stream truncated to `max_bytes` from the tail.
async fn get_task_output_logs(
    toolkit: &Toolkit,
    job_run_id: i64,
    task_id: Option<&str>,
    max_bytes: usize,
) -> anyhow::Result<Vec<TaskRunAttemptLog>> {
    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    let (task_run_attempts, mut task_run_attempt_output) =
        crud.select_task_run_attempt_logs(&mut conn, job_run_id, task_id).await?;

    Ok(task_run_attempts
        .into_iter()
        .map(|task_run_attempt| {
            let streams = task_run_attempt_output.remove(&task_run_attempt.id).unwrap_or_default();

            truncated_task_run_attempt_log(task_run_attempt, streams, max_bytes)
        })
        .collect())
}

/// One attempt's log, both streams truncated to `max_bytes` independently - pulled out of
/// `get_task_output_logs`'s mapping so a test can build a `TaskRunAttemptLog` the same way
/// the tool does, rather than only exercising `truncate_tail` on strings that were never
/// attached to a stream field.
fn truncated_task_run_attempt_log(
    task_run_attempt: TaskRunAttempt,
    streams: TaskRunAttemptOutputStreams,
    max_bytes: usize,
) -> TaskRunAttemptLog {
    TaskRunAttemptLog {
        task_run_attempt,
        stdout: truncate_tail(&streams.stdout, max_bytes),
        stderr: truncate_tail(&streams.stderr, max_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;

    /// The hole this clamp closes: the tail-biased default is what keeps a task's output
    /// from filling a context window, and before this a caller could undo it by naming a
    /// number larger than the log.
    #[test]
    fn a_stream_budget_above_the_cap_clamps_to_it() {
        assert_eq!(clamp_stream_bytes(Some(50_000_000)), MAX_STREAM_BYTES);
        assert_eq!(clamp_stream_bytes(Some(MAX_STREAM_BYTES + 1)), MAX_STREAM_BYTES);
    }

    #[test]
    fn a_stream_budget_at_or_below_the_cap_is_used_as_given() {
        assert_eq!(clamp_stream_bytes(Some(MAX_STREAM_BYTES)), MAX_STREAM_BYTES);
        assert_eq!(clamp_stream_bytes(Some(500)), 500);
    }

    /// Zero asked for a marker line and no output at all, which answers nothing - so it
    /// means the default, the way a non-positive `limit` does.
    #[test]
    fn a_zero_stream_budget_means_the_default() {
        assert_eq!(clamp_stream_bytes(Some(0)), DEFAULT_MAX_BYTES);
    }

    #[test]
    fn an_absent_stream_budget_means_the_default() {
        assert_eq!(clamp_stream_bytes(None), DEFAULT_MAX_BYTES);
    }

    #[test]
    fn a_stream_under_the_limit_is_returned_byte_identical() {
        assert_eq!(truncate_tail("hello", 20), "hello");
    }

    #[test]
    fn a_stream_at_exactly_the_limit_is_returned_byte_identical() {
        assert_eq!(truncate_tail("hello", 5), "hello");
    }

    #[test]
    fn a_stream_over_the_limit_keeps_the_tail_and_names_the_dropped_count() {
        let truncated = truncate_tail("0123456789", 4);

        assert_eq!(truncated, "[truncated: 6 earlier bytes dropped]\n6789");
    }

    /// The end of a log is where the error is, so the kept bytes are the last ones written,
    /// not the first.
    #[test]
    fn truncation_keeps_the_end_of_the_stream_not_the_start() {
        let truncated = truncate_tail("start-middle-end", 3);

        assert!(truncated.ends_with("end"), "{truncated}");
        assert!(!truncated.contains("start"), "{truncated}");
    }

    /// Splitting on a raw byte offset can land inside a multi-byte character, which would
    /// panic when the tail is sliced out - this keeps the character whole instead.
    #[test]
    fn truncation_never_splits_a_multi_byte_character() {
        let stream = "aé€"; // 1 + 2 + 3 = 6 bytes
        let truncated = truncate_tail(stream, 4);

        assert!(truncated.ends_with('€'), "{truncated}");
    }

    #[test]
    fn an_empty_stream_is_returned_byte_identical() {
        assert_eq!(truncate_tail("", 0), "");
    }

    /// A `TaskRunAttempt` with everything but the id filled with filler - what
    /// `truncated_task_run_attempt_log`'s tests build against, standing in for the row
    /// `select_task_run_attempts` would otherwise have to seed to produce.
    fn task_run_attempt_fixture() -> TaskRunAttempt {
        TaskRunAttempt {
            id: 1,
            task_run_id: 1,
            job_run_id: 1,
            job_id: "job".to_string(),
            task_id: "task".to_string(),
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
            attempt: 1,
            status: TaskRunAttemptStatus::Running,
            process_group_id: None,
        }
    }

    /// The property `stdout_and_stderr_are_truncated_independently` used to claim but not
    /// test: `get_task_output`'s real per-attempt mapping - not `truncate_tail` called
    /// twice on unrelated strings - truncates `stdout` and `stderr` each to `max_bytes`,
    /// on their own budget, and the fields land on the `TaskRunAttemptLog` the tool
    /// actually returns.
    #[test]
    fn stdout_and_stderr_are_truncated_independently() {
        let streams = TaskRunAttemptOutputStreams {
            stdout: "0123456789".to_string(),
            stderr: "short".to_string(),
        };

        let log = truncated_task_run_attempt_log(task_run_attempt_fixture(), streams, 4);

        assert_eq!(log.stdout, "[truncated: 6 earlier bytes dropped]\n6789");
        assert_eq!(log.stderr, "[truncated: 1 earlier bytes dropped]\nhort");
    }

    /// The other half of the same property: a stream within the limit is untouched even
    /// when the other stream on the same attempt is truncated - one stream being cut is
    /// not allowed to affect the other's own byte-identical-when-short guarantee.
    #[test]
    fn a_stream_within_the_limit_is_untouched_while_the_other_is_truncated() {
        let streams = TaskRunAttemptOutputStreams {
            stdout: "0123456789".to_string(),
            stderr: "ok".to_string(),
        };

        let log = truncated_task_run_attempt_log(task_run_attempt_fixture(), streams, 4);

        assert_eq!(log.stdout, "[truncated: 6 earlier bytes dropped]\n6789");
        assert_eq!(log.stderr, "ok");
    }
}
