//! The read-only tools: arguments in, the same CRUD a CLI command runs, structures out.
//!
//! Each tool mirrors one CLI command's `--json` branch exactly - same filters, same sort,
//! same shape - so an agent reading a run through MCP and a person reading it through the
//! CLI read the same fields. Every call opens its own connection through a freshly named
//! `mem`, never `self.toolkit`'s own: `list_jobs` seeds it, and the other three never read
//! it at all, but taking a fresh name uniformly is one rule instead of two, and it is what
//! lets a file written into `jobs/` after this process started still reach `list_jobs`.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};

use crate::cli::commands::job_run::{parse_job_run_status, JobRunDetail, TaskRunAttemptLog};
use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter};
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStreams;
use crate::crud::CRUD;
use crate::toolkit::Toolkit;

use super::McpServer;

/// `get_task_output`'s default, applied per stream when the caller does not name one.
const DEFAULT_MAX_BYTES: usize = 20_000;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListJobRuns {
    /// Only runs of this job.
    pub job: Option<String>,
    /// Only runs with this status: pending, running, succeeded, failed, skipped, aborted,
    /// timedout or invalid - the same words `job-run list --status` accepts.
    pub status: Option<String>,
    /// How many runs to show, newest first. Defaults to 20.
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetJobRun {
    /// The id of the job run to show.
    pub job_run_id: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetTaskOutput {
    /// The id of the job run whose task output to read.
    pub job_run_id: i64,
    /// Only this task's attempts, by task id. Every task in the run otherwise.
    pub task: Option<String>,
    /// The most bytes to keep of each stream, counted from the end. Applied to stdout and
    /// stderr independently, on every attempt. Defaults to 20000.
    pub max_bytes: Option<usize>,
}

// `vis = pub(crate)` because `McpServer::new`, in the parent `mcp` module, composes this
// router with `mod.rs`'s own - a private fn (the macro's default) is visible only to this
// module and its descendants, not to the parent that needs to call it.
#[tool_router(router = tools_router, vis = "pub(crate)")]
impl McpServer {

    /// List the jobs declared in the data directory.
    #[tool]
    async fn list_jobs(&self) -> CallToolResult {
        match list_jobs_rows(&self.toolkit).await {
            Ok(jobs) => success_json(jobs),
            Err(err) => error_result(&err),
        }
    }

    /// List job runs, newest first.
    #[tool]
    async fn list_job_runs(&self, Parameters(args): Parameters<ListJobRuns>) -> CallToolResult {
        let status = match args.status.as_deref().map(parse_job_run_status) {
            Some(Ok(status)) => Some(status),
            Some(Err(message)) => return error_result(&anyhow::anyhow!(message)),
            None => None,
        };

        let limit = args.limit.unwrap_or(20);

        match list_job_runs_rows(&self.toolkit, args.job, status, limit).await {
            Ok(job_runs) => success_json(job_runs),
            Err(err) => error_result(&err),
        }
    }

    /// Show one job run and the task runs under it.
    #[tool]
    async fn get_job_run(&self, Parameters(args): Parameters<GetJobRun>) -> CallToolResult {
        match get_job_run_detail(&self.toolkit, args.job_run_id).await {
            Ok(detail) => success_json(detail),
            Err(err) => error_result(&err),
        }
    }

    /// Show what each attempt of a job run wrote to stdout and stderr.
    #[tool]
    async fn get_task_output(&self, Parameters(args): Parameters<GetTaskOutput>) -> CallToolResult {
        let max_bytes = args.max_bytes.unwrap_or(DEFAULT_MAX_BYTES);

        match get_task_output_logs(&self.toolkit, args.job_run_id, args.task.as_deref(), max_bytes).await {
            Ok(logs) => success_json(logs),
            Err(err) => error_result(&err),
        }
    }

}

/// The tool result for a value whose JSON is already the fact in question: the same
/// pretty-printed text `--json` prints, as the text content every client can read, and the
/// identical value again as `structured_content` for a client that reads results as data
/// rather than text - carried in addition to, never instead of, the text.
///
/// The text is serialized directly from `value`, not from a `serde_json::Value` built from
/// it: `Value`'s map is a `BTreeMap`, so a detour through it would alphabetize field names
/// and stop being byte-identical to what `--json` prints, which serializes the struct
/// directly and so keeps declaration order.
fn success_json(value: impl Serialize) -> CallToolResult {
    let text = serde_json::to_string_pretty(&value)
        .expect("every tool result here is a plain data struct, always representable as JSON");
    let structured = serde_json::to_value(&value)
        .expect("every tool result here is a plain data struct, always representable as JSON");

    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    result.structured_content = Some(structured);
    result
}

/// A tool error carrying the anyhow chain verbatim. `{:#}` joins every `.context()` layer
/// into the one sentence a person at a terminal would read, which is exactly what the
/// model needs to fix its own file - a protocol error would hide this text from it.
fn error_result(err: &anyhow::Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(format!("{err:#}"))])
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

/// `list_jobs`'s own connection: a fresh `mem`, seeded exactly as `job list` seeds its own,
/// so a job file written after this process started is visible without a restart.
async fn list_jobs_rows(toolkit: &Toolkit) -> anyhow::Result<Vec<Job>> {
    let toolkit = toolkit.with_fresh_mem();
    let _memory_conn = toolkit.get_memory_conn().await?;
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));
    crud.init(&mut conn).await?;

    crud.select_jobs(&mut conn, &SelectJobsData {
        filter: SelectJobsDataFilter { job_id: None, name_like: None },
        sort: None,
        limit: None,
        offset: None,
    }).await
}

/// `list_job_runs`'s own connection. Reads the disk `job_run` table only, but still takes
/// a fresh `mem` name rather than `toolkit`'s own: one rule - every call gets a fresh name
/// - is easier to hold than a rule with an exception for the read-only tools.
async fn list_job_runs_rows(
    toolkit: &Toolkit,
    job: Option<String>,
    status: Option<JobRunStatus>,
    limit: i64,
) -> anyhow::Result<Vec<JobRun>> {
    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    crud.select_job_runs(&mut conn, &SelectJobRunsData {
        filter: SelectJobRunsDataFilter { id: None, job_id: job, status },
        sort: Some(SelectJobRunsDataSort::IdDesc),
        limit: Some(limit),
        offset: None,
    }).await
}

/// `get_job_run`'s own connection, and the same cross-entity assembly `job-run get` reads
/// through `CRUD::select_job_run_with_task_runs`.
async fn get_job_run_detail(toolkit: &Toolkit, job_run_id: i64) -> anyhow::Result<JobRunDetail> {
    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    let (job_run, task_runs) = crud.select_job_run_with_task_runs(&mut conn, job_run_id).await?;

    Ok(JobRunDetail { job_run, task_runs })
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
            let streams: TaskRunAttemptOutputStreams =
                task_run_attempt_output.remove(&task_run_attempt.id).unwrap_or_default();

            TaskRunAttemptLog {
                task_run_attempt,
                stdout: truncate_tail(&streams.stdout, max_bytes),
                stderr: truncate_tail(&streams.stderr, max_bytes),
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `success_json`'s text must serialize the value directly, not by way of a
    /// `serde_json::Value` (whose map is a `BTreeMap` and would alphabetize field names) -
    /// caught once already, this pins it: a struct declared out of alphabetical order keeps
    /// that order in the text content.
    #[test]
    fn success_json_text_keeps_field_declaration_order_rather_than_alphabetizing() {
        #[derive(Serialize)]
        struct OutOfAlphabeticalOrder {
            zebra: u8,
            apple: u8,
        }

        let result = success_json(OutOfAlphabeticalOrder { zebra: 1, apple: 2 });
        let text = result.content[0].as_text().unwrap().text.as_str();

        assert!(text.find("zebra").unwrap() < text.find("apple").unwrap(), "{text}");
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

    /// `get_task_output` truncates each stream on its own limit, so a long stdout does not
    /// eat into stderr's own budget or vice versa.
    #[test]
    fn stdout_and_stderr_are_truncated_independently() {
        let stdout = truncate_tail("0123456789", 4);
        let stderr = truncate_tail("short", 4);

        assert_eq!(stdout, "[truncated: 6 earlier bytes dropped]\n6789");
        assert_eq!(stderr, "[truncated: 1 earlier bytes dropped]\nhort");
    }
}
