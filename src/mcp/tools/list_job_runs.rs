//! `list_job_runs`: a page of job runs, newest first, as `job-run list --json` prints them.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::CRUD;
use crate::mcp::McpServer;
use crate::shared::job_run::parse_job_run_status;
use crate::toolkit::Toolkit;

use super::result::{error_result, success_json};

/// The default page, applied when the caller does not name one.
const DEFAULT_JOB_RUN_LIMIT: i64 = 20;

/// The most runs this tool will return in one call. A page this long is already more than
/// an agent reads in one turn; a history of thousands is only a context window spent.
const MAX_JOB_RUN_LIMIT: i64 = 200;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListJobRuns {
    /// Only runs of this job.
    pub job: Option<String>,
    /// Only runs with this status: pending, running, succeeded, failed, skipped, aborted,
    /// timedout or invalid.
    pub status: Option<String>,
    /// How many runs to show, newest first. Defaults to 20. A value above 200 shows 200,
    /// and one below 1 shows the default.
    pub limit: Option<i64>,
}

#[tool_router(router = list_job_runs_router, vis = "pub(super)")]
impl McpServer {

    /// List job runs, newest first.
    #[tool]
    async fn list_job_runs(&self, Parameters(args): Parameters<ListJobRuns>) -> CallToolResult {
        let status = match args.status.as_deref().map(parse_job_run_status) {
            Some(Ok(status)) => Some(status),
            Some(Err(message)) => return error_result(&anyhow::anyhow!(message)),
            None => None,
        };

        let limit = clamp_job_run_limit(args.limit);

        match list_job_runs_rows(&self.toolkit, args.job, status, limit).await {
            Ok(job_runs) => success_json(job_runs),
            Err(err) => error_result(&err),
        }
    }
}

/// This tool's bound, beside `clamp_wait_seconds` and `clamp_stream_bytes`: every argument
/// a caller can name that decides how much comes back has a ceiling.
///
/// Anything below 1 means the default rather than "everything". `limit` is bound straight
/// into SQL `LIMIT ?`, and SQLite reads a negative LIMIT as no limit at all - so `-1` asked
/// through this tool returned the whole run history, the opposite of what a smaller number
/// asks for.
fn clamp_job_run_limit(limit: Option<i64>) -> i64 {
    match limit {
        Some(limit) if limit > 0 => limit.min(MAX_JOB_RUN_LIMIT),
        _ => DEFAULT_JOB_RUN_LIMIT,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The same `deny_unknown_fields` rule the module doc states for every argument struct,
    /// pinned on a read tool too - so it is a property of all five, not only of the one
    /// that writes.
    #[test]
    fn a_misspelled_argument_key_on_a_read_tool_is_refused_too() {
        let error = serde_json::from_value::<ListJobRuns>(serde_json::json!({
            "limmit": 5,
        })).unwrap_err().to_string();

        assert!(error.contains("limmit"), "{error}");
    }

    /// The bug this clamp exists for: `limit` is bound into SQL `LIMIT ?`, and SQLite reads
    /// a negative LIMIT as no limit at all, so `-5` returned the entire run history.
    #[test]
    fn a_negative_limit_means_the_default_not_everything() {
        assert_eq!(clamp_job_run_limit(Some(-5)), DEFAULT_JOB_RUN_LIMIT);
        assert_eq!(clamp_job_run_limit(Some(-1)), DEFAULT_JOB_RUN_LIMIT);
    }

    /// `0` is the other non-positive case, and SQLite would honour it literally - an empty
    /// page is not what a caller asking for "no limit in particular" means either.
    #[test]
    fn a_zero_limit_means_the_default() {
        assert_eq!(clamp_job_run_limit(Some(0)), DEFAULT_JOB_RUN_LIMIT);
    }

    #[test]
    fn an_absent_limit_means_the_default() {
        assert_eq!(clamp_job_run_limit(None), DEFAULT_JOB_RUN_LIMIT);
    }

    /// Clamped rather than refused, the same way `clamp_wait_seconds` clamps: a page of 200
    /// is still an answer in the shape the caller already handles.
    #[test]
    fn a_limit_above_the_cap_clamps_to_it() {
        assert_eq!(clamp_job_run_limit(Some(1_000_000)), MAX_JOB_RUN_LIMIT);
        assert_eq!(clamp_job_run_limit(Some(MAX_JOB_RUN_LIMIT + 1)), MAX_JOB_RUN_LIMIT);
    }

    #[test]
    fn a_limit_at_or_below_the_cap_is_used_as_given() {
        assert_eq!(clamp_job_run_limit(Some(MAX_JOB_RUN_LIMIT)), MAX_JOB_RUN_LIMIT);
        assert_eq!(clamp_job_run_limit(Some(1)), 1);
    }
}
