//! `get_job_run`: one job run and the task runs under it, as `job-run get --json` prints
//! them, optionally after waiting for the run to settle.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::cli::commands::job::ensure_data_dir_is_served;
use crate::cli::commands::job_run::JobRunDetail;
use crate::crud::CRUD;
use crate::mcp::wait::{clamp_wait_seconds, wait_for_settled_job_run};
use crate::mcp::McpServer;
use crate::toolkit::Toolkit;

use super::result::{describe_unserved_data_dir, error_result, success_json};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetJobRun {
    /// The id of the job run to show.
    pub job_run_id: i64,
    /// Wait up to this many seconds for the run to finish before returning it. Absent or 0
    /// returns it at once. A value above 300 waits 300. If the wait runs out the run comes
    /// back unfinished rather than as an error.
    pub wait_seconds: Option<u64>,
}

#[tool_router(router = get_job_run_router, vis = "pub(super)")]
impl McpServer {

    /// Show one job run and the task runs under it.
    #[tool]
    async fn get_job_run(&self, Parameters(args): Parameters<GetJobRun>) -> CallToolResult {
        match get_job_run_detail(&self.toolkit, args.job_run_id, args.wait_seconds).await {
            Ok(detail) => success_json(detail),
            Err(err) => error_result(&err),
        }
    }
}

/// `get_job_run`'s own connection, and the same cross-entity assembly `job-run get` reads
/// through `CRUD::select_job_run_with_task_runs`. A wait, if any, is spent on the run
/// alone (`wait_for_settled_job_run` knows nothing of task runs) and the detail is
/// assembled fresh afterwards either way, so a `wait_seconds` of `0` costs nothing beyond
/// the one query `job-run get` already runs.
async fn get_job_run_detail(
    toolkit: &Toolkit,
    job_run_id: i64,
    wait_seconds: Option<u64>,
) -> anyhow::Result<JobRunDetail> {
    let wait_seconds = clamp_wait_seconds(wait_seconds);

    // Before the wait, for the same reason `job-run stop --wait` checks first: nothing but
    // the serve process settles this row, so waiting on a directory nothing serves is a
    // silent hang.
    if wait_seconds > 0 {
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)
            .map_err(describe_unserved_data_dir)?;
    }

    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    if wait_seconds > 0 {
        wait_for_settled_job_run(&crud, &mut conn, job_run_id, wait_seconds).await?;
    }

    let (job_run, task_runs) = crud.select_job_run_with_task_runs(&mut conn, job_run_id).await?;

    Ok(JobRunDetail { job_run, task_runs })
}
