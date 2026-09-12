//! `stop_job_run`: asks for a running job run to be stopped, optionally waiting for it to
//! settle.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::crud::job_run::JobRun;
use crate::crud::CRUD;
use crate::mcp::wait::{clamp_wait_seconds, wait_for_settled_job_run};
use crate::mcp::McpServer;
use crate::shared::job_run::stop_job_run as request_job_run_stop;
use crate::shared::wait::ensure_data_dir_is_served;
use crate::toolkit::Toolkit;

use super::result::{describe_unserved_data_dir, error_result, job_run_result, unserved_directory_warning};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StopJobRun {
    /// The id of the job run to stop.
    pub job_run_id: i64,
    /// Wait up to this many seconds for the run to settle before returning it. Absent or 0
    /// returns as soon as the stop has been requested. A value above 300 waits 300. If the
    /// wait runs out the run comes back unfinished rather than as an error.
    pub wait_seconds: Option<u64>,
}

#[tool_router(router = stop_job_run_router, vis = "pub(super)")]
impl McpServer {

    // Mirrors `JobRunStopCmd::run`, but always returns the `JobRun` row rather than the
    // CLI's `{job_run_id, stop_requested}` shape without a wait, so a caller reads
    // `.status` off the result either way. Kept out of the `///` above for the reason
    // `submit_job`'s rationale is: the model pays for that text on every turn.
    /// Ask for a running job run to be stopped. Returns the job run, whose status is
    /// settled only if `wait_seconds` was long enough; without one it is merely the run as
    /// it stands, with the stop requested.
    #[tool]
    async fn stop_job_run(&self, Parameters(args): Parameters<StopJobRun>) -> CallToolResult {
        match stop_job_run_and_wait(&self.toolkit, args).await {
            Ok((job_run, warning)) => job_run_result(job_run, warning),
            Err(err) => error_result(&err),
        }
    }
}

/// `stop_job_run`'s own connection. Writes the stop row before any wait, mirroring
/// `JobRunStopCmd::run`'s own ordering: the refusal below runs first so a wait nothing can
/// service changes nothing, but once the wait is allowed to proceed the stop is requested
/// regardless of whether `wait_seconds` is `0` - a caller that never asked to wait still
/// gets the stop queued.
///
/// Carries the same warning `submit_job_run` does, for the same reason: without a wait the
/// run comes back `pending` or `running`, and against an unserved directory that status will
/// never change, because only the serve process reads the stop row. `.status` is what this
/// tool's own contract points a caller at, so the silence was a misleading answer rather
/// than merely a missing one.
async fn stop_job_run_and_wait(
    toolkit: &Toolkit,
    args: StopJobRun,
) -> anyhow::Result<(JobRun, Option<String>)> {

    let wait_seconds = clamp_wait_seconds(args.wait_seconds);

    if wait_seconds > 0 {
        ensure_data_dir_is_served(&toolkit.app_config.data_dir)
            .map_err(describe_unserved_data_dir)?;
    }

    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    request_job_run_stop(&crud, &mut conn, args.job_run_id).await?;

    let job_run = wait_for_settled_job_run(&crud, &mut conn, args.job_run_id, wait_seconds).await?;

    // After the stop row is written, for the reason `submit_job_run` reads it after its own
    // write: by this point the stop is queued, and a `status` read failure here is a fact
    // about the warning, not about the stop.
    let warning = unserved_directory_warning(&crud.toolkit.app_config.data_dir);

    Ok((job_run, warning))
}
