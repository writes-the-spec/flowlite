//! `delete_job_run`: removes a run that has not run yet, freeing the occurrence it held.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::crud::job_run::JobRun;
use crate::crud::CRUD;
use crate::mcp::McpServer;
use crate::shared::job_run::{delete_job_run as delete_scheduled_job_run, deleted_occurrence, DeletedOccurrence};
use crate::toolkit::Toolkit;

use super::result::{error_result, job_run_result};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeleteJobRun {
    /// The id of the job run to delete. It must still be waiting for its time.
    pub job_run_id: i64,
}

#[tool_router(router = delete_job_run_router, vis = "pub(super)")]
impl McpServer {

    // The counterpart to `stop_job_run`, and the distinction is the whole reason this tool
    // exists: a stop keeps the occurrence for ever, a delete hands it back. Said in the
    // description too, since choosing between the two is what the caller is doing. No
    // `wait_seconds`: the delete settles the run in this process, so there is nothing to
    // wait for and nothing for an unserved directory to withhold.
    /// Delete a job run that is still scheduled, so that its schedule submits the
    /// occurrence again - under the job definition as the running server now reads it. Use
    /// this to replace an outstanding run that carries an edited job's old definition; use
    /// stop_job_run to call an occurrence off for good. Returns the deleted job run.
    #[tool]
    async fn delete_job_run(&self, Parameters(args): Parameters<DeleteJobRun>) -> CallToolResult {
        match delete_scheduled_run(&self.toolkit, args).await {
            Ok((job_run, warning)) => job_run_result(job_run, warning),
            Err(err) => error_result(&err),
        }
    }
}

/// `delete_job_run`'s own connection. Reads no config: which runs exist and what status
/// they hold is run history, and the schedule that writes the occurrence again reads its
/// own YAML in the serve process - the same reason `job-run delete` seeds none.
async fn delete_scheduled_run(
    toolkit: &Toolkit,
    args: DeleteJobRun,
) -> anyhow::Result<(JobRun, Option<String>)> {

    let toolkit = toolkit.with_fresh_mem();
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));

    let job_run = delete_scheduled_job_run(&crud, &mut conn, args.job_run_id).await?;

    let warning = occurrence_warning(&job_run, crud.toolkit.get_current_ts());

    Ok((job_run, warning))
}

/// `Some` wherever nothing will write the occurrence back, which is the fact the tool's own
/// description promises and the deleted row cannot show: an agent that deleted an ad-hoc
/// run to pick up an edited definition would otherwise wait for a run that never comes.
///
/// The unserved-directory warning the writing tools carry is not repeated here: this call
/// settles the run itself, so its result is true of the directory whether or not anything
/// is serving it.
fn occurrence_warning(job_run: &JobRun, now: chrono::DateTime<chrono::Utc>) -> Option<String> {
    match deleted_occurrence(job_run, now) {
        DeletedOccurrence::Resubmitted => None,
        DeletedOccurrence::NotScheduled => Some(format!(
            "This run was submitted by hand rather than by a schedule, so no occurrence is \
             freed and nothing writes it again. Call submit_job with job_id {} to run it.",
            job_run.job_id,
        )),
        DeletedOccurrence::AlreadyPassed => Some(format!(
            "This run's instant ({}) has already gone by, and the scheduler only fills \
             occurrences still ahead, so it will not be written again. Call submit_job with \
             job_id {} to run it now.",
            job_run.scheduled_at.to_rfc3339(),
            job_run.job_id,
        )),
    }
}
