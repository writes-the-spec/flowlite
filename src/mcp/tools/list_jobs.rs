//! `list_jobs`: the jobs declared in the data directory, as `job list --json` prints them.

use std::sync::Arc;

use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};

use crate::crud::job::{Job, SelectJobsData, SelectJobsDataFilter};
use crate::crud::CRUD;
use crate::mcp::McpServer;
use crate::toolkit::Toolkit;

use super::result::{error_result, success_json};

#[tool_router(router = list_jobs_router, vis = "pub(super)")]
impl McpServer {

    /// List the jobs declared in the data directory.
    #[tool]
    async fn list_jobs(&self) -> CallToolResult {
        match list_jobs_rows(&self.toolkit).await {
            Ok(jobs) => success_json(jobs),
            Err(err) => error_result(&err),
        }
    }
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
