//! `list_jobs`: the jobs declared in the data directory, as `job list --json` prints them.

use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};

use crate::declared::Declared;
use crate::mcp::McpServer;

use super::result::{error_result, success_json};

#[tool_router(router = list_jobs_router, vis = "pub(super)")]
impl McpServer {

    /// List the jobs declared in the data directory.
    #[tool]
    async fn list_jobs(&self) -> CallToolResult {
        // Loaded per call, as `job list` loads it, so a job file written after this
        // process started is listed without a restart.
        match Declared::load(&self.toolkit.app_config) {
            Ok(declared) => success_json(declared.jobs),
            Err(err) => error_result(&err),
        }
    }
}
