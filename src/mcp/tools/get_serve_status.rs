//! `get_serve_status`: whether this data directory is being served, and by what.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::mcp::McpServer;
use crate::serve_state::status;
use crate::shared::serve_status::status_json;

use super::result::{error_result, success_json};

/// No fields: the directory is the one `-D` named when this server was started, as it is
/// for every other tool.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetServeStatus {
}

#[tool_router(router = get_serve_status_router, vis = "pub(super)")]
impl McpServer {

    // The description names `pending` because that is the symptom a caller arrives with:
    // `submit_job` warns once, at the moment it writes, and this is how the agent checks
    // for itself any time after.
    /// Whether a flowlite server is running against this data directory, and on what
    /// address, port and pid. Nothing moves a run along without one, so a run stuck at
    /// pending is what this answers for.
    #[tool]
    async fn get_serve_status(&self, Parameters(_args): Parameters<GetServeStatus>) -> CallToolResult {
        match status(std::path::Path::new(&self.toolkit.app_config.data_dir)) {
            Ok(state) => success_json(status_json(&state)),
            Err(err) => error_result(&err),
        }
    }
}
