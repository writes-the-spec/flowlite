//! `validate_project_dir`: whether `serve` would start on this project directory's configuration.

use std::path::PathBuf;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::app_config::AppConfig;
use crate::mcp::McpServer;
use crate::shared::validate::validate;

use super::result::{error_result, success_json};

/// No fields: the directory is the one `-p` named when this server was started, as it is
/// for every other tool.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidateProjectDir {
}

#[tool_router(router = validate_project_dir_router, vis = "pub(super)")]
impl McpServer {

    // flowlite.toml is read again rather than taken from the toolkit, which holds it as it
    // was when this server started: the agent is asking about the file it just edited.
    /// Check this project directory's flowlite.toml, job and schedule YAML, and secret_env
    /// references the way `flowlite serve` does at startup. Returns the job and schedule
    /// counts, or the first error with the file it is in. Call it after editing any of
    /// them.
    #[tool]
    async fn validate_project_dir(&self, Parameters(_args): Parameters<ValidateProjectDir>) -> CallToolResult {

        let project_dir = PathBuf::from(&self.toolkit.app_config.project_dir);

        match AppConfig::load(Some(project_dir)).and_then(|app_config| validate(&app_config)) {
            Ok(result) => success_json(result),
            Err(err) => error_result(&err),
        }
    }
}
