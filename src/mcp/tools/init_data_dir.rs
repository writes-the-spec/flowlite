//! `init_data_dir`: lay the example job, schedule and config into this server's data
//! directory.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::mcp::McpServer;
use crate::shared::init::{scaffold, InitResult};

use super::result::{error_result, success_json};

/// No fields, and `deny_unknown_fields` is what makes that mean something: the directory is
/// the one `-D` named when this server was started, as it is for every other tool, so a
/// caller naming another path is refused rather than quietly scaffolding somewhere nobody
/// asked for.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InitDataDir {
}

#[tool_router(router = init_data_dir_router, vis = "pub(super)")]
impl McpServer {

    // The description is what a model chooses this tool by, so it says what lands and that
    // nothing is lost. Why it takes no argument is the struct's business, above, and would
    // only cost the caller tokens here.
    /// Create an example job, an example schedule and a commented config.toml in this
    /// server's data directory, so a new directory has something to run. Files already
    /// there are kept as they are, never overwritten.
    #[tool]
    async fn init_data_dir(&self, Parameters(_args): Parameters<InitDataDir>) -> CallToolResult {
        match init_result(&self.toolkit.app_config.data_dir) {
            Ok(result) => success_json(result),
            Err(err) => error_result(&err),
        }
    }
}

/// Takes no connection and seeds nothing, exactly as the command does: this writes the
/// configuration the other tools read, so it has to work in a directory that has no
/// database yet. The files it writes reach `list_jobs` on the very next call anyway, since
/// every call seeds a `mem` of its own.
fn init_result(data_dir: &str) -> anyhow::Result<InitResult> {
    let files = scaffold(std::path::Path::new(data_dir))?;

    Ok(InitResult { data_dir: data_dir.to_string(), files })
}
