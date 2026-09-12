//! `list_limits`: the global cap and every named concurrency limit, each with its current
//! use and its maximum.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::crud::CRUD;
use crate::mcp::McpServer;
use crate::shared::limits::{limit_rows, LimitRow};
use crate::toolkit::Toolkit;

use super::result::{error_result, success_json};

/// No fields: there is nothing to filter here. The rows are the whole answer, and there are
/// as many as `config.toml` declares limits, plus one.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListLimits {
}

#[tool_router(router = list_limits_router, vis = "pub(super)")]
impl McpServer {

    // The pair to `get_serve_status`: that one answers "is anything running this", this one
    // answers "what is it waiting behind". A caller reaches for them in that order.
    /// Why nothing is running: the cap on task attempts across every job, and each named
    /// concurrency limit, with how many attempts currently claim it and how many it
    /// allows. A max of 0 means no ceiling.
    #[tool]
    async fn list_limits(&self, Parameters(_args): Parameters<ListLimits>) -> CallToolResult {
        match limits_rows(&self.toolkit).await {
            Ok(rows) => success_json(rows),
            Err(err) => error_result(&err),
        }
    }
}

/// Reads `config.toml` for the maxima and the disk tables for the counts, which is all
/// `flowlite limits` reads too - so it answers for a directory whose server is down, and
/// neither seeds `mem` nor holds a memory connection, exactly as that command does not.
async fn limits_rows(toolkit: &Toolkit) -> anyhow::Result<Vec<LimitRow>> {
    let toolkit = toolkit.with_fresh_mem();

    let max_running_attempts = toolkit.app_config.orchestrator.max_running_attempts;
    let concurrency_limits = toolkit.app_config.concurrency_limits.clone();

    let mut conn = toolkit.get_conn().await?;
    let crud = CRUD::new(Arc::new(toolkit));

    let running_attempts = crud.count_running_attempts(&mut conn).await?;
    let claimed_limit_slots = crud.claimed_limit_slots(&mut conn).await?;

    Ok(limit_rows(
        max_running_attempts,
        running_attempts,
        &concurrency_limits,
        &claimed_limit_slots,
    ))
}
