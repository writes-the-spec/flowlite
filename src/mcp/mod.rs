use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
use rmcp::{tool_handler, tool_router, ServerHandler};

use crate::toolkit::Toolkit;

mod tools;

/// flowlite as a set of tools an agent calls, over JSON-RPC on stdin and stdout.
///
/// It holds an `Arc<Toolkit>` and nothing mutable, so rmcp is free to run calls
/// concurrently: each tool will open its own connection rather than share a pool.
///
/// `toolkit` is private rather than `pub`: only `tools.rs`, a child module, reads it now,
/// and a private field says so.
pub struct McpServer {
    toolkit: Arc<Toolkit>,
    tool_router: ToolRouter<Self>,
}

/// This block keeps `allow_empty` because it still declares no `#[tool]` fn of its own -
/// the four tools live in `tools.rs`'s own `#[tool_router(router = tools_router)]` block,
/// composed below. The attribute is a compile error on an empty block without it.
#[tool_router(allow_empty)]
impl McpServer {

    pub fn new(toolkit: Arc<Toolkit>) -> Self {
        Self {
            toolkit,
            tool_router: Self::tool_router() + Self::tools_router(),
        }
    }
}

/// `tool_handler` writes `list_tools`, `call_tool` and `get_tool` against that router. It
/// would write `get_info` too, but the name and version are worth saying here rather than
/// in an attribute: they are what a client shows the user when it lists its servers.
#[tool_handler(router = self.tool_router)]
impl ServerHandler for McpServer {

    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("flowlite", env!("CARGO_PKG_VERSION")))
    }
}
