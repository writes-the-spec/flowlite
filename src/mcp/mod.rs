use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
use rmcp::{tool_handler, tool_router, ServerHandler};

use crate::toolkit::Toolkit;

/// flowlite as a set of tools an agent calls, over JSON-RPC on stdin and stdout.
///
/// It holds an `Arc<Toolkit>` and nothing mutable, so rmcp is free to run calls
/// concurrently: each tool will open its own connection rather than share a pool.
pub struct McpServer {
    pub toolkit: Arc<Toolkit>,
    tool_router: ToolRouter<Self>,
}

/// `allow_empty` because no tool is registered yet. The attribute generates
/// `Self::tool_router()` from every `#[tool]` fn in this block, so a tool is added by
/// writing it here - and the attribute loses `allow_empty` when the first one arrives.
#[tool_router(allow_empty)]
impl McpServer {

    pub fn new(toolkit: Arc<Toolkit>) -> Self {
        Self {
            toolkit,
            tool_router: Self::tool_router(),
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
