use std::sync::Arc;

use clap::Args;
use rmcp::transport::stdio;
use rmcp::ServiceExt;

use crate::mcp::McpServer;
use crate::toolkit::Toolkit;

/// No flags of its own: the global `-D/--data-dir` already says which directory, the same
/// way every other command learns it.
#[derive(Args)]
pub struct McpCmd {
}


impl McpCmd {

    /// Nothing in this command - or in any tool it will serve - may print to stdout:
    /// stdout is the protocol stream, and a stray `println!` corrupts the JSON-RPC framing
    /// into a parse error that names nothing. Anything to say goes to stderr.
    pub async fn run(&self, toolkit: Toolkit) -> anyhow::Result<()> {
        let server = McpServer::new(Arc::new(toolkit));

        let running = server.serve(stdio()).await?;

        // Returns when the client closes stdin, which is how an MCP client stops a server
        // it spawned.
        running.waiting().await?;

        Ok(())
    }
}
