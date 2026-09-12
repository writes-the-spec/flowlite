//! The nine tools, one file each: arguments in, the same CRUD a CLI command runs,
//! structures out.
//!
//! Each tool mirrors one CLI command's `--json` branch exactly - same filters, same sort,
//! same shape - so an agent reading a run through MCP and a person reading it through the
//! CLI read the same fields. Every call opens its own connection through a freshly named
//! `mem`, never `self.toolkit`'s own: `list_jobs` and `submit_job` seed it, and the other
//! four never read it at all, but taking a fresh name uniformly is one rule instead of
//! two, and it is what lets a file written into `jobs/` after this process started still
//! reach `list_jobs`, and what lets `submit_job` seed the same inline id twice in one
//! session without the second call colliding with the first's rows.
//!
//! Every argument struct carries `deny_unknown_fields`: a key the struct does not declare
//! is a typo or a guess, and serde's default is to ignore it silently. rmcp adds nothing
//! of its own to an arguments object (it deserializes the caller's `arguments` map
//! verbatim: `Parameters`' `FromContextPart` impl), so every key one of them sees really
//! is the caller's.
//!
//! `submit_job`, `get_job_run` and `stop_job_run` also take `wait_seconds`, bounded and
//! clamped by `crate::mcp::wait` - reused rather than copied three times, since the clamp
//! and the "wait nothing can service is refused" rule are each one concept the three calls
//! must not drift apart on.
//!
//! `result` holds what a tool hands back - the JSON, the wording of an error, the
//! unserved-directory warning - because more than one file needs each of those. Everything
//! else a tool needs, down to its own bounds and their tests, lives in that tool's own file.

use rmcp::handler::server::router::tool::ToolRouter;

use crate::mcp::McpServer;

mod get_job_run;
mod get_job_run_logs;
mod get_serve_status;
mod init_data_dir;
mod list_job_runs;
mod list_jobs;
mod list_limits;
mod result;
mod stop_job_run;
mod submit_job;

impl McpServer {

    /// The nine per-tool routers, added together - so a tool is added or removed by adding
    /// or removing a file and its line here. The order is the one a reader follows, not one
    /// a client sees: rmcp's `list_all` sorts tools by name before sending them.
    ///
    /// `pub(crate)` rather than private: `McpServer::new`, in the parent `mcp` module,
    /// composes this with `mod.rs`'s own router, and a private fn would be visible only to
    /// this module and its descendants.
    pub(crate) fn tools_router() -> ToolRouter<Self> {
        Self::list_jobs_router()
            + Self::submit_job_router()
            + Self::list_job_runs_router()
            + Self::get_job_run_router()
            + Self::get_job_run_logs_router()
            + Self::stop_job_run_router()
            + Self::init_data_dir_router()
            + Self::get_serve_status_router()
            + Self::list_limits_router()
    }
}
