// `pub(crate)` rather than private: `src/mcp/tools/` imports `JobRunDetail`,
// `TaskRunAttemptLog` and `parse_job_run_status` from `commands::job_run`, which stay
// `pub(crate)` themselves rather than moving - two callers does not earn a new home.
pub(crate) mod commands;
mod cli;

pub use cli::Cli;