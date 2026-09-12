//! What more than one frontend needs.
//!
//! `src/cli/`, `src/mcp/` and `src/router/` are three ways to ask the same questions of the
//! same two SQLite files, and none of them imports from another - see the `frontends` skill.
//! Whatever two of them would otherwise have borrowed lives here instead.
//!
//! Shared stops at the fact. A refusal raised here is a typed value, never a finished
//! sentence: the frontends do not agree on what the reader can do about it, so each words
//! its own remedy. `wait::DataDirNotServed` is the worked example.

pub mod format;
pub mod init;
pub mod job;
pub mod job_run;
pub mod limits;
pub mod wait;
