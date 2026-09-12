//! Operations that span more than one entity, and so belong to no single entity file.
//!
//! One file per operation, each taking `&mut SqliteConnection` rather than a generic
//! executor - a sequence of statements forming one logical operation needs *one*
//! connection. `job_run_definition` is the exception: it is not an operation but the
//! vocabulary `submit_job` and `rerun_job` share.

pub mod ad_hoc_job;
pub mod job_run_definition;
pub mod job_run_reads;
pub mod limits;
pub mod rerun_job;
pub mod secret_env;
pub mod submit_job;
