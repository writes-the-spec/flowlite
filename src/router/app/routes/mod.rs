/// How often a page watching something live asks for itself again.
pub const REFRESH_INTERVAL_SECONDS: u32 = 3;

pub mod home;
pub mod job;
pub mod job_dag;
pub mod job_run;
pub mod job_run_table;
pub mod jobs;
pub mod limits_panel;
pub mod login;
pub mod logout;
pub mod schedule;
pub mod schedules;
pub mod task;
pub mod task_run;
