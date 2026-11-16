//! What the YAML under the project directory declares - jobs, their tasks and schedules -
//! parsed, defaulted and validated once into a plain struct every reader borrows.

mod ad_hoc_job;
mod declared;
mod secret_env;
mod validate;

pub use crate::declared::ad_hoc_job::JobIdAlreadyInstalled;
pub use crate::declared::declared::{Declared, Job, Schedule, ScheduleJob, Task};
