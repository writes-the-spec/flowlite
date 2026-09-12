use chrono::{DateTime, Local, Utc};

use crate::crud::job_run::JobRunStatus;
use crate::crud::task_run::TaskRunStatus;
use crate::crud::task_run_attempt::TaskRunAttemptStatus;

pub fn timestamp(at: DateTime<Utc>) -> String {
    at.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Narrow form for the run list, where the column is only wide enough for day and time.
pub fn short_timestamp(at: DateTime<Utc>) -> String {
    at.with_timezone(&Local).format("%m-%d %H:%M").to_string()
}

pub fn duration(seconds: i64) -> String {
    let seconds = seconds.max(0);

    if seconds < 60 {
        return format!("{}s", seconds);
    }

    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;

    if minutes < 60 {
        return format!("{}m {:02}s", minutes, remaining_seconds);
    }

    let hours = minutes / 60;
    let remaining_minutes = minutes % 60;

    format!("{}h {:02}m", hours, remaining_minutes)
}

pub fn job_run_word(status: JobRunStatus) -> &'static str {
    match status {
        JobRunStatus::Pending => "queued",
        JobRunStatus::Running => "running",
        JobRunStatus::Succeeded => "succeeded",
        JobRunStatus::Failed => "failed",
        JobRunStatus::Skipped => "skipped",
        JobRunStatus::Aborted => "aborted",
        JobRunStatus::TimedOut => "timed out",
        JobRunStatus::Invalid => "invalid",
    }
}

pub fn task_run_word(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Pending => "queued",
        TaskRunStatus::Running => "running",
        TaskRunStatus::Succeeded => "succeeded",
        TaskRunStatus::Failed => "failed",
        TaskRunStatus::Skipped => "skipped",
        TaskRunStatus::Aborted => "aborted",
        TaskRunStatus::TimedOut => "timed out",
        TaskRunStatus::Invalid => "invalid",
    }
}

pub fn task_run_attempt_word(status: TaskRunAttemptStatus) -> &'static str {
    match status {
        TaskRunAttemptStatus::Pending => "queued",
        TaskRunAttemptStatus::Running => "running",
        TaskRunAttemptStatus::Succeeded => "succeeded",
        TaskRunAttemptStatus::Failed => "failed",
        TaskRunAttemptStatus::Skipped => "skipped",
        TaskRunAttemptStatus::Aborted => "aborted",
        TaskRunAttemptStatus::TimedOut => "timed out",
        TaskRunAttemptStatus::Invalid => "invalid",
    }
}
