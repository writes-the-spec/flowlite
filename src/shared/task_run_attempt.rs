/// The attempt this process runs inside, if any. The dispatcher sets this variable on every
/// task command after stripping every inherited `FLOWLITE_` variable, so a task's own `env:`
/// cannot forge it.
pub(crate) fn own_task_run_attempt_id() -> Option<i64> {
    std::env::var("FLOWLITE_TASK_RUN_ATTEMPT_ID").ok()?.parse().ok()
}
