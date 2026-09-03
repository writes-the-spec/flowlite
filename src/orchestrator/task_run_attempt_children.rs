use std::collections::HashMap;
use chrono::{DateTime, Utc};
use tokio::process::{Child, ChildStderr, ChildStdout};
use tokio::sync::Mutex;


/// The child process of one task run attempt, kept alive between polls.
pub struct TaskRunAttemptChild {
    pub child: Child,
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
    pub stdout_accumulated: Vec<u8>,
    pub stderr_accumulated: Vec<u8>,
    pub times_out_at: DateTime<Utc>,
}


/// The child processes of the task run attempts, keyed by task run attempt id.
/// Shared by TaskRunAttemptDispatcher, which spawns them, and TaskRunAttemptMonitor,
/// which owns them from there on. Lives only in memory, so a running attempt that is
/// not in here has no process left to wait for - after a restart, for instance.
pub struct TaskRunAttemptChildren {
    children: Mutex<HashMap<i64, TaskRunAttemptChild>>,
}


impl TaskRunAttemptChildren {

    pub fn new() -> Self {
        Self {
            children: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(
        self: &Self,
        task_run_attempt_id: i64,
        running_task_run_attempt: TaskRunAttemptChild,
    ) {
        self.children.lock().await.insert(task_run_attempt_id, running_task_run_attempt);
    }

    pub async fn remove(
        self: &Self,
        task_run_attempt_id: i64,
    ) -> Option<TaskRunAttemptChild> {
        self.children.lock().await.remove(&task_run_attempt_id)
    }

}
