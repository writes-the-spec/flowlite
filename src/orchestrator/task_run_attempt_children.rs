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


impl TaskRunAttemptChild {

    /// Kills the command's whole process group, not just the process flowlite spawned.
    ///
    /// `sh -c` execs only for a single command; for anything with a `;`, a pipe or a
    /// background job it forks, so signalling the child alone leaves the real work
    /// running. TaskRunAttemptDispatcher puts every attempt in its own group, whose id is
    /// the pid of the sh it spawned. The kill that follows reaps that sh.
    pub async fn kill_process_group(&mut self) {

        if let Some(pid) = self.child.id() {
            // Safe: killpg only delivers a signal, and a group that is already gone
            // reports ESRCH, which is exactly the state we wanted.
            unsafe { libc::killpg(pid as i32, libc::SIGKILL) };
        }

        let _ = self.child.kill().await;
    }

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

    /// Kills every process still running, for shutdown. Takes them out of the map as it
    /// goes, so nothing polling afterwards finds a process that is already dead.
    ///
    /// The attempt rows are left Running: the next start settles them through
    /// `TaskRunAttemptMonitor::settle_for_aborted_without_child`, which is where an attempt
    /// with no process belongs, and which this makes truthful.
    pub async fn kill_all(self: &Self) {

        let mut children = self.children.lock().await;

        for (_, mut task_run_attempt_child) in children.drain() {
            task_run_attempt_child.kill_process_group().await;
        }
    }

}


#[cfg(test)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::poller::Service;
    use crate::test_support::TestDb;

    /// Shutting down has to reach the command's whole process tree, the same as a timeout
    /// or a stop: leaving it running is what makes the next start's Aborted a lie.
    #[tokio::test]
    async fn kill_all_kills_the_process_group_of_every_attempt() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let pid_file = db.data_dir().join("shutdown.pid");

        let task_run = db.insert_task_run_for_command(
            job_run.id,
            &format!("sleep 30 & echo $! > {}; wait", pid_file.display()),
            3600,
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        let grandchild = crate::test_support::read_pid_file(&pid_file).await;

        db.children.kill_all().await;

        assert!(
            crate::test_support::has_exited(grandchild).await,
            "the grandchild outlived the shutdown",
        );
    }
}
