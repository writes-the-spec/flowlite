use std::collections::HashMap;
use chrono::{DateTime, Utc};
use crate::orchestrator::task_run_attempt_reader::TaskRunAttemptOutputChunk;
use tokio::process::Child;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;


/// The child process of one task run attempt, kept alive between polls.
pub struct TaskRunAttemptChild {
    pub child: Child,
    /// Everything the readers have delivered and the monitor has not recorded yet.
    ///
    /// `recv` returning None is how the monitor learns both readers reached EOF, so the
    /// only senders in existence are the two the readers own — TaskRunAttemptDispatcher
    /// keeps none of its own, or the channel would never close.
    pub chunks: UnboundedReceiver<TaskRunAttemptOutputChunk>,
    pub readers: [JoinHandle<()>; 2],
    pub times_out_at: DateTime<Utc>,
}


impl TaskRunAttemptChild {

    /// Stops both readers.
    ///
    /// A reader owns its pipe file descriptor and blocks on reading it, so one left behind
    /// after something else kept the pipe open holds that descriptor for as long as this
    /// process lives. Aborting is the only way to reclaim it: the reader will not return on
    /// its own, because the EOF it is waiting for is never coming.
    pub fn abort_readers(&self) {

        for reader in &self.readers {
            reader.abort();
        }
    }

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
    /// The attempt rows are left Running, and the next start cannot settle them either:
    /// its monitor raises on an attempt with no process, so the row stays Running and is
    /// logged once a second.
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
    use crate::test_support::reading_the_environment;

    /// Shutting down has to reach the command's whole process tree, the same as a timeout
    /// or a stop: leaving it running is what makes the next start's Aborted a lie.
    #[tokio::test]
    async fn kill_all_kills_the_process_group_of_every_attempt() {

        let _environment = reading_the_environment();
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
