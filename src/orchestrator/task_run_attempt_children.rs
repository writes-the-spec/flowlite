use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
use chrono::{DateTime, TimeDelta, Utc};
use crate::crud::task_run_attempt::TaskRunAttemptStatus;
use crate::orchestrator::task_run_attempt_reader::TaskRunAttemptOutputChunk;
use tokio::process::Child;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;


/// The child process of one task run attempt, kept alive between polls.
pub struct TaskRunAttemptChild {
    pub child: Child,
    /// The group every signal goes to - the pid of the sh that was spawned, which the
    /// dispatcher made a group leader. Kept rather than read off `child`, which answers
    /// `None` once the leader is reaped while the rest of its group may still be running.
    pub process_group_id: Option<i32>,
    /// Everything the readers have delivered and the monitor has not recorded yet.
    ///
    /// `recv` returning None is how the monitor learns both readers reached EOF, so the
    /// only senders in existence are the two the readers own — TaskRunAttemptDispatcher
    /// keeps none of its own, or the channel would never close.
    pub chunks: UnboundedReceiver<TaskRunAttemptOutputChunk>,
    pub readers: [JoinHandle<()>; 2],
    pub times_out_at: DateTime<Utc>,
    /// `task_run.idle_timeout`, `None` for no limit.
    pub idle_timeout: Option<TimeDelta>,
    /// When a reader last read anything, in Unix milliseconds. Written by the readers on
    /// every read - including those past `max_stream_bytes`, which send nothing - so the
    /// monitor cannot mistake a command whose output the cap is dropping for a silent one.
    pub last_output_at: Arc<AtomicI64>,
    /// Set once the monitor has sent the group SIGTERM; see `Termination`.
    pub terminating: Option<Termination>,
}


/// An attempt the monitor has asked to exit: the status it will settle as however it then
/// exits, and when whatever is left of its group gets SIGKILL.
pub struct Termination {
    pub status: TaskRunAttemptStatus,
    pub kill_at: DateTime<Utc>,
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

        self.signal_process_group(libc::SIGKILL);

        let _ = self.child.kill().await;
    }

    /// Whether the command has gone its whole idle timeout without writing anything.
    pub fn is_idle(&self, now: DateTime<Utc>) -> bool {

        let Some(idle_timeout) = self.idle_timeout else {
            return false;
        };

        let silent_for = now.timestamp_millis() - self.last_output_at.load(Ordering::Relaxed);

        silent_for >= idle_timeout.num_milliseconds()
    }

    /// Asks the command's whole process group to exit, which most tools do cleanly: an
    /// agent finishes its write, git releases its lock.
    pub fn terminate_process_group(&self) {
        self.signal_process_group(libc::SIGTERM);
    }

    /// Whether nothing of the command is left - the sh reaped, and no process of its group
    /// still running. The group can outlive its leader: a grandchild cleaning up after
    /// SIGTERM is still in it.
    pub fn process_group_is_gone(&mut self) -> bool {

        // Reaped first, since an exited but unreaped leader still answers the probe below.
        let leader_exited = matches!(self.child.try_wait(), Ok(Some(_)));

        let Some(process_group_id) = self.process_group_id else {
            return leader_exited;
        };

        // Signal 0 delivers nothing and only asks whether any process is there to receive.
        // Safe for the same reason as `signal_process_group`.
        let probe = unsafe { libc::killpg(process_group_id, 0) };

        probe == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }

    fn signal_process_group(&self, signal: libc::c_int) {

        if let Some(process_group_id) = self.process_group_id {
            // Safe: killpg only delivers a signal, and a group that is already gone
            // reports ESRCH, which is exactly the state we wanted.
            unsafe { libc::killpg(process_group_id, signal) };
        }
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
        &self,
        task_run_attempt_id: i64,
        running_task_run_attempt: TaskRunAttemptChild,
    ) {
        self.children.lock().await.insert(task_run_attempt_id, running_task_run_attempt);
    }

    pub async fn remove(
        &self,
        task_run_attempt_id: i64,
    ) -> Option<TaskRunAttemptChild> {
        self.children.lock().await.remove(&task_run_attempt_id)
    }

    /// Ends every process still running, for shutdown: SIGTERM to every group at once, then
    /// SIGKILL to whatever has not exited when `grace` runs out. Called only once the
    /// pollers have stopped, so nothing takes a child out of the map during the wait.
    ///
    /// The attempt rows are left Running on purpose: `Orchestrator::recover` settles them
    /// on the next start.
    pub async fn terminate_all(&self, grace: Duration) {

        let mut children = self.children.lock().await;

        for task_run_attempt_child in children.values() {
            task_run_attempt_child.terminate_process_group();
        }

        let deadline = tokio::time::Instant::now() + grace;

        while tokio::time::Instant::now() < deadline
            && children.values_mut().any(|task_run_attempt_child| !task_run_attempt_child.process_group_is_gone())
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        for (_, mut task_run_attempt_child) in children.drain() {
            task_run_attempt_child.kill_process_group().await;
        }
    }

}


#[cfg(test)]
#[expect(
    clippy::await_holding_lock,
    reason = "a test holds the environment lock for its whole run, on purpose - see test_support",
)]
mod tests {
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttemptStatus;
    use crate::poller::Service;
    use crate::test_support::TestDb;
    use crate::test_support::reading_the_environment;

    /// Shutting down has to reach the command's whole process tree, the same as a timeout
    /// or a stop: leaving it running is what makes the next start's Invalid a lie.
    #[tokio::test]
    async fn shutdown_kills_the_process_group_of_every_attempt() {

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
            TaskRunAttemptStatus::Queued,
        ).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        let grandchild = crate::test_support::read_pid_file(&pid_file).await;

        db.children.terminate_all(std::time::Duration::ZERO).await;

        assert!(
            crate::test_support::has_exited(grandchild).await,
            "the grandchild outlived the shutdown",
        );
    }

    /// Every command gets SIGTERM together and the same grace, so shutting down with a
    /// cooperative command costs the moment it takes to exit, not the whole grace.
    #[tokio::test]
    async fn shutdown_lets_a_command_exit_cleanly_before_the_grace_runs_out() {

        let _environment = reading_the_environment();
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, crate::crud::task_run::TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;
        let cleaned = db.data_dir().join("cleaned");
        let ready = db.data_dir().join("ready");

        db.spawn_running_child(
            &task_run_attempt,
            &format!("trap 'echo cleaned > {}; exit 0' TERM; echo ready > {}; while true; do sleep 0.1; done", cleaned.display(), ready.display()),
            chrono::Utc::now() + chrono::TimeDelta::seconds(3600),
        ).await;

        crate::test_support::read_command_file(&ready).await;

        let started = std::time::Instant::now();
        db.children.terminate_all(std::time::Duration::from_secs(10)).await;

        assert!(started.elapsed() < std::time::Duration::from_secs(5), "waited {:?}", started.elapsed());
        assert_eq!(std::fs::read_to_string(&cleaned).unwrap().trim(), "cleaned");
    }

    #[tokio::test]
    async fn shutdown_kills_a_command_that_ignores_sigterm_once_the_grace_runs_out() {

        let _environment = reading_the_environment();
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, crate::crud::task_run::TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;
        let pid_file = db.data_dir().join("stubborn.pid");

        db.spawn_running_child(
            &task_run_attempt,
            &format!("trap '' TERM; echo $$ > {}; while true; do sleep 0.1; done", pid_file.display()),
            chrono::Utc::now() + chrono::TimeDelta::seconds(3600),
        ).await;

        let stubborn = crate::test_support::read_pid_file(&pid_file).await;

        let started = std::time::Instant::now();
        db.children.terminate_all(std::time::Duration::from_secs(1)).await;

        assert!(started.elapsed() >= std::time::Duration::from_secs(1), "waited {:?}", started.elapsed());
        assert!(crate::test_support::has_exited(stubborn).await, "the command outlived the shutdown");
    }
}
