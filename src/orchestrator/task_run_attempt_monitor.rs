use std::sync::Arc;
use std::time::Duration;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;
use tokio::io::AsyncReadExt;


/// Waits on the child process TaskRunAttemptDispatcher spawned for every running task
/// run attempt and finishes the attempt once its process exits, runs past its timeout
/// or gets aborted. Takes the process out of TaskRunAttemptChildren and owns it from
/// there on, and aborts an attempt whose process is gone.
/// Runs independently of TaskRunAttemptDispatcher, picking up whatever it set to
/// running. Knows nothing about task runs or retries: it never writes a task run
/// status, and TaskRunMonitor reads the attempt statuses written here to decide what
/// the task run does next.
pub struct TaskRunAttemptMonitor {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub children: Arc<TaskRunAttemptChildren>,
    pub signals: Arc<Signals>,
}


impl TaskRunAttemptMonitor {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        signals: Arc<Signals>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            children,
            signals,
        }
    }

    /// Settles a running attempt as exactly one outcome, from the process
    /// `take_task_run_attempt_child` hands over — which raises when there is none, as
    /// TaskRunMonitor's `get_last_task_run_attempt` does for a running task run with no
    /// attempt: both are rows this program cannot read. Raising settles nothing, so such a
    /// row keeps its status and strands the task run and job run above it.
    ///
    /// **Order decides precedence**, in the succeeded, failed, timed out, aborted, running
    /// ladder all three monitors read in: a real outcome outranks a stop, so
    /// `settle_for_aborted` is the last of the finished ones. A process that has already
    /// exited reports what it exited with rather than being recorded as killed, and one
    /// past its timeout reports the timeout. A process still running when its job run is
    /// stopped is still killed on this same pass, since the three outcomes above it decline.
    /// `settle_for_running` guards nothing at all, so it has to stay last.
    async fn handle_running_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        let mut task_run_attempt_child = self.take_task_run_attempt_child(task_run_attempt).await?;

        if self.settle_for_succeeded(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        if self.settle_for_failed(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        if self.settle_for_timed_out(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        if self.settle_for_aborted(task_run_attempt, &mut task_run_attempt_child).await? {
            return Ok(());
        }

        if self.settle_for_running(task_run_attempt, task_run_attempt_child).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Task run attempt {} settled as nothing: its process had neither succeeded nor \
             failed, it was not past its timeout, its job run was not stopped, and it was \
             not kept running",
            task_run_attempt.id,
        )
    }

    /// Takes the attempt's process out of TaskRunAttemptChildren, raising when there is
    /// none. The map holds only processes *this* program spawned, so a Running row without
    /// one belongs to an earlier run of it — the restart path — or lost its child to an
    /// error mid-pass. Neither is something this monitor can settle from: it has no exit
    /// status, no group to kill, and only whatever output was persisted before.
    async fn take_task_run_attempt_child(
        &self,
        task_run_attempt: &TaskRunAttempt,
    ) -> anyhow::Result<TaskRunAttemptChild> {

        self.children
            .remove(task_run_attempt.id)
            .await
            .ok_or_else(|| anyhow::anyhow!(
                "Task run attempt {} is running with no process to wait on",
                task_run_attempt.id,
            ))

    }

    /// Succeeds the attempt once its process has exited zero.
    ///
    /// Asks `try_wait` for itself, as `settle_for_failed` does rather than the two sharing
    /// one exit-status call: they are two lines of the ladder every monitor reads in, and
    /// which of them claimed the attempt should be readable from the chain. `try_wait`
    /// caches the status it reaped, so asking twice is a repeated question, not a race.
    async fn settle_for_succeeded(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let Some(exit_status) = task_run_attempt_child.child.try_wait()? else {
            return Ok(false);
        };

        if !exit_status.success() {
            return Ok(false);
        }

        Self::read_output(task_run_attempt_child).await;

        self.finish_task_run_attempt(
            task_run_attempt,
            task_run_attempt_child,
            TaskRunAttemptStatus::Succeeded,
        ).await?;

        Ok(true)
    }

    /// Fails the attempt once its process has exited non-zero. Nothing is retried here:
    /// TaskRunMonitor reads this status and decides whether another attempt goes in.
    async fn settle_for_failed(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let Some(exit_status) = task_run_attempt_child.child.try_wait()? else {
            return Ok(false);
        };

        if exit_status.success() {
            return Ok(false);
        }

        Self::read_output(task_run_attempt_child).await;

        self.finish_task_run_attempt(
            task_run_attempt,
            task_run_attempt_child,
            TaskRunAttemptStatus::Failed,
        ).await?;

        Ok(true)
    }

    /// Kills the process that ran past its timeout and times the attempt out.
    async fn settle_for_timed_out(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let timed_out = Utc::now() > task_run_attempt_child.times_out_at;

        if !timed_out {
            return Ok(false);
        }

        Self::read_output(task_run_attempt_child).await;

        task_run_attempt_child.kill_process_group().await;

        self.finish_task_run_attempt(
            task_run_attempt,
            task_run_attempt_child,
            TaskRunAttemptStatus::TimedOut,
        ).await?;

        Ok(true)
    }

    /// Kills the process of a stopped job run and aborts the attempt.
    async fn settle_for_aborted(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        let job_run_stopped = self.is_job_run_stopped(task_run_attempt).await?;

        if !job_run_stopped {
            return Ok(false);
        }

        Self::read_output(task_run_attempt_child).await;

        task_run_attempt_child.kill_process_group().await;

        self.finish_task_run_attempt(
            task_run_attempt,
            task_run_attempt_child,
            TaskRunAttemptStatus::Aborted,
        ).await?;

        Ok(true)
    }

    /// Leaves the attempt running: persists what its process has written so far and puts
    /// the process back for the next pass. Takes every attempt the outcomes above did not,
    /// so the bail is unreachable until one of them grows a guard.
    async fn settle_for_running(
        &self,
        task_run_attempt: &TaskRunAttempt,
        mut task_run_attempt_child: TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        Self::read_output(&mut task_run_attempt_child).await;

        self.update_task_run_attempt_output(task_run_attempt, &task_run_attempt_child).await?;

        self.children.insert(task_run_attempt.id, task_run_attempt_child).await;

        Ok(true)
    }

    /// Drains whatever the process has written so far without blocking on it.
    async fn read_output(running_task_run_attempt: &mut TaskRunAttemptChild) {

        let mut buf = [0; 1024];

        loop {
            match tokio::time::timeout(Duration::from_millis(10), running_task_run_attempt.stdout.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => running_task_run_attempt.stdout_accumulated.extend_from_slice(&buf[..n]),
                _ => break,
            }
        }

        loop {
            match tokio::time::timeout(Duration::from_millis(10), running_task_run_attempt.stderr.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => running_task_run_attempt.stderr_accumulated.extend_from_slice(&buf[..n]),
                _ => break,
            }
        }

    }

    async fn get_task_run_attempts(&self, status: TaskRunAttemptStatus) -> anyhow::Result<Vec<TaskRunAttempt>> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    task_id: None,
                    status: Some(status),
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await

    }

    async fn is_job_run_stopped(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let job_run_stop = self.crud.select_job_run_stop(
            &*self.conn_pool,
            &SelectJobRunStopsData {
                filter: SelectJobRunStopsDataFilter {
                    id: None,
                    job_run_id: Some(task_run_attempt.job_run_id),
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        ).await?;

        Ok(job_run_stop.is_some())

    }

    async fn finish_task_run_attempt(
        &self,
        task_run_attempt: &TaskRunAttempt,
        running_task_run_attempt: &TaskRunAttemptChild,
        status: TaskRunAttemptStatus,
    ) -> anyhow::Result<()> {

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(status),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    stdout: Some(String::from_utf8_lossy(&running_task_run_attempt.stdout_accumulated).to_string()),
                    stderr: Some(String::from_utf8_lossy(&running_task_run_attempt.stderr_accumulated).to_string()),
                },
            },
        ).await?;

        self.signals.publish();

        Ok(())
    }

    async fn update_task_run_attempt_output(
        &self,
        task_run_attempt: &TaskRunAttempt,
        running_task_run_attempt: &TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: None,
                    started_at: None,
                    finished_at: None,
                    stdout: Some(String::from_utf8_lossy(&running_task_run_attempt.stdout_accumulated).to_string()),
                    stderr: Some(String::from_utf8_lossy(&running_task_run_attempt.stderr_accumulated).to_string()),
                },
            },
        ).await?;

        // No publish: this runs on every pass for every running attempt and changes no
        // status, so waking all six pollers here would put the bus back into a loop.
        Ok(())
    }

}


impl Service for TaskRunAttemptMonitor {
    type Row = TaskRunAttempt;

    fn name(&self) -> &'static str {
        "Task Run Attempt Monitor"
    }

    fn row_context(&self, task_run_attempt: &TaskRunAttempt) -> String {
        format!(
            "task run attempt {} of task run {}",
            task_run_attempt.id,
            task_run_attempt.task_run_id,
        )
    }

    async fn select(&self) -> anyhow::Result<Vec<TaskRunAttempt>> {
        self.get_task_run_attempts(TaskRunAttemptStatus::Running).await
    }

    async fn handle(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {
        self.handle_running_task_run_attempt(task_run_attempt).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::test_support::TestDb;
    use chrono::TimeDelta;

    /// A running attempt with a process handed to the monitor, ready for `handle`.
    async fn running_attempt(db: &TestDb) -> TaskRunAttempt {

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await
    }

    /// Pins `settle_for_succeeded` ahead of `settle_for_running`. `settle_for_running`
    /// guards nothing and returns true for every attempt it is asked about, so moving it
    /// up leaves this attempt Running for ever and the task run never finishes.
    #[tokio::test]
    async fn an_exited_process_reports_the_status_it_exited_with() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Succeeded,
        );
    }

    /// The same for `settle_for_failed`, the other half of the exit status.
    #[tokio::test]
    async fn a_process_that_exited_non_zero_fails_the_attempt() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_exited_child(&task_run_attempt, "exit 3", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Failed,
        );
    }

    /// Pins `settle_for_timed_out` ahead of `settle_for_running`.
    #[tokio::test]
    async fn a_process_past_its_deadline_times_the_attempt_out() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_running_child(
            &task_run_attempt,
            "sleep 30",
            Utc::now() - TimeDelta::seconds(1),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::TimedOut,
        );
    }

    /// Pins `settle_for_aborted` ahead of `settle_for_running`, and behind the two above
    /// it: a stop kills a process that is still going, but does not outrank an outcome the
    /// process reached on its own.
    #[tokio::test]
    async fn a_stopped_job_run_aborts_a_process_that_is_still_running() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.insert_job_run_stop(task_run_attempt.job_run_id).await;
        db.spawn_running_child(
            &task_run_attempt,
            "sleep 30",
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Aborted,
        );
    }

    /// A real outcome outranks a stop: a process that had already exited reports its exit
    /// status rather than being recorded as killed, even though its job run was stopped.
    #[tokio::test]
    async fn an_exit_status_outranks_a_stop() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.insert_job_run_stop(task_run_attempt.job_run_id).await;
        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Succeeded,
        );
    }

    /// Pins `settle_for_succeeded` ahead of `settle_for_timed_out`: a process that got
    /// there on its own before the poll pass noticed the deadline reports what it exited
    /// with, rather than being recorded as killed by a timeout that never killed it.
    #[tokio::test]
    async fn an_exit_status_outranks_a_timeout() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_exited_child(
            &task_run_attempt,
            "exit 0",
            Utc::now() - TimeDelta::seconds(1),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Succeeded,
        );
    }

    /// Pins `settle_for_timed_out` ahead of `settle_for_aborted`, the other half of "a real
    /// outcome outranks a stop": a process past its deadline reports the timeout even though
    /// its job run was stopped and it was killed on this same pass.
    #[tokio::test]
    async fn a_timeout_outranks_a_stop() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.insert_job_run_stop(task_run_attempt.job_run_id).await;
        db.spawn_running_child(
            &task_run_attempt,
            "sleep 30",
            Utc::now() - TimeDelta::seconds(1),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::TimedOut,
        );
    }

    /// A command that makes `sh` fork rather than exec leaves a grandchild, which is most
    /// real commands: anything with a `;`, a pipe or a background job. Killing only the
    /// process flowlite spawned reports TimedOut while the actual work carries on.
    ///
    /// Spawned through the real dispatcher, not the fixture, so it covers both halves of
    /// the fix: the process group the dispatcher creates, and the group this monitor kills.
    #[tokio::test]
    async fn a_timeout_kills_the_whole_process_group() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let pid_file = db.data_dir().join("timeout.pid");

        let task_run = db.insert_task_run_for_command(
            job_run.id,
            &format!("sleep 30 & echo $! > {}; wait", pid_file.display()),
            0,
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        let grandchild = crate::test_support::read_pid_file(&pid_file).await;

        let task_run_attempt = db.task_run_attempt(task_run_attempt.id).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::TimedOut,
        );
        assert!(
            crate::test_support::has_exited(grandchild).await,
            "the grandchild outlived the timeout that reported TimedOut",
        );
    }

    /// The same for a stop: making the work stop is the whole point of one. The stop goes
    /// in after the dispatcher has spawned, since it would otherwise skip the attempt.
    #[tokio::test]
    async fn a_stop_kills_the_whole_process_group() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let pid_file = db.data_dir().join("stop.pid");

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

        db.insert_job_run_stop(job_run.id).await;

        let task_run_attempt = db.task_run_attempt(task_run_attempt.id).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Aborted,
        );
        assert!(
            crate::test_support::has_exited(grandchild).await,
            "the grandchild outlived the stop that reported Aborted",
        );
    }

    /// The restart path: a running attempt whose process was spawned by an earlier run of
    /// this program is not in TaskRunAttemptChildren, so there is nothing left to wait for.
    /// Raises, as TaskRunMonitor does for a running task run with no attempt, and leaves
    /// the row Running: nothing else settles it.
    #[tokio::test]
    async fn a_running_attempt_with_no_process_raises() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        assert!(db.task_run_attempt_monitor().handle(&task_run_attempt).await.is_err());
    }

    #[tokio::test]
    async fn a_process_still_running_keeps_the_attempt_running_and_persists_its_output() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_running_child(
            &task_run_attempt,
            // `exec` so sh becomes the sleep rather than forking it: kill() reaches the
            // process it spawned and nothing below it, so a forked grandchild would survive
            // this test. That is true of a real task's command too.
            "echo hello; exec sleep 30",
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let settled = db.task_run_attempt(task_run_attempt.id).await;

        assert_eq!(settled.status, TaskRunAttemptStatus::Running);
        assert_eq!(settled.stdout, "hello\n");

        // The process is handed back for the next pass, so this is the one outcome that
        // leaves one running: take it back and kill it rather than outliving the suite.
        let mut handed_back = db.children.remove(task_run_attempt.id).await
            .expect("settle_for_running must put the process back for the next pass");

        handed_back.child.kill().await.unwrap();
    }
}
