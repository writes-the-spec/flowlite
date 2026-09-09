use std::sync::Arc;
use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::crud::task_run_attempt_output::{InsertTaskRunAttemptOutputData, InsertTaskRunAttemptOutputDataInput, TaskRunAttemptOutputStream};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::poller::Service;
use crate::signals::Signals;
use chrono::Utc;


/// Whether the ladder settled the attempt for good or handed it on to the next pass,
/// which is what decides whether the process goes back into TaskRunAttemptChildren.
enum Settled {
    Terminal,
    Running,
}


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
    pub app_config: AppConfig,
}


impl TaskRunAttemptMonitor {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        children: Arc<TaskRunAttemptChildren>,
        signals: Arc<Signals>,
        app_config: AppConfig,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            children,
            signals,
            app_config,
        }
    }

    /// Settles a running attempt as exactly one outcome, from the process
    /// `TaskRunAttemptChildren` hands over — or as `Invalid` when it holds none, which is
    /// a row this program cannot read rather than an outcome it can name.
    ///
    /// **Order decides precedence**: a real outcome outranks a stop, so a process that has
    /// already exited reports its exit status rather than being recorded as killed.
    /// `settle_for_running` guards nothing, so it has to stay last.
    ///
    /// **In the two rungs that kill, the kill comes before the drain.** A pipe closes when
    /// the process holding it dies, and that EOF is what lets `finish_reading` return;
    /// draining first waits out the whole reader EOF timeout.
    async fn handle_running_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        let Some(mut task_run_attempt_child) = self.children.remove(task_run_attempt.id).await else {
            return self.settle_for_invalid(task_run_attempt).await;
        };

        match self.settle_task_run_attempt(task_run_attempt, &mut task_run_attempt_child).await {
            Ok(Settled::Terminal) => Ok(()),
            Ok(Settled::Running) => {
                self.children.insert(task_run_attempt.id, task_run_attempt_child).await;
                Ok(())
            },
            // Put the process back rather than dropping it. A dropped Child is not killed,
            // so it would keep running unowned, whatever it had written would be lost, and
            // the next pass would find the row Running with no process and settle it
            // Invalid — an unknown ending for an attempt that was merely interrupted.
            Err(e) => {
                self.children.insert(task_run_attempt.id, task_run_attempt_child).await;
                Err(e)
            },
        }
    }

    /// Runs the ladder and reports which side of it claimed the attempt, so the caller
    /// alone decides whether the process goes back into TaskRunAttemptChildren. Keeping
    /// that decision in one place is what lets the error path put it back too.
    async fn settle_task_run_attempt(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<Settled> {

        if self.settle_for_succeeded(task_run_attempt, task_run_attempt_child).await? {
            return Ok(Settled::Terminal);
        }

        if self.settle_for_failed(task_run_attempt, task_run_attempt_child).await? {
            return Ok(Settled::Terminal);
        }

        if self.settle_for_timed_out(task_run_attempt, task_run_attempt_child).await? {
            return Ok(Settled::Terminal);
        }

        if self.settle_for_aborted(task_run_attempt, task_run_attempt_child).await? {
            return Ok(Settled::Terminal);
        }

        if self.settle_for_running(task_run_attempt, task_run_attempt_child).await? {
            return Ok(Settled::Running);
        }

        self.settle_unclaimed(task_run_attempt, task_run_attempt_child).await?;

        Ok(Settled::Terminal)
    }

    /// Unreachable while `settle_for_running` claims unconditionally. See
    /// `JobRunDispatcher::settle_unclaimed` for why it settles rather than raises.
    ///
    /// Alone among the five it holds a live process, so the group is killed first — before
    /// the drain, for the reason `settle_for_timed_out` gives — rather than leaked.
    async fn settle_unclaimed(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        eprintln!(
            "Task run attempt {} was claimed by no outcome: its process had neither \
             succeeded nor failed, it was not past its timeout, its job run was not \
             stopped, and it was not kept running. Killing it and settling it invalid. \
             This is a bug.",
            task_run_attempt.id,
        );

        task_run_attempt_child.kill_process_group().await;

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::Invalid,
        ).await
    }

    /// Settles an attempt whose process this program does not hold — the restart path,
    /// since `TaskRunAttemptChildren` holds only processes this run of the program spawned.
    ///
    /// No exit status, no group to kill, nothing left to drain, so no outcome can honestly
    /// be claimed. Settled rather than raised on: a row left Running strands the task run
    /// and job run above it, holding a parallel slot for ever.
    async fn settle_for_invalid(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        eprintln!(
            "Task run attempt {} was running with no process to wait on, so its outcome is \
             unknown and it has been settled invalid. Its command may still be running.",
            task_run_attempt.id,
        );

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::Invalid,
        ).await
    }

    /// Succeeds the attempt once its process has exited zero.
    ///
    /// Asks `try_wait` for itself rather than sharing one call with `settle_for_failed`, so
    /// each status is its own rung of the ladder. `try_wait` caches the status it reaped,
    /// so asking twice is a repeated question, not a race.
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

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
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

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
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

        // The kill comes before the drain: killing is what closes the pipes, and a closed
        // pipe is the EOF that ends a reader. Draining first would wait out the whole of
        // the EOF timeout on a process that is still running and still holding them.
        task_run_attempt_child.kill_process_group().await;

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
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

        // Killed before drained, for the reason `settle_for_timed_out` gives.
        task_run_attempt_child.kill_process_group().await;

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::Aborted,
        ).await?;

        Ok(true)
    }

    /// Leaves the attempt running: persists what its process has written so far. Takes
    /// every attempt the outcomes above did not, so the bail is unreachable until one of
    /// them grows a guard. Putting the process back for the next pass belongs to
    /// `handle_running_task_run_attempt`, which does it for the error path too.
    async fn settle_for_running(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<bool> {

        self.record_output(task_run_attempt, task_run_attempt_child).await?;

        Ok(true)
    }

    /// Records whatever the readers have delivered so far and returns immediately.
    ///
    /// Never waits: the readers run on their own and the next pass is a second away, so a
    /// running attempt's output reaches the table at most one pass after it was written.
    async fn record_output(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        let mut stdout = String::new();
        let mut stderr = String::new();

        while let Ok(chunk) = task_run_attempt_child.chunks.try_recv() {
            match chunk.stream {
                TaskRunAttemptOutputStream::Stdout => stdout.push_str(&chunk.content),
                TaskRunAttemptOutputStream::Stderr => stderr.push_str(&chunk.content),
            }
        }

        self.insert_output(task_run_attempt, stdout, stderr).await
    }

    /// Records everything the readers will ever deliver, by waiting for the channel to
    /// close — both of them reaching EOF and dropping their senders. Waiting for a real EOF
    /// is what makes this final drain complete rather than a guess at one.
    async fn finish_reading(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        let mut stdout = String::new();
        let mut stderr = String::new();

        // Bounded because EOF is not guaranteed: something that escaped the process group
        // can hold a pipe open after the kill. Poller::run handles rows in sequence, so an
        // unbounded wait on one attempt would stop every other attempt being handled.
        let drained = tokio::time::timeout(self.app_config.orchestrator.reader_eof_timeout(), async {
            while let Some(chunk) = task_run_attempt_child.chunks.recv().await {
                match chunk.stream {
                    TaskRunAttemptOutputStream::Stdout => stdout.push_str(&chunk.content),
                    TaskRunAttemptOutputStream::Stderr => stderr.push_str(&chunk.content),
                }
            }
        }).await;

        // EOF never came, so a reader is still blocked on a pipe something else holds open
        // and will not return on its own.
        if drained.is_err() {
            task_run_attempt_child.abort_readers();
        }

        self.insert_output(task_run_attempt, stdout, stderr).await
    }

    /// Appends one row per stream that has new output, and none for a stream that has not.
    /// One row per pass rather than one per read keeps the write volume proportional to the
    /// bytes the task produced.
    async fn insert_output(
        &self,
        task_run_attempt: &TaskRunAttempt,
        stdout: String,
        stderr: String,
    ) -> anyhow::Result<()> {

        let streams = [
            (TaskRunAttemptOutputStream::Stdout, stdout),
            (TaskRunAttemptOutputStream::Stderr, stderr),
        ];

        for (stream, content) in streams {

            if content.is_empty() {
                continue;
            }

            self.crud.insert_task_run_attempt_output(
                &*self.conn_pool,
                &InsertTaskRunAttemptOutputData {
                    input: InsertTaskRunAttemptOutputDataInput {
                        task_run_attempt_id: task_run_attempt.id,
                        task_run_id: task_run_attempt.task_run_id,
                        job_run_id: task_run_attempt.job_run_id,
                        job_id: task_run_attempt.job_id.clone(),
                        task_id: task_run_attempt.task_id.clone(),
                        stream,
                        content,
                    },
                },
            ).await?;
        }

        // No publish: this runs on every pass for every running attempt and changes no
        // status, so waking all six pollers here would put the bus back into a loop.
        Ok(())
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

    /// Writes the terminal status, and only that: the output already went in.
    ///
    /// Every caller drains through `finish_reading` first, so **an attempt that reads as
    /// terminal has complete output**. Writing the status first would leave a window where
    /// the page shows Succeeded above a truncated log.
    async fn finish_task_run_attempt(
        &self,
        task_run_attempt: &TaskRunAttempt,
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
                },
            },
        ).await?;

        self.signals.publish();

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
    use crate::test_support::{has_exited, read_pid_file};
    use std::time::Duration;
    use crate::crud::job_run::JobRunStatus;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt_output::{SelectTaskRunAttemptOutputsData, SelectTaskRunAttemptOutputsDataFilter, SelectTaskRunAttemptOutputsDataSort};
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

    /// A command that makes `sh` fork rather than exec leaves a grandchild — anything with
    /// a `;`, a pipe or a background job — and killing only the process flowlite spawned
    /// reports TimedOut while the work carries on. Spawned through the real dispatcher, so
    /// it covers both the group created and the group killed.
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
    async fn a_running_attempt_with_no_process_is_invalid() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );
    }

    /// Terminal means terminal: the row carries an end instant like any other settled
    /// attempt, so the run's timeline does not show it still going.
    #[tokio::test]
    async fn an_invalid_attempt_is_given_a_finished_instant() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert!(db.task_run_attempt(task_run_attempt.id).await.finished_at.is_some());
    }

    /// Settling it is the point: a second pass finds nothing left to handle, where the
    /// raise this replaces came back every pass for as long as serve lived.
    #[tokio::test]
    async fn a_settled_invalid_attempt_is_not_picked_up_again() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let running = db.crud.select_task_run_attempts(
            &*db.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    task_id: None,
                    status: Some(TaskRunAttemptStatus::Running),
                },
                sort: None,
            },
        ).await.unwrap();

        assert!(running.is_empty());
    }

    /// The child is out of TaskRunAttemptChildren for the length of a pass, so an error in
    /// the middle of the ladder used to drop it: a dropped Child is not killed, so the
    /// process kept running unowned, its output was lost, and the row stayed Running with
    /// nothing left able to settle it.
    #[tokio::test]
    async fn a_pass_that_errors_puts_the_process_back() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_running_child(
            &task_run_attempt,
            "exec sleep 30",
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        // Closing the pool fails the stop lookup in `settle_for_aborted`, which is the
        // first rung of the ladder that asks the database anything for a process that is
        // still running and not yet past its deadline.
        db.conn_pool.close().await;

        assert!(db.task_run_attempt_monitor().handle(&task_run_attempt).await.is_err());

        let mut handed_back = db.children.remove(task_run_attempt.id).await
            .expect("a pass that errored must put the process back, not orphan it");

        handed_back.child.kill().await.unwrap();
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

        // Passes until the reader has delivered, since `record_output` waits for nothing.
        let stdout = db.poll_until_stdout(&task_run_attempt).await;

        assert_eq!(stdout, "hello\n");
        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Running,
        );

        // The process is handed back for the next pass, so this is the one outcome that
        // leaves one running: take it back and kill it rather than outliving the suite.
        let mut handed_back = db.children.remove(task_run_attempt.id).await
            .expect("settle_for_running must put the process back for the next pass");

        handed_back.child.kill().await.unwrap();
    }

    /// The cap must not stop the reader reading. The pipe is 64 KiB, so a task writing past
    /// the cap blocks on a full one the moment recording stops, and the attempt then never
    /// finishes at all. Two megabytes through a one-megabyte cap is the shape of that bug,
    /// and it fails by hanging rather than by a wrong value.
    #[tokio::test]
    async fn a_task_over_the_cap_still_finishes() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let task_run = db.insert_task_run_for_command(
            job_run.id,
            "exec yes 0123456789012345678901234567890123456789012345678901234567890123 | head -c 2097152",
            3600,
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        let task_run_attempt = db.task_run_attempt(task_run_attempt.id).await;

        // The command has to exit before the ladder can settle it, so keep passing.
        for _ in 0..600 {
            db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

            if db.task_run_attempt(task_run_attempt.id).await.status != TaskRunAttemptStatus::Running {
                break;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let stdout = db.task_run_attempt_output(task_run_attempt.id).await.stdout;

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Succeeded,
        );
        assert!(
            stdout.ends_with(&format!(
                "\n[flowlite: output truncated, exceeded {} bytes]\n",
                crate::app_config::AppConfig::default().orchestrator.max_stream_bytes,
            )),
            "the cap did not report itself",
        );
    }

    /// Output written just before a timeout is still recorded, which is the case the
    /// inverted order exists for: the kill is what closes the pipe and gives the reader the
    /// EOF that `finish_reading` waits for.
    #[tokio::test]
    async fn output_written_before_a_timeout_is_recorded() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        let wrote = db.data_dir().join("wrote.pid");

        db.spawn_running_child(
            &task_run_attempt,
            &format!("echo before the deadline; echo $$ > {}; exec sleep 30", wrote.display()),
            Utc::now() - TimeDelta::seconds(1),
        ).await;

        // The pass kills before it drains, so the echo has to have already happened or the
        // test races the shell's startup instead of testing the drain. The drain this
        // replaced waited out two 10ms timeouts first, which hid that race by accident.
        crate::test_support::read_pid_file(&wrote).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::TimedOut,
        );
        assert_eq!(
            db.task_run_attempt_output(task_run_attempt.id).await.stdout,
            "before the deadline\n",
        );
    }

    /// One row per stream per pass, not one per read: that is what keeps the total bytes
    /// written equal to the bytes the task produced instead of the square of them.
    #[tokio::test]
    async fn a_pass_writes_at_most_one_row_per_stream() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_exited_child(
            &task_run_attempt,
            "echo one; echo two; echo three",
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let rows = db.crud.select_task_run_attempt_outputs(
            &*db.conn_pool,
            &SelectTaskRunAttemptOutputsData {
                filter: SelectTaskRunAttemptOutputsDataFilter {
                    id: None,
                    task_run_attempt_id: Some(task_run_attempt.id),
                    task_run_id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    stream: None,
                },
                sort: Some(SelectTaskRunAttemptOutputsDataSort::Id),
            },
        ).await.unwrap();

        assert_eq!(rows.len(), 1, "three echos drained on one pass are one stdout row");
        assert_eq!(rows[0].content, "one\ntwo\nthree\n");
    }

    /// stderr is recorded apart from stdout, which is the whole reason the two are separate.
    #[tokio::test]
    async fn the_two_streams_are_recorded_apart() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_exited_child(
            &task_run_attempt,
            "echo out; echo err >&2",
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let streams = db.task_run_attempt_output(task_run_attempt.id).await;

        assert_eq!(streams.stdout, "out\n");
        assert_eq!(streams.stderr, "err\n");
    }

    /// Unreachable while `settle_for_running` claims unconditionally, so it is called
    /// directly. Unlike the other four this one holds a live process, so settling the row
    /// terminal without killing its group would leak the command it can no longer account
    /// for - the pid file proves the kill happened.
    #[tokio::test]
    async fn an_unclaimed_attempt_is_killed_and_settled_invalid() {

        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        let pid_file = db.data_dir().join("pid");

        db.spawn_running_child(
            &task_run_attempt,
            &format!("echo $$ > {}; exec sleep 30", pid_file.display()),
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        let pid = read_pid_file(&pid_file).await;

        let mut child = db.children.remove(task_run_attempt.id).await.unwrap();

        db.task_run_attempt_monitor().settle_unclaimed(&task_run_attempt, &mut child).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );

        assert!(has_exited(pid).await, "the process was left running");
    }
}
