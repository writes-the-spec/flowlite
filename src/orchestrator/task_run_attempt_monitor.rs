use std::sync::Arc;
use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::crud::task_run_attempt_output::{InsertTaskRunAttemptOutputData, InsertTaskRunAttemptOutputDataInput, TaskRunAttemptOutputStream};
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::poller::Service;
use crate::run_dir::{job_run_dir, task_output_path};
use crate::signals::Signals;
use chrono::Utc;


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

    /// Takes the process `TaskRunAttemptChildren` holds for the attempt — or settles it
    /// `Invalid` when it holds none, since that is a row this program cannot read rather
    /// than an outcome it can name — then dispatches the write for whatever
    /// `derive_next_status` decides. Deciding only reads.
    ///
    /// `Running` is the only outcome that hands the process back; every other arm consumes
    /// it. A failure to derive puts it back untouched rather than settling `Invalid`: it may
    /// be a transient failure, and the process is still there to ask about next pass.
    async fn handle_running_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        let Some(mut task_run_attempt_child) = self.children.remove(task_run_attempt.id).await else {
            return self.set_to_invalid(task_run_attempt, None).await;
        };

        match self.derive_next_status(task_run_attempt, &mut task_run_attempt_child).await {
            Ok(TaskRunAttemptStatus::Succeeded) =>
                self.set_to_succeeded(task_run_attempt, &mut task_run_attempt_child).await,
            Ok(TaskRunAttemptStatus::Failed) =>
                self.set_to_failed(task_run_attempt, &mut task_run_attempt_child).await,
            Ok(TaskRunAttemptStatus::TimedOut) =>
                self.set_to_timed_out(task_run_attempt, &mut task_run_attempt_child).await,
            Ok(TaskRunAttemptStatus::Aborted) =>
                self.set_to_aborted(task_run_attempt, &mut task_run_attempt_child).await,
            Ok(TaskRunAttemptStatus::Running) => {
                let result = self.record_output(task_run_attempt, &mut task_run_attempt_child).await;
                self.children.insert(task_run_attempt.id, task_run_attempt_child).await;
                result
            },
            Ok(_) => self.set_to_invalid(task_run_attempt, Some(&mut task_run_attempt_child)).await,
            Err(error) => {
                self.children.insert(task_run_attempt.id, task_run_attempt_child).await;
                Err(error)
            },
        }
    }

    /// Derives a running attempt's next status: its exit status if it has one, else whether
    /// it is past its timeout, else whether its job run was stopped, else still running.
    ///
    /// A real outcome outranks a stop, so an exited process reports its exit status and one
    /// past its timeout reports that, ahead of a stop reaching it on the same pass.
    async fn derive_next_status(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<TaskRunAttemptStatus> {

        if let Some(exit_status) = task_run_attempt_child.child.try_wait()? {
            return Ok(if exit_status.success() {
                TaskRunAttemptStatus::Succeeded
            } else {
                TaskRunAttemptStatus::Failed
            });
        }

        if Utc::now() > task_run_attempt_child.times_out_at {
            return Ok(TaskRunAttemptStatus::TimedOut);
        }

        if self.is_job_run_stopped(task_run_attempt).await? {
            return Ok(TaskRunAttemptStatus::Aborted);
        }

        Ok(TaskRunAttemptStatus::Running)
    }

    /// Settles a row this program cannot account for: either the restart path, where
    /// `TaskRunAttemptChildren` holds no process for it at all, or a row
    /// `derive_next_status` failed to decide or derived as something this match doesn't
    /// handle — unreachable while its checks cover every case. See
    /// `JobRunDispatcher::set_to_invalid` for why it settles rather than raises.
    ///
    /// A held process is killed before the drain, for the reason `set_to_timed_out` gives,
    /// rather than leaked; with none held there is nothing to kill or drain.
    async fn set_to_invalid(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: Option<&mut TaskRunAttemptChild>,
    ) -> anyhow::Result<()> {

        match task_run_attempt_child {
            Some(task_run_attempt_child) => {

                eprintln!(
                    "Task run attempt {} was claimed by no outcome, or its next status could \
                     not be derived. Killing it and settling it invalid. This is a bug.",
                    task_run_attempt.id,
                );

                task_run_attempt_child.kill_process_group().await;

                self.finish_reading(task_run_attempt, task_run_attempt_child).await?;
            },
            None => eprintln!(
                "Task run attempt {} was running with no process to wait on, so its outcome \
                 is unknown and it has been settled invalid. Its command may still be \
                 running.",
                task_run_attempt.id,
            ),
        }

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::Invalid,
            None,
        ).await
    }

    /// Succeeds the attempt whose process `derive_next_status` found exited zero — unless
    /// what it wrote to `$FLOWLITE_TASK_OUTPUT` cannot be recorded, which fails it instead.
    ///
    /// Exiting zero is the command's verdict on itself; the result is a second thing it has
    /// to get right, and a result that cannot be recorded is not a success just because the
    /// process said so. `Failed` rather than `Invalid`: flowlite knows exactly what
    /// happened and said so, and the task's own `max_retries` should get another go.
    async fn set_to_succeeded(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        match self.read_task_output(task_run_attempt) {
            Ok(output) => self.finish_task_run_attempt(
                task_run_attempt,
                TaskRunAttemptStatus::Succeeded,
                Some(output),
            ).await,
            Err(rejection) => {
                // Onto the attempt's own stderr, where whoever is asking why it failed is
                // already looking, and through the one mechanism that already carries
                // flowlite's own words in a stream - the way `StreamCapture` marks the
                // bytes it dropped. `task_run_attempt` has no message column and this does
                // not add one for a single sentence.
                self.insert_output(task_run_attempt, String::new(), rejection).await?;

                self.finish_task_run_attempt(
                    task_run_attempt,
                    TaskRunAttemptStatus::Failed,
                    None,
                ).await
            }
        }
    }

    /// What the command wrote to `$FLOWLITE_TASK_OUTPUT`, or the sentence explaining why it
    /// is not going to be recorded.
    ///
    /// Every refusal is an `Err` rather than an error returned to `Poller`, including the
    /// ones that are really I/O failures: this attempt's process is already gone, so
    /// leaving the row `Running` would have the next pass settle it `Invalid` for want of a
    /// process rather than for the reason that actually happened.
    ///
    /// Bounded rather than truncated. A truncated result is a result something downstream
    /// will parse, and half a document parses as a whole one often enough to matter.
    fn read_task_output(&self, task_run_attempt: &TaskRunAttempt) -> Result<String, String> {

        let job_run_dir = job_run_dir(&self.app_config.data_dir, task_run_attempt.job_run_id)
            .map_err(|error| format!("flowlite: {:#}\n", error))?;

        let path = task_output_path(
            &job_run_dir,
            &task_run_attempt.task_id,
            task_run_attempt.attempt,
        );

        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            // Not an error: a task that produces nothing is the ordinary case, and every
            // task that existed before this channel did is one of them.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
            Err(error) => return Err(format!(
                "flowlite: could not read the result at {}: {}\n",
                path.display(),
                error,
            )),
        };

        let max_bytes = self.app_config.orchestrator.max_task_output_bytes;

        if metadata.len() > max_bytes {
            return Err(format!(
                "flowlite: the result written to $FLOWLITE_TASK_OUTPUT is {} bytes, over \
                 the {} allowed by [orchestrator] max_task_output_bytes. The attempt \
                 failed rather than recording part of it, since a dependent task would \
                 have parsed the part it got as the whole result.\n",
                metadata.len(),
                max_bytes,
            ));
        }

        let bytes = std::fs::read(&path).map_err(|error| format!(
            "flowlite: could not read the result at {}: {}\n",
            path.display(),
            error,
        ))?;

        String::from_utf8(bytes).map_err(|_|
            "flowlite: the result written to $FLOWLITE_TASK_OUTPUT is not valid UTF-8. It \
             is stored as text and travels through --json and the dashboard, so it is \
             refused rather than mangled.\n".to_string()
        )
    }

    /// Fails the attempt whose process `derive_next_status` found exited non-zero. Nothing
    /// is retried here: TaskRunMonitor reads this status and decides whether another
    /// attempt goes in.
    async fn set_to_failed(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::Failed,
            None,
        ).await
    }

    /// Kills the process that ran past its timeout and times the attempt out.
    async fn set_to_timed_out(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        // The kill comes before the drain: killing is what closes the pipes, and a closed
        // pipe is the EOF that ends a reader. Draining first would wait out the whole of
        // the EOF timeout on a process that is still running and still holding them.
        task_run_attempt_child.kill_process_group().await;

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::TimedOut,
            None,
        ).await
    }

    /// Kills the process of a stopped job run and aborts the attempt.
    async fn set_to_aborted(
        &self,
        task_run_attempt: &TaskRunAttempt,
        task_run_attempt_child: &mut TaskRunAttemptChild,
    ) -> anyhow::Result<()> {

        // Killed before drained, for the reason `set_to_timed_out` gives.
        task_run_attempt_child.kill_process_group().await;

        self.finish_reading(task_run_attempt, task_run_attempt_child).await?;

        self.finish_task_run_attempt(
            task_run_attempt,
            TaskRunAttemptStatus::Aborted,
            None,
        ).await
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
                    id: None,
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

    /// Writes the terminal status, and stops the runs the attempt submitted unless it
    /// succeeded. The output already went in.
    ///
    /// Every caller drains through `finish_reading` first, so **an attempt that reads as
    /// terminal has complete output**. Writing the status first would leave a window where
    /// the page shows Succeeded above a truncated log.
    async fn finish_task_run_attempt(
        &self,
        task_run_attempt: &TaskRunAttempt,
        status: TaskRunAttemptStatus,
        output: Option<String>,
    ) -> anyhow::Result<()> {

        // Before the status rather than after it: if this fails, the attempt is still
        // Running with its process gone, so the next pass settles it invalid through here
        // and tries again. Written after, a failure would never be retried.
        if status != TaskRunAttemptStatus::Succeeded {
            let mut conn = self.conn_pool.acquire().await?;
            self.crud.stop_child_job_runs(&mut conn, task_run_attempt).await?;
        }

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                    status: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(status),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    process_group_id: None,
                    output,
                    waiting_since: None,
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
    use crate::test_support::{has_exited, read_pid_file, reading_the_environment};
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

    /// Pins the exit-status check ahead of `Running` in `derive_next_status`. `Running` is
    /// what is left once nothing else claims the attempt, so an exited process has to be
    /// read before it, or this attempt is left Running for ever and the task run never
    /// finishes.
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

    /// Writes `content` where the attempt's command would have written its result, so the
    /// monitor finds it the way it would find a real one.
    fn write_result(db: &TestDb, task_run_attempt: &TaskRunAttempt, content: &[u8]) {

        let path = task_output_path(
            &job_run_dir(&db.app_config().data_dir, task_run_attempt.job_run_id).unwrap(),
            &task_run_attempt.task_id,
            task_run_attempt.attempt,
        );

        std::fs::write(path, content).unwrap();
    }

    /// The stderr the monitor recorded for the attempt, which is where its own refusals go.
    async fn stderr_of(db: &TestDb, task_run_attempt: &TaskRunAttempt) -> String {

        let outputs = db.crud.select_task_run_attempt_outputs(
            &*db.conn_pool,
            &SelectTaskRunAttemptOutputsData {
                filter: SelectTaskRunAttemptOutputsDataFilter {
                    id: None,
                    task_run_attempt_id: Some(task_run_attempt.id),
                    task_run_id: None,
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    stream: Some(TaskRunAttemptOutputStream::Stderr),
                },
                sort: None,
            },
        ).await.unwrap();

        outputs.iter().map(|output| output.content.clone()).collect()
    }

    #[tokio::test]
    async fn a_result_written_by_the_command_is_recorded_on_the_attempt() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        write_result(&db, &task_run_attempt, b"the plan");

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let finished = db.task_run_attempt(task_run_attempt.id).await;

        assert_eq!(finished.status, TaskRunAttemptStatus::Succeeded);
        assert_eq!(finished.output, "the plan");
    }

    /// The ordinary case, and every task that existed before this channel did: producing
    /// no result is not a failure.
    #[tokio::test]
    async fn a_command_that_writes_no_result_still_succeeds() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let finished = db.task_run_attempt(task_run_attempt.id).await;

        assert_eq!(finished.status, TaskRunAttemptStatus::Succeeded);
        assert_eq!(finished.output, "");
    }

    /// Exiting zero is the command's verdict on itself. A result that cannot be recorded
    /// is a second thing it had to get right, and the attempt fails rather than succeeding
    /// with a truncated result a dependent would parse as a whole one.
    #[tokio::test]
    async fn a_result_over_the_bound_fails_the_attempt_rather_than_being_truncated() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        write_result(&db, &task_run_attempt, &[b'x'; 64]);

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor_with_max_task_output_bytes(16)
            .handle(&task_run_attempt)
            .await
            .unwrap();

        let finished = db.task_run_attempt(task_run_attempt.id).await;

        assert_eq!(finished.status, TaskRunAttemptStatus::Failed);
        assert_eq!(finished.output, "");
    }

    /// The row carries no message, so the reason goes where whoever is asking why the
    /// attempt failed is already looking.
    #[tokio::test]
    async fn a_refused_result_says_so_on_the_attempts_stderr() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        write_result(&db, &task_run_attempt, &[b'x'; 64]);

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor_with_max_task_output_bytes(16)
            .handle(&task_run_attempt)
            .await
            .unwrap();

        let stderr = stderr_of(&db, &task_run_attempt).await;

        assert!(stderr.contains("64 bytes"), "{stderr}");
        assert!(stderr.contains("max_task_output_bytes"), "{stderr}");
    }

    /// A result exactly at the bound is within it: the refusal is for what is over.
    #[tokio::test]
    async fn a_result_the_size_of_the_bound_is_recorded() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        write_result(&db, &task_run_attempt, &[b'x'; 16]);

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor_with_max_task_output_bytes(16)
            .handle(&task_run_attempt)
            .await
            .unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Succeeded,
        );
    }

    /// The column is TEXT and the value travels through --json and the dashboard, so bytes
    /// that are not text are refused rather than mangled into replacement characters.
    #[tokio::test]
    async fn a_result_that_is_not_utf8_fails_the_attempt() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;

        write_result(&db, &task_run_attempt, &[0xff, 0xfe, 0xfd]);

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        let finished = db.task_run_attempt(task_run_attempt.id).await;

        assert_eq!(finished.status, TaskRunAttemptStatus::Failed);

        let stderr = stderr_of(&db, &task_run_attempt).await;

        assert!(stderr.contains("UTF-8"), "{stderr}");
    }

    /// The file is named for the attempt, so a retry that writes nothing cannot be
    /// credited with the result of the attempt it replaced.
    #[tokio::test]
    async fn a_retry_does_not_inherit_the_previous_attempts_result() {
        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, TaskRunStatus::Running).await;

        let first = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;
        write_result(&db, &first, b"a wrong plan");
        db.spawn_exited_child(&first, "exit 1", Utc::now() + TimeDelta::seconds(3600)).await;
        db.task_run_attempt_monitor().handle(&first).await.unwrap();

        let second = db.insert_task_run_attempt(&task_run, 2, TaskRunAttemptStatus::Running).await;
        db.spawn_exited_child(&second, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;
        db.task_run_attempt_monitor().handle(&second).await.unwrap();

        assert_eq!(db.task_run_attempt(second.id).await.output, "");
    }

    /// The same for a non-zero exit, the other half of the exit status.
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

    /// Pins the timeout check ahead of `Running` in `derive_next_status`.
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

    /// Pins the stop check ahead of `Running` in `derive_next_status`, and behind the exit
    /// status and timeout checks: a stop kills a process that is still going, but does not
    /// outrank an outcome the process reached on its own.
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
    /// The parent is aborted by the stop, and a run it submitted and was waiting on goes
    /// with it.
    #[tokio::test]
    async fn a_stopped_attempt_stops_the_runs_it_submitted() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;
        let child = db.insert_child_job_run(&task_run_attempt, JobRunStatus::Running).await;

        db.insert_job_run_stop(task_run_attempt.job_run_id).await;
        db.spawn_running_child(
            &task_run_attempt,
            "sleep 30",
            Utc::now() + TimeDelta::seconds(3600),
        ).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(db.job_run_stop_count(child.id).await, 1);
    }

    /// A retry submits children of its own, so a failed attempt's must not run beside them.
    #[tokio::test]
    async fn a_failed_attempt_stops_the_runs_it_submitted() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;
        let child = db.insert_child_job_run(&task_run_attempt, JobRunStatus::Running).await;

        db.spawn_exited_child(&task_run_attempt, "exit 1", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(db.task_run_attempt(task_run_attempt.id).await.status, TaskRunAttemptStatus::Failed);
        assert_eq!(db.job_run_stop_count(child.id).await, 1);
    }

    /// Submitting without waiting and exiting 0 hands the child off.
    #[tokio::test]
    async fn a_succeeded_attempt_leaves_the_runs_it_submitted_running() {
        let db = TestDb::new().await;
        let task_run_attempt = running_attempt(&db).await;
        let child = db.insert_child_job_run(&task_run_attempt, JobRunStatus::Running).await;

        db.spawn_exited_child(&task_run_attempt, "exit 0", Utc::now() + TimeDelta::seconds(3600)).await;

        db.task_run_attempt_monitor().handle(&task_run_attempt).await.unwrap();

        assert_eq!(db.task_run_attempt(task_run_attempt.id).await.status, TaskRunAttemptStatus::Succeeded);
        assert_eq!(db.job_run_stop_count(child.id).await, 0);
    }

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

    /// Pins the exit-status check ahead of the timeout check in `derive_next_status`: a
    /// process that got there on its own before the poll pass noticed the deadline reports
    /// what it exited with, rather than being recorded as killed by a timeout that never
    /// killed it.
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

    /// Pins the timeout check ahead of the stop check in `derive_next_status`, the other
    /// half of "a real outcome outranks a stop": a process past its deadline reports the
    /// timeout even though its job run was stopped and it was killed on this same pass.
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

        let _environment = reading_the_environment();
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
            TaskRunAttemptStatus::Queued,
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

        let _environment = reading_the_environment();
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
            TaskRunAttemptStatus::Queued,
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
                    id: None,
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

        // Closing the pool fails the stop lookup in `derive_next_status`, which is the
        // first check that asks the database anything for a process that is still running
        // and not yet past its deadline.
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
            .expect("the Running arm must put the process back for the next pass");

        handed_back.child.kill().await.unwrap();
    }

    /// The cap must not stop the reader reading. The pipe is 64 KiB, so a task writing past
    /// the cap blocks on a full one the moment recording stops, and the attempt then never
    /// finishes at all. Two megabytes through a one-megabyte cap is the shape of that bug,
    /// and it fails by hanging rather than by a wrong value.
    #[tokio::test]
    async fn a_task_over_the_cap_still_finishes() {

        let _environment = reading_the_environment();
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
            TaskRunAttemptStatus::Queued,
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
        let max_stream_bytes = crate::app_config::AppConfig::default().orchestrator.max_stream_bytes;

        assert!(
            stdout.contains(&format!("[flowlite: over {max_stream_bytes} bytes")),
            "the cap did not report itself",
        );
        assert!(
            stdout.contains("bytes dropped]"),
            "the tail was never flushed",
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

    /// Unreachable while `derive_next_status` covers every case, so it is called directly.
    /// Unlike the other four writers this one holds a live process, so settling the row
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

        db.task_run_attempt_monitor().set_to_invalid(&task_run_attempt, Some(&mut child)).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );

        assert!(has_exited(pid).await, "the process was left running");
    }
}
