use std::process::Stdio;
use std::sync::Arc;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::job_run_stop::{SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run::{SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus, UpdateTaskRunAttemptsData, UpdateTaskRunAttemptsDataFilter, UpdateTaskRunAttemptsDataInput};
use crate::crud::task_run_attempt_output::TaskRunAttemptOutputStream;
use crate::orchestrator::task_run_attempt_children::{TaskRunAttemptChild, TaskRunAttemptChildren};
use crate::orchestrator::task_run_attempt_env::build_task_run_attempt_env;
use crate::app_config::AppConfig;
use crate::orchestrator::task_run_attempt_reader::read_task_run_attempt_stream;
use crate::poller::Service;
use crate::signals::Signals;
use anyhow::Context;
use chrono::{TimeDelta, Utc};


/// Picks up pending task run attempts and settles each one as skipped, or as running by
/// spawning its command. Hands the child process to TaskRunAttemptMonitor through
/// TaskRunAttemptChildren, and the attempt itself through its status, never by calling
/// it.
pub struct TaskRunAttemptDispatcher {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub children: Arc<TaskRunAttemptChildren>,
    pub signals: Arc<Signals>,
    pub app_config: AppConfig,
}


impl TaskRunAttemptDispatcher {

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

    /// Settles a pending attempt as exactly one outcome. `settle_as_pending` has to precede
    /// `settle_as_running`, which spawns unconditionally and would start a retry the moment
    /// it was inserted.
    async fn handle_pending_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        if self.settle_as_invalid(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_skipped(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_pending(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_running(task_run_attempt).await? {
            return Ok(());
        }

        self.settle_unclaimed(task_run_attempt).await
    }

    /// Settles a row no outcome claimed. Unreachable while `settle_as_running` claims
    /// unconditionally; see `JobRunDispatcher::settle_unclaimed` for why it settles rather
    /// than raises. Nothing has been spawned by the time the chain falls this far, so there
    /// is no process to account for.
    async fn settle_unclaimed(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        eprintln!(
            "Task run attempt {} was claimed by no outcome: its job run was not stopped and \
             it was not started. Settling it invalid. This is a bug.",
            task_run_attempt.id,
        );

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Invalid),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    process_group_id: None,
                },
            },
        ).await?;

        self.signals.publish();

        Ok(())
    }

    /// Settles a pending attempt a spawn was already begun for, which only a crash between
    /// the spawn and the Running write leaves behind. Its command may be running, so
    /// starting it again would run the command twice — worse than an unknown outcome.
    ///
    /// Asked first: a stop would otherwise skip it, claiming nothing ran.
    async fn settle_as_invalid(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        if task_run_attempt.started_at.is_none() {
            return Ok(false);
        }

        eprintln!(
            "Task run attempt {} was already spawned for but never recorded as running, so \
             its command may have run and it has been settled invalid rather than started \
             a second time",
            task_run_attempt.id,
        );

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Invalid),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    process_group_id: None,
                },
            },
        ).await?;

        self.signals.publish();

        Ok(true)
    }

    /// Skips the attempt if its job run was stopped, so its command never started.
    async fn settle_as_skipped(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let job_run_stopped = self.is_job_run_stopped(task_run_attempt).await?;

        if !job_run_stopped {
            return Ok(false);
        }

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Skipped),
                    started_at: None,
                    finished_at: Some(Some(Utc::now())),
                    process_group_id: None,
                },
            },
        ).await?;

        self.signals.publish();

        Ok(true)
    }

    /// Leaves the attempt pending for any of three reasons: its retry_delay has yet to
    /// pass since it was created — which is when TaskRunMonitor decided to retry — the
    /// global cap on running attempts is already full, or a named limit its task run
    /// claims is already full.
    ///
    /// The delay is checked first: only a retry waits on it (attempt 1 has nothing to wait
    /// for, so it never reaches the task run query below), and it costs no query at all
    /// for the common case of a first attempt. The cap, once reached, applies to every
    /// attempt regardless of retry state. The named-limit check runs last, in
    /// `a_claimed_limit_is_full`.
    async fn settle_as_pending(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        if task_run_attempt.attempt > 1 {
            let task_run = self.get_task_run(task_run_attempt).await?;

            let retry_delay = TimeDelta::seconds(task_run.retry_delay as i64);

            if Utc::now() < task_run_attempt.created_at + retry_delay {
                return Ok(true);
            }
        }

        if self.app_config.orchestrator.max_running_attempts > 0 {
            let mut conn = self.conn_pool.acquire().await?;

            let running_attempts = self.crud.count_running_attempts(&mut conn).await?;

            if running_attempts >= self.app_config.orchestrator.max_running_attempts {
                return Ok(true);
            }
        }

        self.a_claimed_limit_is_full(task_run_attempt).await
    }

    /// The third and last reason `settle_as_pending` leaves a row pending: one of the
    /// named limits its task run claims (`task_run.limits`, the job+task union `submit_job`
    /// snapshotted) is already at its configured maximum.
    ///
    /// A claimed name absent from `self.app_config.concurrency_limits` is treated as
    /// unlimited rather than blocked. Startup validation rejects an unconfigured name at
    /// submit time, so this can only arise when the config changed after the run was
    /// already submitted - blocking here would stall that run forever with no way out, the
    /// exact failure the `Invalid` status exists to remove.
    ///
    /// The warning for such a name is emitted only once the attempt is admitted, not while
    /// deciding. An attempt claiming both an unconfigured name and a full one is asked
    /// again on every poll pass, so warning as each name was examined printed the same line
    /// once a second for as long as the full limit was held - a smaller version of the
    /// stall this method exists to avoid. Emitting after the decision means exactly one
    /// line per attempt, on the pass it actually starts.
    async fn a_claimed_limit_is_full(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let task_run = self.get_task_run(task_run_attempt).await?;

        if task_run.limits.0.is_empty() {
            return Ok(false);
        }

        let mut conn = self.conn_pool.acquire().await?;

        let claimed_limit_slots = self.crud.claimed_limit_slots(&mut conn).await?;

        let mut unconfigured = Vec::new();

        for limit in &task_run.limits.0 {

            let Some(configured_max) = self.app_config.concurrency_limits.get(limit) else {
                unconfigured.push(limit);
                continue;
            };

            if *configured_max == 0 {
                continue;
            }

            let claimed = claimed_limit_slots.get(limit).copied().unwrap_or(0);

            if claimed >= *configured_max {
                return Ok(true);
            }
        }

        for limit in unconfigured {
            eprintln!(
                "Task run attempt {} claims limit '{}', which is not in \
                 [concurrency_limits]. Treating it as unlimited rather than blocking \
                 it - this can only happen if the config changed after its run was \
                 submitted.",
                task_run_attempt.id, limit,
            );
        }

        Ok(false)
    }

    /// Spawns the command of the attempt, hands the child process over and sets the
    /// attempt to running, which is what makes TaskRunAttemptMonitor pick it up.
    ///
    /// The child has to be in TaskRunAttemptChildren before the status is written, or the
    /// monitor sees a running attempt with no process and settles it Invalid.
    async fn settle_as_running(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let task_run = self.get_task_run(task_run_attempt).await?;
        let job_run = self.get_job_run(task_run_attempt).await?;

        let env = build_task_run_attempt_env(
            &task_run,
            &job_run,
            task_run_attempt,
            &self.app_config.data_dir,
            &self.app_config.secrets,
        )?;

        let mut command = tokio::process::Command::new("sh");

        // The child's FLOWLITE_ namespace belongs to flowlite, and is stated rather than
        // inherited. `Command::envs` is an overlay - it cannot unset what this server was
        // started with - so a credential passed the way the README says to pass one,
        // FLOWLITE_SMTP__PASSWORD=... flowlite serve, would otherwise be readable by every
        // command flowlite spawns. Stripped before the overlay is applied, so the names
        // build_task_run_attempt_env means a command to have are put back by it.
        //
        // `vars_os`, not `vars`: an environment is bytes, and `vars` panics on a name or
        // value it cannot decode as UTF-8. This walk is in the spawn path, so one
        // undecodable variable anywhere in the server's environment - nothing to do with
        // flowlite - would turn every task spawn into a panic. The prefix is ASCII, so
        // matching it against the raw bytes needs no decoding at all.
        for (name, _) in std::env::vars_os() {
            if name.as_encoded_bytes().starts_with(b"FLOWLITE_") {
                command.env_remove(&name);
            }
        }

        command
            .arg("-c")
            .arg(&task_run.command)
            .envs(&env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own process group, so a timeout or a stop can signal the command's whole
            // process tree rather than only the sh that flowlite spawned. The group id is
            // this child's pid; TaskRunAttemptMonitor kills by it.
            .process_group(0);

        // Empty means inherit the server's, which is what Command does when nothing is set.
        if !task_run.working_dir.is_empty() {
            command.current_dir(&task_run.working_dir);
        }

        let working_dir_description = if task_run.working_dir.is_empty() {
            "the server's current directory".to_string()
        } else {
            format!("'{}'", task_run.working_dir)
        };

        let started_at = Utc::now();

        // Recorded before the spawn, not after: a crash between the two leaves a pending
        // attempt whose command is running, and `settle_as_invalid` reads this to refuse to
        // start it again.
        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: None,
                    started_at: Some(Some(started_at)),
                    finished_at: None,
                    process_group_id: None,
                },
            },
        ).await?;

        let spawned = command.spawn()
            .with_context(|| format!(
                "Failed to spawn task '{}' in working directory {}",
                task_run.task_id,
                working_dir_description,
            ));

        // Nothing was started, so the intent has to go: left behind it would settle this
        // attempt invalid on the next pass instead of letting it be tried again.
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => {
                self.crud.update_task_run_attempts(
                    &*self.conn_pool,
                    &UpdateTaskRunAttemptsData {
                        filter: UpdateTaskRunAttemptsDataFilter {
                            id: Some(task_run_attempt.id),
                            task_run_id: None,
                        },
                        input: UpdateTaskRunAttemptsDataInput {
                            status: None,
                            started_at: Some(None),
                            finished_at: None,
                            process_group_id: None,
                        },
                    },
                ).await?;

                return Err(error);
            },
        };

        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stdout of task: {}", task_run_attempt.task_id))?;
        let stderr = child.stderr.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stderr of task: {}", task_run_attempt.task_id))?;

        let times_out_at = started_at + TimeDelta::seconds(task_run.timeout as i64);

        let (chunks_sender, chunks) = tokio::sync::mpsc::unbounded_channel();

        let readers = [
            tokio::spawn(read_task_run_attempt_stream(
                stdout,
                TaskRunAttemptOutputStream::Stdout,
                chunks_sender.clone(),
                self.app_config.clone(),
            )),
            // The original sender moves in here rather than being kept: the channel closes
            // when the last sender drops, and that close is how TaskRunAttemptMonitor knows
            // both readers reached EOF. A clone held back here would mean it never closes,
            // and every terminal pass would wait out its whole EOF timeout.
            tokio::spawn(read_task_run_attempt_stream(
                stderr,
                TaskRunAttemptOutputStream::Stderr,
                chunks_sender,
                self.app_config.clone(),
            )),
        ];

        // The group id is this child's pid, `process_group(0)` having made it a group
        // leader. Read before the child moves into the map, where ownership of it ends.
        let process_group_id = child.id().map(|pid| pid as i64);

        let running_task_run_attempt = TaskRunAttemptChild {
            child,
            chunks,
            readers,
            times_out_at,
        };

        self.children.insert(task_run_attempt.id, running_task_run_attempt).await;

        self.crud.update_task_run_attempts(
            &*self.conn_pool,
            &UpdateTaskRunAttemptsData {
                filter: UpdateTaskRunAttemptsDataFilter {
                    id: Some(task_run_attempt.id),
                    task_run_id: None,
                },
                input: UpdateTaskRunAttemptsDataInput {
                    status: Some(TaskRunAttemptStatus::Running),
                    started_at: None,
                    finished_at: None,
                    process_group_id: Some(process_group_id),
                },
            },
        ).await?;

        self.signals.publish();

        Ok(true)
    }

    async fn get_pending_task_run_attempts(&self) -> anyhow::Result<Vec<TaskRunAttempt>> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: None,
                    job_run_id: None,
                    task_id: None,
                    status: Some(TaskRunAttemptStatus::Pending),
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Id),
            }
        ).await

    }

    /// Loads the task run the attempt belongs to, for the command and timeout it was
    /// submitted with. The config is read off the run rather than out of mem.task, so an
    /// attempt spawns what its run was submitted with however the YAML has moved since.
    async fn get_task_run(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<TaskRun> {

        self.crud.select_task_run(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: Some(task_run_attempt.task_run_id),
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            }
        )
            .await?
            .ok_or_else(|| anyhow::anyhow!("Task run not found: {}", task_run_attempt.task_run_id))
    }

    /// Loads the job run the attempt belongs to, for the parameters and the scheduled
    /// instant the run was submitted with. Read off the run rather than out of config, so
    /// an attempt receives what its run was submitted with however the YAML has moved.
    async fn get_job_run(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<JobRun> {

        self.crud.select_job_run(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: Some(task_run_attempt.job_run_id),
                    job_id: None,
                    status: None,
                },
                sort: None,
                limit: Some(1),
                offset: None,
            }
        )
            .await?
            .ok_or_else(|| anyhow::anyhow!("Job run not found: {}", task_run_attempt.job_run_id))
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

}


impl Service for TaskRunAttemptDispatcher {
    type Row = TaskRunAttempt;

    fn name(&self) -> &'static str {
        "Task Run Attempt Dispatcher"
    }

    fn row_context(&self, task_run_attempt: &TaskRunAttempt) -> String {
        format!(
            "task run attempt {} of task run {}",
            task_run_attempt.id,
            task_run_attempt.task_run_id,
        )
    }

    async fn select(&self) -> anyhow::Result<Vec<TaskRunAttempt>> {
        self.get_pending_task_run_attempts().await
    }

    async fn handle(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {
        self.handle_pending_task_run_attempt(task_run_attempt).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::JobRunStatus;
    use crate::test_support::TestDb;
    use crate::test_support::read_command_file;
    use crate::test_support::{reading_the_environment, writing_the_environment};
    use std::collections::BTreeMap;
    use std::os::unix::ffi::OsStringExt;

    /// Calls `settle_as_pending` rather than the whole chain on purpose: falling through it
    /// spawns a real process, which is what these tests are about avoiding.
    async fn is_waiting_to_retry(attempt: u32, retry_delay: u32, created_ago: i64) -> bool {

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, 2, retry_delay).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            attempt,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.backdate_task_run_attempt(
            task_run_attempt.id,
            Utc::now() - TimeDelta::seconds(created_ago),
        ).await;

        let task_run_attempt = db.task_run_attempts(task_run.id).await.pop().unwrap();

        db.task_run_attempt_dispatcher()
            .settle_as_pending(&task_run_attempt)
            .await
            .unwrap()
    }

    /// Attempt 1 has no failure behind it, so it waits out no retry_delay however long
    /// the task run's is.
    #[tokio::test]
    async fn the_first_attempt_never_waits() {
        assert!(!is_waiting_to_retry(1, 60, 0).await);
    }

    #[tokio::test]
    async fn the_retry_waits_while_the_delay_has_not_passed() {
        assert!(is_waiting_to_retry(2, 60, 10).await);
    }

    #[tokio::test]
    async fn the_retry_starts_once_the_delay_has_passed() {
        assert!(!is_waiting_to_retry(2, 60, 61).await);
    }

    #[tokio::test]
    async fn a_retry_delay_of_zero_does_not_wait() {
        assert!(!is_waiting_to_retry(2, 0, 0).await);
    }

    /// Runs the whole chain over a pending attempt and reports what it settled it as.
    ///
    /// Unlike `is_waiting_to_retry` this lets `settle_as_running` spawn, which is the
    /// point: the order of the chain is only observable when the outcome that starts a
    /// process is actually reachable.
    async fn settled_attempt_status(
        attempt: u32,
        retry_delay: u32,
        stop_the_job_run: bool,
    ) -> TaskRunAttemptStatus {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, 2, retry_delay).await;

        if stop_the_job_run {
            db.insert_job_run_stop(job_run.id).await;
        }

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            attempt,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        db.task_run_attempt(task_run_attempt.id).await.status
    }

    /// Pins `settle_as_pending` ahead of `settle_as_running`. Swap them and the retry is
    /// spawned the moment TaskRunMonitor inserts it, and the retry_delay never applies.
    #[tokio::test]
    async fn a_retry_inside_its_delay_is_left_pending_rather_than_started() {
        let status = settled_attempt_status(2, 60, false).await;

        assert_eq!(status, TaskRunAttemptStatus::Pending);
    }

    /// Pins `settle_as_skipped` ahead of `settle_as_running`. Swap them and a stopped job
    /// run still spawns the command it was stopped to prevent.
    #[tokio::test]
    async fn a_stopped_job_run_skips_the_attempt_rather_than_starting_it() {
        let status = settled_attempt_status(1, 0, true).await;

        assert_eq!(status, TaskRunAttemptStatus::Skipped);
    }

    /// Pins `settle_as_skipped` ahead of `settle_as_pending`. Swap them and a retry still
    /// inside its delay is held pending by a job run that was stopped, instead of skipped,
    /// so the stop does not take effect until the delay expires.
    #[tokio::test]
    async fn a_stopped_job_run_skips_a_retry_that_is_still_inside_its_delay() {
        let status = settled_attempt_status(2, 60, true).await;

        assert_eq!(status, TaskRunAttemptStatus::Skipped);
    }

    #[tokio::test]
    async fn an_attempt_past_its_delay_is_started() {
        let status = settled_attempt_status(1, 0, false).await;

        assert_eq!(status, TaskRunAttemptStatus::Running);
    }

    /// Runs the chain over a pending attempt on its own task run while a second task
    /// run's attempt already sits Running, and reports what the pending one settled as -
    /// the global cap counts across every job, so the two task runs are unrelated on
    /// purpose.
    async fn settled_attempt_status_with_one_running(max_running_attempts: u32) -> TaskRunAttemptStatus {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let running_job_run = db.insert_job_run(JobRunStatus::Running).await;
        let running_task_run = db.insert_retryable_task_run(running_job_run.id, 0, 0).await;
        db.insert_task_run_attempt(&running_task_run, 1, TaskRunAttemptStatus::Running).await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, 0, 0).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.task_run_attempt_dispatcher_with_max_running_attempts(max_running_attempts)
            .handle(&task_run_attempt)
            .await
            .unwrap();

        db.task_run_attempt(task_run_attempt.id).await.status
    }

    /// The gate this task adds: a cap of 1 is already spent by the other task run's
    /// Running attempt, so this one is left pending rather than spawned.
    #[tokio::test]
    async fn a_pending_attempt_stays_pending_while_the_global_cap_is_full() {
        let status = settled_attempt_status_with_one_running(1).await;

        assert_eq!(status, TaskRunAttemptStatus::Pending);
    }

    /// 0 is "no limit" - the same Running attempt that fills a cap of 1 does not hold
    /// this one back at all.
    #[tokio::test]
    async fn a_cap_of_zero_does_not_hold_attempts_back() {
        let status = settled_attempt_status_with_one_running(0).await;

        assert_eq!(status, TaskRunAttemptStatus::Running);
    }

    /// The retry-delay check does not regress now that a second reason keeps a row
    /// pending: a retry still inside its delay stays pending even with no cap at all to
    /// blame it on.
    #[tokio::test]
    async fn a_retry_inside_its_delay_stays_pending_even_with_no_cap() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_retryable_task_run(job_run.id, 2, 60).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            2,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher_with_max_running_attempts(0)
            .handle(&task_run_attempt)
            .await
            .unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Pending,
        );
    }

    /// Runs the chain over a pending attempt claiming "warehouse", with `running_claimants`
    /// other task runs' attempts already Running and also claiming "warehouse", and
    /// reports what the pending one settled as. `configured_max` of `None` leaves
    /// "warehouse" out of `[concurrency_limits]` entirely - the unconfigured-name case.
    async fn settled_attempt_status_with_a_claimed_limit(
        running_claimants: u32,
        configured_max: Option<u32>,
    ) -> TaskRunAttemptStatus {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        for _ in 0..running_claimants {
            let job_run = db.insert_job_run(JobRunStatus::Running).await;
            let task_run = db.insert_task_run_with_limits(job_run.id, vec!["warehouse".to_string()]).await;
            db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Running).await;
        }

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_with_limits(job_run.id, vec!["warehouse".to_string()]).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        let concurrency_limits = match configured_max {
            Some(max) => BTreeMap::from([("warehouse".to_string(), max)]),
            None => BTreeMap::new(),
        };

        db.task_run_attempt_dispatcher_with_concurrency_limits(concurrency_limits)
            .handle(&task_run_attempt)
            .await
            .unwrap();

        db.task_run_attempt(task_run_attempt.id).await.status
    }

    /// The gate this task adds: a named limit configured to 1 is already spent by another
    /// task run's Running attempt claiming the same name, so a claimant of it is left
    /// pending - while a task run claiming nothing at all still starts in the same pass,
    /// proving the check is per-attempt rather than a pass-wide freeze.
    #[tokio::test]
    async fn a_claimant_of_a_full_limit_stays_pending_while_a_non_claimant_starts() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let running_job_run = db.insert_job_run(JobRunStatus::Running).await;
        let running_task_run = db.insert_task_run_with_limits(running_job_run.id, vec!["warehouse".to_string()]).await;
        db.insert_task_run_attempt(&running_task_run, 1, TaskRunAttemptStatus::Running).await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let claimant_task_run = db.insert_task_run_with_limits(job_run.id, vec!["warehouse".to_string()]).await;
        let claimant_attempt = db.insert_task_run_attempt(&claimant_task_run, 1, TaskRunAttemptStatus::Pending).await;

        let non_claimant_task_run = db.insert_task_run_with_limits(job_run.id, Vec::new()).await;
        let non_claimant_attempt = db.insert_task_run_attempt(&non_claimant_task_run, 1, TaskRunAttemptStatus::Pending).await;

        let dispatcher = db.task_run_attempt_dispatcher_with_concurrency_limits(
            BTreeMap::from([("warehouse".to_string(), 1)]),
        );

        dispatcher.handle(&claimant_attempt).await.unwrap();
        dispatcher.handle(&non_claimant_attempt).await.unwrap();

        assert_eq!(db.task_run_attempt(claimant_attempt.id).await.status, TaskRunAttemptStatus::Pending);
        assert_eq!(db.task_run_attempt(non_claimant_attempt.id).await.status, TaskRunAttemptStatus::Running);
    }

    /// A task run claiming several names is blocked by ANY of them being full, not only by
    /// the first one examined. Without this, narrowing the loop to return its last
    /// comparison - a plausible simplification - would let a task claiming one free name and
    /// one full name run, escaping the full one.
    #[tokio::test]
    async fn a_claim_on_two_names_is_blocked_when_either_one_is_full() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let running_job_run = db.insert_job_run(JobRunStatus::Running).await;
        let holder_task_run = db.insert_task_run_with_limits(running_job_run.id, vec!["warehouse".to_string()]).await;
        db.insert_task_run_attempt(&holder_task_run, 1, TaskRunAttemptStatus::Running).await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        // The free name comes first, so a loop that stopped at its own verdict would admit
        // this attempt and never look at the full one.
        let claimant_task_run = db.insert_task_run_with_limits(
            job_run.id,
            vec!["openai_api".to_string(), "warehouse".to_string()],
        ).await;
        let claimant_attempt = db.insert_task_run_attempt(&claimant_task_run, 1, TaskRunAttemptStatus::Pending).await;

        let dispatcher = db.task_run_attempt_dispatcher_with_concurrency_limits(
            BTreeMap::from([
                ("openai_api".to_string(), 5),
                ("warehouse".to_string(), 1),
            ]),
        );

        dispatcher.handle(&claimant_attempt).await.unwrap();

        assert_eq!(db.task_run_attempt(claimant_attempt.id).await.status, TaskRunAttemptStatus::Pending);
    }

    /// 0 is "no limit" for a named limit too, the same as the global cap.
    #[tokio::test]
    async fn a_named_limit_configured_to_zero_does_not_block() {
        let status = settled_attempt_status_with_a_claimed_limit(1, Some(0)).await;

        assert_eq!(status, TaskRunAttemptStatus::Running);
    }

    /// A claimed name absent from `[concurrency_limits]` can only happen when config
    /// changed after the run was submitted - startup validation would otherwise have
    /// rejected it. Treated as unlimited rather than blocked: blocking would stall the run
    /// forever with no way out, the exact failure the `Invalid` status was built to remove.
    #[tokio::test]
    async fn a_claim_on_a_name_absent_from_config_runs() {
        let status = settled_attempt_status_with_a_claimed_limit(3, None).await;

        assert_eq!(status, TaskRunAttemptStatus::Running);
    }

    /// A claim that has not yet reached its configured maximum runs.
    #[tokio::test]
    async fn a_claim_under_its_limit_runs() {
        let status = settled_attempt_status_with_a_claimed_limit(1, Some(2)).await;

        assert_eq!(status, TaskRunAttemptStatus::Running);
    }

    /// Follows a parameter, a task env value and an injected id all the way into the
    /// process, through a real spawn. The pure tests pin the composition; this pins that
    /// the composed map actually reaches the command.
    #[tokio::test]
    async fn the_composed_environment_reaches_the_command() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run_with_parameters(
            JobRunStatus::Running,
            [("region".to_string(), "us".to_string())].into_iter().collect(),
            None,
        ).await;

        let seen_path = db.data_dir().join("seen.txt");

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            &format!(
                "printf '%s' \"$FLOWLITE_PARAM_REGION $PYTHONUNBUFFERED $FLOWLITE_JOB_RUN_ID\" > {}",
                seen_path.display(),
            ),
            [("PYTHONUNBUFFERED".to_string(), "1".to_string())].into_iter().collect(),
            "",
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher()
            .handle(&task_run_attempt)
            .await
            .unwrap();

        assert_eq!(
            read_command_file(&seen_path).await,
            format!("us 1 {}", job_run.id),
        );
    }

    /// The leak regression. A secret's value is meant to exist in exactly one place: the
    /// environment handed to one spawned `sh`. This proves both halves of that at once -
    /// the resolved value reaches the real command, and it appears nowhere in the
    /// `task_run` row this attempt was submitted with, read back through CRUD the way any
    /// other reader of that table would see it.
    ///
    /// This is the test that would catch a future refactor moving resolution back to
    /// submit time - if a secret's value were ever written into the row instead of
    /// resolved only at spawn, the row's JSON would carry it and this assertion would fail.
    #[tokio::test]
    async fn a_secret_reaches_the_command_but_never_the_stored_row() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let seen_path = db.data_dir().join("seen.txt");

        let task_run = db.insert_task_run_for_command_with_secret_env(
            job_run.id,
            &format!("printf '%s' \"$WAREHOUSE_PW\" > {}", seen_path.display()),
            [("WAREHOUSE_PW".to_string(), "warehouse_pw".to_string())].into_iter().collect(),
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher_with_secrets(
            [("warehouse_pw".to_string(), "hunter2".to_string())].into_iter().collect(),
        )
            .handle(&task_run_attempt)
            .await
            .unwrap();

        assert_eq!(read_command_file(&seen_path).await, "hunter2");

        let stored_row = db.task_run(task_run.id).await;
        let stored_json = serde_json::to_string(&stored_row).unwrap();

        assert!(
            !stored_json.contains("hunter2"),
            "the secret value leaked into the stored task_run row: {stored_json}",
        );
    }

    /// A spawn's child inherits flowlite's own environment, and `Command::envs` is an
    /// overlay that cannot unset what was inherited. So a credential passed to the server
    /// the way the README says to pass it - `FLOWLITE_SMTP__PASSWORD=... flowlite serve` -
    /// would otherwise be readable by every command flowlite spawns, whether or not that
    /// command has anything to do with mail.
    #[tokio::test]
    async fn a_command_cannot_read_flowlites_own_environment() {

        let _environment = writing_the_environment();

        unsafe {
            std::env::set_var("FLOWLITE_SMTP__PASSWORD", "hunter2");
            std::env::set_var("FLOWLITE_SECRETS__WAREHOUSE_PW", "hunter3");
        }

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let seen_path = db.data_dir().join("seen.txt");

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            &format!(
                "printf '%s|%s' \"$FLOWLITE_SMTP__PASSWORD\" \"$FLOWLITE_SECRETS__WAREHOUSE_PW\" > {}",
                seen_path.display(),
            ),
            std::collections::BTreeMap::new(),
            "",
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher()
            .handle(&task_run_attempt)
            .await
            .unwrap();

        let seen = read_command_file(&seen_path).await;

        // Before the assert, so a failure does not leave them set for whatever runs next.
        unsafe {
            std::env::remove_var("FLOWLITE_SMTP__PASSWORD");
            std::env::remove_var("FLOWLITE_SECRETS__WAREHOUSE_PW");
        }

        assert_eq!(seen, "|");
    }

    /// The strip walks the whole environment, and an environment is bytes rather than
    /// UTF-8 - `std::env::vars()` panics on a variable it cannot decode. Nothing about
    /// such a variable concerns flowlite, but the walk is in the spawn path, so one of
    /// them anywhere in the server's environment would turn every task spawn into a
    /// panic rather than a failed task.
    #[tokio::test]
    async fn a_non_utf8_variable_in_the_servers_environment_does_not_stop_a_spawn() {

        let _environment = writing_the_environment();

        // A name no valid UTF-8 can spell. Not FLOWLITE_-prefixed: the walk reads every
        // name before it looks at the prefix, so any one of them is enough.
        let name = std::ffi::OsString::from_vec(b"NOT_UTF8_\xff".to_vec());

        unsafe { std::env::set_var(&name, "x") };

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let seen_path = db.data_dir().join("seen.txt");

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            &format!("printf ok > {}", seen_path.display()),
            std::collections::BTreeMap::new(),
            "",
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        let handled = db.task_run_attempt_dispatcher().handle(&task_run_attempt).await;

        // Before the assert, so a failure does not leave it set for whatever runs next.
        unsafe { std::env::remove_var(&name) };

        handled.unwrap();

        assert_eq!(read_command_file(&seen_path).await, "ok");
    }

    /// The companion of the strip: the data directory is the one FLOWLITE_ variable a task
    /// command has a reason to read, since a command that calls flowlite itself needs it,
    /// so it is injected instead of inherited.
    #[tokio::test]
    async fn the_data_dir_reaches_the_command() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let seen_path = db.data_dir().join("seen.txt");

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            &format!(
                "printf '%s' \"$FLOWLITE_DATA_DIR\" > {}",
                seen_path.display(),
            ),
            std::collections::BTreeMap::new(),
            "",
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher()
            .handle(&task_run_attempt)
            .await
            .unwrap();

        assert_eq!(
            read_command_file(&seen_path).await,
            db.data_dir().to_string_lossy(),
        );
    }

    /// The behaviour the `env_remove("FLOWLITE_SCHEDULED_AT")` special case used to carry
    /// on its own, kept honest while the strip takes it over: a manual run must not read a
    /// scheduled instant out of flowlite's own environment.
    #[tokio::test]
    async fn a_manual_run_ignores_a_forged_scheduled_at_in_the_environment() {

        let _environment = writing_the_environment();

        unsafe { std::env::set_var("FLOWLITE_SCHEDULED_AT", "1999-01-01T00:00:00Z") };

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let seen_path = db.data_dir().join("seen.txt");

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            &format!(
                // Bracketed because read_command_file waits for a non-empty file, and
                // what this test asserts is that the value is empty.
                "printf '[%s]' \"$FLOWLITE_SCHEDULED_AT\" > {}",
                seen_path.display(),
            ),
            std::collections::BTreeMap::new(),
            "",
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher()
            .handle(&task_run_attempt)
            .await
            .unwrap();

        let seen = read_command_file(&seen_path).await;

        unsafe { std::env::remove_var("FLOWLITE_SCHEDULED_AT") };

        assert_eq!(seen, "[]");
    }

    /// working_dir is where the command runs, not a prefix on it.
    #[tokio::test]
    async fn the_working_dir_is_where_the_command_runs() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let working_dir = db.data_dir().to_string_lossy().into_owned();
        let seen_path = db.data_dir().join("pwd.txt");

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            &format!("pwd > {}", seen_path.display()),
            std::collections::BTreeMap::new(),
            &working_dir,
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        db.task_run_attempt_dispatcher()
            .handle(&task_run_attempt)
            .await
            .unwrap();

        // macOS resolves the temp dir through a symlink, so compare the resolved paths.
        assert_eq!(
            std::fs::canonicalize(read_command_file(&seen_path).await).unwrap(),
            std::fs::canonicalize(&working_dir).unwrap(),
        );
    }

    /// A spawn failure's message has to name the task and the directory itself - otherwise
    /// it reads identically to `sh` being missing, and a bad working_dir is left to spin
    /// forever with no way to tell which task or path is at fault.
    #[tokio::test]
    async fn a_spawn_failure_names_the_task_and_the_working_dir() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;

        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            "echo hi",
            std::collections::BTreeMap::new(),
            "/nope/does/not/exist",
        ).await;

        let task_run_attempt = db.insert_task_run_attempt(
            &task_run,
            1,
            TaskRunAttemptStatus::Pending,
        ).await;

        let error = db.task_run_attempt_dispatcher()
            .handle(&task_run_attempt)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains(&task_run.task_id), "{error}");
        assert!(error.contains("/nope/does/not/exist"), "{error}");
    }

    /// Unreachable while `settle_as_running` claims unconditionally, so it is called
    /// directly. Nothing has been spawned by the time the chain falls this far, so there is
    /// no process to account for.
    #[tokio::test]
    async fn an_unclaimed_attempt_is_settled_invalid() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(job_run.id, crate::crud::task_run::TaskRunStatus::Running).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.task_run_attempt_dispatcher().settle_unclaimed(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );
    }

    /// A crash between `spawn()` and the Running write leaves a pending attempt whose
    /// command is already running. Starting it again runs the command twice, which is
    /// worse than any unknown — so a pending attempt that was already spawned for is
    /// settled rather than started.
    #[tokio::test]
    async fn a_pending_attempt_already_spawned_for_is_invalid() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_for_command(job_run.id, "echo hi", 3600).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.begin_spawn_of_task_run_attempt(task_run_attempt.id).await;

        // Re-read: the poller hands the service the row as stored, and the intent was
        // written after this struct was built.
        let task_run_attempt = db.task_run_attempt(task_run_attempt.id).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Invalid,
        );
    }

    /// And nothing was spawned for it — the point of the whole change is the command not
    /// running twice. Asserted on the children map rather than on a side effect of the
    /// command, which a just-spawned process may not have reached yet.
    #[tokio::test]
    async fn a_pending_attempt_already_spawned_for_is_not_spawned_again() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_for_command(job_run.id, "echo hi", 3600).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.begin_spawn_of_task_run_attempt(task_run_attempt.id).await;

        // Re-read: the poller hands the service the row as stored, and the intent was
        // written after this struct was built.
        let task_run_attempt = db.task_run_attempt(task_run_attempt.id).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        assert!(db.children.remove(task_run_attempt.id).await.is_none());
    }

    /// The ordinary path: an attempt nothing has spawned for still starts.
    #[tokio::test]
    async fn a_fresh_pending_attempt_still_starts() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_for_command(job_run.id, "echo hi", 3600).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.status,
            TaskRunAttemptStatus::Running,
        );
    }

    /// A command that cannot be spawned at all must not look like one that was: the
    /// intent is cleared, so the next pass tries again rather than settling it Invalid.
    #[tokio::test]
    async fn an_attempt_whose_spawn_fails_is_left_startable() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_for_command_with_env(
            job_run.id,
            "echo hi",
            std::collections::BTreeMap::new(),
            "/no/such/working/directory",
        ).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        assert!(db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.is_err());

        let after = db.task_run_attempt(task_run_attempt.id).await;

        assert_eq!(after.status, TaskRunAttemptStatus::Pending);
        assert!(after.started_at.is_none(), "a failed spawn left the attempt looking spawned");
    }

    /// Recorded on the row, because `TaskRunAttemptChildren` is memory: after a restart the
    /// group id is the only way back to a process still running.
    #[tokio::test]
    async fn starting_an_attempt_records_its_process_group() {

        let _environment = reading_the_environment();

        let db = TestDb::new().await;

        let job_run = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run_for_command(job_run.id, "exec sleep 30", 3600).await;
        let task_run_attempt = db.insert_task_run_attempt(&task_run, 1, TaskRunAttemptStatus::Pending).await;

        db.task_run_attempt_dispatcher().handle(&task_run_attempt).await.unwrap();

        let mut child = db.children.remove(task_run_attempt.id).await.unwrap();
        let pid = child.child.id().unwrap() as i64;

        assert_eq!(
            db.task_run_attempt(task_run_attempt.id).await.process_group_id,
            Some(pid),
        );

        child.kill_process_group().await;
    }
}
