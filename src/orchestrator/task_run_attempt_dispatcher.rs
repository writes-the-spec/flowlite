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
use crate::orchestrator::task_run_attempt_reader::read_task_run_attempt_stream;
use crate::poller::Service;
use crate::signals::Signals;
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
}


impl TaskRunAttemptDispatcher {

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

    /// Settles a pending attempt as exactly one outcome. `settle_as_pending` has to precede
    /// `settle_as_running`, which spawns unconditionally and would start a retry the moment
    /// it was inserted.
    async fn handle_pending_task_run_attempt(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<()> {

        if self.settle_as_skipped(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_pending(task_run_attempt).await? {
            return Ok(());
        }

        if self.settle_as_running(task_run_attempt).await? {
            return Ok(());
        }

        anyhow::bail!(
            "Task run attempt {} settled as nothing: its job run was not stopped and it \
             was not started",
            task_run_attempt.id,
        )
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
                },
            },
        ).await?;

        self.signals.publish();

        Ok(true)
    }

    /// Leaves the attempt pending, writing nothing, while the retry_delay the run was
    /// submitted with has yet to pass since this attempt was created, which is when
    /// TaskRunMonitor decided to retry. Returns whether this is what happened.
    ///
    /// Only a retry waits. Attempt 1 is inserted by TaskRunDispatcher as it starts the task
    /// run and has nothing to wait for, so it never reaches the task run query below.
    async fn settle_as_pending(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        if task_run_attempt.attempt <= 1 {
            return Ok(false);
        }

        let task_run = self.get_task_run(task_run_attempt).await?;

        let retry_delay = TimeDelta::seconds(task_run.retry_delay as i64);

        Ok(Utc::now() < task_run_attempt.created_at + retry_delay)
    }

    /// Spawns the command of the attempt, hands the child process over and sets the
    /// attempt to running, which is what makes TaskRunAttemptMonitor pick it up.
    ///
    /// The child has to be in TaskRunAttemptChildren before the status is written, or
    /// the monitor sees a running attempt with no process and aborts it.
    async fn settle_as_running(&self, task_run_attempt: &TaskRunAttempt) -> anyhow::Result<bool> {

        let task_run = self.get_task_run(task_run_attempt).await?;
        let job_run = self.get_job_run(task_run_attempt).await?;

        let env = build_task_run_attempt_env(&task_run, &job_run, task_run_attempt);

        let mut command = tokio::process::Command::new("sh");

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

        let mut child = command.spawn()?;

        let stdout = child.stdout.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stdout of task: {}", task_run_attempt.task_id))?;
        let stderr = child.stderr.take()
            .ok_or_else(|| anyhow::anyhow!("Failed to get stderr of task: {}", task_run_attempt.task_id))?;

        let started_at = Utc::now();
        let times_out_at = started_at + TimeDelta::seconds(task_run.timeout as i64);

        let (chunks_sender, chunks) = tokio::sync::mpsc::unbounded_channel();

        let readers = [
            tokio::spawn(read_task_run_attempt_stream(
                stdout,
                TaskRunAttemptOutputStream::Stdout,
                chunks_sender.clone(),
            )),
            // The original sender moves in here rather than being kept: the channel closes
            // when the last sender drops, and that close is how TaskRunAttemptMonitor knows
            // both readers reached EOF. A clone held back here would mean it never closes,
            // and every terminal pass would wait out its whole EOF timeout.
            tokio::spawn(read_task_run_attempt_stream(
                stderr,
                TaskRunAttemptOutputStream::Stderr,
                chunks_sender,
            )),
        ];

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
                    started_at: Some(Some(started_at)),
                    finished_at: None,
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

    /// Asks whether the attempt is still waiting out its retry_delay.
    ///
    /// Calls `settle_as_pending` rather than the whole chain on purpose: falling through it
    /// means `settle_as_running` spawns a real process, which is what these tests are about
    /// avoiding until the delay has passed.
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

    /// Attempt 1 is inserted by TaskRunDispatcher as it starts the task run, so it has no
    /// failure behind it to wait out however long the retry_delay is.
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

    /// Follows a parameter, a task env value and an injected id all the way into the
    /// process, through a real spawn. The pure tests pin the composition; this pins that
    /// the composed map actually reaches the command.
    #[tokio::test]
    async fn the_composed_environment_reaches_the_command() {

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

    /// working_dir is where the command runs, not a prefix on it.
    #[tokio::test]
    async fn the_working_dir_is_where_the_command_runs() {

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
}
