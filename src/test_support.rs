use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::app_config::AppConfig;
use crate::crud::CRUD;
use crate::crud::job_run::{InsertJobRunData, InsertJobRunDataInput, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::task_run::{InsertTaskRunData, InsertTaskRunDataInput, SelectTaskRunsData, SelectTaskRunsDataFilter, TaskRun, TaskRunStatus};
use crate::crud::task_run_attempt::{InsertTaskRunAttemptData, InsertTaskRunAttemptDataInput, SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, SelectTaskRunAttemptsDataSort, TaskRunAttempt, TaskRunAttemptStatus};
use crate::orchestrator::job_run_monitor::JobRunMonitor;
use crate::orchestrator::task_run_attempt_children::TaskRunAttemptChildren;
use crate::orchestrator::task_run_attempt_dispatcher::TaskRunAttemptDispatcher;
use crate::orchestrator::task_run_monitor::TaskRunMonitor;
use crate::signals::Signals;
use crate::toolkit::Toolkit;


/// One test's own flowlite database, in a temp directory nothing else shares.
///
/// A service is built here with its real CRUD rather than a fake, so a settle chain is
/// asked what it wrote by reading the row back out of the table the next poll pass would
/// read it from — which is the only channel the services have between them, and the one
/// thing a hand-built struct cannot stand in for.
///
/// The `mem` schema is left unmigrated on purpose: no monitor reads a definition, and the
/// memory database is one shared-cache name for the whole process, so seeding it would
/// leak between tests running in parallel.
pub struct TestDb {
    data_dir: PathBuf,
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl TestDb {

    pub async fn new() -> Self {

        let data_dir = std::env::temp_dir().join(format!("flowlite-test-{}", uuid::Uuid::new_v4()));

        let app_config = AppConfig {
            config_dir: data_dir.join("config").to_string_lossy().into_owned(),
            data_dir: data_dir.to_string_lossy().into_owned(),
        };

        let toolkit = Arc::new(Toolkit::new(app_config));

        let conn_pool = toolkit.get_conn_pool().await
            .expect("failed to open the test database");

        Self {
            data_dir,
            crud: Arc::new(CRUD::new(toolkit)),
            conn_pool: Arc::new(conn_pool),
            signals: Arc::new(Signals::new()),
        }
    }

    pub fn job_run_monitor(&self) -> JobRunMonitor {
        JobRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        )
    }

    pub fn task_run_monitor(&self) -> TaskRunMonitor {
        TaskRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            self.signals.clone(),
        )
    }

    pub fn task_run_attempt_dispatcher(&self) -> TaskRunAttemptDispatcher {
        TaskRunAttemptDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            Arc::new(TaskRunAttemptChildren::new()),
            self.signals.clone(),
        )
    }

    pub async fn insert_job_run(&self, status: JobRunStatus) -> JobRun {

        let id = self.crud.insert_job_run(
            &*self.conn_pool,
            &InsertJobRunData {
                input: InsertJobRunDataInput {
                    job_id: "job".to_string(),
                    job_name: "Job".to_string(),
                    job_description: String::new(),
                    status,
                },
            },
        ).await.unwrap();

        self.job_run(id).await
    }

    /// A task run with nothing to retry, for the tests that only care about its status.
    pub async fn insert_task_run(&self, job_run_id: i64, status: TaskRunStatus) -> TaskRun {
        self.insert_task_run_with(job_run_id, status, 0, 60).await
    }

    /// A running task run with retries left, for the tests that drive the retry loop.
    pub async fn insert_retryable_task_run(&self, job_run_id: i64, max_retries: u32, retry_delay: u32) -> TaskRun {
        self.insert_task_run_with(job_run_id, TaskRunStatus::Running, max_retries, retry_delay).await
    }

    async fn insert_task_run_with(
        &self,
        job_run_id: i64,
        status: TaskRunStatus,
        max_retries: u32,
        retry_delay: u32,
    ) -> TaskRun {

        let id = self.crud.insert_task_run(
            &*self.conn_pool,
            &InsertTaskRunData {
                input: InsertTaskRunDataInput {
                    job_run_id,
                    job_id: "job".to_string(),
                    task_id: format!("task-{}", uuid::Uuid::new_v4()),
                    command: "true".to_string(),
                    depends_on: Vec::new(),
                    timeout: 3600,
                    max_retries,
                    retry_delay,
                    status,
                },
            },
        ).await.unwrap();

        self.task_run(id).await
    }

    pub async fn insert_task_run_attempt(
        &self,
        task_run: &TaskRun,
        attempt: u32,
        status: TaskRunAttemptStatus,
    ) -> TaskRunAttempt {

        self.crud.insert_task_run_attempt(
            &*self.conn_pool,
            &InsertTaskRunAttemptData {
                input: InsertTaskRunAttemptDataInput {
                    task_run_id: task_run.id,
                    job_run_id: task_run.job_run_id,
                    job_id: task_run.job_id.clone(),
                    task_id: task_run.task_id.clone(),
                    attempt,
                    status,
                },
            },
        ).await.unwrap();

        self.last_task_run_attempt(task_run.id).await
    }

    /// Moves an attempt's `created_at` back, which is what a retry_delay is measured from.
    /// No CRUD update writes that column — only the insert does — so the test reaches past
    /// CRUD rather than opening a seam in it for a case only a test has.
    pub async fn backdate_task_run_attempt(&self, task_run_attempt_id: i64, created_at: DateTime<Utc>) {

        sqlx::query("UPDATE task_run_attempt SET created_at = ? WHERE id = ?")
            .bind(created_at)
            .bind(task_run_attempt_id)
            .execute(&*self.conn_pool)
            .await
            .unwrap();
    }

    pub async fn job_run(&self, id: i64) -> JobRun {

        self.crud.select_job_run(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: Some(id),
                    job_id: None,
                    status: None,
                },
                sort: None,
                limit: Some(1),
                offset: None,
            },
        ).await.unwrap().unwrap()
    }

    pub async fn task_run(&self, id: i64) -> TaskRun {

        self.crud.select_task_run(
            &*self.conn_pool,
            &SelectTaskRunsData {
                filter: SelectTaskRunsDataFilter {
                    id: Some(id),
                    job_run_id: None,
                    job_id: None,
                    task_id: None,
                    status: None,
                },
                sort: None,
            },
        ).await.unwrap().unwrap()
    }

    pub async fn task_run_attempts(&self, task_run_id: i64) -> Vec<TaskRunAttempt> {

        self.crud.select_task_run_attempts(
            &*self.conn_pool,
            &SelectTaskRunAttemptsData {
                filter: SelectTaskRunAttemptsDataFilter {
                    task_run_id: Some(task_run_id),
                    job_run_id: None,
                    task_id: None,
                    status: None,
                },
                sort: Some(SelectTaskRunAttemptsDataSort::Attempt),
            },
        ).await.unwrap()
    }

    async fn last_task_run_attempt(&self, task_run_id: i64) -> TaskRunAttempt {
        self.task_run_attempts(task_run_id).await.into_iter().last().unwrap()
    }

}


impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}
