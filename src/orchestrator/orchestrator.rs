use std::sync::Arc;
use crate::crud::CRUD;
use crate::orchestrator::job_run_dispatcher::JobRunDispatcher;
use crate::orchestrator::job_run_monitor::JobRunMonitor;
use crate::orchestrator::task_run_attempt_children::TaskRunAttemptChildren;
use crate::orchestrator::task_run_attempt_dispatcher::TaskRunAttemptDispatcher;
use crate::orchestrator::task_run_attempt_monitor::TaskRunAttemptMonitor;
use crate::orchestrator::task_run_dispatcher::TaskRunDispatcher;
use crate::orchestrator::task_run_monitor::TaskRunMonitor;


/// Starts the background services that turn job runs into finished task runs. They
/// coordinate through the database only, so this is the one place that has to know all
/// of them, and the order they are started in doesn't matter.
pub struct Orchestrator {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
}


impl Orchestrator {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
        }
    }

    /// Spawns every service and returns immediately.
    pub fn start(self: &Self) {

        let job_run_dispatcher = JobRunDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
        );

        job_run_dispatcher.start();

        let job_run_monitor = JobRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
        );

        job_run_monitor.start();

        let task_run_dispatcher = TaskRunDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
        );

        task_run_dispatcher.start();

        let task_run_monitor = TaskRunMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
        );

        task_run_monitor.start();

        // The two attempt services share the child processes: the dispatcher spawns
        // them, the monitor waits on them.
        let task_run_attempt_children = TaskRunAttemptChildren::new();
        let task_run_attempt_children = Arc::new(task_run_attempt_children);

        let task_run_attempt_dispatcher = TaskRunAttemptDispatcher::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            task_run_attempt_children.clone(),
        );

        task_run_attempt_dispatcher.start();

        let task_run_attempt_monitor = TaskRunAttemptMonitor::new(
            self.crud.clone(),
            self.conn_pool.clone(),
            task_run_attempt_children.clone(),
        );

        task_run_attempt_monitor.start();

    }

}
