use std::sync::Arc;

use crate::crud::CRUD;
use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::job_run_stop::{InsertJobRunStopData, InsertJobRunStopDataInput, SelectJobRunStopsData, SelectJobRunStopsDataFilter};
use crate::crud::task_run_attempt::{SelectTaskRunAttemptsData, SelectTaskRunAttemptsDataFilter, TaskRunAttemptStatus};
use crate::poller::Service;
use crate::signals::Signals;


/// Stops a run a task submitted once nothing will use it: the run that task belongs to is
/// being stopped, or the attempt that submitted it ended in anything but success - a retry
/// submits its own children, and would otherwise run beside the failed attempt's.
///
/// It stops a run the way a person does, with a `job_run_stop` row, and writes no status:
/// the dispatchers and monitors settle a stopped child exactly as they settle any other.
/// That covers every way a parent ends - a stop from any frontend, a timeout, a crash -
/// without any of them knowing children exist, and a grandchild follows on the pass after
/// its own parent's stop row appears.
pub struct ChildJobRunStopper {
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


impl ChildJobRunStopper {

    pub fn new(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        signals: Arc<Signals>,
    ) -> Self {
        Self {
            crud,
            conn_pool,
            signals,
        }
    }

    async fn handle_child_job_run(&self, job_run: &JobRun) -> anyhow::Result<()> {

        if !self.should_stop(job_run).await? {
            return Ok(());
        }

        self.crud.insert_job_run_stop(&*self.conn_pool, &InsertJobRunStopData {
            input: InsertJobRunStopDataInput {
                job_run_id: job_run.id,
            },
        }).await?;

        self.signals.publish();

        Ok(())
    }

    async fn should_stop(&self, job_run: &JobRun) -> anyhow::Result<bool> {

        let Some(parent_task_run_attempt_id) = job_run.parent_task_run_attempt_id else {
            return Ok(false);
        };

        if self.is_job_run_stopped(job_run.id).await? {
            return Ok(false);
        }

        let attempts = self.crud.select_task_run_attempts(&*self.conn_pool, &SelectTaskRunAttemptsData {
            filter: SelectTaskRunAttemptsDataFilter {
                id: Some(parent_task_run_attempt_id),
                task_run_id: None,
                job_run_id: None,
                task_id: None,
                status: None,
            },
            sort: None,
        }).await?;

        // Retention never deletes a parent while a child is unfinished, so this is a row
        // somebody removed by hand. There is nothing left to follow.
        let Some(parent_attempt) = attempts.into_iter().next() else {
            return Ok(false);
        };

        if self.is_job_run_stopped(parent_attempt.job_run_id).await? {
            return Ok(true);
        }

        Ok(parent_attempt.status.is_finished() && parent_attempt.status != TaskRunAttemptStatus::Succeeded)
    }

    /// Every unfinished run a task submitted. Unfinished runs are the ones a stop can still
    /// reach, and the ones the dispatchers are about to spend a slot on.
    async fn get_unfinished_child_job_runs(&self) -> anyhow::Result<Vec<JobRun>> {

        let unfinished = JobRunStatus::ALL.iter()
            .filter(|status| !status.is_finished())
            .copied()
            .collect();

        let job_runs = self.crud.select_job_runs(&*self.conn_pool, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: None,
                status: None,
                statuses: Some(unfinished),
                schedule_id: None,
                scheduled_at: None,
                parent_job_run_id: None,
            },
            sort: Some(SelectJobRunsDataSort::Id),
            limit: None,
            offset: None,
        }).await?;

        Ok(job_runs
            .into_iter()
            .filter(|job_run| job_run.parent_task_run_attempt_id.is_some())
            .collect())
    }

    async fn is_job_run_stopped(&self, job_run_id: i64) -> anyhow::Result<bool> {

        let job_run_stop = self.crud.select_job_run_stop(&*self.conn_pool, &SelectJobRunStopsData {
            filter: SelectJobRunStopsDataFilter {
                id: None,
                job_run_id: Some(job_run_id),
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        Ok(job_run_stop.is_some())
    }
}


impl Service for ChildJobRunStopper {
    type Row = JobRun;

    fn name(&self) -> &'static str {
        "Child Job Run Stopper"
    }

    fn row_context(&self, job_run: &JobRun) -> String {
        format!("job run {}", job_run.id)
    }

    async fn select(&self) -> anyhow::Result<Vec<JobRun>> {
        self.get_unfinished_child_job_runs().await
    }

    async fn handle(&self, job_run: &JobRun) -> anyhow::Result<()> {
        self.handle_child_job_run(job_run).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::task_run::TaskRunStatus;
    use crate::crud::task_run_attempt::TaskRunAttempt;
    use crate::test_support::TestDb;

    async fn parent_attempt(db: &TestDb, status: TaskRunAttemptStatus) -> TaskRunAttempt {
        let parent = db.insert_job_run(JobRunStatus::Running).await;
        let task_run = db.insert_task_run(parent.id, TaskRunStatus::Running).await;

        db.insert_task_run_attempt(&task_run, 1, status).await
    }

    /// One pass over every row the service selects, the way its Poller runs it.
    async fn pass(db: &TestDb) {
        let stopper = db.child_job_run_stopper();

        for job_run in stopper.select().await.unwrap() {
            stopper.handle(&job_run).await.unwrap();
        }
    }

    async fn is_stopped(db: &TestDb, job_run_id: i64) -> bool {
        db.child_job_run_stopper().is_job_run_stopped(job_run_id).await.unwrap()
    }

    #[tokio::test]
    async fn a_child_of_a_stopped_run_is_stopped() {

        let db = TestDb::new().await;

        let attempt = parent_attempt(&db, TaskRunAttemptStatus::Running).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Running).await;
        db.insert_job_run_stop(attempt.job_run_id).await;

        pass(&db).await;

        assert!(is_stopped(&db, child.id).await);
    }

    /// A retry submits children of its own, so the failed attempt's would run beside them.
    #[tokio::test]
    async fn a_child_of_an_attempt_that_failed_is_stopped() {

        let db = TestDb::new().await;

        let attempt = parent_attempt(&db, TaskRunAttemptStatus::Failed).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Queued).await;

        pass(&db).await;

        assert!(is_stopped(&db, child.id).await);
    }

    /// Submitting and not waiting is a legitimate thing for a task to do; the run it
    /// started is its own from there.
    #[tokio::test]
    async fn a_child_of_an_attempt_that_succeeded_or_is_still_running_is_left_alone() {

        let db = TestDb::new().await;

        let succeeded = parent_attempt(&db, TaskRunAttemptStatus::Succeeded).await;
        let running = parent_attempt(&db, TaskRunAttemptStatus::Running).await;
        let first_child = db.insert_child_job_run(&succeeded, JobRunStatus::Running).await;
        let second_child = db.insert_child_job_run(&running, JobRunStatus::Running).await;

        pass(&db).await;

        assert!(!is_stopped(&db, first_child.id).await);
        assert!(!is_stopped(&db, second_child.id).await);
    }

    #[tokio::test]
    async fn a_finished_child_is_not_stopped() {

        let db = TestDb::new().await;

        let attempt = parent_attempt(&db, TaskRunAttemptStatus::Failed).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Succeeded).await;

        pass(&db).await;

        assert!(!is_stopped(&db, child.id).await);
    }

    /// The child's own stop row is what its children follow, one pass later.
    #[tokio::test]
    async fn a_grandchild_is_stopped_on_the_pass_after_its_parent() {

        let db = TestDb::new().await;

        let attempt = parent_attempt(&db, TaskRunAttemptStatus::Running).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Running).await;
        let child_task_run = db.insert_task_run(child.id, TaskRunStatus::Running).await;
        let child_attempt = db.insert_task_run_attempt(&child_task_run, 1, TaskRunAttemptStatus::Running).await;
        let grandchild = db.insert_child_job_run(&child_attempt, JobRunStatus::Running).await;

        db.insert_job_run_stop(attempt.job_run_id).await;

        pass(&db).await;
        pass(&db).await;

        assert!(is_stopped(&db, child.id).await);
        assert!(is_stopped(&db, grandchild.id).await);
    }

    /// One stop row per child, however many passes see it.
    #[tokio::test]
    async fn a_child_is_stopped_once() {

        let db = TestDb::new().await;

        let attempt = parent_attempt(&db, TaskRunAttemptStatus::Failed).await;
        let child = db.insert_child_job_run(&attempt, JobRunStatus::Running).await;

        pass(&db).await;
        pass(&db).await;

        let stops = db.crud.select_job_run_stops(&*db.conn_pool, &SelectJobRunStopsData {
            filter: SelectJobRunStopsDataFilter {
                id: None,
                job_run_id: Some(child.id),
            },
            sort: None,
            limit: None,
            offset: None,
        }).await.unwrap();

        assert_eq!(stops.len(), 1);
    }
}
