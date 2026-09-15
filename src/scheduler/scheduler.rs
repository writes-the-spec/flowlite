use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::cron_trigger::CronTrigger;
use crate::crud::CRUD;
use crate::crud::job_run::{JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter};
use crate::crud::schedule::{Schedule, SelectSchedulesData, SelectSchedulesDataFilter, SelectSchedulesDataSort};
use crate::crud::schedule_job::{ScheduleJob, SelectScheduleJobsData, SelectScheduleJobsDataFilter, SelectScheduleJobsDataSort};
use crate::poller::Service;
use crate::signals::Signals;
use crate::toolkit::Toolkit;


/// Keeps each schedule's next `submit_ahead` occurrences submitted as job runs, submitting
/// whichever is missing. It never decides a run is due - moving a run from Scheduled to
/// Queued at its time is JobRunReleaser's job.
pub struct Scheduler {
    pub toolkit: Arc<Toolkit>,
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
}


/// Every status whose presence means an occurrence is spoken for - which is all of them
/// except `Deleted`, the tombstone left by a run somebody removed to have it written again.
fn occupying_statuses() -> Vec<JobRunStatus> {
    JobRunStatus::ALL
        .into_iter()
        .filter(|status| *status != JobRunStatus::Deleted)
        .collect()
}


impl Scheduler {

    pub fn new(
        toolkit: Arc<Toolkit>,
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        signals: Arc<Signals>,
    ) -> Self {
        Self {
            toolkit,
            crud,
            conn_pool,
            signals,
        }
    }

    /// Submits whichever of this schedule's desired occurrences has no run yet.
    ///
    /// Additive only: a run this schedule no longer asks for - a lowered `submit_ahead`, an
    /// edited cron, a job dropped from `jobs:` - is left where it is.
    async fn reconcile_schedule(&self, schedule: &Schedule) -> anyhow::Result<()> {

        let now = self.toolkit.get_current_ts();

        // A schedule that is disabled or past its end_date desires nothing, both out of
        // `get_next_runs` itself.
        let desired = CronTrigger::from_schedule(schedule)
            .get_next_runs(Some(now), schedule.submit_ahead);

        let schedule_jobs = self.schedule_jobs(schedule).await?;

        for occurrence in &desired {
            for schedule_job in &schedule_jobs {
                self.submit_if_missing(schedule, schedule_job, *occurrence).await?;
            }
        }

        Ok(())
    }

    /// Submits one (job, occurrence) pair, exactly as `job submit` does, unless a run for it
    /// is already there.
    ///
    /// "Already there" is asked across every status but one, not only Scheduled: stopping a
    /// future-dated scheduled run is allowed, `JobRunReleaser` then skips the row outright
    /// while its instant is still ahead, and a check reading only Scheduled would submit the
    /// occurrence the user just cancelled all over again.
    ///
    /// `Deleted` is the exception, and the only thing that frees an occurrence. A stop says
    /// "do not run this", so its Skipped row goes on holding the instant for ever; deleting
    /// a submitted run says "write this one again", which is how an edited definition
    /// reaches an occurrence already standing. Those are the two halves of the same
    /// question, and this filter is where they are told apart.
    async fn submit_if_missing(
        &self,
        schedule: &Schedule,
        schedule_job: &ScheduleJob,
        occurrence: DateTime<Utc>,
    ) -> anyhow::Result<()> {

        let mut conn = self.conn_pool.acquire().await?;

        // The instant is matched as stored, which holds because the row was inserted with
        // the very value `get_next_runs` hands out here - a cron occurrence, whole seconds
        // and UTC, encoded the same way on both sides.
        let existing = self.crud.select_job_run(&mut *conn, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: Some(schedule_job.job_id.clone()),
                status: None,
                statuses: Some(occupying_statuses()),
                schedule_id: Some(schedule.schedule_id.clone()),
                scheduled_at: Some(occurrence),
            },
            sort: None,
            limit: Some(1),
            offset: None,
        }).await?;

        if existing.is_some() {
            return Ok(());
        }

        // Reported and stepped over rather than raised on: one unsubmittable job must not
        // cost the schedule's other jobs their occurrence, and the next pass tries it again
        // anyway.
        if let Err(e) = self.crud.submit_job(
            &mut conn,
            &schedule_job.job_id,
            &schedule_job.parameters.0,
            occurrence,
            Some(&schedule.schedule_id),
        ).await {
            eprintln!(
                "Scheduler could not submit job {} of schedule {} for {}: {e:?}",
                schedule_job.job_id,
                schedule.schedule_id,
                occurrence.to_rfc3339(),
            );
            return Ok(());
        }

        self.signals.publish();

        Ok(())
    }

    /// Every schedule, not only the due ones: "due" is not a question this service asks at
    /// all.
    async fn get_schedules(&self) -> anyhow::Result<Vec<Schedule>> {

        self.crud.select_schedules(
            &*self.conn_pool,
            &SelectSchedulesData {
                filter: SelectSchedulesDataFilter {
                    schedule_id: None,
                    name_like: None,
                    disabled: None,
                },
                sort: Some(SelectSchedulesDataSort::RowId),
                limit: None,
                offset: None,
            },
        ).await
    }

    /// Read once per pass, so "which jobs does this schedule name?" is one answer for every
    /// occurrence of the pass.
    async fn schedule_jobs(&self, schedule: &Schedule) -> anyhow::Result<Vec<ScheduleJob>> {

        self.crud.select_schedule_jobs(
            &*self.conn_pool,
            &SelectScheduleJobsData {
                filter: SelectScheduleJobsDataFilter {
                    schedule_id: Some(schedule.schedule_id.clone()),
                    job_id: None,
                },
                sort: Some(SelectScheduleJobsDataSort::RowId),
                limit: None,
                offset: None,
            }
        ).await
    }

}


impl Service for Scheduler {
    type Row = Schedule;

    fn name(&self) -> &'static str {
        "Scheduler"
    }

    fn row_context(&self, schedule: &Schedule) -> String {
        format!("schedule {}", schedule.schedule_id)
    }

    async fn select(&self) -> anyhow::Result<Vec<Schedule>> {
        self.get_schedules().await
    }

    async fn handle(&self, schedule: &Schedule) -> anyhow::Result<()> {
        self.reconcile_schedule(schedule).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::job_run::{JobRun, JobRunStatus, SelectJobRunsDataSort};
    use crate::test_support::TestDb;

    /// Every run this schedule currently has outstanding, oldest due first.
    async fn outstanding(db: &TestDb, schedule_id: &str) -> Vec<JobRun> {
        db.crud.select_job_runs(&*db.conn_pool, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: None,
                status: Some(JobRunStatus::Scheduled),
                statuses: None,
                schedule_id: Some(schedule_id.to_string()),
                scheduled_at: None,
            },
            sort: Some(SelectJobRunsDataSort::Id),
            limit: None,
            offset: None,
        }).await.unwrap()
    }

    /// Every run this schedule has ever produced, whatever status it now holds - what the
    /// submit side asks about, and the only way to see a second run written for an
    /// occurrence whose first run has already left Scheduled.
    async fn all_runs(db: &TestDb, schedule_id: &str) -> Vec<JobRun> {
        db.crud.select_job_runs(&*db.conn_pool, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: None,
                status: None,
                statuses: None,
                schedule_id: Some(schedule_id.to_string()),
                scheduled_at: None,
            },
            sort: Some(SelectJobRunsDataSort::Id),
            limit: None,
            offset: None,
        }).await.unwrap()
    }

    /// `new_with_migrated_mem`, not `new`: a schedule lives in `mem`, which the plain
    /// constructor leaves unmigrated on purpose - see `TestDb`'s own doc comment.
    async fn scheduled_db() -> (TestDb, sqlx::SqliteConnection) {
        TestDb::new_with_migrated_mem().await
    }

    #[tokio::test]
    async fn a_schedule_submits_its_next_occurrence_before_it_is_due() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();

        let runs = outstanding(&db, "nightly").await;

        assert_eq!(runs.len(), 1);
        assert!(runs[0].scheduled_at > Utc::now(), "the run should be dated in the future");
    }

    /// The property that makes this a reconcile rather than a fire: running it again
    /// changes nothing.
    #[tokio::test]
    async fn a_second_pass_submits_nothing_new() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();
        let first = outstanding(&db, "nightly").await;

        db.scheduler().handle(&schedule).await.unwrap();
        let second = outstanding(&db, "nightly").await;

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].id, second[0].id);
    }

    /// Exactly what `JobRunReleaser` does to a run whose instant has arrived: Scheduled to
    /// Queued, and nothing else.
    async fn release(db: &TestDb, job_run_id: i64) {

        db.crud.update_job_runs(&*db.conn_pool, &crate::crud::job_run::UpdateJobRunsData {
            input: crate::crud::job_run::UpdateJobRunsDataInput {
                status: Some(JobRunStatus::Queued),
                started_at: None,
                finished_at: None,
            },
            filter: crate::crud::job_run::UpdateJobRunsDataFilter { id: Some(job_run_id) },
        }).await.unwrap();
    }

    /// Once the releaser has taken an occurrence, the desired set has moved on by one and
    /// the next pass submits the new tail - which is the whole of "keep submit_ahead
    /// occurrences submitted", with no advancing cursor.
    #[tokio::test]
    async fn a_released_occurrence_is_topped_up_on_the_next_pass() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        let past_due = db.insert_job_run_at(
            JobRunStatus::Scheduled,
            Utc::now() - chrono::TimeDelta::minutes(5),
            Some("nightly"),
        ).await;

        release(&db, past_due.id).await;

        db.scheduler().handle(&schedule).await.unwrap();

        let topped_up = outstanding(&db, "nightly").await;

        assert_eq!(topped_up.len(), 1);
        assert_ne!(topped_up[0].id, past_due.id);
        assert!(topped_up[0].scheduled_at > Utc::now(), "the top-up should be the next occurrence");
    }

    /// Stopping a single future-dated occurrence has to stick. The stop is honoured by
    /// `JobRunReleaser` skipping the run outright, so the row leaves Scheduled while its
    /// instant is still in the future - and a check reading only Scheduled would decide the
    /// occurrence was never dealt with and submit it again, running the very job the user
    /// cancelled.
    #[tokio::test]
    async fn a_stopped_future_occurrence_is_not_submitted_again() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();

        let submitted = outstanding(&db, "nightly").await;
        assert_eq!(submitted.len(), 1);
        let occurrence = submitted[0].scheduled_at;

        // The stop path, run through the real releaser: a stopped run is skipped however
        // far off its instant is.
        db.insert_job_run_stop(submitted[0].id).await;
        db.job_run_releaser().handle(&submitted[0]).await.unwrap();

        assert_eq!(db.job_run(submitted[0].id).await.status, JobRunStatus::Skipped);

        db.scheduler().handle(&schedule).await.unwrap();

        let at_that_instant = all_runs(&db, "nightly").await
            .into_iter()
            .filter(|run| run.scheduled_at == occurrence)
            .count();

        assert_eq!(at_that_instant, 1, "the stopped occurrence must not be submitted a second time");
    }

    /// The point of the Deleted status. Removing a submitted run by hand says "write this
    /// occurrence again", so unlike a stop it must leave the occurrence free - otherwise the
    /// edited definition the user deleted the run for never reaches the schedule.
    #[tokio::test]
    async fn a_deleted_occurrence_is_submitted_again() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();

        let submitted = outstanding(&db, "nightly").await;
        assert_eq!(submitted.len(), 1);
        let occurrence = submitted[0].scheduled_at;

        let mut conn = db.conn_pool.acquire().await.unwrap();
        assert!(db.crud.delete_job_run(&mut conn, submitted[0].id).await.unwrap());

        db.scheduler().handle(&schedule).await.unwrap();

        let at_that_instant = all_runs(&db, "nightly").await
            .into_iter()
            .filter(|run| run.scheduled_at == occurrence)
            .collect::<Vec<_>>();

        assert_eq!(at_that_instant.len(), 2, "the deleted occurrence should be written again");
        assert_eq!(at_that_instant[0].status, JobRunStatus::Deleted);
        assert_eq!(at_that_instant[1].status, JobRunStatus::Scheduled);
    }

    /// The restart case the schedule_id column exists for. Nothing about a pass is written
    /// down, so a Scheduler that did not ask the table would write the same occurrence
    /// twice - a duplicate nobody would see until they read it.
    #[tokio::test]
    async fn an_occurrence_already_submitted_is_not_submitted_again_after_a_restart() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();

        // Exactly what a restart does: the row is seeded fresh from the YAML.
        let reseeded = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&reseeded).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 1);
    }

    /// The existence check is keyed on the job as well as the instant, so a schedule's two
    /// jobs each get their own run for the same occurrence.
    #[tokio::test]
    async fn every_job_of_a_schedule_gets_its_own_run() {

        let (db, _mem_conn) = scheduled_db().await;

        let both = db.seed_schedule_with_jobs("nightly", "0 0 3 * * *", 1, &["job", "second-job"]).await;

        db.scheduler().handle(&both).await.unwrap();
        db.scheduler().handle(&both).await.unwrap();

        let runs = outstanding(&db, "nightly").await;

        assert_eq!(runs.len(), 2);
        assert_ne!(runs[0].job_id, runs[1].job_id);
    }

    #[tokio::test]
    async fn raising_submit_ahead_submits_the_extra_occurrences() {

        let (db, _mem_conn) = scheduled_db().await;

        let one = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&one).await.unwrap();

        let three = db.seed_schedule("nightly", "0 0 3 * * *", 3).await;
        db.scheduler().handle(&three).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 3);
    }

    /// This reconcile only ever adds. A run the schedule has stopped asking for - here
    /// because submit_ahead came back down - stays submitted and will be released and
    /// executed like any other.
    #[tokio::test]
    async fn lowering_submit_ahead_leaves_the_runs_already_submitted() {

        let (db, _mem_conn) = scheduled_db().await;

        let three = db.seed_schedule("nightly", "0 0 3 * * *", 3).await;
        db.scheduler().handle(&three).await.unwrap();

        let one = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&one).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 3);
    }

    /// A manual run carries no schedule_id, so no schedule's reconcile can see it - and a
    /// schedule whose own occurrence falls at the same instant still submits its own run.
    #[tokio::test]
    async fn a_run_nobody_scheduled_does_not_stand_in_for_an_occurrence() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        let manual = db.insert_job_run_at(
            JobRunStatus::Scheduled,
            Utc::now() + chrono::TimeDelta::days(365),
            None,
        ).await;

        db.scheduler().handle(&schedule).await.unwrap();

        assert_eq!(db.job_run(manual.id).await.status, JobRunStatus::Scheduled);
        assert_eq!(outstanding(&db, "nightly").await.len(), 1);
    }
}
