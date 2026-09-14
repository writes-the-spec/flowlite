use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::cron_trigger::CronTrigger;
use crate::crud::CRUD;
use crate::crud::job_run::{DeleteJobRunsData, DeleteJobRunsDataFilter, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::schedule::{Schedule, SelectSchedulesData, SelectSchedulesDataFilter, SelectSchedulesDataSort};
use crate::crud::schedule_job::{ScheduleJob, SelectScheduleJobsData, SelectScheduleJobsDataFilter, SelectScheduleJobsDataSort};
use crate::poller::Service;
use crate::signals::Signals;
use crate::toolkit::Toolkit;


/// Keeps each schedule's next `submit_ahead` occurrences submitted as job runs, submitting
/// what is missing and deleting what is surplus. It never decides a run is due - moving a
/// run from Submitted to Queued at its time is JobRunReleaser's job.
pub struct Scheduler {
    pub toolkit: Arc<Toolkit>,
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
    pub signals: Arc<Signals>,
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

    /// Makes this schedule's outstanding runs equal its desired occurrences.
    ///
    /// Convergent, not additive: nothing is remembered between passes, so every case - a
    /// first pass, a top-up after a release, a restart, an edited cron - is the same two
    /// loops finding a different difference.
    async fn reconcile_schedule(&self, schedule: &Schedule) -> anyhow::Result<()> {

        let now = self.toolkit.get_current_ts();

        // One instant, passed to every step below: read twice, an occurrence could fall
        // between the reads, go missing from `desired` while still counting as future-dated,
        // and have its run deleted at the moment it came due. A schedule that is disabled or
        // past its end_date desires nothing, both out of `get_next_runs` itself.
        let desired = CronTrigger::from_schedule(schedule)
            .get_next_runs(Some(now), schedule.submit_ahead);

        let schedule_jobs = self.schedule_jobs(schedule).await?;
        let outstanding = self.outstanding_runs(Some(&schedule.schedule_id)).await?;

        self.delete_surplus_runs(&outstanding, &desired, &schedule_jobs, schedule, now).await?;

        // Read after the deletes, and across every status rather than only Submitted - see
        // `runs_of_schedule` for why the submit side asks the wider question.
        let dealt_with = self.runs_of_schedule(&schedule.schedule_id).await?;

        self.submit_missing_runs(&dealt_with, &desired, &schedule_jobs, schedule).await?;

        Ok(())
    }

    /// Deletes the outstanding runs the schedule no longer asks for, keyed on the job and
    /// the instant together - the same pair the submit side writes on, so that a job dropped
    /// from `jobs:` does not keep a run some other job's occurrence still wants.
    ///
    /// Future-dated only: a run whose instant has arrived belongs to JobRunReleaser. Both
    /// conditions are restated on the delete's own filter, which re-resolves inside its
    /// transaction, in case the releaser promoted the row since the select read it.
    ///
    /// Deleted rather than stopped: a run cancelled before it was ever due never happened,
    /// and a stop row would sit in the history beside runs genuinely stood down.
    async fn delete_surplus_runs(
        &self,
        outstanding: &[JobRun],
        desired: &[DateTime<Utc>],
        schedule_jobs: &[ScheduleJob],
        schedule: &Schedule,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {

        for run in outstanding {

            if run.scheduled_at <= now || is_still_wanted(run, desired, schedule_jobs) {
                continue;
            }

            self.delete_run(run.id, &schedule.schedule_id, now).await?;
        }

        Ok(())
    }

    /// Submits whichever (occurrence, job) pair has no run yet, exactly as `job submit`
    /// does. `dealt_with` is every run of this schedule whatever its status - see
    /// `runs_of_schedule`.
    async fn submit_missing_runs(
        &self,
        dealt_with: &[JobRun],
        desired: &[DateTime<Utc>],
        schedule_jobs: &[ScheduleJob],
        schedule: &Schedule,
    ) -> anyhow::Result<()> {

        for occurrence in desired {
            for schedule_job in schedule_jobs {

                let already_submitted = dealt_with.iter().any(|run| {
                    run.job_id == schedule_job.job_id
                        && is_same_instant(run.scheduled_at, std::slice::from_ref(occurrence))
                });

                if already_submitted {
                    continue;
                }

                let mut conn = self.conn_pool.acquire().await?;

                // Reported and stepped over rather than raised on: one unsubmittable job
                // must not cost the schedule's other jobs their occurrence, and the next
                // pass tries it again anyway.
                if let Err(e) = self.crud.submit_job(
                    &mut conn,
                    &schedule_job.job_id,
                    &schedule_job.parameters.0,
                    *occurrence,
                    Some(&schedule.schedule_id),
                ).await {
                    eprintln!(
                        "Scheduler could not submit job {} of schedule {} for {}: {e:?}",
                        schedule_job.job_id,
                        schedule.schedule_id,
                        occurrence.to_rfc3339(),
                    );
                    continue;
                }

                self.signals.publish();
            }
        }

        Ok(())
    }

    /// Takes back the runs of schedules that are no longer in the YAML. Their rows are gone,
    /// so no schedule's own reconcile can collect them - `delete_surplus_runs` only ever
    /// touches the schedule it is reconciling.
    ///
    /// Same terms as `delete_surplus_runs`: future-dated only, status re-asserted. A run
    /// carrying no schedule_id is a manual submission and belongs to nobody's reconcile.
    async fn delete_orphaned_runs(&self, schedules: &[Schedule]) -> anyhow::Result<()> {

        let now = self.toolkit.get_current_ts();

        for run in self.outstanding_runs(None).await? {

            let Some(schedule_id) = run.schedule_id.clone() else {
                continue;
            };

            if run.scheduled_at <= now
                || schedules.iter().any(|schedule| schedule.schedule_id == schedule_id)
            {
                continue;
            }

            self.delete_run(run.id, &schedule_id, now).await?;
        }

        Ok(())
    }

    /// The filter restates what the caller already checked, because the delete re-resolves
    /// it inside its own transaction - so the row it checks is the row it removes.
    async fn delete_run(&self, id: i64, schedule_id: &str, now: DateTime<Utc>) -> anyhow::Result<()> {

        let mut conn = self.conn_pool.acquire().await?;

        self.crud.delete_job_runs_with_children(&mut conn, &DeleteJobRunsData {
            filter: DeleteJobRunsDataFilter {
                id: Some(id),
                job_id: None,
                status: Some(JobRunStatus::Submitted),
                schedule_id: Some(schedule_id.to_string()),
                scheduled_at_gt: Some(now),
            },
        }).await?;

        self.signals.publish();

        Ok(())
    }

    /// Every schedule, not only the due ones: a disabled schedule still has outstanding runs
    /// to take back, and "due" is not a question this service asks at all.
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

    /// The runs this reconcile may still take back: Submitted and nothing else, because a
    /// run that has left that status has been acted on by somebody else.
    async fn outstanding_runs(&self, schedule_id: Option<&str>) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Submitted),
                    statuses: None,
                    schedule_id: schedule_id.map(str::to_string),
                },
                sort: Some(SelectJobRunsDataSort::Id),
                limit: None,
                offset: None,
            },
        ).await
    }

    /// Every run this schedule has ever produced, whatever became of it - what the submit
    /// side compares against, since "have I dealt with this occurrence?" is a wider question
    /// than "is a Submitted row still sitting there?".
    ///
    /// Stopping a future-dated scheduled run is allowed: the row leaves Submitted while its
    /// instant is still ahead. A submit side reading only Submitted would find the
    /// occurrence missing and run the job the user just cancelled.
    async fn runs_of_schedule(&self, schedule_id: &str) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: None,
                    statuses: None,
                    schedule_id: Some(schedule_id.to_string()),
                },
                sort: Some(SelectJobRunsDataSort::Id),
                limit: None,
                offset: None,
            },
        ).await
    }

    /// Read once per pass and handed to both halves of the reconcile, so "does this schedule
    /// still want this job?" is one answer rather than two reads that could disagree.
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


/// Whether the schedule still asks for this exact run. Both halves, because the submit side
/// writes a run per (job, occurrence) pair.
fn is_still_wanted(run: &JobRun, desired: &[DateTime<Utc>], schedule_jobs: &[ScheduleJob]) -> bool {

    schedule_jobs.iter().any(|schedule_job| schedule_job.job_id == run.job_id)
        && is_same_instant(run.scheduled_at, desired)
}


/// Compared at whole seconds: an instant that has been through the database has been through
/// a string, and an equality depending on that round-trip being exact would delete and
/// resubmit the same run every pass.
fn is_same_instant(at: DateTime<Utc>, occurrences: &[DateTime<Utc>]) -> bool {
    occurrences.iter().any(|occurrence| occurrence.timestamp() == at.timestamp())
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

        let schedules = self.get_schedules().await?;

        // Swept here rather than in handle, because a schedule that is gone is not one of
        // the rows handle will be called with. Reported and stepped over rather than raised
        // on: an error out of select kills the Poller's loop for the whole backoff, so one
        // undeletable row would stall every schedule's reconcile, pass after pass.
        if let Err(e) = self.delete_orphaned_runs(&schedules).await {
            eprintln!("Scheduler could not sweep orphaned runs: {e:?}");
        }

        Ok(schedules)
    }

    async fn handle(&self, schedule: &Schedule) -> anyhow::Result<()> {
        self.reconcile_schedule(schedule).await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDb;

    /// Every run this schedule currently has outstanding, oldest due first.
    async fn outstanding(db: &TestDb, schedule_id: &str) -> Vec<JobRun> {
        db.crud.select_job_runs(&*db.conn_pool, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: None,
                status: Some(JobRunStatus::Submitted),
                statuses: None,
                schedule_id: Some(schedule_id.to_string()),
            },
            sort: Some(SelectJobRunsDataSort::Id),
            limit: None,
            offset: None,
        }).await.unwrap()
    }

    /// Every run this schedule has ever produced, whatever status it now holds - what the
    /// submit side reads, and the only way to see a second run written for an occurrence
    /// whose first run has already left Submitted.
    async fn all_runs(db: &TestDb, schedule_id: &str) -> Vec<JobRun> {
        db.crud.select_job_runs(&*db.conn_pool, &SelectJobRunsData {
            filter: SelectJobRunsDataFilter {
                id: None,
                job_id: None,
                status: None,
                statuses: None,
                schedule_id: Some(schedule_id.to_string()),
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

    /// Exactly what `JobRunReleaser` does to a run whose instant has arrived: Submitted to
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

    /// Once the releaser has taken an occurrence, it is no longer outstanding, so the
    /// desired set has moved on by one and the next pass tops it back up - which is the
    /// whole of "keep submit_ahead occurrences submitted", with no advancing cursor.
    ///
    /// The released run is past-due, because that is the only shape a genuine release has:
    /// the releaser promotes a run either when its instant has arrived or when somebody has
    /// stopped it, and the stopped case is the one
    /// `a_stopped_future_occurrence_is_not_submitted_again` pins, with the opposite
    /// expectation. Releasing a future-dated run here instead would make this test assert
    /// that a cancelled occurrence comes straight back.
    #[tokio::test]
    async fn a_released_occurrence_is_topped_up_on_the_next_pass() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        let past_due = db.insert_job_run_at(
            JobRunStatus::Submitted,
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
    /// `JobRunReleaser` skipping the run outright, so the row leaves Submitted while its
    /// instant is still in the future - and a reconcile that read outstanding-ness as "a
    /// Submitted row exists" would decide the occurrence was never dealt with and submit it
    /// again, running the very job the user cancelled.
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

    /// The two halves of the reconcile are keyed the same way, on the job and the instant
    /// together. Keyed on the instant alone the delete side would keep a dropped job's
    /// already-submitted run - the other job of the schedule still wants that occurrence -
    /// and removing a job from `jobs:` would not stop its next run.
    #[tokio::test]
    async fn a_job_removed_from_a_schedule_loses_its_outstanding_run() {

        let (db, _mem_conn) = scheduled_db().await;

        let both = db.seed_schedule_with_jobs("nightly", "0 0 3 * * *", 1, &["job", "second-job"]).await;
        db.scheduler().handle(&both).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 2);

        let one = db.seed_schedule_with_jobs("nightly", "0 0 3 * * *", 1, &["job"]).await;
        db.scheduler().handle(&one).await.unwrap();

        let left = outstanding(&db, "nightly").await;

        assert_eq!(left.len(), 1);
        assert_eq!(left[0].job_id, "job");
    }

    /// The restart case the schedule_id column exists for. next_run is recomputed from
    /// scratch on every startup, so a Scheduler that trusted it would write the same
    /// occurrence twice — a duplicate nobody would see until they read the table.
    #[tokio::test]
    async fn an_occurrence_already_submitted_is_not_submitted_again_after_next_run_resets() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();

        // Exactly what a restart does: the row is seeded fresh from the YAML, with
        // next_run computed from now rather than remembered.
        let reseeded = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&reseeded).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 1);
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

    #[tokio::test]
    async fn lowering_submit_ahead_deletes_the_surplus() {

        let (db, _mem_conn) = scheduled_db().await;

        let three = db.seed_schedule("nightly", "0 0 3 * * *", 3).await;
        db.scheduler().handle(&three).await.unwrap();

        let one = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&one).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 1);
    }

    /// Without this, disabling a schedule would not stop its next run — the surprise that
    /// submitting ahead would otherwise introduce.
    #[tokio::test]
    async fn disabling_a_schedule_takes_back_its_outstanding_runs() {

        let (db, _mem_conn) = scheduled_db().await;

        let enabled = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&enabled).await.unwrap();
        assert_eq!(outstanding(&db, "nightly").await.len(), 1);

        let disabled = db.seed_disabled_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&disabled).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 0);
    }

    #[tokio::test]
    async fn editing_the_cron_replaces_the_outstanding_run() {

        let (db, _mem_conn) = scheduled_db().await;

        let three_am = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&three_am).await.unwrap();
        let before = outstanding(&db, "nightly").await;

        let four_am = db.seed_schedule("nightly", "0 0 4 * * *", 1).await;
        db.scheduler().handle(&four_am).await.unwrap();
        let after = outstanding(&db, "nightly").await;

        assert_eq!(after.len(), 1);
        assert_ne!(before[0].scheduled_at, after[0].scheduled_at);
    }

    /// A schedule past its end_date has nowhere left to go, so the desired set is empty and
    /// the reconcile takes its outstanding runs back - the same deletion that disabling
    /// causes, reached without a branch of its own.
    #[tokio::test]
    async fn a_schedule_past_its_end_date_has_nothing_left_to_keep_submitted() {

        let (db, _mem_conn) = scheduled_db().await;

        let open_ended = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&open_ended).await.unwrap();
        assert_eq!(outstanding(&db, "nightly").await.len(), 1);

        let retired = db.seed_schedule_ending(
            "nightly",
            "0 0 3 * * *",
            1,
            (Utc::now() - chrono::TimeDelta::days(1)).date_naive(),
        ).await;

        db.scheduler().handle(&retired).await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 0);
    }

    /// The guard that keeps the reconcile off JobRunReleaser's territory. A run whose
    /// instant has arrived but which has not been released yet is not one of the
    /// schedule's future occurrences, and deleting it would cancel a run at the exact
    /// moment it came due.
    #[tokio::test]
    async fn a_run_that_has_come_due_is_never_deleted_by_the_reconcile() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        let past_due = db.insert_job_run_at(
            JobRunStatus::Submitted,
            Utc::now() - chrono::TimeDelta::minutes(5),
            Some("nightly"),
        ).await;

        db.scheduler().handle(&schedule).await.unwrap();

        let still_there = outstanding(&db, "nightly").await
            .into_iter()
            .any(|run| run.id == past_due.id);

        assert!(still_there, "a run that has come due belongs to the releaser, not the reconcile");
    }

    /// A schedule deleted from the YAML leaves runs no reconcile can reach, because every
    /// delete in `delete_surplus_runs` is scoped to the schedule being reconciled. The
    /// sweep at the head of `select` is what collects them - without it, deleting a
    /// schedule would not stop its next run from being released and executed.
    #[tokio::test]
    async fn a_run_of_a_schedule_that_is_gone_is_swept_up() {

        let (db, _mem_conn) = scheduled_db().await;

        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;
        db.scheduler().handle(&schedule).await.unwrap();
        assert_eq!(outstanding(&db, "nightly").await.len(), 1);

        // A manual run in the same database: it names no schedule, so the sweep must not
        // read it as one whose schedule has gone missing.
        let manual = db.insert_job_run_at(
            JobRunStatus::Submitted,
            Utc::now() + chrono::TimeDelta::days(365),
            None,
        ).await;

        // Exactly what deleting the YAML file does: CRUD::init rebuilds mem without it.
        // Its jobs go first, because schedule_job's foreign key says so.
        for statement in [
            "DELETE FROM mem.schedule_job WHERE schedule_id = ?",
            "DELETE FROM mem.schedule WHERE schedule_id = ?",
        ] {
            sqlx::query(statement)
                .bind("nightly")
                .execute(&*db.conn_pool)
                .await
                .unwrap();
        }

        db.scheduler().select().await.unwrap();

        assert_eq!(outstanding(&db, "nightly").await.len(), 0);
        assert_eq!(db.job_run(manual.id).await.status, JobRunStatus::Submitted);
    }

    /// The instant comparison is at whole seconds on purpose - see `is_same_instant` - and
    /// nothing else fails when it is written as a plain equality, because the round trip
    /// through SQLite happens to be exact today. Pinned here so that stays a decision.
    #[test]
    fn an_instant_is_the_same_one_within_the_second() {

        let occurrence: DateTime<Utc> = "2026-06-10T03:00:00Z".parse().unwrap();

        assert!(is_same_instant(occurrence + chrono::TimeDelta::milliseconds(400), &[occurrence]));
        assert!(!is_same_instant(occurrence + chrono::TimeDelta::seconds(2), &[occurrence]));
    }

    /// A manual run carries no schedule_id, so no schedule's reconcile can see it — which
    /// is also what keeps a rerun of a scheduled job safe from being cancelled.
    #[tokio::test]
    async fn a_run_nobody_scheduled_is_never_touched() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        let manual = db.insert_job_run_at(
            JobRunStatus::Submitted,
            Utc::now() + chrono::TimeDelta::days(365),
            None,
        ).await;

        db.scheduler().handle(&schedule).await.unwrap();

        assert_eq!(db.job_run(manual.id).await.status, JobRunStatus::Submitted);
    }
}
