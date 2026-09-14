use std::sync::Arc;
use chrono::{DateTime, Utc};
use crate::cron_trigger::CronTrigger;
use crate::crud::CRUD;
use crate::crud::job_run::{DeleteJobRunsData, DeleteJobRunsDataFilter, JobRun, JobRunStatus, SelectJobRunsData, SelectJobRunsDataFilter, SelectJobRunsDataSort};
use crate::crud::schedule::{Schedule, SelectSchedulesData, SelectSchedulesDataFilter, SelectSchedulesDataSort, UpdateSchedulesData, UpdateSchedulesDataFilter, UpdateSchedulesDataInput};
use crate::crud::schedule_job::{SelectScheduleJobsData, SelectScheduleJobsDataFilter, SelectScheduleJobsDataSort};
use crate::poller::Service;
use crate::signals::Signals;
use crate::toolkit::Toolkit;


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

    /// Every schedule, not only the due ones. A disabled schedule still has outstanding
    /// runs to take back, and "due" is no longer a question this service asks at all —
    /// JobRunReleaser asks it, of the runs rather than of the schedules.
    async fn get_schedules(&self) -> anyhow::Result<Vec<Schedule>> {

        self.crud.select_schedules(
            &*self.conn_pool,
            &SelectSchedulesData {
                filter: SelectSchedulesDataFilter {
                    schedule_id: None,
                    name_like: None,
                    next_run_lt: None,
                    disabled: None,
                },
                sort: Some(SelectSchedulesDataSort::RowId),
                limit: None,
                offset: None,
            },
        ).await
    }

    /// The next `submit_ahead` occurrences strictly after `now`: what ought to exist.
    ///
    /// Empty for a disabled schedule, and short or empty for one past its end_date, where
    /// `get_next_run` returns None. Both fall out of the same rule rather than needing a
    /// branch of their own.
    ///
    /// `now` is passed in rather than read here, so that this and the past-due guard in
    /// `delete_surplus_runs` derive from one instant. Reading the clock a second time - even
    /// only as late as the cron parse - opens a window in which an occurrence falls after
    /// the first read and before the second: it would then be missing from `desired` while
    /// still counting as future-dated to every guard, and the reconcile would delete the run
    /// at the exact moment it came due. Same argument as the one that moved
    /// `delete_job_runs_with_children`'s id resolution inside its transaction, one level up.
    fn desired_occurrences(&self, schedule: &Schedule, now: DateTime<Utc>) -> Vec<DateTime<Utc>> {

        if schedule.disabled {
            return Vec::new();
        }

        let cron_trigger = CronTrigger::from_schedule(schedule);

        let mut occurrences = Vec::new();
        let mut from = Some(now);

        for _ in 0..schedule.submit_ahead {
            let Some(next) = cron_trigger.get_next_run(from) else {
                break;
            };

            occurrences.push(next);
            from = Some(next);
        }

        occurrences
    }

    /// Makes this schedule's outstanding runs equal its desired occurrences.
    ///
    /// Convergent on purpose: every case in the design - a first pass, a top-up after a
    /// release, a restart, submit_ahead changing either way, an edited cron, a disabled
    /// schedule - is the same two loops finding a different difference. There is no
    /// "what changed?" to get wrong, because nothing is remembered between passes.
    async fn reconcile_schedule(&self, schedule: &Schedule) -> anyhow::Result<()> {

        let now = self.toolkit.get_current_ts();
        let desired = self.desired_occurrences(schedule, now);
        let outstanding = self.outstanding_runs(Some(&schedule.schedule_id)).await?;

        self.delete_surplus_runs(&outstanding, &desired, schedule, now).await?;
        self.submit_missing_runs(&outstanding, &desired, schedule).await?;

        // next_run is no longer a cursor - nothing advances it, and nothing reads it to
        // decide anything. It is written here purely so the dashboard can show when a
        // schedule next runs.
        self.crud.update_schedules(
            &*self.conn_pool,
            &UpdateSchedulesData {
                input: UpdateSchedulesDataInput {
                    next_run: Some(desired.first().copied()),
                },
                filter: UpdateSchedulesDataFilter {
                    schedule_id: Some(schedule.schedule_id.clone()),
                },
            },
        ).await?;

        Ok(())
    }

    async fn outstanding_runs(&self, schedule_id: Option<&str>) -> anyhow::Result<Vec<JobRun>> {

        self.crud.select_job_runs(
            &*self.conn_pool,
            &SelectJobRunsData {
                filter: SelectJobRunsDataFilter {
                    id: None,
                    job_id: None,
                    status: Some(JobRunStatus::Submitted),
                    statuses: None,
                    scheduled_at_lte: None,
                    schedule_id: schedule_id.map(str::to_string),
                },
                sort: Some(SelectJobRunsDataSort::Id),
                limit: None,
                offset: None,
            },
        ).await
    }

    /// Takes back the runs of schedules that are no longer there.
    ///
    /// A schedule deleted from the YAML has no row to reconcile, so no schedule's own pass
    /// can ever collect its leftovers: `delete_surplus_runs` scopes every delete to the
    /// schedule it is reconciling. Left alone, those runs would be released and would run,
    /// which is the one thing deleting a schedule is supposed to stop.
    ///
    /// It runs once per pass, at the head of `select`, because that is the shape of the
    /// question - "is anything outstanding for a schedule that is gone?" is asked of the
    /// whole set, not of one row - and because `Service::Row` is `Schedule`, so `handle`
    /// has nothing to be called with.
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

            let mut conn = self.conn_pool.acquire().await?;

            self.crud.delete_job_runs_with_children(&mut conn, &DeleteJobRunsData {
                filter: DeleteJobRunsDataFilter {
                    id: Some(run.id),
                    job_id: None,
                    status: Some(JobRunStatus::Submitted),
                    schedule_id: Some(schedule_id),
                    scheduled_at_gt: Some(now),
                },
            }).await?;

            self.signals.publish();
        }

        Ok(())
    }

    /// Deletes the outstanding runs the schedule no longer asks for.
    ///
    /// Only ones still in the future: a run whose instant has arrived belongs to
    /// JobRunReleaser, and deleting it here would cancel a run at the moment it came due.
    /// The same conditions are restated on the delete's own filter, because the releaser may
    /// have promoted the row since this select read it - and the delete re-resolves that
    /// filter inside its transaction, so the row it checks is the row it removes. The check
    /// on the run's due time is the weaker half of that: `scheduled_at` is never updated
    /// after insert, so a stale reading of it is not a thing that can happen.
    ///
    /// Deleted rather than stopped. A run cancelled before it was ever due never happened,
    /// and settling it as skipped would put it in the history beside runs that were
    /// genuinely stood down. This reconcile also recreates whatever it wrongly removes on
    /// the next pass, which a stop row would not.
    async fn delete_surplus_runs(
        &self,
        outstanding: &[JobRun],
        desired: &[DateTime<Utc>],
        schedule: &Schedule,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {

        for run in outstanding {

            if run.scheduled_at <= now || is_same_instant(run.scheduled_at, desired) {
                continue;
            }

            let mut conn = self.conn_pool.acquire().await?;

            self.crud.delete_job_runs_with_children(&mut conn, &DeleteJobRunsData {
                filter: DeleteJobRunsDataFilter {
                    id: Some(run.id),
                    job_id: None,
                    status: Some(JobRunStatus::Submitted),
                    schedule_id: Some(schedule.schedule_id.clone()),
                    scheduled_at_gt: Some(now),
                },
            }).await?;

            self.signals.publish();
        }

        Ok(())
    }

    /// Submits what is missing, per occurrence and per job.
    ///
    /// Per job rather than per occurrence, because a schedule can name several and one of
    /// them failing to submit must not leave the rest of that occurrence permanently
    /// half-written: the next pass finds the gap and fills it.
    async fn submit_missing_runs(
        &self,
        outstanding: &[JobRun],
        desired: &[DateTime<Utc>],
        schedule: &Schedule,
    ) -> anyhow::Result<()> {

        let schedule_jobs = self.crud.select_schedule_jobs(
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
        ).await?;

        for occurrence in desired {
            for schedule_job in schedule_jobs.iter() {

                let already_submitted = outstanding.iter().any(|run| {
                    run.job_id == schedule_job.job_id
                        && is_same_instant(run.scheduled_at, std::slice::from_ref(occurrence))
                });

                if already_submitted {
                    continue;
                }

                let mut conn = self.conn_pool.acquire().await?;

                // Reported and stepped over rather than raised on: one unsubmittable job
                // must not stop the schedule's other jobs, or the other schedules, and the
                // next pass will try it again anyway.
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

}


/// Compared at whole seconds. A cron occurrence has no sub-second part, but an instant that
/// has been through the database has been through a string, and an equality that depends on
/// that round-trip being exact would fail by deleting and resubmitting the same run every
/// pass — the most expensive way this could go wrong and the least visible.
///
/// Both the delete's "is this still wanted?" and the submit's "have I written this already?"
/// ask through here, so the rule is stated once rather than once per caller.
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
        // the rows handle will be called with - see `delete_orphaned_runs`.
        self.delete_orphaned_runs(&schedules).await?;

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
                scheduled_at_lte: None,
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

    /// Once the releaser has taken an occurrence, it is no longer outstanding, so the
    /// desired set has moved on by one and the next pass tops it back up - which is the
    /// whole of "keep submit_ahead occurrences submitted", with no advancing cursor.
    #[tokio::test]
    async fn a_released_occurrence_is_topped_up_on_the_next_pass() {

        let (db, _mem_conn) = scheduled_db().await;
        let schedule = db.seed_schedule("nightly", "0 0 3 * * *", 1).await;

        db.scheduler().handle(&schedule).await.unwrap();
        let first = outstanding(&db, "nightly").await;
        assert_eq!(first.len(), 1);

        // Exactly what JobRunReleaser does to a run whose instant has arrived.
        db.crud.update_job_runs(&*db.conn_pool, &crate::crud::job_run::UpdateJobRunsData {
            input: crate::crud::job_run::UpdateJobRunsDataInput {
                status: Some(JobRunStatus::Queued),
                started_at: None,
                finished_at: None,
            },
            filter: crate::crud::job_run::UpdateJobRunsDataFilter { id: Some(first[0].id) },
        }).await.unwrap();

        db.scheduler().handle(&schedule).await.unwrap();

        let second = outstanding(&db, "nightly").await;

        assert_eq!(second.len(), 1);
        assert_ne!(second[0].id, first[0].id);
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
