use std::sync::Arc;
use crate::cron_trigger::CronTrigger;
use crate::crud::CRUD;
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

    async fn get_due_schedules(&self) -> anyhow::Result<Vec<Schedule>> {

        let current_ts = self.toolkit.get_current_ts();

        self.crud.select_schedules(
            &*self.conn_pool,
            &SelectSchedulesData {
                filter: SelectSchedulesDataFilter {
                    schedule_id: None,
                    name_like: None,
                    next_run_lt: Some(current_ts),
                    disabled: Some(false),
                },
                sort: Some(SelectSchedulesDataSort::RowId),
                limit: None,
                offset: None,
            },
        ).await
    }

    /// Submits every job of one due schedule and moves the schedule on to its next run.
    async fn handle_due_schedule(&self, schedule: &Schedule) -> anyhow::Result<()> {

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

        // A job still busy with an earlier run is passed over rather than queued: the
        // next tick of the schedule is a better time to run it than right after itself.
        for schedule_job in schedule_jobs.iter() {

            let at_max_active_runs = {
                let mut conn = self.conn_pool.acquire().await?;

                self.crud.is_job_at_max_active_runs(
                    &mut conn,
                    &schedule_job.job_id,
                ).await?
            };

            if at_max_active_runs {
                eprintln!(
                    "Scheduler skipped job {} of schedule {}: it is already at its max_active_runs",
                    schedule_job.job_id,
                    schedule.schedule_id,
                );
                continue;
            }

            let mut conn = self.conn_pool.acquire().await?;

            // next_run is advanced only after this loop, so bailing here would re-submit
            // the siblings already committed above on the next tick, and hot-loop the
            // schedule at 1 Hz for as long as this one job stays unsubmittable.
            if let Err(e) = self.crud.submit_job(&mut conn, &schedule_job.job_id).await {
                eprintln!(
                    "Scheduler could not submit job {} of schedule {}: {e:?}",
                    schedule_job.job_id,
                    schedule.schedule_id,
                );
                continue;
            }

            self.signals.publish();
        }

        let cron_trigger = CronTrigger::from_schedule(schedule);
        let next_run = cron_trigger.get_next_run(schedule.next_run);

        self.crud.update_schedules(
            &*self.conn_pool,
            &UpdateSchedulesData {
                input: UpdateSchedulesDataInput {
                    next_run: Some(next_run),
                },
                filter: UpdateSchedulesDataFilter {
                    schedule_id: Some(schedule.schedule_id.clone()),
                },
            },
        ).await?;

        Ok(())
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
        self.get_due_schedules().await
    }

    async fn handle(&self, schedule: &Schedule) -> anyhow::Result<()> {
        self.handle_due_schedule(schedule).await
    }
}
