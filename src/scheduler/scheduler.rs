use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use crate::cron_trigger::CronTrigger;
use crate::crud::CRUD;
use crate::crud::schedule::{Schedule, SelectSchedulesData, SelectSchedulesDataFilter, SelectSchedulesDataSort, UpdateSchedulesData, UpdateSchedulesDataFilter, UpdateSchedulesDataInput};
use crate::crud::schedule_job::{SelectScheduleJobsData, SelectScheduleJobsDataFilter, SelectScheduleJobsDataSort};
use crate::toolkit::Toolkit;


pub struct Scheduler {
    pub toolkit: Arc<Toolkit>,
    pub crud: Arc<CRUD>,
    pub conn_pool: Arc<sqlx::SqlitePool>,
}


impl Scheduler {

    pub fn new(
        toolkit: Arc<Toolkit>,
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> Self {
        Self {
            toolkit,
            crud,
            conn_pool,
        }
    }

    pub fn start(&self) {

        let toolkit = self.toolkit.clone();
        let crud = self.crud.clone();
        let conn_pool = self.conn_pool.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::run(toolkit.clone(), crud.clone(), conn_pool.clone()).await {
                    eprintln!("Scheduler error, restarting in 5s: {e:?}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

    }

    /// Submits the jobs of every due schedule, once per second, until selecting them fails.
    async fn run(
        toolkit: Arc<Toolkit>,
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
    ) -> anyhow::Result<()> {

        let mut timer = interval(Duration::from_secs(1));

        loop {
            
            timer.tick().await;

            let current_ts = toolkit.get_current_ts();

            let schedules = crud.select_schedules(
                &*conn_pool,
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
            )
                .await?;

            // A schedule the service can never handle is logged and left for the next tick:
            // failing the whole loop over it would stop every other schedule from running,
            // since the restarted loop would select the same schedule again.
            for schedule in schedules.iter() {
                if let Err(e) = Self::handle_due_schedule(
                    crud.clone(),
                    conn_pool.clone(),
                    schedule,
                ).await {
                    eprintln!("Scheduler error on schedule {}: {e:?}", schedule.schedule_id);
                }
            }

        }
        
    }

    /// Submits every job of one due schedule and moves the schedule on to its next run.
    async fn handle_due_schedule(
        crud: Arc<CRUD>,
        conn_pool: Arc<sqlx::SqlitePool>,
        schedule: &Schedule,
    ) -> anyhow::Result<()> {

        let schedule_jobs = crud.select_schedule_jobs(
            &*conn_pool,
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
                let mut conn = conn_pool.acquire().await?;

                crud.is_job_at_max_active_runs(
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

            let mut conn = conn_pool.acquire().await?;

            // One unsubmittable job must not stop the schedule: bailing here would leave
            // next_run unadvanced, and the schedule would try again on every tick.
            if let Err(e) = crud.submit_job(&mut conn, &schedule_job.job_id).await {
                eprintln!(
                    "Scheduler could not submit job {} of schedule {}: {e:?}",
                    schedule_job.job_id,
                    schedule.schedule_id,
                );
                continue;
            }
        }

        let cron_trigger = CronTrigger::from_schedule(schedule);
        let next_run = cron_trigger.get_next_run(schedule.next_run);

        crud.update_schedules(
            &*conn_pool,
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
