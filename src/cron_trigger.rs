use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;


use std::str::FromStr;
use crate::crud::schedule::Schedule;

pub struct CronTrigger {
    pub schedule: CronSchedule,
    pub timezone: Tz,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
}

impl CronTrigger {
    
    pub fn new(
        schedule: CronSchedule,
        timezone: Tz,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Self {

        Self {
            schedule,
            timezone,
            start_date,
            end_date,
        }
    }

    pub fn from_schedule(
        schedule: &Schedule,
    ) -> Self {
        Self {
            schedule: CronSchedule::from_str(&*schedule.cron).unwrap(),
            timezone: Tz::from_str(&*schedule.timezone).unwrap(),
            start_date: schedule.start_date,
            end_date: schedule.end_date,
        }
    }

    
    pub fn get_next_run(&self, start_from: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
        self.get_next_run_tz(start_from).map(|dt| dt.with_timezone(&Utc))
    }

    fn get_next_run_tz(&self, start_from: Option<DateTime<Utc>>) -> Option<DateTime<Tz>> {

        let start_from = match start_from {
            Some(t) => t.with_timezone(&self.timezone),
            None => Utc::now().with_timezone(&self.timezone),
        };

        let start_from = if let Some(start_date) = self.start_date {
            let start_dt = start_date
                .and_hms_opt(0, 0, 0)?
                .and_local_timezone(self.timezone)
                .earliest()?;
            if start_from < start_dt {
                start_dt
            } else {
                start_from
            }
        } else {
            start_from
        };

        let next = self.schedule.after(&start_from).next()?;

        if let Some(end_date) = self.end_date {
            let end_dt = end_date
                .and_hms_opt(23, 59, 59)?
                .and_local_timezone(self.timezone)
                .latest()?;
            if next > end_dt {
                return None;
            }
        }

        Some(next)
    }

    pub fn get_next_runs(&self, n: i32, start_from: Option<DateTime<Utc>>) -> Vec<DateTime<Utc>> {
        let mut runs = Vec::new();
        let mut current_start = start_from;

        for _ in 0..n {
            if let Some(next) = self.get_next_run_tz(current_start) {
                let next_utc = next.with_timezone(&Utc);
                runs.push(next_utc);
                current_start = Some(next_utc);
            } else {
                break;
            }
        }

        runs
    }

    pub fn get_previous_run(&self, start_from: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
        self.get_previous_run_tz(start_from).map(|dt| dt.with_timezone(&Utc))
    }

    fn get_previous_run_tz(&self, start_from: Option<DateTime<Utc>>) -> Option<DateTime<Tz>> {
        let start_from = match start_from {
            Some(t) => t.with_timezone(&self.timezone),
            None => Utc::now().with_timezone(&self.timezone),
        };

        let start_from = if let Some(end_date) = self.end_date {
            let end_dt = end_date
                .and_hms_opt(23, 59, 59)?
                .and_local_timezone(self.timezone)
                .latest()?;
            if start_from > end_dt {
                end_dt
            } else {
                start_from
            }
        } else {
            start_from
        };

        let prev = self.schedule.after(&start_from).rev().next()?;

        if let Some(start_date) = self.start_date {
            let start_dt = start_date
                .and_hms_opt(0, 0, 0)?
                .and_local_timezone(self.timezone)
                .earliest()?;
            if prev < start_dt {
                return None;
            }
        }

        Some(prev)
    }

    pub fn get_previous_runs(&self, n: i32, start_from: Option<DateTime<Utc>>) -> Vec<DateTime<Utc>> {
        let mut runs = Vec::new();
        let mut current_start = start_from;

        for _ in 0..n {
            if let Some(prev) = self.get_previous_run_tz(current_start) {
                let prev_utc = prev.with_timezone(&Utc);
                runs.push(prev_utc);
                current_start = Some(prev_utc);
            } else {
                break;
            }
        }

        runs
    }

    pub fn get_runs_between(&self, from_ts: DateTime<Utc>, to_ts: DateTime<Utc>) -> Vec<DateTime<Utc>> {
        let mut runs = Vec::new();
        let mut current_ts = Some(from_ts);

        let to_ts_tz = to_ts.with_timezone(&self.timezone);

        while let Some(next) = self.get_next_run_tz(current_ts) {
            if next > to_ts_tz {
                break;
            }
            let next_utc = next.with_timezone(&Utc);
            runs.push(next_utc);
            current_ts = Some(next_utc);
        }

        runs
    }

    pub fn validate_expression(expression: &str) -> anyhow::Result<()> {
        CronSchedule::from_str(expression)?;
        Ok(())
    }

    pub fn validate_timezone(timezone: &str) -> anyhow::Result<()> {
        Tz::from_str(timezone)?;
        Ok(())
    }

}
