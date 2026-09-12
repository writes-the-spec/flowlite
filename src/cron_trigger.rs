use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;


use std::str::FromStr;
use crate::crud::schedule::Schedule;

/// A schedule's cron expression, the zone it is read in, and the dates that bound it.
///
/// The fields are private because the bounds are the whole point: reading `schedule` and
/// calling `after()` on it directly would skip `start_date` and `end_date`, which is the
/// one thing `get_next_run` is here to apply.
pub struct CronTrigger {
    schedule: CronSchedule,
    timezone: Tz,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
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

}

#[cfg(test)]
mod tests {
    use super::*;

    /// The repo's own spelling: six fields, seconds first, as `.data/schedules/` uses.
    fn trigger(cron: &str, start_date: Option<NaiveDate>, end_date: Option<NaiveDate>) -> CronTrigger {
        CronTrigger::new(
            CronSchedule::from_str(cron).unwrap(),
            Tz::Europe__Vienna,
            start_date,
            end_date,
        )
    }

    fn at(instant: &str) -> DateTime<Utc> {
        instant.parse().unwrap()
    }

    fn date(day: &str) -> Option<NaiveDate> {
        Some(day.parse().unwrap())
    }

    #[test]
    fn the_next_run_is_the_next_time_the_cron_matches() {
        let nightly = trigger("0 30 3 * * *", None, None);

        assert_eq!(
            nightly.get_next_run(Some(at("2026-06-10T00:00:00Z"))),
            Some(at("2026-06-10T01:30:00Z")),
        );
    }

    /// The cron fields are read in the schedule's own zone, so it is the *local* time that
    /// stays put across a DST change and the UTC instant that moves. 03:30 in Vienna is
    /// 02:30Z in winter and 01:30Z in summer - the property the README promises, and the
    /// reason the zone is stored on the row rather than assumed.
    #[test]
    fn the_cron_is_read_in_its_own_zone_so_the_utc_instant_moves_with_dst() {
        let nightly = trigger("0 30 3 * * *", None, None);

        assert_eq!(
            nightly.get_next_run(Some(at("2026-01-10T00:00:00Z"))),
            Some(at("2026-01-10T02:30:00Z")),
        );
        assert_eq!(
            nightly.get_next_run(Some(at("2026-06-10T00:00:00Z"))),
            Some(at("2026-06-10T01:30:00Z")),
        );
    }

    /// `start_date` moves the search forward to midnight on that day, so a schedule seeded
    /// before it begins gets its first run on the date it names rather than tomorrow.
    #[test]
    fn a_start_date_in_the_future_is_where_the_search_begins() {
        let nightly = trigger("0 30 3 * * *", date("2026-06-01"), None);

        assert_eq!(
            nightly.get_next_run(Some(at("2026-01-10T00:00:00Z"))),
            Some(at("2026-06-01T01:30:00Z")),
        );
    }

    /// None is what stops `next_run` advancing, which is how a bounded schedule retires
    /// rather than firing for ever.
    #[test]
    fn nothing_fires_after_the_end_date() {
        let nightly = trigger("0 30 3 * * *", None, date("2026-12-31"));

        assert_eq!(nightly.get_next_run(Some(at("2026-12-31T04:00:00Z"))), None);
    }

    /// The end date is inclusive to its last second, so the run on the day itself is not
    /// the one lost to the bound.
    #[test]
    fn the_end_date_itself_still_fires() {
        let nightly = trigger("0 30 3 * * *", None, date("2026-12-31"));

        assert_eq!(
            nightly.get_next_run(Some(at("2026-12-30T04:00:00Z"))),
            Some(at("2026-12-31T02:30:00Z")),
        );
    }

    /// Vienna skips 02:00-03:00 on 2026-03-29, so a 02:30 schedule has no instant to fire
    /// at that day and the run is lost rather than moved: the next one is the 30th. Worth
    /// knowing before choosing an hour for something that must run every day.
    #[test]
    fn a_time_the_spring_change_skips_loses_that_days_run() {
        let in_the_gap = trigger("0 30 2 * * *", None, None);

        assert_eq!(
            in_the_gap.get_next_run(Some(at("2026-03-28T12:00:00Z"))),
            Some(at("2026-03-30T00:30:00Z")),
        );
    }

    /// And 02:30 happens twice on 2026-10-25, once at UTC+2 and again at UTC+1. The first
    /// is taken, so the schedule fires once rather than twice - the other half of the same
    /// rule, and the reason `.earliest()` is not an arbitrary choice.
    #[test]
    fn a_time_the_autumn_change_repeats_fires_on_the_first_of_the_two() {
        let repeated = trigger("0 30 2 * * *", None, None);

        assert_eq!(
            repeated.get_next_run(Some(at("2026-10-24T12:00:00Z"))),
            Some(at("2026-10-25T00:30:00Z")),
        );
    }
}
