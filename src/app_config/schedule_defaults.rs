use chrono_tz::Tz;
use serde::{Deserialize, Serialize};


/// What a schedule gets for a field its YAML leaves out.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigScheduleDefaults {
    /// The zone a schedule's cron expression is read in. An IANA name, so a schedule that
    /// says nothing follows this zone's daylight saving rather than a fixed offset.
    pub timezone: Tz,
}

impl Default for AppConfigScheduleDefaults {
    fn default() -> Self {
        Self {
            timezone: chrono_tz::UTC,
        }
    }
}
