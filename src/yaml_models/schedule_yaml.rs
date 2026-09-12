use chrono_tz::Tz;
use cron::Schedule;
use serde::Deserialize;
use validator::Validate;
use std::collections::BTreeMap;
use crate::yaml_models::string_map::deserialize_string_map;

use std::path::Path;
use anyhow::Context;

#[derive(Deserialize, Validate, Debug)]
pub struct ScheduleYaml {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub cron: Schedule,
    /// `None` is "not declared", which `CRUD::init` resolves against
    /// `[schedule_defaults]` in config.toml rather than a zone written here.
    #[serde(default)]
    pub timezone: Option<Tz>,
    pub start_date: Option<chrono::NaiveDate>,
    pub end_date: Option<chrono::NaiveDate>,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub jobs: Vec<ScheduleYamlJob>
}

impl ScheduleYaml {
    pub fn from_yaml(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read Schedule YAML from {}", path.display()))?;

        let schedule: ScheduleYaml = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse Schedule YAML from {}", path.display()))?;

        schedule.validate().with_context(|| format!("Invalid Schedule YAML at {}", path.display()))?;

        Ok(schedule)
    }
}


#[derive(Deserialize, Validate, Debug)]
pub struct ScheduleYamlJob {
    pub id: String,
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub parameters: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_yaml` takes a path, so each case is a real file - which is also what makes the
    /// "every message names the file it failed on" half of the contract checkable.
    fn parse(content: &str) -> anyhow::Result<ScheduleYaml> {
        let path = std::env::temp_dir()
            .join(format!("flowlite-schedule-{}.yaml", uuid::Uuid::new_v4()));

        std::fs::write(&path, content).unwrap();

        let parsed = ScheduleYaml::from_yaml(&path);

        std::fs::remove_file(&path).unwrap();

        parsed
    }

    fn nightly(cron: &str, timezone: &str) -> String {
        format!("id: nightly\nname: Nightly\ncron: \"{cron}\"\n{timezone}")
    }

    #[test]
    fn a_six_field_cron_and_a_named_zone_parse() {
        let schedule = parse(&nightly("0 30 3 * * *", "timezone: Europe/Vienna\n")).unwrap();

        assert_eq!(schedule.id, "nightly");
        assert_eq!(schedule.timezone, Some(Tz::Europe__Vienna));
    }

    /// The one gotcha the README calls out: the `cron` crate's dialect puts seconds first,
    /// so a line pasted from crontab is five fields and means nothing here. It is refused at
    /// parse because the field is typed `cron::Schedule`, not `String` - and that refusal is
    /// what lets `CronTrigger::from_schedule` unwrap the stored expression later.
    #[test]
    fn a_five_field_crontab_line_is_refused_naming_the_file() {
        let error = parse(&nightly("30 3 * * *", "timezone: Europe/Vienna\n")).unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("Failed to parse Schedule YAML"), "{message}");
        assert!(message.contains("flowlite-schedule-"), "{message}");
    }

    /// Typed `Option<Tz>` for the same reason, so a zone that does not exist fails startup
    /// rather than silently becoming UTC at the first fire.
    #[test]
    fn an_unknown_timezone_is_refused() {
        let error = parse(&nightly("0 30 3 * * *", "timezone: Europe/Viena\n")).unwrap_err();

        assert!(format!("{error:#}").contains("Failed to parse Schedule YAML"), "{error:#}");
    }

    /// None is "not declared", which `CRUD::init` resolves against `[schedule_defaults]`.
    /// A zone defaulted here instead would be a second place the fallback lives.
    #[test]
    fn a_schedule_naming_no_zone_leaves_it_for_config_to_fill() {
        let schedule = parse(&nightly("0 30 3 * * *", "")).unwrap();

        assert_eq!(schedule.timezone, None);
    }

    /// Both dates are optional and bound the firing window at either end.
    #[test]
    fn start_and_end_dates_parse_when_declared() {
        let schedule = parse(&nightly(
            "0 30 3 * * *",
            "start_date: 2026-01-01\nend_date: 2026-12-31\n",
        )).unwrap();

        assert_eq!(schedule.start_date, Some("2026-01-01".parse().unwrap()));
        assert_eq!(schedule.end_date, Some("2026-12-31".parse().unwrap()));
    }

    /// No model sets `deny_unknown_fields`, so a misspelled key is dropped rather than
    /// refused - the single most likely reason a setting "doesn't work". Pinned rather than
    /// endorsed: if that choice is ever revisited, this test is what says so out loud.
    #[test]
    fn a_misspelled_key_is_silently_ignored() {
        let schedule = parse(&nightly("0 30 3 * * *", "disbaled: true\n")).unwrap();

        assert!(!schedule.disabled, "the typo set nothing, and nothing said so");
    }

    /// A required field is one without `#[serde(default)]`, and its absence fails startup.
    #[test]
    fn a_schedule_without_a_cron_is_refused() {
        let error = parse("id: nightly\nname: Nightly\n").unwrap_err();

        assert!(format!("{error:#}").contains("Failed to parse Schedule YAML"), "{error:#}");
    }
}
