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
