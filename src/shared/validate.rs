//! Whether `serve` would start on this data directory's configuration: what `flowlite
//! validate` and the `validate_data_dir` tool answer, and what `serve` itself loads.

use serde::Serialize;

use crate::app_config::AppConfig;
use crate::declared::Declared;


/// What a configuration that passed holds, for `flowlite validate --json` and the
/// `validate_data_dir` tool.
#[derive(Debug, Serialize)]
pub struct ValidateResult {
    pub jobs: usize,
    pub schedules: usize,
}


/// The jobs and schedules `serve` runs, refused for anything that would stop it starting.
/// One function for `serve` and `validate`, so a configuration that validates is one
/// `serve` starts on.
///
/// The secrets check is here and not in `Declared::load`, because the `job` commands load
/// the same YAML: `flowlite job list` must still work in a terminal that has not exported
/// the production secrets. `serve` is the process that spawns commands, so it fails before
/// the bind rather than at 03:00.
pub fn load_declared_for_serve(app_config: &AppConfig) -> anyhow::Result<Declared> {

    let declared = Declared::load(app_config)?;

    declared.check_secret_env_is_satisfied(&app_config.secrets, None)?;

    Ok(declared)
}


pub fn validate(app_config: &AppConfig) -> anyhow::Result<ValidateResult> {

    let declared = load_declared_for_serve(app_config)?;

    Ok(ValidateResult {
        jobs: declared.jobs.len(),
        schedules: declared.schedules.len(),
    })
}
