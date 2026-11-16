---
name: yaml
description: The YAML config layer (src/yaml_models/ and src/declared/) - how files under the project dir are discovered, parsed into JobYaml/ScheduleYaml and loaded into the Declared struct every reader borrows, plus the serde conventions every model follows. Use when adding or changing a YAML model or field, when working with Declared, Job, Task, Schedule or ScheduleJob, or when a config file parses into something other than what you expected.
---

# YAML models

`src/yaml_models/` holds plain deserialization targets (no behaviour, no queries) for files under the **project dir** (`-p` / `--project-dir` / `FLOWLITE_PROJECT_DIR`, default the current directory):

| Model | Files | Becomes |
|---|---|---|
| [`JobYaml`](references/job_yaml.md) | `<project_dir>/jobs/*.yml` | `Job`, `Task`s |
| [`ScheduleYaml`](references/schedule_yaml.md) | `<project_dir>/schedules/*.yml` | `Schedule`, `ScheduleJob`s |

`flowlite.toml` is app config, not this layer ([src/app_config/](../../../src/app_config/)).

## Declared

[`Declared`](../../../src/declared/declared.rs) is what the YAML declares, parsed, defaulted and validated: four plain `Vec`s (`jobs`, `tasks`, `schedules`, `schedule_jobs`) and lookups (`job`, `tasks_of`, `task`, `schedule`, `jobs_of_schedule`). No database holds config.

- **`Declared::load(&app_config)`** walks the project dir in every process that needs config: `serve` once at startup, `job list` / `job submit` per command, the MCP `list_jobs` / `submit_job` tools per call (so a file written later is seen). `job-run` commands skip it on purpose, so they work when the YAML no longer parses.
- **Who holds it.** `serve` wraps it in an `Arc` and hands it to `Scheduler`, `Orchestrator` (for `JobRunDispatcher`'s `max_parallel_runs`), `RetentionService` (`keep_runs`) and `AppState` (the dashboard). CRUD never holds it: the two methods that read config, `submit_job` and `is_job_at_max_parallel_runs`, take `&Declared`. The dashboard shows what `serve` loaded, not the YAML as it is now.
- **Ad-hoc jobs** (`job submit -f`, MCP `submit_job`) parse one file or inline string and call `add_ad_hoc_job` on their own copy, which goes through the same `add_job` `load` uses, so the two can't drift. It refuses an installed id with `JobIdAlreadyInstalled` and checks only this job's `secret_env`.
- **`check_secret_env_is_satisfied`** runs in `serve` alone, never in `load` - `job list` must work without production secrets ([tests/serve_secret_check.rs](../../../tests/serve_secret_check.rs)).

Discovery: `.yml` or `.yaml` only (`is_yaml_file`); a missing directory means none; files load in `read_dir_sorted` order, and each list keeps that order - so a job's tasks come back as its YAML lists them, which is the order `submit_job` creates task runs in. Renaming a file reorders the jobs list.

## The `from_yaml` pattern

Copy `ScheduleYaml::from_yaml`; every step names the file, the only clue to *which* config is broken:

```rust
pub fn from_yaml(path: &Path) -> anyhow::Result<Self> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read Schedule YAML from {}", path.display()))?;
    let schedule: ScheduleYaml = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse Schedule YAML from {}", path.display()))?;
    schedule.validate().with_context(|| format!("Invalid Schedule YAML at {}", path.display()))?;
    Ok(schedule)
}
```

`JobYaml` adds `from_yaml_str(content, label)` so an inline `yaml` argument gets a file's exact messages (label `"<inline yaml>"`), plus `validate_task_ids` and `validate_secret_env` after `validate()`.

## Serde conventions

- **No `#[serde(default)]` means required**; missing fails startup. `Option<T>` defaults to `None`.
- **A configurable default is `Option<T>`** (`timeout`, `max_retries`, `retry_delay`, `max_parallel_runs`, `keep_runs`, `timezone`), filled by `Declared::load` from `[job_defaults]` / `[schedule_defaults]` ([app_config skill](../app_config/SKILL.md)). Never write that fallback into `#[serde(default = ...)]`: the model says what the file declared.
- **Unknown keys are silently dropped** (no `deny_unknown_fields`) — the likeliest reason a setting "doesn't work". Consider adding `#[serde(deny_unknown_fields)]` when touching a model.
- **Parsing validates types.** `cron: Schedule` and `timezone: Option<Tz>` fail at load naming the file, so `CronTrigger::from_schedule` can unwrap later.

## Validation

Per-field rules go on the model as `#[validate(...)]` (today only `ScheduleYaml` has any); anything needing config, another file or the whole task graph goes in [src/declared/validate.rs](../../../src/declared/validate.rs) (`validate_job_tasks`, `validate_job_notifications`, `validate_job_limits`), called from `Declared::add_job`. Duplicate job or schedule ids and a schedule naming an undeclared job are refused in `declared.rs` itself. Details in [job_yaml.md](references/job_yaml.md) and [schedule_yaml.md](references/schedule_yaml.md).

## Adding a model

1. New file in `src/yaml_models/`, `pub mod` it in [mod.rs](../../../src/yaml_models/mod.rs).
2. Derive `Deserialize, Validate, Debug`; add `from_yaml` as above.
3. Add its struct and a `Vec` of them to `Declared`, and its directory walk to `Declared::load`, wrapping each error with the file it came from.
