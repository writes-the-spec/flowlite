---
name: yaml
description: The YAML config layer (src/yaml_models/) - how files under the data dir are discovered, parsed into JobYaml/ScheduleYaml and seeded into the in-memory schema, plus the serde conventions every model follows. Use when adding or changing a YAML model or field, or when a config file parses into something other than what you expected.
---

# YAML models

`src/yaml_models/` holds plain deserialization targets (no behaviour, no queries) for files under the **data dir** (`-D` / `--data-dir` / `FLOWLITE_DATA_DIR`, default the current directory):

| Model | Files | Seeds |
|---|---|---|
| [`JobYaml`](references/job_yaml.md) | `<data_dir>/jobs/*.yml` | `mem.job`, `mem.task`, `mem.task_dependent` |
| [`ScheduleYaml`](references/schedule_yaml.md) | `<data_dir>/schedules/*.yml` | `mem.schedule`, `mem.schedule_job` |

`config.toml` is app config, not this layer ([src/app_config/](../../../src/app_config/)).

## Where they are read

- **`CRUD::init`** ([src/crud/crud.rs](../../../src/crud/crud.rs)) walks the data dir and seeds `mem` in one transaction, in every process that needs config (`serve`, `job list`, `job submit`, MCP tools). `job-run logs` / `job-run rerun` skip it on purpose, so they work when the YAML no longer parses. Nothing rereads YAML afterwards: at runtime config *is* the `mem` tables ([entities skill](../entities/SKILL.md)).
- **Ad-hoc jobs** (`job submit -f`, MCP `submit_job`) parse one file or inline string and seed it through the same `CRUD::seed_job` `init` uses, so the two can't drift.

Discovery: `.yml` or `.yaml` only (`is_yaml_file`); a missing directory means none; `read_dir_sorted` order sets `row_id`, which is the display order (`SelectTasksDataSort::RowId`) — renaming a file reorders the list.

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
- **A configurable default is `Option<T>`** (`timeout`, `max_retries`, `retry_delay`, `max_parallel_runs`, `keep_runs`, `timezone`), filled by `CRUD::init` from `[job_defaults]` / `[schedule_defaults]` ([app_config skill](../app_config/SKILL.md)). Never write that fallback into `#[serde(default = ...)]`: the model says what the file declared.
- **Unknown keys are silently dropped** (no `deny_unknown_fields`) — the likeliest reason a setting "doesn't work". Consider adding `#[serde(deny_unknown_fields)]` when touching a model.
- **Parsing validates types.** `cron: Schedule` and `timezone: Option<Tz>` fail at load naming the file, so `CronTrigger::from_schedule` can unwrap later.

## Validation

Per-field rules go on the model as `#[validate(...)]` (today only `ScheduleYaml` has any); anything needing config, another table or the whole task graph goes in `CRUD::seed_job` (`validate_job_tasks`, `validate_job_notifications`, `validate_job_limits`). Details in [job_yaml.md](references/job_yaml.md) and [schedule_yaml.md](references/schedule_yaml.md).

## Adding a model

1. New file in `src/yaml_models/`, `pub mod` it in [mod.rs](../../../src/yaml_models/mod.rs).
2. Derive `Deserialize, Validate, Debug`; add `from_yaml` as above.
3. Add its `mem` migration under `db/schemas/memory/migrations/` ([entities](../entities/SKILL.md)).
4. Add its directory walk to `CRUD::init`, incrementing the shared `row_id` and wrapping each insert in `with_context`.
