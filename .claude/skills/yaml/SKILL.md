---
name: yaml
description: The YAML config layer (src/yaml_models/) - how files under the config dir are discovered, parsed into JobYaml/ScheduleYaml and seeded into the in-memory schema, plus the serde conventions every model follows. Use when adding or changing a YAML model or field, or when a config file parses into something other than what you expected.
---

# YAML models

Everything flowlite knows before it runs comes from YAML files under the **config dir** (`--config-dir` / `FLOWLITE_CONFIG_DIR`, defaulting to the platform config dir). `src/yaml_models/` holds one module per file kind, and each one is a plain deserialization target — no behaviour, no queries:

| Model | Files | Seeds |
|---|---|---|
| [`JobYaml`](references/job_yaml.md) | `<config_dir>/jobs/*.yml` | `mem.job`, `mem.task`, `mem.task_dependent` |
| [`ScheduleYaml`](references/schedule_yaml.md) | `<config_dir>/schedules/*.yml` | `mem.schedule`, `mem.schedule_job` |

`<config_dir>/config.toml` is *not* part of this layer — it is app config, read by `AppConfig::load` ([src/app_config.rs](../../../src/app_config.rs)) through figment.

## Where they are read

`CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)) is the **only** caller, and it runs at the start of every process that needs the config — `serve`, `job list` and `job submit`. Not every CLI command does: `job-run logs` and `job-run rerun` read run history from disk alone, so they skip it deliberately and keep working against a config dir that no longer parses. It walks each directory, parses each file into its model, and inserts the rows into the in-memory `mem` schema, all inside one transaction. Nothing reads a YAML file again afterwards: at runtime the config *is* the `mem` tables (see the [db-storage skill](../db-storage/SKILL.md)).

Three discovery rules worth knowing:

- **`.yml` or `.yaml`.** `CRUD::init` accepts either extension (`is_yaml_file`) and ignores every other file in the directory.
- **A missing directory is not an error.** No `jobs/` dir simply means no jobs.
- **File order is the display order.** `read_dir_sorted` sorts the paths, and `row_id` is handed out in that order, which is what `SelectTasksDataSort::RowId` and the UI sort by. Renaming a file reorders the list.

## The `from_yaml` pattern

Both models expose the same three-step constructor, and a new model should copy it verbatim:

```rust
pub fn from_yaml(path: &Path) -> anyhow::Result<Self> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read Job YAML from {}", path.display()))?;

    let job: JobYaml = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse Job YAML from {}", path.display()))?;

    job.validate().with_context(|| format!("Invalid Job YAML at {}", path.display()))?;

    Ok(job)
}
```

Every step names the file it failed on — that context is the only thing that tells a user *which* config file is broken, so keep it on anything new.

## Serde conventions

- **A field without `#[serde(default)]` is required**, and a missing one fails startup. `Option<T>` fields are the exception: serde fills them with `None`.
- **Optional-with-a-value uses a `defaults.rs` helper**: `#[serde(default = "default_u32::<3600>")]`, `#[serde(default = "default_tz")]` ([src/yaml_models/defaults.rs](../../../src/yaml_models/defaults.rs)). Add new ones there rather than inline.
- **Unknown keys are ignored.** No model sets `deny_unknown_fields`, so a misspelled or unsupported key is silently dropped — the single most likely reason a setting "doesn't work". Consider adding `#[serde(deny_unknown_fields)]` if you touch a model and want typos to fail loudly.
- **Parsing does the type validation.** `cron: Schedule` and `timezone: Tz` deserialize into real parsed types, so an invalid value fails at load with the file path — which is why `CronTrigger::from_schedule` can unwrap them later.

## Validation

Both models derive `validator::Validate` and `from_yaml` calls `validate()`, but **no field declares a `#[validate(...)]` rule**, so today it is a no-op kept as a hook. The validation that actually exists is `CRUD::validate_job_tasks`, which runs in `CRUD::init` after parsing because it needs the whole job at once (see [job_yaml.md](references/job_yaml.md)).

Rule of thumb: per-field rules belong on the model via `#[validate(...)]`; anything that has to look at more than one field, or at another table, belongs in `CRUD::init`.

## Adding a model

1. New file in `src/yaml_models/`, `pub mod` it in [mod.rs](../../../src/yaml_models/mod.rs).
2. Derive `Deserialize, Validate, Debug`, add a `from_yaml` in the shape above.
3. Add its `mem` table migration under `db/schemas/memory/migrations/` — see [db-storage](../db-storage/SKILL.md).
4. Add its directory walk to `CRUD::init`, incrementing the shared `row_id` and wrapping each insert in `with_context`.
