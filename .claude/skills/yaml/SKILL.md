---
name: yaml
description: The YAML config layer (src/yaml_models/) - how files under the data dir are discovered, parsed into JobYaml/ScheduleYaml and seeded into the in-memory schema, plus the serde conventions every model follows. Use when adding or changing a YAML model or field, or when a config file parses into something other than what you expected.
---

# YAML models

Everything flowlite knows before it runs comes from YAML files under the **data dir** (`-D` / `--data-dir` / `FLOWLITE_DATA_DIR`, defaulting to the current directory), the same directory `flowlite.db` is written to. `src/yaml_models/` holds one module per file kind, and each one is a plain deserialization target — no behaviour, no queries:

| Model | Files | Seeds |
|---|---|---|
| [`JobYaml`](references/job_yaml.md) | `<data_dir>/jobs/*.yml` | `mem.job`, `mem.task`, `mem.task_dependent` |
| [`ScheduleYaml`](references/schedule_yaml.md) | `<data_dir>/schedules/*.yml` | `mem.schedule`, `mem.schedule_job` |

`<data_dir>/config.toml` is *not* part of this layer — it is app config, read by `AppConfig::load` ([src/app_config/](../../../src/app_config/), one file per `config.toml` section) through figment. It does reach this layer in one place: `timeout`, `max_retries`, `retry_delay`, `max_parallel_runs` and a schedule's `timezone` are `Option` on the models, and `CRUD::init` resolves a `None` against `[job_defaults]` or `[schedule_defaults]` as it inserts — so "the default" for those five is the data dir's, not a value in `#[serde(default = ...)]`.

## Where they are read

`CRUD::init` ([src/crud/crud.rs](../../../src/crud/crud.rs)) is the **only** caller, and it runs at the start of every process that needs the config — `serve`, `job list` and `job submit`. Not every CLI command does: `job-run logs` and `job-run rerun` read run history from disk alone, so they skip it deliberately and keep working against a data dir whose YAML no longer parses. It walks each directory, parses each file into its model, and inserts the rows into the in-memory `mem` schema, all inside one transaction. Nothing reads a YAML file again afterwards: at runtime the config *is* the `mem` tables (see the [entities skill](../entities/SKILL.md)).

Three discovery rules worth knowing:

- **`.yml` or `.yaml`.** `CRUD::init` accepts either extension (`is_yaml_file`) and ignores every other file in the directory.
- **A missing directory is not an error.** No `jobs/` dir simply means no jobs.
- **File order is the display order.** `read_dir_sorted` sorts the paths, and `row_id` is handed out in that order, which is what `SelectTasksDataSort::RowId` and the UI sort by. Renaming a file reorders the list.

## The `from_yaml` pattern

`ScheduleYaml::from_yaml` is the three-step constructor a new model should copy:

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

Every step names the file it failed on — that context is the only thing that tells a user *which* config file is broken, so keep it on anything new.

`JobYaml` has outgrown that shape in two ways, and a new model only needs them if it shares the reasons. It splits the tail into `from_yaml_str(content, label)` so the MCP `submit_job` tool's inline `yaml` argument is parsed and message-formatted exactly as a file is, passing `"<inline yaml>"` where a path would go — which is why every message takes a `label` rather than a `path`. And it runs a fourth step, `validate_secret_env`, because `secret_env` can only come from a file and its self-consistency is worth checking once at parse rather than on every read.

## Serde conventions

- **A field without `#[serde(default)]` is required**, and a missing one fails startup. `Option<T>` fields are the exception: serde fills them with `None`.
- **A field whose default is configurable is `Option<T>`**, filled in by `CRUD::init` from `[job_defaults]` or `[schedule_defaults]` — `timeout`, `max_retries`, `retry_delay`, `max_parallel_runs`, `timezone`. Don't write the fallback into a `#[serde(default = ...)]`; the model's job is to say what the file declared. (There is no longer a `defaults.rs`: its last helper went when `timezone` became configurable.)
- **Unknown keys are ignored.** No model sets `deny_unknown_fields`, so a misspelled or unsupported key is silently dropped — the single most likely reason a setting "doesn't work". Consider adding `#[serde(deny_unknown_fields)]` if you touch a model and want typos to fail loudly.
- **Parsing does the type validation.** `cron: Schedule` and `timezone: Option<Tz>` deserialize into real parsed types, so an invalid value fails at load with the file path — which is why `CronTrigger::from_schedule` can unwrap them later. The configured fallback zone is parsed the same way, by `AppConfig`.

## Validation

Both models derive `validator::Validate` and `from_yaml` calls `validate()`, but **no field declares a `#[validate(...)]` rule**, so today it is a no-op kept as a hook. The validation that actually exists is `CRUD::validate_job_tasks`, which runs in `CRUD::init` after parsing because it needs the whole job at once (see [job_yaml.md](references/job_yaml.md)).

Rule of thumb: per-field rules belong on the model via `#[validate(...)]`; anything that has to look at more than one field, or at another table, belongs in `CRUD::init`.

## Adding a model

1. New file in `src/yaml_models/`, `pub mod` it in [mod.rs](../../../src/yaml_models/mod.rs).
2. Derive `Deserialize, Validate, Debug`, add a `from_yaml` in the shape above.
3. Add its `mem` table migration under `db/schemas/memory/migrations/` — see [entities](../entities/SKILL.md).
4. Add its directory walk to `CRUD::init`, incrementing the shared `row_id` and wrapping each insert in `with_context`.
