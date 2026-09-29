---
name: app_config
description: The config layer (src/app_config/) - how config.toml and FLOWLITE_ environment variables become the one AppConfig every service reads, one file per section, and where a default belongs. Use when adding or changing a config key or section, deciding what a field should default to, wiring a new [section] into AppConfig, working out why a setting in config.toml appears to do nothing, or handling a secret or a concurrency limit.
---

# Config conventions (src/app_config/)

One `AppConfig`, built once at startup and handed to everything. `<data_dir>/config.toml` is read only by [src/app_config/app_config.rs](../../../src/app_config/app_config.rs); each `[section]` is a struct in its own file, re-exported from [mod.rs](../../../src/app_config/mod.rs). It holds what differs per machine; job and schedule definitions are YAML ([yaml skill](../yaml/SKILL.md)).

## How a value is resolved

`AppConfig::load` merges figment layers, lowest first:

1. `Serialized::defaults(AppConfig::default())` — a missing `config.toml` loads fine.
2. `Toml::file(data_dir/config.toml)`.
3. `Env::prefixed("FLOWLITE_").split("__")` — `FLOWLITE_SLACK__TOKEN` is `[slack] token`, `FLOWLITE_SECRETS__WAREHOUSE_PW` one `secrets` entry.
4. `Serialized::default("data_dir", ...)` — last, so a file inside the directory can't redirect it.

Then a concurrency limit named `global` is refused: `flowlite limits` prints the combined cap under that name.

## Adding a section (e.g. `[metrics]`)

1. `src/app_config/metrics.rs`, struct plus `Default` (no per-field serde defaults):

```rust
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigMetrics {
    pub bind: String,
    pub flush_interval_seconds: u64,
}

impl Default for AppConfigMetrics {
    fn default() -> Self {
        Self { bind: "127.0.0.1:9100".to_string(), flush_interval_seconds: 15 }
    }
}

impl AppConfigMetrics {
    pub fn flush_interval(&self) -> Duration { Duration::from_secs(self.flush_interval_seconds) }
}
```

2. `mod metrics;` and `pub use` in [mod.rs](../../../src/app_config/mod.rs).
3. A `#[serde(default)]` field on `AppConfig`, plus its line in `AppConfig::default()` and the manual `Debug`.
4. A test in [app_config.rs](../../../src/app_config/app_config.rs) `mod tests` loading a real `config.toml` from a temp dir.

## Rules

- **Every key has a default, defined here.** No reader writes `unwrap_or(30)`; a key without one breaks every existing `config.toml`.
- **A section with no possible default is `Option<T>`, absent = off**, with `#[serde(default, skip_serializing_if = "Option::is_none")]`. `[smtp]` / `[slack]`: a job naming an unconfigured channel is a **startup error**, not a silent non-delivery. Without `skip_serializing_if`, a `null` in the defaults layer is what a real section would have to merge over.
- **Serde defaults per field only inside `Option<T>` sections.** A `#[serde(default)]` section is in the defaults layer, so figment supplies every key (`[ui]` naming only `page_size` keeps the rest). An `Option<T>` section has nothing underneath: optional keys carry `#[serde(default = "...")]` (`smtp.port`, `slack.api_url`), and those without (`smtp.host`, `smtp.from`, `slack.token`) make a half-written section a startup error.
- **Durations: a `*_seconds` field plus a method returning `Duration`** (`poll_interval_seconds` / `poll_interval()`).
- **`secrets` is redacted; `concurrency_limits` isn't.** `secrets` is `#[serde(skip_serializing)]` and `Debug` prints its names against `<redacted>`, so a stray `{app_config:?}` can't dump credentials. A wrong limit should be visible.
- **`AppConfig`'s `Debug` is hand-written — add every new field.** Deriving would undo the redaction. Known exception: `smtp.password`, `slack.token` print via their sections' derived `Debug`.
- **A default a YAML file can override lives in `[job_defaults]` / `[schedule_defaults]`**; the YAML field is `Option<T>` and `CRUD::init` fills it. No `#[serde(default = ...)]` fallback on the YAML model.
- **A setting that "does nothing" is usually an undeclared key** — figment drops unknown keys silently.

## Who reads what

| Section | Read by |
|---|---|
| `[orchestrator]` | [src/poller.rs](../../../src/poller.rs) and the services ([orchestrator skill](../orchestrator/SKILL.md)) |
| `[ui]` | dashboard paging, refresh, palette ([router skill](../router/SKILL.md)) |
| `[job_defaults]`, `[schedule_defaults]` | `CRUD::init`, for what a YAML omits; `keep_runs` also by `RetentionService` ([src/retention/service.rs](../../../src/retention/service.rs)) for a job id with no `mem.job` row |
| `[retention]` | `RetentionService`: `keep_runs_total` (cross-job ceiling, oldest first, after per-job `keep_runs`), `max_deletes_per_pass` |
| `[smtp]`, `[slack]` | the channels ([notifications skill](../notifications/SKILL.md)) |
| `[secrets]` | `secret_env:`, checked at startup by `CRUD::check_secret_env_is_satisfied` |
| `[concurrency_limits]` | `limits:`, names checked at startup by `CRUD::validate_job_limits`, gated by the attempt dispatcher |
