---
name: app_config
description: The config layer (src/app_config/) - how config.toml and FLOWLITE_ environment variables become the one AppConfig every service reads, one file per section, and where a default belongs. Use when adding or changing a config key or section, deciding what a field should default to, wiring a new [section] into AppConfig, working out why a setting in config.toml appears to do nothing, or handling a secret or a concurrency limit.
---

# Config conventions (src/app_config/)

One `AppConfig` is built once, at startup, and handed to everything. `config.toml` sits in the data directory beside `jobs/` and `schedules/` — [src/app_config/app_config.rs](../../../src/app_config/app_config.rs) is the only thing that reads it, and each `[section]` is a struct in its own file beside it, re-exported from [mod.rs](../../../src/app_config/mod.rs).

Job and schedule *definitions* live in YAML; `config.toml` is what differs per machine. See the [yaml skill](../yaml/SKILL.md) for the other half of that split.

## How a value is resolved

`AppConfig::load` is four figment layers, lowest first:

1. `Serialized::defaults(AppConfig::default())` — every field's default, so a data directory with no `config.toml` loads exactly as well as a complete one.
2. `Toml::file(data_dir/config.toml)`.
3. `Env::prefixed("FLOWLITE_").split("__")` — `__` descends a level, so `FLOWLITE_SLACK__TOKEN` is `[slack] token` and `FLOWLITE_SECRETS__WAREHOUSE_PW` is one entry of the `secrets` map. Both layers reach the same field; which one a deployment uses is its own business.
4. `Serialized::default("data_dir", ...)` — last, because the directory flowlite was pointed at is not up for debate by a file inside it.

The only rule not expressible as a layer is checked straight after: a concurrency limit named `global` is refused, because `flowlite limits` prints the combined cap under that name and a job-named limit sharing it would make the row ambiguous.

## Adding a section (e.g. `[metrics]`)

1. `src/app_config/metrics.rs` with the struct and a `Default` impl. No per-field `#[serde(default = "...")]` is needed — see the last rule for when it is:

```rust
use std::time::Duration;
use serde::{Deserialize, Serialize};

/// What the metrics exporter is timed and addressed by.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigMetrics {
    pub bind: String,
    pub flush_interval_seconds: u64,
}

impl Default for AppConfigMetrics {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9100".to_string(),
            flush_interval_seconds: 15,
        }
    }
}

impl AppConfigMetrics {
    pub fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_interval_seconds)
    }
}
```

2. `mod metrics;` and a `pub use` line in [mod.rs](../../../src/app_config/mod.rs).
3. A `#[serde(default)]` field on `AppConfig`, and its line in `AppConfig::default()` and in the manual `Debug` impl.
4. A test in [app_config.rs](../../../src/app_config/app_config.rs)'s `mod tests`, which writes a real `config.toml` into a temp directory and loads it.

A section with **no** sensible default is `Option<T>` instead, with `#[serde(default, skip_serializing_if = "Option::is_none")]` — see the rule below.

## Rules

- **Every key has a default, and the default lives here.** A reader never says `unwrap_or(30)`. A new key without a default breaks every existing `config.toml`, since a file that predates it names nothing.
- **A section with no possible default is `Option<T>` and absent means off.** `[smtp]` and `[slack]` have no default because there is no default mail relay or Slack workspace. Absent means the channel is unconfigured, and a job asking for it is a **startup error** rather than a message nobody gets at 03:00. Both carry `skip_serializing_if = "Option::is_none"`: serialized as `null`, the defaults layer would become something a real section has to merge *over*, and the section would fight its own default.
- **A duration is a `*_seconds` field plus a method returning `Duration`.** `poll_interval_seconds` / `poll_interval()`. The number is what a person writes in TOML; the `Duration` is what the code wants, and converting once here beats converting at every call site.
- **`secrets` is redacted, `concurrency_limits` deliberately is not.** `secrets` is `#[serde(skip_serializing)]` and printed by a manual `Debug` impl as its names against `<redacted>`, so a `{app_config:?}` added later cannot dump every credential at once — the names stay, because which secrets are loaded is the one thing that field is useful for. `concurrency_limits` keeps both, since a wrong limit should be visible.
- **`AppConfig`'s `Debug` is written by hand, so a new field must be added to it.** Deriving it would be one line and would undo the redaction above. `smtp.password` and `slack.token` are carried by their own derived `Debug` and are a known exception.
- **A default that a YAML file can override belongs in `[job_defaults]` or `[schedule_defaults]`, and the YAML field is `Option<T>`.** The model says what the file declared; `CRUD::init` fills the `None`s from here. Don't write the fallback into a `#[serde(default = ...)]` on the YAML model as well — see the [yaml skill](../yaml/SKILL.md).
- **A merged section needs no per-field serde defaults; an `Option<T>` section needs one per optional key.** A section reached by `#[serde(default)]` is present in the defaults layer, so figment has already supplied every key by the time serde sees the merged value — which is why `[ui]` naming only `page_size` keeps the other two rather than failing on them. An `Option<T>` section is skipped from that layer, so nothing underlies it: each optional key carries its own `#[serde(default = "...")]` (`smtp.port`, `slack.api_url`), and the keys that carry none — `smtp.host`, `smtp.from`, `slack.token` — are exactly what makes a half-written section a startup error rather than a silent default.
- **A setting that "does nothing" is usually a key the model never declared.** Unknown keys in `config.toml` are dropped by figment as silently as unknown keys in a job YAML are dropped by serde.

## Who reads what

| Section | Read by |
|---|---|
| `[orchestrator]` | [src/poller.rs](../../../src/poller.rs) and the services — see the [orchestrator skill](../orchestrator/SKILL.md) |
| `[ui]` | the dashboard's paging and refresh — see the [router skill](../router/SKILL.md) |
| `[job_defaults]`, `[schedule_defaults]` | `CRUD::init`, filling what a YAML left out |
| `[smtp]`, `[slack]` | the channels — see the [notifications skill](../notifications/SKILL.md) |
| `[secrets]` | a task's `secret_env:`, checked at startup by `CRUD::check_secret_env_is_satisfied` |
| `[concurrency_limits]` | a task's `limits:`, gated by the attempt dispatcher |
