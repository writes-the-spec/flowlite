---
name: test_support
description: The shared test fixtures (src/test_support.rs) - TestDb, the environment lock, FakeSlack and the row builders every unit test builds its state with. Use when writing a test that needs a database, a service, a spawned process, a secret or a Slack send; when a test passes alone and fails in a full run; when deciding between a unit test and an integration test in tests/; or when tempted to seed the shared `mem`.
---

# Test support (src/test_support.rs)

`#[cfg(test)]` in [lib.rs](../../../src/lib.rs): unit tests only. `tests/` links the library without `cfg(test)` and can't use it.

## TestDb

`TestDb::new()`: a private `flowlite.db` in a temp dir, plus `CRUD`, pool, `Signals` and one shared `TaskRunAttemptChildren`.

- **Services get their real CRUD, never a fake** (`job_run_dispatcher()`, `task_run_attempt_monitor()`, `notification_service()`, `scheduler()`, …). Assert by reading back the row the next poll pass would read — tables are the only channel between services. Variants name what they vary: `task_run_attempt_dispatcher_with_secrets`, `_with_max_running_attempts`, `_with_concurrency_limits`.
- **Builders / readers:** `insert_job_run`, `insert_task_run_for_command`, `insert_task_run_depending_on`, `insert_task_run_attempt`, `orphan_task_run_attempt`, `backdate_task_run_attempt`; `job_run(id)`, `task_run_attempts(id)`. `map(&[("TZ", "UTC")])` builds the `BTreeMap` for `env`, `secret_env`, `parameters`.
- **Seeded `mem`:** `TestDb::new_with_migrated_mem()` migrates `mem` under a private name and returns the connection keeping it alive (hold it). Only then use `insert_job` / `seed_schedule*`.

## Two hazards of one process

A binary's tests are **threads of one process**, so process-wide state is shared.

- **`mem`:** `TestDb::new()` leaves it unmigrated on purpose — seeding the shared `flowlite_mem` races every other test ("database schema is locked: mem"). Use a private name (`new_with_migrated_mem`, `Toolkit::with_fresh_mem`, `crud_with_private_mem` in crud.rs tests) or an integration test driving the binary (`tests/adhoc_submit.rs`, `tests/serve_secret_check.rs`). See [toolkit skill](../toolkit/SKILL.md).
- **Environment:** a test that *sets* a variable takes `writing_the_environment()`; one that *reads* it (loading `AppConfig`, spawning a command) takes `reading_the_environment()`. A lock, because the failure is a test that passes alone and fails in a full run. Bind it: `let _guard = ...` holds; `let _ = ...` drops at once.

## FakeSlack

A real localhost HTTP server, not a mock. Records each `FakeSlackPost` (auth header, channel, text, blocks); `FakeSlack::refusing` refuses named conversations. `TestDb::notification_service_with_slack` points `[slack]` at it. Lives here because the channel's and the notification service's tests both use it and must agree with `post_payload` ([notifications skill](../notifications/SKILL.md)).

## Rules

- **Add a fixture only when a second test needs it.**
- **Builders insert through `CRUD`**, not raw SQL, to exercise production's write path.
- **Name a variant for what it varies**; don't add a parameter every caller must pass.
- **Prefer a unit test.** `tests/` is for what needs a second process — the built binary, a real lock, a real spawn; each file's module docs say which.
