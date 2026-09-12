---
name: test_support
description: The shared test fixtures (src/test_support.rs) - TestDb, the environment lock, FakeSlack and the row builders every unit test builds its state with. Use when writing a test that needs a database, a service, a spawned process, a secret or a Slack send; when a test passes alone and fails in a full run; when deciding between a unit test and an integration test in tests/; or when tempted to seed the shared `mem`.
---

# Test support (src/test_support.rs)

`#[cfg(test)]` in [lib.rs](../../../src/lib.rs), so it exists only for the crate's own unit tests. The integration tests under `tests/` link the library **without** `cfg(test)` and cannot use any of it — which is part of why they exist at all.

## TestDb

`TestDb::new()` is one test's own `flowlite.db` in a temp directory nothing else shares, plus a `CRUD`, a pool, a `Signals` and one `TaskRunAttemptChildren`.

**Services are built with their real CRUD, never a fake.** `TestDb` hands out a configured `JobRunDispatcher`, `TaskRunAttemptMonitor`, `NotificationService` and the rest, so a settle chain is asked what it wrote by reading the row back out of the table the next poll pass would read it from. That table is the only channel the services have between them, and a hand-built struct cannot stand in for it. The variants say what they vary — `task_run_attempt_dispatcher_with_secrets`, `_with_max_running_attempts`, `_with_concurrency_limits`.

Row builders come in the same spirit: `insert_job_run`, `insert_task_run_for_command`, `insert_task_run_depending_on`, `insert_task_run_attempt`, `orphan_task_run_attempt`, `backdate_task_run_attempt`, and readers like `job_run(id)` / `task_run_attempts(id)` to assert on what a pass wrote. `map(&[("TZ", "UTC")])` builds the `BTreeMap<String, String>` that `env`, `secret_env` and `parameters` all take.

## Two hazards, both about one process

Cargo runs a test binary's tests as **threads of one process**, so anything process-wide is shared between tests running in parallel.

**`mem` is one shared-cache name per process.** `TestDb` deliberately leaves the memory schema unmigrated: no monitor reads a definition, and seeding `flowlite_mem` from a unit test races every other test's connection over that one name — the "database schema is locked: mem" failure. If a test genuinely needs seeded config, take a private name (`Toolkit::with_fresh_mem`, or `CRUD::crud_with_private_mem`) or write an integration test that drives the built binary instead. `tests/adhoc_submit.rs` and `tests/serve_secret_check.rs` are there for exactly this reason. See the [toolkit skill](../toolkit/SKILL.md).

**The environment is one map per process.** A test that *sets* a variable takes `writing_the_environment()`; a test that *reads* the environment — loading an `AppConfig`, spawning a command — takes `reading_the_environment()`. One writer, many readers. It is a lock rather than a convention because the failure it prevents is the worst kind: a test that passes alone and fails in a full run, blaming whichever load or spawn happened to overlap.

Both guards must be **bound to a name**. `let _guard = writing_the_environment();` holds it; `let _ = writing_the_environment();` drops it on the spot and holds nothing.

## FakeSlack

A real HTTP server on localhost, not a mock. It records each `FakeSlackPost` — authorization header, channel, text, blocks — and can be told to refuse a named conversation, so the delivered path is assertable end to end: the message a real run's rows build, the post it becomes, and the `sent` row written afterwards. `TestDb::notification_service_with_slack` points a `[slack]` section at it.

It lives here rather than in `slack.rs` because both the channel's tests and the notification service's use it, and both have to agree with `post_payload` about what a post looks like. See the [notifications skill](../notifications/SKILL.md).

## Rules

- **Add a fixture here only when a second test needs it.** One test's setup belongs in that test.
- **A builder inserts through `CRUD`**, not through hand-written SQL, so a test is exercising the same write path production takes.
- **Name a variant for what it varies**, the way the dispatcher variants do, rather than adding a parameter to the plain one that every existing caller then has to pass.
- **Prefer a unit test.** `tests/` is for what needs a second process — the built binary, a real lock, a real spawn — and its module docs say which reason applies.
