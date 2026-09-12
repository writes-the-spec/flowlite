---
name: poller
description: The Service trait and the Poller that drives it (src/poller.rs) - the loop every background service in flowlite runs on, its wake-ups, its error handling and the two [orchestrator] keys that time it. Use when adding a background service anywhere, when a loop fires too often or not at all, when one bad row appears to stall a service, when a wake-up seems missed, or when deciding what belongs in the loop rather than in a service.
---

# Poller and Service (src/poller.rs)

One loop, written once, driving eight services. A `Service` holds nothing but its own logic; the `Poller` owns the loop, the wake-ups and the error handling.

```rust
pub trait Service: Send + Sync + 'static {
    type Row: Send + Sync;

    fn name(&self) -> &'static str;
    fn row_context(&self, row: &Self::Row) -> String;   // "task run attempt 5 of task run 3"
    async fn select(&self) -> anyhow::Result<Vec<Self::Row>>;
    async fn handle(&self, row: &Self::Row) -> anyhow::Result<()>;
}
```

`Poller::new(Arc::new(service), wakeup, app_config).start()` spawns the loop and returns immediately.

## What the loop guarantees

- **It wakes on whichever comes first**, its `Notify` wake-up ([src/signals.rs](../../../src/signals.rs)) or its interval. A wake-up is advisory and carries no data — it says "look again", never at what. The row's own status column is the only information that travels between services.
- **A wake-up published while the timer arm won is not lost.** A `Notified` future dropped unawaited keeps its permit, so the next `select!` sees it.
- **A failing row is logged and skipped; the pass continues.** `handle` returning an error costs that row and nothing else, named through `row_context`. This is the loop's job, not any service's: failing the whole pass over one row would stop every other row, and the restarted loop would select the same bad row again.
- **A failing `select` ends the pass**, is logged, and the loop restarts after the backoff. That is the only error that propagates.
- **The interval is a safety net, not the driver.** It cannot be turned off: `flowlite job submit` writes a `job_run` from another process and so cannot publish, and the interval is the only thing that notices. Ticks use tokio's default `MissedTickBehavior::Burst`, so a pass longer than the interval is followed immediately by another rather than by a catch-up delay.

## The two knobs

Both live in `[orchestrator]` ([app_config skill](../app_config/SKILL.md)), and neither is a constant in this file:

| Key | Default | What it times |
|---|---|---|
| `poll_interval_seconds` | 1 | the safety-net tick |
| `error_backoff_seconds` | 5 | the wait before restarting after a failed `select` |

The `Poller` takes the whole `AppConfig` and reads both through `app_config.orchestrator`, so a test can build one polling every 5 seconds without touching the code under test.

## Who implements it

| Service | Owned by |
|---|---|
| `JobRunDispatcher`, `JobRunMonitor`, `TaskRunDispatcher`, `TaskRunMonitor`, `TaskRunAttemptDispatcher`, `TaskRunAttemptMonitor` | [orchestrator skill](../orchestrator/SKILL.md) |
| `Scheduler` | [scheduler skill](../scheduler/SKILL.md) |
| `NotificationService` | [notifications skill](../notifications/SKILL.md) |

A service is started where its owner is: the six by `Orchestrator::start`, the other two directly by [serve.rs](../../../src/cli/commands/serve.rs).

## Rules

- **New per-row work goes in `handle`, never in the loop.** The loop is finished; it has no branches for a particular service.
- **`select` returns the rows this service owns, by status.** A service selects on a status another service writes — that is the whole coupling between them, and adding a direct call from one service to another instead is the thing this shape exists to prevent.
- **`row_context` is for an error line**, so name the row the way a person reading a log would: its id and what it belongs to.
- **Wake-ups are registered before any `Poller` is spawned.** A poller's first pass runs the instant it is spawned, so a service registering its own wake-up inside the `Poller::new` call could miss a publish from one already running. See the orchestrator skill for the two-line shape.
- **A service nothing publishes to takes a bare `Notify`** rather than registering with `Signals`, so it is not woken by every unrelated status change. The `Scheduler` is the example.
