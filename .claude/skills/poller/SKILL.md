---
name: poller
description: The Service trait and the Poller that drives it (src/poller.rs) - the loop every background service in flowlite runs on, its wake-ups, its error handling and the two [orchestrator] keys that time it. Use when adding a background service anywhere, when a loop fires too often or not at all, when one bad row appears to stall a service, when a wake-up seems missed, or when deciding what belongs in the loop rather than in a service.
---

# Poller and Service (src/poller.rs)

One loop drives all ten services. A `Service` holds only its own logic; the `Poller` owns the loop, the wake-ups and the error handling.

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

- **Wakes on its `Notify`** ([src/signals.rs](../../../src/signals.rs)) **or its interval, whichever comes first.** A wake-up carries no data, it only means "look again". The row's status column is the only thing that passes between services.
- **A wake-up published while the timer arm won is not lost**: a dropped `Notified` future keeps its permit for the next `select!`.
- **A failing `handle` is logged (via `row_context`) and skipped; the pass continues.** Failing the pass would stall every other row, and the restarted loop would pick the same bad row again.
- **A failing `select` ends the pass**, is logged, and the loop restarts after the backoff. This is the only error that propagates.
- **The interval is a safety net and cannot be turned off.** `flowlite job submit` writes a `job_run` from another process and can't publish, so only the interval notices it. Ticks use tokio's default `MissedTickBehavior::Burst`: a pass longer than the interval is followed straight away by another.

## The two knobs

Both are in `[orchestrator]` ([app_config skill](../app_config/SKILL.md)), not constants here. The `Poller` reads them from the `AppConfig` it is given, so a test can set its own interval.

| Key | Default | Times |
|---|---|---|
| `poll_interval_seconds` | 1 | the safety-net tick |
| `error_backoff_seconds` | 5 | the wait before restarting after a failed `select` |

## Who implements it

| Service | Owned by |
|---|---|
| `JobRunReleaser`, `JobRunDispatcher`, `JobRunMonitor`, `TaskRunDispatcher`, `TaskRunMonitor`, `TaskRunAttemptDispatcher`, `TaskRunAttemptMonitor` | [orchestrator skill](../orchestrator/SKILL.md), started by `Orchestrator::start` |
| `Scheduler` | [scheduler skill](../scheduler/SKILL.md) |
| `NotificationService` | [notifications skill](../notifications/SKILL.md) |
| `RetentionService` | [src/retention/service.rs](../../../src/retention/service.rs). Deletes finished job runs past `[job_defaults] keep_runs` and `[retention] keep_runs_total`, along with the [entities skill](../entities/SKILL.md)'s six disk tables |

The last three are started directly by [serve.rs](../../../src/cli/commands/serve.rs).

## Rules

- **Per-row work goes in `handle`, never in the loop.** The loop has no per-service branches.
- **`select` returns the rows this service owns, by status.** Services are coupled only through the status one writes and another selects on. Never add a direct call from one service to another.
- **`select` only selects.** A `select` error costs the whole service `error_backoff_seconds` on every pass until it clears, so other per-pass work (such as a sweep over some other set) must not go there, or one bad row stalls every row the service owns.
- **`row_context` goes into an error line**: give the row's id and what it belongs to, as a log reader would want.
- **Register wake-ups before spawning any `Poller`.** A poller's first pass runs as soon as it is spawned, so a wake-up registered inside `Poller::new` could miss a publish from a poller already running. The orchestrator skill has the two-line pattern.
- **A service nothing publishes to takes a bare `Notify`** rather than registering with `Signals`, so unrelated status changes don't wake it. `Scheduler` and `RetentionService` do this.
