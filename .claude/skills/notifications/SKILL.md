# Notifications

`src/notifications/` delivers the messages something else decided to send. One background service, one channel per way of reaching somebody, and one table between it and whatever produced the work.

It is **not** part of the [orchestrator](../orchestrator/SKILL.md). `serve` starts it alongside, the way it starts the [Scheduler](../scheduler/SKILL.md) — see [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs).

| File | Holds |
|---|---|
| [service.rs](../../../src/notifications/service.rs) | `NotificationService` — the `Service` the `Poller` drives |
| [channel.rs](../../../src/notifications/channel.rs) | `NotificationChannels` — every channel this process can actually deliver over |
| [email.rs](../../../src/notifications/email.rs) | `EmailChannel` — the only channel today, and the one place that talks SMTP |
| [message.rs](../../../src/notifications/message.rs) | `NotificationMessage`, and what a failed job run says |

## The loop

`NotificationService` is an ordinary [`Service`](../../../src/poller.rs), so it gets the same loop, wake-ups and per-row error handling as every orchestrator service:

- **`select`** returns every **open** [`job_run_notification`](../entities/references/job_run_notification.md) — `status = 'pending'` — oldest first, whatever channel it names and whatever run it is about. A backlog after a restart therefore goes out in the order it built up in.
- **`handle`** builds the message, asks `NotificationChannels` to deliver it, and records the outcome on the row.

Its wake-up is registered in `serve` **before any `Poller` is spawned**, for the reason `Orchestrator::start` registers all of its own up front: a poller's first pass runs the moment it is spawned, and must not publish to a wake-up nobody has registered yet.

The service is started **whatever config.toml configures**. With no channel at all it simply has nothing open to deliver, and a row it cannot deliver is closed as failed with the reason on it — a state you can read, rather than a silence.

## How it is decoupled

The producer and the deliverer meet through a row and nowhere else, which is the same rule the orchestrator's own services follow.

`JobRunMonitor` inserts the notification as it finishes a failed run, in the same transaction as the status write, and then never thinks about it again. It does not call this service, hold a handle to it, or wait on a channel. This service never calls back into the orchestrator.

That is the whole point: delivery is slow and sometimes fails for hours. A monitor that waited on a relay would stop finishing everyone else's runs while it did.

**To notify about something new**, insert an open row from whatever knows the news — do not call the service.

## Channels

`NotificationChannels::from_config` builds every channel `config.toml` configures, once. `NotificationChannel` (the enum, on [the entity](../../../src/crud/job_run_notification.rs) beside the status it sits next to in the table) is the column, so a row says for itself what delivering it means rather than the sender guessing from the recipients.

Delivery is a **`match` on that enum, not a registry of trait objects**. The channels are known at compile time, so adding one is a variant plus an arm the compiler then demands — a better reminder than a `Vec` nobody was told to register in — and it avoids a boxed future per send for a set of two or three. See the [code-style skill](../code-style/SKILL.md) on not reaching for a trait to unify a handful of call sites.

A channel with nothing configured is `None` rather than absent, and asking for it is an error naming what is missing. That is deliberate: `Some`/`None` here is what turns "nobody configured SMTP" into a row you can read instead of a notification that quietly never leaves.

### Adding a channel

1. A variant on `NotificationChannel`, plus its `Display` arm.
2. A module beside [email.rs](../../../src/notifications/email.rs) with its own `send` and `max_output_bytes`.
3. Its config section on `AppConfig`, and a field on `NotificationChannels` built in `from_config`.
4. The arms the compiler now demands in `send` and `max_output_bytes`.
5. Whatever writes the rows has to name the new channel — today that is `JobRunMonitor`, from `job_run.on_failure_emails`. **That column is still email-shaped**; a second channel is the point at which it wants to become a channel-to-recipients map on the run, rather than one list per channel.

## Messages

`NotificationMessage` is `{ subject, body }` — the two parts every channel has some form of. `job_run_failure_message` is the only kind there is today, which is why it is a function beside the service rather than a trait.

**The cap on quoted output belongs to the channel, not the message.** `NotificationChannels::max_output_bytes` is asked per channel and passed into the builder, because the reason for a cap is the transport's own limit — a relay's maximum message size for email. Truncation has to happen while the body is built, since the output is embedded in formatted text no channel could safely cut afterwards.

`stream_tail` keeps the **end** of a stream and cuts the front, on a character boundary: the end is where a command says why it stopped, and that is what makes a capped alert still worth reading.

## What it does not do

**No retries.** Every path through `handle` writes the row, which is what stops a channel that is down from being hammered every second. There is deliberately no delay or attempt count — an alert nobody can see failed is worse than one that failed loudly. If retries are wanted, this table is the right shape for them: add `attempts` and `next_attempt_at`, and select on the latter.

**No decision about *whether* something is worth telling anyone.** That belongs to whatever writes the row — `JobRunMonitor` decides that `Failed` and `TimedOut` are news and `Aborted` is not.
