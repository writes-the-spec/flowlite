---
name: notifications
description: The notification service (src/notifications/) - the background loop that delivers a job run's outcome over email or Slack, the NotificationChannel enum that decides how, and the single NotificationMessage rendered as text, HTML and Slack blocks. Use when adding or changing a channel, changing what a notification says or how it is rendered, tracing why one was sent, skipped or left pending, or working with job_run_notification rows, FakeSlack, or the [smtp] and [slack] config sections.
---

# Notifications

Delivers messages something else decided to send: one service, one channel per transport, and one table between it and the producer. It is **not** part of the [orchestrator](../orchestrator/SKILL.md). `serve` starts it separately, as it does the [Scheduler](../scheduler/SKILL.md) ([src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs)).

| File | Holds |
|---|---|
| [service.rs](../../../src/notifications/service.rs) | `NotificationService`, the `Service` the `Poller` drives |
| [channel.rs](../../../src/notifications/channel.rs) | `NotificationChannels`: every channel this process can deliver over |
| [email.rs](../../../src/notifications/email.rs) | `EmailChannel`, the only code that talks SMTP |
| [slack.rs](../../../src/notifications/slack.rs) | `SlackChannel`, the only code that talks to Slack's API |
| [message.rs](../../../src/notifications/message.rs) | `NotificationMessage` and a finished job run's text, HTML and Slack blocks |
| [templates/notifications/job_run.html](../../../templates/notifications/job_run.html) | the HTML rendering |

## The loop

An ordinary [`Service`](../../../src/poller.rs):

- **`select`** returns every open [`job_run_notification`](../entities/references/job_run_notification.md) (`status = 'pending'`), oldest first, across all channels and runs, so a backlog after a restart goes out in order.
- **`handle`** decides what one row deserves and does it.

**Open does not mean ready.** Rows are written when the run is *submitted*, so most passes find the run still going. `handle` checks whether the run has finished, then whether this row wants that ending:

| The run | `notify_on: failure` | `notify_on: success` |
|---|---|---|
| not finished | left open | left open |
| `Failed`, `TimedOut` | delivered → `sent` / `failed` | `skipped` |
| `Succeeded` | `skipped` | delivered → `sent` / `failed` |
| `Aborted`, `Skipped`, `Deleted` | `skipped` (somebody stopped it and already knows) | `skipped` |
| `Invalid` | delivered → `sent` / `failed` (nobody chose this ending, so it most needs reporting) | `skipped` |

"Has it ended?" is `JobRunStatus::is_finished` ([src/crud/job_run.rs](../../../src/crud/job_run.rs)). "Is this ending mine?" is `NotifyOn::wants` ([src/crud/job_run_notification.rs](../../../src/crud/job_run_notification.rs)), a question for the row, never the service: a job that wants both endings gets **a row for each**, and one ending settles them differently. Both functions match exhaustively, so a new run status won't compile until it states its meaning for failure and for success.

- **The wake-up is registered in `serve` before any `Poller` is spawned** (see the [poller skill](../poller/SKILL.md)).
- **Started whatever config.toml says.** A row it cannot deliver is closed as `failed` with the reason on it, so the failure is visible rather than silent.
- **One row per channel, one per ending**, each selected, delivered and recorded independently, so a Slack outage can't block the mail. Of a job's two ending rows, exactly one is ever delivered.

## Decoupling

`CRUD::submit_job` and `CRUD::rerun_job` write the open rows alongside the run and its task runs, snapshotted like commands and parameters. **The orchestrator writes nothing here**: `JobRunMonitor` finishes a run and publishes without knowing this table exists, and this service never calls back. Only the run's status column crosses. That matters because delivery is slow and can fail for hours, and a monitor stuck waiting on a relay would stop finishing other runs. Writing at submit also means the row exists before the run can fail, so it needn't share the monitor's status-write transaction (a monitor never revisits a finished run).

**To notify about something new**, insert an open row from whatever creates the watched thing. Don't call the service, and don't insert when the news happens.

## Channels

`NotificationChannels::from_config` builds every channel `config.toml` configures, once. `NotificationChannel` (the enum, in [src/crud/job_run_notification.rs](../../../src/crud/job_run_notification.rs)) is a column, so each row says how it is delivered. Each variant is spelled **exactly as the key a job uses** under `on_failure:` / `on_success:`, so an error can name the YAML to fix with no mapping table.

| Channel | Config | Addresses | Delivers |
|---|---|---|---|
| `email` | `[smtp]` | addresses | one message to all of them |
| `slack` | `[slack]` | conversations | one `chat.postMessage` each |

- **Slack uses a bot token, not an incoming webhook.** A webhook URL is its destination, so each extra conversation would put another secret in job YAML, and keeping secrets out of the YAML is why config.toml is separate.
- **A Slack refusal comes back as `ok: false` in a 200**, so the status code tells you nothing.
- **One conversation per call.** One refusal doesn't stop the rest, because an alert delivered somewhere beats one delivered nowhere.
- **Delivery is a `match` on the enum, not a registry of trait objects.** The set is known at compile time, so a new channel means a variant plus the arms the compiler demands, with no boxed future per send. See the [code-style skill](../code-style/SKILL.md).
- **An unconfigured channel is `None`**, and asking for it returns an error naming what's missing. That error is what puts "nobody configured SMTP" on the row instead of a notification that silently never goes out.
- **Tests send to a fake Slack on localhost, not a mock.** `FakeSlack` ([src/test_support.rs](../../../src/test_support.rs)) records posts and refuses the conversations a test names, and `TestDb::notification_service_with_slack` points `[slack]` at it, so the full delivered path can be asserted. It lives in `test_support` because the channel tests and the service tests share it, and both must agree with `post_payload`.

### Adding a channel

1. A `NotificationChannel` variant named as the YAML key, plus its `Display` arm.
2. A module beside [slack.rs](../../../src/notifications/slack.rs) with `send` and `max_output_bytes`.
3. Its config section on `AppConfig`, and a field on `NotificationChannels` built in `from_config`.
4. The compiler-demanded arms in `send` and `max_output_bytes`.
5. A field on `JobYamlNotify` and its line in `job_notify_recipients` ([crud.rs](../../../src/crud/crud.rs)), which is where the YAML's per-channel fields become the job row's `channel -> recipients` map, once per notify block. `job_run_notification_definitions` ([job_run_definition.rs](../../../src/crud/multistatements/job_run_definition.rs)) needs no change.
6. The compiler-demanded arm in `CRUD::validate_job_notifications`, naming the config section the channel requires (checked once per block, not per ending).

Steps 4 and 6 are why it's an enum: a channel can't compile without saying how to deliver it and what makes it deliverable.

## Messages

`NotificationMessage` is `{ subject, body, html, blocks }`. Each channel uses what it can: email reads `html` and ignores `blocks`, and Slack does the reverse. `job_run_message` is the only message kind, so it's a function, not a trait.

- **One shape for both endings.** The run's status drives the wording throughout. The only difference is quoted output: a success has no failures, so that section is empty and not written.
- **The output cap belongs to the channel.** `NotificationChannels::max_output_bytes` is looked up per channel and passed to the builder: for email it's the relay's size limit, and for Slack it's how much anyone will scroll, so Slack's default is smaller. Truncation happens during building, because output embedded in formatted text can't safely be cut afterwards.
- **`stream_tail` keeps the end** of a stream, cutting the front on a character boundary, because the end is where a command says why it stopped.
- **Slack blocks** (`message_blocks`): a header with a status emoji, the summary as two columns of fields, the task list as one section per 3000 chars, and the failing output fenced. Slack's own text rules stay in [slack.rs](../../../src/notifications/slack.rs): `escape` for `&`, `<` and `>` (without it `a < b` gets eaten), and `fence_safe` for a fence the command printed itself.
- **Slack rejects oversize messages, so the builder trims**: 150 chars per header, 3000 per section, 2000 per field, 50 blocks. Escaping makes `&` five chars, so the char cap bites first, and a stream is cut *inside* its fence. Cut after the fence and the close is lost, swallowing the rest into a code block. An overrun message is cut from the end, and its last block repeats the command that shows the rest.
- **`html` and `blocks` are `Option`; channels fall back to text.** A template that won't render makes `message_html` log and return `None`, which costs the formatting, not the notification. `EmailChannel::build` sends `multipart/alternative` when there is HTML, plain text otherwise. `post_payload` sends blocks with the subject in `text` (read by previews and screen readers), or the whole body in one fence when there are no blocks.
- **All three renderings live in [message.rs](../../../src/notifications/message.rs) and share `summary_rows`, `message_lead`, `failure_title`, `logs_command` and `stream_tail`**, so no rendering can miss a fact. `job_run_accent` and `job_run_emoji` match exhaustively side by side, so a new status must say how it reads everywhere.
- **The template** ([job_run.html](../../../templates/notifications/job_run.html)) is one for both endings, laid out with tables and inline styles because mail clients fetch no stylesheet. Askama escapes every value, so quoting command output is safe.

## What it does not do

- **No retries.** Every path that reaches a channel writes the row, so a down channel isn't hammered every second, and an alert that fails should fail loudly. If you want retries, add `attempts` and `next_attempt_at` to this table and select on the latter.
- **Never writes a run status.** Nothing here can change how a run ends.
- **Doesn't choose which endings matter.** The job's YAML declares that and it's frozen onto the row at submit. The service only asks whether this row's ending happened.
