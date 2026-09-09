# Notifications

`src/notifications/` delivers the messages something else decided to send. One background service, one channel per way of reaching somebody, and one table between it and whatever produced the work.

It is **not** part of the [orchestrator](../orchestrator/SKILL.md). `serve` starts it alongside, the way it starts the [Scheduler](../scheduler/SKILL.md) — see [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs).

| File | Holds |
|---|---|
| [service.rs](../../../src/notifications/service.rs) | `NotificationService` — the `Service` the `Poller` drives |
| [channel.rs](../../../src/notifications/channel.rs) | `NotificationChannels` — every channel this process can actually deliver over |
| [email.rs](../../../src/notifications/email.rs) | `EmailChannel` — the one place that talks SMTP |
| [slack.rs](../../../src/notifications/slack.rs) | `SlackChannel` — the one place that talks to Slack's API |
| [message.rs](../../../src/notifications/message.rs) | `NotificationMessage`, and what a finished job run says — as text, as HTML and as Slack blocks |
| [templates/notifications/job_run.html](../../../templates/notifications/job_run.html) | the HTML rendering of a finished job run |

## The loop

`NotificationService` is an ordinary [`Service`](../../../src/poller.rs), so it gets the same loop, wake-ups and per-row error handling as every orchestrator service:

- **`select`** returns every **open** [`job_run_notification`](../entities/references/job_run_notification.md) — `status = 'pending'` — oldest first, whatever channel it names and whatever run it is about. A backlog after a restart therefore goes out in the order it built up in.
- **`handle`** decides what one deserves, and does it.

**Open does not mean ready.** A notification is written when its run is *submitted*, long before anyone knows whether it will be needed, so most passes over one are about a run still going. `handle` asks the run's status first, and then asks the *row* whether that is what it was written for:

| The run | `notify_on: failure` | `notify_on: success` |
|---|---|---|
| not finished | left open, asked again next pass | left open, asked again next pass |
| `Failed`, `TimedOut` | message built and delivered, then `sent` or `failed` | closed as `skipped` |
| `Succeeded` | closed as `skipped` | message built and delivered, then `sent` or `failed` |
| `Aborted`, `Skipped` | closed as `skipped` — somebody stopped it, and they know | closed as `skipped` |
| `Invalid` | message built and delivered, then `sent` or `failed` — nobody chose this ending, so it is the one most worth telling | closed as `skipped` |

The two questions live in different places on purpose. "Has it ended?" is about the run, so it is `JobRunStatus::is_finished` ([src/crud/job_run.rs](../../../src/crud/job_run.rs)). "Is this ending mine?" is about the notification, so it is `NotifyOn::wants` ([src/crud/job_run_notification.rs](../../../src/crud/job_run_notification.rs)) — the service never decides it, because a run whose job asked to hear either way carries **a row for each**, and one run ending has to settle them differently.

Both are matched exhaustively, so a new run status has to say what it means for a failure notification *and* for a success one, or it stops compiling.

Its wake-up is registered in `serve` **before any `Poller` is spawned**, for the reason `Orchestrator::start` registers all of its own up front: a poller's first pass runs the moment it is spawned, and must not publish to a wake-up nobody has registered yet.

The service is started **whatever config.toml configures**. With no channel at all it simply has nothing open to deliver, and a row it cannot deliver is closed as failed with the reason on it — a state you can read, rather than a silence.

A job naming two channels is submitted with **two rows**, and each is selected, delivered and recorded on its own. That is the whole of the isolation between channels: a Slack workspace that is down cannot swallow the mail, and neither row knows the other exists. A job naming both endings is two rows for the same reason, and exactly one of them is ever delivered.

## How it is decoupled

The producer and the deliverer meet through a row and nowhere else, which is the same rule the orchestrator's own services follow.

`CRUD::submit_job` and `CRUD::rerun_job` write the open rows as part of a run's definition, in the same call that writes the run and its task runs — who to tell, and what to tell them about, is snapshotted exactly like the commands and the parameters. **The orchestrator writes nothing at all here**: `JobRunMonitor` finishes a run and publishes, and does not know this table exists. This service never calls back into it either. The only thing that crosses between them is the run's status column, which this service reads.

That is the whole point: delivery is slow and sometimes fails for hours. A monitor that waited on a relay would stop finishing everyone else's runs while it did.

Writing the row at submit rather than at failure also removes a problem instead of guarding one. When the monitor wrote it, the insert had to share a transaction with the status write — a monitor only visits `Running` rows, so a status write that committed alone would leave a finished run nothing ever looks at again. By the time a run can fail, the row is already there.

**To notify about something new**, insert an open row from whatever creates the thing being watched — do not call the service, and do not reach for the moment the news happens.

## Channels

`NotificationChannels::from_config` builds every channel `config.toml` configures, once. `NotificationChannel` (the enum, on [the entity](../../../src/crud/job_run_notification.rs) beside the status it sits next to in the table) is the column, so a row says for itself what delivering it means rather than the sender guessing from the recipients.

Two today, `email` and `slack`, and each variant is spelled **exactly as the key a job declares it under** inside `on_failure:` or `on_success:` — which is what lets an error name the YAML the reader has to go and edit, without a second table mapping one spelling to the other.

| Channel | Config | Addresses | Delivers |
|---|---|---|---|
| `email` | `[smtp]` | addresses | one message to all of them |
| `slack` | `[slack]` | conversations | one `chat.postMessage` each |

Slack is a **bot token, not an incoming webhook**. A webhook URL *is* its destination, so a job naming a second conversation would have to carry a second secret URL in its YAML — and keeping secrets out of the files that sit beside the jobs is the whole reason config.toml and the YAML are split. Two things about its API are worth knowing before touching that file: a refusal comes back as `ok: false` in a **200**, so the status code alone says nothing; and it posts to one conversation per call, so several recipients are several calls — a refusal by one does not stop the rest, since an alert delivered somewhere beats one delivered nowhere.

Delivery is a **`match` on that enum, not a registry of trait objects**. The channels are known at compile time, so adding one is a variant plus an arm the compiler then demands — a better reminder than a `Vec` nobody was told to register in — and it avoids a boxed future per send for a set of two or three. See the [code-style skill](../code-style/SKILL.md) on not reaching for a trait to unify a handful of call sites.

A channel with nothing configured is `None` rather than absent, and asking for it is an error naming what is missing. That is deliberate: `Some`/`None` here is what turns "nobody configured SMTP" into a row you can read instead of a notification that quietly never leaves.

**A send is tested against a Slack that answers on localhost**, not against a mock. `FakeSlack` in [src/test_support.rs](../../../src/test_support.rs) records what was posted and refuses the conversations a test names, and `TestDb::notification_service_with_slack` hands the service a `[slack]` pointing at it — which is what makes the delivered path assertable at all: the message a real run's rows build, the post it becomes, and the `sent` written afterwards. It lives in `test_support` rather than in [slack.rs](../../../src/notifications/slack.rs) because both the channel's tests and the service's use it, and both have to agree with `post_payload` about what a post looks like.

### Adding a channel

1. A variant on `NotificationChannel`, named as the YAML key, plus its `Display` arm.
2. A module beside [slack.rs](../../../src/notifications/slack.rs) with its own `send` and `max_output_bytes`.
3. Its config section on `AppConfig`, and a field on `NotificationChannels` built in `from_config`.
4. The arms the compiler now demands in `send` and `max_output_bytes`.
5. A field on `JobYamlNotify`, and its line in `job_notify_recipients` ([crud.rs](../../../src/crud/crud.rs)) — the one place the YAML's per-channel fields become the `channel -> recipients` map the job row stores, once per notify block. `job_run_notification_definitions` in [misc.rs](../../../src/crud/multistatements/misc.rs) walks those maps and needs no change.
6. The arm the compiler demands in `CRUD::validate_job_notifications`, saying which config section the channel needs to work at all. It is checked once per block, so nothing there is per-ending either.

Steps 4 and 6 are the point of the enum: a channel cannot be added without saying both how to deliver it and what makes it deliverable, because neither match compiles until it does.

## Messages

`NotificationMessage` is `{ subject, body, html, blocks }` — the two parts every channel has some form of, plus the same message rendered again for the transports that can show more than text: HTML for a mail client, Slack Block Kit for Slack. Each channel renders what it can use and ignores the rest — email reads `html` and never looks at `blocks`, Slack reads `blocks` and never looks at `html`. `job_run_message` is the only kind there is today, which is why it is a function beside the service rather than a trait.

**One message shape for both endings, not one per ending.** A success and a failure answer the same question — what did this run do, and what did each of its tasks do — and the run's status supplies the wording throughout, so a succeeded run reads as one rather than as a failure notice with the word swapped. The only difference is the quoted output, and that falls out on its own: the failures a success has none of are an empty list, so the section that quotes them is simply not written.

**The cap on quoted output belongs to the channel, not the message.** `NotificationChannels::max_output_bytes` is asked per channel and passed into the builder, because the reason for a cap is the transport's own limit — a relay's maximum message size for email, and for Slack how much of a chat message anybody scrolls through, which is why its default is the smaller of the two. Truncation has to happen while the body is built, since the output is embedded in formatted text no channel could safely cut afterwards.

**Slack's rendering is blocks, and its text rules live with its transport.** `message_blocks` builds a header carrying the status as an emoji, the summary as two columns of fields, the task list as one section per 3000 characters, and the failing task's output fenced — so the post is read at a glance rather than as one wall of monospaced text. What is Slack's own stays in [slack.rs](../../../src/notifications/slack.rs): `escape` for the three characters Slack reads as its own before anything else (`&`, `<`, `>` — a task printing `a < b` arrives eaten without it), and `fence_safe` for a fence a command printed itself.

**Slack refuses a message that exceeds its limits rather than trimming it**, so the trimming happens in the builder: 150 characters on a header, 3000 on a section, 2000 on a field, 50 blocks on a message. Escaping is what makes the character cap bite before the byte cap does — every `&` becomes five characters — so a stream is cut *inside* its fence, never after it, or the closing fence goes and the rest of the message is swallowed by the code block. A message that overran is cut from the end and the command that shows the rest is written again as its last block.

**`html` and `blocks` are `Option`, and each channel degrades to the text part rather than failing.** A message kind need not have an HTML rendering, and a template that will not render costs the formatting rather than the notification — so `message_html` logs and returns `None`. `EmailChannel::build` then sends `multipart/alternative` when there is one, both parts on the same message, and the body alone when there is not: a client that renders HTML shows the formatted message, and a reader who prefers plain text loses nothing. `post_payload` does the same for Slack — blocks with the subject in `text`, which is what a notification preview and a screen reader read, and the whole body in one fence when a message kind has no blocks.

**All three renderings are built in [message.rs](../../../src/notifications/message.rs), from one set of facts.** `summary_rows`, `message_lead`, `failure_title`, `logs_command` and `stream_tail` are each called by the text builder, the template builder and the block builder, so a fact added to one rendering cannot go missing from another — which is why the HTML and the blocks live beside the text rather than in modules of their own. `job_run_accent` and `job_run_emoji` are matched exhaustively side by side for the same reason: a new run status cannot compile without saying how it reads in every rendering. The template itself is [templates/notifications/job_run.html](../../../templates/notifications/job_run.html): **one template for both endings**, for the same reason there is one message shape, and table layout with inline styles throughout because a mail client fetches no stylesheet. Askama escapes every value it writes, which is what makes it safe to quote a command's own output in it.

`stream_tail` keeps the **end** of a stream and cuts the front, on a character boundary: the end is where a command says why it stopped, and that is what makes a capped alert still worth reading.

## What it does not do

**No retries.** Every path that reaches a channel writes the row, which is what stops a channel that is down from being hammered every second. There is deliberately no delay or attempt count — an alert nobody can see failed is worse than one that failed loudly. If retries are wanted, this table is the right shape for them: add `attempts` and `next_attempt_at`, and select on the latter.

**No opinion about what a run *should* do.** It reads the status and decides what to do with a notification; it never writes a run status, and nothing it does can change how a run ends.

**No opinion about which endings are worth hearing about.** That is the job's, declared in its YAML and frozen onto the row at submit. The service only asks whether this row's ending is the one that happened.
