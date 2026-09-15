<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/brand/logo-dark.png">
  <img src="assets/brand/logo-light.png" alt="flowlite" width="260">
</picture>

flowlite is a job scheduler and orchestrator that ships as a single Rust binary. Point
it at a directory of YAML and it runs your jobs.

## Why flowlite

Simplicity. It is a scheduler you can hold in your head.

- **Nothing to install.** No database, no broker, no workers, no agent — one binary and a
  SQLite file beside your YAML.
- **Jobs are files.** A pipeline is plain YAML in git, reviewed and reverted like any other
  change.
- **Runs remember themselves.** Every run keeps the config it executed, so a rerun replays
  that run rather than today's file.
- **Nothing is left behind.** A timeout or a stop kills the whole process tree, and every
  attempt keeps its output.

One project per server, bound to localhost: no auth to configure, no workers to scale, no
Python.

## Install

```bash
cargo install --git https://github.com/writes-the-spec/flowlite
```

Or clone and `cargo build --release`, which leaves the binary at `target/release/flowlite`.

## Quick start

Lay out a data directory with an example job and an example schedule in it:

```bash
flowlite init
```

That writes `jobs/hello.yaml` (`.yml` works too), `schedules/daily-hello.yaml` and a
`config.toml` that is entirely comments — a map of what can be set and what each key
defaults to, so the directory behaves exactly as one with no `config.toml` at all. `init`
never overwrites: a file already there is kept and reported as kept. The job it leaves
behind:

```yaml
id: hello-world
name: Hello World
tasks:
  - id: say-hello
    description: Says hello
    command: echo "hello from flowlite"
```

Start the server — the scheduler, the orchestrator and the UI — then submit the job:

```bash
flowlite serve
flowlite job submit hello-world
flowlite job list
```

flowlite reads `jobs/`, `schedules/` and an optional `config.toml` from its data directory,
and writes `flowlite.db` there. That directory is the current one unless `-D` /
`--data-dir` / `FLOWLITE_DATA_DIR` says otherwise — which `init` follows like every other
command, so the directory need not exist yet:

```bash
flowlite --data-dir /var/lib/flowlite init
flowlite --data-dir /var/lib/flowlite serve
```

## Jobs

A job is an id, a name and a list of tasks. Tasks run in parallel unless `depends_on` puts
them in order:

```yaml
id: build-and-test
name: Build and Test Pipeline
tasks:
  - id: build
    description: Compiles the binary
    command: make build

  - id: test
    description: Runs the test suite against the build
    depends_on: [build]
    command: make test

  - id: deploy
    description: Ships the tested build
    depends_on: [test]
    command: make deploy
```

A task runs only if everything it depends on succeeded; everything downstream of a task
that did not succeed is skipped.

A job's remaining keys have sections of their own: `parameters`, `env`, `secret_env` and
`working_dir` under [Command inputs](#command-inputs), `timeout`, `max_retries` and
`retry_delay` under [Timeouts and retries](#timeouts-and-retries), `on_failure` and
`on_success` under [Run notifications](#run-notifications), `max_parallel_runs` under
[Overlapping runs](#overlapping-runs), `limits` under
[Concurrency limits](#concurrency-limits) and `keep_runs` under [Retention](#retention).

## Schedules

Schedules submit job runs on a cron expression. Define them in `schedules/nightly.yaml`:

```yaml
id: nightly
name: Nightly pipeline
# Six fields, seconds first: sec min hour day-of-month month day-of-week.
# A pasted five-field crontab line is rejected. This is 03:30:00 every day.
cron: "0 30 3 * * *"
# The zone the cron fields are read in, so the run does not drift with DST.
timezone: Europe/Paris
start_date: 2026-01-01    # nothing fires before this date; omit for "from now on"
end_date: 2026-12-31      # nothing fires after it; omit for "forever"
disabled: false           # true keeps the file and stops the firing
submit_ahead: 1           # how many occurrences to write in advance; at least 1
jobs:
  - id: daily-etl
    # Overrides what the job declares, for every run this schedule submits.
    parameters:
      region: us
```

One schedule may fire several jobs; each entry names a job id and may override that job's
parameters, and a job id may appear only once. `timezone` left out falls back to
`[schedule_defaults]`.

`submit_ahead` writes upcoming occurrences as runs before they are due, so the next run of
every schedule is visible in `job-run list` and on the dashboard ahead of time. A larger
number shows more of the future, at the cost of that many standing rows per schedule. Runs
written ahead sit in the `submitted` status and start at their own instant. The reconcile
only ever adds: taking a schedule's YAML away, disabling it, lowering `submit_ahead` or
editing its cron leaves the runs already written in place, and they will be released and
executed at their instant like any other. `flowlite job-run stop <id>` calls one off, and
`flowlite job-run delete <id>` removes it and lets the schedule write the occurrence again.

### When an edit takes effect

A run **snapshots the job as it stands when the run is written**, not as it stands when it
starts. With `submit_ahead: 1` and a 03:00 cron, tonight's run was written just after 03:00
yesterday, so editing that job's YAML this morning does not change it. The YAML is also read
once, at startup, with no watcher, so every run a server submits carries the file as it
stood when *that server* started.

An edit therefore takes effect only for occurrences submitted after the restart. A run that
was already written keeps the definition it was submitted with, and nothing replaces or
deletes it, so an occurrence written under the old YAML runs under the old YAML.

### Replacing an outstanding run with the edited definition

`job-run rerun` replays the run's own snapshot, so it is no help here: it is the old
definition again. Delete the run instead, and let the schedule write the occurrence back:

```bash
# Edit the job's YAML first, and restart flowlite serve so it re-reads the file.
flowlite job-run list --job nightly --status submitted  # find the outstanding run
flowlite job-run delete 42
```

The next scheduler pass finds no run standing for that occurrence and submits it again,
carrying the job as the restarted server now reads it. Only a run still `submitted` can be
deleted — one already queued or running is the dispatcher's, and `job-run stop` is what
calls that off.

**Deleting is not stopping, and the difference is which one the schedule writes again.** A
stopped run settles `skipped`, and that row goes on holding its instant for ever, so the
occurrence is never submitted a second time — cancelling a single occurrence has to stick.
A deleted run settles `deleted`, the one status the scheduler's existence check ignores, so
the occurrence is free and the next pass fills it. Reach for `stop` to call a run off, and
for `delete` to have it written again.

Nothing is erased. The run keeps its row, its task runs and everything it was submitted
with, so `flowlite job-run get 42` still reads afterwards and `--status deleted` lists what
you removed. Retention reaps a deleted run like any other settled one.

## Command inputs

A command is configured four ways — parameters declared on the job, environment variables
set on the job or a task, environment variables resolved from a named secret, and a working
directory — plus a handful of variables flowlite injects. All of them arrive as environment
variables, since `sh -c <command>` inherits its environment like any process:

```yaml
id: daily-etl
name: Daily ETL
parameters:
  region: us-east-1
env:
  PYTHONUNBUFFERED: "1"
  TZ: UTC
tasks:
  - id: extract
    description: Pulls yesterday's rows out of the source database
    command: ./extract.sh
    env:
      # Overrides the job's TZ; PYTHONUNBUFFERED still comes from the job.
      TZ: Europe/Paris
    working_dir: /srv/etl
```

A job's `env:` applies to every one of its tasks, and a task's own wins any name both set —
a task cannot opt out of the job's block, only override a name in it. `working_dir` left
empty, the default, inherits flowlite's own working directory.

A declared parameter reaches the command **prefixed and upper-cased**: `region` becomes
`FLOWLITE_PARAM_REGION`. The prefix is what stops a parameter named `path` or `home` from
shadowing something the command actually needed.

Override a parameter at submit time:

```bash
flowlite job submit daily-etl --param region=eu-west-1
```

`--param name=value` can be repeated, splitting on the first `=` so a value may contain
one, and a later repeat of the same name wins. Naming a parameter the job doesn't declare
is refused, naming the job, the bad key and the declared names.

### Precedence

Where a name collides, later wins, applied in this order:

1. The environment flowlite itself inherited, less every `FLOWLITE_*` variable in it.
2. The job's `env:`.
3. The task's `env:`.
4. Resolved secrets — the job's and the task's `secret_env:` — see [Secrets](#secrets).
5. `FLOWLITE_PARAM_*`.
6. The variables below, injected by flowlite.

Steps 2 and 3 are merged once, at submit time, onto `task_run.env` (`secret_env:` the same
way, onto `task_run.secret_env`); step 4 resolves those references into values, and steps 5
and 6 are composed at spawn. So a plain `env:` value can never shadow a credential, and
metadata is last so nothing a user writes can make a command lie about which run it belongs
to.

**A command does not inherit flowlite's own configuration.** Every `FLOWLITE_*` variable is
stripped from the child before the layers above are applied — a server started with
`FLOWLITE_SMTP__PASSWORD=...` does not hand that credential to every command it spawns.
What a command is meant to have, flowlite injects by name.

### Injected variables

| Variable | Value |
|---|---|
| `FLOWLITE_JOB_ID` | The job this run is of. |
| `FLOWLITE_JOB_RUN_ID` | This run's id. |
| `FLOWLITE_TASK_ID` | This task. |
| `FLOWLITE_TASK_RUN_ID` | This task's run. |
| `FLOWLITE_TASK_RUN_ATTEMPT_ID` | This attempt. |
| `FLOWLITE_ATTEMPT` | Which attempt this is, starting at 1. |
| `FLOWLITE_DATA_DIR` | The data directory this server is serving — injected, not inherited, so a command that calls `flowlite` itself works on the same directory. |
| `FLOWLITE_SCHEDULED_AT` | The instant a schedule fired for, as RFC3339. Set only for a scheduled run — a manual `job submit` gets no such variable at all, not an empty one. |

**Parameters answer "which caller is this run for," not "which run is this."** A parameter
is a fixed value carried unchanged from submit through every rerun, never re-evaluated, so
a schedule declaring `slice: "2026-09-08"` is wrong on every occurrence after that day.
`FLOWLITE_SCHEDULED_AT` is what tells a recurring job which occurrence it is running; reach
for it whenever the question is "which day, hour or slice is this."

### Secrets

`secret_env:` maps an environment variable to the *name* of a secret, not its value.
It is declared on a job or a task exactly like `env:`, and merged the same way:

```yaml
id: nightly-sync
name: Nightly sync
env:
  PGHOST: warehouse.internal
tasks:
  - id: load
    command: psql "postgres://etl@$PGHOST/prod" -f load.sql
    secret_env:
      PGPASSWORD: warehouse_pw
```

The command never names the password, because `psql` already reads `PGPASSWORD` out of its
own environment. That is where composition belongs — which is why there is no `${...}`
interpolation inside `env:` or `secret_env:`: a block states a fixed name, not a second
templating language to learn.

`warehouse_pw` is resolved against `[secrets]` in `config.toml` or
`FLOWLITE_SECRETS__WAREHOUSE_PW` in the server's own environment — see
[Configuration](#configuration). The name is what travels: it is what a run stores, what
the dashboard and the task pages show (`PGPASSWORD ← warehouse_pw`), and what `--json`
returns. The value is looked up once, when the command is spawned, and exists nowhere but
that one process's environment.

That makes `env:` the right place for a value you are willing to commit and see on a page —
a hostname, a flag, a timezone — since merged `env:` values are shown as written on the run
and task pages. For a credential, reach for `secret_env:`.

Five things are refused when a job's YAML is read, before it is ever served:

- a variable name that is not a valid environment variable name;
- a secret name outside `[a-z0-9_]+`, or containing `__`. `config.toml` can quote a name
  like `"Warehouse-PW"`, but `FLOWLITE_SECRETS__*` cannot reach it, and `__` is the
  separator that form uses for nested keys — `FLOWLITE_SECRETS__WAREHOUSE__PW` sets
  `secrets.warehouse.pw`, never the name `warehouse__pw`. Either way the name would work on
  a development box and be unreachable in production;
- a variable name starting with `FLOWLITE_` — run metadata is applied last under that
  prefix and would silently win, leaving the task's credential quietly missing;
- the same variable name in both `env:` and `secret_env:` **at the same level**. Across
  levels it is intentional layering — a job declaring a default that a task replaces with a
  secret — but at one level it is a contradiction the author should see.

`serve` also refuses to start if a job names a secret that nothing defines — a missing
*value*, not just a malformed name — so a typo is caught before 03:00 rather than at it:

```
Job 'nightly-sync'

Caused by:
    task 'load' needs secret 'warehouse_pw' for PGPASSWORD, but nothing defines it. Add it
    under [secrets] in config.toml, or set FLOWLITE_SECRETS__WAREHOUSE_PW.
```

That check runs only in `serve`. `job-run list` and the other read commands work with no
secrets in the environment at all, because reading a run's status must never require the
credentials that run used.

## Timeouts and retries

`timeout` is the seconds one attempt may run for before its process group is killed.
`max_retries` is how many times a failed attempt is tried *again*, so `max_retries: 2` is
three attempts in all, and `retry_delay` is the seconds to wait after a failure before the
next attempt starts:

```yaml
tasks:
  - id: fetch
    command: curl -fsS https://example.com/data.json -o data.json
    timeout: 120
    max_retries: 2
    retry_delay: 30
```

Each defaults to `[job_defaults]`: an hour, no retries, and 60 seconds between them — on
the grounds that whatever a retry waits on rarely fixes itself within one second. Set
`retry_delay: 0` to retry as soon as possible. Every attempt keeps its own output — see
[Task output](#task-output).

## Run notifications

A job can name who to tell when one of its runs ends — when it does not succeed, when it
does, or both — by email, in Slack, or both:

```yaml
id: nightly-sync
name: Nightly Sync
on_failure:
  email: [oncall@example.com, data-team@example.com]
  slack: ["#oncall"]
on_success:
  slack: ["#data"]
tasks:
  - id: sync
    command: ./sync.sh
```

Each channel is delivered and recorded **separately**, so a Slack workspace that is down
does not swallow the mail, and each says on its own row whether it landed.

The message carries the run's status and timings, every task and how it ended, and — for a
run that broke — the **output of the tasks that broke**, so the alert usually says what went
wrong without opening the dashboard:

```
Job run 42 of 'Nightly Sync' (nightly-sync) failed.

  Job         nightly-sync
  Run         42
  Status      failed
  Started     2026-09-09 02:00:01
  Finished    2026-09-09 02:04:37
  Duration    4m 36s

Tasks

  extract                  succeeded
  transform                failed
  load                     skipped

Output of transform, attempt 3 of 3

stdout:
reading rows
stderr:
psycopg2.OperationalError: connection refused

Run `flowlite job-run logs 42` for every task and attempt.
```

A success is the same message without the quoted output. A scheduled run also carries the
instant it fired for, and a run with parameters the values it was submitted with.

Mail sends that text with an HTML rendering of the same facts alongside it. Slack gets the
same facts as Block Kit, with the output capped shorter than in mail by default — a chat
message is read in a scroll, and the mail is where the long tail belongs.

**`on_failure:` means a real failure** — `failed`, `timed out` and `invalid`, never
`aborted` or `deleted`. A run you stopped or removed yourself is not news, and neither block
is told about one. An
`invalid` run is the opposite case: nobody chose it, so it is the ending most worth being
told about — see [When flowlite loses track of a run](#when-flowlite-loses-track-of-a-run).

A run records who it will tell **when it is submitted**, alongside the commands and
parameters it snapshots. So editing either block does not change a run already in flight, a
rerun tells whoever the original run would have told, and a run whose job YAML has since
been deleted still reaches somebody. A run that named both blocks carries a record for
each, and its one ending settles them in opposite directions: the block it matched is
delivered, the other closed as nothing to report.

Each send is tried **once**. A send that fails is recorded against the run with the error
and reported in the server log, rather than being retried against a relay that may be down
for hours. Slack is recorded on what it *said*, not on the status code — it refuses an
unknown conversation with `ok: false` inside a 200, and one conversation refusing does not
stop the others from getting the alert. Delivery runs as its own background service, so an
unreachable mail server never holds up the runs themselves.

### Where the mail server and the Slack token go

Who to tell is a property of the job, so it lives in the job's YAML. *Where mail goes out
through* is a property of the machine, so it lives in `[smtp]` and `[slack]` in
`config.toml` — see [Configuration](#configuration), which also covers why the password and
the token belong in flowlite's environment rather than in that file.

Slack wants a bot token with `chat:write` rather than an incoming webhook on purpose: a
webhook URL *is* its destination, so a job naming a second conversation would carry a second
secret URL in its YAML. With a token a job names `#oncall` and nothing else; invite the bot
to each conversation you want it to post in.

**A job that names a recipient of a channel `config.toml` does not configure refuses to
start**, naming the job, the block and the file:

```
Invalid notifications of job 'nightly-sync' at /srv/flowlite/jobs/nightly.yaml

Caused by:
    on_failure.slack names #oncall but config.toml has no [slack] section, so nothing
    can be sent by slack. Add one, or remove the recipients.
```

The blocks are checked separately, since a job may ask for mail on a failure and Slack on a
success and only one be deliverable here. A notification that silently never leaves is the
one failure you cannot see from the run afterwards, so it is caught at startup.

## Overlapping runs

A job runs one at a time by default. A run created while another is still going is not
rejected — it waits as a queued job run and starts as soon as the earlier one finishes,
oldest waiting run first. Raise or lift the limit per job:

```yaml
id: nightly-sync
name: Nightly Sync
max_parallel_runs: 2   # 0 for no limit
tasks:
  - id: sync
    command: ./sync.sh
```

The limit is enforced in one place, when a queued run is picked up to start, so `job
submit`, a rerun and the scheduler are all held to it alike. A job that takes longer than
its schedule interval will therefore queue up runs and work through them back to back.

## Concurrency limits

`max_parallel_runs` bounds runs of *one job*. These two knobs bound something orthogonal:
how many task run *attempts* are running at once, regardless of which job or run they
belong to.

```toml
[orchestrator]
max_running_attempts = 32   # 0 for no limit

[concurrency_limits]
warehouse = 3               # 0 for no limit
```

`max_running_attempts` is the one ceiling with no name — every attempt anywhere counts
against it. `[concurrency_limits]` adds ceilings with a name, for a resource narrower than
"the whole server". A task opts into one with `limits:`, at job level, task level, or both;
a job's limits are claimed by *every* one of its tasks, and a task's own are added to them,
not substituted for them:

```yaml
id: nightly-sync
name: Nightly Sync
limits: [warehouse]       # every task below claims warehouse too
tasks:
  - id: load
    command: ./load.sh
    limits: [openai_api]  # this task claims warehouse AND openai_api
```

A limit name that is not a key of `[concurrency_limits]` is a **startup error**, so a typo
cannot silently become a task with no limit at all. This is the same split `secret_env:`
draws between a name and a value: **the number is config, the name is YAML.** A job file
names the resource it competes for; how much of it exists is answered per deployment.

`flowlite limits` prints the combined cap across every job under the reserved name
`global`, which is why `global` is rejected as a key of `[concurrency_limits]`:

```bash
$ flowlite limits
NAME         IN USE  MAX
global           32   32  FULL
openai_api        0    -
warehouse         3    3  FULL
```

A row whose in-use count has reached a non-zero max is marked `FULL`, which is the answer to
"why is nothing running". A limit configured `0` means no ceiling at all, not zero slots,
and renders as `-` rather than a number it could be confused with — the dashboard's own
panel spells it out as `unlimited`. A `0` limit is therefore never `FULL`. `flowlite limits
--json` keeps it as the number `0`, so a script comparing it against `in_use` never has to
special-case a dash.

It reads `config.toml` for the maxima and the on-disk database for the counts directly, so
it answers for a data directory whose server is down as readily as one whose server is up —
the same guarantee `status` makes by reading the lock file instead of asking the process.

## Runs from the command line

`job submit` returns as soon as the run is written, which is what an unattended scheduler
should do. A script usually wants the ending instead, so `--wait` blocks until the run
settles and exits non-zero unless it succeeded:

```bash
flowlite job submit nightly --wait
echo $?     # 0 only when the run succeeded
```

Every ending that is not a success exits 1 — failed, timed out, aborted, skipped or invalid
— so a Makefile, a CI step or a parent job can treat a flowlite run like any other command.
The wait polls the run's row, so it works from a different process, shell or container to
the one running `flowlite serve`, and is refused outright when nothing is serving the data
directory, since the row it would poll has no writer. Only the wait is refused: submitting
into a directory whose server is down still queues the run.

`--schedule-at` dates a run rather than submitting it for now. It takes an RFC3339 instant
with an explicit offset, and a bare `2026-09-15 09:00` is refused rather than guessed at:

```bash
flowlite job submit nightly --schedule-at 2026-09-15T09:00:00+02:00
```

The run is written straight away and sits in the `submitted` status until its instant
arrives, which is also what a schedule's own runs do. An instant in the past is due
immediately, so it may be combined with `--wait`; one in the future may not, since the wait
could only burn its whole timeout and then report a perfectly healthy run.

The run history is readable without opening the dashboard:

```bash
flowlite job-run list                                  # the 20 newest runs
flowlite job-run list --job nightly --status failed
flowlite job-run get 42                                # one run and its task runs
flowlite job-run stop 42                               # ask a running run to stop
flowlite job-run stop 42 --wait                        # ...and block until it has settled
flowlite job-run delete 42                             # remove a run still waiting its turn
```

`stop` writes a request rather than killing anything itself — the `serve` process notices
it on a later pass and kills the task's process group. A run that has already finished is
refused rather than silently accepted. Without `--wait` the command returns once that
request is written, while the run is still going; `--wait` blocks until the run has
settled, which is what a caller that means to start something else next wants:

```bash
flowlite job-run stop 42 --wait && flowlite job-run rerun 42
```

Unlike `job submit --wait` it exits 0 whichever status the run settled to: the stop did
what it was asked either way. It makes the same unserved-directory check, and refuses the
same way.

### A run from a file

A job normally has to live under `jobs/` in the data directory before it can be run. `-f`
submits a definition that does not:

```bash
flowlite job submit -f ./pipeline.yaml
flowlite job submit -f ./pipeline.yaml --param region=eu-west-1 --wait
```

The file is read where it lies and never copied anywhere: it does not appear in
`flowlite job list`, on the dashboard, or in what the next `serve` picks up. What it
produces is an ordinary run — it has an id, it shows in `job-run list`, `--wait` and
`--json` mean the same thing, and the dashboard renders it like any other.

This is for the one-off: a backfill, a migration, a pipeline an agent generated to run once.
Anything you want to keep, or to schedule, is a file under `jobs/` — a schedule names an
installed job by id, and a definition that was never installed has no id to name.

The file is held to every rule an installed job is held to: a cycle, a duplicate task id, a
dependency on a task that isn't there, a notification channel this box cannot send on, an
undefined `limits:` name, or a `secret_env:` naming a secret nothing configures is refused
before any run is written, naming the file. An id that is already a job in the data
directory is refused too, since that id is what every filter and link resolves through:

```
$ flowlite job submit -f etl.yaml
error: 'etl' is already a job in /srv/etl/jobs. Drop -f to submit it: flowlite job submit etl
```

Because the run snapshots its own definition, it stays readable and rerunnable after the
file is gone:

```bash
flowlite job submit -f ./once.yaml --json | jq -r .id    # 42
rm ./once.yaml
flowlite job-run rerun 42                                # still replays what run 42 ran
```

`max_parallel_runs` does not apply to such a run — there is no other run of a definition
that exists for one command. The global `max_running_attempts` cap and any named `limits:`
it claims still do.

### JSON output

Every `job` and `job-run` command takes `--json`, which prints the rows themselves instead
of a table:

```bash
flowlite job-run get 42 --json | jq .status
flowlite job submit nightly --wait --json | jq -r '.id, .status'
flowlite job-run logs 42 --json | jq -r '.[] | select(.status == "failed") | .stderr'
```

The payload is the data with no envelope around it: a list command prints an array, a
single-run command an object, and `job submit --json` prints the run either way, so `.id`
and `.status` read the same with and without `--wait`. Errors are never part of it — they
go to stderr as text and the exit code carries the failure, which keeps
`flowlite job-run list --json > runs.json` a file of runs or nothing at all.

## Task output

Every task's stdout and stderr are captured as it runs and kept per attempt, so a task that
was retried keeps the output of each try. Past `max_stream_bytes` the command keeps running
and the recording stops, with a `[flowlite: output truncated, exceeded N bytes]` marker
where the rest would have been.

```bash
flowlite job-run logs 42                # every task of run 42
flowlite job-run logs 42 --task build   # just one task
```

Or click a task on the run timeline in the dashboard to open its output page, which
refreshes itself while the task is still running.

## Reruns

A run can be run again, with the **Rerun** button on its page or from the command line:

```bash
flowlite job-run rerun 42
```

A rerun replays **the definition the original run executed, not the current YAML.** Every
run snapshots its own commands, `depends_on` edges, timeouts and retry settings, resolved
`parameters`, the `env`, `secret_env` and `working_dir` of every task, and the
`FLOWLITE_SCHEDULED_AT` it fired for — so a run whose job YAML has since been edited or
deleted is still rerunnable. To run the job as it is defined now, `flowlite job submit <id>`.

`secret_env:` is the one exception: what is frozen is the secret's *name*, not its value. A
rerun resolves that name against whatever `[secrets]` or `FLOWLITE_SECRETS__*` currently
holds, so rotating a credential changes what the next rerun uses — the opposite of `env:`,
whose literal values really are frozen forever, and deliberately so: replaying a leaked
password would be the worst thing a rerun could do.

**A run is rerunnable for as long as it is retained, and no longer.** Retention (below)
deletes the config snapshot along with the run, so `job-run rerun` on a run retention has
reaped fails the same way it does for an id that never existed. (A run in the `deleted`
status is a different thing entirely — its rows are all still there, and it reruns fine.)

## Retention

A long-lived data directory accumulates job runs forever unless something prunes them.
`RetentionService` does, on the same poller every other background service runs on, inside
`flowlite serve`. A run becomes a candidate for deletion once it is **finished**, and never
before — never `Queued` or `Running`, and never one that still owes an undelivered
notification.

Each job keeps its own newest runs, from `[job_defaults] keep_runs` or its own YAML:

```yaml
id: nightly-sync
name: Nightly Sync
keep_runs: 20
tasks:
  - id: sync
    command: ./sync.sh
```

`[retention]` adds a ceiling across every job, and a limit on how much of one pass may go to
deleting:

```toml
[retention]
keep_runs_total = 10000        # the most finished runs kept across every job, 0 for no ceiling
max_deletes_per_pass = 100     # the most runs one pass deletes, 0 for no cap
```

`keep_runs_total` is enforced oldest-first across every job, *after* each job's own
`keep_runs` — the two rules stack rather than compete. `max_deletes_per_pass` exists so the
first pass against an already-large backlog cannot hold the single SQLite writer for
minutes.

**Deleting rows frees SQLite's pages for reuse but does not shrink `flowlite.db`** — the
database stops growing rather than gets smaller. Get the space back with the server
stopped:

```bash
sqlite3 <data-dir>/flowlite.db 'VACUUM;'
```

flowlite never runs this itself: `VACUUM` rewrites the whole file under an exclusive lock,
which is the last thing a sidecar process should do to itself unasked.

## When flowlite loses track of a run

Most statuses say what happened to your command: it succeeded, it failed, it ran past its
timeout, you stopped it. `invalid` says something different — that flowlite cannot account
for the row at all.

The case that actually happens is a restart with work in flight. Shutting `serve` down
kills the process groups it spawned and leaves those attempts marked running on purpose; a
crash or a `kill -9` leaves them with the processes still alive. Either way the next start
has no exit status to read and no process to wait on, so no honest outcome can be claimed:

```bash
flowlite job-run list --status invalid
```

An invalid attempt makes its task run invalid, that makes the job run invalid, and
everything downstream is skipped. It is **never retried** — flowlite does not know what
that attempt did, so running it again would be guessing that it left nothing behind. Rerun
it yourself once you have checked.

**The command it left behind is killed on the next start.** Each attempt records the
process group flowlite spawned for it, so a restart finds a command a crash left running
and kills the whole tree, naming the group in the log. What that command had already done
stays unknown, which is why the run is `invalid` rather than `aborted`. One case is refused
rather than guessed at: an attempt that started before the machine last booted cannot still
own its recorded group id — the number has been recycled — so nothing is signalled and the
log says the command may still be running.

## One server per data directory

A data directory is served by exactly one `flowlite serve`. A second one there is refused,
because two servers would run two schedulers over one set of schedules and fire every cron
twice. The refusal is an `flock`, so it needs a filesystem where `flock` actually works —
local disk always qualifies, which is what SQLite already assumes of the data directory.

A running server keeps a `.flowlite/` directory beside its database, holding its lock and
the pid, address and port it bound. Ask about it with:

```bash
flowlite -D /srv/etl status
# serving on http://127.0.0.1:8001 (pid 41207, up 4m 12s, flowlite 0.1.0)

flowlite -D /srv/etl status --json
# {"address":"127.0.0.1","pid":41207,"port":8001,"started_at":"2026-09-10T...","status":"up","uptime_seconds":...,"version":"0.1.0"}
```

`status` reads those files rather than the database, so it answers for a server that is down
as readily as one that is up. There is a third state, `starting`, for the brief window
between the lock being taken and the listener binding — expect it during restarts, not just
on a first start. `status` exits 0 in all three states, so a wrapper should read the
`status` field rather than the exit code.

Add `.flowlite/` to `.gitignore` if your data directory is a repository, but do not delete
it while a server is running: because the lock lives on the inode rather than the path,
removing `serve.lock` out from under a running server (`git clean -xdf`, say) lets the next
`serve` create a fresh inode and start right alongside it.

### Running several services

A second project is a second data directory, with its own database, its own port and its
own `serve`. flowlite does not manage the set of them: `-D` names the one you mean, and
whatever already supervises processes on your machine starts them. A systemd template unit
is usually all it takes:

```ini
# /etc/systemd/system/flowlite@.service
[Service]
EnvironmentFile=/etc/flowlite/%i.env
ExecStart=/usr/local/bin/flowlite -D /srv/%i serve --address ${ADDRESS} --port ${PORT}
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Then `systemctl start flowlite@etl`, and you get restart-on-failure and start-on-boot with
it. In development, a `Justfile` or a `Procfile` does the same job.

## UI

The dashboard is served out of the binary. `flowlite serve` binds `127.0.0.1:8000` unless
`--address` and `--port` say otherwise; visit it for the job list, the run history, a
job's dependency graph and the output of any task.

It has a dark palette and a light one, and by default follows whichever the viewer's own
OS asks for — so a shared instance leaves the choice to whoever opens it. `[ui] theme` takes
`dark` or `light` to overrule that. One stylesheet, both themes, no build step.

It offers three writes, each the browser equivalent of a command and each asking for
confirmation first: **Stop** and **Rerun** on a run's page, and **Submit run** on a job's
page. **Submit is the one that is not idempotent** — stop writes a stop row and rerun
replays a fixed snapshot, but submitting twice is two runs. What that costs is bounded
rather than free: the extra run queues, since `max_parallel_runs` and
`max_running_attempts` decide what actually executes.

Because a button starts work, the three POST routes refuse a request another site caused
your browser to make — otherwise any page you happened to be visiting could submit a run on
your machine. A request with no browser headers at all, such as `curl` or a script, is still
allowed: this guards a browser against being used as a deputy, not the port against someone
who can already reach it.

The submit dialog lists the job's declared parameters with their defaults filled in, and each
value is editable for that run. The names are not: they come from the job's `parameters:`
block, so the browser can change what a run is submitted with but not what the job accepts.

## Driving flowlite from an agent

`flowlite mcp` speaks the Model Context Protocol on stdin and stdout, so an agent drives
flowlite through the same reads and writes the CLI makes:

```bash
flowlite -D ./data init
claude mcp add flowlite -- flowlite -D ./data mcp
```

No port, no HTTP and nothing to start first: the client spawns the binary, talks JSON-RPC
over its stdio, and the process exits when the client closes stdin. The `init` line is
optional, since an agent handed an empty directory can call `init_data_dir` itself.

Nine tools, each a projection of a command that already exists:

| Tool | Answers |
|---|---|
| `init_data_dir` | Set this empty directory up with an example job and schedule. |
| `list_jobs` | What jobs does this data directory declare? |
| `submit_job` | Run this — and, with `wait_seconds`, how did it end? |
| `list_job_runs` | What has run lately, by job and by status? |
| `get_job_run` | What happened to run 42, task by task? |
| `get_job_run_logs` | What did each attempt write to stdout and stderr? |
| `stop_job_run` | Stop run 42, and tell me what it settled to. |
| `get_serve_status` | Is anything actually serving this directory? |
| `list_limits` | What is a run waiting behind? |

Each returns the JSON its `--json` twin prints, so an agent and a shell script reading one
run read the same fields. Three differ, each for a stated reason.

`get_job_run_logs` differs only in length: it keeps the last `max_bytes` of each stream,
20000 by default and 200000 at most, and says on a marker line how many bytes it dropped —
a terminal has a scrollback and a `| tail`, a context window has neither. Asking for more
than the cap gets the cap rather than an error. `list_job_runs` bounds its page the same
way, 20 runs by default and 200 at most.

`stop_job_run` returns the whole run, waited or not, where `job-run stop --json` prints
`{"job_run_id": 42, "stop_requested": true}` unless you passed `--wait`. One shape either
way means an agent reads `.status` off the result instead of branching on which argument it
sent.

`get_serve_status` and `list_limits` are the pair an agent reaches for when a run does not
progress: the first says whether anything is serving the directory at all, the second what
a `queued` run is waiting behind.

`init_data_dir` takes no arguments: the directory is the one `-D` named, so an agent cannot
point it somewhere nobody asked for. Nothing is overwritten, and the result says which files
it wrote and which were already there:

```json
{
  "data_dir": "./data",
  "files": [
    { "path": "jobs/hello.yaml", "created": true },
    { "path": "schedules/daily-hello.yaml", "created": true },
    { "path": "config.toml", "created": false }
  ]
}
```

The job it writes is submittable on the next call — every tool seeds its own view of the
data directory, so nothing has to be restarted first.

`submit_job` names what to run exactly one of three ways:

```jsonc
{ "job": "etl" }                                 // a job installed under jobs/
{ "file": "pipelines/probe.yaml" }               // a path, read where it lies
{ "yaml": "id: probe\ntasks:\n  - id: ..." }     // the definition itself, inline
```

The last two are [a run from a file](#a-run-from-a-file) reached two ways. `params` is a
JSON object rather than repeated `name=value` strings, and a name the job does not declare
is refused exactly as `--param` refuses it.

`submit_job`, `get_job_run` and `stop_job_run` each take `wait_seconds`, which is how an
agent gets an outcome in one call instead of a polling loop. It returns as soon as the run
settles; if the time runs out first the run comes back merely unfinished rather than as an
error, since its id is what lets the agent ask again. A value above 300 clamps to 300.

**A submit into a directory nothing is serving writes a run that will not start.** It is
allowed, as on the command line, and the tool result says so beside the JSON: the run stays
`submitted` however far past its due time, and is picked up once `flowlite serve` runs
against that directory. A `wait_seconds` above 0 is refused there outright, since nothing
would ever settle the row it would poll.

## Configuration

Everything below has a default, so flowlite runs with no `config.toml` at all. Write one in
the data directory to change any of it; a file naming a single key leaves every other
default alone, and each key can also be set as an environment variable
(`FLOWLITE_UI__PAGE_SIZE=10`, `FLOWLITE_ORCHESTRATOR__POLL_INTERVAL_SECONDS=5`).

```toml
[orchestrator]
poll_interval_seconds = 1       # how often a service looks for work itself
error_backoff_seconds = 5       # pause before a failed service restarts
reader_eof_timeout_seconds = 2  # wait for a finished attempt's output to end
max_stream_bytes = 1048576      # per stream, per attempt, then truncated
read_buffer_bytes = 8192        # one read from a running command's pipe
max_running_attempts = 32       # running task attempts across every job, 0 for no limit

[ui]
page_size = 25                  # rows per page on the run, job and schedule lists
max_page_size = 100             # the largest ?page_size= the run list accepts
refresh_interval_seconds = 3    # how often a page showing a live run refreshes
theme = "auto"                  # auto follows the viewer's OS, or dark, or light

[job_defaults]
timeout_seconds = 3600          # what a task with no timeout: gets
max_retries = 0
retry_delay_seconds = 60
max_parallel_runs = 1           # what a job with no max_parallel_runs: gets
keep_runs = 100                 # the newest finished runs of one job to keep, 0 keeps every run

[schedule_defaults]
timezone = "UTC"                # what a schedule with no timezone: reads its cron in

[retention]
keep_runs_total = 10000         # the most finished runs kept across every job, 0 for no ceiling
max_deletes_per_pass = 100      # the most runs one pass deletes, 0 for no cap
```

`[job_defaults]` and `[schedule_defaults]` fill in what a job's or schedule's YAML leaves
out, and they are read when the YAML is — at startup. So a task with no `timeout:` takes
its timeout from the data directory it was read in, and a run already submitted keeps the
value it was submitted with.

`[smtp]` and `[slack]` are the sections with no defaults, because there is no default mail
server and no default workspace: leave one out and that channel is off entirely. See
[Run notifications](#run-notifications).

```toml
[smtp]
host = "smtp.example.com"       # required
from = "flowlite@example.com"   # required
port = 587
username = ""                   # empty for a relay that authenticates nobody
encryption = "starttls"         # "starttls", "tls" (implicit, on 465) or "none"
max_output_bytes = 4096         # per stream, per failed task, in the message

[slack]
timeout_seconds = 10            # how long one post may take
max_output_bytes = 2048         # the most of one stream a post quotes
```

The password and the token belong in the environment rather than the file:

```bash
FLOWLITE_SMTP__PASSWORD=... FLOWLITE_SLACK__TOKEN=xoxb-... flowlite serve
```

The token alone is enough for Slack — with no `[slack]` table in the file at all, that
variable configures the channel.

`[secrets]` is where a job's `secret_env:` resolves its values, by name — see
[Secrets](#secrets). Both sources reach the same map, so use whichever suits the box: the
file suits a development box, the environment variable a real one, for the same reason the
SMTP password does.

```toml
[secrets]
warehouse_pw = "hunter2"
```

```bash
FLOWLITE_SECRETS__WAREHOUSE_PW=hunter2 flowlite serve
```

`[concurrency_limits]` resolves the name a job's `limits:` claims to a maximum — see
[Concurrency limits](#concurrency-limits). It is read the same two ways
(`FLOWLITE_CONCURRENCY_LIMITS__WAREHOUSE=3`), but there is nothing to hide in it: unlike a
credential, a wrong limit should be visible rather than redacted. `global` is reserved here
and refused at startup.

```toml
[concurrency_limits]
warehouse = 3   # 0 for no limit
```

The data directory itself is the one thing not worth setting here (`data_dir` in a file
inside it is circular); pass `-D` / `--data-dir` / `FLOWLITE_DATA_DIR`.

## Upgrading

flowlite keeps two schemas, and they upgrade differently.

The **memory** schema holds jobs, schedules and tasks, and is rebuilt from your YAML into a
fresh in-memory database on every start, so an upgrade asks nothing of you.

The **disk** schema holds run history, which outlives the process. Once flowlite is
released, a change there will be a new migration file rather than an edit, because `sqlx`
checksums every migration it has applied and an edited one makes an existing database
refuse to start:

```
migration 20260703234500 was previously applied but has been modified
```

**While flowlite is pre-release, the disk schema is edited in place too**: a history of how
the tables got here is worth less than one that describes them as they are. This release
does exactly that — `secret_env`, `process_group_id`, `notify_on` and `limits` moved into
the `CREATE TABLE` files that declare their tables, and the four migrations that used to add
them are gone.

So an upgrade across a pre-release version can ask something of you. Start the new binary;
if it refuses with the checksum error above, the remedy is to delete the database:

```bash
rm <data_dir>/flowlite.db
```

**That is not a safe operation — it is the cost of a pre-release schema.** Jobs and
schedules survive it, since they are read from the YAML on every start. The run history
does not, and nothing recreates a run. If a run history matters to you, copy the file
before upgrading.

This release also adds `max_running_attempts`, which defaults to `32`. A deployment that
previously fanned a wide job out past that will now run 32 attempts at a time and queue the
rest, which is a behaviour change even though nothing in your config asked for it. See
[Concurrency limits](#concurrency-limits).

## Status

Early / MVP. APIs and YAML schema may still change.

## License

Zero-Clause BSD — see [LICENSE](LICENSE). The frontend assets embedded in the binary keep
their own licenses, listed in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
