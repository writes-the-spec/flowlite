# flowlite

flowlite is a lightweight scheduler and orchestrator that ships as a single, zero-dependency Rust binary.

No database to install, no message broker, no runtime to configure — download the binary, point it at a directory of YAML, and it runs your jobs.

## Why flowlite

- **Zero dependencies.** Everything is compiled into one Rust binary. No Python, no Redis, no Docker Compose stack.
- **YAML-based jobs and schedules.** Every job and schedule is a plain YAML file, so your setup lives in version control alongside your code — diffable, reviewable, and easy to roll back.
- **Read-only UI.** A simple, pure HTML dashboard lets you see job status, run history, and DAGs at a glance. No JavaScript build step, no interactivity to secure.
- **Lightweight by design.** Meant to run per-project as a sidecar, not as a shared multi-tenant service.

## Install

```bash
cargo install flowlite
```

## Quick start

Define a job in `jobs/hello.yaml` (`.yml` works too):

```yaml
id: hello-world
name: Hello World Job
tasks:
  - id: say-hello
    description: Says hello
    command: echo "hello from flowlite"
```

Start the server (runs the scheduler and the UI) from the directory that `jobs/` sits in:

```bash
flowlite serve
```

flowlite reads `jobs/`, `schedules/` and an optional `config.toml` from its data
directory, and writes `flowlite.db` there. That directory is the current one unless
`-D` / `--data-dir` / `FLOWLITE_DATA_DIR` says otherwise:

```bash
flowlite --data-dir /var/lib/flowlite serve
```

Submit the job:

```bash
flowlite job submit hello-world
```

Check its status:

```bash
flowlite job list
```

## DAGs

Tasks within a job can depend on each other using `depends_on`:

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

`test` only runs if `build` succeeds; `deploy` only runs if `test` succeeds.

## Schedules

Schedules trigger jobs on a cron expression. Define them in `schedules/daily.yaml`:

```yaml
id: daily-schedule
name: Daily Schedule
cron: "*/15 * * * * *"  # every 15 seconds
timezone: UTC
dags:
  - id: hello-world
```

## Command inputs

A command can be given configuration three ways: parameters declared on the job, environment variables set on the job or on a task, and a working directory — plus a handful of variables flowlite injects itself. All of them arrive as environment variables, since `sh -c <command>` inherits its environment like any process:

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
      TZ: Europe/Vienna
    working_dir: /srv/etl
```

An `env:` block on the job applies to every one of its tasks, and a task's own `env:` wins
any name both of them set. The two are merged when the run is submitted, so a task run
records the environment it will actually run with and a rerun replays exactly that. A task
cannot opt out of the job's block — setting the same name to a different value is how you
override it.

A declared parameter reaches the command **prefixed and upper-cased**: `region` becomes `FLOWLITE_PARAM_REGION`. The prefix is what stops a parameter named `path` or `home` from shadowing something the command actually needed.

Override a parameter at submit time:

```bash
flowlite job submit daily-etl --param region=eu-west-1
```

`--param name=value` can be repeated for more than one parameter; splitting happens on the first `=`, so a value may contain one, and a later repeat of the same name wins. Naming a parameter the job doesn't declare is refused, naming the job, the bad key and the declared names — a typo should fail loudly rather than deliver nothing to the command. `working_dir` left empty, the default, means the command inherits flowlite's own working directory.

### Precedence

Where a name collides, later wins, applied in this order:

1. The environment flowlite itself inherited.
2. The job's `env:`.
3. The task's `env:`.
4. `FLOWLITE_PARAM_*`.
5. The variables below, injected by flowlite.

Steps 2 and 3 are merged once, at submit time, onto `task_run.env`; steps 4 and 5 are composed at spawn.

Injected metadata is applied last so nothing a user writes in `env:` or a parameter can make a command lie about which run it belongs to.

### Injected variables

| Variable | Value |
|---|---|
| `FLOWLITE_JOB_ID` | The job this run is of. |
| `FLOWLITE_JOB_RUN_ID` | This run's id. |
| `FLOWLITE_TASK_ID` | This task. |
| `FLOWLITE_TASK_RUN_ID` | This task's run. |
| `FLOWLITE_TASK_RUN_ATTEMPT_ID` | This attempt. |
| `FLOWLITE_ATTEMPT` | Which attempt this is, starting at 1. |
| `FLOWLITE_SCHEDULED_AT` | The instant a schedule fired for, as RFC3339. Set only for a scheduled run — a manual `job submit` gets no such variable at all, not an empty one. |

**Parameters answer "which caller is this run for," not "which run is this."** A schedule declaring `slice: "2026-09-08"` as a parameter default is wrong on every occurrence after today, because a parameter is a fixed value carried unchanged from submit through every rerun — it is not re-evaluated per run. `FLOWLITE_SCHEDULED_AT` is what tells a recurring job which occurrence it is running; reach for it, not a parameter, whenever the question is "which day/hour/slice is this."

A rerun replays the original run's parameters and `FLOWLITE_SCHEDULED_AT` unchanged, not the job's current defaults — see [Reruns](#reruns).

### `env:` is visible in the dashboard, on purpose

The merged `env:` values — the job's and the task's — are shown as written on the run and task pages. The YAML they came from is already plaintext on disk, so rendering it leaks nothing a reader of the data directory couldn't already see, and hiding it would make a wrong `env:` value undebuggable from the run that used it. A secret belongs in the environment flowlite's own process runs in — the command inherits that like any environment, and flowlite neither stores nor displays it.

## Retries

A task that exits non-zero can be retried. `max_retries` is how many times it is tried
*again*, so a task with `max_retries: 2` gets three attempts in all, and `retry_delay`
is the number of seconds to wait after a failure before the next attempt starts:

```yaml
tasks:
  - id: fetch
    command: curl -fsS https://example.com/data.json -o data.json
    max_retries: 2
    retry_delay: 30
```

`max_retries` defaults to 0, so a task is not retried unless you ask for it.
`retry_delay` defaults to 60 seconds, on the grounds that whatever a retry is waiting
on rarely fixes itself within one second; set it to 0 to retry as soon as possible.
Each attempt keeps its own output — see [Task output](#task-output).

## Failure notifications

A job can name who to email when one of its runs does not succeed:

```yaml
id: nightly-sync
name: Nightly Sync
on_failure:
  email: [oncall@example.com, data-team@example.com]
tasks:
  - id: sync
    command: ./sync.sh
```

The message carries the run's status and timings, every task and how it ended, and the
**output of the tasks that broke** — so the mail itself usually says what went wrong,
without opening the dashboard:

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

**Only a real failure is mailed** — `failed` and `timed out`, never `aborted`. A run you
stopped yourself is not news.

### Where the mail server goes

Who to tell is a property of the job, so it lives in the job's YAML alongside it. *Where
mail goes out through* is a property of the machine, so it lives in `config.toml`:

```toml
[smtp]
host = "smtp.example.com"
port = 587                          # default
from = "flowlite@example.com"
username = "flowlite@example.com"   # omit for a relay that authenticates nobody
encryption = "starttls"             # or "tls" for implicit TLS on 465, "none" for a local MTA
```

The password is deliberately **not** a key you should write in the file. Every config key
can be set as an environment variable, so put it in flowlite's own environment:

```bash
FLOWLITE_SMTP__PASSWORD=... flowlite serve
```

That keeps it out of a file that sits beside your jobs in version control — and out of the
dashboard, which renders `env:` as written on purpose.

**A job that names an address while `config.toml` has no `[smtp]` section refuses to
start**, naming the job and the file:

```
Invalid notifications of job 'nightly-sync' at /srv/flowlite/jobs/nightly.yaml

Caused by:
    on_failure.email names oncall@example.com but config.toml has no [smtp] section,
    so no mail can be sent. Add one, or remove the addresses.
```

A notification that silently never leaves is the one failure you cannot see from the run
afterwards, so it is a startup error rather than a surprise at 03:00.

Each send is tried **once**. A send that fails is recorded against the run with the
error, and reported in the server log, rather than being retried against a relay that may
be down for hours.

Delivery runs as its own background service, so a slow or unreachable mail server never
holds up the runs themselves. Email is the only channel today; the run records which
channel it was told over, so more can join it.

## Overlapping runs

A job runs one at a time by default. A run created while another one is still going is
not rejected — it waits as a pending job run and starts as soon as the earlier one
finishes, oldest waiting run first.

Raise or lift the limit per job:

```yaml
id: nightly-sync
name: Nightly Sync
max_parallel_runs: 2   # 0 for no limit
tasks:
  - id: sync
    command: ./sync.sh
```

The limit is enforced in one place, when a pending run is picked up to start, so every
way of creating a run is held to it alike — `flowlite job submit`, a rerun, and the
scheduler. A job that takes longer than its schedule interval will therefore queue up
pending runs and work through them back to back.

## Task output

Every task's stdout and stderr are captured as it runs and kept per attempt, so a task
that was retried keeps the output of each try.

Read them from the command line:

```bash
flowlite job-run logs 42            # every task of run 42
flowlite job-run logs 42 --task build   # just one task
```

Or from the dashboard: click a task on the run timeline to open its output page, which
refreshes itself while the task is still running.

## Reruns

A run can be run again, with the **Rerun** button on its page in the dashboard or from
the command line:

```bash
flowlite job-run rerun 42
```

A rerun replays **the definition the original run executed, not the current YAML.** Every
run carries its own snapshot of the job and its tasks — the commands, the `depends_on`
edges, the timeouts and the retry settings, the resolved `parameters` and the `env` and
`working_dir` of every task — taken when the run was submitted, plus the `FLOWLITE_SCHEDULED_AT`
it fired for. So rerunning an old run reruns its old config for the same occurrence, and
a run whose job YAML has since been edited or deleted is still rerunnable. To run the job
as it is defined now, submit it instead:

```bash
flowlite job submit hello-world
```

## UI

A read-only, pure HTML dashboard is served directly from the binary:

```bash
flowlite serve
```

Visit `http://localhost:8000` to see the job list, run history, DAG status, and the
output of any task.

## Configuration

Everything below has a default, so flowlite runs with no `config.toml` at all. Write one
in the data directory to change any of it; a file naming a single key leaves every other
default alone, and each key can also be set as an environment variable
(`FLOWLITE_UI__PAGE_SIZE=10`, `FLOWLITE_ORCHESTRATOR__POLL_INTERVAL_SECONDS=5`).

```toml
[orchestrator]
poll_interval_seconds = 1       # how often a service looks for work itself
error_backoff_seconds = 5       # pause before a failed service restarts
reader_eof_timeout_seconds = 2  # wait for a finished attempt's output to end
max_stream_bytes = 1048576      # per stream, per attempt, then truncated
read_buffer_bytes = 8192        # one read from a running command's pipe

[ui]
page_size = 25                  # rows per page on the run, job and schedule lists
max_page_size = 100             # the largest ?page_size= the run list accepts
refresh_interval_seconds = 3    # how often a page showing a live run refreshes

[job_defaults]
timeout_seconds = 3600          # what a task with no timeout: gets
max_retries = 0
retry_delay_seconds = 60
max_parallel_runs = 1           # what a job with no max_parallel_runs: gets

[schedule_defaults]
timezone = "UTC"                # what a schedule with no timezone: reads its cron in
```

`[smtp]` is the one section with no defaults, because there is no default mail server:
leave it out and failure notifications are off entirely. See
[Failure notifications](#failure-notifications).

```toml
[smtp]
host = "smtp.example.com"       # required
from = "flowlite@example.com"   # required
port = 587
username = ""                   # empty for a relay that authenticates nobody
encryption = "starttls"         # "starttls", "tls" or "none"
max_output_bytes = 4096         # per stream, per failed task, in the message
```

`[job_defaults]` and `[schedule_defaults]` fill in what a job's or schedule's YAML leaves
out, and they are read when the YAML is — at startup. So a task with no `timeout:` takes its timeout from the data directory it was
read in, and a run already submitted keeps the value it was submitted with.

The data directory itself is the one thing not worth setting here (`data_dir` in a file
inside it is circular); pass `-D` / `--data-dir` / `FLOWLITE_DATA_DIR`. Likewise
`--address` and `--port` are flags on `serve`.

## Upgrading

While flowlite is pre-release, a column is added to an existing table by editing the
migration that created it rather than by adding a new one. `sqlx` checksums the
migrations it has already applied, so an existing database refuses to start after such a
change, with no hint at the remedy:

```
migration 20260703234500 was previously applied but has been modified
```

The remedy is to delete the run history and let it be recreated on the next start:

```bash
rm <data_dir>/flowlite.db
```

`<data_dir>` is whatever you pass to `--data-dir`, and otherwise the directory you run
from. Only run history is lost — jobs and schedules are read from the YAML on every
start.

## Status

Early / MVP. APIs and YAML schema may still change.

## License

TBD
