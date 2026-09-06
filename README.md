# flowlite

flowlite is a lightweight scheduler and orchestrator that ships as a single, zero-dependency Rust binary.

No database to install, no message broker, no runtime to configure — download the binary, point it at a config directory, and it runs your jobs.

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

Define a job in `.config/jobs/hello.yaml` (`.yml` works too):

```yaml
id: hello-world
name: Hello World Job
tasks:
  - id: say-hello
    command: echo "hello from flowlite"
```

Start the server (runs the scheduler and the UI):

```bash
flowlite serve --config-dir .config
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
    command: make build

  - id: test
    depends_on: [build]
    command: make test

  - id: deploy
    depends_on: [test]
    command: make deploy
```

`test` only runs if `build` succeeds; `deploy` only runs if `test` succeeds.

## Schedules

Schedules trigger jobs on a cron expression. Define them in `.config/schedules/daily.yaml`:

```yaml
id: daily-schedule
name: Daily Schedule
cron: "*/15 * * * * *"  # every 15 seconds
timezone: UTC
dags:
  - id: hello-world
```

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

## Overlapping runs

A job runs one at a time by default. A run created while another one is still going is
not rejected — it waits as a pending job run and starts as soon as the earlier one
finishes, oldest waiting run first.

Raise or lift the limit per job:

```yaml
id: nightly-sync
name: Nightly Sync
max_active_runs: 2   # 0 for no limit
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
edges, the timeouts and the retry settings — taken when the run was submitted. So
rerunning an old run reruns its old config, and a run whose job YAML has since been
edited or deleted is still rerunnable. To run the job as it is defined now, submit it
instead:

```bash
flowlite job submit hello-world
```

## UI

A read-only, pure HTML dashboard is served directly from the binary:

```bash
flowlite serve --config-dir .config
```

Visit `http://localhost:8000` to see the job list, run history, DAG status, and the
output of any task.

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

`<data_dir>` is whatever you pass to `--data-dir`, and otherwise your OS data directory
plus `flowlite` (`~/Library/Application Support/flowlite` on macOS,
`~/.local/share/flowlite` on Linux). Only run history is lost — jobs and schedules are
read from the YAML on every start.

## Status

Early / MVP. APIs and YAML schema may still change.

## License

TBD
