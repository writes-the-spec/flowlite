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

Define a job in `.config/jobs/hello.yaml`:

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

## UI

A read-only, pure HTML dashboard is served directly from the binary:

```bash
flowlite serve --config-dir .config
```

Visit `http://localhost:8000` to see the job list, run history, and DAG status.

## Status

Early / MVP. APIs and YAML schema may still change.

## License

TBD
