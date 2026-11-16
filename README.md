<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/brand/logo-dark.png">
  <img src="assets/brand/logo-light.png" alt="flowlite" width="260">
</picture>

flowlite is a job scheduler and orchestrator in a single binary. Jobs and schedules are YAML
files in a project directory; flowlite runs their commands on a cron schedule or on demand,
in dependency order, and records each run in a SQLite file in the same directory. It
includes a web dashboard, a CLI, and an MCP server through which an AI agent can submit and
inspect runs.

## Install

```bash
cargo install --git https://github.com/writes-the-spec/flowlite
```

This needs a Rust toolchain. There is no other service to run: no database server, broker or
worker pool.

## Quick start

```bash
flowlite init                           # writes an example job, schedule and flowlite.toml
flowlite validate                       # runs the checks serve makes at startup
flowlite serve                          # scheduler, orchestrator and dashboard on 127.0.0.1:8000
flowlite job submit hello-world --wait  # in a second shell; exits non-zero unless the run succeeded
```

The project directory is the current one, or the one `-p` names. Jobs are read from `jobs/`,
schedules from `schedules/` and settings from an optional `flowlite.toml`. Everything
flowlite writes goes under `.flowlite/`.

## Example

A job is a list of tasks. Each task is a shell command, and `depends_on` orders them:

```yaml
# jobs/ci-report.yaml
id: ci-report
name: CI report
tasks:
  - id: collect
    command: /srv/ci/failures.sh > "$FLOWLITE_TASK_OUTPUT"   # the task's result
    max_retries: 2                 # three attempts in all

  - id: summarise
    depends_on: [collect]          # starts only if collect succeeded
    command: my-agent > "$FLOWLITE_TASK_OUTPUT"
    stdin: |
      Summarise the test failures listed in the file $FLOWLITE_INPUT_COLLECT.
    timeout: 1800                  # seconds per attempt
    idle_timeout: 300              # ends an attempt with no output for five minutes
```

A schedule submits one or more jobs on a cron expression:

```yaml
# schedules/nightly.yaml
id: nightly
name: Nightly
cron: "0 30 1 * * *"               # six fields, seconds first: 01:30:00 every day
timezone: Europe/Paris
jobs:
  - id: ci-report
```

`my-agent` stands for any command. `stdin:` is passed to it as written, with no shell
expansion, so here the agent receives the variable's name and reads it from its environment.

## Features

- **Scheduling**: six-field cron with time zones; runs can also be submitted from the CLI,
  the dashboard or MCP.
- **Tasks**: a dependency graph per job, parameters, a timeout, an idle timeout, retries
  with a delay, and named concurrency limits shared across jobs.
- **Results**: a task writes a result to `$FLOWLITE_TASK_OUTPUT`, and each task that depends
  on it reads it from `$FLOWLITE_INPUT_<TASK>`. A retry receives the failed attempt's output
  in `$FLOWLITE_PREVIOUS_ATTEMPT_LOG`.
- **Process handling**: a timeout or a stop ends the task's whole process group, with
  `SIGTERM` first and `SIGKILL` after a grace period.
- **Run directories**: a task without a `working_dir` runs in a directory created for its
  run, so concurrent runs do not share files.
- **Child runs**: a task can submit further runs. They are linked to the task and stopped
  if it fails or is stopped.
- **Secrets**: `secret_env:` maps an environment variable to a named secret in
  `flowlite.toml`. `serve` does not start if a job names a secret that is not defined.
- **Notifications**: email and Slack messages on a run's outcome.
- **History**: each run stores the job definition it executed, so a rerun uses that
  definition. `keep_runs` sets how many runs a job keeps.

## MCP

`flowlite mcp` serves the Model Context Protocol on stdin and stdout. Register it with an
MCP client as a stdio server:

```json
{
  "mcpServers": {
    "flowlite": { "command": "flowlite", "args": ["-p", "/path/to/project", "mcp"] }
  }
}
```

Its ten tools list, submit, inspect and stop runs, read their logs, and validate the project
directory. They return the same fields the CLI prints with `--json`. `submit_job` also
accepts inline YAML, which runs once without being installed. An agent running inside a task
can call the same tools; the runs it submits become that task's children. See
[the tool list](REFERENCE.md#driving-flowlite-from-an-agent).

## Security

flowlite does not sandbox tasks. A task runs as the user that runs `flowlite serve`, with
that user's filesystem and network access. It can read `flowlite.toml`, including
`[secrets]`, and a `secret_env:` value is in its environment, so a command or an agent can
write it to a log or a result.

The dashboard binds to `127.0.0.1` with no login by default. `serve --address` and an
`[auth]` section change both; see [Logging in](REFERENCE.md#logging-in).

## Documentation

[REFERENCE.md](REFERENCE.md) describes every YAML key, command, MCP tool and configuration
setting, and how to [upgrade](REFERENCE.md#upgrading).

## Status

The current release is 0.2.0. Run history upgrades in place from 0.1.0. The YAML format, the
CLI and the MCP tools may change before 1.0.

Not implemented: an approval step before a run starts, cost tracking, a success check for a
task that exits 0 with a wrong result, and a sandbox.

## License

Zero-Clause BSD; see [LICENSE](LICENSE). The frontend assets embedded in the binary keep
their own licenses, listed in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
