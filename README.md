<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/brand/logo-dark.png">
  <img src="assets/brand/logo-light.png" alt="flowlite" width="260">
</picture>

**A job scheduler and orchestrator in a single binary, for scripts, pipelines and AI agents
alike.** Put any command in a YAML file and flowlite runs it on a schedule, in order, with
retries, timeouts and alerts. Agentic jobs get more: each step's result is handed to the
next, a retry is told why the last attempt failed, and an agent that hangs is ended. An
agent can also drive flowlite itself over MCP.

## Why flowlite

**For any job:**

- **Nothing to install.** There is no database server, broker, worker pool or Python to run.
  It is one binary and a SQLite file beside your YAML.
- **Jobs are files.** A pipeline is plain YAML in git, reviewed and reverted like any other
  change. Cron with time zones, a DAG per job, retries, timeouts, concurrency limits,
  email and Slack alerts, a localhost dashboard and retention all come built in.
- **Runs remember themselves.** Every run snapshots the definition it executed, so a rerun
  months later replays that run rather than today's file.
- **Nothing is left behind.** A timeout or a stop ends the whole process tree, asked first
  and killed after a grace period, and a crash leaves nothing running past the next start.

**For agentic jobs**, the kind that produce a result something downstream has to read, exit
0 while being wrong, stall on a provider and cost money the whole time:

- **Results, not just exit codes.** A task writes its result to `$FLOWLITE_TASK_OUTPUT`, and
  every task that depends on it reads it from `$FLOWLITE_INPUT_<TASK>`. Logs stay logs.
- **Retries that know what went wrong.** A retried attempt is handed the failed attempt's
  stdout, stderr and result, so the second try need not repeat the first.
- **Hangs end.** `idle_timeout` ends an agent that has gone quiet long before `timeout`
  would.
- **Prompts without shell quoting.** `stdin:` hands a prompt to the command as written, so
  no backtick runs and no `$` expands.
- **Fan-out decided at run time.** A task can submit further runs, from the CLI or over MCP.
  Those runs are linked to the task, stopped if it fails or is stopped, and deleted with it.
- **Provider quotas.** A named limit (`llm_api = 4`) caps how many tasks reach one provider
  at once, and a task waiting with `--wait` on a run it submitted holds no slot.

## Install

```bash
cargo install --git https://github.com/writes-the-spec/flowlite
```

Or clone and `cargo build --release`, which leaves the binary at `target/release/flowlite`.

## Quick start

```bash
flowlite init                           # an example job, schedule and commented flowlite.toml
flowlite serve                          # scheduler, orchestrator and dashboard on :8000
flowlite job submit hello-world --wait  # in a second shell; exits non-zero unless it succeeded
```

Everything lives in the data directory: the current one, or whichever `-D` names. Jobs are
under `jobs/`, schedules under `schedules/`, and run history in `flowlite.db`.

## A scheduled pipeline

A nightly ETL in three steps, each starting only once the one before it has succeeded:

```yaml
# jobs/nightly-etl.yaml
id: nightly-etl
name: Nightly ETL
parameters:
  region: eu-west-1                # reaches every command as $FLOWLITE_PARAM_REGION
tasks:
  - id: extract
    command: /srv/etl/extract.sh
    max_retries: 2                 # three attempts in all, a minute apart

  - id: transform
    depends_on: [extract]
    command: /srv/etl/transform.sh
    timeout: 1800

  - id: load
    depends_on: [transform]
    command: psql -h warehouse.internal -U etl -f /srv/etl/load.sql
    secret_env:
      PGPASSWORD: warehouse_pw     # a secret's name; its value never enters the YAML
```

```yaml
# schedules/nightly.yaml
id: nightly
name: Nightly
cron: "0 30 1 * * *"               # 01:30:00 every day; six fields, seconds first
timezone: Europe/Vienna
jobs:
  - id: nightly-etl
```

```toml
# flowlite.toml
[secrets]
warehouse_pw = "..."
```

`serve` refuses to start on a job that names a secret nothing defines, rather than letting
the run fail at 01:30. Add `on_failure:` with an address or a Slack channel to be told when
a run breaks, with the failing output quoted in the message. Run it now rather than tonight
with `flowlite job submit nightly-etl --wait`.

## An agentic pipeline

One agent reads the CI logs and picks the flaky tests worth fixing, and a second agent
fixes them. `my-agent` stands for whichever agent CLI you run:

```yaml
# jobs/flaky-tests.yaml
id: flaky-tests
name: Fix flaky tests
limits: [llm_api]                  # claims one of [concurrency_limits] llm_api
secret_env:
  LLM_API_KEY: llm_api_key
tasks:
  - id: triage
    command: my-agent > "$FLOWLITE_TASK_OUTPUT"
    stdin: |
      Read last week's CI logs under /srv/ci/logs and list the flaky
      tests worth fixing, one per line, most expensive first.
    idle_timeout: 300              # five quiet minutes and the attempt is ended

  - id: fix
    depends_on: [triage]
    working_dir: /srv/checkouts/api
    command: |
      my-agent "Fix these and open one PR per test: $(cat "$FLOWLITE_INPUT_TRIAGE")"
    timeout: 3600
    max_retries: 1                 # the retry gets $FLOWLITE_PREVIOUS_ATTEMPT_LOG
```

```toml
# flowlite.toml
[concurrency_limits]
llm_api = 4

[secrets]
llm_api_key = "..."
```

`triage` writes its answer to a file rather than to its log, and `fix` reads it from there.
A task with no `working_dir` works in a directory created for its run, so two runs never
edit the same files. Schedule it by adding `- id: flaky-tests` under a schedule's `jobs:`.

## Driving flowlite from an agent

`flowlite mcp` serves the Model Context Protocol on stdin and stdout. There is no port to
open and nothing to start first. Register it with any MCP client as a stdio server:

```json
{
  "mcpServers": {
    "flowlite": { "command": "flowlite", "args": ["-D", "./data", "mcp"] }
  }
}
```

| Tool | Answers |
|---|---|
| `init_data_dir` | Set this empty directory up with an example job and schedule. |
| `list_jobs` | What jobs does this data directory declare? |
| `submit_job` | Run this installed job, file or inline YAML, and optionally wait for the outcome. |
| `list_job_runs` | What has run lately, by job and by status? |
| `get_job_run` | What happened to run 42, task by task, and what did each task produce? |
| `get_job_run_logs` | What did each attempt write to stdout and stderr? |
| `stop_job_run` | Stop run 42, and tell me what it settled to. |
| `delete_job_run` | Remove run 42 before it is due, so its schedule writes it again. |
| `get_serve_status` | Is anything actually serving this directory? |
| `list_limits` | What is a queued run waiting behind? |

Each tool returns the same fields its CLI twin prints with `--json`. `submit_job` accepts a
pipeline an agent wrote itself as inline YAML, and runs it once without installing it.
`wait_seconds` (up to 300) returns the outcome in the same call instead of a polling loop.
An agent running *inside* a task can call the same tools, and the runs it submits become
that task's children.

## What a task can reach

flowlite does not sandbox. A task runs as the user `flowlite serve` runs as, with that user's
filesystem and network. It inherits the server's environment, less every `FLOWLITE_*`
variable. That strip keeps flowlite's own settings out of a task's environment, but not out
of its reach: `flowlite.toml`, `[secrets]` included, is a file the task can read. A
`secret_env:` value is in the process's environment, which means an agent handed a key can
read it and write it into its log or result.

Treat a job file like a shell script you are about to run, and run `serve` as a user whose
reach you would hand to a model. The dashboard binds to `127.0.0.1` and has no
authentication, and MCP is stdio with one process per client.

## Documentation

[REFERENCE.md](REFERENCE.md) covers every key and command:
[command inputs](REFERENCE.md#command-inputs) and [secrets](REFERENCE.md#secrets),
[task results](REFERENCE.md#task-results), [retries](REFERENCE.md#timeouts-and-retries),
[runs a task submits](REFERENCE.md#runs-a-task-submits),
[notifications](REFERENCE.md#run-notifications),
[concurrency limits](REFERENCE.md#concurrency-limits), [reruns](REFERENCE.md#reruns),
[retention](REFERENCE.md#retention), [the MCP tools](REFERENCE.md#driving-flowlite-from-an-agent),
[configuration](REFERENCE.md#configuration) and [upgrading](REFERENCE.md#upgrading).

## Status

0.2.0 is the current release. Since 0.1.0, run history upgrades in place. The YAML format,
the CLI and the MCP tools may still change before 1.0.

Not there yet: an approval gate before a run starts, recording what a run cost, a
`success_when:` check for a task that exits 0 while being wrong, and a sandbox.

## License

Zero-Clause BSD — see [LICENSE](LICENSE). The frontend assets embedded in the binary keep
their own licenses, listed in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
