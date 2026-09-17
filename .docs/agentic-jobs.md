# Running agentic jobs on flowlite

Status: analysis, 2026-09-17. Against `main` at `fdd856d`.

## What an agentic job is, here

A task whose `command` starts an LLM agent — `claude -p`, an SDK script, a coding agent in
a checkout. It differs from `make build` in five ways that the orchestrator currently has
no opinion about:

1. **It produces a result, not just an exit code.** A plan, a patch, a verdict, a list of
   things to do next. Something downstream is meant to read it.
2. **It decides its own shape.** How many files to touch, how many subtasks there are, is
   known when it runs, not when the run was submitted.
3. **It exits 0 while being wrong.** Success is a judgement, not a status code.
4. **It costs money per second and per token**, and a loop that goes wrong costs it fast.
5. **It writes files**, and it writes them as arbitrary model-authored shell.

flowlite already does the hard parts of being an orchestrator: a DAG per run, snapshotted
definitions, retries, timeouts, process-group kills, concurrency gates, crash recovery,
notifications, a dashboard and an MCP surface. None of that has to be rebuilt. What is
missing is almost entirely in the five points above.

## What already carries over unchanged

Worth naming, so the task list below is not read as "rewrite it":

- **The run snapshot.** An agent-written pipeline that is rerun months later replays what
  it actually ran (`README.md`, "Reruns"). This is exactly right for agentic work, where
  the definition is often generated and thrown away.
- **`job submit -f` and the inline `yaml` argument of the `submit_job` MCP tool.** An agent
  can already author a pipeline and run it once without installing it.
- **`secret_env:`** resolving a *name* at spawn — the API key is not frozen into a run.
- **Process-group kill and `Invalid`.** An agent that forks a sandbox, a language server
  and three subprocesses is exactly the case `process_group(0)` plus `killpg` was written
  for, and `Orchestrator::recover` already refuses to guess after a crash.
- **`[concurrency_limits]`.** The name/number split (YAML names the resource, config says
  how much exists) is the right shape for provider quotas; it is the *unit* that is wrong,
  which is task T7.
- **stdin as `/dev/null`.** Right by default: an agent CLI that stops to ask for auth
  should take its non-interactive path immediately rather than hang for an hour. T6 adds a
  way to *supply* stdin, and keeps this as the default.

## The gaps

### 1. A task cannot hand anything to the next task

The only thing a task produces is stdout, stderr and an exit code. Output is captured for a
human to read: `task_run_attempt_output` rows, truncated head-and-tail at
`max_stream_bytes`, with the middle dropped. Nothing downstream can read it, and nothing
would want to read *that* — the middle is where the answer usually is.

So a two-task agentic job — "plan, then execute the plan" — has nowhere to put the plan
except a file at a path both tasks hardcode, which is also the race in gap 3.

This is the single largest gap. Every other one is a convenience next to it.

### 2. The DAG is fixed at submit time

`CRUD::submit_job` writes every `task_run` and its `depends_on` when the run is created.
An agent that finds 40 files to fix cannot fan out over them; a job with an unknown number
of branches has to be written as one task that does everything serially inside one command,
which loses per-item retries, per-item output, per-item limits and the whole timeline.

A task *can* call `flowlite job submit` — `FLOWLITE_DATA_DIR` is injected for exactly that
— but the child run is unlinked: `job_run` has no parent column, so the parent cannot wait
on it, a stop does not cascade, the dashboard does not relate them, and retention reaps
them independently.

### 3. There is no per-run workspace

`working_dir` is a fixed string on the task row. Every concurrent run of a job gets the
same directory. Two runs of an agent job that edits a checkout will interleave their edits.
`max_parallel_runs: 1` hides it for one job and does nothing across jobs or for reruns.

Agents write files as their normal mode of operation. A run needs a directory of its own,
shared by its tasks, named in the environment, and reaped when the run is.

### 4. Success is the exit code, and a retry is an identical repeat

`max_retries` re-runs the same command with the same inputs. For a flaky `curl` that is
right. For an agent it is asking the same question again and hoping — the retry carries no
record of why the last attempt was rejected, because nothing recorded a reason. There is no
place to express "this attempt exited 0 but its output did not satisfy the task", and no
way for a verifier task to send work back.

### 5. Nothing bounds spend

`max_running_attempts` and `[concurrency_limits]` bound *simultaneity*. A provider quota is
requests or tokens per minute, and a budget is currency per run or per day. An agent in a
tool-call loop satisfies every limit flowlite has while spending without bound, and nothing
in the run history says what a run cost.

### 6. Timeouts are wall-clock, and the kill has no grace

`timeout` kills the process group with `SIGKILL`
(`src/orchestrator/task_run_attempt_children.rs:50`) — no `SIGTERM`, no window to flush a
transcript or write a partial result. And wall-clock is the wrong signal: an agent working
steadily for 50 minutes is healthy, an agent silent for 10 is hung. flowlite already knows
when output last arrived, so the second measure is nearly free.

### 7. The trust boundary is undefended

A task is `sh -c <command>` as the flowlite user, with the server's whole inherited
environment less `FLOWLITE_*`, full network and full filesystem. That is a reasonable deal
when a human wrote the command and reviewed it in git. It is a different deal when the
command was written by a model, or when the command *is* an agent that writes further
commands. There is no sandbox, no env allowlist (only the `FLOWLITE_*` denylist), and an
agent handed an API key through `secret_env:` can trivially read it back out of its own
environment.

Some of this is accepted risk that should be written down rather than engineered away. The
allowlist and an optional container runner are not.

### 8. Triggers are cron and manual only

Agentic work is event-shaped: a webhook fires, a PR opens, a file lands, an earlier run
finishes. flowlite has `Scheduler` (cron) and `job submit`. `src/router/api/mod.rs` exists
and is empty — the slot was left, and nothing fills it. There is also no job-to-job trigger,
though `JobYamlNotify`'s own doc comment anticipates one ("the action to run on a failure
can join it later").

### 9. Nothing off-box can drive it

MCP is stdio and spawns a process per client. The dashboard is localhost with a same-origin
check and no authentication at all. An agent on another machine, a CI step, or a webhook
sender has no way in. Adding one means adding auth at the same time — the current design
leans entirely on "bound to localhost".

### 10. YAML is read once, at startup

No watcher, no reload. An agent that writes a new job file has to get the server restarted
before it is installed. `submit -f` covers the one-off case, which is most of it, but not
"the agent maintains the pipeline".

## Tasks

Ordered so each one is useful on its own. T1–T3 are what make an agentic job expressible at
all; T4–T7 are what make it safe to leave running; T8–T12 are reach.

### T1 — Give a task a result channel

Inject `FLOWLITE_TASK_OUTPUT` naming a file path. Whatever a task writes there is stored
against the `task_run` and injected into every task that depends on it, as
`FLOWLITE_INPUT_<TASK_ID>` (a path, not the contents — an agent's result will not fit in an
environment variable).

- New disk table `task_run_result`, or a column on `task_run`; a column is likely enough and
  keeps the snapshot story intact.
- Bound it explicitly, the way `max_stream_bytes` is bounded, and fail the task loudly when
  it overruns rather than truncating a result something will parse.
- Written by `TaskRunAttemptMonitor` when the attempt finishes; read by
  `TaskRunDispatcher` when it starts a dependent. Only the *successful* attempt's file is
  kept as the task's result.
- Show it on the task run page and return it from `get_job_run` / `get_job_run_logs`.

Depends on nothing. Everything else in agentic use reads better with it.

### T2 — Give each run a workspace

Create a directory per job run under the data directory, inject it as `FLOWLITE_RUN_DIR`,
and make it the default `working_dir` for tasks that declare none.

- Created by `JobRunDispatcher` when the run starts, so a `scheduled` run costs nothing.
- Deleted by `RetentionService` with the run, which means retention grows a filesystem
  responsibility it does not have today — that is the real work in this task.
- `FLOWLITE_TASK_OUTPUT` (T1) lives inside it.
- Keep the current inherit-the-server's-cwd behaviour reachable, since a job that edits a
  fixed checkout is still a legitimate thing to write.

### T3 — Link a child run to its parent

Add `parent_job_run_id` to `job_run`, set it when a task submits a run, and add a task key
— `wait_for_children: true`, or a distinct task kind — that holds a task run open until
the runs it spawned have settled.

This is the cheap route to a dynamic DAG: the agent decides the fan-out, flowlite keeps the
accounting. A true dynamic-expansion feature (a task that returns a list and the
orchestrator materialising one task run per item) is the expensive route, and should only be
attempted if T3 turns out to be insufficient in practice.

Also gets: the stop cascade, retention cascading to children, and a parent→children link on
the run page.

### T4 — Separate "the command exited 0" from "the task succeeded"

Add a task-level check that runs after the command and decides the task's status — the
simplest version being `success_when:` holding a shell expression evaluated with the result
file from T1 in scope.

Without this, every verification step in an agentic pipeline is a separate task whose only
job is to `exit 1`, which works but reads badly and costs a process.

### T5 — Make a retry carry why the last one was rejected

Inject the previous attempt's result path and its failure reason into the retried attempt:
`FLOWLITE_PREVIOUS_ATTEMPT_OUTPUT`, `FLOWLITE_PREVIOUS_ATTEMPT_STATUS`. An agent that is
told what was wrong with attempt 1 is a different proposition from one asked the same
question twice.

Small, self-contained, and only meaningful after T1.

### T6 — Let a task supply stdin — **done**

A `stdin:` key on a task, taking literal text or a path. Keep `/dev/null` as the default and
keep the reasoning in the README — this adds a way to supply input, it does not reopen the
terminal.

Prompts are multi-line. Before this, they had to survive YAML quoting *and* shell quoting
inside `command:`, which is how a prompt got silently mangled — backticks ran, `$` expanded,
an apostrophe ended the string, and none of the three failed loudly.

Landed as a `stdin:` key snapshotted onto the task run beside `command`, written to the
child on a pipe from a task of its own so a large input cannot block a poller pass. Empty
stays `/dev/null`.

### T7 — Bound spend, not just simultaneity

Three separate pieces, in this order:

1. **Rate limits.** Extend the `[concurrency_limits]` idea with `N per window`, since a
   provider quota is a rate. Same name/number split; a new section rather than overloading
   the existing one, because "3 at once" and "3 per minute" must not be confusable.
2. **Cost accounting.** A task reports what it spent — a number in the result file from T1,
   or a `FLOWLITE_COST` convention — recorded on the attempt and totalled on the run.
   flowlite should not try to compute cost itself; it should record what it is told.
3. **Budgets.** A per-job and per-day ceiling that `TaskRunAttemptDispatcher` checks before
   it spawns, alongside the limit checks it already makes. A run stopped by a budget needs
   its own terminal status or a clear reason on the existing one.

### T8 — Graceful kill and an idle timeout

- `SIGTERM`, a grace period from `[orchestrator]`, then `SIGKILL`. Applies to the timeout
  path, the stop path and shutdown alike.
- `idle_timeout` on a task: no output for N seconds ends the attempt. The reader already
  timestamps every chunk, so the data is there.

### T9 — Write down the trust boundary, and tighten the cheap parts of it

- A section in the README stating plainly what a task can reach: the server's user, its
  environment, its network, its filesystem. Today a reader has to infer it.
- An env **allowlist** as an option, rather than only the `FLOWLITE_*` denylist.
- Optional per-task sandboxing — a container or a `bwrap`-style runner as a configurable
  spawn strategy. This is a large task and should be specced separately; the point here is
  that `sh -c` is currently the only strategy and nothing in the code anticipates a second.

### T10 — Fill in `src/router/api/mod.rs`

A small HTTP API — submit, get run, get logs, stop — with a bearer token from config, and
`--address` no longer implying "anyone who can reach me may start work". Auth and the API
land together or not at all.

This is what lets a webhook, a CI step or an off-box agent drive flowlite, and it is the
prerequisite for T11.

### T11 — Event triggers

- An HTTP trigger endpoint (needs T10).
- Job-to-job: `on_success:` / `on_failure:` gaining an action that submits another job, in
  the block `JobYamlNotify` was deliberately shaped to accept one.

### T12 — Reload YAML without a restart

A `flowlite reload` command, or a watcher on the data directory. Lowest priority: `submit
-f` covers the one-off, and the "definitions are read at startup" rule is load-bearing in
several places (snapshotting, the secret check, limit-name validation) — a reload has to
re-run all of it, which is why it is last rather than first.

## What to do first

T1, T2 and T3 together are what turn "you can run an agent as a task" into "you can write an
agentic pipeline". T1 alone is worth doing before anything else on this list, because T4,
T5 and most of T7 are unwritable without it.
