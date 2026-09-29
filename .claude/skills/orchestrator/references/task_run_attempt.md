# Task run attempt

`TaskRunAttemptStatus` lives in [src/crud/task_run_attempt.rs](../../../../src/crud/task_run_attempt.rs). One `task_run_attempt` row per execution of a task run's command — the only level that runs a process. Every row is inserted `Queued` by `TaskRunMonitor` (attempt 1 and each retry, `attempt` = `1`, then `last.attempt + 1`); a `UNIQUE (task_run_id, attempt)` index turns a racing second insert into an error rather than a double execution. After the insert, every transition belongs to the attempt services.

| Status | Meaning |
|---|---|
| `Queued` | Inserted, waiting for the dispatcher. |
| `Running` | Command spawned; the monitor owns the process. |
| `Succeeded` | Exited 0 and its result (if any) was recorded. |
| `Failed` | Exited non-zero, or exited 0 with a result that couldn't be recorded. |
| `TimedOut` | Ran past `task_run.timeout` and was killed. |
| `Aborted` | Killed because the job run was stopped. |
| `Skipped` | Job run stopped before dispatch; the command never started. |
| `Invalid` | flowlite cannot account for it — see [Rows flowlite cannot read](../SKILL.md#rows-flowlite-cannot-read). |

A separate enum from `TaskRunStatus` (which has `Planned`/`Waiting` where this has `Queued`); `TaskRunMonitor` maps one onto the other explicitly. `Queued` is what makes an attempt skippable: a stop arriving before dispatch has nothing to kill.

## Dispatcher: Queued → Running / Skipped

`TaskRunAttemptDispatcher` ([src/orchestrator/task_run_attempt_dispatcher.rs](../../../../src/orchestrator/task_run_attempt_dispatcher.rs)) polls all `Queued` attempts. `derive_next_status`, in order:

1. `Invalid` — **already spawned for?** (`started_at` set) → `set_to_invalid_already_spawned`. Only a crash between spawn and the `Running` write leaves this; the command may be running, and running it twice is worse than an unknown outcome. First, so a stop cannot mask it.
2. `Skipped` — **job run stopped?** → `set_to_skipped`: `finished_at` set, `started_at` NULL. Before 3, so a stop beats a waiting retry rather than spawning when the delay ends.
3. `Queued` — **`should_stay_queued`**: a retry inside its delay (`attempt > 1` and `now < created_at + task_run.retry_delay`), `[orchestrator] max_running_attempts` full (0 = no limit), or a claimed named limit (`task_run.limits`, via `a_claimed_limit_is_full`) full → nothing written. A limit name missing from `[concurrency_limits]` is treated as unlimited, with a log line. Must precede 4, which spawns unconditionally.
4. `Running` — `set_to_running`: load the `task_run` and `job_run`, build the env with [`build_task_run_attempt_env`](../../../../src/orchestrator/task_run_attempt_env.rs) (task `env:`, then `FLOWLITE_PARAM_*` from `job_run.parameters`, then injected metadata including `FLOWLITE_TASK_OUTPUT` and one `FLOWLITE_INPUT_<TASK_ID>` per dependency that produced a result — see the [entities skill](../../entities/references/job_run.md)); the server's own `FLOWLITE_*` variables are stripped first. Spawn `sh -c <command>` with piped stdout/stderr, stdin null unless the task declares `stdin:`, working directory `task_run.working_dir` or, when empty, the run's own `.flowlite/runs/<job run id>`.
5. anything else → `set_to_invalid`, logged as a bug.

`set_to_running` write order, each step there for a crash or race:

- `started_at` is written **before** the spawn (that is what step 1 reads), and cleared again if the spawn fails so the attempt can be retried. A spawn failure is the row's error for `Poller::run` to log.
- One reader task per stream is spawned, the child goes **into `TaskRunAttemptChildren` before** `Running` and `process_group_id` are written — the other order lets the monitor see a `Running` attempt with no process and settle it `Invalid`.

**Every attempt gets its own process group** (`process_group(0)`; id = pid of the `sh`). `sh -c` forks for anything with `;`, a pipe or a background job, so only a group kill reaches all of it. Keep the group and `kill_process_group` together.

## Monitor: Running → finished

`TaskRunAttemptMonitor` ([src/orchestrator/task_run_attempt_monitor.rs](../../../../src/orchestrator/task_run_attempt_monitor.rs)) takes each `Running` attempt's child out of `TaskRunAttemptChildren`:

- **No child** → `set_to_invalid`. The map holds only this process's spawns, so this is the restart path (or a child lost to an error mid-pass): no exit status, no group, no readers — no outcome can honestly be claimed. Output persisted earlier stays. Settled, not raised, so the task run and job run above don't strand a parallel slot. A command a crash left running is killed by recovery at the next start — see [Picking up after a crash](../SKILL.md#picking-up-after-a-crash).
- **Child present** → `derive_next_status`, in order:
  1. **exited?** (`try_wait`) → zero: `set_to_succeeded`; non-zero: `Failed`.
  2. **past `times_out_at`?** (spawn time + `task_run.timeout`, so dispatch wait doesn't count) → `TimedOut`, killing the group.
  3. **job run stopped?** → `Aborted`, killing the group.
  4. otherwise `Running` → `record_output`, child put back.
  - any other status → `set_to_invalid`, killing the group, logged as a bug. A derive error puts the child back and returns `Err`.

**Precedence** (same rule as [job_run.md](job_run.md)): a real outcome outranks a stop, so a process that already exited reports its exit and one past its timeout reports the timeout; a still-running one is killed on the same pass. 1 and 2 carry nothing between them.

`set_to_succeeded` reads the result the command wrote to `$FLOWLITE_TASK_OUTPUT`: over `[orchestrator] max_task_output_bytes` or not UTF-8 → the attempt is `Failed` (never truncated — half a document parses as a whole one), with the reason appended to its stderr.

**Every outcome is written by `finish_task_run_attempt`**, which sets `finished_at`, publishes, and — for any status but `Succeeded` — first calls `CRUD::stop_child_job_runs`, so runs the attempt submitted live only as long as it does unless it succeeded (see the [parent skill](../SKILL.md#stopping-a-run)).

**Kills go through `TaskRunAttemptChild::kill_process_group`**, which `killpg`s the group before reaping the `sh`. Its third caller is shutdown: `serve` stops `with_graceful_shutdown` on Ctrl-C or SIGTERM and calls `Orchestrator::shutdown` → `TaskRunAttemptChildren::kill_all`. This is deliberate, since a task in its own group no longer dies with the terminal — and why the map lives on the `Orchestrator`. Shutdown leaves the rows `Running` (writing statuses would race the still-running pollers); `Orchestrator::recover` settles them `Invalid` at the next start.

It never touches a task run row: retries and task run status are `TaskRunMonitor`'s.

## The shared children map

`TaskRunAttemptChildren` ([src/orchestrator/task_run_attempt_children.rs](../../../../src/orchestrator/task_run_attempt_children.rs)) is a `Mutex<HashMap<task_run_attempt_id, TaskRunAttemptChild>>` held by the `Orchestrator` and shared by the two attempt services: the dispatcher inserts, the monitor removes. The orchestrator's only cross-service state outside the database, in memory only.

## Stdout/stderr

**Pipes are never read on the poll pass** (the pass is serial, so per-attempt waits add up across every running attempt). The dispatcher spawns two readers ([src/orchestrator/task_run_attempt_reader.rs](../../../../src/orchestrator/task_run_attempt_reader.rs)) that own the pipes, validate UTF-8, record at most `[orchestrator] max_stream_bytes` per stream (kept as head and tail) and send `String` chunks down one unbounded mpsc. The monitor only drains it:

- `Running`: `record_output` `try_recv`s and never waits, so output lands at most one pass late.
- Terminal: `finish_reading` waits for the channel to close (both readers at EOF), bounded by `[orchestrator] reader_eof_timeout_seconds` (default 2) because a grandchild that escaped the group can hold a pipe open; on timeout it aborts the readers.

**Kill before drain** on timeout and stop: closing the pipes is what produces EOF, so draining first would wait out the whole bound.

Each pass writes at most one [`task_run_attempt_output`](../../entities/references/task_run_attempt_output.md) row per stream with new output. It is data-only, so it **never publishes** — see [How they coordinate](../SKILL.md#how-they-coordinate). The final output lands **before** the status, so a terminal attempt has complete output.

## Invariants

- **A command is spawned at most once per attempt row.** A `Running` attempt without a child, or a `Queued` one with `started_at` set, is settled `Invalid`, never respawned; a retry is a new row from `TaskRunMonitor`.
- **Attempt 1 comes from `TaskRunMonitor`**, inserted on its first pass over a `Running` task run with no attempts.
- **Terminal statuses set `finished_at`** via `finish_task_run_attempt` (or the dispatcher's `set_to_skipped` / `set_to_invalid*`).
- **A new `TaskRunAttemptStatus` needs a return in `TaskRunMonitor`** — see [task_run.md](task_run.md).
