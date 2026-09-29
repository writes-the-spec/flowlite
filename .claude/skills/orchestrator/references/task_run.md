# Task run

`TaskRunStatus` lives in [src/crud/task_run.rs](../../../../src/crud/task_run.rs). One `task_run` per task per job run, created `Planned` by `CRUD::submit_job` for **every** task, not just the roots — ordering is enforced at dispatch.

| Status | Meaning |
|---|---|
| `Planned` | Written with its job run, which hasn't started. **Nothing dispatches it.** (Job-run spelling: `Scheduled`.) |
| `Waiting` | Released by `JobRunDispatcher::set_to_running`; waiting on its dependencies. |
| `Running` | Started and owned by `TaskRunMonitor`, including the gaps between attempts. |
| `Succeeded` | Its command exited 0. |
| `Failed` | Exited non-zero with no retries left. |
| `TimedOut` | Ran past `task_run.timeout` with no retries left. |
| `Aborted` | Stopped after it started — process killed, or its next attempt skipped. |
| `Skipped` | Never started: a dependency didn't succeed, or the job run was stopped while it was `Planned` or `Waiting`. |
| `Invalid` | flowlite cannot account for it — its last attempt was `Invalid`, or no outcome claimed it. |

`Skipped` also covers the dependency case, so alone it isn't a sign of a stop. No `Cancelled`: a stop finds a task run unstarted (`Skipped`, by `TaskRunDispatcher`) or started (`Aborted`, by `TaskRunMonitor`). **The task run's own lifecycle decides**, not its last attempt: one waiting to retry is `Aborted` even though the unspawned attempt is `Skipped`.

## Dispatcher: Waiting → Running / Skipped

`TaskRunDispatcher` ([src/orchestrator/task_run_dispatcher.rs](../../../../src/orchestrator/task_run_dispatcher.rs)) polls all `Waiting` task runs. **It never sees a `Planned` one** — that is the point of the status: otherwise a run due tonight, or held at `max_parallel_runs`, would have its root tasks started early. Because only `JobRunDispatcher::set_to_running` writes `Waiting`, "a `Waiting` task run's job run is `Running`" is an invariant.

`derive_next_status` loads the dependencies once and `handle_waiting_task_run` writes the result:

1. `Skipped` — `should_skip`: **job run stopped**, or **any dependency finished without succeeding** (`Failed`, `Skipped`, `Aborted`, `TimedOut`, `Invalid`). `Invalid` must be listed: it is finished (so 2 won't hold) and not succeeded (so 3 won't start), and without it one unreadable row makes its whole subtree fall through to `Invalid`. The stop check short-circuits first.
2. `Waiting` — `is_still_waiting`: any dependency unfinished → nothing written.
3. `Running` — `all_dependencies_succeeded` → `set_to_running`: `Running`, `started_at = now`. **One status write, nothing else** — attempts are `TaskRunMonitor`'s. (Inserting attempt 1 here too once let a crash between the writes strand the task run on the `(task_run_id, attempt)` unique index.)
4. `None` → `set_to_invalid`, logged as a bug. Unreachable. A read error goes back to `Poller` — settling `Invalid` on it would strand every dependent too.

**One dependency snapshot shared by all three checks**, so a dependency changing mid-decision can't produce a set that is neither all-succeeded nor unfinished and fall through to `None`.

Dependencies are `task_run.depends_on` (copied from `task.depends_on` at submit), resolved to task runs of the same job run by `get_dependent_task_runs`. No dependencies: 2's `any()` is false, 3's `all()` is true.

This transition happens **once per task run**; retries keep it `Running`, so `started_at` covers every attempt.

A job run stopped while `Queued` or `Scheduled` skips all its task runs at once via `CRUD::skip_job_run`.

## Monitor: Running → finished

`TaskRunMonitor` ([src/orchestrator/task_run_monitor.rs](../../../../src/orchestrator/task_run_monitor.rs)) polls `Running` task runs and reads only their `task_run_attempt` rows, never a process. `get_or_start_task_run_attempt` returns the **last** attempt (highest `attempt`, unique per task run), or inserts attempt 1 and reads it back when there is none — the ordinary first pass after the dispatcher set it `Running`. **Every attempt is created in this monitor**, keeping the unique index one file's concern. Read back rather than built, because `created_at` is what `retry_delay` is measured from.

| Last attempt | Task run |
|---|---|
| `Invalid` | `Invalid` — **never retried**: flowlite doesn't know what that attempt did |
| `Succeeded` | `Succeeded` |
| not finished | left `Running` (still the attempt services') |
| `Failed`, no retry left | `Failed` |
| `Failed`, retry left | left `Running`; `insert_next_attempt` writes attempt `last.attempt + 1` `Queued`, which `TaskRunAttemptDispatcher` holds until `retry_delay` passes |
| `TimedOut` | `TimedOut` |
| `is_stopped` (`Aborted`/`Skipped`) | `Aborted` — it had started |
| anything else | `None` → `set_to_invalid`, logged as a bug. Unreachable. A failed retry insert returns `Err` and settles nothing |

The checks are exclusive, so **order carries nothing** here; `Invalid` is first only to read like `JobRunMonitor`. `TaskRunAttemptStatus::is_stopped` needs no failure ruled out first, since an attempt is skipped only by a stop.

Only `Failed` is retried. Attempts count from 1: `1 + max_retries` executions in all, `max_retries: 0` means one. Count and delay come off the `task_run` row, so the submitted policy applies.

A stop landing *between* attempts isn't seen here: the monitor inserts the next attempt, the attempt dispatcher skips it, and the task run finishes `Aborted` from that.

## Invariants

- **`Waiting` has exactly one writer: `JobRunDispatcher::set_to_running`.** Everything else writes `Planned` (the two insert paths), `Running` or a terminal status. A second writer would make `TaskRunDispatcher` depend on the job run's status again.
- **A `Planned`, `Waiting` or `Running` task run keeps its job run `Running`**; one never visited again strands it — see [job_run.md](job_run.md).
- **A `Running` task run always has an unfinished attempt or a finished last attempt to decide on.** An attempt that never finishes stalls the task run and the job run.
- **A new `TaskRunAttemptStatus` needs a return** in `derive_next_status`; otherwise the fall-through in `handle_running_task_run` settles `Invalid` at runtime (which is exactly what `Invalid` itself hit before it had a return). The two `Failed` cases split on `last.attempt` vs `task_run.max_retries + 1`, so a failure is retried or reported, never both or neither.
- **A new terminal `TaskRunStatus` needs three edits**: the failure list in `TaskRunDispatcher::should_skip` (or dependents wait for ever), a return in `JobRunMonitor::derive_next_status` at the right rank, and a badge in `templates/routes/job_runs/job_run_id/route.html`. Exhaustive matches (`is_finished`, `is_stopped`, `Display`, `format::task_run_word`) force the rest; `JobRunStatus::ALL` feeds the dashboard's filter chips and the CLI's `--status` parser.
- **This monitor never writes `Skipped`**; only `TaskRunDispatcher` (or `CRUD::skip_job_run`) skips, and only an unstarted task run.
- **Terminal statuses set `finished_at`** — `TaskRunMonitor::update_task_run_status`, or `TaskRunDispatcher::set_to_skipped`.
