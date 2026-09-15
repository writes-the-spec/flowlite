# Task run

`TaskRunStatus` lives in [src/crud/task_run.rs](../../../../src/crud/task_run.rs). One `task_run` row per task per job run, created `Planned` by `CRUD::submit_job` — for **every** task, not just the ones without dependencies. Ordering is enforced here, at dispatch time.

| Status | Meaning |
|---|---|
| `Planned` | Written with its job run, which has yet to start. **Nothing dispatches it.** The job-run spelling of this is `Scheduled`. |
| `Waiting` | Released by `JobRunDispatcher::set_to_running`, and waiting for the task runs it depends on. |
| `Running` | Started, and owned by `TaskRunMonitor`, which decides which attempt runs next. Covers the gaps between attempts, not just the time a process is alive. |
| `Succeeded` | Its command exited 0. |
| `Failed` | Its command exited non-zero and no retries were left. |
| `TimedOut` | It ran past `task_run.timeout` and no retries were left. |
| `Aborted` | The job run was stopped after this task run started — its process killed mid-flight, or its next attempt skipped before the command began. |
| `Skipped` | It never started: a dependency didn't succeed, or the job run was stopped while it was still `Planned` or `Waiting`. |

`Skipped` also covers the ordinary dependency case, so it is not by itself a sign of a stop. There is no `Cancelled`: a stop finds a task run either not yet started (`Skipped`, written by `TaskRunDispatcher`) or already started (`Aborted`, written by `TaskRunMonitor`). **Which one it is depends on the task run's own lifecycle, not on what its last attempt says** — a task run that already burned an attempt and was waiting to retry is `Aborted`, even though the attempt that never spawned is `Skipped`.

## Dispatcher: Waiting → Running / Skipped

`TaskRunDispatcher` ([src/orchestrator/task_run_dispatcher.rs](../../../../src/orchestrator/task_run_dispatcher.rs)) polls **all** `Waiting` task runs on a signal wake-up or its one-second interval, whichever comes first. `derive_next_status` loads the row's dependencies once and returns one status from them; `handle_waiting_task_run` writes it:

**It never sees a `Planned` one, and that is the whole point of the status.** This used to poll `Queued` task runs whatever their job run's status — and since `submit_job` wrote every task run `Queued` the moment the run was submitted, a run scheduled for tonight had its root task started on the next pass, hours before its `scheduled_at`, and a run held at `max_parallel_runs` ran its tasks while its job run sat `Queued` behind the gate. The release is now a status of its own, written only by `JobRunDispatcher::set_to_running`, so "a `Waiting` task run's job run is `Running`" is an invariant rather than an accident.

1. `Skipped` — `should_skip`: the **job run was stopped**, or **any dependency finished but didn't succeed** (`Failed`, `Skipped`, `Aborted`, `TimedOut`, `Invalid`). Either way the task run can never run. `Invalid` has to be in that list: it is finished, so check 2 does not hold the dependent, and it did not succeed, so check 3 does not start it — leaving it out sent every dependent to the fall-through on every pass, turning one unreadable row into an unreadable subtree.
2. `Waiting` — `is_still_waiting`: **any dependency still unfinished** (`Planned`, `Waiting` or `Running`) → the row stays `Waiting` for the next tick, and nothing is written. **Naming it is the point:** a row held on a dependency and a row nobody decided about are otherwise the same silence.
3. `Running` — `all_dependencies_succeeded` → `set_to_running`: `Running` with `started_at = now`. **One status write and nothing else** — attempts are `TaskRunMonitor`'s, the first as much as the retries. This used to insert attempt 1 here first, so the monitor never saw a `Running` task run without one; the cost was a window between the two writes that a crash could stop inside, leaving an attempt against a `Waiting` task run. Every later pass then hit the unique index on `(task_run_id, attempt)` and errored, so the row never started and the job run above it held a parallel slot for ever — while `TaskRunAttemptDispatcher` ran the command anyway and nothing read the result.
4. `None` → `set_to_invalid`, logged as a bug. Unreachable while the three checks cover every dependency set. An error from any of the three reads is returned to `Poller` instead — settling the terminal `Invalid` on a failed query would strand every dependent below the row as well.

`should_skip`'s two halves short-circuit in order, so a stopped job run costs one query and never looks at the dependency statuses.

**The dependencies are loaded once**, in `derive_next_status`, and shared by all three checks. That is what lets check 3 be asked as its own question — "have they all succeeded?" rather than "whatever 2 declined" — without it being the same question asked twice against two snapshots. An earlier shape loaded them per outcome, which left a window where a dependency failing mid-decision produced a set that was neither all-succeeded nor still-unfinished, and the row fell through to the undecided arm on an ordinary state until the next pass. One snapshot closes that window and drops two queries per row.

Dependencies come from `task_run.depends_on` — the list copied off `task.depends_on` when the run was submitted — resolved to the task runs of the same job run by `get_dependent_task_runs`. A task with no dependencies is turned down by check 2 (`any()` over an empty list is false) and started by check 3 (`all()` over one is true).

This transition runs **at most once per task run**: a retry keeps the row `Running`, so the dependency check happens once and `started_at` means "when the task run started", covering every attempt.

`JobRunDispatcher::set_to_skipped` short-circuits the stop check for a job run stopped while still `Queued`, skipping all of its task runs in one update instead of one per pass.

## Monitor: Running → finished

`TaskRunMonitor` ([src/orchestrator/task_run_monitor.rs](../../../../src/orchestrator/task_run_monitor.rs)) polls `Running` task runs on the same wake-up-or-interval schedule and looks only at their `task_run_attempt` rows — it never touches a process. The **last** attempt (highest `attempt`, which a unique index makes unique per task run) picks the outcome. `get_or_start_task_run_attempt` returns it, starting **attempt 1** and reading it back when the task run has none — a task run the dispatcher has just set `Running` — so the decision always has an attempt to read. **Every attempt a task run ever gets is made in this monitor**, which is what keeps the unique index a concern of one file:

| Last attempt | Outcome | Task run |
|---|---|---|
| `Invalid` | returned first | `Invalid` — and **never retried**: flowlite does not know what that attempt did |
| `Succeeded` | | `Succeeded` |
| not finished | | left `Running` — the attempt is still the attempt services' |
| `Failed`, no retry left | | `Failed` |
| `Failed`, retry left | | left `Running`; `insert_next_attempt` writes the retry row before returning — `TaskRunAttemptDispatcher` holds it `Queued` until `retry_delay` has passed |
| `TimedOut` | | `TimedOut` |
| `is_stopped` — `Aborted` or `Skipped` | | `Aborted` — the task run had started, so a stop aborts it |
| anything left | `None` → `set_to_invalid` | `Invalid`, logged as a bug — unreachable while the returns above cover every attempt status. A failed retry insert returns `Err` instead, and settles nothing |

Before the decision runs at all: **no attempt row whatsoever** → attempt 1 is inserted and read back, and the decision then runs against it. Being `Queued` it is not finished, so it returns `Running` and leaves the task run there until `TaskRunAttemptDispatcher` has run it. That is the ordinary first pass of a task run, not a broken invariant — it was briefly `Invalid` instead, which put the parent's status on something no child had said.

These checks are exclusive, since the last attempt has exactly one status, so **the order here carries nothing** — `Invalid` is returned first to read like `JobRunMonitor`, where the rank is load-bearing. `TaskRunAttemptStatus::is_stopped` needs no failure ruled out first, unlike its `TaskRunStatus` namesake: `TaskRunAttemptDispatcher` skips an attempt for one reason only.

`Failed` is the only retried status. A retry inserts attempt `last.attempt + 1` and leaves the task run `Running`. Attempts count from 1, so total executions are `1 + max_retries` and `max_retries: 0` means one attempt. Both the count and the delay are read off the `task_run` row, so a run retries on the policy it was submitted with rather than on whatever the YAML says now.

Because the decision comes from the attempt rows alone, a stop landing *between* attempts isn't seen here: the monitor starts the next attempt (publishing a wake-up as it inserts the row), the attempt dispatcher skips or aborts it on the next pass, and the task run finishes from that.

## Invariants

- **`Waiting` has exactly one writer: `JobRunDispatcher::set_to_running`.** Every other path into `task_run` writes `Planned` (the two insert paths), a terminal status, or `Running`. That single door is what lets `TaskRunDispatcher` select on status alone without also asking what its job run is doing — add a second writer and the dependency is back, silently.
- **A `Planned`, `Waiting` or `Running` task run keeps its job run `Running`.** A task run that is never visited again strands its whole job run — see [job_run.md](job_run.md).
- **A `Running` task run must always have either an unfinished attempt or a finished last attempt to decide on.** An attempt row that never finishes stalls the task run, and through it the job run.
- **A new `TaskRunAttemptStatus` needs a return** in `derive_next_status`. The `Ok(None)` arm in `handle_running_task_run` catches the omission at runtime rather than the compiler catching it at build time — the task run settles `Invalid` with a log line saying it is a bug, rather than staying `Running` for ever. `Invalid` itself is what this guards against: between being added to the enum and being given a return, it reached exactly this fall-through. The two `Failed` cases are split on one comparison — `last.attempt` against `task_run.max_retries + 1` — so a failed last attempt is either retried or reported, never both and never neither.
- **A new terminal `TaskRunStatus` needs three edits**: the failure list in `TaskRunDispatcher::should_skip` (or downstream task runs wait forever), a transition in `JobRunMonitor::handle_running_job_run`, at the right rank, and a badge arm in `templates/routes/job_runs/job_run_id/route.html`. The exhaustive matches on the enum — `is_finished`, `is_stopped`, `Display`, `format::task_run_word` — force the rest, and `JobRunStatus::ALL` is what the dashboard's filter chips and the CLI's `--status` parser both read.
- **This monitor never writes `Skipped`.** It only ever visits `Running` task runs, which have started; a stop therefore aborts them. Only `TaskRunDispatcher` skips a task run, and only one that never started.
- **Terminal statuses set `finished_at`** — `TaskRunMonitor::update_task_run_status` for the ones it derives, `TaskRunDispatcher::set_to_skipped` for a skip.
