# Job run

`JobRunStatus` lives in [src/crud/job_run.rs](../../../../src/crud/job_run.rs). A `job_run` row is created `Pending` by `CRUD::submit_job`, together with one `Pending` task run per task of the job.

| Status | Meaning |
|---|---|
| `Pending` | Submitted, waiting. Nothing has run; `started_at` is NULL. |
| `Running` | Started. Its task runs are being dispatched, executed and retried. |
| `Succeeded` | Every task run succeeded — also the status of a job with no tasks. |
| `Failed` | A task run failed with retries exhausted. |
| `TimedOut` | A task run exceeded `task_run.timeout` with retries exhausted. |
| `Aborted` | Stopped after it started, however far it had got — a task run killed mid-flight, or one skipped before its command began. |
| `Skipped` | Stopped before it ever started. Nothing of it ran. |

`Aborted` vs `Skipped` is the "stopped" pair, and **the row's own lifecycle decides which**, not what its task runs report: a job run with `started_at` set is `Aborted`, one that never started is `Skipped`. So `JobRunDispatcher` writes `Skipped` and never `Aborted`, `JobRunMonitor` writes `Aborted` and never `Skipped`. There is deliberately no `Cancelled`.

## Dispatcher: Pending → Running / Skipped

`JobRunDispatcher` ([src/orchestrator/job_run_dispatcher.rs](../../../../src/orchestrator/job_run_dispatcher.rs)) polls `Pending` job runs **oldest id first** — on a signal wake-up or its one-second interval, whichever comes first — and asks two questions per row, each of which owns its own guard and returns whether it transitioned:

1. `transition_to_skipped` — **stopped?** (a `job_run_stop` row exists) → the job run goes `Skipped`, and so do all of its task runs in one update. It never runs.
2. `transition_to_running` — **is its job at `max_parallel_runs`?** (`CRUD::is_job_at_max_parallel_runs`, counting that job's `Running` job runs) → if so it transitions nothing and the row simply stays `Pending`, to be reconsidered next pass. Otherwise `status = Running`, `started_at = now`.

**`transition_to_running` is the only place `max_parallel_runs` is enforced.** Nothing rejects a submission for being over the limit — not `job submit`, not a rerun, not the [scheduler](../../scheduler/SKILL.md) — so an over-limit run is created `Pending` like any other and queues here until a slot frees. Two things follow: the oldest-first sort is what makes the queue fair, and a job that takes longer than its schedule interval accumulates pending runs rather than losing them.

Only `Running` runs count against the limit. Counting `Pending` ones too would deadlock the gate, since the row being considered is itself `Pending`.

`started_at` is written exactly once, here; task run retries never touch it.

## Monitor: Running → finished

`JobRunMonitor` ([src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs)) polls `Running` job runs on the same wake-up-or-interval schedule, loads all task runs of each, and asks its transitions in order, the first whose guard matches winning:

0. `has_unfinished_task_run` — any task run `Pending` or `Running` → nothing transitions, the job run stays `Running`
1. `transition_to_aborted` — any `Aborted`
2. `transition_to_timed_out` — any `TimedOut`
3. `transition_to_failed` — any `Failed`
4. `transition_to_aborted_after_stop` — any `Skipped`, which at this rank can only be a stop → `Aborted`
5. `transition_to_succeeded` — the fallthrough, no guard

**This call order in `handle_running_job_run` is the status precedence**, and reordering the lines changes what a mixed set of task runs reports. It is not encoded anywhere else: each transition's guard only asks whether its own status is present, so `transition_to_failed` would happily fire on a set that also holds an `Aborted` if it were asked first.

The status comes **entirely from the task runs** — the monitor never reads a stop signal. A stop reaches the job run only as the task run statuses it produced, through steps 1 and 4, both of which write `Aborted`.

Step 4 does not require *all* task runs to be skipped: skips come either from a stop or from a dependency that didn't succeed, and in the second case that dependency is itself `Failed`/`TimedOut`/`Aborted` or skipped — so once steps 1–3 have not matched, a skip can only mean the run was stopped. It stays a separate transition at rank 4 rather than folding into step 1 precisely so that a real failure outranks a stop.

Every guard is an `any(...)`, so a job run with no task runs falls through to `transition_to_succeeded`.

## Invariants

- **A job run must stay `Running` until every task run is terminal.** The monitor only visits `Running` rows, so finishing one early is final: its task runs keep executing but their outcome is never read again.
- **The guards stay pure.** `has_unfinished_task_run` and `has_task_run_with_status` take `&[TaskRun]` and do no I/O, which is what the unit tests reach. The precedence between them is call order in `handle_running_job_run` and is **not** covered by a test — it has no seam a pure test can reach — so read that function before changing it.
- **This monitor never writes `Skipped`.** It only ever visits `Running` rows, and a job run that reached `Running` has started — its earlier task runs may well have executed — so a stop aborts it. `Skipped` here would claim nothing ran while the logs showed output.
- **Terminal statuses set `finished_at`**, via `update_job_run_status`.
- **A new terminal `TaskRunStatus` needs a transition here**, placed at the right rank, or it falls through to `transition_to_succeeded` and silently reports `Succeeded`. Nothing in the compiler catches the omission: unlike `TaskRunMonitor`'s exhaustive match, this chain has a fallthrough.
- **A new `JobRunStatus` needs a badge** in `templates/routes/home/job_run_table/route.html` and `templates/routes/job_runs/job_run_id/route.html`, plus an entry in the home route's `all_statuses` filter list.
