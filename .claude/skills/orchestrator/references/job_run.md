# Job run

`JobRunStatus` lives in [src/crud/job_run.rs](../../../../src/crud/job_run.rs). A `job_run` row is created `Submitted` by `CRUD::submit_job` or `CRUD::rerun_job`, together with one `Planned` task run per task of the job — `Planned` is the task run spelling of `Submitted`.

| Status | Meaning |
|---|---|
| `Submitted` | Written, but not yet due. `JobRunReleaser` is the only thing that moves a run out of this status — to `Queued` when its instant arrives, or to `Skipped` if somebody stopped it first. |
| `Queued` | Released, waiting. Nothing has run; `started_at` is NULL. |
| `Running` | Started. Its task runs are being dispatched, executed and retried. |
| `Succeeded` | Every task run succeeded — also the status of a job with no tasks. |
| `Failed` | A task run failed with retries exhausted. |
| `TimedOut` | A task run exceeded `task_run.timeout` with retries exhausted. |
| `Aborted` | Stopped after it started, however far it had got — a task run killed mid-flight, or one skipped before its command began. |
| `Skipped` | Stopped before it ever started. Nothing of it ran. |
| `Invalid` | flowlite cannot account for the row — see [Rows flowlite cannot read](../SKILL.md#rows-flowlite-cannot-read) in the parent skill. |

`Aborted` vs `Skipped` is the "stopped" pair, and **the row's own lifecycle decides which**, not what its task runs report: a job run with `started_at` set is `Aborted`, one that never started is `Skipped`. So `JobRunReleaser` and `JobRunDispatcher` write `Skipped` and never `Aborted`, `JobRunMonitor` writes `Aborted` and never `Skipped`. There is deliberately no `Cancelled`.

## Releaser: Submitted → Queued / Skipped

`JobRunReleaser` ([src/orchestrator/job_run_releaser.rs](../../../../src/orchestrator/job_run_releaser.rs)) polls every `Submitted` job run and settles each as exactly one outcome:

1. `settle_as_skipped` — **stopped?** (a `job_run_stop` row exists) → the run goes `Skipped` and every task run it owns goes with it, through `CRUD::skip_job_run`. Asked first, so a run stopped in the same pass it came due is skipped rather than released to be started.
2. `settle_as_queued` — **has `scheduled_at` arrived?** → `status = Queued`, which is what makes `JobRunDispatcher` pick it up. `started_at` stays NULL: nothing has started, and the dispatcher writes it when something does.
3. `settle_as_submitted` — otherwise → left `Submitted`, writing nothing. **It exists to say so.** Unlike five of the other chains this one ends there rather than in a `settle_unclaimed` that writes `Invalid`: a run that is simply not due yet is this service's common case, not the symptom of a missing rung.

The skip half exists so that stopping a run does not have to wait for its due time. It used to be done by releasing such a run to `Queued` early and leaving the skip to `JobRunDispatcher`, which worked but put a run that will never run into the status that means its moment has come, for as long as the dispatcher's next pass took. Both services now write the same pair of updates through `CRUD::skip_job_run`, and they cannot race for a row: `Submitted` is this service's, `Queued` is the dispatcher's.

It is the one service that genuinely depends on the poll interval rather than a signal wake-up: nothing publishes when a future instant simply arrives, so the timer is what notices. This is also the reason the [Scheduler](../../scheduler/SKILL.md) is a separate service from the orchestrator's dispatchers and monitors — it decides *which* runs ought to exist, `JobRunReleaser` decides *when* one of them is due.

A `Submitted` run is never taken back out from under this service. The scheduler only ever inserts, so a future-dated run whose schedule has since changed its mind — a lowered `submit_ahead`, an edited cron, a disabled or deleted schedule — still arrives here and is still released at its instant. The only way one does not run is a [`job_run_stop`](../../entities/references/job_run_stop.md) row, settled by step 1 above.

## Dispatcher: Queued → Running / Skipped

`JobRunDispatcher` ([src/orchestrator/job_run_dispatcher.rs](../../../../src/orchestrator/job_run_dispatcher.rs)) polls `Queued` job runs **oldest id first** — on a signal wake-up or its one-second interval, whichever comes first — and settles each row as exactly one outcome, each owning its own guard and returning whether it is what happened:

1. `settle_as_skipped` — **stopped?** (a `job_run_stop` row exists) → the job run goes `Skipped`, and so do all of its task runs in one update. It never runs.
2. `settle_as_queued` — **is its job at `max_parallel_runs`?** (`CRUD::is_job_at_max_parallel_runs`, counting that job's `Running` job runs) → the row stays `Queued`, to be reconsidered next pass. **It writes nothing, and exists to say so.**
3. `settle_as_running` — otherwise → `status = Running`, `started_at = now`, and every `Planned` task run of the run moves to `Waiting` in one update. **This is the only door into `Waiting`**, so a run still held at step 2 has no task run `TaskRunDispatcher` will start. The task runs are written first: a crash between the two updates then leaves the job run `Queued`, which the next pass settles again, where the other order would leave it `Running` over task runs nothing releases.
4. Past all three → `settle_unclaimed` → `Invalid`. Unreachable while step 3 claims unconditionally; see [SKILL.md](../SKILL.md#the-settle-chain-and-why-its-order-matters).

Step 4 is the point of step 2. A queued job run left alone on purpose and one left alone because nobody handled it look identical from the outside — the row just sits there, indistinguishable from a job legitimately queued behind its limit. Naming the deliberate case makes the accidental one an error the `Poller` logs with the row id, instead of a run that never moves and never explains why. It is unreachable today: step 3 starts whatever step 2 declined, unconditionally. Give it a guard of its own without adding an outcome and step 4 settling the row `Invalid` is what tells you.

**Step 2 has to come before step 3 for that reason**, and it is why the limit is asked once per row rather than twice: the earlier order asked step 3 first, so step 2 had to re-count the job's running runs to claim the rows it turned down. All three dispatchers read skipped, held, running.

They are named `settle_as_*` rather than `transition_to_*` because one of them deliberately writes no status, and calling a no-op a transition would be a lie. Every one of the six services now settles a row this way; the dispatchers use `settle_as_*` and the monitors `settle_for_*`.

**`settle_as_queued` is the only place `max_parallel_runs` is enforced.** Nothing rejects a submission for being over the limit — not `job submit`, not a rerun, not the [scheduler](../../scheduler/SKILL.md) — so an over-limit run is released to `Queued` like any other, once it is due, and queues here until a slot frees. Two things follow: the oldest-first sort is what makes the queue fair, and a job that takes longer than its schedule interval accumulates queued runs rather than losing them.

Only `Running` runs count against the limit. Counting `Queued` ones too would deadlock the gate, since the row being considered is itself `Queued`.

The gate only became real when step 3 took over releasing the task runs. While `submit_job` wrote them ready to dispatch, a run held here had its task runs started anyway — the limit held a status column and nothing else, and `max_running_attempts` was the only cap actually bounding the work.

`started_at` is written exactly once, here; task run retries never touch it.

## Monitor: Running → finished

`JobRunMonitor` ([src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs)) polls `Running` job runs on the same wake-up-or-interval schedule, loads all task runs of each, and settles each row as exactly one outcome:

1. `settle_for_invalid` — any `Invalid`, once they have all finished → the whole run is `Invalid`
2. `settle_for_succeeded` — every task run `Succeeded`
3. `settle_for_failed` — any `Failed`
4. `settle_for_timed_out` — any `TimedOut`
5. `settle_for_aborted` — any task run `is_stopped`, meaning `Aborted` or `Skipped`, the two ways task runs report a stop
6. `settle_for_running` — any task run not finished → writes nothing
7. Past all six → `settle_unclaimed` → `Invalid`. Unreachable while step 6 claims unconditionally.

**Steps 2–4 each ask for every task run having finished** as well as for their own status, so none of them can finish a job run whose work is still going. With step 5 asked last, that guard is the only thing holding such a job run open — and finishing is irreversible, since this monitor only ever visits `Running` rows.

**Order decides precedence.** A real failure outranks a stop, so step 4 is the last of the finished outcomes: a job run with one aborted and one failed task run reports the failure, which is the part worth acting on. Between the two failures, failed outranks timed out.

Step 1 comes first because an unknown outranks every named verdict: naming the failure of a run that is partly unexplained presents an explained result. It re-asks `all_finished` like steps 3–5, so it decides which *finished* verdict wins rather than finishing the run early. Step 2 is exclusive with everything (it needs *every* task run succeeded) and could sit anywhere. Step 6 comes last because it guards nothing of its own, the ladder every monitor reads in — see [SKILL.md](../SKILL.md).

Step 4 folds both stop signals into one outcome, and does not require *all* task runs to be skipped. A skip means a stop or a dependency that didn't succeed, and in the second case that dependency is itself `Failed`/`TimedOut`/`Aborted` or skipped — so once steps 2–3 have not matched, a skip can only mean the run was stopped. Its `Aborted` half needs no such argument: a task run is only aborted by a stop.

The narrow case step 4 exists for is a job run stopped when **nothing was executing** — between two tasks, or before the first one starts. Its task runs end up `Succeeded` + `Skipped`, with nothing failed and nothing aborted, so every other outcome declines. Without this step that job run reaches `settle_unclaimed` and settles `Invalid` instead of `Aborted`.

The status comes **entirely from the task runs** — the monitor never reads a stop signal. A stop reaches the job run only as the task run statuses it produced, through step 4.

Every guard is an `any(...)`/`all(...)`, so a job run with no task runs settles at step 1 immediately.

## Invariants

- **A job run must stay `Running` until every task run is terminal.** The monitor only visits `Running` rows, so finishing one early is final: its task runs keep executing but their outcome is never read again.
- **Which statuses count as finished, and which report a stop, live on the enum.** `TaskRunStatus::is_finished` and `is_stopped` ([src/crud/task_run.rs](../../../../src/crud/task_run.rs)) both match exhaustively, so a new status has to declare its side of each or stop compiling. `is_stopped` is only truthful *after* the failure outcomes: a dependency that didn't succeed skips its dependents too, so a skip means a stop only once a failure has been ruled out.
- **The precedence among steps 2–4 is covered by tests, and is the whole of what the call order carries here.** Steps 1 and 5 are position-independent — step 1 because it is exclusive with everything, step 5 because the three above it re-ask `all_finished` themselves, so it claims the same rows wherever it sits below step 1. Every guard is inlined in its own outcome, so there is no pure function left to reach; the tests in [src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs) instead insert task runs, run `handle` and read the settled status back, so reordering steps 2–4 fails two of them rather than silently changing what a stopped or mixed-outcome run reports. Add a case there when you add an outcome. Those `all_finished` guards are now the only thing holding an unfinished job run open, which is what makes them testable: dropping one from a single failure outcome fails `a_failure_does_not_finish_a_job_run_whose_work_is_still_going`, where it passed while step 5 was asked first.
- **This monitor never writes `Skipped`.** It only ever visits `Running` rows, and a job run that reached `Running` has started — its earlier task runs may well have executed — so a stop aborts it. `Skipped` here would claim nothing ran while the logs showed output.
- **Terminal statuses set `finished_at`**, via `update_job_run_status`, which also queues the run's failure notification in the same transaction when the status is `Failed` or `TimedOut` and the run names anyone — see [`job_run_notification`](../../entities/references/job_run_notification.md).
- **A new terminal `TaskRunStatus` needs an outcome here**, placed at the right rank. Nothing in the compiler catches the omission. `settle_unclaimed` catches it at runtime: the run settles `Invalid` with a log line saying it is a bug, rather than staying `Running` for ever — and `settle_for_succeeded` needs *every* task run succeeded, so an unclaimed status cannot slip out as `Succeeded` either.
- **A new `JobRunStatus` needs a badge** in `templates/routes/home/job_run_table/route.html` and `templates/routes/job_runs/job_run_id/route.html`, plus an entry in the home route's `all_statuses` filter list.
