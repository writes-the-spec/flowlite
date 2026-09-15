# Job run

`JobRunStatus` lives in [src/crud/job_run.rs](../../../../src/crud/job_run.rs). A `job_run` row is created `Scheduled` by `CRUD::submit_job` or `CRUD::rerun_job`, together with one `Planned` task run per task of the job — `Planned` is the task run spelling of `Scheduled`.

| Status | Meaning |
|---|---|
| `Scheduled` | Written, but not yet due. `JobRunReleaser` is the only thing that moves a run out of this status — to `Queued` when its instant arrives, or to `Skipped` if somebody stopped it first. |
| `Queued` | Released, waiting. Nothing has run; `started_at` is NULL. |
| `Running` | Started. Its task runs are being dispatched, executed and retried. |
| `Succeeded` | Every task run succeeded — also the status of a job with no tasks. |
| `Failed` | A task run failed with retries exhausted. |
| `TimedOut` | A task run exceeded `task_run.timeout` with retries exhausted. |
| `Aborted` | Stopped after it started, however far it had got — a task run killed mid-flight, or one skipped before its command began. |
| `Skipped` | Stopped before it ever started. Nothing of it ran. |
| `Invalid` | flowlite cannot account for the row — see [Rows flowlite cannot read](../SKILL.md#rows-flowlite-cannot-read) in the parent skill. |

`Aborted` vs `Skipped` is the "stopped" pair, and **the row's own lifecycle decides which**, not what its task runs report: a job run with `started_at` set is `Aborted`, one that never started is `Skipped`. So `JobRunReleaser` and `JobRunDispatcher` write `Skipped` and never `Aborted`, `JobRunMonitor` writes `Aborted` and never `Skipped`. There is deliberately no `Cancelled`.

## Releaser: Scheduled → Queued / Skipped

`JobRunReleaser` ([src/orchestrator/job_run_releaser.rs](../../../../src/orchestrator/job_run_releaser.rs)) polls every `Scheduled` job run and settles each in two halves: `derive_next_status` reads the row's world and returns the status it ought to hold, then one `set_to_*` writes it. Deciding never writes, and each `set_to_*` trusts what came back rather than re-deriving any part of it.

1. `Skipped` — **stopped?** (a `job_run_stop` row exists) → `set_to_skipped`: the run goes `Skipped` and every task run it owns goes with it, through `CRUD::skip_job_run`. Asked first, so a run stopped in the same pass it came due is skipped rather than released to be started.
2. `Queued` — **has `scheduled_at` arrived?** → `set_to_queued`: `status = Queued`, which is what makes `JobRunDispatcher` pick it up. `started_at` stays NULL: nothing has started, and the dispatcher writes it when something does.
3. `Scheduled` — **neither?** → nothing is written. The status comes back unchanged and the match arm answers it with `Ok(())`, so leaving the row alone is a named outcome rather than an unhandled branch: a run that is simply not due yet is this service's common case, not the symptom of a missing rung.
4. **any other status** → `set_to_invalid`, with a log line saying it is a bug. Unreachable, `is_stopped` and `is_due` covering every case between them, so it exists for the day a status is added and this match is not. A *failed read* is not this case: it goes back to `Poller` untouched.

The skip half exists so that stopping a run does not have to wait for its due time. It used to be done by releasing such a run to `Queued` early and leaving the skip to `JobRunDispatcher`, which worked but put a run that will never run into the status that means its moment has come, for as long as the dispatcher's next pass took. Both services now write the same pair of updates through `CRUD::skip_job_run`, and they cannot race for a row: `Scheduled` is this service's, `Queued` is the dispatcher's.

It is the one service that genuinely depends on the poll interval rather than a signal wake-up: nothing publishes when a future instant simply arrives, so the timer is what notices. This is also the reason the [Scheduler](../../scheduler/SKILL.md) is a separate service from the orchestrator's dispatchers and monitors — it decides *which* runs ought to exist, `JobRunReleaser` decides *when* one of them is due.

A `Scheduled` run is never taken back out from under this service. The scheduler only ever inserts, so a future-dated run whose schedule has since changed its mind — a lowered `submit_ahead`, an edited cron, a disabled or deleted schedule — still arrives here and is still released at its instant. Two things stop one running: a [`job_run_stop`](../../entities/references/job_run_stop.md) row, settled by step 1 above, and `flowlite job-run delete`, which tombstones a `Scheduled` run as `Deleted` before this service ever selects it — and, unlike the stop, frees the occurrence for the scheduler to write again.

## Dispatcher: Queued → Running / Skipped

`JobRunDispatcher` ([src/orchestrator/job_run_dispatcher.rs](../../../../src/orchestrator/job_run_dispatcher.rs)) polls `Queued` job runs **oldest id first** — on a signal wake-up or its one-second interval, whichever comes first — and splits the same way, `derive_next_status` returning one status and one `set_to_*` writing it:

1. `Skipped` — **stopped?** (a `job_run_stop` row exists) → `set_to_skipped`: the job run goes `Skipped`, and so do all of its task runs in one update. It never runs. Returned early rather than gathered with the check below it, so a stopped run's job is never asked about a slot it will not take.
2. `Queued` — **is its job at `max_parallel_runs`?** (`CRUD::is_job_at_max_parallel_runs`, counting that job's `Running` job runs) → nothing is written and the row is reconsidered next pass. **Naming that outcome is the point:** a run left alone on purpose and one left alone because nobody handled it look identical from the outside.
3. `Running` — otherwise → `set_to_running`: `status = Running`, `started_at = now`, and every `Planned` task run of the run moves to `Waiting` in one update. **This is the only door into `Waiting`**, so a run still held at 2 has no task run `TaskRunDispatcher` will start. The task runs are written first: a crash between the two updates then leaves the job run `Queued`, which the next pass settles again, where the other order would leave it `Running` over task runs nothing releases.
4. **any other status** → `set_to_invalid`, logged as a bug. Unreachable while those two checks cover every case. A failed read is returned, not settled.

Outcome 4 is why 2 is named at all. Without it, a row held deliberately behind its limit and a row nobody decided about are the same silence — it just sits there `Queued`. Naming the deliberate case makes the accidental one an error the `Poller` logs with the row id, instead of a run that never moves and never explains why.

**The limit is asked once per row, not twice**: an earlier shape asked "can I start it?" first, so the held case had to re-count the job's running runs to claim the rows it turned down. Deriving the status in one pass over the checks removes that.

The two halves are named `derive_next_status` and `set_to_*` rather than one `transition_to_*` per outcome because one outcome deliberately writes no status, and because a decision that only reads can be re-read, logged and tested without touching the table. Every one of the six services is built this way.

**That `max_parallel_runs` check is the only place the limit is enforced.** Nothing rejects a submission for being over the limit — not `job submit`, not a rerun, not the [scheduler](../../scheduler/SKILL.md) — so an over-limit run is released to `Queued` like any other, once it is due, and queues here until a slot frees. Two things follow: the oldest-first sort is what makes the queue fair, and a job that takes longer than its schedule interval accumulates queued runs rather than losing them.

Only `Running` runs count against the limit. Counting `Queued` ones too would deadlock the gate, since the row being considered is itself `Queued`.

The gate only became real when step 3 took over releasing the task runs. While `submit_job` wrote them ready to dispatch, a run held here had its task runs started anyway — the limit held a status column and nothing else, and `max_running_attempts` was the only cap actually bounding the work.

`started_at` is written exactly once, here; task run retries never touch it.

## Monitor: Running → finished

`JobRunMonitor` ([src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs)) polls `Running` job runs on the same wake-up-or-interval schedule, loads all task runs of each, and hands both to `derive_next_status` — which, unlike the two above, is a pure function of the task runs' statuses and reads nothing further. Its early returns, in order:

1. **not every task run finished** → `Running`, and nothing is written
2. any `Invalid` → `Invalid`
3. every task run `Succeeded` → `Succeeded`
4. any `Failed` → `Failed`
5. any `TimedOut` → `TimedOut`
6. any task run `is_stopped`, meaning `Aborted` or `Skipped`, the two ways task runs report a stop → `Aborted`
7. none of them → `None`, which `set_to_invalid` settles the row on. Unreachable while 1–6 cover every status. It is a value, not an `Err`, because this decision reads nothing that can fail.

**Step 1 is asked first and is the whole of what holds an unfinished job run open.** Every step below it may assume the work is over, which is why none of them re-asks. Finishing is irreversible — this monitor only ever visits `Running` rows — so dropping that one check would report the first failed task run as the run's outcome while its siblings were still executing. `a_failure_does_not_finish_a_job_run_whose_work_is_still_going` is the test that holds it.

**Order decides precedence among the verdicts.** An unknown outranks every named one, so `Invalid` comes first: naming the failure of a run that is partly unexplained presents an explained result. A real failure outranks a stop, so `Aborted` is last of the finished verdicts — a job run with one aborted and one failed task run reports the failure, the part worth acting on. Between the two failures, failed outranks timed out. `Succeeded` needs *every* task run succeeded, so it is exclusive with all of them and could sit anywhere.

Step 6 folds both stop signals into one verdict, and does not require *all* task runs to be skipped. A skip means a stop or a dependency that didn't succeed, and in the second case that dependency is itself `Failed`/`TimedOut`/`Aborted` or skipped — so once 4 and 5 have not matched, a skip can only mean the run was stopped. Its `Aborted` half needs no such argument: a task run is only aborted by a stop.

The narrow case it exists for is a job run stopped when **nothing was executing** — between two tasks, or before the first one starts. Its task runs end up `Succeeded` + `Skipped`, with nothing failed and nothing aborted, so every other verdict declines. Without it that job run falls to step 7 and settles `Invalid` instead of `Aborted`.

The status comes **entirely from the task runs** — the monitor never reads a stop signal. A stop reaches the job run only as the task run statuses it produced, through step 6.

Every check is an `any(...)`/`all(...)`, and `all` over an empty list is true, so a job run with no task runs passes step 1 and settles `Succeeded` at step 3.

## Invariants

- **A job run must stay `Running` until every task run is terminal.** The monitor only visits `Running` rows, so finishing one early is final: its task runs keep executing but their outcome is never read again.
- **Which statuses count as finished, and which report a stop, live on the enum.** `TaskRunStatus::is_finished` and `is_stopped` ([src/crud/task_run.rs](../../../../src/crud/task_run.rs)) both match exhaustively, so a new status has to declare its side of each or stop compiling. `is_stopped` is only truthful *after* the failure outcomes: a dependency that didn't succeed skips its dependents too, so a skip means a stop only once a failure has been ruled out.
- **The precedence among the verdicts is covered by tests, and is the whole of what their order carries.** `derive_next_status` is a pure function of the task run statuses, so the tests in [src/orchestrator/job_run_monitor.rs](../../../../src/orchestrator/job_run_monitor.rs) hand it a list of statuses through `settled_job_run_status` and read the verdict back: reordering the failure returns fails them rather than silently changing what a stopped or mixed-outcome run reports. Add a case there when you add a verdict.
- **This monitor never writes `Skipped`.** It only ever visits `Running` rows, and a job run that reached `Running` has started — its earlier task runs may well have executed — so a stop aborts it. `Skipped` here would claim nothing ran while the logs showed output.
- **Terminal statuses set `finished_at`**, via `update_job_run_status`, which then publishes a signal and nothing more. Telling anybody is not this service's business: the run's [`job_run_notification`](../../entities/references/job_run_notification.md) rows were written when it was submitted, and `NotificationService` reads the status this wrote to decide what to do with them.
- **A new terminal `TaskRunStatus` needs a return here**, placed at the right rank. Nothing in the compiler catches the omission. The final `None` catches it at runtime: the run settles `Invalid` with a log line saying it is a bug, rather than staying `Running` for ever — and the `Succeeded` check needs *every* task run succeeded, so an unclaimed status cannot slip out as `Succeeded` either.
- **A new `JobRunStatus` needs a badge** in `templates/routes/home/job_run_table/route.html` and `templates/routes/job_runs/job_run_id/route.html`, plus an entry in the home route's `all_statuses` filter list.
