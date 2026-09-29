---
name: frontends
description: The boundary between flowlite's three frontends - src/cli/, src/mcp/ and src/router/ - and the src/shared/ module they borrow through. None of the three imports another; whatever two of them need lives in src/shared/. Use when an import would cross from one of those directories into another, when deciding where to put a helper, a type or a formatting function the CLI and the MCP server (or the dashboard) both need, when adding a file to src/shared/, or when tests/frontend_boundaries.rs fails.
---

# Frontend boundaries (src/cli/, src/mcp/, src/router/)

Three frontends over one core (`src/crud/`); how to add to each is in the `cli`, `mcp` and `router` skills.

**The rule: none of the three imports from another (`crate::cli::`, `crate::mcp::`, `crate::router::` never appear in the other two directories). Whatever two of them need lives in `src/shared/`, which itself imports no frontend.** `tests/frontend_boundaries.rs` enforces it. Non-frontend modules may use `src/shared/` freely (e.g. [src/notifications/message.rs](../../../src/notifications/message.rs) uses `shared::format`).

## The one carve-out

A CLI command may **start** another frontend, since the binary's entrypoint must start something: [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs) imports `create_router` and `AppState`, [src/cli/commands/mcp.rs](../../../src/cli/commands/mcp.rs) imports `McpServer`. Those three lines are listed by path and exact text in `CARVE_OUTS` in [tests/frontend_boundaries.rs](../../../tests/frontend_boundaries.rs). `src/mcp/` and `src/router/` start nothing, so never import `src/cli/`.

Starting is allowed; borrowing is not. If the item would still make sense with the other frontend deleted, it is a borrow and goes in `src/shared/`. A fourth `CARVE_OUTS` line is for review to argue about, not to add quietly.

## What is in src/shared/

| File | Holds |
|---|---|
| [format.rs](../../../src/shared/format.rs) | `timestamp`, `short_timestamp`, `duration`, the `*_word` status spellers |
| [init.rs](../../../src/shared/init.rs) | `scaffold`, `ScaffoldedFile`, `InitResult` - what `init` and `init_data_dir` lay down |
| [job.rs](../../../src/shared/job.rs) | `installed_job_id` |
| [job_run.rs](../../../src/shared/job_run.rs) | `select_job_run`, `stop_job_run`, `delete_job_run`, `parse_job_run_status`, the `JobRunDetail` / `TaskRunAttemptLog` shapes |
| [limits.rs](../../../src/shared/limits.rs) | `LimitRow`, `limit_rows`, `is_full`, `waiting_note` |
| [schedule_at.rs](../../../src/shared/schedule_at.rs) | `parse_schedule_at`, `refuse_waiting_for_a_future_run` |
| [serve_status.rs](../../../src/shared/serve_status.rs) | `status_json`, `uptime_seconds` |
| [task_run_attempt.rs](../../../src/shared/task_run_attempt.rs) | `own_task_run_attempt_id` - the attempt a command running inside a task belongs to, from its environment |
| [wait.rs](../../../src/shared/wait.rs) | `wait_for_job_run`, `ensure_data_dir_is_served`, `DataDirNotServed` |

File names mirror `src/crud/` and the `entities` skill. It is not a layer every call passes through: frontends still call `CRUD` directly.

## What does NOT go in src/shared/

- **Wording.** Shared raises a typed value, never a finished sentence, because the frontends disagree on the remedy. `ensure_data_dir_is_served` raises `DataDirNotServed`; the CLI's `describe_unserved_data_dir` ([src/cli/commands/job.rs](../../../src/cli/commands/job.rs)) says "drop `--wait`", the MCP one in [src/mcp/tools/result.rs](../../../src/mcp/tools/result.rs) names no flag. Same pattern one layer down: `JobIdAlreadyInstalled` in [src/crud/multistatements/ad_hoc_job.rs](../../../src/crud/multistatements/ad_hoc_job.rs).
- **One frontend's policy.** `clamp_wait_seconds`'s 300-second ceiling stays in [src/mcp/wait.rs](../../../src/mcp/wait.rs): it is about MCP client timeouts, and `--wait` has none.
- **Anything one frontend uses.** The bar is callers in two frontends.

An MCP tool mirrors a CLI `--json` by sharing the shape, not importing it: `JobRunDetail` lets `job-run get --json` and `get_job_run` print the same fields.

## Moving something to src/shared/

1. Move it to the file its entity names, keeping its visibility (`pub(crate)` unless it was `pub`), with its unit tests. Tests of one frontend's wording or flags stay behind.
2. Point every caller at `crate::shared::<module>`. **No `pub use` re-export left behind** - it is the same import in disguise and the boundary test will not catch it.
3. A finished error sentence splits: the typed fact moves, each frontend keeps its wording.
4. Drop doc comments that justified the old arrangement.
5. `cargo test` (includes `tests/frontend_boundaries.rs`).

Design note: [docs/2026-09-12-frontend-boundaries-design.md](../../../docs/2026-09-12-frontend-boundaries-design.md).
