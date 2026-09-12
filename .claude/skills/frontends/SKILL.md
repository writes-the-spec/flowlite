---
name: frontends
description: The boundary between flowlite's three frontends - src/cli/, src/mcp/ and src/router/ - and the src/shared/ module they borrow through. None of the three imports another; whatever two of them need lives in src/shared/. Use when an import would cross from one of those directories into another, when deciding where to put a helper, a type or a formatting function the CLI and the MCP server (or the dashboard) both need, when adding a file to src/shared/, or when tests/frontend_boundaries.rs fails.
---

# Frontend boundaries (src/cli/, src/mcp/, src/router/)

flowlite has three frontends over one core: `src/cli/` (clap subcommands), `src/mcp/` (tools over JSON-RPC on stdin and stdout) and `src/router/` (the axum dashboard). They are three ways to ask the same questions of the same two SQLite files. `src/crud/` is the data-access layer underneath all three.

**The rule: none of the three imports from another. Whatever two of them need lives in `src/shared/`.**

`tests/frontend_boundaries.rs` enforces it.

## The one carve-out

A CLI command may **construct and start** another frontend, because the binary's entrypoint has to start something:

- [src/cli/commands/serve.rs](../../../src/cli/commands/serve.rs) imports `create_router` and `AppState`.
- [src/cli/commands/mcp.rs](../../../src/cli/commands/mcp.rs) imports `McpServer`.

Both are listed by path and by line in `CARVE_OUTS` in [tests/frontend_boundaries.rs](../../../tests/frontend_boundaries.rs). The exemption is one-directional: `src/mcp/` and `src/router/` are not entrypoints and have nothing to start, so they never import `src/cli/` for any reason.

**The line is between starting a frontend and borrowing from one.** If the item would still make sense with the other frontend deleted, it is a borrow, and it belongs in `src/shared/`. Adding a fourth line to `CARVE_OUTS` is a thing to argue about in review, not to do quietly.

## What is in src/shared/

| File | Holds |
|---|---|
| [format.rs](../../../src/shared/format.rs) | `timestamp`, `short_timestamp`, `duration`, and the `*_word` functions that spell a status |
| [limits.rs](../../../src/shared/limits.rs) | `LimitRow`, `limit_rows`, `is_full` - the concurrency answer the `limits` command and the dashboard panel both render |
| [job.rs](../../../src/shared/job.rs) | `installed_job_id` |
| [job_run.rs](../../../src/shared/job_run.rs) | `select_job_run`, `stop_job_run`, `parse_job_run_status`, and the `JobRunDetail` / `TaskRunAttemptLog` serialize shapes |
| [wait.rs](../../../src/shared/wait.rs) | `wait_for_job_run`, `ensure_data_dir_is_served`, `DataDirNotServed` |

File names mirror `src/crud/` and the `entities` skill, so a job-run thing is in `job_run.rs` where a reader already looks. `src/shared/` is not a layer every call passes through - a frontend still talks to `CRUD` directly, and most of each frontend imports nothing from here.

## What does NOT go in src/shared/

**Wording.** Shared stops at the fact. A refusal raised there is a typed value, never a finished sentence, because the frontends do not agree on what the reader can do about it.

`DataDirNotServed` is the worked example. `shared::wait::ensure_data_dir_is_served` raises the type and stops:

- the CLI's `describe_unserved_data_dir` ([src/cli/commands/job.rs](../../../src/cli/commands/job.rs)) turns it into "drop `--wait`", because a person really did pass that flag;
- the MCP server's, in [src/mcp/tools/result.rs](../../../src/mcp/tools/result.rs), names no flag, because an agent never sent one.

**Policy one frontend sets.** `clamp_wait_seconds` and its 300-second ceiling live in [src/mcp/wait.rs](../../../src/mcp/wait.rs), not in shared: that bound is a statement about MCP client call timeouts, and `--wait` has no ceiling.

**Anything only one frontend uses.** Two callers in two frontends is the bar. One frontend with two call sites keeps its helper at home.

## Moving something to src/shared/

1. Move the item into the `src/shared/` file its entity names, keeping the visibility it already had - `pub(crate)` unless it was `pub`.
2. Move its unit tests with it. Tests about one frontend's wording or flags stay with that frontend.
3. Point every caller at `crate::shared::<module>`. Leave nothing behind - **not even a `pub use` re-export**, which is the same import wearing a hat and which the boundary test will not catch.
4. If the item raised a finished error sentence, split it: the typed fact moves, each frontend keeps its own wording.
5. Drop any doc comment that justified the old arrangement. In `src/shared/` having several callers is the premise, not an exception worth a paragraph.
6. `cargo test`, which includes `tests/frontend_boundaries.rs`.

## Rules

- **No frontend imports another.** `crate::cli::`, `crate::mcp::` and `crate::router::` never appear in the other two directories, except the three `CARVE_OUTS` lines.
- **Non-frontend modules may import `src/shared/` freely.** [src/notifications/message.rs](../../../src/notifications/message.rs) uses `shared::format` to spell a status; that is the module working as intended, not a violation.
- **`src/shared/` imports no frontend.** It sits under all three. A shared item that needs something from `src/router/` is misplaced, or the thing it needs is what should have moved.
- **Shared raises typed values, frontends word the sentence.** See `DataDirNotServed` above and `JobIdAlreadyInstalled` in [src/crud/multistatements/misc.rs](../../../src/crud/multistatements/misc.rs) for the same pattern one layer down.
- **An MCP tool mirrors the matching CLI command's `--json` branch by sharing its shape, not by importing it.** `JobRunDetail` is in `src/shared/`, so `job-run get --json` and the `get_job_run` tool print the same fields without either one reaching into the other. See the `mcp` skill.
- Design note: [docs/2026-09-12-frontend-boundaries-design.md](../../../docs/2026-09-12-frontend-boundaries-design.md).
