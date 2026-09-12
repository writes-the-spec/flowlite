---
name: mcp
description: Add or modify MCP tools in src/mcp/ - flowlite as a set of tools an agent calls over JSON-RPC on stdin and stdout, served by the `flowlite mcp` command and built on rmcp. Use when adding a tool, changing a tool's arguments, result or description, tracing how a tool call reaches CRUD, bounding a tool's wait, or working out why a client sees a parse error or an empty tool list.
---

# MCP module conventions (src/mcp/)

`flowlite mcp` speaks the Model Context Protocol over stdin and stdout. [src/mcp/mod.rs](../../../src/mcp/mod.rs) holds `McpServer`, which owns an `Arc<Toolkit>` and a composed `ToolRouter`; each tool lives in its own file under `src/mcp/tools/`.

Each of the six tools today is one CLI command's `--json` branch, reached without a shell: same filters, same sort, same fields, so a person reading a run through the CLI and an agent reading it through MCP read the same thing.

## Adding a tool (e.g. `list_schedules`)

1. Create `src/mcp/tools/list_schedules.rs`:

```rust
//! `list_schedules`: the schedules declared in the data directory.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::Deserialize;

use crate::crud::schedule::{Schedule, SelectSchedulesData, SelectSchedulesDataFilter};
use crate::crud::CRUD;
use crate::mcp::McpServer;
use crate::toolkit::Toolkit;

use super::result::{error_result, success_json};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListSchedules {
    /// Only schedules whose name contains this text.
    pub name_like: Option<String>,
}

#[tool_router(router = list_schedules_router, vis = "pub(super)")]
impl McpServer {

    /// List the schedules declared in the data directory.
    #[tool]
    async fn list_schedules(&self, Parameters(args): Parameters<ListSchedules>) -> CallToolResult {
        match list_schedules_rows(&self.toolkit, args).await {
            Ok(schedules) => success_json(schedules),
            Err(err) => error_result(&err),
        }
    }
}

/// `list_schedules`'s own connection: a fresh `mem`, seeded the way `job list` seeds its
/// own, so a file written after this process started is visible without a restart.
async fn list_schedules_rows(
    toolkit: &Toolkit,
    args: ListSchedules,
) -> anyhow::Result<Vec<Schedule>> {
    let toolkit = toolkit.with_fresh_mem();
    let _memory_conn = toolkit.get_memory_conn().await?;
    let mut conn = toolkit.get_conn().await?;

    let crud = CRUD::new(Arc::new(toolkit));
    crud.init(&mut conn).await?;

    crud.select_schedules(&mut conn, &SelectSchedulesData {
        filter: SelectSchedulesDataFilter {
            name_like: args.name_like,
            ..Default::default()
        },
        sort: None,
        limit: None,
        offset: None,
    }).await
}
```

2. Register it in [src/mcp/tools/mod.rs](../../../src/mcp/tools/mod.rs) — a `mod` line, and a term in `tools_router`:

```rust
mod list_schedules;
...
Self::list_jobs_router()
    + Self::list_schedules_router()
```

That is the whole registration: a tool is added or removed by adding or removing a file and its two lines here.

3. Add an integration test to [tests/mcp_server.rs](../../../tests/mcp_server.rs), which drives the built binary as a real client would.

There is no `schedule` CLI command today, which is why the example above mirrors nothing. Where a matching command does exist, mirror its `--json` branch field for field - see the first rule below.

## Rules

- **Nothing in `src/mcp/` may print to stdout.** stdout is the protocol stream, and one stray `println!` corrupts the JSON-RPC framing into a parse error that names nothing. Anything to say goes to stderr.
- **One tool per file**, each with its own `#[tool_router(router = <name>_router, vis = "pub(super)")]` block over `impl McpServer`. `mod.rs`'s own block keeps `allow_empty` because it declares no tool of its own; `tools_router` adds the per-tool routers together.
- **A tool that matches a CLI command prints that command's `--json` shape,** through a type in `src/shared/` rather than one of its own - `JobRunDetail` and `TaskRunAttemptLog` are there for this. A tool with no matching command is fine; inventing a second shape for rows the CLI already prints is not.
- **The `///` on the `#[tool]` fn is the tool's description, and the model pays for it on every turn.** Keep it to what a caller must know to choose the tool and read its result. Rationale for how it is built belongs in a `//` comment above the `///`, which ships nowhere - see [stop_job_run.rs](../../../src/mcp/tools/stop_job_run.rs).
- **Arguments are a `#[derive(Debug, Deserialize, JsonSchema)]` struct with `#[serde(deny_unknown_fields)]`,** taken as `Parameters(args)`. A key the struct does not declare is a typo or a guess, and serde would otherwise ignore it silently; rmcp adds nothing of its own to the map, so every key one sees is the caller's. Each field's `///` is its schema description.
- **Every call opens its own connection through `toolkit.with_fresh_mem()`,** never `self.toolkit`'s own `mem`. `McpServer` holds an `Arc<Toolkit>` and nothing mutable, so rmcp runs calls concurrently. A fresh name is what lets a job file written after startup reach `list_jobs`, and what lets `submit_job` seed the same inline id twice in one session. A tool that reads config also holds a `toolkit.get_memory_conn()` binding for the call and runs `crud.init`; one that reads only run history does neither, exactly as the matching CLI command does (see the `cli` skill).
- **A failure is a tool result, not a protocol error:** `error_result(&err)` carries the anyhow chain verbatim via `{:#}`, which is the text the model needs to fix its own input. A protocol error would hide it.
- **Results go through [tools/result.rs](../../../src/mcp/tools/result.rs)** - `success_json` for a value, `job_run_result` for a run that may carry the unserved-directory warning. The JSON is the same text `--json` prints, plus the identical value as `structured_content`; serialize the struct directly rather than detouring through `serde_json::Value`, whose `BTreeMap` would alphabetize the fields and stop matching the CLI.
- **A wait is bounded by [src/mcp/wait.rs](../../../src/mcp/wait.rs),** not by a loop of the tool's own: `clamp_wait_seconds` (absent or `0` returns at once, above 300 clamps to 300) and `wait_for_settled_job_run`, which returns the run unfinished rather than erroring when the bound elapses - "still running, here is the id" is an answer. Before any wait longer than 0, call `ensure_data_dir_is_served`, or the tool hangs on a row with no writer.
- **Never import from `src/cli/` or `src/router/`.** Anything a tool and a command both need lives in `src/shared/` - `JobRunDetail`, `parse_job_run_status`, `stop_job_run` and the rest are there for exactly this. See the `frontends` skill; `tests/frontend_boundaries.rs` enforces it.
- Design note: [docs/2026-09-11-mcp-server-design.md](../../../docs/2026-09-11-mcp-server-design.md).
