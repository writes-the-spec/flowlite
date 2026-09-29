---
name: mcp
description: Add or modify MCP tools in src/mcp/ - flowlite as a set of tools an agent calls over JSON-RPC on stdin and stdout, served by the `flowlite mcp` command and built on rmcp. Use when adding a tool, changing a tool's arguments, result or description, tracing how a tool call reaches CRUD, bounding a tool's wait, or working out why a client sees a parse error or an empty tool list.
---

# MCP module conventions (src/mcp/)

`flowlite mcp` speaks MCP over stdin/stdout. [src/mcp/mod.rs](../../../src/mcp/mod.rs) holds `McpServer` (an `Arc<Toolkit>` and a composed `ToolRouter`); each tool is one file under `src/mcp/tools/`. Nine of the ten tools are a CLI command's `--json` branch without the shell (same filters, sort and fields); `init_data_dir` has no twin (`init` prints prose), so its `InitResult` shape is its own, in `src/shared/`.

## Adding a tool (e.g. `list_schedules`)

1. Create `src/mcp/tools/list_schedules.rs`:

```rust
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
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

async fn list_schedules_rows(toolkit: &Toolkit, args: ListSchedules) -> anyhow::Result<Vec<Schedule>> {
    let toolkit = toolkit.with_fresh_mem();
    let _memory_conn = toolkit.get_memory_conn().await?;
    let mut conn = toolkit.get_conn().await?;
    let crud = CRUD::new(Arc::new(toolkit));
    crud.init(&mut conn).await?;

    crud.select_schedules(&mut conn, &SelectSchedulesData { /* filter from args, sort, limit, offset */ }).await
}
```

2. In [src/mcp/tools/mod.rs](../../../src/mcp/tools/mod.rs) add `mod list_schedules;` and `+ Self::list_schedules_router()` in `tools_router`. That is the whole registration; a tool missing there is missing from the tool list.
3. Add an integration test to [tests/mcp_server.rs](../../../tests/mcp_server.rs), which drives the built binary as a real client.

The example mirrors nothing because there is no `schedule` command; where one exists, mirror its `--json` field for field.

## Rules

- **Nothing in `src/mcp/` prints to stdout.** It is the protocol stream: one stray `println!` becomes a client parse error that names nothing. Use stderr.
- **One tool per file**, each with its own `#[tool_router(router = <name>_router, vis = "pub(super)")]` over `impl McpServer`, summed in `tools_router`. `mod.rs`'s own block keeps `allow_empty`, as it declares no tool.
- **A tool matching a CLI command returns its `--json` shape** via `src/shared/` (`JobRunDetail`, `TaskRunAttemptLog`, `serve_status::status_json`, `LimitRow`), never a shape of its own. Wording (`status_line`, `limits_table`) stays in the CLI. Fields must agree, not whitespace: `limits --json` is one compact line, `list_limits` pretty-prints.
- **The `#[tool]` fn's `///` is the tool description, paid for on every model turn.** Only what a caller needs to choose the tool and read its result; rationale goes in a `//` above it, which ships nowhere (see [stop_job_run.rs](../../../src/mcp/tools/stop_job_run.rs)).
- **Arguments: a `#[derive(Debug, Deserialize, JsonSchema)]` struct with `#[serde(deny_unknown_fields)]`,** taken as `Parameters(args)`; each field's `///` is its schema description. An undeclared key is a typo or guess serde would silently ignore (rmcp adds none of its own). A tool with no arguments still declares an empty struct (`init_data_dir`, `get_serve_status`, `list_limits`), so a caller naming its own data directory is refused instead of quietly served the one `-D` named.
- **A call opens its own connection via `toolkit.with_fresh_mem()`,** never `self.toolkit`'s `mem`: rmcp runs calls concurrently, and a fresh `mem` lets `list_jobs` see a job file written after startup and `submit_job` seed the same inline id twice in a session. Otherwise follow the matching CLI command (`cli` skill):
  - reads config: hold `toolkit.get_memory_conn()` and run `crud.init` (`list_jobs`, `submit_job`);
  - reads only run history or disk tables: neither (`get_job_run`, `list_limits`);
  - reads no database: opens nothing (`init_data_dir`, `get_serve_status`), so it works before a database exists.
- **A failure is a tool result, not a protocol error:** `error_result(&err)` carries the anyhow chain verbatim (`{:#}`) so the model can fix its input.
- **Results go through [tools/result.rs](../../../src/mcp/tools/result.rs):** `success_json` (text as `--json` prints it, plus the same value as `structured_content`), or `job_run_result` for a run that may carry the unserved-directory warning (a warned result drops `structured_content`). Serialize the struct, not a `serde_json::Value`, whose `BTreeMap` alphabetizes fields away from the CLI's order - unless the CLI prints that same `Value` (`status_json`).
- **Waits are bounded by [src/mcp/wait.rs](../../../src/mcp/wait.rs):** `clamp_wait_seconds` (absent or `0` returns at once, above 300 clamps to 300) and `wait_for_settled_job_run`, which returns the unfinished run when the bound elapses - "still running, here is the id" is an answer, not an error. Before any wait over 0, call `ensure_data_dir_is_served`, or the tool hangs on a row nothing writes.
- **Never import from `src/cli/` or `src/router/`;** shared needs (`JobRunDetail`, `parse_job_run_status`, `stop_job_run`, `delete_job_run`, ...) live in `src/shared/`. See the `frontends` skill; `tests/frontend_boundaries.rs` enforces it.
- Design note: [docs/2026-09-11-mcp-server-design.md](../../../docs/2026-09-11-mcp-server-design.md).
