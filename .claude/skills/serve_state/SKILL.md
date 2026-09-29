---
name: serve_state
description: The serve lock and state file (src/serve_state.rs) - how flowlite answers "is this data directory being served", why only one serve may hold one directory, and what `.flowlite/serve.lock` and `serve.json` each mean. Use when working on the serve or status command, when a wait or a warning depends on whether a directory is served, when a second serve is refused or wrongly allowed, when a killed server still reads as up, or when adding anything that asks about a running server.
---

# Serve lock and state (src/serve_state.rs)

The answer lives in `.flowlite/` inside the data directory, so a moved or copied directory still describes itself and there is no central record to disagree with it.

| File | What it is | What it means |
|---|---|---|
| `serve.lock` | an `flock` held by the running server | **the** answer to "is this served" |
| `serve.json` | pid, address, port, started_at, version | *who* serves it. Read only after the lock shows somebody holds it |

`ServeStatus`: `Down` (lock not held), `Starting` (lock held but no readable state yet, i.e. between taking the lock and binding the listener), `Up(ServeState)`.

## The lock answers, never the file

A process killed with `SIGKILL` leaves `serve.json` behind but can't keep an `flock`. So `status` *takes* the lock to ask: if it gets it, nobody held it and the directory is `Down`. It reads the file only after the lock shows it is held. `read_state` is private so that everything has to go through `status`.

## Traps

- **Bind a `ServeLock` to a name.** The lock lives in the file descriptor. `let _lock = ServeLock::acquire(dir)?;` keeps it; `let _ = ...` drops it and releases the lock at once.
- **`status` must not write.** It opens the lock read-only and never creates it, so a supervisor with read access or a read-only mount can still ask. `flock(LOCK_EX)` works on a read-only descriptor.
- **`status` releases the lock by dropping it at the end of the function.** Add nothing between taking the lock and that drop.
- **`write_state` renames a temp file into place** rather than truncating, so a concurrent `status` never reads a half-written file as `Starting`.
- **Winning the lock removes a stale `serve.json`.** There is deliberately no shutdown cleanup. A crash leaves the file for the next acquirer to remove, and until then the lock is free, so nothing trusts the file.
- **Unix-only** (`AsRawFd`, `libc::flock`), not `cfg`-gated.

## Who asks, and why

| Caller | Why |
|---|---|
| [serve.rs](../../../src/cli/commands/serve.rs) | one server per directory; a second is refused, naming the holder's pid and URL |
| [status.rs](../../../src/cli/commands/status.rs) | `flowlite status` asks exactly this |
| [mcp/tools/get_serve_status.rs](../../../src/mcp/tools/get_serve_status.rs) | the MCP equivalent of `flowlite status`; both print `status_json` from [shared/serve_status.rs](../../../src/shared/serve_status.rs) |
| [shared/wait.rs](../../../src/shared/wait.rs) | a wait on an unserved directory would poll a row nothing writes, forever. See the [frontends skill](../frontends/SKILL.md) for how each frontend words the refusal |
| [mcp/tools/result.rs](../../../src/mcp/tools/result.rs) | a submitted run comes back `scheduled` and, unserved, never reaches `queued`, so the result carries a warning |

For anything new that asks:
- **Check once, before the work.** `ensure_data_dir_is_served` checks once before a wait, not on every pass, so a wait can survive a deliberate restart of `serve`.
- **`Starting` counts as served.** That server holds the lock and will reach the row.
