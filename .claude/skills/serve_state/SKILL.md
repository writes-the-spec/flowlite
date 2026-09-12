---
name: serve_state
description: The serve lock and state file (src/serve_state.rs) - how flowlite answers "is this data directory being served", why only one serve may hold one directory, and what `.flowlite/serve.lock` and `serve.json` each mean. Use when working on the serve or status command, when a wait or a warning depends on whether a directory is served, when a second serve is refused or wrongly allowed, when a killed server still reads as up, or when adding anything that asks about a running server.
---

# Serve lock and state (src/serve_state.rs)

A data directory answers for itself. `.flowlite/` inside it holds `serve.lock` and `serve.json`, so a directory that is moved or copied stays self-describing and no central record can disagree with it.

| | What it is | What it means |
|---|---|---|
| `serve.lock` | an `flock` held by the running server | **the** answer to "is this served" |
| `serve.json` | pid, address, port, started_at, version | *who* is serving it, readable only once the lock says somebody is |

`ServeStatus` is the union of the two: `Down` (nobody holds the lock), `Starting` (lock held, no readable state yet — the window between taking the lock and binding the listener), `Up(ServeState)`.

## The rule everything here follows

**The lock answers, never the file.** A process killed with `SIGKILL` leaves `serve.json` behind but cannot keep an `flock`, so a reader that believed the file would report a server that is not there. `status` therefore *takes* the lock to ask — success means nobody held it, so the directory is `Down` — and reads the file only after the lock has proved somebody holds it.

`read_state` is private for that reason. Everything asks through `status`.

## Traps

- **A `ServeLock` must be bound to a name.** The lock lives in the file descriptor, so the value has to outlive the server: `let _lock = ServeLock::acquire(dir)?;` keeps it, and `let _ = ServeLock::acquire(dir)?;` drops it on the spot and releases the lock immediately.
- **`status` must not write.** It opens the lock read-only and never creates it — asking whether a directory is served must not modify it, and read-only also lets a supervisor with read access, or a read-only mount, ask at all. `flock(LOCK_EX)` works fine on a read-only descriptor.
- **`status` releases what it took, by dropping at the end of the function.** Nothing may be added between taking the lock and that drop.
- **`write_state` renames a temp file into place** rather than truncating, so a concurrent `status` never catches a half-written file and calls a running server `Starting`.
- **Winning the lock removes a stale `serve.json`.** That is not shutdown cleanup — there deliberately is none. A crash leaves the file for the next acquirer to remove, and in the meantime the lock is free so nothing reads the file as truth.
- **This file is Unix-only**, through `AsRawFd` and `libc::flock`, and is not `cfg`-gated.

## Who asks, and why

| Caller | Asks because |
|---|---|
| [serve.rs](../../../src/cli/commands/serve.rs) | one server per directory; a second is refused naming the pid and URL that holds it |
| [status.rs](../../../src/cli/commands/status.rs) | `flowlite status` is this question |
| [shared/wait.rs](../../../src/shared/wait.rs) | a wait against an unserved directory would poll a row with no writer, for ever — see the [frontends skill](../frontends/SKILL.md) for how each frontend words that refusal |
| [mcp/tools/result.rs](../../../src/mcp/tools/result.rs) | a submitted run comes back `pending`, and against an unserved directory that status will never change, so the result carries a warning |

Two things follow for anything new that asks. A *check* belongs before the work, once — `ensure_data_dir_is_served` reads it once before a wait rather than on every pass, so a wait may span a deliberate restart of `serve`. And `Starting` counts as served: that server holds the lock and will reach the row.
