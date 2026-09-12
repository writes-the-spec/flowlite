//! The rule: `src/cli/`, `src/mcp/` and `src/router/` are three frontends over one core,
//! and none of them imports from another. Whatever two of them need lives in `src/shared/`.
//! See the `frontends` skill for what belongs there and what stays frontend-side.
//!
//! Here rather than inline because what it reads is the source tree rather than the crate,
//! so it belongs to no module in particular. `skills.rs` and `templates.rs` are here for
//! the same reason; the rest of this directory needs a real second process.

use std::path::{Path, PathBuf};

const FRONTENDS: [&str; 3] = ["cli", "mcp", "router"];

/// Starting another frontend is what the `serve` and `mcp` commands are for - the binary's
/// entrypoint has to start something. Borrowing a helper, a type or a formatting function
/// from one is the thing being refused, and that belongs in `src/shared/`.
///
/// The exemption is one-directional: `src/mcp/` and `src/router/` are not entrypoints and
/// have nothing to start, so neither appears here. A fourth line is worth arguing about in
/// review rather than adding quietly.
const CARVE_OUTS: [(&str, &str); 3] = [
    ("src/cli/commands/serve.rs", "use crate::router::app::app::create_router;"),
    ("src/cli/commands/serve.rs", "use crate::router::app::app_state::AppState;"),
    ("src/cli/commands/mcp.rs", "use crate::mcp::McpServer;"),
];

#[test]
fn no_frontend_reaches_into_another() {

    let mut violations = Vec::new();

    for frontend in FRONTENDS {
        for file in rust_files(&Path::new("src").join(frontend)) {
            let path = file.to_string_lossy().to_string();
            let source = std::fs::read_to_string(&file).unwrap();

            for (offset, line) in source.lines().enumerate() {
                let code = line.trim();

                if code.starts_with("//") {
                    continue;
                }

                let reaches_into = FRONTENDS
                    .iter()
                    .filter(|other| **other != frontend)
                    .any(|other| code.contains(&format!("crate::{}::", other)));

                if reaches_into && !CARVE_OUTS.contains(&(path.as_str(), code)) {
                    violations.push(format!("{}:{}: {}", path, offset + 1, code));
                }
            }
        }
    }

    violations.sort();

    assert!(
        violations.is_empty(),
        "A frontend reaches into another frontend:\n{}\n\nMove what they share to \
         src/shared/. A command may start another frontend, which is what CARVE_OUTS in \
         this file lists, but may not borrow a helper or a type from one.",
        violations.join("\n"),
    );
}

fn rust_files(dir: &Path) -> Vec<PathBuf> {

    let mut files = Vec::new();

    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();

        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }

    files
}
