//! Every path a skill links to exists.
//!
//! The skills under `.claude/skills/` are documentation that names files, and nothing
//! compiles them: a rename or a split leaves a link pointing at nothing, silently. Splitting
//! `src/crud/multistatements/misc.rs` broke six of them across four skills, and every one was
//! found by hand, commits later. This is the check that finds them at once instead.
//!
//! Only markdown links are checked, never a path named in inline code. `src/cli/commands/widget.rs`
//! and the other teaching examples are files a reader is being told to *create*; a link is the
//! only form that claims a file is already there.

use std::path::{Path, PathBuf};

#[test]
fn every_path_a_skill_links_to_exists() {

    let mut broken = Vec::new();

    for document in markdown_files(Path::new(".claude/skills")) {
        let source = std::fs::read_to_string(&document).unwrap();
        let directory = document.parent().unwrap();

        for target in link_targets(&source) {
            // A bare fragment is an anchor within the same page, and a heading that moves is
            // not what this guards. `docs/` is excluded from the repo (.git/info/exclude), so
            // a design note is absent from a fresh clone while still being right to link.
            if target.starts_with('#') || target.starts_with("http") || target.contains("docs/") {
                continue;
            }

            let path = target.split('#').next().unwrap();

            if !directory.join(path).exists() {
                broken.push(format!("{}: {}", document.display(), target));
            }
        }
    }

    broken.sort();

    assert!(
        broken.is_empty(),
        "A skill links to a path that does not exist:\n{}\n\nPoint it at where the file went, \
         or drop the link. A skill that names a file nothing can open is worse than one that \
         names none.",
        broken.join("\n"),
    );
}

/// Every `](target)` in the source. Link text never contains `](`, and no target here
/// contains `)`, so finding the pair is the whole parse.
fn link_targets(source: &str) -> Vec<&str> {
    source
        .split("](")
        .skip(1)
        .filter_map(|rest| rest.split_once(')'))
        .map(|(target, _)| target)
        .collect()
}

fn markdown_files(dir: &Path) -> Vec<PathBuf> {

    let mut files = Vec::new();

    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();

        if path.is_dir() {
            files.extend(markdown_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "md") {
            files.push(path);
        }
    }

    files
}
