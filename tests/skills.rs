//! The skills under `.claude/skills/` are well-formed: every path they link to exists, and
//! every one of them declares the name and description the harness needs to offer it.
//!
//! Nothing compiles a skill, so both kinds of rot are silent. Splitting
//! `src/crud/multistatements/misc.rs` broke six links across four skills, every one found by
//! hand, commits later. The notifications skill had no frontmatter at all, so 2400 words of
//! guidance were offered to the model as the single word "Notifications" - it would have been
//! loaded for almost nothing. Both are the kind of thing a test finds at once.
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

/// A skill is offered to the model by its `description`, and addressed by its `name`. A
/// skill missing either is one the model will not reach for, however good it is - a quieter
/// failure than a broken link, because the file is still perfectly readable by hand.
#[test]
fn every_skill_declares_a_name_matching_its_directory_and_a_description() {

    let mut problems = Vec::new();

    for entry in std::fs::read_dir(".claude/skills").unwrap() {
        let directory = entry.unwrap().path();
        let skill = directory.join("SKILL.md");

        if !skill.exists() {
            continue;
        }

        let name = directory.file_name().unwrap().to_string_lossy().to_string();
        let source = std::fs::read_to_string(&skill).unwrap();

        let Some(frontmatter) = source.strip_prefix("---\n").and_then(|rest| rest.split("\n---").next()) else {
            problems.push(format!("{name}: no YAML frontmatter"));
            continue;
        };

        match field(frontmatter, "name:") {
            Some(declared) if declared == name => {}
            Some(declared) => problems.push(format!("{name}: declares name '{declared}'")),
            None => problems.push(format!("{name}: no name")),
        }

        if field(frontmatter, "description:").is_none() {
            problems.push(format!("{name}: no description"));
        }
    }

    problems.sort();

    assert!(
        problems.is_empty(),
        "A skill is not well-formed:\n{}\n\nEvery SKILL.md opens with YAML frontmatter \
         declaring a `name` equal to its directory and a `description` saying when to use it. \
         The description is the whole of what the model sees when deciding whether to read the \
         skill at all.",
        problems.join("\n"),
    );
}

/// The value of a `key:` line in the frontmatter, if it carries one.
fn field<'a>(frontmatter: &'a str, key: &str) -> Option<&'a str> {
    frontmatter
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}
