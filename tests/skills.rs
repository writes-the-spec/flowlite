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

mod common;
use common::files_under;

#[test]
fn every_path_a_skill_links_to_exists() {

    let mut broken = Vec::new();

    for document in files_under(Path::new(".claude/skills"), "md") {
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

/// A comment that cites a file path with a line number after it is wrong the moment
/// anything above that line moves, and says so to nobody. Eight such citations existed here; one still pointed at
/// what it meant, two had rotted before this check was written, and the rest broke when
/// code moved between files. Cite the symbol instead - `TestDb`, `JobListCmd::run` - which
/// a reader can find wherever it has moved to, and which a rename makes visibly wrong
/// rather than quietly wrong.
#[test]
fn no_comment_cites_a_line_number() {

    let mut citations = Vec::new();

    for file in files_under(Path::new("src"), "rs").into_iter().chain(files_under(Path::new("tests"), "rs")) {
        let source = std::fs::read_to_string(&file).unwrap();

        for (offset, line) in source.lines().enumerate() {
            if let Some(citation) = line_citation(line) {
                citations.push(format!("{}:{}: {}", file.display(), offset + 1, citation));
            }
        }
    }

    citations.sort();

    assert!(
        citations.is_empty(),
        "A comment cites a line number:\n{}\n\nName the item instead. A line number is \
         wrong as soon as anything above it moves, and nothing checks it.",
        citations.join("\n"),
    );
}

/// A `<path>.rs:<line>` reference, if the line carries one.
fn line_citation(line: &str) -> Option<&str> {
    let trimmed = line.trim();

    if !trimmed.starts_with("//") {
        return None;
    }

    let start = trimmed.find(".rs:")?;
    let rest = &trimmed[start + 4..];

    rest.starts_with(|c: char| c.is_ascii_digit()).then(|| {
        let word_start = trimmed[..start].rfind(char::is_whitespace).map_or(0, |i| i + 1);
        let word_end = word_start + trimmed[word_start..]
            .find(|c: char| c.is_whitespace())
            .unwrap_or(trimmed.len() - word_start);
        &trimmed[word_start..word_end]
    })
}

/// The entities skill claims to map every table's columns, and a column added to a
/// migration without a line in its reference file makes that map quietly wrong - the more
/// dangerous kind of wrong, because the map is what a reader trusts instead of the DDL.
/// `task_run_attempt.process_group_id` was missing for exactly that reason.
#[test]
fn every_column_appears_in_its_entity_reference() {

    let mut undocumented = Vec::new();

    for migration in files_under(Path::new("db"), "sql") {
        let name = migration.file_name().unwrap().to_string_lossy().to_string();

        let Some(table) = name.split_once("_create_").and_then(|(_, rest)| rest.strip_suffix("_table.sql")) else {
            continue;
        };

        let reference = PathBuf::from(".claude/skills/entities/references").join(format!("{table}.md"));

        let Ok(documented) = std::fs::read_to_string(&reference) else {
            undocumented.push(format!("{table}: no reference file at {}", reference.display()));
            continue;
        };

        for column in columns(&std::fs::read_to_string(&migration).unwrap()) {
            if !documented.contains(&column) {
                undocumented.push(format!("{table}.{column}: not in {}", reference.display()));
            }
        }
    }

    undocumented.sort();

    assert!(
        undocumented.is_empty(),
        "A column is missing from the entities map:\n{}\n\nAdd a row for it, the way the \
         db-schema skill's \"Before you finish\" says to. A schema the map does not describe \
         is worse than one nobody documented.",
        undocumented.join("\n"),
    );
}

/// Column names from a `CREATE TABLE`: a line whose first word is followed by a SQL type.
/// Constraint lines (`PRIMARY KEY`, `FOREIGN KEY`, `UNIQUE`) start with a keyword instead
/// and so are skipped without naming them.
fn columns(sql: &str) -> Vec<String> {
    const TYPES: [&str; 4] = ["TEXT", "INTEGER", "REAL", "BLOB"];

    sql.lines()
        .filter_map(|line| {
            let mut words = line.trim().split_whitespace();
            let name = words.next()?;
            let kind = words.next()?;

            let is_column = name.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                && TYPES.contains(&kind.trim_end_matches(',').to_uppercase().as_str());

            is_column.then(|| name.to_string())
        })
        .collect()
}

