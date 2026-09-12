//! Templates and the assets they ask for, checked the way nothing else checks them.
//!
//! A `#[template(path = "...")]` naming a file that is not there fails the build, so that
//! direction needs no test. The two that rot silently are the other way round: a template
//! nobody renders any more compiles for ever, and an `/assets/...` URL whose file has moved
//! is a 404 in the browser and nothing at all in the build - rust_embed serves what is in
//! `assets/`, and the page asks for what the markup says. A mistyped CSS custom property
//! and a vendored file updated without its paperwork fail the same quiet way.

use std::path::Path;

mod common;
use common::files_under;

#[test]
fn every_template_is_rendered_or_extended() {

    let rust = read_dir_to_string(Path::new("src"), "rs");
    let markup = read_dir_to_string(Path::new("templates"), "html");

    let mut orphans = Vec::new();

    for template in files_under(Path::new("templates"), "html") {
        let path = template.strip_prefix("templates").unwrap().to_string_lossy().to_string();

        // Rendered by a struct, or used as the layout another template extends.
        let rendered = rust.contains(&format!("template(path = \"{path}\""));
        let extended = markup.contains(&format!("extends \"{path}\""));

        if !rendered && !extended {
            orphans.push(path);
        }
    }

    orphans.sort();

    assert!(
        orphans.is_empty(),
        "A template is never rendered or extended:\n{}\n\nDelete it, or render it. \
         An orphaned template compiles for ever and is read by nobody.",
        orphans.join("\n"),
    );
}

#[test]
fn every_asset_a_page_asks_for_exists() {

    let mut missing = Vec::new();

    let sources = files_under(Path::new("templates"), "html")
        .into_iter()
        .chain(files_under(Path::new("assets"), "css"));

    for source in sources {
        let content = std::fs::read_to_string(&source).unwrap();

        for url in asset_urls(&content) {
            // `/assets/css/app.css` is served from `assets/css/app.css`.
            if !Path::new("assets").join(url.trim_start_matches("/assets/")).exists() {
                missing.push(format!("{}: {url}", source.display()));
            }
        }
    }

    missing.sort();
    missing.dedup();

    assert!(
        missing.is_empty(),
        "A page asks for an asset that is not there:\n{}\n\nThe binary embeds `assets/` and \
         serves exactly what is in it, so a moved file is a 404 the build never mentions.",
        missing.join("\n"),
    );
}

/// Every `/assets/...` path in the text, to the first character that cannot be in one.
fn asset_urls(content: &str) -> Vec<String> {
    content
        .match_indices("/assets/")
        .map(|(start, _)| {
            let rest = &content[start..];
            let end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || "/._-".contains(c)))
                .unwrap_or(rest.len());

            rest[..end].to_string()
        })
        .collect()
}

fn read_dir_to_string(dir: &Path, extension: &str) -> String {
    files_under(dir, extension)
        .iter()
        .map(|f| std::fs::read_to_string(f).unwrap())
        .collect()
}

/// An undefined custom property is not an error: the declaration using it is simply
/// dropped, so a mistyped `var(--accnet)` leaves an element unstyled and says nothing. The
/// ones the stylesheet does not define are set inline by the markup - `--span` on a
/// duration bar, `--at` and `--start` on the run timeline - so both halves count as defined.
#[test]
fn every_css_variable_is_defined_somewhere() {

    let css = std::fs::read_to_string("assets/css/app.css").unwrap();
    let markup = read_dir_to_string(Path::new("templates"), "html");

    let mut undefined = Vec::new();

    for used in custom_properties(&css, "var(") {
        let in_stylesheet = css.contains(&format!("{used}:"));
        let set_inline = markup.contains(&format!("{used}:"));

        if !in_stylesheet && !set_inline {
            undefined.push(used);
        }
    }

    undefined.sort();
    undefined.dedup();

    assert!(
        undefined.is_empty(),
        "A stylesheet reads a custom property nothing sets:\n{}\n\nDefine it, set it inline \
         from the markup, or fix the spelling. An undefined one drops the declaration \
         silently.",
        undefined.join("\n"),
    );
}

/// Vendoring a new version means editing three things: the file, its `SOURCE.md`, and the
/// licenses table. This is the one that is easy to forget, and the one a reader checking
/// what the binary ships trusts.
#[test]
fn every_vendored_package_matches_the_licenses_file() {

    let licenses = std::fs::read_to_string("THIRD_PARTY_LICENSES.md").unwrap();

    let mut problems = Vec::new();

    for entry in std::fs::read_dir("assets/vendors").unwrap() {
        let directory = entry.unwrap().path();
        let name = directory.file_name().unwrap().to_string_lossy().to_string();

        if !directory.join("LICENSE").exists() {
            problems.push(format!("{name}: no LICENSE file"));
        }

        let Ok(source) = std::fs::read_to_string(directory.join("SOURCE.md")) else {
            problems.push(format!("{name}: no SOURCE.md"));
            continue;
        };

        if !licenses.contains(&format!("assets/vendors/{name}/")) {
            problems.push(format!("{name}: not listed in THIRD_PARTY_LICENSES.md"));
            continue;
        }

        // The table's short form, e.g. "v25" where SOURCE.md says "v25 (variable, ...)".
        if let Some(version) = source
            .lines()
            .find(|line| line.trim_start().starts_with("| Version"))
            .and_then(|line| line.split('|').nth(2))
            .and_then(|value| value.split_whitespace().next())
        {
            if !licenses.contains(version) {
                problems.push(format!("{name}: SOURCE.md says {version}, the licenses file does not"));
            }
        }
    }

    problems.sort();

    assert!(
        problems.is_empty(),
        "A vendored package and its paperwork disagree:\n{}\n\nTHIRD_PARTY_LICENSES.md is \
         what says which versions this binary ships.",
        problems.join("\n"),
    );
}

/// Every `--name` following `prefix` in the text.
fn custom_properties(text: &str, prefix: &str) -> Vec<String> {
    text.match_indices(prefix)
        .filter_map(|(start, _)| {
            let rest = &text[start + prefix.len()..];
            rest.starts_with("--").then(|| {
                let end = rest
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '-'))
                    .unwrap_or(rest.len());
                rest[..end].to_string()
            })
        })
        .collect()
}
