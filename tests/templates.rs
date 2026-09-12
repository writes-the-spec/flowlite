//! Templates and the assets they ask for, checked the way nothing else checks them.
//!
//! A `#[template(path = "...")]` naming a file that is not there fails the build, so that
//! direction needs no test. The two that rot silently are the other way round: a template
//! nobody renders any more compiles for ever, and an `/assets/...` URL whose file has moved
//! is a 404 in the browser and nothing at all in the build - rust_embed serves what is in
//! `assets/`, and the page asks for what the markup says.

use std::path::{Path, PathBuf};

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

fn files_under(dir: &Path, extension: &str) -> Vec<PathBuf> {

    let mut files = Vec::new();

    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();

        if path.is_dir() {
            files.extend(files_under(&path, extension));
        } else if path.extension().is_some_and(|found| found == extension) {
            files.push(path);
        }
    }

    files
}
