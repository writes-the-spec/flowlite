use serde::{Deserialize, Serialize};


/// What the web UI pages and refreshes by.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigUi {
    /// Rows per page on the run, job and schedule lists.
    pub page_size: u32,
    /// The largest `page_size` the run list accepts from its query string.
    pub max_page_size: u32,
    /// How often a page that is watching a live run asks for itself again.
    pub refresh_interval_seconds: u32,
    /// Which palette the dashboard serves.
    pub theme: AppConfigUiTheme,
}

impl Default for AppConfigUi {
    fn default() -> Self {
        Self {
            page_size: 25,
            max_page_size: 100,
            refresh_interval_seconds: 3,
            theme: AppConfigUiTheme::Auto,
        }
    }
}


/// The dashboard's palette, and by default the one thing about the page the server does not
/// decide: `Auto` hands the choice to the viewer's own `prefers-color-scheme`, which is why
/// this travels to the page as an attribute the stylesheet reads rather than as a stylesheet
/// the server picks. `Dark` and `Light` overrule the viewer, for an instance that should
/// look the same on every machine that opens it.
///
/// An unrecognised name is a startup error rather than a silent fall back - the same call
/// `schedule_defaults.timezone` makes for a zone it does not know.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AppConfigUiTheme {
    Dark,
    Light,
    Auto,
}

impl AppConfigUiTheme {

    /// What `root.html` puts in `<html data-theme="...">`, and so the selector the
    /// stylesheet's two override blocks are written against.
    pub fn as_attribute(&self) -> &'static str {
        match self {
            AppConfigUiTheme::Dark => "dark",
            AppConfigUiTheme::Light => "light",
            AppConfigUiTheme::Auto => "auto",
        }
    }

}
