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
}

impl Default for AppConfigUi {
    fn default() -> Self {
        Self {
            page_size: 25,
            max_page_size: 100,
            refresh_interval_seconds: 3,
        }
    }
}
