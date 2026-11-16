use serde::{Deserialize, Serialize};


/// What the web UI pages by.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct AppConfigUi {
    /// Rows per page on the run, job and schedule lists.
    pub page_size: u32,
}

impl Default for AppConfigUi {
    fn default() -> Self {
        Self {
            page_size: 25,
        }
    }
}
