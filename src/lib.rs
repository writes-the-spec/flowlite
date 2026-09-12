//! flowlite as a library, which exists for `main.rs` and the integration tests under
//! `tests/`. The binary is the product.
//!
//! Only the modules those two actually reach are `pub`. Everything else is `pub(crate)`,
//! which is what lets rustc report an unused item inside it: a `pub mod` nothing outside
//! the crate uses is a module where dead code cannot be seen, and this file said `pub` to
//! all fifteen until the day that hid four unused `Toolkit` methods and six unused
//! `CronTrigger` ones. Widen one only when something outside the crate genuinely needs it.

pub mod app_config;
pub mod cli;
pub(crate) mod mcp;
pub(crate) mod router;
pub(crate) mod shared;
pub mod toolkit;
pub mod crud;
pub(crate) mod yaml_models;
pub(crate) mod cron_trigger;
pub(crate) mod orchestrator;
pub(crate) mod scheduler;
pub(crate) mod signals;
pub(crate) mod poller;
pub(crate) mod notifications;
pub mod serve_state;

#[cfg(test)]
pub mod test_support;
