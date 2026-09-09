mod service;

pub use crate::notifications::service::NotificationService;

pub mod channel;
pub mod email;
pub mod message;
pub mod slack;
