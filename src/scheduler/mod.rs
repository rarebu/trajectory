//! Scheduler module
//!
//! Uses simple tokio::interval loops instead of tokio-cron-scheduler
//! for better memory behavior and predictability.

pub mod simple;

// Re-export the main run function
pub use simple::run;
