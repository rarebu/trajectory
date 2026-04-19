//! Process-wide tracing/logging bootstrap.
//!
//! Honors `RUST_LOG` for runtime-tunable filtering; falls back to `info` so
//! the daemon is usable without explicit configuration.

use tracing_subscriber::{fmt, EnvFilter};

pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
