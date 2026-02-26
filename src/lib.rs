//! mq-bench library crate exposing modules for reuse and testing.

pub mod crash;
pub mod logging;
pub mod metrics;
pub mod output;
pub mod payload;
pub mod rate;
pub mod roles;
pub mod time_sync;
pub mod transport;
pub mod wire;

// Optional re-exports for convenience in downstream code/tests
pub use crash::{CrashConfig, CrashInjector};
pub use metrics::stats::Stats;
pub use transport::{ConnectOptions, Engine, Transport, TransportBuilder, TransportError};
