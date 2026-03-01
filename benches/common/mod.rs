//! Shared benchmark utilities: constants, logging, UDP writers, packet feeding, time.

pub mod constants;
pub mod logging;
pub mod packet_feed;
pub mod time;
pub mod udp_writers;

pub use constants::*;
pub use logging::{init_bench_logging, record_bench_success};
pub use time::current_ms;
pub use udp_writers::{ClientUdpWriter, ServerUdpWriter};
