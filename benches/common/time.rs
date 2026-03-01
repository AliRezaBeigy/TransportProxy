//! Time utilities for benchmarks.

use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_ms() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32
}
