//! Shared constants for all benchmark modules.

use std::time::Duration;

pub const KCP_OVERHEAD: usize = 24;
#[cfg(feature = "ys-kcp")]
pub const KCP_OVERHEAD_YS: usize = 28; // ys-kcp has token field
#[cfg(feature = "ys-kcp")]
pub const YS_KCP_SLEEP_US: u64 = 200; // bounded wait per iteration; matches kcp_deepseek/kcprs

pub const UDP_READ_TIMEOUT: Duration = Duration::from_millis(50);
pub const KCP_UDP_MAX_ITER: usize = 2000;

pub const ACCEPT_TIMEOUT: Duration = Duration::from_secs(1);
pub const IO_TIMEOUT: Duration = Duration::from_millis(800);
pub const REBIND_DELAY: Duration = Duration::from_millis(200);

/// Slipstream-picoquic QUIC+TLS handshake can exceed 800ms on some systems; use longer timeout for connect only.
#[cfg(feature = "slipstream-picoquic")]
pub const SLIPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// One echo round-trip per throughput iteration, matching all other implementations.
#[cfg(feature = "slipstream-picoquic")]
pub const SLIPSTREAM_THROUGHPUT_ROUNDTRIPS: u32 = 1;
