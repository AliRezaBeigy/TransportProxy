//! Packet feeding helpers for sync KCP implementations.

use crate::common::udp_writers::LoopbackWriter;
use anyhow::Result;

use crate::common::constants::KCP_OVERHEAD;
#[cfg(feature = "ys-kcp")]
use crate::common::constants::KCP_OVERHEAD_YS;

/// Feed all complete KCP packets from `buf` into `kcp.input()`. Remaining bytes stay in buf.
pub fn feed_packets_to_kcp(
    buf: &mut Vec<u8>,
    kcp: &mut kcp_deepseek::Kcp<LoopbackWriter>,
) -> Result<()> {
    while buf.len() >= KCP_OVERHEAD {
        let payload_len = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
        let packet_len = KCP_OVERHEAD + payload_len;
        if buf.len() < packet_len {
            break;
        }
        let packet: Vec<u8> = buf.drain(..packet_len).collect();
        kcp.input(&packet)?;
    }
    Ok(())
}

/// Feed complete KCP packets (24-byte header, len at 20) into kcprs.
pub fn feed_packets_to_kcprs(buf: &mut Vec<u8>, kcp: &mut kcprs::Kcp) -> Result<()> {
    while buf.len() >= KCP_OVERHEAD {
        let payload_len = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
        let packet_len = KCP_OVERHEAD + payload_len;
        if buf.len() < packet_len {
            break;
        }
        let packet: Vec<u8> = buf.drain(..packet_len).collect();
        kcp.input(&packet)?;
    }
    Ok(())
}

#[cfg(feature = "ys-kcp")]
/// Feed complete ys-kcp packets (28-byte header, len at 24). (ys-kcp crate exposes lib as "kcp".)
pub fn feed_packets_to_ys_kcp<O>(buf: &mut Vec<u8>, kcp: &mut kcp::Kcp<O>) -> Result<()> {
    while buf.len() >= KCP_OVERHEAD_YS {
        let payload_len = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]) as usize;
        let packet_len = KCP_OVERHEAD_YS + payload_len;
        if buf.len() < packet_len {
            break;
        }
        let packet: Vec<u8> = buf.drain(..packet_len).collect();
        kcp.input(&packet)?;
    }
    Ok(())
}
