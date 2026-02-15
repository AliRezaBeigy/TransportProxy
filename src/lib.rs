//! KCP proxy library: shared config and relay utilities.

pub mod transport;

use anyhow::Result;
use kcp_tokio::KcpConfig;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Default KCP config for proxy (low latency, reliable).
pub fn default_kcp_config() -> KcpConfig {
    kcp_config_with(None, None, None)
}

/// KCP config with optional overrides (use default when `None`).
/// `window` is used for both send and receive window; default 128.
/// `mtu` default 1400; `connect_timeout_secs` default 10.
pub fn kcp_config_with(
    window: Option<u32>,
    mtu: Option<u32>,
    connect_timeout_secs: Option<u64>,
) -> KcpConfig {
    let w = window.unwrap_or(128);
    let mtu = mtu.unwrap_or(1400);
    let timeout = Duration::from_secs(connect_timeout_secs.unwrap_or(10));
    KcpConfig::new()
        .fast_mode()
        .window_size(w, w)
        .mtu(mtu)
        .connect_timeout(timeout)
        .stream_mode(true)
}

/// Copy data from reader to writer until EOF or error.
pub async fn relay<R, W>(mut reader: R, mut writer: W) -> Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = [0u8; 8192];
    let mut total: u64 = 0;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n]).await?;
        writer.flush().await?;
        total += n as u64;
    }
    Ok(total)
}

/// Bidirectional relay: copy both directions concurrently using tokio's implementation.
/// On EOF from one side, the writer is shut down and the other direction continues until done.
pub async fn relay_bidirectional<A, B>(mut a: A, mut b: B) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin + Send,
    B: AsyncRead + AsyncWrite + Unpin + Send,
{
    tokio::io::copy_bidirectional(&mut a, &mut b)
        .await
        .map(|_| ())
        .map_err(Into::into)
}

/// Handle one proxy connection: forward to upstream TCP or echo back.
/// Used by both KCP/Quinn and slipstream-picoquic server paths.
pub async fn handle_connection(
    mut stream: transport::ProxyStream,
    upstream: Option<SocketAddr>,
) -> Result<()> {
    let peer = stream
        .peer_addr()
        .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());

    if let Some(up) = upstream {
        let tcp = tokio::net::TcpStream::connect(up).await?;
        tracing::info!("Connected to upstream {} for peer {}", up, peer);
        relay_bidirectional(stream, tcp).await?;
    } else {
        let mut buf = [0u8; 8192];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await?;
            stream.flush().await?;
        }
    }

    tracing::debug!("Connection from {} closed", peer);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_builds() {
        let _ = default_kcp_config();
    }
}
