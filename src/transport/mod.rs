//! Transport abstraction: KCP (kcp-tokio), QUIC (quinn), and optional slipstream-picoquic.

use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

mod quinn;
#[cfg(feature = "slipstream-picoquic")]
mod slipstream;

pub use quinn::{
    quinn_client_config_insecure, quinn_client_config_with_cert, quinn_connect_stream,
    quinn_server_config, QuinnBiStream,
};

#[cfg(feature = "slipstream-picoquic")]
pub use slipstream::SlipstreamPicoQuicStream;

/// Connect to a slipstream-picoquic server and open one bidirectional stream.
/// If the server uses a self-signed certificate, pass its PEM path as `trusted_cert_path`
/// so the client can verify it (e.g. for benchmarks or local dev).
#[cfg(feature = "slipstream-picoquic")]
pub async fn slipstream_connect_stream(
    server_addr: SocketAddr,
    server_name: &str,
    trusted_cert_path: Option<&std::path::Path>,
) -> anyhow::Result<ProxyStream> {
    slipstream::connect_stream(server_addr, server_name, trusted_cert_path).await
}

/// Run slipstream-picoquic server.
#[cfg(feature = "slipstream-picoquic")]
pub async fn run_slipstream_picoquic_server(
    listen: SocketAddr,
    accept_timeout: std::time::Duration,
    drain_duration: std::time::Duration,
    upstream: &std::sync::Arc<Option<SocketAddr>>,
    semaphore: &Option<std::sync::Arc<tokio::sync::Semaphore>>,
    shutdown_rx: &mut tokio::sync::broadcast::Receiver<()>,
    cert_key_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
) -> anyhow::Result<()> {
    slipstream::run_server(
        listen,
        accept_timeout,
        drain_duration,
        upstream,
        semaphore,
        shutdown_rx,
        cert_key_paths,
    )
    .await
}

/// Create temporary PEM files (cert + key) for slipstream-picoquic server and client trust.
#[cfg(feature = "slipstream-picoquic")]
pub fn create_slipstream_pem_files(
) -> anyhow::Result<(tempfile::NamedTempFile, tempfile::NamedTempFile)> {
    slipstream::create_pem_files()
}

/// Run first picoquic TLS init and a dummy create/free on the calling thread.
#[cfg(feature = "slipstream-picoquic")]
pub fn ensure_slipstream_picoquic_tls_init() {
    slipstream::ensure_tls_init();
}

/// User-selectable transport for proxy server and client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Transport {
    /// KCP over UDP (kcp-tokio). Default.
    #[default]
    KcpTokio,
    /// QUIC over UDP with TLS (quinn).
    Quinn,
    /// QUIC over UDP via slipstream-picoquic C library (optional feature).
    #[cfg(feature = "slipstream-picoquic")]
    SlipstreamPicoQuic,
}

impl std::str::FromStr for Transport {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kcp-tokio" | "kcptokio" | "kcp" => Ok(Transport::KcpTokio),
            "quinn" | "quic" => Ok(Transport::Quinn),
            #[cfg(feature = "slipstream-picoquic")]
            "slipstream-picoquic" | "slipstream" | "picoquic" => Ok(Transport::SlipstreamPicoQuic),
            _ => {
                #[cfg(feature = "slipstream-picoquic")]
                return Err(format!(
                    "unknown transport '{}'; use kcp-tokio, quinn, or slipstream-picoquic",
                    s
                ));
                #[cfg(not(feature = "slipstream-picoquic"))]
                {
                    if s.to_lowercase().contains("slipstream")
                        || s.to_lowercase().contains("picoquic")
                    {
                        return Err(format!(
                            "transport '{}' requires building with --features slipstream-picoquic; see README",
                            s
                        ));
                    }
                    Err(format!("unknown transport '{}'; use kcp-tokio or quinn", s))
                }
            }
        }
    }
}

impl Transport {
    /// List of transport names for help text.
    pub const fn available() -> &'static [&'static str] {
        #[cfg(feature = "slipstream-picoquic")]
        return &["kcp-tokio", "quinn", "slipstream-picoquic"];
        #[cfg(not(feature = "slipstream-picoquic"))]
        return &["kcp-tokio", "quinn"];
    }
}

/// A stream that can be used with relay_bidirectional.
/// Wraps either a KCP stream, a QUIC (quinn) stream, or slipstream-picoquic stream.
pub enum ProxyStream {
    KcpTokio(kcp_tokio::KcpStream),
    Quinn(QuinnBiStream),
    #[cfg(feature = "slipstream-picoquic")]
    SlipstreamPicoQuic(SlipstreamPicoQuicStream),
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_flush(cx),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl ProxyStream {
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        match self {
            ProxyStream::KcpTokio(s) => Some(*s.peer_addr()),
            ProxyStream::Quinn(_) => None,
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => s.peer_addr(),
        }
    }
}
