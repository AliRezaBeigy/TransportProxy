//! Proxy server: listens on a selectable transport (KCP or QUIC) and forwards to upstream TCP.
//!
//! On Windows, UDP error 10054 (connection reset) can cause kcp-tokio's listener
//! to exit; we recover by using a timeout on accept() and rebinding when needed.
//!
//! Supports graceful shutdown: on SIGINT (Ctrl+C) or SIGTERM (Unix), stops accepting
//! new connections and waits up to --shutdown-drain-secs for in-flight connections to finish.

use anyhow::Result;
use clap::Parser;
use proxy_server::kcp_config_with;
use proxy_server::transport::{ProxyStream, Transport};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info, warn};

#[derive(clap::Parser)]
struct Args {
    /// Transport: kcp-tokio (KCP over UDP) or quinn (QUIC over UDP). Default kcp-tokio.
    /// Transport: kcp-tokio, quinn, or slipstream-picoquic (if built with --features slipstream-picoquic).
    #[arg(long, default_value = "kcp-tokio", value_parser = clap::value_parser!(Transport))]
    transport: Transport,

    /// Address to listen on (e.g. 0.0.0.0:12345)
    #[arg(long, default_value = "0.0.0.0:12345")]
    listen: SocketAddr,

    /// Upstream TCP address to forward to (e.g. 127.0.0.1:8080). If omitted, runs in echo mode.
    #[arg(long)]
    upstream: Option<SocketAddr>,

    /// Seconds to wait for a new connection before rebinding (recovery after Windows UDP 10054). Default 15.
    #[arg(long, default_value = "15")]
    accept_timeout_secs: u64,

    /// Seconds to wait for in-flight connections to drain on shutdown (SIGINT/SIGTERM). Default 5.
    #[arg(long, default_value = "5")]
    shutdown_drain_secs: u64,

    /// Maximum concurrent connections (0 = unlimited). Default 10000.
    #[arg(long, default_value = "10000")]
    max_connections: usize,

    /// KCP window size (send and receive). Default 128. (Only for kcp-tokio.)
    #[arg(long)]
    kcp_window: Option<u32>,

    /// KCP MTU. Default 1400. (Only for kcp-tokio.)
    #[arg(long)]
    kcp_mtu: Option<u32>,

    /// KCP connect timeout in seconds. Default 10. (Only for kcp-tokio.)
    #[arg(long)]
    kcp_connect_timeout_secs: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let args = Args::parse();
    let upstream: Arc<Option<SocketAddr>> = Arc::new(args.upstream);
    let accept_timeout = Duration::from_secs(args.accept_timeout_secs);
    let drain_duration = Duration::from_secs(args.shutdown_drain_secs);
    let semaphore =
        (args.max_connections > 0).then(|| Arc::new(Semaphore::new(args.max_connections)));
    let kcp_config = kcp_config_with(args.kcp_window, args.kcp_mtu, args.kcp_connect_timeout_secs);

    let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);
    let tx1 = shutdown_tx.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx1.send(());
    });
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let tx2 = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(mut sig) = signal(SignalKind::terminate()) {
                let _ = sig.recv().await;
                let _ = tx2.send(());
            }
        });
    }

    match args.transport {
        Transport::KcpTokio => {
            run_kcp_tokio_server(
                args.listen,
                args.accept_timeout_secs,
                accept_timeout,
                drain_duration,
                &upstream,
                &semaphore,
                &kcp_config,
                &mut shutdown_rx,
            )
            .await?;
        }
        Transport::Quinn => {
            run_quinn_server(
                args.listen,
                accept_timeout,
                drain_duration,
                &upstream,
                &semaphore,
                &mut shutdown_rx,
            )
            .await?;
        }
        #[cfg(feature = "slipstream-picoquic")]
        Transport::SlipstreamPicoQuic => {
            proxy_server::transport::run_slipstream_picoquic_server(
                args.listen,
                accept_timeout,
                drain_duration,
                &upstream,
                &semaphore,
                &mut shutdown_rx,
                None,
            )
            .await?;
        }
    }

    info!("Draining in-flight connections for {:?}...", drain_duration);
    tokio::time::sleep(drain_duration).await;
    info!("Shutdown complete");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_kcp_tokio_server(
    listen: SocketAddr,
    accept_timeout_secs: u64,
    accept_timeout: Duration,
    _drain_duration: Duration,
    upstream: &Arc<Option<SocketAddr>>,
    semaphore: &Option<Arc<Semaphore>>,
    kcp_config: &kcp_tokio::KcpConfig,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<()> {
    loop {
        let mut listener = match kcp_tokio::KcpListener::bind(listen, kcp_config.clone()).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind KCP listener: {}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        info!(
            "KCP proxy server listening on {} (accept_timeout={}s for 10054 recovery)",
            listener.local_addr(),
            accept_timeout_secs
        );
        if let Some(u) = upstream.as_ref() {
            info!("Forwarding to upstream TCP {}", u);
        } else {
            info!("Echo mode: no upstream configured");
        }
        if let Some(s) = semaphore {
            info!("Max concurrent connections: {}", s.available_permits());
        } else {
            info!("Max concurrent connections: unlimited");
        }

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping accept loop");
                    return Ok(());
                }
                result = tokio::time::timeout(accept_timeout, listener.accept()) => {
                    match result {
                        Ok(Ok((kcp_stream, peer))) => {
                            let permit = match semaphore {
                                Some(s) => match s.clone().acquire_owned().await {
                                    Ok(p) => Some(p),
                                    Err(_) => {
                                        warn!("Connection limit semaphore closed, dropping");
                                        continue;
                                    }
                                },
                                None => None,
                            };
                            let upstream = Arc::clone(upstream);
                            tokio::spawn(async move {
                                let _permit = permit;
                                let stream = ProxyStream::KcpTokio(kcp_stream);
                                if let Err(e) = proxy_server::handle_connection(stream, *upstream).await {
                                    error!("Connection from {} failed: {}", peer, e);
                                }
                            });
                        }
                        Ok(Err(e)) => {
                            error!("Accept error: {}", e);
                            break;
                        }
                        Err(_) => {
                            warn!(
                                "Accept timed out after {}s (listener likely died from Windows UDP 10054), rebinding...",
                                accept_timeout_secs
                            );
                            break;
                        }
                    }
                }
            }
        }
        warn!("Rebinding in 2s...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_quinn_server(
    listen: SocketAddr,
    accept_timeout: Duration,
    _drain_duration: Duration,
    upstream: &Arc<Option<SocketAddr>>,
    semaphore: &Option<Arc<Semaphore>>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<()> {
    let (server_config, _cert_der, _key_der) = proxy_server::transport::quinn_server_config()?;
    let endpoint = quinn::Endpoint::server(server_config, listen)?;
    let local_addr = endpoint.local_addr()?;
    info!("QUIC proxy server listening on {}", local_addr);
    if let Some(u) = upstream.as_ref() {
        info!("Forwarding to upstream TCP {}", u);
    } else {
        info!("Echo mode: no upstream configured");
    }

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received");
                break;
            }
            incoming = tokio::time::timeout(accept_timeout, endpoint.accept()) => {
                match incoming {
                    Ok(Some(incoming)) => {
                        let permit = match semaphore {
                            Some(s) => match s.clone().acquire_owned().await {
                                Ok(p) => Some(p),
                                Err(_) => {
                                    warn!("Connection limit semaphore closed, dropping");
                                    continue;
                                }
                            },
                            None => None,
                        };
                        let upstream = Arc::clone(upstream);
                        tokio::spawn(async move {
                            let _permit = permit;
                            let conn = match incoming.await {
                                Ok(c) => c,
                                Err(e) => {
                                    error!("QUIC connection failed: {}", e);
                                    return;
                                }
                            };
                            let (send, recv) = match conn.accept_bi().await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("QUIC accept_bi failed: {}", e);
                                    return;
                                }
                            };
                            let stream = ProxyStream::Quinn(proxy_server::transport::QuinnBiStream::new(recv, send));
                            let peer = conn.remote_address();
                            if let Err(e) = proxy_server::handle_connection(stream, *upstream).await {
                                error!("Connection from {} failed: {}", peer, e);
                            }
                        });
                    }
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
        }
    }
    Ok(())
}
