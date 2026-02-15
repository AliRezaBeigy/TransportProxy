//! Proxy client: listens on TCP and forwards each connection to the proxy server over a selectable transport (KCP or QUIC).
//!
//! Supports graceful shutdown on SIGINT (Ctrl+C) or SIGTERM (Unix): stops accepting and
//! waits up to --shutdown-drain-secs for in-flight relays to finish.

use anyhow::Result;
use clap::Parser;
use proxy_server::transport::{ProxyStream, Transport};
use proxy_server::{kcp_config_with, relay_bidirectional};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info, warn};

#[derive(clap::Parser)]
struct Args {
    /// Transport: kcp-tokio (KCP over UDP) or quinn (QUIC). Must match server. Default kcp-tokio.
    #[arg(long, default_value = "kcp-tokio", value_parser = clap::value_parser!(Transport))]
    transport: Transport,

    /// Local TCP address to listen on (e.g. 127.0.0.1:9000)
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,

    /// Proxy server address (e.g. 127.0.0.1:12345)
    #[arg(long, default_value = "127.0.0.1:12345")]
    proxy: SocketAddr,

    /// Seconds to wait for in-flight connections to drain on shutdown. Default 5.
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

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    info!("Proxy client listening on {}", args.listen);
    info!(
        "Proxy server at {} (transport: {:?})",
        args.proxy, args.transport
    );

    let transport = args.transport;

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received");
                break;
            }
            accept_result = listener.accept() => {
                let (tcp_stream, addr) = match accept_result {
                    Ok(pair) => pair,
                    Err(e) => {
                        error!("Accept error: {}", e);
                        continue;
                    }
                };
                let permit = match &semaphore {
                    Some(s) => match s.clone().acquire_owned().await {
                        Ok(p) => Some(p),
                        Err(_) => {
                            warn!("Connection limit semaphore closed, dropping");
                            continue;
                        }
                    },
                    None => None,
                };
                let proxy = args.proxy;
                let config = kcp_config.clone();
                info!("New TCP connection from {}", addr);

                tokio::spawn(async move {
                    let _permit = permit;
                    let proxy_stream = match transport {
                        Transport::KcpTokio => {
                            match kcp_tokio::KcpStream::connect(proxy, config).await {
                                Ok(s) => ProxyStream::KcpTokio(s),
                                Err(e) => {
                                    error!("Failed to connect to proxy for {}: {}", addr, e);
                                    return;
                                }
                            }
                        }
                        Transport::Quinn => {
                            let cfg = match proxy_server::transport::quinn_client_config_insecure() {
                                Ok(c) => c,
                                Err(e) => {
                                    error!("QUIC client config failed: {}", e);
                                    return;
                                }
                            };
                            match proxy_server::transport::quinn_connect_stream(
                                proxy,
                                "localhost",
                                cfg,
                            )
                            .await
                            {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Failed to connect to QUIC proxy for {}: {}", addr, e);
                                    return;
                                }
                            }
                        }
                        #[cfg(feature = "slipstream-picoquic")]
                        Transport::SlipstreamPicoQuic => {
                            match proxy_server::transport::slipstream_connect_stream(
                                proxy,
                                "localhost",
                                None,
                            )
                            .await
                            {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("Failed to connect to slipstream-picoquic proxy for {}: {}", addr, e);
                                    return;
                                }
                            }
                        }
                    };
                    if let Err(e) = relay_bidirectional(tcp_stream, proxy_stream).await {
                        error!("Relay error for {}: {}", addr, e);
                    }
                });
            }
        }
    }

    info!("Draining in-flight connections for {:?}...", drain_duration);
    tokio::time::sleep(drain_duration).await;
    info!("Shutdown complete");
    Ok(())
}
