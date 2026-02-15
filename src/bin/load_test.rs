//! Load test: spawns multiple clients over selectable transport (KCP or QUIC), measures throughput and latency.

use anyhow::Result;
use clap::Parser;
use proxy_server::default_kcp_config;
use proxy_server::transport::{ProxyStream, Transport};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(clap::Parser)]
struct Args {
    /// Transport: kcp-tokio or quinn. Must match server. Default kcp-tokio.
    #[arg(long, default_value = "kcp-tokio", value_parser = clap::value_parser!(Transport))]
    transport: Transport,

    /// Server address (echo or proxy in echo mode)
    #[arg(long, default_value = "127.0.0.1:12345")]
    server: String,

    /// Number of concurrent connections
    #[arg(long, default_value = "10")]
    connections: usize,

    /// Messages per connection
    #[arg(long, default_value = "100")]
    messages: usize,

    /// Message size in bytes
    #[arg(long, default_value = "512")]
    message_size: usize,

    /// Test duration in seconds (overrides messages if set)
    #[arg(long)]
    duration_secs: Option<u64>,
}

static TOTAL_SENT: AtomicU64 = AtomicU64::new(0);
static TOTAL_RECV: AtomicU64 = AtomicU64::new(0);
static TOTAL_LATENCY_MS: AtomicU64 = AtomicU64::new(0);
static LATENCY_COUNT: AtomicU64 = AtomicU64::new(0);

async fn run_client(
    addr: &str,
    transport: Transport,
    client_id: usize,
    max_messages: Option<usize>,
    deadline: Option<Instant>,
    message_size: usize,
) -> Result<()> {
    let addr: std::net::SocketAddr = addr.parse()?;
    let mut stream = match transport {
        Transport::KcpTokio => {
            let config = default_kcp_config();
            ProxyStream::KcpTokio(kcp_tokio::KcpStream::connect(addr, config).await?)
        }
        Transport::Quinn => {
            let cfg = proxy_server::transport::quinn_client_config_insecure()?;
            proxy_server::transport::quinn_connect_stream(addr, "localhost", cfg).await?
        }
        #[cfg(feature = "slipstream-picoquic")]
        Transport::SlipstreamPicoQuic => {
            proxy_server::transport::slipstream_connect_stream(addr, "localhost", None).await?
        }
    };

    let payload: Vec<u8> = (0..message_size).map(|i| (i % 256) as u8).collect();
    let frame_len = message_size + 16;
    let mut buf = vec![0u8; frame_len];
    let mut sent_count = 0usize;

    loop {
        if let Some(limit) = max_messages {
            if sent_count >= limit {
                break;
            }
        }
        if let Some(until) = deadline {
            if Instant::now() >= until {
                break;
            }
        }

        let sent_at = Instant::now();
        let msg = format!("{:08x}{:08x}", client_id, sent_count);
        let prefix = msg.as_bytes();
        stream.write_all(prefix).await?;
        stream.write_all(&payload).await?;
        stream.flush().await?;
        TOTAL_SENT.fetch_add(1, Ordering::Relaxed);

        stream.read_exact(&mut buf).await?;
        let rtt_ms = sent_at.elapsed().as_millis() as u64;
        TOTAL_LATENCY_MS.fetch_add(rtt_ms, Ordering::Relaxed);
        LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);
        TOTAL_RECV.fetch_add(1, Ordering::Relaxed);
        sent_count += 1;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let args = Args::parse();

    let start = Instant::now();
    let deadline = args
        .duration_secs
        .map(|secs| start + Duration::from_secs(secs));
    let mut handles = Vec::with_capacity(args.connections);

    for id in 0..args.connections {
        let addr = args.server.clone();
        let transport = args.transport;
        let max_messages = if args.duration_secs.is_some() {
            None
        } else {
            Some(args.messages)
        };
        let message_size = args.message_size;
        handles.push(tokio::spawn(async move {
            run_client(&addr, transport, id, max_messages, deadline, message_size).await
        }));
    }

    let mut failures = 0usize;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("Load test client error: {}", e);
                failures += 1;
            }
            Err(e) => {
                eprintln!("Load test task join error: {}", e);
                failures += 1;
            }
        }
    }

    let elapsed = start.elapsed();
    let sent = TOTAL_SENT.load(Ordering::Relaxed);
    let recv = TOTAL_RECV.load(Ordering::Relaxed);
    let lat_sum = TOTAL_LATENCY_MS.load(Ordering::Relaxed);
    let lat_n = LATENCY_COUNT.load(Ordering::Relaxed);

    let secs = elapsed.as_secs_f64();
    println!("=== Load test results ===");
    println!("Duration: {:.2}s", secs);
    println!("Messages sent: {}, received: {}", sent, recv);
    println!("Throughput: {:.1} msg/s", sent as f64 / secs);
    if lat_n > 0 {
        println!("Latency avg: {:.2} ms", lat_sum as f64 / lat_n as f64);
    }
    if failures > 0 {
        println!("Failed clients: {}", failures);
    }
    println!("=========================");

    if failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}
