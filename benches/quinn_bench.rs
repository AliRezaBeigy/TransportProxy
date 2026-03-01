//! Quinn (QUIC/TLS) benchmarks: throughput, latency, and concurrent connections.

use anyhow::Result;
use criterion::{black_box, Criterion, Throughput};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;

use crate::common::{init_bench_logging, record_bench_success, ACCEPT_TIMEOUT, IO_TIMEOUT};

const QUINN_ALPN: &[u8] = b"proxy-echo";
const QUINN_READ_LIMIT: usize = 64 * 1024;

static QUINN_RUSTLS_INIT: Once = Once::new();
fn init_quinn_rustls() {
    QUINN_RUSTLS_INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("rustls ring provider");
    });
}

pub fn build_quinn_server_config() -> Result<(
    quinn::ServerConfig,
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    init_quinn_rustls();
    let key_pair = rcgen::KeyPair::generate(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| anyhow::anyhow!("rcgen key: {}", e))?;
    let key_der_bytes = key_pair.serialize_der();
    let mut params = rcgen::CertificateParams::default();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    params.subject_alt_names = vec![
        rcgen::SanType::DnsName("localhost".to_string()),
        rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
    ];
    params.key_pair = Some(key_pair);
    let cert = rcgen::Certificate::from_params(params)
        .map_err(|e| anyhow::anyhow!("rcgen cert: {}", e))?;
    let cert_der = rustls::pki_types::CertificateDer::from(
        cert.serialize_der()
            .map_err(|e| anyhow::anyhow!("cert serialize: {}", e))?,
    );
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(key_der_bytes)
        .map_err(|e| anyhow::anyhow!("key der: {}", e))?;
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der.clone_key())?;
    server_crypto.alpn_protocols = vec![QUINN_ALPN.to_vec()];
    let quic_server = quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server));
    let transport = Arc::get_mut(&mut server_config.transport).unwrap();
    transport.max_concurrent_uni_streams(0_u8.into());
    Ok((server_config, cert_der, key_der))
}

pub fn build_quinn_client_config(
    cert_der: &rustls::pki_types::CertificateDer<'static>,
) -> Result<quinn::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der.clone())?;
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![QUINN_ALPN.to_vec()];
    Ok(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
    )))
}

pub fn start_quinn_echo_server_in_background_with_config(
    rt: &Runtime,
    server_config: &quinn::ServerConfig,
) -> Result<Arc<RwLock<SocketAddr>>> {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let config = server_config.clone();
    let (current_addr, endpoint, local_addr) = rt.block_on(async move {
        let endpoint = quinn::Endpoint::server(config, addr)?;
        let local_addr = endpoint.local_addr()?;
        let current_addr = Arc::new(RwLock::new(local_addr));
        Ok::<_, anyhow::Error>((Arc::clone(&current_addr), endpoint, local_addr))
    })?;
    eprintln!("[bench] Quinn echo server listening on {}", local_addr);

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let _current_addr_for_task = Arc::clone(&current_addr);
    rt.spawn(async move {
        let endpoint = endpoint;
        while r.load(Ordering::Relaxed) {
            match tokio::time::timeout(ACCEPT_TIMEOUT, endpoint.accept()).await {
                Ok(Some(incoming)) => {
                    let conn = match incoming.await {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    tokio::spawn(async move {
                        loop {
                            let (mut send, mut recv) = match conn.accept_bi().await {
                                Ok(s) => s,
                                Err(quinn::ConnectionError::ApplicationClosed(_)) => break,
                                Err(_) => break,
                            };
                            let data = match recv.read_to_end(QUINN_READ_LIMIT).await {
                                Ok(d) => d,
                                Err(_) => break,
                            };
                            let _ = send.write_all(&data).await;
                            let _ = send.finish();
                        }
                    });
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }
    });
    Ok(current_addr)
}

/// Reuses a pre-created Endpoint so socket creation is not in the hot path.
async fn quinn_echo_roundtrip(
    current_addr: &Arc<RwLock<SocketAddr>>,
    payload: &[u8],
    endpoint: &quinn::Endpoint,
) -> Result<usize> {
    let addr = *current_addr.read().await;
    let connecting = endpoint.connect(addr, "localhost")?;
    let conn = tokio::time::timeout(IO_TIMEOUT, connecting).await??;
    let (mut send, mut recv) = conn.open_bi().await?;
    tokio::time::timeout(IO_TIMEOUT, send.write_all(payload)).await??;
    send.finish()?;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, recv.read_exact(&mut buf)).await??;
    conn.close(0u32.into(), b"done");
    Ok(payload.len())
}

/// One echo roundtrip on an already-established quinn::Connection (no reconnect/TLS).
/// Opens a new bi-directional stream per roundtrip — standard QUIC multiplexing.
pub async fn quinn_connection_roundtrip(conn: &quinn::Connection, payload: &[u8]) -> Result<usize> {
    let (mut send, mut recv) = conn.open_bi().await?;
    tokio::time::timeout(IO_TIMEOUT, send.write_all(payload)).await??;
    send.finish()?;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, recv.read_exact(&mut buf)).await??;
    Ok(payload.len())
}

pub fn bench_quinn_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === quinn throughput (QUIC echo, 4 payload sizes) ===");
    info!("starting benchmark group: quinn_throughput");

    let rt = Runtime::new().unwrap();
    let (server_config, cert_der, _) = build_quinn_server_config().unwrap();
    let current_addr =
        start_quinn_echo_server_in_background_with_config(&rt, &server_config).unwrap();
    let client_config = build_quinn_client_config(&cert_der).unwrap();
    // One shared endpoint: avoids per-iteration socket creation so the hot path
    // only measures connect + KCP/QUIC roundtrip, matching kcp_tokio's setup.
    // quinn::Endpoint::client binds a tokio UdpSocket, so it must run inside
    // an active runtime context.
    let _rt_guard = rt.enter();
    let mut client_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(client_config);
    let client_endpoint = Arc::new(client_endpoint);

    let mut group = c.benchmark_group("quinn_throughput");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    for size in [64, 256, 1024, 4096, 8192] {
        group.throughput(Throughput::Bytes(size as u64));
        let name = format!("echo_{}b", size);
        eprintln!(
            "[bench]   running {} (1 echo round-trip, connection reused per payload size)",
            name
        );
        let client_endpoint = Arc::clone(&client_endpoint);
        let func_id = name.clone();
        let payload = vec![0xABu8; size];
        let conn_cell: RefCell<Option<quinn::Connection>> = RefCell::new(None);
        group.bench_function(name, |b| {
            b.iter(|| {
                let n = rt.block_on(async {
                    let mut conn = conn_cell.borrow_mut().take();
                    if conn.is_none() {
                        let addr = *current_addr.read().await;
                        let connecting = match client_endpoint.connect(addr, "localhost") {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("[bench] warning: quinn connect failed: {e}");
                                return 0;
                            }
                        };
                        match tokio::time::timeout(IO_TIMEOUT, connecting).await {
                            Ok(Ok(c)) => {
                                conn = Some(c);
                            }
                            Ok(Err(e)) => {
                                eprintln!("[bench] warning: quinn handshake failed: {e}");
                                return 0;
                            }
                            Err(_) => {
                                eprintln!("[bench] warning: quinn connect timed out");
                                return 0;
                            }
                        }
                    }
                    let result = quinn_connection_roundtrip(conn.as_ref().unwrap(), &payload).await;
                    match result {
                        Ok(n) => {
                            *conn_cell.borrow_mut() = conn;
                            n
                        }
                        Err(e) => {
                            eprintln!("[bench] warning: quinn throughput roundtrip failed: {e}");
                            0
                        }
                    }
                });
                let ok = n > 0;
                record_bench_success("quinn_throughput", &func_id, ok);
                black_box(n);
            })
        });
    }
    group.finish();
}

pub fn bench_quinn_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === quinn latency (echo RTT 64B) ===");
    info!("starting benchmark group: quinn_latency");

    let rt = Runtime::new().unwrap();
    let (server_config, cert_der, _) = build_quinn_server_config().unwrap();
    let current_addr =
        start_quinn_echo_server_in_background_with_config(&rt, &server_config).unwrap();
    let client_config = build_quinn_client_config(&cert_der).unwrap();
    let _rt_guard = rt.enter();
    let mut client_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(client_config);
    let client_endpoint = Arc::new(client_endpoint);

    let mut group = c.benchmark_group("quinn_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    eprintln!("[bench]   running echo_rtt_64b (connect + 1 echo)");
    group.bench_function("echo_rtt_64b", |b| {
        b.iter(|| {
            let n = rt.block_on(async {
                match quinn_echo_roundtrip(&current_addr, &[0xABu8; 64], &client_endpoint).await {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("[bench] warning: quinn latency roundtrip failed: {e}");
                        0
                    }
                }
            });
            let ok = n > 0;
            record_bench_success("quinn_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    group.finish();
}

pub fn bench_quinn_concurrent(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === quinn concurrent (5/10/20 connections × 10 msgs) ===");
    info!("starting benchmark group: quinn_concurrent");

    let rt = Runtime::new().unwrap();
    let (server_config, cert_der, _) = build_quinn_server_config().unwrap();
    let current_addr =
        start_quinn_echo_server_in_background_with_config(&rt, &server_config).unwrap();
    let client_config = build_quinn_client_config(&cert_der).unwrap();
    // One shared endpoint for all concurrent tasks: one UDP socket, N QUIC connections.
    // This reflects QUIC's design and avoids per-task socket creation overhead.
    let _rt_guard = rt.enter();
    let mut client_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(client_config);
    let client_endpoint = Arc::new(client_endpoint);

    let mut group = c.benchmark_group("quinn_concurrent");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    for num_conns in [5, 10, 20] {
        let name = format!("{}_connections_10_msgs", num_conns);
        eprintln!(
            "[bench]   running {} (parallel connect + 10 echo each)",
            name
        );
        let client_endpoint = Arc::clone(&client_endpoint);
        let group_id = "quinn_concurrent";
        let func_id = name.clone();
        group.bench_function(name, |b| {
            b.iter(|| {
                let failures = rt.block_on(async {
                    let mut handles = Vec::with_capacity(num_conns);
                    for _ in 0..num_conns {
                        let current_addr = Arc::clone(&current_addr);
                        let ep = Arc::clone(&client_endpoint);
                        handles.push(tokio::spawn(async move {
                            let a = *current_addr.read().await;
                            let connecting = ep.connect(a, "localhost")?;
                            let conn = tokio::time::timeout(IO_TIMEOUT, connecting).await??;
                            for _ in 0..10 {
                                let (mut send, mut recv) = conn.open_bi().await?;
                                tokio::time::timeout(IO_TIMEOUT, send.write_all(b"ping")).await??;
                                send.finish()?;
                                let mut buf = [0u8; 4];
                                tokio::time::timeout(IO_TIMEOUT, recv.read_exact(&mut buf))
                                    .await??;
                            }
                            Ok::<(), anyhow::Error>(())
                        }));
                    }
                    let mut failures = 0usize;
                    for h in handles {
                        match h.await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                failures += 1;
                                eprintln!("[bench] warning: quinn concurrent worker failed: {e}");
                            }
                            Err(e) => {
                                failures += 1;
                                eprintln!(
                                    "[bench] warning: quinn concurrent worker join failed: {e}"
                                );
                            }
                        }
                    }
                    failures
                });
                let ok = failures == 0;
                record_bench_success(group_id, &func_id, ok);
                black_box(failures);
            })
        });
    }
    group.finish();
}
