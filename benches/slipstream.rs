//! Slipstream-picoquic benchmarks (optional feature): throughput, latency, concurrent.

#![cfg(feature = "slipstream-picoquic")]

use anyhow::Result;
use criterion::{black_box, Criterion, Throughput};
use proxy_server::transport::{
    create_slipstream_pem_files, ensure_slipstream_picoquic_tls_init,
    run_slipstream_picoquic_server, slipstream_connect_stream, ProxyStream,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tracing::info;

use crate::common::{
    init_bench_logging, record_bench_success, ACCEPT_TIMEOUT, IO_TIMEOUT,
    SLIPSTREAM_CONNECT_TIMEOUT, SLIPSTREAM_THROUGHPUT_ROUNDTRIPS,
};

// Each bench group uses a separate port because the C picoquic_packet_loop runs on a dedicated
// blocking thread that holds the UDP socket until the process exits — dropping the shutdown_tx
// only stops the async accept loop, not the C thread. Reusing the same port across groups causes
// the second bind to fail with PICOQUIC_ERROR_UNEXPECTED_ERROR (ret=1051).
const SLIPSTREAM_BENCH_PORT_THROUGHPUT: u16 = 12446;
const SLIPSTREAM_BENCH_PORT_LATENCY: u16 = 12447;
const SLIPSTREAM_BENCH_PORT_CONCURRENT: [u16; 3] = [12448, 12449, 12450];

/// Returns (server_addr, shutdown_tx). Caller must keep shutdown_tx alive or the server exits.
/// When cert_key_paths is Some, use those PEM paths (caller keeps files alive); required so client can trust the same cert.
/// Each bench group must pass a distinct port — the C picoquic_packet_loop thread holds the UDP
/// socket for the process lifetime so the same port cannot be reused across groups.
pub fn start_slipstream_echo_server_in_background(
    rt: &Runtime,
    port: u16,
    cert_key_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
) -> (SocketAddr, tokio::sync::broadcast::Sender<()>) {
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    rt.spawn(async move {
        let _ = run_slipstream_picoquic_server(
            addr,
            ACCEPT_TIMEOUT,
            Duration::from_secs(1),
            &Arc::new(None),
            &None,
            &mut shutdown_rx,
            cert_key_paths,
        )
        .await;
    });
    std::thread::sleep(Duration::from_millis(1200));
    eprintln!(
        "[bench] Slipstream-picoquic echo server listening on {}",
        addr
    );
    info!(addr = %addr, "Slipstream-picoquic echo server started for benchmarks");
    (addr, shutdown_tx)
}

pub async fn slipstream_echo_roundtrip(
    addr: SocketAddr,
    payload: &[u8],
    trusted_cert_path: Option<&std::path::Path>,
) -> Result<usize> {
    let out: Result<usize> = (async {
        let connect_result = tokio::time::timeout(
            SLIPSTREAM_CONNECT_TIMEOUT,
            slipstream_connect_stream(addr, "localhost", trusted_cert_path),
        )
        .await;
        let mut stream = connect_result??;
        let write_result = tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await;
        write_result??;
        let flush_result = tokio::time::timeout(IO_TIMEOUT, stream.flush()).await;
        flush_result??;
        let mut buf = vec![0u8; payload.len()];
        let read_result = tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await;
        read_result??;
        Ok(payload.len())
    })
    .await;
    out
}

/// N echo round-trips on an existing stream (no connect). Used for throughput bench with one connection per payload size.
pub async fn slipstream_echo_roundtrips_on_stream(
    stream: &mut ProxyStream,
    payload: &[u8],
    count: u32,
) -> Result<()> {
    let mut buf = vec![0u8; payload.len()];
    for _ in 0..count {
        tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;
    }
    Ok(())
}

/// One connection, N echo roundtrips on the same stream. Avoids connection storms in concurrent bench.
pub async fn slipstream_echo_roundtrips_one_connection(
    addr: SocketAddr,
    payload: &[u8],
    count: u32,
    trusted_cert_path: Option<&std::path::Path>,
) -> Result<()> {
    let mut stream = tokio::time::timeout(
        SLIPSTREAM_CONNECT_TIMEOUT,
        slipstream_connect_stream(addr, "localhost", trusted_cert_path),
    )
    .await??;
    let mut buf = vec![0u8; payload.len()];
    for _ in 0..count {
        tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;
    }
    Ok(())
}

pub fn bench_slipstream_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!(
        "[bench] === slipstream-picoquic throughput (QUIC over UDP echo, 5 payload sizes) ==="
    );
    let (cert_file, key_file) = create_slipstream_pem_files().expect("slipstream PEM files");
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();
    ensure_slipstream_picoquic_tls_init();
    let rt = Runtime::new().unwrap();
    let (addr, _server_guard) = start_slipstream_echo_server_in_background(
        &rt,
        SLIPSTREAM_BENCH_PORT_THROUGHPUT,
        Some((cert_path.clone(), key_path)),
    );
    let cert_path_ref = cert_file.path();
    let mut group = c.benchmark_group("slipstream_throughput");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    for size in [64, 256, 1024, 4096, 8192] {
        let total_bytes = (size as u64) * (SLIPSTREAM_THROUGHPUT_ROUNDTRIPS as u64);
        group.throughput(Throughput::Bytes(total_bytes));
        let name = format!("echo_{}b", size);
        eprintln!(
            "[bench]   running {} (1 connection reused, {} echo round-trips over UDP)",
            name, SLIPSTREAM_THROUGHPUT_ROUNDTRIPS
        );
        let group_id = "slipstream_throughput";
        let func_id = name.clone();
        let roundtrips = SLIPSTREAM_THROUGHPUT_ROUNDTRIPS;
        let payload = vec![0xABu8; size];
        // One connection per payload size, reused for all samples: connect on first iter, then round-trips only.
        let stream = std::cell::RefCell::new(None::<ProxyStream>);
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut opt = stream.borrow_mut();
                if opt.is_none() {
                    // Build and run connect entirely inside runtime so timeout/sleep run in reactor context.
                    *opt = Some(
                        rt.block_on(async {
                            tokio::time::timeout(
                                SLIPSTREAM_CONNECT_TIMEOUT,
                                slipstream_connect_stream(addr, "localhost", Some(cert_path_ref)),
                            )
                            .await
                            .map_err(|_| anyhow::anyhow!("connect timeout"))?
                            .map_err(|e| anyhow::anyhow!("connect: {}", e))
                        })
                        .expect("slipstream connect failed"),
                    );
                }
                let res = rt.block_on(slipstream_echo_roundtrips_on_stream(
                    opt.as_mut().unwrap(),
                    &payload,
                    roundtrips,
                ));
                let ok = res.is_ok();
                record_bench_success(group_id, &func_id, ok);
                black_box(res)
            })
        });
    }
    drop((cert_file, key_file));
    group.finish();
}

pub fn bench_slipstream_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === slipstream-picoquic latency (QUIC over UDP echo RTT 64B) ===");
    let (cert_file, key_file) = create_slipstream_pem_files().expect("slipstream PEM files");
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();
    ensure_slipstream_picoquic_tls_init();
    let rt = Runtime::new().unwrap();
    let (addr, _server_guard) = start_slipstream_echo_server_in_background(
        &rt,
        SLIPSTREAM_BENCH_PORT_LATENCY,
        Some((cert_path, key_path)),
    );
    let cert_path_ref = cert_file.path();
    let mut group = c.benchmark_group("slipstream_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    let payload = [0xABu8; 64];
    group.bench_function("echo_rtt_64b", |b| {
        b.iter(|| {
            let n = rt
                .block_on(slipstream_echo_roundtrip(
                    addr,
                    &payload,
                    Some(cert_path_ref),
                ))
                .unwrap_or(0);
            let ok = n > 0;
            record_bench_success("slipstream_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    drop((cert_file, key_file));
    group.finish();
}

pub fn bench_slipstream_concurrent(c: &mut Criterion) {
    init_bench_logging();
    eprintln!(
        "[bench] === slipstream-picoquic concurrent (5/10/20 connections × 10 msgs over UDP) ==="
    );
    let (cert_file, key_file) = create_slipstream_pem_files().expect("slipstream PEM files");
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();
    ensure_slipstream_picoquic_tls_init();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("slipstream_concurrent");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    let payload = [0xABu8; 64];
    let mut server_guard: Option<tokio::sync::broadcast::Sender<()>> = None;
    for (idx, num_conns) in [5usize, 10, 20].iter().copied().enumerate() {
        let name = format!("{}_connections_10_msgs", num_conns);
        eprintln!(
            "[bench]   running {} ({} connections in parallel, 10 echo roundtrips each over UDP)",
            name, num_conns
        );
        let group_id = "slipstream_concurrent";
        let func_id = name.clone();
        // Each case uses its own port: packet_loop holds the UDP socket until process exit,
        // so dropping the server guard does not free the port for a subsequent bind.
        drop(server_guard.take());
        let (addr, guard) = start_slipstream_echo_server_in_background(
            &rt,
            SLIPSTREAM_BENCH_PORT_CONCURRENT[idx],
            Some((cert_path.clone(), key_path.clone())),
        );
        server_guard = Some(guard);
        let cert_path_clone = cert_path.clone();
        // Stagger connection starts to avoid thundering herd (helps with 20 concurrent connections).
        let stagger_ms = if num_conns > 10 { 5 } else { 0 };
        if num_conns == 20 {
            std::thread::sleep(Duration::from_millis(500));
        }
        group.bench_function(name.as_str(), |b| {
            b.iter(|| {
                let failures = rt.block_on(async {
                    // Run num_conns connections in parallel. Stagger start to reduce server load spike.
                    let mut handles = Vec::with_capacity(num_conns);
                    for i in 0..num_conns {
                        let a = addr;
                        let cert = cert_path_clone.clone();
                        let pl = payload;
                        let delay_ms = i as u64 * stagger_ms;
                        handles.push(tokio::spawn(async move {
                            if delay_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                            }
                            slipstream_echo_roundtrips_one_connection(
                                a,
                                &pl,
                                10,
                                Some(cert.as_path()),
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!("{}", e))
                        }));
                    }
                    let mut failures = 0usize;
                    let mut first_error: Option<String> = None;
                    for h in handles {
                        match h.await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                failures += 1;
                                if first_error.is_none() {
                                    first_error = Some(e.to_string());
                                }
                            }
                            Err(e) => {
                                failures += 1;
                                if first_error.is_none() {
                                    first_error = Some(format!("join: {}", e));
                                }
                            }
                        }
                    }
                    if failures > 0 {
                        eprintln!(
                            "[bench] slipstream_concurrent: {} worker(s) failed (e.g. broken pipe)",
                            failures
                        );
                        if let Some(ref msg) = first_error {
                            eprintln!("[bench] first error: {}", msg);
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
    drop((cert_file, key_file));
    group.finish();
}
