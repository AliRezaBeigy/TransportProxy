//! kcp-tokio benchmarks: throughput, latency, and concurrent connections.

use anyhow::Result;
use criterion::{black_box, Criterion, Throughput};
use proxy_server::default_kcp_config;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;

use crate::common::{
    init_bench_logging, record_bench_success, ACCEPT_TIMEOUT, IO_TIMEOUT, REBIND_DELAY,
};

pub fn start_echo_server_in_background(rt: &Runtime) -> Arc<RwLock<std::net::SocketAddr>> {
    let config = default_kcp_config();
    let listener = rt
        .block_on(kcp_tokio::KcpListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            config,
        ))
        .unwrap();
    let addr = *listener.local_addr();
    let current_addr = Arc::new(RwLock::new(addr));
    eprintln!("[bench] KCP echo server listening on {}", addr);
    info!(addr = %addr, "KCP echo server started for benchmarks");

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let current_addr_for_task = Arc::clone(&current_addr);
    rt.spawn(async move {
        let mut listener = listener;
        let mut timeout_streak = 0u32;
        while r.load(Ordering::Relaxed) {
            match tokio::time::timeout(ACCEPT_TIMEOUT, listener.accept()).await {
                Ok(Ok((mut stream, _))) => {
                    timeout_streak = 0;
                    tokio::spawn(async move {
                        let mut buf = [0u8; 65536];
                        while let Ok(Ok(n)) =
                            tokio::time::timeout(IO_TIMEOUT, stream.read(&mut buf)).await
                        {
                            if n == 0 {
                                break;
                            }
                            if tokio::time::timeout(IO_TIMEOUT, stream.write_all(&buf[..n]))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            if tokio::time::timeout(IO_TIMEOUT, stream.flush()).await.is_err() {
                                break;
                            }
                        }
                    });
                }
                Ok(Err(e)) => {
                    timeout_streak = 0;
                    eprintln!("[bench] listener accept failed: {e}; rebinding {}", addr);
                    info!(error = %e, addr = %addr, "benchmark listener accept failed; rebinding");
                    match kcp_tokio::KcpListener::bind(
                        "127.0.0.1:0".parse().unwrap(),
                        default_kcp_config(),
                    )
                    .await
                    {
                        Ok(new_listener) => {
                            let new_addr = *new_listener.local_addr();
                            listener = new_listener;
                            *current_addr_for_task.write().await = new_addr;
                            eprintln!(
                                "[bench] listener rebound to new address {new_addr} (old {addr})"
                            );
                            info!(old_addr = %addr, new_addr = %new_addr, "benchmark listener rebound");
                        }
                        Err(bind_err) => {
                            eprintln!("[bench] rebind failed: {bind_err}; retrying soon");
                            info!(error = %bind_err, addr = %addr, "benchmark listener rebind failed");
                            tokio::time::sleep(REBIND_DELAY).await;
                        }
                    }
                }
                Err(_) => {
                    timeout_streak += 1;
                    // On Windows, listener.accept() can stall after UDP 10054.
                    if timeout_streak >= 1 {
                        eprintln!("[bench] listener accept timed out twice; rebinding {}", addr);
                        info!(addr = %addr, "benchmark listener accept timed out; rebinding");
                        match kcp_tokio::KcpListener::bind(
                            "127.0.0.1:0".parse().unwrap(),
                            default_kcp_config(),
                        )
                        .await
                        {
                            Ok(new_listener) => {
                                let new_addr = *new_listener.local_addr();
                                listener = new_listener;
                                *current_addr_for_task.write().await = new_addr;
                                eprintln!(
                                    "[bench] timeout recovery rebound to new address {new_addr} (old {addr})"
                                );
                                info!(old_addr = %addr, new_addr = %new_addr, "benchmark timeout recovery rebound");
                            }
                            Err(bind_err) => {
                                eprintln!(
                                    "[bench] timeout rebind failed: {bind_err}; retrying soon"
                                );
                                info!(error = %bind_err, addr = %addr, "benchmark timeout rebind failed");
                                tokio::time::sleep(REBIND_DELAY).await;
                            }
                        }
                        timeout_streak = 0;
                    }
                }
            }
        }
    });

    current_addr
}

async fn echo_roundtrip(
    current_addr: &Arc<RwLock<std::net::SocketAddr>>,
    payload: &[u8],
) -> Result<usize> {
    // Retry once to handle listener address refresh during recovery.
    for attempt in 0..2 {
        let addr = *current_addr.read().await;
        let config = default_kcp_config();
        let mut stream =
            tokio::time::timeout(IO_TIMEOUT, kcp_tokio::KcpStream::connect(addr, config)).await??;

        tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
        let mut buf = vec![0u8; payload.len()];
        match tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await {
            Ok(Ok(_)) => return Ok(buf.len()),
            Ok(Err(e)) => {
                if attempt == 1 {
                    return Err(e.into());
                }
            }
            Err(e) => {
                if attempt == 1 {
                    return Err(e.into());
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(0)
}

/// One echo roundtrip on an already-connected KcpStream (no reconnect).
/// Used in bench_throughput so connect cost is outside the hot path.
pub async fn kcp_tokio_stream_roundtrip(
    stream: &mut kcp_tokio::KcpStream,
    payload: &[u8],
) -> Result<usize> {
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;
    Ok(buf.len())
}

pub fn bench_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_tokio throughput (4 payload sizes) ===");
    info!("starting benchmark group: kcp_tokio_throughput");

    let rt = Runtime::new().unwrap();
    let current_addr = start_echo_server_in_background(&rt);

    let mut group = c.benchmark_group("kcp_tokio_throughput");
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
        info!(bench = %name, size = size, "throughput benchmark starting");
        let group_id = "kcp_tokio_throughput";
        let func_id = name.clone();
        let payload = vec![0xABu8; size];
        let stream: RefCell<Option<kcp_tokio::KcpStream>> = RefCell::new(None);
        group.bench_function(name, |b| {
            b.iter(|| {
                let n = rt.block_on(async {
                    let mut s = stream.borrow_mut().take();
                    if s.is_none() {
                        let addr = *current_addr.read().await;
                        let config = default_kcp_config();
                        match tokio::time::timeout(
                            IO_TIMEOUT,
                            kcp_tokio::KcpStream::connect(addr, config),
                        )
                        .await
                        {
                            Ok(Ok(kcp_s)) => {
                                s = Some(kcp_s);
                            }
                            Ok(Err(e)) => {
                                eprintln!("[bench] warning: kcp_tokio connect failed: {e}");
                                return 0;
                            }
                            Err(_) => {
                                eprintln!("[bench] warning: kcp_tokio connect timed out");
                                return 0;
                            }
                        }
                    }
                    let result = kcp_tokio_stream_roundtrip(s.as_mut().unwrap(), &payload).await;
                    match result {
                        Ok(n) => {
                            *stream.borrow_mut() = s;
                            n
                        }
                        Err(e) => {
                            eprintln!(
                                "[bench] warning: kcp_tokio throughput roundtrip failed: {e}"
                            );
                            0
                        }
                    }
                });
                let ok = n > 0;
                record_bench_success(group_id, &func_id, ok);
                black_box(n);
            })
        });
    }
    eprintln!("[bench] kcp_tokio_throughput group finished");
    group.finish();
}

pub fn bench_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_tokio latency (echo RTT 64B) ===");
    info!("starting benchmark group: kcp_tokio_latency");

    let rt = Runtime::new().unwrap();
    let current_addr = start_echo_server_in_background(&rt);

    let mut group = c.benchmark_group("kcp_tokio_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    eprintln!("[bench]   running echo_rtt_64b (connect + 1 echo)");
    group.bench_function("echo_rtt_64b", |b| {
        b.iter(|| {
            let n = rt.block_on(async {
                match echo_roundtrip(&current_addr, &[0xABu8; 64]).await {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("[bench] warning: latency roundtrip failed: {e}");
                        0
                    }
                }
            });
            let ok = n > 0;
            record_bench_success("kcp_tokio_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    eprintln!("[bench] kcp_tokio_latency group finished");
    group.finish();
}

pub fn bench_concurrent_connections(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_tokio concurrent (5/10/20 connections × 10 msgs) ===");
    info!("starting benchmark group: kcp_tokio_concurrent");

    let rt = Runtime::new().unwrap();
    let current_addr = start_echo_server_in_background(&rt);

    let mut group = c.benchmark_group("kcp_tokio_concurrent");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    for num_conns in [5, 10, 20] {
        let name = format!("{}_connections_10_msgs", num_conns);
        eprintln!(
            "[bench]   running {} (parallel connect + 10 echo each)",
            name
        );
        info!(bench = %name, connections = num_conns, "concurrent benchmark starting");
        let group_id = "kcp_tokio_concurrent";
        let func_id = name.clone();
        group.bench_function(name, |b| {
            b.iter(|| {
                let failures = rt.block_on(async {
                    let mut handles = Vec::with_capacity(num_conns);
                    for _ in 0..num_conns {
                        let current_addr = Arc::clone(&current_addr);
                        handles.push(tokio::spawn(async move {
                            let a = *current_addr.read().await;
                            let config = default_kcp_config();
                            let mut stream = tokio::time::timeout(
                                IO_TIMEOUT,
                                kcp_tokio::KcpStream::connect(a, config),
                            )
                            .await??;
                            for _ in 0..10 {
                                tokio::time::timeout(IO_TIMEOUT, stream.write_all(b"ping"))
                                    .await??;
                                tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
                                let mut buf = [0u8; 4];
                                tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf))
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
                                eprintln!("[bench] warning: concurrent worker failed: {e}");
                            }
                            Err(e) => {
                                failures += 1;
                                eprintln!("[bench] warning: concurrent worker join failed: {e}");
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
    eprintln!("[bench] kcp_tokio_concurrent group finished");
    group.finish();
}
