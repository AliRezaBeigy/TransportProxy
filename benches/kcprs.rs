//! kcprs benchmarks: throughput, latency, and concurrent connections over real UDP.

use anyhow::Result;
use criterion::{black_box, Criterion, Throughput};
use std::cell::RefCell;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::common::{
    current_ms, init_bench_logging, record_bench_success, KCP_UDP_MAX_ITER, UDP_READ_TIMEOUT,
};

// Thread-local buffers for kcprs output (it uses fn pointer, so we can't capture).
thread_local! {
    static KCPRS_BUF_A_TO_B: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static KCPRS_BUF_B_TO_A: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn kcprs_output_a_to_b(data: &[u8]) -> std::io::Result<()> {
    KCPRS_BUF_A_TO_B.with(|c| c.borrow_mut().extend_from_slice(data));
    Ok(())
}
fn kcprs_output_b_to_a(data: &[u8]) -> std::io::Result<()> {
    KCPRS_BUF_B_TO_A.with(|c| c.borrow_mut().extend_from_slice(data));
    Ok(())
}

/// Echo roundtrip using kcprs over real UDP (localhost). Drains thread-local output buffers to UDP.
pub fn kcprs_echo_roundtrip_udp(payload: &[u8], current_ms: u32) -> Result<usize> {
    let server_socket = UdpSocket::bind("127.0.0.1:0")?;
    let server_addr = server_socket.local_addr()?;
    server_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    server_socket.set_nonblocking(true)?;

    let client_socket = UdpSocket::bind("127.0.0.1:0")?;
    client_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    client_socket.set_nonblocking(true)?;

    let conv = 1u32;
    KCPRS_BUF_A_TO_B.with(|c| c.borrow_mut().clear());
    KCPRS_BUF_B_TO_A.with(|c| c.borrow_mut().clear());

    let mut kcp_a = kcprs::Kcp::new_stream(conv, kcprs_output_a_to_b);
    let mut kcp_b = kcprs::Kcp::new_stream(conv, kcprs_output_b_to_a);

    kcp_a.set_mtu(1400)?;
    kcp_b.set_mtu(1400)?;
    kcp_a.set_wndsize(128, 128);
    kcp_b.set_wndsize(128, 128);
    kcp_a.set_nodelay(true, 20, 2, true);
    kcp_b.set_nodelay(true, 20, 2, true);

    kcp_a.send(payload)?;
    // kcprs requires update() before flush() — output callback is driven by update(), not flush()
    kcp_a.update(current_ms)?;
    kcp_b.update(current_ms)?;
    kcp_a.flush()?;

    let mut current = current_ms;
    let mut recv_len = 0usize;
    let mut client_addr: Option<SocketAddr> = None;
    let mut buf = vec![0u8; payload.len() + 2048];

    for _ in 0..KCP_UDP_MAX_ITER {
        std::thread::sleep(Duration::from_micros(200)); // Bounded wait; Windows may ignore UDP read timeout
                                                        // Produce output first (kcprs pushes to callback from update(), not only flush).
        kcp_a.update(current)?;
        kcp_b.update(current)?;

        let mut send_a2b_ok = false;
        KCPRS_BUF_A_TO_B.with(|cell| {
            let ab = cell.borrow_mut();
            if !ab.is_empty() {
                for _ in 0..10 {
                    match client_socket.send_to(&ab[..], server_addr) {
                        Ok(_) => {
                            send_a2b_ok = true;
                            break;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_micros(100));
                        }
                        Err(_) => break,
                    }
                }
            }
        });
        KCPRS_BUF_A_TO_B.with(|cell| cell.borrow_mut().clear());

        let mut send_b2a_ok = false;
        KCPRS_BUF_B_TO_A.with(|cell| {
            let ba = cell.borrow_mut();
            if !ba.is_empty() {
                if let Some(peer) = client_addr {
                    for _ in 0..10 {
                        match server_socket.send_to(&ba[..], peer) {
                            Ok(_) => {
                                send_b2a_ok = true;
                                break;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_micros(100));
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        KCPRS_BUF_B_TO_A.with(|cell| cell.borrow_mut().clear());

        let _ = (send_a2b_ok, send_b2a_ok); // suppress unused warnings

        if let Ok((n, from)) = server_socket.recv_from(&mut buf) {
            client_addr = Some(from);
            kcp_b.input(&buf[..n])?;
        }
        if let Ok((n, _)) = client_socket.recv_from(&mut buf) {
            kcp_a.input(&buf[..n])?;
        }

        if let Ok(n) = kcp_b.recv(&mut buf) {
            let _ = kcp_b.send(&buf[..n]);
            kcp_b.flush().ok();
        }

        if let Ok(n) = kcp_a.recv(&mut buf) {
            recv_len = n;
            break;
        }
        current += 5;
    }
    Ok(recv_len)
}

pub struct KcprsPersistentState {
    pub client_socket: UdpSocket,
    pub server_socket: UdpSocket,
    pub server_addr: SocketAddr,
    pub kcp_a: kcprs::Kcp,
    pub kcp_b: kcprs::Kcp,
    pub client_addr: Option<SocketAddr>,
}

pub fn kcprs_init_persistent(init_ms: u32) -> Result<KcprsPersistentState> {
    let server_socket = UdpSocket::bind("127.0.0.1:0")?;
    let server_addr = server_socket.local_addr()?;
    server_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    server_socket.set_nonblocking(true)?;

    let client_socket = UdpSocket::bind("127.0.0.1:0")?;
    client_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    client_socket.set_nonblocking(true)?;

    KCPRS_BUF_A_TO_B.with(|c| c.borrow_mut().clear());
    KCPRS_BUF_B_TO_A.with(|c| c.borrow_mut().clear());

    let conv = 1u32;
    let mut kcp_a = kcprs::Kcp::new_stream(conv, kcprs_output_a_to_b);
    let mut kcp_b = kcprs::Kcp::new_stream(conv, kcprs_output_b_to_a);

    kcp_a.set_mtu(1400)?;
    kcp_b.set_mtu(1400)?;
    kcp_a.set_wndsize(128, 128);
    kcp_b.set_wndsize(128, 128);
    kcp_a.set_nodelay(true, 20, 2, true);
    kcp_b.set_nodelay(true, 20, 2, true);

    kcp_a.update(init_ms)?;
    kcp_b.update(init_ms)?;

    Ok(KcprsPersistentState {
        client_socket,
        server_socket,
        server_addr,
        kcp_a,
        kcp_b,
        client_addr: None,
    })
}

pub fn kcprs_persistent_roundtrip(
    state: &mut KcprsPersistentState,
    payload: &[u8],
) -> Result<usize> {
    let mut current = current_ms();

    KCPRS_BUF_A_TO_B.with(|c| c.borrow_mut().clear());
    KCPRS_BUF_B_TO_A.with(|c| c.borrow_mut().clear());

    state.kcp_a.send(payload)?;
    state.kcp_a.update(current)?;
    state.kcp_b.update(current)?;
    state.kcp_a.flush()?;

    let mut recv_len = 0usize;
    let mut buf = vec![0u8; payload.len() + 2048];

    for _ in 0..KCP_UDP_MAX_ITER {
        std::thread::sleep(Duration::from_micros(200));
        state.kcp_a.update(current)?;
        state.kcp_b.update(current)?;

        KCPRS_BUF_A_TO_B.with(|cell| {
            let ab = cell.borrow_mut();
            if !ab.is_empty() {
                for _ in 0..10 {
                    match state.client_socket.send_to(&ab[..], state.server_addr) {
                        Ok(_) => break,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_micros(100));
                        }
                        Err(_) => break,
                    }
                }
            }
        });
        KCPRS_BUF_A_TO_B.with(|cell| cell.borrow_mut().clear());

        KCPRS_BUF_B_TO_A.with(|cell| {
            let ba = cell.borrow_mut();
            if !ba.is_empty() {
                if let Some(peer) = state.client_addr {
                    for _ in 0..10 {
                        match state.server_socket.send_to(&ba[..], peer) {
                            Ok(_) => break,
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_micros(100));
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        KCPRS_BUF_B_TO_A.with(|cell| cell.borrow_mut().clear());

        if let Ok((n, from)) = state.server_socket.recv_from(&mut buf) {
            state.client_addr = Some(from);
            state.kcp_b.input(&buf[..n])?;
        }
        if let Ok((n, _)) = state.client_socket.recv_from(&mut buf) {
            state.kcp_a.input(&buf[..n])?;
        }

        if let Ok(n) = state.kcp_b.recv(&mut buf) {
            let _ = state.kcp_b.send(&buf[..n]);
            state.kcp_b.flush().ok();
        }

        if let Ok(n) = state.kcp_a.recv(&mut buf) {
            recv_len = n;
            break;
        }
        current += 5;
    }
    Ok(recv_len)
}

pub fn bench_kcprs_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcprs throughput (UDP localhost) ===");

    let mut group = c.benchmark_group("kcprs_throughput");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    // kcprs-0.5.0 parse_ack() panics on multi-segment payloads (≥ 4096b at MTU 1400) due to an
    // out-of-bounds bug: it iterates `for i in 0..snd_buf.len()` and calls snd_buf.remove(i)
    // without breaking, so the cached length becomes stale. Sizes ≥ 4096b are skipped here
    // rather than crashing the bench runner. Single-segment sizes (≤ 1024b) reuse state
    // normally, matching the pattern of all other implementations.
    for size in [64, 256, 1024] {
        group.throughput(Throughput::Bytes(size as u64));
        let name = format!("echo_{}b", size);
        eprintln!(
            "[bench]   running {} (1 echo round-trip, connection reused per payload size)",
            name
        );
        let group_id = "kcprs_throughput";
        let func_id = name.clone();
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
        let state: RefCell<Option<KcprsPersistentState>> = RefCell::new(None);
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut opt = state.borrow_mut();
                if opt.is_none() {
                    *opt = Some(kcprs_init_persistent(current_ms()).expect("kcprs init failed"));
                }
                let n = kcprs_persistent_roundtrip(opt.as_mut().unwrap(), &payload).unwrap_or_else(
                    |e| {
                        eprintln!("[bench] warning: kcprs throughput roundtrip failed: {e}");
                        *opt = None;
                        0
                    },
                );
                let ok = n > 0;
                record_bench_success(group_id, &func_id, ok);
                black_box(n);
            })
        });
    }
    for size in [4096usize, 8192] {
        eprintln!(
            "[bench]   skipping echo_{}b — kcprs-0.5.0 panics on multi-segment payloads (parse_ack bug)",
            size
        );
    }
    group.finish();
}

pub fn bench_kcprs_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcprs latency (UDP echo RTT 64B) ===");

    let mut group = c.benchmark_group("kcprs_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    group.bench_function("echo_rtt_64b", |b| {
        let payload = [0xABu8; 64];
        b.iter(|| {
            let base_ms = current_ms();
            let n = kcprs_echo_roundtrip_udp(&payload, base_ms).unwrap_or(0);
            let ok = n > 0;
            record_bench_success("kcprs_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    group.finish();
}

pub fn bench_kcprs_concurrent(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcprs concurrent (5/10/20 × 10 msgs over UDP) ===");

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("kcprs_concurrent");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    let payload = [0xABu8; 64];
    for num_conns in [5, 10, 20] {
        let name = format!("{}_connections_10_msgs", num_conns);
        let group_id = "kcprs_concurrent";
        let func_id = name.clone();
        group.bench_function(name, |b| {
            b.iter(|| {
                let base_ms = current_ms();
                let ok_count = rt.block_on(async {
                    let mut handles = Vec::with_capacity(num_conns);
                    for conn in 0..num_conns {
                        let base = base_ms.wrapping_add((conn as u32).wrapping_mul(5000));
                        handles.push(tokio::task::spawn_blocking(move || {
                            (0..10)
                                .filter(|_| {
                                    kcprs_echo_roundtrip_udp(&payload, base)
                                        .map(|n| n > 0)
                                        .unwrap_or(false)
                                })
                                .count()
                        }));
                    }
                    let mut total = 0usize;
                    for h in handles {
                        total += h.await.unwrap_or(0);
                    }
                    total
                });
                let ok = ok_count == num_conns * 10;
                record_bench_success(group_id, &func_id, ok);
            })
        });
    }
    group.finish();
}
