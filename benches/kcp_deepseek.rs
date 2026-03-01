//! kcp_deepseek benchmarks: throughput, latency, and concurrent connections over real UDP.

use anyhow::Result;
use criterion::{black_box, Criterion, Throughput};
use std::cell::RefCell;
use std::net::{SocketAddr, UdpSocket};
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::common::{
    current_ms, init_bench_logging, record_bench_success, ClientUdpWriter, ServerUdpWriter,
    KCP_UDP_MAX_ITER, UDP_READ_TIMEOUT,
};

fn kcp_deepseek_echo_roundtrip_udp(payload: &[u8], current_ms: u32) -> Result<usize> {
    let server_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    let server_addr = server_socket.local_addr()?;
    server_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    server_socket.set_nonblocking(true)?;

    let client_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    client_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    client_socket.set_nonblocking(true)?;

    let conv = 1u32;
    let server_buf = Rc::new(RefCell::new(Vec::new()));
    let server_peer = Rc::new(RefCell::new(None::<SocketAddr>));

    let mut kcp_a = kcp_deepseek::Kcp::new_stream(
        conv,
        ClientUdpWriter {
            socket: Rc::clone(&client_socket),
            peer: server_addr,
        },
    );
    let mut kcp_b = kcp_deepseek::Kcp::new_stream(
        conv,
        ServerUdpWriter {
            socket: Rc::clone(&server_socket),
            buffer: Rc::clone(&server_buf),
            peer: Rc::clone(&server_peer),
        },
    );

    kcp_a.set_mtu(1400)?;
    kcp_b.set_mtu(1400)?;
    kcp_a.set_wndsize(128, 128);
    kcp_b.set_wndsize(128, 128);
    kcp_a.set_nodelay(true, 20, 2, true);
    kcp_b.set_nodelay(true, 20, 2, true);

    kcp_a.send(payload)?;
    // deepseeksss/kcp: update() drives output (Writer is called from update), so call before flush()
    kcp_a.update(current_ms)?;
    kcp_b.update(current_ms)?;
    kcp_a.flush()?;

    let mut current = current_ms;
    let mut recv_len = 0usize;
    let mut buf = vec![0u8; payload.len() + 2048];

    current += 5;

    for _ in 0..KCP_UDP_MAX_ITER {
        std::thread::sleep(Duration::from_micros(200)); // Bounded wait; Windows may ignore UDP read timeout
        if let Ok((n, from)) = server_socket.recv_from(&mut buf) {
            let server_writer = ServerUdpWriter {
                socket: Rc::clone(&server_socket),
                buffer: Rc::clone(&server_buf),
                peer: Rc::clone(&server_peer),
            };
            server_writer.send_buffered(from)?;
            kcp_b.input(&buf[..n])?;
        }
        if let Ok((n, _)) = client_socket.recv_from(&mut buf) {
            kcp_a.input(&buf[..n])?;
        }

        kcp_a.update(current)?;
        kcp_b.update(current)?;

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

pub struct KcpDeepseekPersistentState {
    pub client_socket: Rc<UdpSocket>,
    pub server_socket: Rc<UdpSocket>,
    pub server_addr: SocketAddr,
    pub server_buf: Rc<RefCell<Vec<u8>>>,
    pub server_peer: Rc<RefCell<Option<SocketAddr>>>,
    pub kcp_a: kcp_deepseek::Kcp<ClientUdpWriter>,
    pub kcp_b: kcp_deepseek::Kcp<ServerUdpWriter>,
}

pub fn kcp_deepseek_init_persistent(init_ms: u32) -> Result<KcpDeepseekPersistentState> {
    let server_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    let server_addr = server_socket.local_addr()?;
    server_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    server_socket.set_nonblocking(true)?;

    let client_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    client_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    client_socket.set_nonblocking(true)?;

    let conv = 1u32;
    let server_buf = Rc::new(RefCell::new(Vec::new()));
    let server_peer = Rc::new(RefCell::new(None::<SocketAddr>));

    let mut kcp_a = kcp_deepseek::Kcp::new_stream(
        conv,
        ClientUdpWriter {
            socket: Rc::clone(&client_socket),
            peer: server_addr,
        },
    );
    let mut kcp_b = kcp_deepseek::Kcp::new_stream(
        conv,
        ServerUdpWriter {
            socket: Rc::clone(&server_socket),
            buffer: Rc::clone(&server_buf),
            peer: Rc::clone(&server_peer),
        },
    );

    kcp_a.set_mtu(1400)?;
    kcp_b.set_mtu(1400)?;
    kcp_a.set_wndsize(128, 128);
    kcp_b.set_wndsize(128, 128);
    kcp_a.set_nodelay(true, 20, 2, true);
    kcp_b.set_nodelay(true, 20, 2, true);

    kcp_a.update(init_ms)?;
    kcp_b.update(init_ms)?;

    Ok(KcpDeepseekPersistentState {
        client_socket,
        server_socket,
        server_addr,
        server_buf,
        server_peer,
        kcp_a,
        kcp_b,
    })
}

pub fn kcp_deepseek_persistent_roundtrip(
    state: &mut KcpDeepseekPersistentState,
    payload: &[u8],
) -> Result<usize> {
    let mut current = current_ms();

    state.kcp_a.send(payload)?;
    state.kcp_a.update(current)?;
    state.kcp_b.update(current)?;
    state.kcp_a.flush()?;

    current += 5;
    let mut recv_len = 0usize;
    let mut buf = vec![0u8; payload.len() + 2048];

    for _ in 0..KCP_UDP_MAX_ITER {
        std::thread::sleep(Duration::from_micros(200));
        if let Ok((n, from)) = state.server_socket.recv_from(&mut buf) {
            let server_writer = ServerUdpWriter {
                socket: Rc::clone(&state.server_socket),
                buffer: Rc::clone(&state.server_buf),
                peer: Rc::clone(&state.server_peer),
            };
            server_writer.send_buffered(from)?;
            state.kcp_b.input(&buf[..n])?;
        }
        if let Ok((n, _)) = state.client_socket.recv_from(&mut buf) {
            state.kcp_a.input(&buf[..n])?;
        }

        state.kcp_a.update(current)?;
        state.kcp_b.update(current)?;

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

pub fn bench_kcp_deepseek_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_deepseek throughput (UDP localhost, 5 payload sizes) ===");

    let mut group = c.benchmark_group("kcp_deepseek_throughput");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    for size in [64, 256, 1024, 4096, 8192] {
        group.throughput(Throughput::Bytes(size as u64));
        let name = format!("echo_{}b", size);
        let group_id = "kcp_deepseek_throughput";
        let func_id = name.clone();
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
        let state: RefCell<Option<KcpDeepseekPersistentState>> = RefCell::new(None);
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut opt = state.borrow_mut();
                if opt.is_none() {
                    *opt = Some(
                        kcp_deepseek_init_persistent(current_ms())
                            .expect("kcp_deepseek init failed"),
                    );
                }
                let n = kcp_deepseek_persistent_roundtrip(opt.as_mut().unwrap(), &payload)
                    .unwrap_or_else(|e| {
                        eprintln!("[bench] warning: kcp_deepseek throughput roundtrip failed: {e}");
                        *opt = None;
                        0
                    });
                let ok = n > 0;
                record_bench_success(group_id, &func_id, ok);
                black_box(n);
            })
        });
    }
    group.finish();
}

/// N parallel UDP "connections": each does 10 echo roundtrips (each on a new port pair).
pub fn bench_kcp_deepseek_concurrent(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_deepseek concurrent (5/10/20 × 10 msgs over UDP) ===");

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("kcp_deepseek_concurrent");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    let payload = [0xABu8; 64];
    for num_conns in [5, 10, 20] {
        let name = format!("{}_connections_10_msgs", num_conns);
        let group_id = "kcp_deepseek_concurrent";
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
                                    kcp_deepseek_echo_roundtrip_udp(&payload, base)
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

pub fn bench_kcp_deepseek_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_deepseek latency (UDP echo RTT 64B) ===");

    let mut group = c.benchmark_group("kcp_deepseek_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    group.bench_function("echo_rtt_64b", |b| {
        let payload = [0xABu8; 64];
        b.iter(|| {
            let base_ms = current_ms();
            let n = kcp_deepseek_echo_roundtrip_udp(&payload, base_ms).unwrap_or(0);
            let ok = n > 0;
            record_bench_success("kcp_deepseek_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    group.finish();
}
