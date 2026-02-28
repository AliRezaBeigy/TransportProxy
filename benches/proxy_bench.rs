//! Criterion benchmarks: throughput and latency of KCP/QUIC echo over localhost.
#![allow(dead_code)]

//! - **kcp_tokio**: kcp-tokio over UDP
//! - **kcp_deepseek**: https://github.com/deepseeksss/kcp — core KCP over UDP (bench bridge)
//! - **kcprs**: https://crates.io/crates/kcprs — pure Rust KCP over UDP (bench bridge)
//! - **quinn**: https://github.com/quinn-rs/quinn — QUIC over UDP (TLS)
//! - **slipstream-picoquic** (optional): QUIC over UDP via C lib
//! - **ys-kcp** (optional, nightly): https://crates.io/crates/ys-kcp — over UDP (bench bridge)
//! - **kcp-sys** (optional, libclang): https://crates.io/crates/kcp-sys — KCP over UDP

use anyhow::Result;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use proxy_server::default_kcp_config;
use std::cell::RefCell;
use std::io::Write;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Once;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::info;

const KCP_OVERHEAD: usize = 24;
#[cfg(feature = "ys-kcp")]
const KCP_OVERHEAD_YS: usize = 28; // ys-kcp has token field
#[cfg(feature = "ys-kcp")]
const YS_KCP_SLEEP_US: u64 = 200; // bounded wait per iteration; matches kcp_deepseek/kcprs

/// Log file for success rate: one line per iteration "group\tfunction\tsuccess" (success 0 or 1).
static BENCH_SUCCESS_LOG: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

fn record_bench_success(group: &str, function: &str, success: bool) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("criterion_bench_success.log");
    let file = BENCH_SUCCESS_LOG.get_or_init(|| {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open criterion_bench_success.log");
        Mutex::new(f)
    });
    if let Ok(mut guard) = file.lock() {
        let _ = writeln!(
            guard,
            "{}\t{}\t{}",
            group,
            function,
            if success { 1 } else { 0 }
        );
        let _ = guard.flush();
    }
}

/// Writer that appends to a shared buffer (for kcp crate loopback).
struct LoopbackWriter(Rc<RefCell<Vec<u8>>>);

impl Write for LoopbackWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Feed all complete KCP packets from `buf` into `kcp.input()`. Remaining bytes stay in buf.
fn feed_packets_to_kcp(
    buf: &mut Vec<u8>,
    kcp: &mut kcp_deepseek::Kcp<LoopbackWriter>,
) -> Result<()> {
    while buf.len() >= KCP_OVERHEAD {
        let payload_len = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
        let packet_len = KCP_OVERHEAD + payload_len;
        if buf.len() < packet_len {
            break;
        }
        let packet: Vec<u8> = buf.drain(..packet_len).collect();
        kcp.input(&packet)?;
    }
    Ok(())
}

/// Feed complete KCP packets (24-byte header, len at 20) into kcprs.
fn feed_packets_to_kcprs(buf: &mut Vec<u8>, kcp: &mut kcprs::Kcp) -> Result<()> {
    while buf.len() >= KCP_OVERHEAD {
        let payload_len = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
        let packet_len = KCP_OVERHEAD + payload_len;
        if buf.len() < packet_len {
            break;
        }
        let packet: Vec<u8> = buf.drain(..packet_len).collect();
        kcp.input(&packet)?;
    }
    Ok(())
}

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
fn kcprs_echo_roundtrip_udp(payload: &[u8], current_ms: u32) -> Result<usize> {
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

#[cfg(feature = "ys-kcp")]
/// Feed complete ys-kcp packets (28-byte header, len at 24). (ys-kcp crate exposes lib as "kcp".)
fn feed_packets_to_ys_kcp<O>(buf: &mut Vec<u8>, kcp: &mut kcp::Kcp<O>) -> Result<()> {
    while buf.len() >= KCP_OVERHEAD_YS {
        let payload_len = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]) as usize;
        let packet_len = KCP_OVERHEAD_YS + payload_len;
        if buf.len() < packet_len {
            break;
        }
        let packet: Vec<u8> = buf.drain(..packet_len).collect();
        kcp.input(&packet)?;
    }
    Ok(())
}

#[cfg(feature = "ys-kcp")]
/// Echo roundtrip using ys-kcp over real UDP (localhost).
/// `max_iter`: cap on poll loop iterations (defaults to KCP_UDP_MAX_ITER when None).
///
fn ys_kcp_echo_roundtrip_udp(
    payload: &[u8],
    current_ms: u32,
    max_iter: Option<usize>,
) -> Result<usize> {
    let max_iter = max_iter.unwrap_or(KCP_UDP_MAX_ITER);
    let server_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    let server_addr = server_socket.local_addr()?;
    server_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    server_socket.set_nonblocking(true)?;

    let client_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    client_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    client_socket.set_nonblocking(true)?;

    let conv = 1u32;
    let token = 0u32;
    let server_buf = Rc::new(RefCell::new(Vec::new()));
    let server_peer = Rc::new(RefCell::new(None::<SocketAddr>));

    let mut kcp_a = kcp::Kcp::new_stream(
        conv,
        token,
        ClientUdpWriter {
            socket: Rc::clone(&client_socket),
            peer: server_addr,
        },
    );
    let mut kcp_b = kcp::Kcp::new_stream(
        conv,
        token,
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
    // ys-kcp requires update() at least once before flush() (otherwise "flush updated() must be called at least once").
    kcp_a.update(current_ms)?;
    kcp_a.flush()?;

    let mut current = current_ms;
    let mut recv_len = 0usize;
    let mut buf = vec![0u8; payload.len() + 2048];
    let mut server_pkt = vec![0u8; 1500]; // separate buffer so client recv_from doesn't overwrite before kcp_b processes
    let mut server_recv_buf: Vec<u8> = Vec::new(); // accumulate and feed via feed_packets_to_ys_kcp (same as in-memory path)

    current += 5;

    for _ in 0..max_iter {
        std::thread::sleep(Duration::from_micros(YS_KCP_SLEEP_US)); // Bounded wait
                                                                    // ys-kcp: update first so current is set and flush runs (matches in-memory loop and update() doc)
        kcp_a.update(current)?;
        kcp_b.update(current)?;
        if let Ok((n, from)) = server_socket.recv_from(&mut server_pkt) {
            let server_writer = ServerUdpWriter {
                socket: Rc::clone(&server_socket),
                buffer: Rc::clone(&server_buf),
                peer: Rc::clone(&server_peer),
            };
            server_writer.send_buffered(from)?;
            server_recv_buf.extend_from_slice(&server_pkt[..n]);
            feed_packets_to_ys_kcp(&mut server_recv_buf, &mut kcp_b)?;
        }
        if let Ok((n, _)) = client_socket.recv_from(&mut buf) {
            kcp_a.input(&buf[..n])?;
        }

        if let Ok(n) = kcp_b.recv(&mut buf) {
            if n > 0 {
                let _ = kcp_b.send(&buf[..n]);
                kcp_b.flush().ok();
                // Push echo to output so B->A is sent next iteration
                kcp_b.update(current).ok();
            }
        }

        kcp_a.update(current).ok(); // ensure input is processed before recv
        if let Ok(n) = kcp_a.recv(&mut buf) {
            if n > 0 {
                recv_len = n;
                break;
            }
        }
        current += 5;
    }
    Ok(recv_len)
}

// ---- kcp_deepseek over real UDP ----

/// Writer that sends KCP output to a fixed peer over UDP.
struct ClientUdpWriter {
    socket: Rc<UdpSocket>,
    peer: SocketAddr,
}

impl Write for ClientUdpWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for _ in 0..10 {
            match self.socket.send_to(buf, self.peer) {
                Ok(_) => return Ok(buf.len()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_micros(100));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Writer for server: buffers until peer is set, then sends. Call send_buffered(peer) when first packet arrives.
struct ServerUdpWriter {
    socket: Rc<UdpSocket>,
    buffer: Rc<RefCell<Vec<u8>>>,
    peer: Rc<RefCell<Option<SocketAddr>>>,
}

impl ServerUdpWriter {
    fn send_buffered(&self, peer: SocketAddr) -> std::io::Result<()> {
        *self.peer.borrow_mut() = Some(peer);
        let buf = std::mem::take(&mut *self.buffer.borrow_mut());
        if !buf.is_empty() {
            self.socket.send_to(&buf, peer)?;
        }
        Ok(())
    }
}

impl Write for ServerUdpWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(peer) = *self.peer.borrow() {
            for _ in 0..10 {
                match self.socket.send_to(buf, peer) {
                    Ok(_) => return Ok(buf.len()),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_micros(100));
                    }
                    Err(e) => return Err(e),
                }
            }
        } else {
            self.buffer.borrow_mut().extend_from_slice(buf);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

const UDP_READ_TIMEOUT: Duration = Duration::from_millis(50);
const KCP_UDP_MAX_ITER: usize = 2000;

/// Echo roundtrip using kcp_deepseek over real UDP (localhost). Same conditions as kcp_tokio.
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

fn current_ms() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32
}

// ---- kcp_deepseek persistent state (for throughput bench connection reuse) ----

struct KcpDeepseekPersistentState {
    client_socket: Rc<UdpSocket>,
    server_socket: Rc<UdpSocket>,
    server_addr: SocketAddr,
    server_buf: Rc<RefCell<Vec<u8>>>,
    server_peer: Rc<RefCell<Option<SocketAddr>>>,
    kcp_a: kcp_deepseek::Kcp<ClientUdpWriter>,
    kcp_b: kcp_deepseek::Kcp<ServerUdpWriter>,
}

fn kcp_deepseek_init_persistent(init_ms: u32) -> Result<KcpDeepseekPersistentState> {
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

fn kcp_deepseek_persistent_roundtrip(
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

// ---- kcprs persistent state (for throughput bench connection reuse) ----

struct KcprsPersistentState {
    client_socket: UdpSocket,
    server_socket: UdpSocket,
    server_addr: SocketAddr,
    kcp_a: kcprs::Kcp,
    kcp_b: kcprs::Kcp,
    client_addr: Option<SocketAddr>,
}

fn kcprs_init_persistent(init_ms: u32) -> Result<KcprsPersistentState> {
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

fn kcprs_persistent_roundtrip(state: &mut KcprsPersistentState, payload: &[u8]) -> Result<usize> {
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

static INIT_LOGGING: Once = Once::new();
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(1);
const IO_TIMEOUT: Duration = Duration::from_millis(800);
/// Slipstream-picoquic QUIC+TLS handshake can exceed 800ms on some systems; use longer timeout for connect only.
#[cfg(feature = "slipstream-picoquic")]
const SLIPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// One echo round-trip per throughput iteration, matching all other implementations.
/// The connection is reused per payload size (connect once outside b.iter), so handshake
/// cost is still amortized — only the per-iteration unit is normalized.
#[cfg(feature = "slipstream-picoquic")]
const SLIPSTREAM_THROUGHPUT_ROUNDTRIPS: u32 = 1;
const REBIND_DELAY: Duration = Duration::from_millis(200);

fn init_bench_logging() {
    INIT_LOGGING.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("info".parse().unwrap());
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(false)
            .compact()
            .init();
        eprintln!("[bench] logging initialized (RUST_LOG=debug for more)");
    });
}

fn start_echo_server_in_background(rt: &Runtime) -> Arc<RwLock<SocketAddr>> {
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
                        while let Ok(Ok(n)) = tokio::time::timeout(IO_TIMEOUT, stream.read(&mut buf)).await
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
                                eprintln!("[bench] timeout rebind failed: {bind_err}; retrying soon");
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

async fn echo_roundtrip(current_addr: &Arc<RwLock<SocketAddr>>, payload: &[u8]) -> Result<usize> {
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
async fn kcp_tokio_stream_roundtrip(
    stream: &mut kcp_tokio::KcpStream,
    payload: &[u8],
) -> Result<usize> {
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;
    Ok(buf.len())
}

// ---- quinn (QUIC echo over UDP, TLS) ----

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

fn build_quinn_server_config() -> Result<(
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

fn build_quinn_client_config(
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

fn start_quinn_echo_server_in_background(rt: &Runtime) -> Result<Arc<RwLock<SocketAddr>>> {
    let (server_config, _cert_der, _) = build_quinn_server_config()?;
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let endpoint = quinn::Endpoint::server(server_config, addr)?;
    let local_addr = endpoint.local_addr()?;
    let current_addr = Arc::new(RwLock::new(local_addr));
    eprintln!("[bench] Quinn echo server listening on {}", local_addr);
    info!(addr = %local_addr, "Quinn echo server started for benchmarks");

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
async fn quinn_connection_roundtrip(conn: &quinn::Connection, payload: &[u8]) -> Result<usize> {
    let (mut send, mut recv) = conn.open_bi().await?;
    tokio::time::timeout(IO_TIMEOUT, send.write_all(payload)).await??;
    send.finish()?;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, recv.read_exact(&mut buf)).await??;
    Ok(payload.len())
}

fn bench_quinn_throughput(c: &mut Criterion) {
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
                    let mut opt = conn_cell.borrow_mut();
                    if opt.is_none() {
                        let addr = *current_addr.read().await;
                        let connecting = match client_endpoint.connect(addr, "localhost") {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("[bench] warning: quinn connect failed: {e}");
                                return 0;
                            }
                        };
                        match tokio::time::timeout(IO_TIMEOUT, connecting).await {
                            Ok(Ok(conn)) => {
                                *opt = Some(conn);
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
                    match quinn_connection_roundtrip(opt.as_ref().unwrap(), &payload).await {
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("[bench] warning: quinn throughput roundtrip failed: {e}");
                            *opt = None;
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

fn start_quinn_echo_server_in_background_with_config(
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

fn bench_quinn_latency(c: &mut Criterion) {
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

fn bench_quinn_concurrent(c: &mut Criterion) {
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

fn bench_throughput(c: &mut Criterion) {
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
                    let mut opt = stream.borrow_mut();
                    if opt.is_none() {
                        let addr = *current_addr.read().await;
                        let config = default_kcp_config();
                        match tokio::time::timeout(
                            IO_TIMEOUT,
                            kcp_tokio::KcpStream::connect(addr, config),
                        )
                        .await
                        {
                            Ok(Ok(s)) => {
                                *opt = Some(s);
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
                    match kcp_tokio_stream_roundtrip(opt.as_mut().unwrap(), &payload).await {
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!(
                                "[bench] warning: kcp_tokio throughput roundtrip failed: {e}"
                            );
                            *opt = None;
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

fn bench_latency(c: &mut Criterion) {
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

fn bench_concurrent_connections(c: &mut Criterion) {
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

// ---- deepseeksss/kcp over real UDP ----

fn bench_kcp_deepseek_throughput(c: &mut Criterion) {
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
fn bench_kcp_deepseek_concurrent(c: &mut Criterion) {
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

fn bench_kcp_deepseek_latency(c: &mut Criterion) {
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

// ---- kcprs (UDP localhost) ----

fn bench_kcprs_throughput(c: &mut Criterion) {
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

fn bench_kcprs_latency(c: &mut Criterion) {
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

fn bench_kcprs_concurrent(c: &mut Criterion) {
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

#[cfg(feature = "ys-kcp")]
struct YsKcpPersistentState {
    client_socket: Rc<UdpSocket>,
    server_socket: Rc<UdpSocket>,
    server_addr: SocketAddr,
    server_buf: Rc<RefCell<Vec<u8>>>,
    server_peer: Rc<RefCell<Option<SocketAddr>>>,
    kcp_a: kcp::Kcp<ClientUdpWriter>,
    kcp_b: kcp::Kcp<ServerUdpWriter>,
    server_recv_buf: Vec<u8>,
}

#[cfg(feature = "ys-kcp")]
fn ys_kcp_init_persistent(init_ms: u32) -> Result<YsKcpPersistentState> {
    let server_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    let server_addr = server_socket.local_addr()?;
    server_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    server_socket.set_nonblocking(true)?;

    let client_socket = Rc::new(UdpSocket::bind("127.0.0.1:0")?);
    client_socket.set_read_timeout(Some(UDP_READ_TIMEOUT))?;
    client_socket.set_nonblocking(true)?;

    let conv = 1u32;
    let token = 0u32;
    let server_buf = Rc::new(RefCell::new(Vec::new()));
    let server_peer = Rc::new(RefCell::new(None::<SocketAddr>));

    let mut kcp_a = kcp::Kcp::new_stream(
        conv,
        token,
        ClientUdpWriter {
            socket: Rc::clone(&client_socket),
            peer: server_addr,
        },
    );
    let mut kcp_b = kcp::Kcp::new_stream(
        conv,
        token,
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

    Ok(YsKcpPersistentState {
        client_socket,
        server_socket,
        server_addr,
        server_buf,
        server_peer,
        kcp_a,
        kcp_b,
        server_recv_buf: Vec::new(),
    })
}

#[cfg(feature = "ys-kcp")]
fn ys_kcp_persistent_roundtrip(state: &mut YsKcpPersistentState, payload: &[u8]) -> Result<usize> {
    let mut current = current_ms();

    state.kcp_a.send(payload)?;
    state.kcp_a.update(current)?;
    state.kcp_a.flush()?;

    current += 5;

    let mut recv_len = 0usize;
    let mut buf = vec![0u8; payload.len() + 2048];
    let mut server_pkt = vec![0u8; 1500];

    for _ in 0..KCP_UDP_MAX_ITER {
        std::thread::sleep(Duration::from_micros(YS_KCP_SLEEP_US));
        state.kcp_a.update(current)?;
        state.kcp_b.update(current)?;

        if let Ok((n, from)) = state.server_socket.recv_from(&mut server_pkt) {
            let server_writer = ServerUdpWriter {
                socket: Rc::clone(&state.server_socket),
                buffer: Rc::clone(&state.server_buf),
                peer: Rc::clone(&state.server_peer),
            };
            server_writer.send_buffered(from)?;
            state.server_recv_buf.extend_from_slice(&server_pkt[..n]);
            feed_packets_to_ys_kcp(&mut state.server_recv_buf, &mut state.kcp_b)?;
        }
        if let Ok((n, _)) = state.client_socket.recv_from(&mut buf) {
            state.kcp_a.input(&buf[..n])?;
        }

        if let Ok(n) = state.kcp_b.recv(&mut buf) {
            if n > 0 {
                let _ = state.kcp_b.send(&buf[..n]);
                state.kcp_b.flush().ok();
                state.kcp_b.update(current).ok();
            }
        }

        state.kcp_a.update(current).ok();
        if let Ok(n) = state.kcp_a.recv(&mut buf) {
            if n > 0 {
                recv_len = n;
                break;
            }
        }
        current += 5;
    }
    Ok(recv_len)
}

#[cfg(feature = "ys-kcp")]
fn bench_ys_kcp_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === ys_kcp throughput (UDP localhost) ===");

    let mut group = c.benchmark_group("ys_kcp_throughput");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    for size in [64, 256, 1024, 4096, 8192] {
        group.throughput(Throughput::Bytes(size as u64));
        let name = format!("echo_{}b", size);
        let group_id = "ys_kcp_throughput";
        let func_id = name.clone();
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
        let state: RefCell<Option<YsKcpPersistentState>> = RefCell::new(None);
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut opt = state.borrow_mut();
                if opt.is_none() {
                    *opt = Some(ys_kcp_init_persistent(current_ms()).expect("ys_kcp init failed"));
                }
                let n = ys_kcp_persistent_roundtrip(opt.as_mut().unwrap(), &payload)
                    .unwrap_or_else(|e| {
                        eprintln!("[bench] warning: ys_kcp throughput roundtrip failed: {e}");
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

#[cfg(feature = "ys-kcp")]
fn bench_ys_kcp_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === ys_kcp latency (UDP echo RTT 64B) ===");

    let mut group = c.benchmark_group("ys_kcp_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    group.bench_function("echo_rtt_64b", |b| {
        let payload = [0xABu8; 64];
        b.iter(|| {
            let base_ms = current_ms();
            let n = ys_kcp_echo_roundtrip_udp(&payload, base_ms, None).unwrap_or(0);
            let ok = n > 0;
            record_bench_success("ys_kcp_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    group.finish();
}

#[cfg(feature = "ys-kcp")]
fn bench_ys_kcp_concurrent(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === ys_kcp concurrent (5/10/20 × 10 msgs over UDP) ===");

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ys_kcp_concurrent");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    let payload = [0xABu8; 64];
    for num_conns in [5, 10, 20] {
        let name = format!("{}_connections_10_msgs", num_conns);
        let group_id = "ys_kcp_concurrent";
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
                                    ys_kcp_echo_roundtrip_udp(&payload, base, None)
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

#[cfg(feature = "kcp-sys")]
async fn kcp_sys_echo_roundtrip(payload: &[u8]) -> Result<usize> {
    use bytes::{Bytes, BytesMut};
    use kcp_sys::endpoint::KcpEndpoint;
    use kcp_sys::packet_def::KcpPacket;
    use kcp_sys::stream::KcpStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UdpSocket;
    use tokio::sync::Mutex;

    // Two endpoints: A (client) and B (server). Packets are routed through real UDP loopback
    // sockets so the kernel network stack is exercised, matching kcp_deepseek/kcprs/ys-kcp.
    let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let server_addr = server_sock.local_addr()?;
    let client_addr = client_sock.local_addr()?;

    let mut ep_a = KcpEndpoint::new();
    let mut ep_b = KcpEndpoint::new();
    let mut out_a = ep_a.output_receiver().take().expect("output receiver");
    let mut out_b = ep_b.output_receiver().take().expect("output receiver");
    let input_a = ep_a.input_sender();
    let input_b = ep_b.input_sender();
    ep_a.run().await;
    ep_b.run().await;

    // Spawn all forwarding tasks with abort handles so they are torn down when this function
    // returns, preventing zombie tasks from interfering with the next iteration's sockets.
    let mut task_handles = Vec::new();

    // ep_a output → UDP → server socket
    let s = Arc::clone(&client_sock);
    task_handles.push(tokio::spawn(async move {
        while let Some(pkt) = out_a.recv().await {
            let bytes: Bytes = pkt.into();
            let _ = s.send_to(&bytes, server_addr).await;
        }
    }));
    // ep_b output → UDP → client socket
    let s = Arc::clone(&server_sock);
    task_handles.push(tokio::spawn(async move {
        while let Some(pkt) = out_b.recv().await {
            let bytes: Bytes = pkt.into();
            let _ = s.send_to(&bytes, client_addr).await;
        }
    }));
    // server socket recv → ep_b input
    let s = Arc::clone(&server_sock);
    let ib = input_b.clone();
    task_handles.push(tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        loop {
            if let Ok((n, _)) = s.recv_from(&mut buf).await {
                let _ = ib.send(KcpPacket::from(BytesMut::from(&buf[..n]))).await;
            }
        }
    }));
    // client socket recv → ep_a input
    let s = Arc::clone(&client_sock);
    let ia = input_a.clone();
    task_handles.push(tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        loop {
            if let Ok((n, _)) = s.recv_from(&mut buf).await {
                let _ = ia.send(KcpPacket::from(BytesMut::from(&buf[..n]))).await;
            }
        }
    }));

    let ep_b = Arc::new(Mutex::new(ep_b));
    let ep_b_echo = Arc::clone(&ep_b);
    task_handles.push(tokio::spawn(async move {
        let conn_id = match ep_b_echo.lock().await.accept().await {
            Ok(c) => c,
            Err(_) => return,
        };
        let mut stream = match KcpStream::new(&*ep_b_echo.lock().await, conn_id) {
            Some(s) => s,
            None => return,
        };
        let mut buf = vec![0u8; 65536];
        while let Ok(n) = stream.read(&mut buf).await {
            if n == 0 {
                break;
            }
            let _ = stream.write_all(&buf[..n]).await;
            let _ = stream.flush().await;
        }
    }));

    let conn_id = ep_a
        .connect(Duration::from_secs(5), 0, 0, Bytes::new())
        .await?;
    let mut stream = KcpStream::new(&ep_a, conn_id).expect("KcpStream");
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;

    // Abort all forwarding/echo tasks so their sockets are released immediately,
    // preventing interference with the next iteration.
    for h in task_handles {
        h.abort();
    }

    Ok(buf.len())
}

/// Persistent state for kcp_sys throughput bench: endpoints, forwarding tasks, and server echo
/// task kept alive across b.iter() calls so only per-stream connect is in the hot path.
#[cfg(feature = "kcp-sys")]
struct KcpSysPersistentState {
    ep_a: kcp_sys::endpoint::KcpEndpoint,
    /// Keep task handles alive so forwarding and server echo tasks keep running.
    _task_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "kcp-sys")]
async fn kcp_sys_init_persistent() -> Result<KcpSysPersistentState> {
    use bytes::{Bytes, BytesMut};
    use kcp_sys::endpoint::KcpEndpoint;
    use kcp_sys::packet_def::KcpPacket;
    use kcp_sys::stream::KcpStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UdpSocket;

    let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let server_addr = server_sock.local_addr()?;
    let client_addr = client_sock.local_addr()?;

    let mut ep_a = KcpEndpoint::new();
    let mut ep_b = KcpEndpoint::new();
    let mut out_a = ep_a.output_receiver().take().expect("output receiver");
    let mut out_b = ep_b.output_receiver().take().expect("output receiver");
    let input_a = ep_a.input_sender();
    let input_b = ep_b.input_sender();
    ep_a.run().await;
    ep_b.run().await;

    let mut task_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // ep_a output → UDP → server
    let s = Arc::clone(&client_sock);
    task_handles.push(tokio::spawn(async move {
        while let Some(pkt) = out_a.recv().await {
            let bytes: Bytes = pkt.into();
            let _ = s.send_to(&bytes, server_addr).await;
        }
    }));
    // ep_b output → UDP → client
    let s = Arc::clone(&server_sock);
    task_handles.push(tokio::spawn(async move {
        while let Some(pkt) = out_b.recv().await {
            let bytes: Bytes = pkt.into();
            let _ = s.send_to(&bytes, client_addr).await;
        }
    }));
    // server socket recv → ep_b input
    let s = Arc::clone(&server_sock);
    let ib = input_b.clone();
    task_handles.push(tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        loop {
            if let Ok((n, _)) = s.recv_from(&mut buf).await {
                let _ = ib.send(KcpPacket::from(BytesMut::from(&buf[..n]))).await;
            }
        }
    }));
    // client socket recv → ep_a input
    let s = Arc::clone(&client_sock);
    let ia = input_a.clone();
    task_handles.push(tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];
        loop {
            if let Ok((n, _)) = s.recv_from(&mut buf).await {
                let _ = ia.send(KcpPacket::from(BytesMut::from(&buf[..n]))).await;
            }
        }
    }));

    // Persistent server echo task: loops on accept() for the lifetime of this state.
    // ep_b is moved directly into the task — no Mutex needed because accept() and
    // KcpStream::new() are both called from the same task without re-entrancy.
    // KcpStream::new() only borrows ep_b to extract channel handles, so it is safe
    // to call immediately after accept() returns without holding any lock.
    task_handles.push(tokio::spawn(async move {
        loop {
            let conn_id = match ep_b.accept().await {
                Ok(c) => c,
                Err(_) => break,
            };
            let mut stream = match KcpStream::new(&ep_b, conn_id) {
                Some(s) => s,
                None => continue,
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    let _ = stream.write_all(&buf[..n]).await;
                    let _ = stream.flush().await;
                }
            });
        }
    }));

    Ok(KcpSysPersistentState {
        ep_a,
        _task_handles: task_handles,
    })
}

/// One roundtrip on a pre-existing ep_a. Opens a new KcpStream (cheap) per iteration;
/// endpoints, sockets, and forwarding tasks are reused.
#[cfg(feature = "kcp-sys")]
async fn kcp_sys_persistent_roundtrip(
    state: &mut KcpSysPersistentState,
    payload: &[u8],
) -> Result<usize> {
    use bytes::Bytes;
    use kcp_sys::stream::KcpStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let conn_id = state
        .ep_a
        .connect(Duration::from_secs(5), 0, 0, Bytes::new())
        .await?;
    let mut stream = KcpStream::new(&state.ep_a, conn_id).expect("KcpStream");
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
    tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
    let mut buf = vec![0u8; payload.len()];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;
    Ok(buf.len())
}

#[cfg(feature = "kcp-sys")]
fn bench_kcp_sys_throughput(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_sys throughput (in-process echo) ===");

    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("kcp_sys_throughput");
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
        let payload: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
        let group_id = "kcp_sys_throughput";
        let func_id = name.clone();
        let state: RefCell<Option<KcpSysPersistentState>> = RefCell::new(None);
        group.bench_function(name, |b| {
            b.iter(|| {
                let n = rt.block_on(async {
                    let mut opt = state.borrow_mut();
                    if opt.is_none() {
                        match kcp_sys_init_persistent().await {
                            Ok(s) => {
                                *opt = Some(s);
                            }
                            Err(e) => {
                                eprintln!("[bench] warning: kcp_sys init failed: {e}");
                                return 0;
                            }
                        }
                    }
                    match kcp_sys_persistent_roundtrip(opt.as_mut().unwrap(), &payload).await {
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("[bench] warning: kcp_sys throughput roundtrip failed: {e}");
                            *opt = None;
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
    group.finish();
}

#[cfg(feature = "kcp-sys")]
fn bench_kcp_sys_latency(c: &mut Criterion) {
    init_bench_logging();
    eprintln!("[bench] === kcp_sys latency (in-process echo 64B) ===");

    let rt = Runtime::new().unwrap();
    let payload = [0xABu8; 64];
    let mut group = c.benchmark_group("kcp_sys_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(30);
    group.bench_function("echo_rtt_64b", |b| {
        b.iter(|| {
            let n = rt.block_on(kcp_sys_echo_roundtrip(&payload)).unwrap_or(0);
            let ok = n > 0;
            record_bench_success("kcp_sys_latency", "echo_rtt_64b", ok);
            black_box(n);
        })
    });
    group.finish();
}

fn bench_noop(_: &mut Criterion) {}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_throughput,
        bench_latency,
        bench_concurrent_connections,
        bench_quinn_throughput,
        bench_quinn_latency,
        bench_quinn_concurrent,
        bench_kcp_deepseek_throughput,
        bench_kcp_deepseek_latency,
        bench_kcp_deepseek_concurrent,
        bench_kcprs_throughput,
        bench_kcprs_latency,
        bench_kcprs_concurrent
}

#[cfg(feature = "ys-kcp")]
criterion_group! {
    name = ys_kcp_benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_ys_kcp_throughput,
        bench_ys_kcp_latency,
        bench_ys_kcp_concurrent
}

#[cfg(not(feature = "ys-kcp"))]
criterion_group! {
    name = ys_kcp_benches;
    config = Criterion::default();
    targets = bench_noop
}

#[cfg(feature = "kcp-sys")]
criterion_group! {
    name = kcp_sys_benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_kcp_sys_throughput,
        bench_kcp_sys_latency
}

#[cfg(not(feature = "kcp-sys"))]
criterion_group! {
    name = kcp_sys_benches;
    config = Criterion::default();
    targets = bench_noop
}

// Slipstream-picoquic: real QUIC over UDP (echo server + client), same conditions as quinn/kcp_tokio.
// Each bench group uses a separate port because the C picoquic_packet_loop runs on a dedicated
// blocking thread that holds the UDP socket until the process exits — dropping the shutdown_tx
// only stops the async accept loop, not the C thread. Reusing the same port across groups causes
// the second bind to fail with PICOQUIC_ERROR_UNEXPECTED_ERROR (ret=1051).
#[cfg(feature = "slipstream-picoquic")]
const SLIPSTREAM_BENCH_PORT_THROUGHPUT: u16 = 12446;
#[cfg(feature = "slipstream-picoquic")]
const SLIPSTREAM_BENCH_PORT_LATENCY: u16 = 12447;
#[cfg(feature = "slipstream-picoquic")]
const SLIPSTREAM_BENCH_PORT_CONCURRENT: [u16; 3] = [12448, 12449, 12450];

/// Returns (server_addr, shutdown_tx). Caller must keep shutdown_tx alive or the server exits.
/// When cert_key_paths is Some, use those PEM paths (caller keeps files alive); required so client can trust the same cert.
/// Each bench group must pass a distinct port — the C picoquic_packet_loop thread holds the UDP
/// socket for the process lifetime so the same port cannot be reused across groups.
#[cfg(feature = "slipstream-picoquic")]
fn start_slipstream_echo_server_in_background(
    rt: &Runtime,
    port: u16,
    cert_key_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
) -> (SocketAddr, tokio::sync::broadcast::Sender<()>) {
    use proxy_server::transport::run_slipstream_picoquic_server;
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

#[cfg(feature = "slipstream-picoquic")]
async fn slipstream_echo_roundtrip(
    addr: SocketAddr,
    payload: &[u8],
    trusted_cert_path: Option<&std::path::Path>,
) -> Result<usize> {
    use proxy_server::transport::slipstream_connect_stream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
#[cfg(feature = "slipstream-picoquic")]
async fn slipstream_echo_roundtrips_on_stream(
    stream: &mut proxy_server::transport::ProxyStream,
    payload: &[u8],
    count: u32,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; payload.len()];
    for _ in 0..count {
        tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload)).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.flush()).await??;
        tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut buf)).await??;
    }
    Ok(())
}

/// One connection, N echo roundtrips on the same stream. Avoids connection storms in concurrent bench.
#[cfg(feature = "slipstream-picoquic")]
async fn slipstream_echo_roundtrips_one_connection(
    addr: SocketAddr,
    payload: &[u8],
    count: u32,
    trusted_cert_path: Option<&std::path::Path>,
) -> Result<()> {
    use proxy_server::transport::slipstream_connect_stream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

#[cfg(feature = "slipstream-picoquic")]
criterion_group! {
    name = slipstream_benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30);
    targets = bench_slipstream_throughput,
        bench_slipstream_latency,
        bench_slipstream_concurrent
}

#[cfg(feature = "slipstream-picoquic")]
fn bench_slipstream_throughput(c: &mut Criterion) {
    use proxy_server::transport::create_slipstream_pem_files;
    init_bench_logging();
    eprintln!(
        "[bench] === slipstream-picoquic throughput (QUIC over UDP echo, 5 payload sizes) ==="
    );
    let (cert_file, key_file) = create_slipstream_pem_files().expect("slipstream PEM files");
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();
    proxy_server::transport::ensure_slipstream_picoquic_tls_init();
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
        let stream = std::cell::RefCell::new(None::<proxy_server::transport::ProxyStream>);
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut opt = stream.borrow_mut();
                if opt.is_none() {
                    // Build and run connect entirely inside runtime so timeout/sleep run in reactor context.
                    *opt = Some(
                        rt.block_on(async {
                            tokio::time::timeout(
                                SLIPSTREAM_CONNECT_TIMEOUT,
                                proxy_server::transport::slipstream_connect_stream(
                                    addr,
                                    "localhost",
                                    Some(cert_path_ref),
                                ),
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

#[cfg(feature = "slipstream-picoquic")]
fn bench_slipstream_latency(c: &mut Criterion) {
    use proxy_server::transport::create_slipstream_pem_files;
    init_bench_logging();
    eprintln!("[bench] === slipstream-picoquic latency (QUIC over UDP echo RTT 64B) ===");
    let (cert_file, key_file) = create_slipstream_pem_files().expect("slipstream PEM files");
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();
    proxy_server::transport::ensure_slipstream_picoquic_tls_init();
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

#[cfg(feature = "slipstream-picoquic")]
fn bench_slipstream_concurrent(c: &mut Criterion) {
    use proxy_server::transport::create_slipstream_pem_files;
    init_bench_logging();
    eprintln!(
        "[bench] === slipstream-picoquic concurrent (5/10/20 connections × 10 msgs over UDP) ==="
    );
    let (cert_file, key_file) = create_slipstream_pem_files().expect("slipstream PEM files");
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();
    proxy_server::transport::ensure_slipstream_picoquic_tls_init();
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

#[cfg(not(feature = "slipstream-picoquic"))]
criterion_group! {
    name = slipstream_benches;
    config = Criterion::default();
    targets = bench_noop
}

criterion_main!(benches, ys_kcp_benches, kcp_sys_benches, slipstream_benches);
