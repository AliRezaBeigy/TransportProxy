//! kcp-sys benchmarks (optional, requires libclang): throughput and latency.

#![cfg(feature = "kcp-sys")]

use criterion::{black_box, Criterion, Throughput};
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::common::{init_bench_logging, record_bench_success, IO_TIMEOUT};

pub async fn kcp_sys_echo_roundtrip(payload: &[u8]) -> anyhow::Result<usize> {
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
pub struct KcpSysPersistentState {
    pub ep_a: kcp_sys::endpoint::KcpEndpoint,
    /// Keep task handles alive so forwarding and server echo tasks keep running.
    pub _task_handles: Vec<tokio::task::JoinHandle<()>>,
}

pub async fn kcp_sys_init_persistent() -> anyhow::Result<KcpSysPersistentState> {
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
pub async fn kcp_sys_persistent_roundtrip(
    state: &mut KcpSysPersistentState,
    payload: &[u8],
) -> anyhow::Result<usize> {
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

pub fn bench_kcp_sys_throughput(c: &mut Criterion) {
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

pub fn bench_kcp_sys_latency(c: &mut Criterion) {
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
