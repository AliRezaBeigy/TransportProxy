//! Integration tests: start KCP or QUIC echo server on a random port, run client, assert echo.

use proxy_server::default_kcp_config;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Spawn a KCP echo server on 127.0.0.1:0, return its SocketAddr.
async fn start_echo_server() -> SocketAddr {
    let config = default_kcp_config();
    let listener = kcp_tokio::KcpListener::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    let addr = *listener.local_addr();

    tokio::spawn(async move {
        let mut listener = listener;
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    let _ = stream.write_all(&buf[..n]).await;
                    let _ = stream.flush().await;
                }
            });
        }
    });

    addr
}

#[tokio::test]
async fn test_kcp_echo_single_message() {
    let addr = start_echo_server().await;
    let config = default_kcp_config();
    let mut stream = kcp_tokio::KcpStream::connect(addr, config).await.unwrap();

    let msg = b"Hello, KCP proxy!";
    stream.write_all(msg).await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, msg.len());
    assert_eq!(&buf[..n], msg);
}

#[tokio::test]
async fn test_kcp_echo_multiple_messages() {
    let addr = start_echo_server().await;
    let config = default_kcp_config();
    let mut stream = kcp_tokio::KcpStream::connect(addr, config).await.unwrap();

    for i in 0..20u32 {
        let msg = format!("message-{}", i);
        stream.write_all(msg.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(n, msg.len());
        assert_eq!(std::str::from_utf8(&buf[..n]).unwrap(), msg);
    }
}

#[tokio::test]
async fn test_kcp_echo_large_message() {
    let addr = start_echo_server().await;
    let config = default_kcp_config();
    let mut stream = kcp_tokio::KcpStream::connect(addr, config).await.unwrap();

    let payload: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
    stream.write_all(&payload).await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = vec![0u8; 8192];
    let mut total = 0;
    while total < payload.len() {
        let n = stream.read(&mut buf[total..]).await.unwrap();
        if n == 0 {
            break;
        }
        total += n;
    }
    assert_eq!(total, payload.len());
    assert_eq!(&buf[..total], &payload[..]);
}

/// Spawn a Quinn echo server on 127.0.0.1:0, return its SocketAddr.
async fn start_quinn_echo_server() -> SocketAddr {
    let (server_config, _cert_der, _key_der) =
        proxy_server::transport::quinn_server_config().unwrap();
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();

    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let conn = match incoming.await {
                Ok(c) => c,
                Err(_) => continue,
            };
            tokio::spawn(async move {
                let (mut send, mut recv) = match conn.accept_bi().await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                const LIMIT: usize = 1024 * 1024;
                if let Ok(data) = recv.read_to_end(LIMIT).await {
                    let _ = send.write_all(&data).await;
                }
                let _ = send.finish();
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            });
        }
    });

    addr
}

fn init_rustls_for_test() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("rustls ring provider");
    });
}

#[tokio::test]
async fn test_quinn_echo_single_message() {
    init_rustls_for_test();
    let addr = start_quinn_echo_server().await;
    let cfg = proxy_server::transport::quinn_client_config_insecure().unwrap();
    let mut stream = proxy_server::transport::quinn_connect_stream(addr, "localhost", cfg)
        .await
        .unwrap();

    let msg = b"Hello, QUIC proxy!";
    stream.write_all(msg).await.unwrap();
    stream.flush().await.unwrap();
    stream.shutdown().await.unwrap();

    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, msg.len());
    assert_eq!(&buf[..n], msg);
}
