//! Transport abstraction: KCP (kcp-tokio), QUIC (quinn), and optional slipstream-picoquic.

use anyhow::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

/// User-selectable transport for proxy server and client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Transport {
    /// KCP over UDP (kcp-tokio). Default.
    #[default]
    KcpTokio,
    /// QUIC over UDP with TLS (quinn).
    Quinn,
    /// QUIC over UDP via slipstream-picoquic C library (optional feature).
    #[cfg(feature = "slipstream-picoquic")]
    SlipstreamPicoQuic,
}

impl std::str::FromStr for Transport {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kcp-tokio" | "kcptokio" | "kcp" => Ok(Transport::KcpTokio),
            "quinn" | "quic" => Ok(Transport::Quinn),
            #[cfg(feature = "slipstream-picoquic")]
            "slipstream-picoquic" | "slipstream" | "picoquic" => Ok(Transport::SlipstreamPicoQuic),
            _ => {
                #[cfg(feature = "slipstream-picoquic")]
                return Err(format!(
                    "unknown transport '{}'; use kcp-tokio, quinn, or slipstream-picoquic",
                    s
                ));
                #[cfg(not(feature = "slipstream-picoquic"))]
                {
                    if s.to_lowercase().contains("slipstream")
                        || s.to_lowercase().contains("picoquic")
                    {
                        return Err(format!(
                            "transport '{}' requires building with --features slipstream-picoquic; see README",
                            s
                        ));
                    }
                    Err(format!("unknown transport '{}'; use kcp-tokio or quinn", s))
                }
            }
        }
    }
}

impl Transport {
    /// List of transport names for help text.
    pub const fn available() -> &'static [&'static str] {
        #[cfg(feature = "slipstream-picoquic")]
        return &["kcp-tokio", "quinn", "slipstream-picoquic"];
        #[cfg(not(feature = "slipstream-picoquic"))]
        return &["kcp-tokio", "quinn"];
    }
}

/// A stream that can be used with relay_bidirectional.
/// Wraps either a KCP stream, a QUIC (quinn) stream, or slipstream-picoquic stream.
pub enum ProxyStream {
    KcpTokio(kcp_tokio::KcpStream),
    Quinn(QuinnBiStream),
    #[cfg(feature = "slipstream-picoquic")]
    SlipstreamPicoQuic(SlipstreamPicoQuicStream),
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_flush(cx),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ProxyStream::KcpTokio(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            ProxyStream::Quinn(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl ProxyStream {
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        match self {
            ProxyStream::KcpTokio(s) => Some(*s.peer_addr()),
            ProxyStream::Quinn(_) => None,
            #[cfg(feature = "slipstream-picoquic")]
            ProxyStream::SlipstreamPicoQuic(s) => s.peer_addr(),
        }
    }
}

#[cfg(feature = "slipstream-picoquic")]
/// Stream wrapper for slipstream-picoquic (placeholder or C API bridge).
pub struct SlipstreamPicoQuicStream {
    inner: SlipstreamStreamInner,
}

#[cfg(feature = "slipstream-picoquic")]
enum SlipstreamStreamInner {
    Placeholder(tokio::io::DuplexStream),
    Bridged {
        recv: slipstream_client::SlipstreamRecvHalf,
        send: slipstream_client::SlipstreamSendHalf,
    },
}

#[cfg(feature = "slipstream-picoquic")]
impl SlipstreamPicoQuicStream {
    pub fn new(inner: tokio::io::DuplexStream) -> Self {
        Self {
            inner: SlipstreamStreamInner::Placeholder(inner),
        }
    }
    pub fn new_bridged(
        recv: slipstream_client::SlipstreamRecvHalf,
        send: slipstream_client::SlipstreamSendHalf,
    ) -> Self {
        Self {
            inner: SlipstreamStreamInner::Bridged { recv, send },
        }
    }
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        None
    }
}

#[cfg(feature = "slipstream-picoquic")]
impl AsyncRead for SlipstreamPicoQuicStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.get_mut().inner {
            SlipstreamStreamInner::Placeholder(inner) => {
                std::pin::Pin::new(inner).poll_read(cx, buf)
            }
            SlipstreamStreamInner::Bridged { recv, .. } => {
                std::pin::Pin::new(recv).poll_read(cx, buf)
            }
        }
    }
}

#[cfg(feature = "slipstream-picoquic")]
impl AsyncWrite for SlipstreamPicoQuicStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut self.get_mut().inner {
            SlipstreamStreamInner::Placeholder(inner) => {
                std::pin::Pin::new(inner).poll_write(cx, buf)
            }
            SlipstreamStreamInner::Bridged { send, .. } => {
                std::pin::Pin::new(send).poll_write(cx, buf)
            }
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.get_mut().inner {
            SlipstreamStreamInner::Placeholder(inner) => std::pin::Pin::new(inner).poll_flush(cx),
            SlipstreamStreamInner::Bridged { send, .. } => std::pin::Pin::new(send).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut self.get_mut().inner {
            SlipstreamStreamInner::Placeholder(inner) => {
                std::pin::Pin::new(inner).poll_shutdown(cx)
            }
            SlipstreamStreamInner::Bridged { send, .. } => {
                std::pin::Pin::new(send).poll_shutdown(cx)
            }
        }
    }
}

/// Wraps quinn's SendStream + RecvStream as a single AsyncRead + AsyncWrite.
pub struct QuinnBiStream {
    recv: quinn::RecvStream,
    send: quinn::SendStream,
}

impl QuinnBiStream {
    pub fn new(recv: quinn::RecvStream, send: quinn::SendStream) -> Self {
        Self { recv, send }
    }
}

impl AsyncRead for QuinnBiStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().recv).poll_read(cx, buf)
    }
}

fn quinn_err_to_io(e: impl std::fmt::Display + Send + Sync + 'static) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

impl AsyncWrite for QuinnBiStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match std::pin::Pin::new(&mut self.get_mut().send).poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => std::task::Poll::Ready(Ok(n)),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(quinn_err_to_io(e))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.get_mut().send).poll_flush(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(quinn_err_to_io(e))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.get_mut().send).poll_shutdown(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(quinn_err_to_io(e))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

// --- Quinn server config (self-signed cert) ---

const QUINN_ALPN: &[u8] = b"proxy-server";

/// Build quinn ServerConfig with a generated self-signed certificate.
/// Returns (ServerConfig, cert_der for clients that need to verify, _key_der).
pub fn quinn_server_config() -> Result<(
    quinn::ServerConfig,
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let key_pair = rcgen::KeyPair::generate(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| anyhow::anyhow!("rcgen key: {}", e))?;
    let key_der_bytes = key_pair.serialize_der();
    let mut params = rcgen::CertificateParams::default();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
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
    let mut server_config = quinn::ServerConfig::with_crypto(std::sync::Arc::new(quic_server));
    let transport = std::sync::Arc::get_mut(&mut server_config.transport).unwrap();
    transport.max_concurrent_uni_streams(0_u8.into());
    Ok((server_config, cert_der, key_der))
}

/// Build quinn ClientConfig that trusts the given server cert (e.g. from quinn_server_config).
pub fn quinn_client_config_with_cert(
    cert_der: &rustls::pki_types::CertificateDer<'static>,
) -> Result<quinn::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der.clone())?;
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![QUINN_ALPN.to_vec()];
    Ok(quinn::ClientConfig::new(std::sync::Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
    )))
}

/// Build quinn ClientConfig that accepts any server certificate (insecure; for local/dev use).
pub fn quinn_client_config_insecure() -> Result<quinn::ClientConfig> {
    #[derive(Debug)]
    struct AcceptAnyVerifier;
    impl rustls::client::danger::ServerCertVerifier for AcceptAnyVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer,
            _intermediates: &[rustls::pki_types::CertificateDer],
            _server_name: &rustls::pki_types::ServerName,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            vec![
                rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                rustls::SignatureScheme::ED25519,
            ]
        }
    }
    let mut client_crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnyVerifier))
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![QUINN_ALPN.to_vec()];
    Ok(quinn::ClientConfig::new(std::sync::Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
    )))
}

/// Connect to a quinn server and open one bidirectional stream.
/// Uses insecure client config (accepts any cert).
pub async fn quinn_connect_stream(
    server_addr: SocketAddr,
    server_name: &str,
    client_config: quinn::ClientConfig,
) -> Result<ProxyStream> {
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse::<SocketAddr>().unwrap())?;
    endpoint.set_default_client_config(client_config);
    let connecting = endpoint.connect(server_addr, server_name)?;
    let conn = connecting.await?;
    let (send, recv) = conn.open_bi().await?;
    Ok(ProxyStream::Quinn(QuinnBiStream::new(recv, send)))
}

// --- Slipstream-picoquic (optional) ---

#[cfg(feature = "slipstream-picoquic")]
#[allow(dead_code)]
const SLIPSTREAM_ALPN: &[u8] = b"proxy-server";

#[cfg(feature = "slipstream-picoquic")]
mod slipstream_client {
    use super::*;
    use libc::size_t;
    use slipstream_picoquic_sys::picoquic::*;
    use std::ffi::CString;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
    use std::sync::mpsc;
    use tokio::sync::mpsc as tokio_mpsc;

    const ALPN: &[u8] = b"proxy-server";
    /// Sentinel for "stream not yet set"; QUIC stream ID 0 is valid for the first client-initiated bidirectional stream.
    const STREAM_ID_NOT_SET: u64 = u64::MAX;

    struct ClientBridgeCtx {
        recv_tx: tokio_mpsc::Sender<Vec<u8>>,
        send_rx: mpsc::Receiver<Vec<u8>>,
        /// When the C loop consumes from send_rx it sends () here so the async writer task is woken.
        wake_tx: tokio_mpsc::Sender<()>,
        cnx: AtomicPtr<picoquic_cnx_t>,
        stream_id: std::sync::atomic::AtomicU64,
        disconnected: AtomicBool,
        ready_tx: Option<tokio_mpsc::Sender<()>>,
        /// Data peeked in time_check; drain this first in after_send so we don't wait up to 1s for next wake.
        peek_buf: std::cell::UnsafeCell<Option<Vec<u8>>>,
    }

    /// SAFETY: callback_ctx is a valid pointer to ClientBridgeCtx for the duration of the picoquic
    /// callbacks; picoquic does not invoke callbacks after picoquic_packet_loop returns.
    unsafe extern "C" fn stream_callback(
        cnx: *mut picoquic_cnx_t,
        _stream_id: u64,
        bytes: *mut u8,
        length: size_t,
        fin_or_event: picoquic_call_back_event_t,
        callback_ctx: *mut std::ffi::c_void,
        _stream_ctx: *mut std::ffi::c_void,
    ) -> c_int {
        let ctx = &*(callback_ctx as *const ClientBridgeCtx);
        use picoquic_call_back_event_t::*;
        match fin_or_event {
            picoquic_callback_ready => {
                ctx.cnx.store(cnx, Ordering::Release);
                let stream_id_local = picoquic_get_next_local_stream_id(cnx, 0);
                if picoquic_mark_active_stream(cnx, stream_id_local, 1, std::ptr::null_mut()) != 0 {
                    return -1;
                }
                ctx.stream_id.store(stream_id_local, Ordering::Release);
                if let Some(ref tx) = ctx.ready_tx {
                    let _ = tx.try_send(());
                }
            }
            picoquic_callback_stream_data => {
                if !bytes.is_null() && length > 0 {
                    let slice = std::slice::from_raw_parts(bytes, length);
                    let v = slice.to_vec();
                    if ctx.recv_tx.try_send(v).is_err() {
                        // channel full or closed
                    }
                }
            }
            picoquic_callback_stream_fin => {
                let _ = ctx.recv_tx.try_send(Vec::new());
            }
            picoquic_callback_close
            | picoquic_callback_application_close
            | picoquic_callback_stateless_reset => {
                ctx.disconnected.store(true, Ordering::Release);
                let _ = ctx.recv_tx.try_send(Vec::new());
            }
            picoquic_callback_stream_reset | picoquic_callback_stop_sending => {
                let _ = ctx.recv_tx.try_send(Vec::new());
            }
            _ => {}
        }
        0
    }

    /// SAFETY: callback_ctx is a valid pointer to ClientBridgeCtx; only used until picoquic_packet_loop returns.
    unsafe extern "C" fn loop_callback(
        _quic: *mut picoquic_quic_t,
        cb_mode: c_int,
        callback_ctx: *mut std::ffi::c_void,
        arg: *mut std::ffi::c_void,
    ) -> c_int {
        let ctx = &*(callback_ctx as *const ClientBridgeCtx);
        if cb_mode == PICOQUIC_PACKET_LOOP_READY {
            if !arg.is_null() {
                let opt = &mut *(arg as *mut picoquic_packet_loop_options_t);
                opt._flags = 1; /* do_time_check */
            }
            return 0;
        }
        if cb_mode == PICOQUIC_PACKET_LOOP_TIME_CHECK {
            if !arg.is_null()
                && !ctx.cnx.load(Ordering::Acquire).is_null()
                && ctx.stream_id.load(Ordering::Acquire) != STREAM_ID_NOT_SET
            {
                let peek = &mut *ctx.peek_buf.get();
                if peek.is_none() {
                    if let Ok(data) = ctx.send_rx.try_recv() {
                        *peek = Some(data);
                    }
                }
                if peek.is_some() {
                    let time_arg = &mut *(arg as *mut packet_loop_time_check_arg_t);
                    time_arg.delta_t = 2000; /* 2ms so loop wakes soon and drains send_rx */
                }
            }
            return 0;
        }
        if cb_mode == PICOQUIC_PACKET_LOOP_AFTER_RECEIVE
            || cb_mode == PICOQUIC_PACKET_LOOP_AFTER_SEND
        {
            let cnx = ctx.cnx.load(Ordering::Acquire);
            let stream_id = ctx.stream_id.load(Ordering::Acquire);
            if !cnx.is_null() && stream_id != STREAM_ID_NOT_SET {
                let mut consumed = false;
                let peek = &mut *ctx.peek_buf.get();
                if let Some(data) = peek.take() {
                    consumed = true;
                    let fin = if data.is_empty() { 1 } else { 0 };
                    let len = data.len();
                    if len == 0 && fin != 0 {
                        let _ = picoquic_add_to_stream(cnx, stream_id, std::ptr::null(), 0, 1);
                    } else if !data.is_empty() {
                        let ret = picoquic_add_to_stream(cnx, stream_id, data.as_ptr(), len, fin);
                        if ret != 0 {
                            *peek = Some(data);
                        }
                    }
                }
                if consumed {
                    let _ = ctx.wake_tx.try_send(());
                }
                while consumed {
                    match ctx.send_rx.try_recv() {
                        Ok(data) => {
                            consumed = true;
                            let fin = if data.is_empty() { 1 } else { 0 };
                            let len = data.len();
                            if len == 0 && fin != 0 {
                                let _ =
                                    picoquic_add_to_stream(cnx, stream_id, std::ptr::null(), 0, 1);
                            } else if !data.is_empty() {
                                let ret =
                                    picoquic_add_to_stream(cnx, stream_id, data.as_ptr(), len, fin);
                                if ret != 0 {
                                    break;
                                }
                            }
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                    }
                }
                if consumed {
                    let _ = ctx.wake_tx.try_send(());
                }
            }
        }
        0
    }

    pub(super) async fn connect_stream(
        server_addr: SocketAddr,
        server_name: &str,
        trusted_cert_path: Option<std::path::PathBuf>,
    ) -> Result<ProxyStream> {
        let port = server_addr.port() as i32;
        let (recv_tx, recv_rx) = tokio_mpsc::channel::<Vec<u8>>(256);
        let (send_tx, send_rx) = mpsc::sync_channel::<Vec<u8>>(64);
        let (wake_tx, wake_rx) = tokio_mpsc::channel::<()>(64);
        let (ready_tx, mut ready_rx) = tokio_mpsc::channel::<()>(1);

        let host = server_addr.ip().to_string();
        let server_name = server_name.to_string();

        std::thread::spawn(move || {
            let host_c = match CString::new(host.as_bytes()) {
                Ok(c) => c,
                Err(_) => return,
            };
            let alpn_c = match CString::new(ALPN) {
                Ok(c) => c,
                Err(_) => return,
            };
            let sni_c = match CString::new(server_name.as_bytes()) {
                Ok(c) => c,
                Err(_) => return,
            };
            let cert_root_c = trusted_cert_path
                .as_ref()
                .and_then(|p| CString::new(p.to_string_lossy().as_bytes()).ok());

            let ctx = Box::new(ClientBridgeCtx {
                recv_tx,
                send_rx,
                wake_tx,
                cnx: AtomicPtr::new(std::ptr::null_mut()),
                stream_id: std::sync::atomic::AtomicU64::new(STREAM_ID_NOT_SET),
                disconnected: AtomicBool::new(false),
                ready_tx: Some(ready_tx),
                peek_buf: std::cell::UnsafeCell::new(None),
            });
            let ctx_ptr = Box::into_raw(ctx);

            // SAFETY: We pass ctx_ptr to picoquic; it is only used in callbacks until
            // picoquic_packet_loop returns. We then reclaim with from_raw or leave to thread exit.
            let ok = unsafe {
                let mut addr_buf = [0u8; 128];
                let mut is_name: c_int = 0;
                if picoquic_get_server_address(
                    host_c.as_ptr(),
                    port,
                    addr_buf.as_mut_ptr() as *mut _,
                    &mut is_name,
                ) != 0
                {
                    false
                } else {
                    let current_time = picoquic_current_time();
                    let cert_root_ptr = cert_root_c
                        .as_ref()
                        .map(|c| c.as_ptr())
                        .unwrap_or(std::ptr::null());
                    let quic = picoquic_create(
                        1,
                        std::ptr::null(),
                        std::ptr::null(),
                        cert_root_ptr,
                        alpn_c.as_ptr(),
                        Some(stream_callback),
                        ctx_ptr as *mut _,
                        None,
                        std::ptr::null_mut(),
                        std::ptr::null(),
                        current_time,
                        std::ptr::null_mut(),
                        std::ptr::null(),
                        std::ptr::null(),
                        0,
                    );
                    if quic.is_null() {
                        false
                    } else {
                        let cnx = picoquic_create_cnx(
                            quic,
                            picoquic_null_connection_id,
                            picoquic_null_connection_id,
                            addr_buf.as_ptr() as *const _,
                            current_time,
                            0,
                            sni_c.as_ptr(),
                            alpn_c.as_ptr(),
                            1,
                        );
                        if cnx.is_null() {
                            picoquic_free(quic);
                            false
                        } else {
                            picoquic_set_callback(cnx, Some(stream_callback), ctx_ptr as *mut _);
                            if picoquic_start_client_cnx(cnx) != 0 {
                                picoquic_free(quic);
                                false
                            } else {
                                let _ = picoquic_packet_loop(
                                    quic,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    Some(loop_callback),
                                    ctx_ptr as *mut _,
                                );
                                picoquic_free(quic);
                                let _ = Box::from_raw(ctx_ptr);
                                true
                            }
                        }
                    }
                }
            };
            if !ok {
                let _ = unsafe { Box::from_raw(ctx_ptr) };
            }
        });

        tokio::select! {
            _ = ready_rx.recv() => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                anyhow::bail!("slipstream-picoquic connection timeout (30s)");
            }
        }

        let read_half = SlipstreamRecvHalf {
            recv_rx,
            pending: Vec::new(),
            pending_off: 0,
        };
        let write_half = SlipstreamSendHalf {
            send_tx,
            wake_rx,
            pending_send: None,
        };
        let stream = super::SlipstreamPicoQuicStream::new_bridged(read_half, write_half);
        Ok(ProxyStream::SlipstreamPicoQuic(stream))
    }

    use std::task::Poll;
    use tokio::io::{AsyncRead, AsyncWrite};

    pub(crate) struct SlipstreamRecvHalf {
        pub recv_rx: tokio_mpsc::Receiver<Vec<u8>>,
        pub pending: Vec<u8>,
        pub pending_off: usize,
    }

    impl AsyncRead for SlipstreamRecvHalf {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.pending_off < this.pending.len() {
                let n = (this.pending.len() - this.pending_off).min(buf.remaining());
                buf.put_slice(&this.pending[this.pending_off..this.pending_off + n]);
                this.pending_off += n;
                if this.pending_off >= this.pending.len() {
                    this.pending.clear();
                    this.pending_off = 0;
                }
                return Poll::Ready(Ok(()));
            }
            match this.recv_rx.poll_recv(cx) {
                Poll::Ready(Some(v)) => {
                    if v.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    this.pending = v;
                    this.pending_off = 0;
                    let n = this.pending.len().min(buf.remaining());
                    buf.put_slice(&this.pending[..n]);
                    this.pending_off = n;
                    if this.pending_off >= this.pending.len() {
                        this.pending.clear();
                        this.pending_off = 0;
                    }
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(None) => Poll::Ready(Ok(())),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    pub(crate) struct SlipstreamSendHalf {
        pub send_tx: mpsc::SyncSender<Vec<u8>>,
        pub wake_rx: tokio_mpsc::Receiver<()>,
        /// Pending data to send after channel was full; (data, original_len for return).
        pending_send: Option<(Vec<u8>, usize)>,
    }

    impl SlipstreamSendHalf {
        pub(crate) fn new(
            send_tx: mpsc::SyncSender<Vec<u8>>,
            wake_rx: tokio_mpsc::Receiver<()>,
        ) -> Self {
            Self {
                send_tx,
                wake_rx,
                pending_send: None,
            }
        }
    }

    impl AsyncWrite for SlipstreamSendHalf {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let mut to_send = this
                .pending_send
                .take()
                .unwrap_or_else(|| (buf.to_vec(), buf.len()));
            loop {
                match this.send_tx.try_send(to_send.0) {
                    Ok(()) => return Poll::Ready(Ok(to_send.1)),
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        return Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()))
                    }
                    Err(mpsc::TrySendError::Full(owned)) => {
                        this.pending_send = Some((owned, to_send.1));
                        match this.wake_rx.poll_recv(cx) {
                            Poll::Ready(Some(_)) => {
                                to_send = this.pending_send.take().unwrap();
                            }
                            Poll::Ready(None) => {
                                return Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()))
                            }
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                }
            }
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            let _ = self.send_tx.try_send(Vec::new());
            Poll::Ready(Ok(()))
        }
    }
}

#[cfg(feature = "slipstream-picoquic")]
use slipstream_client::connect_stream as slipstream_connect_impl;

#[cfg(feature = "slipstream-picoquic")]
mod slipstream_server {
    use super::*;
    use libc::size_t;
    use slipstream_picoquic_sys::picoquic::*;
    use std::collections::HashMap;
    use std::ffi::CString;
    use std::io::Write;
    use std::os::raw::c_int;
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use tokio::sync::mpsc as tokio_mpsc;

    const ALPN: &[u8] = b"proxy-server";

    /// Cached temp dir so it is the same in all threads (spawned server thread can have different current_dir()).
    static PICOQUIC_PEM_TEMP_DIR: OnceLock<std::path::PathBuf> = OnceLock::new();

    /// On Windows: paths under our temp dir are passed as canonical (long) path so OpenSSL BIO_new_file can open them; other paths use short path for the C runtime.
    #[cfg(windows)]
    fn path_for_openssl(p: &std::path::Path) -> String {
        let input_s = p.to_string_lossy().to_string();
        if let Some(base) = PICOQUIC_PEM_TEMP_DIR.get() {
            if p.starts_with(base) {
                if let Ok(canon) = std::fs::canonicalize(p) {
                    return canon.to_string_lossy().into_owned();
                }
                return input_s;
            }
        }
        use std::os::windows::ffi::{OsStrExt, OsStringExt};
        let wide: Vec<u16> = p
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let len = unsafe {
            windows_sys::Win32::Storage::FileSystem::GetShortPathNameW(
                wide.as_ptr(),
                std::ptr::null_mut(),
                0,
            )
        };
        if len == 0 {
            input_s
        } else {
            let mut buf = vec![0u16; (len + 1) as usize];
            let written = unsafe {
                windows_sys::Win32::Storage::FileSystem::GetShortPathNameW(
                    wide.as_ptr(),
                    buf.as_mut_ptr(),
                    len + 1,
                )
            };
            if written == 0 {
                input_s
            } else {
                buf.truncate(written as usize);
                std::ffi::OsString::from_wide(&buf)
                    .to_string_lossy()
                    .into_owned()
            }
        }
    }

    #[cfg(not(windows))]
    fn path_for_openssl(p: &std::path::Path) -> String {
        p.to_string_lossy().into_owned()
    }

    /// Run first server-style picoquic_create (with cert+key) on the calling thread so OpenSSL file loading is first used there (Windows).
    pub(super) fn ensure_tls_init() {
        unsafe { picoquic_tls_api_init() };
        let alpn_c = match CString::new(ALPN) {
            Ok(c) => c,
            Err(_) => return,
        };
        let (temp_cert, temp_key) = match create_temp_pem_files() {
            Ok(pair) => pair,
            Err(_) => return,
        };
        let cert_path = temp_cert.path().to_path_buf();
        let key_path = temp_key.path().to_path_buf();
        // Persist so file handles are closed; C/OpenSSL BIO_new_file can then open the files (Windows file lock).
        let _ = temp_cert.persist(&cert_path);
        let _ = temp_key.persist(&key_path);
        let cert_s = path_for_openssl(&cert_path);
        let key_s = path_for_openssl(&key_path);
        let (cert_c, key_c) = match (
            CString::new(cert_s.as_bytes()),
            CString::new(key_s.as_bytes()),
        ) {
            (Ok(c), Ok(k)) => (c, k),
            _ => return,
        };
        unsafe extern "C" fn noop_stream_cb(
            _cnx: *mut picoquic_cnx_t,
            _stream_id: u64,
            _bytes: *mut u8,
            _length: size_t,
            _fin_or_event: picoquic_call_back_event_t,
            _callback_ctx: *mut std::ffi::c_void,
            _stream_ctx: *mut std::ffi::c_void,
        ) -> c_int {
            0
        }
        unsafe {
            let current_time = picoquic_current_time();
            let quic = picoquic_create(
                8,
                cert_c.as_ptr(),
                key_c.as_ptr(),
                std::ptr::null(),
                alpn_c.as_ptr(),
                Some(noop_stream_cb),
                std::ptr::null_mut(),
                None,
                std::ptr::null_mut(),
                std::ptr::null(),
                current_time,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
                0,
            );
            if quic.is_null() {
                // TLS context creation failed (e.g. cert_load on Windows if files were held open).
            } else {
                picoquic_free(quic);
            }
        }
    }

    struct ServerConnState {
        cnx: *mut picoquic_cnx_t,
        stream_id: Option<u64>,
        recv_tx: Option<tokio_mpsc::Sender<Vec<u8>>>,
        send_rx: Option<mpsc::Receiver<Vec<u8>>>,
        /// Wake the async writer when we consume from send_rx.
        wake_tx: Option<tokio_mpsc::Sender<()>>,
    }

    struct ServerGlobalCtx {
        accept_tx: tokio_mpsc::UnboundedSender<(
            slipstream_client::SlipstreamRecvHalf,
            slipstream_client::SlipstreamSendHalf,
        )>,
        connections: Mutex<HashMap<usize, ServerConnState>>,
    }

    /// SAFETY: callback_ctx is a valid pointer to ServerGlobalCtx for the duration of the picoquic
    /// callbacks; picoquic does not invoke callbacks after picoquic_packet_loop returns.
    unsafe extern "C" fn server_stream_callback(
        cnx: *mut picoquic_cnx_t,
        stream_id: u64,
        bytes: *mut u8,
        length: size_t,
        fin_or_event: picoquic_call_back_event_t,
        callback_ctx: *mut std::ffi::c_void,
        _stream_ctx: *mut std::ffi::c_void,
    ) -> c_int {
        let ctx = &*(callback_ctx as *const ServerGlobalCtx);
        use picoquic_call_back_event_t::*;
        let key = cnx as usize;
        match fin_or_event {
            picoquic_callback_ready => {
                let mut map = ctx.connections.lock().unwrap();
                map.insert(
                    key,
                    ServerConnState {
                        cnx,
                        stream_id: None,
                        recv_tx: None,
                        send_rx: None,
                        wake_tx: None,
                    },
                );
            }
            picoquic_callback_stream_data => {
                let mut map = ctx.connections.lock().unwrap();
                if let Some(state) = map.get_mut(&key) {
                    if state.stream_id.is_none() {
                        state.stream_id = Some(stream_id);
                        let (recv_tx, recv_rx) = tokio_mpsc::channel(256);
                        let (send_tx, send_rx) = mpsc::sync_channel(64);
                        let (wake_tx, wake_rx) = tokio_mpsc::channel(64);
                        state.recv_tx = Some(recv_tx.clone());
                        state.send_rx = Some(send_rx);
                        state.wake_tx = Some(wake_tx);
                        let read_half = slipstream_client::SlipstreamRecvHalf {
                            recv_rx,
                            pending: Vec::new(),
                            pending_off: 0,
                        };
                        let write_half =
                            slipstream_client::SlipstreamSendHalf::new(send_tx, wake_rx);
                        let _ = ctx.accept_tx.send((read_half, write_half));
                        if !bytes.is_null() && length > 0 {
                            let slice = std::slice::from_raw_parts(bytes, length);
                            let _ = state.recv_tx.as_ref().unwrap().try_send(slice.to_vec());
                        }
                    } else if state.stream_id == Some(stream_id) {
                        if !bytes.is_null() && length > 0 {
                            let slice = std::slice::from_raw_parts(bytes, length);
                            if let Some(ref tx) = state.recv_tx {
                                let _ = tx.try_send(slice.to_vec());
                            }
                        }
                    }
                }
            }
            picoquic_callback_stream_fin => {
                let mut map = ctx.connections.lock().unwrap();
                if let Some(state) = map.get_mut(&key) {
                    if state.stream_id == Some(stream_id) {
                        if let Some(ref tx) = state.recv_tx {
                            let _ = tx.try_send(Vec::new());
                        }
                    }
                }
            }
            picoquic_callback_close
            | picoquic_callback_application_close
            | picoquic_callback_stateless_reset => {
                let mut map = ctx.connections.lock().unwrap();
                map.remove(&key);
            }
            _ => {}
        }
        0
    }

    /// SAFETY: callback_ctx is a valid pointer to ServerGlobalCtx; only used until packet_loop returns.
    unsafe extern "C" fn server_loop_callback(
        _quic: *mut picoquic_quic_t,
        cb_mode: c_int,
        callback_ctx: *mut std::ffi::c_void,
        arg: *mut std::ffi::c_void,
    ) -> c_int {
        let ctx = &*(callback_ctx as *const ServerGlobalCtx);
        if cb_mode == PICOQUIC_PACKET_LOOP_READY && !arg.is_null() {
            let opt = &mut *(arg as *mut picoquic_packet_loop_options_t);
            opt._flags = 1; /* do_time_check: wake periodically to drain send_rx */
        }
        if cb_mode == PICOQUIC_PACKET_LOOP_TIME_CHECK && !arg.is_null() {
            let time_arg = &mut *(arg as *mut packet_loop_time_check_arg_t);
            time_arg.delta_t = 2000; /* 2ms so server wakes to drain send_rx (echo replies) */
        }
        if cb_mode == PICOQUIC_PACKET_LOOP_AFTER_RECEIVE
            || cb_mode == PICOQUIC_PACKET_LOOP_AFTER_SEND
        {
            let mut map = ctx.connections.lock().unwrap();
            for (_key, state) in map.iter_mut() {
                if let Some(sid) = state.stream_id {
                    if let Some(ref send_rx) = state.send_rx {
                        let cnx = state.cnx;
                        let wake_tx = state.wake_tx.as_ref();
                        while let Ok(data) = send_rx.try_recv() {
                            let fin = if data.is_empty() { 1 } else { 0 };
                            let len = data.len();
                            if len == 0 && fin != 0 {
                                let _ = picoquic_add_to_stream(cnx, sid, std::ptr::null(), 0, 1);
                            } else if !data.is_empty() {
                                let _ = picoquic_add_to_stream(cnx, sid, data.as_ptr(), len, fin);
                            }
                            if let Some(tx) = wake_tx {
                                let _ = tx.try_send(());
                            }
                        }
                    }
                }
            }
        }
        0
    }

    /// Directory for temporary PEM files used by picoquic. Cached so all threads use the same path (avoids different current_dir() in spawned thread breaking path_for_openssl).
    /// On Windows we use a subdir of the current dir so OpenSSL's BIO_new_file gets a path it can open (paths under system TEMP can fail cert_load). All temp files go in one folder.
    fn picoquic_pem_temp_dir() -> &'static std::path::Path {
        PICOQUIC_PEM_TEMP_DIR.get_or_init(|| {
            #[cfg(windows)]
            let dir = std::env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .join("proxy_server_picoquic_temp");
            #[cfg(not(windows))]
            let dir = std::env::temp_dir().join("proxy_server_picoquic");
            dir
        })
    }

    /// Creates temporary PEM files for picoquic; files are deleted when the returned handles are dropped.
    pub(super) fn create_temp_pem_files(
    ) -> Result<(tempfile::NamedTempFile, tempfile::NamedTempFile)> {
        let key_pair = rcgen::KeyPair::generate(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| anyhow::anyhow!("rcgen key: {}", e))?;
        let key_pem = key_pair.serialize_pem();
        let mut params = rcgen::CertificateParams::default();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "localhost");
        params.key_pair = Some(key_pair);
        let cert = rcgen::Certificate::from_params(params)
            .map_err(|e| anyhow::anyhow!("rcgen cert: {}", e))?;
        let cert_pem = cert
            .serialize_pem()
            .map_err(|e| anyhow::anyhow!("cert pem: {}", e))?;
        let dir = picoquic_pem_temp_dir();
        std::fs::create_dir_all(&dir).map_err(|e| anyhow::anyhow!("temp dir: {}", e))?;
        let cert_file = tempfile::NamedTempFile::new_in(&dir)
            .map_err(|e| anyhow::anyhow!("temp cert file: {}", e))?;
        let key_file = tempfile::NamedTempFile::new_in(&dir)
            .map_err(|e| anyhow::anyhow!("temp key file: {}", e))?;
        let (mut cert_file, mut key_file) = (cert_file, key_file);
        std::io::Write::write_all(&mut cert_file, cert_pem.as_bytes())
            .map_err(|e| anyhow::anyhow!("write cert: {}", e))?;
        std::io::Write::write_all(&mut key_file, key_pem.as_bytes())
            .map_err(|e| anyhow::anyhow!("write key: {}", e))?;
        cert_file
            .flush()
            .map_err(|e| anyhow::anyhow!("flush cert: {}", e))?;
        key_file
            .flush()
            .map_err(|e| anyhow::anyhow!("flush key: {}", e))?;
        Ok((cert_file, key_file))
    }

    pub(super) async fn run_server(
        listen: SocketAddr,
        accept_timeout: std::time::Duration,
        drain_duration: std::time::Duration,
        upstream: &std::sync::Arc<Option<SocketAddr>>,
        semaphore: &Option<std::sync::Arc<tokio::sync::Semaphore>>,
        shutdown_rx: &mut tokio::sync::broadcast::Receiver<()>,
        cert_key_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
    ) -> Result<()> {
        let (cert_path, key_path, keep_pem_files): (
            std::path::PathBuf,
            std::path::PathBuf,
            Option<(tempfile::NamedTempFile, tempfile::NamedTempFile)>,
        ) = match cert_key_paths {
            Some((c, k)) => (c, k, None),
            None => {
                let (cert_file, key_file) = create_temp_pem_files()?;
                let c = cert_file.path().to_path_buf();
                let k = key_file.path().to_path_buf();
                (c, k, Some((cert_file, key_file)))
            }
        };
        let alpn_c = CString::new(ALPN).map_err(|_| anyhow::anyhow!("ALPN"))?;

        // Unbounded so the picoquic callback never drops a connection when the tokio loop is slow.
        let (accept_tx, mut accept_rx) = tokio_mpsc::unbounded_channel::<(
            slipstream_client::SlipstreamRecvHalf,
            slipstream_client::SlipstreamSendHalf,
        )>();
        let port = listen.port() as i32;
        let alpn_c = alpn_c;
        std::thread::spawn(move || {
            // Initialize TLS backend on this thread so cert/key loading (BIO_new_file etc.) runs on the same thread that did init; required on Windows.
            unsafe { picoquic_tls_api_init() };
            let _keep_pem_files = keep_pem_files;
            // Copy cert/key into temp files in picoquic_pem_temp_dir(); persist so handles are closed and C/OpenSSL can open (Windows file lock).
            let (cert_path_use, key_path_use, _server_pem_files): (
                std::path::PathBuf,
                std::path::PathBuf,
                Option<()>,
            ) = match (std::fs::read(&cert_path), std::fs::read(&key_path)) {
                (Ok(cert_bytes), Ok(key_bytes)) => {
                    let dir = picoquic_pem_temp_dir();
                    let _ = std::fs::create_dir_all(&dir);
                    let cert_file = match tempfile::NamedTempFile::new_in(&dir) {
                        Ok(mut f) => {
                            let _ = std::io::Write::write_all(&mut f, &cert_bytes);
                            let _ = f.flush();
                            f
                        }
                        Err(_) => return,
                    };
                    let key_file = match tempfile::NamedTempFile::new_in(&dir) {
                        Ok(mut f) => {
                            let _ = std::io::Write::write_all(&mut f, &key_bytes);
                            let _ = f.flush();
                            f
                        }
                        Err(_) => return,
                    };
                    let cp = cert_file.path().to_path_buf();
                    let kp = key_file.path().to_path_buf();
                    let _ = cert_file.persist(&cp);
                    let _ = key_file.persist(&kp);
                    (cp, kp, Some(()))
                }
                _ => (cert_path.clone(), key_path.clone(), None),
            };
            let cert_str = path_for_openssl(&cert_path_use);
            let key_str = path_for_openssl(&key_path_use);
            let cert_c = match CString::new(cert_str.as_bytes()) {
                Ok(c) => c,
                Err(_) => return,
            };
            let key_c = match CString::new(key_str.as_bytes()) {
                Ok(c) => c,
                Err(_) => return,
            };
            let global_ctx = Box::new(ServerGlobalCtx {
                accept_tx,
                connections: Mutex::new(HashMap::new()),
            });
            let ctx_ptr = Box::into_raw(global_ctx);
            // SAFETY: ctx_ptr is only used in callbacks until picoquic_packet_loop returns; we reclaim with from_raw.
            let ok = unsafe {
                let current_time = picoquic_current_time();
                let mut quic = picoquic_create(
                    8,
                    cert_c.as_ptr(),
                    key_c.as_ptr(),
                    std::ptr::null(),
                    alpn_c.as_ptr(),
                    Some(server_stream_callback),
                    ctx_ptr as *mut _,
                    None,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    current_time,
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                );
                let mut used_fallback_cert = false;
                if quic.is_null() {
                    // Retry with server-created PEM files in case provided cert/key paths were invalid.
                    if let Ok((cert_file, key_file)) = create_temp_pem_files() {
                        let cp = cert_file.path().to_path_buf();
                        let kp = key_file.path().to_path_buf();
                        let _ = cert_file.persist(&cp);
                        let _ = key_file.persist(&kp);
                        let cert_s = path_for_openssl(&cp);
                        let key_s = path_for_openssl(&kp);
                        if let (Ok(c2), Ok(k2)) = (
                            CString::new(cert_s.as_bytes()),
                            CString::new(key_s.as_bytes()),
                        ) {
                            quic = picoquic_create(
                                8,
                                c2.as_ptr(),
                                k2.as_ptr(),
                                std::ptr::null(),
                                alpn_c.as_ptr(),
                                Some(server_stream_callback),
                                ctx_ptr as *mut _,
                                None,
                                std::ptr::null_mut(),
                                std::ptr::null(),
                                current_time,
                                std::ptr::null_mut(),
                                std::ptr::null(),
                                std::ptr::null(),
                                0,
                            );
                            if !quic.is_null() {
                                used_fallback_cert = true;
                            }
                        }
                    }
                }
                if quic.is_null() {
                    false
                } else {
                    let _ = used_fallback_cert;
                    let _ret = picoquic_packet_loop(
                        quic,
                        port,
                        0,
                        0,
                        0,
                        0,
                        Some(server_loop_callback),
                        ctx_ptr as *mut _,
                    );
                    picoquic_free(quic);
                    true
                }
            };
            if !ok {
                let _ = unsafe { Box::from_raw(ctx_ptr) };
            }
        });

        tracing::info!("Slipstream-picoquic proxy server listening on {}", listen);
        if let Some(u) = upstream.as_ref() {
            tracing::info!("Forwarding to upstream TCP {}", u);
        }
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    tracing::info!("Shutdown signal received");
                    break;
                }
                accept_result = tokio::time::timeout(accept_timeout, accept_rx.recv()) => {
                    match accept_result {
                        Ok(Some((read_half, write_half))) => {
                            let permit = match semaphore {
                                Some(s) => match s.clone().acquire_owned().await {
                                    Ok(p) => Some(p),
                                    Err(_) => {
                                        tracing::warn!("Connection limit semaphore closed");
                                        continue;
                                    }
                                },
                                None => None,
                            };
                            let upstream = std::sync::Arc::clone(upstream);
                            tokio::spawn(async move {
                                let _permit = permit;
                                let stream = super::SlipstreamPicoQuicStream::new_bridged(read_half, write_half);
                                let stream = super::ProxyStream::SlipstreamPicoQuic(stream);
                                if let Err(e) = crate::handle_connection(stream, *upstream).await {
                                    tracing::error!("Connection failed: {}", e);
                                }
                            });
                        }
                        Ok(None) => break,
                        Err(_) => {}
                    }
                }
            }
        }
        tokio::time::sleep(drain_duration).await;
        Ok(())
    }
}

/// Connect to a slipstream-picoquic server and open one bidirectional stream.
/// If the server uses a self-signed certificate, pass its PEM path as `trusted_cert_path`
/// so the client can verify it (e.g. for benchmarks or local dev).
#[cfg(feature = "slipstream-picoquic")]
pub async fn slipstream_connect_stream(
    server_addr: SocketAddr,
    server_name: &str,
    trusted_cert_path: Option<&std::path::Path>,
) -> Result<ProxyStream> {
    slipstream_connect_impl(
        server_addr,
        server_name,
        trusted_cert_path.map(std::path::Path::to_path_buf),
    )
    .await
}

/// Run slipstream-picoquic server.
/// If `cert_key_paths` is `Some((cert_path, key_path))`, use those PEM files (caller keeps them alive).
/// If `None`, temporary PEM files are created and used for the lifetime of the server.
#[cfg(feature = "slipstream-picoquic")]
pub async fn run_slipstream_picoquic_server(
    listen: SocketAddr,
    accept_timeout: std::time::Duration,
    drain_duration: std::time::Duration,
    upstream: &std::sync::Arc<Option<SocketAddr>>,
    semaphore: &Option<std::sync::Arc<tokio::sync::Semaphore>>,
    shutdown_rx: &mut tokio::sync::broadcast::Receiver<()>,
    cert_key_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
) -> Result<()> {
    slipstream_server::run_server(
        listen,
        accept_timeout,
        drain_duration,
        upstream,
        semaphore,
        shutdown_rx,
        cert_key_paths,
    )
    .await
}

/// Create temporary PEM files (cert + key) for slipstream-picoquic server and client trust.
/// Files are deleted when the returned handles are dropped. Use the cert path as
/// `trusted_cert_path` when connecting the client to the server using the same cert.
#[cfg(feature = "slipstream-picoquic")]
pub fn create_slipstream_pem_files() -> Result<(tempfile::NamedTempFile, tempfile::NamedTempFile)> {
    slipstream_server::create_temp_pem_files()
}

/// Run first picoquic TLS init and a dummy create/free on the calling thread. Call before starting the server so the first picoquic_create runs here (helps on Windows).
#[cfg(feature = "slipstream-picoquic")]
pub fn ensure_slipstream_picoquic_tls_init() {
    slipstream_server::ensure_tls_init();
}
