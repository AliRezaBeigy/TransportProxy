//! Quinn (QUIC/TLS) transport: stream wrapper, TLS config, and connection helpers.

use anyhow::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

use super::ProxyStream;

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
