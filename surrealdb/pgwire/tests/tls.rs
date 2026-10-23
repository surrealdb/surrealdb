//! TLS negotiation of the Postgres listener, driven the way libpq drives it.

#![recursion_limit = "256"]

use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use rcgen::CertifiedKey;
use surrealdb_core::kvs::Datastore;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_util::sync::CancellationToken;

/// The request code of an `SSLRequest` (80877103).
const SSL_REQUEST_CODE: i32 = 80877103;
/// The protocol version 3.0 of a `StartupMessage`.
const PROTOCOL_3_0: i32 = 196608;
/// Bound on every network exchange, so a server that stops answering fails the
/// test instead of hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on the body of a message read from the server, so a corrupt length fails the test
/// instead of allocating to it.
const MAX_MESSAGE_BODY: usize = 1 << 20;

struct TlsServer {
	addr: SocketAddr,
	shutdown: CancellationToken,
	cert: CertificateDer<'static>,
	_dir: TempDir,
}

async fn start_tls_server() -> TlsServer {
	// The server installs a process-wide provider at startup; a test process
	// must do the same. An `Err` means one is already installed.
	let _ = aws_lc_rs::default_provider().install_default();
	let CertifiedKey {
		cert,
		signing_key,
	} = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
		.expect("generate a self-signed certificate");
	let dir = tempfile::tempdir().expect("temp dir");
	let crt = dir.path().join("server.crt");
	let key = dir.path().join("server.key");
	fs::write(&crt, cert.pem()).expect("write certificate");
	fs::write(&key, signing_key.serialize_pem()).expect("write key");

	let ds = Datastore::new("memory").await.expect("datastore");
	let shutdown = CancellationToken::new();
	let addr = surrealdb_pgwire::start(
		"127.0.0.1:0".parse().expect("loopback address"),
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		Some(crt),
		Some(key),
	)
	.await
	.expect("start the postgres listener with TLS");
	TlsServer {
		addr,
		shutdown,
		cert: cert.der().clone(),
		_dir: dir,
	}
}

/// Negotiate TLS as libpq does: an `SSLRequest`, the server's `S`, then the
/// TLS handshake offering `alpn`.
async fn connect_tls(server: &TlsServer, alpn: &[&[u8]]) -> std::io::Result<TlsStream<TcpStream>> {
	timeout(IO_TIMEOUT, negotiate_tls(server, alpn)).await.expect("the TLS handshake timed out")
}

async fn negotiate_tls(
	server: &TlsServer,
	alpn: &[&[u8]],
) -> std::io::Result<TlsStream<TcpStream>> {
	let mut tcp = TcpStream::connect(server.addr).await?;
	let mut request = Vec::new();
	request.extend_from_slice(&8i32.to_be_bytes());
	request.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
	tcp.write_all(&request).await?;
	let mut answer = [0u8; 1];
	tcp.read_exact(&mut answer).await?;
	assert_eq!(answer[0], b'S', "the server must accept the SSLRequest");

	let mut roots = RootCertStore::empty();
	roots.add(server.cert.clone()).expect("trust the server certificate");
	let mut config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
	config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
	let name = ServerName::try_from("localhost").expect("server name");
	TlsConnector::from(Arc::new(config)).connect(name, tcp).await
}

/// Send a `StartupMessage` over `stream` and read to the first `ReadyForQuery`.
async fn startup(stream: &mut TlsStream<TcpStream>) {
	timeout(IO_TIMEOUT, read_startup(stream)).await.expect("the startup exchange timed out")
}

async fn read_startup(stream: &mut TlsStream<TcpStream>) {
	let mut body = Vec::new();
	body.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
	for (key, value) in [("user", "root"), ("database", "test/test")] {
		body.extend_from_slice(key.as_bytes());
		body.push(0);
		body.extend_from_slice(value.as_bytes());
		body.push(0);
	}
	body.push(0);
	let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
	packet.extend_from_slice(&body);
	stream.write_all(&packet).await.expect("send the startup message");
	loop {
		let kind = stream.read_u8().await.expect("read a message type");
		let len = stream.read_i32().await.expect("read a message length");
		let body_len = len
			.checked_sub(4)
			.and_then(|body| usize::try_from(body).ok())
			.filter(|&body| body <= MAX_MESSAGE_BODY)
			.expect("a message length covers its own field and stays within the bound");
		let mut payload = vec![0u8; body_len];
		stream.read_exact(&mut payload).await.expect("read a message body");
		match kind {
			b'Z' => return,
			b'E' => panic!("startup failed: {}", String::from_utf8_lossy(&payload)),
			_ => {}
		}
	}
}

#[tokio::test]
async fn libpq_offering_the_postgresql_alpn_protocol_connects_over_tls() {
	let server = start_tls_server().await;
	let mut stream = connect_tls(&server, &[b"postgresql"])
		.await
		.expect("a client offering the postgresql ALPN protocol must complete the handshake");
	assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"postgresql"[..]));
	startup(&mut stream).await;
	server.shutdown.cancel();
}

#[tokio::test]
async fn a_client_offering_no_alpn_protocol_connects_over_tls() {
	let server = start_tls_server().await;
	let mut stream = connect_tls(&server, &[])
		.await
		.expect("a client offering no ALPN protocol must complete the handshake");
	assert_eq!(stream.get_ref().1.alpn_protocol(), None);
	startup(&mut stream).await;
	server.shutdown.cancel();
}

#[tokio::test]
async fn a_client_offering_the_postgresql_protocol_among_others_selects_it() {
	let server = start_tls_server().await;
	let mut stream = connect_tls(&server, &[b"http/1.1", b"postgresql"])
		.await
		.expect("a client offering postgresql among other protocols must complete the handshake");
	assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"postgresql"[..]));
	startup(&mut stream).await;
	server.shutdown.cancel();
}

#[tokio::test]
async fn a_client_offering_only_other_alpn_protocols_is_refused() {
	let server = start_tls_server().await;
	let refused = connect_tls(&server, &[b"h2", b"http/1.1"]).await;
	assert!(refused.is_err(), "a client offering no postgresql protocol must not be served");
	server.shutdown.cancel();
}
