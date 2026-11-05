#![recursion_limit = "256"]

use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use rcgen::CertifiedKey;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::{
	AUTH_PLUGIN_CACHING_SHA2_PASSWORD, AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD,
	compute_caching_sha2_password_scramble, compute_mysql_native_password_scramble,
};
use surrealdb_mywire::constants::*;
use surrealdb_mywire::msg::SslRequest;
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_util::sync::CancellationToken;

/// Installs the process-wide rustls provider, as the server does at startup. A test
/// process cannot rely on rustls choosing one: it only can when exactly one provider
/// feature is enabled across the build. An `Err` means a provider is already installed.
fn install_crypto_provider() {
	let _ = aws_lc_rs::default_provider().install_default();
}

async fn setup_datastore_with_root() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	let _ = ds
		.run(surrealdb_core::kvs::QueryRequest::new(
			"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
			&session,
		))
		.await
		.expect("Failed to define root user");
	ds
}

async fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("Failed to read header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let seq = header[3];

	let mut payload = vec![0u8; len];
	if len > 0 {
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
	}
	(seq, payload)
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	if len > 0 {
		stream.write_all(payload).await.expect("Failed to write packet payload");
	}
}

fn build_handshake_response(
	username: &str,
	database: Option<&str>,
	plugin_name: &str,
	auth_data: &[u8],
) -> Vec<u8> {
	let mut resp = Vec::new();
	let caps: u32 =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	resp.extend_from_slice(&caps.to_le_bytes());

	let max_packet: u32 = 16 * 1024 * 1024;
	resp.extend_from_slice(&max_packet.to_le_bytes());
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	resp.extend_from_slice(&[0u8; 23]);

	// Username (null-terminated)
	resp.extend_from_slice(username.as_bytes());
	resp.push(0);

	// Auth response
	if auth_data.is_empty() {
		resp.push(0);
	} else {
		resp.push(auth_data.len() as u8);
		resp.extend_from_slice(auth_data);
	}

	// Database (null-terminated)
	let db = database.unwrap_or("test/test");
	resp.extend_from_slice(db.as_bytes());
	resp.push(0);

	// Auth plugin name (null-terminated)
	resp.extend_from_slice(plugin_name.as_bytes());
	resp.push(0);

	resp
}

fn extract_salt(handshake_payload: &[u8]) -> [u8; 20] {
	let mut pos = 1;
	while pos < handshake_payload.len() && handshake_payload[pos] != 0 {
		pos += 1;
	}
	pos += 1; // skip null
	pos += 4; // connection id (4 bytes)

	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake_payload[pos..pos + 8]);
	pos += 8;
	pos += 1; // filler 0x00
	pos += 2; // capabilities lower
	pos += 1; // charset
	pos += 2; // status flags
	pos += 2; // capabilities upper
	let auth_len = handshake_payload[pos];
	pos += 1;
	pos += 10; // reserved zeros

	let remaining_len = if auth_len > 8 {
		((auth_len - 8) as usize).min(12)
	} else {
		12
	};
	salt[8..8 + remaining_len].copy_from_slice(&handshake_payload[pos..pos + remaining_len]);
	salt
}

fn extract_capabilities(handshake_payload: &[u8]) -> u32 {
	let mut pos = 1;
	while pos < handshake_payload.len() && handshake_payload[pos] != 0 {
		pos += 1;
	}
	pos += 1; // skip null
	pos += 4; // connection id
	pos += 8; // auth part 1
	pos += 1; // filler

	let caps_low = u16::from_le_bytes([handshake_payload[pos], handshake_payload[pos + 1]]);
	pos += 2;
	pos += 1; // charset
	pos += 2; // status flags
	let caps_high = u16::from_le_bytes([handshake_payload[pos], handshake_payload[pos + 1]]);

	((caps_high as u32) << 16) | (caps_low as u32)
}

#[tokio::test]
async fn test_caching_sha2_password_full_auth_then_fast_auth() {
	let ds = setup_datastore_with_root().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	// --- Connection 1: Full authentication (cache miss) ---
	let mut stream1 = TcpStream::connect(addr).await.expect("Failed to connect");

	// 1. Initial HandshakeV10
	let (seq, hs1) = read_packet(&mut stream1).await;
	assert_eq!(seq, 0);
	assert_eq!(hs1[0], 10);
	let salt1 = extract_salt(&hs1);

	// 2. Send HandshakeResponse41 with caching_sha2_password scramble
	let scramble1 = compute_caching_sha2_password_scramble("root", &salt1);
	let resp1 =
		build_handshake_response("root", None, AUTH_PLUGIN_CACHING_SHA2_PASSWORD, &scramble1);
	write_packet(&mut stream1, 1, &resp1).await;

	// 3. Server sends AuthMoreData(0x01, 0x04) = PERFORM_FULL_AUTHENTICATION
	let (seq, more_data) = read_packet(&mut stream1).await;
	assert_eq!(more_data[0], AUTH_MORE_DATA_HEADER);
	assert_eq!(more_data[1], PERFORM_FULL_AUTHENTICATION);

	// 4. Send cleartext password over loopback
	write_packet(&mut stream1, seq.wrapping_add(1), b"root\0").await;

	// 5. Server sends OK packet
	let (_seq, ok_payload) = read_packet(&mut stream1).await;
	assert_eq!(
		ok_payload[0],
		OK_HEADER,
		"Expected handshake OK packet, got error: {:?}",
		String::from_utf8_lossy(&ok_payload)
	);

	// 6. Run a query to confirm session is fully operational
	let mut q = vec![COM_QUERY];
	q.extend_from_slice(b"SELECT 1;");
	write_packet(&mut stream1, 0, &q).await;
	let (_, col_count) = read_packet(&mut stream1).await;
	assert_eq!(col_count[0], 1);

	// --- Connection 2: Fast authentication (cache hit) ---
	let mut stream2 = TcpStream::connect(addr).await.expect("Failed to connect 2");

	// 1. Initial HandshakeV10
	let (seq, hs2) = read_packet(&mut stream2).await;
	assert_eq!(seq, 0);
	let salt2 = extract_salt(&hs2);

	// 2. Send HandshakeResponse41 with caching_sha2_password scramble
	let scramble2 = compute_caching_sha2_password_scramble("root", &salt2);
	let resp2 =
		build_handshake_response("root", None, AUTH_PLUGIN_CACHING_SHA2_PASSWORD, &scramble2);
	write_packet(&mut stream2, 1, &resp2).await;

	// 3. Server checks FastAuthCache and sends AuthMoreData(0x01, 0x03) = FAST_AUTH_SUCCESS
	let (_seq, fast_auth) = read_packet(&mut stream2).await;
	assert_eq!(fast_auth[0], AUTH_MORE_DATA_HEADER);
	assert_eq!(fast_auth[1], FAST_AUTH_SUCCESS);

	// 4. Server immediately follows with OK packet without requesting password
	let (_seq, ok_payload2) = read_packet(&mut stream2).await;
	assert_eq!(ok_payload2[0], OK_HEADER, "Expected OK packet following fast auth success");

	// 5. Run a query over stream2
	write_packet(&mut stream2, 0, &q).await;
	let (_, col_count2) = read_packet(&mut stream2).await;
	assert_eq!(col_count2[0], 1);

	shutdown.cancel();
}

#[tokio::test]
async fn test_caching_sha2_password_wrong_password_rejected() {
	let ds = setup_datastore_with_root().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	let (seq, hs) = read_packet(&mut stream).await;
	assert_eq!(seq, 0);
	let salt = extract_salt(&hs);

	let scramble = compute_caching_sha2_password_scramble("wrong", &salt);
	let resp = build_handshake_response("root", None, AUTH_PLUGIN_CACHING_SHA2_PASSWORD, &scramble);
	write_packet(&mut stream, 1, &resp).await;

	let (seq, more_data) = read_packet(&mut stream).await;
	assert_eq!(more_data[0], AUTH_MORE_DATA_HEADER);
	assert_eq!(more_data[1], PERFORM_FULL_AUTHENTICATION);

	// Send wrong password
	write_packet(&mut stream, seq.wrapping_add(1), b"wrong\0").await;

	// Server should reject with ErrPacket
	let (_seq, err_payload) = read_packet(&mut stream).await;
	assert_eq!(err_payload[0], ERR_HEADER);
	let err_msg = String::from_utf8_lossy(&err_payload[3..]);
	assert!(
		err_msg.contains("Access denied for user 'root'"),
		"Expected access denied, got: {err_msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_native_password_fast_auth_after_first_login() {
	let ds = setup_datastore_with_root().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	// --- Connection 1: First login caches credentials ---
	let mut stream1 = TcpStream::connect(addr).await.expect("Failed to connect");
	let _ = read_packet(&mut stream1).await;

	let resp1 = build_handshake_response("root", None, AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD, &[]);
	write_packet(&mut stream1, 1, &resp1).await;

	let (seq, switch) = read_packet(&mut stream1).await;
	assert_eq!(switch[0], EOF_HEADER); // AuthSwitchRequest

	write_packet(&mut stream1, seq.wrapping_add(1), b"root\0").await;
	let (_, ok1) = read_packet(&mut stream1).await;
	assert_eq!(
		ok1[0],
		OK_HEADER,
		"Expected OK packet, got error: {:?}",
		String::from_utf8_lossy(&ok1)
	);

	// --- Connection 2: Native password scramble verified directly from cache ---
	let mut stream2 = TcpStream::connect(addr).await.expect("Failed to connect 2");
	let (_, hs2) = read_packet(&mut stream2).await;
	let salt2 = extract_salt(&hs2);

	let scramble2 = compute_mysql_native_password_scramble("root", &salt2);
	let resp2 =
		build_handshake_response("root", None, AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD, &scramble2);
	write_packet(&mut stream2, 1, &resp2).await;

	// Server should directly reply with OK packet without an AuthSwitchRequest!
	let (_, ok2) = read_packet(&mut stream2).await;
	assert_eq!(
		ok2[0],
		OK_HEADER,
		"Expected immediate OK packet via native password fast-auth, got error: {:?}",
		String::from_utf8_lossy(&ok2)
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_ssl_request_rejected_gracefully_when_tls_not_configured() {
	let ds = setup_datastore_with_root().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	// 1. Handshake does not have CLIENT_SSL
	let (seq, hs) = read_packet(&mut stream).await;
	assert_eq!(seq, 0);
	let caps = extract_capabilities(&hs);
	assert_eq!(caps & CLIENT_SSL, 0, "CLIENT_SSL should not be advertised without TLS cert");

	// 2. Client sends SslRequest anyway
	let ssl_req =
		SslRequest::new(CLIENT_PROTOCOL_41, 16 * 1024 * 1024, CHARSET_UTF8MB4_GENERAL_CI as u8);
	write_packet(&mut stream, 1, &ssl_req.serialize()).await;

	// 3. Server replies with MySQL ErrPacket (1045 / 28000)
	let (err_seq, err_payload) = read_packet(&mut stream).await;
	assert_eq!(err_seq, 2);
	assert_eq!(err_payload[0], ERR_HEADER);

	let code = u16::from_le_bytes([err_payload[1], err_payload[2]]);
	assert_eq!(code, 1045);

	let sqlstate = &err_payload[4..9];
	assert_eq!(sqlstate, b"28000");

	let err_msg = String::from_utf8_lossy(&err_payload[9..]);
	assert!(
		err_msg.contains("SSL/TLS requested but not configured on this server"),
		"Expected SSL error message, got: {err_msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_server_advertises_ssl_when_configured() {
	install_crypto_provider();
	let ds = setup_datastore_with_root().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	// Generate self-signed cert and key using rcgen
	let CertifiedKey {
		cert,
		signing_key,
	} = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();

	let crt_file = NamedTempFile::new().expect("Failed to create temp crt file");
	let key_file = NamedTempFile::new().expect("Failed to create temp key key file");

	fs::write(crt_file.path(), cert.pem()).expect("Failed to write cert pem");
	fs::write(key_file.path(), signing_key.serialize_pem()).expect("Failed to write key pem");

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(
		bind_addr,
		ds,
		ready,
		shutdown.clone(),
		Some(crt_file.path().to_path_buf()),
		Some(key_file.path().to_path_buf()),
	)
	.await
	.expect("Failed to start mywire server with TLS");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	// Verify HandshakeV10 advertises CLIENT_SSL
	let (seq, hs) = read_packet(&mut stream).await;
	assert_eq!(seq, 0);
	let caps = extract_capabilities(&hs);
	assert_ne!(caps & CLIENT_SSL, 0, "CLIENT_SSL should be advertised when TLS cert is configured");

	// Plaintext connection without SslRequest should still proceed smoothly
	let salt = extract_salt(&hs);
	let scramble = compute_caching_sha2_password_scramble("root", &salt);
	let resp = build_handshake_response("root", None, AUTH_PLUGIN_CACHING_SHA2_PASSWORD, &scramble);
	write_packet(&mut stream, 1, &resp).await;

	let (seq, more_data) = read_packet(&mut stream).await;
	assert_eq!(more_data[0], AUTH_MORE_DATA_HEADER);
	assert_eq!(more_data[1], PERFORM_FULL_AUTHENTICATION);

	write_packet(&mut stream, seq.wrapping_add(1), b"root\0").await;
	let (_, ok) = read_packet(&mut stream).await;
	assert_eq!(
		ok[0],
		OK_HEADER,
		"Expected OK packet, got error: {:?}",
		String::from_utf8_lossy(&ok)
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_tls_handshake_completes_for_a_client_offering_an_alpn_protocol() {
	install_crypto_provider();
	let ds = setup_datastore_with_root().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let CertifiedKey {
		cert,
		signing_key,
	} = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
	let crt_file = NamedTempFile::new().expect("Failed to create temp crt file");
	let key_file = NamedTempFile::new().expect("Failed to create temp key file");
	fs::write(crt_file.path(), cert.pem()).expect("Failed to write cert pem");
	fs::write(key_file.path(), signing_key.serialize_pem()).expect("Failed to write key pem");

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(
		bind_addr,
		ds,
		ready,
		shutdown.clone(),
		Some(crt_file.path().to_path_buf()),
		Some(key_file.path().to_path_buf()),
	)
	.await
	.expect("Failed to start mywire server with TLS");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
	let (_, hs) = read_packet(&mut stream).await;
	assert_ne!(extract_capabilities(&hs) & CLIENT_SSL, 0);
	let ssl_req = SslRequest::new(
		CLIENT_PROTOCOL_41 | CLIENT_SSL,
		16 * 1024 * 1024,
		CHARSET_UTF8MB4_GENERAL_CI as u8,
	);
	write_packet(&mut stream, 1, &ssl_req.serialize()).await;

	// MySQL has no ALPN protocol, but a TLS stack may offer a list anyway; the
	// handshake must complete rather than abort with `no_application_protocol`.
	let mut roots = RootCertStore::empty();
	roots.add(cert.der().clone()).expect("Failed to trust the server certificate");
	let mut config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
	config.alpn_protocols = vec![b"mysql".to_vec(), b"h2".to_vec(), b"http/1.1".to_vec()];
	let name = ServerName::try_from("localhost").unwrap();
	let tls = TlsConnector::from(Arc::new(config)).connect(name, stream).await;
	let tls = tls.expect("TLS handshake with an ALPN list must complete");
	assert_eq!(tls.get_ref().1.alpn_protocol(), None, "the listener selects no ALPN protocol");

	shutdown.cancel();
}
