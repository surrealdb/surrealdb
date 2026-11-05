#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::constants::*;
use surrealdb_rpc::capabilities::{ArbitraryQueryTarget, Capabilities, RouteTarget, Targets};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

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

fn build_handshake_response(username: &str, database: Option<&str>) -> Vec<u8> {
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

	// Empty auth response for initial packet
	resp.push(0);

	// Database (null-terminated)
	let db = database.unwrap_or("test/test");
	resp.extend_from_slice(db.as_bytes());
	resp.push(0);

	// Auth plugin name (null-terminated)
	resp.extend_from_slice(b"mysql_native_password\0");

	resp
}

#[tokio::test]
async fn test_mysql_capability_denial_blocks_connection() {
	let caps =
		Capabilities::default().without_http_routes(Targets::Some([RouteTarget::MySql].into()));
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let _ = ds.set_capabilities(caps);

	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
	let (_seq, payload) = read_packet(&mut stream).await;

	// Server should immediately reject with ErrPacket (0xff)
	assert_eq!(payload[0], 0xff, "Expected error packet due to capability denial");
	let msg = String::from_utf8_lossy(&payload[3..]);
	assert!(
		msg.contains("disabled by capability configuration"),
		"Expected capability error message, got: {msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_readiness_blocks_connection() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let ready = Arc::new(AtomicBool::new(false)); // Not ready
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
	let (_seq, payload) = read_packet(&mut stream).await;

	// Server should immediately reject with ErrPacket (0xff)
	assert_eq!(payload[0], 0xff, "Expected error packet due to not ready");
	let msg = String::from_utf8_lossy(&payload[3..]);
	assert!(
		msg.contains("database system is starting up"),
		"Expected startup error message, got: {msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_wrong_password_rejected() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	let _ = ds
		.run(surrealdb_core::kvs::QueryRequest::new(
			"DEFINE USER root ON ROOT PASSWORD 'correctpassword' ROLES OWNER;",
			&session,
		))
		.await
		.expect("Failed to define root user");

	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	// 1. Read initial handshake (seq 0)
	let (seq, _) = read_packet(&mut stream).await;
	assert_eq!(seq, 0);

	// 2. Send HandshakeResponse41 for user "root"
	let resp = build_handshake_response("root", None);
	write_packet(&mut stream, 1, &resp).await;

	// 3. Server should send AuthSwitchRequest (0xfe) to mysql_clear_password
	let (seq, switch_payload) = read_packet(&mut stream).await;
	assert_eq!(switch_payload[0], 0xfe, "Expected AuthSwitchRequest");

	// 4. Send wrong password
	write_packet(&mut stream, seq.wrapping_add(1), b"wrongpassword\0").await;

	// 5. Server should return ErrPacket (0xff)
	let (_final_seq, err_payload) = read_packet(&mut stream).await;
	assert_eq!(err_payload[0], 0xff, "Expected access denied ErrPacket");
	let err_msg = String::from_utf8_lossy(&err_payload[3..]);
	assert!(
		err_msg.contains("Access denied for user 'root'"),
		"Expected access denied message, got: {err_msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_guest_rejected_when_unauthenticated_disabled() {
	let caps = Capabilities::default()
		.without_arbitrary_query(Targets::Some([ArbitraryQueryTarget::Guest].into()));
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let _ = ds.set_capabilities(caps);

	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	// 1. Read initial handshake (seq 0)
	let (seq, _) = read_packet(&mut stream).await;
	assert_eq!(seq, 0);

	// 2. Send HandshakeResponse41 with empty username (guest)
	let resp = build_handshake_response("", None);
	write_packet(&mut stream, 1, &resp).await;

	// 3. Server should immediately reject guest with ErrPacket (0xff)
	let (_seq, err_payload) = read_packet(&mut stream).await;
	assert_eq!(err_payload[0], 0xff, "Expected access denied for guest");
	let err_msg = String::from_utf8_lossy(&err_payload[3..]);
	assert!(
		err_msg.contains("Access denied for user ''"),
		"Expected guest access denied, got: {err_msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_select_user_reports_authenticated_user_and_ip() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	let _ = ds
		.run(surrealdb_core::kvs::QueryRequest::new(
			"DEFINE USER alice ON ROOT PASSWORD 'alicepass' ROLES OWNER;",
			&session,
		))
		.await
		.expect("Failed to define alice user");

	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	// 1. Initial handshake
	read_packet(&mut stream).await;

	// 2. Handshake response for "alice"
	let resp = build_handshake_response("alice", None);
	write_packet(&mut stream, 1, &resp).await;

	// 3. AuthSwitchRequest
	let (seq, switch_payload) = read_packet(&mut stream).await;
	assert_eq!(switch_payload[0], 0xfe);

	// 4. Send clear password "alicepass"
	write_packet(&mut stream, seq.wrapping_add(1), b"alicepass\0").await;

	// 5. Read OK packet
	let (_ok_seq, ok_payload) = read_packet(&mut stream).await;
	assert_eq!(ok_payload[0], 0x00, "Authentication failed");

	// 6. Query "SELECT USER();"
	let mut q = vec![COM_QUERY];
	q.extend_from_slice(b"SELECT USER();");
	write_packet(&mut stream, 0, &q).await;

	// Read column count
	let (_, col_count) = read_packet(&mut stream).await;
	assert_eq!(col_count[0], 1);

	// Read column def
	let _ = read_packet(&mut stream).await;

	// Read EOF
	let (_, eof1) = read_packet(&mut stream).await;
	assert_eq!(eof1[0], 0xfe);

	// Read row
	let (_, row) = read_packet(&mut stream).await;
	let row_str = String::from_utf8_lossy(&row);
	assert!(
		row_str.contains("alice@127.0.0.1"),
		"Expected alice@127.0.0.1 in USER(), got: {row_str}"
	);

	// Read closing EOF
	let (_, eof2) = read_packet(&mut stream).await;
	assert_eq!(eof2[0], 0xfe);

	// Clean quit
	write_packet(&mut stream, 0, &[COM_QUIT]).await;
	shutdown.cancel();
}
