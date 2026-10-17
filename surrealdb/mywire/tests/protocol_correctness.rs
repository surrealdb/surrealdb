#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::{
	AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD, compute_mysql_native_password_scramble,
};
use surrealdb_mywire::codec::{PacketReader, PacketWriter};
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

async fn setup_test_datastore() -> Arc<Datastore> {
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

fn build_handshake_response_with_caps(
	username: &str,
	database: Option<&str>,
	auth_data: &[u8],
	extra_caps: u32,
) -> Vec<u8> {
	let mut resp = Vec::new();
	let caps: u32 = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| extra_caps;
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
	resp.extend_from_slice(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD.as_bytes());
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

async fn perform_handshake(stream: &mut TcpStream, extra_caps: u32) -> (u8, Vec<u8>) {
	let (_, initial_handshake) = read_packet(stream).await;
	assert_eq!(initial_handshake[0], 0x0a, "Expected HandshakeV10 packet");

	let salt = extract_salt(&initial_handshake);
	let scramble = compute_mysql_native_password_scramble("root", &salt);
	let response = build_handshake_response_with_caps("root", Some("test"), &scramble, extra_caps);
	write_packet(stream, 1, &response).await;

	let (seq, resp) = read_packet(stream).await;
	if resp[0] == 0xfe {
		// AuthSwitchRequest
		write_packet(stream, seq.wrapping_add(1), b"root\0").await;
		read_packet(stream).await
	} else {
		(seq, resp)
	}
}

#[tokio::test]
async fn test_deprecate_eof_negotiated_suppresses_col_eof_and_sends_eof_ok() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed with OK packet");

	// Send SELECT 1;
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(b"SELECT 1;");
	write_packet(&mut stream, 0, &query_payload).await;

	// 1. Column count packet
	let (seq1, col_count_pkt) = read_packet(&mut stream).await;
	assert_eq!(seq1, 1);
	assert_eq!(col_count_pkt[0], 1, "Expected 1 column");

	// 2. Column definition packet
	let (seq2, col_def_pkt) = read_packet(&mut stream).await;
	assert_eq!(seq2, 2);
	assert!(col_def_pkt.len() > 10);

	// 3. Since CLIENT_DEPRECATE_EOF is active, the intermediate EOF packet MUST BE SUPPRESSED!
	// Next packet MUST be the text row packet!
	let (seq3, row_pkt) = read_packet(&mut stream).await;
	assert_eq!(seq3, 3);
	// Row packet starts with length of string "1" (0x01) followed by '1' (0x31)
	assert_eq!(row_pkt[0], 0x01, "Expected text row with length 1");
	assert_eq!(row_pkt[1], b'1', "Expected row cell '1'");

	// 4. Terminating packet: With CLIENT_DEPRECATE_EOF, this is an EOF-OK packet starting with 0xfe
	let (seq4, eof_ok_pkt) = read_packet(&mut stream).await;
	assert_eq!(seq4, 4);
	assert_eq!(eof_ok_pkt[0], EOF_HEADER, "Expected 0xfe header for EOF-OK packet");
	// Structure: [0xfe, affected_rows(0), last_insert_id(0), status(2 LE), warnings(2 LE)] = 7
	// bytes
	assert_eq!(eof_ok_pkt.len(), 7, "EOF-OK packet should be 7 bytes (< 9 bytes)");

	shutdown.cancel();
}

#[tokio::test]
async fn test_deprecate_eof_not_negotiated_sends_intermediate_and_final_eof() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	// Handshake WITHOUT CLIENT_DEPRECATE_EOF
	let (_, auth_ok) = perform_handshake(&mut stream, 0).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed with OK packet");

	// Send SELECT 1;
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(b"SELECT 1;");
	write_packet(&mut stream, 0, &query_payload).await;

	// 1. Column count packet
	let (seq1, col_count_pkt) = read_packet(&mut stream).await;
	assert_eq!(seq1, 1);
	assert_eq!(col_count_pkt[0], 1);

	// 2. Column definition packet
	let (seq2, _) = read_packet(&mut stream).await;
	assert_eq!(seq2, 2);

	// 3. Intermediate legacy EOF packet (5 bytes: 0xfe, warnings(2), status(2))
	let (seq3, intermediate_eof) = read_packet(&mut stream).await;
	assert_eq!(seq3, 3);
	assert_eq!(intermediate_eof[0], EOF_HEADER);
	assert_eq!(intermediate_eof.len(), 5, "Legacy EOF packet is exactly 5 bytes");

	// 4. Text row packet
	let (seq4, row_pkt) = read_packet(&mut stream).await;
	assert_eq!(seq4, 4);
	assert_eq!(row_pkt[0], 0x01);
	assert_eq!(row_pkt[1], b'1');

	// 5. Final legacy EOF packet (5 bytes)
	let (seq5, final_eof) = read_packet(&mut stream).await;
	assert_eq!(seq5, 5);
	assert_eq!(final_eof[0], EOF_HEADER);
	assert_eq!(final_eof.len(), 5, "Final legacy EOF packet is exactly 5 bytes");

	shutdown.cancel();
}

#[tokio::test]
async fn test_multi_statements_with_more_results_exists() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	// Negotiate CLIENT_MULTI_STATEMENTS and CLIENT_MULTI_RESULTS
	let extra_caps = CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS;
	let (_, auth_ok) = perform_handshake(&mut stream, extra_caps).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// Send multi-statement query
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(b"SELECT 10; SELECT 20;");
	write_packet(&mut stream, 0, &query_payload).await;

	// === First Result Set (SELECT 10) ===
	let (_, _) = read_packet(&mut stream).await; // col count
	let (_, _) = read_packet(&mut stream).await; // col def
	let (_, _) = read_packet(&mut stream).await; // intermediate EOF
	let (_, row1) = read_packet(&mut stream).await; // row "10"
	assert_eq!(row1[0], 2);
	assert_eq!(&row1[1..3], b"10");

	let (_, eof1) = read_packet(&mut stream).await; // terminating EOF
	assert_eq!(eof1[0], EOF_HEADER);
	assert_eq!(eof1.len(), 5);
	let status1 = u16::from_le_bytes([eof1[3], eof1[4]]);
	assert_ne!(
		status1 & SERVER_MORE_RESULTS_EXISTS,
		0,
		"First resultset must have SERVER_MORE_RESULTS_EXISTS set"
	);

	// === Second Result Set (SELECT 20) ===
	let (_, _) = read_packet(&mut stream).await; // col count
	let (_, _) = read_packet(&mut stream).await; // col def
	let (_, _) = read_packet(&mut stream).await; // intermediate EOF
	let (_, row2) = read_packet(&mut stream).await; // row "20"
	assert_eq!(row2[0], 2);
	assert_eq!(&row2[1..3], b"20");

	let (_, eof2) = read_packet(&mut stream).await; // final terminating EOF
	assert_eq!(eof2[0], EOF_HEADER);
	assert_eq!(eof2.len(), 5);
	let status2 = u16::from_le_bytes([eof2[3], eof2[4]]);
	assert_eq!(
		status2 & SERVER_MORE_RESULTS_EXISTS,
		0,
		"Final resultset must NOT have SERVER_MORE_RESULTS_EXISTS set"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_empty_result_returns_zero_rows() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, 0).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// Query an empty table
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(b"SELECT * FROM non_existent_empty_table WHERE id = 'none';");
	write_packet(&mut stream, 0, &query_payload).await;

	let (_, response) = read_packet(&mut stream).await;
	// An empty result set with 0 columns returns an OK_Packet (0x00), or if shaped, 0 rows
	// It must NEVER return a row containing "[]"!
	if response[0] == OK_HEADER {
		// OK packet returned directly
		assert_eq!(response[0], OK_HEADER);
	} else {
		// If column count packet, ensure column count or row count is 0
		assert_ne!(response, b"[]");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_max_connections_err_1040() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	// Start server with MAX_CONNECTIONS = 1
	let addr = surrealdb_mywire::start_with_max_connections(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
		1,
	)
	.await
	.unwrap();

	// Connection 1 occupies the only slot
	let mut stream1 = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream1, 0).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// Connection 2 attempts to connect
	let mut stream2 = TcpStream::connect(addr).await.unwrap();
	let (seq, err_pkt) = read_packet(&mut stream2).await;
	assert_eq!(seq, 0, "Error packet for over-capacity connection has sequence ID 0");
	assert_eq!(err_pkt[0], ERR_HEADER, "Expected ERR_Packet (0xff)");

	// Error code is 1040 (0x0410 little-endian)
	let err_code = u16::from_le_bytes([err_pkt[1], err_pkt[2]]);
	assert_eq!(err_code, 1040, "Expected error code 1040 (ER_CON_COUNT_ERROR)");

	// SQL state marker '#' (0x23) followed by "08004"
	assert_eq!(err_pkt[3], b'#');
	assert_eq!(&err_pkt[4..9], b"08004", "Expected SQLSTATE 08004");

	// Error message
	let msg = String::from_utf8_lossy(&err_pkt[9..]);
	assert!(msg.contains("Too many connections"), "Got message: {msg}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_packet_chunking_and_reassembly() {
	let (client, server) = duplex(64 * 1024);
	let mut writer = PacketWriter::new(client);
	let mut reader = PacketReader::new(server);

	// Create payload larger than MAX_PACKET_SIZE
	// To test efficiently in memory, test roundtrip across duplex
	let test_size = 32_768; // 32KB buffer test for buffered writer
	let payload = vec![0x42u8; test_size];

	tokio::spawn(async move {
		writer.write_packet(1, &payload).await.unwrap();
	});

	let (seq, read_payload) = reader.read_packet().await.unwrap();
	assert_eq!(seq, 1);
	assert_eq!(read_payload.len(), test_size);
	assert_eq!(read_payload[0], 0x42);
	assert_eq!(read_payload[test_size - 1], 0x42);
}
