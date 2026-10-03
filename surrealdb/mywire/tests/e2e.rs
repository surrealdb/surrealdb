use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

async fn start_test_mysql_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone())
		.await
		.expect("Failed to start mywire server");

	(addr, shutdown)
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

async fn connect_and_handshake(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to MySQL server");

	// 1. Read Initial Handshake Packet (Seq 0)
	let (seq, payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 0, "Initial handshake packet should be seq 0");
	assert_eq!(payload[0], 10, "Protocol version should be 10 (HandshakeV10)");

	// Extract salt from HandshakeV10:
	// Protocol version (1 byte) + server version (null terminated) + conn id (4 bytes)
	let mut pos = 1;
	while pos < payload.len() && payload[pos] != 0 {
		pos += 1;
	}
	pos += 1; // skip null
	pos += 4; // skip thread id

	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&payload[pos..pos + 8]);
	pos += 9; // 8 bytes auth-plugin-data-part-1 + 1 byte filler (0x00)
	pos += 2; // capability flags (lower 2 bytes)
	pos += 1; // character set
	pos += 2; // status flags
	pos += 2; // capability flags (upper 2 bytes)
	pos += 1; // auth_plugin_data_len
	pos += 10; // reserved 10 bytes

	salt[8..20].copy_from_slice(&payload[pos..pos + 12]);

	// 2. Build HandshakeResponse41 (Seq 1)
	let scramble = compute_mysql_native_password_scramble("root", &salt);

	let mut resp = Vec::new();
	// Client capabilities: CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB |
	// CLIENT_PLUGIN_AUTH
	let caps: u32 =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	resp.extend_from_slice(&caps.to_le_bytes());

	// Max packet size: 16 MB
	let max_packet: u32 = 16 * 1024 * 1024;
	resp.extend_from_slice(&max_packet.to_le_bytes());

	// Charset: UTF8MB4_GENERAL_CI (45)
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);

	// Reserved 23 bytes zeros
	resp.extend_from_slice(&[0u8; 23]);

	// Username: "root\0"
	resp.extend_from_slice(b"root\0");

	// Auth response: lenenc or 1-byte len + scramble
	resp.push(20);
	resp.extend_from_slice(&scramble);

	// Database: "test/test\0"
	resp.extend_from_slice(b"test/test\0");

	// Auth plugin name: "mysql_native_password\0"
	resp.extend_from_slice(b"mysql_native_password\0");

	write_packet(&mut stream, 1, &resp).await;

	// 3. Read OK Packet (Seq 2)
	let (seq, ok_payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 2, "Handshake OK packet should be seq 2");
	assert_eq!(ok_payload[0], 0x00, "Expected OK packet (0x00) from handshake");

	stream
}

enum QueryResult {
	Ok,
	ResultSet {
		columns: usize,
		rows: Vec<Vec<u8>>,
	},
}

async fn read_query_response(stream: &mut TcpStream) -> QueryResult {
	let (_, first) = read_packet(stream).await;
	if first[0] == 0x00 {
		return QueryResult::Ok;
	}
	if first[0] == 0xff {
		panic!("MySQL Error packet: {:?}", String::from_utf8_lossy(&first));
	}

	let col_count = first[0] as usize;
	for _ in 0..col_count {
		let _ = read_packet(stream).await;
	}
	// EOF packet separating cols and rows
	let (_, eof1) = read_packet(stream).await;
	assert_eq!(eof1[0], 0xfe, "Expected EOF after column definitions");

	let mut rows = Vec::new();
	loop {
		let (_, pkt) = read_packet(stream).await;
		if pkt[0] == 0xfe && pkt.len() < 9 {
			break;
		}
		rows.push(pkt);
	}

	QueryResult::ResultSet {
		columns: col_count,
		rows,
	}
}

#[tokio::test]
async fn test_mywire_e2e_ping_and_init_db() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// COM_PING (0x0e)
	write_packet(&mut stream, 0, &[COM_PING]).await;
	let (seq, payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 1);
	assert_eq!(payload[0], 0x00, "COM_PING should return OK packet");

	// COM_INIT_DB (0x02)
	let mut init_db = vec![COM_INIT_DB];
	init_db.extend_from_slice(b"test/test");
	write_packet(&mut stream, 0, &init_db).await;
	let (seq, payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 1);
	assert_eq!(payload[0], 0x00, "COM_INIT_DB should return OK packet");

	// COM_QUIT (0x01)
	write_packet(&mut stream, 0, &[COM_QUIT]).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_e2e_queries() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. SELECT @@version_comment
	let mut query = vec![COM_QUERY];
	query.extend_from_slice(b"SELECT @@version_comment;");
	write_packet(&mut stream, 0, &query).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::ResultSet {
			columns,
			rows,
		} => {
			assert_eq!(columns, 1);
			assert_eq!(rows.len(), 1);
		}
		_ => panic!("Expected ResultSet for @@version_comment"),
	}

	// 2. CREATE record
	let mut create_q = vec![COM_QUERY];
	create_q.extend_from_slice(b"CREATE person SET name = 'Tobie';");
	write_packet(&mut stream, 0, &create_q).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::ResultSet {
			columns,
			rows,
		} => {
			assert_eq!(columns, 2, "Expected id and name columns");
			assert_eq!(rows.len(), 1, "Expected 1 created row");
		}
		_ => panic!("Expected ResultSet for CREATE"),
	}

	// 3. SELECT from person
	let mut select_q = vec![COM_QUERY];
	select_q.extend_from_slice(b"SELECT name FROM person;");
	write_packet(&mut stream, 0, &select_q).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::ResultSet {
			columns,
			rows,
		} => {
			assert_eq!(columns, 1, "Expected name column");
			assert_eq!(rows.len(), 1, "Expected 1 row from person");
		}
		_ => panic!("Expected ResultSet for SELECT"),
	}

	// Clean quit
	write_packet(&mut stream, 0, &[COM_QUIT]).await;

	shutdown.cancel();
}
