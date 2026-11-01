//! `TRUNCATE TABLE` starts the `AUTO_INCREMENT` count of its table over, as MySQL does, whether the
//! client sends it as a text query, as a prepared statement or in a batch, and answers with an OK
//! packet that reports no affected rows.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use surrealdb_mywire::msg::StmtPrepareOk;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

async fn start_test_mysql_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	ds.run(surrealdb_core::kvs::QueryRequest::new(
		"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
		&session,
	))
	.await
	.expect("Failed to define root user");
	let shutdown = CancellationToken::new();
	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(
		bind_addr,
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("Failed to start mywire server");
	(addr, shutdown)
}

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	timeout(Duration::from_secs(10), async {
		let mut header = [0u8; 4];
		stream.read_exact(&mut header).await.expect("Failed to read header");
		let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
		let mut payload = vec![0u8; len];
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
		payload
	})
	.await
	.expect("A packet within 10 s")
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	stream.write_all(payload).await.expect("Failed to write packet payload");
}

/// Connects as `root` to `test/test`, announcing multi-statement and multi-result support, the
/// capabilities under which a client reads one result per statement.
async fn connect_with_multi_results(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to MySQL server");

	// The salt is split in two around the capability flags, charset and status flags of the
	// HandshakeV10 packet.
	let handshake = read_packet(&mut stream).await;
	assert_eq!(handshake[0], 10, "Protocol version should be 10 (HandshakeV10)");
	let mut pos = 1;
	while handshake[pos] != 0 {
		pos += 1;
	}
	pos += 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake[pos..pos + 8]);
	pos += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..].copy_from_slice(&handshake[pos..pos + 12]);

	let caps: u32 = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| CLIENT_MULTI_STATEMENTS
		| CLIENT_MULTI_RESULTS;
	let mut resp = Vec::new();
	resp.extend_from_slice(&caps.to_le_bytes());
	resp.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	resp.extend_from_slice(&[0u8; 23]);
	resp.extend_from_slice(b"root\0");
	resp.push(20);
	resp.extend_from_slice(&compute_mysql_native_password_scramble("root", &salt));
	resp.extend_from_slice(b"test/test\0");
	resp.extend_from_slice(b"mysql_native_password\0");
	write_packet(&mut stream, 1, &resp).await;

	let mut reply = read_packet(&mut stream).await;
	if reply[0] == 0xfe {
		// AuthSwitchRequest to mysql_clear_password.
		write_packet(&mut stream, 3, b"root\0").await;
		reply = read_packet(&mut stream).await;
	}
	assert_eq!(reply[0], OK_HEADER, "the handshake should end with an OK packet");
	stream
}

/// The length-encoded integer at `at` in `payload`, and the position after it.
fn lenenc(payload: &[u8], at: usize) -> (u64, usize) {
	match payload[at] {
		0xfc => (u64::from(u16::from_le_bytes([payload[at + 1], payload[at + 2]])), at + 3),
		0xfd => (
			u64::from_le_bytes([payload[at + 1], payload[at + 2], payload[at + 3], 0, 0, 0, 0, 0]),
			at + 4,
		),
		0xfe => {
			(u64::from_le_bytes(payload[at + 1..at + 9].try_into().expect("eight bytes")), at + 9)
		}
		small => (u64::from(small), at + 1),
	}
}

/// The affected-row count and the last insert id of an OK packet.
fn ok_counters(payload: &[u8]) -> (u64, u64) {
	assert_eq!(payload[0], OK_HEADER, "expected an OK packet, got {payload:?}");
	let (affected, at) = lenenc(payload, 1);
	let (last_insert_id, _) = lenenc(payload, at);
	(affected, last_insert_id)
}

async fn send_query(stream: &mut TcpStream, sql: &str) {
	let mut query = vec![COM_QUERY];
	query.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &query).await;
}

/// Runs `sql`, which must be answered by an OK packet, and returns its counters.
async fn run(stream: &mut TcpStream, sql: &str) -> (u64, u64) {
	send_query(stream, sql).await;
	let reply = read_packet(stream).await;
	assert_ne!(reply[0], ERR_HEADER, "{sql}: {}", String::from_utf8_lossy(&reply[9..]));
	ok_counters(&reply)
}

/// Inserts a row into `table` and returns the number its `AUTO_INCREMENT` column drew.
async fn draw(stream: &mut TcpStream, table: &str) -> u64 {
	run(stream, &format!("INSERT INTO {table} (name) VALUES ('x')")).await.1
}

#[tokio::test]
async fn test_mywire_truncate_starts_the_auto_increment_count_over() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	run(
		&mut stream,
		"CREATE TABLE trunc_auto (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20))",
	)
	.await;
	run(
		&mut stream,
		"CREATE TABLE trunc_auto_keep (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20))",
	)
	.await;
	for expected in 1..=3 {
		assert_eq!(draw(&mut stream, "trunc_auto").await, expected);
	}
	assert_eq!(draw(&mut stream, "trunc_auto_keep").await, 1);

	// An OK packet that reports no row, and the next row is numbered 1 again.
	assert_eq!(run(&mut stream, "TRUNCATE TABLE trunc_auto").await, (0, 0));
	assert_eq!(draw(&mut stream, "trunc_auto").await, 1);

	// An explicit id does not stop the count from starting over, and the table needs no `TABLE`.
	run(&mut stream, "INSERT INTO trunc_auto (id, name) VALUES (100, 'x')").await;
	assert_eq!(run(&mut stream, "TRUNCATE trunc_auto").await, (0, 0));
	assert_eq!(draw(&mut stream, "trunc_auto").await, 1);

	// Another table keeps its count.
	assert_eq!(draw(&mut stream, "trunc_auto_keep").await, 2);

	shutdown.cancel();
}

/// Reads a text result set that carries one row, announced without `CLIENT_DEPRECATE_EOF`: the
/// column count, the column definitions, an EOF packet, the rows and an EOF packet. Returns the
/// first cell of the first row.
async fn first_cell_of_result_set(stream: &mut TcpStream) -> String {
	let columns = read_packet(stream).await;
	for _ in 0..columns[0] {
		read_packet(stream).await;
	}
	assert_eq!(read_packet(stream).await[0], 0xfe, "the column definitions end with an EOF packet");
	let row = read_packet(stream).await;
	let len = usize::from(row[0]);
	let cell = String::from_utf8(row[1..=len].to_vec()).expect("a text cell");
	assert_eq!(read_packet(stream).await[0], 0xfe, "the rows end with an EOF packet");
	cell
}

#[tokio::test]
async fn test_mywire_a_truncate_in_a_batch_starts_the_count_over_for_the_statements_after_it() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	run(
		&mut stream,
		"CREATE TABLE trunc_batch (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20))",
	)
	.await;
	for expected in 1..=3 {
		assert_eq!(draw(&mut stream, "trunc_batch").await, expected);
	}

	// The TRUNCATE is answered by an OK packet, and the two inserts after it by the rows they
	// insert, numbered from 1.
	send_query(
		&mut stream,
		"TRUNCATE TABLE trunc_batch; INSERT INTO trunc_batch (name) VALUES ('x'); \
		 INSERT INTO trunc_batch (name) VALUES ('y')",
	)
	.await;
	assert_eq!(ok_counters(&read_packet(&mut stream).await), (0, 0));
	assert_eq!(first_cell_of_result_set(&mut stream).await, "1");
	assert_eq!(first_cell_of_result_set(&mut stream).await, "2");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_a_prepared_truncate_starts_the_auto_increment_count_over() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	run(
		&mut stream,
		"CREATE TABLE trunc_prep (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20))",
	)
	.await;
	for expected in 1..=3 {
		assert_eq!(draw(&mut stream, "trunc_prep").await, expected);
	}

	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(b"TRUNCATE TABLE trunc_prep");
	write_packet(&mut stream, 0, &prepare).await;
	let prepared =
		StmtPrepareOk::deserialize(&read_packet(&mut stream).await).expect("a prepared statement");
	assert_eq!((prepared.num_params, prepared.num_columns), (0, 0));

	// Executed twice: the count starts over each time.
	for _ in 0..2 {
		let mut execute = vec![COM_STMT_EXECUTE];
		execute.extend_from_slice(&prepared.statement_id.to_le_bytes());
		execute.push(CURSOR_TYPE_NO_CURSOR);
		execute.extend_from_slice(&1u32.to_le_bytes());
		write_packet(&mut stream, 0, &execute).await;
		assert_eq!(ok_counters(&read_packet(&mut stream).await), (0, 0));
		assert_eq!(draw(&mut stream, "trunc_prep").await, 1);
	}

	shutdown.cancel();
}
