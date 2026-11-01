#![recursion_limit = "256"]

//! What a MySQL client reads back of a row that a `REPLACE` finds and leaves columns out of.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// A server over a datastore whose `test`/`test` database `setup` has defined, in SurrealQL.
async fn start(setup: &str) -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("a datastore");
	ds.run(QueryRequest::new(
		"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
		&Session::owner(),
	))
	.await
	.expect("a root user");
	let session = Session::owner().with_ns("test").with_db("test");
	let setup = format!("DEFINE NAMESPACE test; DEFINE DATABASE test; {setup}");
	for result in ds.execute(&setup, &session, None).await.expect("a setup") {
		result.result.expect("a setup statement");
	}
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();
	let bind_addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("an address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("a mywire server");
	(addr, shutdown)
}

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	timeout(Duration::from_secs(10), async {
		let mut header = [0u8; 4];
		stream.read_exact(&mut header).await.expect("a packet header");
		let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
		let mut payload = vec![0u8; len];
		stream.read_exact(&mut payload).await.expect("a packet payload");
		payload
	})
	.await
	.expect("a packet within 10 s")
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("a packet header");
	stream.write_all(payload).await.expect("a packet payload");
}

async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("a connection");
	let handshake = read_packet(&mut stream).await;
	let mut pos = 1;
	while handshake[pos] != 0 {
		pos += 1;
	}
	pos += 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake[pos..pos + 8]);
	pos += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..20].copy_from_slice(&handshake[pos..pos + 12]);

	let scramble = compute_mysql_native_password_scramble("root", &salt);
	let mut response = Vec::new();
	let caps: u32 =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	response.extend_from_slice(&caps.to_le_bytes());
	response.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	response.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	response.extend_from_slice(&[0u8; 23]);
	response.extend_from_slice(b"root\0");
	response.push(20);
	response.extend_from_slice(&scramble);
	response.extend_from_slice(b"test/test\0");
	response.extend_from_slice(b"mysql_native_password\0");
	write_packet(&mut stream, 1, &response).await;

	let reply = read_packet(&mut stream).await;
	if reply[0] == 0xfe {
		write_packet(&mut stream, 3, b"root\0").await;
		let reply = read_packet(&mut stream).await;
		assert_eq!(reply[0], 0x00, "{}", String::from_utf8_lossy(&reply));
	} else {
		assert_eq!(reply[0], 0x00, "{}", String::from_utf8_lossy(&reply));
	}
	stream
}

fn lenenc(buf: &[u8], off: &mut usize) -> Vec<u8> {
	let first = buf[*off];
	let (len, width) = match first {
		0xfc => ((buf[*off + 1] as usize) | ((buf[*off + 2] as usize) << 8), 3),
		0xfd => (
			(buf[*off + 1] as usize)
				| ((buf[*off + 2] as usize) << 8)
				| ((buf[*off + 3] as usize) << 16),
			4,
		),
		_ => (first as usize, 1),
	};
	*off += width;
	let bytes = buf[*off..*off + len].to_vec();
	*off += len;
	bytes
}

fn text_row(packet: &[u8]) -> Vec<Option<String>> {
	let mut off = 0;
	let mut cells = Vec::new();
	while off < packet.len() {
		if packet[off] == 0xfb {
			off += 1;
			cells.push(None);
		} else {
			cells.push(Some(String::from_utf8_lossy(&lenenc(packet, &mut off)).into_owned()));
		}
	}
	cells
}

/// What a client sees of one statement: the text of each cell, and the error as `(code, message)`.
#[derive(Debug, Default)]
struct Reply {
	rows: Vec<Vec<Option<String>>>,
	error: Option<(u16, String)>,
}

fn is_eof(packet: &[u8]) -> bool {
	packet[0] == 0xfe && packet.len() < 9
}

async fn query(stream: &mut TcpStream, sql: &str) -> Reply {
	let mut command = vec![COM_QUERY];
	command.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &command).await;
	let first = read_packet(stream).await;
	let mut reply = Reply::default();
	if first[0] == 0xff {
		reply.error =
			Some((u16::from(first[1]) | (u16::from(first[2]) << 8), message(&first[3..])));
		return reply;
	}
	if first[0] == 0x00 {
		return reply;
	}
	for _ in 0..first[0] {
		read_packet(stream).await;
	}
	assert!(is_eof(&read_packet(stream).await));
	loop {
		let packet = read_packet(stream).await;
		if is_eof(&packet) {
			break;
		}
		reply.rows.push(text_row(&packet));
	}
	reply
}

fn message(error: &[u8]) -> String {
	// The SQL state marker `#` and its five bytes come first when the client asked for protocol
	// 4.1.
	let text = if error.first() == Some(&b'#') {
		&error[6..]
	} else {
		error
	};
	String::from_utf8_lossy(text).into_owned()
}

/// The `rows` of `sql`, sorted: a NULL is `None`, and sorts first.
async fn rows(stream: &mut TcpStream, sql: &str) -> Vec<Vec<Option<String>>> {
	let reply = query(stream, sql).await;
	assert!(reply.error.is_none(), "{sql}: {reply:?}");
	let mut rows = reply.rows;
	rows.sort();
	rows
}

async fn run(stream: &mut TcpStream, statements: &[&str]) {
	for sql in statements {
		let reply = query(stream, sql).await;
		assert!(reply.error.is_none(), "{sql}: {reply:?}");
	}
}

fn cells(rows: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
	rows.iter().map(|row| row.iter().map(|cell| cell.map(str::to_owned)).collect()).collect()
}

#[tokio::test]
async fn a_replace_over_an_existing_row_resets_the_columns_it_leaves_out() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE r (id INT PRIMARY KEY, a INT, b VARCHAR(10), d INT DEFAULT 7)",
			"INSERT INTO r (id, a, b, d) VALUES (1, 10, 'x', 9)",
			"REPLACE INTO r (id, a) VALUES (1, 11)",
		],
	)
	.await;
	assert_eq!(
		rows(&mut stream, "SELECT id, a, b, d FROM r").await,
		cells(&[&[Some("1"), Some("11"), None, Some("7")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn an_insert_that_updates_on_a_duplicate_key_keeps_the_columns_it_does_not_assign() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE r (id INT PRIMARY KEY, a INT, b VARCHAR(10), d INT DEFAULT 7)",
			"INSERT INTO r (id, a, b, d) VALUES (2, 20, 'y', 9)",
			"INSERT INTO r (id, a) VALUES (2, 21) ON DUPLICATE KEY UPDATE a = 22",
		],
	)
	.await;
	assert_eq!(
		rows(&mut stream, "SELECT id, a, b, d FROM r").await,
		cells(&[&[Some("2"), Some("22"), Some("y"), Some("9")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn a_replace_over_an_existing_row_numbers_a_column_it_leaves_out_anew() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE s (id INT PRIMARY KEY, n INT AUTO_INCREMENT UNIQUE, a INT)",
			"INSERT INTO s (id, a) VALUES (1, 1)",
			"INSERT INTO s (id, a) VALUES (2, 1)",
			"REPLACE INTO s (id, a) VALUES (1, 2)",
		],
	)
	.await;
	// MySQL numbers the row 3. The insert that meets the existing row draws a number before it
	// does, so the row here is numbered later; what holds in both is that it is numbered anew.
	assert_eq!(
		rows(&mut stream, "SELECT id, a FROM s WHERE n >= 3").await,
		cells(&[&[Some("1"), Some("2")]])
	);
	assert_eq!(
		rows(&mut stream, "SELECT id, n, a FROM s WHERE id = 2").await,
		cells(&[&[Some("2"), Some("2"), Some("1")]])
	);
	shutdown.cancel();
}
