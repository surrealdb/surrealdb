#![recursion_limit = "256"]

//! What a MySQL client sees of a table whose columns the datastore declares.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("a packet header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let mut payload = vec![0u8; len];
	stream.read_exact(&mut payload).await.expect("a packet payload");
	payload
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

/// The `(name, MySQL type)` of a column definition packet.
fn column(definition: &[u8]) -> (String, u8) {
	let mut off = 0;
	for _ in 0..4 {
		lenenc(definition, &mut off);
	}
	let name = String::from_utf8_lossy(&lenenc(definition, &mut off)).into_owned();
	lenenc(definition, &mut off);
	off += 1 + 2 + 4;
	(name, definition[off])
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

/// What a client sees of one statement: the `(name, MySQL type)` of each column it was told of,
/// the text of each cell, and the error as `(code, message)`.
#[derive(Debug, Default)]
struct Reply {
	columns: Vec<(String, u8)>,
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
		reply.columns.push(column(&read_packet(stream).await));
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

/// The columns a COM_STMT_PREPARE of `sql` announces.
async fn prepared(stream: &mut TcpStream, sql: &str) -> Reply {
	let mut command = vec![COM_STMT_PREPARE];
	command.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &command).await;
	let first = read_packet(stream).await;
	let mut reply = Reply::default();
	if first[0] == 0xff {
		reply.error =
			Some((u16::from(first[1]) | (u16::from(first[2]) << 8), message(&first[3..])));
		return reply;
	}
	let columns = u16::from_le_bytes([first[5], first[6]]);
	let params = u16::from_le_bytes([first[7], first[8]]);
	for _ in 0..params {
		read_packet(stream).await;
	}
	if params > 0 {
		assert!(is_eof(&read_packet(stream).await));
	}
	for _ in 0..columns {
		reply.columns.push(column(&read_packet(stream).await));
	}
	if columns > 0 {
		assert!(is_eof(&read_packet(stream).await));
	}
	reply
}

/// The `(name, MySQL type)` of the columns of `typed_cols` and `empty_cols`, which are declared
/// `id INT PRIMARY KEY, name VARCHAR(50), score DOUBLE, ok BOOLEAN`: the key first, then by name.
fn typed_columns() -> Vec<(String, u8)> {
	[("id", 3), ("name", 253), ("ok", 1), ("score", 5)]
		.map(|(name, kind)| (name.to_owned(), kind))
		.to_vec()
}

async fn typed_tables(stream: &mut TcpStream) {
	for sql in [
		"CREATE TABLE typed_cols (id INT PRIMARY KEY, name VARCHAR(50), score DOUBLE, ok BOOLEAN)",
		"CREATE TABLE empty_cols (id INT PRIMARY KEY, name VARCHAR(50), score DOUBLE, ok BOOLEAN)",
		"INSERT INTO typed_cols (id, name, score, ok) VALUES (1, 'a', 2.5, true)",
	] {
		let reply = query(stream, sql).await;
		assert!(reply.error.is_none(), "{sql}: {reply:?}");
	}
}

#[tokio::test]
async fn a_star_is_told_as_the_columns_the_table_declares() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;

	for sql in ["SELECT * FROM typed_cols", "SELECT t.* FROM typed_cols t"] {
		let reply = query(&mut stream, sql).await;
		assert_eq!(reply.columns, typed_columns(), "{sql}");
		assert_eq!(
			reply.rows,
			[["1", "a", "1", "2.5"].map(|cell| Some(cell.to_owned())).to_vec()],
			"{sql}"
		);
		// What a client is told when it prepares is what the rows then have.
		assert_eq!(prepared(&mut stream, sql).await.columns, typed_columns(), "{sql}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_star_over_a_table_with_no_rows_is_told_as_the_declared_columns() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;
	let sql = "SELECT * FROM empty_cols";

	let reply = query(&mut stream, sql).await;
	assert_eq!(reply.columns, typed_columns());
	assert!(reply.rows.is_empty());
	assert_eq!(prepared(&mut stream, sql).await.columns, typed_columns());
	shutdown.cancel();
}

#[tokio::test]
async fn a_named_column_is_typed_as_the_table_declares_it() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;

	let reply = prepared(&mut stream, "SELECT score, name FROM empty_cols").await;
	assert_eq!(reply.columns, [("score".to_owned(), 5), ("name".to_owned(), 253)]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_column_the_table_does_not_declare_is_an_error() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;

	let reply = query(&mut stream, "SELECT name, nosuch FROM typed_cols").await;
	let (_, message) = reply.error.expect("the column is refused");
	assert!(message.contains("\"nosuch\""), "{message}");
	assert!(reply.rows.is_empty());

	// The connection goes on to serve statements that are valid.
	let reply = query(&mut stream, "SELECT name FROM typed_cols").await;
	assert_eq!(reply.rows.len(), 1);
	shutdown.cancel();
}

#[tokio::test]
async fn a_table_that_does_not_declare_all_its_columns_reads_any_name() {
	let (addr, shutdown) = start(
		"DEFINE TABLE loose SCHEMALESS;
		 CREATE loose:1 SET a = 1, b = 'x' RETURN NONE;",
	)
	.await;
	let mut stream = connect(addr).await;

	let reply = query(&mut stream, "SELECT a, nosuch FROM loose").await;
	assert_eq!(reply.error, None);
	assert_eq!(reply.rows, [[Some("1".to_owned()), None]]);

	let reply = query(&mut stream, "SELECT * FROM loose").await;
	assert_eq!(reply.error, None);
	assert_eq!(reply.rows.len(), 1);
	shutdown.cancel();
}

/// The `(name, type, length, flags, decimals)` of a column definition packet.
fn column_definition(definition: &[u8]) -> (String, u8, u32, u16, u8) {
	let mut off = 0;
	for _ in 0..4 {
		lenenc(definition, &mut off);
	}
	let name = String::from_utf8_lossy(&lenenc(definition, &mut off)).into_owned();
	lenenc(definition, &mut off);
	off += 1 + 2;
	let length = u32::from_le_bytes(definition[off..off + 4].try_into().expect("a length"));
	off += 4;
	let ty = definition[off];
	let flags = u16::from_le_bytes([definition[off + 1], definition[off + 2]]);
	(name, ty, length, flags, definition[off + 3])
}

#[tokio::test]
async fn a_declared_column_is_defined_by_its_mysql_type_and_its_values_by_that_type() {
	let (addr, shutdown) = start(
		"DEFINE TABLE ytypes SCHEMAFULL; \
		 DEFINE FIELD y ON ytypes TYPE int SQL TYPE MYSQL 'YEAR'; \
		 DEFINE FIELD b ON ytypes TYPE int SQL TYPE MYSQL 'BIT(8)'; \
		 DEFINE FIELD d ON ytypes TYPE decimal SQL TYPE MYSQL 'DECIMAL(10,2)'; \
		 DEFINE FIELD dt ON ytypes TYPE datetime SQL TYPE MYSQL 'DATETIME(3)'; \
		 DEFINE FIELD v ON ytypes TYPE string SQL TYPE POSTGRESQL 'varchar(20)'; \
		 CREATE ytypes:1 SET y = 2026, b = 165, d = 12.5dec, dt = d'2026-10-30T12:00:00.5Z', \
		 v = 'ab';",
	)
	.await;
	let mut stream = connect(addr).await;
	let mut command = vec![COM_QUERY];
	command.extend_from_slice(b"SELECT y, b, d, dt, v FROM ytypes");
	write_packet(&mut stream, 0, &command).await;
	let first = read_packet(&mut stream).await;
	assert_eq!(first[0], 5, "{first:?}");
	let mut columns = Vec::new();
	for _ in 0..5 {
		columns.push(column_definition(&read_packet(&mut stream).await));
	}
	assert!(is_eof(&read_packet(&mut stream).await));
	let row = read_packet(&mut stream).await;
	assert!(is_eof(&read_packet(&mut stream).await));

	let year_flags = UNSIGNED_FLAG | ZEROFILL_FLAG;
	assert_eq!(columns[0], ("y".to_owned(), MYSQL_TYPE_YEAR, 4, year_flags, 0));
	assert_eq!(columns[1], ("b".to_owned(), MYSQL_TYPE_BIT, 8, UNSIGNED_FLAG, 0));
	assert_eq!(columns[2], ("d".to_owned(), MYSQL_TYPE_NEWDECIMAL, 12, 0, 2));
	assert_eq!(columns[3], ("dt".to_owned(), MYSQL_TYPE_DATETIME, 23, 0, 3));
	// A PostgreSQL declaration reads as the nearest MySQL type, four bytes per character.
	assert_eq!(columns[4], ("v".to_owned(), MYSQL_TYPE_VAR_STRING, 80, 0, 0));

	let mut off = 0;
	let cells: Vec<Vec<u8>> = (0..5).map(|_| lenenc(&row, &mut off)).collect();
	assert_eq!(cells[0], b"2026");
	// A BIT(8) value is its one byte.
	assert_eq!(cells[1], [0xa5]);
	assert_eq!(cells[2], b"12.50");
	assert_eq!(cells[3], b"2026-10-30 12:00:00.500");
	assert_eq!(cells[4], b"ab");
	shutdown.cancel();
}
