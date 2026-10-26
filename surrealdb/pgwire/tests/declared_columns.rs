// Awaiting a datastore constructor lays out the sealed async bodies it reaches, which nest deeper
// than the default recursion limit.
#![recursion_limit = "256"]

//! What a client sees of a table whose columns the datastore declares.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// A server over a datastore whose `test`/`test` database `setup` has defined, in SurrealQL.
async fn start(setup: &str) -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("a datastore");
	let session = Session::owner().with_ns("test").with_db("test");
	let setup = format!("DEFINE NAMESPACE test; DEFINE DATABASE test; {setup}");
	for result in ds.execute(&setup, &session, None).await.expect("a setup") {
		result.result.expect("a setup statement");
	}
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();
	let bind_addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("an address");
	let addr = surrealdb_pgwire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("a pgwire server");
	(addr, shutdown)
}

type Message = (u8, Vec<u8>);

async fn read_until_ready(stream: &mut TcpStream) -> Vec<Message> {
	let mut messages = Vec::new();
	let res = timeout(Duration::from_secs(10), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			messages.push((msg_type, payload));
			if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok(), "timed out waiting for ReadyForQuery");
	messages
}

async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("a connection");
	let mut body = BytesMut::new();
	body.put_i32(196608);
	for (key, value) in
		[("user", "root"), ("database", "test/test"), ("options", "-c dialect=pgsql")]
	{
		body.put_slice(key.as_bytes());
		body.put_u8(0);
		body.put_slice(value.as_bytes());
		body.put_u8(0);
	}
	body.put_u8(0);
	let mut packet = BytesMut::new();
	packet.put_i32((body.len() + 4) as i32);
	packet.put(body);
	stream.write_all(&packet).await.expect("a startup message");
	for (kind, payload) in read_until_ready(&mut stream).await {
		assert_ne!(kind, b'E', "{:?}", error_fields(&payload));
	}
	stream
}

fn error_fields(payload: &[u8]) -> (String, String) {
	let (mut code, mut message) = (String::new(), String::new());
	for field in payload.split(|byte| *byte == 0) {
		let Some((&kind, text)) = field.split_first() else {
			break;
		};
		match kind {
			b'C' => code = String::from_utf8_lossy(text).into_owned(),
			b'M' => message = String::from_utf8_lossy(text).into_owned(),
			_ => {}
		}
	}
	(code, message)
}

/// What a client sees of one statement: the `(name, type oid)` of each column it was told of, the
/// text of each cell, and the error, as `(SQLSTATE, message)`.
#[derive(Debug, Default)]
struct Reply {
	columns: Option<Vec<(String, i32)>>,
	rows: Vec<Vec<Option<String>>>,
	error: Option<(String, String)>,
}

impl Reply {
	fn names(&self) -> Vec<&str> {
		self.columns
			.as_ref()
			.map(|columns| columns.iter().map(|(name, _)| name.as_str()).collect())
			.unwrap_or_default()
	}

	fn oid(&self, name: &str) -> i32 {
		let columns = self.columns.as_ref().expect("a RowDescription");
		columns.iter().find(|(column, _)| column == name).expect("the column").1
	}
}

fn reply(messages: Vec<Message>) -> Reply {
	let mut reply = Reply::default();
	for (kind, payload) in messages {
		let mut cur = &payload[..];
		match kind {
			b'E' => reply.error = Some(error_fields(&payload)),
			b'T' => {
				let count = cur.get_i16();
				let mut columns = Vec::new();
				for _ in 0..count {
					let end = cur.iter().position(|b| *b == 0).expect("a column name");
					let name = String::from_utf8_lossy(&cur[..end]).into_owned();
					cur.advance(end + 1 + 4 + 2);
					let oid = cur.get_i32();
					cur.advance(2 + 4 + 2);
					columns.push((name, oid));
				}
				reply.columns = Some(columns);
			}
			b'D' => {
				let count = cur.get_i16();
				reply.rows.push(
					(0..count)
						.map(|_| {
							let len = cur.get_i32();
							(len >= 0).then(|| {
								let cell =
									String::from_utf8_lossy(&cur[..len as usize]).into_owned();
								cur.advance(len as usize);
								cell
							})
						})
						.collect(),
				);
			}
			_ => {}
		}
	}
	reply
}

/// `sql` as a simple query.
async fn simple(stream: &mut TcpStream, sql: &str) -> Reply {
	let mut query = sql.as_bytes().to_vec();
	query.push(0);
	let mut packet = BytesMut::new();
	packet.put_u8(b'Q');
	packet.put_i32((query.len() + 4) as i32);
	packet.put_slice(&query);
	stream.write_all(&packet).await.expect("a query");
	reply(read_until_ready(stream).await)
}

fn message(kind: u8, body: &[u8]) -> BytesMut {
	let mut packet = BytesMut::new();
	packet.put_u8(kind);
	packet.put_i32((body.len() + 4) as i32);
	packet.put_slice(body);
	packet
}

/// `sql` as the extended protocol's Parse, Describe of the statement, and Sync: what a driver is
/// told of the columns before it binds anything.
async fn prepared(stream: &mut TcpStream, name: &str, sql: &str) -> Reply {
	let mut parse = BytesMut::new();
	parse.put_slice(name.as_bytes());
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(0);
	let mut describe = BytesMut::new();
	describe.put_u8(b'S');
	describe.put_slice(name.as_bytes());
	describe.put_u8(0);
	for packet in [message(b'P', &parse), message(b'D', &describe), message(b'S', &[])] {
		stream.write_all(&packet).await.expect("a message");
	}
	reply(read_until_ready(stream).await)
}

/// `sql` as the extended protocol's Parse, Bind, Describe of the portal, Execute and Sync, with
/// every column in text format.
async fn extended(stream: &mut TcpStream, sql: &str) -> Reply {
	let mut parse = BytesMut::new();
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(0);
	let mut bind = BytesMut::new();
	bind.put_u8(0);
	bind.put_u8(0);
	bind.put_i16(0);
	bind.put_i16(0);
	bind.put_i16(0);
	let describe = [b'P', 0];
	let mut execute = BytesMut::new();
	execute.put_u8(0);
	execute.put_i32(0);
	for packet in [
		message(b'P', &parse),
		message(b'B', &bind),
		message(b'D', &describe),
		message(b'E', &execute),
		message(b'S', &[]),
	] {
		stream.write_all(&packet).await.expect("a message");
	}
	reply(read_until_ready(stream).await)
}

/// The `(name, type oid)` of the columns of `typed_cols` and `empty_cols`, which are declared
/// `id INT PRIMARY KEY, name TEXT, score FLOAT, ok BOOLEAN`: the key first, then by name.
fn typed_columns() -> Vec<(String, i32)> {
	[("id", 23), ("name", 25), ("ok", 16), ("score", 701)]
		.map(|(name, oid)| (name.to_owned(), oid))
		.to_vec()
}

async fn typed_tables(stream: &mut TcpStream) {
	for sql in [
		"CREATE TABLE typed_cols (id INT PRIMARY KEY, name TEXT, score FLOAT, ok BOOLEAN)",
		"CREATE TABLE empty_cols (id INT PRIMARY KEY, name TEXT, score FLOAT, ok BOOLEAN)",
		"INSERT INTO typed_cols (id, name, score, ok) VALUES (1, 'a', 2.5, true)",
	] {
		let reply = simple(stream, sql).await;
		assert!(reply.error.is_none(), "{sql}: {reply:?}");
	}
}

#[tokio::test]
async fn a_star_is_told_as_the_columns_the_table_declares() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;
	let sql = "SELECT * FROM typed_cols";

	let reply = simple(&mut stream, sql).await;
	assert_eq!(reply.columns, Some(typed_columns()));
	assert_eq!(reply.rows, [["1", "a", "t", "2.5"].map(|cell| Some(cell.to_owned())).to_vec()]);

	// What a driver is told before it binds anything is what the rows then have.
	let reply = prepared(&mut stream, "star", sql).await;
	assert_eq!(reply.columns, Some(typed_columns()));
	let reply = extended(&mut stream, sql).await;
	assert_eq!(reply.columns, Some(typed_columns()));
	assert_eq!(reply.rows.len(), 1);
	shutdown.cancel();
}

#[tokio::test]
async fn a_star_over_a_table_with_no_rows_is_told_as_the_declared_columns() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;
	let sql = "SELECT * FROM empty_cols";

	let reply = simple(&mut stream, sql).await;
	assert_eq!(reply.columns, Some(typed_columns()));
	assert!(reply.rows.is_empty());
	let reply = prepared(&mut stream, "star", sql).await;
	assert_eq!(reply.columns, Some(typed_columns()));
	shutdown.cancel();
}

#[tokio::test]
async fn a_named_column_is_typed_as_the_table_declares_it() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;

	let reply = prepared(&mut stream, "named", "SELECT score, name FROM empty_cols").await;
	assert_eq!(reply.names(), ["score", "name"]);
	assert_eq!((reply.oid("score"), reply.oid("name")), (701, 25));
	shutdown.cancel();
}

#[tokio::test]
async fn a_column_the_table_does_not_declare_is_an_error() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;
	let sql = "SELECT name, nosuch FROM typed_cols";

	let reply = simple(&mut stream, sql).await;
	assert_eq!(
		reply.error,
		Some(("42703".to_owned(), "column \"nosuch\" does not exist".to_owned()))
	);
	assert!(reply.rows.is_empty());

	let reply = extended(&mut stream, sql).await;
	let (_, message) = reply.error.expect("the extended protocol refuses the column too");
	assert!(message.contains("\"nosuch\""), "{message}");

	// The connection goes on to serve statements that are valid.
	let reply = simple(&mut stream, "SELECT name FROM typed_cols").await;
	assert_eq!(reply.rows.len(), 1);
	shutdown.cancel();
}

#[tokio::test]
async fn a_table_that_does_not_declare_all_its_columns_reads_any_name() {
	let (addr, shutdown) = start(
		"DEFINE TABLE loose SCHEMALESS;
		 CREATE loose:1 SET a = 1, b = 'x' RETURN NONE;
		 DEFINE TABLE partial SCHEMALESS;
		 DEFINE FIELD a ON partial TYPE int;
		 CREATE partial:1 SET a = 1, extra = 'e' RETURN NONE;",
	)
	.await;
	let mut stream = connect(addr).await;

	let reply = simple(&mut stream, "SELECT a, nosuch FROM loose").await;
	assert_eq!(reply.error, None);
	assert_eq!(reply.names(), ["a", "nosuch"]);
	assert_eq!(reply.rows, [[Some("1".to_owned()), None]]);

	// A field that is defined on a table that may hold others is not all its columns.
	let reply = simple(&mut stream, "SELECT extra FROM partial").await;
	assert_eq!(reply.error, None);
	assert_eq!(reply.rows, [[Some("e".to_owned())]]);

	let reply = simple(&mut stream, "SELECT * FROM loose").await;
	assert_eq!(reply.error, None);
	assert_eq!(reply.rows.len(), 1);
	shutdown.cancel();
}

#[tokio::test]
async fn a_default_in_a_write_to_a_table_that_declares_its_columns_is_not_a_column() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	typed_tables(&mut stream).await;

	for (statement, sql) in [
		("insert", "INSERT INTO typed_cols (id, name) VALUES (2, DEFAULT) RETURNING id, name"),
		("update", "UPDATE typed_cols SET name = DEFAULT WHERE id = 1 RETURNING id"),
	] {
		let reply = prepared(&mut stream, statement, sql).await;
		assert_eq!(reply.error, None, "{sql}");
		let reply = simple(&mut stream, sql).await;
		assert_eq!(reply.error, None, "{sql}");
	}

	// A column the table does not declare is refused when the write is described.
	let reply =
		prepared(&mut stream, "refused", "INSERT INTO typed_cols (id, nosuch) VALUES (3, 1)").await;
	let (code, message) = reply.error.expect("the column is refused");
	assert_eq!(code, "42703");
	assert!(message.contains("\"nosuch\""), "{message}");
	shutdown.cancel();
}
