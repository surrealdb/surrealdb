// Awaiting a datastore constructor lays out the sealed async bodies it reaches, which nest deeper
// than the default recursion limit.
#![recursion_limit = "256"]

//! What a client reads back of a row that an `INSERT` leaves a column out of.

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

/// What a client sees of one statement: the text of each cell, and the error, as `(SQLSTATE,
/// message)`.
#[derive(Debug, Default)]
struct Reply {
	rows: Vec<Vec<Option<String>>>,
	error: Option<(String, String)>,
}

fn reply(messages: Vec<Message>) -> Reply {
	let mut reply = Reply::default();
	for (kind, payload) in messages {
		let mut cur = &payload[..];
		match kind {
			b'E' => reply.error = Some(error_fields(&payload)),
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

/// The `rows` of `sql`, sorted: a NULL is `None`, and sorts first.
async fn rows(stream: &mut TcpStream, sql: &str) -> Vec<Vec<Option<String>>> {
	let reply = simple(stream, sql).await;
	assert!(reply.error.is_none(), "{sql}: {reply:?}");
	let mut rows = reply.rows;
	rows.sort();
	rows
}

async fn run(stream: &mut TcpStream, statements: &[&str]) {
	for sql in statements {
		let reply = simple(stream, sql).await;
		assert!(reply.error.is_none(), "{sql}: {reply:?}");
	}
}

fn cells(rows: &[&[Option<&str>]]) -> Vec<Vec<Option<String>>> {
	rows.iter().map(|row| row.iter().map(|cell| cell.map(str::to_owned)).collect()).collect()
}

#[tokio::test]
async fn a_row_that_leaves_a_column_out_and_a_row_that_gives_it_null_are_one_value() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO t (id, a) VALUES (1, 10)",
			"INSERT INTO t (id, a, b) VALUES (2, 20, NULL)",
			"INSERT INTO t (id, a, b) VALUES (3, 30, 'x')",
		],
	)
	.await;

	let both = cells(&[&[None], &[Some("x")]]);
	for sql in ["SELECT b FROM t GROUP BY b", "SELECT b FROM t UNION SELECT b FROM t"] {
		assert_eq!(rows(&mut stream, sql).await, both, "{sql}");
	}
	assert_eq!(
		rows(&mut stream, "SELECT count(*) FROM t WHERE b IS NULL").await,
		cells(&[&[Some("2")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn a_column_with_a_default_keeps_the_default_when_it_is_left_out() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE d (id INT PRIMARY KEY, n INT DEFAULT 7, m INT)",
			"INSERT INTO d (id) VALUES (1)",
		],
	)
	.await;
	assert_eq!(rows(&mut stream, "SELECT n, m FROM d").await, cells(&[&[Some("7"), None]]));
	shutdown.cancel();
}

#[tokio::test]
async fn a_not_null_column_that_is_left_out_is_still_refused() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(&mut stream, &["CREATE TABLE r (id INT PRIMARY KEY, n INT NOT NULL)"]).await;
	let reply = simple(&mut stream, "INSERT INTO r (id) VALUES (1)").await;
	assert!(reply.error.is_some(), "{reply:?}");
	assert!(rows(&mut stream, "SELECT id FROM r").await.is_empty());
	shutdown.cancel();
}

#[tokio::test]
async fn default_values_write_null_to_every_column_with_no_default() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE e (id SERIAL PRIMARY KEY, n INT, s TEXT)",
			"INSERT INTO e DEFAULT VALUES",
			"INSERT INTO e (n) VALUES (NULL)",
		],
	)
	.await;
	assert_eq!(rows(&mut stream, "SELECT n FROM e GROUP BY n").await, cells(&[&[None]]));
	shutdown.cancel();
}

#[tokio::test]
async fn the_row_an_upsert_inserts_leaves_no_column_out() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE u (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO u (id, a) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a",
			"INSERT INTO u (id, a, b) VALUES (2, 20, NULL)",
		],
	)
	.await;
	assert_eq!(rows(&mut stream, "SELECT b FROM u GROUP BY b").await, cells(&[&[None]]));
	shutdown.cancel();
}

#[tokio::test]
async fn a_field_with_no_type_holds_null_when_it_is_left_out() {
	let (addr, shutdown) = start(
		"DEFINE TABLE loose SCHEMAFULL PERMISSIONS FULL; \
		 DEFINE FIELD a ON loose TYPE none | null | int; \
		 DEFINE FIELD x ON loose;",
	)
	.await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"INSERT INTO loose (id, a) VALUES (1, 10)",
			"INSERT INTO loose (id, a, x) VALUES (2, 20, NULL)",
			"INSERT INTO loose (id, a, x) VALUES (3, 30, 'x')",
		],
	)
	.await;
	assert_eq!(
		rows(&mut stream, "SELECT x FROM loose GROUP BY x").await,
		cells(&[&[None], &[Some("x")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn the_rows_an_insert_from_a_select_writes_leave_no_column_out() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE src (id INT PRIMARY KEY, a INT)",
			"INSERT INTO src (id, a) VALUES (1, 10), (2, 20)",
			"CREATE TABLE dst (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO dst (id, a) SELECT id, a FROM src",
			"INSERT INTO dst (id, a, b) VALUES (3, 30, NULL)",
		],
	)
	.await;
	assert_eq!(rows(&mut stream, "SELECT b FROM dst GROUP BY b").await, cells(&[&[None]]));
	shutdown.cancel();
}

#[tokio::test]
async fn an_upsert_with_a_condition_reads_a_column_the_proposed_row_leaves_out_as_null() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE w (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO w (id, a, b) VALUES (1, 1, 'old'), (2, 1, 'old2')",
			"INSERT INTO w (id, a) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET b = EXCLUDED.b \
			 WHERE w.a < 5",
			"INSERT INTO w (id, a) VALUES (2, 3) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a \
			 WHERE EXCLUDED.b IS NULL",
		],
	)
	.await;
	assert_eq!(
		rows(&mut stream, "SELECT id, a, b FROM w").await,
		cells(&[&[Some("1"), Some("1"), None], &[Some("2"), Some("3"), Some("old2")]])
	);
	// The row 1 was given `EXCLUDED.b`: it holds NULL, as a row that is given NULL does.
	run(&mut stream, &["INSERT INTO w (id, a, b) VALUES (3, 3, NULL)"]).await;
	assert_eq!(
		rows(&mut stream, "SELECT b FROM w GROUP BY b").await,
		cells(&[&[None], &[Some("old2")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn an_insert_from_a_select_with_no_column_list_keeps_what_it_selects() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE src (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO src (id, a, b) VALUES (1, 10, 'x')",
			"CREATE TABLE dst (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO dst SELECT id, a, b FROM src",
		],
	)
	.await;
	assert_eq!(
		rows(&mut stream, "SELECT id, a, b FROM dst").await,
		cells(&[&[Some("1"), Some("10"), Some("x")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn an_insert_from_a_star_fills_the_columns_it_leaves_out() {
	let (addr, shutdown) = start("").await;
	let mut stream = connect(addr).await;
	run(
		&mut stream,
		&[
			"CREATE TABLE src (id INT PRIMARY KEY, a INT)",
			"INSERT INTO src (id, a) VALUES (1, 10), (2, 20)",
			"CREATE TABLE dst (id INT PRIMARY KEY, a INT, b TEXT)",
			"INSERT INTO dst (id, a) SELECT * FROM src",
			"INSERT INTO dst (id, a, b) VALUES (3, 30, NULL)",
		],
	)
	.await;
	assert_eq!(rows(&mut stream, "SELECT b FROM dst GROUP BY b").await, cells(&[&[None]]));
	shutdown.cancel();
}
