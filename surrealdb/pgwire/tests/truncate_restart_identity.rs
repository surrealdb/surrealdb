//! `TRUNCATE` leaves the numbering of the `SERIAL` columns of its table running, and `TRUNCATE ...
//! RESTART IDENTITY` starts it over, through the simple and the extended protocol, and inside a
//! transaction that is committed or rolled back.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// The protocol version 3.0 of a `StartupMessage`.
const PROTOCOL_3_0: i32 = 196608;
/// Bound on every network exchange, so a server that stops answering fails the test instead of
/// hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on the body of a message read from the server, so a corrupt length fails the test
/// instead of allocating to it.
const MAX_MESSAGE_BODY: usize = 1 << 20;

async fn start_test_pg_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let shutdown = CancellationToken::new();
	let addr = surrealdb_pgwire::start(
		"127.0.0.1:0".parse().expect("Failed to parse socket address"),
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("Failed to start pgwire server");
	(addr, shutdown)
}

/// Reads one message: its type and its body.
async fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	timeout(IO_TIMEOUT, async {
		let msg_type = stream.read_u8().await?;
		let len = stream.read_i32().await?;
		let body_len = usize::try_from(len - 4).expect("a message length covers its own field");
		assert!(body_len <= MAX_MESSAGE_BODY, "message of {body_len} bytes");
		let mut body = vec![0u8; body_len];
		stream.read_exact(&mut body).await?;
		Ok::<_, std::io::Error>((msg_type, body))
	})
	.await
	.expect("Timed out reading a message")
	.expect("Failed to read a message")
}

/// Connects as `root` to `test/test` in the PostgreSQL dialect.
async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to pgwire server");

	let mut body = BytesMut::new();
	body.put_i32(PROTOCOL_3_0);
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
	stream.write_all(&packet).await.expect("Failed to send the startup message");

	loop {
		let (msg_type, body) = read_message(&mut stream).await;
		match msg_type {
			b'Z' => return stream,
			b'E' => panic!("The handshake failed: {}", String::from_utf8_lossy(&body)),
			_ => {}
		}
	}
}

/// The text of the first column of a `DataRow`, `None` for NULL.
fn first_cell(body: &[u8]) -> Option<String> {
	let mut cur = body;
	let columns = cur.get_i16();
	assert!(columns >= 1, "a row with no column");
	let len = usize::try_from(cur.get_i32()).ok()?;
	Some(String::from_utf8(cur[..len].to_vec()).expect("a text column"))
}

/// Sends `sql` as one simple query and returns the first column of every row it answers, or the
/// server's error.
async fn query(stream: &mut TcpStream, sql: &str) -> Result<Vec<Option<String>>, String> {
	let mut packet = BytesMut::new();
	packet.put_u8(b'Q');
	packet.put_i32((sql.len() + 5) as i32);
	packet.put_slice(sql.as_bytes());
	packet.put_u8(0);
	stream.write_all(&packet).await.expect("Failed to send the query");

	let mut rows = Vec::new();
	let mut error = None;
	loop {
		let (msg_type, body) = read_message(stream).await;
		match msg_type {
			b'Z' => break,
			b'D' => rows.push(first_cell(&body)),
			b'E' => error = Some(String::from_utf8_lossy(&body).into_owned()),
			_ => {}
		}
	}
	match error {
		Some(error) => Err(error),
		None => Ok(rows),
	}
}

/// The one number `sql` answers.
async fn number(stream: &mut TcpStream, sql: &str) -> i64 {
	let rows = query(stream, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
	let [Some(cell)] = rows.as_slice() else {
		panic!("{sql}: expected one number: {rows:?}");
	};
	cell.parse().unwrap_or_else(|_| panic!("{sql}: not a number: {rows:?}"))
}

/// Runs `sql`, which must succeed.
async fn run(stream: &mut TcpStream, sql: &str) {
	query(stream, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Runs `sql` as one prepared statement of the extended protocol (Parse, Bind, Execute, Sync) and
/// returns the first column of every row it answers, or the server's error.
async fn query_extended(stream: &mut TcpStream, sql: &str) -> Result<Vec<Option<String>>, String> {
	let mut packet = BytesMut::new();

	let mut body = BytesMut::new();
	body.put_u8(0); // the unnamed statement
	body.put_slice(sql.as_bytes());
	body.put_u8(0);
	body.put_i16(0); // no parameter types
	packet.put_u8(b'P');
	packet.put_i32((body.len() + 4) as i32);
	packet.put(body);

	let mut body = BytesMut::new();
	body.put_u8(0); // the unnamed portal
	body.put_u8(0); // the unnamed statement
	body.put_i16(0); // no parameter formats
	body.put_i16(0); // no parameters
	body.put_i16(0); // no result formats
	packet.put_u8(b'B');
	packet.put_i32((body.len() + 4) as i32);
	packet.put(body);

	packet.put_u8(b'E');
	packet.put_i32(9);
	packet.put_u8(0); // the unnamed portal
	packet.put_i32(0); // every row

	packet.put_u8(b'S');
	packet.put_i32(4);
	stream.write_all(&packet).await.expect("Failed to send the statement");

	let mut rows = Vec::new();
	let mut error = None;
	loop {
		let (msg_type, body) = read_message(stream).await;
		match msg_type {
			b'Z' => break,
			b'D' => rows.push(first_cell(&body)),
			b'E' => error = Some(String::from_utf8_lossy(&body).into_owned()),
			_ => {}
		}
	}
	match error {
		Some(error) => Err(error),
		None => Ok(rows),
	}
}

/// The one number a prepared `sql` answers.
async fn number_extended(stream: &mut TcpStream, sql: &str) -> i64 {
	let rows = query_extended(stream, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
	let [Some(cell)] = rows.as_slice() else {
		panic!("{sql}: expected one number: {rows:?}");
	};
	cell.parse().unwrap_or_else(|_| panic!("{sql}: not a number: {rows:?}"))
}

/// Inserts one row into `table` and returns the number its `id` column drew.
async fn draw(stream: &mut TcpStream, table: &str) -> i64 {
	number(stream, &format!("INSERT INTO {table} (name) VALUES ('x') RETURNING id")).await
}

#[tokio::test]
async fn test_pgwire_truncate_leaves_the_numbering_running_unless_it_restarts_identity() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	run(&mut stream, "CREATE TABLE trunc_ri (id SERIAL PRIMARY KEY, n BIGSERIAL, name TEXT)").await;
	run(&mut stream, "CREATE TABLE trunc_ri_other (id SERIAL PRIMARY KEY, name TEXT)").await;
	for expected in 1..=3 {
		assert_eq!(draw(&mut stream, "trunc_ri").await, expected);
	}
	assert_eq!(draw(&mut stream, "trunc_ri_other").await, 1);

	run(&mut stream, "TRUNCATE TABLE trunc_ri").await;
	assert_eq!(number(&mut stream, "SELECT count(*) FROM trunc_ri").await, 0);
	assert_eq!(draw(&mut stream, "trunc_ri").await, 4);

	run(&mut stream, "TRUNCATE trunc_ri CONTINUE IDENTITY").await;
	assert_eq!(draw(&mut stream, "trunc_ri").await, 5);

	// Both sequences of the table start over, and no other table's.
	run(&mut stream, "TRUNCATE TABLE trunc_ri RESTART IDENTITY").await;
	assert_eq!(number(&mut stream, "SELECT count(*) FROM trunc_ri").await, 0);
	assert_eq!(
		number(&mut stream, "INSERT INTO trunc_ri (name) VALUES ('x') RETURNING n").await,
		1
	);
	assert_eq!(number(&mut stream, "SELECT id FROM trunc_ri").await, 1);
	assert_eq!(draw(&mut stream, "trunc_ri_other").await, 2);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_a_rolled_back_restart_identity_keeps_the_rows_and_the_numbering() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	run(&mut stream, "CREATE TABLE trunc_txn (id SERIAL PRIMARY KEY, name TEXT)").await;
	for expected in 1..=3 {
		assert_eq!(draw(&mut stream, "trunc_txn").await, expected);
	}

	run(&mut stream, "BEGIN").await;
	run(&mut stream, "TRUNCATE TABLE trunc_txn RESTART IDENTITY").await;
	assert_eq!(number(&mut stream, "SELECT count(*) FROM trunc_txn").await, 0);
	assert_eq!(draw(&mut stream, "trunc_txn").await, 1);
	run(&mut stream, "ROLLBACK").await;

	// The rows are back with the numbers they had, and the count carries on past them.
	assert_eq!(number(&mut stream, "SELECT count(*) FROM trunc_txn").await, 3);
	assert_eq!(draw(&mut stream, "trunc_txn").await, 4);

	// Committed, the same statements leave one row numbered 1.
	run(&mut stream, "BEGIN").await;
	run(&mut stream, "TRUNCATE TABLE trunc_txn RESTART IDENTITY").await;
	assert_eq!(draw(&mut stream, "trunc_txn").await, 1);
	run(&mut stream, "COMMIT").await;
	assert_eq!(number(&mut stream, "SELECT count(*) FROM trunc_txn").await, 1);
	assert_eq!(draw(&mut stream, "trunc_txn").await, 2);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_a_prepared_truncate_restarts_identity_only_when_it_says_so() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	run(&mut stream, "CREATE TABLE trunc_prepared (id SERIAL PRIMARY KEY, name TEXT)").await;
	for expected in 1..=3 {
		assert_eq!(draw(&mut stream, "trunc_prepared").await, expected);
	}

	let rows = query_extended(&mut stream, "TRUNCATE TABLE trunc_prepared")
		.await
		.unwrap_or_else(|e| panic!("TRUNCATE: {e}"));
	assert!(rows.is_empty(), "a TRUNCATE answers no row: {rows:?}");
	assert_eq!(draw(&mut stream, "trunc_prepared").await, 4);

	query_extended(&mut stream, "TRUNCATE TABLE trunc_prepared RESTART IDENTITY")
		.await
		.unwrap_or_else(|e| panic!("TRUNCATE RESTART IDENTITY: {e}"));
	assert_eq!(number(&mut stream, "SELECT count(*) FROM trunc_prepared").await, 0);
	assert_eq!(
		number_extended(&mut stream, "INSERT INTO trunc_prepared (name) VALUES ('x') RETURNING id")
			.await,
		1
	);

	shutdown.cancel();
}
