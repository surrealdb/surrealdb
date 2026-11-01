//! A `SERIAL` column draws its numbers from a sequence of its own: connections that insert at the
//! same time never draw the same number, and a table created again, or created in a transaction
//! that is rolled back, counts from 1.

#![recursion_limit = "256"]

use std::collections::BTreeSet;
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

/// The numbers `connections` connections draw when each inserts `inserts` rows into `table` at
/// the same time.
async fn draw_concurrently(
	addr: std::net::SocketAddr,
	table: &str,
	connections: usize,
	inserts: usize,
) -> Vec<i64> {
	let mut tasks = Vec::new();
	for who in 0..connections {
		let table = table.to_string();
		tasks.push(tokio::spawn(async move {
			let mut stream = connect(addr).await;
			let mut drawn = Vec::new();
			for _ in 0..inserts {
				let sql = format!("INSERT INTO {table} (who) VALUES ({who}) RETURNING id");
				drawn.push(number(&mut stream, &sql).await);
			}
			drawn
		}));
	}
	let mut drawn = Vec::new();
	for task in tasks {
		drawn.extend(task.await.expect("a connection panicked"));
	}
	drawn
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pgwire_serial_numbers_are_distinct_across_connections() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;
	let (connections, inserts) = (6, 15);

	run(&mut stream, "CREATE TABLE serial_conc (id SERIAL PRIMARY KEY, who INT)").await;
	let drawn = draw_concurrently(addr, "serial_conc", connections, inserts).await;
	let distinct: BTreeSet<_> = drawn.iter().collect();
	assert_eq!(drawn.len(), connections * inserts);
	assert_eq!(distinct.len(), drawn.len(), "a number was drawn twice: {drawn:?}");
	assert!(drawn.iter().all(|n| *n >= 1), "{drawn:?}");

	// A table created again counts from 1 for the first connection, and the rest never repeat
	// a number it drew.
	run(&mut stream, "DROP TABLE serial_conc").await;
	run(&mut stream, "CREATE TABLE serial_conc (id SERIAL PRIMARY KEY, who INT)").await;
	assert_eq!(
		number(&mut stream, "INSERT INTO serial_conc (who) VALUES (0) RETURNING id").await,
		1
	);
	let drawn = draw_concurrently(addr, "serial_conc", connections, inserts).await;
	let distinct: BTreeSet<_> = drawn.iter().collect();
	assert_eq!(distinct.len(), drawn.len(), "a number was drawn twice: {drawn:?}");
	assert!(drawn.iter().all(|n| *n >= 2), "{drawn:?}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_serial_counts_from_one_after_a_rolled_back_create_table() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	// A dropped table leaves its count behind; a table created in its place starts it over, even
	// when the first attempt at the table is rolled back.
	run(&mut stream, "CREATE TABLE serial_txn (id SERIAL PRIMARY KEY, name TEXT)").await;
	for name in ["a", "b"] {
		let sql = format!("INSERT INTO serial_txn (name) VALUES ('{name}') RETURNING id");
		number(&mut stream, &sql).await;
	}
	run(&mut stream, "DROP TABLE serial_txn").await;

	run(&mut stream, "BEGIN").await;
	run(&mut stream, "CREATE TABLE serial_txn (id SERIAL PRIMARY KEY, name TEXT)").await;
	assert_eq!(
		number(&mut stream, "INSERT INTO serial_txn (name) VALUES ('x') RETURNING id").await,
		1
	);
	run(&mut stream, "ROLLBACK").await;

	run(&mut stream, "CREATE TABLE serial_txn (id SERIAL PRIMARY KEY, name TEXT)").await;
	assert_eq!(
		number(&mut stream, "INSERT INTO serial_txn (name) VALUES ('y') RETURNING id").await,
		1
	);

	// A rolled back insert uses up its number, as in PostgreSQL.
	run(&mut stream, "BEGIN").await;
	assert_eq!(
		number(&mut stream, "INSERT INTO serial_txn (name) VALUES ('z') RETURNING id").await,
		2
	);
	run(&mut stream, "ROLLBACK").await;
	assert_eq!(
		number(&mut stream, "INSERT INTO serial_txn (name) VALUES ('w') RETURNING id").await,
		3
	);

	shutdown.cancel();
}
