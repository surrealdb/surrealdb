//! A SQL statement that defines several things at once (a table with its columns and indexes, a
//! `DROP TABLE` of several tables) is answered by one `CommandComplete`, the one the client sent
//! it for. A prepared statement is answered once too, and fails as a whole.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// The protocol version 3.0 of a `StartupMessage`.
const PROTOCOL_3_0: i32 = 196608;
/// Bound on every network exchange, so a server that stops answering fails the test instead of
/// hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(5);
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

/// Sends `sql` as one simple query and returns the number of `CommandComplete` messages that
/// answer it, up to `ReadyForQuery`: one for each statement the query holds.
async fn simple_query(stream: &mut TcpStream, sql: &str) -> usize {
	let mut packet = BytesMut::new();
	packet.put_u8(b'Q');
	packet.put_i32((sql.len() + 5) as i32);
	packet.put_slice(sql.as_bytes());
	packet.put_u8(0);
	stream.write_all(&packet).await.expect("Failed to send the query");

	let mut completions = 0;
	loop {
		match read_message(stream).await.0 {
			b'Z' => return completions,
			b'C' => completions += 1,
			_ => {}
		}
	}
}

#[tokio::test]
async fn test_pgwire_create_table_with_columns_and_an_index_answers_once() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	let completions = simple_query(
		&mut stream,
		"CREATE TABLE answers_once (id INT PRIMARY KEY, name TEXT, age INT)",
	)
	.await;
	assert_eq!(completions, 1);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_alter_and_drop_of_several_things_answer_once() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	for sql in [
		"CREATE TABLE answers_once_a (id INT)",
		"CREATE TABLE answers_once_b (id INT)",
		"ALTER TABLE answers_once_a ADD COLUMN c INT, ADD COLUMN d INT",
		"DROP TABLE answers_once_a, answers_once_b",
	] {
		assert_eq!(simple_query(&mut stream, sql).await, 1, "{sql}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_drop_schema_public_cascade_removes_every_table_and_answers_once() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	for sql in ["CREATE TABLE schema_once_a (id INT)", "CREATE TABLE schema_once_b (id INT)"] {
		assert_eq!(simple_query(&mut stream, sql).await, 1, "{sql}");
	}
	assert_eq!(simple_query(&mut stream, "DROP SCHEMA IF EXISTS public CASCADE").await, 1);

	// Both tables are gone: a table of the same name is defined again, which it could not be if
	// one were left.
	for sql in ["CREATE TABLE schema_once_a (id INT)", "CREATE TABLE schema_once_b (id INT)"] {
		assert_eq!(simple_query(&mut stream, sql).await, 1, "{sql}");
	}
	let answer = extended_query(&mut stream, "DROP SCHEMA public CASCADE").await;
	assert!(!answer.failed);
	assert_eq!(answer.completions, 1);
	for sql in ["CREATE TABLE schema_once_a (id INT)", "CREATE TABLE schema_once_b (id INT)"] {
		assert_eq!(simple_query(&mut stream, sql).await, 1, "{sql}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_each_statement_of_a_batch_gets_its_own_answer() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	let completions = simple_query(
		&mut stream,
		"CREATE TABLE batch_once_a (id INT PRIMARY KEY, name TEXT); \
		 CREATE TABLE batch_once_b (id INT PRIMARY KEY, name TEXT)",
	)
	.await;
	assert_eq!(completions, 2);

	shutdown.cancel();
}

/// What the server answered to one extended-protocol exchange.
struct ExtendedAnswer {
	/// The number of `CommandComplete` messages.
	completions: usize,
	/// Whether an `ErrorResponse` came back.
	failed: bool,
}

/// Parses `sql` as an unnamed prepared statement, binds it without parameters, executes it and
/// syncs, then reads up to `ReadyForQuery`.
async fn extended_query(stream: &mut TcpStream, sql: &str) -> ExtendedAnswer {
	let mut packet = BytesMut::new();

	// Parse: unnamed statement, the query, no parameter types.
	packet.put_u8(b'P');
	packet.put_i32((4 + 1 + sql.len() + 1 + 2) as i32);
	packet.put_u8(0);
	packet.put_slice(sql.as_bytes());
	packet.put_u8(0);
	packet.put_i16(0);

	// Bind: unnamed portal and statement, no parameter or result formats, no parameters.
	packet.put_u8(b'B');
	packet.put_i32(4 + 1 + 1 + 2 + 2 + 2);
	packet.put_u8(0);
	packet.put_u8(0);
	packet.put_i16(0);
	packet.put_i16(0);
	packet.put_i16(0);

	// Execute: unnamed portal, no row limit.
	packet.put_u8(b'E');
	packet.put_i32(4 + 1 + 4);
	packet.put_u8(0);
	packet.put_i32(0);

	// Sync.
	packet.put_u8(b'S');
	packet.put_i32(4);
	stream.write_all(&packet).await.expect("Failed to send the prepared statement");

	let mut answer = ExtendedAnswer {
		completions: 0,
		failed: false,
	};
	loop {
		match read_message(stream).await.0 {
			b'Z' => return answer,
			b'C' => answer.completions += 1,
			b'E' => answer.failed = true,
			_ => {}
		}
	}
}

#[tokio::test]
async fn test_pgwire_a_prepared_ddl_statement_answers_once_and_fails_as_a_whole() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	let answer =
		extended_query(&mut stream, "CREATE TABLE prepared_once (id INT PRIMARY KEY, name TEXT)")
			.await;
	assert!(!answer.failed);
	assert_eq!(answer.completions, 1);

	// A definition that fails takes the table it belongs to with it, so the table can be created
	// again.
	let answer = extended_query(&mut stream, "CREATE TABLE prepared_failed (a INT, a INT)").await;
	assert!(answer.failed);
	assert_eq!(answer.completions, 0);
	assert_eq!(simple_query(&mut stream, "CREATE TABLE prepared_failed (a INT)").await, 1);

	shutdown.cancel();
}
