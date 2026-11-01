//! The prepared-statement gauge counts every connection in the process. This test asserts its exact
//! value, so it is the only test in its binary: any other test that prepares a statement would move
//! the gauge between two of its reads.

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

/// A frontend message of `kind` whose body is `body`.
fn message(kind: u8, body: &[u8]) -> BytesMut {
	let mut packet = BytesMut::new();
	packet.put_u8(kind);
	packet.put_i32((body.len() + 4) as i32);
	packet.put_slice(body);
	packet
}

fn cstr(body: &mut BytesMut, text: &str) {
	body.put_slice(text.as_bytes());
	body.put_u8(0);
}

fn build_parse(stmt_name: &str, sql: &str, param_types: &[i32]) -> BytesMut {
	let mut body = BytesMut::new();
	cstr(&mut body, stmt_name);
	cstr(&mut body, sql);
	body.put_i16(param_types.len() as i16);
	for &oid in param_types {
		body.put_i32(oid);
	}
	message(b'P', &body)
}

fn build_bind(portal_name: &str, stmt_name: &str, params: &[&[u8]]) -> BytesMut {
	let mut body = BytesMut::new();
	cstr(&mut body, portal_name);
	cstr(&mut body, stmt_name);
	body.put_i16(0); // every parameter in text format
	body.put_i16(params.len() as i16);
	for param in params {
		body.put_i32(param.len() as i32);
		body.put_slice(param);
	}
	body.put_i16(0); // every result column in text format
	message(b'B', &body)
}

fn build_describe_portal(name: &str) -> BytesMut {
	let mut body = BytesMut::new();
	body.put_u8(b'P');
	cstr(&mut body, name);
	message(b'D', &body)
}

fn build_execute(portal_name: &str) -> BytesMut {
	let mut body = BytesMut::new();
	cstr(&mut body, portal_name);
	body.put_i32(0); // no row limit
	message(b'E', &body)
}

fn build_close_statement(name: &str) -> BytesMut {
	let mut body = BytesMut::new();
	body.put_u8(b'S');
	cstr(&mut body, name);
	message(b'C', &body)
}

fn build_sync() -> BytesMut {
	message(b'S', &[])
}

/// Sends `packets` and returns the `DataRow` bodies and the type of every message received, up to
/// `ReadyForQuery`.
async fn run_extended_batch(
	stream: &mut TcpStream,
	packets: &[BytesMut],
) -> (Vec<Vec<u8>>, Vec<u8>) {
	for packet in packets {
		stream.write_all(packet).await.expect("Failed to send a packet");
	}
	let mut rows = Vec::new();
	let mut msg_types = Vec::new();
	loop {
		let (msg_type, body) = read_message(stream).await;
		msg_types.push(msg_type);
		match msg_type {
			b'D' => rows.push(body),
			b'Z' => return (rows, msg_types),
			_ => {}
		}
	}
}

/// The text of the only column of a `DataRow` body.
fn only_column(row: &[u8]) -> String {
	assert_eq!(i16::from_be_bytes([row[0], row[1]]), 1, "expected one column");
	let len = i32::from_be_bytes([row[2], row[3], row[4], row[5]]);
	let len = usize::try_from(len).expect("a non-null column");
	String::from_utf8_lossy(&row[6..6 + len]).into_owned()
}

fn active_prepared_statements() -> u64 {
	surrealdb_sqlplan::global_metrics().snapshot().prepared_statements_active
}

#[tokio::test]
async fn test_prepared_statement_lifecycle_and_caching() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	let initial_prep = active_prepared_statements();

	// 1. Parse statement "prep_add"
	let (rows, msgs) = run_extended_batch(
		&mut stream,
		&[
			build_parse("prep_add", "SELECT ($1::int + $2::int) AS sum;", &[23, 23]),
			build_bind("portal1", "prep_add", &[b"10", b"20"]),
			build_describe_portal("portal1"),
			build_execute("portal1"),
			build_sync(),
		],
	)
	.await;
	assert!(msgs.contains(&b'1'), "Expected ParseComplete '1'");
	assert!(msgs.contains(&b'2'), "Expected BindComplete '2'");
	assert!(msgs.contains(&b'C'), "Expected CommandComplete 'C'");
	assert_eq!(rows.len(), 1);
	assert_eq!(only_column(&rows[0]), "30");

	// Statement should now be cached
	assert_eq!(active_prepared_statements(), initial_prep + 1);

	// 2. Bind and execute again WITHOUT re-parsing
	let (rows2, msgs2) = run_extended_batch(
		&mut stream,
		&[
			build_bind("portal2", "prep_add", &[b"100", b"250"]),
			build_describe_portal("portal2"),
			build_execute("portal2"),
			build_sync(),
		],
	)
	.await;
	assert!(!msgs2.contains(&b'1'), "Did not expect ParseComplete since query was not re-parsed");
	assert!(msgs2.contains(&b'2'), "Expected BindComplete '2'");
	assert!(msgs2.contains(&b'C'), "Expected CommandComplete 'C'");
	assert_eq!(rows2.len(), 1);
	assert_eq!(only_column(&rows2[0]), "350");

	// 3. Close the prepared statement
	let (_, msgs3) =
		run_extended_batch(&mut stream, &[build_close_statement("prep_add"), build_sync()]).await;
	assert!(msgs3.contains(&b'3'), "Expected CloseComplete '3'");

	assert_eq!(active_prepared_statements(), initial_prep);

	shutdown.cancel();
}
