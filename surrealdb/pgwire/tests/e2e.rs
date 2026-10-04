use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

async fn start_test_pg_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_pgwire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start pgwire server");

	(addr, shutdown)
}

async fn connect_and_handshake(addr: std::net::SocketAddr) -> TcpStream {
	connect_and_handshake_with_options(addr, Some("-c dialect=pgsql")).await
}

async fn connect_and_handshake_with_options(
	addr: std::net::SocketAddr,
	options: Option<&str>,
) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to pgwire server");

	// Send StartupMessage (Protocol 3.0)
	let mut body = BytesMut::new();
	body.put_i32(196608); // Protocol 3.0 (3 << 16)
	body.put_slice(b"user\0root\0");
	body.put_slice(b"database\0test/test\0");
	if let Some(opts) = options {
		body.put_slice(b"options\0");
		body.put_slice(opts.as_bytes());
		body.put_u8(0);
	}
	body.put_u8(0);

	let len = (body.len() + 4) as i32;
	let mut packet = BytesMut::new();
	packet.put_i32(len);
	packet.put(body);

	stream.write_all(&packet).await.expect("Failed to send startup message");

	// Drain messages until ReadyForQuery ('Z')
	let mut ready_for_query = false;
	let res = timeout(Duration::from_secs(5), async {
		while !ready_for_query {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'Z' {
				ready_for_query = true;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for ReadyForQuery");
	stream
}

async fn run_simple_query(stream: &mut TcpStream, sql: &str) -> (Vec<Vec<u8>>, u8) {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send query");

	let mut rows = Vec::new();
	let mut last_type = 0u8;

	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'D' {
				rows.push(payload);
			} else if msg_type == b'Z' {
				break;
			} else {
				if msg_type == b'E' {
					eprintln!("PG ERROR: {:?}", String::from_utf8_lossy(&payload));
				}
				last_type = msg_type;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for query completion: {sql}");
	(rows, last_type)
}

fn decode_data_row(payload: &[u8]) -> Vec<Option<String>> {
	use bytes::Buf;
	let mut cur = payload;
	let col_count = cur.get_i16() as usize;
	let mut cols = Vec::with_capacity(col_count);
	for _ in 0..col_count {
		let len = cur.get_i32();
		if len < 0 {
			cols.push(None);
		} else {
			let mut bytes = vec![0u8; len as usize];
			use bytes::Buf;
			(&mut cur).copy_to_slice(&mut bytes);
			cols.push(Some(String::from_utf8_lossy(&bytes).to_string()));
		}
	}
	cols
}

async fn run_extended_query(
	stream: &mut TcpStream,
	sql: &str,
	param_types: &[i32],
	params: &[Option<&[u8]>],
) -> (Vec<Vec<u8>>, u8) {
	run_extended_query_with_formats(stream, sql, param_types, &[], params).await
}

async fn run_extended_query_with_formats(
	stream: &mut TcpStream,
	sql: &str,
	param_types: &[i32],
	param_formats: &[i16],
	params: &[Option<&[u8]>],
) -> (Vec<Vec<u8>>, u8) {
	// 1. Parse
	let mut parse = BytesMut::new();
	parse.put_u8(b'P');
	let mut body = BytesMut::new();
	body.put_u8(0); // unnamed statement
	body.put_slice(sql.as_bytes());
	body.put_u8(0);
	body.put_i16(param_types.len() as i16);
	for &oid in param_types {
		body.put_i32(oid);
	}
	parse.put_i32((body.len() + 4) as i32);
	parse.put(body);

	// 2. Bind
	let mut bind = BytesMut::new();
	bind.put_u8(b'B');
	let mut body = BytesMut::new();
	body.put_u8(0); // unnamed portal
	body.put_u8(0); // unnamed statement
	body.put_i16(param_formats.len() as i16);
	for &f in param_formats {
		body.put_i16(f);
	}
	body.put_i16(params.len() as i16);
	for p in params {
		match p {
			Some(bytes) => {
				body.put_i32(bytes.len() as i32);
				body.put_slice(bytes);
			}
			None => {
				body.put_i32(-1);
			}
		}
	}
	body.put_i16(0); // 0 result formats
	bind.put_i32((body.len() + 4) as i32);
	bind.put(body);

	// 3. Describe portal
	let mut desc = BytesMut::new();
	desc.put_u8(b'D');
	desc.put_i32(6);
	desc.put_u8(b'P');
	desc.put_u8(0);

	// 4. Execute
	let mut exec = BytesMut::new();
	exec.put_u8(b'E');
	exec.put_i32(9);
	exec.put_u8(0);
	exec.put_i32(0); // fetch all

	// 5. Sync
	let mut sync = BytesMut::new();
	sync.put_u8(b'S');
	sync.put_i32(4);

	stream.write_all(&parse).await.expect("send parse");
	stream.write_all(&bind).await.expect("send bind");
	stream.write_all(&desc).await.expect("send desc");
	stream.write_all(&exec).await.expect("send exec");
	stream.write_all(&sync).await.expect("send sync");

	let mut rows = Vec::new();
	let mut last_type = 0u8;

	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'D' {
				rows.push(payload);
			} else if msg_type == b'Z' {
				break;
			} else {
				if msg_type == b'E' {
					eprintln!("PG ERROR: {:?}", String::from_utf8_lossy(&payload));
				}
				last_type = msg_type;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for extended query: {sql}");
	(rows, last_type)
}

#[tokio::test]
async fn test_pgwire_e2e_simple_query_lifecycle() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. SELECT 1 AS num
	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 AS num;").await;
	assert_eq!(last, b'C', "Expected CommandComplete for SELECT 1");
	assert_eq!(rows.len(), 1, "Expected 1 row from SELECT 1");

	// 2. INSERT records (PostgreSQL syntax)
	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO person (name) VALUES ('Tobie');").await;
	assert_eq!(last, b'C', "Expected CommandComplete for INSERT person");
	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO person (name) VALUES ('Jaime');").await;
	assert_eq!(last, b'C', "Expected CommandComplete for INSERT person");

	// 3. SELECT from table
	let (rows, last) = run_simple_query(&mut stream, "SELECT name FROM person;").await;
	assert_eq!(last, b'C', "Expected CommandComplete for SELECT name");
	assert_eq!(rows.len(), 2, "Expected 2 rows from person table");

	// 4. Terminate cleanly
	let mut term_pkt = BytesMut::new();
	term_pkt.put_u8(b'X');
	term_pkt.put_i32(4);
	stream.write_all(&term_pkt).await.expect("Failed to send terminate");

	let mut buf = [0u8; 1];
	let n = stream.read(&mut buf).await.unwrap_or_default();
	assert_eq!(n, 0, "Connection should be closed after Terminate");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_e2e_transactions() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Create table first so table exists after rollback
	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE item (title text);").await;
	assert_eq!(last, b'C');

	// Begin transaction
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');
	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO item (title) VALUES ('To Rollback');").await;
	assert_eq!(last, b'C');
	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');

	// Verify rollback
	let (rows, last) = run_simple_query(&mut stream, "SELECT * FROM item;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 0, "Item should have been rolled back");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_dialect_switching_and_fail_closed() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// In default PostgreSQL dialect: SurrealQL query is rejected with error 'E'
	let (_, last) = run_simple_query(&mut stream, "CREATE person SET name = 'Invalid';").await;
	assert_eq!(last, b'E', "SurrealQL CREATE SET must fail in default PostgreSQL dialect");

	// Switch dialect to SurrealQL via SET dialect = 'surrealql'
	let (_, last) = run_simple_query(&mut stream, "SET dialect = 'surrealql';").await;
	assert_eq!(last, b'C', "SET dialect = 'surrealql' must succeed");

	// Now SurrealQL CREATE SET succeeds!
	let (_, last) = run_simple_query(&mut stream, "CREATE person SET name = 'Surreal';").await;
	assert_eq!(last, b'C', "SurrealQL query must succeed when dialect = surrealql");

	// In SurrealQL dialect, invalid SurrealQL must fail closed without fallback to PostgreSQL
	let (_, last) = run_simple_query(&mut stream, "SELECT @@version_comment;").await;
	assert_eq!(last, b'E', "Invalid query must fail closed in SurrealQL dialect");

	// Switch dialect back to PostgreSQL via SET dialect = 'pgsql'
	let (_, last) = run_simple_query(&mut stream, "SET dialect = 'pgsql';").await;
	assert_eq!(last, b'C', "SET dialect = 'pgsql' must succeed");

	// Now PostgreSQL queries work again
	let (rows, last) = run_simple_query(&mut stream, "SELECT name FROM person;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);

	// Also verify that a connection without startup options starts in SurrealQL dialect
	let mut default_stream = connect_and_handshake_with_options(addr, None).await;
	let (_, last) =
		run_simple_query(&mut default_stream, "CREATE item SET title = 'SurrealDefault';").await;
	assert_eq!(last, b'C', "Connection without startup options must default to SurrealQL dialect");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_parameter_binding_across_positions() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Projection: SELECT $1::int4 with declared type
	let (rows, last) =
		run_extended_query(&mut stream, "SELECT $1::int4;", &[23], &[Some(b"42")]).await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols.len(), 1);
	assert_eq!(cols[0].as_deref(), Some("42"), "SELECT $1::int4 should return 42");

	// 1b. Projection: SELECT $1 with unspecified parameter type in Parse
	let (rows, last) = run_extended_query(&mut stream, "SELECT $1;", &[], &[Some(b"42")]).await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols.len(), 1);
	assert_eq!(cols[0].as_deref(), Some("42"), "SELECT $1 with untyped Parse should return 42");

	// 1c. Projection with alias: SELECT $1 AS p
	let (rows, last) =
		run_extended_query(&mut stream, "SELECT $1 AS p;", &[], &[Some(b"hello")]).await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols.len(), 1);
	assert_eq!(cols[0].as_deref(), Some("hello"), "SELECT $1 AS p should return 'hello'");

	// 1d. Projection expression: SELECT $1 + $2 (common driver ping)
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT $1::int4 + $2::int4;",
		&[23, 23],
		&[Some(b"10"), Some(b"32")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols.len(), 1);
	assert_eq!(cols[0].as_deref(), Some("42"), "SELECT $1 + $2 should return 42");

	// 2. VALUES position: INSERT INTO person (name, age) VALUES ($1, $2)
	run_simple_query(&mut stream, "CREATE TABLE person (name text, age int);").await;
	let (_, last) = run_extended_query(
		&mut stream,
		"INSERT INTO person (name, age) VALUES ($1, $2::int4);",
		&[25, 23],
		&[Some(b"Alice"), Some(b"30")],
	)
	.await;
	assert_eq!(last, b'C');

	let (_, last) = run_extended_query(
		&mut stream,
		"INSERT INTO person (name, age) VALUES ($1, $2::int4);",
		&[25, 23],
		&[Some(b"Bob"), Some(b"20")],
	)
	.await;
	assert_eq!(last, b'C');

	// 3. WHERE position: SELECT name FROM person WHERE age > $1
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT name FROM person WHERE age > $1::int4;",
		&[23],
		&[Some(b"25")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("Alice"), "WHERE age > $1 should match Alice");

	// 4. LIMIT position: SELECT name FROM person ORDER BY age LIMIT $1
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT name FROM person ORDER BY age LIMIT $1::int4;",
		&[23],
		&[Some(b"1")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("Bob"), "LIMIT $1 should return 1 row (Bob)");

	// 5. Function argument position: SELECT abs($1::int4)
	let (rows, last) =
		run_extended_query(&mut stream, "SELECT abs($1::int4);", &[23], &[Some(b"-42")]).await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("42"), "SELECT abs($1::int4) should return 42");

	// 6. Binary format binding:
	// OID_INT4 = 23, format 1 (binary), value 42i32 in big-endian
	let int4_bytes = 42i32.to_be_bytes();
	let (rows, last) = run_extended_query_with_formats(
		&mut stream,
		"SELECT $1::int4;",
		&[23],
		&[1], // format 1 = binary
		&[Some(&int4_bytes)],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("42"), "Binary format INT4 parameter should decode to 42");

	// OID_INT8 = 20, format 1 (binary), value 9876543210i64 in big-endian
	let int8_bytes = 9876543210i64.to_be_bytes();
	let (rows, last) = run_extended_query_with_formats(
		&mut stream,
		"SELECT $1::int8;",
		&[20],
		&[1], // format 1 = binary
		&[Some(&int8_bytes)],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(
		cols[0].as_deref(),
		Some("9876543210"),
		"Binary format INT8 parameter should decode to 9876543210"
	);

	shutdown.cancel();
}
