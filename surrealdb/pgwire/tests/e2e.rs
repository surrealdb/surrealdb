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
	// Connect without options: default dialect is SurrealQL
	let mut stream = connect_and_handshake_with_options(addr, None).await;

	// In default SurrealQL dialect: SurrealQL query succeeds!
	let (_, last) = run_simple_query(&mut stream, "CREATE person SET name = 'Surreal';").await;
	assert_eq!(last, b'C', "SurrealQL CREATE SET must succeed in default SurrealQL dialect");

	// In SurrealQL dialect, invalid SurrealQL must fail closed without fallback to PostgreSQL
	let (_, last) = run_simple_query(&mut stream, "SELECT @@version_comment;").await;
	assert_eq!(last, b'E', "Invalid query must fail closed in SurrealQL dialect");

	// Switch dialect to PostgreSQL via SET dialect = 'pgsql'
	let (_, last) = run_simple_query(&mut stream, "SET dialect = 'pgsql';").await;
	assert_eq!(last, b'C', "SET dialect = 'pgsql' must succeed");

	// Now PostgreSQL queries work
	let (rows, last) = run_simple_query(&mut stream, "SELECT name FROM person;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);

	// In PostgreSQL dialect: SurrealQL query is rejected with error 'E'
	let (_, last) = run_simple_query(&mut stream, "CREATE person SET name = 'Invalid';").await;
	assert_eq!(last, b'E', "SurrealQL CREATE SET must fail in PostgreSQL dialect");

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

#[tokio::test]
async fn test_pgwire_query_matrix_arithmetic_and_expressions() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Basic arithmetic operations
	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 + 2 * 3 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("7"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 100 - 35 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("65"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 50 / 2 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("25"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 17 % 5 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("2"));

	// 2. Parentheses & precedence
	let (rows, last) = run_simple_query(&mut stream, "SELECT (10 + 20) * 3 / 2 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("45"));

	// 3. Unary negation
	let (rows, last) = run_simple_query(&mut stream, "SELECT -42 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("-42"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT -(-10) AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("10"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_query_matrix_booleans_and_comparisons() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Boolean logic
	let (rows, last) = run_simple_query(&mut stream, "SELECT true AND false AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("f"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT false OR true AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT NOT false AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	// 2. Comparisons
	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 = 1 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 <> 2 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 5 > 3 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 2 < 4 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	// 3. BETWEEN & IN
	let (rows, last) = run_simple_query(&mut stream, "SELECT 5 BETWEEN 1 AND 10 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 15 BETWEEN 1 AND 10 AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("f"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 3 IN (1, 2, 3) AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 4 IN (1, 2, 3) AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("f"));

	// 4. IS NULL / IS NOT NULL
	let (rows, last) = run_simple_query(&mut stream, "SELECT NULL IS NULL AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 IS NOT NULL AS res;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("t"));

	// 5. CASE expressions
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT CASE WHEN 1 = 1 THEN 'alpha' ELSE 'beta' END AS res;",
	)
	.await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("alpha"));

	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT CASE WHEN 1 = 2 THEN 'no' WHEN 2 = 2 THEN 'yes' ELSE 'fallback' END AS res;",
	)
	.await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("yes"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_query_matrix_strings_and_typecasts() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. String concatenation
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT 'hello' || ' ' || 'world' AS msg;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("hello world"));

	// 2. Typecasts
	let (rows, last) = run_simple_query(&mut stream, "SELECT '42'::int4 AS val;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("42"));

	let (rows, last) = run_simple_query(&mut stream, "SELECT 100::text AS txt;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("100"));

	// 3. Dollar quoted strings
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT $$dollar quoted string$$ AS dqs;").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("dollar quoted string"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_query_matrix_crud_and_clauses() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. CREATE TABLE
	let (_, last) = run_simple_query(
		&mut stream,
		"CREATE TABLE products (id int, title text, price int, in_stock boolean);",
	)
	.await;
	assert_eq!(last, b'C');

	// 2. INSERT multiple rows
	let inserts = [
		"INSERT INTO products (id, title, price, in_stock) VALUES (1, 'Widget', 10, true);",
		"INSERT INTO products (id, title, price, in_stock) VALUES (2, 'Gadget', 25, false);",
		"INSERT INTO products (id, title, price, in_stock) VALUES (3, 'Doohickey', 50, true);",
		"INSERT INTO products (id, title, price, in_stock) VALUES (4, 'Thingamajig', 15, true);",
	];
	for q in inserts {
		let (_, last) = run_simple_query(&mut stream, q).await;
		assert_eq!(last, b'C', "Failed insert: {q}");
	}

	// 3. SELECT WHERE filter
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT title FROM products WHERE price > 20;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2, "Expected 2 products with price > 20");

	// 4. SELECT WHERE compound condition
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT title FROM products WHERE in_stock = true AND price < 30;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2, "Expected Widget and Thingamajig");

	// 5. SELECT ORDER BY ASC / DESC
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT title FROM products ORDER BY price ASC;").await;
	assert_eq!(last, b'C');
	let first_title = decode_data_row(&rows[0]);
	assert_eq!(first_title[0].as_deref(), Some("Widget"), "Cheapest should be Widget (10)");

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT title FROM products ORDER BY price DESC;").await;
	assert_eq!(last, b'C');
	let first_title = decode_data_row(&rows[0]);
	assert_eq!(
		first_title[0].as_deref(),
		Some("Doohickey"),
		"Most expensive should be Doohickey (50)"
	);

	// 6. SELECT LIMIT and OFFSET
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT title FROM products ORDER BY price ASC LIMIT 2;")
			.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);

	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT title FROM products ORDER BY price ASC LIMIT 2 OFFSET 2;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let first_title = decode_data_row(&rows[0]);
	assert_eq!(first_title[0].as_deref(), Some("Gadget"), "3rd cheapest should be Gadget (25)");

	// 7. UPDATE
	let (_, last) =
		run_simple_query(&mut stream, "UPDATE products SET price = 12 WHERE title = 'Widget';")
			.await;
	assert_eq!(last, b'C');

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT price FROM products WHERE title = 'Widget';").await;
	assert_eq!(last, b'C');
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("12"), "Widget price should be updated to 12");

	// 8. DELETE
	let (_, last) =
		run_simple_query(&mut stream, "DELETE FROM products WHERE title = 'Thingamajig';").await;
	assert_eq!(last, b'C');

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT title FROM products WHERE title = 'Thingamajig';")
			.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 0, "Thingamajig should be deleted");

	// 9. DROP TABLE
	let (_, last) = run_simple_query(&mut stream, "DROP TABLE products;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_query_matrix_aggregations_and_group_by() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) = run_simple_query(
		&mut stream,
		"CREATE TABLE items (id int, category text, price int, in_stock boolean);",
	)
	.await;
	assert_eq!(last, b'C');

	let inserts = [
		"INSERT INTO items (id, category, price, in_stock) VALUES (1, 'electronics', 100, true);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (2, 'electronics', 200, true);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (3, 'books', 30, false);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (4, 'books', 20, true);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (5, 'clothing', 50, true);",
	];
	for q in inserts {
		let (_, last) = run_simple_query(&mut stream, q).await;
		assert_eq!(last, b'C');
	}

	// 1. Global aggregations without GROUP BY
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT count(*), sum(price), min(price), max(price) FROM items;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("5"), "count(*) should be 5");
	assert_eq!(cols[1].as_deref(), Some("400"), "sum(price) should be 400");
	assert_eq!(cols[2].as_deref(), Some("20"), "min(price) should be 20");
	assert_eq!(cols[3].as_deref(), Some("200"), "max(price) should be 200");

	// 2. GROUP BY single column
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT category, count(*) FROM items GROUP BY category ORDER BY category ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 3);
	let row0 = decode_data_row(&rows[0]);
	assert_eq!(row0[0].as_deref(), Some("books"));
	assert_eq!(row0[1].as_deref(), Some("2"));

	// 3. GROUP BY positional 1
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT category, count(*) FROM items GROUP BY 1 ORDER BY 1 ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 3);
	let row0 = decode_data_row(&rows[0]);
	assert_eq!(row0[0].as_deref(), Some("books"));
	assert_eq!(row0[1].as_deref(), Some("2"));

	// 4. HAVING clause with aggregate present in targets
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT category, count(*) as cnt FROM items GROUP BY category HAVING count(*) > 1 ORDER BY category ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2, "books and electronics have count > 1");
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("electronics"));

	// 5. HAVING clause with aggregate NOT present in targets
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT category FROM items GROUP BY category HAVING count(*) > 1 ORDER BY category ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("electronics"));

	// 6. Global COUNT(DISTINCT col)
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT count(DISTINCT category) FROM items;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("3"), "3 distinct categories");

	// 7. Grouped COUNT(DISTINCT col)
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT in_stock, count(DISTINCT category) FROM items GROUP BY in_stock ORDER BY in_stock ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r_false = decode_data_row(&rows[0]);
	assert_eq!(r_false[0].as_deref(), Some("false"));
	assert_eq!(r_false[1].as_deref(), Some("1"));
	let r_true = decode_data_row(&rows[1]);
	assert_eq!(r_true[0].as_deref(), Some("true"));
	assert_eq!(r_true[1].as_deref(), Some("3"));

	// 8. Multi-column GROUP BY
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT category, in_stock, count(*) FROM items GROUP BY category, in_stock ORDER BY category ASC, in_stock ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 4);

	// 9. Aggregates with FILTER (WHERE ...) clause (PostgreSQL standard)
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT count(*) FILTER (WHERE price > 50), sum(price) FILTER (WHERE in_stock = true) FROM items;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("2"), "count(*) FILTER (WHERE price > 50)");
	assert_eq!(cols[1].as_deref(), Some("370"), "sum(price) FILTER (WHERE in_stock = true)");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_multi_statement_simple_query() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Multi-statement simple query: SET followed by SELECT
	let (rows, last) =
		run_simple_query(&mut stream, "SET client_encoding = 'UTF8'; SELECT 1 AS num;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1, "SELECT 1 should return 1 row after SET statement");
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	// Sequential SELECT statements in single query packet
	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 AS a; SELECT 2 AS b;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2, "Both SELECT statements should have executed");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_honest_session_fail_closed() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. SAVEPOINT statements must fail closed
	let (_, last) = run_simple_query(&mut stream, "SAVEPOINT sp1;").await;
	assert_eq!(last, b'E', "SAVEPOINT must fail closed");

	let (_, last) = run_simple_query(&mut stream, "RELEASE SAVEPOINT sp1;").await;
	assert_eq!(last, b'E', "RELEASE SAVEPOINT must fail closed");

	let (_, last) = run_simple_query(&mut stream, "ROLLBACK TO SAVEPOINT sp1;").await;
	assert_eq!(last, b'E', "ROLLBACK TO SAVEPOINT must fail closed");

	// 2. PREPARE / DEALLOCATE in simple query must fail closed
	let (_, last) = run_simple_query(&mut stream, "PREPARE p1 AS SELECT 1;").await;
	assert_eq!(last, b'E', "SQL PREPARE must fail closed");

	let (_, last) = run_simple_query(&mut stream, "DEALLOCATE p1;").await;
	assert_eq!(last, b'E', "SQL DEALLOCATE must fail closed");

	// 3. Unmodelled SET statements must fail closed
	let (_, last) = run_simple_query(&mut stream, "SET ROLE admin;").await;
	assert_eq!(last, b'E', "SET ROLE must fail closed");

	let (_, last) = run_simple_query(&mut stream, "SET SESSION AUTHORIZATION alice;").await;
	assert_eq!(last, b'E', "SET SESSION AUTHORIZATION must fail closed");

	let (_, last) =
		run_simple_query(&mut stream, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").await;
	assert_eq!(last, b'E', "SET TRANSACTION ISOLATION LEVEL must fail closed");

	// 4. Out of bounds GROUP BY ordinal must fail closed
	let (_, last) = run_simple_query(&mut stream, "SELECT 1 GROUP BY 99;").await;
	assert_eq!(last, b'E', "Out of bounds GROUP BY ordinal must fail closed");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_ddl_matrix() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. CREATE TABLE with constraints, types, defaults
	let (_, last) = run_simple_query(
		&mut stream,
		"CREATE TABLE accounts (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) NOT NULL,
			email TEXT UNIQUE,
			balance NUMERIC(10, 2) DEFAULT 100.50,
			is_active BOOLEAN DEFAULT true
		);",
	)
	.await;
	assert_eq!(last, b'C', "CREATE TABLE accounts failed");

	// Verify insertion into the created table
	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO accounts (id, username, email) VALUES (1, 'alice', 'alice@example.com');",
	)
	.await;
	assert_eq!(last, b'C', "INSERT into accounts failed");

	let (rows, last) = run_simple_query(&mut stream, "SELECT username, email FROM accounts;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("alice"));
	assert_eq!(cols[1].as_deref(), Some("alice@example.com"));

	// 2. CREATE INDEX
	let (_, last) =
		run_simple_query(&mut stream, "CREATE INDEX idx_accounts_username ON accounts (username);")
			.await;
	assert_eq!(last, b'C', "CREATE INDEX failed");

	// 3. CREATE VIEW
	let (_, last) = run_simple_query(
		&mut stream,
		"CREATE VIEW v_active_accounts AS SELECT id, username FROM accounts WHERE is_active = true;",
	)
	.await;
	assert_eq!(last, b'C', "CREATE VIEW failed");

	// 4. ALTER TABLE ADD COLUMN
	let (_, last) =
		run_simple_query(&mut stream, "ALTER TABLE accounts ADD COLUMN bio TEXT;").await;
	assert_eq!(last, b'C', "ALTER TABLE ADD COLUMN failed");

	// 5. ALTER TABLE DROP COLUMN
	let (_, last) = run_simple_query(&mut stream, "ALTER TABLE accounts DROP COLUMN bio;").await;
	assert_eq!(last, b'C', "ALTER TABLE DROP COLUMN failed");

	// 6. DROP VIEW
	let (_, last) = run_simple_query(&mut stream, "DROP VIEW v_active_accounts;").await;
	assert_eq!(last, b'C', "DROP VIEW failed");

	// 7. DROP TABLE
	let (_, last) = run_simple_query(&mut stream, "DROP TABLE accounts;").await;
	assert_eq!(last, b'C', "DROP TABLE failed");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_schema_namespaces_decision_d4() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Create table in public schema (implicit and explicit public)
	let (_, last) =
		run_simple_query(&mut stream, "CREATE TABLE public.users (user_id INT, name VARCHAR(50));")
			.await;
	assert_eq!(last, b'C', "CREATE TABLE public.users failed");

	// 2. Create table in custom schema (auth.accounts) -> dotted table name
	let (_, last) = run_simple_query(
		&mut stream,
		"CREATE TABLE auth.accounts (account_id INT, user_id INT, username VARCHAR(50));",
	)
	.await;
	assert_eq!(last, b'C', "CREATE TABLE auth.accounts failed");

	// 3. Insert into public schema via both bare name and public.users
	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO users (user_id, name) VALUES (1, 'Alice');")
			.await;
	assert_eq!(last, b'C', "INSERT INTO users failed");

	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO public.users (user_id, name) VALUES (2, 'Bob');",
	)
	.await;
	assert_eq!(last, b'C', "INSERT INTO public.users failed");

	// 4. Insert into custom schema
	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO auth.accounts (account_id, user_id, username) VALUES (10, 1, 'alice_admin'), (20, 2, 'bob_user');",
	)
	.await;
	assert_eq!(last, b'C', "INSERT INTO auth.accounts failed");

	// 5. Query both tables: both 'users' and 'public.users' resolve to the same table
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT user_id, name FROM users ORDER BY user_id;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let cols0 = decode_data_row(&rows[0]);
	let cols1 = decode_data_row(&rows[1]);
	assert_eq!(cols0[1].as_deref(), Some("Alice"));
	assert_eq!(cols1[1].as_deref(), Some("Bob"));

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT user_id, name FROM public.users ORDER BY user_id;")
			.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);

	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT account_id, username FROM auth.accounts ORDER BY account_id;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let cols0 = decode_data_row(&rows[0]);
	assert_eq!(cols0[1].as_deref(), Some("alice_admin"));

	// 6. Cross-schema join: joins across schemas work because dotted table names coexist
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT u.name, a.username FROM users u JOIN auth.accounts a ON u.user_id = a.user_id ORDER BY u.user_id;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let cols0 = decode_data_row(&rows[0]);
	let cols1 = decode_data_row(&rows[1]);
	assert_eq!(cols0[0].as_deref(), Some("Alice"));
	assert_eq!(cols0[1].as_deref(), Some("alice_admin"));
	assert_eq!(cols1[0].as_deref(), Some("Bob"));
	assert_eq!(cols1[1].as_deref(), Some("bob_user"));

	// 7. Verify cross-database reference rejection (3-part with wrong db, or 4-part name)
	let (_, last) = run_simple_query(&mut stream, "SELECT * FROM otherdb.public.users;").await;
	assert_eq!(last, b'E', "Cross-database reference should be rejected with ErrorResponse");

	shutdown.cancel();
}

fn build_parse(stmt_name: &str, sql: &str, param_types: &[i32]) -> BytesMut {
	let mut parse = BytesMut::new();
	parse.put_u8(b'P');
	let mut body = BytesMut::new();
	body.put_slice(stmt_name.as_bytes());
	body.put_u8(0);
	body.put_slice(sql.as_bytes());
	body.put_u8(0);
	body.put_i16(param_types.len() as i16);
	for &oid in param_types {
		body.put_i32(oid);
	}
	parse.put_i32((body.len() + 4) as i32);
	parse.put(body);
	parse
}

fn build_bind(
	portal_name: &str,
	stmt_name: &str,
	param_formats: &[i16],
	params: &[Option<&[u8]>],
) -> BytesMut {
	let mut bind = BytesMut::new();
	bind.put_u8(b'B');
	let mut body = BytesMut::new();
	body.put_slice(portal_name.as_bytes());
	body.put_u8(0);
	body.put_slice(stmt_name.as_bytes());
	body.put_u8(0);
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
	bind
}

fn build_desc(target: u8, name: &str) -> BytesMut {
	let mut desc = BytesMut::new();
	desc.put_u8(b'D');
	let mut body = BytesMut::new();
	body.put_u8(target);
	body.put_slice(name.as_bytes());
	body.put_u8(0);
	desc.put_i32((body.len() + 4) as i32);
	desc.put(body);
	desc
}

fn build_exec(portal_name: &str, max_rows: i32) -> BytesMut {
	let mut exec = BytesMut::new();
	exec.put_u8(b'E');
	let mut body = BytesMut::new();
	body.put_slice(portal_name.as_bytes());
	body.put_u8(0);
	body.put_i32(max_rows);
	exec.put_i32((body.len() + 4) as i32);
	exec.put(body);
	exec
}

fn build_close(target: u8, name: &str) -> BytesMut {
	let mut close = BytesMut::new();
	close.put_u8(b'C');
	let mut body = BytesMut::new();
	body.put_u8(target);
	body.put_slice(name.as_bytes());
	body.put_u8(0);
	close.put_i32((body.len() + 4) as i32);
	close.put(body);
	close
}

fn build_sync() -> BytesMut {
	let mut sync = BytesMut::new();
	sync.put_u8(b'S');
	sync.put_i32(4);
	sync
}

async fn run_extended_batch(
	stream: &mut TcpStream,
	packets: &[BytesMut],
) -> (Vec<Vec<u8>>, Vec<u8>) {
	for p in packets {
		stream.write_all(p).await.expect("send packet");
	}
	let mut rows = Vec::new();
	let mut msg_types = Vec::new();

	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			msg_types.push(msg_type);
			if msg_type == b'D' {
				rows.push(payload);
			} else if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for extended batch response");
	(rows, msg_types)
}

#[tokio::test]
async fn test_prepared_statement_lifecycle_and_caching() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let initial_prep = surrealdb_sqlplan::global_metrics().snapshot().prepared_statements_active;

	// 1. Parse statement "prep_add"
	let p1 = build_parse("prep_add", "SELECT ($1::int + $2::int) AS sum;", &[23, 23]);
	let b1 = build_bind("portal1", "prep_add", &[], &[Some(b"10"), Some(b"20")]);
	let d1 = build_desc(b'P', "portal1");
	let e1 = build_exec("portal1", 0);
	let s1 = build_sync();

	let (rows, msgs) = run_extended_batch(&mut stream, &[p1, b1, d1, e1, s1]).await;
	assert!(msgs.contains(&b'1'), "Expected ParseComplete '1'");
	assert!(msgs.contains(&b'2'), "Expected BindComplete '2'");
	assert!(msgs.contains(&b'C'), "Expected CommandComplete 'C'");
	assert_eq!(rows.len(), 1);
	let cols = decode_data_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("30"));

	// Statement should now be cached
	let mid_prep = surrealdb_sqlplan::global_metrics().snapshot().prepared_statements_active;
	assert_eq!(mid_prep, initial_prep + 1);

	// 2. Bind and execute again WITHOUT re-parsing
	let b2 = build_bind("portal2", "prep_add", &[], &[Some(b"100"), Some(b"250")]);
	let d2 = build_desc(b'P', "portal2");
	let e2 = build_exec("portal2", 0);
	let s2 = build_sync();

	let (rows2, msgs2) = run_extended_batch(&mut stream, &[b2, d2, e2, s2]).await;
	assert!(!msgs2.contains(&b'1'), "Did not expect ParseComplete since query was not re-parsed");
	assert!(msgs2.contains(&b'2'), "Expected BindComplete '2'");
	assert!(msgs2.contains(&b'C'), "Expected CommandComplete 'C'");
	assert_eq!(rows2.len(), 1);
	let cols2 = decode_data_row(&rows2[0]);
	assert_eq!(cols2[0].as_deref(), Some("350"));

	// 3. Close the prepared statement
	let c1 = build_close(b'S', "prep_add");
	let s3 = build_sync();
	let (_, msgs3) = run_extended_batch(&mut stream, &[c1, s3]).await;
	assert!(msgs3.contains(&b'3'), "Expected CloseComplete '3'");

	let final_prep = surrealdb_sqlplan::global_metrics().snapshot().prepared_statements_active;
	assert_eq!(final_prep, initial_prep);

	shutdown.cancel();
}

#[tokio::test]
async fn test_observability_metrics_and_redaction() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let snap_before = surrealdb_sqlplan::global_metrics().snapshot();

	// Run a query
	let (rows, last) = run_simple_query(&mut stream, "SELECT 42 AS val;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);

	let snap_after = surrealdb_sqlplan::global_metrics().snapshot();
	assert!(
		snap_after.pgsql_queries > snap_before.pgsql_queries,
		"Postgres query count should increase"
	);

	// Test credential redaction
	let sensitive_sql = "CREATE USER 'developer'@'localhost' IDENTIFIED BY 'ultra_secret_pw_123';";
	let redacted = surrealdb_sqlplan::redact_sql_credentials(sensitive_sql);
	assert!(!redacted.contains("ultra_secret_pw_123"));
	assert!(redacted.contains("'[REDACTED]'"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_prepared_statement_byte_limit_rejection() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Each single message is within MAX_MESSAGE_SIZE (16 MiB),
	// but cumulative statements exceed MAX_PREPARED_BYTES (32 MiB).
	let chunk_14mb = "x".repeat(14 * 1024 * 1024);
	let query1 = format!("SELECT 1; /* {chunk_14mb} */");
	let p1 = build_parse("stmt1", &query1, &[]);
	let s1 = build_sync();
	let (_, msgs1) = run_extended_batch(&mut stream, &[p1, s1]).await;
	assert!(msgs1.contains(&b'1'), "First 14MB stmt should parse successfully");

	let query2 = format!("SELECT 2; /* {chunk_14mb} */");
	let p2 = build_parse("stmt2", &query2, &[]);
	let s2 = build_sync();
	let (_, msgs2) = run_extended_batch(&mut stream, &[p2, s2]).await;
	assert!(msgs2.contains(&b'1'), "Second 14MB stmt should parse successfully (total ~28MB)");

	// Third 6MB statement brings total to ~34MB > 32MB limit
	let chunk_6mb = "y".repeat(6 * 1024 * 1024);
	let query3 = format!("SELECT 3; /* {chunk_6mb} */");
	let p3 = build_parse("stmt3", &query3, &[]);
	let s3 = build_sync();
	let (_, msgs3) = run_extended_batch(&mut stream, &[p3, s3]).await;
	assert!(
		msgs3.contains(&b'E'),
		"Third stmt should be rejected with ErrorResponse 'E' when exceeding cumulative byte limit"
	);

	shutdown.cancel();
}
