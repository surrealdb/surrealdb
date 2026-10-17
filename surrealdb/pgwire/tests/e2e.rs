// Awaiting a datastore constructor lays out the sealed async bodies it reaches, which nest deeper
// than the default recursion limit.
#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use jsonwebtoken::{EncodingKey, Header, encode};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::token::Claims;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::ToSql;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

async fn start_test_pg_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	start_test_pg_server_with_ds(ds).await
}

async fn start_test_pg_server_with_ds(
	ds: Arc<Datastore>,
) -> (std::net::SocketAddr, CancellationToken) {
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
	connect_and_handshake_with_auth(addr, "root", "test/test", None, options)
		.await
		.expect("Handshake failed")
}

async fn connect_and_handshake_with_auth(
	addr: std::net::SocketAddr,
	user: &str,
	database: &str,
	password: Option<&str>,
	options: Option<&str>,
) -> Result<TcpStream, (String, String)> {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to pgwire server");

	// Send StartupMessage (Protocol 3.0)
	let mut body = BytesMut::new();
	body.put_i32(196608); // Protocol 3.0 (3 << 16)
	body.put_slice(b"user\0");
	body.put_slice(user.as_bytes());
	body.put_u8(0);
	body.put_slice(b"database\0");
	body.put_slice(database.as_bytes());
	body.put_u8(0);
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

	let mut ready = false;
	let mut err: Option<(String, String)> = None;

	let res = timeout(Duration::from_secs(5), async {
		while !ready && err.is_none() {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			match msg_type {
				b'R' => {
					let mut cur = &payload[..];
					let auth_type = cur.get_i32();
					if auth_type == 3 {
						// AuthenticationCleartextPassword
						if let Some(pass) = password {
							let mut p_pkt = BytesMut::new();
							p_pkt.put_u8(b'p');
							p_pkt.put_i32((pass.len() + 5) as i32);
							p_pkt.put_slice(pass.as_bytes());
							p_pkt.put_u8(0);
							stream.write_all(&p_pkt).await?;
						}
					}
				}
				b'E' => {
					err = Some(parse_pg_error_fields(&payload));
				}
				b'Z' => {
					ready = true;
				}
				_ => {}
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out during handshake");
	if let Some(e) = err {
		Err(e)
	} else {
		Ok(stream)
	}
}

fn parse_pg_error_fields(payload: &[u8]) -> (String, String) {
	let mut code = String::new();
	let mut message = String::new();
	let mut idx = 0;
	while idx < payload.len() {
		let field_type = payload[idx];
		if field_type == 0 {
			break;
		}
		idx += 1;
		let start = idx;
		while idx < payload.len() && payload[idx] != 0 {
			idx += 1;
		}
		let val = String::from_utf8_lossy(&payload[start..idx]).to_string();
		if idx < payload.len() {
			idx += 1; // skip null
		}
		if field_type == b'C' {
			code = val;
		} else if field_type == b'M' {
			message = val;
		}
	}
	(code, message)
}

fn make_test_jwt(ns: &str, db: &str, ac: &str, sub: &str, secret: &str) -> String {
	let claims = Claims {
		iss: Some("surrealdb-test".to_string()),
		iat: Some(chrono::Utc::now().timestamp()),
		nbf: Some(chrono::Utc::now().timestamp()),
		exp: Some((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
		ns: Some(ns.to_string()),
		db: Some(db.to_string()),
		ac: Some(ac.to_string()),
		id: Some(sub.to_string()),
		..Claims::default()
	};
	let key = EncodingKey::from_secret(secret.as_bytes());
	encode(&Header::default(), &claims, &key).expect("Failed to encode JWT")
}

async fn run_simple_query(stream: &mut TcpStream, sql: &str) -> (Vec<Vec<u8>>, u8) {
	let (rows, last_type, _) = run_simple_query_with_err(stream, sql).await;
	(rows, last_type)
}

async fn run_simple_query_with_err(
	stream: &mut TcpStream,
	sql: &str,
) -> (Vec<Vec<u8>>, u8, Option<(String, String)>) {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send query");

	let mut rows = Vec::new();
	let mut last_type = 0u8;
	let mut last_err = None;

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
					last_err = Some(parse_pg_error_fields(&payload));
				}
				last_type = msg_type;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for query completion: {sql}");
	(rows, last_type, last_err)
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
async fn surrealql_prepared_queries_reuse_the_parse_with_fresh_bindings() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake_with_options(addr, Some("-c dialect=surrealql")).await;
	let parse = build_parse("add", "RETURN $1 + $2", &[23, 23]);
	let bind = build_bind("first", "add", &[], &[Some(b"10"), Some(b"20")]);
	let execute = build_exec("first", 0);
	let sync = build_sync();
	let (rows, messages) = run_extended_batch(&mut stream, &[parse, bind, execute, sync]).await;
	assert!(messages.contains(&b'1'));
	assert!(messages.contains(&b'C'));
	assert_eq!(rows.len(), 1);
	assert_eq!(decode_data_row(&rows[0])[0].as_deref(), Some("30"));

	let bind = build_bind("second", "add", &[], &[Some(b"100"), Some(b"250")]);
	let execute = build_exec("second", 0);
	let sync = build_sync();
	let (rows, messages) = run_extended_batch(&mut stream, &[bind, execute, sync]).await;
	assert!(!messages.contains(&b'1'));
	assert!(messages.contains(&b'C'));
	assert_eq!(rows.len(), 1);
	assert_eq!(decode_data_row(&rows[0])[0].as_deref(), Some("350"));
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

#[tokio::test]
async fn test_pgwire_set_operations_and_joins() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. UNION ALL with outer ORDER BY
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT 10 AS num UNION ALL SELECT 20 AS num ORDER BY num ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r0[0].as_deref(), Some("10"));
	assert_eq!(r1[0].as_deref(), Some("20"));

	// 2. UNION (distinct)
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT 1 AS x UNION SELECT 1 AS x UNION SELECT 2 AS x;")
			.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2, "Duplicate 1 must be deduplicated by UNION");

	// 3. Parenthesised query
	let (rows, last) =
		run_simple_query(&mut stream, "(SELECT 100 AS val) UNION ALL (SELECT 200 AS val);").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);

	// 4. Bare VALUES statement
	let (rows, last) = run_simple_query(&mut stream, "VALUES (1, 'alice'), (2, 'bob');").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("1"));
	assert_eq!(r0[1].as_deref(), Some("alice"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("2"));
	assert_eq!(r1[1].as_deref(), Some("bob"));

	// 5. Table joins with USING and multi-equi conditions
	let (_, last) =
		run_simple_query(&mut stream, "CREATE TABLE employees (id int, dept_id int, name text);")
			.await;
	assert_eq!(last, b'C');

	let (_, last) =
		run_simple_query(&mut stream, "CREATE TABLE depts (dept_id int, dept_name text);").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO employees (id, dept_id, name) VALUES (1, 10, 'Alice'), (2, 20, 'Bob'), (3, 99, 'Charlie');",
	)
	.await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO depts (dept_id, dept_name) VALUES (10, 'Engineering'), (20, 'Sales'), (30, 'HR');",
	)
	.await;
	assert_eq!(last, b'C');

	// Inner join with USING (dept_id)
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT employees.name, depts.dept_name FROM employees JOIN depts USING (dept_id) ORDER BY employees.id ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Alice"));
	assert_eq!(r0[1].as_deref(), Some("Engineering"));

	// RIGHT OUTER JOIN rewrite
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT employees.name, depts.dept_name FROM employees RIGHT JOIN depts ON employees.dept_id = depts.dept_id ORDER BY depts.dept_id ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 3, "All 3 departments should be present");

	shutdown.cancel();
}

#[tokio::test]
async fn test_subqueries_e2e() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) =
		run_simple_query(&mut stream, "CREATE TABLE employees (id INT, name TEXT, dept_id INT);")
			.await;
	assert_eq!(last, b'C');
	let (_, last) =
		run_simple_query(&mut stream, "CREATE TABLE depts (dept_id INT, dept_name TEXT);").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10);",
	)
	.await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO depts (dept_id, dept_name) VALUES (10, 'Engineering'), (20, 'Sales');",
	)
	.await;
	assert_eq!(last, b'C');

	// 1. IN (SELECT ...)
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT employees.name FROM employees WHERE employees.dept_id IN (SELECT dept_id FROM depts WHERE dept_name = 'Engineering') ORDER BY employees.id ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Alice"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("Charlie"));

	// 2. EXISTS (SELECT ...)
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT depts.dept_name FROM depts WHERE EXISTS (SELECT 1 FROM employees WHERE employees.dept_id = depts.dept_id) ORDER BY depts.dept_id ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);

	shutdown.cancel();
}

#[tokio::test]
async fn test_cte_e2e() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) =
		run_simple_query(&mut stream, "CREATE TABLE staff (id INT, name TEXT, salary INT);").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(
		&mut stream,
		"INSERT INTO staff (id, name, salary) VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300);",
	)
	.await;
	assert_eq!(last, b'C');

	// 1. Basic CTE
	let (rows, last) = run_simple_query(
		&mut stream,
		"WITH high_earners AS (SELECT name, salary FROM staff WHERE salary > 150) SELECT name FROM high_earners ORDER BY salary ASC;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Bob"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("Charlie"));

	// 2. CTE with column aliases
	let (rows, last) = run_simple_query(
		&mut stream,
		"WITH summary(emp_name, pay) AS (SELECT name, salary FROM staff) SELECT emp_name FROM summary WHERE pay = 100;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Alice"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_savepoints_and_rollback() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE sp_demo (val text);").await;
	assert_eq!(last, b'C');

	// Begin transaction
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO sp_demo (val) VALUES ('first');").await;
	assert_eq!(last, b'C');

	// Create savepoint 1
	let (_, last) = run_simple_query(&mut stream, "SAVEPOINT sp1;").await;
	assert_eq!(last, b'C');

	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO sp_demo (val) VALUES ('second');").await;
	assert_eq!(last, b'C');

	// Create savepoint 2
	let (_, last) = run_simple_query(&mut stream, "SAVEPOINT sp2;").await;
	assert_eq!(last, b'C');

	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO sp_demo (val) VALUES ('third');").await;
	assert_eq!(last, b'C');

	// Rollback to savepoint 2 (undo 'third')
	let (_, last) = run_simple_query(&mut stream, "ROLLBACK TO SAVEPOINT sp2;").await;
	assert_eq!(last, b'C');

	// Release savepoint 1 (commits savepoints up to sp1)
	let (_, last) = run_simple_query(&mut stream, "RELEASE SAVEPOINT sp1;").await;
	assert_eq!(last, b'C');

	// Commit outer transaction
	let (_, last) = run_simple_query(&mut stream, "COMMIT;").await;
	assert_eq!(last, b'C');

	// Verify 'first' and 'second' are present, 'third' was rolled back
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT val FROM sp_demo ORDER BY val ASC;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("first"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("second"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_savepoint_error_recovery() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE err_rec (val text);").await;
	assert_eq!(last, b'C');

	// Begin transaction
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO err_rec (val) VALUES ('good1');").await;
	assert_eq!(last, b'C');

	// Savepoint before intentional failure
	let (_, last) = run_simple_query(&mut stream, "SAVEPOINT sp_before_err;").await;
	assert_eq!(last, b'C');

	// Execute an erroneous query (referencing non-existent table)
	let (_, last) = run_simple_query(&mut stream, "SELECT * FROM nonexistent_table_xyz;").await;
	assert_eq!(last, b'E', "Query against non-existent table must error");

	// In Postgres, any normal query within aborted transaction fails with 25P02
	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO err_rec (val) VALUES ('should_fail');").await;
	assert_eq!(last, b'E', "Subsequent queries in failed transaction must be rejected");

	// Rollback to savepoint clears the aborted state (25P02)!
	let (_, last) = run_simple_query(&mut stream, "ROLLBACK TO SAVEPOINT sp_before_err;").await;
	assert_eq!(last, b'C');

	// After rolling back to savepoint, queries succeed again!
	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO err_rec (val) VALUES ('good2');").await;
	assert_eq!(last, b'C', "Query after rollback to savepoint must succeed");

	// Commit transaction
	let (_, last) = run_simple_query(&mut stream, "COMMIT;").await;
	assert_eq!(last, b'C');

	// Verify committed data
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT val FROM err_rec ORDER BY val ASC;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("good1"));
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("good2"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_guc_show_set_reset() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Initial default timezone is UTC
	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("UTC"));

	// SHOW server_version
	let (rows, last) = run_simple_query(&mut stream, "SHOW server_version;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let r0 = decode_data_row(&rows[0]);
	assert!(r0[0].as_deref().unwrap().contains("16.0-surrealdb"));

	// SET timezone = 'Asia/Tokyo';
	let (_, last) = run_simple_query(&mut stream, "SET timezone = 'Asia/Tokyo';").await;
	assert_eq!(last, b'C');

	// Verify changed timezone
	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Asia/Tokyo"));

	// SHOW TIME ZONE (case-insensitive syntax with space)
	let (rows, last) = run_simple_query(&mut stream, "SHOW TIME ZONE;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Asia/Tokyo"));

	// SET TIME ZONE 'Europe/London';
	let (_, last) = run_simple_query(&mut stream, "SET TIME ZONE 'Europe/London';").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Europe/London"));

	// RESET timezone;
	let (_, last) = run_simple_query(&mut stream, "RESET timezone;").await;
	assert_eq!(last, b'C');

	// Reverted to default UTC
	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("UTC"));

	// SET NAMES 'utf8';
	let (_, last) = run_simple_query(&mut stream, "SET NAMES 'utf8';").await;
	assert_eq!(last, b'C');

	// RESET ALL;
	let (_, last) = run_simple_query(&mut stream, "RESET ALL;").await;
	assert_eq!(last, b'C');

	// Cannot change readonly parameter
	let (_, last) = run_simple_query(&mut stream, "SET server_version = '99.0';").await;
	assert_eq!(last, b'E');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_guc_show_all() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (rows, last) = run_simple_query(&mut stream, "SHOW ALL;").await;
	assert_eq!(last, b'C');
	assert!(rows.len() >= 10, "SHOW ALL must return all catalog entries");

	let mut found_tz = false;
	let mut found_encoding = false;
	let mut found_ver = false;

	for row_bytes in &rows {
		let cols = decode_data_row(row_bytes);
		assert_eq!(cols.len(), 3, "SHOW ALL rows must have 3 columns: name, setting, description");
		let name = cols[0].as_deref().unwrap_or("");
		if name == "TimeZone" {
			found_tz = true;
			assert_eq!(cols[1].as_deref(), Some("UTC"));
		} else if name == "client_encoding" {
			found_encoding = true;
			assert_eq!(cols[1].as_deref(), Some("UTF8"));
		} else if name == "server_version" {
			found_ver = true;
			assert!(cols[1].as_deref().unwrap().contains("16.0-surrealdb"));
		}
	}

	assert!(found_tz, "TimeZone must be in SHOW ALL");
	assert!(found_encoding, "client_encoding must be in SHOW ALL");
	assert!(found_ver, "server_version must be in SHOW ALL");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_guc_transaction_and_savepoint_scoping() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Establish session value
	let (_, last) = run_simple_query(&mut stream, "SET timezone = 'America/New_York';").await;
	assert_eq!(last, b'C');

	// Inside transaction, SET LOCAL should revert on ROLLBACK
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(&mut stream, "SET LOCAL timezone = 'Europe/Paris';").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Europe/Paris"));

	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');

	// Reverted to session value
	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("America/New_York"));

	// SET LOCAL should revert even on COMMIT!
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(&mut stream, "SET LOCAL timezone = 'Asia/Tokyo';").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Asia/Tokyo"));

	let (_, last) = run_simple_query(&mut stream, "COMMIT;").await;
	assert_eq!(last, b'C');

	// Reverted to session value after COMMIT
	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("America/New_York"));

	// Savepoint scoping test
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(&mut stream, "SAVEPOINT sp_guc;").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(&mut stream, "SET timezone = 'Asia/Seoul';").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Asia/Seoul"));

	let (_, last) = run_simple_query(&mut stream, "ROLLBACK TO SAVEPOINT sp_guc;").await;
	assert_eq!(last, b'C');

	// Reverted to value before savepoint
	let (rows, last) = run_simple_query(&mut stream, "SHOW timezone;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("America/New_York"));

	let (_, last) = run_simple_query(&mut stream, "COMMIT;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_current_setting_query() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT current_setting('timezone') AS tz;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("UTC"));

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT current_setting('server_version') AS ver;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let r0 = decode_data_row(&rows[0]);
	assert!(r0[0].as_deref().unwrap().contains("16.0-surrealdb"));

	// Missing parameter with missing_ok=true returns null / none
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT current_setting('missing_param', true) AS missing;")
			.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);

	// Missing parameter without missing_ok errors
	let (_, last) =
		run_simple_query(&mut stream, "SELECT current_setting('missing_param') AS missing;").await;
	assert_eq!(last, b'E');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_unknown_database_3d000() {
	let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();
	let sess = Session::owner().with_ns("test").with_db("test");
	let secret = "secret-key-that-is-at-least-32-characters-long!";
	ds.execute(
		&format!(
			r#"
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE ACCESS token ON DATABASE TYPE RECORD
			WITH JWT ALGORITHM HS256 KEY '{secret}'
			DURATION FOR SESSION 30d;
		CREATE user:alice;
		"#
		),
		&sess,
		None,
	)
	.await
	.unwrap();

	let (addr, shutdown) = start_test_pg_server_with_ds(ds).await;
	let token = make_test_jwt("test", "test", "token", "user:alice", secret);

	// Record user connecting to nonexistent DB in namespace fails with 3D000
	let res = connect_and_handshake_with_auth(
		addr,
		"user:alice",
		"test/nonexistent_db",
		Some(&token),
		Some("-c dialect=pgsql"),
	)
	.await;

	match res {
		Ok(_) => panic!("Expected connection failure for nonexistent database"),
		Err((code, msg)) => {
			assert_eq!(code, "3D000", "Expected SQLSTATE 3D000, got: {code} ({msg})");
			assert!(
				msg.contains("does not exist"),
				"Expected message to contain 'does not exist', got: {msg}"
			);
		}
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_password_as_jwt_and_permissions() {
	let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();
	let sess = Session::owner().with_ns("test").with_db("test");
	let secret = "secret-key-that-is-at-least-32-characters-long!";
	ds.execute(
		&format!(
			r#"
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE ACCESS token ON DATABASE TYPE RECORD
			WITH JWT ALGORITHM HS256 KEY '{secret}'
			DURATION FOR SESSION 30d;

		DEFINE TABLE document SCHEMAFULL PERMISSIONS
			FOR select WHERE author = $auth;
		DEFINE FIELD title ON TABLE document TYPE string;
		DEFINE FIELD author ON TABLE document TYPE record<user>;

		CREATE user:alice;
		CREATE user:bob;
		CREATE document:1 SET title = 'Alice Private Doc', author = user:alice;
		CREATE document:2 SET title = 'Bob Private Doc', author = user:bob;
		"#
		),
		&sess,
		None,
	)
	.await
	.unwrap();

	let (addr, shutdown) = start_test_pg_server_with_ds(ds).await;

	let token = make_test_jwt("test", "test", "token", "user:alice", secret);

	// Connect as user:alice with password = JWT token
	let mut stream = connect_and_handshake_with_auth(
		addr,
		"user:alice",
		"test/test",
		Some(&token),
		Some("-c dialect=pgsql"),
	)
	.await
	.expect("Password-as-JWT handshake failed");

	// Query documents - table permissions should only return Alice's document!
	let (rows, last) = run_simple_query(&mut stream, "SELECT title FROM document;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1, "Expected only 1 row due to record user table permissions");
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Alice Private Doc"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_set_local_role_narrowing_and_elevation() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// In transaction: SET LOCAL ROLE viewer
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(&mut stream, "SET LOCAL ROLE viewer;").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW role;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("viewer"));

	// Commit reverts local role override
	let (_, last) = run_simple_query(&mut stream, "COMMIT;").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW role;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("none"));

	// Narrowing to anon works
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) = run_simple_query(&mut stream, "SET LOCAL ROLE anon;").await;
	assert_eq!(last, b'C');

	let (rows, last) = run_simple_query(&mut stream, "SHOW role;").await;
	assert_eq!(last, b'C');
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("anon"));

	// Elevating from unauthenticated anon fails with 42501
	let (_, last, err) =
		run_simple_query_with_err(&mut stream, "SET LOCAL ROLE authenticated;").await;
	assert_eq!(last, b'E');
	let (code, msg) = err.expect("Expected error fields");
	assert_eq!(code, "42501", "Expected 42501 insufficient_privilege, got: {code} ({msg})");

	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_set_local_claims_narrowing_and_elevation() {
	let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();
	let sess = Session::owner().with_ns("test").with_db("test");
	let secret = "secret-key-that-is-at-least-32-characters-long!";
	ds.execute(
		&format!(
			r#"
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE ACCESS token ON DATABASE TYPE RECORD
			WITH JWT ALGORITHM HS256 KEY '{secret}'
			DURATION FOR SESSION 30d;
		CREATE user:alice;
		"#
		),
		&sess,
		None,
	)
	.await
	.unwrap();

	let (addr, shutdown) = start_test_pg_server_with_ds(ds).await;
	let token = make_test_jwt("test", "test", "token", "user:alice", secret);

	let mut stream = connect_and_handshake_with_auth(
		addr,
		"user:alice",
		"test/test",
		Some(&token),
		Some("-c dialect=pgsql"),
	)
	.await
	.expect("Password-as-JWT handshake failed");

	// Narrowing role via claims works
	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');

	let (_, last) =
		run_simple_query(&mut stream, r#"SET LOCAL "request.jwt.claims" = '{"role": "anon"}';"#)
			.await;
	assert_eq!(last, b'C');

	// Attempting to impersonate a different user fails with 42501
	let (_, last, err) = run_simple_query_with_err(
		&mut stream,
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "user:bob"}';"#,
	)
	.await;
	assert_eq!(last, b'E');
	let (code, msg) = err.expect("Expected error fields");
	assert_eq!(code, "42501", "Expected 42501 insufficient_privilege, got: {code} ({msg})");

	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

/// Runs one `SET LOCAL` inside its own transaction and returns the SQLSTATE of a refusal.
async fn set_local_refused_with(stream: &mut TcpStream, set: &str) -> Option<String> {
	let (_, last) = run_simple_query(stream, "BEGIN;").await;
	assert_eq!(last, b'C');
	let (_, last, err) = run_simple_query_with_err(stream, set).await;
	let (_, rolled_back) = run_simple_query(stream, "ROLLBACK;").await;
	assert_eq!(rolled_back, b'C');
	if last == b'E' {
		Some(err.expect("Expected error fields").0)
	} else {
		None
	}
}

#[tokio::test]
async fn test_pgwire_claims_subject_must_match_session_identity_exactly() {
	let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();
	let sess = Session::owner().with_ns("test").with_db("test");
	let secret = "secret-key-that-is-at-least-32-characters-long!";
	ds.execute(
		&format!(
			r#"
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE ACCESS token ON DATABASE TYPE RECORD
			WITH JWT ALGORITHM HS256 KEY '{secret}'
			DURATION FOR SESSION 30d;
		CREATE user:alice;
		CREATE user:malice;
		CREATE user:11;
		"#
		),
		&sess,
		None,
	)
	.await
	.unwrap();

	let (addr, shutdown) = start_test_pg_server_with_ds(ds).await;
	let token = make_test_jwt("test", "test", "token", "user:malice", secret);
	let mut stream = connect_and_handshake_with_auth(
		addr,
		"user:malice",
		"test/test",
		Some(&token),
		Some("-c dialect=pgsql"),
	)
	.await
	.expect("Password-as-JWT handshake failed");

	// A subject that is only the tail of this session's record id, or only starts with it,
	// names a different identity.
	for set in [
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "alice"}';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "user:alice"}';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"id": "alice"}';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "user:malice:x"}';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "user:malice x"}';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'alice';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'user:alice';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'ser:malice';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'user:malice garbage';"#,
		// The setting name is case-insensitive.
		r#"SET LOCAL "Request.JWT.Claim.Sub" = 'alice';"#,
		r#"SET LOCAL "REQUEST.JWT.CLAIMS" = '{"sub": "alice"}';"#,
	] {
		assert_eq!(
			set_local_refused_with(&mut stream, set).await.as_deref(),
			Some("42501"),
			"{set}"
		);
	}

	// The session's own identity is accepted as the full record id and as its key.
	for set in [
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "user:malice"}';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "malice"}';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'user:malice';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'malice';"#,
		r#"SET LOCAL "Request.JWT.Claim.Sub" = 'malice';"#,
	] {
		assert_eq!(set_local_refused_with(&mut stream, set).await, None, "{set}");
	}

	// A numeric key names exactly that number.
	let token = make_test_jwt("test", "test", "token", "user:11", secret);
	let mut stream = connect_and_handshake_with_auth(
		addr,
		"user:11",
		"test/test",
		Some(&token),
		Some("-c dialect=pgsql"),
	)
	.await
	.expect("Password-as-JWT handshake failed");
	for set in [
		r#"SET LOCAL "request.jwt.claim.sub" = '1';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = '111';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'user:1';"#,
	] {
		assert_eq!(
			set_local_refused_with(&mut stream, set).await.as_deref(),
			Some("42501"),
			"{set}"
		);
	}
	for set in [
		r#"SET LOCAL "request.jwt.claim.sub" = '11';"#,
		r#"SET LOCAL "request.jwt.claim.sub" = 'user:11';"#,
	] {
		assert_eq!(set_local_refused_with(&mut stream, set).await, None, "{set}");
	}

	shutdown.cancel();
}

/// How a `COPY ... FROM STDIN` data stream ends.
enum CopyEnd {
	Done,
	Fail,
}

/// What the server sent back for a `COPY ... FROM STDIN`.
struct CopyOutcome {
	/// The SQLSTATE of every ErrorResponse, in order.
	errors: Vec<String>,
	/// The message of every ErrorResponse, in order.
	messages: Vec<String>,
	/// The last CommandComplete tag, empty if there was none.
	complete: String,
	/// The ReadyForQuery transaction status.
	ready: u8,
}

/// Sends `sql` and waits for the server to ask for data.
async fn copy_in_begin(stream: &mut TcpStream, sql: &str) {
	let mut query = sql.to_string();
	query.push('\0');
	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());
	stream.write_all(&q_pkt).await.expect("Failed to send query");

	let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
	let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
	let mut payload = vec![0u8; msg_len - 4];
	stream.read_exact(&mut payload).await.expect("Failed to read payload");
	assert_eq!(msg_type, b'G', "Expected CopyInResponse ('G'), got {}", msg_type as char);
}

/// Sends one CopyData message.
async fn copy_in_send(stream: &mut TcpStream, chunk: &[u8]) {
	let mut d_pkt = BytesMut::new();
	d_pkt.put_u8(b'd');
	d_pkt.put_i32((chunk.len() + 4) as i32);
	d_pkt.put_slice(chunk);
	stream.write_all(&d_pkt).await.expect("Failed to send copy data");
}

/// Ends the data stream and reads to ReadyForQuery.
async fn copy_in_finish(stream: &mut TcpStream, end: CopyEnd) -> CopyOutcome {
	timeout(Duration::from_secs(120), async {
		let mut end_pkt = BytesMut::new();
		match end {
			CopyEnd::Done => {
				end_pkt.put_u8(b'c');
				end_pkt.put_i32(4);
			}
			CopyEnd::Fail => {
				end_pkt.put_u8(b'f');
				end_pkt.put_i32(4 + 7);
				end_pkt.put_slice(b"client\0");
			}
		}
		stream.write_all(&end_pkt).await.expect("Failed to end the copy");

		let mut errors = Vec::new();
		let mut messages = Vec::new();
		let mut complete = String::new();
		loop {
			let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
			let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await.expect("Failed to read payload");
			match msg_type {
				b'E' => {
					let fields = parse_pg_fields_map(&payload);
					errors.push(fields[&'C'].clone());
					messages.push(fields[&'M'].clone());
				}
				b'C' => {
					complete = String::from_utf8_lossy(&payload).trim_end_matches('\0').to_string();
				}
				b'Z' => {
					return CopyOutcome {
						errors,
						messages,
						complete,
						ready: payload[0],
					};
				}
				_ => {}
			}
		}
	})
	.await
	.expect("COPY did not complete in time")
}

/// Sends `sql`, streams `chunks` as CopyData, ends the stream, and reads to ReadyForQuery.
async fn copy_in_raw(
	stream: &mut TcpStream,
	sql: &str,
	chunks: &[&[u8]],
	end: CopyEnd,
) -> CopyOutcome {
	timeout(Duration::from_secs(120), async {
		copy_in_begin(stream, sql).await;
		for chunk in chunks {
			copy_in_send(stream, chunk).await;
		}
		copy_in_finish(stream, end).await
	})
	.await
	.expect("COPY did not complete in time")
}

/// `data` cut into messages of `size` bytes, wherever that splits it.
fn pieces(data: &[u8], size: usize) -> Vec<&[u8]> {
	data.chunks(size).collect()
}

/// A record longer than the limit on a record, with no newline, in 1 MiB pieces.
fn overlong_record() -> Vec<u8> {
	vec![b'1'; 3 << 20]
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_streams_many_text_rows() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let data: String = (0..30_000).map(|i| format!("{i}\tn{i}\n")).collect();
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_many (id, name) FROM STDIN;",
		&pieces(data.as_bytes(), 7001),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 30000");
	assert_eq!(stored_count(&ds, "copy_many").await, 30_000);
	assert_eq!(
		try_query_rendered(&ds, "SELECT VALUE name FROM copy_many:29999;").await.as_deref(),
		Some("['n29999']")
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_inserts_rows_while_the_stream_is_open() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	copy_in_begin(&mut stream, "COPY copy_early (id) FROM STDIN;").await;
	let data: String = (0..60_000).map(|i| format!("{i}\n")).collect();
	for piece in pieces(data.as_bytes(), 8192) {
		copy_in_send(&mut stream, piece).await;
	}
	// The rows already received, all but the last slice, are inserted without waiting for CopyDone.
	timeout(Duration::from_secs(30), async {
		while stored_count(&ds, "copy_early").await < 10_000 {
			sleep(Duration::from_millis(50)).await;
		}
	})
	.await
	.expect("rows must be inserted before CopyDone");

	let outcome = copy_in_finish(&mut stream, CopyEnd::Done).await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 60000");
	assert_eq!(stored_count(&ds, "copy_early").await, 60_000);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_csv_split_at_every_byte() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// A quoted newline, a doubled quote, CRLF, a multi-byte character, a quoted
	// delimiter, and a last record with no newline.
	let data = "1,\"a\nb\"\r\n2,\"c\"\"d\"\n3,é,\"x,y\"\n4,end";
	let outcome = copy_in_raw(
		&mut stream,
		"COPY csv_split (id, v, w) FROM STDIN WITH (FORMAT CSV);",
		&pieces(data.as_bytes(), 1),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 4");
	for (select, expected) in [
		("SELECT VALUE v FROM csv_split:1;", r"['a\nb']"),
		("SELECT VALUE v FROM csv_split:2;", r#"['c"d']"#),
		("SELECT VALUE v FROM csv_split:3;", "['é']"),
		("SELECT VALUE w FROM csv_split:3;", "['x,y']"),
		("SELECT VALUE v FROM csv_split:4;", "['end']"),
	] {
		assert_eq!(try_query_rendered(&ds, select).await.as_deref(), Some(expected), "{select}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_text_split_at_every_byte_stops_at_the_end_marker() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let data = "1\talice\n2\t\\N\n3\té\n\\.\n4\tignored\n";
	let outcome = copy_in_raw(
		&mut stream,
		"COPY text_split (id, name) FROM STDIN;",
		&pieces(data.as_bytes(), 1),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 3");
	for (select, expected) in [
		("SELECT VALUE name FROM text_split:1;", "['alice']"),
		("SELECT VALUE name FROM text_split:2;", "[NULL]"),
		("SELECT VALUE name FROM text_split:3;", "['é']"),
		("SELECT VALUE name FROM text_split:4;", "[]"),
	] {
		assert_eq!(try_query_rendered(&ds, select).await.as_deref(), Some(expected), "{select}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_refuses_an_overlong_record_once() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let record = overlong_record();
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_long (n) FROM STDIN;",
		&pieces(&record, 1 << 20),
		CopyEnd::Done,
	)
	.await;
	assert_eq!(outcome.errors, ["54000"]);
	assert_eq!(outcome.ready, b'I');

	// The rest of the stream was discarded, so the connection is still in sync and usable.
	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_overlong_record_then_copy_fail_reports_one_error() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let record = overlong_record();
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_long (n) FROM STDIN;",
		&pieces(&record, 1 << 20),
		CopyEnd::Fail,
	)
	.await;
	assert_eq!(outcome.errors, ["54000"]);
	assert_eq!(outcome.ready, b'I');

	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_overlong_record_aborts_the_transaction() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');
	let record = overlong_record();
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_long (n) FROM STDIN;",
		&pieces(&record, 1 << 20),
		CopyEnd::Done,
	)
	.await;
	assert_eq!(outcome.errors, ["54000"]);
	assert_eq!(outcome.ready, b'E', "the failed COPY aborts the open transaction");

	let (_, last, err) = run_simple_query_with_err(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'E');
	assert_eq!(err.expect("Expected error fields").0, "25P02");
	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');
	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_refuses_a_record_with_too_many_fields() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let wide = format!("{}1\n", "1\t".repeat(1600));
	let outcome =
		copy_in_raw(&mut stream, "COPY copy_wide FROM STDIN;", &[wide.as_bytes()], CopyEnd::Done)
			.await;
	assert_eq!(outcome.errors, ["54011"]);
	assert_eq!(outcome.ready, b'I');

	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

/// `COPY` data that parses in many slices: valid single-column rows before and after `middle`.
fn rows_around(middle: &str) -> String {
	let valid: String = (0..30_000).map(|i| format!("{i}\n")).collect();
	format!("{valid}{middle}{valid}")
}

/// A datastore with a table whose `n` must be an integer, so a row holding anything else fails
/// to insert.
async fn datastore_with_typed_table() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	ds.execute(
		r#"
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE TABLE copy_typed SCHEMAFULL;
		DEFINE FIELD n ON copy_typed TYPE int;
		"#,
		&Session::owner().with_ns("test").with_db("test"),
		None,
	)
	.await
	.expect("Failed to define the table");
	ds
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_insert_failure_midway_reports_once_and_keeps_earlier_batches()
{
	let ds = datastore_with_typed_table().await;
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// The 60,001st row is not an integer. The server finds that while the data is still being
	// sent, reports one error, and reads the rest of the stream without acting on it.
	let data = rows_around("abc\n");
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_typed (n) FROM STDIN;",
		&pieces(data.as_bytes(), 8192),
		CopyEnd::Done,
	)
	.await;
	assert_eq!(outcome.errors.len(), 1, "{:?}", outcome.messages);
	assert_eq!(outcome.ready, b'I');
	// Each batch commits as it is inserted: the 60 whole batches before the failing one stay.
	assert_eq!(stored_count(&ds, "copy_typed").await, 30_000);

	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_insert_failure_midway_aborts_the_transaction() {
	let ds = datastore_with_typed_table().await;
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');
	let data = rows_around("abc\n");
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_typed (n) FROM STDIN;",
		&pieces(data.as_bytes(), 8192),
		CopyEnd::Done,
	)
	.await;
	assert_eq!(outcome.errors.len(), 1, "{:?}", outcome.messages);
	assert_eq!(outcome.ready, b'E');

	let (_, last, err) = run_simple_query_with_err(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'E');
	assert_eq!(err.expect("Expected error fields").0, "25P02");
	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');
	assert_eq!(stored_count(&ds, "copy_typed").await, 0);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_inside_a_transaction_commits_or_rolls_back_as_a_whole() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let data: String = (0..30_000).map(|i| format!("{i}\n")).collect();
	for (end, table, expected) in
		[("ROLLBACK;", "copy_rolled_back", 0), ("COMMIT;", "copy_committed", 30_000)]
	{
		let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
		assert_eq!(last, b'C');
		let outcome = copy_in_raw(
			&mut stream,
			&format!("COPY {table} (n) FROM STDIN;"),
			&pieces(data.as_bytes(), 8192),
			CopyEnd::Done,
		)
		.await;
		assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
		assert_eq!(outcome.complete, "COPY 30000");
		assert_eq!(outcome.ready, b'T');
		// Nothing is visible to other sessions until the transaction ends.
		assert_eq!(stored_count(&ds, table).await, 0);
		let (_, last) = run_simple_query(&mut stream, end).await;
		assert_eq!(last, b'C');
		assert_eq!(stored_count(&ds, table).await, expected, "{end}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_copy_fail_midway_keeps_the_batches_already_inserted() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let data: String = (0..30_000).map(|i| format!("{i}\n")).collect();
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_failed (n) FROM STDIN;",
		&pieces(data.as_bytes(), 8192),
		CopyEnd::Fail,
	)
	.await;
	assert_eq!(outcome.errors, ["57014"]);
	assert_eq!(outcome.ready, b'I');
	// Rows are inserted as they arrive, so those received before the abort remain.
	assert!(stored_count(&ds, "copy_failed").await > 0);

	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_refuses_invalid_utf8_midway_and_stays_in_step() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let mut data = rows_around("").into_bytes();
	let at = data.len() / 2;
	data.splice(at..at, *b"\xff\n");
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_utf8 (n) FROM STDIN;",
		&pieces(&data, 8192),
		CopyEnd::Done,
	)
	.await;
	assert_eq!(outcome.errors, ["22P04"]);
	assert_eq!(outcome.ready, b'I');
	assert!(stored_count(&ds, "copy_utf8").await > 0);

	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_one_message_per_row() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let rows: Vec<String> = (0..20_000).map(|i| format!("{i}\n")).collect();
	let messages: Vec<&[u8]> = rows.iter().map(|row| row.as_bytes()).collect();
	let outcome =
		copy_in_raw(&mut stream, "COPY copy_tiny (n) FROM STDIN;", &messages, CopyEnd::Done).await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 20000");
	assert_eq!(stored_count(&ds, "copy_tiny").await, 20_000);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_accepts_a_record_of_the_largest_size() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// A record, newline included, of exactly 1 MiB.
	let mut record = vec![b'x'; (1 << 20) - 1];
	record.push(b'\n');
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_largest (v) FROM STDIN;",
		&pieces(&record, 100_000),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 1");
	assert_eq!(stored_count(&ds, "copy_largest").await, 1);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_wide_rows_are_batched_by_size() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// Twelve rows of 400 KB: well under 500 rows, so only their size splits them into batches.
	let row = format!("{}\n", "y".repeat(400_000));
	let data = row.repeat(12);
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_wide_rows (v) FROM STDIN;",
		&pieces(data.as_bytes(), 65_536),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 12");
	assert_eq!(stored_count(&ds, "copy_wide_rows").await, 12);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_with_no_rows_copies_nothing() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let outcome =
		copy_in_raw(&mut stream, "COPY copy_none (n) FROM STDIN;", &[], CopyEnd::Done).await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 0");

	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_none FROM STDIN WITH (FORMAT CSV, HEADER);",
		&[b"a,b\n"],
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 0");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_refuses_a_csv_record_with_too_many_fields() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let wide = format!("{}1\n", "1,".repeat(1600));
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_wide FROM STDIN WITH (FORMAT CSV);",
		&[wide.as_bytes()],
		CopyEnd::Done,
	)
	.await;
	assert_eq!(outcome.errors, ["54011"]);
	assert_eq!(outcome.ready, b'I');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_text_with_crlf_stops_at_the_end_marker() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_crlf (id) FROM STDIN;",
		&[b"1\r\n2\r\n\\.\r\n3\r\n"],
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 2");
	assert_eq!(stored_count(&ds, "copy_crlf").await, 2);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_ignores_what_follows_the_end_marker() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// After the marker come more than a record's worth of bytes with no newline, and bytes that
	// are not UTF-8.
	let mut data = b"1\n2\n\\.\n".to_vec();
	data.extend(vec![b'z'; 2 << 20]);
	data.extend_from_slice(b"\xff");
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_ended (id) FROM STDIN;",
		&pieces(&data, 100_000),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 2");
	assert_eq!(stored_count(&ds, "copy_ended").await, 2);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_csv_with_header_streams_quoted_newlines_across_slices() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// Over 100 KB of records, each with a quoted newline and doubled quotes, in 1 KiB messages
	// that fall in the middle of fields, so several slices are cut inside or beside quotes.
	let mut data = String::from("id,note\n");
	for i in 0..3000 {
		data.push_str(&format!("{i},\"line {i}\nsecond \"\"quoted\"\" line\"\n"));
	}
	let outcome = copy_in_raw(
		&mut stream,
		"COPY csv_big FROM STDIN WITH (FORMAT CSV, HEADER);",
		&pieces(data.as_bytes(), 1000),
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 3000");
	assert_eq!(stored_count(&ds, "csv_big").await, 3000);
	for i in [0, 1499, 2999] {
		assert_eq!(
			try_query_rendered(&ds, &format!("SELECT VALUE note FROM csv_big:{i};")).await,
			Some(format!(r#"['line {i}\nsecond "quoted" line']"#)),
		);
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_refuses_options_it_cannot_scan_for() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	for (sql, code) in [
		("COPY copy_opts FROM STDIN WITH (FORMAT CSV, QUOTE 'é');", "0A000"),
		("COPY copy_opts FROM STDIN WITH (DELIMITER 'é');", "0A000"),
		("COPY copy_opts FROM STDIN WITH (DELIMITER '\n');", "22023"),
	] {
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(last, b'E', "{sql}");
		assert_eq!(err.expect("Expected error fields").0, code, "{sql}");
	}
	let (_, last) = run_simple_query(&mut stream, "SELECT 1;").await;
	assert_eq!(last, b'C');

	shutdown.cancel();
}

/// Runs a query against the datastore and renders its result, or the error as text.
async fn query_rendered(ds: &Datastore, sql: &str) -> Result<String, String> {
	let sess = Session::owner().with_ns("test").with_db("test");
	let mut res = ds.execute(sql, &sess, None).await.map_err(|err| err.to_string())?;
	res.remove(0).result.map(|value| value.to_sql()).map_err(|err| err.to_string())
}

/// Runs a query against the datastore and renders its result, or `None` if it fails.
async fn try_query_rendered(ds: &Datastore, sql: &str) -> Option<String> {
	query_rendered(ds, sql).await.ok()
}

/// The number of rows in a table, 0 while the table does not exist.
async fn stored_count(ds: &Datastore, table: &str) -> usize {
	match query_rendered(ds, &format!("SELECT count() FROM {table} GROUP ALL;")).await {
		Ok(out) => out
			.split("count: ")
			.nth(1)
			.and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
			.and_then(|digits| digits.parse().ok())
			.unwrap_or(0),
		Err(err) if err.contains("does not exist") => 0,
		Err(err) => panic!("Failed to count the rows of {table}: {err}"),
	}
}

/// The rows of a table as the datastore stores them, rendered as SurrealQL.
async fn stored_rows(ds: &Datastore, table: &str) -> String {
	let sess = Session::owner().with_ns("test").with_db("test");
	let mut res = ds
		.execute(&format!("SELECT * FROM {table};"), &sess, None)
		.await
		.expect("Failed to read the table");
	res.remove(0).result.expect("The select failed").to_sql()
}

#[tokio::test]
async fn test_pgwire_copy_quotes_names_it_puts_into_statements() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last) =
		run_simple_query(&mut stream, "INSERT INTO victim (name) VALUES ('keep');").await;
	assert_eq!(last, b'C');

	// A column name that, spliced into the generated INSERT unquoted, closes the object and adds
	// a DELETE of another table.
	let hostile = "x: 1}]; DELETE victim; INSERT INTO t [{y";
	let (_, complete) =
		run_copy_in(&mut stream, &format!(r#"COPY t ("{hostile}") FROM STDIN;"#), &[b"1\n"]).await;
	assert_eq!(complete, "COPY 1");
	assert!(
		stored_rows(&ds, "victim").await.contains("keep"),
		"the victim table must be untouched"
	);
	let stored = stored_rows(&ds, "t").await;
	assert!(stored.contains(&format!("{hostile}\": 1")), "the name is stored as one key: {stored}");

	// The same name arriving through a CSV header row.
	let hostile = "x: 1}]; DELETE victim; INSERT INTO t2 [{y";
	let header = format!("\"{hostile}\"\n2\n");
	let (_, complete) = run_copy_in(
		&mut stream,
		"COPY t2 FROM STDIN WITH (FORMAT CSV, HEADER);",
		&[header.as_bytes()],
	)
	.await;
	assert_eq!(complete, "COPY 1");
	assert!(
		stored_rows(&ds, "victim").await.contains("keep"),
		"the victim table must be untouched"
	);
	let stored = stored_rows(&ds, "t2").await;
	assert!(stored.contains(&format!("{hostile}\": 2")), "the name is stored as one key: {stored}");

	// Names with an embedded quote or backslash are stored as written.
	let (_, complete) =
		run_copy_in(&mut stream, r#"COPY t3 ("a""b\c") FROM STDIN;"#, &[b"3\n"]).await;
	assert_eq!(complete, "COPY 1");
	let stored = stored_rows(&ds, "t3").await;
	assert!(stored.contains(r#""a\"b\\c": 3"#), "unexpected rows: {stored}");

	let (_, complete) =
		run_copy_in(&mut stream, r#"COPY "w""x\y'z" ("v") FROM STDIN;"#, &[b"4\n"]).await;
	assert_eq!(complete, "COPY 1");
	let stored = stored_rows(&ds, r#"`w"x\\y'z`"#).await;
	assert!(stored.contains("v: 4"), "unexpected rows: {stored}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_without_columns_accepts_a_quote_in_the_table_name() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	// With no column list and no header the columns are looked up by table name first.
	let outcome =
		copy_in_raw(&mut stream, r#"COPY "o'brien" FROM STDIN;"#, &[b"5\n"], CopyEnd::Done).await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	let stored = stored_rows(&ds, "`o'brien`").await;
	assert!(stored.contains("col_1: 5"), "unexpected rows: {stored}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_to_stdout_quotes_table_and_column_names() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, complete) =
		run_copy_in(&mut stream, r#"COPY "a b" ("c d") FROM STDIN;"#, &[b"7\n"]).await;
	assert_eq!(complete, "COPY 1");

	// Names with a space are quoted into the generated query, so it reads the right table
	// and column.
	let (chunks, complete) = run_copy_out(&mut stream, r#"COPY "a b" ("c d") TO STDOUT;"#).await;
	assert_eq!(complete, "COPY 1");
	assert_eq!(chunks.concat(), b"7\n");

	shutdown.cancel();
}

fn parse_pg_fields_map(payload: &[u8]) -> std::collections::HashMap<char, String> {
	let mut map = std::collections::HashMap::new();
	let mut idx = 0;
	while idx < payload.len() {
		let field_type = payload[idx];
		if field_type == 0 {
			break;
		}
		idx += 1;
		let start = idx;
		while idx < payload.len() && payload[idx] != 0 {
			idx += 1;
		}
		let val = String::from_utf8_lossy(&payload[start..idx]).to_string();
		if idx < payload.len() {
			idx += 1; // skip null
		}
		map.insert(field_type as char, val);
	}
	map
}

async fn run_simple_query_with_notices(
	stream: &mut TcpStream,
	sql: &str,
) -> (Vec<Vec<u8>>, u8, Option<(String, String)>, Vec<(String, String)>) {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send query");

	let mut rows = Vec::new();
	let mut notices = Vec::new();
	let mut last_type = 0u8;
	let mut last_err = None;

	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'D' {
				rows.push(payload);
			} else if msg_type == b'N' {
				notices.push(parse_pg_error_fields(&payload));
			} else if msg_type == b'Z' {
				break;
			} else {
				if msg_type == b'E' {
					last_err = Some(parse_pg_error_fields(&payload));
				}
				last_type = msg_type;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for query completion: {sql}");
	(rows, last_type, last_err, notices)
}

#[tokio::test]
async fn test_pgwire_notice_response_for_ignored_guc() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last, err, notices) =
		run_simple_query_with_notices(&mut stream, "SET check_function_bodies = 'off';").await;
	assert_eq!(last, b'C');
	assert!(err.is_none());
	assert_eq!(notices.len(), 1, "Expected 1 notice response, got: {notices:?}");
	let (code, msg) = &notices[0];
	assert_eq!(code, "01000");
	assert!(
		msg.contains("check_function_bodies") && msg.contains("ignored"),
		"Expected ignored notice message, got: {msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_notice_response_for_set_local_outside_transaction() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, last, err, notices) =
		run_simple_query_with_notices(&mut stream, "SET LOCAL timezone = 'UTC';").await;
	assert_eq!(last, b'C');
	assert!(err.is_none());
	assert_eq!(notices.len(), 1, "Expected 1 notice response, got: {notices:?}");
	let (code, msg) = &notices[0];
	assert_eq!(code, "01000");
	assert!(
		msg.contains("SET LOCAL will only have effect in transactions"),
		"Expected transaction warning notice, got: {msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_error_structured_fields_syntax_position() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream =
		connect_and_handshake_with_auth(addr, "root", "test/test", None, Some("-c dialect=pgsql"))
			.await
			.expect("Handshake failed");

	// Send Parse with invalid syntax + Sync
	let sql = "SELECT * FROM ;";
	let mut parse = BytesMut::new();
	parse.put_u8(b'P');
	let mut body = BytesMut::new();
	body.put_u8(0); // unnamed statement
	body.put_slice(sql.as_bytes());
	body.put_u8(0);
	body.put_i16(0); // 0 param types
	parse.put_i32((body.len() + 4) as i32);
	parse.put(body);

	let mut sync = BytesMut::new();
	sync.put_u8(b'S');
	sync.put_i32(4);

	stream.write_all(&parse).await.unwrap();
	stream.write_all(&sync).await.unwrap();

	// Read response: expect ErrorResponse 'E', then ReadyForQuery 'Z'
	let mut err_fields = std::collections::HashMap::new();
	let mut ready = false;
	let res = timeout(Duration::from_secs(5), async {
		while !ready {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'E' {
				err_fields = parse_pg_fields_map(&payload);
			} else if msg_type == b'Z' {
				ready = true;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok(), "Timed out waiting for error response");

	assert_eq!(err_fields.get(&'C').map(|s| s.as_str()), Some("42601"));
	assert!(
		err_fields.contains_key(&'P'),
		"Expected 'P' (Position) field in syntax error response: {err_fields:?}"
	);
	let pos: u32 = err_fields[&'P'].parse().expect("Valid position integer");
	assert!(pos > 0, "Position should be 1-based, got {pos}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_error_sqlstate_duplicate_table() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// First create table succeeds
	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE dup_tbl (id int);").await;
	assert_eq!(last, b'C');

	// Second create table fails with 42P07 (duplicate_table)
	let (_, last, err) =
		run_simple_query_with_err(&mut stream, "CREATE TABLE dup_tbl (id int);").await;
	assert_eq!(last, b'E');
	let (code, msg) = err.expect("Expected error fields");
	assert_eq!(code, "42P07", "Expected 42P07 duplicate_table, got: {code} ({msg})");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_catalog_pg_type_and_namespace() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Query pg_type
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT typname, typlen FROM pg_catalog.pg_type WHERE oid = 23;",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1, "Expected 1 row for oid 23");
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("int4"));
	assert_eq!(r0[1].as_deref(), Some("4"));

	// Query pg_namespace
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT nspname FROM pg_catalog.pg_namespace;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 4, "Expected 4 namespaces");
	let namespaces: Vec<_> = rows.iter().map(|r| decode_data_row(r)[0].clone().unwrap()).collect();
	assert!(namespaces.contains(&"pg_catalog".to_string()));
	assert!(namespaces.contains(&"public".to_string()));
	assert!(namespaces.contains(&"information_schema".to_string()));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_catalog_pg_class_and_settings() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Query pg_class
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'pg_type';",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1, "Expected 1 row for pg_type in pg_class");
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("pg_type"));
	assert_eq!(r0[1].as_deref(), Some("r"));

	// Query pg_settings
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'server_version';",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1, "Expected 1 row for server_version setting");
	let r0 = decode_data_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("server_version"));
	assert_eq!(r0[1].as_deref(), Some("16.0"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_introspection_functions() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT version(), current_schema(), current_user, session_user, user, pg_is_in_recovery(), format_type(23, -1);",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let r0 = decode_data_row(&rows[0]);
	assert!(r0[0].as_deref().unwrap().contains("PostgreSQL 16.0"));
	assert_eq!(r0[1].as_deref(), Some("public"));
	assert_eq!(r0[2].as_deref(), Some("root"));
	assert_eq!(r0[3].as_deref(), Some("root"));
	assert_eq!(r0[4].as_deref(), Some("root"));
	assert_eq!(r0[5].as_deref(), Some("f"));
	assert_eq!(r0[6].as_deref(), Some("integer"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_information_schema() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Query information_schema.schemata
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT schema_name FROM information_schema.schemata;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 4);
	let schemata: Vec<_> = rows.iter().map(|r| decode_data_row(r)[0].clone().unwrap()).collect();
	assert!(schemata.contains(&"public".to_string()));
	assert!(schemata.contains(&"pg_catalog".to_string()));

	// Query information_schema.views
	let (rows, last) =
		run_simple_query(&mut stream, "SELECT table_name FROM information_schema.views;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 4);

	shutdown.cancel();
}

async fn run_copy_in(stream: &mut TcpStream, sql: &str, data_chunks: &[&[u8]]) -> (u8, String) {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send query");

	// Expect CopyInResponse ('G')
	let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
	let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
	let mut payload = vec![0u8; msg_len - 4];
	stream.read_exact(&mut payload).await.expect("Failed to read payload");
	assert_eq!(msg_type, b'G', "Expected CopyInResponse ('G'), got {}", msg_type as char);

	// Send CopyData chunks ('d')
	for chunk in data_chunks {
		let mut d_pkt = BytesMut::new();
		d_pkt.put_u8(b'd');
		d_pkt.put_i32((chunk.len() + 4) as i32);
		d_pkt.put_slice(chunk);
		stream.write_all(&d_pkt).await.expect("Failed to send copy data");
	}

	// Send CopyDone ('c')
	let mut c_pkt = BytesMut::new();
	c_pkt.put_u8(b'c');
	c_pkt.put_i32(4);
	stream.write_all(&c_pkt).await.expect("Failed to send copy done");

	// Read until ReadyForQuery ('Z')
	let mut last_complete = String::new();
	let mut last_type = 0u8;
	loop {
		let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
		let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
		let mut payload = vec![0u8; msg_len - 4];
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
		if msg_type == b'C' {
			last_complete = String::from_utf8_lossy(&payload).trim_end_matches('\0').to_string();
			last_type = msg_type;
		} else if msg_type == b'Z' {
			break;
		} else {
			last_type = msg_type;
		}
	}
	(last_type, last_complete)
}

async fn run_copy_out(stream: &mut TcpStream, sql: &str) -> (Vec<Vec<u8>>, String) {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send query");

	// Expect CopyOutResponse ('H')
	let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
	let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
	let mut payload = vec![0u8; msg_len - 4];
	stream.read_exact(&mut payload).await.expect("Failed to read payload");
	assert_eq!(msg_type, b'H', "Expected CopyOutResponse ('H'), got {}", msg_type as char);

	let mut data_chunks = Vec::new();
	let mut last_complete = String::new();

	loop {
		let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
		let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
		let mut payload = vec![0u8; msg_len - 4];
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
		match msg_type {
			b'd' => {
				data_chunks.push(payload);
			}
			b'c' => {
				// CopyDone
			}
			b'C' => {
				last_complete =
					String::from_utf8_lossy(&payload).trim_end_matches('\0').to_string();
			}
			b'Z' => {
				break;
			}
			other => {
				panic!("Unexpected message in copy out: {}", other as char);
			}
		}
	}
	(data_chunks, last_complete)
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_text() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, complete) = run_copy_in(
		&mut stream,
		"COPY copy_text (user_id, name) FROM STDIN;",
		&[b"1\talice\n2\tbob\n"],
	)
	.await;
	assert_eq!(complete, "COPY 2");

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT user_id, name FROM copy_text ORDER BY user_id;")
			.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r0[0].as_deref(), Some("1"));
	assert_eq!(r0[1].as_deref(), Some("alice"));
	assert_eq!(r1[0].as_deref(), Some("2"));
	assert_eq!(r1[1].as_deref(), Some("bob"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_from_stdin_csv_header() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let (_, complete) = run_copy_in(
		&mut stream,
		"COPY copy_csv FROM STDIN WITH (FORMAT CSV, HEADER);",
		&[b"user_id,name\n10,charlie\n20,david\n"],
	)
	.await;
	assert_eq!(complete, "COPY 2");

	let (rows, last) =
		run_simple_query(&mut stream, "SELECT user_id, name FROM copy_csv ORDER BY user_id;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 2);
	let r0 = decode_data_row(&rows[0]);
	let r1 = decode_data_row(&rows[1]);
	assert_eq!(r0[0].as_deref(), Some("10"));
	assert_eq!(r0[1].as_deref(), Some("charlie"));
	assert_eq!(r1[0].as_deref(), Some("20"));
	assert_eq!(r1[1].as_deref(), Some("david"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_fail() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let mut query = "COPY copy_fail (user_id) FROM STDIN;".to_string();
	query.push('\0');
	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());
	stream.write_all(&q_pkt).await.expect("Failed to send query");

	// Expect CopyInResponse ('G')
	let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
	let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
	let mut payload = vec![0u8; msg_len - 4];
	stream.read_exact(&mut payload).await.expect("Failed to read payload");
	assert_eq!(msg_type, b'G');

	// Send CopyFail ('f')
	let fail_msg = b"user cancelled operation\0";
	let mut f_pkt = BytesMut::new();
	f_pkt.put_u8(b'f');
	f_pkt.put_i32((fail_msg.len() + 4) as i32);
	f_pkt.put_slice(fail_msg);
	stream.write_all(&f_pkt).await.expect("Failed to send copy fail");

	// Expect ErrorResponse ('E') and ReadyForQuery ('Z')
	let mut saw_error = false;
	let mut err_code = String::new();
	loop {
		let msg_type = stream.read_u8().await.expect("Failed to read msg_type");
		let msg_len = stream.read_i32().await.expect("Failed to read len") as usize;
		let mut payload = vec![0u8; msg_len - 4];
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
		if msg_type == b'E' {
			saw_error = true;
			let (c, _) = parse_pg_error_fields(&payload);
			err_code = c;
		} else if msg_type == b'Z' {
			break;
		}
	}
	assert!(saw_error);
	assert_eq!(err_code, "57014");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_to_stdout_text_and_csv() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Populate data
	let (_, complete) = run_copy_in(
		&mut stream,
		"COPY copy_export (user_id, name) FROM STDIN;",
		&[b"1\talice\n2\tbob\n"],
	)
	.await;
	assert_eq!(complete, "COPY 2");

	// COPY table TO STDOUT (Text)
	let (chunks, complete) =
		run_copy_out(&mut stream, "COPY copy_export (user_id, name) TO STDOUT;").await;
	assert_eq!(complete, "COPY 2");
	let combined: Vec<u8> = chunks.into_iter().flatten().collect();
	let text = String::from_utf8(combined).unwrap();
	assert!(text.contains("1\talice\n"));
	assert!(text.contains("2\tbob\n"));

	// COPY query TO STDOUT (CSV with HEADER)
	let (chunks, complete) = run_copy_out(
		&mut stream,
		"COPY (SELECT user_id, name FROM copy_export ORDER BY user_id) TO STDOUT WITH (FORMAT CSV, HEADER);",
	)
	.await;
	assert_eq!(complete, "COPY 2");
	let combined: Vec<u8> = chunks.into_iter().flatten().collect();
	let csv_text = String::from_utf8(combined).unwrap();
	assert_eq!(csv_text, "user_id,name\n1,alice\n2,bob\n");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_multi_statement_rejection() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Attempt to Parse multiple statements in extended query protocol
	let parse = build_parse("multi", "SELECT 1; SELECT 2;", &[]);
	let sync = build_sync();
	stream.write_all(&parse).await.expect("send parse");
	stream.write_all(&sync).await.expect("send sync");

	let mut err = None;
	let mut got_ready = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'E' {
				err = Some(parse_pg_error_fields(&payload));
			} else if msg_type == b'Z' {
				got_ready = true;
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for response");
	assert!(got_ready, "Expected ReadyForQuery after error");
	let (code, msg) = err.expect("Expected ErrorResponse for multi-statement parse");
	assert_eq!(code, "42601", "Expected SQLSTATE 42601 for multi-statement parse");
	assert!(
		msg.contains("cannot insert multiple commands into a prepared statement"),
		"Unexpected error message: {msg}"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_describe_parameter_inference() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Parse statement with unspecified parameter type (OID 0)
	let parse = build_parse("stmt_infer", "SELECT ($1::int + 5) AS sum;", &[0]);
	let desc = build_desc(b'S', "stmt_infer");
	let sync = build_sync();
	stream.write_all(&parse).await.expect("send parse");
	stream.write_all(&desc).await.expect("send desc");
	stream.write_all(&sync).await.expect("send sync");

	let mut param_oids = Vec::new();
	let mut got_row_desc = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b't' {
				// ParameterDescription
				use bytes::Buf;
				let mut cur = &payload[..];
				let count = cur.get_i16() as usize;
				for _ in 0..count {
					param_oids.push(cur.get_i32());
				}
			} else if msg_type == b'T' {
				got_row_desc = true;
			} else if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for response");
	assert!(got_row_desc, "Expected RowDescription for SELECT statement");
	assert_eq!(param_oids.len(), 1, "Expected 1 parameter description");
	assert_eq!(param_oids[0], 23, "Expected parameter type inferred as int4 (OID 23)");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_describe_write_nodata_and_no_premature_exec() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Create table
	run_simple_query(&mut stream, "CREATE TABLE desc_test (x int);").await;

	// 1. Describe Statement on INSERT returns NoData ('n')
	let p1 = build_parse("ins_stmt", "INSERT INTO desc_test (x) VALUES (1);", &[]);
	let d1 = build_desc(b'S', "ins_stmt");
	let s1 = build_sync();
	stream.write_all(&p1).await.expect("send parse");
	stream.write_all(&d1).await.expect("send desc");
	stream.write_all(&s1).await.expect("send sync");

	let mut has_nodata_stmt = false;
	let mut has_row_desc_stmt = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'n' {
				has_nodata_stmt = true;
			} else if msg_type == b'T' {
				has_row_desc_stmt = true;
			} else if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok());
	assert!(has_nodata_stmt, "Describe(Statement) on INSERT must return NoData ('n')");
	assert!(
		!has_row_desc_stmt,
		"Describe(Statement) on INSERT must not return RowDescription ('T')"
	);

	// 2. In a transaction, Bind portal and Describe Portal returns NoData ('n') WITHOUT executing
	run_simple_query(&mut stream, "BEGIN;").await;

	let b2 = build_bind("ins_portal", "ins_stmt", &[], &[]);
	let d2 = build_desc(b'P', "ins_portal");
	let s2 = build_sync();
	stream.write_all(&b2).await.expect("send bind");
	stream.write_all(&d2).await.expect("send desc");
	stream.write_all(&s2).await.expect("send sync");

	let mut has_nodata_portal = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'n' {
				has_nodata_portal = true;
			} else if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok());
	assert!(has_nodata_portal, "Describe(Portal) on INSERT must return NoData ('n')");

	// Verify table is still empty (Describe Portal did NOT execute the statement!)
	let (rows, _) = run_simple_query(&mut stream, "SELECT x FROM desc_test;").await;
	assert_eq!(rows.len(), 0, "Describe(Portal) must not have executed the INSERT!");

	// 3. Now Execute the portal
	let e3 = build_exec("ins_portal", 0);
	let s3 = build_sync();
	stream.write_all(&e3).await.expect("send exec");
	stream.write_all(&s3).await.expect("send sync");

	let mut cmd_complete = String::new();
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'C' {
				cmd_complete = String::from_utf8_lossy(&payload).trim_end_matches('\0').to_string();
			} else if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok());
	assert_eq!(cmd_complete, "INSERT 0 1");

	// Verify row was now inserted
	let (rows, _) = run_simple_query(&mut stream, "SELECT x FROM desc_test;").await;
	assert_eq!(rows.len(), 1, "Execute must have inserted 1 row");

	run_simple_query(&mut stream, "COMMIT;").await;
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_real_command_tags() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Execute helper that runs Parse + Bind + Execute + Sync and returns the CommandComplete tag
	async fn exec_tag(stream: &mut TcpStream, stmt_name: &str, sql: &str) -> String {
		let p = build_parse(stmt_name, sql, &[]);
		let b = build_bind("", stmt_name, &[], &[]);
		let e = build_exec("", 0);
		let s = build_sync();
		stream.write_all(&p).await.expect("send parse");
		stream.write_all(&b).await.expect("send bind");
		stream.write_all(&e).await.expect("send exec");
		stream.write_all(&s).await.expect("send sync");

		let mut tag = String::new();
		let res = timeout(Duration::from_secs(5), async {
			loop {
				let msg_type = stream.read_u8().await?;
				let msg_len = stream.read_i32().await? as usize;
				let mut payload = vec![0u8; msg_len - 4];
				stream.read_exact(&mut payload).await?;
				if msg_type == b'C' {
					tag = String::from_utf8_lossy(&payload).trim_end_matches('\0').to_string();
				} else if msg_type == b'Z' {
					break;
				}
			}
			Ok::<_, std::io::Error>(())
		})
		.await;
		assert!(res.is_ok());
		tag
	}

	assert_eq!(
		exec_tag(&mut stream, "s1", "CREATE TABLE tag_tbl (val int);").await,
		"CREATE TABLE"
	);
	assert_eq!(
		exec_tag(&mut stream, "s2", "INSERT INTO tag_tbl (val) VALUES (10);").await,
		"INSERT 0 1"
	);
	assert_eq!(
		exec_tag(&mut stream, "s3", "UPDATE tag_tbl SET val = 20 WHERE val = 10;").await,
		"UPDATE 1"
	);
	assert_eq!(
		exec_tag(&mut stream, "s4", "DELETE FROM tag_tbl WHERE val = 20;").await,
		"DELETE 1"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_portal_survives_sync_in_transaction() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	run_simple_query(&mut stream, "CREATE TABLE portal_tx (v int);").await;
	run_simple_query(&mut stream, "INSERT INTO portal_tx (v) VALUES (1), (2), (3);").await;

	// Begin an explicit transaction
	run_simple_query(&mut stream, "BEGIN;").await;

	// Parse and Bind a portal
	let p = build_parse("p_stmt", "SELECT v FROM portal_tx ORDER BY v ASC;", &[]);
	let b = build_bind("p_port", "p_stmt", &[], &[]);
	stream.write_all(&p).await.expect("send parse");
	stream.write_all(&b).await.expect("send bind");

	// Execute 1 row
	let e1 = build_exec("p_port", 1);
	let s1 = build_sync();
	stream.write_all(&e1).await.expect("send exec 1");
	stream.write_all(&s1).await.expect("send sync");

	let mut rows1 = Vec::new();
	let mut suspended1 = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'D' {
				rows1.push(payload);
			} else if msg_type == b's' {
				suspended1 = true;
			} else if msg_type == b'Z' {
				assert_eq!(payload[0], b'T', "Transaction block status should be 'T'");
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok());
	assert_eq!(rows1.len(), 1);
	assert!(suspended1, "Expected PortalSuspended ('s') for partial fetch");
	let cols1 = decode_data_row(&rows1[0]);
	assert_eq!(cols1[0].as_deref(), Some("1"));

	// Execute 1 more row from the SAME portal across Sync!
	let e2 = build_exec("p_port", 1);
	let s2 = build_sync();
	stream.write_all(&e2).await.expect("send exec 2");
	stream.write_all(&s2).await.expect("send sync");

	let mut rows2 = Vec::new();
	let mut suspended2 = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'D' {
				rows2.push(payload);
			} else if msg_type == b's' {
				suspended2 = true;
			} else if msg_type == b'Z' {
				assert_eq!(payload[0], b'T');
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok());
	assert_eq!(rows2.len(), 1);
	assert!(suspended2, "Expected PortalSuspended ('s') for second partial fetch");
	let cols2 = decode_data_row(&rows2[0]);
	assert_eq!(cols2[0].as_deref(), Some("2"));

	// Fetch the remaining row
	let e3 = build_exec("p_port", 10);
	let s3 = build_sync();
	stream.write_all(&e3).await.expect("send exec 3");
	stream.write_all(&s3).await.expect("send sync");

	let mut rows3 = Vec::new();
	let mut completed3 = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'D' {
				rows3.push(payload);
			} else if msg_type == b'C' {
				completed3 = true;
			} else if msg_type == b'Z' {
				assert_eq!(payload[0], b'T');
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok());
	assert_eq!(rows3.len(), 1);
	assert!(completed3, "Expected CommandComplete ('C') when portal is exhausted");
	let cols3 = decode_data_row(&rows3[0]);
	assert_eq!(cols3[0].as_deref(), Some("3"));

	run_simple_query(&mut stream, "COMMIT;").await;
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_reject_replication_startup_parameter() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");

	// Send StartupMessage with replication=database
	let mut body = BytesMut::new();
	body.put_i32(196608); // Protocol 3.0
	body.put_slice(b"user\0root\0");
	body.put_slice(b"database\0test\0");
	body.put_slice(b"replication\0database\0");
	body.put_u8(0);

	let len = (body.len() + 4) as i32;
	let mut packet = BytesMut::new();
	packet.put_i32(len);
	packet.put(body);
	stream.write_all(&packet).await.expect("send startup");

	let mut err = None;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'E' {
				err = Some(parse_pg_error_fields(&payload));
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for startup error");
	let (code, msg) = err.expect("Expected ErrorResponse for replication");
	assert_eq!(code, "0A000", "Expected feature_not_supported code 0A000");
	assert!(msg.contains("replication is not supported"), "Unexpected message: {msg}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_reject_function_call_message() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Send FunctionCall ('F') message followed by Sync ('S')
	let mut f_msg = BytesMut::new();
	f_msg.put_u8(b'F');
	f_msg.put_i32(4);
	let sync = build_sync();
	stream.write_all(&f_msg).await.expect("send FunctionCall");
	stream.write_all(&sync).await.expect("send Sync");

	let mut err = None;
	let mut got_ready = false;
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			if msg_type == b'E' {
				err = Some(parse_pg_error_fields(&payload));
			} else if msg_type == b'Z' {
				got_ready = true;
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for FunctionCall response");
	assert!(got_ready, "Expected ReadyForQuery after FunctionCall");
	let (code, msg) = err.expect("Expected ErrorResponse for FunctionCall");
	assert_eq!(code, "0A000", "Expected feature_not_supported 0A000");
	assert!(msg.contains("FunctionCall is not supported"), "Unexpected message: {msg}");

	shutdown.cancel();
}
