// Awaiting a datastore constructor lays out the sealed async bodies it reaches, which nest deeper
// than the default recursion limit.
#![recursion_limit = "256"]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
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

/// The datastore of every server a test started, oldest first, held weakly so that a server that
/// has shut down releases it. An entry is never removed, and the system hands a port out again once
/// its server is gone, so the newest entry of an address is the live server.
static SERVERS: std::sync::Mutex<Vec<(std::net::SocketAddr, Weak<Datastore>)>> =
	std::sync::Mutex::new(Vec::new());

/// Runs SurrealQL in the `test`/`test` database of the datastore behind the server at `addr`.
/// The wire speaks only PostgreSQL, so fixtures are written in-process.
async fn seed_surrealql(addr: std::net::SocketAddr, sql: &str) {
	if let Err(err) = try_surrealql(addr, sql).await {
		panic!("{sql}: {err}");
	}
}

/// As `seed_surrealql`, returning the first error instead of panicking.
async fn try_surrealql(addr: std::net::SocketAddr, sql: &str) -> Result<(), String> {
	let ds = SERVERS
		.lock()
		.expect("lock the servers")
		.iter()
		.rev()
		.find(|(a, _)| *a == addr)
		.and_then(|(_, ds)| ds.upgrade())
		.expect("a live server started by the test helpers");
	let sess = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(sql, &sess, None).await.map_err(|err| err.to_string())? {
		response.result.map_err(|err| err.to_string())?;
	}
	Ok(())
}

async fn start_test_pg_server_with_ds(
	ds: Arc<Datastore>,
) -> (std::net::SocketAddr, CancellationToken) {
	let ds_for_seed = Arc::downgrade(&ds);
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_pgwire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start pgwire server");
	SERVERS.lock().expect("lock the servers").push((addr, ds_for_seed));

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
async fn test_pgwire_function_errors_carry_the_sqlstate_of_the_reference_server() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for (sql, expected) in [
		("SELECT mod(1, 0);", "22012"),
		("SELECT substring('x', 1, -1);", "22011"),
		("SELECT ln(0);", "2201E"),
		("SELECT log(-1);", "2201E"),
		("SELECT split_part('a', ',', 0);", "22023"),
		("SELECT date_part('foo', CAST('2024-03-15T10:20:30Z' AS timestamp));", "22023"),
		("SELECT substring('foobar' FROM 'o.b');", "0A000"),
	] {
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(last, b'E', "{sql} must fail");
		let (code, message) = err.expect("an error carries its fields");
		assert_eq!(code, expected, "{sql}: {message}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_function_values_are_sent_in_the_type_the_descriptor_declares() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for (sql, check) in
		[("SELECT round(2.567, 2) AS v;", "2.57"), ("SELECT pg_typeof(true) AS v;", "boolean")]
	{
		let (rows, last) = run_simple_query(&mut stream, sql).await;
		assert_eq!(last, b'C', "{sql}");
		let cols = decode_data_row(&rows[0]);
		assert_eq!(cols[0].as_deref(), Some(check), "{sql}");
	}
	let (rows, last) = run_simple_query(&mut stream, "SELECT current_date AS v;").await;
	assert_eq!(last, b'C');
	let day = decode_data_row(&rows[0])[0].clone().expect("a date");
	assert_eq!(day.len(), 10, "{day}");
	assert!(day.chars().filter(|c| *c == '-').count() == 2, "{day}");
	let (rows, last) = run_simple_query(&mut stream, "SELECT gen_random_uuid() AS v;").await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0])[0].as_deref().map(str::len), Some(36));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_dialect_switching_and_fail_closed() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake_with_options(addr, None).await;

	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE person (name text);").await;
	assert_eq!(last, b'C', "the default dialect is PostgreSQL");
	let (_, last) = run_simple_query(&mut stream, "CREATE person SET name = 'Invalid';").await;
	assert_eq!(last, b'E', "SurrealQL must fail closed in the PostgreSQL dialect");

	for dialect in ["surrealql", "mysql"] {
		let sql = format!("SET dialect = '{dialect}';");
		let (_, last, err) = run_simple_query_with_err(&mut stream, &sql).await;
		assert_eq!(last, b'E', "{sql}");
		assert_eq!(err.expect("error fields").0, "0A000", "{sql}");
	}
	let (_, last) = run_simple_query(&mut stream, "SET dialect = 'pgsql';").await;
	assert_eq!(last, b'C', "SET dialect = 'pgsql' must succeed");
	let (_, last) = run_simple_query(&mut stream, "CREATE person SET name = 'Invalid';").await;
	assert_eq!(last, b'E', "a refused switch leaves the dialect as it was");

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
	assert_eq!(r_false[0].as_deref(), Some("f"));
	assert_eq!(r_false[1].as_deref(), Some("1"));
	let r_true = decode_data_row(&rows[1]);
	assert_eq!(r_true[0].as_deref(), Some("t"));
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

	// 2. SQL-level PREPARE / DEALLOCATE are kept by the connection, and a name that was never
	// prepared is refused
	let (_, last) = run_simple_query(&mut stream, "PREPARE p1 AS SELECT 1;").await;
	assert_eq!(last, b'C', "SQL PREPARE must complete");

	let (_, last) = run_simple_query(&mut stream, "PREPARE p1 AS SELECT 2;").await;
	assert_eq!(last, b'E', "SQL PREPARE of a name in use must fail");

	let (_, last) = run_simple_query(&mut stream, "DEALLOCATE p1;").await;
	assert_eq!(last, b'C', "SQL DEALLOCATE must complete");

	let (_, last) = run_simple_query(&mut stream, "DEALLOCATE p1;").await;
	assert_eq!(last, b'E', "SQL DEALLOCATE of an unknown name must fail");

	let (_, last) = run_simple_query(&mut stream, "EXECUTE p1;").await;
	assert_eq!(last, b'E', "SQL EXECUTE of an unknown name must fail");

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
async fn the_startup_options_refuse_the_surrealql_dialect() {
	let (addr, shutdown) = start_test_pg_server().await;
	let result = connect_and_handshake_with_auth(
		addr,
		"root",
		"test/test",
		None,
		Some("-c dialect=surrealql"),
	)
	.await;
	let (code, _) = result.expect_err("the options parameter must refuse SurrealQL");
	assert_eq!(code, "0A000");
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
		// Record users cannot forge custom JWT claims or non-anonymous role claims.
		r#"SET LOCAL "request.jwt.claim.tenant" = 'other';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"sub": "user:malice", "tenant": "other"}';"#,
		r#"SET LOCAL "request.jwt.claims" = '{"role": "owner"}';"#,
		r#"SET LOCAL "request.jwt.claim.role" = 'owner';"#,
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
async fn test_pgwire_copy_from_stdin_stores_a_key_in_a_link_column_as_the_link() {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	query_rendered(&ds, "DEFINE NAMESPACE IF NOT EXISTS test")
		.await
		.expect("Failed to define the namespace");
	query_rendered(&ds, "DEFINE DATABASE IF NOT EXISTS test")
		.await
		.expect("Failed to define the database");
	query_rendered(
		&ds,
		"DEFINE TABLE copy_dept SCHEMALESS; \
		 DEFINE TABLE copy_emp SCHEMAFULL; \
		 DEFINE FIELD name ON copy_emp TYPE string; \
		 DEFINE FIELD dept ON copy_emp TYPE record<copy_dept>; \
		 DEFINE FIELD boss ON copy_emp TYPE option<record<copy_emp>>;",
	)
	.await
	.expect("Failed to define the tables");
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;

	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_emp (id, name, dept) FROM STDIN;",
		&[b"1\tEve\t7\n"],
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_emp (id, name, dept, boss) FROM STDIN;",
		&[b"2\tSam\tops\t1\n"],
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(outcome.complete, "COPY 1");
	assert_eq!(
		try_query_rendered(&ds, "SELECT VALUE [dept, boss ?? NONE] FROM copy_emp ORDER BY id;")
			.await
			.as_deref(),
		Some("[[copy_dept:7, NONE], [copy_dept:ops, copy_emp:1]]")
	);

	// Text that is not an integer names the string key it spells, whatever it looks like.
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_emp (id, name, dept) FROM STDIN;",
		&[b"3\tFlag\ttrue\n4\tRatio\t1.5\n5\tShort\tt\n"],
		CopyEnd::Done,
	)
	.await;
	assert!(outcome.errors.is_empty(), "COPY failed: {:?}", outcome.messages);
	assert_eq!(
		try_query_rendered(
			&ds,
			"SELECT VALUE dept FROM copy_emp WHERE record::id(id) IN [3, 4, 5] ORDER BY id;"
		)
		.await
		.as_deref(),
		Some("[copy_dept:true, copy_dept:`1.5`, copy_dept:t]")
	);

	// The empty text is no key: the column's own type check refuses it.
	let outcome = copy_in_raw(
		&mut stream,
		"COPY copy_emp (id, name, dept) FROM STDIN;",
		&[b"6\tBad\t\n"],
		CopyEnd::Done,
	)
	.await;
	assert!(!outcome.errors.is_empty(), "the empty text is no key of a link");
	assert_eq!(stored_count(&ds, "copy_emp").await, 5);

	shutdown.cancel();
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

#[tokio::test]
async fn test_pgwire_copy_to_stdout_shows_record_ids_as_keys() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(addr, "CREATE emp:7 SET name = 'Eve', dept = dept:1 RETURN NONE").await;
	let (chunks, complete) =
		run_copy_out(&mut stream, "COPY (SELECT id, dept FROM emp) TO STDOUT;").await;
	assert_eq!(complete, "COPY 1");
	assert_eq!(chunks.concat(), b"7\t1\n");
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
	assert_eq!(rows.len(), 7);

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

/// Every message of an extended-protocol batch up to ReadyForQuery, as (type, payload).
async fn run_extended_batch_messages(
	stream: &mut TcpStream,
	packets: &[BytesMut],
) -> Vec<(u8, Vec<u8>)> {
	for p in packets {
		stream.write_all(p).await.expect("send packet");
	}
	let mut messages = Vec::new();
	let res = timeout(Duration::from_secs(5), async {
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
	assert!(res.is_ok(), "Timed out waiting for extended batch response");
	messages
}

/// The bytes of the SQLSTATE field (`C`) of an ErrorResponse payload, as they are on the wire.
fn sqlstate_bytes(payload: &[u8]) -> Vec<u8> {
	let mut idx = 0;
	while idx < payload.len() && payload[idx] != 0 {
		let field = payload[idx];
		let start = idx + 1;
		let end = start + payload[start..].iter().position(|b| *b == 0).expect("terminated field");
		if field == b'C' {
			return payload[start..end].to_vec();
		}
		idx = end + 1;
	}
	panic!("no SQLSTATE field in {payload:?}");
}

/// The SQLSTATE bytes of every ErrorResponse among `messages`.
fn error_sqlstates(messages: &[(u8, Vec<u8>)]) -> Vec<Vec<u8>> {
	messages
		.iter()
		.filter(|(ty, _)| *ty == b'E')
		.map(|(_, payload)| sqlstate_bytes(payload))
		.collect()
}

/// Statements the planner refuses, each with the SQLSTATE the refusal carries: a feature that is
/// not supported is `0A000`, a path that is not a text array is `22P02` and an index that is
/// neither text nor a 32-bit integer is an operator that does not exist, `42883`.
const JSON_REFUSALS: [(&str, &[u8]); 10] = [
	("SELECT data @> '{\"a\":1}' FROM jt", b"0A000"),
	("SELECT data #> $1 FROM jt", b"0A000"),
	("SELECT data #>> $1 FROM jt", b"0A000"),
	("SELECT data #>> '{a,' FROM jt", b"22P02"),
	("SELECT data #>> 'a' FROM jt", b"22P02"),
	("UPDATE jt SET n = 1 WHERE data ->> 'a' = 'b'", b"0A000"),
	("SELECT data #>> '{a,,b}' FROM jt", b"22P02"),
	("SELECT data ->> 3000000000 FROM jt", b"42883"),
	("SELECT data -> 1.5 FROM jt", b"42883"),
	("SELECT data -> true FROM jt", b"42883"),
];

#[tokio::test]
async fn test_pgwire_planner_refusals_carry_their_sqlstate_in_the_simple_protocol() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE jt (n int, data jsonb);").await;
	assert_eq!(last, b'C');

	for (sql, sqlstate) in JSON_REFUSALS {
		let (rows, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(rows.is_empty(), "{sql}");
		assert_eq!(last, b'E', "{sql}");
		let (code, message) = err.expect("an ErrorResponse");
		assert_eq!(code.as_bytes(), sqlstate, "{sql}: {message}");
	}

	// A syntax error is still a syntax error, and the session carries on after every refusal.
	let (_, last, err) = run_simple_query_with_err(&mut stream, "SELECT FROM WHERE;").await;
	assert_eq!(last, b'E');
	assert_eq!(err.expect("an ErrorResponse").0, "42601");
	let (rows, last) = run_simple_query(&mut stream, "SELECT 1 AS one;").await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_planner_refusals_carry_their_sqlstate_in_the_extended_protocol() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE jt (n int, data jsonb);").await;
	assert_eq!(last, b'C');

	for (sql, sqlstate) in JSON_REFUSALS {
		let packets = [
			build_parse("", sql, &[]),
			build_bind("", "", &[], &[]),
			build_exec("", 0),
			build_sync(),
		];
		let messages = run_extended_batch_messages(&mut stream, &packets).await;
		let errors = error_sqlstates(&messages);
		assert_eq!(errors, vec![sqlstate.to_vec()], "{sql}: {messages:?}");
		let types: Vec<u8> = messages.iter().map(|(ty, _)| *ty).collect();
		assert_eq!(types.last(), Some(&b'Z'), "{sql}");
		assert!(!types.contains(&b'C'), "{sql}: a refused statement completes nothing");
	}

	// A SELECT is refused by Parse, which gives no ParseComplete; a write is refused when it runs.
	for (sql, sqlstate) in JSON_REFUSALS.iter().filter(|(sql, _)| sql.starts_with("SELECT")) {
		let messages =
			run_extended_batch_messages(&mut stream, &[build_parse("", sql, &[]), build_sync()])
				.await;
		let types: Vec<u8> = messages.iter().map(|(ty, _)| *ty).collect();
		assert_eq!(types, vec![b'E', b'Z'], "{sql}");
		assert_eq!(error_sqlstates(&messages), vec![sqlstate.to_vec()], "{sql}");
	}
	let messages = run_extended_batch_messages(
		&mut stream,
		&[build_parse("", JSON_REFUSALS[5].0, &[]), build_sync()],
	)
	.await;
	let types: Vec<u8> = messages.iter().map(|(ty, _)| *ty).collect();
	assert_eq!(types, vec![b'1', b'Z']);

	// The session carries on, and a statement that is not refused still runs.
	let packets = [
		build_parse("", "SELECT data ->> 'a' AS a FROM jt", &[]),
		build_bind("", "", &[], &[]),
		build_exec("", 0),
		build_sync(),
	];
	let messages = run_extended_batch_messages(&mut stream, &packets).await;
	assert!(error_sqlstates(&messages).is_empty(), "{messages:?}");
	assert!(messages.iter().any(|(ty, _)| *ty == b'C'), "{messages:?}");

	shutdown.cancel();
}

/// The unsupported-feature counters hold the kind of construct a refusal names. The planner's
/// message goes on to describe the client's statement, and none of that text is a counter key.
#[tokio::test]
async fn test_pgwire_planner_refusals_leave_the_statement_out_of_the_metrics() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, last) = run_simple_query(&mut stream, "CREATE TABLE jt (n int, data jsonb);").await;
	assert_eq!(last, b'C');

	// A SELECT is refused while it is bound, and a write while it is lowered. Each one is sent
	// as a simple query and as a prepared statement, with a literal of its own.
	let refusals = [
		"SELECT ARRAY[918273641] AS a FROM jt",
		"SELECT (918273642, 1) AS a FROM jt",
		"UPDATE jt SET n = ARRAY[918273643] WHERE n = 1",
		"UPDATE jt SET n = 1 WHERE (918273644, 2) = (n, n)",
	];
	for sql in refusals {
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(last, b'E', "{sql}");
		assert_eq!(err.expect("an ErrorResponse").0, "0A000", "{sql}");
		let packets = [
			build_parse("", &sql.replace("9182736", "9182737"), &[]),
			build_bind("", "", &[], &[]),
			build_exec("", 0),
			build_sync(),
		];
		let messages = run_extended_batch_messages(&mut stream, &packets).await;
		assert_eq!(error_sqlstates(&messages), vec![b"0A000".to_vec()], "{sql}: {messages:?}");
	}

	// A form of SET that the connection does not model is refused by the connection itself.
	for sql in ["SET TRANSACTION READ ONLY 918273645", "SET 918273646"] {
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(last, b'E', "{sql}");
		assert_eq!(err.expect("an ErrorResponse").0, "0A000", "{sql}");
	}

	let constructs = surrealdb_sqlplan::global_metrics().snapshot().unsupported_constructs;
	let keys: Vec<&String> = constructs.keys().collect();
	assert!(
		keys.iter().all(|key| !key.contains("91827")),
		"a counter key holds the client's statement: {keys:?}"
	);
	assert!(
		keys.iter().any(|key| key.as_str() == "unsupported expression"),
		"the refusals are counted by the construct they name: {keys:?}"
	);

	shutdown.cancel();
}

/// A result as a client sees it: the type OID of each column, and every row's cells.
struct Described {
	columns: Vec<(String, i32)>,
	rows: Vec<Vec<Option<Vec<u8>>>>,
}

impl Described {
	fn index(&self, name: &str) -> usize {
		self.columns
			.iter()
			.position(|(column, _)| column == name)
			.unwrap_or_else(|| panic!("no column {name} in {:?}", self.columns))
	}

	fn oid(&self, name: &str) -> i32 {
		self.columns[self.index(name)].1
	}

	fn raw(&self, row: usize, name: &str) -> Option<&[u8]> {
		self.rows[row][self.index(name)].as_deref()
	}

	fn cell(&self, row: usize, name: &str) -> Option<String> {
		self.raw(row, name).map(|bytes| String::from_utf8(bytes.to_vec()).expect("a UTF-8 cell"))
	}
}

fn parse_row_description(payload: &[u8]) -> Vec<(String, i32)> {
	let mut cur = payload;
	let count = cur.get_i16();
	let mut columns = Vec::new();
	for _ in 0..count {
		let end = cur.iter().position(|b| *b == 0).expect("a column name");
		let name = String::from_utf8_lossy(&cur[..end]).to_string();
		cur.advance(end + 1);
		cur.advance(4 + 2); // table oid, attribute number
		let type_oid = cur.get_i32();
		cur.advance(2 + 4 + 2); // type length, type modifier, format code
		columns.push((name, type_oid));
	}
	columns
}

fn parse_data_row_cells(payload: &[u8]) -> Vec<Option<Vec<u8>>> {
	let mut cur = payload;
	let count = cur.get_i16();
	(0..count)
		.map(|_| {
			let len = cur.get_i32();
			(len >= 0).then(|| {
				let cell = cur[..len as usize].to_vec();
				cur.advance(len as usize);
				cell
			})
		})
		.collect()
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
	let mut messages = Vec::new();
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			let ready = msg_type == b'Z';
			messages.push((msg_type, payload));
			if ready {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok(), "Timed out waiting for ReadyForQuery");
	messages
}

fn described(messages: Vec<(u8, Vec<u8>)>, sql: &str) -> Described {
	let mut columns = None;
	let mut rows = Vec::new();
	for (msg_type, payload) in messages {
		match msg_type {
			b'E' => panic!("{sql}: {:?}", parse_pg_error_fields(&payload)),
			b'T' => columns = Some(parse_row_description(&payload)),
			b'D' => rows.push(parse_data_row_cells(&payload)),
			_ => {}
		}
	}
	Described {
		columns: columns.unwrap_or_else(|| panic!("{sql}: no RowDescription")),
		rows,
	}
}

async fn describe_simple(stream: &mut TcpStream, sql: &str) -> Described {
	let mut query = sql.as_bytes().to_vec();
	query.push(0);
	let mut packet = BytesMut::new();
	packet.put_u8(b'Q');
	packet.put_i32((query.len() + 4) as i32);
	packet.put_slice(&query);
	stream.write_all(&packet).await.expect("send query");
	described(read_until_ready(stream).await, sql)
}

/// Run `sql` through the extended protocol the way psql and libpq do (Parse, Bind, Describe of
/// the portal, Execute, Sync), asking for every column in `result_format`.
async fn describe_extended(stream: &mut TcpStream, sql: &str, result_format: i16) -> Described {
	let mut bind = BytesMut::new();
	bind.put_u8(b'B');
	let mut body = BytesMut::new();
	body.put_u8(0); // unnamed portal
	body.put_u8(0); // unnamed statement
	body.put_i16(0); // no parameter formats
	body.put_i16(0); // no parameters
	body.put_i16(1); // one result format, for every column
	body.put_i16(result_format);
	bind.put_i32((body.len() + 4) as i32);
	bind.put(body);
	for packet in
		[build_parse("", sql, &[]), bind, build_desc(b'P', ""), build_exec("", 0), build_sync()]
	{
		stream.write_all(&packet).await.expect("send packet");
	}
	described(read_until_ready(stream).await, sql)
}

/// A schemaless `emp` table whose fields cover what a text column can be handed: datetimes with
/// and without a fraction and before 1970, durations of under and over a day, a bool, arrays, a
/// float, a decimal, a uuid, a geometry and an object.
async fn seed_emp(addr: std::net::SocketAddr) {
	seed_surrealql(
		addr,
		&[
			"DEFINE TABLE emp SCHEMALESS",
			"CREATE emp:1 CONTENT { name: 'Ada', born: d'1980-01-01T00:00:00Z', \
		 seen: d'2020-01-01T12:34:56.789Z', took: 1h30m, flag: true, tags: ['rust', 'db'], \
		 ratio: 1.5, dec: 1.25dec, u: u'018f6b2e-4c3d-7a11-9a52-0123456789ab', geo: (1.0, 2.0), \
		 meta: { a: 1, at: d'2020-01-01T00:00:00Z' } } RETURN NONE",
			"CREATE emp:2 CONTENT { name: 'Bob', born: d'1969-12-31T23:59:59Z', \
		 seen: d'2024-01-15T12:34:56.123456Z', took: 26h, flag: false, tags: [], \
		 ratio: 3.0 } RETURN NONE",
			"CREATE emp:3 CONTENT { name: 'Cy', born: d'2000-02-29T00:00:00.5Z', \
		 seen: d'2020-06-01T00:00:00Z', took: 0s, flag: true, tags: ['a b'], \
		 ratio: 0.25 } RETURN NONE",
		]
		.join("; "),
	)
	.await;
}

const EMP_FIELDS: [&str; 11] =
	["born", "seen", "took", "flag", "tags", "ratio", "dec", "u", "geo", "meta", "name"];

fn emp_named_select() -> String {
	format!("SELECT {} FROM emp ORDER BY id", EMP_FIELDS.join(", "))
}

const TIMESTAMPTZ_OID: i32 = 1184;
const INTERVAL_OID: i32 = 1186;
const BOOL_OID: i32 = 16;
const TEXT_ARRAY_OID: i32 = 1009;
const TEXT_OID: i32 = 25;

/// The text a field has in each row of `emp`, as a typed column shows it.
const EMP_TEXT: [(&str, [&str; 3]); 6] = [
	("born", ["1980-01-01 00:00:00+00", "1969-12-31 23:59:59+00", "2000-02-29 00:00:00.5+00"]),
	(
		"seen",
		["2020-01-01 12:34:56.789+00", "2024-01-15 12:34:56.123456+00", "2020-06-01 00:00:00+00"],
	),
	("took", ["01:30:00", "1 day 02:00:00", "00:00:00"]),
	("flag", ["t", "f", "t"]),
	("tags", ["{rust,db}", "{}", "{\"a b\"}"]),
	("ratio", ["1.5", "3", "0.25"]),
];

/// The text columns of `named` and the typed columns of `star` hold one text per field and row.
fn assert_one_text_per_value(star: &Described, named: &Described) {
	assert_eq!(named.rows.len(), 3);
	assert_eq!(star.rows.len(), 3);
	for field in EMP_FIELDS {
		for row in 0..3 {
			assert_eq!(
				named.raw(row, field).map(String::from_utf8_lossy),
				star.raw(row, field).map(String::from_utf8_lossy),
				"{field}, row {row}: a named column and `SELECT *` differ"
			);
		}
	}
	for (field, texts) in EMP_TEXT {
		for (row, text) in texts.iter().enumerate() {
			assert_eq!(named.cell(row, field).as_deref(), Some(*text), "{field}, row {row}");
		}
	}
	assert_eq!(
		named.cell(0, "u").as_deref(),
		Some("018f6b2e-4c3d-7a11-9a52-0123456789ab"),
		"a uuid"
	);
	assert_eq!(named.cell(0, "dec").as_deref(), Some("1.25"), "a decimal");
	assert_eq!(
		named.cell(0, "geo").as_deref(),
		Some("{\"coordinates\":[1.0,2.0],\"type\":\"Point\"}"),
		"a geometry"
	);
	assert_eq!(
		named.cell(0, "meta").as_deref(),
		Some("{\"a\":1,\"at\":\"2020-01-01T00:00:00Z\"}"),
		"an object is a JSON document"
	);
	assert_eq!(named.cell(1, "meta"), None);
}

/// The columns of `star` are typed from its rows, and the columns of `named` are text.
fn assert_typed_apart(star: &Described, named: &Described) {
	for (field, oid) in [
		("born", TIMESTAMPTZ_OID),
		("seen", TIMESTAMPTZ_OID),
		("took", INTERVAL_OID),
		("flag", BOOL_OID),
		("tags", TEXT_ARRAY_OID),
	] {
		assert_eq!(star.oid(field), oid, "`SELECT *` types {field}");
		assert_eq!(named.oid(field), TEXT_OID, "a named column {field} is text");
	}
}

#[tokio::test]
async fn test_pgwire_simple_query_named_columns_and_select_star_show_one_text() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_emp(addr).await;
	let star = describe_simple(&mut stream, "SELECT * FROM emp ORDER BY id").await;
	let named = describe_simple(&mut stream, &emp_named_select()).await;
	assert_typed_apart(&star, &named);
	assert_one_text_per_value(&star, &named);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_query_named_columns_and_select_star_show_one_text() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_emp(addr).await;
	let star = describe_extended(&mut stream, "SELECT * FROM emp ORDER BY id", 0).await;
	let named = describe_extended(&mut stream, &emp_named_select(), 0).await;
	assert_typed_apart(&star, &named);
	assert_one_text_per_value(&star, &named);
	// The extended protocol shows what the simple protocol does.
	let simple = describe_simple(&mut stream, "SELECT * FROM emp ORDER BY id").await;
	assert_eq!(star.rows, simple.rows);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_extended_query_binary_text_column_is_the_text_of_the_typed_column() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_emp(addr).await;
	let star_text = describe_extended(&mut stream, "SELECT * FROM emp ORDER BY id", 0).await;
	let named_binary = describe_extended(&mut stream, &emp_named_select(), 1).await;
	assert_typed_apart(&star_text, &named_binary);
	// A text column's binary cell is its text, which is the text cell of the typed column.
	assert_one_text_per_value(&star_text, &named_binary);
	// A typed column in binary format is the binary type, not text.
	let star_binary = describe_extended(&mut stream, "SELECT * FROM emp ORDER BY id", 1).await;
	assert_eq!(star_binary.raw(0, "flag"), Some(&[1u8][..]));
	let micros_since_2000 = (315_532_800i64 - 946_684_800) * 1_000_000;
	assert_eq!(star_binary.raw(0, "born"), Some(&micros_since_2000.to_be_bytes()[..]));
	assert_eq!(star_binary.raw(0, "took").map(<[u8]>::len), Some(16));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_copy_to_stdout_exports_a_text_column_as_the_typed_column_shows_it() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_emp(addr).await;
	let select = "SELECT id, tags, born, seen, took, flag FROM emp ORDER BY id";

	let (chunks, complete) =
		run_copy_out(&mut stream, &format!("COPY ({select}) TO STDOUT (FORMAT csv);")).await;
	assert_eq!(complete, "COPY 3");
	assert_eq!(
		String::from_utf8(chunks.concat()).unwrap(),
		"1,\"{rust,db}\",1980-01-01 00:00:00+00,2020-01-01 12:34:56.789+00,01:30:00,t\n\
		 2,{},1969-12-31 23:59:59+00,2024-01-15 12:34:56.123456+00,1 day 02:00:00,f\n\
		 3,\"{\"\"a b\"\"}\",2000-02-29 00:00:00.5+00,2020-06-01 00:00:00+00,00:00:00,t\n"
	);

	let (chunks, complete) =
		run_copy_out(&mut stream, &format!("COPY ({select}) TO STDOUT;")).await;
	assert_eq!(complete, "COPY 3");
	assert_eq!(
		String::from_utf8(chunks.concat()).unwrap(),
		"1\t{rust,db}\t1980-01-01 00:00:00+00\t2020-01-01 12:34:56.789+00\t01:30:00\tt\n\
		 2\t{}\t1969-12-31 23:59:59+00\t2024-01-15 12:34:56.123456+00\t1 day 02:00:00\tf\n\
		 3\t{\"a b\"}\t2000-02-29 00:00:00.5+00\t2020-06-01 00:00:00+00\t00:00:00\tt\n"
	);

	// The table form exports the same text.
	let (chunks, complete) =
		run_copy_out(&mut stream, "COPY emp (born, took, flag) TO STDOUT (FORMAT csv);").await;
	assert_eq!(complete, "COPY 3");
	let exported = String::from_utf8(chunks.concat()).unwrap();
	let mut lines: Vec<&str> = exported.lines().collect();
	lines.sort_unstable();
	assert_eq!(
		lines,
		[
			"1969-12-31 23:59:59+00,1 day 02:00:00,f",
			"1980-01-01 00:00:00+00,01:30:00,t",
			"2000-02-29 00:00:00.5+00,00:00:00,t",
		]
	);
	shutdown.cancel();
}

async fn seed_link_comparisons() -> (TcpStream, CancellationToken) {
	let (addr, shutdown) = start_test_pg_server().await;
	let stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		&[
			"DEFINE FIELD dept ON link_cmp TYPE option<record<dept>>",
			"CREATE link_cmp:1 SET dept = dept:1 RETURN NONE",
			"CREATE link_cmp:2 SET dept = dept:2 RETURN NONE",
			"CREATE link_cmp:3 RETURN NONE",
		]
		.join("; "),
	)
	.await;
	(stream, shutdown)
}

#[tokio::test]
async fn test_pgwire_link_nonkeys_remain_nonnull() {
	let (mut stream, shutdown) = seed_link_comparisons().await;
	for sql in [
		"SELECT id FROM link_cmp WHERE dept <> '' ORDER BY id",
		"SELECT id FROM link_cmp WHERE dept <> lower('') ORDER BY id",
	] {
		let (rows, _, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		let rows: Vec<_> = rows.iter().map(|row| decode_data_row(row)).collect();
		assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("2".into())]], "{sql}");
	}
	for sql in [
		"SELECT id FROM link_cmp WHERE dept IS NOT DISTINCT FROM ''",
		"SELECT id FROM link_cmp WHERE dept IS NOT DISTINCT FROM lower('')",
	] {
		let (rows, _, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		assert!(rows.is_empty(), "{sql}: {rows:?}");
	}
	for (sql, expected) in [
		(
			"SELECT id FROM link_cmp WHERE dept <> $1 ORDER BY id",
			vec![vec![Some("1".into())], vec![Some("2".into())]],
		),
		("SELECT id FROM link_cmp WHERE dept IS NOT DISTINCT FROM $1", vec![]),
	] {
		let (rows, last) = run_extended_query(&mut stream, sql, &[25], &[Some(b"")]).await;
		assert_eq!(last, b'C', "{sql}");
		let rows: Vec<_> = rows.iter().map(|row| decode_data_row(row)).collect();
		assert_eq!(rows, expected, "{sql}");
	}
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT id FROM link_cmp WHERE dept IS NOT DISTINCT FROM $1",
		&[25],
		&[None],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(
		rows.iter().map(|row| decode_data_row(row)).collect::<Vec<_>>(),
		vec![vec![Some("3".into())]]
	);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_link_integral_numeric_literals_match_integer_keys() {
	let (mut stream, shutdown) = seed_link_comparisons().await;
	for predicate in [
		"dept = 1",
		"dept = 1.0",
		"dept IN (1.0)",
		"dept IN (1.0, 9.0)",
		"dept = CAST(1 AS double precision)",
		"dept IS NOT DISTINCT FROM 1.0",
	] {
		let sql = format!("SELECT id FROM link_cmp WHERE {predicate}");
		let (rows, _, err) = run_simple_query_with_err(&mut stream, &sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		assert_eq!(
			rows.iter().map(|row| decode_data_row(row)).collect::<Vec<_>>(),
			vec![vec![Some("1".into())]],
			"{sql}"
		);
	}
	for predicate in ["dept = 1.5", "dept IN (1.5)"] {
		let sql = format!("SELECT id FROM link_cmp WHERE {predicate}");
		let (rows, _, err) = run_simple_query_with_err(&mut stream, &sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		assert!(rows.is_empty(), "{sql}: {rows:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_declared_scalar_types_drive_results_and_parameters() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		"CREATE TABLE scalar_types (id int PRIMARY KEY, age int, enabled boolean, name text)",
		"INSERT INTO scalar_types (id, age, enabled, name) VALUES (1, 42, true, 'Ada')",
		"INSERT INTO scalar_types (id, age, enabled, name) VALUES (2, NULL, false, 'Bob')",
	] {
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		assert_eq!(last, b'C', "{sql}");
	}
	let described = describe_simple(
		&mut stream,
		"SELECT p.id, p.age, p.enabled, p.name FROM scalar_types p ORDER BY p.id",
	)
	.await;
	assert_eq!(
		described.columns,
		vec![("id".into(), 23), ("age".into(), 23), ("enabled".into(), 16), ("name".into(), 25)]
	);
	assert_eq!(described.cell(0, "age").as_deref(), Some("42"));
	assert_eq!(described.cell(1, "age"), None);

	for (sql, params) in [
		("SELECT id FROM scalar_types p WHERE p.id = $1", vec![Some(b"1".as_slice())]),
		("SELECT id FROM scalar_types p WHERE p.age = $1", vec![Some(b"42".as_slice())]),
		("SELECT id FROM scalar_types p WHERE p.enabled = $1", vec![Some(b"true".as_slice())]),
		(
			"SELECT id FROM scalar_types WHERE id = $2 AND $1 IS NULL",
			vec![None, Some(b"1".as_slice())],
		),
		("SELECT id FROM scalar_types WHERE id = $1::int", vec![Some(b"1".as_slice())]),
	] {
		let (rows, last) = run_extended_query(&mut stream, sql, &[], &params).await;
		assert_eq!(last, b'C', "{sql}");
		assert_eq!(
			rows.iter().map(|row| decode_data_row(row)).collect::<Vec<_>>(),
			vec![vec![Some("1".into())]],
			"{sql}"
		);
	}
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT id FROM scalar_types WHERE id = $1",
		&[25],
		&[Some(b"1")],
	)
	.await;
	assert_eq!(last, b'C');
	assert!(rows.is_empty(), "an explicit text key must stay text");

	stream
		.write_all(&build_parse(
			"scalar_desc",
			"SELECT p.age, p.enabled FROM scalar_types p WHERE p.id = $2 AND $1 IS NULL",
			&[],
		))
		.await
		.unwrap();
	stream.write_all(&build_desc(b'S', "scalar_desc")).await.unwrap();
	stream.write_all(&build_sync()).await.unwrap();
	let messages = read_until_ready(&mut stream).await;
	let parameters = messages.iter().find(|(tag, _)| *tag == b't').expect("parameter description");
	let mut body = parameters.1.as_slice();
	assert_eq!(body.get_i16(), 2);
	assert_eq!(body.get_i32(), 25);
	assert_eq!(body.get_i32(), 23);
	let columns = messages.iter().find(|(tag, _)| *tag == b'T').expect("row description");
	assert_eq!(parse_row_description(&columns.1), vec![("age".into(), 23), ("enabled".into(), 16)]);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_scalar_types_use_the_current_transaction_schema() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		"BEGIN",
		"CREATE TABLE transaction_types (id int, enabled boolean)",
		"INSERT INTO transaction_types (id, enabled) VALUES (1, true)",
	] {
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		assert_eq!(last, b'C', "{sql}");
	}
	let desc = describe_extended(&mut stream, "SELECT id, enabled FROM transaction_types", 1).await;
	assert_eq!(desc.columns, vec![("id".into(), 23), ("enabled".into(), 16)]);
	assert_eq!(desc.raw(0, "id"), Some(1_i32.to_be_bytes().as_slice()));
	assert_eq!(desc.raw(0, "enabled"), Some([1_u8].as_slice()));
	let (_, last, err) = run_simple_query_with_err(&mut stream, "ROLLBACK").await;
	assert!(err.is_none(), "{err:?}");
	assert_eq!(last, b'C');
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_schema_discovery_preserves_analysis_sqlstates() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for (sql, code) in [("SELECT missing", "42703"), ("SELECT unknown.age FROM person p", "42P01")]
	{
		let (_, last, err) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(last, b'E', "{sql}");
		assert_eq!(err.expect("analysis error").0, code, "{sql}");
	}
	shutdown.cancel();
}

async fn assert_sql_ids(stream: &mut TcpStream, sql: &str, expected: &[&str]) {
	let (rows, _, error) = run_simple_query_with_err(stream, sql).await;
	assert!(error.is_none(), "{sql}: {error:?}");
	let mut ids: Vec<_> =
		rows.iter().map(|row| decode_data_row(row)[0].clone().expect("id")).collect();
	ids.sort();
	assert_eq!(ids, expected, "{sql}");
}

#[tokio::test]
async fn test_pgwire_sql_null_predicates_select_update_and_delete_agree() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		&[
			"CREATE null_pred:1 SET v = NULL, flag = NULL RETURN NONE",
			"CREATE null_pred:2 SET v = 1, flag = true RETURN NONE",
			"CREATE null_pred:3 SET v = 2, flag = false RETURN NONE",
			"CREATE null_pred:4 RETURN NONE",
			"CREATE null_pred:5 SET v = 3, flag = false RETURN NONE",
		]
		.join("; "),
	)
	.await;
	let cases: &[(&str, &[&str])] = &[
		("v <> 1", &["3", "5"]),
		("NOT (v = 1)", &["3", "5"]),
		("NOT (v <> 1)", &["2"]),
		("v = NULL", &[]),
		("NOT (v = NULL)", &[]),
		("v IS NULL", &["1", "4"]),
		("v IS NOT NULL", &["2", "3", "5"]),
		("v NOT IN (1, NULL)", &[]),
		("v IN (2, NULL)", &["3"]),
		("NOT (v IN (1, NULL))", &[]),
		("v BETWEEN 1 AND 2", &["2", "3"]),
		("v NOT BETWEEN 1 AND 2", &["5"]),
		("NOT (v BETWEEN NULL AND 0)", &["2", "3", "5"]),
		("NOT flag", &["3", "5"]),
		("flag OR v = 1", &["2"]),
		("NOT (flag AND v = NULL)", &["3", "5"]),
		("NOT (flag OR v = NULL)", &[]),
		("v = NULL OR true", &["1", "2", "3", "4", "5"]),
		("NOT (v = NULL AND false)", &["1", "2", "3", "4", "5"]),
	];
	for &(predicate, expected) in cases {
		assert_sql_ids(
			&mut stream,
			&format!("UPDATE null_pred SET touched = true WHERE {predicate} RETURNING id"),
			expected,
		)
		.await;
		assert_sql_ids(
			&mut stream,
			&format!("SELECT id FROM null_pred WHERE {predicate}"),
			expected,
		)
		.await;
		let (_, _, error) = run_simple_query_with_err(&mut stream, "BEGIN").await;
		assert!(error.is_none(), "{error:?}");
		assert_sql_ids(
			&mut stream,
			&format!("DELETE FROM null_pred WHERE {predicate} RETURNING id"),
			expected,
		)
		.await;
		let (_, _, error) = run_simple_query_with_err(&mut stream, "ROLLBACK").await;
		assert!(error.is_none(), "{error:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_quoted_names_preserve_case_through_crud_and_inference() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		r#"CREATE TABLE "User" (id int, "ID" int, "createdAt" text, createdat text)"#,
		r#"INSERT INTO "User" (id, "ID", "createdAt", createdat) VALUES (1, 99, 'upper', 'lower')"#,
		r#"CREATE TABLE "user" (id int)"#,
		r#"INSERT INTO "user" (id) VALUES (7)"#,
	] {
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(error.is_none(), "{sql}: {error:?}");
	}
	let result = describe_simple(
		&mut stream,
		r#"SELECT "U".id, "U"."ID", "U"."createdAt" AS "createdAt", "U".CREATEDAT FROM "User" "U""#,
	)
	.await;
	assert_eq!(
		result.columns,
		vec![
			("id".into(), 23),
			("ID".into(), 23),
			("createdAt".into(), 25),
			("createdat".into(), 25)
		]
	);
	assert_eq!(result.cell(0, "id").as_deref(), Some("1"));
	assert_eq!(result.cell(0, "ID").as_deref(), Some("99"));
	assert_eq!(result.cell(0, "createdAt").as_deref(), Some("upper"));
	assert_eq!(result.cell(0, "createdat").as_deref(), Some("lower"));
	let (rows, last) = run_extended_query(
		&mut stream,
		r#"SELECT "U"."createdAt" FROM "User" "U" WHERE "U".id = $1"#,
		&[],
		&[Some(b"1")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), vec![Some("upper".into())]);
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, r#"SELECT u.id FROM "User" "U""#).await;
	assert_eq!(error.expect("case-sensitive alias").0, "42P01");
	let result = describe_simple(
		&mut stream,
		r#"UPDATE "User" SET "createdAt" = 'updated' WHERE "ID" = 99 RETURNING "createdAt""#,
	)
	.await;
	assert_eq!(result.cell(0, "createdAt").as_deref(), Some("updated"));
	let result = describe_simple(
		&mut stream,
		r#"WITH "Recent" AS (SELECT "createdAt" FROM "User") SELECT "createdAt" FROM "Recent""#,
	)
	.await;
	assert_eq!(result.cell(0, "createdAt").as_deref(), Some("updated"));
	assert_sql_ids(&mut stream, r#"DELETE FROM "User" WHERE id = 1 RETURNING id"#, &["1"]).await;
	assert_sql_ids(&mut stream, r#"SELECT id FROM "user""#, &["7"]).await;
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_nullable_link_parameters_keep_sql_unknown() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		&[
			"DEFINE FIELD dept ON nullable_links TYPE record<dept> | null | none",
			"CREATE nullable_links:1 SET dept = NULL RETURN NONE",
			"CREATE nullable_links:2 SET dept = dept:1 RETURN NONE",
			"CREATE nullable_links:3 SET dept = dept:2 RETURN NONE",
			"CREATE nullable_links:4 RETURN NONE",
		]
		.join("; "),
	)
	.await;
	for (predicate, param, expected) in [
		("dept = $1", None, vec![]),
		("dept <> $1", None, vec![]),
		("NOT (dept = $1)", None, vec![]),
		("dept IN ($1)", None, vec![]),
		("dept NOT IN ($1)", None, vec![]),
		("dept = $1", Some(b"".as_slice()), vec![]),
		("dept <> $1", Some(b"".as_slice()), vec!["2", "3"]),
		("dept NOT IN ($1)", Some(b"".as_slice()), vec!["2", "3"]),
	] {
		for sql in [
			format!("SELECT id FROM nullable_links WHERE {predicate}"),
			format!("UPDATE nullable_links SET touched = true WHERE {predicate} RETURNING id"),
		] {
			let (rows, last) = run_extended_query(&mut stream, &sql, &[25], &[param]).await;
			assert_eq!(last, b'C', "{sql}");
			let mut ids: Vec<_> =
				rows.iter().map(|row| decode_data_row(row)[0].clone().unwrap()).collect();
			ids.sort();
			assert_eq!(ids, expected, "{sql}: {param:?}");
		}
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_null_membership_in_an_empty_subquery_is_false() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		"INSERT INTO empty_membership (id, v) VALUES (1, NULL), (2, 2)",
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	assert_sql_ids(
		&mut stream,
		"SELECT id FROM empty_membership WHERE v NOT IN (SELECT v FROM empty_membership WHERE id = 99)",
		&["1", "2"],
	)
	.await;
	assert_sql_ids(
		&mut stream,
		"SELECT id FROM empty_membership WHERE v IN (SELECT v FROM empty_membership WHERE id = 99)",
		&[],
	)
	.await;
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_null_write_values_and_returning_preserve_unknown() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		"INSERT INTO null_values (id, v) VALUES (1, NULL), (2, 1), (3, 2)",
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	let result = describe_simple(
		&mut stream,
		"UPDATE null_values SET comparison = v <> 1 RETURNING id, comparison",
	)
	.await;
	for (row, comparison) in [(0, None), (1, Some("f")), (2, Some("t"))] {
		assert_eq!(result.cell(row, "comparison").as_deref(), comparison);
	}
	let result =
		describe_simple(&mut stream, "DELETE FROM null_values RETURNING id, v <> 1 AS comparison")
			.await;
	for (row, comparison) in [(0, None), (1, Some("f")), (2, Some("t"))] {
		assert_eq!(result.cell(row, "comparison").as_deref(), comparison);
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_quoted_schema_and_qualified_write_fields() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		r#"CREATE TABLE "Audit"."Log" (id int, "entryValue" text, other text)"#,
		r#"INSERT INTO "Audit"."Log" (id, "entryValue", other) VALUES (1, 'first', 'second')"#,
		r#"UPDATE "Audit"."Log" SET "entryValue" = "Audit"."Log".other WHERE "Audit"."Log".id = 1"#,
	] {
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(error.is_none(), "{sql}: {error:?}");
	}
	let result =
		describe_simple(&mut stream, r#"SELECT "Audit"."Log"."entryValue" FROM "Audit"."Log""#)
			.await;
	assert_eq!(result.cell(0, "entryValue").as_deref(), Some("second"));
	shutdown.cancel();
}

async fn describe_write_statement(
	stream: &mut TcpStream,
	name: &str,
	sql: &str,
	declared: &[i32],
) -> (Vec<i32>, Option<Vec<(String, i32)>>) {
	for packet in [build_parse(name, sql, declared), build_desc(b'S', name), build_sync()] {
		stream.write_all(&packet).await.expect("send statement description");
	}
	let messages = read_until_ready(stream).await;
	assert!(!messages.iter().any(|(tag, _)| *tag == b'E'), "{sql}: {messages:?}");
	let body = &messages.iter().find(|(tag, _)| *tag == b't').expect("parameter description").1;
	let mut cursor = body.as_slice();
	let count = cursor.get_i16();
	let parameters = (0..count).map(|_| cursor.get_i32()).collect();
	let columns =
		messages.iter().find(|(tag, _)| *tag == b'T').map(|(_, body)| parse_row_description(body));
	(parameters, columns)
}

#[tokio::test]
async fn test_pgwire_write_parameters_use_declared_scalar_types() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		r#"CREATE TABLE "WriteTypes" (id int, age int, enabled boolean, name text)"#,
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	let (parameters, columns) = describe_write_statement(
		&mut stream,
		"typed_insert",
		r#"INSERT INTO "WriteTypes" (id, age, enabled, name) VALUES ($1, $2, $3, $4)"#,
		&[],
	)
	.await;
	assert_eq!(parameters, vec![23, 23, 16, 25]);
	assert_eq!(columns, None);
	let (rows, last) = run_extended_query(&mut stream,
        r#"INSERT INTO "WriteTypes" (id, age, enabled, name) VALUES ($1, $2, $3, $4) RETURNING name, id, enabled, age"#,
        &[], &[Some(b"1"), Some(b"42"), Some(b"true"), Some(b"Ada")]).await;
	assert_eq!(last, b'C');
	assert_eq!(
		decode_data_row(&rows[0]),
		vec![Some("Ada".into()), Some("1".into()), Some("t".into()), Some("42".into())]
	);
	let (parameters, _) = describe_write_statement(
		&mut stream,
		"typed_update",
		r#"UPDATE "WriteTypes" SET age = $2, enabled = $3 WHERE id = $1 RETURNING name, id"#,
		&[],
	)
	.await;
	assert_eq!(parameters, vec![23, 23, 16]);
	let (parameters, _) = describe_write_statement(
		&mut stream,
		"typed_delete",
		r#"DELETE FROM "WriteTypes" WHERE id = $2 AND $1 IS NULL RETURNING id"#,
		&[],
	)
	.await;
	assert_eq!(parameters, vec![25, 23]);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_unspecified_array_parameters_use_declared_array_types() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		r#"CREATE TABLE "ArrayTypes" (id int, tags text[], scores int[])"#,
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	let (parameters, _) = describe_write_statement(
		&mut stream,
		"array_insert",
		r#"INSERT INTO "ArrayTypes" (id, tags, scores) VALUES ($1, $2, $3)"#,
		&[],
	)
	.await;
	assert_eq!(parameters, vec![23, 1009, 1007]);
	let (rows, last) = run_extended_query(
		&mut stream,
		r#"INSERT INTO "ArrayTypes" (id, tags, scores) VALUES ($1, $2, $3) RETURNING tags, scores"#,
		&[],
		&[Some(b"1"), Some(br#"{"NULL","a b",""}"#), Some(b"{1, -2}")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(
		decode_data_row(&rows[0]),
		vec![Some(r#"{"NULL","a b",""}"#.into()), Some("{1,-2}".into())]
	);
	let (rows, last) = run_extended_query(
		&mut stream,
		r#"UPDATE "ArrayTypes" SET tags = $2 WHERE id = $1 RETURNING tags"#,
		&[],
		&[Some(b"1"), Some(b"{x,y}")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), vec![Some("{x,y}".into())]);
	let (rows, last) = run_extended_query(
		&mut stream,
		r#"SELECT id FROM "ArrayTypes" WHERE tags = $1"#,
		&[],
		&[Some(b"{x,y}")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(
		rows.iter().map(|row| decode_data_row(row)).collect::<Vec<_>>(),
		vec![vec![Some("1".into())]]
	);
	let (_, last) = run_extended_query(
		&mut stream,
		r#"UPDATE "ArrayTypes" SET tags = $1"#,
		&[],
		&[Some(b"{x,,y}")],
	)
	.await;
	assert_eq!(last, b'E', "a malformed array literal is refused");
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_jsonb_columns_keep_nested_keys() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		r#"CREATE TABLE "Docs" (id int PRIMARY KEY, payload jsonb NOT NULL, extra json)"#,
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	let (rows, last) = run_extended_query(
		&mut stream,
		r#"INSERT INTO "Docs" (id, payload, extra) VALUES ($1, $2, $3) RETURNING payload, extra"#,
		&[],
		&[Some(b"1"), Some(br#"{"nested":{"ok":true},"n":3}"#), Some(br#"{"k":[1,{"x":"y"}]}"#)],
	)
	.await;
	assert_eq!(last, b'C', "{rows:?}");
	let row = decode_data_row(&rows[0]);
	let json = |cell: &Option<String>| -> serde_json::Value {
		serde_json::from_str(cell.as_deref().expect("a value")).expect("JSON text")
	};
	assert_eq!(json(&row[0]), serde_json::json!({"nested": {"ok": true}, "n": 3}));
	assert_eq!(json(&row[1]), serde_json::json!({"k": [1, {"x": "y"}]}));
	let (rows, last) = run_extended_query(
		&mut stream,
		r#"SELECT payload FROM "Docs" WHERE id = $1"#,
		&[],
		&[Some(b"1")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(
		json(&decode_data_row(&rows[0])[0]),
		serde_json::json!({"nested": {"ok": true}, "n": 3})
	);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_serial_ids_are_generated_in_order() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		r#"CREATE TABLE "Gen" (id serial PRIMARY KEY, name text NOT NULL, seq bigserial)"#,
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	let mut returned = Vec::new();
	for sql in [
		r#"INSERT INTO "Gen" (name) VALUES ($1) RETURNING id, name, seq"#,
		r#"INSERT INTO "Gen" (id, name, seq) VALUES (DEFAULT, $1, DEFAULT) RETURNING id, name, seq"#,
	] {
		let (rows, last) = run_extended_query(&mut stream, sql, &[], &[Some(b"a")]).await;
		assert_eq!(last, b'C', "{sql}: {rows:?}");
		returned.push(decode_data_row(&rows[0]));
	}
	assert_eq!(
		returned,
		vec![
			vec![Some("1".into()), Some("a".into()), Some("1".into())],
			vec![Some("2".into()), Some("a".into()), Some("2".into())],
		]
	);
	let (rows, last) = run_simple_query(
		&mut stream,
		r#"INSERT INTO "Gen" (id, name) VALUES (10, 'explicit') RETURNING id, seq"#,
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), vec![Some("10".into()), Some("3".into())]);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_namespace_option_selects_the_namespace_of_a_bare_database() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake_with_auth(
		addr,
		"root",
		"prisma",
		None,
		Some("-c namespace=orm -c dialect=pgsql"),
	)
	.await
	.expect("handshake");
	let (rows, last) = run_simple_query(&mut stream, "SELECT current_database() AS db;").await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), vec![Some("prisma".into())]);
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, "CREATE TABLE ns_option (id int PRIMARY KEY)").await;
	assert!(error.is_none(), "a database is selected: {error:?}");

	let refused = connect_and_handshake_with_auth(
		addr,
		"root",
		"other/prisma",
		None,
		Some("-c namespace=orm -c dialect=pgsql"),
	)
	.await;
	assert_eq!(refused.err().map(|(code, _)| code).as_deref(), Some("22023"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_returning_metadata_preserves_order_without_executing() {
	let ds = Datastore::new("memory").await.unwrap();
	let (addr, shutdown) = start_test_pg_server_with_ds(Arc::clone(&ds)).await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		"CREATE TABLE returning_order (id int, age int, enabled boolean, name text)",
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	let sql = "INSERT INTO returning_order (id, age, enabled, name) VALUES (1, 42, true, 'Ada') RETURNING name, id, enabled, age";
	let (_, columns) = describe_write_statement(&mut stream, "ordered_insert", sql, &[]).await;
	assert_eq!(
		columns,
		Some(vec![
			("name".into(), 25),
			("id".into(), 23),
			("enabled".into(), 16),
			("age".into(), 23)
		])
	);
	assert_eq!(
		stored_count(&ds, "returning_order").await,
		0,
		"Parse/Describe must not execute the write"
	);
	let (_, _, error) = run_simple_query_with_err(&mut stream, "BEGIN").await;
	assert!(error.is_none(), "{error:?}");
	for packet in [
		build_bind("ordered_portal", "ordered_insert", &[], &[]),
		build_desc(b'P', "ordered_portal"),
		build_sync(),
	] {
		stream.write_all(&packet).await.unwrap();
	}
	let messages = read_until_ready(&mut stream).await;
	assert!(messages.iter().any(|(tag, _)| *tag == b'T'));
	assert_eq!(
		stored_count(&ds, "returning_order").await,
		0,
		"Describe(portal) must not execute a static RETURNING"
	);
	for packet in [build_exec("ordered_portal", 0), build_sync()] {
		stream.write_all(&packet).await.unwrap();
	}
	let messages = read_until_ready(&mut stream).await;
	let row = &messages
		.iter()
		.find(|(tag, _)| *tag == b'D')
		.unwrap_or_else(|| panic!("returned row: {messages:?}"))
		.1;
	assert_eq!(
		decode_data_row(row),
		vec![Some("Ada".into()), Some("1".into()), Some("t".into()), Some("42".into())]
	);
	let (_, _, error) = run_simple_query_with_err(&mut stream, "COMMIT").await;
	assert!(error.is_none(), "{error:?}");
	let empty = describe_simple(
		&mut stream,
		"UPDATE returning_order SET age = 50 WHERE id = 99 RETURNING name, id, enabled, age",
	)
	.await;
	assert_eq!(empty.columns, columns.unwrap());
	assert!(empty.rows.is_empty());
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		"INSERT INTO returning_order (id, age) VALUES (2, 20) RETURNING id, age AS id",
	)
	.await;
	assert_eq!(error.expect("duplicate labels rejected").0, "0A000");
	assert_eq!(stored_count(&ds, "returning_order").await, 1);
	let expression = describe_simple(
		&mut stream,
		"UPDATE returning_order SET age = 43 RETURNING age + 1, name AS person",
	)
	.await;
	assert_eq!(expression.columns, [("?column?".into(), 23), ("person".into(), 25)]);
	assert_eq!(expression.cell(0, "?column?").as_deref(), Some("44"));
	assert_eq!(expression.cell(0, "person").as_deref(), Some("Ada"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_write_binary_types_casts_and_command_tags() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in ["BEGIN", "CREATE TABLE binary_writes (id int, age int, active boolean, name text)"]
	{
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(error.is_none(), "{sql}: {error:?}");
	}
	let (parameters, columns) = describe_write_statement(
		&mut stream,
		"explicit_write",
		"INSERT INTO binary_writes (id) VALUES ($1::int4) RETURNING $1 AS input, id",
		&[25],
	)
	.await;
	assert_eq!(parameters, [25], "client-declared OIDs take precedence");
	assert_eq!(columns, Some(vec![("input".into(), 25), ("id".into(), 23)]));
	let bytes = 7_i32.to_be_bytes();
	let (rows, last) = run_extended_query_with_formats(
		&mut stream,
		"INSERT INTO binary_writes (id, active, name) VALUES ($1, $2, $3) RETURNING name, active, id",
		&[],
		&[1, 1, 0],
		&[Some(&bytes), Some(&[1]), Some(b"Ada")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), [Some("Ada".into()), Some("t".into()), Some("7".into())]);
	let binary = describe_extended(
		&mut stream,
		"UPDATE binary_writes SET active = false WHERE id = 7 RETURNING active, id",
		1,
	)
	.await;
	assert_eq!(binary.columns, [("active".into(), 16), ("id".into(), 23)]);
	assert_eq!(binary.raw(0, "active"), Some([0_u8].as_slice()));
	assert_eq!(binary.raw(0, "id"), Some(bytes.as_slice()));
	let (rows, last) = run_extended_query(
		&mut stream,
		"UPDATE binary_writes SET age = $1::int4 WHERE id = 7 RETURNING age",
		&[25],
		&[Some(b"8")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), [Some("8".into())]);
	for (sql, expected) in [
		("INSERT INTO binary_writes (id) VALUES (9), (10)", "INSERT 0 2"),
		("UPDATE binary_writes SET active = true WHERE id = 9", "UPDATE 1"),
		("DELETE FROM binary_writes WHERE id = 10", "DELETE 1"),
		("DELETE FROM binary_writes WHERE id = 99", "DELETE 0"),
	] {
		let mut packet = BytesMut::new();
		packet.put_u8(b'Q');
		packet.put_i32((sql.len() + 5) as i32);
		packet.put_slice(sql.as_bytes());
		packet.put_u8(0);
		stream.write_all(&packet).await.unwrap();
		let messages = read_until_ready(&mut stream).await;
		assert!(
			!messages.iter().any(|(tag, _)| matches!(tag, b'E' | b'T' | b'D')),
			"{sql}: {messages:?}"
		);
		let tag = &messages.iter().find(|(tag, _)| *tag == b'C').unwrap().1;
		assert_eq!(&tag[..tag.len() - 1], expected.as_bytes());
	}
	let (_, _, error) = run_simple_query_with_err(&mut stream, "ROLLBACK").await;
	assert!(error.is_none(), "{error:?}");
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_joined_writes_reject_without_mutating_either_table() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		"CREATE TABLE write_target (id int, active boolean)",
		"CREATE TABLE write_source (id int, active boolean)",
		"INSERT INTO write_target (id, active) VALUES (1, false)",
		"INSERT INTO write_source (id, active) VALUES (1, false)",
	] {
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(error.is_none(), "{sql}: {error:?}");
	}
	for sql in [
		"UPDATE write_target SET active = true FROM write_source",
		"DELETE FROM write_target USING write_source",
	] {
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(error.expect("joined write must be rejected").0, "0A000", "{sql}");
		for table in ["write_target", "write_source"] {
			let result =
				describe_simple(&mut stream, &format!("SELECT id, active FROM {table}")).await;
			assert_eq!(result.rows.len(), 1, "{sql}: {table}");
			assert_eq!(result.cell(0, "active").as_deref(), Some("f"), "{sql}: {table}");
		}
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_pagination_parameters_infer_integer_types() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, "CREATE TABLE page_values (id int)").await;
	assert!(error.is_none(), "{error:?}");
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, "INSERT INTO page_values (id) VALUES (1), (2), (3)")
			.await;
	assert!(error.is_none(), "{error:?}");
	let (parameters, _) = describe_write_statement(
		&mut stream,
		"page",
		"SELECT id FROM page_values ORDER BY id LIMIT $1 OFFSET $2",
		&[],
	)
	.await;
	assert_eq!(parameters, [20, 20]);
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT id FROM page_values ORDER BY id LIMIT $1 OFFSET $2",
		&[],
		&[Some(b"1"), Some(b"1")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	assert_eq!(decode_data_row(&rows[0]), [Some("2".into())]);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_session_database_and_numeric_server_version() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let result = describe_simple(&mut stream, "SELECT current_database() AS db").await;
	assert_eq!(result.cell(0, "db").as_deref(), Some("test"));
	let result =
		describe_simple(&mut stream, "SELECT current_setting('server_version_num') AS v").await;
	assert_eq!(result.cell(0, "v").as_deref(), Some("160000"));
	let result = describe_simple(&mut stream, "SHOW server_version_num").await;
	assert_eq!(result.cell(0, "server_version_num").as_deref(), Some("160000"));
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, "SET server_version_num = '150000'").await;
	assert_eq!(error.expect("read-only setting").0, "55P02");
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_scalar_functions_in_from() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let result = describe_simple(&mut stream, "SELECT * FROM current_schema()").await;
	assert_eq!(result.cell(0, "current_schema").as_deref(), Some("public"));
	let (parameters, columns) = describe_write_statement(
		&mut stream,
		"function_source",
		"SELECT * FROM abs($1::int4) WITH ORDINALITY AS a(n, ord)",
		&[],
	)
	.await;
	assert_eq!(parameters, [23]);
	assert_eq!(columns, Some(vec![("n".into(), 23), ("ord".into(), 20)]));
	let (rows, last) = run_extended_query(
		&mut stream,
		"SELECT * FROM abs($1::int4) WITH ORDINALITY AS a(n, ord)",
		&[],
		&[Some(b"-2")],
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(decode_data_row(&rows[0]), [Some("2".into()), Some("1".into())]);
	let same = describe_simple(&mut stream, "SELECT r AS a, r AS b FROM random() AS r").await;
	assert_eq!(
		same.cell(0, "a"),
		same.cell(0, "b"),
		"one function result shared by both references"
	);
	for sql in
		["SELECT * FROM generate_series(1, 2)", "SELECT * FROM abs(2) WITH ORDINALITY AS a(n,n)"]
	{
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(error.expect("unsupported function source").0, "0A000", "{sql}");
	}
	shutdown.cancel();
}

const PRISMA_UPSERT: &str = r#"INSERT INTO "public"."CompatUser" ("id","name","age","active") VALUES ($1,$2,$3,$4) ON CONFLICT ("id") DO UPDATE SET "age" = $5 WHERE ("public"."CompatUser"."id" = $6 AND 1=1) RETURNING "public"."CompatUser"."id", "public"."CompatUser"."name", "public"."CompatUser"."age", "public"."CompatUser"."active", "public"."CompatUser"."nickname""#;

/// The rows, the command tag and the error of one extended-protocol execution, with parameters of
/// unspecified type.
async fn run_untyped_extended(
	stream: &mut TcpStream,
	sql: &str,
	params: &[&[u8]],
) -> (Vec<Vec<Option<String>>>, Option<String>, Option<(String, String)>) {
	let params: Vec<Option<&[u8]>> = params.iter().map(|param| Some(*param)).collect();
	for packet in [
		build_parse("", sql, &[]),
		build_bind("", "", &[], &params),
		build_exec("", 0),
		build_sync(),
	] {
		stream.write_all(&packet).await.expect("send statement");
	}
	let mut rows = Vec::new();
	let mut tag = None;
	let mut error = None;
	for (kind, body) in read_until_ready(stream).await {
		match kind {
			b'D' => rows.push(decode_data_row(&body)),
			b'C' => tag = Some(String::from_utf8_lossy(&body[..body.len() - 1]).to_string()),
			b'E' => error = Some(parse_pg_error_fields(&body)),
			_ => {}
		}
	}
	(rows, tag, error)
}

#[tokio::test]
async fn test_pgwire_prisma_upsert_applies_do_update_where() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		r#"CREATE TABLE "CompatUser" (id int, name text, age int, active boolean, nickname text)"#,
	)
	.await;
	assert!(error.is_none(), "{error:?}");

	let (parameters, columns) =
		describe_write_statement(&mut stream, "prisma_upsert", PRISMA_UPSERT, &[]).await;
	assert_eq!(parameters, vec![23, 25, 23, 16, 23, 23]);
	assert_eq!(columns.expect("RETURNING columns").len(), 5);

	// No conflict: the row is inserted and returned, and the DO UPDATE clause is not applied.
	let (rows, tag, error) = run_untyped_extended(
		&mut stream,
		PRISMA_UPSERT,
		&[b"1", b"Ada", b"36", b"true", b"99", b"1"],
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	assert_eq!(tag.as_deref(), Some("INSERT 0 1"));
	assert_eq!(
		rows,
		vec![vec![Some("1".into()), Some("Ada".into()), Some("36".into()), Some("t".into()), None]]
	);

	// Conflict with a true condition: the existing row is updated and returned.
	let (rows, tag, error) = run_untyped_extended(
		&mut stream,
		PRISMA_UPSERT,
		&[b"1", b"Other", b"1", b"false", b"50", b"1"],
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	assert_eq!(tag.as_deref(), Some("INSERT 0 1"));
	assert_eq!(
		rows,
		vec![vec![Some("1".into()), Some("Ada".into()), Some("50".into()), Some("t".into()), None]]
	);

	// Conflict with a false condition: the row is left as it is, and nothing is returned or
	// counted.
	let (rows, tag, error) = run_untyped_extended(
		&mut stream,
		PRISMA_UPSERT,
		&[b"1", b"Other", b"1", b"false", b"77", b"2"],
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	assert!(rows.is_empty(), "{rows:?}");
	assert_eq!(tag.as_deref(), Some("INSERT 0 0"));
	let result = describe_simple(&mut stream, r#"SELECT age, name FROM "CompatUser""#).await;
	assert_eq!(result.cell(0, "age").as_deref(), Some("50"));
	assert_eq!(result.cell(0, "name").as_deref(), Some("Ada"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_on_conflict_where_reads_the_existing_and_proposed_rows() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		r#"CREATE TABLE "Counter" (id int, hits int)"#,
		r#"INSERT INTO "Counter" (id, hits) VALUES (1, 5)"#,
	] {
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert!(error.is_none(), "{sql}: {error:?}");
	}
	let sql = r#"INSERT INTO "Counter" AS c (id, hits) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET hits = c.hits + EXCLUDED.hits WHERE c.hits < EXCLUDED.hits + $3 RETURNING hits"#;
	// 5 < 3 + 4 holds: the existing hits grow by the proposed ones.
	let (rows, tag, error) = run_untyped_extended(&mut stream, sql, &[b"1", b"3", b"4"]).await;
	assert!(error.is_none(), "{error:?}");
	assert_eq!(tag.as_deref(), Some("INSERT 0 1"));
	assert_eq!(rows, vec![vec![Some("8".into())]]);
	// 8 < 3 + 4 does not hold.
	let (rows, tag, error) = run_untyped_extended(&mut stream, sql, &[b"1", b"3", b"4"]).await;
	assert!(error.is_none(), "{error:?}");
	assert!(rows.is_empty(), "{rows:?}");
	assert_eq!(tag.as_deref(), Some("INSERT 0 0"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_on_conflict_target_predicate_is_not_supported() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, r#"CREATE TABLE "Partial" (id int, name text)"#)
			.await;
	assert!(error.is_none(), "{error:?}");
	for sql in [
		r#"INSERT INTO "Partial" (id, name) VALUES ($1, $2) ON CONFLICT (id) WHERE id > 0 DO NOTHING"#,
		r#"INSERT INTO "Partial" (id, name) VALUES ($1, $2) ON CONFLICT (id) WHERE id > 0 DO UPDATE SET name = $2"#,
	] {
		let (_, _, error) = run_untyped_extended(&mut stream, sql, &[b"1", b"a"]).await;
		assert_eq!(error.expect("unsupported conflict target").0, "0A000", "{sql}");
	}
	shutdown.cancel();
}

async fn catalog_rows(stream: &mut TcpStream, sql: &str) -> Vec<Vec<Option<String>>> {
	let (rows, _, error) = run_simple_query_with_err(stream, sql).await;
	assert!(error.is_none(), "{sql}: {error:?}");
	rows.iter().map(|row| decode_data_row(row)).collect()
}

fn strings(rows: &[Vec<Option<String>>]) -> Vec<Vec<&str>> {
	rows.iter()
		.map(|row| row.iter().map(|col| col.as_deref().unwrap_or("NULL")).collect())
		.collect()
}

async fn create_compat_tables(stream: &mut TcpStream) {
	for sql in [
		r#"CREATE TABLE "CompatUser" (id serial PRIMARY KEY, name text NOT NULL, tags text[], payload jsonb)"#,
		r#"CREATE TABLE "CompatPost" (id serial PRIMARY KEY, title text)"#,
	] {
		let (_, _, error) = run_simple_query_with_err(stream, sql).await;
		assert!(error.is_none(), "{sql}: {error:?}");
	}
}

#[tokio::test]
async fn test_pgwire_information_schema_lists_the_users_tables_and_columns() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	create_compat_tables(&mut stream).await;

	let rows = catalog_rows(
		&mut stream,
		"SELECT table_schema, table_name FROM information_schema.tables \
		 WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY table_name",
	)
	.await;
	assert_eq!(strings(&rows), [["public", "CompatPost"], ["public", "CompatUser"]]);

	let rows = catalog_rows(
		&mut stream,
		"SELECT column_name, ordinal_position, data_type, is_nullable, udt_name, udt_schema \
		 FROM information_schema.columns WHERE table_name = 'CompatUser' \
		 ORDER BY ordinal_position",
	)
	.await;
	let columns = strings(&rows);
	assert_eq!(columns[0][..4], ["id", "1", "integer", "NO"]);
	assert_eq!(columns.len(), 4, "{columns:?}");
	let find = |name: &str| columns.iter().find(|column| column[0] == name).expect(name).clone();
	assert_eq!(find("name")[2..], ["text", "NO", "text", "pg_catalog"]);
	assert_eq!(find("tags")[2..], ["ARRAY", "YES", "_text", "pg_catalog"]);
	assert_eq!(find("payload")[2..], ["jsonb", "YES", "jsonb", "pg_catalog"]);
	let mut ordinals: Vec<_> = columns.iter().map(|column| column[1]).collect();
	ordinals.dedup();
	assert_eq!(ordinals, ["1", "2", "3", "4"]);

	let rows = catalog_rows(
		&mut stream,
		"SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE 'Compat%' ORDER BY relname",
	)
	.await;
	// The primary key of each table is an index relation of its own, and the sequence behind
	// each serial key is a relation too, as in PostgreSQL.
	assert_eq!(
		strings(&rows),
		[
			["CompatPost"],
			["CompatPost_id_seq"],
			["CompatPost_pkey"],
			["CompatUser"],
			["CompatUser_id_seq"],
			["CompatUser_pkey"]
		]
	);
	let rows = catalog_rows(
		&mut stream,
		"SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE 'Compat%' AND relkind = 'r' \
		 ORDER BY relname",
	)
	.await;
	assert_eq!(strings(&rows), [["CompatPost"], ["CompatUser"]]);

	let rows = catalog_rows(
		&mut stream,
		"SELECT a.attname FROM pg_catalog.pg_attribute a \
		 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid WHERE c.relname = 'CompatPost' \
		 ORDER BY a.attnum",
	)
	.await;
	assert_eq!(strings(&rows), [["id"], ["title"]]);

	// The statement TypeORM issues to find the tables it manages.
	let rows = catalog_rows(
		&mut stream,
		r#"SELECT "table_schema", "table_name", obj_description((quote_ident("table_schema") || '.' || quote_ident("table_name"))::regclass, 'pg_class') AS table_comment FROM "information_schema"."tables" WHERE ("table_schema" = 'public' AND "table_name" = 'CompatUser') OR ("table_schema" = 'public' AND "table_name" = 'CompatPost')"#,
	)
	.await;
	assert_eq!(rows.len(), 2, "{rows:?}");

	// The user's own tables stay readable alongside the views.
	let (_, _, error) =
		run_simple_query_with_err(&mut stream, r#"SELECT name FROM "CompatUser""#).await;
	assert!(error.is_none(), "{error:?}");
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_information_schema_lists_a_table_created_in_the_open_transaction() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let listing = "SELECT table_name FROM information_schema.tables WHERE table_name = 'InTx'";

	let (_, last) = run_simple_query(&mut stream, "BEGIN;").await;
	assert_eq!(last, b'C');
	let (_, _, error) = run_simple_query_with_err(
		&mut stream,
		r#"CREATE TABLE "InTx" (id serial PRIMARY KEY, v text)"#,
	)
	.await;
	assert!(error.is_none(), "{error:?}");
	assert_eq!(strings(&catalog_rows(&mut stream, listing).await), [["InTx"]]);
	let (_, last) = run_simple_query(&mut stream, "ROLLBACK;").await;
	assert_eq!(last, b'C');
	assert!(catalog_rows(&mut stream, listing).await.is_empty());
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_typeorm_column_introspection_query() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	for ddl in [
		"CREATE TABLE \"CompatUser\" (id integer PRIMARY KEY, name text NOT NULL, age integer, active boolean NOT NULL, nickname text)",
		"CREATE TABLE \"CompatPost\" (id integer PRIMARY KEY, title text NOT NULL, \"authorId\" integer NOT NULL)",
	] {
		let (_, last) = run_simple_query(&mut stream, ddl).await;
		assert_eq!(last, b'C', "{ddl}");
	}

	let sql = "SELECT columns.*, pg_catalog.col_description((quote_ident(table_catalog) || '.' || quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass::oid, ordinal_position) AS description, ('\"' || \"udt_schema\" || '\".\"' || \"udt_name\" || '\"')::\"regtype\"::text AS \"regtype\", pg_catalog.format_type(\"col_attr\".\"atttypid\", \"col_attr\".\"atttypmod\") AS \"format_type\" FROM \"information_schema\".\"columns\" LEFT JOIN \"pg_catalog\".\"pg_attribute\" AS \"col_attr\" ON \"col_attr\".\"attname\" = \"columns\".\"column_name\" AND \"col_attr\".\"attrelid\" = ( SELECT \"cls\".\"oid\" FROM \"pg_catalog\".\"pg_class\" AS \"cls\" LEFT JOIN \"pg_catalog\".\"pg_namespace\" AS \"ns\" ON \"ns\".\"oid\" = \"cls\".\"relnamespace\" WHERE \"cls\".\"relname\" = \"columns\".\"table_name\" AND \"ns\".\"nspname\" = \"columns\".\"table_schema\" ) WHERE (\"table_schema\" = 'public' AND \"table_name\" = 'CompatPost') OR (\"table_schema\" = 'public' AND \"table_name\" = 'CompatUser')";
	let (rows, last) = run_simple_query(&mut stream, sql).await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 8, "one row per column of the two tables");

	// Positions in `columns.*`: table_name 2, column_name 3, is_nullable 6, data_type 7,
	// udt_name 27; then description, regtype and format_type close the row.
	let mut found: Vec<[String; 7]> = rows
		.iter()
		.map(|row| {
			let cells = decode_data_row(row);
			let n = cells.len();
			let cell = |i: usize| cells[i].clone().unwrap_or_default();
			assert_eq!(cells[n - 3], None, "col_description is NULL");
			[cell(2), cell(3), cell(6), cell(27), cell(n - 2), cell(n - 1), cell(7)]
		})
		.collect();
	found.sort();
	let expect = |table: &str, column: &str, nullable: &str, udt: &str, regtype: &str| {
		[
			table.to_string(),
			column.to_string(),
			nullable.to_string(),
			udt.to_string(),
			regtype.to_string(),
			regtype.to_string(),
		]
	};
	let expected = [
		expect("CompatPost", "authorId", "NO", "int4", "integer"),
		expect("CompatPost", "id", "NO", "int4", "integer"),
		expect("CompatPost", "title", "NO", "text", "text"),
		expect("CompatUser", "active", "NO", "bool", "boolean"),
		expect("CompatUser", "age", "YES", "int4", "integer"),
		expect("CompatUser", "id", "NO", "int4", "integer"),
		expect("CompatUser", "name", "NO", "text", "text"),
		expect("CompatUser", "nickname", "YES", "text", "text"),
	];
	assert_eq!(found.len(), expected.len());
	for (got, want) in found.iter().zip(expected.iter()) {
		assert_eq!(&got[..6], &want[..], "{got:?}");
	}

	shutdown.cancel();
}

/// TypeORM's constraint query, restricted to the tables `tables` names.
fn typeorm_constraints_query(tables: &[&str]) -> String {
	let filter = tables
		.iter()
		.map(|t| format!("(\"ns\".\"nspname\" = 'public' AND \"t\".\"relname\" = '{t}')"))
		.collect::<Vec<_>>()
		.join(" OR ");
	format!(
		"SELECT \"ns\".\"nspname\" AS \"table_schema\", \"t\".\"relname\" AS \"table_name\", \
		 \"cnst\".\"conname\" AS \"constraint_name\", pg_get_constraintdef(\"cnst\".\"oid\") AS \
		 \"expression\", CASE \"cnst\".\"contype\" WHEN 'p' THEN 'PRIMARY' WHEN 'u' THEN \
		 'UNIQUE' WHEN 'c' THEN 'CHECK' WHEN 'x' THEN 'EXCLUDE' END AS \"constraint_type\", \
		 \"a\".\"attname\" AS \"column_name\" FROM \"pg_constraint\" \"cnst\" INNER JOIN \
		 \"pg_class\" \"t\" ON \"t\".\"oid\" = \"cnst\".\"conrelid\" INNER JOIN \
		 \"pg_namespace\" \"ns\" ON \"ns\".\"oid\" = \"cnst\".\"connamespace\" LEFT JOIN \
		 \"pg_attribute\" \"a\" ON \"a\".\"attrelid\" = \"cnst\".\"conrelid\" AND \
		 \"a\".\"attnum\" = ANY (\"cnst\".\"conkey\") WHERE \"t\".\"relkind\" IN ('r', 'p') \
		 AND ({filter}) ORDER BY 2, 3"
	)
}

/// TypeORM's foreign key query, restricted to the tables `tables` names.
fn typeorm_foreign_keys_query(tables: &[&str]) -> String {
	let filter = tables
		.iter()
		.map(|t| format!("(\"ns\".\"nspname\" = 'public' AND \"cl\".\"relname\" = '{t}')"))
		.collect::<Vec<_>>()
		.join(" OR ");
	format!(
		"SELECT \"con\".\"conname\" AS \"constraint_name\", \"con\".\"nspname\" AS \
		 \"table_schema\", \"con\".\"relname\" AS \"table_name\", \"att2\".\"attname\" AS \
		 \"column_name\", \"ns\".\"nspname\" AS \"referenced_table_schema\", \"cl\".\"relname\" \
		 AS \"referenced_table_name\", \"att\".\"attname\" AS \"referenced_column_name\", \
		 \"con\".\"confdeltype\" AS \"on_delete\", \"con\".\"confupdtype\" AS \"on_update\", \
		 \"con\".\"condeferrable\" AS \"deferrable\", \"con\".\"condeferred\" AS \"deferred\" \
		 FROM ( SELECT UNNEST (\"con1\".\"conkey\") AS \"parent\", UNNEST (\"con1\".\"confkey\") \
		 AS \"child\", \"con1\".\"confrelid\", \"con1\".\"conrelid\", \"con1\".\"conname\", \
		 \"con1\".\"contype\", \"ns\".\"nspname\", \"cl\".\"relname\", \
		 \"con1\".\"condeferrable\", CASE WHEN \"con1\".\"condeferred\" THEN 'INITIALLY \
		 DEFERRED' ELSE 'INITIALLY IMMEDIATE' END as condeferred, CASE \"con1\".\"confdeltype\" \
		 WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' THEN 'CASCADE' WHEN 'n' \
		 THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END as \"confdeltype\", CASE \
		 \"con1\".\"confupdtype\" WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT' WHEN 'c' \
		 THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'd' THEN 'SET DEFAULT' END as \
		 \"confupdtype\" FROM \"pg_class\" \"cl\" INNER JOIN \"pg_namespace\" \"ns\" ON \
		 \"cl\".\"relnamespace\" = \"ns\".\"oid\" INNER JOIN \"pg_constraint\" \"con1\" ON \
		 \"con1\".\"conrelid\" = \"cl\".\"oid\" WHERE \"con1\".\"contype\" = 'f' AND ({filter}) ) \
		 \"con\" INNER JOIN \"pg_attribute\" \"att\" ON \"att\".\"attrelid\" = \
		 \"con\".\"confrelid\" AND \"att\".\"attnum\" = \"con\".\"child\" INNER JOIN \
		 \"pg_class\" \"cl\" ON \"cl\".\"oid\" = \"con\".\"confrelid\" AND \
		 \"cl\".\"relispartition\" = 'f' INNER JOIN \"pg_namespace\" \"ns\" ON \
		 \"cl\".\"relnamespace\" = \"ns\".\"oid\" INNER JOIN \"pg_attribute\" \"att2\" ON \
		 \"att2\".\"attrelid\" = \"con\".\"conrelid\" AND \"att2\".\"attnum\" = \"con\".\"parent\""
	)
}

/// The rows of `sql` as text, `NULL` for an absent value.
async fn text_rows(stream: &mut TcpStream, sql: &str) -> Vec<Vec<String>> {
	let (rows, last, err) = run_simple_query_with_err(stream, sql).await;
	assert!(err.is_none(), "{sql}: {err:?}");
	assert_eq!(last, b'C', "{sql}");
	rows.iter()
		.map(|row| {
			decode_data_row(row)
				.into_iter()
				.map(|value| value.unwrap_or_else(|| "NULL".to_owned()))
				.collect()
		})
		.collect()
}

#[tokio::test]
async fn test_pgwire_typeorm_constraint_and_foreign_key_catalog_queries() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for ddl in [
		r#"CREATE TABLE "FkUser" (id integer PRIMARY KEY, name text NOT NULL, email text UNIQUE)"#,
		r#"CREATE TABLE "FkPost" (id integer PRIMARY KEY, title text NOT NULL, "authorId" integer NOT NULL)"#,
	] {
		let (_, last) = run_simple_query(&mut stream, ddl).await;
		assert_eq!(last, b'C', "{ddl}");
	}
	// The column holds record links to the other table, which SurrealQL declares as a reference.
	seed_surrealql(
		addr,
		"DEFINE FIELD OVERWRITE authorId ON FkPost TYPE record<FkUser> REFERENCE ON DELETE CASCADE",
	)
	.await;

	let constraints =
		text_rows(&mut stream, &typeorm_constraints_query(&["FkPost", "FkUser"])).await;
	let row = |table: &str, name: &str, expression: &str, kind: &str, column: &str| {
		vec![
			"public".to_owned(),
			table.to_owned(),
			name.to_owned(),
			expression.to_owned(),
			kind.to_owned(),
			column.to_owned(),
		]
	};
	assert_eq!(
		constraints,
		vec![
			row(
				"FkPost",
				"FkPost_authorId_fkey",
				"FOREIGN KEY (\"authorId\") REFERENCES \"FkUser\"(id) ON DELETE CASCADE",
				"NULL",
				"authorId"
			),
			row("FkPost", "FkPost_pkey", "PRIMARY KEY (id)", "PRIMARY", "id"),
			row("FkUser", "FkUser_email_uniq", "UNIQUE (email)", "UNIQUE", "email"),
			row("FkUser", "FkUser_pkey", "PRIMARY KEY (id)", "PRIMARY", "id"),
		]
	);

	let keys = text_rows(&mut stream, &typeorm_foreign_keys_query(&["FkPost", "FkUser"])).await;
	assert_eq!(
		keys,
		vec![vec![
			"FkPost_authorId_fkey".to_owned(),
			"public".to_owned(),
			"FkPost".to_owned(),
			"authorId".to_owned(),
			"public".to_owned(),
			"FkUser".to_owned(),
			"id".to_owned(),
			"CASCADE".to_owned(),
			"NO ACTION".to_owned(),
			"f".to_owned(),
			"INITIALLY IMMEDIATE".to_owned(),
		]]
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_information_schema_reports_array_columns_as_array() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	let (_, last) = run_simple_query(
		&mut stream,
		r#"CREATE TABLE "ArrCols" (id integer PRIMARY KEY, tags text[], scores int[], big bigint[], plain text)"#,
	)
	.await;
	assert_eq!(last, b'C');

	let rows = text_rows(
		&mut stream,
		"SELECT column_name, data_type, udt_name, udt_schema FROM information_schema.columns \
		 WHERE table_name = 'ArrCols' AND column_name <> 'id' ORDER BY ordinal_position",
	)
	.await;
	assert_eq!(
		rows,
		vec![
			["big", "ARRAY", "_int8", "pg_catalog"],
			["plain", "text", "text", "pg_catalog"],
			["scores", "ARRAY", "_int4", "pg_catalog"],
			["tags", "ARRAY", "_text", "pg_catalog"],
		]
	);

	// The array type is still reachable through the other catalogs.
	let rows = text_rows(
		&mut stream,
		"SELECT a.attname, a.atttypid::int, pg_catalog.format_type(a.atttypid, a.atttypmod), \
		 a.attndims::int FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
		 WHERE c.relname = 'ArrCols' AND a.attname IN ('tags', 'scores') ORDER BY a.attnum",
	)
	.await;
	assert_eq!(rows, vec![["scores", "1007", "integer[]", "1"], ["tags", "1009", "text[]", "1"]]);

	shutdown.cancel();
}

async fn sqlstate_of(stream: &mut TcpStream, sql: &str) -> Option<(String, String)> {
	let (_, _, err) = run_simple_query_with_err(stream, sql).await;
	err
}

#[tokio::test]
async fn test_pgwire_alter_table_drop_constraint_resolves_the_names_the_catalog_lists() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for ddl in [
		r#"CREATE TABLE "DcUser" (id integer PRIMARY KEY, email text UNIQUE)"#,
		r#"CREATE TABLE "DcPost" (id integer PRIMARY KEY, title text NOT NULL, "authorId" integer NOT NULL)"#,
		r#"CREATE TABLE "DcKeyed" (id integer, a text, b text, PRIMARY KEY (a, b))"#,
	] {
		let (_, last) = run_simple_query(&mut stream, ddl).await;
		assert_eq!(last, b'C', "{ddl}");
	}
	seed_surrealql(
		addr,
		"DEFINE FIELD OVERWRITE authorId ON DcPost TYPE record<DcUser> REFERENCE ON DELETE CASCADE; \
		 CREATE DcUser:1 SET email = 'a@x'; CREATE DcPost:1 SET title = 't', authorId = DcUser:1;",
	)
	.await;

	let names = |rows: Vec<Vec<String>>| rows.into_iter().map(|r| r[0].clone()).collect::<Vec<_>>();
	let constraint_names = |rows: Vec<Vec<String>>| names(rows);
	let all = "SELECT conname FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t \
		 ON t.oid = c.conrelid WHERE t.relname LIKE 'Dc%' ORDER BY conname";
	assert_eq!(
		constraint_names(text_rows(&mut stream, all).await),
		[
			"DcKeyed_pkey".to_owned(),
			"DcPost_authorId_fkey".to_owned(),
			"DcPost_pkey".to_owned(),
			"DcUser_email_uniq".to_owned(),
			"DcUser_pkey".to_owned(),
		]
	);

	// An unknown name is undefined, and `IF EXISTS` makes it a no-op.
	let (code, message) =
		sqlstate_of(&mut stream, r#"ALTER TABLE "DcUser" DROP CONSTRAINT nope"#).await.unwrap();
	assert_eq!(code, "42704");
	assert!(
		message.ends_with(r#"constraint "nope" of relation "DcUser" does not exist"#),
		"{message}"
	);
	assert!(
		sqlstate_of(&mut stream, r#"ALTER TABLE "DcUser" DROP CONSTRAINT IF EXISTS nope"#)
			.await
			.is_none()
	);

	// A table whose primary key is only its record id has no index to drop: the constraint is
	// refused, not ignored.
	seed_surrealql(addr, "DEFINE TABLE DcPlain SCHEMALESS").await;
	let (code, message) =
		sqlstate_of(&mut stream, r#"ALTER TABLE "DcPlain" DROP CONSTRAINT "DcPlain_pkey""#)
			.await
			.unwrap();
	assert_eq!(code, "0A000", "{message}");
	assert!(message.contains("record id"), "{message}");

	// A unique constraint drops its index: a duplicate is then accepted.
	assert!(try_surrealql(addr, "CREATE DcUser:2 SET email = 'a@x'").await.is_err());
	assert!(
		sqlstate_of(&mut stream, r#"ALTER TABLE "DcUser" DROP CONSTRAINT "DcUser_email_uniq""#)
			.await
			.is_none()
	);
	seed_surrealql(addr, "CREATE DcUser:2 SET email = 'a@x'").await;

	// A foreign key drops the reference and keeps the column's type.
	seed_surrealql(addr, "DELETE DcUser:2").await;
	assert!(
		sqlstate_of(&mut stream, r#"ALTER TABLE "DcPost" DROP CONSTRAINT "DcPost_authorId_fkey""#)
			.await
			.is_none()
	);
	let rows = text_rows(
		&mut stream,
		"SELECT conname FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t \
		 ON t.oid = c.conrelid WHERE t.relname = 'DcPost' ORDER BY conname",
	)
	.await;
	assert_eq!(names(rows), ["DcPost_pkey"]);
	assert!(
		try_surrealql(addr, "UPDATE DcPost:1 SET authorId = 5").await.is_err(),
		"the column keeps its record<DcUser> type"
	);
	// With the reference gone, deleting the author no longer cascades.
	seed_surrealql(addr, "DELETE DcUser:1").await;
	let rows = text_rows(&mut stream, r#"SELECT count(*) FROM "DcPost""#).await;
	assert_eq!(rows, vec![["1"]]);

	// A primary key backed by a `_pk` index drops that index.
	assert!(
		sqlstate_of(&mut stream, r#"ALTER TABLE "DcKeyed" DROP CONSTRAINT "DcKeyed_pkey""#)
			.await
			.is_none()
	);

	shutdown.cancel();
}

/// Run each statement of `statements` on `stream`, each of which must succeed.
async fn run_all(stream: &mut TcpStream, statements: &[&str]) {
	for sql in statements {
		let (_, last, err) = run_simple_query_with_err(stream, sql).await;
		assert!(err.is_none(), "{sql}: {err:?}");
		assert_eq!(last, b'C', "{sql}");
	}
}

#[tokio::test]
async fn test_pgwire_alter_table_add_foreign_key_converts_a_scalar_column_and_its_rows() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	run_all(
		&mut stream,
		&[
			r#"CREATE TABLE "AfUser" (id integer PRIMARY KEY, name text)"#,
			r#"CREATE TABLE "AfPost" (id integer PRIMARY KEY, "authorId" integer NOT NULL, "editorId" integer)"#,
			r#"INSERT INTO "AfUser" (id, name) VALUES (1, 'a'), (2, 'b')"#,
			r#"INSERT INTO "AfPost" (id, "authorId", "editorId") VALUES (1, 1, NULL), (2, 2, 1), (3, 99, NULL)"#,
		],
	)
	.await;

	// A row that names no row of the target fails the statement, as PostgreSQL's check does, and
	// leaves the column as it was.
	let add_author = r#"ALTER TABLE "AfPost" ADD CONSTRAINT fk_a FOREIGN KEY ("authorId") REFERENCES "AfUser"(id) ON DELETE CASCADE"#;
	let (code, message) = sqlstate_of(&mut stream, add_author).await.unwrap();
	assert_eq!(code, "23503", "{message}");
	assert!(message.contains(r#"constraint "AfPost_authorId_fkey""#), "{message}");
	run_all(&mut stream, &[r#"INSERT INTO "AfPost" (id, "authorId") VALUES (4, 98)"#]).await;
	run_all(&mut stream, &[r#"DELETE FROM "AfPost" WHERE id IN (3, 4)"#]).await;

	// Both columns become links; the rows keep reading and joining as keys.
	run_all(
		&mut stream,
		&[
			add_author,
			r#"ALTER TABLE "AfPost" ADD FOREIGN KEY ("editorId") REFERENCES "AfUser"(id) ON DELETE RESTRICT"#,
		],
	)
	.await;
	let rows =
		text_rows(&mut stream, r#"SELECT id, "authorId", "editorId" FROM "AfPost" ORDER BY id"#)
			.await;
	assert_eq!(rows, vec![["1", "1", "NULL"], ["2", "2", "1"]]);
	let rows = text_rows(
		&mut stream,
		r#"SELECT p.id, u.name FROM "AfPost" p JOIN "AfUser" u ON p."authorId" = u.id ORDER BY p.id"#,
	)
	.await;
	assert_eq!(rows, vec![["1", "a"], ["2", "b"]]);
	let rows = text_rows(&mut stream, r#"SELECT id FROM "AfPost" WHERE "authorId" = 2"#).await;
	assert_eq!(rows, vec![["2"]]);
	let rows = text_rows(&mut stream, r#"SELECT id FROM "AfPost" WHERE "editorId" IS NULL"#).await;
	assert_eq!(rows, vec![["1"]]);

	// The catalog reports the column with the type of the key it links to, and lists the keys.
	let rows = text_rows(
		&mut stream,
		"SELECT column_name, data_type, is_nullable FROM information_schema.columns \
		 WHERE table_name = 'AfPost' AND column_name <> 'id' ORDER BY ordinal_position",
	)
	.await;
	assert_eq!(rows, vec![["authorId", "integer", "NO"], ["editorId", "integer", "YES"]]);
	let rows = text_rows(
		&mut stream,
		"SELECT conname, confdeltype FROM pg_catalog.pg_constraint \
		 WHERE conname LIKE 'AfPost_%_fkey' ORDER BY conname",
	)
	.await;
	assert_eq!(rows, vec![["AfPost_authorId_fkey", "c"], ["AfPost_editorId_fkey", "a"]]);

	// A write that names no row of the target is a foreign key violation, an insert and an update.
	for sql in [
		r#"INSERT INTO "AfPost" (id, "authorId") VALUES (5, 99)"#,
		r#"UPDATE "AfPost" SET "authorId" = 99 WHERE id = 1"#,
		r#"INSERT INTO "AfPost" (id, "authorId", "editorId") VALUES (5, 1, 99)"#,
	] {
		let (code, message) = sqlstate_of(&mut stream, sql).await.unwrap();
		assert_eq!(code, "23503", "{sql}: {message}");
	}
	run_all(
		&mut stream,
		&[
			r#"INSERT INTO "AfPost" (id, "authorId", "editorId") VALUES (5, 2, 2)"#,
			r#"UPDATE "AfPost" SET "authorId" = 1 WHERE id = 5"#,
		],
	)
	.await;
	let rows =
		text_rows(&mut stream, r#"SELECT id FROM "AfPost" WHERE "authorId" = 1 ORDER BY id"#).await;
	assert_eq!(rows, vec![["1"], ["5"]]);

	// The actions of the keys act on the rows that were there before the conversion: the delete is
	// refused while a row names the user as its editor, and cascades once none does.
	let (code, _) = sqlstate_of(&mut stream, r#"DELETE FROM "AfUser" WHERE id = 1"#).await.unwrap();
	assert_eq!(code, "23503");
	run_all(
		&mut stream,
		&[r#"DELETE FROM "AfPost" WHERE id = 2"#, r#"DELETE FROM "AfUser" WHERE id = 1"#],
	)
	.await;
	let rows = text_rows(&mut stream, r#"SELECT count(*) FROM "AfPost""#).await;
	assert_eq!(rows, vec![["0"]]);

	// Dropping the constraint keeps the column a link that is written as a key, and lifts the
	// check.
	run_all(&mut stream, &[r#"ALTER TABLE "AfPost" DROP CONSTRAINT "AfPost_authorId_fkey""#]).await;
	run_all(&mut stream, &[r#"INSERT INTO "AfPost" (id, "authorId") VALUES (6, 77)"#]).await;
	let rows = text_rows(&mut stream, r#"SELECT "authorId" FROM "AfPost""#).await;
	assert_eq!(rows, vec![["77"]]);

	// A composite key and a key to a column other than the id are refused, not ignored; so is a
	// column of a type that is no record key.
	run_all(
		&mut stream,
		&[r#"CREATE TABLE "AfOther" (id integer PRIMARY KEY, a integer, b boolean)"#],
	)
	.await;
	for (sql, reason) in [
		(
			r#"ALTER TABLE "AfOther" ADD FOREIGN KEY (a, id) REFERENCES "AfUser"(id, id)"#,
			"one column",
		),
		(r#"ALTER TABLE "AfOther" ADD FOREIGN KEY (a) REFERENCES "AfUser"(name)"#, "id column"),
		(
			r#"ALTER TABLE "AfOther" ADD FOREIGN KEY (b) REFERENCES "AfUser"(id)"#,
			"integer, text or uuid",
		),
	] {
		let (code, message) = sqlstate_of(&mut stream, sql).await.unwrap();
		assert_eq!(code, "0A000", "{sql}: {message}");
		assert!(message.contains(reason), "{sql}: {message}");
	}
	// The same over the extended protocol, which lowers against the schema as well.
	let (_, last) = run_extended_query(
		&mut stream,
		r#"ALTER TABLE "AfOther" ADD FOREIGN KEY (b) REFERENCES "AfUser"(id)"#,
		&[],
		&[],
	)
	.await;
	assert_eq!(last, b'E');

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_null_in_a_foreign_key_column_follows_the_column() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	run_all(
		&mut stream,
		&[
			r#"CREATE TABLE "NfUser" (id integer PRIMARY KEY)"#,
			r#"CREATE TABLE "NfPost" (id integer PRIMARY KEY, "authorId" integer, "ownerId" integer NOT NULL REFERENCES "NfUser"(id))"#,
			r#"INSERT INTO "NfUser" (id) VALUES (1)"#,
			r#"INSERT INTO "NfPost" (id, "authorId", "ownerId") VALUES (1, 1, 1)"#,
		],
	)
	.await;
	run_all(
		&mut stream,
		&[r#"ALTER TABLE "NfPost" ADD FOREIGN KEY ("authorId") REFERENCES "NfUser"(id)"#],
	)
	.await;

	// Prisma disconnects an optional relation with an UPDATE of the key to NULL, over the extended
	// protocol with the row's id as a parameter.
	let (_, last) = run_extended_query(
		&mut stream,
		r#"UPDATE "NfPost" SET "authorId" = NULL WHERE id = $1"#,
		&[20],
		&[Some(b"1")],
	)
	.await;
	assert_eq!(last, b'C');
	let rows = text_rows(&mut stream, r#"SELECT id, "authorId" FROM "NfPost""#).await;
	assert_eq!(rows, vec![["1", "NULL"]]);
	let rows = text_rows(&mut stream, r#"SELECT id FROM "NfPost" WHERE "authorId" IS NULL"#).await;
	assert_eq!(rows, vec![["1"]]);

	// An INSERT of NULL, a parameter that is NULL, and a key again.
	run_all(
		&mut stream,
		&[r#"INSERT INTO "NfPost" (id, "authorId", "ownerId") VALUES (2, NULL, 1)"#],
	)
	.await;
	let (_, last) = run_extended_query(
		&mut stream,
		r#"INSERT INTO "NfPost" (id, "authorId", "ownerId") VALUES (3, $1, 1)"#,
		&[20],
		&[None],
	)
	.await;
	assert_eq!(last, b'C');
	run_all(&mut stream, &[r#"UPDATE "NfPost" SET "authorId" = 1 WHERE id = 2"#]).await;
	let rows = text_rows(&mut stream, r#"SELECT id, "authorId" FROM "NfPost" ORDER BY id"#).await;
	assert_eq!(rows, vec![["1", "NULL"], ["2", "1"], ["3", "NULL"]]);

	// A required foreign key refuses NULL, as a NOT NULL violation.
	for sql in [
		r#"INSERT INTO "NfPost" (id, "ownerId") VALUES (4, NULL)"#,
		r#"UPDATE "NfPost" SET "ownerId" = NULL WHERE id = 1"#,
	] {
		let (code, message) = sqlstate_of(&mut stream, sql).await.unwrap();
		assert_eq!(code, "23502", "{sql}: {message}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_foreign_key_in_create_table_is_a_link_column() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	run_all(
		&mut stream,
		&[
			r#"CREATE TABLE "CfUser" (id integer PRIMARY KEY, name text)"#,
			r#"CREATE TABLE "CfTag" (id text PRIMARY KEY)"#,
			r#"CREATE TABLE "CfPost" (id integer PRIMARY KEY, "authorId" integer NOT NULL REFERENCES "CfUser"(id) ON DELETE CASCADE, tag text REFERENCES "CfTag", "editorId" integer, FOREIGN KEY ("editorId") REFERENCES "CfUser"(id) ON DELETE SET NULL)"#,
			r#"CREATE TABLE "CfNode" (id integer PRIMARY KEY, parent integer REFERENCES "CfNode"(id))"#,
			r#"INSERT INTO "CfUser" (id, name) VALUES (1, 'a'), (2, 'b')"#,
			r#"INSERT INTO "CfTag" (id) VALUES ('t1')"#,
			r#"INSERT INTO "CfPost" (id, "authorId", tag, "editorId") VALUES (1, 1, 't1', 2), (2, 2, 't1', 2)"#,
		],
	)
	.await;
	let rows = text_rows(
		&mut stream,
		r#"SELECT p.id, p."authorId", p.tag, u.name FROM "CfPost" p JOIN "CfUser" u ON p."editorId" = u.id ORDER BY p.id"#,
	)
	.await;
	assert_eq!(rows, vec![["1", "1", "t1", "b"], ["2", "2", "t1", "b"]]);
	let rows = text_rows(
		&mut stream,
		"SELECT column_name, data_type, is_nullable FROM information_schema.columns \
		 WHERE table_name = 'CfPost' AND column_name <> 'id' ORDER BY ordinal_position",
	)
	.await;
	assert_eq!(
		rows,
		vec![["authorId", "integer", "NO"], ["editorId", "integer", "YES"], ["tag", "text", "YES"]]
	);
	let rows = text_rows(
		&mut stream,
		"SELECT conname, confdeltype FROM pg_catalog.pg_constraint \
		 WHERE conname LIKE 'CfPost_%_fkey' ORDER BY conname",
	)
	.await;
	assert_eq!(
		rows,
		vec![
			["CfPost_authorId_fkey", "c"],
			["CfPost_editorId_fkey", "n"],
			["CfPost_tag_fkey", "a"]
		]
	);

	for sql in [
		r#"INSERT INTO "CfPost" (id, "authorId") VALUES (3, 9)"#,
		r#"INSERT INTO "CfPost" (id, "authorId", tag) VALUES (3, 1, 'nope')"#,
		r#"UPDATE "CfPost" SET "editorId" = 9 WHERE id = 1"#,
	] {
		let (code, message) = sqlstate_of(&mut stream, sql).await.unwrap();
		assert_eq!(code, "23503", "{sql}: {message}");
	}

	// The row of a table that links to itself may name itself, and a row that exists.
	run_all(
		&mut stream,
		&[
			r#"INSERT INTO "CfNode" (id, parent) VALUES (1, 1)"#,
			r#"INSERT INTO "CfNode" (id, parent) VALUES (2, 1)"#,
		],
	)
	.await;
	let (code, _) = sqlstate_of(&mut stream, r#"INSERT INTO "CfNode" (id, parent) VALUES (3, 8)"#)
		.await
		.unwrap();
	assert_eq!(code, "23503");

	// ON DELETE: SET NULL clears the key, CASCADE deletes the row.
	run_all(&mut stream, &[r#"DELETE FROM "CfUser" WHERE id = 2"#]).await;
	let rows = text_rows(&mut stream, r#"SELECT id, "editorId" FROM "CfPost" ORDER BY id"#).await;
	assert_eq!(rows, vec![["1", "NULL"]]);

	// A composite key and a key to another column are refused.
	for sql in [
		r#"CREATE TABLE "CfBad" (a integer, b integer, FOREIGN KEY (a, b) REFERENCES "CfUser"(id, id))"#,
		r#"CREATE TABLE "CfBad" (a integer REFERENCES "CfUser"(name))"#,
	] {
		let (code, message) = sqlstate_of(&mut stream, sql).await.unwrap();
		assert_eq!(code, "0A000", "{sql}: {message}");
	}

	shutdown.cancel();
}

/// A migration sent as one query creates the tables and alters them in the same request, so the
/// tables are in no catalog when it is lowered.
#[tokio::test]
async fn test_pgwire_foreign_key_added_in_the_request_that_creates_the_table() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	run_all(
		&mut stream,
		&[
			r#"CREATE TABLE "MgPost" (id integer PRIMARY KEY NOT NULL, "authorId" integer NOT NULL);
			   CREATE TABLE "MgUser" (id integer PRIMARY KEY NOT NULL, name text NOT NULL);
			   ALTER TABLE "MgPost" ADD CONSTRAINT "MgPost_authorId_fkey" FOREIGN KEY ("authorId") REFERENCES "MgUser"("id") ON DELETE RESTRICT ON UPDATE CASCADE;"#,
			r#"INSERT INTO "MgUser" (id, name) VALUES (1, 'a')"#,
			r#"INSERT INTO "MgPost" (id, "authorId") VALUES (1, 1)"#,
		],
	)
	.await;
	let (code, _) =
		sqlstate_of(&mut stream, r#"INSERT INTO "MgPost" (id, "authorId") VALUES (2, 5)"#)
			.await
			.unwrap();
	assert_eq!(code, "23503");
	let (code, _) = sqlstate_of(&mut stream, r#"DELETE FROM "MgUser" WHERE id = 1"#).await.unwrap();
	assert_eq!(code, "23503");
	let rows = text_rows(
		&mut stream,
		"SELECT conname FROM pg_catalog.pg_constraint WHERE conname LIKE 'MgPost_%_fkey'",
	)
	.await;
	assert_eq!(rows, vec![["MgPost_authorId_fkey"]]);

	// A transaction sees the tables an earlier statement of it created.
	run_all(
		&mut stream,
		&[
			"BEGIN",
			r#"CREATE TABLE "TxPost" (id integer PRIMARY KEY NOT NULL, "authorId" integer NOT NULL)"#,
			r#"CREATE TABLE "TxUser" (id integer PRIMARY KEY NOT NULL)"#,
			r#"ALTER TABLE "TxPost" ADD CONSTRAINT x FOREIGN KEY ("authorId") REFERENCES "TxUser"(id)"#,
			"COMMIT",
		],
	)
	.await;
	let rows = text_rows(
		&mut stream,
		"SELECT conname FROM pg_catalog.pg_constraint WHERE conname LIKE 'TxPost_%_fkey'",
	)
	.await;
	assert_eq!(rows, vec![["TxPost_authorId_fkey"]]);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_quantified_comparisons_against_an_array_are_three_valued() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		"DEFINE TABLE qa SCHEMALESS; \
		 CREATE qa:1 SET x = 1, arr = [1, 2]; \
		 CREATE qa:2 SET x = 3, arr = [1, 2]; \
		 CREATE qa:3 SET x = 3, arr = [1, NULL]; \
		 CREATE qa:4 SET x = NULL, arr = [1]; \
		 CREATE qa:5 SET x = 1, arr = NULL; \
		 CREATE qa:6 SET x = 1, arr = []; \
		 CREATE qa:7 SET x = 2, arr = [2, 2]; \
		 CREATE qa:8 SET x = 1, arr = [1, NULL]; \
		 CREATE qa:9 SET x = NULL, arr = []; \
		 CREATE qa:10 SET x = 2, arr = [1, 3];",
	)
	.await;

	// Rows 1 to 10: match, no match, no match beside a NULL item, NULL value, NULL array, empty
	// array, all items equal, match beside a NULL item, NULL value and empty array, a differing
	// item.
	let cases = [
		("x = ANY (arr)", ["t", "f", "NULL", "NULL", "NULL", "f", "t", "t", "f", "f"]),
		("x <> ALL (arr)", ["f", "t", "NULL", "NULL", "NULL", "t", "f", "f", "t", "t"]),
		("x = ALL (arr)", ["f", "f", "f", "NULL", "NULL", "t", "t", "NULL", "t", "f"]),
		("x <> ANY (arr)", ["t", "t", "t", "NULL", "NULL", "f", "f", "NULL", "f", "t"]),
		("x = SOME (arr)", ["t", "f", "NULL", "NULL", "NULL", "f", "t", "t", "f", "f"]),
	];
	for (predicate, expected) in cases {
		let rows =
			text_rows(&mut stream, &format!("SELECT ({predicate}) AS r FROM qa ORDER BY id")).await;
		assert_eq!(rows.len(), 10, "{predicate}");
		let got: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
		assert_eq!(got, expected, "{predicate}");
	}

	// Unknown is not true: neither the predicate nor its negation selects the NULL rows.
	let rows = text_rows(&mut stream, "SELECT 1 FROM qa WHERE NOT (x = ANY (arr))").await;
	assert_eq!(rows.len(), 4);

	let (_, _, err) = run_simple_query_with_err(&mut stream, "SELECT x < ANY (arr) FROM qa").await;
	let (code, _) = err.expect("an unsupported operator is refused");
	assert_eq!(code, "0A000");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_drizzle_sequence_user_and_index_catalog_queries() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	for ddl in [
		"CREATE TABLE \"Counter\" (id serial PRIMARY KEY, label text NOT NULL)",
		"CREATE TABLE \"Plain\" (id integer PRIMARY KEY)",
		"CREATE INDEX \"Counter_label_idx\" ON \"Counter\" (label)",
	] {
		let (_, last) = run_simple_query(&mut stream, ddl).await;
		assert_eq!(last, b'C', "{ddl}");
	}

	// The sequences query lists the sequence of each serial column, and only those.
	let (rows, last) = run_simple_query(
		&mut stream,
		"select schemaname, sequencename, start_value, min_value, max_value, increment_by, cycle, \
		 cache_size from pg_sequences as seq WHERE schemaname = 'public';",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cells: Vec<_> =
		decode_data_row(&rows[0]).into_iter().map(Option::unwrap_or_default).collect();
	assert_eq!(cells, ["public", "Counter_id_seq", "1", "1", "9223372036854775807", "1", "f", "1"]);

	// The schema listing joins each namespace to its owner; every namespace has one, and
	// `public` is not listed.
	let (rows, last) = run_simple_query(
		&mut stream,
		"select s.nspname as table_schema from pg_catalog.pg_namespace s join pg_catalog.pg_user u \
		 on u.usesysid = s.nspowner where nspname not in ('information_schema', 'pg_catalog', \
		 'public') and nspname not like 'pg_toast%' and nspname not like 'pg_temp_%' order by \
		 table_schema;",
	)
	.await;
	assert_eq!(last, b'C');
	assert!(rows.is_empty());
	let (rows, _) = run_simple_query(
		&mut stream,
		"select count(*) from pg_catalog.pg_namespace s join pg_catalog.pg_user u on u.usesysid = \
		 s.nspowner",
	)
	.await;
	assert_ne!(decode_data_row(&rows[0])[0].as_deref(), Some("0"));

	// A serial column has a default that draws from its sequence, and the sequence is found by
	// the name of the table, however it is written.
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT ad.adnum, pg_get_expr(ad.adbin, ad.adrelid) AS expr, \
		 pg_get_serial_sequence('\"public\".\"Counter\"', 'id')::regclass AS seq_name, \
		 pg_get_serial_sequence('\"public\".\"Counter\"', 'label') AS none, \
		 20 = ANY ('{int,int8,int2}'::regtype[]) AS wide, CASE 20 WHEN 'int8'::regtype THEN \
		 'bigserial' END AS kind FROM pg_attrdef ad",
	)
	.await;
	assert_eq!(last, b'C');
	assert_eq!(rows.len(), 1);
	let cells = decode_data_row(&rows[0]);
	assert_eq!(cells[0].as_deref(), Some("1"));
	assert_eq!(cells[1].as_deref(), Some("nextval('\"Counter_id_seq\"'::regclass)"));
	assert_eq!(cells[2].as_deref(), Some("\"Counter_id_seq\""));
	assert_eq!(cells[3], None);
	assert_eq!(cells[4].as_deref(), Some("t"));
	assert_eq!(cells[5].as_deref(), Some("bigserial"));

	// The index query expands the key of each index through a LATERAL subquery, and the
	// constraint behind the primary key index is told apart from a plain index.
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT DISTINCT ON (t.relname, ic.relname, k.i) ic.relname AS indexname, i.indisunique AS \
		 is_unique, coalesce(a.attname, pg_get_indexdef(i.indexrelid, k.i, false)) AS column_name, \
		 i.indoption[k.i-1] & 1 = 1 AS descending, opc.opcname FROM pg_class t LEFT JOIN pg_index \
		 i ON t.oid = i.indrelid LEFT JOIN pg_class ic ON ic.oid = i.indexrelid CROSS JOIN LATERAL \
		 (SELECT unnest(i.indkey), generate_subscripts(i.indkey, 1) + 1) AS k(attnum, i) LEFT JOIN \
		 pg_attribute AS a ON i.indrelid = a.attrelid AND k.attnum = a.attnum JOIN pg_namespace c \
		 ON c.oid = t.relnamespace JOIN pg_opclass opc ON opc.oid = ANY(i.indclass) WHERE \
		 c.nspname = 'public' AND t.relname = 'Counter' ORDER BY ic.relname",
	)
	.await;
	assert_eq!(last, b'C');
	let found: Vec<Vec<Option<String>>> = rows.iter().map(|row| decode_data_row(row)).collect();
	let summary: Vec<_> = found
		.iter()
		.map(|cells| {
			(
				cells[0].clone().unwrap_or_default(),
				cells[1].clone().unwrap_or_default(),
				cells[2].clone().unwrap_or_default(),
				cells[3].clone().unwrap_or_default(),
				cells[4].clone().unwrap_or_default(),
			)
		})
		.collect();
	let row = |index: &str, unique: &str, column: &str, opclass: &str| {
		(index.into(), unique.into(), column.into(), "f".into(), opclass.into())
	};
	assert_eq!(
		summary,
		[
			row("Counter_label_idx", "f", "label", "text_ops"),
			row("Counter_pkey", "t", "id", "int4_ops"),
		]
	);

	// The primary key index belongs to a constraint, the plain index does not.
	let (rows, _) = run_simple_query(
		&mut stream,
		"SELECT idx.indexrelname, CASE WHEN con.conname IS NOT NULL THEN 1 ELSE 0 END AS \
		 generated_by_constraint FROM pg_stat_user_indexes idx LEFT JOIN pg_constraint con ON \
		 con.conindid = idx.indexrelid WHERE idx.relname = 'Counter' AND schemaname = 'public' \
		 GROUP BY indexrelname, generated_by_constraint ORDER BY indexrelname",
	)
	.await;
	let found: Vec<_> = rows
		.iter()
		.map(|row| {
			let cells = decode_data_row(row);
			(cells[0].clone().unwrap_or_default(), cells[1].clone().unwrap_or_default())
		})
		.collect();
	assert_eq!(
		found,
		[
			("Counter_label_idx".to_owned(), "0".to_owned()),
			("Counter_pkey".to_owned(), "1".to_owned())
		]
	);

	shutdown.cancel();
}

/// A connection together with the key a CancelRequest for it must carry.
async fn connect_with_backend_key(addr: std::net::SocketAddr) -> (TcpStream, i32, i32) {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let mut body = BytesMut::new();
	body.put_i32(196608);
	for (name, value) in
		[("user", "root"), ("database", "test/test"), ("options", "-c dialect=pgsql")]
	{
		body.put_slice(name.as_bytes());
		body.put_u8(0);
		body.put_slice(value.as_bytes());
		body.put_u8(0);
	}
	body.put_u8(0);
	stream.write_i32((body.len() + 4) as i32).await.expect("send length");
	stream.write_all(&body).await.expect("send startup");
	let mut key = None;
	timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await.expect("read type");
			let msg_len = stream.read_i32().await.expect("read length") as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await.expect("read payload");
			match msg_type {
				b'K' => {
					key = Some((
						i32::from_be_bytes(payload[..4].try_into().expect("process id")),
						i32::from_be_bytes(payload[4..8].try_into().expect("secret key")),
					));
				}
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.expect("handshake");
	let (pid, secret) = key.expect("BackendKeyData");
	(stream, pid, secret)
}

/// Ask the server to cancel the query of the connection holding `(pid, secret)`.
async fn send_cancel_request(addr: std::net::SocketAddr, pid: i32, secret: i32) {
	let mut side = TcpStream::connect(addr).await.expect("connect");
	let mut packet = BytesMut::new();
	packet.put_i32(16);
	packet.put_i32(80877102);
	packet.put_i32(pid);
	packet.put_i32(secret);
	side.write_all(&packet).await.expect("send cancel request");
	// The server closes the side connection once it has acted on the request.
	let mut sink = Vec::new();
	let _ = timeout(Duration::from_secs(5), side.read_to_end(&mut sink)).await;
}

async fn send_simple_query(stream: &mut TcpStream, sql: &str) {
	let mut query = sql.to_string();
	query.push('\0');
	let mut packet = BytesMut::new();
	packet.put_u8(b'Q');
	packet.put_i32((query.len() + 4) as i32);
	packet.put_slice(query.as_bytes());
	stream.write_all(&packet).await.expect("send query");
}

/// Read up to the next ReadyForQuery: the data rows and the error, if any.
async fn read_response(
	stream: &mut TcpStream,
	wait: Duration,
) -> Option<(Vec<Vec<Option<String>>>, Option<(String, String)>)> {
	let mut rows = Vec::new();
	let mut error = None;
	timeout(wait, async {
		loop {
			let msg_type = stream.read_u8().await.expect("read type");
			let msg_len = stream.read_i32().await.expect("read length") as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await.expect("read payload");
			match msg_type {
				b'D' => rows.push(decode_data_row(&payload)),
				b'E' => error = Some(parse_pg_error_fields(&payload)),
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.ok()?;
	Some((rows, error))
}

/// The one cell of the one row a query returns.
async fn one_cell(stream: &mut TcpStream, sql: &str) -> Option<String> {
	send_simple_query(stream, sql).await;
	let (rows, error) = read_response(stream, Duration::from_secs(5)).await.expect("response");
	assert_eq!(error, None, "{sql}");
	assert_eq!(rows.len(), 1, "{sql}");
	rows[0][0].clone()
}

#[tokio::test]
async fn test_pgwire_advisory_lock_blocks_until_the_holder_unlocks() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut holder = connect_and_handshake(addr).await;
	let mut waiter = connect_and_handshake(addr).await;

	assert_eq!(
		one_cell(&mut holder, "SELECT pg_advisory_lock(72707369)").await.as_deref(),
		Some("")
	);
	assert_eq!(
		one_cell(&mut waiter, "SELECT pg_try_advisory_lock(72707369)").await.as_deref(),
		Some("f")
	);
	assert_eq!(
		one_cell(&mut waiter, "SELECT pg_advisory_unlock(72707369)").await.as_deref(),
		Some("f"),
		"a lock another session holds is not the waiter's to release"
	);

	send_simple_query(&mut waiter, "SELECT pg_advisory_lock(72707369)").await;
	assert!(
		read_response(&mut waiter, Duration::from_millis(300)).await.is_none(),
		"the second lock waits while the first is held"
	);

	assert_eq!(
		one_cell(&mut holder, "SELECT pg_advisory_unlock(72707369)").await.as_deref(),
		Some("t")
	);
	let (rows, error) = read_response(&mut waiter, Duration::from_secs(5))
		.await
		.expect("the waiter takes the lock once it is released");
	assert_eq!(error, None);
	assert_eq!(rows, vec![vec![Some(String::new())]]);

	assert_eq!(
		one_cell(&mut holder, "SELECT pg_try_advisory_lock(72707369)").await.as_deref(),
		Some("f"),
		"the waiter holds it now"
	);
	assert_eq!(
		one_cell(&mut holder, "SELECT pg_advisory_unlock(72707369)").await.as_deref(),
		Some("f")
	);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_advisory_locks_are_counted_per_session_and_keyed_by_form() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut a = connect_and_handshake(addr).await;
	let mut b = connect_and_handshake(addr).await;

	assert_eq!(one_cell(&mut a, "SELECT pg_try_advisory_lock(5)").await.as_deref(), Some("t"));
	assert_eq!(one_cell(&mut a, "SELECT pg_advisory_lock(5)").await.as_deref(), Some(""));
	assert_eq!(one_cell(&mut a, "SELECT pg_advisory_unlock(5)").await.as_deref(), Some("t"));
	assert_eq!(
		one_cell(&mut b, "SELECT pg_try_advisory_lock(5)").await.as_deref(),
		Some("f"),
		"one hold is left"
	);
	assert_eq!(one_cell(&mut a, "SELECT pg_advisory_unlock(5)").await.as_deref(), Some("t"));
	assert_eq!(one_cell(&mut a, "SELECT pg_advisory_unlock(5)").await.as_deref(), Some("f"));
	assert_eq!(one_cell(&mut b, "SELECT pg_try_advisory_lock(5)").await.as_deref(), Some("t"));

	// The two-key form is a different key space from the one-key form.
	assert_eq!(one_cell(&mut a, "SELECT pg_try_advisory_lock(0, 5)").await.as_deref(), Some("t"));
	assert_eq!(one_cell(&mut b, "SELECT pg_try_advisory_lock(0, 5)").await.as_deref(), Some("f"));

	// `pg_advisory_unlock_all` drops every hold, however often a key was taken.
	assert_eq!(one_cell(&mut b, "SELECT pg_try_advisory_lock(5)").await.as_deref(), Some("t"));
	assert_eq!(one_cell(&mut b, "SELECT pg_advisory_unlock_all()").await.as_deref(), Some(""));
	assert_eq!(one_cell(&mut a, "SELECT pg_try_advisory_lock(5)").await.as_deref(), Some("t"));
	assert_eq!(one_cell(&mut b, "SELECT pg_try_advisory_lock(0, 5)").await.as_deref(), Some("f"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_advisory_locks_are_released_when_the_connection_ends() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut holder = connect_and_handshake(addr).await;
	let mut other = connect_and_handshake(addr).await;
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_lock(9)").await.as_deref(), Some(""));
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_lock(9)").await.as_deref(), Some(""));
	assert_eq!(one_cell(&mut other, "SELECT pg_try_advisory_lock(9)").await.as_deref(), Some("f"));

	// The client vanishes without a Terminate.
	drop(holder);
	let mut freed = false;
	for _ in 0..100 {
		if one_cell(&mut other, "SELECT pg_try_advisory_lock(9)").await.as_deref() == Some("t") {
			freed = true;
			break;
		}
		sleep(Duration::from_millis(50)).await;
	}
	assert!(freed, "the lock is released once its session is gone");
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_a_waiting_advisory_lock_ends_on_cancel_request_and_on_disconnect() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut holder = connect_and_handshake(addr).await;
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_lock(11)").await.as_deref(), Some(""));

	let (mut waiter, pid, secret) = connect_with_backend_key(addr).await;
	send_simple_query(&mut waiter, "SELECT pg_advisory_lock(11)").await;
	assert!(read_response(&mut waiter, Duration::from_millis(200)).await.is_none());
	send_cancel_request(addr, pid, secret).await;
	let (rows, error) = read_response(&mut waiter, Duration::from_secs(5))
		.await
		.expect("a CancelRequest ends the wait");
	assert!(rows.is_empty());
	assert_eq!(error.expect("canceled").0, "57014");
	// The connection stays usable, and the cancelled wait took nothing.
	assert_eq!(
		one_cell(&mut waiter, "SELECT pg_try_advisory_lock(12)").await.as_deref(),
		Some("t")
	);
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_unlock(11)").await.as_deref(), Some("t"));
	assert_eq!(
		one_cell(&mut holder, "SELECT pg_try_advisory_lock(11)").await.as_deref(),
		Some("t")
	);
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_unlock(11)").await.as_deref(), Some("t"));

	// A waiter whose client goes away stops waiting, so the key goes to the next in line.
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_lock(13)").await.as_deref(), Some(""));
	let mut abandoned = connect_and_handshake(addr).await;
	send_simple_query(&mut abandoned, "SELECT pg_advisory_lock(13)").await;
	sleep(Duration::from_millis(200)).await;
	drop(abandoned);
	sleep(Duration::from_millis(200)).await;
	assert_eq!(one_cell(&mut holder, "SELECT pg_advisory_unlock(13)").await.as_deref(), Some("t"));
	assert_eq!(
		one_cell(&mut holder, "SELECT pg_try_advisory_lock(13)").await.as_deref(),
		Some("t"),
		"the departed waiter did not keep the key"
	);
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_advisory_lock_functions_take_parameters_and_describe_their_result() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut a = connect_and_handshake(addr).await;
	let mut b = connect_and_handshake(addr).await;

	let (rows, _) =
		run_extended_query(&mut a, "SELECT pg_try_advisory_lock($1)", &[], &[Some(b"7")]).await;
	assert_eq!(decode_data_row(&rows[0]), vec![Some("t".to_string())]);
	let (rows, _) =
		run_extended_query(&mut b, "SELECT pg_try_advisory_lock($1)", &[], &[Some(b"7")]).await;
	assert_eq!(decode_data_row(&rows[0]), vec![Some("f".to_string())]);
	let (rows, _) =
		run_extended_query(&mut b, "SELECT pg_try_advisory_lock($1)", &[], &[None]).await;
	assert_eq!(decode_data_row(&rows[0]), vec![None], "a NULL key returns NULL and takes nothing");
	let (rows, _) =
		run_extended_query(&mut a, "SELECT pg_advisory_unlock($1)", &[], &[Some(b"7")]).await;
	assert_eq!(decode_data_row(&rows[0]), vec![Some("t".to_string())]);

	// Describe: one int8 parameter, one column named for the function, of type void.
	run_extended_batch(
		&mut a,
		&[build_parse("lock", "SELECT pg_advisory_lock($1)", &[]), build_sync()],
	)
	.await;
	for packet in [build_desc(b'S', "lock"), build_sync()] {
		a.write_all(&packet).await.expect("send");
	}
	let mut messages = Vec::new();
	timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = a.read_u8().await.expect("type");
			let msg_len = a.read_i32().await.expect("length") as usize;
			let mut payload = vec![0u8; msg_len - 4];
			a.read_exact(&mut payload).await.expect("payload");
			messages.push((msg_type, payload));
			if msg_type == b'Z' {
				break;
			}
		}
	})
	.await
	.expect("describe response");
	let parameters = messages.iter().find(|(t, _)| *t == b't').expect("ParameterDescription");
	let mut cur = parameters.1.as_slice();
	assert_eq!(cur.get_i16(), 1);
	assert_eq!(cur.get_i32(), 20, "the key is a bigint");
	let columns = messages.iter().find(|(t, _)| *t == b'T').expect("RowDescription");
	let mut cur = columns.1.as_slice();
	assert_eq!(cur.get_i16(), 1);
	let name_end = cur.iter().position(|b| *b == 0).expect("column name");
	assert_eq!(&cur[..name_end], b"pg_advisory_lock");
	cur.advance(name_end + 1 + 4 + 2);
	assert_eq!(cur.get_i32(), 2278, "pg_advisory_lock returns void");
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_advisory_lock_inside_a_larger_query_is_refused() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for sql in [
		"SELECT pg_advisory_lock(1) FROM pg_class",
		"SELECT pg_try_advisory_lock(1), 2",
		"SELECT 1 WHERE pg_advisory_unlock(1)",
	] {
		let (_, _, error) = run_simple_query_with_err(&mut stream, sql).await;
		assert_eq!(error.expect(sql).0, "0A000", "{sql}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_catalog_vectors_count_from_zero_and_user_tables_have_row_types() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	for ddl in [
		"CREATE TABLE \"Counter\" (id serial PRIMARY KEY, label text NOT NULL)",
		"CREATE INDEX \"Counter_label_idx\" ON \"Counter\" (label)",
	] {
		let (_, last) = run_simple_query(&mut stream, ddl).await;
		assert_eq!(last, b'C', "{ddl}");
	}

	// The key columns of an index are numbered from zero, as `int2vector` is, and an element
	// is read at the same number.
	let (rows, last) = run_simple_query(
		&mut stream,
		"SELECT ic.relname, unnest(i.indkey) AS attnum, generate_subscripts(i.indkey, 1) AS pos, \
		 i.indkey[0] AS first FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid WHERE \
		 ic.relname = 'Counter_label_idx'",
	)
	.await;
	assert_eq!(last, b'C');
	let cells: Vec<_> = rows.iter().map(|row| decode_data_row(row)).collect();
	assert_eq!(cells.len(), 1);
	assert_eq!(cells[0][2].as_deref(), Some("0"));
	assert_eq!(cells[0][1], cells[0][3]);

	// A table's row type is the `pg_type` row its `pg_class` row names.
	let (rows, _) = run_simple_query(
		&mut stream,
		"SELECT t.typname, t.typtype, t.typrelid = c.oid AS own FROM pg_class c JOIN pg_type t ON \
		 t.oid = c.reltype WHERE c.relname = 'Counter'",
	)
	.await;
	assert_eq!(rows.len(), 1);
	assert_eq!(
		decode_data_row(&rows[0]),
		[Some("Counter".to_string()), Some("c".to_string()), Some("t".to_string())]
	);

	// A column compared with a column of another relation on the left of a left join does not
	// drop the left row.
	let (rows, _) = run_simple_query(
		&mut stream,
		"SELECT c.relname, d.adnum FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
		 LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND n.nspname = c.relname WHERE c.relname = \
		 'Counter'",
	)
	.await;
	assert_eq!(rows.len(), 1);
	assert_eq!(decode_data_row(&rows[0]), [Some("Counter".to_string()), None]);

	shutdown.cancel();
}

/// Every message the server sends after `packets` until ReadyForQuery.
async fn exchange(stream: &mut TcpStream, packets: &[BytesMut]) -> Vec<(u8, Vec<u8>)> {
	for p in packets {
		stream.write_all(p).await.expect("send packet");
	}
	let mut msgs = Vec::new();
	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;
			let done = msg_type == b'Z';
			msgs.push((msg_type, payload));
			if done {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;
	assert!(res.is_ok(), "Timed out waiting for ReadyForQuery");
	msgs
}

fn simple_query_packet(sql: &str) -> BytesMut {
	let mut q = BytesMut::new();
	q.put_u8(b'Q');
	q.put_i32((sql.len() + 5) as i32);
	q.put_slice(sql.as_bytes());
	q.put_u8(0);
	q
}

/// The names and type OIDs of the first RowDescription in `msgs`.
fn row_description_fields(msgs: &[(u8, Vec<u8>)]) -> Vec<(String, i32)> {
	let (_, payload) = msgs.iter().find(|(t, _)| *t == b'T').expect("RowDescription");
	let mut cur = payload.as_slice();
	let n = cur.get_i16();
	let mut fields = Vec::new();
	for _ in 0..n {
		let name_end = cur.iter().position(|b| *b == 0).expect("field name");
		let name = String::from_utf8_lossy(&cur[..name_end]).to_string();
		cur.advance(name_end + 1);
		cur.advance(4 + 2);
		fields.push((name, cur.get_i32()));
		cur.advance(2 + 4 + 2);
	}
	fields
}

/// The type OIDs of the first RowDescription in `msgs`.
fn row_description_oids(msgs: &[(u8, Vec<u8>)]) -> Vec<i32> {
	row_description_fields(msgs).into_iter().map(|(_, oid)| oid).collect()
}

fn data_rows(msgs: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<String>>> {
	msgs.iter().filter(|(t, _)| *t == b'D').map(|(_, p)| decode_data_row(p)).collect()
}

fn error_code(msgs: &[(u8, Vec<u8>)]) -> Option<(String, String)> {
	msgs.iter().find(|(t, _)| *t == b'E').map(|(_, p)| parse_pg_error_fields(p))
}

async fn extended(
	stream: &mut TcpStream,
	sql: &str,
	param_types: &[i32],
	formats: &[i16],
	params: &[Option<&[u8]>],
) -> Vec<(u8, Vec<u8>)> {
	exchange(
		stream,
		&[
			build_parse("", sql, param_types),
			build_bind("", "", formats, params),
			build_desc(b'P', ""),
			build_exec("", 0),
			build_sync(),
		],
	)
	.await
}

#[tokio::test]
async fn test_pgwire_sized_integer_columns() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let msgs = exchange(
		&mut stream,
		&[simple_query_packet(
			"CREATE TABLE g (id serial PRIMARY KEY, n integer, s smallint, b bigint, r real)",
		)],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "CREATE TABLE: {:?}", error_code(&msgs));

	// Column types are the declared widths.
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT id, n, s, b, r FROM g")]).await;
	assert_eq!(row_description_oids(&msgs), vec![23, 23, 21, 20, 700]);

	// Simple-query insert; the id comes from the serial column.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet(
			"INSERT INTO g (n, s, b, r) VALUES (7, 8, 9000000000, 1.5) RETURNING id, n, s, b, r",
		)],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "INSERT: {:?}", error_code(&msgs));
	assert_eq!(row_description_oids(&msgs), vec![23, 23, 21, 20, 700]);
	let rows = data_rows(&msgs);
	assert_eq!(
		rows,
		vec![vec![
			Some("1".to_string()),
			Some("7".to_string()),
			Some("8".to_string()),
			Some("9000000000".to_string()),
			Some("1.5".to_string())
		]]
	);

	// A `*` has its columns known only from the rows, typed by the width of the values they hold.
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT * FROM g")]).await;
	let fields: std::collections::HashMap<String, i32> =
		row_description_fields(&msgs).into_iter().collect();
	assert_eq!(
		[fields["n"], fields["s"], fields["b"], fields["r"]],
		[23, 21, 20, 700],
		"{fields:?}"
	);

	// Untyped (OID 0) text parameters.
	let msgs = extended(
		&mut stream,
		"INSERT INTO g (n, s, b, r) VALUES ($1, $2, $3, $4) RETURNING id, n, s, b, r",
		&[0, 0, 0, 0],
		&[],
		&[Some(b"11"), Some(b"12"), Some(b"13"), Some(b"2.5")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "untyped INSERT: {:?}", error_code(&msgs));
	assert_eq!(
		data_rows(&msgs),
		vec![vec![
			Some("2".to_string()),
			Some("11".to_string()),
			Some("12".to_string()),
			Some("13".to_string()),
			Some("2.5".to_string())
		]]
	);

	// Binary int4 / int2 / int8 / float4 parameters.
	let n = 21i32.to_be_bytes();
	let s = 22i16.to_be_bytes();
	let b = 23i64.to_be_bytes();
	let r = 3.5f32.to_be_bytes();
	let msgs = extended(
		&mut stream,
		"INSERT INTO g (n, s, b, r) VALUES ($1, $2, $3, $4) RETURNING id, n, s, b, r",
		&[23, 21, 20, 700],
		&[1],
		&[Some(&n), Some(&s), Some(&b), Some(&r)],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "typed INSERT: {:?}", error_code(&msgs));
	assert_eq!(
		data_rows(&msgs),
		vec![vec![
			Some("3".to_string()),
			Some("21".to_string()),
			Some("22".to_string()),
			Some("23".to_string()),
			Some("3.5".to_string())
		]]
	);

	// `id = $1` finds the row, with an untyped and an int4 parameter.
	let msgs =
		extended(&mut stream, "SELECT n FROM g WHERE id = $1", &[0], &[], &[Some(b"2")]).await;
	assert_eq!(data_rows(&msgs), vec![vec![Some("11".to_string())]]);
	let id = 3i32.to_be_bytes();
	let msgs =
		extended(&mut stream, "SELECT n FROM g WHERE id = $1", &[23], &[1], &[Some(&id)]).await;
	assert_eq!(data_rows(&msgs), vec![vec![Some("21".to_string())]]);
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT n FROM g WHERE id = 1")]).await;
	assert_eq!(data_rows(&msgs), vec![vec![Some("7".to_string())]]);

	// A value outside the width of the column is refused.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("INSERT INTO g (n, s, b, r) VALUES (3000000000, 1, 1, 1.0)")],
	)
	.await;
	let (_, message) = error_code(&msgs).expect("out-of-range integer refused");
	assert!(message.contains("i32"), "{message}");
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("INSERT INTO g (n, s, b, r) VALUES (1, 40000, 1, 1.0)")],
	)
	.await;
	assert!(error_code(&msgs).is_some(), "out-of-range smallint refused");
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT count(*) FROM g")]).await;
	assert_eq!(data_rows(&msgs), vec![vec![Some("3".to_string())]]);

	// An `integer` column joins to the serial key it holds, as an `id` compared with it.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("CREATE TABLE h (id serial PRIMARY KEY, g_id integer)")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "CREATE TABLE h: {:?}", error_code(&msgs));
	let msgs =
		exchange(&mut stream, &[simple_query_packet("INSERT INTO h (g_id) VALUES (2)")]).await;
	assert!(error_code(&msgs).is_none(), "INSERT h: {:?}", error_code(&msgs));
	let msgs =
		exchange(&mut stream, &[simple_query_packet("SELECT g.n FROM h JOIN g ON g.id = h.g_id")])
			.await;
	assert_eq!(data_rows(&msgs), vec![vec![Some("11".to_string())]]);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_wildcard_columns_take_their_declared_types() {
	let ds = Datastore::new("memory").await.expect("datastore");
	ds.execute(
		r#"
		DEFINE NAMESPACE test;
		DEFINE DATABASE test;
		DEFINE TABLE loose SCHEMALESS;
		CREATE loose:1 SET a = 5, b = 'x';
		"#,
		&Session::owner().with_ns("test").with_db("test"),
		None,
	)
	.await
	.expect("define a schemaless table");
	let (addr, shutdown) = start_test_pg_server_with_ds(ds).await;
	let mut stream = connect_and_handshake(addr).await;

	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("CREATE TABLE w (id serial PRIMARY KEY, n integer, t text)")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "CREATE TABLE: {:?}", error_code(&msgs));
	let want = vec![("id".to_string(), 23), ("n".to_string(), 23), ("t".to_string(), 25)];
	let row = vec![Some("1".to_string()), Some("7".to_string()), Some("a".to_string())];

	// RETURNING * over the extended protocol, with the portal described.
	let msgs = extended(
		&mut stream,
		"INSERT INTO w (n, t) VALUES ($1, $2) RETURNING *",
		&[],
		&[],
		&[Some(b"7"), Some(b"a")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "RETURNING *: {:?}", error_code(&msgs));
	assert_eq!(row_description_fields(&msgs), want);
	assert_eq!(data_rows(&msgs), vec![row.clone()]);

	// The same over the simple protocol.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("INSERT INTO w (n, t) VALUES (8, 'b') RETURNING *")],
	)
	.await;
	assert_eq!(row_description_fields(&msgs), want);
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT * FROM w ORDER BY id")]).await;
	assert_eq!(row_description_fields(&msgs), want);
	assert_eq!(data_rows(&msgs)[0], row);
	let msgs =
		extended(&mut stream, "SELECT * FROM w WHERE id = $1", &[], &[], &[Some(b"1")]).await;
	assert_eq!(row_description_fields(&msgs), want);
	assert_eq!(data_rows(&msgs), vec![row]);
	let msgs =
		extended(&mut stream, "UPDATE w SET n = 9 WHERE id = 1 RETURNING *", &[], &[], &[]).await;
	assert_eq!(row_description_fields(&msgs), want);
	let msgs = extended(&mut stream, "DELETE FROM w WHERE id = 2 RETURNING *", &[], &[], &[]).await;
	assert_eq!(row_description_fields(&msgs), want);

	// A schemaless table declares nothing: its columns are typed from the rows.
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT * FROM loose")]).await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	let fields: std::collections::HashMap<String, i32> =
		row_description_fields(&msgs).into_iter().collect();
	assert_eq!(fields["a"], 20);
	assert_eq!(fields["b"], 25);

	// A column whose rows hold only NULL answers with its declared type.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("INSERT INTO w (n, t) VALUES (NULL, 'c') RETURNING *")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	assert_eq!(row_description_fields(&msgs), want);

	// Several statements in one request are typed from their values, not from one statement's
	// declarations, so a bigint beyond the range of another table's integer column survives.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("CREATE TABLE big (id serial PRIMARY KEY, n bigint, t text)")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("INSERT INTO big (n, t) VALUES (3000000000, 'z')")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet("SELECT * FROM big; SELECT * FROM w WHERE id = 1")],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	assert_eq!(data_rows(&msgs)[0][1], Some("3000000000".to_string()));

	shutdown.cancel();
}

/// The names, type OIDs and type modifiers of the first RowDescription in `msgs`.
fn row_description_typmods(msgs: &[(u8, Vec<u8>)]) -> Vec<(String, i32, i32)> {
	let (_, payload) = msgs.iter().find(|(t, _)| *t == b'T').expect("RowDescription");
	let mut cur = payload.as_slice();
	let n = cur.get_i16();
	let mut fields = Vec::new();
	for _ in 0..n {
		let name_end = cur.iter().position(|b| *b == 0).expect("field name");
		let name = String::from_utf8_lossy(&cur[..name_end]).to_string();
		cur.advance(name_end + 1);
		cur.advance(4 + 2);
		let oid = cur.get_i32();
		cur.advance(2);
		let typmod = cur.get_i32();
		cur.advance(2);
		fields.push((name, oid, typmod));
	}
	fields
}

#[tokio::test]
async fn test_pgwire_declared_sql_types_are_reported_with_their_modifiers() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	for sql in [
		"CREATE TABLE decl (id integer PRIMARY KEY, v varchar(20), c char(5), n numeric(10,2), ts \
		 timestamp(3))",
		"INSERT INTO decl (id, v, c, n, ts) VALUES (1, 'ab', 'xy', 1.5, \
		 '2026-10-30 12:00:00.123'::timestamp)",
	] {
		let msgs = exchange(&mut stream, &[simple_query_packet(sql)]).await;
		assert!(error_code(&msgs).is_none(), "{sql}: {:?}", error_code(&msgs));
	}
	let numeric = (10 << 16 | 2) + 4;
	let expected = vec![
		("v".to_owned(), 1043, 24),
		("c".to_owned(), 1042, 9),
		("n".to_owned(), 1700, numeric),
		("ts".to_owned(), 1114, 3),
	];

	// A listed column is described by its declared type and modifier, and a character(n) value
	// is padded to n.
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT v, c, n, ts FROM decl")]).await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	assert_eq!(row_description_typmods(&msgs), expected);
	let rows = data_rows(&msgs);
	assert_eq!(rows[0][0].as_deref(), Some("ab"));
	assert_eq!(rows[0][1].as_deref(), Some("xy   "));

	// So is a column of a `*`.
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT * FROM decl")]).await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	let described: Vec<_> =
		row_description_typmods(&msgs).into_iter().filter(|(name, _, _)| name != "id").collect();
	let mut sorted = expected.clone();
	sorted.sort();
	let mut described_sorted = described.clone();
	described_sorted.sort();
	assert_eq!(described_sorted, sorted);

	// The extended protocol describes the same, and the binary form of a character(n) is padded.
	let msgs = extended(&mut stream, "SELECT c, v FROM decl WHERE id = 1", &[], &[1, 1], &[]).await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	assert_eq!(
		row_description_typmods(&msgs),
		vec![("c".to_owned(), 1042, 9), ("v".to_owned(), 1043, 24)]
	);
	assert_eq!(data_rows(&msgs)[0][0].as_deref(), Some("xy   "));

	// The catalog reports the modifier through atttypmod and format_type; a field's attnum follows
	// the order of the field names.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet(
			"SELECT a.attname, a.atttypmod, format_type(a.atttypid, a.atttypmod) FROM \
			 pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'decl' AND \
			 a.attnum > 0 ORDER BY a.attnum",
		)],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	let row = |name: &str, typmod: i32, format: &str| {
		vec![Some(name.to_owned()), Some(typmod.to_string()), Some(format.to_owned())]
	};
	assert_eq!(
		data_rows(&msgs),
		vec![
			row("id", -1, "integer"),
			row("c", 9, "character(5)"),
			row("n", numeric, "numeric(10,2)"),
			row("ts", 3, "timestamp(3) without time zone"),
			row("v", 24, "character varying(20)"),
		]
	);

	// information_schema.columns lists the lengths, precision, scale and datetime precision.
	let msgs = exchange(
		&mut stream,
		&[simple_query_packet(
			"SELECT column_name, data_type, character_maximum_length, numeric_precision, \
			 numeric_scale, datetime_precision, udt_name FROM information_schema.columns WHERE \
			 table_name = 'decl' AND column_name <> 'id' ORDER BY ordinal_position",
		)],
	)
	.await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	let cells = |values: [Option<&str>; 7]| values.map(|v| v.map(str::to_owned)).to_vec();
	assert_eq!(
		data_rows(&msgs),
		vec![
			cells([Some("c"), Some("character"), Some("5"), None, None, None, Some("bpchar")]),
			cells([Some("n"), Some("numeric"), None, Some("10"), Some("2"), None, Some("numeric")]),
			cells([
				Some("ts"),
				Some("timestamp without time zone"),
				None,
				None,
				None,
				Some("3"),
				Some("timestamp"),
			]),
			cells([
				Some("v"),
				Some("character varying"),
				Some("20"),
				None,
				None,
				None,
				Some("varchar")
			]),
		]
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_a_mysql_declared_column_is_reported_as_the_nearest_postgres_type() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		"DEFINE TABLE mydecl SCHEMAFULL; DEFINE FIELD dt ON mydecl TYPE datetime SQL TYPE MYSQL \
		 'datetime(3)'; DEFINE FIELD d ON mydecl TYPE decimal SQL TYPE MYSQL 'decimal(10,2)'; \
		 DEFINE FIELD y ON mydecl TYPE int SQL TYPE MYSQL 'year'; CREATE mydecl:1 SET dt = \
		 d'2026-10-30T12:00:00.123Z', d = 1.25dec, y = 2026;",
	)
	.await;
	let msgs = exchange(&mut stream, &[simple_query_packet("SELECT dt, d, y FROM mydecl")]).await;
	assert!(error_code(&msgs).is_none(), "{:?}", error_code(&msgs));
	assert_eq!(
		row_description_typmods(&msgs),
		vec![
			("dt".to_owned(), 1114, 3),
			("d".to_owned(), 1700, (10 << 16 | 2) + 4),
			("y".to_owned(), 21, -1),
		]
	);
	assert_eq!(
		data_rows(&msgs),
		vec![vec![
			Some("2026-10-30 12:00:00.123".to_owned()),
			Some("1.25".to_owned()),
			Some("2026".to_owned())
		]]
	);
	shutdown.cancel();
}
