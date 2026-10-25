//! The SQLSTATE a client reads in the ErrorResponse of a failed statement, over the simple and the
//! extended protocol.

#![recursion_limit = "256"]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use jsonwebtoken::{EncodingKey, Header, encode};
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::token::Claims;
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Bound on every network exchange, so a server that stops answering fails the test instead of
/// hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// An ErrorResponse: the SQLSTATE (`C`), the message (`M`) and the detail (`D`).
#[derive(Debug)]
struct ErrorResponse {
	code: String,
	message: String,
	detail: Option<String>,
}

impl ErrorResponse {
	fn parse(payload: &[u8]) -> Self {
		let mut error = Self {
			code: String::new(),
			message: String::new(),
			detail: None,
		};
		for field in payload.split(|byte| *byte == 0).filter(|field| !field.is_empty()) {
			let value = String::from_utf8_lossy(&field[1..]).into_owned();
			match field[0] {
				b'C' => error.code = value,
				b'M' => error.message = value,
				b'D' => error.detail = Some(value),
				_ => {}
			}
		}
		error
	}
}

fn message(kind: u8, body: &[u8]) -> BytesMut {
	let mut message = BytesMut::new();
	message.put_u8(kind);
	message.put_i32(body.len() as i32 + 4);
	message.put_slice(body);
	message
}

async fn start(ds: Arc<Datastore>) -> (SocketAddr, CancellationToken) {
	let shutdown = CancellationToken::new();
	let addr = surrealdb_pgwire::start(
		"127.0.0.1:0".parse().expect("loopback address"),
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("start the postgres listener");
	(addr, shutdown)
}

/// Connects to `test/test` as `user`, answering a request for a cleartext password with
/// `password`.
async fn connect(addr: SocketAddr, user: &str, password: &str) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let mut body = BytesMut::new();
	body.put_i32(196608);
	for (name, value) in
		[("user", user), ("database", "test/test"), ("options", "-c dialect=pgsql")]
	{
		body.put_slice(name.as_bytes());
		body.put_u8(0);
		body.put_slice(value.as_bytes());
		body.put_u8(0);
	}
	body.put_u8(0);
	let mut startup = BytesMut::new();
	startup.put_i32(body.len() as i32 + 4);
	startup.put(body);
	stream.write_all(&startup).await.expect("send the startup message");
	timeout(IO_TIMEOUT, async {
		loop {
			let kind = stream.read_u8().await.expect("a message type");
			let length = stream.read_i32().await.expect("a message length");
			let mut payload = vec![0u8; usize::try_from(length - 4).expect("a message length")];
			stream.read_exact(&mut payload).await.expect("a message body");
			match kind {
				// AuthenticationCleartextPassword
				b'R' if payload.starts_with(&3i32.to_be_bytes()) => {
					let mut body = BytesMut::new();
					body.put_slice(password.as_bytes());
					body.put_u8(0);
					stream.write_all(&message(b'p', &body)).await.expect("send the password");
				}
				b'E' => panic!("the startup failed: {:?}", ErrorResponse::parse(&payload)),
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.expect("the server answered the startup");
	stream
}

/// What the server answered to a statement: its rows, as text, and its ErrorResponses.
struct Reply {
	rows: Vec<Vec<Option<String>>>,
	errors: Vec<ErrorResponse>,
}

/// A DataRow: the number of columns, then each as its length (-1 for NULL) and its text.
fn data_row(payload: &[u8]) -> Vec<Option<String>> {
	let columns = i16::from_be_bytes([payload[0], payload[1]]);
	let mut at = 2;
	let mut row = Vec::new();
	for _ in 0..columns {
		let length = i32::from_be_bytes(payload[at..at + 4].try_into().expect("a length"));
		at += 4;
		match usize::try_from(length) {
			Ok(length) => {
				row.push(Some(String::from_utf8_lossy(&payload[at..at + length]).into_owned()));
				at += length;
			}
			Err(_) => row.push(None),
		}
	}
	row
}

/// Reads messages up to the next ReadyForQuery.
async fn read_until_ready(stream: &mut TcpStream) -> Reply {
	let mut reply = Reply {
		rows: Vec::new(),
		errors: Vec::new(),
	};
	timeout(IO_TIMEOUT, async {
		loop {
			let kind = stream.read_u8().await.expect("a message type");
			let length = stream.read_i32().await.expect("a message length");
			let mut payload = vec![0u8; usize::try_from(length - 4).expect("a message length")];
			stream.read_exact(&mut payload).await.expect("a message body");
			match kind {
				b'Z' => break,
				b'D' => reply.rows.push(data_row(&payload)),
				b'E' => reply.errors.push(ErrorResponse::parse(&payload)),
				_ => {}
			}
		}
	})
	.await
	.expect("the server answered");
	reply
}

/// Runs `sql` as a simple query.
async fn simple(stream: &mut TcpStream, sql: &str) -> Reply {
	let mut body = BytesMut::new();
	body.put_slice(sql.as_bytes());
	body.put_u8(0);
	stream.write_all(&message(b'Q', &body)).await.expect("send the query");
	read_until_ready(stream).await
}

/// Runs `sql` as an unnamed prepared statement with the text parameters `params`, where `None` is
/// NULL.
async fn prepared(stream: &mut TcpStream, sql: &str, params: &[Option<&str>]) -> Reply {
	let mut parse = BytesMut::new();
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(0);
	let mut bind = BytesMut::new();
	bind.put_u8(0);
	bind.put_u8(0);
	bind.put_i16(0);
	bind.put_i16(params.len() as i16);
	for param in params {
		match param {
			Some(text) => {
				bind.put_i32(text.len() as i32);
				bind.put_slice(text.as_bytes());
			}
			None => bind.put_i32(-1),
		}
	}
	bind.put_i16(0);
	let mut execute = BytesMut::new();
	execute.put_u8(0);
	execute.put_i32(0);
	let mut batch = BytesMut::new();
	batch.put(message(b'P', &parse));
	batch.put(message(b'B', &bind));
	batch.put(message(b'E', &execute));
	batch.put(message(b'S', &[]));
	stream.write_all(&batch).await.expect("send the statement");
	read_until_ready(stream).await
}

/// Connects to a server with two tables: one keyed by `id` with a UNIQUE `email` and a NOT NULL
/// `name`, and one keyed by the pair `(a, b)`.
async fn with_tables() -> (TcpStream, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("datastore");
	let (addr, shutdown) = start(ds).await;
	let mut stream = connect(addr, "root", "").await;
	for sql in [
		"CREATE TABLE sqlstate_people (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL, age INT, active BOOLEAN)",
		"INSERT INTO sqlstate_people (id, email, name, age) VALUES (1, 'ada@example.com', 'Ada', 36), (2, 'bob@example.com', 'Bob', 41)",
		"CREATE TABLE sqlstate_pairs (a INT, b INT, name TEXT NOT NULL, PRIMARY KEY (a, b))",
		"INSERT INTO sqlstate_pairs (a, b, name) VALUES (1, 2, 'first')",
	] {
		let errors = simple(&mut stream, sql).await.errors;
		assert!(errors.is_empty(), "{sql}: {errors:?}");
	}
	(stream, shutdown)
}

/// Asserts that `sql` fails with exactly one error, which has the SQLSTATE `code`.
fn assert_sqlstate(sql: &str, errors: &[ErrorResponse], code: &str) {
	let [error] = errors else {
		panic!("{sql}: expected one error, got {errors:?}");
	};
	assert_eq!(error.code, code, "{sql}: {}", error.message);
}

#[tokio::test]
async fn a_failed_statement_reports_the_sqlstate_of_its_kind() {
	let (mut stream, shutdown) = with_tables().await;

	for (sql, code) in [
		// what the planner refuses or finds wrong in the statement
		("SELECT no_such_fn(1)", "42883"),
		("SELECT * FROM sqlstate_no_such_table", "42P01"),
		("SELEC 1", "42601"),
		("SELECT 1 / 0", "22012"),
		("SELECT 'abc'::int", "22P02"),
		("SELECT 1 FROM sqlstate_people WHERE row_number() OVER () > 1", "42P20"),
		// a key that is already stored, by the record id, by a UNIQUE index and by a composite key
		(
			"INSERT INTO sqlstate_people (id, email, name) VALUES (1, 'new@example.com', 'Eve')",
			"23505",
		),
		(
			"INSERT INTO sqlstate_people (id, email, name) VALUES (3, 'ada@example.com', 'Eve')",
			"23505",
		),
		("UPDATE sqlstate_people SET email = 'ada@example.com' WHERE id = 2", "23505"),
		("INSERT INTO sqlstate_pairs (a, b, name) VALUES (1, 2, 'again')", "23505"),
		// no value for a column that must have one
		(
			"INSERT INTO sqlstate_people (id, email, name) VALUES (3, 'eve@example.com', NULL)",
			"23502",
		),
		("INSERT INTO sqlstate_people (id, email) VALUES (3, 'eve@example.com')", "23502"),
		("UPDATE sqlstate_people SET name = NULL WHERE id = 2", "23502"),
		// a value of another type than its column's
		("INSERT INTO sqlstate_people (id, name, age) VALUES (3, 'Eve', 'old')", "42804"),
		("UPDATE sqlstate_people SET age = true WHERE id = 2", "42804"),
		("UPDATE sqlstate_people SET active = 5 WHERE id = 2", "42804"),
	] {
		assert_sqlstate(sql, &simple(&mut stream, sql).await.errors, code);
	}

	// The failed statements changed nothing, and the connection is still usable.
	let reply =
		simple(&mut stream, "SELECT id, email, name, age FROM sqlstate_people ORDER BY id").await;
	assert!(reply.errors.is_empty(), "{:?}", reply.errors);
	let row = |cells: [&str; 4]| cells.map(|cell| Some(cell.to_string())).to_vec();
	assert_eq!(
		reply.rows,
		[row(["1", "ada@example.com", "Ada", "36"]), row(["2", "bob@example.com", "Bob", "41"])]
	);

	shutdown.cancel();
}

#[tokio::test]
async fn a_value_in_a_refused_statement_does_not_choose_the_sqlstate() {
	let (mut stream, shutdown) = with_tables().await;

	// Each text is wording that the engine's own messages contain, written where an integer
	// belongs. The refusal quotes it in its message, but what was refused is its type.
	for text in [
		"division by zero",
		"not null",
		"check constraint",
		"foreign key",
		"already contains",
		"already exists",
		"timed out",
		"conflict",
		"database",
		"Database index `sqlstate_people_email_uniq` already contains 1, with record `sqlstate_people:1`",
		"Couldn't coerce value for field `name` of `sqlstate_people:1`: Expected `string` but found `NULL`",
	] {
		let literal = text.replace('\'', "''");
		let sql = format!("UPDATE sqlstate_people SET age = '{literal}' WHERE id = 2");
		assert_sqlstate(&sql, &simple(&mut stream, &sql).await.errors, "42804");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn a_failed_prepared_statement_reports_the_sqlstate_of_its_kind() {
	let (mut stream, shutdown) = with_tables().await;

	for (sql, params, code) in [
		// what the planner finds wrong in the statement
		("SELECT no_such_fn(1)", vec![], "42883"),
		("SELECT now(1, 2, 3)", vec![], "42883"),
		("SELECT row_number()", vec![], "42809"),
		("SELECT 1 FROM sqlstate_people WHERE row_number() OVER () > 1", vec![], "42P20"),
		("SELECT no_such_column", vec![], "42703"),
		("SELECT t9.id FROM sqlstate_people", vec![], "42P01"),
		("SELECT * FROM sqlstate_people ORDER BY 9", vec![], "42P10"),
		("SELECT * FROM sqlstate_no_such_table", vec![], "42P01"),
		("SELECT 1 / 0", vec![], "22012"),
		("SELECT $1::int", vec![Some("abc")], "22P02"),
		// a key that is already stored
		(
			"INSERT INTO sqlstate_people (id, email, name) VALUES ($1, $2, $3)",
			vec![Some("1"), Some("new@example.com"), Some("Eve")],
			"23505",
		),
		(
			"INSERT INTO sqlstate_people (id, email, name) VALUES ($1, $2, $3)",
			vec![Some("3"), Some("ada@example.com"), Some("Eve")],
			"23505",
		),
		(
			"UPDATE sqlstate_people SET email = $1 WHERE id = 2",
			vec![Some("ada@example.com")],
			"23505",
		),
		(
			"INSERT INTO sqlstate_pairs (a, b, name) VALUES ($1, $2, $3)",
			vec![Some("1"), Some("2"), Some("again")],
			"23505",
		),
		// no value for a column that must have one
		(
			"INSERT INTO sqlstate_people (id, email, name) VALUES ($1, $2, $3)",
			vec![Some("3"), Some("eve@example.com"), None],
			"23502",
		),
		// a value of another type than its column's
		(
			"INSERT INTO sqlstate_people (id, email, name, active) VALUES ($1, $2, $3, 5)",
			vec![Some("3"), Some("eve@example.com"), Some("Eve")],
			"42804",
		),
	] {
		assert_sqlstate(sql, &prepared(&mut stream, sql, &params).await.errors, code);
	}

	shutdown.cancel();
}

#[tokio::test]
async fn a_unique_violation_does_not_name_a_record_the_caller_may_not_read() {
	let ds =
		Datastore::builder().with_auth(true).build_with_path("memory").await.expect("datastore");
	let secret = "secret-key-that-is-at-least-32-characters-long!";
	let owner = Session::owner().with_ns("test").with_db("test");
	let defined = ds
		.execute(
			&format!(
				"DEFINE NAMESPACE test;
				DEFINE DATABASE test;
				DEFINE ACCESS token ON DATABASE TYPE RECORD
					WITH JWT ALGORITHM HS256 KEY '{secret}' DURATION FOR SESSION 30d;
				DEFINE TABLE sqlstate_vault SCHEMALESS PERMISSIONS FOR select NONE, FOR create FULL;
				DEFINE INDEX sqlstate_vault_slug ON sqlstate_vault FIELDS slug UNIQUE;
				CREATE sqlstate_vault:1 SET slug = 'held-by-a-record-nobody-may-read';"
			),
			&owner,
			None,
		)
		.await
		.expect("define the vault");
	for response in defined {
		response.result.expect("a definition");
	}
	let (addr, shutdown) = start(ds).await;

	let claims = Claims {
		iss: Some("surrealdb-test".to_string()),
		iat: Some(chrono::Utc::now().timestamp()),
		nbf: Some(chrono::Utc::now().timestamp()),
		exp: Some((chrono::Utc::now() + chrono::Duration::hours(1)).timestamp()),
		ns: Some("test".to_string()),
		db: Some("test".to_string()),
		ac: Some("token".to_string()),
		id: Some("user:alice".to_string()),
		..Claims::default()
	};
	let token = encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes()))
		.expect("a token");
	let mut stream = connect(addr, "user:alice", &token).await;

	let sql =
		"INSERT INTO sqlstate_vault (id, slug) VALUES (2, 'held-by-a-record-nobody-may-read')";
	let errors = simple(&mut stream, sql).await.errors;
	assert_sqlstate(sql, &errors, "23505");
	let message = &errors[0].message;
	assert!(message.contains("conflicting value in another record"), "{message}");
	assert!(!message.contains("held-by-a-record-nobody-may-read"), "{message}");
	assert!(!message.contains("sqlstate_vault:1"), "{message}");
	assert_eq!(errors[0].detail, None);

	shutdown.cancel();
}
