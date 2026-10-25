//! `LIKE` and `ILIKE` over the PostgreSQL wire when the client binds the pattern, which is the
//! form every driver sends.

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

/// The OID of `text`.
const TEXT: i32 = 25;

/// Bound on every network exchange, so a server that stops answering fails the test instead of
/// hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

async fn start() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("datastore");
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

async fn connect(addr: std::net::SocketAddr) -> TcpStream {
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
	let mut startup = BytesMut::new();
	startup.put_i32(body.len() as i32 + 4);
	startup.put(body);
	stream.write_all(&startup).await.expect("send the startup message");
	timeout(IO_TIMEOUT, async {
		loop {
			let (kind, payload) = read_message(&mut stream).await;
			match kind {
				b'E' => panic!("the startup failed: {}", error_message(&payload)),
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.expect("the server answered the startup");
	stream
}

async fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	let kind = stream.read_u8().await.expect("a message type");
	let length = stream.read_i32().await.expect("a message length");
	let mut payload = vec![0u8; usize::try_from(length - 4).expect("a message length")];
	stream.read_exact(&mut payload).await.expect("a message body");
	(kind, payload)
}

/// The text of the `M` field of an ErrorResponse.
fn error_message(payload: &[u8]) -> String {
	payload
		.split(|byte| *byte == 0)
		.find(|field| field.first() == Some(&b'M'))
		.map(|field| String::from_utf8_lossy(&field[1..]).into_owned())
		.unwrap_or_default()
}

type Row = Vec<Option<String>>;

/// A DataRow: the number of columns, then each as its length (-1 for NULL) and its text.
fn data_row(mut payload: &[u8]) -> Row {
	let columns = payload.get_i16();
	(0..columns)
		.map(|_| {
			let length = payload.get_i32();
			let length = usize::try_from(length).ok()?;
			let text = String::from_utf8_lossy(&payload[..length]).into_owned();
			payload.advance(length);
			Some(text)
		})
		.collect()
}

fn message(kind: u8, body: &[u8]) -> BytesMut {
	let mut message = BytesMut::new();
	message.put_u8(kind);
	message.put_i32(body.len() as i32 + 4);
	message.put_slice(body);
	message
}

/// Runs `sql` through the extended protocol with the parameters bound in text format, and gives
/// its rows, or the message of its ErrorResponse. The connection stays usable after an error.
async fn extended(
	stream: &mut TcpStream,
	sql: &str,
	param_types: &[i32],
	params: &[Option<&str>],
) -> Result<Vec<Row>, String> {
	let mut parse = BytesMut::new();
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(param_types.len() as i16);
	for oid in param_types {
		parse.put_i32(*oid);
	}
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
	for packet in
		[message(b'P', &parse), message(b'B', &bind), message(b'E', &execute), message(b'S', &[])]
	{
		stream.write_all(&packet).await.expect("send");
	}
	timeout(IO_TIMEOUT, async {
		let mut rows = Vec::new();
		let mut error = None;
		loop {
			let (kind, payload) = read_message(stream).await;
			match kind {
				b'D' => rows.push(data_row(&payload)),
				b'E' => error = Some(error_message(&payload)),
				b'Z' => break,
				_ => {}
			}
		}
		error.map_or(Ok(rows), Err)
	})
	.await
	.unwrap_or_else(|_| panic!("the server answered `{sql}`"))
}

/// `subject LIKE pattern` and `subject ILIKE pattern` as PostgreSQL 17 answers them.
const CASES: &[(&str, &str, &str, &str)] = &[
	// Wildcards.
	("Alice", "A%", "t", "t"),
	("Bob", "A%", "f", "f"),
	("Alice", "_lice", "t", "t"),
	("Alice", "%lic%", "t", "t"),
	("ab", "_", "f", "f"),
	("", "", "t", "t"),
	("", "%", "t", "t"),
	("x", "", "f", "f"),
	("abc", "%", "t", "t"),
	("a", "a ", "f", "f"),
	// Case.
	("Alice", "alice", "f", "t"),
	("A", "a", "f", "t"),
	// A backslash makes the next character literal.
	("50%", "50\\%", "t", "t"),
	("500", "50\\%", "f", "f"),
	("a_b", "a\\_b", "t", "t"),
	("axb", "a\\_b", "f", "f"),
	("%", "\\%", "t", "t"),
	("_", "\\_", "t", "t"),
	("a%c", "a\\%%", "t", "t"),
	("a_", "a\\_", "t", "t"),
	("ab", "a\\_", "f", "f"),
	// A doubled backslash is one backslash.
	("a\\b", "a\\\\b", "t", "t"),
	("a\\xyz", "a\\\\%", "t", "t"),
	("axyz", "a\\\\%", "f", "f"),
	("a\\", "a\\\\", "t", "t"),
	("\\", "\\\\", "t", "t"),
	("\\\\", "\\\\\\\\", "t", "t"),
	("\\", "\\\\\\\\", "f", "f"),
	// A backslash before any other character leaves that character itself.
	("axb", "a\\xb", "t", "t"),
	("a\\xb", "a\\xb", "f", "f"),
	("a", "\\a", "t", "t"),
	// Characters that mean something to a regular expression mean nothing here.
	("abc", "a.c", "f", "f"),
	("a.c", "a.c", "t", "t"),
	("aab", "a+b", "f", "f"),
	("a+b", "a+b", "t", "t"),
	("(x)", "(x)", "t", "t"),
	("x", "(x)", "f", "f"),
	("[x]", "[x]", "t", "t"),
	("x", "[x]", "f", "f"),
	("a{1}", "a{1}", "t", "t"),
	("a", "a{1}", "f", "f"),
	("^x$", "^x$", "t", "t"),
	("x", "^x$", "f", "f"),
	("a|b", "a|b", "t", "t"),
	("a", "a|b", "f", "f"),
	("a*", "a*", "t", "t"),
	("aa", "a*", "f", "f"),
	("a?", "a?", "t", "t"),
	("a", "a?", "f", "f"),
	// A line break is a character like any other.
	("a\nb", "a%b", "t", "t"),
	("a\nb", "a_b", "t", "t"),
	("a\n", "a", "f", "f"),
	// A character is a character, whatever its width in bytes.
	("é", "_", "t", "t"),
	("éa", "_a", "t", "t"),
	("日本語", "日_語", "t", "t"),
	("日本語", "日%", "t", "t"),
];

#[tokio::test]
async fn a_bound_pattern_matches_as_postgresql_matches() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (subject, pattern, like, ilike) in CASES {
		for (operator, expected) in [("LIKE", like), ("ILIKE", ilike)] {
			let sql = format!("SELECT $1::text {operator} $2::text AS m;");
			let got = extended(&mut stream, &sql, &[TEXT, TEXT], &[Some(subject), Some(pattern)])
				.await
				.map(|rows| rows[0][0].clone());
			assert_eq!(
				got.as_ref().map(|cell| cell.as_deref()),
				Ok(Some(*expected)),
				"{subject:?} {operator} {pattern:?}"
			);
		}
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_bound_pattern_of_unspecified_type_matches_the_same() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (subject, pattern, like, _) in CASES {
		let got = extended(
			&mut stream,
			"SELECT $1::text LIKE $2 AS m;",
			&[],
			&[Some(subject), Some(pattern)],
		)
		.await
		.map(|rows| rows[0][0].clone());
		assert_eq!(got, Ok(Some((*like).to_string())), "{subject:?} LIKE {pattern:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn not_like_and_null_follow_the_pattern() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	let mut one = async |sql: &str, params: &[Option<&str>]| {
		extended(&mut stream, sql, &[TEXT; 2][..params.len()], params)
			.await
			.expect(sql)
			.remove(0)
			.remove(0)
	};
	let both = [Some("50%"), Some("50\\%")];
	assert_eq!(one("SELECT $1::text NOT LIKE $2::text;", &both).await.as_deref(), Some("f"));
	assert_eq!(one("SELECT $1::text NOT ILIKE $2::text;", &both).await.as_deref(), Some("f"));
	let other = [Some("500"), Some("50\\%")];
	assert_eq!(one("SELECT $1::text NOT LIKE $2::text;", &other).await.as_deref(), Some("t"));
	assert_eq!(one("SELECT $1::text LIKE $2::text;", &[Some("a"), None]).await, None);
	assert_eq!(one("SELECT $1::text LIKE $2::text;", &[None, Some("a")]).await, None);
	assert_eq!(one("SELECT $1::text NOT LIKE $2::text;", &[Some("a"), None]).await, None);
	shutdown.cancel();
}

#[tokio::test]
async fn a_bound_pattern_filters_the_rows_of_a_table() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	extended(&mut stream, "CREATE TABLE like_people (name text);", &[], &[])
		.await
		.expect("create the table");
	for name in ["Alice", "Alan", "Bob", "50% off", "500 off", "a_c", "abc", "a.c"] {
		extended(
			&mut stream,
			"INSERT INTO like_people (name) VALUES ($1);",
			&[TEXT],
			&[Some(name)],
		)
		.await
		.expect("insert a row");
	}
	for (pattern, expected) in [
		("A%", &["Alan", "Alice"][..]),
		("%l%", &["Alan", "Alice"]),
		("a_c", &["a.c", "a_c", "abc"]),
		("a\\_c", &["a_c"]),
		("a.c", &["a.c"]),
		("50\\%%", &["50% off"]),
		("50%", &["50% off", "500 off"]),
		// A bound pattern is a pattern, not a regular expression.
		(".*", &[]),
		("(", &[]),
		("[", &[]),
	] {
		let rows = extended(
			&mut stream,
			"SELECT name FROM like_people WHERE name LIKE $1 ORDER BY name;",
			&[TEXT],
			&[Some(pattern)],
		)
		.await
		.unwrap_or_else(|message| panic!("LIKE {pattern:?}: {message}"));
		let names: Vec<_> = rows.iter().map(|row| row[0].as_deref().expect("a name")).collect();
		assert_eq!(names, expected, "LIKE {pattern:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_pattern_that_ends_in_a_backslash_ends_in_that_backslash() {
	// PostgreSQL refuses such a pattern once a match reaches its end. Here the backslash has
	// nothing to escape and stands for itself, as it does in MySQL.
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (subject, pattern, expected) in
		[("a\\", "a\\", "t"), ("ab", "a\\", "f"), ("x", "a\\", "f"), ("a\\b", "a\\\\%", "t")]
	{
		let rows = extended(
			&mut stream,
			"SELECT $1::text LIKE $2::text;",
			&[TEXT, TEXT],
			&[Some(subject), Some(pattern)],
		)
		.await
		.expect("LIKE");
		assert_eq!(rows[0][0].as_deref(), Some(expected), "{subject:?} LIKE {pattern:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_pattern_too_long_to_convert_is_an_error_and_the_connection_survives() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for pattern in ["_".repeat(2_000_000), "%".repeat(2_000_000), "(".repeat(2_000_000)] {
		let outcome =
			extended(&mut stream, "SELECT 'a' LIKE $1::text;", &[TEXT], &[Some(&pattern)]).await;
		assert!(outcome.is_err(), "a pattern of {} characters was converted", pattern.len());
	}
	let rows = extended(&mut stream, "SELECT 'a' LIKE $1::text;", &[TEXT], &[Some("a")])
		.await
		.expect("a short pattern");
	assert_eq!(rows[0][0].as_deref(), Some("t"));
	shutdown.cancel();
}
