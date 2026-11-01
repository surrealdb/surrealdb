//! What a client reads after a statement of a simple query: a command answers with its command
//! tag alone, a query with its rows, and a string that holds no statement with an empty query
//! response. A driver reads the affected-row count from the tag and decides from the first message
//! whether the statement returned rows.

#![recursion_limit = "256"]

use std::net::SocketAddr;
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

async fn start_test_pg_server() -> (SocketAddr, CancellationToken) {
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
async fn connect(addr: SocketAddr) -> TcpStream {
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

/// What the server sent for one simple query: every message up to `ReadyForQuery`, and the
/// transaction status that message reports.
struct Answer {
	messages: Vec<(u8, Vec<u8>)>,
	status: u8,
}

impl Answer {
	/// The type of each message, in order.
	fn kinds(&self) -> Vec<u8> {
		self.messages.iter().map(|(kind, _)| *kind).collect()
	}

	/// The command tag of each `CommandComplete`, in order.
	fn tags(&self) -> Vec<String> {
		self.messages
			.iter()
			.filter(|(kind, _)| *kind == b'C')
			.map(|(_, body)| String::from_utf8_lossy(body).trim_end_matches('\0').to_string())
			.collect()
	}

	/// The text of the first column of each `DataRow`, in order, `NULL` for a NULL.
	fn first_cells(&self) -> Vec<String> {
		self.messages
			.iter()
			.filter(|(kind, _)| *kind == b'D')
			.map(|(_, body)| {
				let len = i32::from_be_bytes(body[2..6].try_into().expect("a cell length"));
				match usize::try_from(len) {
					Ok(len) => String::from_utf8_lossy(&body[6..6 + len]).into_owned(),
					Err(_) => "NULL".to_string(),
				}
			})
			.collect()
	}
}

/// Sends `sql` as one simple query and reads its answer.
async fn simple_query(stream: &mut TcpStream, sql: &str) -> Answer {
	let mut packet = BytesMut::new();
	packet.put_u8(b'Q');
	packet.put_i32((sql.len() + 5) as i32);
	packet.put_slice(sql.as_bytes());
	packet.put_u8(0);
	stream.write_all(&packet).await.expect("Failed to send the query");

	let mut messages = Vec::new();
	loop {
		let (kind, body) = read_message(stream).await;
		if kind == b'Z' {
			return Answer {
				messages,
				status: body[0],
			};
		}
		messages.push((kind, body));
	}
}

/// Every command that returns no rows answers with its command tag alone, the one PostgreSQL
/// answers it with. `tag_t` is the table the statements define, change and drop in turn.
#[tokio::test]
async fn test_a_command_answers_with_its_command_tag_alone() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	for (sql, tag) in [
		// A table with its columns and a primary key lowers to several statements.
		("CREATE TABLE tag_t (k int PRIMARY KEY, name text, extra int)", "CREATE TABLE"),
		("CREATE TABLE IF NOT EXISTS tag_t (k int)", "CREATE TABLE"),
		("CREATE INDEX tag_t_name ON tag_t (name)", "CREATE INDEX"),
		("ALTER TABLE tag_t ADD COLUMN a int, ADD COLUMN b int", "ALTER TABLE"),
		// An action the engine has no statement for still answers.
		("ALTER TABLE tag_t RENAME COLUMN a TO c", "ALTER TABLE"),
		("CREATE TABLE tag_base (k int)", "CREATE TABLE"),
		("CREATE VIEW tag_v AS SELECT k FROM tag_base", "CREATE VIEW"),
		("CREATE SCHEMA tag_schema", "CREATE SCHEMA"),
		("INSERT INTO tag_t (k, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')", "INSERT 0 3"),
		("UPDATE tag_t SET name = 'z' WHERE k < 3", "UPDATE 2"),
		("UPDATE tag_t SET name = 'q' WHERE k > 100", "UPDATE 0"),
		("DELETE FROM tag_t WHERE k = 3", "DELETE 1"),
		("DELETE FROM tag_t WHERE k > 100", "DELETE 0"),
		("TRUNCATE TABLE tag_t", "TRUNCATE TABLE"),
		("DROP TABLE tag_t", "DROP TABLE"),
		("DROP TABLE IF EXISTS tag_t", "DROP TABLE"),
		("DROP SCHEMA IF EXISTS tag_schema", "DROP SCHEMA"),
		("DROP SCHEMA public CASCADE", "DROP SCHEMA"),
	] {
		let answer = simple_query(&mut stream, sql).await;
		assert_eq!(answer.kinds(), vec![b'C'], "{sql}: expected a lone CommandComplete");
		assert_eq!(answer.tags(), vec![tag.to_string()], "{sql}");
		assert_eq!(answer.status, b'I', "{sql}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_a_command_with_returning_streams_its_rows_and_answers_with_its_own_tag() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;
	simple_query(&mut stream, "CREATE TABLE ret_t (k int PRIMARY KEY, name text)").await;

	for (sql, rows, tag) in [
		("INSERT INTO ret_t (k, name) VALUES (1, 'a'), (2, 'b') RETURNING k", 2, "INSERT 0 2"),
		("UPDATE ret_t SET name = 'z' RETURNING k", 2, "UPDATE 2"),
		("UPDATE ret_t SET name = 'q' WHERE k > 100 RETURNING k", 0, "UPDATE 0"),
		("DELETE FROM ret_t WHERE k = 1 RETURNING k", 1, "DELETE 1"),
		("DELETE FROM ret_t WHERE k > 100 RETURNING k", 0, "DELETE 0"),
		("SELECT k FROM ret_t", 1, "SELECT 1"),
	] {
		let answer = simple_query(&mut stream, sql).await;
		let mut expected = vec![b'T'];
		expected.extend(std::iter::repeat_n(b'D', rows));
		expected.push(b'C');
		assert_eq!(answer.kinds(), expected, "{sql}");
		assert_eq!(answer.tags(), vec![tag.to_string()], "{sql}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_each_statement_of_a_batch_is_answered_with_its_own_tag() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	let answer = simple_query(
		&mut stream,
		"CREATE TABLE batch_t (k int PRIMARY KEY); \
		 INSERT INTO batch_t (k) VALUES (1); \
		 INSERT INTO batch_t (k) VALUES (2), (3); \
		 SELECT k FROM batch_t ORDER BY k; \
		 DELETE FROM batch_t",
	)
	.await;
	assert_eq!(
		answer.kinds(),
		vec![b'C', b'C', b'C', b'T', b'D', b'D', b'D', b'C', b'C'],
		"each command answers with a lone CommandComplete and the query with its rows"
	);
	assert_eq!(
		answer.tags(),
		vec!["CREATE TABLE", "INSERT 0 1", "INSERT 0 2", "SELECT 3", "DELETE 3"]
	);
	assert_eq!(answer.first_cells(), vec!["1", "2", "3"]);

	shutdown.cancel();
}

#[tokio::test]
async fn test_a_failed_command_answers_with_its_error_alone() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;
	simple_query(&mut stream, "CREATE TABLE fail_t (k int)").await;

	let answer = simple_query(&mut stream, "CREATE TABLE fail_t (k int)").await;
	assert_eq!(answer.kinds(), vec![b'E'], "a table that exists cannot be created again");
	assert_eq!(answer.status, b'I');

	let answer = simple_query(&mut stream, "DROP TABLE fail_missing").await;
	assert_eq!(answer.kinds(), vec![b'E'], "a table that does not exist cannot be dropped");
	assert_eq!(answer.status, b'I');

	// A definition that fails after the ones before it answers with no tag for them.
	let answer = simple_query(&mut stream, "CREATE TABLE fail_cols (a int, a int)").await;
	assert_eq!(answer.kinds(), vec![b'E'], "a column cannot be defined twice");
	assert_eq!(answer.status, b'I');

	// Inside a transaction the failure aborts it, and the commands after it are refused.
	let answer = simple_query(&mut stream, "BEGIN").await;
	assert_eq!(answer.status, b'T');
	let answer = simple_query(&mut stream, "CREATE TABLE fail_t (k int)").await;
	assert_eq!(answer.kinds(), vec![b'E']);
	assert_eq!(answer.status, b'E');
	let answer = simple_query(&mut stream, "DROP TABLE fail_t").await;
	assert_eq!(answer.kinds(), vec![b'E']);
	assert_eq!(answer.status, b'E');
	let answer = simple_query(&mut stream, "ROLLBACK").await;
	assert_eq!(answer.tags(), vec!["ROLLBACK"]);
	assert_eq!(answer.status, b'I');

	shutdown.cancel();
}

#[tokio::test]
async fn test_a_command_in_a_transaction_answers_with_its_tag_and_keeps_the_status() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	let answer = simple_query(&mut stream, "BEGIN").await;
	assert_eq!((answer.tags(), answer.status), (vec!["BEGIN".to_string()], b'T'));
	for (sql, tag) in [
		("CREATE TABLE txn_t (k int)", "CREATE TABLE"),
		("INSERT INTO txn_t (k) VALUES (1), (2)", "INSERT 0 2"),
		("DELETE FROM txn_t WHERE k = 1", "DELETE 1"),
	] {
		let answer = simple_query(&mut stream, sql).await;
		assert_eq!(answer.kinds(), vec![b'C'], "{sql}");
		assert_eq!(answer.tags(), vec![tag.to_string()], "{sql}");
		assert_eq!(answer.status, b'T', "{sql}");
	}
	let answer = simple_query(&mut stream, "COMMIT").await;
	assert_eq!((answer.tags(), answer.status), (vec!["COMMIT".to_string()], b'I'));

	shutdown.cancel();
}

#[tokio::test]
async fn test_a_string_with_no_statement_answers_as_an_empty_query() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	for sql in [
		"",
		" ",
		";",
		"  ;  ;",
		"-- a comment",
		"/* a comment */ ;",
		"/**/",
		"/* a /* nested */ comment */",
		"; -- a comment\n;",
		// PostgreSQL reads a vertical tab and a form feed as white space, so they separate
		// comments and statements like a space does.
		"\x0b",
		"\x0b;\x0b",
		"/* a */\x0b/* b */",
		"/* a */\x0c/* b */",
		"-- a\n\x0b-- b",
	] {
		let answer = simple_query(&mut stream, sql).await;
		assert_eq!(answer.kinds(), vec![b'I'], "{sql:?}: expected an EmptyQueryResponse");
		assert_eq!(answer.status, b'I', "{sql:?}");
	}

	// A comment that does not end is a syntax error, not an empty query.
	for sql in ["/* a comment", "/* a /* nested */ comment"] {
		let answer = simple_query(&mut stream, sql).await;
		assert_eq!(answer.kinds(), vec![b'E'], "{sql:?}");
		assert_eq!(answer.status, b'I', "{sql:?}");
	}

	// Empty statements around a statement are skipped and add no answer.
	for sql in [
		"; SELECT 1",
		"SELECT 1;",
		"SELECT 1; ;",
		"SELECT 1; -- a comment",
		"SELECT 1; /* a */\x0b/* b */",
		"\x0b; SELECT 1;\x0b",
		"SET application_name = 'tags'; ;",
	] {
		let answer = simple_query(&mut stream, sql).await;
		let kinds = answer.kinds();
		assert!(!kinds.contains(&b'I') && !kinds.contains(&b'E'), "{sql:?}: {kinds:?}");
		assert_eq!(answer.tags().len(), 1, "{sql:?}");
	}
	let answer = simple_query(&mut stream, "SELECT 1;;SELECT 2").await;
	assert_eq!(answer.tags(), vec!["SELECT 1", "SELECT 1"]);
	assert_eq!(answer.first_cells(), vec!["1", "2"]);

	shutdown.cancel();
}

/// The splitter that cuts a simple query into statements reads a backslash as an escape inside a
/// quoted string; the parser does not. A string it cannot cut stays one piece holding several
/// statements, which cannot be matched to the results one by one: their rows must still arrive.
#[tokio::test]
async fn test_a_piece_of_several_statements_does_not_lose_the_rows_of_a_query() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect(addr).await;

	simple_query(&mut stream, "CREATE TABLE piece_t (k int)").await;
	let answer =
		simple_query(&mut stream, r"SELECT 'a\'; INSERT INTO piece_t (k) VALUES (1)").await;
	assert_eq!(answer.first_cells().first().map(String::as_str), Some(r"a\"));
	assert_eq!(answer.kinds().last(), Some(&b'C'));
	assert_eq!(answer.status, b'I');

	shutdown.cancel();
}
