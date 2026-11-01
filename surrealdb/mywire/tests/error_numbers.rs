//! The number and SQLSTATE of the ERR packet that answers a failed statement.
//!
//! A client chooses what to do with a failure from these two fields, so each statement below is
//! checked against the pair MySQL 8.4 answers for the same condition. The message is the engine's
//! own text and is checked only where a test is about it.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("read the packet header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let mut payload = vec![0u8; len];
	stream.read_exact(&mut payload).await.expect("read the packet payload");
	payload
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("write the packet header");
	stream.write_all(payload).await.expect("write the packet payload");
}

/// Start a server whose datastore has a root user and a viewer user, as `root` and `viewer`.
async fn start_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("create the datastore");
	ds.run(QueryRequest::new(
		"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER; \
		 DEFINE USER viewer ON ROOT PASSWORD 'viewer' ROLES VIEWER;",
		&Session::owner(),
	))
	.await
	.expect("define the users");
	let shutdown = CancellationToken::new();
	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().expect("parse the address"),
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("start the server");
	(addr, shutdown)
}

async fn connect(addr: std::net::SocketAddr, user: &str) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let handshake = read_packet(&mut stream).await;
	assert_eq!(handshake[0], 10, "HandshakeV10");
	let mut pos = 1;
	while handshake[pos] != 0 {
		pos += 1;
	}
	// The terminator, the connection id, then the first part of the salt.
	pos += 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake[pos..pos + 8]);
	// The rest of the first part and the filler, the capabilities and the status, the length of
	// the salt and the reserved bytes.
	pos += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..20].copy_from_slice(&handshake[pos..pos + 12]);

	let caps =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	let mut response = Vec::new();
	response.extend_from_slice(&caps.to_le_bytes());
	response.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	response.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	response.extend_from_slice(&[0u8; 23]);
	response.extend_from_slice(user.as_bytes());
	response.push(0);
	response.push(20);
	response.extend_from_slice(&compute_mysql_native_password_scramble(user, &salt));
	response.extend_from_slice(b"test/test\0");
	response.extend_from_slice(b"mysql_native_password\0");
	write_packet(&mut stream, 1, &response).await;

	let mut reply = read_packet(&mut stream).await;
	if reply[0] == 0xfe {
		// AuthSwitchRequest to the clear-text plugin.
		write_packet(&mut stream, 3, format!("{user}\0").as_bytes()).await;
		reply = read_packet(&mut stream).await;
	}
	assert_eq!(reply[0], 0x00, "login: {}", String::from_utf8_lossy(&reply));
	stream
}

/// What the server answered to a statement.
#[derive(Debug)]
enum Answer {
	Ok,
	Rows,
	Err {
		code: u16,
		state: String,
		message: String,
	},
}

impl Answer {
	/// The number and SQLSTATE of an error, and `(0, "")` for an answer that is not one.
	fn code_and_state(&self) -> (u16, &str) {
		match self {
			Answer::Err {
				code,
				state,
				..
			} => (*code, state),
			Answer::Ok | Answer::Rows => (0, ""),
		}
	}
}

/// Read the answer to a command: an OK packet, an ERR packet or a text result set.
async fn read_answer(stream: &mut TcpStream) -> Answer {
	let first = read_packet(stream).await;
	match first[0] {
		0x00 => Answer::Ok,
		0xff => {
			assert_eq!(first[3], b'#', "ERR packet carries a SQLSTATE");
			Answer::Err {
				code: u16::from_le_bytes([first[1], first[2]]),
				state: String::from_utf8_lossy(&first[4..9]).into_owned(),
				message: String::from_utf8_lossy(&first[9..]).into_owned(),
			}
		}
		columns => {
			for _ in 0..columns {
				read_packet(stream).await;
			}
			assert_eq!(read_packet(stream).await[0], 0xfe, "EOF after the column definitions");
			loop {
				let row = read_packet(stream).await;
				if row[0] == 0xfe && row.len() < 9 {
					return Answer::Rows;
				}
			}
		}
	}
}

async fn query(stream: &mut TcpStream, sql: &str) -> Answer {
	let mut payload = vec![COM_QUERY];
	payload.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &payload).await;
	read_answer(stream).await
}

/// Run `sql`, which must succeed.
async fn run(stream: &mut TcpStream, sql: &str) {
	let answer = query(stream, sql).await;
	assert!(matches!(answer, Answer::Ok | Answer::Rows), "{sql}: {answer:?}");
}

/// Prepare `sql`, which has no parameters and no columns to describe, and return its id.
async fn prepare(stream: &mut TcpStream, sql: &str) -> u32 {
	let mut payload = vec![COM_STMT_PREPARE];
	payload.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &payload).await;
	let reply = read_packet(stream).await;
	assert_eq!(reply[0], 0x00, "{sql}: {}", String::from_utf8_lossy(&reply));
	assert_eq!(u16::from_le_bytes([reply[5], reply[6]]), 0, "{sql}: columns");
	assert_eq!(u16::from_le_bytes([reply[7], reply[8]]), 0, "{sql}: parameters");
	u32::from_le_bytes([reply[1], reply[2], reply[3], reply[4]])
}

async fn execute(stream: &mut TcpStream, statement: u32) -> Answer {
	let mut payload = vec![COM_STMT_EXECUTE];
	payload.extend_from_slice(&statement.to_le_bytes());
	payload.push(CURSOR_TYPE_NO_CURSOR);
	payload.extend_from_slice(&1u32.to_le_bytes());
	write_packet(stream, 0, &payload).await;
	read_answer(stream).await
}

/// The table every test below writes to.
const SETUP: [&str; 2] = [
	"CREATE TABLE coded (id INT PRIMARY KEY, name VARCHAR(10) NOT NULL, email VARCHAR(50) UNIQUE, \
	 age INT)",
	"INSERT INTO coded (id, name, email, age) VALUES (1, 'a', 'a@x', 10)",
];

#[tokio::test]
async fn a_failed_statement_has_the_number_of_its_kind() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr, "root").await;
	for sql in SETUP {
		run(&mut stream, sql).await;
	}

	// (statement, error number, SQLSTATE), as MySQL 8.4 answers the same condition.
	let cases = [
		// A table that does not exist, one that does, and a column defined twice.
		("SELECT * FROM no_such_table", 1146, "42S02"),
		("CREATE TABLE coded (id INT)", 1050, "42S01"),
		("CREATE TABLE twice (id INT, id INT)", 1060, "42S21"),
		// Not SQL.
		("SELEC 1", 1064, "42000"),
		("SELECT 1 +", 1064, "42000"),
		// A key that is held already: the primary key and a UNIQUE column.
		("INSERT INTO coded (id, name) VALUES (1, 'b')", 1062, "23000"),
		("INSERT INTO coded (id, name, email) VALUES (2, 'b', 'a@x')", 1062, "23000"),
		// NOT NULL: written as NULL, left out, and a value of another type.
		("INSERT INTO coded (id, name) VALUES (3, NULL)", 1048, "23000"),
		("INSERT INTO coded (id) VALUES (4)", 1364, "HY000"),
		("INSERT INTO coded (id, name, age) VALUES (5, 'e', 'abc')", 1366, "HY000"),
		("UPDATE coded SET age = 'abc'", 1366, "HY000"),
		// A column the table does not have, and a table the statement does not name.
		("UPDATE coded SET nosuch = 1", 1054, "42S22"),
		("SELECT t9.id FROM coded", 1054, "42S22"),
		("SELECT * FROM coded ORDER BY 9", 1054, "42S22"),
		("SELECT * FROM coded GROUP BY 9", 1054, "42S22"),
		// Functions.
		("SELECT nosuchfn(1)", 1305, "42000"),
		("SELECT abs(1, 2)", 1582, "42000"),
		("SELECT row_number() FROM coded", 1064, "42000"),
		("SELECT * FROM coded WHERE row_number() OVER () > 1", 3593, "HY000"),
		("SELECT * FROM coded GROUP BY sum(age)", 1111, "HY000"),
		// Arithmetic and constructs the engine does not run.
		("SELECT 9223372036854775807 + 1", 1690, "22003"),
		("SELECT count(DISTINCT age) OVER () FROM coded", 1235, "42000"),
	];
	for (sql, code, state) in cases {
		let answer = query(&mut stream, sql).await;
		assert_eq!(answer.code_and_state(), (code, state), "{sql}: {answer:?}");
		// A failure leaves the connection usable.
		let after = query(&mut stream, "SELECT 1").await;
		assert!(matches!(after, Answer::Rows), "{sql}: SELECT 1 after the error: {after:?}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn the_message_of_a_failed_statement_is_the_engines_text() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr, "root").await;
	for sql in SETUP {
		run(&mut stream, sql).await;
	}

	let answer =
		query(&mut stream, "INSERT INTO coded (id, name, email) VALUES (2, 'b', 'a@x')").await;
	let Answer::Err {
		message,
		..
	} = answer
	else {
		panic!("expected an error, got {answer:?}");
	};
	assert!(message.starts_with("Database index `"), "{message}");
	assert!(message.contains("already contains 'a@x'"), "{message}");

	let answer = query(&mut stream, "SELECT * FROM no_such_table").await;
	let Answer::Err {
		message,
		..
	} = answer
	else {
		panic!("expected an error, got {answer:?}");
	};
	assert_eq!(message, "The table 'no_such_table' does not exist");

	shutdown.cancel();
}

#[tokio::test]
async fn a_prepared_statement_that_fails_has_the_same_number() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr, "root").await;
	for sql in SETUP {
		run(&mut stream, sql).await;
	}

	for (sql, code, state) in [
		("SELECT * FROM no_such_table", 1146, "42S02"),
		("INSERT INTO coded (id, name) VALUES (1, 'b')", 1062, "23000"),
		("INSERT INTO coded (id, name) VALUES (3, NULL)", 1048, "23000"),
		("SELECT t9.id FROM coded", 1054, "42S22"),
	] {
		let statement = prepare(&mut stream, sql).await;
		let answer = execute(&mut stream, statement).await;
		assert_eq!(answer.code_and_state(), (code, state), "{sql}: {answer:?}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn a_user_who_may_not_write_has_the_number_of_a_denied_command() {
	let (addr, shutdown) = start_server().await;
	let mut root = connect(addr, "root").await;
	for sql in SETUP {
		run(&mut root, sql).await;
	}

	let mut viewer = connect(addr, "viewer").await;
	for sql in ["CREATE TABLE denied (id INT)", "DROP TABLE coded"] {
		let answer = query(&mut viewer, sql).await;
		assert_eq!(answer.code_and_state(), (1142, "42000"), "{sql}: {answer:?}");
	}
	// What the viewer may do still works.
	let answer = query(&mut viewer, "SELECT * FROM coded").await;
	assert!(matches!(answer, Answer::Rows), "{answer:?}");

	shutdown.cancel();
}

#[tokio::test]
async fn the_other_protocols_dialects_are_refused_under_one_number() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr, "root").await;

	for dialect in ["surrealql", "pgsql"] {
		let sql = format!("SET dialect = '{dialect}'");
		let answer = query(&mut stream, &sql).await;
		assert!(matches!(answer, Answer::Err { .. }), "{sql}: {answer:?}");
	}
	for (sql, code, state) in [
		("THROW 'boom'", 1064, "42000"),
		("SELEC 1", 1064, "42000"),
		("SELECT * FROM no_such_table", 1146, "42S02"),
	] {
		let answer = query(&mut stream, sql).await;
		assert_eq!(answer.code_and_state(), (code, state), "{sql}: {answer:?}");
	}

	shutdown.cancel();
}

/// What a client wrote in a statement must not choose the number of the error the statement
/// fails with: each failure below is one of the same few kinds whatever text the client put in
/// its value, its record id or its names, including the words of other errors.
#[tokio::test]
async fn text_the_client_wrote_cannot_choose_the_number() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr, "root").await;
	for sql in SETUP {
		run(&mut stream, sql).await;
	}

	let phrases = [
		"division by zero",
		"already contains",
		"duplicate key",
		"timed out",
		"conflict",
		"parse error",
		"unexpected token",
		"does not exist",
		"but found `NULL`",
		"MySQL SELECT lowering error: bigint out of range",
		// Strings that spell the values a NOT NULL column refuses.
		"NULL",
		"NONE",
	];
	for (i, phrase) in phrases.iter().enumerate() {
		let id = 100 + i;
		// A string written to an INT column.
		let sql = format!("INSERT INTO coded (id, name, age) VALUES ({id}, 'x', '{phrase}')");
		let answer = query(&mut stream, &sql).await;
		assert_eq!(answer.code_and_state(), (1366, "HY000"), "{sql}: {answer:?}");

		// A string used as the record id, which the engine names in its message.
		let sql = format!("INSERT INTO coded (id, name) VALUES ('{phrase}', 'x')");
		let answer = query(&mut stream, &sql).await;
		assert_eq!(answer.code_and_state(), (1366, "HY000"), "{sql}: {answer:?}");
	}

	// A table that carries the words of another error is still a missing table.
	for phrase in phrases {
		let sql = format!("SELECT * FROM `{}`", phrase.replace('`', ""));
		let answer = query(&mut stream, &sql).await;
		assert_eq!(answer.code_and_state(), (1146, "42S02"), "{sql}: {answer:?}");
	}

	shutdown.cancel();
}
