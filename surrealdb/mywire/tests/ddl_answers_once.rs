//! A SQL statement that defines several things at once (a table with its columns and indexes, a
//! `DROP TABLE` of several tables) is answered by one packet, the one the client sent it for.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Bound on reading one packet from the server, so a server that stops answering fails the test
/// instead of hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on the payload of a packet read from the server, so a corrupt length fails the test
/// instead of allocating to it.
const MAX_PACKET_PAYLOAD: usize = 1 << 20;

async fn start_test_mysql_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	ds.run(surrealdb_core::kvs::QueryRequest::new(
		"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
		&session,
	))
	.await
	.expect("Failed to define root user");
	let shutdown = CancellationToken::new();
	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(
		bind_addr,
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("Failed to start mywire server");
	(addr, shutdown)
}

/// Reads one packet and returns its payload.
async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	timeout(IO_TIMEOUT, async {
		let mut header = [0u8; 4];
		stream.read_exact(&mut header).await?;
		let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
		assert!(len <= MAX_PACKET_PAYLOAD, "packet of {len} bytes");
		let mut payload = vec![0u8; len];
		stream.read_exact(&mut payload).await?;
		Ok::<_, std::io::Error>(payload)
	})
	.await
	.expect("Timed out reading a packet")
	.expect("Failed to read a packet")
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	stream.write_all(payload).await.expect("Failed to write packet payload");
}

/// Connects as `root` to `test/test`, announcing multi-statement and multi-result support, the
/// capabilities under which a client reads one result per statement.
async fn connect_with_multi_results(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to MySQL server");

	// The salt is split in two around the capability flags, charset and status flags of the
	// HandshakeV10 packet.
	let handshake = read_packet(&mut stream).await;
	assert_eq!(handshake[0], 10, "Protocol version should be 10 (HandshakeV10)");
	let mut pos = 1;
	while handshake[pos] != 0 {
		pos += 1;
	}
	pos += 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake[pos..pos + 8]);
	pos += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..].copy_from_slice(&handshake[pos..pos + 12]);

	let caps: u32 = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| CLIENT_MULTI_STATEMENTS
		| CLIENT_MULTI_RESULTS;
	let mut resp = Vec::new();
	resp.extend_from_slice(&caps.to_le_bytes());
	resp.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	resp.extend_from_slice(&[0u8; 23]);
	resp.extend_from_slice(b"root\0");
	resp.push(20);
	resp.extend_from_slice(&compute_mysql_native_password_scramble("root", &salt));
	resp.extend_from_slice(b"test/test\0");
	resp.extend_from_slice(b"mysql_native_password\0");
	write_packet(&mut stream, 1, &resp).await;

	let mut reply = read_packet(&mut stream).await;
	if reply[0] == 0xfe {
		// AuthSwitchRequest to mysql_clear_password.
		write_packet(&mut stream, 3, b"root\0").await;
		reply = read_packet(&mut stream).await;
	}
	assert_eq!(reply[0], OK_HEADER, "the handshake should end with an OK packet");
	stream
}

/// The status flags of an OK packet: the two length-encoded counters, then the flags.
fn ok_status_flags(payload: &[u8]) -> u16 {
	assert_eq!(payload[0], OK_HEADER, "expected an OK packet, got {payload:?}");
	let mut at = 1;
	for _ in 0..2 {
		at += match payload[at] {
			0xfc => 3,
			0xfd => 4,
			0xfe => 9,
			_ => 1,
		};
	}
	u16::from_le_bytes([payload[at], payload[at + 1]])
}

async fn send_query(stream: &mut TcpStream, sql: &str) {
	let mut query = vec![COM_QUERY];
	query.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &query).await;
}

/// Reads the answer to `sql`, which is one OK packet that promises no more results, and checks
/// that the connection is then ready for the next statement: `SELECT 1` is answered by its own
/// result set (a column count) and not by an OK packet left over from `sql`.
async fn assert_answered_once(stream: &mut TcpStream, sql: &str) {
	send_query(stream, sql).await;
	let ok = read_packet(stream).await;
	assert_eq!(
		ok_status_flags(&ok) & SERVER_MORE_RESULTS_EXISTS,
		0,
		"{sql} is answered by one OK packet"
	);

	send_query(stream, "SELECT 1").await;
	let next = read_packet(stream).await;
	assert_eq!(next, [1], "{sql} left a second answer behind: {next:?}");
	// The column definition, its end, the row and the end of the result set.
	for _ in 0..4 {
		read_packet(stream).await;
	}
}

#[tokio::test]
async fn test_mywire_create_table_with_columns_and_an_index_answers_once() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	assert_answered_once(
		&mut stream,
		"CREATE TABLE answers_once (id INT PRIMARY KEY, name VARCHAR(20), age INT)",
	)
	.await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_drop_of_several_tables_answers_once() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	assert_answered_once(&mut stream, "CREATE TABLE drop_once_a (id INT)").await;
	assert_answered_once(&mut stream, "CREATE TABLE drop_once_b (id INT)").await;
	assert_answered_once(&mut stream, "DROP TABLE drop_once_a, drop_once_b").await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_each_statement_of_a_batch_gets_its_own_answer() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	send_query(
		&mut stream,
		"CREATE TABLE batch_once_a (id INT PRIMARY KEY, name VARCHAR(20)); \
		 CREATE TABLE batch_once_b (id INT PRIMARY KEY, name VARCHAR(20))",
	)
	.await;
	let first = read_packet(&mut stream).await;
	assert_ne!(
		ok_status_flags(&first) & SERVER_MORE_RESULTS_EXISTS,
		0,
		"the first of two statements promises another answer"
	);
	let second = read_packet(&mut stream).await;
	assert_eq!(
		ok_status_flags(&second) & SERVER_MORE_RESULTS_EXISTS,
		0,
		"the second of two statements is the last answer"
	);

	shutdown.cancel();
}

/// Sends `sql` and returns the message of the ERR packet that answers it, or `None` for an OK
/// packet.
async fn error_of(stream: &mut TcpStream, sql: &str) -> Option<String> {
	send_query(stream, sql).await;
	let reply = read_packet(stream).await;
	match reply[0] {
		OK_HEADER => None,
		ERR_HEADER => Some(String::from_utf8_lossy(&reply[9..]).into_owned()),
		other => panic!("{sql}: expected an OK or ERR packet, got a 0x{other:02x} packet"),
	}
}

#[tokio::test]
async fn test_mywire_ddl_commits_on_its_own_when_autocommit_is_off() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;
	assert_eq!(error_of(&mut stream, "SET autocommit = 0").await, None);

	// A table that repeats a column is refused and nothing of it is left behind.
	let error = error_of(&mut stream, "CREATE TABLE autocommit_off (a INT, a INT)").await;
	assert!(error.is_some_and(|e| e.contains("already exists")));
	// The refused statement does not make the connection refuse the next one, and the next one is
	// a definition of its own: a ROLLBACK does not take it back.
	assert_eq!(error_of(&mut stream, "CREATE TABLE autocommit_off (a INT)").await, None);
	assert_eq!(error_of(&mut stream, "ROLLBACK").await, None);
	let error = error_of(&mut stream, "CREATE TABLE autocommit_off (a INT)").await;
	assert!(error.is_some_and(|e| e.contains("already exists")));

	shutdown.cancel();
}

/// Sends `sql` as a prepared statement and returns the message of the ERR packet that answers its
/// execution, or `None` for an OK packet. The statements this is used with have no parameters and
/// no result columns.
async fn prepared_error_of(stream: &mut TcpStream, sql: &str) -> Option<String> {
	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &prepare).await;
	let prepared = read_packet(stream).await;
	assert_eq!(prepared[0], 0x00, "{sql}: expected a prepare OK, got {prepared:?}");
	assert_eq!(&prepared[5..9], &[0, 0, 0, 0], "{sql}: expected no columns and no parameters");

	let mut execute = vec![COM_STMT_EXECUTE];
	execute.extend_from_slice(&prepared[1..5]);
	execute.push(CURSOR_TYPE_NO_CURSOR);
	execute.extend_from_slice(&1u32.to_le_bytes());
	write_packet(stream, 0, &execute).await;
	let reply = read_packet(stream).await;
	match reply[0] {
		OK_HEADER => None,
		ERR_HEADER => Some(String::from_utf8_lossy(&reply[9..]).into_owned()),
		other => panic!("{sql}: expected an OK or ERR packet, got a 0x{other:02x} packet"),
	}
}

/// How a statement reaches the server.
#[derive(Clone, Copy, Debug)]
enum Protocol {
	Text,
	Prepared,
}

async fn error_of_over(stream: &mut TcpStream, protocol: Protocol, sql: &str) -> Option<String> {
	match protocol {
		Protocol::Text => error_of(stream, sql).await,
		Protocol::Prepared => prepared_error_of(stream, sql).await,
	}
}

/// Opens a transaction in the MySQL dialect, writes a row of `table` in it, switches to `dialect`
/// and runs `ddl` over `protocol`. Returns the connection with that transaction still to be
/// ended and the `ddl` outcome.
async fn ddl_inside_a_transaction(
	stream: &mut TcpStream,
	protocol: Protocol,
	dialect: &str,
	table: &str,
	ddl: &str,
) -> Option<String> {
	assert_eq!(error_of(stream, &format!("CREATE TABLE {table} (id INT PRIMARY KEY)")).await, None);
	assert_eq!(error_of(stream, "BEGIN").await, None);
	assert_eq!(error_of(stream, &format!("INSERT INTO {table} (id) VALUES (1)")).await, None);
	assert_eq!(error_of(stream, &format!("SET dialect = '{dialect}'")).await, None);
	error_of_over(stream, protocol, ddl).await
}

/// Runs one test body per protocol, as two tests that fail independently.
macro_rules! over_each_protocol {
	($scenario:ident, $text:ident, $prepared:ident) => {
		#[tokio::test]
		async fn $text() {
			$scenario(Protocol::Text).await;
		}

		#[tokio::test]
		async fn $prepared() {
			$scenario(Protocol::Prepared).await;
		}
	};
}

/// MySQL commits the open transaction before a DDL statement, so a `ROLLBACK` takes back neither
/// the row written before it nor the definition.
async fn mysql_ddl_commits_the_open_transaction(protocol: Protocol) {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_with_multi_results(addr).await;

	let ddl = "CREATE TABLE my_side (a INT)";
	let outcome = ddl_inside_a_transaction(&mut stream, protocol, "mysql", "my_rows", ddl).await;
	assert_eq!(outcome, None);
	assert_eq!(error_of(&mut stream, "ROLLBACK").await, None);

	let error = error_of(&mut stream, ddl).await;
	assert!(error.is_some_and(|e| e.contains("already exists")), "the table stays");
	let insert = "INSERT INTO my_rows (id) VALUES (1)";
	assert!(error_of(&mut stream, insert).await.is_some(), "the row stays");

	shutdown.cancel();
}

over_each_protocol!(
	mysql_ddl_commits_the_open_transaction,
	test_mywire_ddl_in_the_mysql_dialect_commits_the_open_transaction,
	test_mywire_prepared_ddl_in_the_mysql_dialect_commits_the_open_transaction
);
