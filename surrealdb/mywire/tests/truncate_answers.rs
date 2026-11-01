//! `TRUNCATE` is answered by an OK packet that reports no affected rows, never by a result set of
//! the rows it removed, in every form a MySQL client can send it.

#![recursion_limit = "256"]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Builder, Datastore, QueryRequest};
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// The datastore of every server a test started, oldest first, held weakly so that a server that
/// has shut down releases it. An entry is never removed, and the system hands a port out again once
/// its server is gone, so the newest entry of an address is the live server.
static SERVERS: std::sync::Mutex<Vec<(std::net::SocketAddr, Weak<Datastore>)>> =
	std::sync::Mutex::new(Vec::new());

/// Runs SurrealQL in the `test`/`test` database of the datastore behind the server at `addr`.
/// The wire speaks only MySQL, so fixtures are written in-process.
async fn seed_surrealql(addr: std::net::SocketAddr, sql: &str) {
	let ds = SERVERS
		.lock()
		.expect("lock the servers")
		.iter()
		.rev()
		.find(|(a, _)| *a == addr)
		.and_then(|(_, ds)| ds.upgrade())
		.expect("a live server started by the test helpers");
	let sess = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(sql, &sess, None).await.expect("seed") {
		response.result.unwrap_or_else(|err| panic!("{sql}: {err}"));
	}
}

/// A server that enforces authorization, with an owner `root` and a `viewer`, a role whose writes
/// are held to the permissions of the tables it writes.
async fn start_test_mysql_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Builder::new()
		.with_auth(true)
		.build_with_path("memory")
		.await
		.expect("Failed to create datastore");
	ds.run(QueryRequest::new(
		"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER; \
		 DEFINE USER viewer ON ROOT PASSWORD 'viewer' ROLES VIEWER;",
		&Session::owner(),
	))
	.await
	.expect("Failed to define the users");
	let ds_for_seed = Arc::downgrade(&ds);
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
	SERVERS.lock().expect("lock the servers").push((addr, ds_for_seed));
	(addr, shutdown)
}

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("Failed to read header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let mut payload = vec![0u8; len];
	stream.read_exact(&mut payload).await.expect("Failed to read payload");
	payload
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	stream.write_all(payload).await.expect("Failed to write packet payload");
}

/// Connects to `test/test` as `user`. `multi` announces CLIENT_MULTI_STATEMENTS and
/// CLIENT_MULTI_RESULTS, the capabilities under which a client reads one result per statement.
async fn connect(addr: std::net::SocketAddr, user: &str, multi: bool) -> TcpStream {
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

	let mut caps: u32 =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	if multi {
		caps |= CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS;
	}
	let mut resp = Vec::new();
	resp.extend_from_slice(&caps.to_le_bytes());
	resp.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	resp.extend_from_slice(&[0u8; 23]);
	resp.extend_from_slice(user.as_bytes());
	resp.push(0);
	resp.push(20);
	resp.extend_from_slice(&compute_mysql_native_password_scramble(user, &salt));
	resp.extend_from_slice(b"test/test\0");
	resp.extend_from_slice(b"mysql_native_password\0");
	write_packet(&mut stream, 1, &resp).await;

	let mut reply = read_packet(&mut stream).await;
	if reply[0] == EOF_HEADER {
		// AuthSwitchRequest to mysql_clear_password.
		write_packet(&mut stream, 3, format!("{user}\0").as_bytes()).await;
		reply = read_packet(&mut stream).await;
	}
	assert_eq!(reply[0], OK_HEADER, "the handshake of {user} should end with an OK packet");
	stream
}

/// A length-encoded integer at `*at`, which is moved past it.
fn lenenc_int(buf: &[u8], at: &mut usize) -> u64 {
	let first = buf[*at];
	*at += 1;
	let width = match first {
		0xfc => 2,
		0xfd => 3,
		0xfe => 8,
		small => return u64::from(small),
	};
	let mut value = 0u64;
	for i in 0..width {
		value |= u64::from(buf[*at + i]) << (8 * i);
	}
	*at += width;
	value
}

/// What the server answered to one statement.
#[derive(Debug)]
enum Reply {
	Ok {
		affected_rows: u64,
		last_insert_id: u64,
		status: u16,
	},
	Err {
		message: String,
	},
	ResultSet {
		/// The text of each row, a NULL being `None`.
		rows: Vec<Vec<Option<String>>>,
		status: u16,
	},
}

impl Reply {
	fn more_results(&self) -> bool {
		match self {
			Reply::Ok {
				status,
				..
			}
			| Reply::ResultSet {
				status,
				..
			} => status & SERVER_MORE_RESULTS_EXISTS != 0,
			Reply::Err {
				..
			} => false,
		}
	}
}

fn text_row(payload: &[u8]) -> Vec<Option<String>> {
	let mut at = 0;
	let mut cells = Vec::new();
	while at < payload.len() {
		if payload[at] == 0xfb {
			at += 1;
			cells.push(None);
			continue;
		}
		let len = lenenc_int(payload, &mut at) as usize;
		cells.push(Some(String::from_utf8_lossy(&payload[at..at + len]).into_owned()));
		at += len;
	}
	cells
}

/// Reads one answer: an OK packet, an ERR packet, or a result set of the text or the binary
/// protocol. The rows of a binary result set are not decoded.
async fn read_reply(stream: &mut TcpStream, text: bool) -> Reply {
	let first = read_packet(stream).await;
	match first[0] {
		OK_HEADER => {
			let mut at = 1;
			let affected_rows = lenenc_int(&first, &mut at);
			let last_insert_id = lenenc_int(&first, &mut at);
			Reply::Ok {
				affected_rows,
				last_insert_id,
				status: u16::from_le_bytes([first[at], first[at + 1]]),
			}
		}
		ERR_HEADER => Reply::Err {
			message: String::from_utf8_lossy(&first[9..]).into_owned(),
		},
		_ => {
			let mut at = 0;
			let columns = lenenc_int(&first, &mut at);
			// The column definitions and the EOF that ends them.
			for _ in 0..=columns {
				read_packet(stream).await;
			}
			let mut rows = Vec::new();
			loop {
				let packet = read_packet(stream).await;
				if packet[0] == EOF_HEADER && packet.len() < 9 {
					return Reply::ResultSet {
						rows,
						status: u16::from_le_bytes([packet[3], packet[4]]),
					};
				}
				rows.push(if text {
					text_row(&packet)
				} else {
					Vec::new()
				});
			}
		}
	}
}

/// Sends `sql` and reads every answer it gets: one for a statement, one for each statement of a
/// batch when the client announced that it reads several.
async fn query_all(stream: &mut TcpStream, sql: &str) -> Vec<Reply> {
	let mut query = vec![COM_QUERY];
	query.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &query).await;
	let mut replies = Vec::new();
	loop {
		let reply = read_reply(stream, true).await;
		let more = reply.more_results();
		replies.push(reply);
		if !more {
			return replies;
		}
	}
}

/// Sends one statement, which is answered once.
async fn query(stream: &mut TcpStream, sql: &str) -> Reply {
	let mut replies = query_all(stream, sql).await;
	assert_eq!(replies.len(), 1, "{sql} is answered once, got {replies:?}");
	replies.remove(0)
}

/// Prepares `sql` and returns the answer to COM_STMT_PREPARE.
async fn prepare(stream: &mut TcpStream, sql: &str) -> Vec<u8> {
	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &prepare).await;
	read_packet(stream).await
}

/// Executes the statement `prepared` (the answer to COM_STMT_PREPARE) holds.
async fn execute(stream: &mut TcpStream, prepared: &[u8]) -> Reply {
	assert_eq!(prepared[0], OK_HEADER, "the statement is prepared: {prepared:?}");
	let mut execute = vec![COM_STMT_EXECUTE];
	execute.extend_from_slice(&prepared[1..5]);
	execute.push(CURSOR_TYPE_NO_CURSOR);
	execute.extend_from_slice(&1u32.to_le_bytes());
	write_packet(stream, 0, &execute).await;
	read_reply(stream, false).await
}

/// Runs `sql`, which must be answered by an OK packet.
async fn run(stream: &mut TcpStream, sql: &str) {
	let reply = query(stream, sql).await;
	assert!(matches!(reply, Reply::Ok { .. }), "{sql}: {reply:?}");
}

/// The message of `reply`, which must be the ERR packet that refuses `sql`.
fn refusal(reply: Reply, sql: &str) -> String {
	let Reply::Err {
		message,
	} = reply
	else {
		panic!("{sql} is refused, got {reply:?}");
	};
	message
}

/// The number of rows `table` holds.
async fn count(stream: &mut TcpStream, table: &str) -> String {
	let sql = format!("SELECT count(*) FROM {table}");
	let Reply::ResultSet {
		rows,
		..
	} = query(stream, &sql).await
	else {
		panic!("{sql} is answered by a result set");
	};
	assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
	rows[0][0].clone().expect("a count is not NULL")
}

fn assert_truncated(reply: &Reply, sql: &str) {
	let Reply::Ok {
		affected_rows,
		last_insert_id,
		..
	} = reply
	else {
		panic!("{sql} is answered by an OK packet, got {reply:?}");
	};
	assert_eq!((*affected_rows, *last_insert_id), (0, 0), "{sql} reports no affected rows");
}

async fn table_with_two_rows(stream: &mut TcpStream, table: &str) {
	run(stream, &format!("CREATE TABLE {table} (id INT, a INT)")).await;
	run(stream, &format!("INSERT INTO {table} (id, a) VALUES (1, 7), (2, 8)")).await;
	assert_eq!(count(stream, table).await, "2");
}

#[tokio::test]
async fn test_mywire_truncate_answers_with_an_ok_packet_and_empties_the_table() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect(addr, "root", false).await;
	table_with_two_rows(&mut stream, "trunc_text").await;

	let sql = "TRUNCATE TABLE trunc_text";
	assert_truncated(&query(&mut stream, sql).await, sql);
	assert_eq!(count(&mut stream, "trunc_text").await, "0");

	// The table keeps its definition, and the keyword TABLE is optional.
	run(&mut stream, "INSERT INTO trunc_text (id, a) VALUES (3, 9)").await;
	assert_eq!(count(&mut stream, "trunc_text").await, "1");
	let sql = "TRUNCATE trunc_text";
	assert_truncated(&query(&mut stream, sql).await, sql);
	assert_eq!(count(&mut stream, "trunc_text").await, "0");

	// A table with no rows is answered the same way.
	let sql = "TRUNCATE TABLE trunc_text";
	assert_truncated(&query(&mut stream, sql).await, sql);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_truncate_only_empties_the_table_it_names() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect(addr, "root", false).await;
	table_with_two_rows(&mut stream, "trunc_named").await;
	table_with_two_rows(&mut stream, "trunc_other").await;

	let sql = "TRUNCATE TABLE trunc_named";
	assert_truncated(&query(&mut stream, sql).await, sql);
	assert_eq!(count(&mut stream, "trunc_named").await, "0");
	assert_eq!(count(&mut stream, "trunc_other").await, "2");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_prepared_truncate_answers_with_an_ok_packet() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect(addr, "root", false).await;
	table_with_two_rows(&mut stream, "trunc_prepared").await;

	let prepared = prepare(&mut stream, "TRUNCATE TABLE trunc_prepared").await;
	assert_eq!(&prepared[5..9], &[0, 0, 0, 0], "TRUNCATE has no columns and no parameters");

	for round in 0..2 {
		let reply = execute(&mut stream, &prepared).await;
		assert_truncated(&reply, "COM_STMT_EXECUTE of TRUNCATE TABLE");
		assert_eq!(count(&mut stream, "trunc_prepared").await, "0", "round {round}");

		run(&mut stream, "INSERT INTO trunc_prepared (id, a) VALUES (1, 7)").await;
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_truncate_in_a_batch_is_answered_by_its_own_ok_packet() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut setup = connect(addr, "root", false).await;
	table_with_two_rows(&mut setup, "trunc_batch_a").await;
	table_with_two_rows(&mut setup, "trunc_batch_b").await;

	let mut stream = connect(addr, "root", true).await;
	let replies = query_all(
		&mut stream,
		"TRUNCATE TABLE trunc_batch_a; SELECT count(*) FROM trunc_batch_b; \
		 TRUNCATE TABLE trunc_batch_b",
	)
	.await;
	assert_eq!(replies.len(), 3, "one answer for each statement, got {replies:?}");
	assert_truncated(&replies[0], "the first TRUNCATE of the batch");
	let Reply::ResultSet {
		rows,
		..
	} = &replies[1]
	else {
		panic!("the SELECT of the batch is answered by a result set, got {:?}", replies[1]);
	};
	assert_eq!(rows, &[vec![Some("2".to_owned())]]);
	assert_truncated(&replies[2], "the second TRUNCATE of the batch");
	assert_eq!(count(&mut stream, "trunc_batch_a").await, "0");
	assert_eq!(count(&mut stream, "trunc_batch_b").await, "0");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_truncate_of_a_batch_without_multi_results_is_answered_once() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect(addr, "root", false).await;
	table_with_two_rows(&mut stream, "trunc_single_a").await;
	table_with_two_rows(&mut stream, "trunc_single_b").await;

	let sql = "TRUNCATE TABLE trunc_single_a; TRUNCATE TABLE trunc_single_b";
	assert_truncated(&query(&mut stream, sql).await, sql);
	assert_eq!(count(&mut stream, "trunc_single_a").await, "0");
	assert_eq!(count(&mut stream, "trunc_single_b").await, "0");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_truncate_of_a_missing_table_is_an_error() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect(addr, "root", false).await;

	let sql = "TRUNCATE TABLE trunc_missing";
	let message = refusal(query(&mut stream, sql).await, sql);
	assert!(message.contains("trunc_missing"), "{message}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_truncate_removes_no_row_the_table_does_not_let_the_user_delete() {
	let (addr, shutdown) = start_test_mysql_server().await;
	// A table whose permissions let a checked principal read and write rows but not delete them,
	// defined in SurrealQL.
	let _owner = connect(addr, "root", false).await;
	seed_surrealql(
		addr,
		"DEFINE TABLE trunc_kept SCHEMALESS PERMISSIONS FOR select, create, update FULL, FOR delete NONE; \
		 CREATE trunc_kept:1 SET a = 7; \
		 CREATE trunc_kept:2 SET a = 8;",
	)
	.await;

	let mut viewer = connect(addr, "viewer", false).await;
	assert_eq!(count(&mut viewer, "trunc_kept").await, "2");
	// The statement is not answered with the rows it was not allowed to remove, and removes none.
	let reply = query(&mut viewer, "TRUNCATE TABLE trunc_kept").await;
	assert!(!matches!(reply, Reply::ResultSet { .. }), "got {reply:?}");
	let prepared = prepare(&mut viewer, "TRUNCATE TABLE trunc_kept").await;
	let reply = execute(&mut viewer, &prepared).await;
	assert!(!matches!(reply, Reply::ResultSet { .. }), "got {reply:?}");
	assert_eq!(count(&mut viewer, "trunc_kept").await, "2");

	// What held the viewer back is the table's permission: an owner is not subject to it.
	let mut owner = connect(addr, "root", false).await;
	let sql = "TRUNCATE TABLE trunc_kept";
	assert_truncated(&query(&mut owner, sql).await, sql);
	assert_eq!(count(&mut viewer, "trunc_kept").await, "0");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_truncate_cannot_name_a_table_of_another_database() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect(addr, "root", false).await;
	// The same table in two databases, so a statement that lost its qualifier would empty the
	// table of the database the connection is in.
	table_with_two_rows(&mut stream, "trunc_scoped").await;
	run(&mut stream, "USE trunc_elsewhere").await;
	table_with_two_rows(&mut stream, "trunc_scoped").await;

	let sql = "TRUNCATE TABLE test.trunc_scoped";
	let message = refusal(query(&mut stream, sql).await, sql);
	assert!(message.contains("test.trunc_scoped"), "{message}");
	let prepared = prepare(&mut stream, sql).await;
	let message = refusal(execute(&mut stream, &prepared).await, sql);
	assert!(message.contains("test.trunc_scoped"), "{message}");

	assert_eq!(count(&mut stream, "trunc_scoped").await, "2");
	run(&mut stream, "USE test").await;
	assert_eq!(count(&mut stream, "trunc_scoped").await, "2");

	shutdown.cancel();
}
