#![recursion_limit = "256"]

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Weak};
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::codec::{MAX_ALLOWED_PACKET, MAX_PACKET_SIZE};
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
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

async fn start_test_mysql_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	let _ = ds
		.run(surrealdb_core::kvs::QueryRequest::new(
			"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
			&session,
		))
		.await
		.expect("Failed to define root user");
	let ds_for_seed = Arc::downgrade(&ds);
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");
	SERVERS.lock().expect("lock the servers").push((addr, ds_for_seed));

	(addr, shutdown)
}

async fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("Failed to read header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let seq = header[3];

	let mut payload = vec![0u8; len];
	if len > 0 {
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
	}
	(seq, payload)
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	if len > 0 {
		stream.write_all(payload).await.expect("Failed to write packet payload");
	}
}

async fn connect_and_handshake(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to MySQL server");

	// 1. Read Initial Handshake Packet (Seq 0)
	let (seq, payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 0, "Initial handshake packet should be seq 0");
	assert_eq!(payload[0], 10, "Protocol version should be 10 (HandshakeV10)");

	// Extract salt from HandshakeV10:
	// Protocol version (1 byte) + server version (null terminated) + conn id (4 bytes)
	let mut pos = 1;
	while pos < payload.len() && payload[pos] != 0 {
		pos += 1;
	}
	pos += 1; // skip null
	pos += 4; // skip thread id

	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&payload[pos..pos + 8]);
	pos += 9; // 8 bytes auth-plugin-data-part-1 + 1 byte filler (0x00)
	pos += 2; // capability flags (lower 2 bytes)
	pos += 1; // character set
	pos += 2; // status flags
	pos += 2; // capability flags (upper 2 bytes)
	pos += 1; // auth_plugin_data_len
	pos += 10; // reserved 10 bytes

	salt[8..20].copy_from_slice(&payload[pos..pos + 12]);

	// 2. Build HandshakeResponse41 (Seq 1)
	let scramble = compute_mysql_native_password_scramble("root", &salt);

	let mut resp = Vec::new();
	// Client capabilities: CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB |
	// CLIENT_PLUGIN_AUTH
	let caps: u32 =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	resp.extend_from_slice(&caps.to_le_bytes());

	// Max packet size: 16 MB
	let max_packet: u32 = 16 * 1024 * 1024;
	resp.extend_from_slice(&max_packet.to_le_bytes());

	// Charset: UTF8MB4_GENERAL_CI (45)
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);

	// Reserved 23 bytes zeros
	resp.extend_from_slice(&[0u8; 23]);

	// Username: "root\0"
	resp.extend_from_slice(b"root\0");

	// Auth response: lenenc or 1-byte len + scramble
	resp.push(20);
	resp.extend_from_slice(&scramble);

	// Database: "test/test\0"
	resp.extend_from_slice(b"test/test\0");

	// Auth plugin name: "mysql_native_password\0"
	resp.extend_from_slice(b"mysql_native_password\0");

	write_packet(&mut stream, 1, &resp).await;

	// 3. Read response (either OK packet 0x00 or AuthSwitchRequest 0xFE)
	let (seq, payload) = read_packet(&mut stream).await;
	if payload[0] == 0xfe {
		// AuthSwitchRequest to mysql_clear_password
		write_packet(&mut stream, seq.wrapping_add(1), b"root\0").await;
		let (_final_seq, ok_payload) = read_packet(&mut stream).await;
		assert_eq!(
			ok_payload[0],
			0x00,
			"Handshake OK expected, got error: {:?}",
			String::from_utf8_lossy(&ok_payload)
		);
	} else {
		assert_eq!(payload[0], 0x00, "Expected OK packet (0x00) from handshake");
	}

	stream
}

#[derive(Debug)]
enum QueryResult {
	Ok,
	Error {
		code: u16,
		message: String,
	},
	ResultSet {
		columns: usize,
		rows: Vec<Vec<u8>>,
	},
}

async fn read_query_response(stream: &mut TcpStream) -> QueryResult {
	let (_, first) = read_packet(stream).await;
	if first[0] == 0x00 {
		return QueryResult::Ok;
	}
	if first[0] == 0xff {
		let code = if first.len() >= 3 {
			(first[1] as u16) | ((first[2] as u16) << 8)
		} else {
			0
		};
		let message = String::from_utf8_lossy(&first[3..]).to_string();
		return QueryResult::Error {
			code,
			message,
		};
	}

	let col_count = first[0] as usize;
	for _ in 0..col_count {
		let _ = read_packet(stream).await;
	}
	// EOF packet separating cols and rows
	let (_, eof1) = read_packet(stream).await;
	assert_eq!(eof1[0], 0xfe, "Expected EOF after column definitions");

	let mut rows = Vec::new();
	loop {
		let (_, pkt) = read_packet(stream).await;
		if pkt[0] == 0xfe && pkt.len() < 9 {
			break;
		}
		rows.push(pkt);
	}

	QueryResult::ResultSet {
		columns: col_count,
		rows,
	}
}

#[tokio::test]
async fn test_mywire_e2e_ping_and_init_db() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// COM_PING (0x0e)
	write_packet(&mut stream, 0, &[COM_PING]).await;
	let (seq, payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 1);
	assert_eq!(payload[0], 0x00, "COM_PING should return OK packet");

	// COM_INIT_DB (0x02)
	let mut init_db = vec![COM_INIT_DB];
	init_db.extend_from_slice(b"test/test");
	write_packet(&mut stream, 0, &init_db).await;
	let (seq, payload) = read_packet(&mut stream).await;
	assert_eq!(seq, 1);
	assert_eq!(payload[0], 0x00, "COM_INIT_DB should return OK packet");

	// COM_QUIT (0x01)
	write_packet(&mut stream, 0, &[COM_QUIT]).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_e2e_queries() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. SELECT @@version_comment
	let mut query = vec![COM_QUERY];
	query.extend_from_slice(b"SELECT @@version_comment;");
	write_packet(&mut stream, 0, &query).await;

	let res = read_query_response(&mut stream).await;
	let QueryResult::ResultSet {
		columns,
		rows,
	} = res
	else {
		panic!("Expected ResultSet for @@version_comment");
	};
	assert_eq!(columns, 1);
	assert_eq!(rows.len(), 1);

	// 2. INSERT record (MySQL syntax)
	let mut insert_q = vec![COM_QUERY];
	insert_q.extend_from_slice(b"INSERT INTO person (name) VALUES ('Tobie');");
	write_packet(&mut stream, 0, &insert_q).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::Ok => {}
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert!(!rows.is_empty(), "Expected inserted row");
		}
		QueryResult::Error {
			code,
			message,
		} => {
			panic!("INSERT failed with code {code}: {message}");
		}
	}

	// 3. SELECT from person (MySQL syntax)
	let mut select_q = vec![COM_QUERY];
	select_q.extend_from_slice(b"SELECT name FROM person;");
	write_packet(&mut stream, 0, &select_q).await;

	let res = read_query_response(&mut stream).await;
	let QueryResult::ResultSet {
		columns,
		rows,
	} = res
	else {
		panic!("Expected ResultSet for SELECT");
	};
	assert_eq!(columns, 1, "Expected name column");
	assert_eq!(rows.len(), 1, "Expected 1 row from person");

	// Clean quit
	write_packet(&mut stream, 0, &[COM_QUIT]).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_dialect_switching_and_fail_closed() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let res = query_mysql(&mut stream, "CREATE person SET name = 'Invalid';").await;
	let QueryResult::Error {
		code,
		..
	} = res
	else {
		panic!("SurrealQL query should have failed in MySQL dialect");
	};
	assert_eq!(code, 1064, "SurrealQL query must fail in MySQL dialect with 1064");

	for dialect in ["surrealql", "pgsql"] {
		let res = query_mysql(&mut stream, &format!("SET dialect = '{dialect}';")).await;
		assert!(matches!(res, QueryResult::Error { .. }), "SET dialect = '{dialect}': {res:?}");
	}
	let res = query_mysql(&mut stream, "SET dialect = 'mysql';").await;
	assert!(matches!(res, QueryResult::Ok), "SET dialect = 'mysql' must succeed");

	let res = query_mysql(&mut stream, "CREATE person SET name = 'Invalid';").await;
	assert!(
		matches!(
			res,
			QueryResult::Error {
				code: 1064,
				..
			}
		),
		"a refused switch leaves the dialect as it was: {res:?}"
	);

	write_packet(&mut stream, 0, &[COM_QUIT]).await;
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_parameter_placeholder_rejection() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// In MySQL text protocol (COM_QUERY), unbound '?' parameter placeholders
	// must be rejected fail-closed rather than returning silent NULL.
	let mut q = vec![COM_QUERY];
	q.extend_from_slice(b"SELECT ?;");
	write_packet(&mut stream, 0, &q).await;

	let res = read_query_response(&mut stream).await;
	let QueryResult::Error {
		code,
		message,
		..
	} = res
	else {
		panic!("Expected Error packet for SELECT ?, got: {res:?}");
	};
	assert_eq!(code, 1064);
	assert!(
		message.contains("parameter placeholder") || message.contains("not supported"),
		"Expected error mentioning parameter placeholders, got: {message}"
	);

	// Also in INSERT VALUES (?)
	let mut q2 = vec![COM_QUERY];
	q2.extend_from_slice(b"INSERT INTO person (name) VALUES (?);");
	write_packet(&mut stream, 0, &q2).await;

	let res2 = read_query_response(&mut stream).await;
	let QueryResult::Error {
		code,
		..
	} = res2
	else {
		panic!("Expected Error packet for INSERT VALUES (?), got: {res2:?}");
	};
	assert_eq!(code, 1064);

	shutdown.cancel();
}

fn decode_mysql_text_row(payload: &[u8]) -> Vec<Option<String>> {
	let mut cols = Vec::new();
	let mut offset = 0;
	while offset < payload.len() {
		let b = payload[offset];
		if b == 0xfb {
			cols.push(None);
			offset += 1;
		} else {
			let (len, len_bytes) = match b {
				0xfc => {
					let l = (payload[offset + 1] as usize) | ((payload[offset + 2] as usize) << 8);
					(l, 3)
				}
				0xfd => {
					let l = (payload[offset + 1] as usize)
						| ((payload[offset + 2] as usize) << 8)
						| ((payload[offset + 3] as usize) << 16);
					(l, 4)
				}
				0xfe => {
					let mut l = 0usize;
					for i in 0..8 {
						l |= (payload[offset + 1 + i] as usize) << (i * 8);
					}
					(l, 9)
				}
				_ => (b as usize, 1),
			};
			offset += len_bytes;
			let s = String::from_utf8_lossy(&payload[offset..offset + len]).to_string();
			cols.push(Some(s));
			offset += len;
		}
	}
	cols
}

async fn query_mysql(stream: &mut TcpStream, sql: &str) -> QueryResult {
	let mut q = vec![COM_QUERY];
	q.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &q).await;
	read_query_response(stream).await
}

#[tokio::test]
async fn test_mywire_query_matrix_arithmetic_and_expressions() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Basic arithmetic operations
	let res = query_mysql(&mut stream, "SELECT 1 + 2 * 3;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("7"));

	let res = query_mysql(&mut stream, "SELECT 100 - 35;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("65"));

	let res = query_mysql(&mut stream, "SELECT 50 / 2;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("25"));

	let res = query_mysql(&mut stream, "SELECT 17 % 5;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("2"));

	// 2. Parentheses & precedence
	let res = query_mysql(&mut stream, "SELECT (10 + 20) * 3 / 2;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("45"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_query_matrix_booleans_and_comparisons() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Comparisons
	let res = query_mysql(&mut stream, "SELECT 1 = 1;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	let res = query_mysql(&mut stream, "SELECT 1 != 2;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	let res = query_mysql(&mut stream, "SELECT 5 > 3;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	let res = query_mysql(&mut stream, "SELECT 2 < 4;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	// 2. BETWEEN & IN
	let res = query_mysql(&mut stream, "SELECT 5 BETWEEN 1 AND 10;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	let res = query_mysql(&mut stream, "SELECT 15 BETWEEN 1 AND 10;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("0"));

	let res = query_mysql(&mut stream, "SELECT 3 IN (1, 2, 3);").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	let res = query_mysql(&mut stream, "SELECT 4 IN (1, 2, 3);").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("0"));

	// 3. IS NULL / IS NOT NULL
	let res = query_mysql(&mut stream, "SELECT NULL IS NULL;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	let res = query_mysql(&mut stream, "SELECT 1 IS NOT NULL;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("1"));

	// 4. CASE expressions
	let res =
		query_mysql(&mut stream, "SELECT CASE WHEN 1 = 1 THEN 'alpha' ELSE 'beta' END;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("alpha"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_query_matrix_crud_and_clauses() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. CREATE TABLE
	let res = query_mysql(&mut stream, "CREATE TABLE items (id int, name text, qty int);").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// 2. INSERT multiple rows
	let inserts = [
		"INSERT INTO items (id, name, qty) VALUES (1, 'Apples', 10);",
		"INSERT INTO items (id, name, qty) VALUES (2, 'Oranges', 20);",
		"INSERT INTO items (id, name, qty) VALUES (3, 'Bananas', 30);",
	];
	for q in inserts {
		let res = query_mysql(&mut stream, q).await;
		assert!(
			matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
			"Failed insert: {q}"
		);
	}

	// 3. SELECT WHERE filter
	let res = query_mysql(&mut stream, "SELECT name FROM items WHERE qty > 15;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2, "Expected Oranges and Bananas");

	// 4. SELECT ORDER BY ASC / DESC
	let res = query_mysql(&mut stream, "SELECT name FROM items ORDER BY qty ASC;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("Apples"));

	let res = query_mysql(&mut stream, "SELECT name FROM items ORDER BY qty DESC;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("Bananas"));

	// 5. SELECT LIMIT and OFFSET
	let res =
		query_mysql(&mut stream, "SELECT name FROM items ORDER BY qty ASC LIMIT 1 OFFSET 1;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 1);
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("Oranges"));

	// 6. UPDATE
	let res = query_mysql(&mut stream, "UPDATE items SET qty = 15 WHERE name = 'Apples';").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(&mut stream, "SELECT qty FROM items WHERE name = 'Apples';").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("15"));

	// 7. DELETE
	let res = query_mysql(&mut stream, "DELETE FROM items WHERE name = 'Bananas';").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(&mut stream, "SELECT name FROM items;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2, "Expected 2 items remaining after delete");

	// 8. DROP TABLE
	let res = query_mysql(&mut stream, "DROP TABLE items;").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_honest_session_fail_closed() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Unmodelled SET statements must return code 1235 (ER_NOT_SUPPORTED_YET)
	let res = query_mysql(&mut stream, "SET ROLE admin;").await;
	let QueryResult::Error {
		code,
		..
	} = res
	else {
		panic!("Expected Error 1235 for SET ROLE, got {res:?}");
	};
	assert_eq!(code, 1235);

	let res = query_mysql(&mut stream, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").await;
	let QueryResult::Error {
		code,
		..
	} = res
	else {
		panic!("Expected Error 1235 for SET TRANSACTION, got {res:?}");
	};
	assert_eq!(code, 1235);

	// A variable MySQL does not have is an unknown system variable, not an unsupported SET.
	let res = query_mysql(&mut stream, "SET unknown_variable = 1;").await;
	let QueryResult::Error {
		code,
		..
	} = res
	else {
		panic!("Expected Error 1193 for unknown SET, got {res:?}");
	};
	assert_eq!(code, 1193);

	let res = query_mysql(&mut stream, "SET @@max_connections = 1;").await;
	let QueryResult::Error {
		code,
		..
	} = res
	else {
		panic!("Expected Error 1235 for a SET the server cannot honour, got {res:?}");
	};
	assert_eq!(code, 1235);

	// 2. Driver noise SET statements should succeed with OK packet
	let res = query_mysql(&mut stream, "SET autocommit = 1;").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(&mut stream, "SET NAMES utf8mb4;").await;
	assert!(matches!(res, QueryResult::Ok));

	// 3. Out of bounds GROUP BY ordinal must fail closed
	let res = query_mysql(&mut stream, "SELECT 1 GROUP BY 99;").await;
	let QueryResult::Error {
		..
	} = res
	else {
		panic!("Expected Error for out of bounds GROUP BY ordinal, got {res:?}");
	};

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_ddl_matrix() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. CREATE TABLE with constraints
	let res = query_mysql(
		&mut stream,
		"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL, email VARCHAR(100) UNIQUE);",
	)
	.await;
	assert!(
		matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
		"CREATE TABLE users failed"
	);

	// 2. INSERT into users
	let res = query_mysql(
		&mut stream,
		"INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@mysql.com');",
	)
	.await;
	assert!(
		matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
		"INSERT into users failed"
	);

	// 3. SELECT from users
	let res = query_mysql(&mut stream, "SELECT name, email FROM users;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 1);
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("Alice"));
	assert_eq!(cols[1].as_deref(), Some("alice@mysql.com"));

	// 4. DROP TABLE
	let res = query_mysql(&mut stream, "DROP TABLE users;").await;
	assert!(
		matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
		"DROP TABLE users failed"
	);

	// 5. Multiple table drop
	let _ = query_mysql(&mut stream, "CREATE TABLE t1 (id INT PRIMARY KEY);").await;
	let _ = query_mysql(&mut stream, "CREATE TABLE t2 (id INT PRIMARY KEY);").await;
	let res = query_mysql(&mut stream, "DROP TABLE t1, t2;").await;
	assert!(
		matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
		"DROP TABLE t1, t2 failed"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_database_namespaces_decision_d4() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Initial database is "test"
	let res = query_mysql(&mut stream, "SELECT DATABASE();").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet for SELECT DATABASE(), got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("test"));

	// 2. Create table in "test" database
	let res =
		query_mysql(&mut stream, "CREATE TABLE accounts (user_id INT, name VARCHAR(50));").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// 3. Insert with bare table and active db qualified name
	let res =
		query_mysql(&mut stream, "INSERT INTO accounts (user_id, name) VALUES (1, 'Alice');").await;
	assert!(
		matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
		"INSERT accounts failed: {res:?}"
	);

	let res =
		query_mysql(&mut stream, "INSERT INTO test.accounts (user_id, name) VALUES (2, 'Bob');")
			.await;
	assert!(
		matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
		"INSERT test.accounts failed: {res:?}"
	);

	// 4. Query accounts
	let res =
		query_mysql(&mut stream, "SELECT user_id, name FROM test.accounts ORDER BY user_id;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);
	let cols0 = decode_mysql_text_row(&rows[0]);
	let cols1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(cols0[1].as_deref(), Some("Alice"));
	assert_eq!(cols1[1].as_deref(), Some("Bob"));

	// 5. Switch database using text `USE analytics;`
	let res = query_mysql(&mut stream, "USE analytics;").await;
	assert!(matches!(res, QueryResult::Ok), "USE analytics failed");

	// 6. SELECT DATABASE() reflects new active database
	let res = query_mysql(&mut stream, "SELECT DATABASE();").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet for SELECT DATABASE(), got {res:?}");
	};
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("analytics"));

	// 7. Create table and insert in new database "analytics"
	let res =
		query_mysql(&mut stream, "CREATE TABLE events (event_id INT, action VARCHAR(50));").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res =
		query_mysql(&mut stream, "INSERT INTO events (event_id, action) VALUES (10, 'click');")
			.await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(&mut stream, "SELECT event_id, action FROM events;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 1);
	let cols0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols0[1].as_deref(), Some("click"));

	// 8. Cross-database reference rejection: while connected to "analytics",
	// referencing "test.accounts" must be strictly rejected
	let res = query_mysql(&mut stream, "SELECT * FROM test.accounts;").await;
	assert!(matches!(res, QueryResult::Error { .. }), "Cross-database query should be rejected");

	let res = query_mysql(&mut stream, "INSERT INTO test.accounts VALUES (3, 'Charlie');").await;
	assert!(matches!(res, QueryResult::Error { .. }), "Cross-database write should be rejected");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_query_matrix_aggregations_and_group_by() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Seed test data
	let res = query_mysql(
		&mut stream,
		"CREATE TABLE items (id INT, category VARCHAR(50), price INT, in_stock BOOLEAN);",
	)
	.await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	for q in [
		"INSERT INTO items (id, category, price, in_stock) VALUES (1, 'electronics', 100, true);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (2, 'electronics', 200, true);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (3, 'books', 15, false);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (4, 'books', 20, true);",
		"INSERT INTO items (id, category, price, in_stock) VALUES (5, 'clothing', 50, true);",
	] {
		let res = query_mysql(&mut stream, q).await;
		assert!(
			matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }),
			"Failed {q}: {res:?}"
		);
	}

	// 1. Global aggregations without GROUP BY
	let res =
		query_mysql(&mut stream, "SELECT count(*), sum(price), min(price), max(price) FROM items;")
			.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 1);
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("5"), "count(*)");
	assert_eq!(cols[1].as_deref(), Some("385"), "sum(price)");
	assert_eq!(cols[2].as_deref(), Some("15"), "min(price)");
	assert_eq!(cols[3].as_deref(), Some("200"), "max(price)");

	// 2. Single-column GROUP BY
	let res = query_mysql(
		&mut stream,
		"SELECT category, count(*), sum(price) FROM items GROUP BY category ORDER BY category ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 3);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));
	assert_eq!(r0[1].as_deref(), Some("2"));
	assert_eq!(r0[2].as_deref(), Some("35"));
	let r1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("clothing"));
	assert_eq!(r1[1].as_deref(), Some("1"));
	assert_eq!(r1[2].as_deref(), Some("50"));
	let r2 = decode_mysql_text_row(&rows[2]);
	assert_eq!(r2[0].as_deref(), Some("electronics"));
	assert_eq!(r2[1].as_deref(), Some("2"));
	assert_eq!(r2[2].as_deref(), Some("300"));

	// 3. Positional GROUP BY 1
	let res =
		query_mysql(&mut stream, "SELECT category, count(*) FROM items GROUP BY 1 ORDER BY 1 ASC;")
			.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 3);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));
	let r1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("clothing"));
	let r2 = decode_mysql_text_row(&rows[2]);
	assert_eq!(r2[0].as_deref(), Some("electronics"));

	// 4. GROUP BY alias (MySQL feature)
	let res = query_mysql(
		&mut stream,
		"SELECT category AS cat, count(*) FROM items GROUP BY cat ORDER BY cat ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 3);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));

	// 5. HAVING clause with aggregate present in targets
	let res = query_mysql(
		&mut stream,
		"SELECT category, count(*) AS cnt FROM items GROUP BY category HAVING count(*) > 1 ORDER BY category ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));
	let r1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("electronics"));

	// 6. HAVING clause with aggregate NOT present in targets
	let res = query_mysql(
		&mut stream,
		"SELECT category FROM items GROUP BY category HAVING count(*) > 1 ORDER BY category ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("books"));
	let r1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("electronics"));

	// 7. Global COUNT(DISTINCT col)
	let res = query_mysql(&mut stream, "SELECT count(DISTINCT category) FROM items;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 1);
	let cols = decode_mysql_text_row(&rows[0]);
	assert_eq!(cols[0].as_deref(), Some("3"), "3 distinct categories");

	// 8. Multi-column GROUP BY
	let res = query_mysql(
		&mut stream,
		"SELECT category, in_stock, count(*) FROM items GROUP BY category, in_stock ORDER BY category ASC, in_stock ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 4);

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_set_operations_and_joins() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. UNION ALL with outer ORDER BY
	let res =
		query_mysql(&mut stream, "SELECT 10 AS num UNION ALL SELECT 20 AS num ORDER BY num ASC;")
			.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);
	let r0 = decode_mysql_text_row(&rows[0]);
	let r1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(r0[0].as_deref(), Some("10"));
	assert_eq!(r1[0].as_deref(), Some("20"));

	// 2. UNION (distinct)
	let res =
		query_mysql(&mut stream, "SELECT 1 AS x UNION SELECT 1 AS x UNION SELECT 2 AS x;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2, "Duplicate 1 must be deduplicated by UNION");

	// 3. Parenthesised query
	let res = query_mysql(&mut stream, "(SELECT 100 AS val) UNION ALL (SELECT 200 AS val);").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);

	// 4. Table joins with USING and multi-equi conditions
	let res =
		query_mysql(&mut stream, "CREATE TABLE employees (id int, dept_id int, name text);").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(&mut stream, "CREATE TABLE depts (dept_id int, dept_name text);").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(
		&mut stream,
		"INSERT INTO employees (id, dept_id, name) VALUES (1, 10, 'Alice'), (2, 20, 'Bob'), (3, 99, 'Charlie');",
	)
	.await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(
		&mut stream,
		"INSERT INTO depts (dept_id, dept_name) VALUES (10, 'Engineering'), (20, 'Sales'), (30, 'HR');",
	)
	.await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Inner join with USING (dept_id)
	let res = query_mysql(
		&mut stream,
		"SELECT employees.name, depts.dept_name FROM employees JOIN depts USING (dept_id) ORDER BY employees.id ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("Alice"));
	assert_eq!(r0[1].as_deref(), Some("Engineering"));

	// RIGHT OUTER JOIN rewrite
	let res = query_mysql(
		&mut stream,
		"SELECT employees.name, depts.dept_name FROM employees RIGHT JOIN depts ON employees.dept_id = depts.dept_id ORDER BY depts.dept_id ASC;",
	)
	.await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 3, "All 3 departments should be present");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_interactive_transactions_and_savepoints() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let res = query_mysql(&mut stream, "CREATE TABLE sp_demo (val text);").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Start transaction
	let res = query_mysql(&mut stream, "START TRANSACTION;").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(&mut stream, "INSERT INTO sp_demo (val) VALUES ('first');").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Savepoint 1
	let res = query_mysql(&mut stream, "SAVEPOINT sp1;").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(&mut stream, "INSERT INTO sp_demo (val) VALUES ('second');").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Savepoint 2
	let res = query_mysql(&mut stream, "SAVEPOINT sp2;").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(&mut stream, "INSERT INTO sp_demo (val) VALUES ('third');").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Rollback to savepoint 2 (undo 'third')
	let res = query_mysql(&mut stream, "ROLLBACK TO SAVEPOINT sp2;").await;
	assert!(matches!(res, QueryResult::Ok));

	// Release savepoint 1
	let res = query_mysql(&mut stream, "RELEASE SAVEPOINT sp1;").await;
	assert!(matches!(res, QueryResult::Ok));

	// Commit outer transaction
	let res = query_mysql(&mut stream, "COMMIT;").await;
	assert!(matches!(res, QueryResult::Ok));

	// Verify 'first' and 'second' are present, 'third' was rolled back
	let res = query_mysql(&mut stream, "SELECT val FROM sp_demo ORDER BY val ASC;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 2);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("first"));
	let r1 = decode_mysql_text_row(&rows[1]);
	assert_eq!(r1[0].as_deref(), Some("second"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_autocommit_toggle_and_ddl_implicit_commit() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let res = query_mysql(&mut stream, "CREATE TABLE auto_demo (val text);").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Disable autocommit
	let res = query_mysql(&mut stream, "SET autocommit = 0;").await;
	assert!(matches!(res, QueryResult::Ok));

	// Insert in interactive transaction block
	let res =
		query_mysql(&mut stream, "INSERT INTO auto_demo (val) VALUES ('persisted_by_ddl');").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// DDL statement implicitly commits active transaction in MySQL!
	let res = query_mysql(&mut stream, "CREATE TABLE extra_tbl (id int);").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	// Rollback should NOT roll back the previous insert because CREATE TABLE implicitly committed
	// it
	let res = query_mysql(&mut stream, "ROLLBACK;").await;
	assert!(matches!(res, QueryResult::Ok));

	// Verify row was persisted
	let res = query_mysql(&mut stream, "SELECT val FROM auto_demo;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	assert_eq!(rows.len(), 1);
	let r0 = decode_mysql_text_row(&rows[0]);
	assert_eq!(r0[0].as_deref(), Some("persisted_by_ddl"));

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_oversized_packet_is_refused_with_er_net_packet_too_large() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Continuation packets that together exceed `max_allowed_packet`. The last header is sent
	// without its body: the server refuses on the declared size.
	let chunks = MAX_ALLOWED_PACKET / MAX_PACKET_SIZE + 1;
	let body = vec![0u8; MAX_PACKET_SIZE];
	for seq in 0..chunks - 1 {
		write_packet(&mut stream, seq as u8, &body).await;
	}
	stream
		.write_all(&[0xff, 0xff, 0xff, (chunks - 1) as u8])
		.await
		.expect("Failed to write the final packet header");

	let (seq, payload) = timeout(Duration::from_secs(60), read_packet(&mut stream))
		.await
		.expect("the server must answer an oversized packet");
	assert_eq!(seq as usize, chunks, "the error follows the last packet the client sent");
	assert_eq!(payload[0], 0xff, "Expected an ERR packet");
	assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), 1153);
	assert_eq!(&payload[3..9], b"#08S01");

	// The connection is closed after the error.
	let mut rest = Vec::new();
	let closed = timeout(Duration::from_secs(60), stream.read_to_end(&mut rest))
		.await
		.expect("the server must close the connection");
	if closed.is_ok() {
		assert!(rest.is_empty());
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_advertised_max_allowed_packet_is_the_enforced_limit() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	let res = query_mysql(&mut stream, "SELECT @@max_allowed_packet;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let row = decode_mysql_text_row(&rows[0]);
	assert_eq!(row[0].as_deref(), Some(MAX_ALLOWED_PACKET.to_string().as_str()));

	shutdown.cancel();
}

/// Two sessions on one server. Session `a` holds an interactive transaction that wrote a row;
/// session `b` has since rewritten that row and committed, so `a`'s commit conflicts. With
/// `autocommit_off`, `a` gets its transaction from `SET autocommit = 0` instead of
/// `START TRANSACTION`.
async fn conflicting_sessions(
	addr: std::net::SocketAddr,
	autocommit_off: bool,
) -> (TcpStream, TcpStream) {
	let mut a = connect_and_handshake(addr).await;
	let mut b = connect_and_handshake(addr).await;

	let res = query_mysql(&mut a, "CREATE TABLE conflict_t (id int, v int);").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));
	let res = query_mysql(&mut a, "INSERT INTO conflict_t (id, v) VALUES (1, 0);").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let begin = if autocommit_off {
		"SET autocommit = 0;"
	} else {
		"START TRANSACTION;"
	};
	let res = query_mysql(&mut a, begin).await;
	assert!(matches!(res, QueryResult::Ok));
	let res = query_mysql(&mut a, "UPDATE conflict_t SET v = 1 WHERE v = 0;").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));
	let res = query_mysql(&mut b, "UPDATE conflict_t SET v = 2 WHERE v = 0;").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));
	(a, b)
}

fn assert_commit_failed(res: &QueryResult) {
	let QueryResult::Error {
		code,
		..
	} = *res
	else {
		panic!("Expected the failed commit to be reported, got {res:?}");
	};
	assert_eq!(code, 1180);
}

/// The winning write is intact and the connection still answers.
async fn assert_usable_and_winner_kept(stream: &mut TcpStream) {
	let res = query_mysql(stream, "SELECT v FROM conflict_t;").await;
	let QueryResult::ResultSet {
		rows,
		..
	} = res
	else {
		panic!("Expected ResultSet, got {res:?}");
	};
	let row = decode_mysql_text_row(&rows[0]);
	assert_eq!(row[0].as_deref(), Some("2"));
}

#[tokio::test]
async fn test_mysql_failed_implicit_commit_before_ddl_is_reported_and_the_ddl_is_skipped() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let (mut a, mut b) = conflicting_sessions(addr, false).await;

	// The DDL commits `a`'s transaction first. That commit fails, so it is reported and the
	// DDL does not run.
	assert_commit_failed(&query_mysql(&mut a, "CREATE TABLE after_conflict (id int);").await);
	let res = query_mysql(&mut b, "SELECT * FROM after_conflict;").await;
	assert!(matches!(res, QueryResult::Error { .. }), "the DDL must not have run, got {res:?}");
	assert_usable_and_winner_kept(&mut a).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_failed_implicit_commit_before_prepared_ddl_is_reported_and_the_ddl_is_skipped()
{
	let (addr, shutdown) = start_test_mysql_server().await;
	let (mut a, mut b) = conflicting_sessions(addr, false).await;

	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(b"CREATE TABLE after_conflict_ps (id int)");
	write_packet(&mut a, 0, &prepare).await;
	let (_, resp) = read_packet(&mut a).await;
	assert_eq!(resp[0], 0x00, "Expected a prepare OK, got {resp:?}");
	let stmt_id = [resp[1], resp[2], resp[3], resp[4]];
	assert_eq!(&resp[5..9], &[0, 0, 0, 0], "a DDL statement has no columns or parameters");

	let mut execute = vec![COM_STMT_EXECUTE];
	execute.extend_from_slice(&stmt_id);
	execute.push(CURSOR_TYPE_NO_CURSOR);
	execute.extend_from_slice(&1u32.to_le_bytes());
	write_packet(&mut a, 0, &execute).await;
	assert_commit_failed(&read_query_response(&mut a).await);

	let res = query_mysql(&mut b, "SELECT * FROM after_conflict_ps;").await;
	assert!(matches!(res, QueryResult::Error { .. }), "the DDL must not have run, got {res:?}");

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_failed_implicit_commit_before_start_transaction_is_reported() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let (mut a, _b) = conflicting_sessions(addr, false).await;

	assert_commit_failed(&query_mysql(&mut a, "START TRANSACTION;").await);
	assert_usable_and_winner_kept(&mut a).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_failed_implicit_commit_on_autocommit_on_is_reported() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let (mut a, _b) = conflicting_sessions(addr, true).await;

	assert_commit_failed(&query_mysql(&mut a, "SET autocommit = 1;").await);
	assert_usable_and_winner_kept(&mut a).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mysql_failed_commit_is_reported_and_keeps_the_connection() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let (mut a, _b) = conflicting_sessions(addr, false).await;

	assert_commit_failed(&query_mysql(&mut a, "COMMIT;").await);
	assert_usable_and_winner_kept(&mut a).await;

	shutdown.cancel();
}

/// Skip one length-encoded string in a packet, returning its bytes.
fn lenenc_bytes(buf: &[u8], off: &mut usize) -> Vec<u8> {
	let b = buf[*off];
	let (len, n) = match b {
		0xfc => ((buf[*off + 1] as usize) | ((buf[*off + 2] as usize) << 8), 3),
		0xfd => (
			(buf[*off + 1] as usize)
				| ((buf[*off + 2] as usize) << 8)
				| ((buf[*off + 3] as usize) << 16),
			4,
		),
		_ => (b as usize, 1),
	};
	*off += n;
	let out = buf[*off..*off + len].to_vec();
	*off += len;
	out
}

/// A text-protocol result: each column's name and MySQL type code, and the rows' cells.
struct TextResult {
	columns: Vec<(String, u8)>,
	rows: Vec<Vec<Option<String>>>,
}

impl TextResult {
	fn type_of(&self, name: &str) -> u8 {
		self.columns.iter().find(|(n, _)| n == name).unwrap_or_else(|| panic!("no column {name}")).1
	}

	fn cell(&self, row: usize, name: &str) -> Option<&str> {
		let idx = self
			.columns
			.iter()
			.position(|(n, _)| n == name)
			.unwrap_or_else(|| panic!("no column {name}"));
		self.rows[row][idx].as_deref()
	}
}

async fn query_text(stream: &mut TcpStream, sql: &str) -> TextResult {
	let mut q = vec![COM_QUERY];
	q.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &q).await;
	let (_, first) = read_packet(stream).await;
	assert!(first[0] != 0xff, "query failed: {sql}: {}", String::from_utf8_lossy(&first[3..]));
	assert!(first[0] != 0x00, "query returned no result set: {sql}");
	let col_count = first[0] as usize;
	let mut columns = Vec::new();
	for _ in 0..col_count {
		let (_, def) = read_packet(stream).await;
		let mut off = 0;
		for _ in 0..4 {
			lenenc_bytes(&def, &mut off);
		}
		let name = String::from_utf8_lossy(&lenenc_bytes(&def, &mut off)).to_string();
		lenenc_bytes(&def, &mut off);
		off += 1 + 2 + 4;
		columns.push((name, def[off]));
	}
	let (_, eof) = read_packet(stream).await;
	assert_eq!(eof[0], 0xfe);
	let mut rows = Vec::new();
	loop {
		let (_, pkt) = read_packet(stream).await;
		if pkt[0] == 0xfe && pkt.len() < 9 {
			break;
		}
		rows.push(decode_mysql_text_row(&pkt));
	}
	TextResult {
		columns,
		rows,
	}
}

async fn seed_person_with_json_and_arrays(addr: std::net::SocketAddr) {
	seed_surrealql(
		addr,
		&[
			"DEFINE TABLE person SCHEMALESS",
			"CREATE person:tobie CONTENT { name: 'Tobie', tags: ['rust', 'db'], scores: [1, 2, 3], \
		 profile: { city: 'London', langs: ['en', 'fr'], geo: { lat: 51.5, lon: -0.12 } }, \
		 friends: [person:jaime, person:silvana], objs: [{ a: 1 }, { b: 2 }], \
		 joined: [1, NONE, 3], grid: [[1, 2], [3, 4]], born: d'1980-01-01T00:00:00Z', \
		 seen: d'1980-01-01T12:30:45.123456Z', took: 26h, nothing: NULL } RETURN NONE",
		]
		.join("; "),
	)
	.await;
}

#[tokio::test]
async fn test_mywire_composites_are_json_columns_holding_json() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_person_with_json_and_arrays(addr).await;
	let res = query_text(&mut stream, "SELECT * FROM person WHERE name = 'Tobie'").await;
	assert_eq!(res.rows.len(), 1);
	for column in ["tags", "scores", "profile", "friends", "objs", "joined", "grid"] {
		assert_eq!(res.type_of(column), MYSQL_TYPE_JSON, "{column} is a JSON column");
	}
	assert_eq!(res.cell(0, "tags"), Some("[\"rust\",\"db\"]"));
	assert_eq!(res.cell(0, "scores"), Some("[1,2,3]"));
	assert_eq!(
		res.cell(0, "profile"),
		Some(
			"{\"city\":\"London\",\"geo\":{\"lat\":51.5,\"lon\":-0.12},\"langs\":[\"en\",\"fr\"]}"
		)
	);
	assert_eq!(res.cell(0, "friends"), Some("[\"jaime\",\"silvana\"]"));
	assert_eq!(res.cell(0, "objs"), Some("[{\"a\":1},{\"b\":2}]"));
	assert_eq!(res.cell(0, "joined"), Some("[1,null,3]"));
	assert_eq!(res.cell(0, "grid"), Some("[[1,2],[3,4]]"));
	assert_eq!(res.cell(0, "nothing"), None);
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_datetimes_and_durations_use_mysql_text_forms() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_person_with_json_and_arrays(addr).await;
	let res = query_text(&mut stream, "SELECT * FROM person WHERE name = 'Tobie'").await;
	assert_eq!(res.type_of("born"), MYSQL_TYPE_TIMESTAMP);
	assert_eq!(res.cell(0, "born"), Some("1980-01-01 00:00:00"));
	assert_eq!(res.cell(0, "seen"), Some("1980-01-01 12:30:45.123456"));
	assert_eq!(res.type_of("took"), MYSQL_TYPE_TIME);
	assert_eq!(res.cell(0, "took"), Some("26:00:00"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_mysql_dialect_select_star_over_a_schemaless_table_returns_its_columns() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_person_with_json_and_arrays(addr).await;
	let res = query_mysql(&mut stream, "SET dialect = 'mysql'").await;
	assert!(matches!(res, QueryResult::Ok));
	let res = query_text(&mut stream, "SELECT * FROM person WHERE name = 'Tobie'").await;
	assert_eq!(res.rows.len(), 1);
	let names: Vec<&str> = res.columns.iter().map(|(n, _)| n.as_str()).collect();
	assert!(!names.contains(&"*"), "the wildcard must not surface as a column: {names:?}");
	for expected in ["name", "tags", "profile", "born", "nothing"] {
		assert!(names.contains(&expected), "missing column {expected} in {names:?}");
	}
	assert_eq!(res.cell(0, "name"), Some("Tobie"));
	assert_eq!(res.cell(0, "tags"), Some("[\"rust\",\"db\"]"));
	assert_eq!(res.cell(0, "born"), Some("1980-01-01 00:00:00"));
	assert_eq!(res.cell(0, "nothing"), None);
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_mysql_dialect_reads_arrays_objects_and_datetimes() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_person_with_json_and_arrays(addr).await;
	let res = query_mysql(&mut stream, "SET dialect = 'mysql'").await;
	assert!(matches!(res, QueryResult::Ok));
	for (column, text) in [
		("tags", "[\"rust\",\"db\"]"),
		("grid", "[[1,2],[3,4]]"),
		("friends", "[\"jaime\",\"silvana\"]"),
		("born", "1980-01-01 00:00:00"),
		("took", "26:00:00"),
	] {
		let res =
			query_text(&mut stream, &format!("SELECT {column} FROM person WHERE name = 'Tobie'"))
				.await;
		assert_eq!(res.rows.len(), 1, "{column}");
		assert_eq!(res.cell(0, column), Some(text), "{column}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_sql_dialects_show_every_record_id_as_its_key() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		&[
			"DEFINE TABLE emp SCHEMALESS",
			"CREATE emp:7 CONTENT { name: 'Eve', dept: dept:1, friends: [emp:8, emp:9], \
		 meta: { boss: emp:8 }, handle: emp:tobie } RETURN NONE",
		]
		.join("; "),
	)
	.await;
	for dialect in ["mysql"] {
		let res = query_mysql(&mut stream, &format!("SET dialect = '{dialect}'")).await;
		assert!(matches!(res, QueryResult::Ok), "{dialect}: {res:?}");
		let sql = "SELECT id, dept, friends, meta, handle FROM emp WHERE name = 'Eve'";
		let res = query_text(&mut stream, sql).await;
		assert_eq!(res.rows.len(), 1, "{dialect}");
		assert_eq!(res.cell(0, "id"), Some("7"), "{dialect}");
		assert_eq!(res.cell(0, "dept"), Some("1"), "{dialect}");
		assert_eq!(res.cell(0, "friends"), Some("[8,9]"), "{dialect}");
		assert_eq!(res.cell(0, "meta"), Some("{\"boss\":8}"), "{dialect}");
		assert_eq!(res.cell(0, "handle"), Some("tobie"), "{dialect}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_select_star_with_no_rows_still_answers_with_a_result_set() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_person_with_json_and_arrays(addr).await;
	let res = query_mysql(&mut stream, "SET dialect = 'mysql'").await;
	assert!(matches!(res, QueryResult::Ok));
	let res = query_text(&mut stream, "SELECT * FROM person WHERE name = 'nobody'").await;
	assert!(res.rows.is_empty());
	assert!(!res.columns.is_empty(), "a result set carries at least one column");
	let res = query_text(&mut stream, "SELECT * FROM person WHERE name = 'Tobie'").await;
	assert_eq!(res.rows.len(), 1, "the same statement with a match still lists its columns");
	assert!(res.columns.iter().any(|(n, _)| n == "name"));
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_a_column_of_mixed_kinds_is_text() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		&[
			"DEFINE TABLE thing SCHEMALESS",
			"CREATE thing:1 SET n = 'a', v = 3 RETURN NONE",
			"CREATE thing:2 SET n = 'b', v = 'three' RETURN NONE",
			"CREATE thing:3 SET n = 'c', w = 1, x = 2 RETURN NONE",
			"CREATE thing:4 SET n = 'd', w = 1.5, x = 2 RETURN NONE",
		]
		.join("; "),
	)
	.await;
	for dialect in ["mysql"] {
		let res = query_mysql(&mut stream, &format!("SET dialect = '{dialect}'")).await;
		assert!(matches!(res, QueryResult::Ok));
		let sql = "SELECT * FROM thing ORDER BY n";
		let res = query_text(&mut stream, sql).await;
		assert_eq!(
			res.type_of("v"),
			MYSQL_TYPE_VAR_STRING,
			"{dialect}: ints and text in one column"
		);
		assert_eq!(res.cell(0, "v"), Some("3"), "{dialect}");
		assert_eq!(res.cell(1, "v"), Some("three"), "{dialect}");
		assert_eq!(res.type_of("w"), MYSQL_TYPE_VAR_STRING, "{dialect}: an int and a float");
		assert_eq!(
			res.type_of("x"),
			MYSQL_TYPE_LONGLONG,
			"{dialect}: a column of one kind keeps its type"
		);
	}
	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_a_named_column_and_select_star_show_one_text_for_a_value() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;
	seed_surrealql(
		addr,
		&[
			"DEFINE TABLE emp SCHEMALESS",
			"CREATE emp:1 CONTENT { name: 'Ada', born: d'1980-01-01T00:00:00Z', \
		 seen: d'1980-01-01T12:30:45.123456Z', early: d'1969-12-31T23:59:59Z', took: 26h, \
		 short: 1h30m, zero: 0s, flag: true, ratio: 1.5, price: 1.25dec, \
		 u: u'018f6b2e-4c3d-7a11-9a52-0123456789ab', tags: ['rust', 'db'], \
		 meta: { a: 1 } } RETURN NONE",
		]
		.join("; "),
	)
	.await;
	let fields = [
		"born", "seen", "early", "took", "short", "zero", "flag", "ratio", "price", "u", "tags",
		"meta",
	];
	let star = query_text(&mut stream, "SELECT * FROM emp WHERE name = 'Ada'").await;
	let named = query_text(
		&mut stream,
		&format!("SELECT {} FROM emp WHERE name = 'Ada'", fields.join(", ")),
	)
	.await;
	for field in fields {
		assert_eq!(named.cell(0, field), star.cell(0, field), "{field}");
	}
	// The temporal fields are typed apart in the two results, and show one text.
	for (field, ty, text) in [
		("born", MYSQL_TYPE_TIMESTAMP, "1980-01-01 00:00:00"),
		("seen", MYSQL_TYPE_TIMESTAMP, "1980-01-01 12:30:45.123456"),
		("early", MYSQL_TYPE_TIMESTAMP, "1969-12-31 23:59:59"),
		("took", MYSQL_TYPE_TIME, "26:00:00"),
		("short", MYSQL_TYPE_TIME, "01:30:00"),
		("zero", MYSQL_TYPE_TIME, "00:00:00"),
	] {
		assert_eq!(star.type_of(field), ty, "`SELECT *` types {field}");
		assert_ne!(named.type_of(field), ty, "a named column {field} is not typed from its rows");
		assert_eq!(named.cell(0, field), Some(text), "{field}");
	}
	assert_eq!(named.cell(0, "flag"), Some("1"));
	assert_eq!(named.cell(0, "price"), Some("1.25"));
	assert_eq!(named.cell(0, "u"), Some("018f6b2e-4c3d-7a11-9a52-0123456789ab"));
	assert_eq!(named.cell(0, "tags"), Some("[\"rust\",\"db\"]"));
	shutdown.cancel();
}
