use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

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
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_mywire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start mywire server");

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
	match res {
		QueryResult::ResultSet {
			columns,
			rows,
		} => {
			assert_eq!(columns, 1);
			assert_eq!(rows.len(), 1);
		}
		_ => panic!("Expected ResultSet for @@version_comment"),
	}

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
	match res {
		QueryResult::ResultSet {
			columns,
			rows,
		} => {
			assert_eq!(columns, 1, "Expected name column");
			assert_eq!(rows.len(), 1, "Expected 1 row from person");
		}
		_ => panic!("Expected ResultSet for SELECT"),
	}

	// Clean quit
	write_packet(&mut stream, 0, &[COM_QUIT]).await;

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_dialect_switching_and_fail_closed() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// In default MySQL dialect: SurrealQL query is rejected with Error packet (1064)
	let mut fail_q = vec![COM_QUERY];
	fail_q.extend_from_slice(b"CREATE person SET name = 'Invalid';");
	write_packet(&mut stream, 0, &fail_q).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::Error {
			code,
			..
		} => {
			assert_eq!(code, 1064, "SurrealQL query must fail in MySQL dialect with 1064");
		}
		_ => panic!("SurrealQL query should have failed in MySQL dialect"),
	}

	// Switch dialect to SurrealQL via SET dialect = 'surrealql'
	let mut set_q = vec![COM_QUERY];
	set_q.extend_from_slice(b"SET dialect = 'surrealql';");
	write_packet(&mut stream, 0, &set_q).await;

	let res = read_query_response(&mut stream).await;
	assert!(matches!(res, QueryResult::Ok), "SET dialect = 'surrealql' must succeed");

	// Now SurrealQL CREATE SET succeeds!
	let mut surreal_q = vec![COM_QUERY];
	surreal_q.extend_from_slice(b"CREATE person SET name = 'Surreal';");
	write_packet(&mut stream, 0, &surreal_q).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1, "Expected created row in SurrealQL mode");
		}
		QueryResult::Ok => {}
		QueryResult::Error {
			code,
			message,
		} => {
			panic!("CREATE person SET failed in SurrealQL mode: {code} {message}");
		}
	}

	// In SurrealQL dialect, invalid SurrealQL must fail closed without fallback to MySQL
	let mut err_q = vec![COM_QUERY];
	err_q.extend_from_slice(b"SELECT 1 FROM WHERE;");
	write_packet(&mut stream, 0, &err_q).await;

	let res = read_query_response(&mut stream).await;
	assert!(
		matches!(res, QueryResult::Error { .. }),
		"Invalid SurrealQL must fail closed in SurrealQL dialect"
	);

	// Switch dialect back to MySQL via SET dialect = 'mysql'
	let mut back_q = vec![COM_QUERY];
	back_q.extend_from_slice(b"SET dialect = 'mysql';");
	write_packet(&mut stream, 0, &back_q).await;

	let res = read_query_response(&mut stream).await;
	assert!(matches!(res, QueryResult::Ok), "SET dialect = 'mysql' must succeed");

	// Now MySQL queries work again
	let mut select_q = vec![COM_QUERY];
	select_q.extend_from_slice(b"SELECT name FROM person;");
	write_packet(&mut stream, 0, &select_q).await;

	let res = read_query_response(&mut stream).await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1);
		}
		_ => panic!("Expected ResultSet from SELECT"),
	}

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
	match res {
		QueryResult::Error {
			code,
			message,
			..
		} => {
			assert_eq!(code, 1064);
			assert!(
				message.contains("parameter placeholder") || message.contains("not supported"),
				"Expected error mentioning parameter placeholders, got: {message}"
			);
		}
		_ => panic!("Expected Error packet for SELECT ?, got: {res:?}"),
	}

	// Also in INSERT VALUES (?)
	let mut q2 = vec![COM_QUERY];
	q2.extend_from_slice(b"INSERT INTO person (name) VALUES (?);");
	write_packet(&mut stream, 0, &q2).await;

	let res2 = read_query_response(&mut stream).await;
	match res2 {
		QueryResult::Error {
			code,
			..
		} => {
			assert_eq!(code, 1064);
		}
		_ => panic!("Expected Error packet for INSERT VALUES (?), got: {res2:?}"),
	}

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
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("7"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 100 - 35;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("65"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 50 / 2;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("25"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 17 % 5;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("2"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 2. Parentheses & precedence
	let res = query_mysql(&mut stream, "SELECT (10 + 20) * 3 / 2;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("45"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	shutdown.cancel();
}

#[tokio::test]
async fn test_mywire_query_matrix_booleans_and_comparisons() {
	let (addr, shutdown) = start_test_mysql_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. Comparisons
	let res = query_mysql(&mut stream, "SELECT 1 = 1;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 1 != 2;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 5 > 3;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 2 < 4;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 2. BETWEEN & IN
	let res = query_mysql(&mut stream, "SELECT 5 BETWEEN 1 AND 10;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 15 BETWEEN 1 AND 10;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("0"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 3 IN (1, 2, 3);").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 4 IN (1, 2, 3);").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("0"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 3. IS NULL / IS NOT NULL
	let res = query_mysql(&mut stream, "SELECT NULL IS NULL;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT 1 IS NOT NULL;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("1"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 4. CASE expressions
	let res =
		query_mysql(&mut stream, "SELECT CASE WHEN 1 = 1 THEN 'alpha' ELSE 'beta' END;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("alpha"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

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
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 2, "Expected Oranges and Bananas");
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 4. SELECT ORDER BY ASC / DESC
	let res = query_mysql(&mut stream, "SELECT name FROM items ORDER BY qty ASC;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("Apples"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SELECT name FROM items ORDER BY qty DESC;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("Bananas"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 5. SELECT LIMIT and OFFSET
	let res =
		query_mysql(&mut stream, "SELECT name FROM items ORDER BY qty ASC LIMIT 1 OFFSET 1;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1);
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("Oranges"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 6. UPDATE
	let res = query_mysql(&mut stream, "UPDATE items SET qty = 15 WHERE name = 'Apples';").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(&mut stream, "SELECT qty FROM items WHERE name = 'Apples';").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("15"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 7. DELETE
	let res = query_mysql(&mut stream, "DELETE FROM items WHERE name = 'Bananas';").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(&mut stream, "SELECT name FROM items;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 2, "Expected 2 items remaining after delete");
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

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
	match res {
		QueryResult::Error {
			code,
			..
		} => assert_eq!(code, 1235),
		other => panic!("Expected Error 1235 for SET ROLE, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").await;
	match res {
		QueryResult::Error {
			code,
			..
		} => assert_eq!(code, 1235),
		other => panic!("Expected Error 1235 for SET TRANSACTION, got {other:?}"),
	}

	let res = query_mysql(&mut stream, "SET unknown_variable = 1;").await;
	match res {
		QueryResult::Error {
			code,
			..
		} => assert_eq!(code, 1235),
		other => panic!("Expected Error 1235 for unknown SET, got {other:?}"),
	}

	// 2. Driver noise SET statements should succeed with OK packet
	let res = query_mysql(&mut stream, "SET autocommit = 1;").await;
	assert!(matches!(res, QueryResult::Ok));

	let res = query_mysql(&mut stream, "SET NAMES utf8mb4;").await;
	assert!(matches!(res, QueryResult::Ok));

	// 3. Out of bounds GROUP BY ordinal must fail closed
	let res = query_mysql(&mut stream, "SELECT 1 GROUP BY 99;").await;
	match res {
		QueryResult::Error {
			..
		} => {}
		other => panic!("Expected Error for out of bounds GROUP BY ordinal, got {other:?}"),
	}

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
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1);
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("Alice"));
			assert_eq!(cols[1].as_deref(), Some("alice@mysql.com"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

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
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("test"));
		}
		other => panic!("Expected ResultSet for SELECT DATABASE(), got {other:?}"),
	}

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
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 2);
			let cols0 = decode_mysql_text_row(&rows[0]);
			let cols1 = decode_mysql_text_row(&rows[1]);
			assert_eq!(cols0[1].as_deref(), Some("Alice"));
			assert_eq!(cols1[1].as_deref(), Some("Bob"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 5. Switch database using text `USE analytics;`
	let res = query_mysql(&mut stream, "USE analytics;").await;
	assert!(matches!(res, QueryResult::Ok), "USE analytics failed");

	// 6. SELECT DATABASE() reflects new active database
	let res = query_mysql(&mut stream, "SELECT DATABASE();").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("analytics"));
		}
		other => panic!("Expected ResultSet for SELECT DATABASE(), got {other:?}"),
	}

	// 7. Create table and insert in new database "analytics"
	let res =
		query_mysql(&mut stream, "CREATE TABLE events (event_id INT, action VARCHAR(50));").await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res =
		query_mysql(&mut stream, "INSERT INTO events (event_id, action) VALUES (10, 'click');")
			.await;
	assert!(matches!(res, QueryResult::Ok | QueryResult::ResultSet { .. }));

	let res = query_mysql(&mut stream, "SELECT event_id, action FROM events;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1);
			let cols0 = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols0[1].as_deref(), Some("click"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

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
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1);
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("5"), "count(*)");
			assert_eq!(cols[1].as_deref(), Some("385"), "sum(price)");
			assert_eq!(cols[2].as_deref(), Some("15"), "min(price)");
			assert_eq!(cols[3].as_deref(), Some("200"), "max(price)");
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 2. Single-column GROUP BY
	let res = query_mysql(
		&mut stream,
		"SELECT category, count(*), sum(price) FROM items GROUP BY category ORDER BY category ASC;",
	)
	.await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
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
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 3. Positional GROUP BY 1
	let res =
		query_mysql(&mut stream, "SELECT category, count(*) FROM items GROUP BY 1 ORDER BY 1 ASC;")
			.await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 3);
			let r0 = decode_mysql_text_row(&rows[0]);
			assert_eq!(r0[0].as_deref(), Some("books"));
			let r1 = decode_mysql_text_row(&rows[1]);
			assert_eq!(r1[0].as_deref(), Some("clothing"));
			let r2 = decode_mysql_text_row(&rows[2]);
			assert_eq!(r2[0].as_deref(), Some("electronics"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 4. GROUP BY alias (MySQL feature)
	let res = query_mysql(
		&mut stream,
		"SELECT category AS cat, count(*) FROM items GROUP BY cat ORDER BY cat ASC;",
	)
	.await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 3);
			let r0 = decode_mysql_text_row(&rows[0]);
			assert_eq!(r0[0].as_deref(), Some("books"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 5. HAVING clause with aggregate present in targets
	let res = query_mysql(
		&mut stream,
		"SELECT category, count(*) AS cnt FROM items GROUP BY category HAVING count(*) > 1 ORDER BY category ASC;",
	)
	.await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 2);
			let r0 = decode_mysql_text_row(&rows[0]);
			assert_eq!(r0[0].as_deref(), Some("books"));
			let r1 = decode_mysql_text_row(&rows[1]);
			assert_eq!(r1[0].as_deref(), Some("electronics"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 6. HAVING clause with aggregate NOT present in targets
	let res = query_mysql(
		&mut stream,
		"SELECT category FROM items GROUP BY category HAVING count(*) > 1 ORDER BY category ASC;",
	)
	.await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 2);
			let r0 = decode_mysql_text_row(&rows[0]);
			assert_eq!(r0[0].as_deref(), Some("books"));
			let r1 = decode_mysql_text_row(&rows[1]);
			assert_eq!(r1[0].as_deref(), Some("electronics"));
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 7. Global COUNT(DISTINCT col)
	let res = query_mysql(&mut stream, "SELECT count(DISTINCT category) FROM items;").await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 1);
			let cols = decode_mysql_text_row(&rows[0]);
			assert_eq!(cols[0].as_deref(), Some("3"), "3 distinct categories");
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	// 8. Multi-column GROUP BY
	let res = query_mysql(
		&mut stream,
		"SELECT category, in_stock, count(*) FROM items GROUP BY category, in_stock ORDER BY category ASC, in_stock ASC;",
	)
	.await;
	match res {
		QueryResult::ResultSet {
			rows,
			..
		} => {
			assert_eq!(rows.len(), 4);
		}
		other => panic!("Expected ResultSet, got {other:?}"),
	}

	shutdown.cancel();
}
