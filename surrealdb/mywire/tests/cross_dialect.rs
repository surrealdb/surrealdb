#![recursion_limit = "512"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_mywire::auth::{
	AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD, compute_mysql_native_password_scramble,
};
use surrealdb_mywire::codec::read_lenenc_str;
use surrealdb_mywire::constants::*;
use surrealdb_mywire::msg::{ColumnDefinition41, StmtPrepareOk};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Initialize a shared datastore with a root administrative user.
async fn setup_shared_datastore() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("Failed to create memory datastore");
	let session = Session::owner();
	let _ = ds
		.run(QueryRequest::new("DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;", &session))
		.await
		.expect("Failed to define root user");
	ds
}

/// Start both PostgreSQL wire and MySQL wire servers on the same shared datastore.
async fn start_cross_dialect_servers(
	ds: Arc<Datastore>,
) -> (std::net::SocketAddr, std::net::SocketAddr, CancellationToken) {
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let pg_bind: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let pg_addr = surrealdb_pgwire::start(
		pg_bind,
		Arc::clone(&ds),
		Arc::clone(&ready),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("Failed to start pgwire server");

	let my_bind: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let my_addr = surrealdb_mywire::start(
		my_bind,
		Arc::clone(&ds),
		Arc::clone(&ready),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("Failed to start mywire server");

	(pg_addr, my_addr, shutdown)
}

// =========================================================================
// PostgreSQL Client Helpers
// =========================================================================

async fn pg_connect(
	addr: std::net::SocketAddr,
	database: &str,
	options: Option<&str>,
) -> Result<TcpStream, String> {
	let mut stream = TcpStream::connect(addr).await.map_err(|e| format!("Connect failed: {e}"))?;

	let mut body = BytesMut::new();
	body.put_i32(196608); // Protocol 3.0
	body.put_slice(b"user\0root\0");
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
	let mut pkt = BytesMut::new();
	pkt.put_i32(len);
	pkt.put(body);

	stream.write_all(&pkt).await.map_err(|e| format!("Write startup failed: {e}"))?;

	let mut ready = false;
	let mut err: Option<String> = None;

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
						let mut p_pkt = BytesMut::new();
						p_pkt.put_u8(b'p');
						p_pkt.put_i32(9); // 4 (len) + 4 (password) + 1 (null)
						p_pkt.put_slice(b"root\0");
						stream.write_all(&p_pkt).await?;
					}
				}
				b'Z' => {
					ready = true;
				}
				b'E' => {
					err = Some(String::from_utf8_lossy(&payload).to_string());
				}
				_ => {}
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	if res.is_err() || !ready {
		Err(err.unwrap_or_else(|| "PostgreSQL handshake timed out".to_string()))
	} else {
		Ok(stream)
	}
}

async fn pg_query(
	stream: &mut TcpStream,
	sql: &str,
) -> Result<Vec<Vec<Option<String>>>, (String, String)> {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send PG query");

	let mut raw_rows = Vec::new();
	let mut last_err = None;

	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'D' {
				raw_rows.push(payload);
			} else if msg_type == b'Z' {
				break;
			} else if msg_type == b'E' {
				last_err = Some(parse_pg_error(&payload));
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for PG query: {sql}");

	if let Some(e) = last_err {
		Err(e)
	} else {
		let decoded = raw_rows.iter().map(|r| decode_pg_row(r)).collect();
		Ok(decoded)
	}
}

fn parse_pg_error(payload: &[u8]) -> (String, String) {
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
			idx += 1;
		}
		if field_type == b'C' {
			code = val;
		} else if field_type == b'M' {
			message = val;
		}
	}
	(code, message)
}

fn decode_pg_row(payload: &[u8]) -> Vec<Option<String>> {
	let mut cur = payload;
	let col_count = cur.get_i16() as usize;
	let mut cols = Vec::with_capacity(col_count);
	for _ in 0..col_count {
		let len = cur.get_i32();
		if len < 0 {
			cols.push(None);
		} else {
			let mut bytes = vec![0u8; len as usize];
			cur.copy_to_slice(&mut bytes);
			cols.push(Some(String::from_utf8_lossy(&bytes).to_string()));
		}
	}
	cols
}

// =========================================================================
// MySQL Client Helpers
// =========================================================================

async fn mysql_read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
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

async fn mysql_write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	if len > 0 {
		stream.write_all(payload).await.expect("Failed to write packet payload");
	}
}

fn extract_mysql_salt(handshake_payload: &[u8]) -> [u8; 20] {
	let mut pos = 1;
	while pos < handshake_payload.len() && handshake_payload[pos] != 0 {
		pos += 1;
	}
	pos += 1; // skip null
	pos += 4; // connection id (4 bytes)

	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake_payload[pos..pos + 8]);
	pos += 8;
	pos += 1; // filler 0x00
	pos += 2; // capabilities lower
	pos += 1; // charset
	pos += 2; // status flags
	pos += 2; // capabilities upper
	let auth_len = handshake_payload[pos];
	pos += 1;
	pos += 10; // reserved zeros

	let remaining_len = if auth_len > 8 {
		((auth_len - 8) as usize).min(12)
	} else {
		12
	};
	salt[8..8 + remaining_len].copy_from_slice(&handshake_payload[pos..pos + remaining_len]);
	salt
}

async fn mysql_connect(
	addr: std::net::SocketAddr,
	database: &str,
	deprecate_eof: bool,
) -> TcpStream {
	let mut stream =
		TcpStream::connect(addr).await.expect("Failed to connect to MySQL wire server");

	// 1. Read Initial Handshake Packet
	let (_, initial_handshake) = mysql_read_packet(&mut stream).await;
	assert_eq!(initial_handshake[0], 0x0a, "Expected HandshakeV10");

	let salt = extract_mysql_salt(&initial_handshake);
	let scramble = compute_mysql_native_password_scramble("root", &salt);

	// 2. Build HandshakeResponse41
	let mut extra_caps = 0u32;
	if deprecate_eof {
		extra_caps |= CLIENT_DEPRECATE_EOF;
	}
	let caps: u32 = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| extra_caps;

	let mut resp = Vec::new();
	resp.extend_from_slice(&caps.to_le_bytes());
	let max_packet: u32 = 16 * 1024 * 1024;
	resp.extend_from_slice(&max_packet.to_le_bytes());
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	resp.extend_from_slice(&[0u8; 23]);

	resp.extend_from_slice(b"root\0");
	resp.push(scramble.len() as u8);
	resp.extend_from_slice(&scramble);

	resp.extend_from_slice(database.as_bytes());
	resp.push(0);

	resp.extend_from_slice(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD.as_bytes());
	resp.push(0);

	mysql_write_packet(&mut stream, 1, &resp).await;

	// 3. Read response
	let (seq, auth_resp) = mysql_read_packet(&mut stream).await;
	if auth_resp[0] == 0xfe {
		// AuthSwitchRequest
		mysql_write_packet(&mut stream, seq.wrapping_add(1), b"root\0").await;
		let (_, ok_pkt) = mysql_read_packet(&mut stream).await;
		assert_eq!(ok_pkt[0], OK_HEADER, "Expected OK packet after auth switch");
	} else {
		assert_eq!(auth_resp[0], OK_HEADER, "Expected OK packet from initial handshake");
	}

	stream
}

#[allow(dead_code)]
#[derive(Debug)]
enum MyQueryResult {
	Ok,
	Error {
		code: u16,
		message: String,
	},
	ResultSet {
		columns: usize,
		rows: Vec<Vec<Option<String>>>,
	},
}

async fn mysql_query(stream: &mut TcpStream, sql: &str) -> MyQueryResult {
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(sql.as_bytes());
	mysql_write_packet(stream, 0, &query_payload).await;

	let (_, first) = mysql_read_packet(stream).await;
	if first[0] == OK_HEADER {
		return MyQueryResult::Ok;
	}
	if first[0] == ERR_HEADER {
		let code = if first.len() >= 3 {
			(first[1] as u16) | ((first[2] as u16) << 8)
		} else {
			0
		};
		let message = String::from_utf8_lossy(&first[3..]).to_string();
		return MyQueryResult::Error {
			code,
			message,
		};
	}

	let col_count = first[0] as usize;
	for _ in 0..col_count {
		let _ = mysql_read_packet(stream).await;
	}

	// Read EOF packet separating columns and rows (if non-deprecate EOF)
	let (_, intermediate) = mysql_read_packet(stream).await;
	let mut rows = Vec::new();

	if intermediate[0] == EOF_HEADER && intermediate.len() < 9 {
		// Non-deprecate EOF mode
		loop {
			let (_, pkt) = mysql_read_packet(stream).await;
			if pkt[0] == EOF_HEADER && pkt.len() < 9 {
				break;
			}
			rows.push(decode_mysql_text_row(&pkt, col_count));
		}
	} else {
		// Deprecate EOF mode: first row or EOF_OK was read
		if intermediate[0] == EOF_HEADER && intermediate.len() >= 9 {
			// Empty resultset with EOF_OK
		} else {
			rows.push(decode_mysql_text_row(&intermediate, col_count));
			loop {
				let (_, pkt) = mysql_read_packet(stream).await;
				if pkt[0] == EOF_HEADER {
					break;
				}
				rows.push(decode_mysql_text_row(&pkt, col_count));
			}
		}
	}

	MyQueryResult::ResultSet {
		columns: col_count,
		rows,
	}
}

fn decode_mysql_text_row(mut payload: &[u8], col_count: usize) -> Vec<Option<String>> {
	let mut cols = Vec::with_capacity(col_count);
	for _ in 0..col_count {
		if payload.is_empty() {
			break;
		}
		if payload[0] == 0xfb {
			cols.push(None);
			payload = &payload[1..];
		} else {
			let s = read_lenenc_str(&mut payload).map(|v| v.to_string());
			cols.push(s);
		}
	}
	cols
}

// =========================================================================
// Cross-Dialect Integration Tests (Phase 6)
// =========================================================================

#[tokio::test]
async fn test_cross_dialect_pg_ddl_and_insert_mysql_query_and_update_pg_verify() {
	let ds = setup_shared_datastore().await;
	let (pg_addr, my_addr, shutdown) = start_cross_dialect_servers(ds).await;

	let mut pg_stream = pg_connect(pg_addr, "test/test", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection failed");
	let mut my_stream = mysql_connect(my_addr, "test/test", false).await;

	// 1. PostgreSQL creates table
	let res = pg_query(
		&mut pg_stream,
		"CREATE TABLE products (sku VARCHAR(64), item_name VARCHAR(128), price DOUBLE PRECISION, in_stock INT);",
	)
	.await;
	assert!(res.is_ok(), "PG DDL CREATE TABLE should succeed: {res:?}");

	// 2. PostgreSQL inserts initial inventory
	let res = pg_query(
		&mut pg_stream,
		"INSERT INTO products (sku, item_name, price, in_stock) VALUES \
		 ('SKU-001', 'Ergonomic Keyboard', 89.99, 40), \
		 ('SKU-002', 'Wireless Mouse', 24.50, 150);",
	)
	.await;
	assert!(res.is_ok(), "PG INSERT should succeed: {res:?}");

	// 3. MySQL queries the table created and populated by PostgreSQL
	let my_res = mysql_query(
		&mut my_stream,
		"SELECT sku, item_name, price, in_stock FROM products ORDER BY sku ASC;",
	)
	.await;

	let MyQueryResult::ResultSet {
		columns,
		rows,
	} = my_res
	else {
		panic!("Expected ResultSet from MySQL query, got: {my_res:?}");
	};
	assert_eq!(columns, 4);
	assert_eq!(rows.len(), 2);
	assert_eq!(
		rows[0],
		vec![
			Some("SKU-001".to_string()),
			Some("Ergonomic Keyboard".to_string()),
			Some("89.99".to_string()),
			Some("40".to_string()),
		]
	);
	assert_eq!(
		rows[1],
		vec![
			Some("SKU-002".to_string()),
			Some("Wireless Mouse".to_string()),
			Some("24.5".to_string()),
			Some("150".to_string()),
		]
	);

	// 4. MySQL updates one of the rows
	let update_res = mysql_query(
		&mut my_stream,
		"UPDATE products SET in_stock = in_stock - 10 WHERE sku = 'SKU-001';",
	)
	.await;
	assert!(matches!(update_res, MyQueryResult::Ok), "MySQL UPDATE should succeed");

	// 5. PostgreSQL verifies the updated value
	let pg_verify =
		pg_query(&mut pg_stream, "SELECT sku, in_stock FROM products WHERE sku = 'SKU-001';")
			.await
			.expect("PG SELECT should succeed");

	assert_eq!(
		pg_verify,
		vec![vec![Some("SKU-001".to_string()), Some("30".to_string())]],
		"PostgreSQL must observe the update performed by MySQL"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_cross_dialect_null_semantics_and_coalesce() {
	let ds = setup_shared_datastore().await;
	let (pg_addr, my_addr, shutdown) = start_cross_dialect_servers(ds).await;

	let mut pg_stream = pg_connect(pg_addr, "test/test", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection failed");
	let mut my_stream = mysql_connect(my_addr, "test/test", false).await;

	// Setup table via PG
	pg_query(
		&mut pg_stream,
		"CREATE TABLE items (code VARCHAR(32), label VARCHAR(64), rating DOUBLE PRECISION, count INT);",
	)
	.await
	.expect("Create items table");

	// MySQL inserts row containing explicit NULLs
	let ins = mysql_query(
		&mut my_stream,
		"INSERT INTO items (code, label, rating, count) VALUES ('ITEM-X', 'Mystery Device', NULL, NULL);",
	)
	.await;
	assert!(matches!(ins, MyQueryResult::Ok), "MySQL INSERT with NULLs should succeed");

	// PG queries row and asserts NULL fields evaluate to None
	let pg_rows = pg_query(
		&mut pg_stream,
		"SELECT code, label, rating, count FROM items WHERE rating IS NULL;",
	)
	.await
	.expect("PG query IS NULL");

	assert_eq!(
		pg_rows,
		vec![vec![Some("ITEM-X".to_string()), Some("Mystery Device".to_string()), None, None]]
	);

	// PG updates the NULL values to concrete numbers
	pg_query(&mut pg_stream, "UPDATE items SET rating = 4.75, count = 12 WHERE code = 'ITEM-X';")
		.await
		.expect("PG update");

	// MySQL selects with COALESCE function
	let my_rows = mysql_query(
		&mut my_stream,
		"SELECT code, COALESCE(rating, 0.0), count FROM items WHERE code = 'ITEM-X';",
	)
	.await;

	let MyQueryResult::ResultSet {
		rows,
		..
	} = my_rows
	else {
		panic!("Expected ResultSet, got: {my_rows:?}");
	};
	assert_eq!(
		rows,
		vec![vec![Some("ITEM-X".to_string()), Some("4.75".to_string()), Some("12".to_string()),]]
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_cross_dialect_transaction_isolation_and_rollback() {
	let ds = setup_shared_datastore().await;
	let (pg_addr, my_addr, shutdown) = start_cross_dialect_servers(ds).await;

	let mut pg_stream = pg_connect(pg_addr, "test/test", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection failed");
	let mut my_stream = mysql_connect(my_addr, "test/test", false).await;

	// Create table
	pg_query(
		&mut pg_stream,
		"CREATE TABLE ledger (entry_id VARCHAR(32), amount DOUBLE PRECISION);",
	)
	.await
	.expect("Create ledger");

	// 1. MySQL begins transaction and inserts an uncommitted entry
	let tx_start = mysql_query(&mut my_stream, "START TRANSACTION;").await;
	assert!(matches!(tx_start, MyQueryResult::Ok));

	let tx_insert = mysql_query(
		&mut my_stream,
		"INSERT INTO ledger (entry_id, amount) VALUES ('TX-ABORTED', 500.0);",
	)
	.await;
	assert!(matches!(tx_insert, MyQueryResult::Ok));

	// Inside the transaction, MySQL sees the row
	let my_tx_view =
		mysql_query(&mut my_stream, "SELECT count(*) FROM ledger WHERE entry_id = 'TX-ABORTED';")
			.await;
	let MyQueryResult::ResultSet {
		rows,
		..
	} = my_tx_view
	else {
		panic!("Expected row inside tx, got: {my_tx_view:?}");
	};
	assert_eq!(rows, vec![vec![Some("1".to_string())]]);

	// Rollback transaction in MySQL
	let tx_rollback = mysql_query(&mut my_stream, "ROLLBACK;").await;
	assert!(matches!(tx_rollback, MyQueryResult::Ok));

	// PostgreSQL verifies that the rolled-back row does not exist
	let pg_view =
		pg_query(&mut pg_stream, "SELECT count(*) FROM ledger WHERE entry_id = 'TX-ABORTED';")
			.await
			.expect("PG query after rollback");
	assert_eq!(
		pg_view,
		vec![vec![Some("0".to_string())]],
		"Rolled-back transaction must not persist"
	);

	// 2. MySQL starts another transaction and commits
	let tx2_start = mysql_query(&mut my_stream, "START TRANSACTION;").await;
	assert!(matches!(tx2_start, MyQueryResult::Ok));

	let tx2_insert = mysql_query(
		&mut my_stream,
		"INSERT INTO ledger (entry_id, amount) VALUES ('TX-COMMITTED', 1250.0);",
	)
	.await;
	assert!(matches!(tx2_insert, MyQueryResult::Ok));

	let tx2_commit = mysql_query(&mut my_stream, "COMMIT;").await;
	assert!(matches!(tx2_commit, MyQueryResult::Ok));

	// PostgreSQL verifies the committed entry is visible
	let pg_committed = pg_query(
		&mut pg_stream,
		"SELECT entry_id, amount FROM ledger WHERE entry_id = 'TX-COMMITTED';",
	)
	.await
	.expect("PG query after commit");
	assert_eq!(
		pg_committed,
		vec![vec![Some("TX-COMMITTED".to_string()), Some("1250".to_string())]],
		"Committed transaction must be immediately visible to PG"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_cross_dialect_database_namespace_resolution() {
	let ds = setup_shared_datastore().await;
	let (pg_addr, my_addr, shutdown) = start_cross_dialect_servers(ds).await;

	let mut pg_stream = pg_connect(pg_addr, "analytics/analytics", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection to analytics/analytics failed");
	let mut my_stream = mysql_connect(my_addr, "analytics/analytics", false).await;

	// In MySQL, "analytics/analytics" connects with NS=analytics, DB=analytics
	let ddl = mysql_query(
		&mut my_stream,
		"CREATE TABLE metrics (metric_name VARCHAR(64), metric_val DOUBLE);",
	)
	.await;
	assert!(matches!(ddl, MyQueryResult::Ok), "DDL failed: {ddl:?}");

	let ins = mysql_query(
		&mut my_stream,
		"INSERT INTO metrics (metric_name, metric_val) VALUES ('cpu_util', 84.5);",
	)
	.await;
	assert!(matches!(ins, MyQueryResult::Ok));

	// PG connected to "analytics/analytics" must query the exact same table
	let pg_rows = pg_query(
		&mut pg_stream,
		"SELECT metric_name, metric_val FROM metrics WHERE metric_name = 'cpu_util';",
	)
	.await
	.expect("PG query in analytics/analytics");

	assert_eq!(pg_rows, vec![vec![Some("cpu_util".to_string()), Some("84.5".to_string())]]);

	// A third PG connection connected to a different DB ("other/other") must not see this table
	let mut pg_other = pg_connect(pg_addr, "other/other", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection to other/other");

	let err = pg_query(&mut pg_other, "SELECT count(*) FROM metrics;").await;
	assert!(err.is_err(), "Table 'metrics' must not exist in 'other/other' database");

	shutdown.cancel();
}

#[tokio::test]
async fn test_cross_dialect_prepared_statement_interop() {
	let ds = setup_shared_datastore().await;
	let (pg_addr, my_addr, shutdown) = start_cross_dialect_servers(ds).await;

	let mut pg_stream = pg_connect(pg_addr, "test/test", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection failed");
	let mut my_stream = mysql_connect(my_addr, "test/test", true).await; // CLIENT_DEPRECATE_EOF

	// Setup table with records
	pg_query(
		&mut pg_stream,
		"CREATE TABLE catalog (item_id INT, title VARCHAR(64), price DOUBLE PRECISION);",
	)
	.await
	.expect("Create catalog table");

	pg_query(
		&mut pg_stream,
		"INSERT INTO catalog (item_id, title, price) VALUES \
		 (1, 'Book A', 15.0), \
		 (2, 'Book B', 25.0), \
		 (3, 'Book C', 45.0), \
		 (4, 'Book D', 80.0);",
	)
	.await
	.expect("Insert catalog rows");

	// 1. MySQL prepares statement: SELECT title, price FROM catalog WHERE price > ? ORDER BY price
	//    ASC
	let mut prep_payload = vec![COM_STMT_PREPARE];
	prep_payload
		.extend_from_slice(b"SELECT title, price FROM catalog WHERE price > ? ORDER BY price ASC;");
	mysql_write_packet(&mut my_stream, 0, &prep_payload).await;

	let (_, prep_resp) = mysql_read_packet(&mut my_stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	assert_eq!(ok_prep.num_params, 1);
	assert_eq!(ok_prep.num_columns, 2);

	// Read param def packet
	let (_, param_def) = mysql_read_packet(&mut my_stream).await;
	let _ = ColumnDefinition41::deserialize(&param_def).expect("Valid param def");

	// Read 2 column def packets
	let (_, col1) = mysql_read_packet(&mut my_stream).await;
	let c1 = ColumnDefinition41::deserialize(&col1).expect("Valid col1 def");
	assert_eq!(c1.name, "title");

	let (_, col2) = mysql_read_packet(&mut my_stream).await;
	let c2 = ColumnDefinition41::deserialize(&col2).expect("Valid col2 def");
	assert_eq!(c2.name, "price");

	// 2. MySQL executes prepared statement with parameter price = 30.0 (DOUBLE)
	let mut exec_payload = vec![COM_STMT_EXECUTE];
	exec_payload.extend_from_slice(&1u32.to_le_bytes()); // stmt_id = 1
	exec_payload.push(CURSOR_TYPE_NO_CURSOR);
	exec_payload.extend_from_slice(&1u32.to_le_bytes()); // iteration_count = 1

	// Null bitmap (1 param -> 1 byte)
	exec_payload.push(0x00);

	// new_params_bound_flag = 1
	exec_payload.push(0x01);

	// Param type: MYSQL_TYPE_DOUBLE (0x05), flags: 0x00
	exec_payload.push(MYSQL_TYPE_DOUBLE);
	exec_payload.push(0x00);

	// Param value: 30.0f64 LE
	exec_payload.extend_from_slice(&30.0f64.to_le_bytes());

	mysql_write_packet(&mut my_stream, 0, &exec_payload).await;

	// Column count packet
	let (_, col_count_pkt) = mysql_read_packet(&mut my_stream).await;
	assert_eq!(col_count_pkt[0], 2);

	// Column definitions (2 cols)
	let _ = mysql_read_packet(&mut my_stream).await;
	let _ = mysql_read_packet(&mut my_stream).await;

	// Read binary rows
	let mut my_rows = Vec::new();
	loop {
		let (_, row_pkt) = mysql_read_packet(&mut my_stream).await;
		if row_pkt[0] == EOF_HEADER {
			break;
		}
		assert_eq!(row_pkt[0], 0x00, "Binary row header must be 0x00");
		// Binary row payload: header(1) + null bitmap (1 byte for 2 cols: (2+2)/8 = 1 byte)
		// Row 0: "Book C", 45.0
		// Row 1: "Book D", 80.0
		my_rows.push(row_pkt);
	}
	assert_eq!(my_rows.len(), 2, "MySQL prepared statement should return 2 rows with price > 30.0");

	// 3. PostgreSQL executes equivalent query and matches MySQL prepared statement result
	let pg_exec = pg_query(
		&mut pg_stream,
		"SELECT title, price FROM catalog WHERE price > 30.0 ORDER BY price ASC;",
	)
	.await
	.expect("PG SELECT");

	assert_eq!(
		pg_exec,
		vec![
			vec![Some("Book C".to_string()), Some("45".to_string())],
			vec![Some("Book D".to_string()), Some("80".to_string())],
		],
		"PostgreSQL query results must match MySQL prepared query results"
	);

	shutdown.cancel();
}

#[tokio::test]
async fn test_cross_dialect_concurrent_reads() {
	let ds = setup_shared_datastore().await;
	let (pg_addr, my_addr, shutdown) = start_cross_dialect_servers(ds).await;

	// Setup data using PG
	let mut setup_pg = pg_connect(pg_addr, "test/test", Some("-c dialect=pgsql"))
		.await
		.expect("PG connection failed");
	pg_query(&mut setup_pg, "CREATE TABLE logs (log_id INT, msg VARCHAR(64));")
		.await
		.expect("Create logs table");
	pg_query(
		&mut setup_pg,
		"INSERT INTO logs (log_id, msg) VALUES (1, 'info'), (2, 'warning'), (3, 'error');",
	)
	.await
	.expect("Insert logs");

	// Spawn concurrent tasks reading concurrently from PG and MySQL
	let mut handles = Vec::new();

	// 2 PG workers
	for worker_id in 0..2 {
		let addr = pg_addr;
		handles.push(tokio::spawn(async move {
			let mut conn = pg_connect(addr, "test/test", Some("-c dialect=pgsql"))
				.await
				.unwrap_or_else(|e| panic!("PG worker {worker_id} connect failed: {e}"));
			for _ in 0..5 {
				let rows = pg_query(&mut conn, "SELECT count(*) FROM logs;")
					.await
					.unwrap_or_else(|e| panic!("PG worker {worker_id} query failed: {e:?}"));
				assert_eq!(rows, vec![vec![Some("3".to_string())]]);
			}
		}));
	}

	// 2 MySQL workers
	for worker_id in 0..2 {
		let addr = my_addr;
		handles.push(tokio::spawn(async move {
			let mut conn = mysql_connect(addr, "test/test", false).await;
			for _ in 0..5 {
				let res = mysql_query(&mut conn, "SELECT count(*) FROM logs;").await;
				let MyQueryResult::ResultSet {
					rows,
					..
				} = res
				else {
					panic!("MySQL worker {worker_id} query failed: {res:?}");
				};
				assert_eq!(rows, vec![vec![Some("3".to_string())]]);
			}
		}));
	}

	for handle in handles {
		handle.await.expect("Worker task finished successfully");
	}

	shutdown.cancel();
}
