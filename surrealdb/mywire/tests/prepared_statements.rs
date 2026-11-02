#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::{
	AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD, compute_mysql_native_password_scramble,
};
use surrealdb_mywire::constants::*;
use surrealdb_mywire::msg::{ColumnDefinition41, StmtPrepareOk};
use surrealdb_types::ToSql;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

async fn setup_test_datastore() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let session = Session::owner();
	let _ = ds
		.run(surrealdb_core::kvs::QueryRequest::new(
			"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
			&session,
		))
		.await
		.expect("Failed to define root user");
	ds
}

/// Runs SurrealQL in the `test`/`test` database. The wire speaks only MySQL, so fixtures are
/// written in-process.
async fn seed_surrealql(ds: &Datastore, sql: &str) {
	let sess = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(sql, &sess, None).await.expect("seed") {
		response.result.unwrap_or_else(|err| panic!("{sql}: {err}"));
	}
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

fn build_handshake_response_with_caps(
	username: &str,
	database: Option<&str>,
	auth_data: &[u8],
	extra_caps: u32,
) -> Vec<u8> {
	let mut resp = Vec::new();
	let caps: u32 = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| extra_caps;
	resp.extend_from_slice(&caps.to_le_bytes());

	let max_packet: u32 = 16 * 1024 * 1024;
	resp.extend_from_slice(&max_packet.to_le_bytes());
	resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	resp.extend_from_slice(&[0u8; 23]);

	// Username (null-terminated)
	resp.extend_from_slice(username.as_bytes());
	resp.push(0);

	// Auth response
	if auth_data.is_empty() {
		resp.push(0);
	} else {
		resp.push(auth_data.len() as u8);
		resp.extend_from_slice(auth_data);
	}

	// Database (null-terminated)
	let db = database.unwrap_or("test/test");
	resp.extend_from_slice(db.as_bytes());
	resp.push(0);

	// Auth plugin name (null-terminated)
	resp.extend_from_slice(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD.as_bytes());
	resp.push(0);

	resp
}

fn extract_salt(handshake_payload: &[u8]) -> [u8; 20] {
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

async fn perform_handshake(stream: &mut TcpStream, extra_caps: u32) -> (u8, Vec<u8>) {
	let (_, initial_handshake) = read_packet(stream).await;
	assert_eq!(initial_handshake[0], 0x0a, "Expected HandshakeV10 packet");

	let salt = extract_salt(&initial_handshake);
	let scramble = compute_mysql_native_password_scramble("root", &salt);
	let response = build_handshake_response_with_caps("root", Some("test"), &scramble, extra_caps);
	write_packet(stream, 1, &response).await;

	let (seq, resp) = read_packet(stream).await;
	if resp[0] == 0xfe {
		// AuthSwitchRequest
		write_packet(stream, seq.wrapping_add(1), b"root\0").await;
		read_packet(stream).await
	} else {
		(seq, resp)
	}
}

async fn execute_query(stream: &mut TcpStream, sql: &str) {
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &query_payload).await;

	let (_, resp) = read_packet(stream).await;
	assert_eq!(resp[0], OK_HEADER, "Expected OK packet for query: {sql}");
}

#[tokio::test]
async fn test_prepared_statement_prepare_and_execute_simple_select() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed with OK packet");

	// 1. COM_STMT_PREPARE: SELECT 1 + 1 AS sum
	let mut prep_payload = vec![COM_STMT_PREPARE];
	prep_payload.extend_from_slice(b"SELECT 1 + 1 AS sum");
	write_packet(&mut stream, 0, &prep_payload).await;

	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	assert_eq!(ok_prep.num_params, 0);
	assert_eq!(ok_prep.num_columns, 1);

	// Read column def packet for 'sum'
	let (_, col_def_pkt) = read_packet(&mut stream).await;
	let col_def = ColumnDefinition41::deserialize(&col_def_pkt).expect("Valid column definition");
	assert_eq!(col_def.name, "sum");

	// 2. COM_STMT_EXECUTE statement 1
	let mut exec_payload = vec![COM_STMT_EXECUTE];
	exec_payload.extend_from_slice(&1u32.to_le_bytes()); // stmt id
	exec_payload.push(CURSOR_TYPE_NO_CURSOR); // flags
	exec_payload.extend_from_slice(&1u32.to_le_bytes()); // iteration count
	write_packet(&mut stream, 0, &exec_payload).await;

	// Resultset:
	// - Column count packet
	let (_, col_count_pkt) = read_packet(&mut stream).await;
	assert_eq!(col_count_pkt[0], 1);

	// - Column def packet
	let (_, exec_col_def) = read_packet(&mut stream).await;
	let col_def = ColumnDefinition41::deserialize(&exec_col_def).expect("Valid column def");
	assert_eq!(col_def.name, "sum");

	// - Binary row packet: [0x00 (header), 0x00 (null_bitmap), ...]
	let (_, row_pkt) = read_packet(&mut stream).await;
	assert_eq!(row_pkt[0], 0x00, "Binary row header must be 0x00");
	// Null bitmap for 1 column is 1 byte, offset 2 -> bit 2 must be 0 (not null)
	assert_eq!(row_pkt[1] & (1 << 2), 0, "Column 0 should not be null");

	// - Terminating OK packet (CLIENT_DEPRECATE_EOF is set, so starts with 0xfe)
	let (_, term_pkt) = read_packet(&mut stream).await;
	assert_eq!(term_pkt[0], 0xfe, "Expected terminating EOF_OK packet");
}

#[tokio::test]
async fn test_prepared_statement_with_parameters_and_binary_rows() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed");

	// Setup table
	execute_query(
		&mut stream,
		"CREATE TABLE users (user_id INT, name VARCHAR(255), score DOUBLE);",
	)
	.await;

	// 1. Prepare INSERT: INSERT INTO users (user_id, name, score) VALUES (?, ?, ?)
	let mut prep_insert = vec![COM_STMT_PREPARE];
	prep_insert.extend_from_slice(b"INSERT INTO users (user_id, name, score) VALUES (?, ?, ?)");
	write_packet(&mut stream, 0, &prep_insert).await;

	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	assert_eq!(ok_prep.num_params, 3);
	assert_eq!(ok_prep.num_columns, 0);

	// Read 3 parameter definitions
	for _ in 0..3 {
		let (_, p_pkt) = read_packet(&mut stream).await;
		let p_def = ColumnDefinition41::deserialize(&p_pkt).expect("Valid param def");
		assert_eq!(p_def.name, "?");
	}

	// 2. Execute INSERT: (1, "Alice", 99.5)
	let mut exec_insert1 = vec![COM_STMT_EXECUTE];
	exec_insert1.extend_from_slice(&1u32.to_le_bytes()); // stmt_id = 1
	exec_insert1.push(CURSOR_TYPE_NO_CURSOR);
	exec_insert1.extend_from_slice(&1u32.to_le_bytes()); // iteration count = 1
	// Null bitmap for 3 params: 1 byte: 0x00 (none null)
	exec_insert1.push(0x00);
	// new_params_bound = 1
	exec_insert1.push(0x01);
	// Types: LONG (0x03, 0x00), VAR_STRING (0xfe, 0x00), DOUBLE (0x05, 0x00)
	exec_insert1.extend_from_slice(&[MYSQL_TYPE_LONG, 0x00]);
	exec_insert1.extend_from_slice(&[MYSQL_TYPE_VAR_STRING, 0x00]);
	exec_insert1.extend_from_slice(&[MYSQL_TYPE_DOUBLE, 0x00]);
	// Value 1: user_id = 1 (i32 LE)
	exec_insert1.extend_from_slice(&1i32.to_le_bytes());
	// Value 2: name = "Alice" (lenenc str)
	exec_insert1.push(5);
	exec_insert1.extend_from_slice(b"Alice");
	// Value 3: score = 99.5 (f64 LE)
	exec_insert1.extend_from_slice(&99.5f64.to_le_bytes());

	write_packet(&mut stream, 0, &exec_insert1).await;
	let (_, ins1_resp) = read_packet(&mut stream).await;
	assert_eq!(ins1_resp[0], OK_HEADER, "INSERT 1 should succeed");
	assert_eq!(ins1_resp[1], 1, "Affected rows should be 1");

	// 3. Execute INSERT: (2, "Bob", NULL)
	let mut exec_insert2 = vec![COM_STMT_EXECUTE];
	exec_insert2.extend_from_slice(&1u32.to_le_bytes()); // stmt_id = 1
	exec_insert2.push(CURSOR_TYPE_NO_CURSOR);
	exec_insert2.extend_from_slice(&1u32.to_le_bytes()); // iteration count = 1
	// Null bitmap: param 2 (index 2) is NULL -> bit 2 is set (1 << 2 = 0x04)
	exec_insert2.push(0x04);
	// new_params_bound = 1
	exec_insert2.push(0x01);
	// Types
	exec_insert2.extend_from_slice(&[MYSQL_TYPE_LONG, 0x00]);
	exec_insert2.extend_from_slice(&[MYSQL_TYPE_VAR_STRING, 0x00]);
	exec_insert2.extend_from_slice(&[MYSQL_TYPE_DOUBLE, 0x00]);
	// Value 1: user_id = 2
	exec_insert2.extend_from_slice(&2i32.to_le_bytes());
	// Value 2: name = "Bob"
	exec_insert2.push(3);
	exec_insert2.extend_from_slice(b"Bob");
	// Value 3 is NULL, no value payload written

	write_packet(&mut stream, 0, &exec_insert2).await;
	let (_, ins2_resp) = read_packet(&mut stream).await;
	assert_eq!(ins2_resp[0], OK_HEADER, "INSERT 2 should succeed");
	assert_eq!(ins2_resp[1], 1, "Affected rows should be 1");

	// 4. Prepare SELECT: SELECT user_id, name, score FROM users WHERE user_id = ?
	let mut prep_select = vec![COM_STMT_PREPARE];
	prep_select.extend_from_slice(b"SELECT user_id, name, score FROM users WHERE user_id = ?");
	write_packet(&mut stream, 0, &prep_select).await;

	let (_, sel_prep_resp) = read_packet(&mut stream).await;
	let ok_sel_prep = StmtPrepareOk::deserialize(&sel_prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_sel_prep.statement_id, 2);
	assert_eq!(ok_sel_prep.num_params, 1);
	assert_eq!(ok_sel_prep.num_columns, 3);

	// Read 1 param def
	let (_, p_pkt) = read_packet(&mut stream).await;
	let p_def = ColumnDefinition41::deserialize(&p_pkt).expect("Valid param def");
	assert_eq!(p_def.name, "?");

	// Read 3 col defs
	let (_, col1_pkt) = read_packet(&mut stream).await;
	assert_eq!(ColumnDefinition41::deserialize(&col1_pkt).unwrap().name, "user_id");
	let (_, col2_pkt) = read_packet(&mut stream).await;
	assert_eq!(ColumnDefinition41::deserialize(&col2_pkt).unwrap().name, "name");
	let (_, col3_pkt) = read_packet(&mut stream).await;
	assert_eq!(ColumnDefinition41::deserialize(&col3_pkt).unwrap().name, "score");

	// 5. Execute SELECT WHERE user_id = 1
	let mut exec_sel1 = vec![COM_STMT_EXECUTE];
	exec_sel1.extend_from_slice(&2u32.to_le_bytes()); // stmt_id = 2
	exec_sel1.push(CURSOR_TYPE_NO_CURSOR);
	exec_sel1.extend_from_slice(&1u32.to_le_bytes());
	exec_sel1.push(0x00); // null bitmap (1 param = 1 byte)
	exec_sel1.push(0x01); // new params bound
	exec_sel1.extend_from_slice(&[MYSQL_TYPE_LONG, 0x00]);
	exec_sel1.extend_from_slice(&1i32.to_le_bytes()); // user_id = 1

	write_packet(&mut stream, 0, &exec_sel1).await;

	// Read col count packet
	let (_, col_count) = read_packet(&mut stream).await;
	assert_eq!(col_count[0], 3);

	// Read 3 col defs
	for _ in 0..3 {
		let (_, _) = read_packet(&mut stream).await;
	}

	// Read binary row packet
	let (_, row1) = read_packet(&mut stream).await;
	assert_eq!(row1[0], 0x00, "Binary row header must be 0x00");
	// Null bitmap for 3 columns: (3 + 9) / 8 = 1 byte.
	// Offset is 2. Bits 2, 3, 4 should all be 0 (none null).
	assert_eq!(row1[1], 0x00, "No column should be null in row 1");

	// Read terminating EOF_OK packet
	let (_, term) = read_packet(&mut stream).await;
	assert_eq!(term[0], 0xfe);

	// 6. Execute SELECT WHERE user_id = 2 (Bob with NULL score)
	let mut exec_sel2 = vec![COM_STMT_EXECUTE];
	exec_sel2.extend_from_slice(&2u32.to_le_bytes()); // stmt_id = 2
	exec_sel2.push(CURSOR_TYPE_NO_CURSOR);
	exec_sel2.extend_from_slice(&1u32.to_le_bytes());
	exec_sel2.push(0x00); // null bitmap
	exec_sel2.push(0x01); // new params bound
	exec_sel2.extend_from_slice(&[MYSQL_TYPE_LONG, 0x00]);
	exec_sel2.extend_from_slice(&2i32.to_le_bytes()); // user_id = 2

	write_packet(&mut stream, 0, &exec_sel2).await;

	// Read col count packet + 3 col defs
	let (_, col_count) = read_packet(&mut stream).await;
	assert_eq!(col_count[0], 3);
	for _ in 0..3 {
		let (_, _) = read_packet(&mut stream).await;
	}

	// Read binary row packet
	let (_, row2) = read_packet(&mut stream).await;
	assert_eq!(row2[0], 0x00);
	// Null bitmap: column index 2 is NULL -> bit (2 + 2) = bit 4 must be set!
	let null_bitmap = row2[1];
	assert_eq!(null_bitmap & (1 << 4), 1 << 4, "Column 2 (score) must be null");

	// Read terminating EOF_OK packet
	let (_, term) = read_packet(&mut stream).await;
	assert_eq!(term[0], 0xfe);
}

#[tokio::test]
async fn test_prepared_statement_send_long_data_and_reset() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	execute_query(&mut stream, "CREATE TABLE docs (id INT, body VARCHAR(255));").await;

	// Prepare INSERT: INSERT INTO docs (id, body) VALUES (?, ?)
	let mut prep = vec![COM_STMT_PREPARE];
	prep.extend_from_slice(b"INSERT INTO docs (id, body) VALUES (?, ?)");
	write_packet(&mut stream, 0, &prep).await;

	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	assert_eq!(ok_prep.num_params, 2);

	// Read 2 param defs
	let _ = read_packet(&mut stream).await;
	let _ = read_packet(&mut stream).await;

	// Send chunk 1 with COM_STMT_SEND_LONG_DATA for param 1
	let mut chunk1 = vec![COM_STMT_SEND_LONG_DATA];
	chunk1.extend_from_slice(&1u32.to_le_bytes()); // stmt_id = 1
	chunk1.extend_from_slice(&1u16.to_le_bytes()); // param_id = 1
	chunk1.extend_from_slice(b"Hello, ");
	write_packet(&mut stream, 0, &chunk1).await;
	// No response expected for COM_STMT_SEND_LONG_DATA

	// Send chunk 2
	let mut chunk2 = vec![COM_STMT_SEND_LONG_DATA];
	chunk2.extend_from_slice(&1u32.to_le_bytes());
	chunk2.extend_from_slice(&1u16.to_le_bytes());
	chunk2.extend_from_slice(b"SurrealDB!");
	write_packet(&mut stream, 0, &chunk2).await;

	// Execute with param 0 = 100
	let mut exec_payload = vec![COM_STMT_EXECUTE];
	exec_payload.extend_from_slice(&1u32.to_le_bytes());
	exec_payload.push(CURSOR_TYPE_NO_CURSOR);
	exec_payload.extend_from_slice(&1u32.to_le_bytes());
	exec_payload.push(0x00); // null bitmap (2 params = 1 byte)
	exec_payload.push(0x01); // new params bound
	exec_payload.extend_from_slice(&[MYSQL_TYPE_LONG, 0x00]);
	exec_payload.extend_from_slice(&[MYSQL_TYPE_VAR_STRING, 0x00]);
	// Param 0 value: 100
	exec_payload.extend_from_slice(&100i32.to_le_bytes());
	// Param 1 is provided via long_data, no extra bytes needed

	write_packet(&mut stream, 0, &exec_payload).await;
	let (_, exec_resp) = read_packet(&mut stream).await;
	assert_eq!(exec_resp[0], OK_HEADER, "Execution with long data should succeed");

	// Test COM_STMT_RESET
	let mut reset_payload = vec![COM_STMT_RESET];
	reset_payload.extend_from_slice(&1u32.to_le_bytes());
	write_packet(&mut stream, 0, &reset_payload).await;
	let (_, reset_resp) = read_packet(&mut stream).await;
	assert_eq!(reset_resp[0], OK_HEADER, "COM_STMT_RESET should return OK packet");
}

#[tokio::test]
async fn test_prepared_statement_close_and_reset_connection() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// 1. Prepare statement
	let mut prep = vec![COM_STMT_PREPARE];
	prep.extend_from_slice(b"SELECT 42");
	write_packet(&mut stream, 0, &prep).await;

	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	let _ = read_packet(&mut stream).await; // column def

	// 2. Close statement with COM_STMT_CLOSE
	let mut close_payload = vec![COM_STMT_CLOSE];
	close_payload.extend_from_slice(&1u32.to_le_bytes());
	write_packet(&mut stream, 0, &close_payload).await;
	// No response expected for COM_STMT_CLOSE

	// 3. Attempt to execute closed statement -> error 1243
	let mut exec_payload = vec![COM_STMT_EXECUTE];
	exec_payload.extend_from_slice(&1u32.to_le_bytes());
	exec_payload.push(CURSOR_TYPE_NO_CURSOR);
	exec_payload.extend_from_slice(&1u32.to_le_bytes());
	write_packet(&mut stream, 0, &exec_payload).await;

	let (_, err_resp) = read_packet(&mut stream).await;
	assert_eq!(err_resp[0], ERR_HEADER, "Expected error packet for closed statement");
	let err_code = u16::from_le_bytes([err_resp[1], err_resp[2]]);
	assert_eq!(err_code, 1243, "Error code should be 1243 (HY000)");

	// 4. Prepare another statement
	let mut prep2 = vec![COM_STMT_PREPARE];
	prep2.extend_from_slice(b"SELECT 100");
	write_packet(&mut stream, 0, &prep2).await;
	let (_, prep2_resp) = read_packet(&mut stream).await;
	let ok_prep2 = StmtPrepareOk::deserialize(&prep2_resp).expect("Valid StmtPrepareOk");
	let _ = read_packet(&mut stream).await; // column def

	// 5. Send COM_RESET_CONNECTION
	let reset_conn = vec![COM_RESET_CONNECTION];
	write_packet(&mut stream, 0, &reset_conn).await;
	let (_, reset_resp) = read_packet(&mut stream).await;
	assert_eq!(reset_resp[0], OK_HEADER, "COM_RESET_CONNECTION should return OK");

	// 6. Attempt to execute statement prepared before reset -> error 1243
	let mut exec_payload2 = vec![COM_STMT_EXECUTE];
	exec_payload2.extend_from_slice(&ok_prep2.statement_id.to_le_bytes());
	exec_payload2.push(CURSOR_TYPE_NO_CURSOR);
	exec_payload2.extend_from_slice(&1u32.to_le_bytes());
	write_packet(&mut stream, 0, &exec_payload2).await;

	let (_, err_resp2) = read_packet(&mut stream).await;
	assert_eq!(err_resp2[0], ERR_HEADER, "Expected error after COM_RESET_CONNECTION");
	let err_code2 = u16::from_le_bytes([err_resp2[1], err_resp2[2]]);
	assert_eq!(err_code2, 1243);
}

#[tokio::test]
async fn test_prepared_statement_reexecute_reusing_cached_types() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// Prepare: SELECT ? + 10 AS res
	let mut prep = vec![COM_STMT_PREPARE];
	prep.extend_from_slice(b"SELECT ? + 10 AS res");
	write_packet(&mut stream, 0, &prep).await;

	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	assert_eq!(ok_prep.num_params, 1);
	assert_eq!(ok_prep.num_columns, 1);

	// Read param def & col def
	let _ = read_packet(&mut stream).await;
	let _ = read_packet(&mut stream).await;

	// Execute 1: new_params_bound = 1, type = MYSQL_TYPE_LONG, value = 5
	let mut exec1 = vec![COM_STMT_EXECUTE];
	exec1.extend_from_slice(&1u32.to_le_bytes());
	exec1.push(CURSOR_TYPE_NO_CURSOR);
	exec1.extend_from_slice(&1u32.to_le_bytes());
	exec1.push(0x00); // null bitmap
	exec1.push(0x01); // new_params_bound = 1
	exec1.extend_from_slice(&[MYSQL_TYPE_LONG, 0x00]);
	exec1.extend_from_slice(&5i32.to_le_bytes());

	write_packet(&mut stream, 0, &exec1).await;
	let (_, col_count1) = read_packet(&mut stream).await;
	assert_eq!(col_count1[0], 1);
	let _ = read_packet(&mut stream).await; // col def
	let (_, row1) = read_packet(&mut stream).await;
	assert_eq!(row1[0], 0x00); // binary row header
	let _ = read_packet(&mut stream).await; // EOF_OK

	// Execute 2: new_params_bound = 0 (no types encoded!), value = 20
	let mut exec2 = vec![COM_STMT_EXECUTE];
	exec2.extend_from_slice(&1u32.to_le_bytes());
	exec2.push(CURSOR_TYPE_NO_CURSOR);
	exec2.extend_from_slice(&1u32.to_le_bytes());
	exec2.push(0x00); // null bitmap
	exec2.push(0x00); // new_params_bound = 0!
	exec2.extend_from_slice(&20i32.to_le_bytes()); // value directly

	write_packet(&mut stream, 0, &exec2).await;
	let (_, col_count2) = read_packet(&mut stream).await;
	assert_eq!(col_count2[0], 1);
	let _ = read_packet(&mut stream).await; // col def
	let (_, row2) = read_packet(&mut stream).await;
	assert_eq!(row2[0], 0x00); // binary row header
	let _ = read_packet(&mut stream).await; // EOF_OK
}

#[tokio::test]
async fn test_prepared_statement_without_deprecate_eof() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	// Handshake WITHOUT CLIENT_DEPRECATE_EOF
	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, 0).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// Prepare: SELECT ? AS echoed
	let mut prep = vec![COM_STMT_PREPARE];
	prep.extend_from_slice(b"SELECT ? AS echoed");
	write_packet(&mut stream, 0, &prep).await;

	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(ok_prep.statement_id, 1);
	assert_eq!(ok_prep.num_params, 1);
	assert_eq!(ok_prep.num_columns, 1);

	// Read param def packet
	let (_, p_pkt) = read_packet(&mut stream).await;
	assert_eq!(ColumnDefinition41::deserialize(&p_pkt).unwrap().name, "?");

	// In non-deprecate-eof mode, param defs are terminated by an EOF packet
	let (_, p_eof) = read_packet(&mut stream).await;
	assert_eq!(p_eof[0], EOF_HEADER, "Param defs must be terminated by EOF packet");

	// Read col def packet
	let (_, c_pkt) = read_packet(&mut stream).await;
	assert_eq!(ColumnDefinition41::deserialize(&c_pkt).unwrap().name, "echoed");

	// In non-deprecate-eof mode, col defs in prepare response are terminated by an EOF packet
	let (_, c_eof) = read_packet(&mut stream).await;
	assert_eq!(c_eof[0], EOF_HEADER, "Col defs must be terminated by EOF packet");

	// Execute statement 1
	let mut exec = vec![COM_STMT_EXECUTE];
	exec.extend_from_slice(&1u32.to_le_bytes());
	exec.push(CURSOR_TYPE_NO_CURSOR);
	exec.extend_from_slice(&1u32.to_le_bytes());
	exec.push(0x00);
	exec.push(0x01);
	exec.extend_from_slice(&[MYSQL_TYPE_VAR_STRING, 0x00]);
	exec.push(4);
	exec.extend_from_slice(b"wire");

	write_packet(&mut stream, 0, &exec).await;

	// Column count packet
	let (_, cc_pkt) = read_packet(&mut stream).await;
	assert_eq!(cc_pkt[0], 1);

	// Column def packet
	let (_, cd_pkt) = read_packet(&mut stream).await;
	assert_eq!(ColumnDefinition41::deserialize(&cd_pkt).unwrap().name, "echoed");

	// Intermediate EOF packet
	let (_, inter_eof) = read_packet(&mut stream).await;
	assert_eq!(inter_eof[0], EOF_HEADER, "Intermediate EOF packet expected");

	// Binary row packet
	let (_, row_pkt) = read_packet(&mut stream).await;
	assert_eq!(row_pkt[0], 0x00, "Binary row header expected");

	// Final EOF packet
	let (_, final_eof) = read_packet(&mut stream).await;
	assert_eq!(final_eof[0], EOF_HEADER, "Final EOF packet expected in non-deprecate-eof mode");
}

#[tokio::test]
async fn test_prepared_statement_unbound_param_rejected_in_simple_query() {
	let ds = setup_test_datastore().await;
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER);

	// COM_QUERY with unbound '?'
	let mut query_payload = vec![COM_QUERY];
	query_payload.extend_from_slice(b"SELECT ?;");
	write_packet(&mut stream, 0, &query_payload).await;

	let (_, err_resp) = read_packet(&mut stream).await;
	assert_eq!(err_resp[0], ERR_HEADER, "Unbound '?' in simple query must fail-closed");
	let err_code = u16::from_le_bytes([err_resp[1], err_resp[2]]);
	assert_eq!(err_code, 1064);
	assert_eq!(&err_resp[4..9], b"42000");
}

#[tokio::test]
async fn test_prepared_statement_binary_rows_carry_datetimes_durations_and_json() {
	let ds = setup_test_datastore().await;
	let seed_ds = Arc::clone(&ds);
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();

	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed");

	seed_surrealql(
		&seed_ds,
		"DEFINE TABLE person SCHEMAFULL; \
		 DEFINE FIELD tags ON person TYPE array<string>; \
		 DEFINE FIELD born ON person TYPE datetime; \
		 DEFINE FIELD seen ON person TYPE datetime; \
		 DEFINE FIELD took ON person TYPE duration; \
		 CREATE person:tobie CONTENT { tags: ['rust', 'db'], born: d'1980-01-01T00:00:00Z', \
		 seen: d'1980-01-01T12:30:45.123456Z', took: 26h } RETURN NONE",
	)
	.await;

	let mut prep = vec![COM_STMT_PREPARE];
	prep.extend_from_slice(b"SELECT born, seen, tags, took FROM person");
	write_packet(&mut stream, 0, &prep).await;
	let (_, prep_resp) = read_packet(&mut stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	assert_eq!(
		ok_prep.num_columns, 4,
		"the named columns are known when the statement is prepared"
	);
	for _ in 0..ok_prep.num_columns {
		read_packet(&mut stream).await;
	}

	let mut exec = vec![COM_STMT_EXECUTE];
	exec.extend_from_slice(&ok_prep.statement_id.to_le_bytes());
	exec.push(CURSOR_TYPE_NO_CURSOR);
	exec.extend_from_slice(&1u32.to_le_bytes());
	write_packet(&mut stream, 0, &exec).await;

	let (_, col_count) = read_packet(&mut stream).await;
	assert_eq!(col_count[0], 4);
	let mut types = Vec::new();
	for _ in 0..4 {
		let (_, def) = read_packet(&mut stream).await;
		let def = ColumnDefinition41::deserialize(&def).expect("Valid column def");
		types.push((def.name, def.column_type));
	}
	assert_eq!(
		types,
		vec![
			("born".to_string(), MYSQL_TYPE_TIMESTAMP),
			("seen".to_string(), MYSQL_TYPE_TIMESTAMP),
			("tags".to_string(), MYSQL_TYPE_JSON),
			("took".to_string(), MYSQL_TYPE_TIME),
		]
	);

	let (_, row) = read_packet(&mut stream).await;
	let mut expected: Vec<u8> = vec![0x00, 0x00];
	// born: DATETIME 1980-01-01 00:00:00 is a date-only value (length 4).
	expected.extend_from_slice(&[4, 0xbc, 0x07, 1, 1]);
	// seen: DATETIME 1980-01-01 12:30:45.123456 (length 11).
	expected.extend_from_slice(&[11, 0xbc, 0x07, 1, 1, 12, 30, 45, 0x40, 0xe2, 0x01, 0x00]);
	// tags: JSON as a length-encoded string.
	expected.push(13);
	expected.extend_from_slice(b"[\"rust\",\"db\"]");
	// took: a duration is a binary TIME of 1 day and 2 hours (length 8, positive).
	expected.extend_from_slice(&[8, 0, 1, 0, 0, 0, 2, 0, 0]);
	assert_eq!(row, expected);

	let (_, term) = read_packet(&mut stream).await;
	assert_eq!(term[0], 0xfe, "Expected terminating EOF_OK packet");
	shutdown.cancel();
}

async fn execute_prepared(
	stream: &mut TcpStream,
	sql: &str,
) -> (u64, Vec<(String, u8)>, Vec<Vec<u8>>) {
	let mut prep = vec![COM_STMT_PREPARE];
	prep.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &prep).await;
	let (_, prep_resp) = read_packet(stream).await;
	let ok_prep = StmtPrepareOk::deserialize(&prep_resp).expect("Valid StmtPrepareOk");
	for _ in 0..ok_prep.num_columns {
		read_packet(stream).await;
	}
	let mut exec = vec![COM_STMT_EXECUTE];
	exec.extend_from_slice(&ok_prep.statement_id.to_le_bytes());
	exec.push(CURSOR_TYPE_NO_CURSOR);
	exec.extend_from_slice(&1u32.to_le_bytes());
	write_packet(stream, 0, &exec).await;
	let (_, count) = read_packet(stream).await;
	assert!(count[0] != 0xff && count[0] != 0x00, "a result set was expected: {count:?}");
	let mut columns = Vec::new();
	for _ in 0..count[0] {
		let (_, def) = read_packet(stream).await;
		let def = ColumnDefinition41::deserialize(&def).expect("Valid column def");
		columns.push((def.name, def.column_type));
	}
	let mut rows = Vec::new();
	loop {
		let (_, pkt) = read_packet(stream).await;
		if pkt[0] == 0xfe && pkt.len() < 9 {
			break;
		}
		rows.push(pkt);
	}
	(u64::from(ok_prep.num_columns), columns, rows)
}

#[tokio::test]
async fn test_prepared_statement_mysql_dialect_shows_record_ids_as_keys_and_lists_wildcard_columns()
{
	let ds = setup_test_datastore().await;
	let seed_ds = Arc::clone(&ds);
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();
	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();
	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed");

	seed_surrealql(
		&seed_ds,
		"DEFINE TABLE emp SCHEMALESS; CREATE emp:7 SET name = 'Eve', dept = dept:1 RETURN NONE",
	)
	.await;

	// Named columns are typed before the statement runs, and the id and the link are keys.
	let (prepared_columns, columns, rows) =
		execute_prepared(&mut stream, "SELECT id, dept FROM emp WHERE name = 'Eve'").await;
	assert_eq!(prepared_columns, 2);
	assert_eq!(columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["id", "dept"]);
	assert_eq!(rows, vec![vec![0x00, 0x00, 1, b'7', 1, b'1']]);

	// A * the planner cannot list is described when the statement runs, from its rows.
	let (prepared_columns, columns, rows) =
		execute_prepared(&mut stream, "SELECT * FROM emp WHERE name = 'Eve'").await;
	assert_eq!(prepared_columns, 0);
	assert_eq!(columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["dept", "id", "name"]);
	let mut expected: Vec<u8> = vec![0x00, 0x00];
	expected.extend_from_slice(&1i64.to_le_bytes());
	expected.extend_from_slice(&7i64.to_le_bytes());
	expected.extend_from_slice(&[3, b'E', b'v', b'e']);
	assert_eq!(rows, vec![expected]);

	// With no rows the statement still answers with a result set.
	let (_, columns, rows) =
		execute_prepared(&mut stream, "SELECT * FROM emp WHERE name = 'nobody'").await;
	assert!(!columns.is_empty());
	assert!(rows.is_empty());
	shutdown.cancel();
}

#[tokio::test]
async fn test_prepared_statement_mysql_dialect_writes_a_key_into_a_link_field_and_finds_it_again() {
	let ds = setup_test_datastore().await;
	let seed_ds = Arc::clone(&ds);
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();
	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().unwrap(),
		ds,
		ready,
		shutdown.clone(),
		None,
		None,
	)
	.await
	.unwrap();
	let mut stream = TcpStream::connect(addr).await.unwrap();
	let (_, auth_ok) = perform_handshake(&mut stream, CLIENT_DEPRECATE_EOF).await;
	assert_eq!(auth_ok[0], OK_HEADER, "Handshake should succeed");

	seed_surrealql(
		&seed_ds,
		"DEFINE TABLE dept SCHEMALESS; \
		 CREATE dept:1 SET dname = 'Engineering' RETURN NONE; \
		 DEFINE TABLE emp SCHEMAFULL; \
		 DEFINE FIELD name ON emp TYPE string; \
		 DEFINE FIELD dept ON emp TYPE record<dept>",
	)
	.await;

	// A plain key is stored as the link to the declared table, by a simple query and by a
	// prepared statement alike.
	execute_query(&mut stream, "INSERT INTO emp (id, name, dept) VALUES (8, 'Sam', 1)").await;
	let (_, _, rows) = execute_prepared(&mut stream, "SELECT name FROM emp WHERE dept = 1").await;
	assert_eq!(rows, vec![vec![0x00, 0x00, 3, b'S', b'a', b'm']]);
	let (_, _, rows) = execute_prepared(&mut stream, "SELECT name FROM emp WHERE dept = 2").await;
	assert!(rows.is_empty());

	// What is not a key reaches the field's own type check.
	let mut bad = vec![COM_QUERY];
	bad.extend_from_slice(b"INSERT INTO emp (id, name, dept) VALUES (9, 'Bad', 1.5)");
	write_packet(&mut stream, 0, &bad).await;
	let (_, resp) = read_packet(&mut stream).await;
	assert_eq!(resp[0], 0xff, "a float is no key of a link");

	// The stored value is a link: SurrealQL follows it.
	let sess = Session::owner().with_ns("test").with_db("test");
	let mut res = seed_ds.execute("SELECT VALUE dept.dname FROM emp:8", &sess, None).await.unwrap();
	assert_eq!(res.remove(0).result.unwrap().to_sql(), "['Engineering']");
	shutdown.cancel();
}
