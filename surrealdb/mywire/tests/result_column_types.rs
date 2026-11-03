//! The type a result column is announced as when the values of an expression may differ in type
//! (`CASE`, `COALESCE`, `GREATEST`, `floor` and the like), and the form its values take: the type
//! must be one every value of the column has, over the text protocol and the binary one.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::{
	AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD, compute_mysql_native_password_scramble,
};
use surrealdb_mywire::constants::*;
use surrealdb_mywire::msg::{ColumnDefinition41, StmtPrepareOk};
use surrealdb_types::NumberKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Bound on every read from the server, so a server that stops answering fails the test instead
/// of hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

async fn start() -> (std::net::SocketAddr, CancellationToken) {
	start_on(Datastore::new("memory").await.expect("create the datastore")).await
}

async fn start_on(ds: Arc<Datastore>) -> (std::net::SocketAddr, CancellationToken) {
	ds.run(surrealdb_core::kvs::QueryRequest::new(
		"DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;",
		&Session::owner(),
	))
	.await
	.expect("define the root user");
	let shutdown = CancellationToken::new();
	let addr = surrealdb_mywire::start(
		"127.0.0.1:0".parse().expect("loopback address"),
		ds,
		Arc::new(AtomicBool::new(true)),
		shutdown.clone(),
		None,
		None,
	)
	.await
	.expect("start the mysql listener");
	(addr, shutdown)
}

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	timeout(IO_TIMEOUT, async {
		let mut header = [0u8; 4];
		stream.read_exact(&mut header).await.expect("a packet header");
		let length =
			usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
		let mut payload = vec![0u8; length];
		stream.read_exact(&mut payload).await.expect("a packet body");
		payload
	})
	.await
	.expect("the server sent a packet")
}

async fn write_packet(stream: &mut TcpStream, sequence: u8, payload: &[u8]) {
	let length = payload.len();
	let header = [
		(length & 0xff) as u8,
		((length >> 8) & 0xff) as u8,
		((length >> 16) & 0xff) as u8,
		sequence,
	];
	stream.write_all(&header).await.expect("write a packet header");
	stream.write_all(payload).await.expect("write a packet body");
}

/// The salt of a HandshakeV10 packet.
fn salt(handshake: &[u8]) -> [u8; 20] {
	let mut pos = 1;
	while handshake[pos] != 0 {
		pos += 1;
	}
	pos += 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&handshake[pos..pos + 8]);
	pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..].copy_from_slice(&handshake[pos..pos + 12]);
	salt
}

/// Connects as `root` to `test/test` with the end of a result set marked by an OK packet.
async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let handshake = read_packet(&mut stream).await;
	assert_eq!(handshake[0], 10, "a HandshakeV10 packet");
	let scramble = compute_mysql_native_password_scramble("root", &salt(&handshake));
	let capabilities = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| CLIENT_DEPRECATE_EOF;
	let mut response = Vec::new();
	response.extend_from_slice(&capabilities.to_le_bytes());
	response.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	response.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	response.extend_from_slice(&[0u8; 23]);
	response.extend_from_slice(b"root\0");
	response.push(20);
	response.extend_from_slice(&scramble);
	response.extend_from_slice(b"test/test\0");
	response.extend_from_slice(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD.as_bytes());
	response.push(0);
	write_packet(&mut stream, 1, &response).await;
	let mut answer = read_packet(&mut stream).await;
	if answer[0] == 0xfe {
		write_packet(&mut stream, 3, b"root\0").await;
		answer = read_packet(&mut stream).await;
	}
	assert_eq!(answer[0], 0x00, "the handshake: {}", String::from_utf8_lossy(&answer));
	stream
}

/// What the server answered to a statement: the type and flags of each column, each cell shown as
/// text, and the message of an error.
#[derive(Debug, Default)]
struct Answer {
	types: Vec<u8>,
	flags: Vec<u16>,
	rows: Vec<Vec<String>>,
	error: Option<String>,
}

/// The bytes of a length-encoded string at the start of `cursor`, and the rest.
fn lenenc_str(cursor: &[u8]) -> (&[u8], &[u8]) {
	let (length, rest) = match cursor[0] {
		0xfc => (usize::from(u16::from_le_bytes([cursor[1], cursor[2]])), &cursor[3..]),
		length => (usize::from(length), &cursor[1..]),
	};
	rest.split_at(length)
}

async fn read_result(stream: &mut TcpStream, binary: bool) -> Answer {
	let first = read_packet(stream).await;
	if first[0] == 0xff {
		return Answer {
			error: Some(String::from_utf8_lossy(&first[9..]).into_owned()),
			..Answer::default()
		};
	}
	if first[0] == 0x00 {
		return Answer::default();
	}
	let mut answer = Answer::default();
	for _ in 0..first[0] {
		let definition = read_packet(stream).await;
		let column = ColumnDefinition41::deserialize(&definition).expect("a column");
		answer.types.push(column.column_type);
		answer.flags.push(column.flags);
	}
	loop {
		let packet = read_packet(stream).await;
		if packet[0] == 0xfe && packet.len() < 9 {
			break;
		}
		let mut row = Vec::new();
		if binary {
			let bitmap = (answer.types.len() + 9) / 8;
			let mut cursor = &packet[1 + bitmap..];
			for (i, ty) in answer.types.iter().enumerate() {
				if packet[1 + (i + 2) / 8] & (1 << ((i + 2) % 8)) != 0 {
					row.push("NULL".to_string());
					continue;
				}
				let unsigned = answer.flags[i] & UNSIGNED_FLAG != 0;
				let (cell, rest) = match *ty {
					MYSQL_TYPE_TINY => {
						let (n, rest) = cursor.split_at(1);
						let cell = if unsigned {
							n[0].to_string()
						} else {
							i8::from_le_bytes([n[0]]).to_string()
						};
						(cell, rest)
					}
					MYSQL_TYPE_SHORT => {
						let (n, rest) = cursor.split_at(2);
						let n: [u8; 2] = n.try_into().expect("2 bytes");
						let cell = if unsigned {
							u16::from_le_bytes(n).to_string()
						} else {
							i16::from_le_bytes(n).to_string()
						};
						(cell, rest)
					}
					MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => {
						let (n, rest) = cursor.split_at(4);
						let n: [u8; 4] = n.try_into().expect("4 bytes");
						let cell = if unsigned {
							u32::from_le_bytes(n).to_string()
						} else {
							i32::from_le_bytes(n).to_string()
						};
						(cell, rest)
					}
					MYSQL_TYPE_LONGLONG => {
						let (n, rest) = cursor.split_at(8);
						let n: [u8; 8] = n.try_into().expect("8 bytes");
						let cell = if unsigned {
							u64::from_le_bytes(n).to_string()
						} else {
							i64::from_le_bytes(n).to_string()
						};
						(cell, rest)
					}
					MYSQL_TYPE_FLOAT => {
						let (n, rest) = cursor.split_at(4);
						(f32::from_le_bytes(n.try_into().expect("4 bytes")).to_string(), rest)
					}
					MYSQL_TYPE_DOUBLE => {
						let (n, rest) = cursor.split_at(8);
						(f64::from_le_bytes(n.try_into().expect("8 bytes")).to_string(), rest)
					}
					_ => {
						let (text, rest) = lenenc_str(cursor);
						(String::from_utf8_lossy(text).into_owned(), rest)
					}
				};
				row.push(cell);
				cursor = rest;
			}
		} else {
			let mut cursor = &packet[..];
			for _ in 0..answer.types.len() {
				if cursor[0] == 0xfb {
					row.push("NULL".to_string());
					cursor = &cursor[1..];
				} else {
					let (text, rest) = lenenc_str(cursor);
					row.push(String::from_utf8_lossy(text).into_owned());
					cursor = rest;
				}
			}
		}
		answer.rows.push(row);
	}
	answer
}

/// A statement over the text protocol.
async fn text(stream: &mut TcpStream, sql: &str) -> Answer {
	let mut payload = vec![COM_QUERY];
	payload.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &payload).await;
	read_result(stream, false).await
}

/// A statement prepared and executed over the binary protocol.
async fn binary(stream: &mut TcpStream, sql: &str) -> Answer {
	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &prepare).await;
	let prepared = read_packet(stream).await;
	if prepared[0] == 0xff {
		return Answer {
			error: Some(String::from_utf8_lossy(&prepared[9..]).into_owned()),
			..Answer::default()
		};
	}
	let prepared = StmtPrepareOk::deserialize(&prepared).expect("a prepared statement");
	for _ in 0..prepared.num_columns {
		read_packet(stream).await;
	}
	let mut execute = vec![COM_STMT_EXECUTE];
	execute.extend_from_slice(&prepared.statement_id.to_le_bytes());
	execute.push(CURSOR_TYPE_NO_CURSOR);
	execute.extend_from_slice(&1u32.to_le_bytes());
	write_packet(stream, 0, &execute).await;
	read_result(stream, true).await
}

/// Asserts that `sql` is answered with one column whose rows show as `rows`, over the text
/// protocol and over the binary one, and that each value reads as the type the column is announced
/// as. The type is `column_type` when it is given.
async fn assert_answer(stream: &mut TcpStream, sql: &str, column_type: Option<u8>, rows: &[&str]) {
	for (protocol, answer) in
		[("text", text(stream, sql).await), ("binary", binary(stream, sql).await)]
	{
		assert_eq!(answer.error, None, "{sql} ({protocol})");
		assert_eq!(answer.types.len(), 1, "{sql} ({protocol})");
		if let Some(column_type) = column_type {
			assert_eq!(answer.types, [column_type], "{sql} ({protocol})");
		}
		let cells: Vec<&str> = answer.rows.iter().map(|row| row[0].as_str()).collect();
		assert_eq!(cells, rows, "{sql} ({protocol})");
		for cell in cells.iter().filter(|cell| **cell != "NULL") {
			let reads = match answer.types[0] {
				MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_INT24
				| MYSQL_TYPE_LONGLONG => cell.parse::<i128>().is_ok(),
				MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE | MYSQL_TYPE_NEWDECIMAL => {
					cell.parse::<f64>().is_ok()
				}
				_ => true,
			};
			assert!(reads, "{sql} ({protocol}): {cell:?} is not a value of its column's type");
		}
	}
}

#[tokio::test]
async fn a_number_function_is_announced_as_the_type_its_results_have() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, column_type, rows) in [
		("SELECT floor(1.5)", MYSQL_TYPE_NEWDECIMAL, ["1"]),
		("SELECT ceil(1.2)", MYSQL_TYPE_NEWDECIMAL, ["2"]),
		("SELECT ceiling(-1.2)", MYSQL_TYPE_NEWDECIMAL, ["-1"]),
		("SELECT floor(1)", MYSQL_TYPE_LONGLONG, ["1"]),
		("SELECT ceil(7)", MYSQL_TYPE_LONGLONG, ["7"]),
		("SELECT greatest(1, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT greatest(3, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["3"]),
		("SELECT least(1, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["1"]),
		("SELECT least(4, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT greatest(1, 2, 3)", MYSQL_TYPE_LONGLONG, ["3"]),
		("SELECT coalesce(1, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["1"]),
		("SELECT coalesce(NULL, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT coalesce(NULL, NULL, 7)", MYSQL_TYPE_LONGLONG, ["7"]),
		("SELECT coalesce(NULL, 'a', 1)", MYSQL_TYPE_VAR_STRING, ["a"]),
		("SELECT ifnull(1, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["1"]),
		("SELECT ifnull(NULL, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT ifnull(NULL, 'a')", MYSQL_TYPE_VAR_STRING, ["a"]),
	] {
		assert_answer(&mut stream, sql, Some(column_type), &rows).await;
	}
	shutdown.cancel();
}

#[tokio::test]
async fn an_if_or_a_case_is_announced_as_the_type_all_its_branches_have() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, column_type, rows) in [
		("SELECT IF(false, 1, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT IF(true, 1, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["1"]),
		("SELECT IF(false, 1.5, 2)", MYSQL_TYPE_NEWDECIMAL, ["2"]),
		("SELECT IF(true, 1, 2)", MYSQL_TYPE_LONGLONG, ["1"]),
		("SELECT IF(false, 1, 'a')", MYSQL_TYPE_VAR_STRING, ["a"]),
		("SELECT IF(true, 1, 'a')", MYSQL_TYPE_VAR_STRING, ["1"]),
		("SELECT IF(true, 1, NULL)", MYSQL_TYPE_LONGLONG, ["1"]),
		("SELECT IF(false, 1, NULL)", MYSQL_TYPE_LONGLONG, ["NULL"]),
		("SELECT IF(false, NULL, 2.5)", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT CASE WHEN false THEN 1 ELSE 2.5 END", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT CASE WHEN true THEN 1 ELSE 2.5 END", MYSQL_TYPE_NEWDECIMAL, ["1"]),
		("SELECT CASE WHEN false THEN 1.5 ELSE 2 END", MYSQL_TYPE_NEWDECIMAL, ["2"]),
		("SELECT CASE WHEN true THEN 1 ELSE 2 END", MYSQL_TYPE_LONGLONG, ["1"]),
		("SELECT CASE 2 WHEN 1 THEN 1 WHEN 2 THEN 2.5 END", MYSQL_TYPE_NEWDECIMAL, ["2.5"]),
		("SELECT CASE WHEN true THEN 1 ELSE 'a' END", MYSQL_TYPE_VAR_STRING, ["1"]),
		("SELECT CASE WHEN false THEN 1 ELSE 'a' END", MYSQL_TYPE_VAR_STRING, ["a"]),
		("SELECT CASE WHEN false THEN 1 ELSE NULL END", MYSQL_TYPE_LONGLONG, ["NULL"]),
		("SELECT CASE WHEN false THEN NULL ELSE 1.5 END", MYSQL_TYPE_NEWDECIMAL, ["1.5"]),
	] {
		assert_answer(&mut stream, sql, Some(column_type), &rows).await;
	}
	shutdown.cancel();
}

/// A table with an integer, a decimal, a float and a text column, so what a CASE or a function
/// returns for one row differs in kind from what it returns for the next.
async fn with_table(stream: &mut TcpStream) {
	for sql in [
		"CREATE TABLE rt (id BIGINT PRIMARY KEY, i INT, d DECIMAL(10,2), f DOUBLE, t VARCHAR(20))",
		"INSERT INTO rt (id, i, d, f, t) VALUES (1, 2, 3.25, 4.5, 'x'), (2, 3, 4.25, 5.5, 'y')",
	] {
		let answer = text(stream, sql).await;
		assert_eq!(answer.error, None, "{sql}");
	}
}

/// The type a MySQL client is told for the column of a table is not pinned here: the values are
/// checked, and that each reads as the type its column is announced as.
#[tokio::test]
async fn the_values_of_a_result_over_a_table_read_as_the_type_of_their_column() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	with_table(&mut stream).await;
	for (select, rows) in [
		("CASE WHEN id = 1 THEN d ELSE f END", ["3.25", "5.5"]),
		("CASE WHEN id = 1 THEN f ELSE d END", ["4.5", "4.25"]),
		("CASE WHEN id = 1 THEN i ELSE d END", ["2", "4.25"]),
		("CASE WHEN id = 1 THEN i ELSE f END", ["2", "5.5"]),
		("CASE WHEN id = 1 THEN i ELSE t END", ["2", "y"]),
		("IF(id = 1, d, f)", ["3.25", "5.5"]),
		("IF(id = 1, i, t)", ["2", "y"]),
		("greatest(i, d)", ["3.25", "4.25"]),
		("least(d, f)", ["3.25", "4.25"]),
		("coalesce(d, f)", ["3.25", "4.25"]),
		("ifnull(i, f)", ["2", "3"]),
		("floor(d)", ["3", "4"]),
		("ceil(d)", ["4", "5"]),
		("floor(f)", ["4", "5"]),
		("floor(i)", ["2", "3"]),
	] {
		let sql = format!("SELECT {select} FROM rt ORDER BY id");
		assert_answer(&mut stream, &sql, None, &rows).await;
	}
	shutdown.cancel();
}

/// A field of one number kind: the SurrealQL type that declares it, a literal of the kind, what the
/// field, its floor and its ceiling show as, the number the field holds, and the family of types
/// a listed value of the kind is announced under.
struct SizedField {
	surql_type: &'static str,
	literal: &'static str,
	shown: &'static str,
	floor: &'static str,
	ceil: &'static str,
	value: f64,
	listed_type: u8,
}

/// The field of a number kind. A kind without an arm is a compile error, so a number kind added to
/// the engine has to be given here the family of types it is announced under when a row is listed
/// with `*`: an integer under an integer type that holds its range (`BIGINT` when nothing
/// describes the table, the declared width when the table's columns are known), a float of any
/// width under `FLOAT` or `DOUBLE`, a decimal and a `u64` above `i64::MAX` under
/// `DECIMAL`.
fn sized_field(kind: NumberKind) -> SizedField {
	let (surql_type, literal, shown, floor, ceil, value, listed_type) = match kind {
		NumberKind::I8 => ("i8", "-5i8", "-5", "-5", "-5", -5.0, MYSQL_TYPE_LONGLONG),
		NumberKind::U8 => ("u8", "200u8", "200", "200", "200", 200.0, MYSQL_TYPE_LONGLONG),
		NumberKind::I16 => ("i16", "-300i16", "-300", "-300", "-300", -300.0, MYSQL_TYPE_LONGLONG),
		NumberKind::U16 => {
			("u16", "60000u16", "60000", "60000", "60000", 60000.0, MYSQL_TYPE_LONGLONG)
		}
		NumberKind::I32 => {
			("i32", "-70000i32", "-70000", "-70000", "-70000", -70000.0, MYSQL_TYPE_LONGLONG)
		}
		NumberKind::U32 => (
			"u32",
			"4000000000u32",
			"4000000000",
			"4000000000",
			"4000000000",
			4e9,
			MYSQL_TYPE_LONGLONG,
		),
		NumberKind::Int => (
			"int",
			"-5000000000",
			"-5000000000",
			"-5000000000",
			"-5000000000",
			-5e9,
			MYSQL_TYPE_LONGLONG,
		),
		NumberKind::U64 => (
			"u64",
			"18446744073709551615u64",
			"18446744073709551615",
			"18446744073709551615",
			"18446744073709551615",
			1.8446744073709552e19,
			MYSQL_TYPE_LONGLONG,
		),
		NumberKind::F16 => ("f16", "1.5f16", "1.5", "1", "2", 1.5, MYSQL_TYPE_DOUBLE),
		NumberKind::F32 => ("f32", "0.1f32", "0.1", "0", "1", 0.1, MYSQL_TYPE_DOUBLE),
		NumberKind::Float => ("float", "0.25f", "0.25", "0", "1", 0.25, MYSQL_TYPE_DOUBLE),
		NumberKind::Decimal => {
			("decimal", "3.25dec", "3.25", "3", "4", 3.25, MYSQL_TYPE_NEWDECIMAL)
		}
		NumberKind::BigDecimal => {
			unimplemented!("the SurrealQL front end has no bigdecimal kind name or literal")
		}
	};
	SizedField {
		surql_type,
		literal,
		shown,
		floor,
		ceil,
		value,
		listed_type,
	}
}

/// Every number kind: each is given a field in `sized_field`.
const KINDS: [NumberKind; 12] = [
	NumberKind::I8,
	NumberKind::U8,
	NumberKind::I16,
	NumberKind::U16,
	NumberKind::I32,
	NumberKind::U32,
	NumberKind::Int,
	NumberKind::U64,
	NumberKind::F16,
	NumberKind::F32,
	NumberKind::Float,
	NumberKind::Decimal,
];

fn column(kind: NumberKind) -> String {
	format!("v_{}", sized_field(kind).surql_type)
}

/// A server whose table `rs` has one field of every number kind, written by SurrealQL, and one row.
async fn start_with_sized_fields() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("create the datastore");
	let mut script =
		String::from("DEFINE NAMESPACE test; DEFINE DATABASE test; DEFINE TABLE rs SCHEMAFULL;");
	let mut assignments = Vec::new();
	for kind in KINDS {
		let field = sized_field(kind);
		script.push_str(&format!("DEFINE FIELD {} ON rs TYPE {};", column(kind), field.surql_type));
		assignments.push(format!("{} = {}", column(kind), field.literal));
	}
	script.push_str(&format!("CREATE rs:1 SET {};", assignments.join(", ")));
	let session = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(&script, &session, None).await.expect("run the setup") {
		response.result.expect("a setup statement");
	}
	start_on(ds).await
}

/// Asserts that the one cell `sql` answers over the text protocol and over the binary one reads as
/// `value` to the precision of a float.
async fn assert_number(stream: &mut TcpStream, sql: &str, value: f64) {
	for (protocol, answer) in
		[("text", text(stream, sql).await), ("binary", binary(stream, sql).await)]
	{
		assert_eq!(answer.error, None, "{sql} ({protocol})");
		assert_eq!(answer.rows.len(), 1, "{sql} ({protocol})");
		let cell = &answer.rows[0][0];
		let read: f64 = cell.parse().expect("a number");
		assert!(
			(read - value).abs() <= value.abs() * 1e-6,
			"{sql} ({protocol}): {cell} is not {value}"
		);
	}
}

#[tokio::test]
async fn a_function_over_a_field_of_every_number_kind_is_sent() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	for kind in KINDS {
		let field = sized_field(kind);
		let v = column(kind);
		for (select, shown) in [
			(v.clone(), field.shown),
			(format!("floor({v})"), field.floor),
			(format!("ceil({v})"), field.ceil),
			(format!("CASE WHEN true THEN {v} ELSE {v} END"), field.shown),
			(format!("coalesce(NULL, {v})"), field.shown),
			(format!("ifnull(NULL, {v})"), field.shown),
			(format!("IF(true, {v}, {v})"), field.shown),
			(format!("greatest({v}, {v})"), field.shown),
			(format!("least({v}, {v})"), field.shown),
		] {
			assert_answer(&mut stream, &format!("SELECT {select} FROM rs"), None, &[shown]).await;
		}
	}
	shutdown.cancel();
}

#[tokio::test]
async fn numbers_of_any_two_kinds_are_sent() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	for a in KINDS {
		for b in KINDS {
			let (first, second) = (sized_field(a), sized_field(b));
			let (v, w) = (column(a), column(b));
			for (select, value) in [
				(format!("CASE WHEN true THEN {v} ELSE {w} END"), first.value),
				(format!("IF(true, {v}, {w})"), first.value),
				(format!("coalesce({v}, {w})"), first.value),
				(format!("greatest({v}, {w})"), first.value.max(second.value)),
				(format!("least({v}, {w})"), first.value.min(second.value)),
			] {
				assert_number(&mut stream, &format!("SELECT {select} FROM rs"), value).await;
			}
		}
	}
	shutdown.cancel();
}

/// A `u64` field is announced as `BIGINT UNSIGNED`, which is `LONGLONG` with the `UNSIGNED` flag,
/// and its maximum reads back exactly over both protocols.
#[tokio::test]
async fn a_u64_field_is_announced_as_unsigned_longlong_and_holds_its_maximum() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	let mut names: Vec<String> = KINDS.iter().map(|kind| column(*kind)).collect();
	names.push("id".to_string());
	names.sort();
	let position =
		names.iter().position(|name| *name == column(NumberKind::U64)).expect("a column");
	for (protocol, answer) in [
		("text", text(&mut stream, "SELECT * FROM rs").await),
		("binary", binary(&mut stream, "SELECT * FROM rs").await),
	] {
		assert_eq!(answer.error, None, "{protocol}");
		assert_eq!(answer.types[position], MYSQL_TYPE_LONGLONG, "{protocol}");
		assert!(answer.flags[position] & UNSIGNED_FLAG != 0, "{protocol}");
		assert_eq!(answer.rows[0][position], "18446744073709551615", "{protocol}");
	}
	shutdown.cancel();
}

/// Whether a column announced as `column_type`, unsigned or not, can carry `value` as a client
/// reads it: an integer type holds the values of its range, a float type any float, and a decimal
/// type a decimal. `family` is the type a field of the kind is announced as when nothing
/// describes it: the family of types the kind may be announced under, whether by its range or by
/// the width it is declared with.
fn announced_type_holds(family: u8, column_type: u8, flags: u16, value: f64) -> bool {
	let unsigned = flags & UNSIGNED_FLAG != 0;
	match family {
		MYSQL_TYPE_LONGLONG => {
			let bits = match column_type {
				MYSQL_TYPE_TINY => 8,
				MYSQL_TYPE_SHORT => 16,
				MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => 32,
				MYSQL_TYPE_LONGLONG => 64,
				_ => return false,
			};
			let (low, high) = if unsigned {
				(0.0, 2f64.powi(bits) - 1.0)
			} else {
				(-(2f64.powi(bits - 1)), 2f64.powi(bits - 1) - 1.0)
			};
			(low..=high).contains(&value)
		}
		MYSQL_TYPE_DOUBLE => matches!(column_type, MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE),
		MYSQL_TYPE_NEWDECIMAL => {
			column_type == MYSQL_TYPE_NEWDECIMAL
				|| (column_type == MYSQL_TYPE_LONGLONG && unsigned && value >= 0.0)
		}
		_ => column_type == family,
	}
}

#[tokio::test]
async fn a_listed_value_of_every_number_kind_is_announced_by_its_range() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	// The columns of a listed row are in the order of their names.
	let mut names: Vec<String> = KINDS.iter().map(|kind| column(*kind)).collect();
	names.push("id".to_string());
	names.sort();
	for (protocol, answer) in [
		("text", text(&mut stream, "SELECT * FROM rs").await),
		("binary", binary(&mut stream, "SELECT * FROM rs").await),
	] {
		assert_eq!(answer.error, None, "{protocol}");
		assert_eq!(answer.types.len(), names.len(), "{protocol}");
		for kind in KINDS {
			let field = sized_field(kind);
			let position = names.iter().position(|name| *name == column(kind)).expect("a column");
			assert!(
				announced_type_holds(
					field.listed_type,
					answer.types[position],
					answer.flags[position],
					field.value
				),
				"{} ({protocol}): announced as type {} with flags {:#x}",
				column(kind),
				answer.types[position],
				answer.flags[position]
			);
			let cell = &answer.rows[0][position];
			let read: f64 = cell.parse().expect("a number");
			assert!(
				(read - field.value).abs() <= field.value.abs() * 1e-6,
				"{} ({protocol}): {cell} is not {}",
				column(kind),
				field.value
			);
		}
	}
	shutdown.cancel();
}
