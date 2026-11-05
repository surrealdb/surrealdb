//! The type a result column is announced as when the values of an expression may differ in type
//! (`CASE`, `COALESCE`, `GREATEST`, `floor` and the like), and the form its values take: the type
//! must be one every value of the column has, in the text format and in the binary one.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::NumberKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const IO_TIMEOUT: Duration = Duration::from_secs(10);

const OID_BOOL: i32 = 16;
const OID_INT8: i32 = 20;
const OID_INT2: i32 = 21;
const OID_INT4: i32 = 23;
const OID_TEXT: i32 = 25;
const OID_FLOAT4: i32 = 700;
const OID_FLOAT8: i32 = 701;
const OID_DATE: i32 = 1082;
const OID_TIME: i32 = 1083;
const OID_TIMESTAMP: i32 = 1114;
const OID_TIMESTAMPTZ: i32 = 1184;
const OID_INTERVAL: i32 = 1186;
const OID_NUMERIC: i32 = 1700;
const OID_JSON_ARRAY: i32 = 199;
const OID_JSONB_ARRAY: i32 = 3807;

async fn start() -> (std::net::SocketAddr, CancellationToken) {
	start_on(Datastore::new("memory").await.expect("create the datastore")).await
}

async fn start_on(ds: Arc<Datastore>) -> (std::net::SocketAddr, CancellationToken) {
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

fn message(kind: u8, body: &[u8]) -> BytesMut {
	let mut message = BytesMut::new();
	message.put_u8(kind);
	message.put_i32(body.len() as i32 + 4);
	message.put_slice(body);
	message
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
				b'E' => panic!("the startup failed: {}", String::from_utf8_lossy(&payload)),
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

/// One column of a RowDescription: its type OID and the format its values travel in.
#[derive(Debug, Clone, PartialEq)]
struct Column {
	oid: i32,
	format: i16,
}

/// What the server answered to a statement.
#[derive(Debug, Default)]
struct Answer {
	columns: Vec<Column>,
	rows: Vec<Vec<Option<Vec<u8>>>>,
	error: Option<String>,
}

fn parse_row_description(payload: &[u8]) -> Vec<Column> {
	let mut cur = payload;
	let count = cur.get_i16();
	let mut columns = Vec::new();
	for _ in 0..count {
		let end = cur.iter().position(|b| *b == 0).expect("a column name");
		cur.advance(end + 1);
		cur.advance(4 + 2);
		let oid = cur.get_i32();
		cur.advance(2 + 4);
		let format = cur.get_i16();
		columns.push(Column {
			oid,
			format,
		});
	}
	columns
}

fn parse_data_row(payload: &[u8]) -> Vec<Option<Vec<u8>>> {
	let mut cur = payload;
	let count = cur.get_i16();
	(0..count)
		.map(|_| {
			let length = cur.get_i32();
			(length >= 0).then(|| {
				let mut bytes = vec![0u8; length as usize];
				cur.copy_to_slice(&mut bytes);
				bytes
			})
		})
		.collect()
}

async fn collect(stream: &mut TcpStream) -> Answer {
	let mut answer = Answer::default();
	timeout(IO_TIMEOUT, async {
		loop {
			let (kind, payload) = read_message(stream).await;
			match kind {
				b'T' => answer.columns = parse_row_description(&payload),
				b'D' => answer.rows.push(parse_data_row(&payload)),
				b'E' => answer.error = Some(String::from_utf8_lossy(&payload).into_owned()),
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.expect("the server answered the statement");
	answer
}

/// A statement over the simple protocol, whose values come in text.
async fn simple(stream: &mut TcpStream, sql: &str) -> Answer {
	let mut body = sql.as_bytes().to_vec();
	body.push(0);
	stream.write_all(&message(b'Q', &body)).await.expect("send the query");
	collect(stream).await
}

/// A statement over the extended protocol, whose values come in `format` (0 text, 1 binary).
async fn extended(stream: &mut TcpStream, sql: &str, format: i16) -> Answer {
	let mut parse = BytesMut::new();
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(0);
	let mut bind = BytesMut::new();
	bind.put_u8(0);
	bind.put_u8(0);
	bind.put_i16(0);
	bind.put_i16(0);
	bind.put_i16(1);
	bind.put_i16(format);
	let mut describe = BytesMut::new();
	describe.put_u8(b'P');
	describe.put_u8(0);
	let mut execute = BytesMut::new();
	execute.put_u8(0);
	execute.put_i32(0);
	for packet in [
		message(b'P', &parse),
		message(b'B', &bind),
		message(b'D', &describe),
		message(b'E', &execute),
		message(b'S', &[]),
	] {
		stream.write_all(&packet).await.expect("send an extended message");
	}
	collect(stream).await
}

/// The columns a client is told of by describing the statement before it binds any value.
async fn described_statement(stream: &mut TcpStream, sql: &str) -> Vec<Column> {
	let mut parse = BytesMut::new();
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(0);
	let mut describe = BytesMut::new();
	describe.put_u8(b'S');
	describe.put_u8(0);
	for packet in [message(b'P', &parse), message(b'D', &describe), message(b'S', &[])] {
		stream.write_all(&packet).await.expect("send an extended message");
	}
	let answer = collect(stream).await;
	assert_eq!(answer.error, None, "{sql}");
	answer.columns
}

/// The base-10000 digits of a binary `numeric`, as the decimal text a client shows.
fn numeric_text(bytes: &[u8]) -> String {
	let mut cur = bytes;
	let ndigits = cur.get_i16();
	let weight = i32::from(cur.get_i16());
	let sign = cur.get_u16();
	let dscale = cur.get_i16();
	assert!(sign == 0 || sign == 0x4000, "a finite numeric");
	let digits: Vec<i16> = (0..ndigits).map(|_| cur.get_i16()).collect();
	let mut int_part = String::new();
	let mut frac_part = String::new();
	for place in (0..=weight.max(0)).rev() {
		let index = weight - place;
		let digit = usize::try_from(index).ok().and_then(|i| digits.get(i)).copied().unwrap_or(0);
		if int_part.is_empty() {
			int_part.push_str(&digit.to_string());
		} else {
			int_part.push_str(&format!("{digit:04}"));
		}
	}
	if int_part.is_empty() {
		int_part.push('0');
	}
	let mut place = -1;
	while frac_part.len() < usize::try_from(dscale).unwrap_or(0) {
		let index = weight - place;
		let digit = usize::try_from(index).ok().and_then(|i| digits.get(i)).copied().unwrap_or(0);
		frac_part.push_str(&format!("{digit:04}"));
		place -= 1;
	}
	frac_part.truncate(usize::try_from(dscale).unwrap_or(0));
	let sign = if sign == 0x4000 {
		"-"
	} else {
		""
	};
	if frac_part.is_empty() {
		format!("{sign}{int_part}")
	} else {
		format!("{sign}{int_part}.{frac_part}")
	}
}

/// `HH:MM:SS` and the fraction of a second with its trailing zeros trimmed, for a count of
/// microseconds since midnight.
fn clock_text(micros: i64) -> String {
	let (secs, fraction) = (micros / 1_000_000, micros % 1_000_000);
	let mut out = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
	if fraction != 0 {
		out.push_str(format!(".{fraction:06}").trim_end_matches('0'));
	}
	out
}

/// A cell as the text a client shows for it, decoded from the format and the type the server
/// announced.
fn shown(column: &Column, cell: &Option<Vec<u8>>) -> String {
	let Some(bytes) = cell else {
		return "NULL".to_string();
	};
	if column.format == 0 {
		return String::from_utf8_lossy(bytes).into_owned();
	}
	match column.oid {
		OID_BOOL => (bytes[0] != 0).to_string(),
		OID_INT2 => i16::from_be_bytes(bytes.as_slice().try_into().expect("2 bytes")).to_string(),
		OID_INT4 => i32::from_be_bytes(bytes.as_slice().try_into().expect("4 bytes")).to_string(),
		OID_INT8 => i64::from_be_bytes(bytes.as_slice().try_into().expect("8 bytes")).to_string(),
		OID_FLOAT4 => f32::from_be_bytes(bytes.as_slice().try_into().expect("4 bytes")).to_string(),
		OID_FLOAT8 => f64::from_be_bytes(bytes.as_slice().try_into().expect("8 bytes")).to_string(),
		OID_NUMERIC => numeric_text(bytes),
		OID_TEXT => String::from_utf8_lossy(bytes).into_owned(),
		OID_TIMESTAMPTZ => format!("timestamptz:{}", bytes.len()),
		OID_DATE => {
			let days = i32::from_be_bytes(bytes.as_slice().try_into().expect("4 bytes"));
			let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("an epoch");
			(epoch + chrono::TimeDelta::days(i64::from(days))).format("%Y-%m-%d").to_string()
		}
		OID_TIME => {
			let micros = i64::from_be_bytes(bytes.as_slice().try_into().expect("8 bytes"));
			clock_text(micros)
		}
		OID_TIMESTAMP => {
			let micros = i64::from_be_bytes(bytes.as_slice().try_into().expect("8 bytes"));
			let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("an epoch");
			let day = epoch + chrono::TimeDelta::days(micros.div_euclid(86_400_000_000));
			format!("{} {}", day.format("%Y-%m-%d"), clock_text(micros.rem_euclid(86_400_000_000)))
		}
		other => panic!("a binary cell of type {other} has no reader here"),
	}
}

/// The statement over the simple protocol, and over the extended one with its values in text
/// and in binary: every way a client can ask for it.
async fn every_way(stream: &mut TcpStream, sql: &str) -> [(&'static str, Answer); 3] {
	[
		("simple", simple(stream, sql).await),
		("extended, text", extended(stream, sql, 0).await),
		("extended, binary", extended(stream, sql, 1).await),
	]
}

/// Asserts that `sql` is answered with one column announced as `oid` whose rows show as `rows`,
/// whichever way it is asked for, and that each value reads as the announced type. The type is the
/// same when the statement is described before any value is bound.
async fn assert_answer(stream: &mut TcpStream, sql: &str, oid: i32, rows: &[&str]) {
	let before_binding: Vec<i32> =
		described_statement(stream, sql).await.iter().map(|column| column.oid).collect();
	assert_eq!(before_binding, [oid], "{sql} (described before it is bound)");
	for (way, answer) in every_way(stream, sql).await {
		assert_eq!(answer.error, None, "{sql} ({way})");
		assert_eq!(
			answer.columns.iter().map(|column| column.oid).collect::<Vec<_>>(),
			[oid],
			"{sql} ({way})"
		);
		let cells: Vec<String> =
			answer.rows.iter().map(|row| shown(&answer.columns[0], &row[0])).collect();
		assert_eq!(cells, rows, "{sql} ({way})");
		for cell in &answer.rows {
			assert_reads_as(oid, &shown(&answer.columns[0], &cell[0]), sql, way);
		}
	}
}

/// A value a client reads as `oid` is of that type: an integer column holds only integers.
fn assert_reads_as(oid: i32, cell: &str, sql: &str, way: &str) {
	if cell == "NULL" {
		return;
	}
	let reads = match oid {
		OID_INT2 => cell.parse::<i16>().is_ok(),
		OID_INT4 => cell.parse::<i32>().is_ok(),
		OID_INT8 => cell.parse::<i64>().is_ok(),
		OID_FLOAT4 | OID_FLOAT8 | OID_NUMERIC => cell.parse::<f64>().is_ok(),
		_ => true,
	};
	assert!(reads, "{sql} ({way}): {cell:?} is not a value of type {oid}");
}

#[tokio::test]
async fn a_number_function_is_announced_as_the_type_its_results_have() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, oid, rows) in [
		("SELECT floor(1.5)", OID_NUMERIC, ["1"]),
		("SELECT ceil(1.2)", OID_NUMERIC, ["2"]),
		("SELECT ceiling(-1.2)", OID_NUMERIC, ["-1"]),
		("SELECT floor(1)", OID_INT8, ["1"]),
		("SELECT ceil(7)", OID_INT8, ["7"]),
		("SELECT greatest(1, 2.5)", OID_NUMERIC, ["2.5"]),
		("SELECT greatest(3, 2.5)", OID_NUMERIC, ["3"]),
		("SELECT least(1, 2.5)", OID_NUMERIC, ["1"]),
		("SELECT least(4, 2.5)", OID_NUMERIC, ["2.5"]),
		("SELECT greatest(1, 2, 3)", OID_INT8, ["3"]),
		("SELECT coalesce(1, 2.5)", OID_NUMERIC, ["1"]),
		("SELECT coalesce(NULL, 2.5)", OID_NUMERIC, ["2.5"]),
		("SELECT coalesce(NULL, NULL, 7)", OID_INT8, ["7"]),
		("SELECT coalesce(NULL, 'a', 1)", OID_TEXT, ["a"]),
		("SELECT coalesce(NULL, NULL)", OID_TEXT, ["NULL"]),
	] {
		assert_answer(&mut stream, sql, oid, &rows).await;
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_case_is_announced_as_the_type_all_its_branches_have() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, oid, rows) in [
		("SELECT CASE WHEN false THEN 1 ELSE 2.5 END", OID_NUMERIC, ["2.5"]),
		("SELECT CASE WHEN true THEN 1 ELSE 2.5 END", OID_NUMERIC, ["1"]),
		("SELECT CASE WHEN false THEN 1.5 ELSE 2 END", OID_NUMERIC, ["2"]),
		("SELECT CASE WHEN true THEN 1 ELSE 2 END", OID_INT4, ["1"]),
		("SELECT CASE WHEN false THEN 1 WHEN true THEN 2.5 ELSE 3 END", OID_NUMERIC, ["2.5"]),
		("SELECT CASE 2 WHEN 1 THEN 1 WHEN 2 THEN 2.5 END", OID_NUMERIC, ["2.5"]),
		// Text holds a value of any kind, so a CASE of different kinds is read as text.
		("SELECT CASE WHEN true THEN 1 ELSE 'a' END", OID_TEXT, ["1"]),
		("SELECT CASE WHEN false THEN 1 ELSE 'a' END", OID_TEXT, ["a"]),
		("SELECT CASE WHEN false THEN true ELSE 'a' END", OID_TEXT, ["a"]),
		("SELECT CASE WHEN true THEN true ELSE 1.5 END", OID_TEXT, ["t"]),
		// A NULL branch has no value of its own.
		("SELECT CASE WHEN true THEN 1 ELSE NULL END", OID_INT8, ["1"]),
		("SELECT CASE WHEN false THEN 1 ELSE NULL END", OID_INT8, ["NULL"]),
		("SELECT CASE WHEN false THEN NULL ELSE 1.5 END", OID_NUMERIC, ["1.5"]),
		("SELECT CASE WHEN false THEN 1 END", OID_INT4, ["NULL"]),
	] {
		assert_answer(&mut stream, sql, oid, &rows).await;
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_value_of_any_kind_is_sent_in_a_text_column() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	// The alternatives of one kind keep their own type.
	for (way, answer) in every_way(&mut stream, "SELECT coalesce(NULL, now())").await {
		assert_eq!(answer.error, None, "{way}");
		assert_eq!(answer.columns.iter().map(|c| c.oid).collect::<Vec<_>>(), [OID_TIMESTAMPTZ]);
		assert!(answer.rows[0][0].is_some(), "{way}");
	}
	// Alternatives of different kinds are read as text, whatever kind each value is.
	for sql in [
		"SELECT CASE WHEN true THEN now() ELSE 'a' END",
		"SELECT CASE WHEN false THEN 'a' ELSE now() END",
		"SELECT CASE WHEN true THEN gen_random_uuid() ELSE 1 END",
		"SELECT CASE WHEN true THEN now() ELSE gen_random_uuid() END",
	] {
		for (way, answer) in every_way(&mut stream, sql).await {
			assert_eq!(answer.error, None, "{sql} ({way})");
			assert_eq!(
				answer.columns,
				[Column {
					oid: OID_TEXT,
					format: answer.columns[0].format
				}],
				"{sql} ({way})"
			);
			let cell = shown(&answer.columns[0], &answer.rows[0][0]);
			assert!(cell.len() > 8 && cell != "NULL", "{sql} ({way}): {cell:?}");
		}
	}
	shutdown.cancel();
}

/// A table with an integer, a decimal, a float and a text column, so what a CASE or a function
/// returns for one row differs in kind from what it returns for the next.
async fn with_table(stream: &mut TcpStream) {
	for sql in [
		"CREATE TABLE rt (id BIGINT PRIMARY KEY, i INT, d NUMERIC(10,2), f DOUBLE PRECISION, \
		 t TEXT)",
		"INSERT INTO rt (id, i, d, f, t) VALUES (1, 2, 3.25, 4.5, 'x'), (2, 3, 4.25, 5.5, 'y')",
	] {
		let answer = simple(stream, sql).await;
		assert_eq!(answer.error, None, "{sql}");
	}
}

#[tokio::test]
async fn a_result_over_a_table_is_announced_as_the_type_all_its_rows_have() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	with_table(&mut stream).await;
	for (select, oid, rows) in [
		("CASE WHEN id = 1 THEN d ELSE f END", OID_FLOAT8, ["3.25", "5.5"]),
		("CASE WHEN id = 1 THEN f ELSE d END", OID_FLOAT8, ["4.5", "4.25"]),
		("CASE WHEN id = 1 THEN i ELSE d END", OID_NUMERIC, ["2", "4.25"]),
		("CASE WHEN id = 1 THEN i ELSE f END", OID_FLOAT8, ["2", "5.5"]),
		("CASE WHEN id = 1 THEN i ELSE t END", OID_TEXT, ["2", "y"]),
		("greatest(i, d)", OID_NUMERIC, ["3.25", "4.25"]),
		("greatest(i, f)", OID_FLOAT8, ["4.5", "5.5"]),
		("least(d, f)", OID_FLOAT8, ["3.25", "4.25"]),
		("coalesce(d, f)", OID_FLOAT8, ["3.25", "4.25"]),
		("floor(d)", OID_NUMERIC, ["3", "4"]),
		("ceil(d)", OID_NUMERIC, ["4", "5"]),
		("floor(f)", OID_FLOAT8, ["4", "5"]),
		("ceiling(f)", OID_FLOAT8, ["5", "6"]),
		("floor(i)", OID_INT4, ["2", "3"]),
		// The sum of a decimal and a float is a float, as in PostgreSQL.
		("d + f", OID_FLOAT8, ["7.75", "9.75"]),
	] {
		let sql = format!("SELECT {select} FROM rt ORDER BY id");
		assert_answer(&mut stream, &sql, oid, &rows).await;
	}
	// An aggregate takes the type of the CASE it adds up.
	assert_answer(
		&mut stream,
		"SELECT sum(CASE WHEN i > 2 THEN i ELSE NULL END) FROM rt",
		OID_INT8,
		&["3"],
	)
	.await;
	shutdown.cancel();
}

/// A field of one number kind: the SurrealQL type that declares it, a literal of the kind, the type
/// a client is told the field has, and what the field, its floor and its ceiling show as.
struct SizedField {
	surql_type: &'static str,
	literal: &'static str,
	oid: i32,
	shown: &'static str,
	floor: &'static str,
	ceil: &'static str,
	value: f64,
}

/// The field of a number kind. A kind without an arm is a compile error, so a number kind added to
/// the engine has to be given here the type it is announced as. PostgreSQL has no unsigned or
/// one-byte integer: each integer kind is announced as the smallest of its integer types that holds
/// the whole range of the kind, and a `u64` as a `numeric`.
fn sized_field(kind: NumberKind) -> SizedField {
	let (surql_type, literal, oid, shown, floor, ceil, value) = match kind {
		NumberKind::I8 => ("i8", "-5i8", OID_INT2, "-5", "-5", "-5", -5.0),
		NumberKind::U8 => ("u8", "200u8", OID_INT2, "200", "200", "200", 200.0),
		NumberKind::I16 => ("i16", "-300i16", OID_INT2, "-300", "-300", "-300", -300.0),
		NumberKind::U16 => ("u16", "60000u16", OID_INT4, "60000", "60000", "60000", 60000.0),
		NumberKind::I32 => ("i32", "-70000i32", OID_INT4, "-70000", "-70000", "-70000", -70000.0),
		NumberKind::U32 => {
			("u32", "4000000000u32", OID_INT8, "4000000000", "4000000000", "4000000000", 4e9)
		}
		NumberKind::Int => {
			("int", "-5000000000", OID_INT8, "-5000000000", "-5000000000", "-5000000000", -5e9)
		}
		NumberKind::U64 => (
			"u64",
			"18446744073709551615u64",
			OID_NUMERIC,
			"18446744073709551615",
			"18446744073709551615",
			"18446744073709551615",
			1.8446744073709552e19,
		),
		NumberKind::F16 => ("f16", "1.5f16", OID_FLOAT4, "1.5", "1", "2", 1.5),
		NumberKind::F32 => ("f32", "0.1f32", OID_FLOAT4, "0.1", "0", "1", 0.1),
		NumberKind::Float => ("float", "0.25f", OID_FLOAT8, "0.25", "0", "1", 0.25),
		NumberKind::Decimal => ("decimal", "3.25dec", OID_NUMERIC, "3.25", "3", "4", 3.25),
		NumberKind::BigDecimal => {
			unimplemented!("the SurrealQL front end has no bigdecimal kind name or literal")
		}
	};
	SizedField {
		surql_type,
		literal,
		oid,
		shown,
		floor,
		ceil,
		value,
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

#[tokio::test]
async fn a_field_of_every_number_kind_is_announced_as_the_type_that_holds_it() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	for kind in KINDS {
		let field = sized_field(kind);
		let sql = format!("SELECT {} FROM rs", column(kind));
		assert_answer(&mut stream, &sql, field.oid, &[field.shown]).await;
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_function_over_a_field_of_every_number_kind_is_announced_as_the_type_its_results_have() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	for kind in KINDS {
		let field = sized_field(kind);
		let v = column(kind);
		// A float of any width is rounded as a double precision, which holds all of them.
		let rounded_oid = if field.oid == OID_FLOAT4 {
			OID_FLOAT8
		} else {
			field.oid
		};
		for (select, oid, shown) in [
			(format!("floor({v})"), rounded_oid, field.floor),
			(format!("ceil({v})"), rounded_oid, field.ceil),
			(format!("CASE WHEN true THEN {v} ELSE {v} END"), field.oid, field.shown),
			(format!("coalesce(NULL, {v})"), field.oid, field.shown),
			(format!("greatest({v}, {v})"), field.oid, field.shown),
			(format!("least({v}, {v})"), field.oid, field.shown),
		] {
			assert_answer(&mut stream, &format!("SELECT {select} FROM rs"), oid, &[shown]).await;
		}
	}
	shutdown.cancel();
}

/// The number types in the order PostgreSQL converts them implicitly: the type of a `CASE`,
/// `COALESCE`, `GREATEST` or `LEAST` over numbers of several types is the latest of them.
const NUMBER_ORDER: [i32; 6] = [OID_INT2, OID_INT4, OID_INT8, OID_NUMERIC, OID_FLOAT4, OID_FLOAT8];

#[tokio::test]
async fn numbers_of_any_two_kinds_are_announced_as_the_later_of_their_types() {
	let (addr, shutdown) = start_with_sized_fields().await;
	let mut stream = connect(addr).await;
	let position =
		|oid: i32| NUMBER_ORDER.iter().position(|number| *number == oid).expect("a number");
	for a in KINDS {
		for b in KINDS {
			let (first, second) = (sized_field(a), sized_field(b));
			let oid = NUMBER_ORDER[position(first.oid).max(position(second.oid))];
			let (v, w) = (column(a), column(b));
			for (select, value) in [
				(format!("CASE WHEN true THEN {v} ELSE {w} END"), first.value),
				(format!("coalesce({v}, {w})"), first.value),
				(format!("greatest({v}, {w})"), first.value.max(second.value)),
				(format!("least({v}, {w})"), first.value.min(second.value)),
			] {
				let sql = format!("SELECT {select} FROM rs");
				let before_binding: Vec<i32> = described_statement(&mut stream, &sql)
					.await
					.iter()
					.map(|column| column.oid)
					.collect();
				assert_eq!(before_binding, [oid], "{sql} (described before it is bound)");
				let mut cells = Vec::new();
				for (way, answer) in every_way(&mut stream, &sql).await {
					assert_eq!(answer.error, None, "{sql} ({way})");
					assert_eq!(
						answer.columns.iter().map(|column| column.oid).collect::<Vec<_>>(),
						[oid],
						"{sql} ({way})"
					);
					let cell = shown(&answer.columns[0], &answer.rows[0][0]);
					assert_reads_as(oid, &cell, &sql, way);
					let read: f64 = cell.parse().expect("a number");
					assert!(
						(read - value).abs() <= value.abs() * 1e-6,
						"{sql} ({way}): {cell} is not {value}"
					);
					cells.push(cell);
				}
				// The text and the binary format carry the same value, to the precision of the
				// type.
				let same = |left: &String, right: &String| {
					if oid == OID_FLOAT4 {
						left.parse::<f32>() == right.parse::<f32>()
					} else {
						left == right
					}
				};
				assert!(cells.iter().all(|cell| same(cell, &cells[0])), "{sql}: {cells:?}");
			}
		}
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_date_time_or_timestamp_is_announced_as_its_own_type() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, oid, rows) in [
		("SELECT CAST('2026-10-27' AS date)", OID_DATE, ["2026-10-27"]),
		("SELECT CAST('2026-10-27 12:30:00' AS date)", OID_DATE, ["2026-10-27"]),
		("SELECT make_date(2026, 10, 27)", OID_DATE, ["2026-10-27"]),
		("SELECT CAST('2026-10-27' AS date) + 3", OID_DATE, ["2026-10-30"]),
		("SELECT current_date - current_date", OID_INT4, ["0"]),
		("SELECT CAST('12:30:00.5' AS time)", OID_TIME, ["12:30:00.5"]),
		("SELECT make_time(1, 2, 3)", OID_TIME, ["01:02:03"]),
		("SELECT CAST('2026-10-27 12:30:00' AS timestamp)", OID_TIMESTAMP, ["2026-10-27 12:30:00"]),
		(
			"SELECT CAST('1999-12-31 23:59:59.25' AS timestamp)",
			OID_TIMESTAMP,
			["1999-12-31 23:59:59.25"],
		),
		(
			"SELECT CAST('2026-10-27' AS date) + CAST('01:00:00' AS time)",
			OID_TIMESTAMP,
			["2026-10-27 01:00:00"],
		),
		(
			"SELECT date_trunc('day', CAST('2026-10-27 12:30:00' AS timestamp))",
			OID_TIMESTAMP,
			["2026-10-27 00:00:00"],
		),
	] {
		assert_answer(&mut stream, sql, oid, &rows).await;
	}
	shutdown.cancel();
}

#[tokio::test]
async fn an_interval_is_announced_as_an_interval_with_its_months_days_and_negatives() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, text, binary) in [
		(
			"SELECT CAST('1 year 2 mons 3 days 04:05:06.5' AS interval)",
			"1 year 2 mons 3 days 04:05:06.5",
			(14i32, 3i32, 14_706_500_000i64),
		),
		(
			"SELECT CAST('-1 days -02:00:00' AS interval)",
			"-1 days -02:00:00",
			(0, -1, -7_200_000_000),
		),
		(
			"SELECT age(CAST('2026-10-27' AS timestamp), CAST('2020-02-29' AS timestamp))",
			"6 years 7 mons 27 days",
			(79, 27, 0),
		),
	] {
		let simple = simple(&mut stream, sql).await;
		assert_eq!(simple.error, None, "{sql}");
		assert_eq!(
			simple.columns.iter().map(|c| c.oid).collect::<Vec<_>>(),
			[OID_INTERVAL],
			"{sql}"
		);
		assert_eq!(shown(&simple.columns[0], &simple.rows[0][0]), text, "{sql}");
		let binary_answer = extended(&mut stream, sql, 1).await;
		assert_eq!(binary_answer.error, None, "{sql}");
		let cell = binary_answer.rows[0][0].clone().expect("a cell");
		let (months, days, micros) = binary;
		let mut want = Vec::new();
		want.extend_from_slice(&micros.to_be_bytes());
		want.extend_from_slice(&days.to_be_bytes());
		want.extend_from_slice(&months.to_be_bytes());
		assert_eq!(cell, want, "{sql}");
	}
	shutdown.cancel();
}

/// A `json[]` or `jsonb[]` column, of one dimension or two, nullable or not, is announced as the
/// array type it is declared with: its kind, an array of `any`, names no type, and its declared
/// SQL type does.
#[tokio::test]
async fn a_json_array_column_is_announced_as_an_array_of_json() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	let sql = "CREATE TABLE ja (id INT PRIMARY KEY, j JSON[], jb JSONB[] NOT NULL, grid JSONB[][])";
	assert_eq!(simple(&mut stream, sql).await.error, None, "{sql}");
	let sql = "SELECT j, jb, grid FROM ja";
	let oids: Vec<i32> =
		described_statement(&mut stream, sql).await.iter().map(|column| column.oid).collect();
	assert_eq!(oids, [OID_JSON_ARRAY, OID_JSONB_ARRAY, OID_JSONB_ARRAY], "{sql}");
	shutdown.cancel();
}
