//! The columns of a `SELECT` over the MySQL wire: each one answers under the name it has and
//! holds its own value, however its select list repeats or omits names.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

type Cell = Option<String>;

/// A result set: the name of each column and the text of each cell.
#[derive(Debug, PartialEq)]
struct Rows {
	columns: Vec<String>,
	rows: Vec<Vec<Cell>>,
}

async fn start_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("datastore");
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
	.expect("start mywire");
	(addr, shutdown)
}

async fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("packet header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let mut payload = vec![0u8; len];
	stream.read_exact(&mut payload).await.expect("packet payload");
	(header[3], payload)
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("packet header");
	stream.write_all(payload).await.expect("packet payload");
}

async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let (_, greeting) = read_packet(&mut stream).await;
	// The salt: 8 bytes after the connection id, 12 more after the fixed fields that follow.
	let version_end = greeting.iter().position(|b| *b == 0).expect("server version");
	let mut at = version_end + 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&greeting[at..at + 8]);
	at += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..].copy_from_slice(&greeting[at..at + 12]);

	let mut response = Vec::new();
	let caps =
		CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
	response.extend_from_slice(&caps.to_le_bytes());
	response.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	response.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	response.extend_from_slice(&[0u8; 23]);
	response.extend_from_slice(b"root\0");
	response.push(20);
	response.extend_from_slice(&compute_mysql_native_password_scramble("root", &salt));
	response.extend_from_slice(b"test/test\0");
	response.extend_from_slice(b"mysql_native_password\0");
	write_packet(&mut stream, 1, &response).await;

	let (seq, reply) = read_packet(&mut stream).await;
	let reply = if reply[0] == 0xfe {
		write_packet(&mut stream, seq.wrapping_add(1), b"root\0").await;
		read_packet(&mut stream).await.1
	} else {
		reply
	};
	assert_eq!(reply[0], 0x00, "handshake refused: {}", String::from_utf8_lossy(&reply));
	stream
}

/// The length-encoded string at `at`, which moves past it.
fn lenenc(buf: &[u8], at: &mut usize) -> Vec<u8> {
	let (len, skip) = match buf[*at] {
		0xfc => ((buf[*at + 1] as usize) | ((buf[*at + 2] as usize) << 8), 3),
		0xfd => (
			(buf[*at + 1] as usize)
				| ((buf[*at + 2] as usize) << 8)
				| ((buf[*at + 3] as usize) << 16),
			4,
		),
		first => (first as usize, 1),
	};
	*at += skip;
	let bytes = buf[*at..*at + len].to_vec();
	*at += len;
	bytes
}

/// Run `sql` as a text query and return its one result set, or the error's text.
async fn query(stream: &mut TcpStream, sql: &str) -> Result<Rows, String> {
	let mut text = vec![COM_QUERY];
	text.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &text).await;
	let (_, first) = read_packet(stream).await;
	match first[0] {
		0xff => return Err(String::from_utf8_lossy(&first[9..]).into_owned()),
		0x00 => {
			return Ok(Rows {
				columns: Vec::new(),
				rows: Vec::new(),
			});
		}
		_ => {}
	}
	let mut columns = Vec::new();
	for _ in 0..first[0] {
		let (_, definition) = read_packet(stream).await;
		let mut at = 0;
		for _ in 0..4 {
			lenenc(&definition, &mut at);
		}
		columns.push(String::from_utf8_lossy(&lenenc(&definition, &mut at)).into_owned());
	}
	let (_, eof) = read_packet(stream).await;
	assert_eq!(eof[0], 0xfe, "columns end with EOF");
	let mut rows = Vec::new();
	loop {
		let (_, packet) = read_packet(stream).await;
		if packet[0] == 0xfe && packet.len() < 9 {
			break;
		}
		let mut at = 0;
		let mut row = Vec::new();
		while at < packet.len() {
			if packet[at] == 0xfb {
				row.push(None);
				at += 1;
			} else {
				row.push(Some(String::from_utf8_lossy(&lenenc(&packet, &mut at)).into_owned()));
			}
		}
		rows.push(row);
	}
	Ok(Rows {
		columns,
		rows,
	})
}

async fn run(stream: &mut TcpStream, sql: &str) {
	if let Err(error) = query(stream, sql).await {
		panic!("{sql}: {error}");
	}
}

async fn seed(stream: &mut TcpStream) {
	for sql in [
		"CREATE TABLE okey_a (id INT, k INT, v VARCHAR(10), n INT)",
		"INSERT INTO okey_a (id, k, v, n) VALUES (1, 1, 'x', 10), (2, 2, NULL, 20), (3, 3, 'z', NULL)",
		"CREATE TABLE okey_b (id INT, k INT, w INT)",
		"INSERT INTO okey_b (id, k, w) VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)",
	] {
		run(stream, sql).await;
	}
}

fn cells(rows: &[&[Option<&str>]]) -> Vec<Vec<Cell>> {
	rows.iter().map(|row| row.iter().map(|cell| cell.map(str::to_string)).collect()).collect()
}

fn names(rows: &Rows) -> Vec<&str> {
	rows.columns.iter().map(String::as_str).collect()
}

/// Prepare and execute `sql` with no parameters, and return the binary rows of its result set.
async fn prepared(stream: &mut TcpStream, sql: &str) -> (Vec<String>, Vec<Vec<u8>>) {
	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &prepare).await;
	let (_, reply) = read_packet(stream).await;
	assert_ne!(reply[0], 0xff, "{sql}: {}", String::from_utf8_lossy(&reply));
	let statement_id = u32::from_le_bytes(reply[1..5].try_into().expect("statement id"));
	let described = u16::from_le_bytes(reply[5..7].try_into().expect("column count"));
	// The column definitions, and the EOF that ends them unless the client deprecates it.
	for _ in 0..described {
		read_packet(stream).await;
	}
	if described > 0 {
		read_packet(stream).await;
	}
	let mut execute = vec![COM_STMT_EXECUTE];
	execute.extend_from_slice(&statement_id.to_le_bytes());
	execute.push(CURSOR_TYPE_NO_CURSOR);
	execute.extend_from_slice(&1u32.to_le_bytes());
	write_packet(stream, 0, &execute).await;
	let (_, count) = read_packet(stream).await;
	assert!(count[0] != 0xff && count[0] != 0x00, "{sql} answers a result set: {count:?}");
	let mut columns = Vec::new();
	for _ in 0..count[0] {
		let (_, definition) = read_packet(stream).await;
		let mut at = 0;
		for _ in 0..4 {
			lenenc(&definition, &mut at);
		}
		columns.push(String::from_utf8_lossy(&lenenc(&definition, &mut at)).into_owned());
	}
	let (_, eof) = read_packet(stream).await;
	assert_eq!(eof[0], 0xfe, "columns end with EOF");
	let mut rows = Vec::new();
	loop {
		let (_, packet) = read_packet(stream).await;
		if packet[0] == 0xfe && packet.len() < 9 {
			break;
		}
		rows.push(packet);
	}
	(columns, rows)
}

#[tokio::test]
async fn a_join_selecting_a_column_two_tables_share_answers_each_tables_value() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(
		&mut stream,
		"SELECT a.k, b.k, b.w FROM okey_a a JOIN okey_b b ON a.k = b.k ORDER BY b.w",
	)
	.await
	.expect("inner join");
	assert_eq!(names(&rows), ["k", "k", "w"]);
	assert_eq!(
		rows.rows,
		cells(&[
			&[Some("1"), Some("1"), Some("100")],
			&[Some("1"), Some("1"), Some("200")],
			&[Some("2"), Some("2"), Some("300")],
		])
	);

	// The side of an outer join that has no row is NULL, and the other side keeps its value.
	let rows = query(
		&mut stream,
		"SELECT a.k, b.k, a.v FROM okey_a a LEFT JOIN okey_b b ON a.k = b.k ORDER BY a.k, b.w",
	)
	.await
	.expect("outer join");
	assert_eq!(names(&rows), ["k", "k", "v"]);
	assert_eq!(
		rows.rows,
		cells(&[
			&[Some("1"), Some("1"), Some("x")],
			&[Some("1"), Some("1"), Some("x")],
			&[Some("2"), Some("2"), None],
			&[Some("3"), None, Some("z")],
		])
	);

	let rows = query(
		&mut stream,
		"SELECT a.id, b.id, a.k, b.k FROM okey_a a JOIN okey_b b ON a.k = b.k ORDER BY b.id",
	)
	.await
	.expect("record ids");
	assert_eq!(names(&rows), ["id", "id", "k", "k"]);
	assert_eq!(
		rows.rows,
		cells(&[
			&[Some("1"), Some("1"), Some("1"), Some("1")],
			&[Some("1"), Some("2"), Some("1"), Some("1")],
			&[Some("2"), Some("3"), Some("2"), Some("2")],
		])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn columns_that_repeat_a_name_answer_their_own_values() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(&mut stream, "SELECT k AS n, v AS n FROM okey_a ORDER BY k")
		.await
		.expect("one alias twice");
	assert_eq!(names(&rows), ["n", "n"]);
	assert_eq!(
		rows.rows,
		cells(&[&[Some("1"), Some("x")], &[Some("2"), None], &[Some("3"), Some("z")]])
	);

	let rows = query(&mut stream, "SELECT k, k, v FROM okey_a ORDER BY k DESC LIMIT 2")
		.await
		.expect("one column twice");
	assert_eq!(names(&rows), ["k", "k", "v"]);
	assert_eq!(
		rows.rows,
		cells(&[&[Some("3"), Some("3"), Some("z")], &[Some("2"), Some("2"), None]])
	);

	let rows = query(&mut stream, "SELECT 1 AS a, 2 AS a, 'c' AS a").await.expect("literals");
	assert_eq!(names(&rows), ["a", "a", "a"]);
	assert_eq!(rows.rows, cells(&[&[Some("1"), Some("2"), Some("c")]]));
	shutdown.cancel();
}

#[tokio::test]
async fn columns_without_a_name_answer_in_the_order_they_were_selected() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(&mut stream, "SELECT 1, 'a', NULL, 2 + 3").await.expect("expressions");
	assert_eq!(rows.rows, cells(&[&[Some("1"), Some("a"), None, Some("5")]]));

	let rows = query(&mut stream, "SELECT 1, 1, 1").await.expect("one literal thrice");
	assert_eq!(rows.rows, cells(&[&[Some("1"), Some("1"), Some("1")]]));

	let rows = query(&mut stream, "SELECT k + 1, k + 1, v FROM okey_a ORDER BY k")
		.await
		.expect("expressions over a table");
	assert_eq!(
		rows.rows,
		cells(&[
			&[Some("2"), Some("2"), Some("x")],
			&[Some("3"), Some("3"), None],
			&[Some("4"), Some("4"), Some("z")],
		])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn aggregates_that_share_a_name_answer_their_own_values() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(&mut stream, "SELECT COUNT(*), COUNT(v), COUNT(n), SUM(n) FROM okey_a")
		.await
		.expect("aggregates");
	assert_eq!(rows.rows, cells(&[&[Some("3"), Some("2"), Some("2"), Some("30")]]));

	let rows = query(
		&mut stream,
		"SELECT k, COUNT(*), COUNT(v) FROM okey_a GROUP BY k HAVING COUNT(v) > 0 \
		 ORDER BY COUNT(*), k",
	)
	.await
	.expect("grouped aggregates");
	assert_eq!(
		rows.rows,
		cells(&[&[Some("1"), Some("1"), Some("1")], &[Some("3"), Some("1"), Some("1")]])
	);
	shutdown.cancel();
}

#[tokio::test]
async fn the_queries_of_a_set_operation_answer_under_the_columns_of_the_first() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(
		&mut stream,
		"SELECT a.k, b.k FROM okey_a a JOIN okey_b b ON a.k = b.k UNION ALL SELECT 7, 8 \
		 ORDER BY 1, 2",
	)
	.await
	.expect("a join and a row");
	assert_eq!(names(&rows), ["k", "k"]);
	assert_eq!(
		rows.rows,
		cells(&[
			&[Some("1"), Some("1")],
			&[Some("1"), Some("1")],
			&[Some("2"), Some("2")],
			&[Some("7"), Some("8")],
		])
	);

	let rows = query(&mut stream, "SELECT 1, 2 UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6")
		.await
		.expect("three rows");
	assert_eq!(
		rows.rows,
		cells(&[&[Some("1"), Some("2")], &[Some("3"), Some("4")], &[Some("5"), Some("6")]])
	);

	let rows =
		query(&mut stream, "SELECT k, n FROM okey_a UNION SELECT k, w FROM okey_b ORDER BY 1, 2")
			.await
			.expect("differently named columns");
	assert_eq!(names(&rows), ["k", "n"]);
	assert_eq!(rows.rows.len(), 6);
	assert_eq!(rows.rows[0], cells(&[&[Some("1"), Some("10")]])[0]);
	assert_eq!(rows.rows[5], cells(&[&[Some("3"), None]])[0]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_prepared_statement_answers_each_column_under_its_own_field() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	// A binary row: the header, the null bitmap, then each cell in the encoding of its type.
	let (columns, rows) = prepared(&mut stream, "SELECT 1 AS a, 2 AS a").await;
	assert_eq!(columns, ["a", "a"]);
	let mut expected = vec![0x00, 0x00];
	expected.extend_from_slice(&1i64.to_le_bytes());
	expected.extend_from_slice(&2i64.to_le_bytes());
	assert_eq!(rows, vec![expected]);

	let (_, rows) = prepared(&mut stream, "SELECT 3, 4").await;
	let mut expected = vec![0x00, 0x00];
	expected.extend_from_slice(&3i64.to_le_bytes());
	expected.extend_from_slice(&4i64.to_le_bytes());
	assert_eq!(rows, vec![expected]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_select_of_a_wildcard_and_a_column_still_answers_from_its_rows() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows =
		query(&mut stream, "SELECT *, k AS kk FROM okey_b WHERE id = 3").await.expect("wildcard");
	let mut answered: Vec<_> = rows.columns.iter().zip(&rows.rows[0]).collect();
	answered.sort();
	let expected = [("id", "3"), ("k", "2"), ("kk", "2"), ("w", "300")];
	assert_eq!(answered.len(), expected.len());
	for ((name, cell), (expected_name, expected_cell)) in answered.into_iter().zip(expected) {
		assert_eq!((name.as_str(), cell.as_deref()), (expected_name, Some(expected_cell)));
	}
	shutdown.cancel();
}
