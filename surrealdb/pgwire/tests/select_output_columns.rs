//! The columns of a `SELECT` over the PostgreSQL wire: each one answers under the name it has and
//! holds its own value, however its select list repeats or omits names.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
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
	.expect("start pgwire");
	(addr, shutdown)
}

async fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	let kind = stream.read_u8().await.expect("message type");
	let len = stream.read_i32().await.expect("message length") as usize;
	let mut payload = vec![0u8; len - 4];
	stream.read_exact(&mut payload).await.expect("message body");
	(kind, payload)
}

async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let mut body = BytesMut::new();
	body.put_i32(196608);
	for (key, value) in
		[("user", "root"), ("database", "test/test"), ("options", "-c dialect=pgsql")]
	{
		body.put_slice(key.as_bytes());
		body.put_u8(0);
		body.put_slice(value.as_bytes());
		body.put_u8(0);
	}
	body.put_u8(0);
	stream.write_i32(body.len() as i32 + 4).await.expect("startup length");
	stream.write_all(&body).await.expect("startup");
	timeout(Duration::from_secs(5), async {
		loop {
			let (kind, payload) = read_message(&mut stream).await;
			assert_ne!(kind, b'E', "startup refused: {}", String::from_utf8_lossy(&payload));
			if kind == b'Z' {
				break;
			}
		}
	})
	.await
	.expect("startup completes");
	stream
}

/// The first line of a null-terminated string at the head of `buf`, which is advanced past it.
fn cstring(buf: &mut &[u8]) -> String {
	let end = buf.iter().position(|b| *b == 0).expect("terminated string");
	let text = String::from_utf8_lossy(&buf[..end]).into_owned();
	buf.advance(end + 1);
	text
}

/// Read the replies to one request, up to its `ReadyForQuery`: each result set, or the error's
/// text.
async fn collect(stream: &mut TcpStream, sql: &str) -> Result<Vec<Rows>, String> {
	let mut results: Vec<Rows> = Vec::new();
	let mut error = None;
	timeout(Duration::from_secs(10), async {
		loop {
			let (kind, payload) = read_message(stream).await;
			let mut cur = &payload[..];
			match kind {
				b'T' => {
					let count = cur.get_i16();
					let mut columns = Vec::new();
					for _ in 0..count {
						columns.push(cstring(&mut cur));
						// The table, attribute, type, size, modifier and format of the column.
						cur.advance(18);
					}
					results.push(Rows {
						columns,
						rows: Vec::new(),
					});
				}
				b'D' => {
					let count = cur.get_i16();
					let mut row = Vec::new();
					for _ in 0..count {
						let len = cur.get_i32();
						row.push((len >= 0).then(|| {
							let cell = String::from_utf8_lossy(&cur[..len as usize]).into_owned();
							cur.advance(len as usize);
							cell
						}));
					}
					results.last_mut().expect("rows follow a description").rows.push(row);
				}
				b'E' => {
					let mut message = String::new();
					while cur.first().is_some_and(|field| *field != 0) {
						let field = cur.get_u8();
						let value = cstring(&mut cur);
						if field == b'M' {
							message = value;
						}
					}
					error = Some(message);
				}
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.unwrap_or_else(|_| panic!("{sql} did not complete"));
	match error {
		Some(message) => Err(message),
		None => Ok(results),
	}
}

/// Run `sql` as a simple query and return each of its result sets, or the error's text.
async fn query_all(stream: &mut TcpStream, sql: &str) -> Result<Vec<Rows>, String> {
	let mut text = sql.as_bytes().to_vec();
	text.push(0);
	stream.write_u8(b'Q').await.expect("query type");
	stream.write_i32(text.len() as i32 + 4).await.expect("query length");
	stream.write_all(&text).await.expect("query");
	collect(stream, sql).await
}

/// Run `sql` and return its one result set, or the error's text.
async fn query(stream: &mut TcpStream, sql: &str) -> Result<Rows, String> {
	let mut results = query_all(stream, sql).await?;
	assert_eq!(results.len(), 1, "{sql} answers one result set");
	Ok(results.remove(0))
}

/// Run `sql` with the text parameters `params` through Parse, Bind, Describe, Execute and Sync,
/// and return its result set.
async fn extended(stream: &mut TcpStream, sql: &str, params: &[(i32, &str)]) -> Rows {
	let mut request = BytesMut::new();
	let mut body = BytesMut::new();
	body.put_u8(0);
	body.put_slice(sql.as_bytes());
	body.put_u8(0);
	body.put_i16(params.len() as i16);
	for (oid, _) in params {
		body.put_i32(*oid);
	}
	request.put_u8(b'P');
	request.put_i32(body.len() as i32 + 4);
	request.put(body);

	let mut body = BytesMut::new();
	body.put_u8(0);
	body.put_u8(0);
	body.put_i16(0);
	body.put_i16(params.len() as i16);
	for (_, value) in params {
		body.put_i32(value.len() as i32);
		body.put_slice(value.as_bytes());
	}
	body.put_i16(0);
	request.put_u8(b'B');
	request.put_i32(body.len() as i32 + 4);
	request.put(body);

	request.put_slice(&[b'D', 0, 0, 0, 6, b'P', 0]);
	request.put_slice(&[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]);
	request.put_slice(&[b'S', 0, 0, 0, 4]);
	stream.write_all(&request).await.expect("extended request");
	let mut results = collect(stream, sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"));
	assert_eq!(results.len(), 1, "{sql} answers one result set");
	results.remove(0)
}

async fn run(stream: &mut TcpStream, sql: &str) {
	if let Err(error) = query_all(stream, sql).await {
		panic!("{sql}: {error}");
	}
}

async fn seed(stream: &mut TcpStream) {
	for sql in [
		"CREATE TABLE okey_a (id int, k int, v text, n int)",
		"INSERT INTO okey_a (id, k, v, n) VALUES (1, 1, 'x', 10), (2, 2, NULL, 20), (3, 3, 'z', NULL)",
		"CREATE TABLE okey_b (id int, k int, w int)",
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
	assert_eq!(names(&rows), ["?column?"; 4]);
	assert_eq!(rows.rows, cells(&[&[Some("1"), Some("a"), None, Some("5")]]));

	let rows = query(&mut stream, "SELECT 1, 1, 1").await.expect("one literal thrice");
	assert_eq!(rows.rows, cells(&[&[Some("1"), Some("1"), Some("1")]]));

	let rows = query(&mut stream, "SELECT k + 1, k + 1, v FROM okey_a ORDER BY k")
		.await
		.expect("expressions over a table");
	assert_eq!(names(&rows), ["?column?", "?column?", "v"]);
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

	let rows = query(&mut stream, "SELECT count(*), count(v), count(n), sum(n) FROM okey_a")
		.await
		.expect("aggregates");
	assert_eq!(names(&rows), ["count", "count", "count", "sum"]);
	assert_eq!(rows.rows, cells(&[&[Some("3"), Some("2"), Some("2"), Some("30")]]));

	let rows = query(
		&mut stream,
		"SELECT k, count(*), count(v) FROM okey_a GROUP BY k HAVING count(v) > 0 \
		 ORDER BY count(*), k",
	)
	.await
	.expect("grouped aggregates");
	assert_eq!(names(&rows), ["k", "count", "count"]);
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

	// 25 is the OID of `text` and 20 that of `int8`.
	let rows = extended(&mut stream, "SELECT 1 AS a, $1::text AS a, $1::text", &[(25, "x")]).await;
	assert_eq!(names(&rows), ["a", "a", "?column?"]);
	assert_eq!(rows.rows, cells(&[&[Some("1"), Some("x"), Some("x")]]));

	let rows = extended(
		&mut stream,
		"SELECT a.k, b.k FROM okey_a a LEFT JOIN okey_b b ON a.k = b.k WHERE a.k > $1 \
		 ORDER BY a.k",
		&[(20, "1")],
	)
	.await;
	assert_eq!(names(&rows), ["k", "k"]);
	assert_eq!(rows.rows, cells(&[&[Some("2"), Some("2")], &[Some("3"), None]]));
	shutdown.cancel();
}

#[tokio::test]
async fn the_columns_of_a_returning_list_answer_under_their_labels() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(
		&mut stream,
		"INSERT INTO okey_b (id, k, w) VALUES (9, 9, 900) RETURNING w::int8, k AS kk, id",
	)
	.await
	.expect("returning");
	assert_eq!(names(&rows), ["w", "kk", "id"]);
	assert_eq!(rows.rows, cells(&[&[Some("900"), Some("9"), Some("9")]]));
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
