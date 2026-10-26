//! The pgvector surface over the wire: the `vector` type announced to a client in the text and
//! binary formats, a vector bound as a parameter in either, the `vector` row of `pg_type` that
//! client libraries look the type up by, and `CREATE EXTENSION vector`.

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

const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// The type OID of `vector`, and of an array of it.
const OID_VECTOR: i32 = 16400;
const OID_VECTOR_ARRAY: i32 = 16401;
const OID_FLOAT8: i32 = 701;
const OID_INT4: i32 = 23;
const OID_INT8: i32 = 20;

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
	assert!((4..=1 << 20).contains(&length), "message length {length} is invalid or too large");
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
	tags: Vec<String>,
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
				b'C' => {
					let end = payload.iter().position(|b| *b == 0).unwrap_or(payload.len());
					answer.tags.push(String::from_utf8_lossy(&payload[..end]).into_owned());
				}
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

/// A statement over the extended protocol with parameters: `types` are the OIDs the client
/// declares (0 leaves the type to the server), and each parameter is its format (0 text, 1
/// binary) and value. Values come back in `result_format`.
async fn bound(
	stream: &mut TcpStream,
	sql: &str,
	types: &[i32],
	params: &[(i16, Option<Vec<u8>>)],
	result_format: i16,
) -> Answer {
	let mut parse = BytesMut::new();
	parse.put_u8(0);
	parse.put_slice(sql.as_bytes());
	parse.put_u8(0);
	parse.put_i16(types.len() as i16);
	for oid in types {
		parse.put_i32(*oid);
	}
	let mut bind = BytesMut::new();
	bind.put_u8(0);
	bind.put_u8(0);
	bind.put_i16(params.len() as i16);
	for (format, _) in params {
		bind.put_i16(*format);
	}
	bind.put_i16(params.len() as i16);
	for (_, value) in params {
		match value {
			Some(bytes) => {
				bind.put_i32(bytes.len() as i32);
				bind.put_slice(bytes);
			}
			None => bind.put_i32(-1),
		}
	}
	bind.put_i16(1);
	bind.put_i16(result_format);
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

/// The parameter types the server announces for a statement it is not given the types of.
async fn parameter_oids(stream: &mut TcpStream, sql: &str) -> Vec<i32> {
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
	let mut oids = Vec::new();
	timeout(IO_TIMEOUT, async {
		loop {
			let (kind, payload) = read_message(stream).await;
			match kind {
				b't' => {
					let mut cur = payload.as_slice();
					let count = cur.get_i16();
					oids = (0..count).map(|_| cur.get_i32()).collect();
				}
				b'E' => panic!("the statement failed: {}", String::from_utf8_lossy(&payload)),
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.expect("the server answered");
	oids
}

fn text_cells(answer: &Answer, column: usize) -> Vec<String> {
	answer
		.rows
		.iter()
		.map(|row| {
			row[column]
				.as_ref()
				.map_or("NULL".to_string(), |b| String::from_utf8_lossy(b).into_owned())
		})
		.collect()
}

/// The binary form of a vector: its dimensions, a zero, and each element as a big-endian `f32`.
fn binary_vector(elements: &[f32]) -> Vec<u8> {
	let mut out = Vec::new();
	out.put_i16(elements.len() as i16);
	out.put_i16(0);
	for element in elements {
		out.put_f32(*element);
	}
	out
}

fn read_binary_vector(bytes: &[u8]) -> Vec<f32> {
	let mut cur = bytes;
	let dimensions = cur.get_i16();
	assert_eq!(cur.get_i16(), 0);
	let elements: Vec<f32> = (0..dimensions).map(|_| cur.get_f32()).collect();
	assert!(cur.is_empty(), "the whole cell is read");
	elements
}

async fn setup(stream: &mut TcpStream) {
	for sql in [
		"CREATE EXTENSION IF NOT EXISTS vector",
		"CREATE TABLE items (id INT PRIMARY KEY, embedding vector(3), label TEXT)",
		"INSERT INTO items (id, embedding, label) VALUES (1, '[1,2,3]', 'a'), (2, '[4,5,6]', 'b'), (3, '[1,0,0]', 'c')",
	] {
		let answer = simple(stream, sql).await;
		assert_eq!(answer.error, None, "{sql}");
	}
}

#[tokio::test]
async fn a_vector_column_is_announced_as_the_vector_type() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;

	let simple_answer = simple(&mut stream, "SELECT embedding FROM items ORDER BY id").await;
	assert_eq!(simple_answer.error, None);
	assert_eq!(simple_answer.columns[0].oid, OID_VECTOR);
	assert_eq!(text_cells(&simple_answer, 0), ["[1,2,3]", "[4,5,6]", "[1,0,0]"]);

	// The type is the same when the statement is described before any value is bound.
	let described =
		described_statement(&mut stream, "SELECT embedding FROM items ORDER BY id").await;
	assert_eq!(described[0].oid, OID_VECTOR);

	// And the cell is the binary form of a vector when the client asks for binary.
	let binary = bound(&mut stream, "SELECT embedding FROM items ORDER BY id", &[], &[], 1).await;
	assert_eq!(binary.error, None);
	assert_eq!(
		binary.columns[0],
		Column {
			oid: OID_VECTOR,
			format: 1
		}
	);
	let cells: Vec<Vec<f32>> = binary
		.rows
		.iter()
		.map(|row| read_binary_vector(row[0].as_ref().expect("a vector")))
		.collect();
	assert_eq!(cells, [vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], vec![1.0, 0.0, 0.0]]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_distance_is_a_double_precision_value() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;

	let answer = simple(
		&mut stream,
		"SELECT id, embedding <-> '[1,2,3]' AS l2, embedding <+> '[1,2,3]' AS l1, \
		 embedding <#> '[1,2,3]' AS ip, l2_distance(embedding, '[4,6,3]') AS l2b \
		 FROM items ORDER BY id",
	)
	.await;
	assert_eq!(answer.error, None);
	let oids: Vec<i32> = answer.columns.iter().map(|column| column.oid).collect();
	assert_eq!(oids, [OID_INT4, OID_FLOAT8, OID_FLOAT8, OID_FLOAT8, OID_FLOAT8]);
	assert_eq!(text_cells(&answer, 1)[0], "0");
	assert_eq!(text_cells(&answer, 2)[1], "9");
	assert_eq!(text_cells(&answer, 3)[0], "-14");
	assert_eq!(text_cells(&answer, 4)[0], "5");
	shutdown.cancel();
}

#[tokio::test]
async fn the_vector_type_is_found_by_name_in_pg_type() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	let answer =
		simple(&mut stream, "SELECT oid, typarray, typname FROM pg_type WHERE typname = 'vector'")
			.await;
	assert_eq!(answer.error, None);
	assert_eq!(text_cells(&answer, 0), [OID_VECTOR.to_string()]);
	assert_eq!(text_cells(&answer, 1), [OID_VECTOR_ARRAY.to_string()]);
	let array = simple(&mut stream, "SELECT typelem FROM pg_type WHERE typname = '_vector'").await;
	assert_eq!(text_cells(&array, 0), [OID_VECTOR.to_string()]);
	shutdown.cancel();
}

#[tokio::test]
async fn client_libraries_find_the_type_the_way_they_look_it_up() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	// pgvector-python for psycopg2.
	let psycopg2 = simple(
		&mut stream,
		"SELECT typname, oid FROM pg_type WHERE oid IN (to_regtype('vector'), to_regtype('_vector'), to_regtype('halfvec'), to_regtype('_halfvec'), to_regtype('sparsevec'), to_regtype('_sparsevec'))",
	)
	.await;
	assert_eq!(psycopg2.error, None);
	assert_eq!(text_cells(&psycopg2, 0), ["vector", "_vector"]);
	assert_eq!(text_cells(&psycopg2, 1), [OID_VECTOR.to_string(), OID_VECTOR_ARRAY.to_string()]);
	// pgvector-node.
	let node = bound(
		&mut stream,
		"SELECT typname, oid FROM pg_type WHERE typname IN ($1, $2, $3)",
		&[],
		&[
			(0, Some(b"vector".to_vec())),
			(0, Some(b"halfvec".to_vec())),
			(0, Some(b"sparsevec".to_vec())),
		],
		0,
	)
	.await;
	assert_eq!(node.error, None);
	assert_eq!(text_cells(&node, 0), ["vector"]);
	// pgvector-python for asyncpg, which names the schema.
	let asyncpg = simple(
		&mut stream,
		"SELECT t.oid FROM pg_type t JOIN pg_namespace ns ON t.typnamespace = ns.oid WHERE t.typname = 'vector' AND ns.nspname = 'public'",
	)
	.await;
	assert_eq!(asyncpg.error, None);
	assert_eq!(text_cells(&asyncpg, 0), [OID_VECTOR.to_string()]);
	// pgvector-python for psycopg 3.
	let psycopg3 = bound(
		&mut stream,
		"SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid",
		&[],
		&[(0, Some(b"vector".to_vec()))],
		0,
	)
	.await;
	assert_eq!(psycopg3.error, None);
	assert_eq!(text_cells(&psycopg3, 0), ["vector"]);
	assert_eq!(text_cells(&psycopg3, 2), [OID_VECTOR_ARRAY.to_string()]);
	assert_eq!(text_cells(&psycopg3, 3), ["vector"]);
	// The name of a type that is bound.
	let bound_name =
		bound(&mut stream, "SELECT to_regtype($1)", &[], &[(0, Some(b"vector".to_vec()))], 0).await;
	assert_eq!(bound_name.error, None);
	assert_eq!(text_cells(&bound_name, 0), [OID_VECTOR.to_string()]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_vector_is_bound_as_text_or_as_binary() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let sql = "SELECT id FROM items ORDER BY embedding <-> $1 LIMIT 2";

	// The server says what the parameter is, so a client that declares nothing is told.
	assert_eq!(parameter_oids(&mut stream, sql).await, [OID_VECTOR]);

	for (label, types, param) in [
		("declared, text", vec![OID_VECTOR], (0, Some(b"[1,0,0]".to_vec()))),
		("declared, binary", vec![OID_VECTOR], (1, Some(binary_vector(&[1.0, 0.0, 0.0])))),
		("not declared, text", vec![], (0, Some(b"[1,0,0]".to_vec()))),
		("not declared, binary", vec![], (1, Some(binary_vector(&[1.0, 0.0, 0.0])))),
	] {
		let answer = bound(&mut stream, sql, &types, &[param], 0).await;
		assert_eq!(answer.error, None, "{label}");
		assert_eq!(text_cells(&answer, 0), ["3", "1"], "{label}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_bound_vector_searches_the_index_and_the_scan_alike() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let scan = "SELECT id FROM items ORDER BY embedding <-> $1 LIMIT 2";
	let limit_bound = "SELECT id FROM items ORDER BY embedding <-> $1 LIMIT $2";
	let query = |text: &str| (0, Some(text.as_bytes().to_vec()));
	let two = (1, Some(2i64.to_be_bytes().to_vec()));

	let before = bound(&mut stream, scan, &[OID_VECTOR], &[query("[1,0,0]")], 0).await;
	assert_eq!(before.error, None);
	assert_eq!(text_cells(&before, 0), ["3", "1"]);

	let created = simple(
		&mut stream,
		"CREATE INDEX items_embedding_idx ON items USING hnsw (embedding vector_l2_ops) WITH (m = 8, ef_construction = 32)",
	)
	.await;
	assert_eq!(created.error, None);

	// The index answers the same search.
	let indexed = bound(&mut stream, scan, &[OID_VECTOR], &[query("[1,0,0]")], 0).await;
	assert_eq!(indexed.error, None);
	assert_eq!(text_cells(&indexed, 0), ["3", "1"]);

	// A limit that is bound is not known when the statement is planned, so it sorts the rows.
	let sorted =
		bound(&mut stream, limit_bound, &[OID_VECTOR, OID_INT8], &[query("[1,0,0]"), two], 0).await;
	assert_eq!(sorted.error, None);
	assert_eq!(text_cells(&sorted, 0), ["3", "1"]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_vector_is_written_as_text_or_as_binary() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let sql = "INSERT INTO items (id, embedding, label) VALUES ($1, $2, 'p')";
	assert_eq!(parameter_oids(&mut stream, sql).await[1], OID_VECTOR);

	let id = |n: i64| (1, Some(n.to_be_bytes().to_vec()));
	let text =
		bound(&mut stream, sql, &[OID_INT8, 0], &[id(10), (0, Some(b"[7,8,9]".to_vec()))], 0).await;
	assert_eq!(text.error, None);
	let binary = bound(
		&mut stream,
		sql,
		&[OID_INT8, OID_VECTOR],
		&[id(11), (1, Some(binary_vector(&[0.5, 1.5, 2.5])))],
		0,
	)
	.await;
	assert_eq!(binary.error, None);

	let rows = simple(&mut stream, "SELECT embedding FROM items WHERE id >= 10 ORDER BY id").await;
	assert_eq!(text_cells(&rows, 0), ["[7,8,9]", "[0.5,1.5,2.5]"]);
	shutdown.cancel();
}

#[tokio::test]
async fn a_malformed_vector_is_refused_with_its_own_code() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let sql = "SELECT id FROM items ORDER BY embedding <-> $1 LIMIT 2";
	for (label, format, value) in [
		("text", 0, b"[1,x,3]".to_vec()),
		("text without brackets", 0, b"1,2,3".to_vec()),
		("text with NaN", 0, b"[NaN,2,3]".to_vec()),
		("binary, short", 1, vec![0, 3, 0, 0, 0]),
		("binary, nonzero header", 1, vec![0, 0, 0, 1]),
	] {
		let answer = bound(&mut stream, sql, &[OID_VECTOR], &[(format, Some(value))], 0).await;
		let error = answer.error.unwrap_or_else(|| panic!("{label} was accepted"));
		assert!(error.contains("22P02"), "{label}: {error}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_vector_of_other_dimensions_is_refused_where_it_is_written_and_compared() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;

	let written =
		simple(&mut stream, "INSERT INTO items (id, embedding) VALUES (9, '[1,2]')").await;
	let error = written.error.expect("two dimensions in a column of three");
	assert!(error.contains("22000"), "{error}");
	assert!(error.contains("expected 3 dimensions, not 2"), "{error}");

	let compared = simple(&mut stream, "SELECT embedding <-> '[1,2]' FROM items").await;
	let error = compared.error.expect("vectors of two and three dimensions");
	assert!(error.contains("22000"), "{error}");
	shutdown.cancel();
}

#[tokio::test]
async fn ddl_over_the_extended_protocol_defines_what_the_simple_protocol_does() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for (sql, tag) in [
		("CREATE EXTENSION IF NOT EXISTS vector", "CREATE EXTENSION"),
		("CREATE TABLE ext_items (id INT PRIMARY KEY, embedding vector(3))", "CREATE TABLE"),
	] {
		let answer = bound(&mut stream, sql, &[], &[], 0).await;
		assert_eq!(answer.error, None, "{sql}");
		assert_eq!(answer.tags, [tag], "{sql}");
	}
	let write = bound(
		&mut stream,
		"INSERT INTO ext_items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[0,1,0]')",
		&[],
		&[],
		0,
	)
	.await;
	assert_eq!(write.error, None);
	// The table is the one of an earlier request, so the index is told its dimensions.
	let index = bound(
		&mut stream,
		"CREATE INDEX ext_items_idx ON ext_items USING hnsw (embedding vector_cosine_ops)",
		&[],
		&[],
		0,
	)
	.await;
	assert_eq!(index.error, None);
	assert_eq!(index.tags, ["CREATE INDEX"]);
	let found = bound(
		&mut stream,
		"SELECT id FROM ext_items ORDER BY embedding <=> $1 LIMIT 1",
		&[],
		&[(0, Some(b"[0,1,0.1]".to_vec()))],
		0,
	)
	.await;
	assert_eq!(found.error, None);
	assert_eq!(text_cells(&found, 0), ["2"]);
	let refused = bound(&mut stream, "CREATE EXTENSION postgis", &[], &[], 0).await;
	assert!(refused.error.expect("no such extension").contains("58P01"));
	shutdown.cancel();
}

#[tokio::test]
async fn a_vector_is_announced_whichever_way_it_is_selected() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	for sql in [
		"SELECT * FROM items ORDER BY id",
		"SELECT embedding FROM items ORDER BY id",
		"SELECT i.embedding FROM items i ORDER BY id",
		"SELECT embedding AS e FROM items ORDER BY id",
		"SELECT '[1,2,3]'::vector",
		"SELECT NULL::vector",
	] {
		let answer = simple(&mut stream, sql).await;
		assert_eq!(answer.error, None, "{sql}");
		let oids: Vec<i32> = answer.columns.iter().map(|column| column.oid).collect();
		assert!(oids.contains(&OID_VECTOR), "{sql}: {oids:?}");
	}
	let returning = simple(
		&mut stream,
		"INSERT INTO items (id, embedding, label) VALUES (7, '[7,7,7]', 'r') RETURNING embedding",
	)
	.await;
	assert_eq!(returning.error, None);
	assert_eq!(returning.columns[0].oid, OID_VECTOR);
	shutdown.cancel();
}

#[tokio::test]
async fn a_request_with_ddl_still_reads_text_written_to_an_existing_vector_column() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let batch = simple(
		&mut stream,
		"CREATE TABLE audit (id INT PRIMARY KEY); INSERT INTO items (id, embedding) VALUES (20, '[2,2,2]')",
	)
	.await;
	assert_eq!(batch.error, None);
	let found = simple(&mut stream, "SELECT embedding FROM items WHERE id = 20").await;
	assert_eq!(text_cells(&found, 0), ["[2,2,2]"]);
	shutdown.cancel();
}

#[tokio::test]
async fn text_selected_into_a_vector_column_is_a_vector_and_a_parameter_has_its_dimensions() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let selected = simple(
		&mut stream,
		"INSERT INTO items (id, embedding) SELECT id + 29 AS id, '[3,3,3]' FROM items WHERE id = 1",
	)
	.await;
	assert_eq!(selected.error, None);
	let found = simple(&mut stream, "SELECT embedding FROM items WHERE id = 30").await;
	assert_eq!(text_cells(&found, 0), ["[3,3,3]"]);

	let sql = "INSERT INTO items (id, embedding) VALUES ($1, $2)";
	let id = (1, Some(31i64.to_be_bytes().to_vec()));
	let short =
		bound(&mut stream, sql, &[OID_INT8, 0], &[id, (0, Some(b"[1,2]".to_vec()))], 0).await;
	assert!(short.error.is_some(), "a vector of two dimensions in a column of three");
	let none = simple(&mut stream, "SELECT id FROM items WHERE id = 31").await;
	assert!(none.rows.is_empty());
	shutdown.cancel();
}

#[tokio::test]
async fn computed_text_is_read_with_the_pgvector_grammar() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	setup(&mut stream).await;
	let write = simple(
		&mut stream,
		"INSERT INTO items (id, embedding) VALUES (40, concat('[+', '1, 2,3.5]')::vector(3))",
	)
	.await;
	assert_eq!(write.error, None);
	let read = simple(&mut stream, "SELECT embedding FROM items WHERE id = 40").await;
	assert_eq!(text_cells(&read, 0), ["[1,2,3.5]"]);
	let select = simple(&mut stream, "SELECT concat('[', '4.5,5]')::vector(2)").await;
	assert_eq!(select.error, None);
	assert_eq!(text_cells(&select, 0), ["[4.5,5]"]);
	let tabbed = simple(&mut stream, "SELECT concat('[1,', E'\\t', '2]')::vector(2)").await;
	assert_eq!(tabbed.error, None);
	assert_eq!(text_cells(&tabbed, 0), ["[1,2]"]);
	for malformed in ["concat('[Na', 'N,2,3]')", "concat('1,', '2,3')", "concat('[1,', '2,x]')"] {
		let refused = simple(&mut stream, &format!("SELECT {malformed}::vector(3)")).await;
		assert!(refused.error.is_some(), "{malformed} was accepted");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn create_extension_answers_with_its_own_tag() {
	let (addr, shutdown) = start().await;
	let mut stream = connect(addr).await;
	for sql in ["CREATE EXTENSION vector", "CREATE EXTENSION IF NOT EXISTS vector"] {
		let tags = simple_tags(&mut stream, sql).await;
		assert_eq!(tags, ["CREATE EXTENSION"], "{sql}");
	}
	let answer = simple(&mut stream, "CREATE EXTENSION postgis").await;
	let error = answer.error.expect("no such extension");
	assert!(error.contains("58P01"), "{error}");
	shutdown.cancel();
}

/// The command tags the server answers a simple query with.
async fn simple_tags(stream: &mut TcpStream, sql: &str) -> Vec<String> {
	let mut body = sql.as_bytes().to_vec();
	body.push(0);
	stream.write_all(&message(b'Q', &body)).await.expect("send the query");
	let mut tags = Vec::new();
	timeout(IO_TIMEOUT, async {
		loop {
			let (kind, payload) = read_message(stream).await;
			match kind {
				b'C' => {
					let end = payload.iter().position(|b| *b == 0).unwrap_or(payload.len());
					tags.push(String::from_utf8_lossy(&payload[..end]).into_owned());
				}
				b'E' => panic!("the statement failed: {}", String::from_utf8_lossy(&payload)),
				b'Z' => break,
				_ => {}
			}
		}
	})
	.await
	.expect("the server answered");
	tags
}
