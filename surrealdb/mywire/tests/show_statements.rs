//! `SHOW` and `DESCRIBE` over the MySQL wire: the columns, types and rows MySQL answers with,
//! what each session may see, and what a statement may not reach.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_mysql::system_vars::{mysql_system_variable_scope, mysql_system_variables};
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use surrealdb_rpc::capabilities::{Capabilities, Targets};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

async fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("Failed to read header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	let mut payload = vec![0u8; len];
	if len > 0 {
		stream.read_exact(&mut payload).await.expect("Failed to read payload");
	}
	(header[3], payload)
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("Failed to write packet header");
	if len > 0 {
		stream.write_all(payload).await.expect("Failed to write packet payload");
	}
}

/// One length-encoded integer at `off`.
fn lenenc_int(buf: &[u8], off: &mut usize) -> usize {
	let first = buf[*off];
	*off += 1;
	match first {
		0xfc => {
			let n = (buf[*off] as usize) | ((buf[*off + 1] as usize) << 8);
			*off += 2;
			n
		}
		0xfd => {
			let n = (buf[*off] as usize)
				| ((buf[*off + 1] as usize) << 8)
				| ((buf[*off + 2] as usize) << 16);
			*off += 3;
			n
		}
		n => n as usize,
	}
}

/// One length-encoded string at `off`.
fn lenenc_str(buf: &[u8], off: &mut usize) -> String {
	let len = lenenc_int(buf, off);
	let text = String::from_utf8_lossy(&buf[*off..*off + len]).into_owned();
	*off += len;
	text
}

/// The cells of a text-protocol row.
fn text_row(payload: &[u8], columns: usize) -> Vec<Option<String>> {
	let mut off = 0;
	(0..columns)
		.map(|_| {
			if payload[off] == 0xfb {
				off += 1;
				None
			} else {
				Some(lenenc_str(payload, &mut off))
			}
		})
		.collect()
}

#[derive(Debug, Clone, PartialEq)]
struct Column {
	name: String,
	ty: u8,
}

/// A text-protocol answer.
#[derive(Debug)]
enum Reply {
	Ok,
	Err {
		code: u16,
		message: String,
	},
	Rows {
		columns: Vec<Column>,
		rows: Vec<Vec<Option<String>>>,
	},
}

impl Reply {
	fn rows(&self) -> (&[Column], &[Vec<Option<String>>]) {
		let Reply::Rows {
			columns,
			rows,
		} = self
		else {
			panic!("expected a result set, got {self:?}");
		};
		(columns, rows)
	}

	/// Every row as its cells, `NULL` as `None`.
	fn cells(&self) -> Vec<Vec<Option<&str>>> {
		self.rows().1.iter().map(|row| row.iter().map(|c| c.as_deref()).collect()).collect()
	}

	/// The first column of every row.
	fn first_column(&self) -> Vec<&str> {
		self.rows().1.iter().map(|row| row[0].as_deref().unwrap_or("<NULL>")).collect()
	}
}

struct Client {
	stream: TcpStream,
	/// The version the server's greeting announced.
	server_version: String,
}

struct Server {
	ds: Arc<Datastore>,
	addr: std::net::SocketAddr,
	shutdown: CancellationToken,
}

impl Server {
	/// A server over a fresh in-memory datastore with the root user `root`.
	async fn start() -> Server {
		let ds = Datastore::new("memory").await.expect("Failed to create datastore");
		let shutdown = CancellationToken::new();
		let addr = surrealdb_mywire::start(
			"127.0.0.1:0".parse().expect("an address"),
			Arc::clone(&ds),
			Arc::new(AtomicBool::new(true)),
			shutdown.clone(),
			None,
			None,
		)
		.await
		.expect("Failed to start mywire server");
		let server = Server {
			ds,
			addr,
			shutdown,
		};
		server.surql("DEFINE USER root ON ROOT PASSWORD 'root' ROLES OWNER;").await;
		server
	}

	/// Run SurrealQL as the owner of `ns`/`db`, panicking when a statement fails.
	async fn surql_in(&self, ns: &str, db: &str, surql: &str) {
		let definitions = format!(
			"DEFINE NAMESPACE IF NOT EXISTS `{ns}`; USE NS `{ns}`; DEFINE DATABASE IF NOT EXISTS `{db}`;"
		);
		for response in self
			.ds
			.run(QueryRequest::new(definitions.as_str(), &Session::owner()))
			.await
			.expect("query was refused")
		{
			response.result.expect("defining the database failed");
		}
		let session = Session::owner().with_ns(ns).with_db(db);
		let responses =
			self.ds.run(QueryRequest::new(surql, &session)).await.expect("query was refused");
		for response in responses {
			response.result.unwrap_or_else(|e| panic!("`{surql}` failed: {e}"));
		}
	}

	async fn surql(&self, surql: &str) {
		self.surql_in("test", "test", surql).await;
	}

	/// Connect as `user` to `database` (`ns/db`).
	async fn connect_as(&self, user: &str, password: &str, database: &str) -> Client {
		Client::connect(self.addr, user, password, Some(database)).await
	}

	/// Connect as the root user without choosing a database.
	async fn connect_without_database(&self) -> Client {
		Client::connect(self.addr, "root", "root", None).await
	}

	async fn connect(&self) -> Client {
		self.connect_as("root", "root", "test/test").await
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		self.shutdown.cancel();
	}
}

impl Client {
	async fn connect(
		addr: std::net::SocketAddr,
		user: &str,
		password: &str,
		database: Option<&str>,
	) -> Client {
		let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
		let (_, payload) = read_packet(&mut stream).await;
		let mut pos = 1;
		while payload[pos] != 0 {
			pos += 1;
		}
		let server_version = String::from_utf8_lossy(&payload[1..pos]).into_owned();
		pos += 1 + 4;
		let mut salt = [0u8; 20];
		salt[..8].copy_from_slice(&payload[pos..pos + 8]);
		pos += 9 + 2 + 1 + 2 + 2 + 1 + 10;
		salt[8..20].copy_from_slice(&payload[pos..pos + 12]);

		let scramble = compute_mysql_native_password_scramble(password, &salt);
		let mut resp = Vec::new();
		let caps: u32 = CLIENT_PROTOCOL_41
			| CLIENT_SECURE_CONNECTION
			| CLIENT_PLUGIN_AUTH
			| if database.is_some() {
				CLIENT_CONNECT_WITH_DB
			} else {
				0
			};
		resp.extend_from_slice(&caps.to_le_bytes());
		resp.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
		resp.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
		resp.extend_from_slice(&[0u8; 23]);
		resp.extend_from_slice(user.as_bytes());
		resp.push(0);
		resp.push(scramble.len() as u8);
		resp.extend_from_slice(&scramble);
		if let Some(database) = database {
			resp.extend_from_slice(database.as_bytes());
			resp.push(0);
		}
		resp.extend_from_slice(b"mysql_native_password\0");
		write_packet(&mut stream, 1, &resp).await;

		let (seq, payload) = read_packet(&mut stream).await;
		let payload = if payload[0] == 0xfe {
			let mut clear = password.as_bytes().to_vec();
			clear.push(0);
			write_packet(&mut stream, seq.wrapping_add(1), &clear).await;
			read_packet(&mut stream).await.1
		} else {
			payload
		};
		assert_eq!(
			payload[0],
			0x00,
			"handshake failed: {}",
			String::from_utf8_lossy(&payload[3..])
		);
		Client {
			stream,
			server_version,
		}
	}

	async fn query(&mut self, sql: &str) -> Reply {
		let mut q = vec![COM_QUERY];
		q.extend_from_slice(sql.as_bytes());
		write_packet(&mut self.stream, 0, &q).await;
		self.read_reply().await
	}

	async fn read_reply(&mut self) -> Reply {
		let (_, first) = read_packet(&mut self.stream).await;
		match first[0] {
			0x00 => return Reply::Ok,
			0xff => {
				return Reply::Err {
					code: (first[1] as u16) | ((first[2] as u16) << 8),
					message: String::from_utf8_lossy(&first[9..]).into_owned(),
				};
			}
			_ => {}
		}
		let mut off = 0;
		let count = lenenc_int(&first, &mut off);
		let mut columns = Vec::new();
		for _ in 0..count {
			let (_, def) = read_packet(&mut self.stream).await;
			let mut off = 0;
			for _ in 0..4 {
				lenenc_str(&def, &mut off);
			}
			let name = lenenc_str(&def, &mut off);
			lenenc_str(&def, &mut off);
			off += 1 + 2 + 4;
			columns.push(Column {
				name,
				ty: def[off],
			});
		}
		let (_, eof) = read_packet(&mut self.stream).await;
		assert_eq!(eof[0], 0xfe, "expected EOF after the column definitions");
		let mut rows = Vec::new();
		loop {
			let (_, pkt) = read_packet(&mut self.stream).await;
			if pkt[0] == 0xfe && pkt.len() < 9 {
				break;
			}
			rows.push(text_row(&pkt, count));
		}
		Reply::Rows {
			columns,
			rows,
		}
	}
}

impl Client {
	/// Prepare `sql`, execute it once with no parameters and decode the binary rows.
	async fn prepared(&mut self, sql: &str) -> Reply {
		let mut prepare = vec![COM_STMT_PREPARE];
		prepare.extend_from_slice(sql.as_bytes());
		write_packet(&mut self.stream, 0, &prepare).await;
		let (_, ok) = read_packet(&mut self.stream).await;
		if ok[0] == 0xff {
			return Reply::Err {
				code: (ok[1] as u16) | ((ok[2] as u16) << 8),
				message: String::from_utf8_lossy(&ok[9..]).into_owned(),
			};
		}
		assert_eq!(ok[0], 0x00, "COM_STMT_PREPARE answered {ok:?}");
		let statement_id = u32::from_le_bytes([ok[1], ok[2], ok[3], ok[4]]);
		let described = u16::from_le_bytes([ok[5], ok[6]]);
		let params = u16::from_le_bytes([ok[7], ok[8]]);
		assert_eq!(params, 0, "a SHOW statement has no parameters");
		if described > 0 {
			for _ in 0..described {
				read_packet(&mut self.stream).await;
			}
			let (_, eof) = read_packet(&mut self.stream).await;
			assert_eq!(eof[0], 0xfe);
		}

		let mut execute = vec![COM_STMT_EXECUTE];
		execute.extend_from_slice(&statement_id.to_le_bytes());
		execute.push(CURSOR_TYPE_NO_CURSOR);
		execute.extend_from_slice(&1u32.to_le_bytes());
		write_packet(&mut self.stream, 0, &execute).await;

		let (_, first) = read_packet(&mut self.stream).await;
		if first[0] == 0xff {
			return Reply::Err {
				code: (first[1] as u16) | ((first[2] as u16) << 8),
				message: String::from_utf8_lossy(&first[9..]).into_owned(),
			};
		}
		let count = first[0] as usize;
		let mut columns = Vec::new();
		for _ in 0..count {
			let (_, def) = read_packet(&mut self.stream).await;
			let mut off = 0;
			for _ in 0..4 {
				lenenc_str(&def, &mut off);
			}
			let name = lenenc_str(&def, &mut off);
			lenenc_str(&def, &mut off);
			off += 1 + 2 + 4;
			columns.push(Column {
				name,
				ty: def[off],
			});
		}
		let (_, eof) = read_packet(&mut self.stream).await;
		assert_eq!(eof[0], 0xfe, "expected EOF after the column definitions");
		let mut rows = Vec::new();
		loop {
			let (_, pkt) = read_packet(&mut self.stream).await;
			if pkt[0] == 0xfe && pkt.len() < 9 {
				break;
			}
			rows.push(binary_row(&pkt, &columns));
		}
		Reply::Rows {
			columns,
			rows,
		}
	}
}

/// The cells of a binary-protocol row of strings and integers.
fn binary_row(payload: &[u8], columns: &[Column]) -> Vec<Option<String>> {
	assert_eq!(payload[0], 0x00, "a binary row starts with 0x00");
	let bitmap = (columns.len() + 7 + 2) / 8;
	let mut off = 1 + bitmap;
	columns
		.iter()
		.enumerate()
		.map(|(i, column)| {
			if payload[1 + (i + 2) / 8] & (1 << ((i + 2) % 8)) != 0 {
				return None;
			}
			Some(match column.ty {
				MYSQL_TYPE_VAR_STRING => lenenc_str(payload, &mut off),
				MYSQL_TYPE_LONG => {
					let n =
						i32::from_le_bytes(payload[off..off + 4].try_into().expect("four bytes"));
					off += 4;
					n.to_string()
				}
				MYSQL_TYPE_LONGLONG => {
					let n =
						i64::from_le_bytes(payload[off..off + 8].try_into().expect("eight bytes"));
					off += 8;
					n.to_string()
				}
				other => panic!("column `{}` has the unexpected type {other}", column.name),
			})
		})
		.collect()
}

/// The names and types of a result's columns.
fn layout(reply: &Reply) -> Vec<(&str, u8)> {
	reply.rows().0.iter().map(|c| (c.name.as_str(), c.ty)).collect()
}

fn assert_refused(reply: &Reply, expected: &str) {
	let Reply::Err {
		code,
		message,
	} = reply
	else {
		panic!("expected the error `{expected}`, got {reply:?}");
	};
	assert!(message.contains(expected), "error {code} `{message}` does not mention `{expected}`");
}

const SHOP: &str = "
	DEFINE TABLE accounts SCHEMAFULL;
	DEFINE FIELD id ON accounts TYPE int;
	DEFINE FIELD email ON accounts TYPE string;
	DEFINE FIELD note ON accounts TYPE none | null | string;
	DEFINE FIELD balance ON accounts TYPE decimal DEFAULT 0dec;
	DEFINE INDEX accounts_id_pk ON accounts FIELDS id UNIQUE;
	DEFINE INDEX accounts_email_uniq ON accounts FIELDS email UNIQUE;
	DEFINE INDEX accounts_note ON accounts FIELDS note, balance;
	DEFINE TABLE ledger SCHEMAFULL;
	DEFINE FIELD n ON ledger TYPE int;
	DEFINE FIELD amount ON ledger TYPE int;
";

/// A server whose `test` namespace has a `test` database holding the `SHOP` tables, and a second
/// database and a second namespace the session in `test/test` has no business seeing.
async fn shop() -> Server {
	let server = Server::start().await;
	server.surql(SHOP).await;
	server.surql_in("test", "archive", "DEFINE TABLE old_orders SCHEMAFULL;").await;
	server
		.surql_in(
			"elsewhere",
			"hidden",
			"DEFINE TABLE secret_plans SCHEMAFULL;
			 DEFINE FIELD codename ON secret_plans TYPE string;",
		)
		.await;
	server
}

const TEXT: u8 = MYSQL_TYPE_VAR_STRING;
const LONG: u8 = MYSQL_TYPE_LONG;
const LONGLONG: u8 = MYSQL_TYPE_LONGLONG;

#[tokio::test]
async fn show_tables_lists_the_tables_of_the_sessions_database() {
	let server = shop().await;
	let mut c = server.connect().await;

	let reply = c.query("SHOW TABLES").await;
	assert_eq!(layout(&reply), [("Tables_in_test", TEXT)]);
	assert_eq!(reply.first_column(), ["accounts", "ledger"]);

	let reply = c.query("SHOW FULL TABLES").await;
	assert_eq!(layout(&reply), [("Tables_in_test", TEXT), ("Table_type", TEXT)]);
	assert_eq!(
		reply.cells(),
		[[Some("accounts"), Some("BASE TABLE")], [Some("ledger"), Some("BASE TABLE")]]
	);

	// The session's own database may be named, with FROM or IN, in any case.
	for sql in ["SHOW TABLES FROM test", "SHOW TABLES IN test", "SHOW TABLES FROM TEST"] {
		assert_eq!(c.query(sql).await.first_column(), ["accounts", "ledger"], "{sql}");
	}

	// LIKE matches table names as written, so case matters.
	assert_eq!(c.query("SHOW TABLES LIKE 'acc%'").await.first_column(), ["accounts"]);
	assert_eq!(c.query("SHOW TABLES LIKE 'l_dger'").await.first_column(), ["ledger"]);
	assert!(c.query("SHOW TABLES LIKE 'ACC%'").await.first_column().is_empty());
}

#[tokio::test]
async fn a_brace_in_a_like_pattern_stands_for_itself() {
	let server = Server::start().await;
	server
		.surql(
			"DEFINE TABLE `a`; DEFINE TABLE `aa`; DEFINE TABLE `a{1}`; \
			 DEFINE TABLE `a{2,3}`; DEFINE TABLE `a{`; DEFINE TABLE `}`;",
		)
		.await;
	let mut c = server.connect().await;

	// A pattern that reads as a repetition or as an unbalanced brace to a regular expression
	// still names exactly one table, in a plain and in a prepared statement.
	for (like, table) in [("a{1}", "a{1}"), ("a{2,3}", "a{2,3}"), ("a{", "a{"), ("}", "}")] {
		let sql = format!("SHOW TABLES LIKE '{like}'");
		assert_eq!(c.query(&sql).await.first_column(), [table], "{sql}");
		assert_eq!(c.prepared(&sql).await.first_column(), [table], "{sql}");
	}
}

#[tokio::test]
async fn show_databases_lists_the_databases_of_the_sessions_namespace_only() {
	let server = shop().await;
	let mut c = server.connect().await;

	let reply = c.query("SHOW DATABASES").await;
	assert_eq!(layout(&reply), [("Database", TEXT)]);
	assert_eq!(reply.first_column(), ["archive", "test"]);
	assert_eq!(c.query("SHOW SCHEMAS").await.first_column(), ["archive", "test"]);
	assert_eq!(c.query("SHOW DATABASES LIKE 'te%'").await.first_column(), ["test"]);

	// Neither the other namespace nor anything it holds shows up in any listing.
	for sql in ["SHOW DATABASES", "SHOW TABLES", "SHOW DATABASES LIKE '%'"] {
		let reply = c.query(sql).await;
		for name in reply.first_column() {
			assert!(!["hidden", "elsewhere", "secret_plans"].contains(&name), "{sql} lists {name}");
		}
	}
}

#[tokio::test]
async fn show_columns_describes_each_field_as_mysql_does() {
	let server = shop().await;
	let mut c = server.connect().await;

	let columns = [
		("Field", TEXT),
		("Type", TEXT),
		("Null", TEXT),
		("Key", TEXT),
		("Default", TEXT),
		("Extra", TEXT),
	];
	let expected = [
		[Some("balance"), Some("decimal(65,30)"), Some("NO"), Some(""), Some("0"), Some("")],
		[Some("email"), Some("text"), Some("NO"), Some("UNI"), None, Some("")],
		[Some("id"), Some("bigint"), Some("NO"), Some("PRI"), None, Some("")],
		[Some("note"), Some("text"), Some("YES"), Some("MUL"), None, Some("")],
	];
	// The same statement under each of its spellings.
	for sql in [
		"SHOW COLUMNS FROM accounts",
		"SHOW FIELDS FROM accounts",
		"SHOW COLUMNS IN accounts",
		"SHOW COLUMNS FROM accounts FROM test",
		"SHOW COLUMNS FROM test.accounts",
		"DESCRIBE accounts",
		"DESC accounts",
		"EXPLAIN accounts",
	] {
		let reply = c.query(sql).await;
		assert_eq!(layout(&reply), columns, "{sql}");
		assert_eq!(reply.cells(), expected, "{sql}");
	}

	// A column name or a LIKE pattern narrows the rows; columns match without regard to case.
	assert_eq!(c.query("SHOW COLUMNS FROM accounts LIKE 'E%'").await.first_column(), ["email"]);
	assert_eq!(c.query("DESCRIBE accounts email").await.first_column(), ["email"]);

	let reply = c.query("SHOW FULL COLUMNS FROM ledger").await;
	assert_eq!(
		layout(&reply),
		[
			("Field", TEXT),
			("Type", TEXT),
			("Collation", TEXT),
			("Null", TEXT),
			("Key", TEXT),
			("Default", TEXT),
			("Extra", TEXT),
			("Privileges", TEXT),
			("Comment", TEXT),
		]
	);
	assert_eq!(
		reply.cells()[0],
		[
			Some("amount"),
			Some("bigint"),
			None,
			Some("NO"),
			Some(""),
			None,
			Some(""),
			Some("select,insert,update,references"),
			Some(""),
		]
	);
	assert_eq!(reply.first_column(), ["amount", "n"]);
}

#[tokio::test]
async fn show_index_lists_a_row_per_indexed_column_with_mysqls_types() {
	let server = shop().await;
	let mut c = server.connect().await;

	let reply = c.query("SHOW INDEX FROM accounts").await;
	assert_eq!(
		layout(&reply),
		[
			("Table", TEXT),
			("Non_unique", LONG),
			("Key_name", TEXT),
			("Seq_in_index", LONG),
			("Column_name", TEXT),
			("Collation", TEXT),
			("Cardinality", LONGLONG),
			("Sub_part", LONGLONG),
			("Packed", TEXT),
			("Null", TEXT),
			("Index_type", TEXT),
			("Comment", TEXT),
			("Index_comment", TEXT),
			("Visible", TEXT),
			("Expression", TEXT),
		]
	);
	let summary: Vec<_> =
		reply.cells().into_iter().map(|row| (row[1], row[2], row[3], row[4])).collect();
	assert_eq!(
		summary,
		[
			(Some("0"), Some("PRIMARY"), Some("1"), Some("id")),
			(Some("0"), Some("accounts_email_uniq"), Some("1"), Some("email")),
			(Some("1"), Some("accounts_note"), Some("1"), Some("note")),
			(Some("1"), Some("accounts_note"), Some("2"), Some("balance")),
		]
	);
	for spelling in ["SHOW INDEXES FROM accounts", "SHOW KEYS FROM accounts"] {
		assert_eq!(c.query(spelling).await.cells(), reply.cells(), "{spelling}");
	}
	assert!(c.query("SHOW INDEX FROM ledger").await.cells().is_empty());
}

#[tokio::test]
async fn show_create_table_answers_with_the_tables_definition() {
	let server = shop().await;
	let mut c = server.connect().await;

	let reply = c.query("SHOW CREATE TABLE ledger").await;
	assert_eq!(layout(&reply), [("Table", TEXT), ("Create Table", TEXT)]);
	let rows = reply.cells();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0][0], Some("ledger"));
	let definition = rows[0][1].expect("a definition");
	assert!(definition.contains("DEFINE TABLE ledger"), "{definition}");
	assert!(definition.contains("DEFINE FIELD n ON ledger TYPE int"), "{definition}");
	assert!(definition.contains("DEFINE FIELD amount ON ledger TYPE int"), "{definition}");
}

#[tokio::test]
async fn a_table_that_does_not_exist_is_an_error() {
	let server = shop().await;
	let mut c = server.connect().await;

	for sql in [
		"SHOW COLUMNS FROM nosuch",
		"DESCRIBE nosuch",
		"SHOW INDEX FROM nosuch",
		"SHOW CREATE TABLE nosuch",
	] {
		assert_refused(&c.query(sql).await, "Table 'nosuch' doesn't exist");
	}
	// The failure leaves the connection usable.
	assert_eq!(c.query("SHOW TABLES").await.first_column(), ["accounts", "ledger"]);
}

#[tokio::test]
async fn a_statement_with_nothing_to_report_still_answers_with_its_columns() {
	let server = shop().await;
	let mut c = server.connect().await;

	for (sql, columns) in [
		("SHOW TABLES LIKE 'nomatch'", vec![("Tables_in_test", TEXT)]),
		("SHOW DATABASES LIKE 'nomatch'", vec![("Database", TEXT)]),
		(
			"SHOW COLUMNS FROM ledger LIKE 'nomatch'",
			vec![
				("Field", TEXT),
				("Type", TEXT),
				("Null", TEXT),
				("Key", TEXT),
				("Default", TEXT),
				("Extra", TEXT),
			],
		),
		("SHOW STATUS", vec![("Variable_name", TEXT), ("Value", TEXT)]),
		("SHOW WARNINGS", vec![("Level", TEXT), ("Code", LONG), ("Message", TEXT)]),
		("SHOW ERRORS", vec![("Level", TEXT), ("Code", LONG), ("Message", TEXT)]),
		(
			"SHOW PROCESSLIST",
			vec![
				("Id", LONGLONG),
				("User", TEXT),
				("Host", TEXT),
				("db", TEXT),
				("Command", TEXT),
				("Time", LONG),
				("State", TEXT),
				("Info", TEXT),
			],
		),
	] {
		let reply = c.query(sql).await;
		assert_eq!(layout(&reply), columns, "{sql}");
		assert!(reply.cells().is_empty(), "{sql}");
	}
}

#[tokio::test]
async fn show_variables_agrees_with_the_server_and_filters_with_like() {
	let server = shop().await;
	let mut c = server.connect().await;

	let reply = c.query("SHOW VARIABLES").await;
	assert_eq!(layout(&reply), [("Variable_name", TEXT), ("Value", TEXT)]);
	assert!(reply.cells().iter().flatten().all(Option::is_some));
	let value = |reply: &Reply, name: &str| -> Option<String> {
		reply
			.cells()
			.iter()
			.find(|row| row[0] == Some(name))
			.and_then(|row| row[1].map(str::to_owned))
	};

	// The rows are the table of system variables: every variable in ascending order of name, as
	// MySQL lists its value.
	let table: Vec<(String, String)> = mysql_system_variables()
		.iter()
		.map(|(name, variable)| ((*name).to_string(), variable.listed()))
		.collect();
	let listed: Vec<(String, String)> = reply
		.cells()
		.iter()
		.map(|row| match (row[0], row[1]) {
			(Some(name), Some(value)) => (name.to_string(), value.to_string()),
			other => panic!("a variable without a name or a value: {other:?}"),
		})
		.collect();
	assert_eq!(listed, table);
	assert!(listed.windows(2).all(|pair| pair[0].0 < pair[1].0), "not in name order");
	for (name, expected) in [
		("time_zone", "SYSTEM"),
		("collation_server", "utf8mb4_0900_ai_ci"),
		("collation_connection", "utf8mb4_0900_ai_ci"),
		("max_allowed_packet", "67108864"),
	] {
		assert_eq!(value(&reply, name).as_deref(), Some(expected), "{name}");
	}
	// The version is the one the greeting and `SELECT VERSION()` announce.
	assert_eq!(value(&reply, "version"), Some(c.server_version.clone()));
	assert_eq!(
		c.query("SELECT VERSION()").await.cells()[0][0].map(str::to_owned),
		value(&reply, "version")
	);
	// Every variable that `SELECT @@name` also answers carries the same value.
	for name in ["sql_mode", "version_comment", "max_allowed_packet", "character_set_client"] {
		let selected = c.query(&format!("SELECT @@{name}")).await;
		assert_eq!(
			selected.cells()[0][0].map(str::to_owned),
			value(&reply, name),
			"SELECT @@{name} and SHOW VARIABLES disagree"
		);
	}

	assert_eq!(
		c.query("SHOW VARIABLES LIKE 'version%'").await.first_column(),
		["version", "version_comment"]
	);
	// Variable names match without regard to case, as in MySQL.
	assert_eq!(c.query("SHOW GLOBAL VARIABLES LIKE 'SQL_MODE'").await.first_column(), ["sql_mode"]);
	assert!(c.query("SHOW VARIABLES LIKE 'no_such_variable'").await.first_column().is_empty());
}

#[tokio::test]
async fn show_variables_lists_variables_mysql_has_with_the_values_mysql_lists() {
	let server = shop().await;
	let mut c = server.connect().await;

	// Every listed variable is one that MySQL 8 has.
	let reply = c.query("SHOW VARIABLES").await;
	for name in reply.first_column() {
		assert!(mysql_system_variable_scope(name).is_some(), "{name} is not a variable of MySQL");
	}

	// A switch is listed as ON or OFF and a number as its text.
	for (name, value) in [
		("autocommit", "ON"),
		("session_track_schema", "ON"),
		("sql_auto_is_null", "OFF"),
		("transaction_read_only", "OFF"),
		("max_allowed_packet", "67108864"),
		("lower_case_table_names", "0"),
		("default_storage_engine", "InnoDB"),
		("character_set_system", "utf8mb3"),
	] {
		let reply = c.query(&format!("SHOW VARIABLES LIKE '{name}'")).await;
		assert_eq!(reply.cells(), [[Some(name), Some(value)]], "{name}");
	}
	assert_eq!(
		c.query("SHOW VARIABLES LIKE 'character_set%'").await.first_column(),
		[
			"character_set_client",
			"character_set_connection",
			"character_set_database",
			"character_set_filesystem",
			"character_set_results",
			"character_set_server",
			"character_set_system",
		]
	);

	// The query cache and the old names of the isolation level and the read-only flag went in
	// MySQL 8.0.3, so MySQL lists none of them.
	for like in ["tx_isolation", "tx_read_only", "query_cache%", "no_such_variable"] {
		assert!(c.query(&format!("SHOW VARIABLES LIKE '{like}'")).await.first_column().is_empty());
	}
}

#[tokio::test]
async fn show_engines_collation_and_character_set_list_what_the_server_supports() {
	let server = shop().await;
	let mut c = server.connect().await;

	let engines = c.query("SHOW ENGINES").await;
	assert_eq!(engines.first_column(), ["InnoDB"]);
	// Every cell of these fixed rows holds a value: a column the rows do not carry would be NULL.
	assert!(engines.cells().iter().flatten().all(Option::is_some));
	let reply = c.query("SHOW COLLATION").await;
	assert_eq!(
		layout(&reply),
		[
			("Collation", TEXT),
			("Charset", TEXT),
			("Id", LONGLONG),
			("Default", TEXT),
			("Compiled", TEXT),
			("Sortlen", LONG),
			("Pad_attribute", TEXT),
		]
	);
	assert_eq!(reply.first_column(), ["utf8mb4_0900_ai_ci"]);
	assert!(reply.cells().iter().flatten().all(Option::is_some));
	let reply = c.query("SHOW CHARACTER SET").await;
	assert_eq!(
		layout(&reply),
		[("Charset", TEXT), ("Description", TEXT), ("Default collation", TEXT), ("Maxlen", LONG)]
	);
	assert_eq!(reply.first_column(), ["utf8mb4"]);
	assert_eq!(c.query("SHOW CHARSET").await.cells(), reply.cells());
	assert!(reply.cells().iter().flatten().all(Option::is_some));
}

#[tokio::test]
async fn prepared_show_statements_answer_with_the_same_columns_and_rows() {
	let server = shop().await;
	let mut c = server.connect().await;

	for sql in [
		"SHOW TABLES",
		"SHOW FULL TABLES",
		"SHOW DATABASES",
		"SHOW COLUMNS FROM accounts",
		"SHOW FULL COLUMNS FROM accounts",
		"SHOW INDEX FROM accounts",
		"SHOW CREATE TABLE ledger",
		"SHOW VARIABLES LIKE 'version%'",
		"SHOW WARNINGS",
		"DESCRIBE ledger",
	] {
		let text = c.query(sql).await;
		let binary = c.prepared(sql).await;
		assert_eq!(layout(&binary), layout(&text), "{sql}");
		assert_eq!(binary.cells(), text.cells(), "{sql}");
	}
	// The statements that refuse to run refuse the same way.
	let reply = c.prepared("SHOW TABLES FROM archive").await;
	assert_refused(&reply, "cross-database queries not supported");
	let reply = c.prepared("SHOW COLUMNS FROM nosuch").await;
	assert_refused(&reply, "Table 'nosuch' doesn't exist");
}

#[tokio::test]
async fn a_name_from_another_database_is_refused_everywhere() {
	let server = shop().await;
	let mut c = server.connect().await;

	// Another database of the same namespace.
	for sql in [
		"SHOW TABLES FROM archive",
		"SHOW TABLES IN archive",
		"SHOW COLUMNS FROM old_orders FROM archive",
		"SHOW COLUMNS FROM archive.old_orders",
		"DESCRIBE archive.old_orders",
		"SHOW INDEX FROM archive.old_orders",
		"SHOW CREATE TABLE archive.old_orders",
		// Another namespace's database cannot be reached by naming it either.
		"SHOW TABLES FROM hidden",
		"SHOW COLUMNS FROM hidden.secret_plans",
		"DESCRIBE hidden.secret_plans",
	] {
		assert_refused(&c.query(sql).await, "cross-database queries not supported");
		assert_refused(&c.prepared(sql).await, "cross-database queries not supported");
	}
	// The error names what was asked for and nothing it did not.
	let Reply::Err {
		message,
		..
	} = c.query("SHOW COLUMNS FROM hidden.secret_plans").await
	else {
		panic!("expected an error");
	};
	assert!(!message.contains("codename"), "{message}");
	assert!(!message.contains("elsewhere"), "{message}");

	// Naming the session's own database is no refusal.
	assert_eq!(c.query("SHOW COLUMNS FROM test.ledger").await.first_column(), ["amount", "n"]);
	// Switching database changes which one counts as the session's own.
	assert!(matches!(c.query("USE archive").await, Reply::Ok));
	assert_eq!(c.query("SHOW TABLES").await.first_column(), ["old_orders"]);
	assert_refused(&c.query("SHOW TABLES FROM test").await, "cross-database queries not supported");
}

#[tokio::test]
async fn a_name_cannot_close_its_quotes_and_run_a_statement() {
	let server = shop().await;
	let mut c = server.connect().await;

	for sql in [
		"SHOW COLUMNS FROM `ledger``'; REMOVE TABLE accounts; `",
		"DESCRIBE `x``'; REMOVE TABLE accounts; `",
		"SHOW INDEX FROM `x``'; REMOVE TABLE accounts; `",
		"SHOW CREATE TABLE `x``'; REMOVE TABLE accounts; `",
		"SHOW COLUMNS FROM ledger FROM `x``'; REMOVE TABLE accounts; `",
		"SHOW TABLES FROM `x``'; REMOVE TABLE accounts; `",
		"SHOW TABLES LIKE 'x''; REMOVE TABLE accounts; '",
		"SHOW COLUMNS FROM ledger LIKE 'x''; REMOVE TABLE accounts; '",
		"SHOW COLUMNS FROM ledger LIKE '\\\\''; REMOVE TABLE accounts; '",
		"SHOW VARIABLES LIKE 'x''; REMOVE TABLE accounts; '",
		"SHOW DATABASES LIKE 'x''; DEFINE USER intruder ON ROOT PASSWORD 'p' ROLES OWNER; '",
	] {
		// Whether the statement is refused or answers, it does nothing else.
		let _ = c.query(sql).await;
		assert_eq!(c.query("SHOW TABLES").await.first_column(), ["accounts", "ledger"], "{sql}");
	}

	// The same through the prepared protocol.
	let _ = c.prepared("SHOW COLUMNS FROM `ledger``'; REMOVE TABLE accounts; `").await;
	assert_eq!(c.query("SHOW TABLES").await.first_column(), ["accounts", "ledger"]);

	// No user was defined either.
	let session = Session::owner();
	let responses =
		server.ds.run(QueryRequest::new("INFO FOR ROOT", &session)).await.expect("INFO FOR ROOT");
	for response in responses {
		use surrealdb_types::ToSql;
		let info = response.result.expect("INFO FOR ROOT answered").to_sql();
		assert!(!info.contains("intruder"), "{info}");
	}
}

#[tokio::test]
async fn a_database_user_sees_its_own_database_and_nothing_wider() {
	let server = shop().await;
	server.surql("DEFINE USER reader ON DATABASE PASSWORD 'reader' ROLES VIEWER;").await;
	let mut c = server.connect_as("reader", "reader", "test/test").await;

	// Its own database is open to it.
	assert_eq!(c.query("SHOW TABLES").await.first_column(), ["accounts", "ledger"]);
	assert_eq!(c.query("SHOW COLUMNS FROM ledger").await.first_column(), ["amount", "n"]);
	assert_eq!(c.query("SHOW INDEX FROM accounts").await.rows().1.len(), 4);
	assert!(c.query("SHOW CREATE TABLE ledger").await.cells()[0][1].is_some());

	// A listing of the namespace's databases is not: the user may view one database, not the
	// namespace, and no part of the listing is given.
	assert_refused(&c.query("SHOW DATABASES").await, "Not enough permissions");
	// Another database, in this namespace or another, is out of reach by name.
	for sql in ["SHOW TABLES FROM archive", "SHOW COLUMNS FROM hidden.secret_plans"] {
		assert_refused(&c.query(sql).await, "cross-database queries not supported");
	}
}

#[tokio::test]
async fn a_session_sees_the_databases_of_its_own_namespace_only() {
	let server = shop().await;

	// A session in the other namespace lists that namespace's databases and tables, and the
	// database `test` of this namespace is another database to it.
	let mut other = server.connect_as("root", "root", "elsewhere/hidden").await;
	assert_eq!(other.query("SHOW DATABASES").await.first_column(), ["hidden"]);
	assert_eq!(other.query("SHOW TABLES").await.first_column(), ["secret_plans"]);
	assert_eq!(other.query("SHOW COLUMNS FROM secret_plans").await.first_column(), ["codename"]);
	for sql in
		["SHOW TABLES FROM test", "SHOW COLUMNS FROM test.accounts", "DESCRIBE archive.old_orders"]
	{
		assert_refused(&other.query(sql).await, "cross-database queries not supported");
	}
	assert_refused(
		&other.query("SHOW COLUMNS FROM accounts").await,
		"Table 'accounts' doesn't exist",
	);
}

#[tokio::test]
async fn without_a_database_selected_a_statement_is_refused() {
	let server = shop().await;
	let mut c = server.connect_without_database().await;

	// No namespace or database is chosen, so there is nothing for these statements to read.
	for sql in ["SHOW DATABASES", "SHOW TABLES", "SHOW COLUMNS FROM accounts", "DESCRIBE accounts"]
	{
		let reply = c.query(sql).await;
		assert!(matches!(reply, Reply::Err { .. }), "{sql} answered {reply:?}");
	}
	// Once a database is chosen they answer.
	assert!(matches!(c.query("USE test").await, Reply::Ok));
	assert_eq!(c.query("SHOW TABLES").await.first_column(), ["accounts", "ledger"]);
}

#[tokio::test]
async fn an_instance_that_denies_functions_refuses_the_statements_that_call_them() {
	let server = shop().await;
	server
		.ds
		.set_capabilities(Capabilities::default().without_functions(Targets::All))
		.expect("capabilities");
	let mut c = server.connect().await;

	// The catalog is read through functions, so with none allowed no listing is given.
	for sql in [
		"SHOW DATABASES",
		"SHOW TABLES",
		"SHOW COLUMNS FROM ledger",
		"SHOW INDEX FROM accounts",
		"SHOW CREATE TABLE ledger",
	] {
		let reply = c.query(sql).await;

		assert!(matches!(reply, Reply::Err { .. }), "{sql} answered {reply:?}");
	}
}
