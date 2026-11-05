//! `LIKE` over the MySQL wire when the client binds the pattern (`LIKE ?`), which is the form
//! every driver sends, and when the statement carries it as a literal.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

type Cell = Option<String>;

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

/// Bound on every network exchange, so a server that stops answering fails the test instead of
/// hanging it.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

async fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
	timeout(IO_TIMEOUT, async {
		let mut header = [0u8; 4];
		stream.read_exact(&mut header).await.expect("packet header");
		let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
		let mut payload = vec![0u8; len];
		stream.read_exact(&mut payload).await.expect("packet payload");
		payload
	})
	.await
	.expect("the server answered")
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
	let len = payload.len();
	let header = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];
	stream.write_all(&header).await.expect("packet header");
	stream.write_all(payload).await.expect("packet payload");
}

/// Connects as `root` to `test/test` with the end-of-file packets after column definitions
/// dropped, as current clients ask for.
async fn connect(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let greeting = read_packet(&mut stream).await;
	// The salt: 8 bytes after the connection id, 12 more after the fixed fields that follow.
	let version_end = greeting.iter().position(|b| *b == 0).expect("server version");
	let mut at = version_end + 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&greeting[at..at + 8]);
	at += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..].copy_from_slice(&greeting[at..at + 12]);

	let mut response = Vec::new();
	let caps = CLIENT_PROTOCOL_41
		| CLIENT_SECURE_CONNECTION
		| CLIENT_CONNECT_WITH_DB
		| CLIENT_PLUGIN_AUTH
		| CLIENT_DEPRECATE_EOF;
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

	let reply = read_packet(&mut stream).await;
	let reply = if reply[0] == 0xfe {
		write_packet(&mut stream, 3, b"root\0").await;
		read_packet(&mut stream).await
	} else {
		reply
	};
	assert_eq!(reply[0], OK_HEADER, "handshake refused: {}", String::from_utf8_lossy(&reply));
	stream
}

/// The length-encoded integer at `at`, which moves past it.
fn lenenc_int(buf: &[u8], at: &mut usize) -> usize {
	let (value, size) = match buf[*at] {
		0xfc => (usize::from(buf[*at + 1]) | (usize::from(buf[*at + 2]) << 8), 3),
		0xfd => (
			usize::from(buf[*at + 1])
				| (usize::from(buf[*at + 2]) << 8)
				| (usize::from(buf[*at + 3]) << 16),
			4,
		),
		first => (usize::from(first), 1),
	};
	*at += size;
	value
}

/// The length-encoded string at `at`, which moves past it.
fn lenenc_str(buf: &[u8], at: &mut usize) -> String {
	let len = lenenc_int(buf, at);
	let text = String::from_utf8_lossy(&buf[*at..*at + len]).into_owned();
	*at += len;
	text
}

fn put_lenenc_str(out: &mut Vec<u8>, text: &str) {
	let len = text.len();
	if len < 0xfb {
		out.push(len as u8);
	} else if len <= 0xffff {
		out.push(0xfc);
		out.extend_from_slice(&(len as u16).to_le_bytes());
	} else {
		out.push(0xfd);
		out.extend_from_slice(&(len as u32).to_le_bytes()[..3]);
	}
	out.extend_from_slice(text.as_bytes());
}

/// Prepares `sql`, which holds `params.len()` placeholders, and returns its statement id.
async fn prepare(stream: &mut TcpStream, sql: &str, params: usize) -> u32 {
	let mut payload = vec![COM_STMT_PREPARE];
	payload.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &payload).await;
	let reply = read_packet(stream).await;
	assert_eq!(reply[0], OK_HEADER, "prepare `{sql}`: {}", String::from_utf8_lossy(&reply));
	let id = u32::from_le_bytes(reply[1..5].try_into().expect("a statement id"));
	let columns = usize::from(u16::from_le_bytes([reply[5], reply[6]]));
	assert_eq!(usize::from(u16::from_le_bytes([reply[7], reply[8]])), params);
	for _ in 0..params + columns {
		read_packet(stream).await;
	}
	id
}

/// Executes a prepared statement with its parameters bound as text (`None` binds NULL), and
/// gives its rows, or the message of its error packet.
async fn execute(
	stream: &mut TcpStream,
	id: u32,
	params: &[Option<&str>],
) -> Result<Vec<Vec<Cell>>, String> {
	let mut payload = vec![COM_STMT_EXECUTE];
	payload.extend_from_slice(&id.to_le_bytes());
	payload.push(CURSOR_TYPE_NO_CURSOR);
	payload.extend_from_slice(&1u32.to_le_bytes());
	if !params.is_empty() {
		let mut nulls = vec![0u8; params.len().div_ceil(8)];
		for (at, param) in params.iter().enumerate() {
			if param.is_none() {
				nulls[at / 8] |= 1 << (at % 8);
			}
		}
		payload.extend_from_slice(&nulls);
		payload.push(1);
		for _ in params {
			payload.extend_from_slice(&[MYSQL_TYPE_VAR_STRING, 0]);
		}
		for text in params.iter().flatten() {
			put_lenenc_str(&mut payload, text);
		}
	}
	write_packet(stream, 0, &payload).await;
	read_binary_result(stream).await
}

/// The rows of a binary result set whose columns are integers or strings; none for a statement
/// that answers with an OK packet.
async fn read_binary_result(stream: &mut TcpStream) -> Result<Vec<Vec<Cell>>, String> {
	let first = read_packet(stream).await;
	if first[0] == ERR_HEADER {
		// The error code, the `#` marker and the five characters of the SQLSTATE precede the text.
		return Err(String::from_utf8_lossy(&first[9..]).into_owned());
	}
	if first[0] == OK_HEADER {
		return Ok(Vec::new());
	}
	let mut at = 0;
	let columns = lenenc_int(&first, &mut at);
	let mut types = Vec::new();
	for _ in 0..columns {
		let definition = read_packet(stream).await;
		let mut at = 0;
		// The catalog, schema, table, original table, name and original name.
		for _ in 0..6 {
			lenenc_str(&definition, &mut at);
		}
		// The length of the fixed fields, the character set and the column length.
		at += 1 + 2 + 4;
		types.push(definition[at]);
	}
	let mut rows = Vec::new();
	loop {
		let packet = read_packet(stream).await;
		if packet[0] == 0xfe {
			break;
		}
		assert_eq!(packet[0], 0x00, "a binary row");
		let bitmap_len = (columns + 7 + 2) / 8;
		let mut at = 1 + bitmap_len;
		let mut row = Vec::new();
		for (column, column_type) in types.iter().enumerate() {
			if packet[1 + (column + 2) / 8] & (1 << ((column + 2) % 8)) != 0 {
				row.push(None);
				continue;
			}
			let width = match *column_type {
				MYSQL_TYPE_TINY => 1,
				MYSQL_TYPE_SHORT => 2,
				MYSQL_TYPE_LONG => 4,
				MYSQL_TYPE_LONGLONG => 8,
				_ => 0,
			};
			if width == 0 {
				row.push(Some(lenenc_str(&packet, &mut at)));
			} else {
				let mut bytes = [0u8; 8];
				bytes[..width].copy_from_slice(&packet[at..at + width]);
				at += width;
				row.push(Some(i64::from_le_bytes(bytes).to_string()));
			}
		}
		rows.push(row);
	}
	Ok(rows)
}

/// Runs `sql` as a text query and gives its rows, or the message of its error packet.
async fn query(stream: &mut TcpStream, sql: &str) -> Result<Vec<Vec<Cell>>, String> {
	let mut payload = vec![COM_QUERY];
	payload.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &payload).await;
	let first = read_packet(stream).await;
	if first[0] == ERR_HEADER {
		return Err(String::from_utf8_lossy(&first[9..]).into_owned());
	}
	if first[0] == OK_HEADER {
		return Ok(Vec::new());
	}
	let mut at = 0;
	let columns = lenenc_int(&first, &mut at);
	for _ in 0..columns {
		read_packet(stream).await;
	}
	let mut rows = Vec::new();
	loop {
		let packet = read_packet(stream).await;
		if packet[0] == 0xfe && packet.len() < 9 {
			break;
		}
		let mut at = 0;
		let mut row = Vec::new();
		for _ in 0..columns {
			if packet[at] == 0xfb {
				at += 1;
				row.push(None);
			} else {
				row.push(Some(lenenc_str(&packet, &mut at)));
			}
		}
		rows.push(row);
	}
	Ok(rows)
}

/// `(subject, pattern, matches)`: whether `subject LIKE pattern` holds in MySQL 8.4 under its
/// default collation, which ignores case. The pattern is the text of a bound parameter, so every
/// backslash in it is a backslash of the `LIKE` pattern.
const CASES: &[(&str, &str, bool)] = &[
	// Wildcards.
	("Alice", "A%", true),
	("Bob", "A%", false),
	("Alice", "_lice", true),
	("Alice", "%lic%", true),
	("ab", "_", false),
	("", "", true),
	("", "%", true),
	("x", "", false),
	("abc", "%", true),
	("a", "a ", false),
	// Case.
	("Alice", "alice", true),
	("A", "a", true),
	// A backslash makes the next character literal.
	("50%", "50\\%", true),
	("500", "50\\%", false),
	("a_b", "a\\_b", true),
	("axb", "a\\_b", false),
	("%", "\\%", true),
	("_", "\\_", true),
	("a%c", "a\\%%", true),
	("a_", "a\\_", true),
	("ab", "a\\_", false),
	// A doubled backslash is one backslash.
	("a\\b", "a\\\\b", true),
	("a\\xyz", "a\\\\%", true),
	("axyz", "a\\\\%", false),
	("a\\", "a\\\\", true),
	("\\", "\\\\", true),
	("\\\\", "\\\\\\\\", true),
	("\\", "\\\\\\\\", false),
	// A backslash before any other character leaves that character itself.
	("axb", "a\\xb", true),
	("a\\xb", "a\\xb", false),
	("a", "\\a", true),
	// A backslash that ends the pattern has nothing to escape and is itself.
	("a\\", "a\\", true),
	("ab", "a\\", false),
	// Characters that mean something to a regular expression mean nothing here.
	("abc", "a.c", false),
	("a.c", "a.c", true),
	("aab", "a+b", false),
	("a+b", "a+b", true),
	("(x)", "(x)", true),
	("x", "(x)", false),
	("[x]", "[x]", true),
	("x", "[x]", false),
	("a{1}", "a{1}", true),
	("a", "a{1}", false),
	("^x$", "^x$", true),
	("x", "^x$", false),
	("a|b", "a|b", true),
	("a", "a|b", false),
	("a*", "a*", true),
	("aa", "a*", false),
	("a?", "a?", true),
	("a", "a?", false),
	// A line break is a character like any other.
	("a\nb", "a%b", true),
	("a\nb", "a_b", true),
	("a\n", "a", false),
	// A character is a character, whatever its width in bytes.
	("é", "_", true),
	("éa", "_a", true),
	("日本語", "日_語", true),
	("日本語", "日%", true),
];

#[tokio::test]
async fn a_bound_pattern_matches_as_mysql_matches() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	let like = prepare(&mut stream, "SELECT ? LIKE ? AS m", 2).await;
	let not_like = prepare(&mut stream, "SELECT ? NOT LIKE ? AS m", 2).await;
	for (subject, pattern, expected) in CASES {
		let params = [Some(*subject), Some(*pattern)];
		let rows = execute(&mut stream, like, &params).await.expect("LIKE");
		assert_eq!(rows, [[Some(u8::from(*expected).to_string())]], "{subject:?} LIKE {pattern:?}");
		let rows = execute(&mut stream, not_like, &params).await.expect("NOT LIKE");
		assert_eq!(
			rows,
			[[Some(u8::from(!*expected).to_string())]],
			"{subject:?} NOT LIKE {pattern:?}"
		);
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_literal_pattern_matches_as_mysql_matches() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	// What a driver that interpolates its parameters writes: a backslash is doubled, a
	// quote is doubled, and a line break stays a line break.
	let literal = |text: &str| format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"));
	for (subject, pattern, expected) in CASES {
		let sql = format!("SELECT {} LIKE {} AS m", literal(subject), literal(pattern));
		let rows =
			query(&mut stream, &sql).await.unwrap_or_else(|message| panic!("{sql}: {message}"));
		assert_eq!(rows, [[Some(u8::from(*expected).to_string())]], "{sql}");
	}
	// What a person writes: `\%` and `\_` stay two characters in the literal, so the `LIKE`
	// reads them as escaped wildcards.
	for (sql, expected) in [
		(r"SELECT '50%' LIKE '50\%'", "1"),
		(r"SELECT '500' LIKE '50\%'", "0"),
		(r"SELECT 'a_b' LIKE 'a\_b'", "1"),
		(r"SELECT 'axb' LIKE 'a\_b'", "0"),
		(r"SELECT 'a\%b'", r"a\%b"),
		(r"SELECT 'a\_b'", r"a\_b"),
	] {
		let rows =
			query(&mut stream, sql).await.unwrap_or_else(|message| panic!("{sql}: {message}"));
		assert_eq!(rows, [[Some(expected.to_string())]], "{sql}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn null_operands_give_null() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	let like = prepare(&mut stream, "SELECT ? LIKE ? AS m", 2).await;
	for params in [[Some("a"), None], [None, Some("a")], [None, None]] {
		let rows = execute(&mut stream, like, &params).await.expect("LIKE");
		assert_eq!(rows, [[None]], "{params:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_bound_pattern_filters_the_rows_of_a_table() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	query(&mut stream, "CREATE TABLE like_people (name VARCHAR(40))").await.expect("create");
	let insert = prepare(&mut stream, "INSERT INTO like_people (name) VALUES (?)", 1).await;
	for name in ["Alice", "Alan", "Bob", "50% off", "500 off", "a_c", "abc", "a.c"] {
		execute(&mut stream, insert, &[Some(name)]).await.expect("insert a row");
	}
	let select =
		prepare(&mut stream, "SELECT name FROM like_people WHERE name LIKE ? ORDER BY name", 1)
			.await;
	for (pattern, expected) in [
		// `LIKE` ignores case.
		("A%", &["Alan", "Alice", "a.c", "a_c", "abc"][..]),
		("%L%", &["Alan", "Alice"]),
		("a_c", &["a.c", "a_c", "abc"]),
		("a\\_c", &["a_c"]),
		("a.c", &["a.c"]),
		("50\\%%", &["50% off"]),
		("50%", &["50% off", "500 off"]),
		// A bound pattern is a pattern, not a regular expression.
		(".*", &[]),
		("(", &[]),
		("[", &[]),
	] {
		let rows = execute(&mut stream, select, &[Some(pattern)])
			.await
			.unwrap_or_else(|message| panic!("LIKE {pattern:?}: {message}"));
		let names: Vec<_> = rows.iter().map(|row| row[0].as_deref().expect("a name")).collect();
		assert_eq!(names, expected, "LIKE {pattern:?}");
	}
	shutdown.cancel();
}

#[tokio::test]
async fn a_pattern_too_long_to_convert_is_an_error_and_the_connection_survives() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	let like = prepare(&mut stream, "SELECT 'a' LIKE ? AS m", 1).await;
	for pattern in ["_".repeat(2_000_000), "%".repeat(2_000_000), "(".repeat(2_000_000)] {
		let outcome = execute(&mut stream, like, &[Some(&pattern)]).await;
		assert!(outcome.is_err(), "a pattern of {} characters was converted", pattern.len());
	}
	let rows = execute(&mut stream, like, &[Some("a")]).await.expect("a short pattern");
	assert_eq!(rows, [[Some("1".to_string())]]);
	shutdown.cancel();
}
