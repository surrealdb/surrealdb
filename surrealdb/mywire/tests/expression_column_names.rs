//! The names a MySQL client sees for the columns of a `SELECT` that have no alias.
//!
//! MySQL names the column of an unaliased expression with the text of the expression as it was
//! written, so `SELECT COUNT(*)` answers a column named `COUNT(*)` and `SELECT a + b` one named
//! `a + b`. The expected names in this file are those MySQL 8.4 reports.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_mysql::ast::stmt::Statement;
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::constants::*;
use surrealdb_mywire::typing::name_columns_as_written;
use surrealdb_sqlplan::ResultDescriptor;
use surrealdb_sqlplan::binder::mysql::bind_select;
use surrealdb_sqlplan::catalog::MemoryCatalog;
use surrealdb_sqlplan::dialect::DialectPolicy;
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
		"CREATE TABLE exname_t (a INT, b INT, label VARCHAR(10))",
		"INSERT INTO exname_t (a, b, label) VALUES (1, 2, 'x'), (3, 4, NULL)",
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

/// The descriptor the connection builds for `sql`: the planner's, with the names MySQL shows.
fn described(sql: &str) -> ResultDescriptor {
	let statements = surrealdb_mysql::parse_statements(sql).expect("a statement that parses");
	let Some(Statement::Select(select)) = statements.first() else {
		panic!("{sql} is not a select");
	};
	let catalog = MemoryCatalog::new().with_database("test");
	let policy = DialectPolicy::mysql();
	let bound = bind_select(select, &catalog, &policy, None).expect("a select that binds");
	name_columns_as_written(bound.describe(&catalog, &policy), select, sql)
}

fn described_names(sql: &str) -> Vec<String> {
	described(sql).columns.into_iter().map(|column| column.name).collect()
}

/// The column names of the result of `sql`, as MySQL 8.4 reports them.
const MYSQL_NAMES: &[(&str, &[&str])] = &[
	// Literals and arithmetic are named by their text as written, spacing included.
	("SELECT 1", &["1"]),
	("SELECT 1+2", &["1+2"]),
	("SELECT 1 + 2", &["1 + 2"]),
	("SELECT   1   +   2  ", &["1   +   2"]),
	("SELECT 1.50", &["1.50"]),
	("SELECT .5", &[".5"]),
	("SELECT 1E+3", &["1E+3"]),
	("SELECT 007", &["007"]),
	("SELECT 12345678901234567890", &["12345678901234567890"]),
	("SELECT -1", &["-1"]),
	("SELECT - 1", &["- 1"]),
	("SELECT +-1", &["+-1"]),
	("SELECT 0x41", &["0x41"]),
	("SELECT x'41'", &["x'41'"]),
	("SELECT b'1'", &["b'1'"]),
	("SELECT _binary x'41'", &["_binary x'41'"]),
	("SELECT TRUE", &["TRUE"]),
	("SELECT FaLsE", &["FaLsE"]),
	("SELECT true, false, Null", &["true", "false", "NULL"]),
	// A number, a string, NULL and a column have a name of their own, which parentheses and a plus
	// sign leave alone.
	("SELECT ?, (?), +?, ( ? ), ?+1", &["?", "?", "?", "?", "?+1"]),
	("SELECT +1", &["1"]),
	("SELECT (1)", &["1"]),
	("SELECT ( 1 )", &["1"]),
	("SELECT ((1))", &["1"]),
	("SELECT +(1)", &["1"]),
	("SELECT + + 1", &["1"]),
	("SELECT (+1)", &["1"]),
	("SELECT (1.50)", &["1.50"]),
	("SELECT +1.50", &["1.50"]),
	("SELECT ( 007 )", &["007"]),
	("SELECT NULL", &["NULL"]),
	("SELECT null", &["NULL"]),
	("SELECT (NULL)", &["NULL"]),
	("SELECT +NULL", &["NULL"]),
	("SELECT ((NULL))", &["NULL"]),
	("SELECT 'x'", &["x"]),
	("SELECT \"x\"", &["x"]),
	("SELECT 'it''s'", &["it's"]),
	("SELECT 'a\\'b'", &["a'b"]),
	("SELECT 'a\\nb'", &["a\nb"]),
	("SELECT _utf8mb4'x'", &["x"]),
	("SELECT _latin1'x'", &["x"]),
	("SELECT ' lead and trail '", &["lead and trail "]),
	("SELECT '\\t x'", &["x"]),
	("SELECT '\\n\\r\\t x'", &["x"]),
	("SELECT +'  x'", &["x"]),
	("SELECT '  '", &[""]),
	("SELECT '', 1", &["", "1"]),
	("SELECT 'é', '日本語'", &["é", "日本語"]),
	("SELECT ('x')", &["x"]),
	("SELECT (( 'x' ))", &["x"]),
	("SELECT (_utf8mb4'x')", &["x"]),
	("SELECT +'x'", &["x"]),
	("SELECT a, a FROM t1", &["a", "a"]),
	("SELECT a, t1.a FROM t1", &["a", "a"]),
	("SELECT A FROM t1", &["A"]),
	("SELECT `A` FROM t1", &["A"]),
	("SELECT `t1`.`a` FROM t1", &["a"]),
	("SELECT (a) FROM t1", &["a"]),
	("SELECT ((a)) FROM t1", &["a"]),
	("SELECT (t1.a) FROM t1", &["a"]),
	("SELECT +a FROM t1", &["a"]),
	("SELECT + a FROM t1", &["a"]),
	("SELECT +(a) FROM t1", &["a"]),
	("SELECT t1 . a FROM t1", &["a"]),
	// Any other expression is named by its text, parentheses and a plus sign included.
	("SELECT (-1)", &["(-1)"]),
	("SELECT -a FROM t1", &["-a"]),
	("SELECT -'1'", &["-'1'"]),
	("SELECT (true)", &["(true)"]),
	("SELECT (FALSE)", &["(FALSE)"]),
	("SELECT (0x41)", &["(0x41)"]),
	("SELECT (x'41')", &["(x'41')"]),
	("SELECT (b'1')", &["(b'1')"]),
	("SELECT (_binary x'41')", &["(_binary x'41')"]),
	("SELECT (@a)", &["(@a)"]),
	("SELECT +@a", &["+@a"]),
	("SELECT (@@autocommit)", &["(@@autocommit)"]),
	("SELECT ( 1 )+1", &["( 1 )+1"]),
	("SELECT (1)+(2)", &["(1)+(2)"]),
	("SELECT ( 'x' ) + 1", &["( 'x' ) + 1"]),
	("SELECT (a)+1 FROM t1", &["(a)+1"]),
	("SELECT a+(b) FROM t1", &["a+(b)"]),
	("SELECT (1 + 2) * 3", &["(1 + 2) * 3"]),
	("SELECT ((1 + 2))", &["((1 + 2))"]),
	("SELECT -9223372036854775808", &["-9223372036854775808"]),
	("SELECT -(9223372036854775808)", &["-(9223372036854775808)"]),
	("SELECT 9223372036854775808", &["9223372036854775808"]),
	// Operators and predicates.
	("SELECT 1 IS NULL", &["1 IS NULL"]),
	("SELECT (1 IS NULL)", &["(1 IS NULL)"]),
	("SELECT a IS NOT NULL FROM t1", &["a IS NOT NULL"]),
	("SELECT a is  not  null FROM t1", &["a is  not  null"]),
	("SELECT a IN (1,2) FROM t1", &["a IN (1,2)"]),
	("SELECT (a IN (1, 2)) FROM t1", &["(a IN (1, 2))"]),
	("SELECT a NOT IN (1) FROM t1", &["a NOT IN (1)"]),
	("SELECT a BETWEEN 1 AND 2 FROM t1", &["a BETWEEN 1 AND 2"]),
	("SELECT a NOT BETWEEN 1 AND 2 FROM t1", &["a NOT BETWEEN 1 AND 2"]),
	("SELECT NOT 1", &["NOT 1"]),
	("SELECT !1", &["!1"]),
	("SELECT !a FROM t1", &["!a"]),
	("SELECT 1 DIV 2", &["1 DIV 2"]),
	("SELECT 5 MOD 2, 5 % 2", &["5 MOD 2", "5 % 2"]),
	("SELECT a MOD 2 FROM t1", &["a MOD 2"]),
	("SELECT 1<=>1", &["1<=>1"]),
	("SELECT a<>1 FROM t1", &["a<>1"]),
	("SELECT a != 1 FROM t1", &["a != 1"]),
	("SELECT 1 || 2", &["1 || 2"]),
	("SELECT 1 && 2", &["1 && 2"]),
	("SELECT a=1 AND b=2 FROM t1", &["a=1 AND b=2"]),
	("SELECT 'a' LIKE 'a'", &["'a' LIKE 'a'"]),
	("SELECT 'a' NOT LIKE 'b'", &["'a' NOT LIKE 'b'"]),
	("SELECT 'a' REGEXP 'a'", &["'a' REGEXP 'a'"]),
	("SELECT 'x' + 1", &["'x' + 1"]),
	("SELECT 'x'+1", &["'x'+1"]),
	("SELECT a + b FROM t1", &["a + b"]),
	("SELECT a+b, a  +  b FROM t1", &["a+b", "a  +  b"]),
	("SELECT t1.a + 1 FROM t1", &["t1.a + 1"]),
	("SELECT `a`+1 FROM t1", &["`a`+1"]),
	("SELECT t1.`a` + 1 FROM t1", &["t1.`a` + 1"]),
	("SELECT t1.a + t2.c FROM t1 JOIN t2 ON t1.a = t2.a", &["t1.a + t2.c"]),
	("SELECT 日本 + 1 FROM (SELECT 1 AS 日本) s", &["日本 + 1"]),
	// Functions keep the spacing and the letter case they were written in.
	("SELECT COUNT(*) FROM t1", &["COUNT(*)"]),
	("SELECT count( * ) FROM t1", &["count( * )"]),
	("SELECT COUNT(DISTINCT a) FROM t1", &["COUNT(DISTINCT a)"]),
	("SELECT count(distinct a) FROM t1", &["count(distinct a)"]),
	("SELECT COUNT(1) FROM t1", &["COUNT(1)"]),
	("SELECT NOW()", &["NOW()"]),
	("SELECT UPPER('a')", &["UPPER('a')"]),
	("SELECT upper( 'a' )", &["upper( 'a' )"]),
	("SELECT CONCAT('a','b')", &["CONCAT('a','b')"]),
	("SELECT CAST(1 AS CHAR)", &["CAST(1 AS CHAR)"]),
	("SELECT cast( 1 as signed )", &["cast( 1 as signed )"]),
	("SELECT CONVERT(1, CHAR)", &["CONVERT(1, CHAR)"]),
	("SELECT IFNULL(a, 0) FROM t1", &["IFNULL(a, 0)"]),
	("SELECT COALESCE(a, 0) FROM t1", &["COALESCE(a, 0)"]),
	("SELECT IF(a=1,'y','n') FROM t1", &["IF(a=1,'y','n')"]),
	("SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM t1", &["SUM(a)", "AVG(a)", "MIN(a)", "MAX(a)"]),
	("SELECT a, COUNT(*) FROM t1 GROUP BY a", &["a", "COUNT(*)"]),
	("SELECT COUNT(*), COUNT(*) FROM t1", &["COUNT(*)", "COUNT(*)"]),
	("SELECT count(*), COUNT(*) FROM t1", &["count(*)", "COUNT(*)"]),
	("SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t1", &["ROW_NUMBER() OVER (ORDER BY a)"]),
	("SELECT row_number() over ( order by a ) FROM t1", &["row_number() over ( order by a )"]),
	("SELECT SUM(a) OVER () FROM t1", &["SUM(a) OVER ()"]),
	("SELECT CONCAT('日本語', 'x')", &["CONCAT('日本語', 'x')"]),
	// CASE and subqueries.
	(
		"SELECT CASE WHEN a=1 THEN 'y' ELSE 'n' END FROM t1",
		&["CASE WHEN a=1 THEN 'y' ELSE 'n' END"],
	),
	(
		"SELECT case when a = 1 then 'y' else 'n' end   FROM t1",
		&["case when a = 1 then 'y' else 'n' end"],
	),
	("SELECT (SELECT MAX(a) FROM t1)", &["(SELECT MAX(a) FROM t1)"]),
	("SELECT (SELECT 1)", &["(SELECT 1)"]),
	("SELECT EXISTS(SELECT 1)", &["EXISTS(SELECT 1)"]),
	("SELECT a = ANY (SELECT a FROM t1) FROM t1", &["a = ANY (SELECT a FROM t1)"]),
	("SELECT a > ALL (SELECT a FROM t1) FROM t1", &["a > ALL (SELECT a FROM t1)"]),
	// Variables are named by their text.
	("SELECT @a", &["@a"]),
	("SELECT @A", &["@A"]),
	("SELECT @@autocommit", &["@@autocommit"]),
	("SELECT @@AUTOCOMMIT", &["@@AUTOCOMMIT"]),
	("SELECT @@global.autocommit", &["@@global.autocommit"]),
	("SELECT @@SESSION.autocommit", &["@@SESSION.autocommit"]),
	("SELECT @@Global.autocommit", &["@@Global.autocommit"]),
	// An alias is the name, whatever the expression.
	("SELECT 1 AS x, 1 AS x", &["x", "x"]),
	("SELECT 1 AS `a b`", &["a b"]),
	("SELECT 1 `x`", &["x"]),
	("SELECT 1 x", &["x"]),
	("SELECT a AS z FROM t1", &["z"]),
	("SELECT COUNT(*) AS n FROM t1", &["n"]),
	("SELECT 1 + 1 AS `1 + 1`", &["1 + 1"]),
	("SELECT 'x' AS `y`", &["y"]),
	("SELECT COUNT(*) c1, COUNT(*) FROM t1", &["c1", "COUNT(*)"]),
	("SELECT 1 AS `` ", &[""]),
	("SELECT 'a' AS `a`, 'a'", &["a", "a"]),
	("SELECT 1, 1", &["1", "1"]),
	// Comments and line breaks inside an expression are part of its name; those around it are not.
	("SELECT 1 /* c */ + 2", &["1 /* c */ + 2"]),
	("SELECT 1 -- c\n + 2", &["1 -- c\n + 2"]),
	("SELECT 1 # c\n + 2", &["1 # c\n + 2"]),
	("SELECT 1 +\n  2", &["1 +\n  2"]),
	("SELECT\n1\n+\n2", &["1\n+\n2"]),
	("SELECT 1\t+\t2", &["1\t+\t2"]),
	("SELECT /* lead */ 1", &["1"]),
	("SELECT 1 /* trail */ , 2", &["1", "2"]),
	("SELECT 1 + 2 /* trailing */", &["1 + 2"]),
	("SELECT /* l */ 1 + 2 /* t */, 3", &["1 + 2", "3"]),
	("SELECT 1 + 2 -- trailing\n, 3", &["1 + 2", "3"]),
	("SELECT a/*x*/+/*y*/1 FROM t1", &["a/*x*/+/*y*/1"]),
	("SELECT /*+ NO_ICP(t1) */ a+1 FROM t1", &["a+1"]),
	("SELECT 1+2,\n3+4", &["1+2", "3+4"]),
	("SELECT 1 + 2 ,3", &["1 + 2", "3"]),
	("SELECT 1;", &["1"]),
	("SELECT 1 ;  ", &["1"]),
	// The rest of the statement does not matter.
	("SELECT DISTINCT a + b FROM t1", &["a + b"]),
	("SELECT DISTINCT 1+1", &["1+1"]),
	("SELECT DISTINCTROW 1+1", &["1+1"]),
	("SELECT ALL 1+1", &["1+1"]),
	("SELECT 1+1 FROM DUAL", &["1+1"]),
	("SELECT 1+1, a FROM t1", &["1+1", "a"]),
	("SELECT a FROM t1 ORDER BY a LIMIT 1", &["a"]),
	("SELECT 1 FROM t1 GROUP BY 1", &["1"]),
	// A set operation is named after its first query.
	("SELECT 1+1 UNION SELECT 2+2", &["1+1"]),
	("SELECT 1, 2+2 UNION ALL SELECT 3, 4", &["1", "2+2"]),
	("SELECT a FROM t1 UNION SELECT 1+1 FROM t2", &["a"]),
	("SELECT 1+1 FROM t1 UNION SELECT a FROM t1", &["1+1"]),
	// A query over a derived table.
	("SELECT `1+1` + 1 FROM (SELECT 1+1) s", &["`1+1` + 1"]),
	("SELECT x FROM (SELECT a+b AS x FROM t1) s", &["x"]),
	("SELECT s.`a + b` FROM (SELECT a + b FROM t1) s", &["a + b"]),
];

#[test]
fn a_column_without_an_alias_is_named_as_mysql_names_it() {
	let differing: Vec<String> = MYSQL_NAMES
		.iter()
		.filter_map(|(sql, expected)| {
			let got = described_names(sql);
			(got != *expected)
				.then(|| format!("{sql:?}\n    MySQL: {expected:?}\n    got:   {got:?}"))
		})
		.collect();
	assert!(
		differing.is_empty(),
		"{} of {} differ from MySQL:\n{}",
		differing.len(),
		MYSQL_NAMES.len(),
		differing.join("\n")
	);
}

/// The name of the one column of `SELECT <expression>`.
fn name_of(expression: &str) -> String {
	let mut names = described_names(&format!("SELECT {expression}"));
	assert_eq!(names.len(), 1, "{expression}");
	names.remove(0)
}

#[test]
fn a_name_is_cut_to_255_bytes_at_a_character_boundary() {
	// MySQL keeps at most 255 bytes of the text, and never half a character.
	let long = format!("CONCAT('{}', 'b')", "a".repeat(300));
	assert_eq!(name_of(&long), long[..255]);
	assert_eq!(name_of(&format!("'{}'", "a".repeat(300))), "a".repeat(255));

	// 8 bytes of `CONCAT('` and 82 characters of 3 bytes end at byte 254: the next character
	// would end at 257.
	let wide = format!("CONCAT('{}', 'b')", "日".repeat(300));
	let name = name_of(&wide);
	assert_eq!(name, format!("CONCAT('{}", "日".repeat(82)));
	assert_eq!(name.len(), 254);

	// Characters of two bytes end exactly on the limit when the prefix is odd.
	let accents = format!("CONCAT('{}', 'b')", "é".repeat(300));
	assert_eq!(name_of(&accents).len(), 254);
	let exact = format!("CONCAT('{}', 'b')", "a".repeat(247));
	assert_eq!(name_of(&exact), exact[..255]);
	let shorter = format!("CONCAT('{}')", "a".repeat(200));
	assert_eq!(name_of(&shorter), shorter);

	// A string of 300 characters of 3 bytes is 85 of them.
	assert_eq!(name_of(&format!("'{}'", "日".repeat(300))), "日".repeat(85));
}

#[test]
fn naming_a_column_leaves_the_field_its_values_are_read_from_alone() {
	for sql in ["SELECT 1+1, 1+1, COUNT(*), 'x', NULL", "SELECT a + b, a + b AS a, a FROM t"] {
		let statements = surrealdb_mysql::parse_statements(sql).expect("parses");
		let Some(Statement::Select(select)) = statements.first() else {
			panic!("{sql}");
		};
		let catalog = MemoryCatalog::new().with_database("test");
		let policy = DialectPolicy::mysql();
		let planned = bind_select(select, &catalog, &policy, None)
			.expect("binds")
			.describe(&catalog, &policy);
		let keys = |descriptor: &ResultDescriptor| {
			descriptor.columns.iter().map(|column| column.key.clone()).collect::<Vec<_>>()
		};
		let named = name_columns_as_written(planned.clone(), select, sql);
		assert_eq!(keys(&named), keys(&planned), "{sql}");
		assert!(keys(&named).iter().all(Option::is_some), "{sql}");
	}
}

#[test]
fn a_descriptor_that_does_not_fit_the_select_list_keeps_its_names() {
	let one = "SELECT 1";
	let statements = surrealdb_mysql::parse_statements("SELECT 1+1, 2+2").expect("parses");
	let Some(Statement::Select(two)) = statements.first() else {
		panic!("a select");
	};
	// Two items against one column, as when a wildcard has been expanded.
	let descriptor = described(one);
	assert_eq!(
		name_columns_as_written(descriptor.clone(), two, "SELECT 1+1, 2+2").columns,
		descriptor.columns
	);

	// Text the spans do not index, or index in the middle of a character, leaves the name alone.
	let statements = surrealdb_mysql::parse_statements("SELECT 1+1").expect("parses");
	let Some(Statement::Select(select)) = statements.first() else {
		panic!("a select");
	};
	let descriptor = described("SELECT 1+1");
	for text in ["", "SELECT", "SELECT é日本"] {
		let named = name_columns_as_written(descriptor.clone(), select, text);
		assert_eq!(named.columns, described("SELECT 1+1").columns, "{text:?}");
	}
}

#[tokio::test]
async fn a_select_answers_its_unaliased_columns_under_the_text_they_were_written_as() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let rows = query(
		&mut stream,
		"SELECT a + b, a+b, (a + b) * 2, 'x', NULL, 1 IS NULL, label FROM exname_t ORDER BY a",
	)
	.await
	.expect("expressions");
	assert_eq!(names(&rows), ["a + b", "a+b", "(a + b) * 2", "x", "NULL", "1 IS NULL", "label"]);
	assert_eq!(
		rows.rows,
		cells(&[
			&[Some("3"), Some("3"), Some("6"), Some("x"), None, Some("0"), Some("x")],
			&[Some("7"), Some("7"), Some("14"), Some("x"), None, Some("0"), None],
		])
	);

	let rows = query(&mut stream, "SELECT COUNT(*), SUM(b), count( * ), MAX(a) AS m FROM exname_t")
		.await
		.expect("aggregates");
	assert_eq!(names(&rows), ["COUNT(*)", "SUM(b)", "count( * )", "m"]);
	assert_eq!(rows.rows, cells(&[&[Some("2"), Some("6"), Some("2"), Some("3")]]));

	let rows =
		query(&mut stream, "SELECT a + 1 FROM exname_t UNION ALL SELECT b + 1 FROM exname_t")
			.await
			.expect("a union");
	assert_eq!(names(&rows), ["a + 1"]);
	let mut values: Vec<_> = rows.rows.iter().map(|row| row[0].clone()).collect();
	values.sort();
	assert_eq!(values, cells(&[&[Some("2")], &[Some("3")], &[Some("4")], &[Some("5")]]).concat());

	// Letter case and the text of a column are what was written, apart from the table's own.
	let rows = query(&mut stream, "SELECT A, `B`, exname_t.a FROM exname_t ORDER BY a")
		.await
		.expect("columns");
	assert_eq!(names(&rows), ["A", "B", "a"]);
	assert_eq!(
		rows.rows,
		cells(&[&[Some("1"), Some("2"), Some("1")], &[Some("3"), Some("4"), Some("3")]])
	);

	// A line break inside the expression stays in its name.
	let rows = query(&mut stream, "SELECT a +\n  b FROM exname_t ORDER BY a LIMIT 1")
		.await
		.expect("a multi-line expression");
	assert_eq!(names(&rows), ["a +\n  b"]);
	assert_eq!(rows.rows, cells(&[&[Some("3")]]));
	shutdown.cancel();
}

#[tokio::test]
async fn a_name_shared_with_another_column_does_not_share_its_value() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	// Two columns can answer under one name; each holds its own value.
	let rows = query(&mut stream, "SELECT 1+1, 5 AS `1+1`, 1+1, 7 AS `1+1`")
		.await
		.expect("an expression and aliases of its text");
	assert_eq!(names(&rows), ["1+1", "1+1", "1+1", "1+1"]);
	assert_eq!(rows.rows, cells(&[&[Some("2"), Some("5"), Some("2"), Some("7")]]));

	let rows = query(&mut stream, "SELECT a + b, b + a, a + b FROM exname_t ORDER BY a LIMIT 1")
		.await
		.expect("the same text twice");
	assert_eq!(names(&rows), ["a + b", "b + a", "a + b"]);
	assert_eq!(rows.rows, cells(&[&[Some("3"), Some("3"), Some("3")]]));
	shutdown.cancel();
}

#[tokio::test]
async fn a_name_over_the_wire_is_at_most_255_bytes() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;

	let long = format!("CONCAT('{}', 'b')", "日".repeat(300));
	let rows = query(&mut stream, &format!("SELECT {long}")).await.expect("a long expression");
	assert_eq!(rows.columns, [format!("CONCAT('{}", "日".repeat(82))]);
	assert_eq!(rows.rows.len(), 1);
	assert_eq!(rows.rows[0][0].as_deref(), Some(format!("{}b", "日".repeat(300)).as_str()));
	shutdown.cancel();
}

/// Prepare `sql`, execute it with the integer `params` (at most eight), and return the column
/// names the prepare answer and the execute answer carry.
async fn prepared_names(
	stream: &mut TcpStream,
	sql: &str,
	params: &[i64],
) -> (Vec<String>, Vec<String>) {
	async fn read_names(stream: &mut TcpStream, count: usize) -> Vec<String> {
		let mut found = Vec::new();
		for _ in 0..count {
			let (_, definition) = read_packet(stream).await;
			let mut at = 0;
			for _ in 0..4 {
				lenenc(&definition, &mut at);
			}
			found.push(String::from_utf8_lossy(&lenenc(&definition, &mut at)).into_owned());
		}
		found
	}

	let mut prepare = vec![COM_STMT_PREPARE];
	prepare.extend_from_slice(sql.as_bytes());
	write_packet(stream, 0, &prepare).await;
	let (_, reply) = read_packet(stream).await;
	assert_ne!(reply[0], 0xff, "{sql}: {}", String::from_utf8_lossy(&reply));
	let statement_id = u32::from_le_bytes(reply[1..5].try_into().expect("statement id"));
	let described = u16::from_le_bytes(reply[5..7].try_into().expect("column count")) as usize;
	let parameters = u16::from_le_bytes(reply[7..9].try_into().expect("parameter count"));
	if parameters > 0 {
		// The parameter definitions, and the EOF that ends them.
		for _ in 0..=parameters {
			read_packet(stream).await;
		}
	}
	let at_prepare = read_names(stream, described).await;
	if described > 0 {
		read_packet(stream).await;
	}

	let mut execute = vec![COM_STMT_EXECUTE];
	execute.extend_from_slice(&statement_id.to_le_bytes());
	execute.push(CURSOR_TYPE_NO_CURSOR);
	execute.extend_from_slice(&1u32.to_le_bytes());
	if !params.is_empty() {
		// The null bitmap, the new-parameters-bound flag, the types and then the values.
		execute.push(0);
		execute.push(1);
		for _ in params {
			execute.extend_from_slice(&[MYSQL_TYPE_LONGLONG, 0x00]);
		}
		for value in params {
			execute.extend_from_slice(&value.to_le_bytes());
		}
	}
	write_packet(stream, 0, &execute).await;
	let (_, count) = read_packet(stream).await;
	assert!(count[0] != 0xff && count[0] != 0x00, "{sql} answers a result set: {count:?}");
	let at_execute = read_names(stream, count[0] as usize).await;
	let (_, eof) = read_packet(stream).await;
	assert_eq!(eof[0], 0xfe, "columns end with EOF");
	loop {
		let (_, packet) = read_packet(stream).await;
		if packet[0] == 0xfe && packet.len() < 9 {
			break;
		}
	}
	(at_prepare, at_execute)
}

#[tokio::test]
async fn a_prepared_select_names_its_columns_when_prepared_and_when_executed() {
	let (addr, shutdown) = start_server().await;
	let mut stream = connect(addr).await;
	seed(&mut stream).await;

	let (at_prepare, at_execute) =
		prepared_names(&mut stream, "SELECT COUNT(*), SUM(b) * 2 FROM exname_t", &[]).await;
	assert_eq!(at_prepare, ["COUNT(*)", "SUM(b) * 2"]);
	assert_eq!(at_execute, at_prepare);

	// A placeholder is written `?` in the name, as MySQL shows it.
	let (at_prepare, at_execute) =
		prepared_names(&mut stream, "SELECT ? + 1, a + ? FROM exname_t WHERE a = 1", &[41, 1])
			.await;
	assert_eq!(at_prepare, ["? + 1", "a + ?"]);
	assert_eq!(at_execute, at_prepare);
	shutdown.cancel();
}
