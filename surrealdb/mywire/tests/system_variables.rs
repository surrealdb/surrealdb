//! A `SELECT` of system variables over the MySQL wire: the statement a driver sends to read its
//! session settings answers one column per variable, under the name and with the type MySQL gives
//! it, and reads nothing about the host the server runs on. A variable MySQL does not have is
//! refused with the error MySQL gives.

#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_mywire::auth::compute_mysql_native_password_scramble;
use surrealdb_mywire::codec::MAX_ALLOWED_PACKET;
use surrealdb_mywire::constants::*;
use surrealdb_mywire::msg::ColumnDefinition41;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

type Cell = Option<String>;

/// A result set: the name and MySQL type of each column, and the text of each cell.
#[derive(Debug)]
struct Rows {
	columns: Vec<(String, u8)>,
	rows: Vec<Vec<Cell>>,
}

impl Rows {
	fn names(&self) -> Vec<&str> {
		self.columns.iter().map(|(name, _)| name.as_str()).collect()
	}

	fn types(&self) -> Vec<u8> {
		self.columns.iter().map(|(_, ty)| *ty).collect()
	}

	/// The cells of the one row the result set holds.
	fn row(&self) -> Vec<Option<&str>> {
		assert_eq!(self.rows.len(), 1, "one row: {self:?}");
		self.rows[0].iter().map(|cell| cell.as_deref()).collect()
	}
}

/// A connection, and the version its server announced in the greeting.
struct Client {
	stream: TcpStream,
	server_version: String,
}

async fn start_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("datastore");
	ds.run(QueryRequest::new(
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

async fn connect(addr: std::net::SocketAddr) -> Client {
	connect_to(addr, Some("test/test")).await
}

/// Connect, selecting `database` in the handshake when there is one.
async fn connect_to(addr: std::net::SocketAddr, database: Option<&str>) -> Client {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let (_, greeting) = read_packet(&mut stream).await;
	let version_end = greeting.iter().position(|b| *b == 0).expect("server version");
	let server_version = String::from_utf8_lossy(&greeting[1..version_end]).into_owned();
	// The salt: 8 bytes after the connection id, 12 more after the fixed fields that follow.
	let mut at = version_end + 1 + 4;
	let mut salt = [0u8; 20];
	salt[..8].copy_from_slice(&greeting[at..at + 8]);
	at += 9 + 2 + 1 + 2 + 2 + 1 + 10;
	salt[8..].copy_from_slice(&greeting[at..at + 12]);

	let mut response = Vec::new();
	let mut caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
	if database.is_some() {
		caps |= CLIENT_CONNECT_WITH_DB;
	}
	response.extend_from_slice(&caps.to_le_bytes());
	response.extend_from_slice(&(16u32 * 1024 * 1024).to_le_bytes());
	response.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	response.extend_from_slice(&[0u8; 23]);
	response.extend_from_slice(b"root\0");
	response.push(20);
	response.extend_from_slice(&compute_mysql_native_password_scramble("root", &salt));
	if let Some(database) = database {
		response.extend_from_slice(database.as_bytes());
		response.push(0);
	}
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
	Client {
		stream,
		server_version,
	}
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

fn column(definition: &[u8]) -> (String, u8) {
	let definition = ColumnDefinition41::deserialize(definition).expect("column definition");
	(definition.name, definition.column_type)
}

impl Client {
	/// Run `sql` as a text query and return its one result set, or the error's number and text.
	async fn query(&mut self, sql: &str) -> Result<Rows, (u16, String)> {
		let mut text = vec![COM_QUERY];
		text.extend_from_slice(sql.as_bytes());
		write_packet(&mut self.stream, 0, &text).await;
		let (_, first) = read_packet(&mut self.stream).await;
		match first[0] {
			0xff => {
				let code = u16::from_le_bytes([first[1], first[2]]);
				return Err((code, String::from_utf8_lossy(&first[9..]).into_owned()));
			}
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
			let (_, definition) = read_packet(&mut self.stream).await;
			columns.push(column(&definition));
		}
		let (_, eof) = read_packet(&mut self.stream).await;
		assert_eq!(eof[0], 0xfe, "columns end with EOF");
		let mut rows = Vec::new();
		loop {
			let (_, packet) = read_packet(&mut self.stream).await;
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

	/// Select `database` with COM_INIT_DB.
	async fn init_db(&mut self, database: &str) {
		let mut init = vec![COM_INIT_DB];
		init.extend_from_slice(database.as_bytes());
		write_packet(&mut self.stream, 0, &init).await;
		let (_, reply) = read_packet(&mut self.stream).await;
		assert_eq!(reply[0], 0x00, "{database}: {}", String::from_utf8_lossy(&reply));
	}

	async fn ok(&mut self, sql: &str) -> Rows {
		match self.query(sql).await {
			Ok(rows) => rows,
			Err((code, message)) => panic!("{sql}: error {code}: {message}"),
		}
	}

	/// The error packet of `command` followed by `sql`: the number, the SQLSTATE and the message.
	async fn refusal(&mut self, command: u8, sql: &str) -> (u16, String, String) {
		let mut packet = vec![command];
		packet.extend_from_slice(sql.as_bytes());
		write_packet(&mut self.stream, 0, &packet).await;
		let (_, reply) = read_packet(&mut self.stream).await;
		assert_eq!(reply[0], 0xff, "{sql} is refused: {reply:?}");
		assert_eq!(reply[3], b'#');
		(
			u16::from_le_bytes([reply[1], reply[2]]),
			String::from_utf8_lossy(&reply[4..9]).into_owned(),
			String::from_utf8_lossy(&reply[9..]).into_owned(),
		)
	}

	/// The error of a text query that the server refuses.
	async fn error(&mut self, sql: &str) -> (u16, String, String) {
		self.refusal(COM_QUERY, sql).await
	}

	/// The error of a statement that the server refuses to prepare.
	async fn prepare_error(&mut self, sql: &str) -> (u16, String, String) {
		self.refusal(COM_STMT_PREPARE, sql).await
	}

	/// Prepare `sql`, which names no parameter, and execute it. Returns the columns it was
	/// described with, the columns it answered with, and the binary row of its result set.
	async fn prepared(&mut self, sql: &str) -> (Vec<(String, u8)>, Vec<(String, u8)>, Vec<u8>) {
		let mut prepare = vec![COM_STMT_PREPARE];
		prepare.extend_from_slice(sql.as_bytes());
		write_packet(&mut self.stream, 0, &prepare).await;
		let (_, reply) = read_packet(&mut self.stream).await;
		assert_ne!(reply[0], 0xff, "{sql}: {}", String::from_utf8_lossy(&reply));
		let statement_id = u32::from_le_bytes(reply[1..5].try_into().expect("statement id"));
		let described = u16::from_le_bytes(reply[5..7].try_into().expect("column count"));
		let mut described_columns = Vec::new();
		for _ in 0..described {
			let (_, definition) = read_packet(&mut self.stream).await;
			described_columns.push(column(&definition));
		}
		if described > 0 {
			let (_, eof) = read_packet(&mut self.stream).await;
			assert_eq!(eof[0], 0xfe, "described columns end with EOF");
		}

		let mut execute = vec![COM_STMT_EXECUTE];
		execute.extend_from_slice(&statement_id.to_le_bytes());
		execute.push(CURSOR_TYPE_NO_CURSOR);
		execute.extend_from_slice(&1u32.to_le_bytes());
		write_packet(&mut self.stream, 0, &execute).await;
		let (_, count) = read_packet(&mut self.stream).await;
		assert!(count[0] != 0xff && count[0] != 0x00, "{sql} answers a result set: {count:?}");
		let mut columns = Vec::new();
		for _ in 0..count[0] {
			let (_, definition) = read_packet(&mut self.stream).await;
			columns.push(column(&definition));
		}
		let (_, eof) = read_packet(&mut self.stream).await;
		assert_eq!(eof[0], 0xfe, "columns end with EOF");
		let (_, row) = read_packet(&mut self.stream).await;
		let (_, end) = read_packet(&mut self.stream).await;
		assert_eq!(end[0], 0xfe, "one row, then EOF");
		(described_columns, columns, row)
	}
}

/// The SQL_MODE of a MySQL 8.4 server that was not configured.
const SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
	ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

const INT: u8 = MYSQL_TYPE_LONGLONG;
const TEXT: u8 = MYSQL_TYPE_VAR_STRING;

/// The statement Connector/J 8 sends as soon as it connects to a server of version 8.0.3 or later,
/// to read the settings of the session.
const CONNECTOR_J_SETUP: &str = "/* mysql-connector-j-8.0.33 (Revision: x) */SELECT \
	@@session.auto_increment_increment AS auto_increment_increment, \
	@@character_set_client AS character_set_client, \
	@@character_set_connection AS character_set_connection, \
	@@character_set_results AS character_set_results, \
	@@character_set_server AS character_set_server, \
	@@collation_server AS collation_server, \
	@@collation_connection AS collation_connection, \
	@@init_connect AS init_connect, \
	@@interactive_timeout AS interactive_timeout, \
	@@license AS license, \
	@@lower_case_table_names AS lower_case_table_names, \
	@@max_allowed_packet AS max_allowed_packet, \
	@@net_write_timeout AS net_write_timeout, \
	@@performance_schema AS performance_schema, \
	@@sql_mode AS sql_mode, \
	@@system_time_zone AS system_time_zone, \
	@@time_zone AS time_zone, \
	@@transaction_isolation AS transaction_isolation, \
	@@wait_timeout AS wait_timeout";

/// The statement Connector/J 5.1 sends to a server of version 8.0.3 or later, which asks for a
/// few settings the newer one does not.
const CONNECTOR_J_5_SETUP: &str = "/* mysql-connector-java-5.1.49 */SELECT \
	@@session.auto_increment_increment AS auto_increment_increment, \
	@@character_set_client AS character_set_client, \
	@@character_set_results AS character_set_results, \
	@@init_connect AS init_connect, \
	@@interactive_timeout AS interactive_timeout, \
	@@license AS license, \
	@@lower_case_table_names AS lower_case_table_names, \
	@@max_allowed_packet AS max_allowed_packet, \
	@@net_buffer_length AS net_buffer_length, \
	@@net_write_timeout AS net_write_timeout, \
	@@sql_mode AS sql_mode, \
	@@system_time_zone AS system_time_zone, \
	@@time_zone AS time_zone, \
	@@transaction_isolation AS transaction_isolation, \
	@@wait_timeout AS wait_timeout";

#[tokio::test]
async fn the_session_setup_of_connector_j_answers_a_column_for_each_variable() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let rows = client.ok(CONNECTOR_J_SETUP).await;
	assert_eq!(
		rows.names(),
		[
			"auto_increment_increment",
			"character_set_client",
			"character_set_connection",
			"character_set_results",
			"character_set_server",
			"collation_server",
			"collation_connection",
			"init_connect",
			"interactive_timeout",
			"license",
			"lower_case_table_names",
			"max_allowed_packet",
			"net_write_timeout",
			"performance_schema",
			"sql_mode",
			"system_time_zone",
			"time_zone",
			"transaction_isolation",
			"wait_timeout",
		]
	);
	assert_eq!(
		rows.types(),
		[
			INT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, TEXT, INT, INT, INT, INT, TEXT,
			TEXT, TEXT, TEXT, INT,
		]
	);
	let max_allowed_packet = MAX_ALLOWED_PACKET.to_string();
	assert_eq!(
		rows.row(),
		[
			Some("1"),
			Some("utf8mb4"),
			Some("utf8mb4"),
			Some("utf8mb4"),
			Some("utf8mb4"),
			Some("utf8mb4_0900_ai_ci"),
			Some("utf8mb4_0900_ai_ci"),
			Some(""),
			Some("28800"),
			Some("GPL"),
			Some("0"),
			Some(max_allowed_packet.as_str()),
			Some("60"),
			Some("0"),
			Some(SQL_MODE),
			Some("UTC"),
			Some("SYSTEM"),
			Some("REPEATABLE-READ"),
			Some("28800"),
		]
	);

	let rows = client.ok(CONNECTOR_J_5_SETUP).await;
	assert_eq!(rows.columns.len(), 15);
	assert_eq!(rows.names()[8], "net_buffer_length");
	assert_eq!(rows.names()[13], "transaction_isolation");
	assert_eq!(rows.types()[8], INT);
	assert_eq!(rows.types()[13], TEXT);
	assert_eq!(rows.row()[8], Some("16384"));
	assert_eq!(rows.row()[13], Some("REPEATABLE-READ"));

	shutdown.cancel();
}

#[tokio::test]
async fn the_mysql_client_banner_query_answers_one_row() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let rows = client.ok("select @@version_comment limit 1").await;
	assert_eq!(rows.names(), ["@@version_comment"]);
	assert_eq!(rows.row(), [Some("SurrealDB")]);

	// `LIMIT` and `OFFSET` count the one row of the statement.
	assert!(client.ok("SELECT @@version_comment LIMIT 0").await.rows.is_empty());
	assert!(client.ok("SELECT @@version_comment LIMIT 1 OFFSET 1").await.rows.is_empty());
	assert_eq!(client.ok("SELECT @@version_comment LIMIT 5").await.rows.len(), 1);

	shutdown.cancel();
}

#[tokio::test]
async fn several_variables_answer_in_the_order_asked_with_the_type_of_each() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let rows = client
		.ok("SELECT @@version, @@sql_mode, @@max_allowed_packet, @@global.time_zone, \
			 @@character_set_client")
		.await;
	assert_eq!(
		rows.names(),
		[
			"@@version",
			"@@sql_mode",
			"@@max_allowed_packet",
			"@@global.time_zone",
			"@@character_set_client"
		]
	);
	assert_eq!(rows.types(), [TEXT, TEXT, INT, TEXT, TEXT]);
	let max_allowed_packet = MAX_ALLOWED_PACKET.to_string();
	assert_eq!(
		rows.row(),
		[
			Some(client.server_version.as_str()),
			Some(SQL_MODE),
			Some(max_allowed_packet.as_str()),
			Some("SYSTEM"),
			Some("utf8mb4"),
		]
	);

	// A variable repeated, and a variable under two aliases, is a column each time.
	let rows =
		client.ok("SELECT @@autocommit, @@autocommit, @@autocommit AS a, @@autocommit AS b").await;
	assert_eq!(rows.names(), ["@@autocommit", "@@autocommit", "a", "b"]);
	assert_eq!(rows.row(), [Some("1"), Some("1"), Some("1"), Some("1")]);

	shutdown.cancel();
}

#[tokio::test]
async fn every_scope_and_spelling_of_a_variable_reads_the_same_value() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let rows = client
		.ok("SELECT @@autocommit AS plain, @@session.autocommit AS via_session, \
			 @@local.autocommit AS via_local, @@global.autocommit AS via_global, \
			 @@SESSION.AUTOCOMMIT AS upper, @@`autocommit` AS quoted, \
			 @@global.`autocommit` AS quoted_global")
		.await;
	assert_eq!(rows.types(), [INT; 7]);
	assert_eq!(rows.row(), [Some("1"); 7]);

	shutdown.cancel();
}

#[tokio::test]
async fn an_integer_variable_is_a_number_and_every_other_variable_is_text() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	for (name, ty) in [
		("autocommit", INT),
		("max_allowed_packet", INT),
		("net_buffer_length", INT),
		("interactive_timeout", INT),
		("wait_timeout", INT),
		("net_write_timeout", INT),
		("net_read_timeout", INT),
		("lower_case_table_names", INT),
		("auto_increment_increment", INT),
		("auto_increment_offset", INT),
		("transaction_read_only", INT),
		("sql_auto_is_null", INT),
		("performance_schema", INT),
		("session_track_schema", INT),
		("session_track_state_change", INT),
		("session_track_system_variables", TEXT),
		("version", TEXT),
		("version_comment", TEXT),
		("sql_mode", TEXT),
		("character_set_client", TEXT),
		("collation_connection", TEXT),
		("time_zone", TEXT),
		("system_time_zone", TEXT),
		("transaction_isolation", TEXT),
		("character_set_filesystem", TEXT),
		("character_set_system", TEXT),
		("default_storage_engine", TEXT),
		("init_connect", TEXT),
		("license", TEXT),
	] {
		let rows = client.ok(&format!("SELECT @@{name} AS v")).await;
		assert_eq!(rows.types(), [ty], "the type of @@{name}");
		let value = rows.row()[0].map(str::to_owned).expect("a variable of the server answers");
		assert_eq!(ty == INT, value.parse::<i64>().is_ok(), "@@{name} answers {value:?}");
	}

	shutdown.cancel();
}

#[tokio::test]
async fn a_variable_beside_other_columns_keeps_its_value_and_type() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let rows = client
		.ok("SELECT 'a' AS text, @@autocommit AS flag, NULL AS nothing, @@version AS version, \
			 @@max_allowed_packet + 1 AS next_packet")
		.await;
	assert_eq!(rows.names(), ["text", "flag", "nothing", "version", "next_packet"]);
	let next = (MAX_ALLOWED_PACKET + 1).to_string();
	assert_eq!(
		rows.row(),
		[Some("a"), Some("1"), None, Some(client.server_version.as_str()), Some(next.as_str()),]
	);

	// A condition on the row decides whether the variable is answered, and the columns stay.
	let rows = client.ok("SELECT @@version AS v WHERE 1 = 0").await;
	assert_eq!(rows.names(), ["v"]);
	assert!(rows.rows.is_empty());
	let rows = client.ok("SELECT @@version AS v WHERE 1 = 1").await;
	assert_eq!(rows.row(), [Some(client.server_version.as_str())]);

	shutdown.cancel();
}

#[tokio::test]
async fn a_variable_mysql_has_and_the_server_does_not_model_answers_null_and_none_describes_the_host()
 {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	// MySQL answers these with the paths, the host name, the port and the platform of its server.
	for name in [
		"datadir",
		"basedir",
		"tmpdir",
		"plugin_dir",
		"socket",
		"pid_file",
		"log_error",
		"secure_file_priv",
		"hostname",
		"report_host",
		"port",
		"bind_address",
		"version_compile_os",
		"version_compile_machine",
		// Variables of MySQL that describe nothing about the host, and are not modelled.
		"max_connections",
		"have_ssl",
		"innodb_buffer_pool_size",
	] {
		for scoped in [format!("@@{name}"), format!("@@global.{name}")] {
			let rows = client.ok(&format!("SELECT {scoped} AS v")).await;
			assert_eq!(rows.names(), ["v"]);
			assert_eq!(rows.row(), [None], "{scoped}");
		}
	}

	// A user variable is never set, so it is NULL.
	let rows = client.ok("SELECT @never_set AS v").await;
	assert_eq!(rows.row(), [None]);

	shutdown.cancel();
}

#[tokio::test]
async fn the_version_is_the_one_the_greeting_announced() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let version = client.server_version.clone();
	assert_eq!(client.ok("SELECT @@version AS v").await.row(), [Some(version.as_str())]);
	assert_eq!(client.ok("SELECT @@global.version AS v").await.row(), [Some(version.as_str())]);
	assert_eq!(client.ok("SELECT VERSION()").await.row(), [Some(version.as_str())]);
	assert_eq!(
		client.ok("SELECT VERSION() AS v, 1 AS one").await.row(),
		[Some(version.as_str()), Some("1")]
	);

	shutdown.cancel();
}

#[tokio::test]
async fn a_prepared_select_of_variables_describes_and_answers_a_column_for_each() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	let (described, answered, row) = client
		.prepared(
			"SELECT @@max_allowed_packet AS packet, @@version AS version, \
			 @@net_buffer_length AS buffer, @@max_connections AS missing",
		)
		.await;
	let expected = [
		("packet".to_string(), INT),
		("version".to_string(), TEXT),
		("buffer".to_string(), INT),
		("missing".to_string(), TEXT),
	];
	assert_eq!(described, expected);
	assert_eq!(answered, expected);

	// The binary row: a header, the NULL bitmap of four columns (bit 2 + 3 is the fourth), then the
	// values of the columns that are not NULL.
	assert_eq!(row[0], 0x00);
	assert_eq!(row[1], 1 << (3 + 2), "only the fourth column is NULL");
	let mut at = 2;
	assert_eq!(
		u64::from_le_bytes(row[at..at + 8].try_into().expect("integer")),
		MAX_ALLOWED_PACKET as u64
	);
	at += 8;
	assert_eq!(lenenc(&row, &mut at), client.server_version.as_bytes());
	assert_eq!(u64::from_le_bytes(row[at..at + 8].try_into().expect("integer")), 16384);
	at += 8;
	assert_eq!(at, row.len());

	shutdown.cancel();
}

#[tokio::test]
async fn a_select_of_variables_answers_while_autocommit_is_off() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	client.ok("SET autocommit = 0").await;
	assert_eq!(client.ok("SELECT @@sql_mode AS mode").await.row(), [Some(SQL_MODE)]);
	assert_eq!(client.ok("SELECT @@version AS v").await.rows.len(), 1);
	client.ok("COMMIT").await;
	assert_eq!(client.ok("SELECT @@sql_mode AS mode").await.row(), [Some(SQL_MODE)]);

	shutdown.cancel();
}

#[tokio::test]
async fn a_connection_with_no_database_answers_a_select_of_variables_as_with_one() {
	let (addr, shutdown) = start_server().await;
	let mut with_database = connect(addr).await;
	let mut client = connect_to(addr, None).await;

	// What a driver sends as it connects, before it selects a database.
	let rows = client.ok("select @@version_comment limit 1").await;
	assert_eq!(rows.names(), ["@@version_comment"]);
	assert_eq!(rows.row(), [Some("SurrealDB")]);
	assert!(client.ok("select @@version_comment limit 0").await.rows.is_empty());

	let setup = client.ok(CONNECTOR_J_SETUP).await;
	let expected = with_database.ok(CONNECTOR_J_SETUP).await;
	assert_eq!(setup.columns, expected.columns);
	assert_eq!(setup.rows, expected.rows);
	let setup = client.ok(CONNECTOR_J_5_SETUP).await;
	let expected = with_database.ok(CONNECTOR_J_5_SETUP).await;
	assert_eq!(setup.columns, expected.columns);
	assert_eq!(setup.rows, expected.rows);

	for sql in [
		"SELECT @@version, @@sql_mode, @@max_allowed_packet, @@global.time_zone, @@max_connections",
		"SELECT @@version AS v, @@autocommit AS a",
		"SELECT @@version LIMIT 0",
		"SELECT @@max_allowed_packet;",
	] {
		let answered = client.ok(sql).await;
		let expected = with_database.ok(sql).await;
		assert_eq!(answered.columns, expected.columns, "{sql}");
		assert_eq!(answered.rows, expected.rows, "{sql}");
	}

	// A statement the engine must run still needs a database, and a connection that selects one
	// runs it.
	let sql = "SELECT @@version AS v, 1 AS one";
	assert!(client.query(sql).await.is_err());
	client.init_db("test/test").await;
	assert_eq!(client.ok(sql).await.row(), [Some(client.server_version.as_str()), Some("1")]);

	shutdown.cancel();
}

/// Each statement names one variable MySQL 8.4 does not have, and the message MySQL answers.
const REFUSED_NAMES: &[(&str, &str)] = &[
	("SELECT @@no_such_variable", "no_such_variable"),
	("SELECT @@NoSuch", "NoSuch"),
	("SELECT @@global.no_such_variable", "no_such_variable"),
	("SELECT @@session.NoSuch", "NoSuch"),
	("SELECT @@local.no_such_variable", "no_such_variable"),
	("SELECT @@`no such`", "no such"),
	("SELECT @@`a``b`", "a`b"),
	("SELECT @@global.`x y`", "x y"),
	("SELECT @@`é`", "é"),
	("SELECT @@no_such_variable AS v", "no_such_variable"),
	("SELECT @@version, @@no_such_variable", "no_such_variable"),
	("SELECT @@no_such_variable, @@version", "no_such_variable"),
	("SELECT IFNULL(@@no_such_variable, 1)", "no_such_variable"),
	("SELECT 1 WHERE 0 AND @@no_such_variable", "no_such_variable"),
	("SELECT * FROM (SELECT @@no_such_variable) AS t", "no_such_variable"),
	// The query cache and the old names of the isolation level and the read-only flag went in
	// MySQL 8.0.3.
	("SELECT @@tx_isolation", "tx_isolation"),
	("SELECT @@session.tx_isolation", "tx_isolation"),
	("SELECT @@tx_read_only", "tx_read_only"),
	("SELECT @@query_cache_size", "query_cache_size"),
	("SELECT @@query_cache_type", "query_cache_type"),
];

#[tokio::test]
async fn a_variable_mysql_does_not_have_is_error_1193_with_the_message_mysql_gives() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	for (sql, name) in REFUSED_NAMES {
		let (code, state, message) = client.error(sql).await;
		assert_eq!(
			(code, state.as_str(), message.as_str()),
			(1193, "HY000", format!("Unknown system variable '{name}'").as_str()),
			"{sql}"
		);
	}

	// A refused statement leaves the connection usable.
	assert_eq!(client.ok("SELECT @@version AS v").await.rows.len(), 1);

	shutdown.cancel();
}

#[tokio::test]
async fn a_connection_with_no_database_refuses_a_variable_mysql_does_not_have_too() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect_to(addr, None).await;

	for (sql, name) in REFUSED_NAMES.iter().filter(|(sql, _)| !sql.contains("FROM (")) {
		let (code, state, message) = client.error(sql).await;
		assert_eq!(
			(code, state.as_str(), message.as_str()),
			(1193, "HY000", format!("Unknown system variable '{name}'").as_str()),
			"{sql}"
		);
	}
	// A variable MySQL has is not enough to have the statement answered.
	let (code, _, message) =
		client.error("select @@version_comment, @@no_such_variable limit 1").await;
	assert_eq!((code, message.as_str()), (1193, "Unknown system variable 'no_such_variable'"));
	assert_eq!(client.ok("select @@version_comment limit 1").await.rows.len(), 1);

	shutdown.cancel();
}

#[tokio::test]
async fn a_prepared_statement_that_names_such_a_variable_is_refused_when_it_is_prepared() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	for sql in [
		"SELECT @@no_such_variable",
		"SELECT @@global.NoSuch AS v",
		"SELECT 1 WHERE @@no_such_variable = 1",
		"SELECT @@tx_isolation",
		"SET @@no_such_variable = 1",
	] {
		let (code, state, message) = client.prepare_error(sql).await;
		assert_eq!((code, state.as_str()), (1193, "HY000"), "{sql}");
		assert!(message.starts_with("Unknown system variable '"), "{sql}: {message}");
	}
	let (code, state, message) = client.prepare_error("SELECT @@global.timestamp").await;
	assert_eq!(
		(code, state.as_str(), message.as_str()),
		(1238, "HY000", "Variable 'timestamp' is a SESSION variable")
	);

	// A variable MySQL has prepares and answers, and the refusals left the connection usable.
	let (described, answered, _) = client.prepared("SELECT @@version AS v").await;
	assert_eq!(described, [("v".to_string(), TEXT)]);
	assert_eq!(answered, [("v".to_string(), TEXT)]);

	shutdown.cancel();
}

#[tokio::test]
async fn the_name_an_error_repeats_is_cut_at_64_bytes() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	for (length, shown) in [(63, 63), (64, 64), (65, 64), (300, 64), (100_000, 64)] {
		let name = "a".repeat(length);
		let (code, _, message) = client.error(&format!("SELECT @@{name}")).await;
		assert_eq!(code, 1193);
		assert_eq!(message, format!("Unknown system variable '{}'", "a".repeat(shown)));
	}
	// A character is not cut in two.
	let (_, _, message) = client.error(&format!("SELECT @@`{}`", "é".repeat(100))).await;
	assert_eq!(message, format!("Unknown system variable '{}'", "é".repeat(32)));
	let (_, _, message) = client.error(&format!("SELECT @@`{}`", "€".repeat(100))).await;
	assert_eq!(message, format!("Unknown system variable '{}'", "€".repeat(21)));

	shutdown.cancel();
}

#[tokio::test]
async fn a_variable_only_a_session_has_is_refused_when_read_with_the_global_scope() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	for (sql, name) in [
		("SELECT @@global.timestamp", "timestamp"),
		("SELECT @@GLOBAL.TIMESTAMP", "timestamp"),
		("SELECT @@global.sql_log_bin", "sql_log_bin"),
		("SELECT @@version, @@global.warning_count", "warning_count"),
		("SELECT @@global.`last_insert_id`", "last_insert_id"),
	] {
		let (code, state, message) = client.error(sql).await;
		assert_eq!(
			(code, state.as_str(), message.as_str()),
			(1238, "HY000", format!("Variable '{name}' is a SESSION variable").as_str()),
			"{sql}"
		);
	}

	// With no scope, or the session's, the variable is one MySQL has. The server does not model
	// it, so it reads NULL; a variable the server models reads the same with every scope.
	let rows = client
		.ok("SELECT @@timestamp AS a, @@session.timestamp AS b, @@local.sql_log_bin AS c")
		.await;
	assert_eq!(rows.row(), [None, None, None]);

	shutdown.cancel();
}

#[tokio::test]
async fn a_set_of_a_variable_mysql_does_not_have_is_error_1193() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	for sql in [
		"SET @@no_such_variable = 1",
		"SET no_such_variable = 1",
		"SET SESSION no_such_variable = 1",
		"SET GLOBAL no_such_variable = 1",
		"SET @@global.no_such_variable = 1",
		"SET @@session.no_such_variable = 1",
		"SET autocommit = 1, no_such_variable = 2",
		"SET no_such_variable = 2, autocommit = 1",
		"SET autocommit = @@no_such_variable",
		"SET @a = @@no_such_variable",
		"Set @@no_such_variable = 1",
		"set no_such_variable=1;",
	] {
		let (code, state, message) = client.error(sql).await;
		assert_eq!(
			(code, state.as_str(), message.as_str()),
			(1193, "HY000", "Unknown system variable 'no_such_variable'"),
			"{sql}"
		);
	}

	// The statements a driver sends as it connects set variables MySQL has.
	for sql in [
		"SET NAMES utf8mb4",
		"SET autocommit = 1",
		"SET SESSION autocommit = 1",
		"SET @@session.autocommit = 1",
		"SET character_set_results = NULL",
		"SET sql_mode = 'STRICT_TRANS_TABLES'",
		"SET time_zone = '+00:00'",
		"SET wait_timeout = 28800, interactive_timeout = 28800",
	] {
		client.ok(sql).await;
	}
	// A variable MySQL has and this server cannot set is not an unknown variable.
	let (code, _, message) = client.error("SET @@max_connections = 5").await;
	assert_eq!(code, 1235, "{message}");

	shutdown.cancel();
}

#[tokio::test]
async fn text_that_only_spells_the_name_is_not_a_reference_to_the_variable() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	assert_eq!(
		client.ok("SELECT '@@no_such_variable' AS s").await.row(),
		[Some("@@no_such_variable")]
	);
	assert_eq!(client.ok("SELECT 1 AS one /* @@no_such_variable */").await.row(), [Some("1")]);
	assert_eq!(client.ok("SELECT 1 AS one -- @@no_such_variable").await.row(), [Some("1")]);
	assert_eq!(client.ok("SELECT 1 AS `@@no_such_variable`").await.names(), ["@@no_such_variable"]);
	assert_eq!(client.ok("SELECT @no_such_variable AS v").await.row(), [None]);

	shutdown.cancel();
}

#[tokio::test]
async fn the_names_of_the_8_0_series_that_8_4_dropped_read_as_null() {
	let (addr, shutdown) = start_server().await;
	let mut client = connect(addr).await;

	// The server announces an 8.0 version, and MySQL 8.0 answers these.
	assert!(client.server_version.starts_with("8.0."));
	let rows = client
		.ok(
			"SELECT @@have_ssl AS a, @@default_authentication_plugin AS b, @@expire_logs_days AS c, \
			 @@global.master_info_repository AS d",
		)
		.await;
	assert_eq!(rows.row(), [None, None, None, None]);

	shutdown.cancel();
}

/// What each driver or tool sends to read its settings as it connects, which a real MySQL server
/// answers.
const DRIVER_SETUP: &[(&str, &str, usize)] = &[
	("the mysql client", "select @@version_comment limit 1", 1),
	("Go", "SELECT @@max_allowed_packet", 1),
	(
		"MariaDB Connector/J",
		"SELECT @@max_allowed_packet, @@system_time_zone, @@time_zone, @@auto_increment_increment",
		4,
	),
	("SQLAlchemy", "SELECT @@transaction_isolation", 1),
	("SQLAlchemy", "SELECT @@sql_mode", 1),
	("Connector/J", "SELECT @@session.transaction_read_only", 1),
	("Prisma", "SELECT @@version", 1),
	(
		"Django",
		"SELECT VERSION(), @@sql_mode, @@default_storage_engine, @@sql_auto_is_null, \
		 @@lower_case_table_names",
		5,
	),
	(
		"a client of the character set",
		"SELECT @@character_set_database, @@collation_database, @@character_set_system, \
		 @@character_set_filesystem",
		4,
	),
	(
		"a client of the timeouts",
		"SELECT @@wait_timeout, @@interactive_timeout, @@net_read_timeout, @@net_write_timeout",
		4,
	),
	("a monitor", "SELECT @@global.max_connections, @@global.sql_mode", 2),
	("a client of the session", "SELECT @@session.sql_auto_is_null, @@session.wait_timeout", 2),
];

#[tokio::test]
async fn what_drivers_read_as_they_connect_is_answered() {
	let (addr, shutdown) = start_server().await;
	let mut with_database = connect(addr).await;
	let mut no_database = connect_to(addr, None).await;

	for (driver, sql, columns) in DRIVER_SETUP {
		let rows = with_database.ok(sql).await;
		assert_eq!(rows.columns.len(), *columns, "{driver}: {sql}");
		assert_eq!(rows.rows.len(), 1, "{driver}: {sql}");
	}
	// Before the connection selects a database, those that read only variables.
	for (driver, sql, columns) in
		DRIVER_SETUP.iter().filter(|(_, sql, _)| !sql.contains("VERSION()"))
	{
		let rows = no_database.ok(sql).await;
		assert_eq!(rows.columns.len(), *columns, "{driver}: {sql}");
		assert_eq!(rows.rows.len(), 1, "{driver}: {sql}");
	}

	shutdown.cancel();
}
