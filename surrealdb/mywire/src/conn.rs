//! MySQL client connection handler.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::verify::basic;
use surrealdb_core::kvs::{Datastore, Dialect, QueryRequest, QuerySource};
use surrealdb_rpc::capabilities::RouteTarget;
use tokio::io::{ReadHalf, WriteHalf, split};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;

use crate::LOG;
use crate::auth::*;
use crate::codec::*;
use crate::constants::*;
use crate::msg::*;
use crate::typing::*;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
static NEXT_CONN_ID: AtomicU32 = AtomicU32::new(1);

pub(crate) type MyStream = Either<TcpStream, TlsStream<TcpStream>>;

pub struct Connection {
	reader: PacketReader<ReadHalf<MyStream>>,
	writer: PacketWriter<WriteHalf<MyStream>>,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	_ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	session: Session,
	_connection_id: u32,
	_auth_salt: [u8; 20],
	_client_capabilities: u32,
	current_ns: Option<String>,
	current_db: Option<String>,
	_is_tls: bool,
	username: String,
	dialect: Dialect,
	_permit: OwnedSemaphorePermit,
}

/// Handle a single incoming MySQL client connection.
pub async fn handle(
	stream: TcpStream,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	permit: OwnedSemaphorePermit,
	acceptor: Option<Arc<TlsAcceptor>>,
) {
	// 1. Check capability: RouteTarget::MySql
	if !ds.allows_http_route(&RouteTarget::MySql) {
		let mut writer = PacketWriter::new(stream);
		let err = ErrPacket::general(
			1045,
			b"28000",
			"MySQL wire protocol connections are disabled by capability configuration",
		);
		let _ = writer.write_packet(2, &err.serialize()).await;
		return;
	}

	// 2. Check readiness
	if !ready.load(Ordering::SeqCst) {
		let mut writer = PacketWriter::new(stream);
		let err = ErrPacket::general(1045, b"08S01", "The database system is starting up");
		let _ = writer.write_packet(2, &err.serialize()).await;
		return;
	}

	// 3. Run handshake with timeout
	let conn_res = timeout(
		STARTUP_TIMEOUT,
		perform_handshake(stream, peer, ds, ready, shutdown.clone(), permit, acceptor),
	)
	.await;

	match conn_res {
		Ok(Ok(Some(mut conn))) => {
			if let Err(err) = conn.command_loop().await {
				debug!(target: LOG, "MySQL connection from {peer} ended with error: {err}");
			}
		}
		Ok(Ok(None)) => {
			// Handshake ended cleanly (e.g. auth failed, error packet already sent)
		}
		Ok(Err(err)) => {
			debug!(target: LOG, "MySQL connection handshake from {peer} failed: {err}");
		}
		Err(_) => {
			debug!(target: LOG, "MySQL connection handshake from {peer} timed out");
		}
	}
}

async fn perform_handshake(
	mut stream: TcpStream,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	permit: OwnedSemaphorePermit,
	acceptor: Option<Arc<TlsAcceptor>>,
) -> Result<Option<Connection>> {
	let connection_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
	let auth_salt = generate_auth_salt();

	let server_capabilities = if acceptor.is_some() {
		DEFAULT_SERVER_CAPABILITIES | CLIENT_SSL
	} else {
		DEFAULT_SERVER_CAPABILITIES
	};

	// 1. Send HandshakeV10 packet (seq 0)
	let handshake = HandshakeV10 {
		server_version: "8.0.35-SurrealDB".to_string(),
		connection_id,
		auth_plugin_data: auth_salt,
		capabilities: server_capabilities,
		character_set: CHARSET_UTF8MB4_GENERAL_CI as u8,
		status_flags: SERVER_STATUS_AUTOCOMMIT,
		auth_plugin_name: AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD.to_string(),
	};

	{
		let mut writer = PacketWriter::new(&mut stream);
		writer.write_packet(0, &handshake.serialize()).await?;
	}

	// 2. Read first response packet (seq 1)
	let (_seq, payload) = {
		let mut reader = PacketReader::new(&mut stream);
		reader.read_packet().await?
	};

	// 3. Negotiate TLS upgrade if SSL requested
	let (mut reader, mut writer, hs_payload, is_tls) = if SslRequest::is_ssl_request(&payload) {
		let acceptor = acceptor
			.as_ref()
			.ok_or_else(|| anyhow::anyhow!("SSL requested but no TLS certificate configured"))?;
		let tls_stream = acceptor.accept(stream).await?;
		let my_stream = Either::Right(tls_stream);
		let (read_half, write_half) = split(my_stream);
		let mut reader = PacketReader::new(read_half);
		let writer = PacketWriter::new(write_half);
		let (_hs_seq, hs_payload) = reader.read_packet().await?;
		(reader, writer, hs_payload, true)
	} else {
		let my_stream = Either::Left(stream);
		let (read_half, write_half) = split(my_stream);
		let reader = PacketReader::new(read_half);
		let writer = PacketWriter::new(write_half);
		(reader, writer, payload, false)
	};

	// 4. Parse HandshakeResponse41
	let response = match HandshakeResponse41::deserialize(&hs_payload) {
		Ok(resp) => resp,
		Err(e) => {
			let err = ErrPacket::general(1045, b"28000", format!("Malformed handshake: {e}"));
			let _ = writer.write_packet(2, &err.serialize()).await;
			return Ok(None);
		}
	};

	// 5. Authenticate
	let username = response.username.trim().to_string();
	let mut session = Session {
		ip: Some(peer.ip().to_string()),
		id: Some(uuid::Uuid::new_v4()),
		..Default::default()
	};

	let (auth_ok, final_seq) = if username.is_empty() {
		// Check if datastore allows unauthenticated queries (guest)
		(ds.allows_query_by_subject(session.au.as_ref()), 2)
	} else {
		// Authenticated user requested.
		// Enforce TLS or loopback connection
		let is_secure = is_tls || peer.ip().is_loopback();
		if !is_secure {
			let err = ErrPacket::general(
				1045,
				b"28000",
				"Authentication requires an encrypted (TLS) connection or loopback",
			);
			let _ = writer.write_packet(2, &err.serialize()).await;
			return Ok(None);
		}

		let (password, resp_seq) = if response.auth_plugin_name.as_deref()
			== Some("mysql_clear_password")
			&& !response.auth_response.is_empty()
		{
			let clean = if response.auth_response.ends_with(&[0]) {
				&response.auth_response[..response.auth_response.len() - 1]
			} else {
				&response.auth_response[..]
			};
			(std::str::from_utf8(clean).unwrap_or("").to_string(), 2)
		} else {
			// Send AuthSwitchRequest for mysql_clear_password
			let auth_switch = AuthSwitchRequest::clear_password();
			writer.write_packet(2, &auth_switch.serialize()).await?;

			let (_auth_seq, auth_payload) = reader.read_packet().await?;
			let clean = if auth_payload.ends_with(&[0]) {
				&auth_payload[..auth_payload.len() - 1]
			} else {
				&auth_payload[..]
			};
			(std::str::from_utf8(clean).unwrap_or("").to_string(), 3)
		};

		// Parse namespace/database from response.database if present
		let (ns_opt, db_opt) = match response.database.as_deref() {
			Some(db_name) => {
				let parts: Vec<&str> = db_name.split('/').collect();
				if parts.len() >= 2 {
					(Some(parts[0]), Some(parts[1]))
				} else {
					(None, Some(db_name))
				}
			}
			None => (None, None),
		};

		let ok = if let (Some(ns), Some(db)) = (ns_opt, db_opt) {
			if basic(&ds, &mut session, &username, &password, Some(ns), Some(db)).await.is_ok() {
				true
			} else {
				basic(&ds, &mut session, &username, &password, None, None).await.is_ok()
			}
		} else {
			basic(&ds, &mut session, &username, &password, None, None).await.is_ok()
		};

		(ok, resp_seq)
	};

	if !auth_ok {
		let err = ErrPacket::access_denied(format!("Access denied for user '{}'", username));
		let _ = writer.write_packet(final_seq, &err.serialize()).await;
		return Ok(None);
	}

	// Send OK packet
	let ok = OkPacket::new(0, 0);
	writer.write_packet(final_seq, &ok.serialize()).await?;

	let mut conn = Connection {
		reader,
		writer,
		peer,
		ds,
		_ready: ready,
		shutdown,
		session,
		_connection_id: connection_id,
		_auth_salt: auth_salt,
		_client_capabilities: response.capabilities,
		current_ns: None,
		current_db: None,
		_is_tls: is_tls,
		username: if username.is_empty() {
			"unauthenticated".to_string()
		} else {
			username
		},
		dialect: Dialect::MySql,
		_permit: permit,
	};

	// 6. Set initial database if requested, without swallowing errors
	if let Some(ref db) = response.database {
		conn.set_current_db(db).await?;
	}

	Ok(Some(conn))
}

impl Connection {
	fn check_dialect(&self, dialect: Dialect) -> Result<()> {
		let allowed = match dialect {
			Dialect::MySql => self.ds.allows_http_route(&RouteTarget::MySql),
			Dialect::PgSql => self.ds.allows_http_route(&RouteTarget::Postgres),
			Dialect::Gql => self.ds.allows_http_route(&RouteTarget::Gql),
			Dialect::SurrealQl => self.ds.allows_http_route(&RouteTarget::MySql),
		};
		if !allowed {
			anyhow::bail!("Forbidden: the {dialect:?} dialect is not allowed on this server");
		}
		Ok(())
	}

	fn select_dialect(&mut self, dialect: Dialect) -> Result<()> {
		self.check_dialect(dialect)?;
		self.dialect = dialect;
		Ok(())
	}

	async fn set_current_db(&mut self, db_name: &str) -> Result<()> {
		let parts: Vec<&str> = db_name.split('/').collect();
		let (ns, db) = if parts.len() >= 2 {
			(parts[0], parts[1])
		} else {
			("test", db_name)
		};

		self.current_ns = Some(ns.to_string());
		self.current_db = Some(db.to_string());

		self.ds
			.process_use(None, &mut self.session, Some(ns.to_string()), Some(db.to_string()))
			.await?;
		Ok(())
	}

	async fn command_loop(&mut self) -> Result<()> {
		loop {
			tokio::select! {
				_ = self.shutdown.cancelled() => return Ok(()),
				res = self.reader.read_packet() => {
					let (seq, payload) = match res {
						Ok(val) => val,
						Err(_) => return Ok(()), // EOF or disconnect
					};

					if payload.is_empty() {
						continue;
					}

					let cmd = payload[0];
					let data = &payload[1..];

					match cmd {
						COM_QUIT => return Ok(()),
						COM_PING => {
							let ok = OkPacket::new(0, 0);
							self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
						}
						COM_INIT_DB => {
							let db_name = String::from_utf8_lossy(data);
							self.set_current_db(db_name.trim()).await?;
							let ok = OkPacket::new(0, 0);
							self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
						}
						COM_QUERY => {
							let sql = String::from_utf8_lossy(data);
							self.handle_query(&sql, seq).await?;
						}
						_ => {
							let err = ErrPacket::general(1047, b"08S01", format!("Unknown MySQL command: 0x{cmd:02x}"));
							self.writer.write_packet(seq.wrapping_add(1), &err.serialize()).await?;
						}
					}
				}
			}
		}
	}

	async fn handle_query(&mut self, sql: &str, seq: u8) -> Result<()> {
		let trimmed = sql.trim().trim_end_matches(';').trim();

		// Handle connection configuration / variable inspection queries
		if trimmed.eq_ignore_ascii_case("SELECT 1") {
			let mut res_seq = seq.wrapping_add(1);
			self.emit_scalar_result("1", "1", &mut res_seq).await?;
			return Ok(());
		}

		if let Some(res) = parse_mywire_set_dialect(trimmed) {
			match res {
				Ok(dialect) => {
					if let Err(e) = self.select_dialect(dialect) {
						let err_pkt = ErrPacket::general(1045, b"28000", e.to_string());
						self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
						return Ok(());
					}
					let ok = OkPacket::new(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
				Err(err_msg) => {
					let err_pkt = ErrPacket::general(1238, b"42000", err_msg);
					self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
					return Ok(());
				}
			}
		}

		if trimmed.starts_with("SET ") || trimmed.starts_with("set ") {
			let ok = OkPacket::new(0, 0);
			self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
			return Ok(());
		}

		// Handle common introspection queries from drivers
		let lower = trimmed.to_ascii_lowercase();
		if lower.starts_with("select @@") {
			return self.handle_variable_select(trimmed, seq).await;
		}

		if lower == "select database()" {
			let db_name = self.current_db.clone().unwrap_or_else(|| "test".to_string());
			let mut res_seq = seq.wrapping_add(1);
			self.emit_scalar_result("DATABASE()", &db_name, &mut res_seq).await?;
			return Ok(());
		}

		if lower == "select user()" || lower == "select current_user()" {
			let user = format!("{}@{}", self.username, self.peer.ip());
			let mut res_seq = seq.wrapping_add(1);
			self.emit_scalar_result("USER()", &user, &mut res_seq).await?;
			return Ok(());
		}

		if lower == "select version()" {
			let ver = "8.0.35-SurrealDB";
			let mut res_seq = seq.wrapping_add(1);
			self.emit_scalar_result("VERSION()", ver, &mut res_seq).await?;
			return Ok(());
		}

		// Check dialect capability before execution
		if let Err(e) = self.check_dialect(self.dialect) {
			let err_pkt = ErrPacket::general(1045, b"28000", e.to_string());
			self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
			return Ok(());
		}

		// Execute against datastore with the selected dialect.
		// Fail-closed execution: NEVER fall back to SurrealQL on error!
		let req = match self.dialect {
			Dialect::MySql => QueryRequest::new(QuerySource::mysql(trimmed), &self.session),
			Dialect::SurrealQl => QueryRequest::new(trimmed, &self.session),
			Dialect::PgSql => QueryRequest::new(QuerySource::pgsql(trimmed), &self.session),
			Dialect::Gql => QueryRequest::new(QuerySource::gql(trimmed), &self.session),
		};
		let res = self.ds.run(req).await;
		match res {
			Ok(results) => {
				let mut res_seq = seq.wrapping_add(1);
				if results.is_empty() {
					let ok = OkPacket::new(0, 0);
					self.writer.write_packet(res_seq, &ok.serialize()).await?;
					return Ok(());
				}

				for r in results {
					match r.result {
						Ok(val) => {
							self.emit_value_result(val, &mut res_seq).await?;
						}
						Err(e) => {
							let err_pkt = ErrPacket::general(1064, b"42000", e.to_string());
							self.writer.write_packet(res_seq, &err_pkt.serialize()).await?;
							return Ok(());
						}
					}
				}
				Ok(())
			}
			Err(err) => {
				let err_pkt = ErrPacket::general(1064, b"42000", err.to_string());
				self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
				Ok(())
			}
		}
	}

	async fn handle_variable_select(&mut self, sql: &str, seq: u8) -> Result<()> {
		let mut res_seq = seq.wrapping_add(1);
		// Return friendly defaults for common MySQL variables
		if sql.contains("version_comment") {
			self.emit_scalar_result("@@version_comment", "SurrealDB", &mut res_seq).await?;
		} else if sql.contains("sql_mode") {
			self.emit_scalar_result(
				"@@sql_mode",
				"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
				&mut res_seq,
			).await?;
		} else if sql.contains("character_set") {
			self.emit_scalar_result("@@character_set_client", "utf8mb4", &mut res_seq).await?;
		} else {
			self.emit_scalar_result("@@var", "1", &mut res_seq).await?;
		}
		Ok(())
	}

	async fn emit_scalar_result(&mut self, col_name: &str, val: &str, seq: &mut u8) -> Result<()> {
		let schema = self.current_db.as_deref().unwrap_or("test");
		let col = ColumnDefinition41::new(schema, "", col_name, MYSQL_TYPE_VAR_STRING);

		// 1. Column count packet
		let mut count_buf = Vec::new();
		write_lenenc_int(&mut count_buf, 1);
		self.writer.write_packet(*seq, &count_buf).await?;
		*seq = seq.wrapping_add(1);

		// 2. Column definition packet
		self.writer.write_packet(*seq, &col.serialize()).await?;
		*seq = seq.wrapping_add(1);

		// 3. EOF packet separating columns and rows
		let eof = EofPacket::default();
		self.writer.write_packet(*seq, &eof.serialize()).await?;
		*seq = seq.wrapping_add(1);

		// 4. Text row packet
		let row_buf = encode_text_row(&[Some(val.to_string())]);
		self.writer.write_packet(*seq, &row_buf).await?;
		*seq = seq.wrapping_add(1);

		// 5. Final EOF packet
		self.writer.write_packet(*seq, &eof.serialize()).await?;
		*seq = seq.wrapping_add(1);

		Ok(())
	}

	async fn emit_value_result(&mut self, val: surrealdb_types::Value, seq: &mut u8) -> Result<()> {
		let schema = self.current_db.as_deref().unwrap_or("test");
		let shape = shape_result(schema, "", val);

		if shape.columns.is_empty() {
			let ok = OkPacket::new(0, 0);
			self.writer.write_packet(*seq, &ok.serialize()).await?;
			*seq = seq.wrapping_add(1);
			return Ok(());
		}

		// 1. Column count packet
		let mut count_buf = Vec::new();
		write_lenenc_int(&mut count_buf, shape.columns.len() as u64);
		self.writer.write_packet(*seq, &count_buf).await?;
		*seq = seq.wrapping_add(1);

		// 2. Column definitions
		for col in &shape.columns {
			self.writer.write_packet(*seq, &col.serialize()).await?;
			*seq = seq.wrapping_add(1);
		}

		// 3. EOF packet
		let eof = EofPacket::default();
		self.writer.write_packet(*seq, &eof.serialize()).await?;
		*seq = seq.wrapping_add(1);

		// 4. Rows
		for row in &shape.rows {
			let row_buf = encode_text_row(row);
			self.writer.write_packet(*seq, &row_buf).await?;
			*seq = seq.wrapping_add(1);
		}

		// 5. Final EOF packet
		self.writer.write_packet(*seq, &eof.serialize()).await?;
		*seq = seq.wrapping_add(1);

		Ok(())
	}
}

/// Parse a `SET dialect = ...` or `SET dialect TO ...` statement.
fn parse_mywire_set_dialect(trimmed: &str) -> Option<std::result::Result<Dialect, String>> {
	let lower = trimmed.to_ascii_lowercase();
	let rest = lower.strip_prefix("set")?.trim();
	if !rest.starts_with("dialect") {
		return None;
	}
	let after = rest.strip_prefix("dialect")?.trim();
	let after = after.strip_prefix('=').or_else(|| after.strip_prefix("to"))?.trim();
	let val = after.trim_matches('\'').trim_matches('"').trim();
	match val {
		"mysql" | "sql" => Some(Ok(Dialect::MySql)),
		"surrealql" | "surql" => Some(Ok(Dialect::SurrealQl)),
		"postgres" | "postgresql" | "pgsql" => Some(Ok(Dialect::PgSql)),
		"gql" => Some(Ok(Dialect::Gql)),
		other => Some(Err(format!("Variable 'dialect' cannot be set to the value of '{other}'"))),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_parse_mywire_set_dialect() {
		assert_eq!(parse_mywire_set_dialect("SET dialect = 'mysql'"), Some(Ok(Dialect::MySql)));
		assert_eq!(
			parse_mywire_set_dialect("set dialect='surrealql'"),
			Some(Ok(Dialect::SurrealQl))
		);
		assert_eq!(parse_mywire_set_dialect("SET dialect TO \"pgsql\""), Some(Ok(Dialect::PgSql)));
		assert_eq!(parse_mywire_set_dialect("SET dialect = gql"), Some(Ok(Dialect::Gql)));
		assert!(parse_mywire_set_dialect("SET dialect = unknown").unwrap().is_err());
		assert!(parse_mywire_set_dialect("SET autocommit = 1").is_none());
	}
}
