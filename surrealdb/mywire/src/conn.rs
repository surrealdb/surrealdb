//! MySQL client connection handler.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::verify::basic;
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_core::syn;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::auth::*;
use crate::codec::*;
use crate::constants::*;
use crate::msg::*;
use crate::query::translate_query;
use crate::typing::*;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
static NEXT_CONN_ID: AtomicU32 = AtomicU32::new(1);

pub struct Connection {
	reader: PacketReader<OwnedReadHalf>,
	writer: PacketWriter<OwnedWriteHalf>,
	_peer: SocketAddr,
	ds: Arc<Datastore>,
	_ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	session: Session,
	connection_id: u32,
	auth_salt: [u8; 20],
	client_capabilities: u32,
	current_ns: Option<String>,
	current_db: Option<String>,
	_permit: OwnedSemaphorePermit,
}

impl Connection {
	pub fn new(
		stream: TcpStream,
		peer: SocketAddr,
		ds: Arc<Datastore>,
		ready: Arc<AtomicBool>,
		shutdown: CancellationToken,
		permit: OwnedSemaphorePermit,
	) -> Self {
		let (read_half, write_half) = stream.into_split();
		let connection_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
		let auth_salt = generate_auth_salt();

		let session = Session {
			ip: Some(peer.ip().to_string()),
			id: Some(uuid::Uuid::new_v4()),
			..Default::default()
		};

		Self {
			reader: PacketReader::new(read_half),
			writer: PacketWriter::new(write_half),
			_peer: peer,
			ds,
			_ready: ready,
			shutdown,
			session,
			connection_id,
			auth_salt,
			client_capabilities: 0,
			current_ns: None,
			current_db: None,
			_permit: permit,
		}
	}

	pub async fn run(&mut self) -> Result<()> {
		// Complete handshake within timeout
		let authed = match timeout(STARTUP_TIMEOUT, self.handshake()).await {
			Ok(res) => res?,
			Err(_) => {
				let err = ErrPacket::general(1159, b"08S01", "Handshake timeout");
				let _ = self.writer.write_packet(2, &err.serialize()).await;
				return Ok(());
			}
		};

		if !authed {
			return Ok(());
		}

		self.command_loop().await
	}

	async fn handshake(&mut self) -> Result<bool> {
		// 1. Send HandshakeV10 packet (seq 0)
		let handshake = HandshakeV10 {
			server_version: "8.0.35-SurrealDB".to_string(),
			connection_id: self.connection_id,
			auth_plugin_data: self.auth_salt,
			capabilities: DEFAULT_SERVER_CAPABILITIES,
			character_set: CHARSET_UTF8MB4_GENERAL_CI as u8,
			status_flags: SERVER_STATUS_AUTOCOMMIT,
			auth_plugin_name: AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD.to_string(),
		};

		self.writer.write_packet(0, &handshake.serialize()).await?;

		// 2. Read HandshakeResponse41 (seq 1)
		let (seq, payload) = self.reader.read_packet().await?;
		let response = match HandshakeResponse41::deserialize(&payload) {
			Ok(resp) => resp,
			Err(e) => {
				let err = ErrPacket::general(1045, b"28000", format!("Malformed handshake: {e}"));
				self.writer.write_packet(seq.wrapping_add(1), &err.serialize()).await?;
				return Ok(false);
			}
		};

		self.client_capabilities = response.capabilities;

		// Set initial database if requested
		if let Some(ref db) = response.database {
			self.set_current_db(db).await?;
		}

		// Authenticate
		let username = response.username.trim();
		let scramble = response.auth_response;

		let auth_ok = if username.is_empty() || username == "root" {
			// Check if datastore allows unauthenticated queries or empty password
			if self.ds.allows_query_by_subject(self.session.au.as_ref()) {
				true
			} else {
				// Try basic auth with empty or common root password if scramble matches
				let matches_empty = verify_mysql_native_password(&scramble, &self.auth_salt, "");
				let matches_root = verify_mysql_native_password(&scramble, &self.auth_salt, "root");
				if matches_empty {
					basic(&self.ds, &mut self.session, "root", "", None, None).await.is_ok()
				} else if matches_root {
					basic(&self.ds, &mut self.session, "root", "root", None, None).await.is_ok()
				} else {
					false
				}
			}
		} else {
			self.ds.allows_query_by_subject(self.session.au.as_ref())
		};

		if auth_ok {
			let ok = OkPacket::new(0, 0);
			self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
			Ok(true)
		} else {
			let err = ErrPacket::access_denied(format!("Access denied for user '{}'", username));
			self.writer.write_packet(seq.wrapping_add(1), &err.serialize()).await?;
			Ok(false)
		}
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

		let _ = self
			.ds
			.process_use(None, &mut self.session, Some(ns.to_string()), Some(db.to_string()))
			.await;
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
			let user = "root@localhost";
			let mut res_seq = seq.wrapping_add(1);
			self.emit_scalar_result("USER()", user, &mut res_seq).await?;
			return Ok(());
		}

		if lower == "select version()" {
			let ver = "8.0.35-SurrealDB";
			let mut res_seq = seq.wrapping_add(1);
			self.emit_scalar_result("VERSION()", ver, &mut res_seq).await?;
			return Ok(());
		}

		// Translate MySQL query if needed
		let config = self.ds.parser_config();
		let translated;
		let query_to_run =
			match syn::parse_with_capabilities(trimmed, &self.ds.get_capabilities(), &config) {
				Ok(_) => trimmed,
				Err(_) => match translate_query(trimmed) {
					Ok(lowered) => {
						translated = lowered;
						&translated
					}
					Err(err) => {
						let err_pkt = ErrPacket::syntax(format!("MySQL translation error: {err}"));
						self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
						return Ok(());
					}
				},
			};

		// Execute against datastore
		let res = self.ds.run(QueryRequest::new(query_to_run, &self.session)).await;
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
