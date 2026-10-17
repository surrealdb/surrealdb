//! MySQL client connection handler.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::verify::basic;
use surrealdb_core::kvs::{Datastore, Dialect, QueryRequest, QuerySource};
use surrealdb_datastore::Transaction;
use surrealdb_kvs::TransactionType;
use surrealdb_mysql::ast::stmt::{Statement as MyStatement, TableRef};
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_sqlplan::SqlDialect;
use surrealdb_sqlplan::binder::mysql::bind_select;
use surrealdb_sqlplan::catalog::MemoryCatalog;
use surrealdb_sqlplan::dialect::DialectPolicy;
use surrealdb_sqlplan::observability::global_metrics;
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

/// State of a prepared statement on this connection.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
	pub id: u32,
	pub query: String,
	pub param_count: u16,
	pub param_types: Vec<(u8, u8)>,
	pub column_defs: Vec<ColumnDefinition41>,
	pub long_data: HashMap<u16, Vec<u8>>,
}

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
	client_capabilities: u32,
	current_ns: Option<String>,
	current_db: Option<String>,
	_is_tls: bool,
	username: String,
	dialect: Dialect,
	transaction: Option<Arc<Transaction>>,
	savepoints: Vec<String>,
	autocommit: bool,
	_permit: OwnedSemaphorePermit,
	prepared_statements: HashMap<u32, PreparedStatement>,
	next_statement_id: u32,
}

/// Handle a single incoming MySQL client connection.
#[allow(clippy::too_many_arguments)]
pub async fn handle(
	stream: TcpStream,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	permit: OwnedSemaphorePermit,
	acceptor: Option<Arc<TlsAcceptor>>,
	auth_cache: Arc<FastAuthCache>,
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
		perform_handshake(stream, peer, ds, ready, shutdown.clone(), permit, acceptor, auth_cache),
	)
	.await;

	match conn_res {
		Ok(Ok(Some(mut conn))) => {
			if let Err(err) = conn.command_loop().await {
				debug!(target: LOG, "MySQL connection from {peer} ended with error: {err}");
			}
			if let Some(tx) = conn.transaction.take() {
				let _ = tx.cancel().await;
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

fn clean_password(payload: &[u8]) -> String {
	let clean = if payload.ends_with(&[0]) {
		&payload[..payload.len() - 1]
	} else {
		payload
	};
	std::str::from_utf8(clean).unwrap_or("").to_string()
}

async fn verify_credentials(
	ds: &Datastore,
	session: &mut Session,
	username: &str,
	password: &str,
	ns_opt: Option<&str>,
	db_opt: Option<&str>,
) -> bool {
	if let (Some(ns), Some(db)) = (ns_opt, db_opt)
		&& basic(ds, session, username, password, Some(ns), Some(db)).await.is_ok()
	{
		return true;
	}
	basic(ds, session, username, password, None, None).await.is_ok()
}

#[allow(clippy::too_many_arguments)]
async fn perform_handshake(
	mut stream: TcpStream,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	permit: OwnedSemaphorePermit,
	acceptor: Option<Arc<TlsAcceptor>>,
	auth_cache: Arc<FastAuthCache>,
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
		auth_plugin_name: AUTH_PLUGIN_CACHING_SHA2_PASSWORD.to_string(),
	};

	{
		let mut writer = PacketWriter::new(&mut stream);
		writer.write_packet(0, &handshake.serialize()).await?;
	}

	// 2. Read first response packet (seq 1)
	let (first_seq, payload) = {
		let mut reader = PacketReader::new(&mut stream);
		reader.read_packet().await?
	};

	// 3. Negotiate TLS upgrade if SSL requested
	let (mut reader, mut writer, hs_payload, hs_seq, is_tls) =
		if SslRequest::is_ssl_request(&payload) {
			let Some(acceptor) = acceptor.as_ref() else {
				let mut writer = PacketWriter::new(stream);
				let err = ErrPacket::general(
					1045,
					b"28000",
					"SSL/TLS requested but not configured on this server",
				);
				let _ = writer.write_packet(first_seq.wrapping_add(1), &err.serialize()).await;
				return Ok(None);
			};
			let tls_stream = acceptor.accept(stream).await?;
			let my_stream = Either::Right(tls_stream);
			let (read_half, write_half) = split(my_stream);
			let mut reader = PacketReader::new(read_half);
			let writer = PacketWriter::new(write_half);
			let (hs_seq, hs_payload) = reader.read_packet().await?;
			(reader, writer, hs_payload, hs_seq, true)
		} else {
			let my_stream = Either::Left(stream);
			let (read_half, write_half) = split(my_stream);
			let reader = PacketReader::new(read_half);
			let writer = PacketWriter::new(write_half);
			(reader, writer, payload, first_seq, false)
		};

	let mut cur_seq = hs_seq.wrapping_add(1);

	// 4. Parse HandshakeResponse41
	let response = match HandshakeResponse41::deserialize(&hs_payload) {
		Ok(resp) => resp,
		Err(e) => {
			let err = ErrPacket::general(1045, b"28000", format!("Malformed handshake: {e}"));
			let _ = writer.write_packet(cur_seq, &err.serialize()).await;
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

	let is_secure = is_tls || peer.ip().is_loopback();

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

	let client_plugin =
		response.auth_plugin_name.as_deref().unwrap_or(AUTH_PLUGIN_CACHING_SHA2_PASSWORD);

	if username.is_empty() {
		// Guest
		if !ds.allows_query_by_subject(session.au.as_ref()) {
			let err = ErrPacket::access_denied("Access denied for user ''".to_string());
			let _ = writer.write_packet(cur_seq, &err.serialize()).await;
			return Ok(None);
		}
		let ok = OkPacket::new(0, 0);
		writer.write_packet(cur_seq, &ok.serialize()).await?;
	} else if client_plugin == AUTH_PLUGIN_CACHING_SHA2_PASSWORD {
		let mut fast_auth_succeeded = false;
		if response.auth_response.len() == 32
			&& let Some(cached) = auth_cache.get(&username)
			&& verify_caching_sha2_password_fast_auth(
				&response.auth_response,
				&auth_salt,
				&cached.sha2_stage2,
			) {
			if let Some(ref au) = cached.session_auth {
				session.au = Arc::clone(au);
			}
			session.exp = cached.session_exp;
			fast_auth_succeeded = true;
		}

		if fast_auth_succeeded {
			let more_data = AuthMoreData::fast_auth_success();
			writer.write_packet(cur_seq, &more_data.serialize()).await?;
			cur_seq = cur_seq.wrapping_add(1);

			let ok = OkPacket::new(0, 0);
			writer.write_packet(cur_seq, &ok.serialize()).await?;
		} else {
			if !is_secure {
				let err = ErrPacket::general(
					1045,
					b"28000",
					"Authentication requires an encrypted (TLS) connection or loopback",
				);
				let _ = writer.write_packet(cur_seq, &err.serialize()).await;
				return Ok(None);
			}

			let more_data = AuthMoreData::perform_full_authentication();
			writer.write_packet(cur_seq, &more_data.serialize()).await?;

			let (resp_seq, pass_payload) = reader.read_packet().await?;
			cur_seq = resp_seq.wrapping_add(1);

			let password = clean_password(&pass_payload);
			let ok =
				verify_credentials(&ds, &mut session, &username, &password, ns_opt, db_opt).await;

			if !ok {
				let err =
					ErrPacket::access_denied(format!("Access denied for user '{}'", username));
				let _ = writer.write_packet(cur_seq, &err.serialize()).await;
				return Ok(None);
			}

			auth_cache.insert(&username, &password, Some(Arc::clone(&session.au)), session.exp);

			let ok = OkPacket::new(0, 0);
			writer.write_packet(cur_seq, &ok.serialize()).await?;
		}
	} else if client_plugin == AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD {
		let mut fast_auth_succeeded = false;
		if response.auth_response.len() == 20
			&& let Some(cached) = auth_cache.get(&username)
			&& verify_mysql_native_password_fast_auth(
				&response.auth_response,
				&auth_salt,
				&cached.native_stage2,
			) {
			if let Some(ref au) = cached.session_auth {
				session.au = Arc::clone(au);
			}
			session.exp = cached.session_exp;
			fast_auth_succeeded = true;
		}

		if fast_auth_succeeded {
			let ok = OkPacket::new(0, 0);
			writer.write_packet(cur_seq, &ok.serialize()).await?;
		} else {
			if !is_secure {
				let err = ErrPacket::general(
					1045,
					b"28000",
					"Authentication requires an encrypted (TLS) connection or loopback",
				);
				let _ = writer.write_packet(cur_seq, &err.serialize()).await;
				return Ok(None);
			}

			let auth_switch = AuthSwitchRequest::clear_password();
			writer.write_packet(cur_seq, &auth_switch.serialize()).await?;

			let (resp_seq, pass_payload) = reader.read_packet().await?;
			cur_seq = resp_seq.wrapping_add(1);

			let password = clean_password(&pass_payload);
			let ok =
				verify_credentials(&ds, &mut session, &username, &password, ns_opt, db_opt).await;

			if !ok {
				let err =
					ErrPacket::access_denied(format!("Access denied for user '{}'", username));
				let _ = writer.write_packet(cur_seq, &err.serialize()).await;
				return Ok(None);
			}

			auth_cache.insert(&username, &password, Some(Arc::clone(&session.au)), session.exp);

			let ok = OkPacket::new(0, 0);
			writer.write_packet(cur_seq, &ok.serialize()).await?;
		}
	} else if client_plugin == AUTH_PLUGIN_MYSQL_CLEAR_PASSWORD {
		if !is_secure {
			let err = ErrPacket::general(
				1045,
				b"28000",
				"Authentication requires an encrypted (TLS) connection or loopback",
			);
			let _ = writer.write_packet(cur_seq, &err.serialize()).await;
			return Ok(None);
		}

		let password = if !response.auth_response.is_empty() {
			clean_password(&response.auth_response)
		} else {
			let auth_switch = AuthSwitchRequest::clear_password();
			writer.write_packet(cur_seq, &auth_switch.serialize()).await?;

			let (resp_seq, pass_payload) = reader.read_packet().await?;
			cur_seq = resp_seq.wrapping_add(1);
			clean_password(&pass_payload)
		};

		let ok = verify_credentials(&ds, &mut session, &username, &password, ns_opt, db_opt).await;
		if !ok {
			let err = ErrPacket::access_denied(format!("Access denied for user '{}'", username));
			let _ = writer.write_packet(cur_seq, &err.serialize()).await;
			return Ok(None);
		}

		auth_cache.insert(&username, &password, Some(Arc::clone(&session.au)), session.exp);
		let ok = OkPacket::new(0, 0);
		writer.write_packet(cur_seq, &ok.serialize()).await?;
	} else {
		// Switch to caching_sha2_password
		let auth_switch = AuthSwitchRequest::caching_sha2_password(&auth_salt);
		writer.write_packet(cur_seq, &auth_switch.serialize()).await?;

		let (resp_seq, switch_resp) = reader.read_packet().await?;
		cur_seq = resp_seq.wrapping_add(1);

		let mut fast_auth_succeeded = false;
		if switch_resp.len() == 32
			&& let Some(cached) = auth_cache.get(&username)
			&& verify_caching_sha2_password_fast_auth(&switch_resp, &auth_salt, &cached.sha2_stage2)
		{
			if let Some(ref au) = cached.session_auth {
				session.au = Arc::clone(au);
			}
			session.exp = cached.session_exp;
			fast_auth_succeeded = true;
		}

		if fast_auth_succeeded {
			let more_data = AuthMoreData::fast_auth_success();
			writer.write_packet(cur_seq, &more_data.serialize()).await?;
			cur_seq = cur_seq.wrapping_add(1);

			let ok = OkPacket::new(0, 0);
			writer.write_packet(cur_seq, &ok.serialize()).await?;
		} else {
			if !is_secure {
				let err = ErrPacket::general(
					1045,
					b"28000",
					"Authentication requires an encrypted (TLS) connection or loopback",
				);
				let _ = writer.write_packet(cur_seq, &err.serialize()).await;
				return Ok(None);
			}

			let more_data = AuthMoreData::perform_full_authentication();
			writer.write_packet(cur_seq, &more_data.serialize()).await?;

			let (resp_seq, pass_payload) = reader.read_packet().await?;
			cur_seq = resp_seq.wrapping_add(1);

			let password = clean_password(&pass_payload);
			let ok =
				verify_credentials(&ds, &mut session, &username, &password, ns_opt, db_opt).await;

			if !ok {
				let err =
					ErrPacket::access_denied(format!("Access denied for user '{}'", username));
				let _ = writer.write_packet(cur_seq, &err.serialize()).await;
				return Ok(None);
			}

			auth_cache.insert(&username, &password, Some(Arc::clone(&session.au)), session.exp);
			let ok = OkPacket::new(0, 0);
			writer.write_packet(cur_seq, &ok.serialize()).await?;
		}
	}

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
		client_capabilities: response.capabilities,
		current_ns: None,
		current_db: None,
		_is_tls: is_tls,
		username: if username.is_empty() {
			"unauthenticated".to_string()
		} else {
			username
		},
		dialect: Dialect::MySql,
		transaction: None,
		savepoints: Vec::new(),
		autocommit: true,
		_permit: permit,
		prepared_statements: HashMap::new(),
		next_statement_id: 1,
	};

	// 6. Set initial database if requested, without swallowing errors
	if let Some(ref db) = response.database {
		conn.set_current_db(db).await?;
	}

	Ok(Some(conn))
}

impl Connection {
	pub fn client_capabilities(&self) -> u32 {
		self.client_capabilities
	}

	pub fn deprecate_eof(&self) -> bool {
		(self.client_capabilities & CLIENT_DEPRECATE_EOF) != 0
	}

	fn status_flags(&self) -> u16 {
		let mut flags = 0;
		if self.autocommit {
			flags |= SERVER_STATUS_AUTOCOMMIT;
		}
		if self.transaction.is_some() {
			flags |= SERVER_STATUS_IN_TRANS;
		}
		flags
	}

	fn ok_packet(&self, affected_rows: u64, last_insert_id: u64) -> OkPacket {
		OkPacket::new(affected_rows, last_insert_id).with_status_flags(self.status_flags())
	}

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
			(parts[0].to_string(), parts[1].to_string())
		} else {
			(self.current_ns.clone().unwrap_or_else(|| "test".to_string()), db_name.to_string())
		};

		self.current_ns = Some(ns.clone());
		self.current_db = Some(db.clone());

		self.ds.process_use(None, &mut self.session, Some(ns), Some(db)).await?;
		Ok(())
	}

	async fn command_loop(&mut self) -> Result<()> {
		loop {
			tokio::select! {
				_ = self.shutdown.cancelled() => return Ok(()),
				res = self.reader.read_packet() => {
					let (seq, payload) = match res {
						Ok(val) => val,
						Err(err) => {
							if let Some(too_large) = err.downcast_ref::<PacketTooLarge>() {
								let err_pkt = ErrPacket::general(
									1153,
									b"08S01",
									"Got a packet bigger than 'max_allowed_packet' bytes",
								);
								self.writer
									.write_packet(too_large.seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
							}
							return Ok(()); // EOF, disconnect or an oversized packet
						}
					};

					if payload.is_empty() {
						continue;
					}

					let cmd = payload[0];
					let data = &payload[1..];

					match cmd {
						COM_QUIT => return Ok(()),
						COM_PING => {
							let ok = self.ok_packet(0, 0);
							self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
						}
						COM_INIT_DB => {
							let db_name = String::from_utf8_lossy(data);
							if let Err(e) = self.set_current_db(db_name.trim()).await {
								let err_pkt = ErrPacket::bad_db(format!("Unknown database '{}': {e}", db_name.trim()));
								self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
							} else {
								let ok = self.ok_packet(0, 0);
								self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
							}
						}
						COM_QUERY => {
							let sql = String::from_utf8_lossy(data);
							self.handle_query(&sql, seq).await?;
						}
						COM_STMT_PREPARE => {
							self.handle_stmt_prepare(data, seq).await?;
						}
						COM_STMT_EXECUTE => {
							self.handle_stmt_execute(data, seq).await?;
						}
						COM_STMT_SEND_LONG_DATA => {
							self.handle_stmt_send_long_data(data).await?;
						}
						COM_STMT_CLOSE => {
							self.handle_stmt_close(data).await?;
						}
						COM_STMT_RESET => {
							self.handle_stmt_reset(data, seq).await?;
						}
						COM_STMT_FETCH => {
							self.handle_stmt_fetch(data, seq).await?;
						}
						COM_RESET_CONNECTION => {
							self.prepared_statements.clear();
							self.next_statement_id = 1;
							self.autocommit = true;
							if let Some(tx) = self.transaction.take() {
								let _ = tx.cancel().await;
							}
							self.savepoints.clear();
							let ok = self.ok_packet(0, 0);
							self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
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

	/// Commits the open interactive transaction, as MySQL does implicitly before
	/// DDL, `START TRANSACTION` and `SET autocommit = 1`, and for an explicit
	/// `COMMIT`. Returns whether the caller may carry on: when the commit fails the
	/// client has been sent ERR 1180 and the caller must stop without running its
	/// statement.
	async fn commit_open_transaction(&mut self, seq: u8) -> Result<bool> {
		let Some(tx) = self.transaction.take() else {
			return Ok(true);
		};
		self.savepoints.clear();
		if let Err(e) = tx.commit().await {
			let err_pkt =
				ErrPacket::general(1180, b"HY000", format!("Got error during COMMIT: {e}"));
			self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
			return Ok(false);
		}
		Ok(true)
	}

	async fn handle_query(&mut self, sql: &str, seq: u8) -> Result<()> {
		let trimmed = sql.trim().trim_end_matches(';').trim();

		// In MySQL text protocol (COM_QUERY), reject unbound '?' parameter placeholders fail-closed
		if trimmed.contains('?')
			&& let Ok(tokens) = surrealdb_mysql::tokenize(trimmed)
			&& tokens.iter().any(|t| matches!(t.kind, surrealdb_mysql::TokenKind::Param(_)))
		{
			let err_pkt = ErrPacket::general(
				1064,
				b"42000",
				"parameter placeholder '?' is not supported in simple queries without prepared statements",
			);
			self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
			return Ok(());
		}

		// Handle transaction control statements
		if let Some(op) = mysql_transaction_control(trimmed) {
			match op {
				MyTxnOp::Begin {
					read_only,
				} => {
					// In MySQL, BEGIN/START TRANSACTION implicitly commits any active transaction
					if !self.commit_open_transaction(seq).await? {
						return Ok(());
					}
					let txn_type = if read_only {
						TransactionType::Read
					} else {
						TransactionType::Write
					};
					let tx =
						self.ds.transaction(txn_type).await.map_err(|e| anyhow::anyhow!("{e}"))?;
					self.transaction = Some(Arc::new(tx));
					self.savepoints.clear();
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
				MyTxnOp::Commit => {
					if !self.commit_open_transaction(seq).await? {
						return Ok(());
					}
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
				MyTxnOp::Rollback => {
					self.savepoints.clear();
					if let Some(tx) = self.transaction.take() {
						let _ = tx.cancel().await;
					}
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
				MyTxnOp::Savepoint(name) => {
					if let Some(tx) = &self.transaction {
						tx.new_save_point().await.map_err(|e| anyhow::anyhow!("{e}"))?;
					} else {
						let tx = self
							.ds
							.transaction(TransactionType::Write)
							.await
							.map_err(|e| anyhow::anyhow!("{e}"))?;
						tx.new_save_point().await.map_err(|e| anyhow::anyhow!("{e}"))?;
						self.transaction = Some(Arc::new(tx));
					}
					self.savepoints.push(name);
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
				MyTxnOp::ReleaseSavepoint(name) => {
					let pos = self.savepoints.iter().rposition(|s| s.eq_ignore_ascii_case(&name));
					let Some(idx) = pos else {
						let err_pkt = ErrPacket::general(
							1305,
							b"42000",
							format!("SAVEPOINT {name} does not exist"),
						);
						self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
						return Ok(());
					};
					if let Some(tx) = &self.transaction {
						while self.savepoints.len() > idx {
							tx.release_last_save_point()
								.await
								.map_err(|e| anyhow::anyhow!("{e}"))?;
							self.savepoints.pop();
						}
					}
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
				MyTxnOp::RollbackSavepoint(name) => {
					let pos = self.savepoints.iter().rposition(|s| s.eq_ignore_ascii_case(&name));
					let Some(idx) = pos else {
						let err_pkt = ErrPacket::general(
							1305,
							b"42000",
							format!("SAVEPOINT {name} does not exist"),
						);
						self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
						return Ok(());
					};
					if let Some(tx) = &self.transaction {
						while self.savepoints.len() > idx + 1 {
							tx.rollback_to_save_point()
								.await
								.map_err(|e| anyhow::anyhow!("{e}"))?;
							self.savepoints.pop();
						}
						tx.rollback_to_save_point().await.map_err(|e| anyhow::anyhow!("{e}"))?;
						tx.new_save_point().await.map_err(|e| anyhow::anyhow!("{e}"))?;
					}
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
					return Ok(());
				}
			}
		}

		if let Some(val) = parse_autocommit_setting(trimmed) {
			if self.autocommit != val {
				// Turning autocommit ON implicitly commits any active transaction
				if val && !self.commit_open_transaction(seq).await? {
					return Ok(());
				}
				self.autocommit = val;
			}
			let ok = self.ok_packet(0, 0);
			self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
			return Ok(());
		}

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
					let ok = self.ok_packet(0, 0);
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
			if is_mysql_driver_noise_set(trimmed) {
				let ok = self.ok_packet(0, 0);
				self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
				return Ok(());
			} else {
				let err_pkt = ErrPacket::general(
					1235,
					b"42000",
					format!("SET statement is not supported: {trimmed}"),
				);
				self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
				return Ok(());
			}
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

		// Handle USE <db>
		if lower.starts_with("use ")
			&& let Ok(MyStatement::Use(use_stmt)) = surrealdb_mysql::parse_statement(trimmed)
		{
			if let Err(e) = self.set_current_db(&use_stmt.database).await {
				let err_pkt = ErrPacket::general(
					1049,
					b"42000",
					format!("Unknown database '{}': {e}", use_stmt.database),
				);
				self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
				return Ok(());
			}
			let ok = self.ok_packet(0, 0);
			self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
			return Ok(());
		}

		// Check dialect capability before execution
		if let Err(e) = self.check_dialect(self.dialect) {
			let err_pkt = ErrPacket::general(1045, b"28000", e.to_string());
			self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
			return Ok(());
		}

		// Cross-database and namespace validation for MySQL dialect
		if self.dialect == Dialect::MySql {
			let cur_db = self.current_db.clone().unwrap_or_else(|| "test".to_string());
			let catalog = MemoryCatalog::new().with_database(&cur_db);
			if let Ok(stmts) = surrealdb_mysql::parse_statements(trimmed) {
				for stmt in &stmts {
					match stmt {
						MyStatement::Select(select) => {
							let policy = DialectPolicy::mysql();
							if let Err(e) = bind_select(select, &catalog, &policy, None) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::Insert(insert) => {
							if let Err(e) = surrealdb_sqlplan::resolve_mysql_table_name(
								&insert.table.parts,
								Some(&catalog),
							) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::Replace(replace) => {
							if let Err(e) = surrealdb_sqlplan::resolve_mysql_table_name(
								&replace.table.parts,
								Some(&catalog),
							) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::Update(update) => {
							fn check_tref(
								t: &TableRef,
								cat: &MemoryCatalog,
							) -> Result<(), surrealdb_sqlplan::PlanError> {
								match t {
									TableRef::Table {
										name,
										..
									} => {
										surrealdb_sqlplan::resolve_mysql_table_name(
											&name.parts,
											Some(cat),
										)?;
									}
									TableRef::Join {
										left,
										right,
										..
									} => {
										check_tref(left, cat)?;
										check_tref(right, cat)?;
									}
									_ => {}
								}
								Ok(())
							}
							if let Err(e) = check_tref(&update.table, &catalog) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::Delete(delete) => {
							if let Err(e) = surrealdb_sqlplan::resolve_mysql_table_name(
								&delete.table.parts,
								Some(&catalog),
							) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::Truncate(truncate) => {
							if let Err(e) = surrealdb_sqlplan::resolve_mysql_table_name(
								&truncate.table.parts,
								Some(&catalog),
							) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::CreateTable(create) => {
							if let Err(e) = surrealdb_sqlplan::resolve_mysql_table_name(
								&create.name.parts,
								Some(&catalog),
							) {
								let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
								self.writer
									.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
									.await?;
								return Ok(());
							}
						}
						MyStatement::DropTable(drop) => {
							for name in &drop.names {
								if let Err(e) = surrealdb_sqlplan::resolve_mysql_table_name(
									&name.parts,
									Some(&catalog),
								) {
									let err_pkt = ErrPacket::general(1105, b"HY000", e.to_string());
									self.writer
										.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
										.await?;
									return Ok(());
								}
							}
						}
						_ => {}
					}
				}
			}
		}

		// Execute against datastore with the selected dialect.
		// Fail-closed execution: NEVER fall back to SurrealQL on error!
		let descriptor = if self.dialect == Dialect::MySql {
			if let Ok(stmts) = surrealdb_mysql::parse_statements(trimmed) {
				if let Some(MyStatement::Select(select)) = stmts.first() {
					let cur_db = self.current_db.clone().unwrap_or_else(|| "test".to_string());
					let catalog = MemoryCatalog::new().with_database(&cur_db);
					let policy = DialectPolicy::mysql();
					bind_select(select, &catalog, &policy, None)
						.ok()
						.map(|bound| bound.describe(&catalog, &policy))
				} else {
					None
				}
			} else {
				None
			}
		} else {
			None
		};

		match self.dialect {
			Dialect::MySql => global_metrics().record_query(SqlDialect::MySql),
			Dialect::PgSql => global_metrics().record_query(SqlDialect::Postgres),
			_ => {}
		}

		// In MySQL, DDL statements cause an implicit commit of any active interactive transaction
		if is_ddl_statement(trimmed) && !self.commit_open_transaction(seq).await? {
			return Ok(());
		}

		// When autocommit is disabled and no transaction is active, start one
		if !self.autocommit && self.transaction.is_none() {
			let tx = self
				.ds
				.transaction(TransactionType::Write)
				.await
				.map_err(|e| anyhow::anyhow!("{e}"))?;
			self.transaction = Some(Arc::new(tx));
			self.savepoints.clear();
		}

		let req = match self.dialect {
			Dialect::MySql => QueryRequest::new(QuerySource::mysql(trimmed), &self.session),
			Dialect::SurrealQl => QueryRequest::new(trimmed, &self.session),
			Dialect::PgSql => QueryRequest::new(QuerySource::pgsql(trimmed), &self.session),
			Dialect::Gql => QueryRequest::new(QuerySource::gql(trimmed), &self.session),
		}
		.with_optional_transaction(self.transaction.clone());
		let res = self.ds.run(req).await;
		match res {
			Ok(results) => {
				let mut res_seq = seq.wrapping_add(1);
				if results.is_empty() {
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(res_seq, &ok.serialize()).await?;
					return Ok(());
				}

				// Check for errors first across all execution results
				for r in &results {
					if let Err(e) = &r.result {
						let err_pkt = ErrPacket::general(1064, b"42000", e.to_string());
						self.writer.write_packet(res_seq, &err_pkt.serialize()).await?;
						return Ok(());
					}
				}

				let supports_multi = (self.client_capabilities
					& (CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS))
					!= 0;

				let schema = self.current_db.clone().unwrap_or_else(|| "test".to_string());

				if results.len() > 1 && supports_multi {
					let num_results = results.len();
					for (idx, r) in results.into_iter().enumerate() {
						let has_more = idx + 1 < num_results;
						let mut status = self.status_flags();
						if has_more {
							status |= SERVER_MORE_RESULTS_EXISTS;
						}

						if let Ok(val) = r.result {
							let shape = if idx == 0 {
								if let Some(desc) = descriptor.as_ref() {
									shape_result_with_descriptor(desc, &schema, "", val.clone())
								} else {
									shape_result(&schema, "", val.clone())
								}
							} else {
								shape_result(&schema, "", val.clone())
							};

							if shape.columns.is_empty() {
								let (aff, last_id) = extract_dml_counts(&val);
								let ok = self.ok_packet(aff, last_id).with_status_flags(status);
								self.writer.write_packet(res_seq, &ok.serialize()).await?;
								res_seq = res_seq.wrapping_add(1);
							} else {
								self.emit_shape_result(&shape, &mut res_seq, status).await?;
							}
						}
					}
					return Ok(());
				}

				// If there's a result with tabular columns (e.g. SELECT), emit that,
				// otherwise emit the last result (e.g. from DDL/DML multi-statements).
				let target = results
					.iter()
					.rev()
					.find(|r| {
						if let Ok(val) = &r.result {
							let shape = shape_result(&schema, "", val.clone());
							!shape.columns.is_empty()
						} else {
							false
						}
					})
					.or_else(|| results.last());

				if let Some(r) = target {
					if let Ok(val) = &r.result {
						// Check if single statement in MySQL dialect is DML
						let is_dml = if self.dialect == Dialect::MySql
							&& let Ok(stmts) = surrealdb_mysql::parse_statements(trimmed)
							&& stmts.len() == 1 && matches!(
							&stmts[0],
							MyStatement::Insert(_)
								| MyStatement::Update(_) | MyStatement::Delete(_)
								| MyStatement::Replace(_)
						) {
							true
						} else {
							false
						};

						if is_dml {
							let (aff, last_id) = extract_dml_counts(val);
							let ok = self.ok_packet(aff, last_id);
							self.writer.write_packet(res_seq, &ok.serialize()).await?;
						} else {
							self.emit_value_result(descriptor.as_ref(), val.clone(), &mut res_seq)
								.await?;
						}
					}
				} else {
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(res_seq, &ok.serialize()).await?;
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
		} else if sql.contains("max_allowed_packet") {
			self.emit_scalar_result(
				"@@max_allowed_packet",
				&MAX_ALLOWED_PACKET.to_string(),
				&mut res_seq,
			)
			.await?;
		} else {
			self.emit_scalar_result("@@var", "1", &mut res_seq).await?;
		}
		Ok(())
	}

	async fn emit_shape_result(
		&mut self,
		shape: &ResultShape,
		seq: &mut u8,
		status_flags: u16,
	) -> Result<()> {
		if shape.columns.is_empty() {
			let ok = self.ok_packet(0, 0).with_status_flags(status_flags);
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

		// 3. Intermediate EOF packet (only if EOF not deprecated)
		if !self.deprecate_eof() {
			let eof = EofPacket::default().with_status_flags(status_flags);
			self.writer.write_packet(*seq, &eof.serialize()).await?;
			*seq = seq.wrapping_add(1);
		}

		// 4. Rows
		for row in &shape.rows {
			let row_buf = encode_text_row(row);
			self.writer.write_packet(*seq, &row_buf).await?;
			*seq = seq.wrapping_add(1);
		}

		// 5. Final terminating packet (OK starting with 0xfe if deprecated, else legacy EOF)
		if self.deprecate_eof() {
			let ok = OkPacket::new(0, 0).with_status_flags(status_flags);
			self.writer.write_packet(*seq, &ok.serialize_eof_ok()).await?;
		} else {
			let eof = EofPacket::default().with_status_flags(status_flags);
			self.writer.write_packet(*seq, &eof.serialize()).await?;
		}
		*seq = seq.wrapping_add(1);

		Ok(())
	}

	async fn emit_scalar_result_with_status(
		&mut self,
		col_name: &str,
		val: &str,
		seq: &mut u8,
		status_flags: u16,
	) -> Result<()> {
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

		// 3. Intermediate EOF packet separating columns and rows (only if EOF not deprecated)
		if !self.deprecate_eof() {
			let eof = EofPacket::default().with_status_flags(status_flags);
			self.writer.write_packet(*seq, &eof.serialize()).await?;
			*seq = seq.wrapping_add(1);
		}

		// 4. Text row packet
		let row_buf = encode_text_row(&[Some(val.to_string())]);
		self.writer.write_packet(*seq, &row_buf).await?;
		*seq = seq.wrapping_add(1);

		// 5. Final terminating packet
		if self.deprecate_eof() {
			let ok = OkPacket::new(0, 0).with_status_flags(status_flags);
			self.writer.write_packet(*seq, &ok.serialize_eof_ok()).await?;
		} else {
			let eof = EofPacket::default().with_status_flags(status_flags);
			self.writer.write_packet(*seq, &eof.serialize()).await?;
		}
		*seq = seq.wrapping_add(1);

		Ok(())
	}

	async fn emit_scalar_result(&mut self, col_name: &str, val: &str, seq: &mut u8) -> Result<()> {
		let status = self.status_flags();
		self.emit_scalar_result_with_status(col_name, val, seq, status).await
	}

	async fn emit_value_result_with_status(
		&mut self,
		descriptor: Option<&surrealdb_sqlplan::ResultDescriptor>,
		val: surrealdb_types::Value,
		seq: &mut u8,
		status_flags: u16,
	) -> Result<()> {
		let schema = self.current_db.as_deref().unwrap_or("test");
		let shape = if let Some(desc) = descriptor {
			shape_result_with_descriptor(desc, schema, "", val)
		} else {
			shape_result(schema, "", val)
		};

		self.emit_shape_result(&shape, seq, status_flags).await
	}

	async fn emit_value_result(
		&mut self,
		descriptor: Option<&surrealdb_sqlplan::ResultDescriptor>,
		val: surrealdb_types::Value,
		seq: &mut u8,
	) -> Result<()> {
		let status = self.status_flags();
		self.emit_value_result_with_status(descriptor, val, seq, status).await
	}

	async fn emit_binary_shape_result(
		&mut self,
		shape: &ResultShape,
		seq: &mut u8,
		status_flags: u16,
	) -> Result<()> {
		if shape.columns.is_empty() {
			let ok = self.ok_packet(0, 0).with_status_flags(status_flags);
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

		// 3. Intermediate EOF packet (only if EOF not deprecated)
		if !self.deprecate_eof() {
			let eof = EofPacket::default().with_status_flags(status_flags);
			self.writer.write_packet(*seq, &eof.serialize()).await?;
			*seq = seq.wrapping_add(1);
		}

		// 4. Binary Rows
		for raw_row in &shape.raw_rows {
			let row_buf = serialize_binary_row(&shape.columns, raw_row);
			self.writer.write_packet(*seq, &row_buf).await?;
			*seq = seq.wrapping_add(1);
		}

		// 5. Final terminating packet
		if self.deprecate_eof() {
			let ok = OkPacket::new(0, 0).with_status_flags(status_flags);
			self.writer.write_packet(*seq, &ok.serialize_eof_ok()).await?;
		} else {
			let eof = EofPacket::default().with_status_flags(status_flags);
			self.writer.write_packet(*seq, &eof.serialize()).await?;
		}
		*seq = seq.wrapping_add(1);

		Ok(())
	}

	async fn emit_binary_value_result_with_status(
		&mut self,
		descriptor: Option<&surrealdb_sqlplan::ResultDescriptor>,
		val: surrealdb_types::Value,
		seq: &mut u8,
		status_flags: u16,
	) -> Result<()> {
		let schema = self.current_db.as_deref().unwrap_or("test");
		let shape = if let Some(desc) = descriptor {
			shape_result_with_descriptor(desc, schema, "", val)
		} else {
			shape_result(schema, "", val)
		};

		self.emit_binary_shape_result(&shape, seq, status_flags).await
	}

	async fn emit_binary_value_result(
		&mut self,
		descriptor: Option<&surrealdb_sqlplan::ResultDescriptor>,
		val: surrealdb_types::Value,
		seq: &mut u8,
	) -> Result<()> {
		let status = self.status_flags();
		self.emit_binary_value_result_with_status(descriptor, val, seq, status).await
	}

	async fn handle_stmt_prepare(&mut self, data: &[u8], seq: u8) -> Result<()> {
		let query_str = String::from_utf8_lossy(data);
		let trimmed = query_str.trim().trim_end_matches(';').trim();

		let param_count = if let Ok(tokens) = surrealdb_mysql::tokenize(trimmed) {
			tokens.iter().filter(|t| matches!(t.kind, surrealdb_mysql::TokenKind::Param(_))).count()
				as u16
		} else {
			0
		};

		let descriptor = if self.dialect == Dialect::MySql {
			if let Ok(stmts) = surrealdb_mysql::parse_statements(trimmed) {
				if let Some(MyStatement::Select(select)) = stmts.first() {
					let cur_db = self.current_db.clone().unwrap_or_else(|| "test".to_string());
					let catalog = MemoryCatalog::new().with_database(&cur_db);
					let policy = DialectPolicy::mysql();
					bind_select(select, &catalog, &policy, None)
						.ok()
						.map(|bound| bound.describe(&catalog, &policy))
				} else {
					None
				}
			} else {
				None
			}
		} else {
			None
		};

		let schema = self.current_db.clone().unwrap_or_else(|| "test".to_string());
		let column_defs: Vec<ColumnDefinition41> = if let Some(desc) = descriptor {
			desc.columns
				.iter()
				.map(|c| {
					let tbl = c.table.as_deref().unwrap_or("");
					ColumnDefinition41::new(&schema, tbl, &c.name, c.ty.to_mysql_type())
				})
				.collect()
		} else {
			Vec::new()
		};

		let stmt_id = self.next_statement_id;
		self.next_statement_id = if self.next_statement_id == u32::MAX {
			1
		} else {
			self.next_statement_id + 1
		};

		let stmt = PreparedStatement {
			id: stmt_id,
			query: trimmed.to_string(),
			param_count,
			param_types: Vec::new(),
			column_defs: column_defs.clone(),
			long_data: HashMap::new(),
		};
		self.prepared_statements.insert(stmt_id, stmt);

		let mut res_seq = seq.wrapping_add(1);
		let ok_pkt = StmtPrepareOk::new(stmt_id, column_defs.len() as u16, param_count);
		self.writer.write_packet(res_seq, &ok_pkt.serialize()).await?;
		res_seq = res_seq.wrapping_add(1);

		if param_count > 0 {
			for _ in 0..param_count {
				let p_def = ColumnDefinition41::param("?");
				self.writer.write_packet(res_seq, &p_def.serialize()).await?;
				res_seq = res_seq.wrapping_add(1);
			}
			if !self.deprecate_eof() {
				let eof = EofPacket::default().with_status_flags(self.status_flags());
				self.writer.write_packet(res_seq, &eof.serialize()).await?;
				res_seq = res_seq.wrapping_add(1);
			}
		}

		if !column_defs.is_empty() {
			for col in &column_defs {
				self.writer.write_packet(res_seq, &col.serialize()).await?;
				res_seq = res_seq.wrapping_add(1);
			}
			if !self.deprecate_eof() {
				let eof = EofPacket::default().with_status_flags(self.status_flags());
				self.writer.write_packet(res_seq, &eof.serialize()).await?;
				let _ = res_seq.wrapping_add(1);
			}
		}

		Ok(())
	}

	async fn handle_stmt_send_long_data(&mut self, data: &[u8]) -> Result<()> {
		if data.len() < 6 {
			return Ok(());
		}
		let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
		let param_id = u16::from_le_bytes([data[4], data[5]]);
		let chunk = &data[6..];

		if let Some(stmt) = self.prepared_statements.get_mut(&stmt_id) {
			stmt.long_data.entry(param_id).or_default().extend_from_slice(chunk);
		}
		Ok(())
	}

	async fn handle_stmt_reset(&mut self, data: &[u8], seq: u8) -> Result<()> {
		if data.len() < 4 {
			let err = ErrPacket::general(1064, b"42000", "Malformed COM_STMT_RESET payload");
			self.writer.write_packet(seq.wrapping_add(1), &err.serialize()).await?;
			return Ok(());
		}
		let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
		if let Some(stmt) = self.prepared_statements.get_mut(&stmt_id) {
			stmt.long_data.clear();
			let ok = self.ok_packet(0, 0);
			self.writer.write_packet(seq.wrapping_add(1), &ok.serialize()).await?;
		} else {
			let err = ErrPacket::general(
				1243,
				b"HY000",
				format!(
					"Unknown prepared statement handler ({stmt_id}) given to mysqld_stmt_reset"
				),
			);
			self.writer.write_packet(seq.wrapping_add(1), &err.serialize()).await?;
		}
		Ok(())
	}

	async fn handle_stmt_close(&mut self, data: &[u8]) -> Result<()> {
		if data.len() >= 4 {
			let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
			self.prepared_statements.remove(&stmt_id);
		}
		Ok(())
	}

	async fn handle_stmt_fetch(&mut self, _data: &[u8], seq: u8) -> Result<()> {
		let mut status = self.status_flags();
		status |= SERVER_STATUS_LAST_ROW_SENT;
		if self.deprecate_eof() {
			let ok = OkPacket::new(0, 0).with_status_flags(status);
			self.writer.write_packet(seq.wrapping_add(1), &ok.serialize_eof_ok()).await?;
		} else {
			let eof = EofPacket::default().with_status_flags(status);
			self.writer.write_packet(seq.wrapping_add(1), &eof.serialize()).await?;
		}
		Ok(())
	}

	async fn handle_stmt_execute(&mut self, data: &[u8], seq: u8) -> Result<()> {
		if data.len() < 9 {
			let err_pkt = ErrPacket::general(1064, b"42000", "Malformed COM_STMT_EXECUTE packet");
			self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
			return Ok(());
		}

		let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
		let _ = data[4];
		let _ = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

		let (query, param_count) = match self.prepared_statements.get(&stmt_id) {
			Some(stmt) => (stmt.query.clone(), stmt.param_count),
			None => {
				let err_pkt = ErrPacket::general(
					1243,
					b"HY000",
					format!(
						"Unknown prepared statement handler ({stmt_id}) given to mysqld_stmt_execute"
					),
				);
				self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
				return Ok(());
			}
		};

		let mut cursor = &data[9..];
		let mut params = Vec::with_capacity(param_count as usize);

		if param_count > 0 {
			let null_bitmap_len = (param_count as usize).div_ceil(8);
			if cursor.len() < null_bitmap_len {
				let err_pkt =
					ErrPacket::general(1064, b"42000", "Malformed null bitmap in COM_STMT_EXECUTE");
				self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
				return Ok(());
			}
			let null_bitmap = &cursor[..null_bitmap_len];
			cursor = &cursor[null_bitmap_len..];

			if cursor.is_empty() {
				let err_pkt = ErrPacket::general(
					1064,
					b"42000",
					"Missing new_params_bound_flag in COM_STMT_EXECUTE",
				);
				self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
				return Ok(());
			}
			let new_params_bound = cursor[0];
			cursor = &cursor[1..];

			if new_params_bound == 1 {
				let types_len = (param_count as usize) * 2;
				if cursor.len() < types_len {
					let err_pkt = ErrPacket::general(
						1064,
						b"42000",
						"Malformed param types in COM_STMT_EXECUTE",
					);
					self.writer.write_packet(seq.wrapping_add(1), &err_pkt.serialize()).await?;
					return Ok(());
				}
				let mut types = Vec::with_capacity(param_count as usize);
				for _ in 0..param_count {
					let ty = cursor[0];
					let fl = cursor[1];
					cursor = &cursor[2..];
					types.push((ty, fl));
				}
				if let Some(stmt) = self.prepared_statements.get_mut(&stmt_id) {
					stmt.param_types = types;
				}
			}

			let stmt = match self.prepared_statements.get_mut(&stmt_id) {
				Some(stmt) => stmt,
				None => unreachable!(),
			};

			for i in 0..param_count as usize {
				let is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;
				if is_null {
					params.push(surrealdb_types::Value::Null);
				} else if let Some(long_bytes) = stmt.long_data.remove(&(i as u16)) {
					match String::from_utf8(long_bytes.clone()) {
						Ok(s) => params.push(surrealdb_types::Value::String(s)),
						Err(_) => params.push(surrealdb_types::Value::Bytes(long_bytes.into())),
					}
				} else {
					let (ty, fl) =
						stmt.param_types.get(i).copied().unwrap_or((MYSQL_TYPE_VAR_STRING, 0));
					let unsigned = (fl & 0x80) != 0;
					match decode_binary_param(&mut cursor, ty, unsigned) {
						Ok(val) => params.push(val),
						Err(e) => {
							let err_pkt = ErrPacket::general(
								1064,
								b"42000",
								format!("Failed to decode parameter {i}: {e}"),
							);
							self.writer
								.write_packet(seq.wrapping_add(1), &err_pkt.serialize())
								.await?;
							return Ok(());
						}
					}
				}
			}
		}

		let mut vars = surrealdb_types::Variables::new();
		for (idx, val) in params.into_iter().enumerate() {
			let param_num = idx + 1;
			let var_name = surrealdb_types::positional_param_name(param_num);
			vars.insert(var_name, val.clone());
			vars.insert(format!("param_{param_num}"), val);
		}

		self.execute_prepared_query(&query, vars, seq).await
	}

	async fn execute_prepared_query(
		&mut self,
		trimmed: &str,
		vars: surrealdb_types::Variables,
		seq: u8,
	) -> Result<()> {
		let descriptor = if self.dialect == Dialect::MySql {
			if let Ok(stmts) = surrealdb_mysql::parse_statements(trimmed) {
				if let Some(MyStatement::Select(select)) = stmts.first() {
					let cur_db = self.current_db.clone().unwrap_or_else(|| "test".to_string());
					let catalog = MemoryCatalog::new().with_database(&cur_db);
					let policy = DialectPolicy::mysql();
					bind_select(select, &catalog, &policy, None)
						.ok()
						.map(|bound| bound.describe(&catalog, &policy))
				} else {
					None
				}
			} else {
				None
			}
		} else {
			None
		};

		match self.dialect {
			Dialect::MySql => global_metrics().record_query(SqlDialect::MySql),
			Dialect::PgSql => global_metrics().record_query(SqlDialect::Postgres),
			_ => {}
		}

		if is_ddl_statement(trimmed) && !self.commit_open_transaction(seq).await? {
			return Ok(());
		}

		if !self.autocommit && self.transaction.is_none() {
			let tx = self
				.ds
				.transaction(TransactionType::Write)
				.await
				.map_err(|e| anyhow::anyhow!("{e}"))?;
			self.transaction = Some(Arc::new(tx));
			self.savepoints.clear();
		}

		let req = match self.dialect {
			Dialect::MySql => QueryRequest::new(QuerySource::mysql(trimmed), &self.session),
			Dialect::SurrealQl => QueryRequest::new(trimmed, &self.session),
			Dialect::PgSql => QueryRequest::new(QuerySource::pgsql(trimmed), &self.session),
			Dialect::Gql => QueryRequest::new(QuerySource::gql(trimmed), &self.session),
		}
		.with_variables(Some(vars))
		.with_optional_transaction(self.transaction.clone());

		let res = self.ds.run(req).await;
		match res {
			Ok(results) => {
				let mut res_seq = seq.wrapping_add(1);
				if results.is_empty() {
					let ok = self.ok_packet(0, 0);
					self.writer.write_packet(res_seq, &ok.serialize()).await?;
					return Ok(());
				}

				for r in &results {
					if let Err(e) = &r.result {
						let err_pkt = ErrPacket::general(1064, b"42000", e.to_string());
						self.writer.write_packet(res_seq, &err_pkt.serialize()).await?;
						return Ok(());
					}
				}

				let schema = self.current_db.clone().unwrap_or_else(|| "test".to_string());
				let target = results
					.iter()
					.rev()
					.find(|r| {
						if let Ok(val) = &r.result {
							let shape = shape_result(&schema, "", val.clone());
							!shape.columns.is_empty()
						} else {
							false
						}
					})
					.or_else(|| results.last());

				if let Some(r) = target
					&& let Ok(val) = &r.result
				{
					let is_dml = if self.dialect == Dialect::MySql
						&& let Ok(stmts) = surrealdb_mysql::parse_statements(trimmed)
						&& stmts.len() == 1
						&& matches!(
							&stmts[0],
							MyStatement::Insert(_)
								| MyStatement::Update(_) | MyStatement::Delete(_)
								| MyStatement::Replace(_)
						) {
						true
					} else {
						false
					};

					if is_dml {
						let (aff, last_id) = extract_dml_counts(val);
						let ok = self.ok_packet(aff, last_id);
						self.writer.write_packet(res_seq, &ok.serialize()).await?;
					} else {
						self.emit_binary_value_result(
							descriptor.as_ref(),
							val.clone(),
							&mut res_seq,
						)
						.await?;
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

/// Returns true if a MySQL SET statement is setting only allowed driver-noise variables.
fn is_mysql_driver_noise_set(sql: &str) -> bool {
	let lower = sql.to_ascii_lowercase();
	let rest = lower.strip_prefix("set ").unwrap_or(&lower).trim();
	let rest = rest.trim_end_matches(';').trim();
	if rest.starts_with("names ") {
		return true;
	}
	let rest = rest
		.strip_prefix("session ")
		.or_else(|| rest.strip_prefix("global "))
		.unwrap_or(rest)
		.trim();
	let rest = rest
		.strip_prefix("@@session.")
		.or_else(|| rest.strip_prefix("@@global."))
		.or_else(|| rest.strip_prefix("@@"))
		.unwrap_or(rest)
		.trim();

	const ALLOWED_VARS: &[&str] = &[
		"autocommit",
		"character_set_client",
		"character_set_connection",
		"character_set_results",
		"character_set_server",
		"collation_connection",
		"collation_server",
		"sql_mode",
		"time_zone",
		"max_allowed_packet",
		"net_write_timeout",
		"wait_timeout",
		"interactive_timeout",
	];

	for part in rest.split(',') {
		let var = part.split('=').next().unwrap_or("").trim();
		let var = var.trim_matches('`').trim();
		if var.is_empty() || !ALLOWED_VARS.contains(&var) {
			return false;
		}
	}
	true
}

/// Remove SQL comments (`-- …`, `# …` to EOL and `/* … */`) so lexical
/// statement matching is not fooled by an embedded comment.
fn strip_sql_comments(sql: &str) -> String {
	let bytes = sql.as_bytes();
	let mut out = String::with_capacity(sql.len());
	let mut i = 0;
	while i < bytes.len() {
		match bytes[i] {
			b'-' if bytes.get(i + 1) == Some(&b'-') => {
				while i < bytes.len() && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'#' => {
				while i < bytes.len() && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'/' if bytes.get(i + 1) == Some(&b'*') => {
				i += 2;
				while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
					i += 1;
				}
				i += 2;
				out.push(' ');
			}
			_ => {
				let ch = sql[i..].chars().next().expect("valid char boundary");
				out.push(ch);
				i += ch.len_utf8();
			}
		}
	}
	out
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum MyTxnOp {
	Begin {
		read_only: bool,
	},
	Commit,
	Rollback,
	Savepoint(String),
	ReleaseSavepoint(String),
	RollbackSavepoint(String),
}

fn mysql_transaction_control(trimmed: &str) -> Option<MyTxnOp> {
	let stripped = strip_sql_comments(trimmed);
	let stripped = stripped.trim().trim_end_matches(';').trim();
	if stripped.contains(';') {
		return None;
	}
	let mut tokens_raw = stripped.split_whitespace();
	let first = tokens_raw.next()?;
	let first_upper = first.to_ascii_uppercase();

	if first_upper == "SAVEPOINT" {
		let name = tokens_raw.next()?;
		if tokens_raw.next().is_none() {
			let clean = name.trim_matches('`').trim_matches('\'').trim_matches('"').to_string();
			if !clean.is_empty() {
				return Some(MyTxnOp::Savepoint(clean));
			}
		}
		return None;
	}

	if first_upper == "RELEASE" {
		let mut next = tokens_raw.next()?;
		if next.eq_ignore_ascii_case("SAVEPOINT") {
			next = tokens_raw.next()?;
		}
		if tokens_raw.next().is_none() {
			let clean = next.trim_matches('`').trim_matches('\'').trim_matches('"').to_string();
			if !clean.is_empty() {
				return Some(MyTxnOp::ReleaseSavepoint(clean));
			}
		}
		return None;
	}

	if first_upper == "ROLLBACK" {
		let mut next_opt = tokens_raw.next();
		if let Some(t) = next_opt
			&& t.eq_ignore_ascii_case("WORK")
		{
			next_opt = tokens_raw.next();
		}
		if let Some(t) = next_opt
			&& t.eq_ignore_ascii_case("TO")
		{
			let mut name_opt = tokens_raw.next()?;
			if name_opt.eq_ignore_ascii_case("SAVEPOINT") {
				name_opt = tokens_raw.next()?;
			}
			if tokens_raw.next().is_none() {
				let clean =
					name_opt.trim_matches('`').trim_matches('\'').trim_matches('"').to_string();
				if !clean.is_empty() {
					return Some(MyTxnOp::RollbackSavepoint(clean));
				}
			}
			return None;
		}
	}

	let mut tokens = stripped.split_whitespace().map(str::to_ascii_uppercase);
	let first = tokens.next()?;
	match first.as_str() {
		"BEGIN" => {
			let remaining: Vec<String> = tokens.collect();
			if remaining.is_empty() || (remaining.len() == 1 && remaining[0] == "WORK") {
				Some(MyTxnOp::Begin {
					read_only: false,
				})
			} else {
				None
			}
		}
		"START" => {
			if tokens.next().as_deref() != Some("TRANSACTION") {
				return None;
			}
			let mut read_only = false;
			let remaining: Vec<String> = tokens.collect();
			for w in remaining.windows(2) {
				if w[0] == "READ" && w[1] == "ONLY" {
					read_only = true;
				}
			}
			Some(MyTxnOp::Begin {
				read_only,
			})
		}
		"COMMIT" => {
			let remaining: Vec<String> = tokens.collect();
			if remaining.iter().all(|t| ["WORK", "AND", "NO", "CHAIN"].contains(&t.as_str())) {
				Some(MyTxnOp::Commit)
			} else {
				None
			}
		}
		"ROLLBACK" => {
			let remaining: Vec<String> = tokens.collect();
			if remaining.iter().all(|t| ["WORK", "AND", "NO", "CHAIN"].contains(&t.as_str())) {
				Some(MyTxnOp::Rollback)
			} else {
				None
			}
		}
		_ => None,
	}
}

fn parse_autocommit_setting(sql: &str) -> Option<bool> {
	let lower = sql.to_ascii_lowercase();
	let rest = lower.strip_prefix("set ")?.trim().trim_end_matches(';').trim();
	let rest = rest
		.strip_prefix("session ")
		.or_else(|| rest.strip_prefix("global "))
		.unwrap_or(rest)
		.trim();
	let rest = rest
		.strip_prefix("@@session.")
		.or_else(|| rest.strip_prefix("@@global."))
		.or_else(|| rest.strip_prefix("@@"))
		.unwrap_or(rest)
		.trim();
	let (var, val) = rest.split_once('=')?;
	let var = var.trim().trim_matches('`').trim();
	if var != "autocommit" {
		return None;
	}
	let val = val.trim().trim_matches('\'').trim_matches('"').trim();
	match val {
		"1" | "on" | "true" => Some(true),
		"0" | "off" | "false" => Some(false),
		_ => None,
	}
}

fn is_ddl_statement(sql: &str) -> bool {
	let stripped = strip_sql_comments(sql);
	let stripped = stripped.trim();
	let mut tokens = stripped.split_whitespace();
	let Some(first) = tokens.next() else {
		return false;
	};
	let first_upper = first.to_ascii_uppercase();
	matches!(first_upper.as_str(), "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME")
}

fn extract_id_u64(id_val: &surrealdb_types::Value) -> Option<u64> {
	match id_val {
		surrealdb_types::Value::Number(n) => {
			let int_val = n.to_int()?;
			if int_val >= 0 {
				Some(int_val as u64)
			} else {
				None
			}
		}
		surrealdb_types::Value::RecordId(r) => match &r.key {
			surrealdb_types::RecordIdKey::Number(n) => {
				if *n >= 0 {
					Some(*n as u64)
				} else {
					None
				}
			}
			_ => None,
		},
		_ => None,
	}
}

fn extract_dml_counts(val: &surrealdb_types::Value) -> (u64, u64) {
	match val {
		surrealdb_types::Value::Array(items) => {
			let affected = items.len() as u64;
			let mut last_id = 0;
			if let Some(surrealdb_types::Value::Object(obj)) = items.first()
				&& let Some(id_val) = obj.get("id")
			{
				last_id = extract_id_u64(id_val).unwrap_or(0);
			}
			(affected, last_id)
		}
		surrealdb_types::Value::Object(obj) => {
			let mut last_id = 0;
			if let Some(id_val) = obj.get("id") {
				last_id = extract_id_u64(id_val).unwrap_or(0);
			}
			(1, last_id)
		}
		_ => (0, 0),
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

	#[test]
	fn test_is_mysql_driver_noise_set() {
		assert!(is_mysql_driver_noise_set("SET autocommit = 1"));
		assert!(is_mysql_driver_noise_set("SET NAMES utf8mb4"));
		assert!(is_mysql_driver_noise_set("SET NAMES 'utf8mb4'"));
		assert!(is_mysql_driver_noise_set("SET SESSION sql_mode = 'TRADITIONAL'"));
		assert!(is_mysql_driver_noise_set("SET @@session.time_zone = 'UTC'"));
		assert!(is_mysql_driver_noise_set(
			"SET `character_set_client` = utf8mb4, `character_set_connection` = utf8mb4"
		));
		// Unmodelled/unhonoured settings return false
		assert!(!is_mysql_driver_noise_set("SET ROLE admin"));
		assert!(!is_mysql_driver_noise_set("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
		assert!(!is_mysql_driver_noise_set("SET arbitrary_setting = 1"));
	}

	#[test]
	fn test_mysql_transaction_control() {
		assert_eq!(
			mysql_transaction_control("BEGIN"),
			Some(MyTxnOp::Begin {
				read_only: false,
			})
		);
		assert_eq!(
			mysql_transaction_control("BEGIN WORK;"),
			Some(MyTxnOp::Begin {
				read_only: false,
			})
		);
		assert_eq!(
			mysql_transaction_control("START TRANSACTION"),
			Some(MyTxnOp::Begin {
				read_only: false,
			})
		);
		assert_eq!(
			mysql_transaction_control("START TRANSACTION READ ONLY"),
			Some(MyTxnOp::Begin {
				read_only: true,
			})
		);
		assert_eq!(mysql_transaction_control("COMMIT"), Some(MyTxnOp::Commit));
		assert_eq!(mysql_transaction_control("COMMIT AND NO CHAIN"), Some(MyTxnOp::Commit));
		assert_eq!(mysql_transaction_control("ROLLBACK"), Some(MyTxnOp::Rollback));
		assert_eq!(mysql_transaction_control("ROLLBACK WORK"), Some(MyTxnOp::Rollback));
		assert_eq!(
			mysql_transaction_control("SAVEPOINT sp1"),
			Some(MyTxnOp::Savepoint("sp1".to_string()))
		);
		assert_eq!(
			mysql_transaction_control("SAVEPOINT `sp_2`"),
			Some(MyTxnOp::Savepoint("sp_2".to_string()))
		);
		assert_eq!(
			mysql_transaction_control("RELEASE SAVEPOINT sp1"),
			Some(MyTxnOp::ReleaseSavepoint("sp1".to_string()))
		);
		assert_eq!(
			mysql_transaction_control("RELEASE sp1"),
			Some(MyTxnOp::ReleaseSavepoint("sp1".to_string()))
		);
		assert_eq!(
			mysql_transaction_control("ROLLBACK TO SAVEPOINT sp1"),
			Some(MyTxnOp::RollbackSavepoint("sp1".to_string()))
		);
		assert_eq!(
			mysql_transaction_control("ROLLBACK WORK TO sp1"),
			Some(MyTxnOp::RollbackSavepoint("sp1".to_string()))
		);
		assert_eq!(
			mysql_transaction_control("/* comment */ SAVEPOINT sp1;"),
			Some(MyTxnOp::Savepoint("sp1".to_string()))
		);
		// Not a transaction control statement
		assert_eq!(mysql_transaction_control("SELECT 1"), None);
		assert_eq!(mysql_transaction_control("COMMIT; SELECT 1"), None);
	}

	#[test]
	fn test_parse_autocommit_setting() {
		assert_eq!(parse_autocommit_setting("SET autocommit = 1"), Some(true));
		assert_eq!(parse_autocommit_setting("SET autocommit = 0"), Some(false));
		assert_eq!(parse_autocommit_setting("SET @@autocommit = 1"), Some(true));
		assert_eq!(parse_autocommit_setting("SET @@session.autocommit = 'OFF'"), Some(false));
		assert_eq!(parse_autocommit_setting("SET SESSION autocommit = ON"), Some(true));
		assert_eq!(parse_autocommit_setting("SET names utf8mb4"), None);
	}

	#[test]
	fn test_is_ddl_statement() {
		assert!(is_ddl_statement("CREATE TABLE users (id int)"));
		assert!(is_ddl_statement("DROP TABLE users"));
		assert!(is_ddl_statement("ALTER TABLE users ADD COLUMN age int"));
		assert!(is_ddl_statement("TRUNCATE TABLE users"));
		assert!(is_ddl_statement("/* comment */ CREATE TABLE t1 (id int)"));
		assert!(!is_ddl_statement("SELECT * FROM users"));
		assert!(!is_ddl_statement("INSERT INTO users VALUES (1)"));
	}

	#[test]
	fn test_extract_dml_counts() {
		// Empty array
		let empty = surrealdb_types::Value::Array(surrealdb_types::Array::new());
		assert_eq!(extract_dml_counts(&empty), (0, 0));

		// Object with integer ID
		let mut obj = surrealdb_types::Object::new();
		obj.insert(
			"id".to_string(),
			surrealdb_types::Value::Number(surrealdb_types::Number::Int(42)),
		);
		let (aff, id) = extract_dml_counts(&surrealdb_types::Value::Object(obj.clone()));
		assert_eq!(aff, 1);
		assert_eq!(id, 42);

		// Array of objects
		let arr = surrealdb_types::Value::Array(
			vec![
				surrealdb_types::Value::Object(obj),
				surrealdb_types::Value::Object(surrealdb_types::Object::new()),
			]
			.into(),
		);
		let (aff, id) = extract_dml_counts(&arr);
		assert_eq!(aff, 2);
		assert_eq!(id, 42);
	}
}
