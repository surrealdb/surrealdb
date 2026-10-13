use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use bytes::BytesMut;
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::verify::{self, ScramAuth, basic};
use surrealdb_core::kvs::{Datastore, Dialect, QueryRequest, QuerySource};
use surrealdb_core::pgsql::lower::lower_stmt;
use surrealdb_core::syn;
use surrealdb_core::syn::{Query as Ast, TopLevelExpr};
use surrealdb_datastore::Transaction;
use surrealdb_kvs::TransactionType;
use surrealdb_pgsql::ast::stmt::Statement as PgStatement;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_sqlplan::binder::pgsql::bind_select;
use surrealdb_sqlplan::catalog::MemoryCatalog;
use surrealdb_sqlplan::dialect::DialectPolicy;
use surrealdb_sqlplan::observability::global_metrics;
use surrealdb_sqlplan::{
	LogicalOp, SqlDialect, lower_pgsql_ddl, lower_pgsql_delete, lower_pgsql_insert,
	lower_pgsql_truncate, lower_pgsql_update,
};
use surrealdb_types::{Value, Variables};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;

use super::error::PgError;
use super::msg::{self, DescribeTarget, Frontend, StartupMessage};
use super::typing::{
	PgColumn, PgType, ResultShape, shape_result, shape_result_jsonb, shape_result_with_descriptor,
};
use super::{CancelRegistry, LOG, encode, sasl};
use crate::PKG_VERSION;

/// A client connection stream: plaintext, or upgraded to TLS after an accepted
/// SSLRequest. `Either` forwards `AsyncRead`/`AsyncWrite` to whichever it is,
/// so the rest of the connection code is transport-agnostic.
type PgStream = Either<TcpStream, TlsStream<TcpStream>>;

/// Result-format codes are jsonb-single-column for driver-prepared statements
/// and typed for portal-described flows. `PROTOCOL 3.0` binary result flag.
const FORMAT_TEXT: i16 = 0;
const FORMAT_BINARY: i16 = 1;

/// Upper bound on live prepared statements per connection.
const MAX_PREPARED: usize = 1024;

/// Upper memory bound (32 MiB) on total live prepared statements per connection.
const MAX_PREPARED_BYTES: usize = 32 * 1024 * 1024;

/// Upper bound on live portals per connection.
const MAX_PORTALS: usize = 1024;

/// Upper memory bound (64 MiB) on total live portals per connection.
const MAX_PORTAL_BYTES: usize = 64 * 1024 * 1024;

/// Upper bound on the parameters a prepared statement may reference. The client
/// controls the positional index in the query text (`$n`), so without a cap a
/// value like `$999999999` would force a multi-gigabyte ParameterDescription
/// allocation; `i16::MAX` also keeps the on-wire parameter count in range.
const MAX_PARAMS: usize = i16::MAX as usize;

/// Pre-parsed and pre-bound plan execution artifact.
#[derive(Clone)]
enum CachedPlan {
	SurrealQl(syn::Query),
	PgSelect {
		plan: Box<LogicalOp>,
	},
	PgAst(Ast),
}

/// A parsed, not-yet-bound prepared statement.
struct PreparedStatement {
	/// The (positional-rewritten) query text; used for GQL, and as the source
	/// for error messages.
	query: String,
	/// Pre-parsed and pre-bound query plan (SurrealQL AST or PgSQL LogicalOp/AST),
	/// cached at Parse so repeated Execute does not re-parse or re-bind.
	cached_plan: Option<CachedPlan>,
	/// Client-declared parameter type OIDs (`0` = unspecified); may be shorter
	/// than the number of `$n` placeholders in the query.
	param_types: Vec<i32>,
	/// Highest positional parameter index (`$n`) referenced by the query, so
	/// ParameterDescription reports the true parameter count.
	param_count: usize,
	/// Whether the query is empty (no statements) — its portal answers with
	/// EmptyQueryResponse instead of a result set.
	empty: bool,
	/// Whether `Describe(statement)` was issued — pins this statement's
	/// results to the single-`jsonb`-column shape for consistency.
	described: bool,
	/// Set for a standalone transaction-control or `SET` statement, which is
	/// intercepted at Execute rather than run through the executor (mirroring
	/// the simple-query path); `None` for an ordinary query.
	control: Option<Control>,
	/// Static result descriptor from the query planner if available.
	descriptor: Option<surrealdb_sqlplan::ResultDescriptor>,
}

impl PreparedStatement {
	fn estimated_size(&self) -> usize {
		std::mem::size_of::<Self>()
			+ self.query.capacity()
			+ self.param_types.capacity() * std::mem::size_of::<i32>()
			+ if self.cached_plan.is_some() {
				1024
			} else {
				0
			} + if self.descriptor.is_some() {
			512
		} else {
			0
		}
	}
}

/// An extended-protocol statement that is intercepted rather than executed. The
/// simple-query path intercepts standalone `BEGIN`/`COMMIT`/`ROLLBACK` and
/// `SET`; classifying them at Parse lets the extended protocol do the same, so
/// drivers using server-side prepared statements get interactive transactions
/// and dialect switching too.
#[derive(Clone)]
enum Control {
	Transaction(TxnOp),
	Set(SetStatement),
}

impl Control {
	/// The Postgres command tag reported for this control statement's
	/// CommandComplete.
	fn command_tag(&self) -> &'static str {
		match self {
			Control::Transaction(TxnOp::Begin) => "BEGIN",
			Control::Transaction(TxnOp::Commit) => "COMMIT",
			Control::Transaction(TxnOp::Rollback) => "ROLLBACK",
			Control::Set(_) => "SET",
		}
	}
}

/// A bound portal: a statement plus its parameter values and result formats.
struct Portal {
	statement: String,
	params: Variables,
	result_formats: Vec<i16>,
	/// Populated when the portal is first described or executed.
	executed: Option<Executed>,
	/// For a control-statement portal: whether its side effect has already run,
	/// so a re-Execute before Sync does not repeat `BEGIN`/`COMMIT`/etc.
	control_ran: bool,
}

impl Portal {
	fn estimated_size(&self) -> usize {
		let mut size = std::mem::size_of::<Self>()
			+ self.statement.capacity()
			+ self.result_formats.capacity() * std::mem::size_of::<i16>();
		if let Some(executed) = &self.executed {
			size += std::mem::size_of::<Executed>()
				+ executed.columns.capacity() * std::mem::size_of::<PgColumn>();
			for row in &executed.rows {
				size += std::mem::size_of::<Vec<Option<Value>>>()
					+ row.capacity() * std::mem::size_of::<Option<Value>>();
				for v in row.iter().flatten() {
					size += match v {
						Value::String(s) => s.len(),
						Value::Bytes(b) => b.len(),
						Value::Array(a) => a.len() * 32,
						Value::Object(o) => o.len() * 64,
						_ => 16,
					};
				}
			}
		}
		size
	}
}

/// A portal's materialized result, drained across one or more Execute calls.
struct Executed {
	columns: Vec<PgColumn>,
	rows: VecDeque<Vec<Option<Value>>>,
}

/// Flush the response buffer to the socket once it grows past this size, so
/// large result sets stream instead of accumulating a full wire copy.
const FLUSH_THRESHOLD: usize = 64 * 1024;

/// Deadline for a connection to complete startup and authentication.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum size of a message read before authentication completes (the
/// password message). Kept small so an unauthenticated peer cannot force a
/// large allocation.
const MAX_AUTH_MESSAGE_SIZE: usize = 4096;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle(
	stream: TcpStream,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	permit: OwnedSemaphorePermit,
	registry: Arc<CancelRegistry>,
	pid: i32,
	secret: i32,
	acceptor: Option<Arc<TlsAcceptor>>,
) {
	let cancel = CancelHandle::new();
	registry.insert((pid, secret), cancel.clone());
	// Negotiate an optional TLS upgrade before the startup exchange. A
	// CancelRequest is fully handled here (it carries no post-handshake work).
	// Bound the whole negotiation (reads + TLS handshake) by the startup
	// deadline so an unauthenticated peer cannot hold a connection permit
	// open indefinitely (slow-loris).
	let negotiated =
		timeout(STARTUP_TIMEOUT, negotiate_tls(stream, acceptor.as_deref(), &registry, &shutdown))
			.await;
	let Some((stream, prelude)) = negotiated.ok().flatten() else {
		registry.remove(&(pid, secret));
		return;
	};
	let mut conn = Connection {
		stream,
		prelude,
		peer,
		ds,
		ready,
		shutdown,
		session: Session::default(),
		statements: HashMap::new(),
		portals: HashMap::new(),
		prepared_bytes: 0,
		portal_bytes: 0,
		dialect: Dialect::SurrealQl,
		transaction: None,
		txn_failed: false,
		txn_lets: Vec::new(),
		cancel,
		registry: Arc::clone(&registry),
		pid,
		secret,
		_permit: permit,
	};
	if let Err(err) = conn.run().await {
		debug!(target: LOG, "postgres connection from {peer} ended: {err}");
	}
	// Roll back any transaction the client left open, then deregister.
	if let Some(tx) = conn.transaction.take() {
		let _ = tx.cancel().await;
	}
	registry.remove(&(pid, secret));
}

/// Handle the optional leading SSLRequest/GSSENCRequest and CancelRequest,
/// upgrading to TLS when an acceptor is configured and the client asks.
///
/// Returns the ready stream plus an optional already-read StartupMessage
/// (plaintext path); on the TLS path the message is read afresh over the
/// encrypted stream so the prelude is `None`. Returns `None` to close (a
/// CancelRequest, a declined-then-abandoned handshake, or an I/O error).
async fn negotiate_tls(
	mut tcp: TcpStream,
	acceptor: Option<&TlsAcceptor>,
	registry: &CancelRegistry,
	shutdown: &CancellationToken,
) -> Option<(PgStream, Option<Vec<u8>>)> {
	loop {
		let payload = read_startup_packet_raw(&mut tcp, shutdown).await?;
		match msg::parse_startup(&payload) {
			Ok(StartupMessage::SslRequest) => match acceptor {
				Some(acceptor) => {
					tcp.write_all(b"S").await.ok()?;
					match acceptor.accept(tcp).await {
						Ok(tls) => return Some((Either::Right(tls), None)),
						Err(err) => {
							debug!(target: LOG, "postgres TLS handshake failed: {err}");
							return None;
						}
					}
				}
				None => tcp.write_all(b"N").await.ok()?,
			},
			// GSSAPI encryption is not supported; decline and await startup.
			Ok(StartupMessage::GssEncRequest) => tcp.write_all(b"N").await.ok()?,
			Ok(StartupMessage::CancelRequest {
				pid,
				secret,
			}) => {
				if let Some(handle) = registry.get(&(pid, secret)) {
					handle.trip();
				}
				return None;
			}
			Ok(StartupMessage::Startup {
				..
			}) => return Some((Either::Left(tcp), Some(payload))),
			Err(_) => return None,
		}
	}
}

/// Read one startup-phase packet's payload from a raw stream (length prefix
/// consumed). Returns `None` on shutdown, clean disconnect, or a bad length.
async fn read_startup_packet_raw(
	tcp: &mut TcpStream,
	shutdown: &CancellationToken,
) -> Option<Vec<u8>> {
	let len = tokio::select! {
		biased;
		_ = shutdown.cancelled() => return None,
		len = tcp.read_i32() => len.ok()?,
	};
	read_startup_payload(tcp, len).await.ok().flatten()
}

/// Validate an already-read startup-phase length prefix and read the remaining
/// payload. `Ok(None)` means the length was outside the accepted range; the
/// caller decides how to react (each startup reader handles the bad-length case
/// differently). Shared by all three startup-phase framing readers.
async fn read_startup_payload<R>(reader: &mut R, len: i32) -> Result<Option<Vec<u8>>>
where
	R: AsyncReadExt + Unpin,
{
	let len = usize::try_from(len).unwrap_or(0);
	if !(8..=msg::MAX_STARTUP_PACKET_SIZE).contains(&len) {
		return Ok(None);
	}
	let mut payload = vec![0u8; len - 4];
	reader.read_exact(&mut payload).await?;
	Ok(Some(payload))
}

/// Complete just enough of the handshake to tell an over-the-limit client the
/// server is full, then close. Runs without a connection permit.
pub(super) async fn reject_overloaded(mut stream: TcpStream) {
	let _ = reject_overloaded_inner(&mut stream).await;
}

async fn reject_overloaded_inner(stream: &mut TcpStream) -> Result<()> {
	// Answer SSL/GSS probes with 'N' until the startup packet arrives, so the
	// client reaches the point where an ErrorResponse is understood.
	loop {
		let len = stream.read_i32().await?;
		let Some(payload) = read_startup_payload(stream, len).await? else {
			return Ok(());
		};
		match msg::parse_startup(&payload) {
			Ok(StartupMessage::SslRequest) | Ok(StartupMessage::GssEncRequest) => {
				let mut buf = BytesMut::new();
				msg::write_ssl_response(&mut buf, false);
				stream.write_all(&buf).await?;
			}
			_ => break,
		}
	}
	let mut buf = BytesMut::new();
	msg::write_error_response(&mut buf, &PgError::too_many_connections());
	stream.write_all(&buf).await?;
	stream.flush().await?;
	Ok(())
}

struct Connection {
	stream: PgStream,
	/// A StartupMessage packet already read during TLS negotiation (plaintext
	/// path), consumed on the first startup read.
	prelude: Option<Vec<u8>>,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	session: Session,
	statements: HashMap<String, PreparedStatement>,
	portals: HashMap<String, Portal>,
	prepared_bytes: usize,
	portal_bytes: usize,
	/// The query language this connection speaks. Chosen at startup via
	/// `options=-c dialect=...` and switchable in-session with `SET dialect`.
	/// Only ever assigned through [`Connection::select_dialect`], which applies
	/// the per-dialect capability gate.
	dialect: Dialect,
	/// The active interactive transaction opened by a standalone `BEGIN`.
	transaction: Option<Arc<Transaction>>,
	/// Set when a statement fails inside an interactive transaction; further
	/// queries are refused (SQLSTATE 25P02) until `COMMIT`/`ROLLBACK`.
	txn_failed: bool,
	/// Values of the top-level `LET` statements that succeeded inside the open
	/// interactive transaction, in execution order. They are applied to the
	/// session variables when the transaction commits and discarded when it
	/// ends any other way.
	txn_lets: Vec<(String, Value)>,
	/// Cancellation handle for the in-flight query, tripped by a matching
	/// CancelRequest on a side connection.
	cancel: CancelHandle,
	/// Shared (pid, secret) → cancel-handle map, consulted when this
	/// connection receives a CancelRequest for another.
	registry: Arc<CancelRegistry>,
	pid: i32,
	secret: i32,
	/// Held for the connection's lifetime to bound concurrent connections.
	_permit: OwnedSemaphorePermit,
}

impl Connection {
	async fn run(&mut self) -> Result<()> {
		// Bound the pre-command phase so an idle or slow-loris client cannot
		// hold a connection (and its resources) open indefinitely before
		// authenticating.
		let authed = match timeout(STARTUP_TIMEOUT, self.startup_and_auth()).await {
			Ok(res) => res?,
			Err(_) => {
				let _ = self.report(&PgError::auth_timeout()).await;
				return Ok(());
			}
		};
		if !authed {
			return Ok(());
		}
		self.command_loop().await
	}

	async fn startup_and_auth(&mut self) -> Result<bool> {
		let Some(params) = self.startup().await? else {
			return Ok(false);
		};
		self.authenticate(params).await
	}

	/// Read and process the StartupMessage (TLS/SSL/Cancel were handled during
	/// negotiation). Returns the startup parameters, or `None` to close.
	async fn startup(&mut self) -> Result<Option<Vec<(String, String)>>> {
		// The plaintext path already read the StartupMessage during
		// negotiation; the TLS path reads it fresh over the encrypted stream.
		let payload = match self.prelude.take() {
			Some(payload) => payload,
			None => match self.read_startup_packet().await? {
				Some(payload) => payload,
				None => return Ok(None),
			},
		};
		match msg::parse_startup(&payload) {
			Ok(StartupMessage::Startup {
				version,
				params,
			}) => {
				let major = version >> 16;
				let minor = version & 0xffff;
				if major != 3 {
					self.report(
						&PgError::protocol(format!(
							"unsupported protocol version {major}.{minor} (only 3.x is supported)"
						))
						.fatal(),
					)
					.await?;
					return Ok(None);
				}
				// A 3.x client newer than 3.0 (or one that sent `_pq_.*`
				// protocol options) must be told exactly which version and
				// options this server supports before proceeding.
				if minor > 0 || params.iter().any(|(k, _)| k.starts_with("_pq_.")) {
					let unsupported: Vec<String> = params
						.iter()
						.filter(|(k, _)| k.starts_with("_pq_."))
						.map(|(k, _)| k.clone())
						.collect();
					let mut buf = BytesMut::new();
					msg::write_negotiate_protocol_version(&mut buf, &unsupported);
					self.stream.write_all(&buf).await?;
				}
				Ok(Some(params))
			}
			// A second SSL/GSS/Cancel request here is a protocol violation.
			Ok(_) => {
				self.report(&PgError::protocol("unexpected request during startup").fatal())
					.await?;
				Ok(None)
			}
			Err(err) => {
				self.report(&err).await?;
				Ok(None)
			}
		}
	}

	/// Authenticate the connection, returning `false` when it must close.
	async fn authenticate(&mut self, params: Vec<(String, String)>) -> Result<bool> {
		if !self.ds.allows_http_route(&RouteTarget::Postgres) {
			warn!(
				target: LOG,
				"Capabilities denied postgres connection attempt from {}", self.peer
			);
			self.report(
				&PgError::insufficient_privilege(
					"Forbidden: the postgres protocol is not allowed on this server",
				)
				.fatal(),
			)
			.await?;
			return Ok(false);
		}
		if !self.ready.load(Ordering::SeqCst) {
			self.report(&PgError::cannot_connect_now("the database system is starting up").fatal())
				.await?;
			return Ok(false);
		}
		let mut user = None;
		let mut database = None;
		let mut options = None;
		for (key, value) in params {
			match key.as_str() {
				"user" => user = Some(value),
				"database" => database = Some(value),
				"options" => options = Some(value),
				_ => {}
			}
		}
		let Some(user) = user else {
			self.report(&PgError::protocol("no user specified in startup packet").fatal()).await?;
			return Ok(false);
		};
		// Apply a connect-time `options=-c dialect=...` selection. A dialect the
		// deployment denies closes the connection rather than leaving it in
		// SurrealQL, which would answer the client's queries with parse errors
		// instead of the reason they were refused.
		if let Some(options) = options.as_deref()
			&& let Some(dialect) = dialect_from_options(options)
			&& let Err(err) = self.select_dialect(dialect)
		{
			self.report(&err.fatal()).await?;
			return Ok(false);
		}
		let (ns, db) = parse_database_param(&user, database.as_deref());
		self.session = Session::default();
		self.session.ip = Some(self.peer.ip().to_string());
		self.session.id = Some(uuid::Uuid::new_v4());
		if self.ds.is_auth_enabled() {
			// Prefer SASL/SCRAM-SHA-256 when the target user has SCRAM verifier
			// material; fall back to cleartext-password (over TLS) for users
			// that only carry a legacy Argon2 hash.
			let scram =
				match verify::scram_lookup(&self.ds, &user, ns.as_deref(), db.as_deref()).await {
					Ok(scram) => scram,
					Err(err) => {
						// A lookup failure is not disclosed to the client; it is
						// reported as a generic authentication failure.
						debug!(target: LOG, "postgres SCRAM lookup failed for user '{user}': {err}");
						self.report(&auth_failed(&user)).await?;
						return Ok(false);
					}
				};
			let authenticated = match scram {
				Some(scram) => self.authenticate_scram(&user, &scram).await?,
				None => {
					if self.is_tls() {
						self.authenticate_cleartext(&user, ns.as_deref(), db.as_deref()).await?
					} else {
						self.authenticate_scram_mock(&user).await?
					}
				}
			};
			if !authenticated {
				return Ok(false);
			}
		}
		// Enforce the query-capability gate BEFORE selecting the namespace and
		// database: `process_use` can materialize a namespace/database as a
		// side effect, so a caller that is not permitted to query must never
		// reach it.
		if !self.ds.allows_query_by_subject(self.session.au.as_ref()) {
			self.report(
				&PgError::insufficient_privilege(
					"Forbidden: this user is not allowed to query the database",
				)
				.fatal(),
			)
			.await?;
			return Ok(false);
		}
		if ns.is_some() || db.is_some() {
			// Select (and, when authorized, materialize) the namespace and
			// database exactly as a `USE` statement would.
			if let Err(err) = self.ds.process_use(None, &mut self.session, ns, db).await {
				self.report(&PgError::from(&err).fatal()).await?;
				return Ok(false);
			}
		}
		let mut buf = BytesMut::new();
		msg::write_authentication_ok(&mut buf);
		let server_version = format!("16.0-surrealdb.{}", PKG_VERSION);
		for (key, value) in [
			("server_version", server_version.as_str()),
			("server_encoding", "UTF8"),
			("client_encoding", "UTF8"),
			("DateStyle", "ISO, MDY"),
			("integer_datetimes", "on"),
			("standard_conforming_strings", "on"),
			("TimeZone", "UTC"),
		] {
			msg::write_parameter_status(&mut buf, key, value);
		}
		msg::write_backend_key_data(&mut buf, self.pid, self.secret);
		msg::write_ready_for_query(&mut buf, b'I');
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		debug!(target: LOG, "postgres connection established from {} for user '{user}'", self.peer);
		Ok(true)
	}

	/// Verify credentials against the levels the `database` startup parameter
	/// makes possible, most specific first: database, then namespace, then
	/// root. The Postgres startup message has no separate "authentication
	/// level" field (unlike the HTTP auth headers), so a database-scoped
	/// connection string must still let namespace and root users in.
	async fn verify_credentials(
		&mut self,
		user: &str,
		pass: &str,
		ns: Option<&str>,
		db: Option<&str>,
	) -> bool {
		if let (Some(ns), Some(db)) = (ns, db)
			&& basic(&self.ds, &mut self.session, user, pass, Some(ns), Some(db)).await.is_ok()
		{
			return true;
		}
		if let Some(ns) = ns
			&& basic(&self.ds, &mut self.session, user, pass, Some(ns), None).await.is_ok()
		{
			return true;
		}
		match basic(&self.ds, &mut self.session, user, pass, None, None).await {
			Ok(()) => true,
			Err(err) => {
				debug!(target: LOG, "postgres authentication failed for user '{user}': {err}");
				false
			}
		}
	}

	/// Cleartext-password authentication: request a password, then verify it
	/// against the credential levels the connection permits. Returns whether
	/// the client authenticated; on failure it has already reported the error.
	async fn authenticate_cleartext(
		&mut self,
		user: &str,
		ns: Option<&str>,
		db: Option<&str>,
	) -> Result<bool> {
		// Never ask for a cleartext password over an unencrypted connection.
		// Report the standard generic authentication failure to avoid user enumeration.
		if !self.is_tls() {
			self.report(&auth_failed(user)).await?;
			return Ok(false);
		}
		let mut buf = BytesMut::new();
		msg::write_authentication_cleartext_password(&mut buf);
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		// A password message is tiny; cap it well below the regular message
		// ceiling so an unauthenticated peer cannot force a large allocation.
		let Some((tag, payload)) = self.read_message(MAX_AUTH_MESSAGE_SIZE).await? else {
			return Ok(false);
		};
		let Frontend::Password(pass) = msg::parse_frontend(tag, &payload)? else {
			self.report(&PgError::protocol("expected a password message").fatal()).await?;
			return Ok(false);
		};
		if !self.verify_credentials(user, &pass, ns, db).await {
			self.report(&auth_failed(user)).await?;
			return Ok(false);
		}
		Ok(true)
	}

	/// Run a SCRAM-SHA-256 SASL exchange against the user's resolved verifier.
	/// The two client messages both arrive on the frontend `p` tag: a
	/// SASLInitialResponse (mechanism + client-first) then a SASLResponse (the
	/// bare client-final). On success the session is established from the
	/// resolved user and level. Channel binding is not offered.
	async fn authenticate_scram(&mut self, user: &str, scram: &ScramAuth) -> Result<bool> {
		let mut buf = BytesMut::new();
		msg::write_authentication_sasl(&mut buf, &[sasl::MECHANISM]);
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		// Client-first, carried in a SASLInitialResponse.
		let Some((tag, payload)) = self.read_message(MAX_AUTH_MESSAGE_SIZE).await? else {
			return Ok(false);
		};
		if tag != b'p' {
			self.report(&PgError::protocol("expected a SASL response message").fatal()).await?;
			return Ok(false);
		}
		let (mechanism, client_first) = match msg::parse_sasl_initial(&payload) {
			Ok(parsed) => parsed,
			Err(err) => {
				self.report(&err.fatal()).await?;
				return Ok(false);
			}
		};
		if mechanism != sasl::MECHANISM {
			self.report(
				&PgError::protocol(format!("unsupported SASL mechanism \"{mechanism}\"")).fatal(),
			)
			.await?;
			return Ok(false);
		}
		let (exchange, server_first) =
			match sasl::ScramExchange::start(&client_first, scram.salt(), scram.iterations()) {
				Ok(started) => started,
				Err(err) => {
					self.report(&err.fatal()).await?;
					return Ok(false);
				}
			};
		let mut buf = BytesMut::new();
		msg::write_authentication_sasl_continue(&mut buf, server_first.as_bytes());
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		// Client-final, carried in a SASLResponse (its whole payload is the
		// SCRAM client-final message).
		let Some((tag, payload)) = self.read_message(MAX_AUTH_MESSAGE_SIZE).await? else {
			return Ok(false);
		};
		if tag != b'p' {
			self.report(&PgError::protocol("expected a SASL response message").fatal()).await?;
			return Ok(false);
		}
		let server_final = match exchange.finish(&payload, scram) {
			Ok(final_message) => final_message,
			Err(_) => {
				self.report(&auth_failed(user)).await?;
				return Ok(false);
			}
		};
		let mut buf = BytesMut::new();
		msg::write_authentication_sasl_final(&mut buf, server_final.as_bytes());
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		// The client proof verified, so establish the session for the resolved
		// user and level.
		if let Err(err) = scram.apply(&mut self.session) {
			self.report(&PgError::internal(err.to_string()).fatal()).await?;
			return Ok(false);
		}
		Ok(true)
	}

	/// Run a mock SCRAM exchange for unknown users over unencrypted connections,
	/// ensuring unknown and known-bad users are indistinguishable.
	async fn authenticate_scram_mock(&mut self, user: &str) -> Result<bool> {
		let mut buf = BytesMut::new();
		msg::write_authentication_sasl(&mut buf, &[sasl::MECHANISM]);
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		let Some((tag, payload)) = self.read_message(MAX_AUTH_MESSAGE_SIZE).await? else {
			return Ok(false);
		};
		if tag != b'p' {
			self.report(&PgError::protocol("expected a SASL response message").fatal()).await?;
			return Ok(false);
		}
		let (mechanism, client_first) = match msg::parse_sasl_initial(&payload) {
			Ok(parsed) => parsed,
			Err(err) => {
				self.report(&err.fatal()).await?;
				return Ok(false);
			}
		};
		if mechanism != sasl::MECHANISM {
			self.report(
				&PgError::protocol(format!("unsupported SASL mechanism \"{mechanism}\"")).fatal(),
			)
			.await?;
			return Ok(false);
		}
		use sha2::{Digest, Sha256};
		let mut hasher = Sha256::new();
		hasher.update(b"mock_salt_pgwire:");
		hasher.update(user.as_bytes());
		let mock_salt = &hasher.finalize()[..16];

		let (_exchange, server_first) =
			match sasl::ScramExchange::start(&client_first, mock_salt, 4096) {
				Ok(started) => started,
				Err(err) => {
					self.report(&err.fatal()).await?;
					return Ok(false);
				}
			};
		let mut buf = BytesMut::new();
		msg::write_authentication_sasl_continue(&mut buf, server_first.as_bytes());
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;

		let Some((tag, _payload)) = self.read_message(MAX_AUTH_MESSAGE_SIZE).await? else {
			return Ok(false);
		};
		if tag != b'p' {
			self.report(&PgError::protocol("expected a SASL response message").fatal()).await?;
			return Ok(false);
		}
		self.report(&auth_failed(user)).await?;
		Ok(false)
	}

	async fn command_loop(&mut self) -> Result<()> {
		// Backend replies to extended-protocol messages accumulate here and are
		// flushed on Sync (with a trailing ReadyForQuery) or Flush.
		let mut out = BytesMut::new();
		// Once an extended-protocol message errors, later Parse/Bind/Describe/
		// Execute/Close are ignored until the next Sync (Postgres semantics).
		let mut skip_until_sync = false;
		loop {
			let Some((tag, payload)) = self.read_message(msg::MAX_MESSAGE_SIZE).await? else {
				return Ok(());
			};
			let message = match msg::parse_frontend(tag, &payload) {
				Ok(message) => message,
				Err(err) => {
					msg::write_error_response(&mut out, &err);
					skip_until_sync = true;
					continue;
				}
			};
			match message {
				Frontend::Query(sql) => {
					self.flush(&mut out).await?;
					skip_until_sync = false;
					self.simple_query(&sql).await?;
				}
				Frontend::Parse {
					name,
					query,
					param_types,
				} => {
					if skip_until_sync {
						continue;
					}
					// Transaction-control statements (BEGIN/COMMIT/ROLLBACK) run
					// even in a failed transaction so the client can end the block
					// over the extended protocol, matching the simple-query path.
					if transaction_control(&query).is_none()
						&& let Some(err) = self.aborted_txn_guard()
					{
						msg::write_error_response(&mut out, &err);
						skip_until_sync = true;
						continue;
					}
					if let Err(err) = self.handle_parse(name, &query, param_types, &mut out) {
						self.note_extended_error(&err, &mut out);
						skip_until_sync = true;
					}
				}
				Frontend::Bind {
					portal,
					statement,
					param_formats,
					params,
					result_formats,
				} => {
					if skip_until_sync {
						continue;
					}
					if !self.statement_is_txn_control(&statement)
						&& let Some(err) = self.aborted_txn_guard()
					{
						msg::write_error_response(&mut out, &err);
						skip_until_sync = true;
						continue;
					}
					if let Err(err) = self.handle_bind(
						portal,
						statement,
						&param_formats,
						&params,
						result_formats,
						&mut out,
					) {
						self.note_extended_error(&err, &mut out);
						skip_until_sync = true;
					}
				}
				Frontend::Describe {
					target,
					name,
				} => {
					if skip_until_sync {
						continue;
					}
					let is_txn_control = match target {
						DescribeTarget::Statement => self.statement_is_txn_control(&name),
						DescribeTarget::Portal => self.portal_is_txn_control(&name),
					};
					if !is_txn_control && let Some(err) = self.aborted_txn_guard() {
						msg::write_error_response(&mut out, &err);
						skip_until_sync = true;
						continue;
					}
					if let Err(err) = self.handle_describe(target, &name, &mut out).await {
						self.note_extended_error(&err, &mut out);
						skip_until_sync = true;
					}
				}
				Frontend::Execute {
					portal,
					max_rows,
				} => {
					if skip_until_sync {
						continue;
					}
					if !self.portal_is_txn_control(&portal)
						&& let Some(err) = self.aborted_txn_guard()
					{
						msg::write_error_response(&mut out, &err);
						skip_until_sync = true;
						continue;
					}
					if let Err(err) = self.handle_execute(&portal, max_rows, &mut out).await? {
						self.note_extended_error(&err, &mut out);
						skip_until_sync = true;
					}
				}
				Frontend::Close {
					target,
					name,
				} => {
					if skip_until_sync {
						continue;
					}
					self.handle_close(target, &name, &mut out);
				}
				Frontend::Sync => {
					skip_until_sync = false;
					// End of the implicit transaction: portals do not survive it.
					self.portals.clear();
					self.portal_bytes = 0;
					let status = self.ready_status();
					msg::write_ready_for_query(&mut out, status);
					self.flush(&mut out).await?;
				}
				Frontend::Flush => self.flush(&mut out).await?,
				Frontend::Terminate => return Ok(()),
				Frontend::Password(_) => {
					self.report(&PgError::protocol("unexpected password message").fatal()).await?;
					return Ok(());
				}
				Frontend::Unknown(tag) => {
					self.report(
						&PgError::protocol(format!(
							"unexpected message type '{}'",
							char::from(tag)
						))
						.fatal(),
					)
					.await?;
					return Ok(());
				}
			}
		}
	}

	/// Write and drain the pending backend buffer to the socket.
	async fn flush(&mut self, out: &mut BytesMut) -> Result<()> {
		if !out.is_empty() {
			self.stream.write_all(out).await?;
			out.clear();
		}
		self.stream.flush().await?;
		Ok(())
	}

	fn insert_statement(&mut self, name: String, stmt: PreparedStatement) -> Result<(), PgError> {
		let size = stmt.estimated_size();
		let old_size = self.statements.get(&name).map(|s| s.estimated_size()).unwrap_or(0);
		let new_total = (self.prepared_bytes + size).saturating_sub(old_size);
		if new_total > MAX_PREPARED_BYTES {
			return Err(PgError::too_many_prepared("prepared statement cache byte limit exceeded"));
		}
		self.prepared_bytes = new_total;
		global_metrics().record_prepared_stmt(size);
		if old_size > 0 {
			global_metrics().record_closed_stmt(old_size);
		}
		self.statements.insert(name, stmt);
		Ok(())
	}

	fn remove_statement(&mut self, name: &str) -> Option<PreparedStatement> {
		if let Some(stmt) = self.statements.remove(name) {
			let size = stmt.estimated_size();
			self.prepared_bytes = self.prepared_bytes.saturating_sub(size);
			global_metrics().record_closed_stmt(size);
			Some(stmt)
		} else {
			None
		}
	}

	fn insert_portal(&mut self, name: String, portal: Portal) -> Result<(), PgError> {
		let size = portal.estimated_size();
		let old_size = self.portals.get(&name).map(|p| p.estimated_size()).unwrap_or(0);
		let new_total = (self.portal_bytes + size).saturating_sub(old_size);
		if new_total > MAX_PORTAL_BYTES {
			return Err(PgError::too_many_prepared("portal memory limit exceeded"));
		}
		self.portal_bytes = new_total;
		self.portals.insert(name, portal);
		Ok(())
	}

	fn remove_portal(&mut self, name: &str) -> Option<Portal> {
		if let Some(portal) = self.portals.remove(name) {
			let size = portal.estimated_size();
			self.portal_bytes = self.portal_bytes.saturating_sub(size);
			Some(portal)
		} else {
			None
		}
	}

	fn handle_parse(
		&mut self,
		name: String,
		query: &str,
		param_types: Vec<i32>,
		out: &mut BytesMut,
	) -> Result<(), PgError> {
		// A named statement may not be redefined while it exists; the unnamed
		// statement ("") is always replaceable.
		if !name.is_empty() && self.statements.contains_key(&name) {
			return Err(PgError::duplicate_statement(&name));
		}
		if !self.statements.contains_key(&name) && self.statements.len() >= MAX_PREPARED {
			return Err(PgError::too_many_prepared("prepared statements"));
		}
		// Standalone transaction-control and `SET` statements are intercepted in
		// the extended protocol just as in the simple-query path — before the
		// SurrealQL parser, which rejects `SET` and would run a bare `BEGIN` as an
		// ordinary executor statement. Transaction control is dialect-independent;
		// `SET` interception is gated by `should_intercept_set` so that in the GQL
		// dialect a `SET` mutation clause still reaches the GQL executor.
		if let Some(control) = transaction_control(query).map(Control::Transaction).or_else(|| {
			parse_set(query).filter(|set| self.should_intercept_set(set)).map(Control::Set)
		}) {
			self.insert_statement(
				name,
				PreparedStatement {
					query: query.to_string(),
					cached_plan: None,
					param_types: Vec::new(),
					param_count: 0,
					empty: false,
					described: false,
					control: Some(control),
					descriptor: None,
				},
			)?;
			msg::write_parse_complete(out);
			return Ok(());
		}
		let (query, cached_plan, param_count, empty, descriptor) = match self.dialect {
			Dialect::SurrealQl => {
				// Parse once so syntax errors surface at Parse (as Postgres does)
				// and the AST can be cached for repeated Execute without re-parsing.
				let (rewritten, positional) = rewrite_positional_params(query);
				let config = self.ds.parser_config();
				let ast =
					syn::parse_with_capabilities(&rewritten, &self.ds.get_capabilities(), &config)
						.map_err(|err| PgError::syntax(err.to_string()))?;
				let empty = ast.num_statements() == 0;
				let param_count = param_types.len().max(positional);
				let cached_plan = if empty {
					None
				} else {
					Some(CachedPlan::SurrealQl(ast))
				};
				(rewritten, cached_plan, param_count, empty, None)
			}
			Dialect::PgSql => {
				global_metrics().record_query(SqlDialect::Postgres);
				let stmts = surrealdb_pgsql::parse_statements(query)
					.map_err(|err| PgError::syntax(err.to_string()))?;
				let empty = stmts.is_empty();
				let (_, positional) = rewrite_positional_params(query);
				let mut param_count = param_types.len().max(positional);
				let mut descriptor = None;
				let mut cached_plan = None;
				if !empty {
					match &stmts[0] {
						PgStatement::Select(select) => {
							let mut catalog = MemoryCatalog::new();
							if let Some(db) = &self.session.db {
								catalog.set_database(Some(db.clone()));
							}
							let policy = DialectPolicy::postgres();
							if let Ok(bound) = bind_select(select, &catalog, &policy, None) {
								let desc = bound.describe(&catalog, &policy);
								let param_desc = bound.describe_parameters(&catalog);
								param_count = param_count.max(param_desc.param_types.len());
								descriptor = Some(desc);
							}
							if let Ok(op) = lower_stmt(&stmts[0]) {
								cached_plan = Some(CachedPlan::PgSelect {
									plan: Box::new(op),
								});
							}
						}
						PgStatement::Insert(insert) => {
							if let Ok(expr) = lower_pgsql_insert(insert) {
								cached_plan = Some(CachedPlan::PgAst(Ast {
									expressions: vec![TopLevelExpr::Expr(expr)],
								}));
							}
						}
						PgStatement::Update(update) => {
							if let Ok(expr) = lower_pgsql_update(update) {
								cached_plan = Some(CachedPlan::PgAst(Ast {
									expressions: vec![TopLevelExpr::Expr(expr)],
								}));
							}
						}
						PgStatement::Delete(delete) => {
							if let Ok(expr) = lower_pgsql_delete(delete) {
								cached_plan = Some(CachedPlan::PgAst(Ast {
									expressions: vec![TopLevelExpr::Expr(expr)],
								}));
							}
						}
						PgStatement::Truncate(truncate) => {
							if let Ok(expr) = lower_pgsql_truncate(truncate) {
								cached_plan = Some(CachedPlan::PgAst(Ast {
									expressions: vec![TopLevelExpr::Expr(expr)],
								}));
							}
						}
						PgStatement::CreateTable(_)
						| PgStatement::CreateIndex(_)
						| PgStatement::CreateView(_)
						| PgStatement::AlterTable(_)
						| PgStatement::Drop(_) => {
							if let Ok(exprs) = lower_pgsql_ddl(&stmts[0]) {
								cached_plan = Some(CachedPlan::PgAst(Ast {
									expressions: exprs
										.into_iter()
										.map(TopLevelExpr::Expr)
										.collect(),
								}));
							}
						}
						_ => {}
					}
				}
				(query.to_string(), cached_plan, param_count, empty, descriptor)
			}
			Dialect::Gql | Dialect::MySql => {
				let (_, positional) = rewrite_positional_params(query);
				let param_count = param_types.len().max(positional);
				let empty = query.trim().is_empty();
				(query.to_string(), None, param_count, empty, None)
			}
		};
		// Reject an absurd parameter count before it reaches the
		// ParameterDescription allocation or the i16 wire count.
		if param_count > MAX_PARAMS {
			return Err(PgError::protocol(format!(
				"statement references {param_count} parameters, exceeding the maximum of {MAX_PARAMS}"
			)));
		}
		self.insert_statement(
			name,
			PreparedStatement {
				query,
				cached_plan,
				param_count,
				param_types,
				empty,
				described: false,
				control: None,
				descriptor,
			},
		)?;
		msg::write_parse_complete(out);
		Ok(())
	}

	fn handle_bind(
		&mut self,
		portal: String,
		statement: String,
		param_formats: &[i16],
		params: &[Option<Vec<u8>>],
		result_formats: Vec<i16>,
		out: &mut BytesMut,
	) -> Result<(), PgError> {
		let Some(stmt) = self.statements.get(&statement) else {
			return Err(PgError::invalid_statement(&statement));
		};
		// Postgres rejects an under-supplied Bind at Bind time, not later.
		if params.len() < stmt.param_count {
			return Err(PgError::protocol(format!(
				"bind message supplies {} parameters, but prepared statement \"{statement}\" requires {}",
				params.len(),
				stmt.param_count
			)));
		}
		if !self.portals.contains_key(&portal) && self.portals.len() >= MAX_PORTALS {
			return Err(PgError::too_many_prepared("portals"));
		}
		// Decode only the parameters the statement actually references (bounded by
		// param_count, guaranteed <= params.len() above). Extra client-supplied
		// params are ignored, and a control statement (param_count 0) decodes
		// none — so binding a stray value to `BEGIN`/`SET` can't spuriously fail.
		let mut vars = Variables::new();
		for (i, raw) in params.iter().enumerate().take(stmt.param_count) {
			let oid = stmt.param_types.get(i).copied().unwrap_or(0);
			let format = format_at(param_formats, i);
			let value = encode::decode_param(raw.as_deref(), oid, format)?;
			// Positional `$1..$n` were rewritten to `$_1..$_n`, so bind to the
			// matching `_1.._n` variable names.
			let param_num = i + 1;
			vars.insert(surrealdb_types::positional_param_name(param_num), value.clone());
			vars.insert(format!("param_{param_num}"), value);
		}
		self.insert_portal(
			portal,
			Portal {
				statement,
				params: vars,
				result_formats,
				executed: None,
				control_ran: false,
			},
		)?;
		msg::write_bind_complete(out);
		Ok(())
	}

	async fn handle_describe(
		&mut self,
		target: DescribeTarget,
		name: &str,
		out: &mut BytesMut,
	) -> Result<(), PgError> {
		match target {
			DescribeTarget::Statement => {
				let Some(stmt) = self.statements.get_mut(name) else {
					return Err(PgError::invalid_statement(name));
				};
				// A control statement (transaction op / SET) takes no parameters
				// and produces no result rows.
				if stmt.control.is_some() {
					msg::write_parameter_description(out, &[]);
					msg::write_no_data(out);
					return Ok(());
				}
				// If a static descriptor is available, report true columns and types;
				// otherwise pin this statement to the single-`jsonb`-column shape.
				stmt.described = true;
				let empty = stmt.empty;
				// Report one OID per referenced parameter, using the declared
				// type where given and unspecified (0) otherwise.
				let oids: Vec<i32> = (0..stmt.param_count)
					.map(|i| stmt.param_types.get(i).copied().unwrap_or(0))
					.collect();
				msg::write_parameter_description(out, &oids);
				if empty {
					msg::write_no_data(out);
				} else if let Some(desc) = &stmt.descriptor {
					let columns: Vec<PgColumn> = desc
						.columns
						.iter()
						.map(|c| PgColumn {
							name: c.name.clone(),
							ty: PgType::from_sql_type(c.ty),
						})
						.collect();
					msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
				} else {
					let columns = [PgColumn {
						name: "result".to_string(),
						ty: PgType::Jsonb,
					}];
					// Bind has not happened yet, so the format is nominal (text);
					// the actual DataRow format follows the eventual Bind.
					msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
				}
			}
			DescribeTarget::Portal => {
				// A control statement produces no result rows.
				if self.portal_is_control(name) {
					msg::write_no_data(out);
					return Ok(());
				}
				if self.portal_is_empty(name)? {
					msg::write_no_data(out);
					return Ok(());
				}
				self.ensure_executed(name).await?;
				let portal = self.portals.get(name).expect("ensured above");
				let formats = portal.result_formats.clone();
				let columns = &portal.executed.as_ref().expect("ensured above").columns;
				msg::write_row_description(out, columns, |i| format_at(&formats, i));
			}
		}
		Ok(())
	}

	/// Stream a portal's rows. The outer `Result` is I/O (a flush failure
	/// terminates the connection); the inner `Result<(), PgError>` is a
	/// query-level error to report as an ErrorResponse. Rows are drained one at
	/// a time and flushed past `FLUSH_THRESHOLD`, so a large fetch-all Execute
	/// does not buffer the whole encoded result set in memory.
	async fn handle_execute(
		&mut self,
		portal_name: &str,
		max_rows: i32,
		out: &mut BytesMut,
	) -> Result<Result<(), PgError>> {
		// A control statement (transaction op / SET) is run by its handler rather
		// than the query executor; it produces a CommandComplete, not rows.
		if let Some(control) = self.portal_control(portal_name) {
			// Run the side effect at most once per portal: a re-Execute before
			// Sync must not repeat BEGIN/COMMIT/etc. A completed control portal
			// re-reports its command tag with no further effect (as Postgres does
			// for an exhausted non-row portal).
			if self.portals.get(portal_name).is_some_and(|p| p.control_ran) {
				msg::write_command_complete(out, control.command_tag());
				return Ok(Ok(()));
			}
			if let Some(portal) = self.portals.get_mut(portal_name) {
				portal.control_ran = true;
			}
			// Propagate a handler error as the inner Result so the dispatch loop
			// reports it and poisons the batch (skip-until-Sync), matching how an
			// ordinary statement error is handled.
			let result = match control {
				Control::Transaction(op) => self.handle_transaction_control(op, out).await,
				Control::Set(set) => self.handle_set(set, out),
			};
			return Ok(result);
		}
		match self.portal_is_empty(portal_name) {
			Ok(true) => {
				msg::write_empty_query_response(out);
				return Ok(Ok(()));
			}
			Ok(false) => {}
			Err(err) => return Ok(Err(err)),
		}
		if let Err(err) = self.ensure_executed(portal_name).await {
			return Ok(Err(err));
		}
		// Snapshot the (small) column + format metadata so the per-row loop can
		// hold the portal borrow only briefly, freeing `self.stream` for flushes.
		let (columns, formats) = {
			let portal = self.portals.get(portal_name).expect("ensured above");
			let executed = portal.executed.as_ref().expect("ensured above");
			(executed.columns.clone(), portal.result_formats.clone())
		};
		let limit = if max_rows <= 0 {
			usize::MAX
		} else {
			max_rows as usize
		};
		let mut sent = 0usize;
		while sent < limit {
			let row = {
				let portal = self.portals.get_mut(portal_name).expect("ensured above");
				portal.executed.as_mut().expect("ensured above").rows.pop_front()
			};
			let Some(row) = row else {
				break;
			};
			let cells = match encode_row(row, &columns, &formats) {
				Ok(cells) => cells,
				Err(err) => return Ok(Err(err)),
			};
			msg::write_data_row(out, &cells);
			sent += 1;
			if out.len() >= FLUSH_THRESHOLD {
				self.stream.write_all(out).await?;
				out.clear();
			}
		}
		let remaining = {
			let portal = self.portals.get(portal_name).expect("ensured above");
			!portal.executed.as_ref().expect("ensured above").rows.is_empty()
		};
		if remaining {
			// More rows remain for a subsequent Execute on this portal.
			msg::write_portal_suspended(out);
		} else {
			msg::write_command_complete(out, &format!("SELECT {sent}"));
		}
		Ok(Ok(()))
	}

	/// Whether the portal's prepared statement has an empty query. Errors if
	/// the portal or its statement is missing.
	fn portal_is_empty(&self, portal_name: &str) -> Result<bool, PgError> {
		let portal =
			self.portals.get(portal_name).ok_or_else(|| PgError::invalid_cursor(portal_name))?;
		let stmt = self
			.statements
			.get(&portal.statement)
			.ok_or_else(|| PgError::invalid_statement(&portal.statement))?;
		Ok(stmt.empty)
	}

	/// The intercepted control action for a prepared statement, if any (borrowed).
	/// Sole `statements -> control` accessor, so the predicates below cannot drift.
	fn statement_control(&self, name: &str) -> Option<&Control> {
		self.statements.get(name)?.control.as_ref()
	}

	/// The intercepted control action for a portal's statement, if any (borrowed).
	fn portal_control_ref(&self, portal_name: &str) -> Option<&Control> {
		self.statement_control(&self.portals.get(portal_name)?.statement)
	}

	/// Owned clone of a portal's control action, for callers that need to run it
	/// after releasing the `&self` borrow (e.g. [`Self::handle_execute`]).
	fn portal_control(&self, portal_name: &str) -> Option<Control> {
		self.portal_control_ref(portal_name).cloned()
	}

	/// Whether a portal's statement is an intercepted control statement.
	fn portal_is_control(&self, portal_name: &str) -> bool {
		self.portal_control_ref(portal_name).is_some()
	}

	/// Whether a prepared statement is a standalone transaction-control op.
	/// These may run even inside a failed transaction (like the simple-query
	/// path) so the client can end the block over the extended protocol.
	fn statement_is_txn_control(&self, name: &str) -> bool {
		matches!(self.statement_control(name), Some(Control::Transaction(_)))
	}

	/// Whether a portal's statement is a standalone transaction-control op.
	fn portal_is_txn_control(&self, portal_name: &str) -> bool {
		matches!(self.portal_control_ref(portal_name), Some(Control::Transaction(_)))
	}

	fn handle_close(&mut self, target: DescribeTarget, name: &str, out: &mut BytesMut) {
		match target {
			DescribeTarget::Statement => {
				self.remove_statement(name);
				// Drop any portals bound to the closed statement.
				let doomed: Vec<String> = self
					.portals
					.iter()
					.filter(|(_, p)| p.statement == name)
					.map(|(k, _)| k.clone())
					.collect();
				for p in doomed {
					self.remove_portal(&p);
				}
			}
			DescribeTarget::Portal => {
				self.remove_portal(name);
			}
		}
		// Closing a non-existent statement/portal is not an error in Postgres.
		msg::write_close_complete(out);
	}

	/// Execute a bound portal's query (once) and buffer its result rows in the
	/// portal, choosing the wire shape from whether the statement was described.
	async fn ensure_executed(&mut self, portal_name: &str) -> Result<(), PgError> {
		if self.portals.get(portal_name).is_some_and(|p| p.executed.is_some()) {
			return Ok(());
		}
		let Some(portal) = self.portals.get(portal_name) else {
			return Err(PgError::invalid_cursor(portal_name));
		};
		let statement = portal.statement.clone();
		let params = portal.params.clone();
		let Some(stmt) = self.statements.get(&statement) else {
			return Err(PgError::invalid_statement(&statement));
		};
		let jsonb_shape = stmt.described && stmt.descriptor.is_none();
		let descriptor = stmt.descriptor.clone();
		let cached_plan = stmt.cached_plan.clone();
		let query = stmt.query.clone();
		let value = self.execute_query(cached_plan, &query, params).await?;
		let shape = if let Some(desc) = &descriptor {
			shape_result_with_descriptor(desc, value)
		} else if jsonb_shape {
			shape_result_jsonb(value)
		} else {
			shape_result(value)
		};
		if shape.columns.len() > msg::MAX_COLUMNS {
			return Err(PgError::feature_not_supported(format!(
				"result has {} columns, exceeding the maximum of {}",
				shape.columns.len(),
				msg::MAX_COLUMNS
			)));
		}
		let old_portal_size = {
			let portal = self.portals.get(portal_name).expect("checked above");
			portal.estimated_size()
		};
		let portal = self.portals.get_mut(portal_name).expect("checked above");
		portal.executed = Some(Executed {
			columns: shape.columns,
			rows: shape.rows.into(),
		});
		let new_portal_size = portal.estimated_size();
		let new_total = (self.portal_bytes + new_portal_size).saturating_sub(old_portal_size);
		if new_total > MAX_PORTAL_BYTES {
			portal.executed = None;
			return Err(PgError::too_many_prepared("portal memory limit exceeded"));
		}
		self.portal_bytes = new_total;
		Ok(())
	}

	/// Run a bound extended-protocol query with its parameters, returning the
	/// value of its last statement. The first statement error aborts the whole
	/// query. Honors the connection dialect and any open interactive
	/// transaction. SurrealQL executes the AST cached at Parse (no re-parse);
	/// GQL executes its query text through the GQL executor.
	async fn execute_query(
		&mut self,
		cached_plan: Option<CachedPlan>,
		query: &str,
		params: Variables,
	) -> Result<Value, PgError> {
		self.check_current_dialect()?;
		if self.dialect == Dialect::Gql && self.transaction.is_some() {
			return Err(PgError::feature_not_supported(
				"GQL queries cannot run inside an interactive transaction",
			));
		}
		let cancel = self.arm_cancel();
		let results = match (self.dialect, self.transaction.clone(), cached_plan) {
			// GQL is never parsed at Parse time (different parser, different
			// parameter syntax), so it reaches here as text. It cannot be in a
			// transaction: the guard above refused that.
			(Dialect::Gql, _, _) => self
				.ds
				.run(
					QueryRequest::new(QuerySource::gql(query), &self.session)
						.with_variables(Some(params))
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
			(
				Dialect::PgSql,
				transaction,
				Some(CachedPlan::PgSelect {
					plan,
				}),
			) => self
				.ds
				.run(
					QueryRequest::new(QuerySource::Plan(vec![*plan]), &self.session)
						.with_variables(Some(params))
						.with_optional_transaction(transaction)
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
			(Dialect::PgSql, transaction, Some(CachedPlan::PgAst(ast))) => self
				.ds
				.run(
					QueryRequest::new(ast, &self.session)
						.with_variables(Some(params))
						.with_optional_transaction(transaction)
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
			(Dialect::PgSql, transaction, _) => self
				.ds
				.run(
					QueryRequest::new(QuerySource::pgsql(query), &self.session)
						.with_variables(Some(params))
						.with_optional_transaction(transaction)
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
			(Dialect::MySql, transaction, _) => self
				.ds
				.run(
					QueryRequest::new(QuerySource::mysql(query), &self.session)
						.with_variables(Some(params))
						.with_optional_transaction(transaction)
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
			(Dialect::SurrealQl, transaction, Some(CachedPlan::SurrealQl(ast))) => self
				.ds
				.run(
					QueryRequest::new(ast, &self.session)
						.with_variables(Some(params))
						.with_optional_transaction(transaction)
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
			(Dialect::SurrealQl, transaction, _) => self
				.ds
				.run(
					QueryRequest::new(query, &self.session)
						.with_variables(Some(params))
						.with_optional_transaction(transaction)
						.with_cancel(cancel),
				)
				.await
				.map_err(|e| PgError::from(&e)),
		}?;
		let mut last = Value::None;
		for result in results {
			match result.result {
				Ok(value) => last = value,
				Err(err) => return Err(PgError::from(&err)),
			}
		}
		Ok(last)
	}

	async fn simple_query(&mut self, sql: &str) -> Result<()> {
		let mut out = BytesMut::new();
		let mut remaining = sql;

		// Intercept leading `SET` statements (e.g. `SET client_encoding = 'UTF8'; <rest>`).
		while !remaining.trim().is_empty() {
			let (first, rest) = split_first_statement(remaining);
			let trimmed_first = first.trim();
			if let Some(set) = parse_set(trimmed_first).filter(|set| self.should_intercept_set(set))
			{
				if let Err(err) = self.handle_set(set, &mut out) {
					self.note_simple_error(&err, &mut out);
					return self.finish_query(out).await;
				}
				remaining = rest;
			} else {
				break;
			}
		}

		let trimmed = remaining.trim();
		if trimmed.is_empty() {
			if out.is_empty() {
				msg::write_empty_query_response(&mut out);
			}
			return self.finish_query(out).await;
		}

		// Transaction-control statements are intercepted (they manage the
		// connection's interactive transaction rather than run in the executor).
		if let Some(op) = transaction_control(trimmed) {
			if let Err(err) = self.handle_transaction_control(op, &mut out).await {
				msg::write_error_response(&mut out, &err);
			}
			return self.finish_query(out).await;
		}

		// Inside a failed transaction, everything but COMMIT/ROLLBACK is refused.
		if self.txn_failed {
			msg::write_error_response(
				&mut out,
				&PgError::in_failed_transaction(
					"current transaction is aborted, commands ignored until end of transaction block",
				),
			);
			return self.finish_query(out).await;
		}

		if let Err(err) = self.check_current_dialect() {
			self.note_simple_error(&err, &mut out);
			return self.finish_query(out).await;
		}

		match self.dialect {
			Dialect::SurrealQl => {
				if self.transaction.is_some() {
					let _ = self.run_in_transaction(trimmed, &mut out).await;
				} else {
					let _ = self.run_surrealql_autocommit(trimmed, &mut out).await;
				}
			}
			Dialect::PgSql => {
				for stmt in split_statements(remaining) {
					let trimmed_stmt = stmt.trim();
					if trimmed_stmt.is_empty() {
						continue;
					}
					if let Some(op) = transaction_control(trimmed_stmt) {
						if let Err(err) = self.handle_transaction_control(op, &mut out).await {
							msg::write_error_response(&mut out, &err);
							break;
						}
						continue;
					}
					if self.txn_failed {
						msg::write_error_response(
							&mut out,
							&PgError::in_failed_transaction(
								"current transaction is aborted, commands ignored until end of transaction block",
							),
						);
						break;
					}
					let res = self.run_pgsql(trimmed_stmt, &mut out).await;
					if res.is_err() || self.txn_failed {
						break;
					}
				}
			}
			Dialect::MySql => {
				for stmt in split_statements(remaining) {
					let trimmed_stmt = stmt.trim();
					if trimmed_stmt.is_empty() {
						continue;
					}
					if let Some(op) = transaction_control(trimmed_stmt) {
						if let Err(err) = self.handle_transaction_control(op, &mut out).await {
							msg::write_error_response(&mut out, &err);
							break;
						}
						continue;
					}
					if self.txn_failed {
						msg::write_error_response(
							&mut out,
							&PgError::in_failed_transaction(
								"current transaction is aborted, commands ignored until end of transaction block",
							),
						);
						break;
					}
					let res = self.run_mysql(trimmed_stmt, &mut out).await;
					if res.is_err() || self.txn_failed {
						break;
					}
				}
			}
			Dialect::Gql => {
				if self.transaction.is_some() {
					self.note_simple_error(
						&PgError::feature_not_supported(
							"GQL queries cannot run inside an interactive transaction",
						),
						&mut out,
					);
				} else {
					let _ = self.run_gql(trimmed, &mut out).await;
				}
			}
		}
		self.finish_query(out).await
	}

	async fn run_pgsql(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		global_metrics().record_query(SqlDialect::Postgres);
		let mut descriptor = None;
		if let Ok(stmts) = surrealdb_pgsql::parse_statements(sql) {
			let mut catalog = MemoryCatalog::new();
			if let Some(db) = &self.session.db {
				catalog.set_database(Some(db.clone()));
			}
			let policy = DialectPolicy::postgres();
			for stmt in &stmts {
				match stmt {
					PgStatement::Select(select) => {
						match bind_select(select, &catalog, &policy, None) {
							Ok(bound) => {
								if descriptor.is_none() {
									descriptor = Some(bound.describe(&catalog, &policy));
								}
							}
							Err(e) => {
								self.note_simple_error(
									&PgError::undefined_table(e.to_string()),
									out,
								);
								return Ok(());
							}
						}
					}
					PgStatement::Insert(insert) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&insert.relation.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::Update(update) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&update.relation.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::Delete(delete) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&delete.relation.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::Truncate(truncate) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&truncate.table.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::CreateTable(create) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&create.name.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::Drop(drop) => {
						for name in &drop.names {
							if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
								&name.parts,
								Some(&catalog),
							) {
								self.note_simple_error(
									&PgError::undefined_table(e.to_string()),
									out,
								);
								return Ok(());
							}
						}
					}
					_ => {}
				}
			}
		}
		let cancel = self.arm_cancel();
		let req = QueryRequest::new(QuerySource::pgsql(sql), &self.session)
			.with_optional_transaction(self.transaction.clone())
			.with_cancel(cancel);
		match self.ds.run(req).await {
			Ok(results) => {
				for result in results {
					match result.result {
						Ok(value) => {
							let ok = if let Some(desc) = &descriptor {
								self.emit_result_with_descriptor(desc, value, out).await?
							} else {
								self.emit_result(value, out).await?
							};
							if !ok {
								break;
							}
						}
						Err(err) => {
							self.note_simple_error(&PgError::from(&err), out);
							break;
						}
					}
				}
			}
			Err(err) => self.note_simple_error(&PgError::from(&err), out),
		}
		Ok(())
	}

	async fn run_mysql(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		global_metrics().record_query(SqlDialect::MySql);
		let cancel = self.arm_cancel();
		let req = QueryRequest::new(QuerySource::mysql(sql), &self.session)
			.with_optional_transaction(self.transaction.clone())
			.with_cancel(cancel);
		match self.ds.run(req).await {
			Ok(results) => {
				for result in results {
					match result.result {
						Ok(value) => {
							if !self.emit_result(value, out).await? {
								break;
							}
						}
						Err(err) => {
							self.note_simple_error(&PgError::from(&err), out);
							break;
						}
					}
				}
			}
			Err(err) => self.note_simple_error(&PgError::from(&err), out),
		}
		Ok(())
	}

	/// Execute SurrealQL outside any interactive transaction: one execution
	/// unit at a time (a single statement or a whole `BEGIN..COMMIT` block),
	/// stopping at the first error — Postgres simple-query semantics.
	///
	/// DIVERGENCE: SurrealDB auto-commits each top-level statement, so units
	/// that succeeded before the error are not rolled back (unlike Postgres,
	/// which wraps a multi-statement simple query in an implicit transaction).
	/// Wrap statements in an explicit `BEGIN..COMMIT` for all-or-nothing.
	async fn run_surrealql_autocommit(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		let config = self.ds.parser_config();
		let ast = match syn::parse_with_capabilities(sql, &self.ds.get_capabilities(), &config) {
			Ok(ast) => ast,
			Err(err) => {
				msg::write_error_response(out, &PgError::syntax(err.to_string()));
				return Ok(());
			}
		};
		if ast.num_statements() == 0 {
			msg::write_empty_query_response(out);
			return Ok(());
		}
		// `unit` is left un-annotated on purpose: the core `Ast` type is
		// crate-private, so it can only be used through its (public) methods.
		for mut unit in ast.into_execution_units() {
			// A stray COMMIT/CANCEL with no open transaction is a Postgres
			// warning, not a fatal error — emit a notice and keep going rather
			// than letting the executor abort the rest of the query.
			if unit.is_sole_commit() {
				msg::write_notice(out, "25P01", "there is no transaction in progress");
				msg::write_command_complete(out, "COMMIT");
				continue;
			}
			if unit.is_sole_cancel() {
				msg::write_notice(out, "25P01", "there is no transaction in progress");
				msg::write_command_complete(out, "ROLLBACK");
				continue;
			}
			// Session persistence, matching the interactive shell
			// (cli/sql.rs): the values of this unit's LET statements and the
			// resulting session are appended as trailing `$param` expressions
			// and read back from the last results after execution.
			//
			// Skip this for a unit that ends in `CANCEL`: a LET/USE inside a
			// rolled-back transaction must not leak its (uncommitted) value
			// into the persistent connection session. Committed blocks still
			// persist their LET/USE, matching how the interactive shell behaves.
			let capture_session = !unit.contains_cancel();
			let has_begin = unit.expressions.iter().any(|e| matches!(e, TopLevelExpr::Begin));
			let let_vars = if capture_session {
				let names = unit.get_let_statements();
				for name in &names {
					unit.add_param(name.clone());
				}
				unit.add_param("session".to_string());
				names
			} else {
				Vec::new()
			};
			let cancel = self.arm_cancel();
			let mut results =
				match self.ds.run(QueryRequest::new(unit, &self.session).with_cancel(cancel)).await
				{
					Ok(results) => results,
					Err(err) => {
						msg::write_error_response(out, &PgError::from(&err));
						break;
					}
				};
			// The appended capture params are always the final top-level
			// expressions and always execute, so they are the last
			// `let_vars.len() + 1` results (or none, when capture was skipped).
			let trailing = if capture_session && results.len() > let_vars.len() {
				results.split_off(results.len() - (let_vars.len() + 1))
			} else {
				Vec::new()
			};
			// A block that did not commit reverts its LETs, so the appended
			// reads return the reverted values: a block whose final result is
			// an error contributes no LET values. The `$session` half still
			// applies, as a `USE` is never undone.
			let let_vars = if has_begin && results.last().is_some_and(|r| r.result.is_err()) {
				Vec::new()
			} else {
				let_vars
			};
			let mut ok = true;
			for result in results {
				match result.result {
					Ok(value) => {
						if !self.emit_result(value, out).await? {
							ok = false;
							break;
						}
					}
					Err(err) => {
						msg::write_error_response(out, &PgError::from(&err));
						ok = false;
						break;
					}
				}
			}
			if capture_session {
				self.apply_session_updates(let_vars, trailing);
			}
			if !ok {
				break;
			}
		}
		Ok(())
	}

	/// Execute SurrealQL within the open interactive transaction. On any
	/// statement error the transaction is poisoned (25P02) until the client
	/// ends it with COMMIT/ROLLBACK.
	///
	/// Text that parses as SurrealQL has a `$name` read appended for each of
	/// its top-level `LET` statements; the successful values are held in
	/// `txn_lets` until the transaction commits. The appended reads always
	/// execute, so they are the last results. Text that parses only after
	/// translation captures nothing, and `USE` is not captured.
	async fn run_in_transaction(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		let tx = self.transaction.clone().expect("caller checked transaction is open");
		let cancel = self.arm_cancel();
		let config = self.ds.parser_config();
		let lowered;
		let mut let_vars = Vec::new();
		let source = match syn::parse_with_capabilities(sql, &self.ds.get_capabilities(), &config) {
			Ok(mut ast) => {
				let_vars = ast.get_let_statements();
				for name in &let_vars {
					ast.add_param(name.clone());
				}
				QuerySource::from(ast)
			}
			Err(_) => match surrealdb_pgsql::translate_sql(sql) {
				Ok(translated) => {
					lowered = translated;
					QuerySource::from(lowered.as_str())
				}
				Err(_) => QuerySource::from(sql),
			},
		};
		let results = self
			.ds
			.run(QueryRequest::new(source, &self.session).with_transaction(tx).with_cancel(cancel))
			.await;
		match results {
			Ok(mut results) => {
				let trailing = results.split_off(results.len().saturating_sub(let_vars.len()));
				for result in results {
					match result.result {
						Ok(value) => {
							if !self.emit_result(value, out).await? {
								self.txn_failed = true;
								break;
							}
						}
						Err(err) => {
							msg::write_error_response(out, &PgError::from(&err));
							self.txn_failed = true;
							break;
						}
					}
				}
				for (name, result) in let_vars.into_iter().zip(trailing) {
					// Protected names are never written into the session
					// variables, as in `apply_session_updates`.
					if surrealdb_rpc::check_protected_param(&name).is_err() {
						continue;
					}
					if let Ok(value) = result.result {
						self.txn_lets.push((name, value));
					}
				}
			}
			Err(err) => {
				msg::write_error_response(out, &PgError::from(&err));
				self.txn_failed = true;
			}
		}
		Ok(())
	}

	/// Execute a GQL query and stream its results (as typed columns, like the
	/// SurrealQL simple path). GQL runs against the current namespace/database.
	async fn run_gql(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		let cancel = self.arm_cancel();
		match self
			.ds
			.run(QueryRequest::new(QuerySource::gql(sql), &self.session).with_cancel(cancel))
			.await
		{
			Ok(results) => {
				for result in results {
					match result.result {
						Ok(value) => {
							if !self.emit_result(value, out).await? {
								break;
							}
						}
						Err(err) => {
							msg::write_error_response(out, &PgError::from(&err));
							break;
						}
					}
				}
			}
			Err(err) => msg::write_error_response(out, &PgError::from(&err)),
		}
		Ok(())
	}

	/// Handle an intercepted `BEGIN`/`COMMIT`/`ROLLBACK`. Notices and the success
	/// CommandComplete are written to `out`; an actual failure (a transaction
	/// that could not begin or commit) is returned so the caller can report it
	/// and, in the extended protocol, poison the batch.
	async fn handle_transaction_control(
		&mut self,
		op: TxnOp,
		out: &mut BytesMut,
	) -> Result<(), PgError> {
		match op {
			TxnOp::Begin => {
				if self.transaction.is_some() {
					msg::write_notice(out, "25001", "there is already a transaction in progress");
				} else {
					let tx = self
						.ds
						.transaction(TransactionType::Write)
						.await
						.map_err(|err| PgError::internal(err.to_string()))?;
					self.transaction = Some(Arc::new(tx));
					self.txn_failed = false;
					self.txn_lets.clear();
				}
				msg::write_command_complete(out, "BEGIN");
			}
			TxnOp::Commit => match self.transaction.take() {
				// Committing an aborted transaction rolls it back; report so.
				Some(tx) if self.txn_failed => {
					self.txn_failed = false;
					self.txn_lets.clear();
					let _ = tx.cancel().await;
					msg::write_command_complete(out, "ROLLBACK");
				}
				Some(tx) => {
					// The transaction was already taken, so on failure
					// ready_status() reports 'I' and the caller emits only the
					// error (no trailing CommandComplete), keeping the sequence valid.
					let lets = std::mem::take(&mut self.txn_lets);
					tx.commit().await.map_err(|err| PgError::internal(err.to_string()))?;
					// Captured LETs become session variables only once the
					// transaction that bound them is durable.
					for (name, value) in lets {
						self.session.variables.insert(name, value);
					}
					msg::write_command_complete(out, "COMMIT");
				}
				None => {
					msg::write_notice(out, "25P01", "there is no transaction in progress");
					msg::write_command_complete(out, "COMMIT");
				}
			},
			TxnOp::Rollback => {
				self.txn_lets.clear();
				match self.transaction.take() {
					Some(tx) => {
						let _ = tx.cancel().await;
					}
					None => {
						msg::write_notice(out, "25P01", "there is no transaction in progress");
					}
				}
				self.txn_failed = false;
				msg::write_command_complete(out, "ROLLBACK");
			}
		}
		Ok(())
	}

	/// Switch the connection to `dialect`, enforcing the capability that gates
	/// it. Every assignment to `self.dialect` goes through here, so a dialect a
	/// deployment has switched off cannot be entered from any selection point.
	fn select_dialect(&mut self, dialect: Dialect) -> Result<(), PgError> {
		self.check_dialect(dialect)?;
		self.dialect = dialect;
		Ok(())
	}

	/// Re-check the dialect the connection already speaks, before running a
	/// query in it.
	///
	/// Capabilities are swapped atomically at runtime, so the gate is evaluated
	/// per query and not only when the dialect was chosen: denying `gql` must
	/// stop an established connection from issuing more GQL, the way it
	/// immediately stops `/gql` (checked per request) and the `gql` RPC method
	/// (checked per dispatch). The connection is not stranded — `SET dialect`
	/// is intercepted before this runs, so a client can switch back to
	/// SurrealQL. `RouteTarget::Postgres` stays connection-scoped by contrast,
	/// matching `RouteTarget::Rpc` on a WebSocket, which is checked once at
	/// upgrade.
	fn check_current_dialect(&self) -> Result<(), PgError> {
		self.check_dialect(self.dialect)
	}

	/// The dialect gate itself. The match is exhaustive so that a new dialect
	/// cannot be added without deciding what gates it.
	fn check_dialect(&self, dialect: Dialect) -> Result<(), PgError> {
		let allowed = match dialect {
			Dialect::PgSql | Dialect::SurrealQl => {
				self.ds.allows_http_route(&RouteTarget::Postgres)
			}
			Dialect::MySql => self.ds.allows_http_route(&RouteTarget::MySql),
			Dialect::Gql => self.ds.allows_http_route(&RouteTarget::Gql),
		};
		if !allowed {
			let name = dialect_name(dialect);
			warn!(
				target: LOG,
				"Capabilities denied postgres {name} dialect request from {}", self.peer
			);
			return Err(PgError::insufficient_privilege(format!(
				"Forbidden: the {name} dialect is not allowed on this server"
			)));
		}
		Ok(())
	}

	/// Whether a `SET` should be intercepted rather than executed. In the
	/// SurrealQL, PostgreSQL, and MySQL dialects every `SET` is intercepted
	/// (top-level `SET` is configuration). In the GQL dialect `SET` is a mutation
	/// clause, so only what a driver means as configuration is intercepted — the
	/// `dialect` control knob, the known GUC allowlist, and unmodelled `SET` syntax
	/// (empty name, e.g. `SET TIME ZONE ...`); a real `property = value` mutation reaches the
	/// executor.
	fn should_intercept_set(&self, set: &SetStatement) -> bool {
		self.dialect == Dialect::SurrealQl
			|| self.dialect == Dialect::PgSql
			|| self.dialect == Dialect::MySql
			|| set.name.is_empty()
			|| set.name.eq_ignore_ascii_case("dialect")
			|| is_no_op_guc(&set.name)
	}

	/// Handle an intercepted `SET`: switch dialect, accept a known no-op GUC,
	/// or reject an unknown one.
	fn handle_set(&mut self, set: SetStatement, out: &mut BytesMut) -> Result<(), PgError> {
		let SetStatement {
			name,
			value,
		} = set;
		if name.is_empty() {
			let msg = if !value.is_empty() {
				format!("SET {value} is not supported")
			} else {
				"this form of SET statement is not supported".to_string()
			};
			return Err(PgError::feature_not_supported(msg));
		}
		if name.eq_ignore_ascii_case("dialect") {
			let dialect = parse_dialect(&value)
				.ok_or_else(|| PgError::invalid_text(format!("unrecognized dialect: {value}")))?;
			self.select_dialect(dialect)?;
			msg::write_command_complete(out, "SET");
			return Ok(());
		}
		if is_no_op_guc(&name) {
			msg::write_command_complete(out, "SET");
			Ok(())
		} else {
			Err(PgError::undefined_object(format!(
				"unrecognized configuration parameter \"{name}\""
			)))
		}
	}

	/// Install a fresh cancellation handle for the next query and register it
	/// so a CancelRequest can find it. A handle is single-shot (trip is
	/// irreversible), so each query gets its own.
	fn arm_cancel(&mut self) -> CancelHandle {
		let cancel = CancelHandle::new();
		self.cancel = cancel.clone();
		self.registry.insert((self.pid, self.secret), cancel.clone());
		cancel
	}

	/// Whether the connection stream has been upgraded to TLS.
	fn is_tls(&self) -> bool {
		matches!(self.stream, Either::Right(_))
	}

	/// The ReadyForQuery transaction-status byte.
	fn ready_status(&self) -> u8 {
		if self.transaction.is_none() {
			b'I'
		} else if self.txn_failed {
			b'E'
		} else {
			b'T'
		}
	}

	/// When an interactive transaction is aborted, extended-protocol messages
	/// are refused with 25P02 until the client ends the block.
	fn aborted_txn_guard(&self) -> Option<PgError> {
		self.txn_failed.then(|| {
			PgError::in_failed_transaction(
				"current transaction is aborted, commands ignored until end of transaction block",
			)
		})
	}

	/// Report an extended-protocol handler error and, if it happened inside an
	/// interactive transaction, poison the transaction so the next Sync reports
	/// status 'E' — matching the simple-protocol path and Postgres semantics.
	fn note_extended_error(&mut self, err: &PgError, out: &mut BytesMut) {
		if err.code == "0A000" {
			global_metrics().record_unsupported_feature(&err.message);
		}
		msg::write_error_response(out, err);
		if self.transaction.is_some() {
			self.txn_failed = true;
		}
	}

	/// Report a simple-protocol error raised by a statement the connection
	/// intercepted rather than executed, poisoning an open transaction the same
	/// way an executor error does. Any error inside a transaction block aborts
	/// it, so the next statement must be refused with 25P02 until
	/// `COMMIT`/`ROLLBACK` — an intercepted statement is not an exception.
	fn note_simple_error(&mut self, err: &PgError, out: &mut BytesMut) {
		if err.code == "0A000" {
			global_metrics().record_unsupported_feature(&err.message);
		}
		msg::write_error_response(out, err);
		if self.transaction.is_some() {
			self.txn_failed = true;
		}
	}

	/// Encode and stream one statement result as RowDescription + DataRows +
	/// CommandComplete. Returns `false` (after writing an ErrorResponse) when a
	/// value could not be encoded. Rows are encoded and flushed as they go
	/// (past `FLUSH_THRESHOLD`) rather than buffered whole, so a large result
	/// set does not hold a second full copy in memory. An encode error after
	/// some rows have streamed yields a partial result then an ErrorResponse —
	/// a valid Postgres sequence — but such errors are internal invariant
	/// failures that inferred column types make unreachable in practice.
	async fn emit_shape(&mut self, shape: ResultShape, out: &mut BytesMut) -> Result<bool> {
		let ResultShape {
			columns,
			rows,
		} = shape;
		if columns.len() > msg::MAX_COLUMNS {
			msg::write_error_response(
				out,
				&PgError::feature_not_supported(format!(
					"result has {} columns, exceeding the maximum of {}",
					columns.len(),
					msg::MAX_COLUMNS
				)),
			);
			return Ok(false);
		}
		msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
		let mut count = 0usize;
		for row in rows {
			let cells = match encode_row(row, &columns, &[]) {
				Ok(cells) => cells,
				Err(err) => {
					msg::write_error_response(out, &err);
					return Ok(false);
				}
			};
			msg::write_data_row(out, &cells);
			count += 1;
			if out.len() >= FLUSH_THRESHOLD {
				self.stream.write_all(out).await?;
				out.clear();
			}
		}
		msg::write_command_complete(out, &format!("SELECT {count}"));
		Ok(true)
	}

	async fn emit_result(&mut self, value: Value, out: &mut BytesMut) -> Result<bool> {
		self.emit_shape(shape_result(value), out).await
	}

	async fn emit_result_with_descriptor(
		&mut self,
		descriptor: &surrealdb_sqlplan::ResultDescriptor,
		value: Value,
		out: &mut BytesMut,
	) -> Result<bool> {
		self.emit_shape(shape_result_with_descriptor(descriptor, value), out).await
	}

	/// Fold the trailing `$param` results back into the connection session:
	/// LET values into the session variables, and the `$session` object's
	/// namespace/database selection (which reflects any `USE` statements the
	/// unit executed).
	fn apply_session_updates(
		&mut self,
		let_vars: Vec<String>,
		mut trailing: Vec<surrealdb_rpc::QueryResult>,
	) {
		let Some(session_result) = trailing.pop() else {
			return;
		};
		for (name, result) in let_vars.into_iter().zip(trailing) {
			// Never write a protected name ($session/$auth/$token/$access)
			// into the session variables: the executor rejects those on every
			// subsequent query, which would poison the connection.
			if surrealdb_rpc::check_protected_param(&name).is_err() {
				continue;
			}
			if let Ok(value) = result.result {
				self.session.variables.insert(name, value);
			}
		}
		if let Ok(Value::Object(obj)) = session_result.result {
			let mut map = obj.into_inner();
			self.session.ns = match map.remove("ns") {
				Some(Value::String(ns)) => Some(ns),
				_ => None,
			};
			self.session.db = match map.remove("db") {
				Some(Value::String(db)) => Some(db),
				_ => None,
			};
		}
	}

	async fn finish_query(&mut self, mut out: BytesMut) -> Result<()> {
		let status = self.ready_status();
		msg::write_ready_for_query(&mut out, status);
		self.stream.write_all(&out).await?;
		self.stream.flush().await?;
		Ok(())
	}

	/// Send an ErrorResponse without closing the stream; the caller decides
	/// whether the connection survives.
	async fn report(&mut self, err: &PgError) -> Result<()> {
		let mut buf = BytesMut::new();
		msg::write_error_response(&mut buf, err);
		self.stream.write_all(&buf).await?;
		self.stream.flush().await?;
		Ok(())
	}

	/// Read one startup-phase packet payload. Returns `None` on shutdown or
	/// clean disconnect.
	async fn read_startup_packet(&mut self) -> Result<Option<Vec<u8>>> {
		let len = tokio::select! {
			biased;
			_ = self.shutdown.cancelled() => return Ok(None),
			len = self.stream.read_i32() => match len {
				Ok(len) => len,
				Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
				Err(err) => return Err(err.into()),
			},
		};
		match read_startup_payload(&mut self.stream, len).await? {
			Some(payload) => Ok(Some(payload)),
			None => {
				let _ =
					self.report(&PgError::protocol("invalid startup packet length").fatal()).await;
				Ok(None)
			}
		}
	}

	/// Read one tagged message, rejecting anything larger than `max`. Returns
	/// `None` on shutdown or clean disconnect at a message boundary.
	async fn read_message(&mut self, max: usize) -> Result<Option<(u8, Vec<u8>)>> {
		let tag = tokio::select! {
			biased;
			_ = self.shutdown.cancelled() => return Ok(None),
			tag = self.stream.read_u8() => match tag {
				Ok(tag) => tag,
				Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
				Err(err) => return Err(err.into()),
			},
		};
		let len = self.stream.read_i32().await?;
		let len = usize::try_from(len).unwrap_or(0);
		if !(4..=max).contains(&len) {
			// Report before closing so the client sees a diagnostic rather
			// than a bare dropped socket. A framing error cannot be recovered, so
			// the connection terminates.
			let _ = self.report(&PgError::protocol("invalid message length").fatal()).await;
			return Ok(None);
		}
		let mut payload = vec![0u8; len - 4];
		self.stream.read_exact(&mut payload).await?;
		Ok(Some((tag, payload)))
	}
}

/// Rewrite Postgres positional parameters (`$1`, `$2`, …) into SurrealQL
/// parameters (`$_1`, `$_2`, …), skipping string literals and comments, and
/// return the rewritten query plus the highest positional index seen.
///
/// SurrealQL only accepts a parameter name that begins with a letter or
/// underscore, so a bare `$1` fails to parse; prefixing the digits with `_`
/// makes it a valid name that binds to the `_N` variable set in
/// [`Connection::handle_bind`].
fn rewrite_positional_params(query: &str) -> (String, usize) {
	#[derive(PartialEq)]
	enum State {
		Normal,
		Single,
		Double,
		Backtick,
		/// A `⟨…⟩`-delimited SurrealQL identifier/record-id.
		Angle,
		Line,
		Block,
	}
	let chars: Vec<char> = query.chars().collect();
	let mut out = String::with_capacity(query.len() + 8);
	let mut state = State::Normal;
	let mut max_index = 0usize;
	let mut i = 0;
	while i < chars.len() {
		let c = chars[i];
		let peek = chars.get(i + 1).copied();
		match state {
			State::Normal => match c {
				'\'' => {
					state = State::Single;
					out.push(c);
					i += 1;
				}
				'"' => {
					state = State::Double;
					out.push(c);
					i += 1;
				}
				'`' => {
					state = State::Backtick;
					out.push(c);
					i += 1;
				}
				'\u{27e8}' => {
					state = State::Angle;
					out.push(c);
					i += 1;
				}
				'-' if peek == Some('-') => {
					state = State::Line;
					out.push_str("--");
					i += 2;
				}
				'#' => {
					state = State::Line;
					out.push('#');
					i += 1;
				}
				'/' if peek == Some('*') => {
					state = State::Block;
					out.push_str("/*");
					i += 2;
				}
				'$' if peek.is_some_and(|p| p.is_ascii_digit()) => {
					out.push_str("$_");
					i += 1;
					let mut digits = String::new();
					while i < chars.len() && chars[i].is_ascii_digit() {
						digits.push(chars[i]);
						out.push(chars[i]);
						i += 1;
					}
					if let Ok(index) = digits.parse::<usize>() {
						max_index = max_index.max(index);
					}
				}
				_ => {
					out.push(c);
					i += 1;
				}
			},
			State::Single | State::Double => {
				out.push(c);
				i += 1;
				if c == '\\' {
					if let Some(next) = chars.get(i) {
						out.push(*next);
						i += 1;
					}
				} else if (state == State::Single && c == '\'')
					|| (state == State::Double && c == '"')
				{
					state = State::Normal;
				}
			}
			State::Backtick => {
				out.push(c);
				i += 1;
				if c == '`' {
					state = State::Normal;
				}
			}
			State::Angle => {
				out.push(c);
				i += 1;
				if c == '\u{27e9}' {
					state = State::Normal;
				}
			}
			State::Line => {
				out.push(c);
				i += 1;
				if c == '\n' {
					state = State::Normal;
				}
			}
			State::Block => {
				out.push(c);
				i += 1;
				if c == '*' && chars.get(i) == Some(&'/') {
					out.push('/');
					i += 1;
					state = State::Normal;
				}
			}
		}
	}
	(out, max_index)
}

/// The wire format code (text/binary) for column/parameter `i`, following the
/// Postgres convention: an empty list means all-text, a single entry applies
/// to every position, otherwise it is indexed per position.
fn format_at(formats: &[i16], i: usize) -> i16 {
	match formats.len() {
		0 => FORMAT_TEXT,
		1 => formats[0],
		_ => formats.get(i).copied().unwrap_or(FORMAT_TEXT),
	}
}

/// Whether an encoded DataRow fits the wire's `i32` length fields. The message
/// length and every field length are `i32`, so a row (or single cell) beyond
/// `i32::MAX` bytes would wrap negative and desync framing — reject it instead.
fn row_within_wire_limit(cells: &[Option<Vec<u8>>]) -> bool {
	wire_row_fits(cells.iter().map(|c| c.as_ref().map_or(0, Vec::len)))
}

/// Length-only core of [`row_within_wire_limit`], so the overflow boundary can
/// be tested without allocating multi-gigabyte buffers.
fn wire_row_fits(cell_lengths: impl Iterator<Item = usize>) -> bool {
	// DataRow overhead: 4-byte message length + 2-byte field count.
	let mut total: u64 = 6;
	for len in cell_lengths {
		// Each field is a 4-byte length prefix plus its data.
		total += 4 + len as u64;
		if total > i32::MAX as u64 {
			return false;
		}
	}
	true
}

/// Encode one cell in the requested wire format.
fn encode_cell(value: Value, ty: PgType, format: i16) -> Result<Vec<u8>, PgError> {
	if format == FORMAT_BINARY {
		encode::encode_binary(value, ty)
	} else {
		encode::encode_text(value, ty)
	}
}

/// Encode one result row's cells to wire bytes in the given per-column formats
/// (an empty `formats` slice means all-text), rejecting a row that would exceed
/// the i32 wire-length limit. Shared by the simple- and extended-protocol
/// result paths so the per-cell encoding and the wire-size guard live in one
/// place.
fn encode_row(
	row: Vec<Option<Value>>,
	columns: &[PgColumn],
	formats: &[i16],
) -> Result<Vec<Option<Vec<u8>>>, PgError> {
	let mut cells = Vec::with_capacity(row.len());
	for (i, (cell, column)) in row.into_iter().zip(columns).enumerate() {
		match cell {
			Some(value) => cells.push(Some(encode_cell(value, column.ty, format_at(formats, i))?)),
			None => cells.push(None),
		}
	}
	if !row_within_wire_limit(&cells) {
		return Err(PgError::feature_not_supported(
			"result row exceeds the maximum Postgres wire size",
		));
	}
	Ok(cells)
}

/// Interpret the startup `database` parameter as a `namespace/database`
/// selection.
///
/// A bare value with no `/` selects a namespace only — unless it equals the
/// username, in which case it is ignored: libpq defaults the database name to
/// the username, so `psql -U root` would otherwise silently select a
/// namespace called `root`.
fn parse_database_param(user: &str, database: Option<&str>) -> (Option<String>, Option<String>) {
	let non_empty = |s: &str| {
		if s.is_empty() {
			None
		} else {
			Some(s.to_string())
		}
	};
	match database {
		None => (None, None),
		Some(database) => match database.split_once('/') {
			Some((ns, db)) => (non_empty(ns), non_empty(db)),
			None if database == user => (None, None),
			None => (non_empty(database), None),
		},
	}
}

/// The connection-terminating error for any failed authentication. Kept uniform
/// across cleartext and SCRAM so the failure reveals nothing about which
/// mechanism was attempted or why it failed.
fn auth_failed(user: &str) -> PgError {
	PgError::invalid_password(format!("password authentication failed for user \"{user}\"")).fatal()
}

/// An intercepted transaction-control statement.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TxnOp {
	Begin,
	Commit,
	Rollback,
}

/// Recognise a standalone transaction-control statement, tolerating the
/// modifier clauses drivers and ORMs emit (`BEGIN ISOLATION LEVEL ...`,
/// `START TRANSACTION READ WRITE`, `COMMIT WORK`, ...) and SQL comments.
/// Only a single statement is matched — a script with a second `;`-separated
/// statement passes through to the executor unchanged.
fn transaction_control(trimmed: &str) -> Option<TxnOp> {
	let stripped = strip_sql_comments(trimmed);
	let stripped = stripped.trim().trim_end_matches(';').trim();
	// A second statement in the same string is not a standalone control op.
	if stripped.contains(';') {
		return None;
	}
	let mut tokens = stripped.split_whitespace().map(str::to_ascii_uppercase);
	let (op, start) = match tokens.next()?.as_str() {
		"BEGIN" => (TxnOp::Begin, false),
		"START" => (TxnOp::Begin, true), // START must be followed by TRANSACTION
		"COMMIT" | "END" => (TxnOp::Commit, false),
		"ROLLBACK" | "ABORT" => (TxnOp::Rollback, false),
		_ => return None,
	};
	if start && tokens.next().as_deref() != Some("TRANSACTION") {
		return None;
	}
	// Every remaining token must be a recognised transaction modifier; anything
	// else means this is a real query that merely starts with the keyword.
	const MODIFIERS: &[&str] = &[
		"TRANSACTION",
		"WORK",
		"ISOLATION",
		"LEVEL",
		"SERIALIZABLE",
		"REPEATABLE",
		"READ",
		"WRITE",
		"ONLY",
		"COMMITTED",
		"UNCOMMITTED",
		"DEFERRABLE",
		"NOT",
		"AND",
		"NO",
		"CHAIN",
	];
	if tokens.all(|t| MODIFIERS.contains(&t.as_str())) {
		Some(op)
	} else {
		None
	}
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
				// Copy one full UTF-8 char.
				let ch = sql[i..].chars().next().expect("valid char boundary");
				out.push(ch);
				i += ch.len_utf8();
			}
		}
	}
	out
}

/// An intercepted `SET` statement. An empty `name` marks a `SET` whose syntax
/// we do not model (e.g. `SET TIME ZONE ...`), accepted as a no-op.
#[derive(Clone)]
struct SetStatement {
	name: String,
	value: String,
}

/// GUCs that drivers set on connect; accepted as silent no-ops (their effect is
/// either irrelevant to SurrealDB or already the fixed server default). Matched
/// in any dialect, since these are never valid GQL mutation property names.
const NO_OP_GUCS: &[&str] = &[
	"extra_float_digits",
	"application_name",
	"client_encoding",
	"datestyle",
	"timezone",
	"statement_timeout",
	"search_path",
	"standard_conforming_strings",
	"client_min_messages",
	"bytea_output",
	"intervalstyle",
];

/// Whether a GUC name is in the connect-time no-op allowlist (case-insensitive).
fn is_no_op_guc(name: &str) -> bool {
	NO_OP_GUCS.iter().any(|g| name.eq_ignore_ascii_case(g))
}

/// Recognise a `SET name = value` / `SET name TO value` statement.
fn parse_set(trimmed: &str) -> Option<SetStatement> {
	let stripped = strip_sql_comments(trimmed);
	let rest = stripped.trim().trim_end_matches(';').trim();
	if rest.contains(';') {
		return None;
	}
	let mut words = rest.splitn(2, char::is_whitespace);
	if !words.next()?.eq_ignore_ascii_case("SET") {
		return None;
	}
	let assignment = words.next().unwrap_or("").trim();
	let first_word = assignment.split_whitespace().next().unwrap_or("");
	if first_word.eq_ignore_ascii_case("ROLE")
		|| first_word.eq_ignore_ascii_case("SESSION")
		|| first_word.eq_ignore_ascii_case("TRANSACTION")
		|| first_word.eq_ignore_ascii_case("LOCAL")
	{
		return Some(SetStatement {
			name: String::new(),
			value: assignment.to_string(),
		});
	}
	// Split on the first `=` or a ` TO ` keyword.
	let split = assignment.find('=').map(|i| (assignment[..i].trim(), assignment[i + 1..].trim()));
	let split = split.or_else(|| {
		assignment
			.to_ascii_uppercase()
			.find(" TO ")
			.map(|i| (assignment[..i].trim(), assignment[i + 4..].trim()))
	});
	Some(match split {
		Some((name, value)) => SetStatement {
			name: name.to_string(),
			value: value.to_string(),
		},
		None => SetStatement {
			name: String::new(),
			value: assignment.to_string(),
		},
	})
}

/// Split the first statement before the first unquoted semicolon, returning
/// `(first_stmt, remainder)`. If no semicolon is found, returns `(sql, "")`.
fn split_first_statement(sql: &str) -> (&str, &str) {
	let bytes = sql.as_bytes();
	let mut i = 0;
	let len = bytes.len();

	while i < len {
		match bytes[i] {
			b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
				i += 2;
				while i < len && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'#' => {
				i += 1;
				while i < len && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
				i += 2;
				while i < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
					i += 1;
				}
				if i + 1 < len {
					i += 2;
				}
			}
			b'\'' => {
				i += 1;
				while i < len {
					if bytes[i] == b'\'' {
						if i + 1 < len && bytes[i + 1] == b'\'' {
							i += 2;
						} else {
							i += 1;
							break;
						}
					} else if bytes[i] == b'\\' {
						i += 2;
					} else {
						i += 1;
					}
				}
			}
			b'"' => {
				i += 1;
				while i < len {
					if bytes[i] == b'"' {
						if i + 1 < len && bytes[i + 1] == b'"' {
							i += 2;
						} else {
							i += 1;
							break;
						}
					} else {
						i += 1;
					}
				}
			}
			b'$' => {
				let tag_start = i;
				let mut j = i + 1;
				while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
					j += 1;
				}
				if j < len && bytes[j] == b'$' {
					let tag = &sql[tag_start..=j];
					i = j + 1;
					if let Some(pos) = sql[i..].find(tag) {
						i += pos + tag.len();
					} else {
						i = len;
					}
				} else {
					i += 1;
				}
			}
			b';' => {
				return (&sql[..i], &sql[i + 1..]);
			}
			_ => {
				i += 1;
			}
		}
	}
	(sql, "")
}

/// Split a SQL string containing multiple statements separated by semicolons into individual
/// statement slices, respecting single quotes, double quotes, dollar quotes, and SQL comments.
fn split_statements(sql: &str) -> Vec<&str> {
	let bytes = sql.as_bytes();
	let mut stmts = Vec::new();
	let mut start = 0;
	let mut i = 0;
	let len = bytes.len();

	while i < len {
		match bytes[i] {
			b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
				i += 2;
				while i < len && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'#' => {
				i += 1;
				while i < len && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
				i += 2;
				while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
					i += 1;
				}
				if i + 1 < len {
					i += 2;
				}
			}
			b'\'' => {
				i += 1;
				while i < len {
					if bytes[i] == b'\'' {
						if i + 1 < len && bytes[i + 1] == b'\'' {
							i += 2;
						} else {
							i += 1;
							break;
						}
					} else if bytes[i] == b'\\' {
						i += 2;
					} else {
						i += 1;
					}
				}
			}
			b'"' => {
				i += 1;
				while i < len {
					if bytes[i] == b'"' {
						if i + 1 < len && bytes[i + 1] == b'"' {
							i += 2;
						} else {
							i += 1;
							break;
						}
					} else {
						i += 1;
					}
				}
			}
			b'$' => {
				let tag_start = i;
				let mut j = i + 1;
				while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
					j += 1;
				}
				if j < len && bytes[j] == b'$' {
					let tag = &sql[tag_start..=j];
					i = j + 1;
					if let Some(pos) = sql[i..].find(tag) {
						i += pos + tag.len();
					} else {
						i = len;
					}
				} else {
					i += 1;
				}
			}
			b';' => {
				let stmt = sql[start..i].trim();
				if !stmt.is_empty() {
					stmts.push(stmt);
				}
				i += 1;
				start = i;
			}
			_ => {
				i += 1;
			}
		}
	}
	let trailing = sql[start..].trim();
	if !trailing.is_empty() {
		stmts.push(trailing);
	}
	stmts
}

/// Parse a Postgres `options` startup string for a `-c dialect=<name>` (or
/// bare `dialect=<name>`) setting.
fn dialect_from_options(options: &str) -> Option<Dialect> {
	let mut tokens = options.split_whitespace().peekable();
	while let Some(token) = tokens.next() {
		let setting = if token == "-c" {
			tokens.next()?
		} else {
			token.strip_prefix("-c").unwrap_or(token)
		};
		if let Some(value) = setting.strip_prefix("dialect=") {
			return parse_dialect(value);
		}
	}
	None
}

/// Parse a dialect name (case-insensitive) as used by `options` and `SET`.
fn parse_dialect(value: &str) -> Option<Dialect> {
	match value.trim().trim_matches('\'').trim_matches('"').to_ascii_lowercase().as_str() {
		"surrealql" | "surql" => Some(Dialect::SurrealQl),
		"postgres" | "postgresql" | "pgsql" | "sql" => Some(Dialect::PgSql),
		"mysql" => Some(Dialect::MySql),
		"gql" => Some(Dialect::Gql),
		_ => None,
	}
}

/// The canonical name of a dialect, as [`parse_dialect`] accepts it. Used in the
/// messages a client sees when a dialect is refused.
fn dialect_name(dialect: Dialect) -> &'static str {
	match dialect {
		Dialect::SurrealQl => "surrealql",
		Dialect::Gql => "gql",
		Dialect::PgSql => "pgsql",
		Dialect::MySql => "mysql",
	}
}

#[cfg(test)]
mod tests {
	use super::{
		Dialect, TxnOp, dialect_from_options, dialect_name, parse_database_param, parse_dialect,
		parse_set, rewrite_positional_params, row_within_wire_limit, split_statements,
		transaction_control, wire_row_fits,
	};

	fn rewrite(query: &str) -> String {
		rewrite_positional_params(query).0
	}

	#[test]
	fn recognises_transaction_control() {
		assert_eq!(transaction_control("BEGIN"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("begin;"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("START TRANSACTION"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("COMMIT"), Some(TxnOp::Commit));
		assert_eq!(transaction_control("End Transaction ;"), Some(TxnOp::Commit));
		assert_eq!(transaction_control("ROLLBACK"), Some(TxnOp::Rollback));
		assert_eq!(transaction_control("ABORT"), Some(TxnOp::Rollback));
		// A multi-statement script is not intercepted.
		assert_eq!(transaction_control("BEGIN; RETURN 1; COMMIT"), None);
		assert_eq!(transaction_control("RETURN 1"), None);
	}

	#[test]
	fn recognises_transaction_preambles() {
		// Comment stripping still allows clean transaction control.
		assert_eq!(transaction_control("BEGIN /* jdbc */"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("COMMIT -- done"), Some(TxnOp::Commit));
		// Modifiers like isolation levels, read only, chain are recognised.
		assert_eq!(transaction_control("BEGIN ISOLATION LEVEL SERIALIZABLE"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("START TRANSACTION READ WRITE"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("ROLLBACK AND NO CHAIN"), Some(TxnOp::Rollback));
		// `START` without `TRANSACTION` is not a transaction statement.
		assert_eq!(transaction_control("START"), None);
		// A real query that merely begins with a keyword-like token is not one.
		assert_eq!(transaction_control("BEGIN something weird"), None);
		// `COMMIT; SELECT 1` is multi-statement — not intercepted here.
		assert_eq!(transaction_control("COMMIT; SELECT 1"), None);
	}

	#[test]
	fn test_split_statements() {
		assert_eq!(split_statements("SELECT 1;"), vec!["SELECT 1"]);
		assert_eq!(
			split_statements("SET client_encoding = 'UTF8'; SELECT 1;"),
			vec!["SET client_encoding = 'UTF8'", "SELECT 1"]
		);
		assert_eq!(
			split_statements("SELECT 'hello;world'; SELECT $$semi;colon$$;"),
			vec!["SELECT 'hello;world'", "SELECT $$semi;colon$$"]
		);
		assert_eq!(
			split_statements("BEGIN; INSERT INTO t VALUES (1); COMMIT;"),
			vec!["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"]
		);
		assert!(split_statements("; ; ; ").is_empty());
	}

	#[test]
	fn row_wire_limit() {
		// Ordinary rows are within the limit.
		assert!(row_within_wire_limit(&[Some(vec![1, 2, 3]), None]));
		// Overflow boundary is checked from lengths alone (no huge allocation):
		// a single ~2.1 GiB cell, or several cells summing past i32::MAX.
		assert!(wire_row_fits(std::iter::once(i32::MAX as usize - 16)));
		assert!(!wire_row_fits(std::iter::once(i32::MAX as usize)));
		assert!(!wire_row_fits(std::iter::repeat_n(1_000_000_000, 3)));
	}

	#[test]
	fn parses_set_statements() {
		let set = parse_set("SET dialect = 'gql'").unwrap();
		assert_eq!(set.name, "dialect");
		assert_eq!(set.value, "'gql'");
		let set = parse_set("SET extra_float_digits TO 3").unwrap();
		assert_eq!(set.name, "extra_float_digits");
		assert_eq!(set.value, "3");
		// Unmodelled SET syntax parses with an empty name (rejected by handle_set).
		let set = parse_set("SET TIME ZONE 'UTC'").unwrap();
		assert_eq!(set.name, "");
		assert_eq!(set.value, "TIME ZONE 'UTC'");
		let set = parse_set("SET ROLE admin").unwrap();
		assert_eq!(set.name, "");
		assert_eq!(set.value, "ROLE admin");
		// Multi-statement SET is not a standalone SET statement.
		assert!(parse_set("SET a = 1; SELECT 1").is_none());
		// Not a SET statement.
		assert!(parse_set("SELECT 1").is_none());
	}

	#[test]
	fn parses_dialects() {
		assert_eq!(parse_dialect("gql"), Some(Dialect::Gql));
		assert_eq!(parse_dialect("'GQL'"), Some(Dialect::Gql));
		assert_eq!(parse_dialect("surrealql"), Some(Dialect::SurrealQl));
		assert_eq!(parse_dialect("postgres"), Some(Dialect::PgSql));
		assert_eq!(parse_dialect("pgsql"), Some(Dialect::PgSql));
		assert_eq!(parse_dialect("mysql"), Some(Dialect::MySql));
		assert_eq!(parse_dialect("nope"), None);
	}

	#[test]
	fn dialect_names_round_trip() {
		// The name a refusal reports must be one a client can actually select,
		// so it names the setting to change.
		for dialect in [Dialect::SurrealQl, Dialect::Gql, Dialect::PgSql, Dialect::MySql] {
			assert_eq!(parse_dialect(dialect_name(dialect)), Some(dialect));
		}
	}

	#[test]
	fn parses_dialect_from_options() {
		assert_eq!(dialect_from_options("-c dialect=gql"), Some(Dialect::Gql));
		assert_eq!(dialect_from_options("-cdialect=gql"), Some(Dialect::Gql));
		assert_eq!(dialect_from_options("dialect=surrealql"), Some(Dialect::SurrealQl));
		assert_eq!(dialect_from_options("-c statement_timeout=5000"), None);
	}

	#[test]
	fn rewrites_positional_params() {
		assert_eq!(rewrite("RETURN $1 + $2"), "RETURN $_1 + $_2");
		assert_eq!(rewrite("SELECT * FROM t WHERE id = $10"), "SELECT * FROM t WHERE id = $_10");
	}

	#[test]
	fn reports_highest_positional_index() {
		assert_eq!(rewrite_positional_params("RETURN $1 + $3").1, 3);
		assert_eq!(rewrite_positional_params("RETURN 'no $9 here'").1, 0);
		assert_eq!(rewrite_positional_params("RETURN $name").1, 0);
	}

	#[test]
	fn leaves_named_params_untouched() {
		assert_eq!(rewrite("RETURN $name"), "RETURN $name");
		assert_eq!(rewrite("RETURN $_1"), "RETURN $_1");
	}

	#[test]
	fn does_not_rewrite_inside_strings() {
		assert_eq!(rewrite("RETURN 'price $1'"), "RETURN 'price $1'");
		assert_eq!(rewrite(r#"RETURN "cost $2""#), r#"RETURN "cost $2""#);
		// An escaped quote does not end the string early.
		assert_eq!(rewrite(r#"RETURN 'a\'b $1' + $2"#), r#"RETURN 'a\'b $1' + $_2"#);
	}

	#[test]
	fn does_not_rewrite_inside_angle_identifiers() {
		// `⟨…⟩` delimits a SurrealQL identifier/record-id; a `$1` inside it is
		// literal text and must not be rewritten.
		assert_eq!(
			rewrite("SELECT * FROM ⟨tbl$1⟩ WHERE x = $2"),
			"SELECT * FROM ⟨tbl$1⟩ WHERE x = $_2"
		);
		// A parameter with a delimited name (`$⟨name⟩`) is already valid and is
		// left untouched.
		assert_eq!(rewrite("RETURN $⟨name⟩"), "RETURN $⟨name⟩");
	}

	#[test]
	fn does_not_rewrite_inside_comments() {
		assert_eq!(rewrite("RETURN 1 -- $1\n+ $2"), "RETURN 1 -- $1\n+ $_2");
		assert_eq!(rewrite("RETURN /* $1 */ $2"), "RETURN /* $1 */ $_2");
	}

	#[test]
	fn database_param_parsing() {
		let s = |v: &str| Some(v.to_string());
		assert_eq!(parse_database_param("root", None), (None, None));
		assert_eq!(parse_database_param("root", Some("root")), (None, None));
		assert_eq!(parse_database_param("root", Some("")), (None, None));
		assert_eq!(parse_database_param("root", Some("ns")), (s("ns"), None));
		assert_eq!(parse_database_param("root", Some("ns/")), (s("ns"), None));
		assert_eq!(parse_database_param("root", Some("ns/db")), (s("ns"), s("db")));
		assert_eq!(parse_database_param("root", Some("/db")), (None, s("db")));
		assert_eq!(parse_database_param("root", Some("root/db")), (s("root"), s("db")));
	}

	#[test]
	fn translates_postgres_queries_to_surrealql() {
		let pg_insert = "INSERT INTO users (id, name) VALUES (1, 'Alice');";
		let translated_insert = surrealdb_pgsql::translate_sql(pg_insert);
		assert!(matches!(
			translated_insert,
			Err(surrealdb_pgsql::TranslationError::Unsupported(_))
		));
	}
}
