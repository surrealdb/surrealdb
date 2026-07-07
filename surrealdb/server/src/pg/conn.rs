use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use bytes::BytesMut;
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::iam::verify::{self, ScramAuth, basic};
use surrealdb_core::kvs::{Datastore, LockType, Transaction, TransactionType};
use surrealdb_core::sql::Ast;
use surrealdb_core::syn;
use surrealdb_types::{Value, Variables};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio_rustls::TlsAcceptor;
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;

use super::error::PgError;
use super::msg::{self, DescribeTarget, Frontend, StartupMessage};
use super::typing::{PgColumn, PgType, ResultShape, shape_result, shape_result_jsonb};
use super::{CancelRegistry, LOG, encode, sasl};
use crate::cnf::PKG_VERSION;

/// A client connection stream: plaintext, or upgraded to TLS after an accepted
/// SSLRequest. `Either` forwards `AsyncRead`/`AsyncWrite` to whichever it is,
/// so the rest of the connection code is transport-agnostic.
type PgStream = Either<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;

/// Result-format codes are jsonb-single-column for driver-prepared statements
/// and typed for portal-described flows. `PROTOCOL 3.0` binary result flag.
const FORMAT_TEXT: i16 = 0;
const FORMAT_BINARY: i16 = 1;

/// The query language a connection speaks. Chosen at startup via
/// `options=-c dialect=...` and switchable in-session with `SET dialect`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
	SurrealQl,
	Gql,
}

/// Upper bound on live prepared statements / portals per connection, so a
/// client cannot accumulate them without limit.
const MAX_PREPARED: usize = 1024;

/// Upper bound on the parameters a prepared statement may reference. The client
/// controls the positional index in the query text (`$n`), so without a cap a
/// value like `$999999999` would force a multi-gigabyte ParameterDescription
/// allocation; `i16::MAX` also keeps the on-wire parameter count in range.
const MAX_PARAMS: usize = i16::MAX as usize;

/// A parsed, not-yet-bound prepared statement.
struct PreparedStatement {
	/// The (positional-rewritten) query text; used for GQL, and as the source
	/// for error messages.
	query: String,
	/// The parsed SurrealQL AST, cached at Parse so repeated Execute does not
	/// re-parse. `None` for the GQL dialect (parsed by its own executor) and
	/// for empty queries.
	parsed: Option<Ast>,
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
	let negotiated = tokio::time::timeout(
		STARTUP_TIMEOUT,
		negotiate_tls(stream, acceptor.as_deref(), &registry, &shutdown),
	)
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
		dialect: Dialect::SurrealQl,
		transaction: None,
		txn_failed: false,
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
	dialect: Dialect,
	/// The active interactive transaction opened by a standalone `BEGIN`.
	transaction: Option<Arc<Transaction>>,
	/// Set when a statement fails inside an interactive transaction; further
	/// queries are refused (SQLSTATE 25P02) until `COMMIT`/`ROLLBACK`.
	txn_failed: bool,
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
		let authed = match tokio::time::timeout(STARTUP_TIMEOUT, self.startup_and_auth()).await {
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
		// Apply a connect-time `options=-c dialect=...` selection.
		if let Some(options) = options.as_deref()
			&& let Some(dialect) = dialect_from_options(options)
		{
			self.dialect = dialect;
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
				None => self.authenticate_cleartext(&user, ns.as_deref(), db.as_deref()).await?,
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
		let server_version = format!("16.0-surrealdb.{}", *PKG_VERSION);
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
		// Never ask for a cleartext password over an unencrypted connection. A
		// user without SCRAM material (e.g. imported with only a password hash)
		// can authenticate only once the link is encrypted; refuse rather than
		// expose the password to an on-path observer.
		if !self.is_tls() {
			self.report(
				&PgError::invalid_password(
					"password authentication requires an encrypted connection for this user; \
					 connect with TLS (sslmode=require) or define the user with a password so \
					 SCRAM material is generated",
				)
				.fatal(),
			)
			.await?;
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
			self.statements.insert(
				name,
				PreparedStatement {
					query: query.to_string(),
					parsed: None,
					param_types: Vec::new(),
					param_count: 0,
					empty: false,
					described: false,
					control: Some(control),
				},
			);
			msg::write_parse_complete(out);
			return Ok(());
		}
		let (query, parsed, param_count, empty) = match self.dialect {
			Dialect::SurrealQl => {
				// Map Postgres `$1..$n` to SurrealQL `$_1..$_n`, then parse once
				// so syntax errors surface at Parse (as Postgres does) and the
				// AST can be cached for repeated Execute without re-parsing.
				let (query, positional) = rewrite_positional_params(query);
				let config = self.ds.config();
				let ast = syn::parse_with_capabilities(&query, self.ds.get_capabilities(), &config)
					.map_err(|e| PgError::syntax(e.to_string()))?;
				let empty = ast.num_statements() == 0;
				// Report at least as many parameters as the query references,
				// so a driver relying on ParameterDescription sees them all.
				let param_count = param_types.len().max(positional);
				let parsed = if empty {
					None
				} else {
					Some(ast)
				};
				(query, parsed, param_count, empty)
			}
			// GQL uses a different parser and parameter syntax; store the query
			// verbatim and let its executor parse it.
			Dialect::Gql => {
				let empty = query.trim().is_empty();
				(query.to_string(), None, param_types.len(), empty)
			}
		};
		// Reject an absurd parameter count before it reaches the
		// ParameterDescription allocation or the i16 wire count.
		if param_count > MAX_PARAMS {
			return Err(PgError::protocol(format!(
				"statement references {param_count} parameters, exceeding the maximum of {MAX_PARAMS}"
			)));
		}
		self.statements.insert(
			name,
			PreparedStatement {
				query,
				parsed,
				param_count,
				param_types,
				empty,
				described: false,
				control: None,
			},
		);
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
		if !self.portals.contains_key(&portal) && self.portals.len() >= MAX_PREPARED {
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
			vars.insert(format!("_{}", i + 1), value);
		}
		self.portals.insert(
			portal,
			Portal {
				statement,
				params: vars,
				result_formats,
				executed: None,
				control_ran: false,
			},
		);
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
				// Pin this statement to the single-`jsonb`-column shape: its
				// column types cannot be known before execution.
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
				self.statements.remove(name);
				// Drop any portals bound to the closed statement.
				self.portals.retain(|_, p| p.statement != name);
			}
			DescribeTarget::Portal => {
				self.portals.remove(name);
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
		let jsonb_shape = stmt.described;
		let parsed = stmt.parsed.clone();
		let query = stmt.query.clone();
		let value = self.execute_query(parsed, &query, params).await?;
		let shape = if jsonb_shape {
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
		let portal = self.portals.get_mut(portal_name).expect("checked above");
		portal.executed = Some(Executed {
			columns: shape.columns,
			rows: shape.rows.into(),
		});
		Ok(())
	}

	/// Run a bound extended-protocol query with its parameters, returning the
	/// value of its last statement. The first statement error aborts the whole
	/// query. Honors the connection dialect and any open interactive
	/// transaction. SurrealQL executes the AST cached at Parse (no re-parse);
	/// GQL executes its query text through the GQL executor.
	async fn execute_query(
		&mut self,
		parsed: Option<Ast>,
		query: &str,
		params: Variables,
	) -> Result<Value, PgError> {
		if self.dialect == Dialect::Gql && self.transaction.is_some() {
			return Err(PgError::feature_not_supported(
				"GQL queries cannot run inside an interactive transaction",
			));
		}
		let cancel = self.arm_cancel();
		let results = match (self.dialect, self.transaction.clone(), parsed) {
			(Dialect::Gql, _, _) => self.execute_gql_query(query, params).await,
			(Dialect::SurrealQl, Some(tx), Some(ast)) => self
				.ds
				.process_with_transaction_and_cancel(ast, &self.session, Some(params), tx, cancel)
				.await
				.map_err(|e| PgError::from(&e)),
			(Dialect::SurrealQl, None, Some(ast)) => self
				.ds
				.process_with_cancel(ast, &self.session, Some(params), cancel)
				.await
				.map_err(|e| PgError::from(&e)),
			// An empty SurrealQL statement has no AST; empty portals are
			// short-circuited before execution, so this yields no value.
			(Dialect::SurrealQl, _, None) => Ok(Vec::new()),
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

	#[cfg(feature = "gql")]
	async fn execute_gql_query(
		&self,
		query: &str,
		params: Variables,
	) -> Result<Vec<surrealdb_core::dbs::QueryResult>, PgError> {
		self.ds.execute_gql(query, &self.session, Some(params)).await.map_err(|e| PgError::from(&e))
	}

	#[cfg(not(feature = "gql"))]
	async fn execute_gql_query(
		&self,
		_query: &str,
		_params: Variables,
	) -> Result<Vec<surrealdb_core::dbs::QueryResult>, PgError> {
		Err(PgError::feature_not_supported("this server was not built with GQL support"))
	}

	async fn simple_query(&mut self, sql: &str) -> Result<()> {
		let mut out = BytesMut::new();
		let trimmed = sql.trim();
		if trimmed.is_empty() {
			msg::write_empty_query_response(&mut out);
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
		// `SET` is not SurrealQL; intercept it for dialect switching and the GUC
		// no-op allowlist before it reaches the parser. In the GQL dialect only a
		// `SET dialect` is intercepted (see `should_intercept_set`); other `SET`
		// is a GQL mutation clause and reaches the executor.
		if let Some(set) = parse_set(trimmed).filter(|set| self.should_intercept_set(set)) {
			if let Err(err) = self.handle_set(set, &mut out) {
				msg::write_error_response(&mut out, &err);
			}
			return self.finish_query(out).await;
		}
		match self.dialect {
			// GQL has no transaction-aware execution path, so running it inside
			// an interactive transaction would silently auto-commit outside the
			// block. Refuse rather than mislead.
			Dialect::Gql if self.transaction.is_some() => {
				msg::write_error_response(
					&mut out,
					&PgError::feature_not_supported(
						"GQL queries cannot run inside an interactive transaction",
					),
				);
			}
			Dialect::Gql => self.run_gql(sql, &mut out).await?,
			Dialect::SurrealQl if self.transaction.is_some() => {
				self.run_in_transaction(sql, &mut out).await?;
			}
			Dialect::SurrealQl => self.run_surrealql_autocommit(sql, &mut out).await?,
		}
		self.finish_query(out).await
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
		let config = self.ds.config();
		let ast = match syn::parse_with_capabilities(sql, self.ds.get_capabilities(), &config) {
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
			// Session persistence, borrowed from the interactive shell
			// (cli/sql.rs): the values of this unit's LET statements and the
			// resulting session are appended as trailing `$param` expressions
			// and read back from the last results after execution.
			//
			// Skip this for a unit that ends in `CANCEL`: a LET/USE inside a
			// rolled-back transaction must not leak its (uncommitted) value
			// into the persistent connection session. Committed blocks still
			// persist their LET/USE, matching how the interactive shell behaves.
			let capture_session = !unit.contains_cancel();
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
				match self.ds.process_with_cancel(unit, &self.session, None, cancel).await {
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
	async fn run_in_transaction(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		let tx = self.transaction.clone().expect("caller checked transaction is open");
		let cancel = self.arm_cancel();
		let results =
			self.ds.execute_with_transaction_and_cancel(sql, &self.session, None, tx, cancel).await;
		match results {
			Ok(results) => {
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
	#[cfg(feature = "gql")]
	async fn run_gql(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		match self.ds.execute_gql(sql, &self.session, None).await {
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

	#[cfg(not(feature = "gql"))]
	async fn run_gql(&mut self, _sql: &str, out: &mut BytesMut) -> Result<()> {
		msg::write_error_response(
			out,
			&PgError::feature_not_supported("this server was not built with GQL support"),
		);
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
						.transaction(TransactionType::Write, LockType::Optimistic)
						.await
						.map_err(|err| PgError::internal(err.to_string()))?;
					self.transaction = Some(Arc::new(tx));
					self.txn_failed = false;
				}
				msg::write_command_complete(out, "BEGIN");
			}
			TxnOp::Commit => match self.transaction.take() {
				// Committing an aborted transaction rolls it back; report so.
				Some(tx) if self.txn_failed => {
					self.txn_failed = false;
					let _ = tx.cancel().await;
					msg::write_command_complete(out, "ROLLBACK");
				}
				Some(tx) => {
					// The transaction was already taken, so on failure
					// ready_status() reports 'I' and the caller emits only the
					// error (no trailing CommandComplete), keeping the sequence valid.
					tx.commit().await.map_err(|err| PgError::internal(err.to_string()))?;
					msg::write_command_complete(out, "COMMIT");
				}
				None => {
					msg::write_notice(out, "25P01", "there is no transaction in progress");
					msg::write_command_complete(out, "COMMIT");
				}
			},
			TxnOp::Rollback => {
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

	/// Whether a `SET` should be intercepted rather than executed. In the
	/// SurrealQL dialect every `SET` is intercepted (top-level `SET` is not
	/// SurrealQL). In the GQL dialect `SET` is a mutation clause, so only what a
	/// driver means as configuration is intercepted — the `dialect` control knob,
	/// the known GUC allowlist, and unmodelled `SET` syntax (empty name, e.g.
	/// `SET TIME ZONE ...`); a real `property = value` mutation reaches the executor.
	fn should_intercept_set(&self, set: &SetStatement) -> bool {
		self.dialect == Dialect::SurrealQl
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
			// Unmodelled SET syntax — accept as a no-op.
			msg::write_command_complete(out, "SET");
			return Ok(());
		}
		if name.eq_ignore_ascii_case("dialect") {
			let dialect = parse_dialect(&value)
				.ok_or_else(|| PgError::invalid_text(format!("unrecognized dialect: {value}")))?;
			self.dialect = dialect;
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
	async fn emit_result(&mut self, value: Value, out: &mut BytesMut) -> Result<bool> {
		let ResultShape {
			columns,
			rows,
		} = shape_result(value);
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

	/// Fold the trailing `$param` results back into the connection session:
	/// LET values into the session variables, and the `$session` object's
	/// namespace/database selection (which reflects any `USE` statements the
	/// unit executed).
	fn apply_session_updates(
		&mut self,
		let_vars: Vec<String>,
		mut trailing: Vec<surrealdb_core::dbs::QueryResult>,
	) {
		let Some(session_result) = trailing.pop() else {
			return;
		};
		for (name, result) in let_vars.into_iter().zip(trailing) {
			// Never write a protected name ($session/$auth/$token/$access)
			// into the session variables: the executor rejects those on every
			// subsequent query, which would poison the connection.
			if surrealdb_core::rpc::check_protected_param(&name).is_err() {
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
			// than a bare dropped socket. The framing is now unrecoverable, so
			// the connection still terminates.
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
	let rest = trimmed.trim_end_matches(';').trim();
	let mut words = rest.splitn(2, char::is_whitespace);
	if !words.next()?.eq_ignore_ascii_case("SET") {
		return None;
	}
	let assignment = words.next().unwrap_or("").trim();
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
		// Unmodelled SET syntax: accept as a no-op.
		None => SetStatement {
			name: String::new(),
			value: String::new(),
		},
	})
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
	match value.trim().trim_matches('\'').to_ascii_lowercase().as_str() {
		"surrealql" | "surql" | "sql" => Some(Dialect::SurrealQl),
		"gql" => Some(Dialect::Gql),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::{
		Dialect, TxnOp, dialect_from_options, parse_database_param, parse_dialect, parse_set,
		rewrite_positional_params, row_within_wire_limit, transaction_control, wire_row_fits,
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
		// Modifier clauses that drivers/ORMs attach are still recognised.
		assert_eq!(transaction_control("BEGIN ISOLATION LEVEL SERIALIZABLE"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("START TRANSACTION READ WRITE"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("BEGIN /* jdbc */"), Some(TxnOp::Begin));
		assert_eq!(transaction_control("ROLLBACK AND NO CHAIN"), Some(TxnOp::Rollback));
		// `START` without `TRANSACTION` is not a transaction statement.
		assert_eq!(transaction_control("START"), None);
		// A real query that merely begins with a keyword-like token is not one.
		assert_eq!(transaction_control("BEGIN something weird"), None);
		// `COMMIT; SELECT 1` is multi-statement — not intercepted here.
		assert_eq!(transaction_control("COMMIT; SELECT 1"), None);
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
		// Unmodelled SET syntax parses with an empty name (accepted as no-op).
		assert_eq!(parse_set("SET TIME ZONE 'UTC'").unwrap().name, "");
		// Not a SET statement.
		assert!(parse_set("SELECT 1").is_none());
	}

	#[test]
	fn parses_dialects() {
		assert_eq!(parse_dialect("gql"), Some(Dialect::Gql));
		assert_eq!(parse_dialect("'GQL'"), Some(Dialect::Gql));
		assert_eq!(parse_dialect("surrealql"), Some(Dialect::SurrealQl));
		assert_eq!(parse_dialect("nope"), None);
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
}
