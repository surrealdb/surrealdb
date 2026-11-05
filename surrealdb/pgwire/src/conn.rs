use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use bytes::BytesMut;
use common::time::timeout;
use surrealdb_catalog::providers::DatabaseProvider;
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::Session;
use surrealdb_core::iam::verify::{self, ScramAuth, basic};
use surrealdb_core::kvs::{
	Datastore, Dialect, Isolation, QueryRequest, QuerySource, ReadOnlyOptions,
	is_isolation_not_supported,
};
use surrealdb_core::syn;
use surrealdb_core::syn::{Query as Ast, TopLevelExpr};
use surrealdb_datastore::Transaction;
use surrealdb_iam::{Actor, Auth, Level, Role};
use surrealdb_kvs::TransactionType;
use surrealdb_pgsql::ast::stmt::{
	CopyDirection, CopyFormat, CopyStmt, CopyTarget, DropObjectType, Statement as PgStatement,
	TableRef,
};
use surrealdb_pgsql::printer::{print_ident, print_string_literal};
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_sqlplan::binder::pgsql::bind_select;
use surrealdb_sqlplan::catalog::MemoryCatalog;
use surrealdb_sqlplan::descriptor::pgsql::{PgWriteAnalysis, PgWriteMetadata};
use surrealdb_sqlplan::dialect::DialectPolicy;
use surrealdb_sqlplan::observability::global_metrics;
use surrealdb_sqlplan::{
	EmptyCatalog, PlanError, SqlDialect, bind_and_lower_pgsql, lower_pgsql_ddl,
	lower_pgsql_truncate,
};
use surrealdb_types::{RecordIdKey, SurrealValue, ToSql, Value, Variables};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "listener")]
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::either::Either;
use tokio_util::sync::CancellationToken;

use super::advisory::{
	self, AdvisoryArg, AdvisoryCall, AdvisoryKey, AdvisoryLocks, AdvisoryOp, AdvisorySession,
	Interrupted,
};
use super::copy::{
	BATCH_BYTES, BATCH_ROWS, Format, Ingest, MAX_RECORD_BYTES, Row, SLICE_BYTES, Scan, row_cost,
};
use super::error::{PgError, unsupported_construct};
use super::guc::{GucStore, ShowResult, normalize_name};
use super::msg::{self, DescribeTarget, Frontend, StartupMessage};
use super::sqlprepare::{Argument, BindError, PrepareError, SqlPrepared, first_keyword};
use super::typing::{
	PgColumn, PgType, ResultShape, WildcardTypes, shape_result, shape_result_declared,
	shape_result_jsonb, shape_result_with_descriptor,
};
use super::{AuthRequest, Authenticator, BoxedIo, CancelRegistry, LOG, Start, encode, sasl};
use crate::tls::{TlsAcceptor, TlsStream};

/// A client connection stream: plaintext, or upgraded to TLS after an accepted
/// SSLRequest. `Either` forwards `AsyncRead`/`AsyncWrite` to whichever it is,
/// so the rest of the connection code is transport-agnostic.
type PgStream = Either<BoxedIo, TlsStream<BoxedIo>>;

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

/// Pre-parsed execution artifact.
///
/// A PostgreSQL `SELECT`, `INSERT`, `UPDATE` or `DELETE` is never held here:
/// how it lowers depends on the schema at the time it runs (a column declared
/// `record<T>` is compared and assigned by its key), so it is parsed and
/// lowered by the datastore on every execution.
#[derive(Clone)]
enum CachedPlan {
	PgAst(Ast),
}

/// Syntactic category of a prepared statement for command tag generation and row expectations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementKind {
	Select,
	Insert,
	Update,
	Delete,
	CreateTable,
	DropTable,
	DropSchema,
	AlterTable,
	CreateIndex,
	CreateView,
	CreateSchema,
	CreateExtension,
	CreateType,
	AlterType,
	CreateDomain,
	AlterDomain,
	DropType,
	DropDomain,
	Truncate,
	Other,
}

impl StatementKind {
	/// The kind of a parsed PostgreSQL command and whether it answers with
	/// rows (a `RETURNING` clause), or `None` for a statement that is not a
	/// command: a query, or a statement the connection answers on its own terms.
	fn of_pg_command(stmt: &PgStatement) -> Option<(Self, bool)> {
		match stmt {
			PgStatement::Insert(insert) => Some((Self::Insert, insert.returning.is_some())),
			PgStatement::Update(update) => Some((Self::Update, update.returning.is_some())),
			PgStatement::Delete(delete) => Some((Self::Delete, delete.returning.is_some())),
			PgStatement::Truncate(_) => Some((Self::Truncate, false)),
			PgStatement::CreateTable(_) => Some((Self::CreateTable, false)),
			PgStatement::CreateIndex(_) => Some((Self::CreateIndex, false)),
			PgStatement::CreateView(_) => Some((Self::CreateView, false)),
			PgStatement::CreateSchema(_) => Some((Self::CreateSchema, false)),
			PgStatement::CreateExtension(_) => Some((Self::CreateExtension, false)),
			PgStatement::AlterTable(_) => Some((Self::AlterTable, false)),
			PgStatement::CreateType(_) | PgStatement::CreateCompositeType(_) => {
				Some((Self::CreateType, false))
			}
			PgStatement::AlterType(_) => Some((Self::AlterType, false)),
			PgStatement::CreateDomain(_) => Some((Self::CreateDomain, false)),
			PgStatement::AlterDomain(_) => Some((Self::AlterDomain, false)),
			PgStatement::Drop(drop) => Some((Self::of_drop(drop.object_type), false)),
			PgStatement::Select(_)
			| PgStatement::Copy(_)
			| PgStatement::Transaction(_)
			| PgStatement::Set(_)
			| PgStatement::Show(_)
			| PgStatement::Reset(_)
			| PgStatement::Explain(_)
			| PgStatement::Prepare(_)
			| PgStatement::Execute(_)
			| PgStatement::Deallocate(_)
			| PgStatement::CreatePropertyGraph(_)
			| PgStatement::DropPropertyGraph(_)
			| PgStatement::Empty => None,
		}
	}

	/// The kind of a `DROP` of `object`: a schema, a type and a domain have tags of their own, and
	/// every other object answers as a table.
	fn of_drop(object: DropObjectType) -> Self {
		if matches!(object, DropObjectType::Type) {
			Self::DropType
		} else if matches!(object, DropObjectType::Domain) {
			Self::DropDomain
		} else if matches!(object, DropObjectType::Schema) {
			Self::DropSchema
		} else {
			Self::DropTable
		}
	}

	fn command_tag(self, count: usize) -> String {
		match self {
			Self::Select => format!("SELECT {count}"),
			Self::Insert => format!("INSERT 0 {count}"),
			Self::Update => format!("UPDATE {count}"),
			Self::Delete => format!("DELETE {count}"),
			Self::CreateTable => "CREATE TABLE".to_string(),
			Self::DropTable => "DROP TABLE".to_string(),
			Self::DropSchema => "DROP SCHEMA".to_string(),
			Self::AlterTable => "ALTER TABLE".to_string(),
			Self::CreateIndex => "CREATE INDEX".to_string(),
			Self::CreateView => "CREATE VIEW".to_string(),
			Self::CreateSchema => "CREATE SCHEMA".to_string(),
			Self::CreateExtension => "CREATE EXTENSION".to_string(),
			Self::CreateType => "CREATE TYPE".to_string(),
			Self::AlterType => "ALTER TYPE".to_string(),
			Self::CreateDomain => "CREATE DOMAIN".to_string(),
			Self::AlterDomain => "ALTER DOMAIN".to_string(),
			Self::DropType => "DROP TYPE".to_string(),
			Self::DropDomain => "DROP DOMAIN".to_string(),
			Self::Truncate => "TRUNCATE TABLE".to_string(),
			Self::Other => format!("SELECT {count}"),
		}
	}
}

/// The type OID of a parameter type the planner inferred, `0` for one it could not.
fn inferred_oid(ty: &surrealdb_sqlplan::SqlType) -> i32 {
	if *ty == surrealdb_sqlplan::SqlType::Unknown {
		0
	} else {
		PgType::from_sql_type(*ty).oid()
	}
}

/// A parsed, not-yet-bound prepared statement.
struct PreparedStatement {
	/// The (positional-rewritten) query text; used for GQL, and as the source
	/// for error messages.
	query: String,
	/// Pre-parsed query (a SurrealQL AST, or the AST of a PostgreSQL DDL
	/// statement), cached at Parse so repeated Execute does not re-parse it.
	cached_plan: Option<CachedPlan>,
	/// Client-declared parameter type OIDs (`0` = unspecified); may be shorter
	/// than the number of `$n` placeholders in the query.
	param_types: Vec<i32>,
	/// The type OID the planner infers for each parameter from the column or expression it is
	/// compared with or stored into (`0` = none), so a bound temporal parameter can take that
	/// type. May be shorter than `param_count`.
	inferred_param_types: Vec<i32>,
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
	/// The wire types the schema declares for the columns a `*` result of this statement holds,
	/// for a statement over one base table whose columns the planner cannot list. Resolved when
	/// the statement is parsed; a declaration that no longer holds the values is ignored when
	/// the result is shaped.
	wildcard_types: Arc<WildcardTypes>,
	/// Syntactic kind of statement (Select, Insert, Update, CreateTable, etc.)
	kind: StatementKind,
	/// Whether this statement produces result rows (e.g. SELECT, or INSERT/UPDATE with RETURNING).
	returns_rows: bool,
}

impl PreparedStatement {
	fn estimated_size(&self) -> usize {
		std::mem::size_of::<Self>()
			+ self.query.capacity()
			+ self.param_types.capacity() * std::mem::size_of::<i32>()
			+ self
				.wildcard_types
				.names()
				.map(|name| name.capacity() + std::mem::size_of::<(String, PgType)>())
				.sum::<usize>()
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

/// What a statement made of SQL-level prepared-statement commands asks of the caller.
enum SqlPreparedStep {
	/// The statement is not one of them.
	NotOurs,
	/// The statement has been answered.
	Done,
	/// The statement to run in place of an `EXECUTE`.
	Run(String),
}

/// The refusal of a SQL-level prepared statement name that was never prepared.
fn missing_sql_statement(name: &str) -> PgError {
	PgError::new("26000", format!("prepared statement \"{name}\" does not exist"))
}

/// An extended-protocol statement that is intercepted rather than executed. The
/// simple-query path intercepts standalone `BEGIN`/`COMMIT`/`ROLLBACK` and
/// `SET`; classifying them at Parse lets the extended protocol do the same, so
/// drivers using server-side prepared statements get interactive transactions
/// and dialect switching too.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Control {
	Transaction(TxnOp),
	Set(SetStatement),
	Show(String),
	Reset(String),
	/// A whole-statement call of an advisory lock function; see [`advisory`].
	Advisory(AdvisoryCall),
}

impl Control {
	/// The Postgres command tag reported for this control statement's
	/// CommandComplete.
	fn command_tag(&self) -> &'static str {
		match self {
			Control::Transaction(TxnOp::Begin {
				..
			}) => "BEGIN",
			Control::Transaction(TxnOp::Commit) => "COMMIT",
			Control::Transaction(TxnOp::Rollback) => "ROLLBACK",
			Control::Transaction(TxnOp::Savepoint(_)) => "SAVEPOINT",
			Control::Transaction(TxnOp::ReleaseSavepoint(_)) => "RELEASE",
			Control::Transaction(TxnOp::RollbackSavepoint(_)) => "ROLLBACK",
			Control::Set(_) => "SET",
			Control::Show(_) => "SHOW",
			Control::Reset(_) => "RESET",
			// The tag of a portal that has already delivered its row.
			Control::Advisory(_) => "SELECT 0",
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
						Value::None
						| Value::Null
						| Value::Bool(_)
						| Value::Number(_)
						| Value::Duration(_)
						| Value::Datetime(_)
						| Value::Date(_)
						| Value::Time(_)
						| Value::Timestamp(_)
						| Value::Interval(_)
						| Value::Uuid(_)
						| Value::Geometry(_)
						| Value::Table(_)
						| Value::RecordId(_)
						| Value::File(_)
						| Value::Range(_)
						| Value::Regex(_)
						| Value::Set(_) => 16,
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
	stream: BoxedIo,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	permit: Option<OwnedSemaphorePermit>,
	registry: Arc<CancelRegistry>,
	advisory_locks: Arc<AdvisoryLocks>,
	pid: i32,
	secret: i32,
	acceptor: Option<Arc<TlsAcceptor>>,
	start: Start,
	authenticator: Option<Arc<dyn Authenticator>>,
	dialect: Dialect,
) {
	let cancel = CancelHandle::new();
	registry.insert((pid, secret), cancel.clone());
	// Negotiate an optional TLS upgrade before the startup exchange. A
	// CancelRequest is fully handled here (it carries no post-handshake work).
	// Bound the whole negotiation (reads + TLS handshake) by the startup
	// deadline so an unauthenticated peer cannot hold a connection permit
	// open indefinitely (slow-loris).
	let negotiated = timeout(
		STARTUP_TIMEOUT,
		negotiate_tls(stream, acceptor.as_deref(), start, &registry, &shutdown),
	)
	.await;
	let Some((stream, prelude)) = negotiated.ok().flatten() else {
		registry.remove(&(pid, secret));
		return;
	};
	let mut conn = Connection {
		stream,
		secured: start == Start::Secured,
		prelude,
		peer,
		ds,
		ready,
		shutdown,
		session: Session::default(),
		base_session: None,
		txn_session_snapshot: None,
		txn_has_local_session_override: false,
		statements: HashMap::new(),
		sql_prepared: SqlPrepared::default(),
		portals: HashMap::new(),
		prepared_bytes: 0,
		portal_bytes: 0,
		dialect,
		gucs: GucStore::new(),
		transaction: None,
		savepoints: Vec::new(),
		savepoint_sessions: Vec::new(),
		txn_failed: false,
		txn_lets: Vec::new(),
		cancel,
		registry: Arc::clone(&registry),
		pid,
		secret,
		advisory: AdvisorySession::new(advisory_locks, pid),
		_permit: permit,
		authenticator,
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
	mut tcp: BoxedIo,
	acceptor: Option<&TlsAcceptor>,
	start: Start,
	registry: &CancelRegistry,
	shutdown: &CancellationToken,
) -> Option<(PgStream, Option<Vec<u8>>)> {
	if start == Start::Secured {
		return Some((Either::Left(tcp), None));
	}
	if start == Start::Tls {
		// The SSLRequest was answered before this connection was handed over: the TLS handshake
		// is the next thing on the stream.
		let acceptor = acceptor?;
		return match acceptor.accept(tcp).await {
			Ok(tls) => Some((Either::Right(tls), None)),
			Err(err) => {
				debug!(target: LOG, "postgres TLS handshake failed: {err}");
				None
			}
		};
	}
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
	tcp: &mut BoxedIo,
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
#[cfg(feature = "listener")]
pub(super) async fn reject_overloaded(mut stream: TcpStream) {
	let _ = reject_overloaded_inner(&mut stream).await;
}

#[cfg(feature = "listener")]
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
	/// The stream is not TLS but the host that handed it over ended the client's TLS
	/// ([`Start::Secured`]).
	secured: bool,
	/// A StartupMessage packet already read during TLS negotiation (plaintext
	/// path), consumed on the first startup read.
	prelude: Option<Vec<u8>>,
	peer: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	session: Session,
	/// The baseline session identity established at connection / authentication time.
	base_session: Option<Session>,
	/// Session snapshot taken at transaction begin.
	txn_session_snapshot: Option<Session>,
	/// Whether a transaction-local session identity override is active.
	txn_has_local_session_override: bool,
	statements: HashMap<String, PreparedStatement>,
	sql_prepared: SqlPrepared,
	portals: HashMap<String, Portal>,
	prepared_bytes: usize,
	portal_bytes: usize,
	/// The query language this connection speaks. Chosen at startup via
	/// `options=-c dialect=...` and switchable in-session with `SET dialect`.
	/// Only ever assigned through [`Connection::select_dialect`], which applies
	/// the per-dialect capability gate.
	dialect: Dialect,
	/// grand unified configuration (GUC) parameter store for this session.
	gucs: GucStore,
	/// The active interactive transaction opened by a standalone `BEGIN`.
	transaction: Option<Arc<Transaction>>,
	/// Active savepoint stack for the current transaction.
	savepoints: Vec<String>,
	/// Savepoint snapshots of the session: stack of `(savepoint_name,
	/// session_snapshot, number of captured LETs)`.
	savepoint_sessions: Vec<(String, Session, usize)>,
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
	/// The advisory locks this connection holds, released when it ends.
	advisory: AdvisorySession,
	/// Held for the connection's lifetime to bound concurrent connections.
	_permit: Option<OwnedSemaphorePermit>,
	/// Judges credentials in place of the datastore's own users; `None` uses the datastore.
	authenticator: Option<Arc<dyn Authenticator>>,
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
				"replication" => {
					self.report(
						&PgError::feature_not_supported("replication is not supported").fatal(),
					)
					.await?;
					return Ok(false);
				}

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
		let namespace = options.as_deref().and_then(|options| option_setting(options, "namespace"));
		let (ns, db) = match parse_database_param(&user, database.as_deref(), namespace) {
			Ok(selection) => selection,
			Err(err) => {
				self.report(&err.fatal()).await?;
				return Ok(false);
			}
		};
		self.session = Session::default();
		self.session.ip = Some(self.peer.ip().to_string());
		self.session.id = Some(uuid::Uuid::new_v4());
		if !self.ds.is_auth_enabled() && self.authenticator.is_none() {
			self.session.au =
				Arc::new(Auth::new(Actor::new(user.clone(), vec![Role::Owner], Level::Root)));
		}
		if self.ds.is_auth_enabled() || self.authenticator.is_some() {
			// Prefer SASL/SCRAM-SHA-256 when the target user has SCRAM verifier
			// material; fall back to cleartext-password (over TLS) for users
			// that only carry a legacy Argon2 hash.
			// An external authenticator holds no SCRAM verifier, so the password is asked for in
			// cleartext, over TLS only.
			let scram: Result<Option<ScramAuth>> = if self.authenticator.is_some() {
				Ok(None)
			} else {
				verify::scram_lookup(&self.ds, &user, ns.as_deref(), db.as_deref()).await
			};
			let scram = match scram {
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
					if self.is_tls()
						|| (self.authenticator.is_none() && self.peer.ip().is_loopback())
					{
						self.authenticate_cleartext(&user, ns.as_deref(), db.as_deref()).await?
					} else if self.authenticator.is_some() {
						// A cleartext password is never solicited outside TLS.
						self.report(&auth_failed(&user)).await?;
						false
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
		let target_ns = ns.clone().or_else(|| self.session.ns.clone());
		let target_db = db.clone().or_else(|| self.session.db.clone());
		if let (Some(ns_name), Some(db_name)) = (target_ns.as_deref(), target_db.as_deref()) {
			let tx = self
				.ds
				.transaction(TransactionType::Read)
				.await
				.map_err(|err| PgError::internal(err.to_string()))?;
			let exists = tx
				.get_db_by_name(ns_name, db_name, None)
				.await
				.map_err(|err| PgError::internal(err.to_string()))?
				.is_some();
			let can_materialize = self
				.ds
				.should_materialize_db_on_use(&tx, &self.session.au, ns_name, db_name)
				.await
				.unwrap_or(false);
			let _ = tx.cancel().await;
			if !exists && !can_materialize {
				self.report(
					&PgError::invalid_catalog_name(format!(
						"database \"{db_name}\" does not exist"
					))
					.fatal(),
				)
				.await?;
				return Ok(false);
			}
		}
		if ns.is_some() || db.is_some() {
			// Select (and, when authorized, materialize) the namespace and
			// database exactly as a `USE` statement would.
			if let Err(err) =
				self.ds.process_use(None, &mut self.session, ns.clone(), db.clone()).await
			{
				self.report(&PgError::from(&err).fatal()).await?;
				return Ok(false);
			}
		}
		self.base_session = Some(self.session.clone());
		let mut buf = BytesMut::new();
		msg::write_authentication_ok(&mut buf);
		for (key, value) in self.gucs.startup_status_params() {
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
		let clean_session = self.session.clone();
		if let Some(authenticator) = self.authenticator.clone() {
			let request = AuthRequest {
				user,
				password: pass,
				namespace: ns,
				database: db,
			};
			return match authenticator.authenticate(&clean_session, request).await {
				Some(session) => {
					self.session = session;
					true
				}
				None => {
					debug!(target: LOG, "postgres authentication failed for user '{user}'");
					false
				}
			};
		}
		if let (Some(ns), Some(db)) = (ns, db) {
			let mut sess = clean_session.clone();
			if basic(&self.ds, &mut sess, user, pass, Some(ns), Some(db)).await.is_ok() {
				self.session = sess;
				return true;
			}
		}
		if let Some(ns) = ns {
			let mut sess = clean_session.clone();
			if basic(&self.ds, &mut sess, user, pass, Some(ns), None).await.is_ok() {
				self.session = sess;
				return true;
			}
		}
		let mut sess = clean_session.clone();
		if basic(&self.ds, &mut sess, user, pass, None, None).await.is_ok() {
			self.session = sess;
			return true;
		}
		// Password-as-JWT through verify::token so record-access sessions can exist on the wire
		// (P-02 / D6)
		let mut sess = clean_session.clone();
		if verify::token(&self.ds, &mut sess, pass).await.is_ok() {
			self.session = sess;
			return true;
		}
		let mut sess = clean_session.clone();
		if verify::token(&self.ds, &mut sess, user).await.is_ok() {
			self.session = sess;
			return true;
		}
		debug!(target: LOG, "postgres authentication failed for user '{user}'");
		false
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
		// Never ask for a cleartext password over an unencrypted connection (except loopback for
		// local dev/testing). Report the standard generic authentication failure to avoid user
		// enumeration.
		if !self.is_tls() && (self.authenticator.is_some() || !self.peer.ip().is_loopback()) {
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
					if let Err(err) = self.handle_parse(name, &query, param_types, &mut out).await {
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
					// Portals survive Sync inside an active transaction.
					// At the end of an implicit transaction (no active transaction),
					// portals are cleared.
					if self.transaction.is_none() {
						self.portals.clear();
						self.portal_bytes = 0;
					}
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
				Frontend::FunctionCall => {
					if skip_until_sync {
						continue;
					}
					msg::write_error_response(
						&mut out,
						&PgError::feature_not_supported("FunctionCall is not supported"),
					);
					skip_until_sync = true;
				}

				Frontend::CopyData(_) | Frontend::CopyDone | Frontend::CopyFail(_) => {
					self.report(
						&PgError::protocol("unexpected copy message outside of copy mode").fatal(),
					)
					.await?;
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

	async fn describe_pgsql_write(
		&self,
		statement: &PgStatement,
		declared: &[i32],
	) -> Result<Option<PgWriteMetadata>, PgError> {
		let Some(analysis) = PgWriteAnalysis::new(statement) else {
			return Ok(None);
		};
		let catalog = self
			.ds
			.pgsql_catalog(&self.session, self.transaction.as_ref(), &analysis.query)
			.await
			.map_err(|error| PgError::from(&error))?;
		analysis
			.describe(&catalog, declared)
			.map(Some)
			.map_err(|error| PgError::new(error.pg_sqlstate(), error.to_string()))
	}

	async fn handle_parse(
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
		if let Some(control) = self.classify_control(query) {
			let returns_rows = matches!(control, Control::Show(_) | Control::Advisory(_));
			let (param_count, param_types) = match &control {
				Control::Advisory(call) => advisory_parameters(call, &param_types)?,
				Control::Transaction(_)
				| Control::Set(_)
				| Control::Show(_)
				| Control::Reset(_) => (0, Vec::new()),
			};
			self.insert_statement(
				name,
				PreparedStatement {
					query: query.to_string(),
					cached_plan: None,
					param_types,
					inferred_param_types: Vec::new(),
					param_count,
					empty: false,
					described: false,
					control: Some(control),
					descriptor: None,
					wildcard_types: Arc::default(),
					kind: StatementKind::Other,
					returns_rows,
				},
			)?;
			msg::write_parse_complete(out);
			return Ok(());
		}
		let mut wildcard_types = WildcardTypes::default();
		let mut inferred_param_types = Vec::new();
		let (query, cached_plan, param_count, param_types, empty, descriptor, kind, returns_rows) =
			match self.dialect {
				Dialect::SurrealQl => return Err(not_over_pgwire(Dialect::SurrealQl)),
				Dialect::PgSql => {
					global_metrics().record_query(SqlDialect::Postgres);
					let stmts = surrealdb_pgsql::parse_statements(query).map_err(|err| {
						PgError::syntax(err.to_string()).with_position(err.span.start + 1)
					})?;
					if stmts.len() > 1 {
						return Err(PgError::syntax(
							"cannot insert multiple commands into a prepared statement",
						));
					}

					let empty = stmts.is_empty();
					let (_, positional) = rewrite_positional_params(query);
					let mut param_types = param_types;
					let mut param_count = param_types.len().max(positional);
					if param_count > MAX_PARAMS {
						return Err(PgError::protocol(format!(
							"statement references {param_count} parameters, exceeding the maximum of {MAX_PARAMS}"
						)));
					}

					let mut descriptor = None;
					let mut cached_plan = None;
					let mut kind = StatementKind::Other;
					let mut returns_rows = false;
					if !empty {
						match &stmts[0] {
							PgStatement::Select(select) => {
								kind = StatementKind::Select;
								returns_rows = true;
								let catalog = self
									.ds
									.pgsql_catalog(&self.session, self.transaction.as_ref(), select)
									.await
									.map_err(|err| PgError::from(&err))?;
								let policy = DialectPolicy::postgres();
								if let Ok(bound) = bind_select(select, &catalog, &policy, None) {
									let param_desc = bound.describe_parameters(&catalog);
									param_count = param_count.max(param_desc.param_types.len());
									param_types.resize(param_count, 0);
									inferred_param_types =
										param_desc.param_types.iter().map(inferred_oid).collect();
									descriptor = bound.describe_static(&catalog, &policy);
									wildcard_types = WildcardTypes::of(
										&bound.wildcard_column_types(&catalog),
										&bound.wildcard_declared_sql_types(&catalog),
									);
									for (idx, id) in param_desc.enum_params.iter().enumerate() {
										if let Some(id) = id
											&& idx < param_types.len() && param_types[idx] == 0
										{
											param_types[idx] = PgType::Enum(*id).oid();
										}
									}
									for (idx, sql_ty) in param_desc.param_types.iter().enumerate() {
										if *sql_ty != surrealdb_sqlplan::SqlType::Unknown {
											let oid = PgType::from_sql_type(*sql_ty).oid();
											if idx < param_types.len() {
												if param_types[idx] == 0 {
													param_types[idx] = oid;
												}
											} else {
												param_types.push(oid);
											}
										}
									}
								}
								// The statement is lowered here only to report what the planner
								// refuses; the plan is not kept, because how a statement lowers
								// depends on the schema when it runs.
								// A feature the planner does not lower, a text it cannot read as
								// the value it must be, or an operator that does not exist for
								// its operands fails every Execute of this statement, so it is
								// reported here as PostgreSQL reports a statement it cannot
								// analyse.
								if let Err(
									err @ (PlanError::UnsupportedExpression(_)
									| PlanError::UnsupportedStatement(_)
									| PlanError::InvalidTextRepresentation(_)
									| PlanError::UndefinedOperator(_)),
								) = bind_and_lower_pgsql(select, &EmptyCatalog, &policy)
								{
									return Err(PgError::new(err.pg_sqlstate(), err.to_string()));
								}
							}
							PgStatement::Insert(insert) => {
								kind = StatementKind::Insert;
								returns_rows = insert.returning.is_some();
							}
							PgStatement::Update(update) => {
								kind = StatementKind::Update;
								returns_rows = update.returning.is_some();
							}
							PgStatement::Delete(delete) => {
								kind = StatementKind::Delete;
								returns_rows = delete.returning.is_some();
							}
							PgStatement::Truncate(truncate) => {
								kind = StatementKind::Truncate;
								returns_rows = false;
								let cur_db = self
									.session
									.db
									.clone()
									.unwrap_or_else(|| "postgres".to_string());
								let catalog = MemoryCatalog::new().with_database(&cur_db);
								if truncate.tables.iter().all(|table| {
									surrealdb_sqlplan::resolve_pgsql_table_name(
										&table.parts,
										Some(&catalog),
									)
									.is_ok()
								}) && let Ok(expr) = lower_pgsql_truncate(truncate)
								{
									cached_plan = Some(CachedPlan::PgAst(Ast {
										expressions: vec![TopLevelExpr::Expr(expr)],
									}));
								}
							}
							PgStatement::CreateTable(create) => {
								kind = StatementKind::CreateTable;
								returns_rows = false;
								let cur_db = self
									.session
									.db
									.clone()
									.unwrap_or_else(|| "postgres".to_string());
								let catalog = MemoryCatalog::new().with_database(&cur_db);
								if surrealdb_sqlplan::resolve_pgsql_table_name(
									&create.name.parts,
									Some(&catalog),
								)
								.is_ok() && let Ok(exprs) = lower_pgsql_ddl(&stmts[0])
								{
									cached_plan = Some(CachedPlan::PgAst(Ast {
										expressions: exprs
											.into_iter()
											.map(TopLevelExpr::Expr)
											.collect(),
									}));
								}
							}
							PgStatement::CreateIndex(idx) => {
								kind = StatementKind::CreateIndex;
								returns_rows = false;
								let cur_db = self
									.session
									.db
									.clone()
									.unwrap_or_else(|| "postgres".to_string());
								let catalog = MemoryCatalog::new().with_database(&cur_db);
								if surrealdb_sqlplan::resolve_pgsql_table_name(
									&idx.table.parts,
									Some(&catalog),
								)
								.is_ok() && let Ok(exprs) = lower_pgsql_ddl(&stmts[0])
								{
									cached_plan = Some(CachedPlan::PgAst(Ast {
										expressions: exprs
											.into_iter()
											.map(TopLevelExpr::Expr)
											.collect(),
									}));
								}
							}
							PgStatement::CreateView(view) => {
								kind = StatementKind::CreateView;
								returns_rows = false;
								let cur_db = self
									.session
									.db
									.clone()
									.unwrap_or_else(|| "postgres".to_string());
								let catalog = MemoryCatalog::new().with_database(&cur_db);
								if surrealdb_sqlplan::resolve_pgsql_table_name(
									&view.name.parts,
									Some(&catalog),
								)
								.is_ok() && view.query.from.iter().all(|tr| {
									if let TableRef::Table {
										name,
										..
									} = tr
									{
										surrealdb_sqlplan::resolve_pgsql_table_name(
											&name.parts,
											Some(&catalog),
										)
										.is_ok()
									} else {
										true
									}
								}) && let Ok(exprs) = lower_pgsql_ddl(&stmts[0])
								{
									cached_plan = Some(CachedPlan::PgAst(Ast {
										expressions: exprs
											.into_iter()
											.map(TopLevelExpr::Expr)
											.collect(),
									}));
								}
							}
							// The plan is not kept: `DROP CONSTRAINT` and `ADD FOREIGN KEY`
							// lower against the schema when the statement runs.
							PgStatement::AlterTable(_) => {
								kind = StatementKind::AlterTable;
								returns_rows = false;
							}
							PgStatement::Drop(drop) => {
								kind = StatementKind::of_drop(drop.object_type);
								returns_rows = false;
								let cur_db = self
									.session
									.db
									.clone()
									.unwrap_or_else(|| "postgres".to_string());
								let catalog = MemoryCatalog::new().with_database(&cur_db);
								// `DROP SCHEMA` lowers against the tables of the database
								// when the statement runs, so no plan is kept.
								if !matches!(drop.object_type, DropObjectType::Schema)
									&& drop.names.iter().all(|n| {
										surrealdb_sqlplan::resolve_pgsql_table_name(
											&n.parts,
											Some(&catalog),
										)
										.is_ok()
									}) && let Ok(exprs) = lower_pgsql_ddl(&stmts[0])
								{
									cached_plan = Some(CachedPlan::PgAst(Ast {
										expressions: exprs
											.into_iter()
											.map(TopLevelExpr::Expr)
											.collect(),
									}));
								}
							}
							PgStatement::Copy(_)
							| PgStatement::CreateSchema(_)
							| PgStatement::Transaction(_)
							| PgStatement::Set(_)
							| PgStatement::Show(_)
							| PgStatement::Reset(_)
							| PgStatement::Explain(_)
							| PgStatement::Prepare(_)
							| PgStatement::Execute(_)
							| PgStatement::Deallocate(_)
							| PgStatement::CreatePropertyGraph(_)
							| PgStatement::DropPropertyGraph(_)
							| PgStatement::Empty => {
								kind = StatementKind::Other;
								returns_rows = false;
							}
							PgStatement::CreateExtension(_) => {
								kind = StatementKind::CreateExtension;
								returns_rows = false;
							}
							PgStatement::CreateType(_) => {
								kind = StatementKind::CreateType;
								returns_rows = false;
							}
							PgStatement::AlterType(_) => {
								kind = StatementKind::AlterType;
								returns_rows = false;
							}
							PgStatement::CreateDomain(_) => {
								kind = StatementKind::CreateDomain;
								returns_rows = false;
							}
							PgStatement::AlterDomain(_) => {
								kind = StatementKind::AlterDomain;
								returns_rows = false;
							}
							PgStatement::CreateCompositeType(_) => {
								kind = StatementKind::CreateType;
								returns_rows = false;
							}
						}
					}
					if let Some(statement) = stmts.first()
						&& let Some(metadata) =
							self.describe_pgsql_write(statement, &param_types).await?
					{
						param_count = param_count.max(metadata.parameters.param_types.len());
						param_types.resize(param_count, 0);
						inferred_param_types =
							metadata.parameters.param_types.iter().map(inferred_oid).collect();
						for (slot, id) in
							param_types.iter_mut().zip(&metadata.parameters.enum_params)
						{
							if *slot == 0
								&& let Some(id) = id
							{
								*slot = PgType::Enum(*id).oid();
							}
						}
						for (slot, ty) in
							param_types.iter_mut().zip(metadata.parameters.param_types)
						{
							if *slot == 0 && ty != surrealdb_sqlplan::SqlType::Unknown {
								*slot = PgType::from_sql_type(ty).oid();
							}
						}
						descriptor = metadata.result;
						wildcard_types = WildcardTypes::of(
							&metadata.wildcard_types,
							&metadata.wildcard_declared,
						);
					}

					param_types.resize(param_count, 0);
					for oid in &mut param_types {
						if *oid == 0 {
							*oid = PgType::Text.oid();
						}
					}

					(
						query.to_string(),
						cached_plan,
						param_count,
						param_types,
						empty,
						descriptor,
						kind,
						returns_rows,
					)
				}
				Dialect::Gql | Dialect::MySql => {
					let (_, positional) = rewrite_positional_params(query);
					let param_count = param_types.len().max(positional);
					let empty = query.trim().is_empty();
					(
						query.to_string(),
						None,
						param_count,
						param_types,
						empty,
						None,
						StatementKind::Other,
						!empty,
					)
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
				inferred_param_types,
				empty,
				described: false,
				control: None,
				descriptor,
				wildcard_types: Arc::new(wildcard_types),
				kind,
				returns_rows,
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
			let inferred = stmt.inferred_param_types.get(i).copied().unwrap_or(0);
			let value = encode::coerce_temporal_param(value, oid, inferred);
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
				let control = self.statements.get(name).map(|s| s.control.clone());
				if let Some(Some(control)) = control {
					if let Control::Advisory(call) = control {
						let oids = self
							.statements
							.get(name)
							.map(|s| s.param_types.clone())
							.unwrap_or_default();
						msg::write_parameter_description(out, &oids);
						msg::write_row_description(out, &advisory_columns(&call), |_| FORMAT_TEXT);
						return Ok(());
					}
					msg::write_parameter_description(out, &[]);
					if let Control::Show(param) = control {
						self.describe_show(&param, out)?;
					} else {
						msg::write_no_data(out);
					}
					return Ok(());
				}
				let Some(stmt) = self.statements.get_mut(name) else {
					return Err(PgError::invalid_statement(name));
				};
				// If a static descriptor is available, report true columns and types;
				// otherwise pin this statement to the single-`jsonb`-column shape.
				stmt.described = true;
				let empty = stmt.empty;
				let returns_rows = stmt.returns_rows;
				// Report one OID per referenced parameter, using the declared
				// type where given and unspecified (0) otherwise.
				let oids: Vec<i32> = (0..stmt.param_count)
					.map(|i| stmt.param_types.get(i).copied().unwrap_or(0))
					.collect();
				msg::write_parameter_description(out, &oids);
				if empty || !returns_rows {
					msg::write_no_data(out);
				} else if let Some(desc) = &stmt.descriptor {
					let columns: Vec<PgColumn> =
						desc.columns.iter().map(PgColumn::of_result_column).collect();
					msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
				} else {
					let columns = [PgColumn {
						typmod: -1,
						name: "result".to_string(),
						ty: PgType::Jsonb,
					}];
					// Bind has not happened yet, so the format is nominal (text);
					// the actual DataRow format follows the eventual Bind.
					msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
				}
			}
			DescribeTarget::Portal => {
				// Control statements produce no result rows, except SHOW.
				if let Some(control) = self.portal_control(name) {
					if let Control::Show(param) = control {
						self.describe_show(&param, out)?;
					} else if let Control::Advisory(call) = control {
						let formats = self
							.portals
							.get(name)
							.map(|p| p.result_formats.clone())
							.unwrap_or_default();
						msg::write_row_description(out, &advisory_columns(&call), |i| {
							format_at(&formats, i)
						});
					} else {
						msg::write_no_data(out);
					}
					return Ok(());
				}
				if self.portal_is_empty(name)? {
					msg::write_no_data(out);
					return Ok(());
				}
				let portal = self.portals.get(name).ok_or_else(|| PgError::invalid_cursor(name))?;
				let stmt = self
					.statements
					.get(&portal.statement)
					.ok_or_else(|| PgError::invalid_statement(&portal.statement))?;
				if !stmt.returns_rows {
					msg::write_no_data(out);
					return Ok(());
				}
				let formats = portal.result_formats.clone();
				if let Some(executed) = &portal.executed {
					msg::write_row_description(out, &executed.columns, |i| format_at(&formats, i));
				} else if let Some(desc) = &stmt.descriptor {
					let columns: Vec<PgColumn> =
						desc.columns.iter().map(PgColumn::of_result_column).collect();
					msg::write_row_description(out, &columns, |i| format_at(&formats, i));
				} else if stmt.described {
					let columns = [PgColumn {
						typmod: -1,
						name: "result".to_string(),
						ty: PgType::Jsonb,
					}];
					msg::write_row_description(out, &columns, |i| format_at(&formats, i));
				} else {
					self.ensure_executed(name).await?;
					let portal = self.portals.get(name).expect("ensured above");
					let columns = &portal.executed.as_ref().expect("ensured above").columns;
					msg::write_row_description(out, columns, |i| format_at(&formats, i));
				}
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
				Control::Show(param) => self.handle_show(&param, out),
				Control::Reset(param) => self.handle_reset(&param, out),
				Control::Advisory(call) => {
					let (vars, formats) = self
						.portals
						.get(portal_name)
						.map(|p| (p.params.clone(), p.result_formats.clone()))
						.unwrap_or_default();
					self.handle_advisory(call, &vars, &formats, false, out).await
				}
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
		let (returns_rows, stmt_kind) = {
			let portal = self.portals.get(portal_name).expect("ensured above");
			if let Some(stmt) = self.statements.get(&portal.statement) {
				(stmt.returns_rows, stmt.kind)
			} else {
				(true, StatementKind::Select)
			}
		};
		if !returns_rows {
			let count = {
				let portal = self.portals.get_mut(portal_name).expect("ensured above");
				let count = portal.executed.as_ref().map(|e| e.rows.len()).unwrap_or(0);
				if let Some(executed) = portal.executed.as_mut() {
					executed.rows.clear();
				}
				count
			};
			msg::write_command_complete(out, &stmt_kind.command_tag(count));
			return Ok(Ok(()));
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
			msg::write_command_complete(out, &stmt_kind.command_tag(sent));
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
		let declared = Arc::clone(&stmt.wildcard_types);
		let cached_plan = stmt.cached_plan.clone();
		let query = stmt.query.clone();
		let value = self.execute_query(cached_plan, &query, params).await?;
		let shape = if let Some(desc) = &descriptor {
			shape_result_with_descriptor(desc, value)
		} else if jsonb_shape {
			shape_result_jsonb(value)
		} else {
			shape_result_declared(value, &declared)
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
			(Dialect::SurrealQl, ..) => Err(not_over_pgwire(Dialect::SurrealQl)),
		}?;
		let mut last = Value::None;
		for result in results {
			match result.result {
				Ok(value) => last = value,
				Err(err) => return Err(PgError::from(&err)),
			}
		}
		Ok(self.present(last))
	}

	/// A result as the connection's dialect shows it: the SQL dialects show a
	/// record id as its bare key, since SQL values carry no table.
	fn present(&self, value: Value) -> Value {
		match self.dialect {
			Dialect::PgSql | Dialect::MySql => surrealdb_sqlplan::show_record_ids_as_keys(value),
			Dialect::SurrealQl | Dialect::Gql => value,
		}
	}

	async fn simple_query(&mut self, sql: &str) -> Result<()> {
		let mut out = BytesMut::new();
		let mut remaining = sql;

		// Intercept leading control statements (e.g. `SET client_encoding = 'UTF8'; <rest>`).
		// A `BEGIN` ahead of further statements is not intercepted when the active dialect
		// is run by the executor as one batch: the block is SurrealQL and its closing
		// `COMMIT`/`CANCEL` must reach the executor together with it.
		while !remaining.trim().is_empty() {
			let (first, rest) = split_first_statement(remaining);
			let trimmed_first = first.trim();
			if let Some(control) = self.classify_control(trimmed_first) {
				if matches!(control, Control::Transaction(TxnOp::Begin { .. }))
					&& matches!(self.dialect, Dialect::SurrealQl | Dialect::Gql)
					&& !rest.trim().is_empty()
				{
					break;
				}
				if let Err(err) = self.execute_control(control, &mut out).await {
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

		// Control statements are intercepted (they manage the connection's
		// interactive transaction or session configuration rather than run in the executor).
		if let Some(control) = self.classify_control(trimmed) {
			if let Err(err) = self.execute_control(control, &mut out).await {
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
				self.note_simple_error(&not_over_pgwire(Dialect::SurrealQl), &mut out);
			}
			Dialect::PgSql => {
				let mut has_statement = false;
				for stmt in split_statements(remaining) {
					let trimmed_stmt = stmt.trim();
					if is_blank_pg_sql(trimmed_stmt) {
						continue;
					}
					has_statement = true;
					if let Some(tag) = crate::maintenance::command_tag(trimmed_stmt) {
						msg::write_command_complete(&mut out, tag);
						continue;
					}
					let expanded;
					let trimmed_stmt = match self.sql_prepared_step(trimmed_stmt, &mut out) {
						Ok(SqlPreparedStep::NotOurs) => trimmed_stmt,
						Ok(SqlPreparedStep::Done) => continue,
						Ok(SqlPreparedStep::Run(sql)) => {
							expanded = sql;
							expanded.as_str()
						}
						Err(err) => {
							self.note_simple_error(&err, &mut out);
							break;
						}
					};
					if let Some(control) = self.classify_control(trimmed_stmt) {
						if let Err(err) = self.execute_control(control, &mut out).await {
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
				// A string of only `;` and comments holds no statement.
				if !has_statement && out.is_empty() {
					msg::write_empty_query_response(&mut out);
				}
			}
			Dialect::MySql => {
				for stmt in split_statements(remaining) {
					let trimmed_stmt = stmt.trim();
					if trimmed_stmt.is_empty() {
						continue;
					}
					if let Some(control) = self.classify_control(trimmed_stmt) {
						if let Err(err) = self.execute_control(control, &mut out).await {
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

	/// Runs `PREPARE`, `DEALLOCATE` and the first half of `EXECUTE` against the connection's
	/// SQL-level prepared statements. An `EXECUTE` answers with the statement its arguments make,
	/// for the caller to run in its place.
	fn sql_prepared_step(
		&mut self,
		stmt: &str,
		out: &mut BytesMut,
	) -> Result<SqlPreparedStep, PgError> {
		let first = first_keyword(stmt);
		if !["prepare", "execute", "deallocate"].iter().any(|kw| first.eq_ignore_ascii_case(kw)) {
			return Ok(SqlPreparedStep::NotOurs);
		}
		let Ok(parsed) = surrealdb_pgsql::parse_statement(stmt) else {
			return Ok(SqlPreparedStep::NotOurs);
		};
		let source = |span: surrealdb_pgsql::Span| stmt.get(span.start as usize..span.end as usize);
		if let PgStatement::Prepare(prepare) = &parsed {
			let Some(text) = source(prepare.statement.span()) else {
				return Ok(SqlPreparedStep::NotOurs);
			};
			let preparable = matches!(
				prepare.statement.as_ref(),
				PgStatement::Select(_)
					| PgStatement::Insert(_)
					| PgStatement::Update(_)
					| PgStatement::Delete(_)
			);
			if !preparable {
				return Err(PgError::new(
					"42601",
					"a prepared statement is a SELECT, INSERT, UPDATE or DELETE",
				));
			}
			let types: Vec<String> = prepare
				.param_types
				.iter()
				.filter_map(|ty| source(ty.span).map(str::to_string))
				.collect();
			match self.sql_prepared.prepare(&prepare.name, text.to_string(), types) {
				Ok(()) => {}
				Err(PrepareError::Exists) => {
					return Err(PgError::new(
						"42P05",
						format!("prepared statement \"{}\" already exists", prepare.name),
					));
				}
				Err(PrepareError::Full) => {
					return Err(PgError::new("54000", "too many prepared statements"));
				}
				Err(PrepareError::TooManyParameters) => {
					return Err(PgError::new(
						"54000",
						"too many parameters in a prepared statement",
					));
				}
			}
			msg::write_command_complete(out, "PREPARE");
			Ok(SqlPreparedStep::Done)
		} else if let PgStatement::Deallocate(dealloc) = &parsed {
			match &dealloc.name {
				Some(name) => {
					if !self.sql_prepared.deallocate(name) {
						return Err(missing_sql_statement(name));
					}
				}
				None => self.sql_prepared.deallocate_all(),
			}
			msg::write_command_complete(out, "DEALLOCATE");
			Ok(SqlPreparedStep::Done)
		} else if let PgStatement::Execute(execute) = &parsed {
			let arguments: Vec<Argument> = execute
				.params
				.iter()
				.filter_map(|param| source(param.span()).map(Argument::new))
				.collect();
			match self.sql_prepared.bind(&execute.name, &arguments) {
				None => Err(missing_sql_statement(&execute.name)),
				Some(Ok(bound)) => Ok(SqlPreparedStep::Run(bound)),
				Some(Err(BindError::WrongCount)) => Err(PgError::new(
					"42601",
					format!(
						"wrong number of parameters for prepared statement \"{}\"",
						execute.name
					),
				)),
				Some(Err(BindError::Unbound(name))) => {
					Err(PgError::new("42703", format!("column \"{name}\" does not exist")))
				}
				Some(Err(BindError::TooLarge)) => {
					Err(PgError::new("54000", "the statement an EXECUTE makes is too large"))
				}
				Some(Err(BindError::Reused(n))) => Err(PgError::feature_not_supported(format!(
					"the argument for parameter ${n} must be a single value, as the statement reads it more than once"
				))),
			}
		} else {
			Ok(SqlPreparedStep::NotOurs)
		}
	}

	async fn run_pgsql(&mut self, sql: &str, out: &mut BytesMut) -> Result<()> {
		global_metrics().record_query(SqlDialect::Postgres);
		let mut descriptor = None;
		let mut wildcard_types = WildcardTypes::default();
		let mut write_kind = None;
		let mut returns_rows = true;
		if let Ok(stmts) = surrealdb_pgsql::parse_statements(sql) {
			if let Some(PgStatement::Copy(copy)) = stmts.first() {
				return self.run_copy(copy, out).await;
			}
			// Results are matched to statements only for a lone statement: when the
			// splitter and the parser disagree about where a statement ends, one
			// string holds several and each is answered as a query.
			if let [stmt] = stmts.as_slice()
				&& let Some((kind, rows)) = StatementKind::of_pg_command(stmt)
			{
				write_kind = Some(kind);
				returns_rows = rows;
			}
			let lone = stmts.len() == 1;
			let mut catalog = MemoryCatalog::new();
			if let Some(db) = &self.session.db {
				catalog.set_database(Some(db.clone()));
			}
			let policy = DialectPolicy::postgres();
			for stmt in &stmts {
				match self.describe_pgsql_write(stmt, &[]).await {
					Ok(Some(metadata)) => {
						descriptor = metadata.result;
						wildcard_types = WildcardTypes::of(
							&metadata.wildcard_types,
							&metadata.wildcard_declared,
						);
					}
					Ok(None) => {}
					Err(error) => {
						self.note_simple_error(&error, out);
						return Ok(());
					}
				}
				match stmt {
					PgStatement::Select(select) => {
						let catalog = match self
							.ds
							.pgsql_catalog(&self.session, self.transaction.as_ref(), select)
							.await
						{
							Ok(catalog) => catalog,
							Err(err) => {
								self.note_simple_error(&PgError::from(&err), out);
								return Ok(());
							}
						};

						match bind_select(select, &catalog, &policy, None) {
							Ok(bound) => {
								if descriptor.is_none() {
									descriptor = bound.describe_static(&catalog, &policy);
									wildcard_types = WildcardTypes::of(
										&bound.wildcard_column_types(&catalog),
										&bound.wildcard_declared_sql_types(&catalog),
									);
								}
							}
							Err(e) => {
								self.note_simple_error(
									&PgError::new(e.pg_sqlstate(), e.to_string()),
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
						for table in &truncate.tables {
							if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
								&table.parts,
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
					PgStatement::CreateIndex(idx) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&idx.table.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::CreateView(view) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&view.name.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
						for tr in &view.query.from {
							if let TableRef::Table {
								name,
								..
							} = tr && let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
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
					PgStatement::AlterTable(alter) => {
						if let Err(e) = surrealdb_sqlplan::resolve_pgsql_table_name(
							&alter.name.parts,
							Some(&catalog),
						) {
							self.note_simple_error(&PgError::undefined_table(e.to_string()), out);
							return Ok(());
						}
					}
					PgStatement::Copy(_)
					| PgStatement::CreateSchema(_)
					| PgStatement::CreateExtension(_)
					| PgStatement::CreateType(_)
					| PgStatement::AlterType(_)
					| PgStatement::CreateDomain(_)
					| PgStatement::AlterDomain(_)
					| PgStatement::CreateCompositeType(_)
					| PgStatement::Transaction(_)
					| PgStatement::Set(_)
					| PgStatement::Show(_)
					| PgStatement::Reset(_)
					| PgStatement::Explain(_)
					| PgStatement::Prepare(_)
					| PgStatement::Execute(_)
					| PgStatement::Deallocate(_)
					| PgStatement::CreatePropertyGraph(_)
					| PgStatement::DropPropertyGraph(_)
					| PgStatement::Empty => {}
				}
			}
			// The declared types of one statement's `*` columns say nothing of another's, and the
			// results of several are not matched to their statements, so only a lone statement's
			// result is typed from its declarations.
			if !lone {
				wildcard_types.clear();
			}
		}
		let cancel = self.arm_cancel();
		let req = QueryRequest::new(QuerySource::pgsql(sql), &self.session)
			.with_optional_transaction(self.transaction.clone())
			.with_cancel(cancel);
		match self.ds.run(req).await {
			Ok(results) => {
				// A command that returns no rows answers with one CommandComplete,
				// however many statements it lowers to (none, for one the engine
				// has nothing to run): the records its lowering answers with are an
				// implementation detail, and a driver reads the affected-row count
				// from the tag. Only INSERT, UPDATE and DELETE carry a count, and each
				// lowers to one statement whose records are the rows written; the
				// statements a DDL command lowers to change the schema and no tag
				// counts them, so the counts of several statements are never added
				// up. A statement that fails answers with its error alone.
				if let Some(kind) = write_kind
					&& !returns_rows
				{
					let mut affected = 0;
					for result in results {
						match result.result {
							Ok(value) => affected = affected_rows(&value),
							Err(err) => {
								self.note_simple_error(&PgError::from(&err), out);
								return Ok(());
							}
						}
					}
					msg::write_command_complete(out, &kind.command_tag(affected));
					return Ok(());
				}
				for result in results {
					match result.result {
						Ok(value) => {
							let value = self.present(value);
							if let Some(kind) = write_kind {
								let shape = if let Some(desc) = &descriptor {
									shape_result_with_descriptor(desc, value)
								} else {
									shape_result_declared(value, &wildcard_types)
								};
								if !self.emit_shape_with_kind(shape, kind, out).await? {
									break;
								}
								continue;
							}
							let ok = if let Some(desc) = &descriptor {
								self.emit_result_with_descriptor(desc, value, out).await?
							} else {
								let shape = shape_result_declared(value, &wildcard_types);
								self.emit_shape(shape, out).await?
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

	async fn run_copy(&mut self, copy: &CopyStmt, out: &mut BytesMut) -> Result<()> {
		match copy.direction {
			CopyDirection::FromStdin => self.run_copy_in(copy, out).await,
			CopyDirection::ToStdout => self.run_copy_out(copy, out).await,
		}
	}

	/// Runs `COPY ... FROM STDIN`. The data is cut into records as it arrives and
	/// inserted a batch at a time, each batch its own statement, so what is held
	/// in memory does not grow with the amount sent. Inside an explicit
	/// transaction the batches belong to it and roll back with it; otherwise each
	/// batch commits as it is inserted, so a COPY that fails or is aborted after
	/// the first batch leaves the earlier batches in place.
	async fn run_copy_in(&mut self, copy: &CopyStmt, out: &mut BytesMut) -> Result<()> {
		let (table_name, columns) = match &copy.target {
			CopyTarget::Table {
				table,
				columns,
			} => {
				let name = table.parts.last().cloned().unwrap_or_default();
				(name, columns.clone())
			}
			CopyTarget::Query(_) => {
				self.note_simple_error(
					&PgError::syntax("cannot COPY FROM STDIN into a query"),
					out,
				);
				return Ok(());
			}
		};

		let is_csv = matches!(copy.options.format, CopyFormat::Csv);
		let format = Format {
			csv: is_csv,
			delimiter: copy.options.delimiter.unwrap_or(if is_csv {
				b','
			} else {
				b'\t'
			}),
			quote: copy.options.quote.unwrap_or(b'"'),
			escape: copy.options.escape.unwrap_or(if is_csv {
				b'"'
			} else {
				b'\\'
			}),
			null: copy.options.null.clone().unwrap_or_else(|| {
				if is_csv {
					String::new()
				} else {
					"\\N".to_string()
				}
			}),
		};
		let has_header = copy.options.header.unwrap_or(false);

		if let Some(err) = copy_options_error(&format) {
			self.note_simple_error(&err, out);
			return Ok(());
		}

		// Send CopyInResponse to client
		msg::write_copy_in_response(out, 0, columns.len() as i16);
		self.flush(out).await?;

		let table = surrealdb_types::EscapeSqonIdent(&table_name).to_sql();
		let mut ingest = Ingest::new(format, table_name, table, columns, has_header);

		// The data is cut into records as it arrives and inserted in batches, so
		// what is held in memory does not grow with the amount sent. Once the COPY
		// has failed the rest of the stream is still read, and discarded, up to
		// CopyDone or CopyFail so the connection stays in step.
		let mut pending: Vec<u8> = Vec::new();
		let mut failed = false;
		loop {
			let Some((tag, payload)) = self.read_message(msg::MAX_MESSAGE_SIZE).await? else {
				return Ok(());
			};
			match msg::parse_frontend(tag, &payload)? {
				Frontend::CopyData(data) => {
					if failed || ingest.ended {
						continue;
					}
					pending.extend_from_slice(&data);
					if let Some(err) = self.copy_consume(&mut ingest, &mut pending, false).await {
						failed = true;
						pending = Vec::new();
						self.note_simple_error(&err, out);
						self.flush(out).await?;
					}
				}
				Frontend::CopyDone => {
					if !failed {
						let mut err = self.copy_consume(&mut ingest, &mut pending, true).await;
						if err.is_none() {
							err = self.copy_flush_batch(&mut ingest).await;
						}
						match err {
							Some(err) => self.note_simple_error(&err, out),
							None => {
								msg::write_command_complete(out, &format!("COPY {}", ingest.total))
							}
						}
					}
					return Ok(());
				}
				Frontend::CopyFail(err_msg) => {
					if !failed {
						self.note_simple_error(
							&PgError::new("57014", format!("COPY aborted by client: {err_msg}")),
							out,
						);
					}
					return Ok(());
				}
				Frontend::Sync => {}
				Frontend::Terminate => return Ok(()),
				other @ (Frontend::Query(_)
				| Frontend::Password(_)
				| Frontend::Parse {
					..
				}
				| Frontend::Bind {
					..
				}
				| Frontend::Describe {
					..
				}
				| Frontend::Execute {
					..
				}
				| Frontend::Close {
					..
				}
				| Frontend::Flush
				| Frontend::FunctionCall
				| Frontend::Unknown(_)) => {
					if !failed {
						self.note_simple_error(
							&PgError::protocol(format!("unexpected message {other:?} during COPY")),
							out,
						);
					}
					return Ok(());
				}
			}
		}
	}

	/// Parses and inserts the complete records in `pending`, dropping them from
	/// it. Unless `finish` is set, input stops being consumed once fewer than
	/// [`SLICE_BYTES`] of complete records remain, so records are handled in
	/// slices; `finish` takes everything, including a final record that has no
	/// newline. Returns the error that ends the COPY, if any.
	async fn copy_consume(
		&mut self,
		ingest: &mut Ingest,
		pending: &mut Vec<u8>,
		finish: bool,
	) -> Option<PgError> {
		let min = if finish {
			usize::MAX
		} else {
			SLICE_BYTES
		};
		let mut consumed = 0;
		let outcome = loop {
			if ingest.ended {
				consumed = pending.len();
				break None;
			}
			let rest = &pending[consumed..];
			if rest.is_empty() {
				break None;
			}
			let end = match ingest.format.scan(rest, min, &mut ingest.scan) {
				Scan::Boundary(end) => end,
				Scan::NeedMore if finish => rest.len(),
				Scan::NeedMore => break None,
				Scan::TooLong {
					complete,
				} => {
					// The records ahead of the overlong one are whole: take them first, since one
					// may be the end marker, which makes whatever follows it irrelevant.
					if complete > 0 {
						match ingest.format.parse_records(&rest[..complete], &mut ingest.ended) {
							Ok(rows) => {
								consumed += complete;
								if let Some(err) = self.copy_add_rows(ingest, rows).await {
									break Some(err);
								}
							}
							Err(err) => break Some(err.into_pg()),
						}
						if ingest.ended {
							continue;
						}
					}
					break Some(PgError::new(
						"54000",
						format!("COPY record exceeds the limit of {MAX_RECORD_BYTES} bytes"),
					));
				}
			};
			let rows = match ingest.format.parse_records(&rest[..end], &mut ingest.ended) {
				Ok(rows) => rows,
				Err(err) => break Some(err.into_pg()),
			};
			consumed += end;
			if let Some(err) = self.copy_add_rows(ingest, rows).await {
				break Some(err);
			}
		};
		pending.drain(..consumed);
		// A large message leaves its room behind; give it back once it is not needed.
		if pending.capacity() > 4 * (SLICE_BYTES + MAX_RECORD_BYTES) {
			pending.shrink_to(SLICE_BYTES + MAX_RECORD_BYTES);
		}
		outcome
	}

	/// Adds parsed rows to the batch, inserting a batch each time one fills up.
	async fn copy_add_rows(&mut self, ingest: &mut Ingest, rows: Vec<Row>) -> Option<PgError> {
		for row in rows {
			if ingest.header_pending {
				ingest.header_pending = false;
				if ingest.columns.is_empty() {
					ingest.columns = row.into_iter().map(|c| c.unwrap_or_default()).collect();
				}
				continue;
			}
			if !ingest.columns_settled {
				ingest.columns_settled = true;
				if ingest.columns.is_empty()
					&& let Some(err) = self.copy_discover_columns(ingest).await
				{
					return Some(err);
				}
				while ingest.columns.len() < row.len() {
					ingest.columns.push(format!("col_{}", ingest.columns.len() + 1));
				}
				ingest.key_lens =
					ingest.columns.iter().map(|c| surql_object_key(c).len()).collect();
				let links = match self
					.ds
					.record_link_columns(
						&self.session,
						self.transaction.as_ref(),
						&ingest.table_name,
					)
					.await
				{
					Ok(links) => links,
					Err(err) => return Some(PgError::from(&err)),
				};
				ingest.links = ingest
					.columns
					.iter()
					.map(|column| {
						links
							.iter()
							.find(|(name, _, _)| name == column)
							.map(|(_, to, admits_none)| (to.clone(), *admits_none))
					})
					.collect();
			}
			ingest.batch_bytes += row_cost(&row, &ingest.key_lens);
			ingest.batch.push(row);
			if (ingest.batch.len() >= BATCH_ROWS || ingest.batch_bytes >= BATCH_BYTES)
				&& let Some(err) = self.copy_flush_batch(ingest).await
			{
				return Some(err);
			}
		}
		None
	}

	/// Looks up the target table's column names, for a COPY that names none. A
	/// table with no columns on record leaves the names to be generated.
	async fn copy_discover_columns(&mut self, ingest: &mut Ingest) -> Option<PgError> {
		let col_query = copy_column_lookup_query(&ingest.table_name);
		let req = QueryRequest::new(QuerySource::pgsql(&col_query), &self.session)
			.with_optional_transaction(self.transaction.clone());
		let mut results = match self.ds.run(req).await {
			Ok(results) => results,
			Err(err) => return Some(PgError::from(&err)),
		};
		match results.pop().map(|r| r.result) {
			Some(Ok(Value::Array(arr))) => {
				for item in arr {
					if let Value::Object(obj) = item
						&& let Some(Value::String(c)) = obj.get("column_name")
					{
						ingest.columns.push(c.clone());
					}
				}
				None
			}
			Some(Err(err)) => Some(PgError::from(&err)),
			_ => None,
		}
	}

	/// Inserts the batched rows, as one statement, and empties the batch.
	async fn copy_flush_batch(&mut self, ingest: &mut Ingest) -> Option<PgError> {
		if ingest.batch.is_empty() {
			return None;
		}
		let rows = std::mem::take(&mut ingest.batch);
		ingest.batch_bytes = 0;
		let mut q = format!("INSERT INTO {} [", ingest.table);
		for (i, row) in rows.iter().enumerate() {
			if i > 0 {
				q.push_str(", ");
			}
			q.push('{');
			for (j, val) in row.iter().enumerate() {
				if j > 0 {
					q.push_str(", ");
				}
				let col = ingest.columns.get(j).map(|s| s.as_str()).unwrap_or("col");
				let literal = match ingest.links.get(j) {
					Some(Some((target, admits_none))) => {
						link_to_surrealql(val, target, *admits_none)
					}
					_ => value_to_surrealql(val),
				};
				q.push_str(&format!("{}: {}", surql_object_key(col), literal));
			}
			q.push('}');
		}
		q.push_str("];");
		let cancel = self.arm_cancel();
		let req = QueryRequest::new(QuerySource::text(&q), &self.session)
			.with_optional_transaction(self.transaction.clone())
			.with_cancel(cancel);
		match self.ds.run(req).await {
			Ok(results) => {
				for res in results {
					if let Err(err) = res.result {
						return Some(PgError::from(&err));
					}
				}
			}
			Err(err) => return Some(PgError::from(&err)),
		}
		ingest.total += rows.len();
		None
	}

	async fn run_copy_out(&mut self, copy: &CopyStmt, out: &mut BytesMut) -> Result<()> {
		let (sql, select_stmt) = match &copy.target {
			CopyTarget::Query(query) => (format!("{query};"), Some(query.clone())),
			CopyTarget::Table {
				table,
				columns,
			} => {
				let tbl_name = pg_ident(table.parts.last().map(String::as_str).unwrap_or_default());
				let sql = if columns.is_empty() {
					format!("SELECT * FROM {tbl_name};")
				} else {
					let cols = columns.iter().map(|c| pg_ident(c)).collect::<Vec<_>>().join(", ");
					format!("SELECT {cols} FROM {tbl_name};")
				};
				let stmt = if let Ok(stmts) = surrealdb_pgsql::parse_statements(&sql) {
					if let Some(PgStatement::Select(s)) = stmts.into_iter().next() {
						Some(s)
					} else {
						None
					}
				} else {
					None
				};
				(sql, stmt)
			}
		};

		let is_csv = matches!(copy.options.format, CopyFormat::Csv);
		let delimiter = copy.options.delimiter.unwrap_or(if is_csv {
			b','
		} else {
			b'\t'
		});
		let quote = copy.options.quote.unwrap_or(b'"');
		let escape = copy.options.escape.unwrap_or(if is_csv {
			b'"'
		} else {
			b'\\'
		});
		let null_str = copy.options.null.as_deref().unwrap_or(if is_csv {
			""
		} else {
			"\\N"
		});
		let has_header = copy.options.header.unwrap_or(false);

		let cancel = self.arm_cancel();
		let req = QueryRequest::new(QuerySource::pgsql(&sql), &self.session)
			.with_optional_transaction(self.transaction.clone())
			.with_cancel(cancel);

		let results = match self.ds.run(req).await {
			Ok(r) => r,
			Err(err) => {
				self.note_simple_error(&PgError::from(&err), out);
				return Ok(());
			}
		};

		let Some(first_res) = results.into_iter().next() else {
			msg::write_copy_out_response(out, 0, 0);
			msg::write_copy_done(out);
			msg::write_command_complete(out, "COPY 0");
			return Ok(());
		};

		// The query is lowered by the PostgreSQL front end whatever the connection's dialect, so
		// its record ids are shown as keys throughout.
		let value = match first_res.result {
			Ok(v) => surrealdb_sqlplan::show_record_ids_as_keys(v),
			Err(err) => {
				self.note_simple_error(&PgError::from(&err), out);
				return Ok(());
			}
		};

		let mut descriptor = None;
		if let Some(s) = &select_stmt {
			let mut catalog = MemoryCatalog::new();
			if let Some(db) = &self.session.db {
				catalog.set_database(Some(db.clone()));
			}
			let policy = DialectPolicy::postgres();
			if let Ok(bound) = bind_select(s, &catalog, &policy, None) {
				descriptor = bound.describe_static(&catalog, &policy);
			}
		}

		let shape = if let Some(desc) = &descriptor {
			shape_result_with_descriptor(desc, value)
		} else {
			shape_result(value)
		};

		let num_cols = shape.columns.len() as i16;
		msg::write_copy_out_response(out, 0, num_cols);

		// Header row
		if is_csv && has_header {
			let header_line = shape
				.columns
				.iter()
				.map(|col| format_csv_cell(&col.name, delimiter, quote, escape, null_str))
				.collect::<Vec<_>>()
				.join(&(delimiter as char).to_string())
				+ "\n";
			msg::write_copy_data(out, header_line.as_bytes());
		}

		// Data rows
		let row_count = shape.rows.len();
		for row in shape.rows {
			let line = if is_csv {
				let mut cells = Vec::with_capacity(row.len());
				for (i, cell) in row.into_iter().enumerate() {
					let ty = shape.columns.get(i).map(|c| &c.ty).unwrap_or(&PgType::Text);
					match cell {
						None => cells.push(null_str.to_string()),
						Some(v) => {
							let raw = crate::encode::encode_text(v, ty)
								.map(|b| String::from_utf8_lossy(&b).into_owned())
								.unwrap_or_default();
							cells.push(format_csv_cell(&raw, delimiter, quote, escape, null_str));
						}
					}
				}
				cells.join(&(delimiter as char).to_string()) + "\n"
			} else {
				let mut cells = Vec::with_capacity(row.len());
				for (i, cell) in row.into_iter().enumerate() {
					let ty = shape.columns.get(i).map(|c| &c.ty).unwrap_or(&PgType::Text);
					match cell {
						None => cells.push(null_str.to_string()),
						Some(v) => {
							let raw = crate::encode::encode_text(v, ty)
								.map(|b| String::from_utf8_lossy(&b).into_owned())
								.unwrap_or_default();
							cells.push(format_text_cell(&raw, delimiter));
						}
					}
				}
				cells.join(&(delimiter as char).to_string()) + "\n"
			};
			msg::write_copy_data(out, line.as_bytes());
			if out.len() >= FLUSH_THRESHOLD {
				self.flush(out).await?;
			}
		}

		msg::write_copy_done(out);
		msg::write_command_complete(out, &format!("COPY {row_count}"));
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
							let value = self.present(value);
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
			TxnOp::Begin {
				read_only,
				isolation,
			} => {
				if self.transaction.is_some() {
					msg::write_notice(out, "25001", "there is already a transaction in progress");
				} else {
					// A `BEGIN` that names no level takes the session's default.
					let isolation = match isolation {
						Some(isolation) => Some(isolation),
						None => match self.gucs.get("default_transaction_isolation") {
							Some(setting) => Some(isolation_setting(setting)?),
							None => None,
						},
					};
					let (tx, _) = self
						.ds
						.begin_transaction(
							isolation,
							read_only.then_some(ReadOnlyOptions {
								max_staleness: std::time::Duration::ZERO,
								strict: false,
							}),
						)
						.await
						.map_err(|err| {
							if is_isolation_not_supported(&err) {
								PgError::feature_not_supported(err.to_string())
							} else {
								PgError::internal(err.to_string())
							}
						})?;
					self.transaction = Some(Arc::new(tx));
					self.txn_failed = false;
					self.txn_session_snapshot = Some(self.session.clone());
					self.txn_has_local_session_override = false;
					self.txn_lets.clear();
					self.savepoints.clear();
					self.savepoint_sessions.clear();
					self.gucs.begin_transaction();
				}
				msg::write_command_complete(out, "BEGIN");
			}
			TxnOp::Commit => {
				self.savepoints.clear();
				self.savepoint_sessions.clear();
				self.portals.clear();
				self.portal_bytes = 0;
				match self.transaction.take() {
					// Committing an aborted transaction rolls it back; report so.
					Some(tx) if self.txn_failed => {
						self.txn_failed = false;
						self.txn_lets.clear();
						if let Some(snap) = self.txn_session_snapshot.take() {
							self.session = snap;
						}
						self.txn_has_local_session_override = false;
						let _ = tx.cancel().await;
						let changed = self.gucs.rollback_transaction();
						for (canon, val) in changed {
							msg::write_parameter_status(out, canon, &val);
						}
						msg::write_command_complete(out, "ROLLBACK");
					}
					Some(tx) => {
						// The transaction was already taken, so on failure
						// ready_status() reports 'I' and the caller emits only the
						// error (no trailing CommandComplete), keeping the sequence valid.
						if self.txn_has_local_session_override {
							if let Some(snap) = self.txn_session_snapshot.take() {
								self.session = snap;
							}
						} else {
							self.txn_session_snapshot = None;
						}
						self.txn_has_local_session_override = false;
						let lets = std::mem::take(&mut self.txn_lets);
						self.gucs.commit_transaction();
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
				}
			}
			TxnOp::Rollback => {
				self.savepoints.clear();
				self.savepoint_sessions.clear();
				self.portals.clear();
				self.portal_bytes = 0;
				if let Some(snap) = self.txn_session_snapshot.take() {
					self.session = snap;
				}
				self.txn_has_local_session_override = false;
				self.txn_lets.clear();
				match self.transaction.take() {
					Some(tx) => {
						let _ = tx.cancel().await;
						let changed = self.gucs.rollback_transaction();
						for (canon, val) in changed {
							msg::write_parameter_status(out, canon, &val);
						}
					}
					None => {
						msg::write_notice(out, "25P01", "there is no transaction in progress");
					}
				}
				self.txn_failed = false;
				msg::write_command_complete(out, "ROLLBACK");
			}

			TxnOp::Savepoint(name) => {
				let Some(tx) = self.transaction.as_ref() else {
					return Err(PgError::no_active_transaction(
						"SAVEPOINT can only be used in transaction blocks",
					));
				};
				if self.txn_failed {
					return Err(PgError::in_failed_transaction(
						"current transaction is aborted, commands ignored until end of transaction block",
					));
				}
				tx.new_save_point().await.map_err(|err| PgError::internal(err.to_string()))?;
				self.gucs.savepoint(&name);
				self.savepoints.push(name.clone());
				self.savepoint_sessions.push((name, self.session.clone(), self.txn_lets.len()));
				msg::write_command_complete(out, "SAVEPOINT");
			}
			TxnOp::ReleaseSavepoint(name) => {
				let Some(tx) = self.transaction.as_ref() else {
					return Err(PgError::no_active_transaction(
						"RELEASE SAVEPOINT can only be used in transaction blocks",
					));
				};
				if self.txn_failed {
					return Err(PgError::in_failed_transaction(
						"current transaction is aborted, commands ignored until end of transaction block",
					));
				}
				let pos = self.savepoints.iter().rposition(|s| s.eq_ignore_ascii_case(&name));
				let Some(idx) = pos else {
					return Err(PgError::invalid_savepoint(format!(
						"savepoint \"{name}\" does not exist"
					)));
				};
				while self.savepoints.len() > idx {
					tx.release_last_save_point()
						.await
						.map_err(|err| PgError::internal(err.to_string()))?;
					self.savepoints.pop();
				}
				if let Some(s_idx) = self
					.savepoint_sessions
					.iter()
					.rposition(|(s, _, _)| s.eq_ignore_ascii_case(&name))
				{
					self.savepoint_sessions.truncate(s_idx);
				}
				let _ = self.gucs.release_savepoint(&name);
				msg::write_command_complete(out, "RELEASE");
			}
			TxnOp::RollbackSavepoint(name) => {
				let Some(tx) = self.transaction.as_ref() else {
					return Err(PgError::no_active_transaction(
						"ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
					));
				};
				let pos = self.savepoints.iter().rposition(|s| s.eq_ignore_ascii_case(&name));
				let Some(idx) = pos else {
					return Err(PgError::invalid_savepoint(format!(
						"savepoint \"{name}\" does not exist"
					)));
				};
				while self.savepoints.len() > idx + 1 {
					tx.rollback_to_save_point()
						.await
						.map_err(|err| PgError::internal(err.to_string()))?;
					self.savepoints.pop();
				}
				tx.rollback_to_save_point()
					.await
					.map_err(|err| PgError::internal(err.to_string()))?;
				tx.new_save_point().await.map_err(|err| PgError::internal(err.to_string()))?;
				if let Some(s_idx) = self
					.savepoint_sessions
					.iter()
					.rposition(|(s, _, _)| s.eq_ignore_ascii_case(&name))
				{
					self.session = self.savepoint_sessions[s_idx].1.clone();
					// The LETs made after the savepoint are undone along with its writes.
					self.txn_lets.truncate(self.savepoint_sessions[s_idx].2);
					self.savepoint_sessions.truncate(s_idx + 1);
				}
				if let Ok(changed) = self.gucs.rollback_to_savepoint(&name) {
					for (canon, val) in changed {
						msg::write_parameter_status(out, canon, &val);
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
	///
	/// This protocol speaks PostgreSQL SQL, and GQL on request; SurrealQL and
	/// MySQL are reachable over their own endpoints and are refused here.
	fn select_dialect(&mut self, dialect: Dialect) -> Result<(), PgError> {
		if matches!(dialect, Dialect::SurrealQl | Dialect::MySql) {
			return Err(not_over_pgwire(dialect));
		}
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
	/// PostgreSQL. `RouteTarget::Postgres` stays connection-scoped by contrast,
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
	/// PostgreSQL and MySQL dialects every `SET` is intercepted
	/// (top-level `SET` is configuration). In the GQL dialect `SET` is a mutation
	/// clause, so only what a driver means as configuration is intercepted — the
	/// `dialect` control knob, the known GUC allowlist, and unmodelled `SET` syntax
	/// (empty name, e.g. `SET TIME ZONE ...`); a real `property = value` mutation reaches the
	/// executor.
	fn should_intercept_set(&self, set: &SetStatement) -> bool {
		self.dialect == Dialect::PgSql
			|| self.dialect == Dialect::MySql
			|| set.name.is_empty()
			|| set.name.eq_ignore_ascii_case("dialect")
			|| set.name.eq_ignore_ascii_case("role")
			|| set.name.eq_ignore_ascii_case("request.jwt.claims")
			|| set.name.to_ascii_lowercase().starts_with("request.jwt.claim.")
			|| is_no_op_guc(&set.name)
	}

	/// Execute any control statement.
	async fn execute_control(
		&mut self,
		control: Control,
		out: &mut BytesMut,
	) -> Result<(), PgError> {
		match control {
			Control::Transaction(op) => self.handle_transaction_control(op, out).await,
			Control::Set(set) => self.handle_set(set, out),
			Control::Show(param) => self.handle_show(&param, out),
			Control::Reset(param) => self.handle_reset(&param, out),
			Control::Advisory(call) => {
				self.handle_advisory(call, &Variables::new(), &[], true, out).await
			}
		}
	}

	/// Classify a query into a control statement if applicable.
	fn classify_control(&self, query: &str) -> Option<Control> {
		if let Some(op) = transaction_control(query) {
			return Some(Control::Transaction(op));
		}
		if let Some(show_param) = parse_show(query) {
			return Some(Control::Show(show_param));
		}
		if let Some(reset_param) = parse_reset(query) {
			return Some(Control::Reset(reset_param));
		}
		if self.dialect == Dialect::PgSql
			&& let Some(call) = advisory::parse_call(&strip_sql_comments(query))
		{
			return Some(Control::Advisory(call));
		}
		if let Some(set) = parse_set(query)
			&& self.should_intercept_set(&set)
		{
			return Some(Control::Set(set));
		}
		None
	}

	/// Run an advisory lock function as a whole statement and answer with its one-row result.
	///
	/// `vars` are the statement's bound parameters and `formats` the result formats;
	/// `describe` is whether to precede the row with its RowDescription, as the simple
	/// protocol does. Taking a lock waits for other sessions without holding any datastore
	/// transaction; the wait ends, without the lock, on a CancelRequest, on server shutdown
	/// and when the client closes the connection. A NULL key returns NULL and takes nothing,
	/// as the strict PostgreSQL functions do.
	async fn handle_advisory(
		&mut self,
		call: AdvisoryCall,
		vars: &Variables,
		formats: &[i16],
		describe: bool,
		out: &mut BytesMut,
	) -> Result<(), PgError> {
		self.check_current_dialect()?;
		if self.txn_failed {
			return Err(PgError::in_failed_transaction(
				"current transaction is aborted, commands ignored until end of transaction block",
			));
		}
		let key = advisory_key(&call, vars)?;
		let session = self.advisory.id();
		let locks = self.advisory.locks();
		let outcome = match (call.op, key) {
			(AdvisoryOp::UnlockAll, _) => {
				locks.unlock_all(session);
				Some(false)
			}
			(_, None) => None,
			(AdvisoryOp::Lock, Some(key)) => {
				let cancel = self.arm_cancel().token();
				let shutdown = self.shutdown.clone();
				let stream = &self.stream;
				let abort = async {
					tokio::select! {
						() = cancel.cancelled() => {}
						() = shutdown.cancelled() => {}
						() = peer_closed(stream) => {}
					}
				};
				match locks.lock(session, key, abort).await? {
					Ok(()) => Some(false),
					Err(Interrupted::Aborted) => {
						return Err(if shutdown.is_cancelled() {
							PgError::new(
								"57P01",
								"terminating connection due to administrator command",
							)
							.fatal()
						} else {
							PgError::query_canceled("canceling statement due to user request")
						});
					}
				}
			}
			(AdvisoryOp::TryLock, Some(key)) => Some(locks.try_lock(session, key)?),
			(AdvisoryOp::Unlock, Some(key)) => Some(locks.unlock(session, key)),
		};
		let column = advisory_columns(&call);
		if describe {
			msg::write_row_description(out, &column, |_| FORMAT_TEXT);
		}
		let cell = match (outcome, call.op.returns_void()) {
			// A void result is the empty string, but a NULL key makes it NULL.
			(Some(_), true) => {
				Some(encode_cell(Value::None, &PgType::Void, format_at(formats, 0))?)
			}
			(Some(value), false) => {
				Some(encode_cell(Value::Bool(value), &PgType::Bool, format_at(formats, 0))?)
			}
			(None, _) => None,
		};
		msg::write_data_row(out, &[cell]);
		msg::write_command_complete(out, "SELECT 1");
		Ok(())
	}

	/// Handle an intercepted `SET`: switch dialect, update GucStore, and emit ParameterStatus if
	/// needed.
	fn handle_set(&mut self, set: SetStatement, out: &mut BytesMut) -> Result<(), PgError> {
		let SetStatement {
			name,
			value,
			is_local,
		} = set;
		if name.eq_ignore_ascii_case("default_transaction_isolation")
			|| name.eq_ignore_ascii_case("transaction_isolation")
		{
			// A level the connection cannot give is refused here, not stored and
			// ignored.
			isolation_setting(&value)?;
		}
		if name.is_empty() {
			// The message is the key the unsupported-feature counter and the log line carry, so it
			// holds none of the client's statement.
			return Err(PgError::feature_not_supported(
				"this form of SET statement is not supported",
			));
		}
		if name.eq_ignore_ascii_case("dialect") {
			let dialect = parse_dialect(&value)
				.ok_or_else(|| PgError::invalid_text(format!("unrecognized dialect: {value}")))?;
			self.select_dialect(dialect)?;
			msg::write_command_complete(out, "SET");
			return Ok(());
		}

		if name.eq_ignore_ascii_case("role") {
			self.apply_role_setting(&value, is_local)?;
			msg::write_command_complete(out, "SET");
			return Ok(());
		}

		if name.eq_ignore_ascii_case("request.jwt.claims")
			|| name.to_ascii_lowercase().starts_with("request.jwt.claim.")
		{
			self.apply_claims_setting(&name, &value, is_local)?;
			msg::write_command_complete(out, "SET");
			return Ok(());
		}

		let in_txn = self.transaction.is_some();
		if is_local && !in_txn {
			msg::write_notice(out, "01000", "SET LOCAL will only have effect in transactions");
		}
		if self.gucs.is_ignored(&name) {
			msg::write_notice(out, "01000", &format!("parameter \"{name}\" is ignored"));
		}
		let changed_status = self.gucs.set(&name, &value, is_local, in_txn)?;
		if let Some((canon, new_val)) = changed_status {
			msg::write_parameter_status(out, canon, &new_val);
		}
		msg::write_command_complete(out, "SET");
		Ok(())
	}

	/// Handle `SHOW <param>` and `SHOW ALL`.
	fn handle_show(&mut self, param: &str, out: &mut BytesMut) -> Result<(), PgError> {
		let result = self.gucs.show(param)?;
		match result {
			ShowResult::Single {
				name: _,
				value,
			} => {
				self.describe_show(param, out)?;
				msg::write_data_row(out, &[Some(value.into_bytes())]);
				msg::write_command_complete(out, "SHOW");
			}
			ShowResult::All(entries) => {
				self.describe_show(param, out)?;
				for entry in entries {
					msg::write_data_row(
						out,
						&[
							Some(entry.name.into_bytes()),
							Some(entry.current_value.into_bytes()),
							Some(entry.description.into_bytes()),
						],
					);
				}
				msg::write_command_complete(out, "SHOW");
			}
		}
		Ok(())
	}

	/// Describe the row format of a `SHOW` statement.
	fn describe_show(&self, param: &str, out: &mut BytesMut) -> Result<(), PgError> {
		if param.eq_ignore_ascii_case("ALL") {
			let columns = vec![
				PgColumn {
					typmod: -1,
					name: "name".to_string(),
					ty: PgType::Text,
				},
				PgColumn {
					typmod: -1,
					name: "setting".to_string(),
					ty: PgType::Text,
				},
				PgColumn {
					typmod: -1,
					name: "description".to_string(),
					ty: PgType::Text,
				},
			];
			msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
		} else {
			let norm = normalize_name(param);
			let columns = vec![PgColumn {
				typmod: -1,
				name: norm,
				ty: PgType::Text,
			}];
			msg::write_row_description(out, &columns, |_| FORMAT_TEXT);
		}
		Ok(())
	}

	/// Handle `RESET <param>` and `RESET ALL`.
	fn handle_reset(&mut self, param: &str, out: &mut BytesMut) -> Result<(), PgError> {
		if param.eq_ignore_ascii_case("role") {
			self.apply_role_setting("none", false)?;
		} else if (param.eq_ignore_ascii_case("request.jwt.claims")
			|| param.to_ascii_lowercase().starts_with("request.jwt.claim."))
			&& let Some(base) = &self.base_session
		{
			self.session.tk = base.tk.clone();
			self.session.rd = base.rd.clone();
			self.session.au = Arc::clone(&base.au);
		}
		let changed_status = self.gucs.reset(param)?;
		for (canon, new_val) in changed_status {
			msg::write_parameter_status(out, canon, &new_val);
		}
		msg::write_command_complete(out, "RESET");
		Ok(())
	}

	fn apply_role_setting(&mut self, role_val: &str, is_local: bool) -> Result<(), PgError> {
		let clean = role_val.trim().trim_matches('\'').trim_matches('"').trim();
		let in_txn = self.transaction.is_some();
		if is_local {
			self.txn_has_local_session_override = true;
		}

		let base_au = self
			.base_session
			.as_ref()
			.map(|b| Arc::clone(&b.au))
			.unwrap_or_else(|| Arc::clone(&self.session.au));

		if clean.eq_ignore_ascii_case("NONE")
			|| clean.eq_ignore_ascii_case("DEFAULT")
			|| clean.eq_ignore_ascii_case("RESET")
			|| clean.is_empty()
		{
			self.session.au = base_au;
			let _ = self.gucs.set("role", "none", is_local, in_txn)?;
			return Ok(());
		}

		if clean.eq_ignore_ascii_case("anon") || clean.eq_ignore_ascii_case("anonymous") {
			self.session.au =
				Arc::new(Auth::new(Actor::new("anon".to_string(), vec![], Level::No)));
			let _ = self.gucs.set("role", "anon", is_local, in_txn)?;
			return Ok(());
		}

		if clean.eq_ignore_ascii_case("authenticated") {
			if matches!(self.session.au.level(), Level::No) || matches!(base_au.level(), Level::No)
			{
				return Err(PgError::insufficient_privilege(
					"permission denied to set role \"authenticated\": cannot elevate unauthenticated session",
				));
			}
			self.session.au = base_au;
			let _ = self.gucs.set("role", "authenticated", is_local, in_txn)?;
			return Ok(());
		}

		if let Ok(target_role) = clean.parse::<Role>() {
			if matches!(self.session.au.level(), Level::No)
				|| matches!(base_au.level(), Level::No | Level::Record(..))
			{
				return Err(PgError::insufficient_privilege(format!(
					"permission denied to set role \"{clean}\": privilege elevation not permitted",
				)));
			}
			let max_base_role = if matches!(base_au.level(), Level::Root) {
				Some(Role::Owner)
			} else {
				base_au.max_role()
			};

			if let Some(max_role) = max_base_role
				&& target_role <= max_role
			{
				let actor_id = self.session.au.id().to_string();
				self.session.au = Arc::new(Auth::new(Actor::new(
					actor_id,
					vec![target_role],
					self.session.au.level().clone(),
				)));
				let _ = self.gucs.set("role", clean, is_local, in_txn)?;
				return Ok(());
			}
			return Err(PgError::insufficient_privilege(format!(
				"permission denied to set role \"{clean}\": privilege elevation not permitted",
			)));
		}

		Err(PgError::insufficient_privilege(format!("permission denied to set role \"{clean}\"",)))
	}

	fn apply_claims_setting(
		&mut self,
		name: &str,
		val: &str,
		is_local: bool,
	) -> Result<(), PgError> {
		let clean = val.trim().trim_matches('\'').trim_matches('"').trim();
		// GUC names are case-insensitive; the claim checks below match lower-case names.
		let name = name.to_ascii_lowercase();
		let name = name.as_str();
		let in_txn = self.transaction.is_some();
		if is_local {
			self.txn_has_local_session_override = true;
		}
		let base_au = self
			.base_session
			.as_ref()
			.map(|b| Arc::clone(&b.au))
			.unwrap_or_else(|| Arc::clone(&self.session.au));
		let base_rd = self
			.base_session
			.as_ref()
			.and_then(|b| b.rd.clone())
			.or_else(|| self.session.rd.clone());

		let is_system_session =
			matches!(base_au.level(), Level::Root | Level::Namespace(..) | Level::Database(..))
				&& !matches!(self.session.au.level(), Level::No);

		if name.eq_ignore_ascii_case("request.jwt.claims") {
			let parsed_val: serde_json::Value = serde_json::from_str(clean).map_err(|err| {
				PgError::invalid_parameter_value(format!(
					"invalid JSON in request.jwt.claims: {err}"
				))
			})?;

			let serde_json::Value::Object(claims_obj) = &parsed_val else {
				return Err(PgError::invalid_parameter_value(
					"request.jwt.claims must be a JSON object",
				));
			};

			if !is_system_session {
				for key in claims_obj.keys() {
					if !key.eq_ignore_ascii_case("role")
						&& !key.eq_ignore_ascii_case("sub")
						&& !key.eq_ignore_ascii_case("id")
					{
						return Err(PgError::insufficient_privilege(format!(
							"permission denied: non-system session cannot set JWT claim \"{key}\""
						)));
					}
				}
			}

			let mut narrowed_to_anon = false;

			// Role claim check
			if let Some(role_val) = claims_obj.get("role") {
				let Some(role) = role_val.as_str() else {
					return Err(PgError::insufficient_privilege(
						"permission denied: invalid role claim in request.jwt.claims",
					));
				};
				if role.eq_ignore_ascii_case("anon") || role.eq_ignore_ascii_case("anonymous") {
					self.session.au =
						Arc::new(Auth::new(Actor::new("anon".to_string(), vec![], Level::No)));
					self.session.rd = None;
					self.session.tk = None;
					narrowed_to_anon = true;
				} else if !is_system_session {
					return Err(PgError::insufficient_privilege(
						"permission denied: non-system session cannot elevate via request.jwt.claims",
					));
				}
			}

			// Sub claim check
			let sub_val = claims_obj.get("sub").or_else(|| claims_obj.get("id"));
			if let Some(sub_val) = sub_val {
				let sub_str = match sub_val {
					serde_json::Value::String(s) => s.clone(),
					other @ (serde_json::Value::Null
					| serde_json::Value::Bool(_)
					| serde_json::Value::Number(_)
					| serde_json::Value::Array(_)
					| serde_json::Value::Object(_)) => other.to_string(),
				};
				if matches!(self.session.au.level(), Level::No)
					|| matches!(base_au.level(), Level::No)
				{
					return Err(PgError::insufficient_privilege(
						"permission denied: unauthenticated session cannot set identity via request.jwt.claims",
					));
				}
				if let Some(current_rd) = &base_rd {
					if !subject_names_record(current_rd, &sub_str) {
						return Err(PgError::insufficient_privilege(
							"permission denied: cannot impersonate a different user via request.jwt.claims",
						));
					}
				} else if is_system_session {
					let ns = self.session.ns.clone().unwrap_or_else(|| "test".to_string());
					let db = self.session.db.clone().unwrap_or_else(|| "test".to_string());
					let ac = self.session.ac.clone().unwrap_or_else(|| "jwt".to_string());
					let rid = match syn::record_id(&sub_str) {
						Ok(rid) => rid,
						Err(_) => surrealdb_types::RecordId::new("user", sub_str.clone()),
					};
					self.session.rd = Some(Value::RecordId(rid.clone()));
					self.session.au = Arc::new(Auth::for_record(rid.to_sql(), &ns, &db, &ac));
				}
			}

			if is_system_session && !narrowed_to_anon {
				self.session.tk = Some(parsed_val.into_value());
			}
			let _ = self.gucs.set(name, clean, is_local, in_txn)?;
			return Ok(());
		}

		if let Some(claim_key) = name.strip_prefix("request.jwt.claim.") {
			let mut narrowed_to_anon = false;
			if claim_key.eq_ignore_ascii_case("role") {
				if clean.eq_ignore_ascii_case("anon") || clean.eq_ignore_ascii_case("anonymous") {
					self.session.au =
						Arc::new(Auth::new(Actor::new("anon".to_string(), vec![], Level::No)));
					self.session.rd = None;
					self.session.tk = None;
					narrowed_to_anon = true;
				} else if !is_system_session {
					return Err(PgError::insufficient_privilege(
						"permission denied: non-system session cannot elevate via request.jwt.claim.role",
					));
				}
			} else if claim_key.eq_ignore_ascii_case("sub") || claim_key.eq_ignore_ascii_case("id")
			{
				if matches!(self.session.au.level(), Level::No)
					|| matches!(base_au.level(), Level::No)
				{
					return Err(PgError::insufficient_privilege(
						"permission denied: unauthenticated session cannot set identity via request.jwt.claim.sub",
					));
				}
				if let Some(current_rd) = &base_rd {
					if !subject_names_record(current_rd, clean) {
						return Err(PgError::insufficient_privilege(
							"permission denied: cannot impersonate a different user via request.jwt.claim.sub",
						));
					}
				} else if is_system_session {
					let ns = self.session.ns.clone().unwrap_or_else(|| "test".to_string());
					let db = self.session.db.clone().unwrap_or_else(|| "test".to_string());
					let ac = self.session.ac.clone().unwrap_or_else(|| "jwt".to_string());
					let rid = match syn::record_id(clean) {
						Ok(rid) => rid,
						Err(_) => surrealdb_types::RecordId::new("user", clean.to_string()),
					};
					self.session.rd = Some(Value::RecordId(rid.clone()));
					self.session.au = Arc::new(Auth::for_record(rid.to_sql(), &ns, &db, &ac));
				}
			} else if !is_system_session {
				return Err(PgError::insufficient_privilege(format!(
					"permission denied: non-system session cannot set request.jwt.claim.{claim_key}"
				)));
			}

			if is_system_session && !narrowed_to_anon {
				let mut obj = match self.session.tk.take() {
					Some(Value::Object(map)) => map,
					_ => surrealdb_types::Object::default(),
				};
				obj.insert(claim_key.to_string(), Value::String(clean.to_string()));
				self.session.tk = Some(Value::Object(obj));
			}
			let _ = self.gucs.set(name, clean, is_local, in_txn)?;
			return Ok(());
		}

		let _ = self.gucs.set(name, clean, is_local, in_txn)?;
		Ok(())
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
		self.secured || matches!(self.stream, Either::Right(_))
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
		record_unsupported(err);
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
		record_unsupported(err);
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
		self.emit_shape_with_kind(shape, StatementKind::Select, out).await
	}

	async fn emit_shape_with_kind(
		&mut self,
		shape: ResultShape,
		kind: StatementKind,
		out: &mut BytesMut,
	) -> Result<bool> {
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
		msg::write_command_complete(out, &kind.command_tag(count));
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

/// The parameter count and type oids of a prepared advisory call: the oid the client declared
/// where it declared one, and the key's type otherwise.
fn advisory_parameters(
	call: &AdvisoryCall,
	declared: &[i32],
) -> Result<(usize, Vec<i32>), PgError> {
	let count = call.param_count().max(declared.len());
	if count > MAX_PARAMS {
		return Err(PgError::protocol(format!(
			"statement references {count} parameters, exceeding the maximum of {MAX_PARAMS}"
		)));
	}
	let mut types = vec![0; count];
	for (slot, oid) in types.iter_mut().zip(declared) {
		*slot = *oid;
	}
	for arg in &call.args {
		if let AdvisoryArg::Param(n) = arg
			&& types[*n - 1] == 0
		{
			types[*n - 1] = call.arg_oid();
		}
	}
	Ok((count, types))
}

/// The single column an advisory call returns.
fn advisory_columns(call: &AdvisoryCall) -> [PgColumn; 1] {
	[PgColumn {
		typmod: -1,
		name: call.column.clone(),
		ty: if call.op.returns_void() {
			PgType::Void
		} else {
			PgType::Bool
		},
	}]
}

/// The lock key an advisory call names, with `$n` read from `vars`, or `None` when an argument
/// is NULL (or for the call that takes no key). The two-key form takes two `int4`s.
fn advisory_key(call: &AdvisoryCall, vars: &Variables) -> Result<Option<AdvisoryKey>, PgError> {
	let mut values = Vec::with_capacity(call.args.len());
	for arg in &call.args {
		let value = match arg {
			AdvisoryArg::Literal(value) => *value,
			AdvisoryArg::Null => return Ok(None),
			AdvisoryArg::Param(n) => match vars.get(&surrealdb_types::positional_param_name(*n)) {
				Some(Value::Number(surrealdb_types::Number::Int(value))) => *value,
				Some(Value::Null | Value::None) => return Ok(None),
				Some(_) => {
					return Err(PgError::invalid_parameter_value(format!(
						"parameter ${n} is not an integer"
					)));
				}
				None => {
					return Err(PgError::new("42P02", format!("there is no parameter ${n}")));
				}
			},
		};
		values.push(value);
	}
	let out_of_range = || PgError::new("22003", "integer out of range");
	Ok(match values.as_slice() {
		[] => None,
		[key] => Some(AdvisoryKey::Big(*key)),
		[high, low] => Some(AdvisoryKey::Pair(
			i32::try_from(*high).map_err(|_| out_of_range())?,
			i32::try_from(*low).map_err(|_| out_of_range())?,
		)),
		[..] => None,
	})
}

/// Resolves when the peer has closed its end of the connection. It resolves only on a close
/// seen before any further bytes: a client that has already sent its next message cannot be told
/// from one that has gone, so the wait for a lock does not end for it.
async fn peer_closed(stream: &PgStream) {
	match stream {
		Either::Left(io) => io.as_ref().peer_closed().await,
		Either::Right(tls) => tls.get_ref().0.as_ref().peer_closed().await,
	}
}

/// The number of rows a command reports in its command tag: the records its
/// lowering answers with.
fn affected_rows(value: &Value) -> usize {
	match value {
		Value::Array(rows) => rows.len(),
		Value::None | Value::Null => 0,
		Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Object(_)
		| Value::Set(_) => 1,
	}
}

fn format_csv_cell(s: &str, delimiter: u8, quote: u8, escape: u8, null_str: &str) -> String {
	let delim = delimiter as char;
	let q = quote as char;
	let esc = escape as char;
	let needs_quotes = s.is_empty()
		|| s == null_str
		|| s.contains(delim)
		|| s.contains(q)
		|| s.contains('\n')
		|| s.contains('\r');

	if needs_quotes {
		let mut res = String::with_capacity(s.len() + 2);
		res.push(q);
		for c in s.chars() {
			if c == q {
				if esc == q {
					res.push(q);
					res.push(q);
				} else {
					res.push(esc);
					res.push(q);
				}
			} else {
				res.push(c);
			}
		}
		res.push(q);
		res
	} else {
		s.to_string()
	}
}

fn format_text_cell(s: &str, delimiter: u8) -> String {
	let delim = delimiter as char;
	let mut res = String::with_capacity(s.len());
	for c in s.chars() {
		match c {
			'\\' => res.push_str("\\\\"),
			'\n' => res.push_str("\\n"),
			'\r' => res.push_str("\\r"),
			'\t' if delim != '\t' => res.push_str("\\t"),
			c if c == delim => {
				res.push('\\');
				res.push(c);
			}
			c => res.push(c),
		}
	}
	res
}

/// Whether a claimed subject names exactly the record `rd`: its full record id
/// (`user:alice`) or its key alone (`alice`, `42`, a UUID). The whole claim must
/// be that spelling, so a subject that is only the tail of the record id
/// (`alice` against `user:malice`) or that merely starts with it
/// (`user:malice garbage`) never matches.
fn subject_names_record(rd: &Value, claimed: &str) -> bool {
	if claimed == rd.to_sql() {
		return true;
	}
	let Value::RecordId(rid) = rd else {
		return false;
	};
	match &rid.key {
		RecordIdKey::String(key) => claimed == key,
		RecordIdKey::Number(key) => claimed == key.to_string(),
		RecordIdKey::Uuid(key) => claimed == key.to_string(),
		RecordIdKey::Array(_) | RecordIdKey::Object(_) | RecordIdKey::Range(_) => false,
	}
}

/// Count a `0A000` refusal in the unsupported-feature metrics, and log it.
///
/// A refusal by the SQL planner is counted under the kind of construct it refused and not under
/// its message, which describes the client's statement: the counter and the log line carry no
/// client text.
fn record_unsupported(err: &PgError) {
	if err.code != "0A000" {
		return;
	}
	let construct = unsupported_construct(&err.message).unwrap_or(&err.message);
	global_metrics().record_unsupported_feature(construct);
}

/// The error for a COPY option that the record scanner cannot honour: the
/// delimiter, and for CSV the quote and escape, must each be one ASCII byte, and
/// the delimiter cannot be a line end.
fn copy_options_error(format: &Format) -> Option<PgError> {
	if !format.delimiter.is_ascii()
		|| (format.csv && !(format.quote.is_ascii() && format.escape.is_ascii()))
	{
		return Some(PgError::feature_not_supported(
			"COPY delimiter, quote and escape must be single one-byte characters",
		));
	}
	if format.delimiter == b'\n' || format.delimiter == b'\r' {
		return Some(PgError::invalid_parameter_value(
			"COPY delimiter cannot be newline or carriage return",
		));
	}
	None
}

/// Renders `name` as a PostgreSQL identifier, quoting it unless it is a plain
/// one. A name of `*` is quoted: it names a column, not a wildcard.
fn pg_ident(name: &str) -> String {
	if name == "*" {
		return "\"*\"".to_string();
	}
	struct Ident<'a>(&'a str);
	impl std::fmt::Display for Ident<'_> {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			print_ident(f, self.0)
		}
	}
	Ident(name).to_string()
}

/// The query that lists a table's column names in order.
fn copy_column_lookup_query(table: &str) -> String {
	format!(
		"SELECT column_name FROM information_schema.columns WHERE table_name = {} ORDER BY ordinal_position;",
		pg_string_literal(table)
	)
}

/// Renders `value` as a PostgreSQL string literal.
fn pg_string_literal(value: &str) -> String {
	struct Literal<'a>(&'a str);
	impl std::fmt::Display for Literal<'_> {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			print_string_literal(f, self.0)
		}
	}
	Literal(value).to_string()
}

/// Renders `key` as a SurrealQL object key: bare when it is a plain identifier,
/// otherwise as a double-quoted string.
fn surql_object_key(key: &str) -> String {
	let plain = key.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
		&& key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
	if plain {
		key.to_string()
	} else {
		format!("\"{}\"", key.replace('\\', "\\\\").replace('"', "\\\""))
	}
}

/// A COPY field for a column declared as a link to `target`: text that reads as an integer names
/// an integer key, and any other non-empty text, `true` and `1.5` included, names the string key
/// it spells. NULL is NONE where the column admits that (`admits_none`) and stays NULL, which the
/// type check of the column refuses, where it does not. The empty text stays the empty string,
/// which the type check of the column refuses.
fn link_to_surrealql(val: &Option<String>, target: &str, admits_none: bool) -> String {
	let Some(text) = val else {
		return if admits_none {
			"NONE"
		} else {
			"NULL"
		}
		.to_string();
	};
	if text.is_empty() {
		return "''".to_string();
	}
	let literal = value_to_surrealql(val);
	let key = if literal.parse::<i64>().is_ok() {
		literal
	} else {
		string_literal(text)
	};
	format!("type::record({}, {key})", string_literal(target))
}

/// `text` as a quoted SurrealQL string.
fn string_literal(text: &str) -> String {
	format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn value_to_surrealql(val: &Option<String>) -> String {
	match val {
		None => "NULL".to_string(),
		Some(s) => {
			if s.eq_ignore_ascii_case("t") || s.eq_ignore_ascii_case("true") {
				return "true".to_string();
			}
			if s.eq_ignore_ascii_case("f") || s.eq_ignore_ascii_case("false") {
				return "false".to_string();
			}
			if s.len() > 1 && s.starts_with('0') && !s.contains('.') {
				return string_literal(s);
			}
			if let Ok(i) = s.parse::<i64>() {
				return i.to_string();
			}
			if let Ok(f) = s.parse::<f64>()
				&& f.is_finite()
				&& (s.contains('.') || s.contains('e') || s.contains('E'))
			{
				return f.to_string();
			}
			string_literal(s)
		}
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
fn encode_cell(value: Value, ty: &PgType, format: i16) -> Result<Vec<u8>, PgError> {
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
			Some(value) => cells.push(Some(encode_cell(value, &column.ty, format_at(formats, i))?)),
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
///
/// With `namespace`, from an `options=-c namespace=<ns>` setting, the
/// `database` parameter is the database name alone, so a client whose
/// connection-string parser keeps only the first path segment can still select
/// both. A `database` that names a namespace too (`ns/db`) is refused rather
/// than reconciled with it.
fn parse_database_param(
	user: &str,
	database: Option<&str>,
	namespace: Option<&str>,
) -> Result<(Option<String>, Option<String>), PgError> {
	let non_empty = |s: &str| {
		if s.is_empty() {
			None
		} else {
			Some(s.to_string())
		}
	};
	if let Some(namespace) = namespace.and_then(non_empty) {
		return match database {
			Some(database) if database.contains('/') => {
				Err(PgError::invalid_parameter_value(format!(
					"database \"{database}\" names a namespace, and options names namespace \"{namespace}\""
				)))
			}
			None => Ok((Some(namespace), None)),
			Some(database) if database == user => Ok((Some(namespace), None)),
			Some(database) => Ok((Some(namespace), non_empty(database))),
		};
	}
	Ok(match database {
		None => (None, None),
		Some(database) => match database.split_once('/') {
			Some((ns, db)) => (non_empty(ns), non_empty(db)),
			None if database == user => (None, None),
			None => (non_empty(database), None),
		},
	})
}

/// The connection-terminating error for any failed authentication. Kept uniform
/// across cleartext and SCRAM so the failure reveals nothing about which
/// mechanism was attempted or why it failed.
fn auth_failed(user: &str) -> PgError {
	PgError::invalid_password(format!("password authentication failed for user \"{user}\"")).fatal()
}

/// The isolation level a `default_transaction_isolation` or `transaction_isolation`
/// setting names. `repeatable read` is snapshot isolation, as in PostgreSQL;
/// `read committed` and `read uncommitted` are refused for the reason `BEGIN`
/// refuses them.
fn isolation_setting(value: &str) -> Result<Isolation, PgError> {
	let level = value.trim().trim_matches(|c| c == '\'' || c == '"').trim();
	let normalised = level.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase();
	match normalised.as_str() {
		"serializable" => Ok(Isolation::Serializable),
		"repeatable read" | "default" => Ok(Isolation::Snapshot),
		"read committed" | "read uncommitted" => Err(PgError::feature_not_supported(format!(
			"isolation level \"{normalised}\" is not supported; use \"repeatable read\" or \"serializable\""
		))),
		_ => Err(PgError::invalid_parameter_value(format!(
			"invalid value for isolation level: \"{level}\""
		))),
	}
}

/// The modes of a `BEGIN` or `START TRANSACTION`, after its keywords: whether it
/// is read-only and the isolation level it asks for.
///
/// Every mode is honoured. `SERIALIZABLE` is `Serializable` and `REPEATABLE READ`
/// is `Snapshot`, which is what PostgreSQL's own `REPEATABLE READ` is. The levels
/// below it are refused: they see commits made while the transaction runs, which a
/// snapshot never does. `DEFERRABLE`
/// is accepted only on a serializable read-only transaction, where it has no
/// effect here. `None` means a token that is not a mode, or a combination that
/// is not accepted, so the statement is not intercepted and the dialect parser
/// reports it.
fn begin_modes(tokens: &[String]) -> Option<(bool, Option<Isolation>)> {
	let at = |i: usize| tokens.get(i).map(String::as_str);
	let mut read_only = false;
	let mut isolation = None;
	let mut deferrable = false;
	let mut i = 0;
	while i < tokens.len() {
		match tokens[i].as_str() {
			"TRANSACTION" | "WORK" => i += 1,
			"ISOLATION" => {
				if at(i + 1) != Some("LEVEL") {
					return None;
				}
				match (at(i + 2), at(i + 3)) {
					(Some("SERIALIZABLE"), _) => {
						isolation = Some(Isolation::Serializable);
						i += 3;
					}
					(Some("REPEATABLE"), Some("READ")) => {
						isolation = Some(Isolation::Snapshot);
						i += 4;
					}
					// `READ COMMITTED` and `READ UNCOMMITTED` are not intercepted, so the
					// PostgreSQL parser reports that they are not supported.
					_ => return None,
				}
			}
			"READ" => {
				match at(i + 1) {
					Some("ONLY") => read_only = true,
					Some("WRITE") => read_only = false,
					_ => return None,
				}
				i += 2;
			}
			"NOT" => {
				if at(i + 1) != Some("DEFERRABLE") {
					return None;
				}
				i += 2;
			}
			"DEFERRABLE" => {
				deferrable = true;
				i += 1;
			}
			_ => return None,
		}
	}
	if deferrable && !(read_only && isolation == Some(Isolation::Serializable)) {
		return None;
	}
	Some((read_only, isolation))
}

/// An intercepted transaction-control statement.
#[derive(Clone, PartialEq, Eq, Debug)]
enum TxnOp {
	Begin {
		read_only: bool,
		isolation: Option<Isolation>,
	},
	Commit,
	Rollback,
	Savepoint(String),
	ReleaseSavepoint(String),
	RollbackSavepoint(String),
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
	let mut tokens_raw = stripped.split_whitespace();
	let first = tokens_raw.next()?;
	let first_upper = first.to_ascii_uppercase();

	if first_upper == "SAVEPOINT" {
		let name = tokens_raw.next()?;
		if tokens_raw.next().is_none() {
			let clean = name.trim_matches('"').trim_matches('`').to_string();
			if !clean.is_empty() {
				return Some(TxnOp::Savepoint(clean));
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
			let clean = next.trim_matches('"').trim_matches('`').to_string();
			if !clean.is_empty() {
				return Some(TxnOp::ReleaseSavepoint(clean));
			}
		}
		return None;
	}

	if first_upper == "ROLLBACK" || first_upper == "ABORT" {
		let mut next_opt = tokens_raw.next();
		if let Some(t) = next_opt
			&& (t.eq_ignore_ascii_case("WORK") || t.eq_ignore_ascii_case("TRANSACTION"))
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
				let clean = name_opt.trim_matches('"').trim_matches('`').to_string();
				if !clean.is_empty() {
					return Some(TxnOp::RollbackSavepoint(clean));
				}
			}
			return None;
		}
	}

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

	// Modes may be separated by commas, as in `BEGIN ISOLATION LEVEL READ
	// COMMITTED, READ ONLY`.
	let spaced = stripped.replace(',', " ");
	let mut tokens = spaced.split_whitespace().map(str::to_ascii_uppercase);
	let first = tokens.next()?;
	match first.as_str() {
		"BEGIN" => {
			let remaining: Vec<String> = tokens.collect();
			let (read_only, isolation) = begin_modes(&remaining)?;
			Some(TxnOp::Begin {
				read_only,
				isolation,
			})
		}
		"START" => {
			if tokens.next().as_deref() != Some("TRANSACTION") {
				return None;
			}
			let remaining: Vec<String> = tokens.collect();
			let (read_only, isolation) = begin_modes(&remaining)?;
			Some(TxnOp::Begin {
				read_only,
				isolation,
			})
		}
		"COMMIT" | "END" => {
			let remaining: Vec<String> = tokens.collect();
			if remaining.iter().all(|t| MODIFIERS.contains(&t.as_str())) {
				Some(TxnOp::Commit)
			} else {
				None
			}
		}
		"ROLLBACK" | "ABORT" => {
			let remaining: Vec<String> = tokens.collect();
			if remaining.iter().all(|t| MODIFIERS.contains(&t.as_str())) {
				Some(TxnOp::Rollback)
			} else {
				None
			}
		}
		_ => None,
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

/// An intercepted `SET` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SetStatement {
	name: String,
	value: String,
	is_local: bool,
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

fn strip_prefix_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
	if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
		Some(&s[prefix.len()..])
	} else {
		None
	}
}

/// Recognise a `SET [LOCAL|SESSION] name = value` / `SET name TO value` statement.
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
	let mut assignment = words.next().unwrap_or("").trim();
	let mut is_local = false;

	if let Some(after) = strip_prefix_case(assignment, "LOCAL ") {
		is_local = true;
		assignment = after.trim();
	} else if let Some(after) = strip_prefix_case(assignment, "SESSION ") {
		assignment = after.trim();
	}

	// SET [LOCAL] TIME ZONE ...
	if let Some(after) = strip_prefix_case(assignment, "TIME ZONE") {
		let val = after.trim();
		let val = val.strip_prefix('=').unwrap_or(val).trim();
		let val = if let Some(after_to) = strip_prefix_case(val, "TO ") {
			after_to.trim()
		} else {
			val
		};
		return Some(SetStatement {
			name: "timezone".to_string(),
			value: val.to_string(),
			is_local,
		});
	}

	// SET NAMES ...
	if let Some(after) = strip_prefix_case(assignment, "NAMES ") {
		let val = after.trim();
		return Some(SetStatement {
			name: "client_encoding".to_string(),
			value: val.to_string(),
			is_local,
		});
	}

	let first_word = assignment.split_whitespace().next().unwrap_or("");
	if first_word.eq_ignore_ascii_case("ROLE") {
		let after = assignment[first_word.len()..].trim();
		let val = if let Some(after_to) = strip_prefix_case(after, "TO ") {
			after_to.trim()
		} else if let Some(after_eq) = after.strip_prefix('=') {
			after_eq.trim()
		} else {
			after
		};
		return Some(SetStatement {
			name: "role".to_string(),
			value: val.to_string(),
			is_local,
		});
	}
	if first_word.eq_ignore_ascii_case("TRANSACTION") {
		return Some(SetStatement {
			name: String::new(),
			value: assignment.to_string(),
			is_local,
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
		Some((name, value)) => {
			let clean_name = name.trim_matches('"').trim_matches('`').to_string();
			SetStatement {
				name: clean_name,
				value: value.to_string(),
				is_local,
			}
		}
		None => SetStatement {
			name: String::new(),
			value: assignment.to_string(),
			is_local,
		},
	})
}

/// Recognise a `SHOW <param>` or `SHOW ALL` statement.
fn parse_show(trimmed: &str) -> Option<String> {
	let stripped = strip_sql_comments(trimmed);
	let rest = stripped.trim().trim_end_matches(';').trim();
	if rest.contains(';') {
		return None;
	}
	let mut words = rest.split_whitespace();
	if !words.next()?.eq_ignore_ascii_case("SHOW") {
		return None;
	}
	let first = words.next()?;
	if first.eq_ignore_ascii_case("TIME") {
		if let Some(second) = words.next()
			&& second.eq_ignore_ascii_case("ZONE")
		{
			return Some("timezone".to_string());
		}
		return Some("time".to_string());
	}
	if first.eq_ignore_ascii_case("ALL") {
		return Some("ALL".to_string());
	}
	Some(first.trim_matches('"').trim_matches('`').to_string())
}

/// Recognise a `RESET <param>` or `RESET ALL` statement.
fn parse_reset(trimmed: &str) -> Option<String> {
	let stripped = strip_sql_comments(trimmed);
	let rest = stripped.trim().trim_end_matches(';').trim();
	if rest.contains(';') {
		return None;
	}
	let mut words = rest.split_whitespace();
	if !words.next()?.eq_ignore_ascii_case("RESET") {
		return None;
	}
	let first = words.next()?;
	if first.eq_ignore_ascii_case("TIME") {
		if let Some(second) = words.next()
			&& second.eq_ignore_ascii_case("ZONE")
		{
			return Some("timezone".to_string());
		}
		return Some("time".to_string());
	}
	if first.eq_ignore_ascii_case("ALL") {
		return Some("ALL".to_string());
	}
	Some(first.trim_matches('"').trim_matches('`').to_string())
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

/// Whether `sql` holds nothing but whitespace and PostgreSQL comments (`-- …` to
/// the end of the line, and `/* … */`, which nests): no statement. Whitespace is
/// what PostgreSQL's lexer skips: space, tab, newline, carriage return, form
/// feed and vertical tab. An unterminated block comment is not blank, so the
/// parser reports it.
fn is_blank_pg_sql(sql: &str) -> bool {
	let bytes = sql.as_bytes();
	let mut i = 0;
	while i < bytes.len() {
		match bytes[i] {
			b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c => i += 1,
			b'-' if bytes.get(i + 1) == Some(&b'-') => {
				while i < bytes.len() && bytes[i] != b'\n' {
					i += 1;
				}
			}
			b'/' if bytes.get(i + 1) == Some(&b'*') => {
				let mut depth = 1;
				i += 2;
				while depth > 0 {
					match (bytes.get(i), bytes.get(i + 1)) {
						(Some(b'/'), Some(b'*')) => {
							depth += 1;
							i += 2;
						}
						(Some(b'*'), Some(b'/')) => {
							depth -= 1;
							i += 2;
						}
						(Some(_), _) => i += 1,
						(None, _) => return false,
					}
				}
			}
			_ => return false,
		}
	}
	true
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

/// The value of the setting `name` in a Postgres `options` startup string,
/// given as `-c name=value`, `-cname=value` or a bare `name=value`; the first
/// occurrence wins.
fn option_setting<'a>(options: &'a str, name: &str) -> Option<&'a str> {
	let mut tokens = options.split_whitespace();
	while let Some(token) = tokens.next() {
		let setting = if token == "-c" {
			tokens.next()?
		} else {
			token.strip_prefix("-c").unwrap_or(token)
		};
		if let Some((key, value)) = setting.split_once('=')
			&& key == name
		{
			return Some(value);
		}
	}
	None
}

/// Parse a Postgres `options` startup string for a `-c dialect=<name>` (or
/// bare `dialect=<name>`) setting.
fn dialect_from_options(options: &str) -> Option<Dialect> {
	option_setting(options, "dialect").and_then(parse_dialect)
}

/// The refusal of a dialect that belongs to another protocol.
fn not_over_pgwire(dialect: Dialect) -> PgError {
	PgError::feature_not_supported(format!(
		"the {} dialect is not available over the PostgreSQL protocol",
		dialect_name(dialect)
	))
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
	use surrealdb_core::kvs::Isolation;
	use surrealdb_types::{RecordId, RecordIdKey, Value};

	use super::{
		Dialect, TxnOp, copy_column_lookup_query, dialect_from_options, dialect_name,
		is_blank_pg_sql, isolation_setting, option_setting, parse_database_param, parse_dialect,
		parse_set, pg_ident, pg_string_literal, rewrite_positional_params, row_within_wire_limit,
		split_statements, subject_names_record, surql_object_key, transaction_control,
		wire_row_fits,
	};

	fn rewrite(query: &str) -> String {
		rewrite_positional_params(query).0
	}

	#[test]
	fn recognises_transaction_control() {
		assert_eq!(
			transaction_control("BEGIN"),
			Some(TxnOp::Begin {
				read_only: false,
				isolation: None
			})
		);
		assert_eq!(
			transaction_control("begin;"),
			Some(TxnOp::Begin {
				read_only: false,
				isolation: None
			})
		);
		assert_eq!(
			transaction_control("START TRANSACTION"),
			Some(TxnOp::Begin {
				read_only: false,
				isolation: None
			})
		);
		assert_eq!(transaction_control("COMMIT"), Some(TxnOp::Commit));
		assert_eq!(transaction_control("End Transaction ;"), Some(TxnOp::Commit));
		assert_eq!(transaction_control("ROLLBACK"), Some(TxnOp::Rollback));
		assert_eq!(transaction_control("ABORT"), Some(TxnOp::Rollback));
		assert_eq!(transaction_control("SAVEPOINT sp1"), Some(TxnOp::Savepoint("sp1".to_string())));
		assert_eq!(
			transaction_control("savepoint \"sp1\";"),
			Some(TxnOp::Savepoint("sp1".to_string()))
		);
		assert_eq!(
			transaction_control("RELEASE SAVEPOINT sp1"),
			Some(TxnOp::ReleaseSavepoint("sp1".to_string()))
		);
		assert_eq!(
			transaction_control("RELEASE sp1;"),
			Some(TxnOp::ReleaseSavepoint("sp1".to_string()))
		);
		assert_eq!(
			transaction_control("ROLLBACK TO SAVEPOINT sp1"),
			Some(TxnOp::RollbackSavepoint("sp1".to_string()))
		);
		assert_eq!(
			transaction_control("ROLLBACK WORK TO sp1;"),
			Some(TxnOp::RollbackSavepoint("sp1".to_string()))
		);
		// A multi-statement script is not intercepted.
		assert_eq!(transaction_control("BEGIN; RETURN 1; COMMIT"), None);
		assert_eq!(transaction_control("RETURN 1"), None);
	}

	#[test]
	fn an_isolation_setting_names_a_level_the_connection_can_give() {
		for (value, expected) in [
			("serializable", Isolation::Serializable),
			("'SERIALIZABLE'", Isolation::Serializable),
			("repeatable read", Isolation::Snapshot),
			("\"Repeatable   Read\"", Isolation::Snapshot),
			("default", Isolation::Snapshot),
		] {
			assert_eq!(isolation_setting(value).ok(), Some(expected), "{value}");
		}
		for value in ["read committed", "'read uncommitted'", "snapshot", "", "chaos"] {
			assert!(isolation_setting(value).is_err(), "{value}");
		}
	}

	#[test]
	fn recognises_transaction_preambles() {
		// Comment stripping still allows clean transaction control.
		assert_eq!(
			transaction_control("BEGIN /* jdbc */"),
			Some(TxnOp::Begin {
				read_only: false,
				isolation: None
			})
		);
		assert_eq!(transaction_control("COMMIT -- done"), Some(TxnOp::Commit));
		// Modifiers like isolation levels, read only, chain are recognised.
		assert_eq!(
			transaction_control("BEGIN ISOLATION LEVEL SERIALIZABLE"),
			Some(TxnOp::Begin {
				read_only: false,
				isolation: Some(Isolation::Serializable)
			})
		);
		assert_eq!(
			transaction_control("START TRANSACTION READ WRITE"),
			Some(TxnOp::Begin {
				read_only: false,
				isolation: None
			})
		);
		assert_eq!(
			transaction_control("BEGIN READ ONLY"),
			Some(TxnOp::Begin {
				read_only: true,
				isolation: None
			})
		);
		for (sql, read_only, isolation) in [
			("BEGIN ISOLATION LEVEL REPEATABLE READ", false, Some(Isolation::Snapshot)),
			(
				"START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
				true,
				Some(Isolation::Serializable),
			),
			("BEGIN NOT DEFERRABLE", false, None),
			("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY", true, Some(Isolation::Snapshot)),
			(
				"BEGIN ISOLATION LEVEL SERIALIZABLE,READ ONLY , DEFERRABLE",
				true,
				Some(Isolation::Serializable),
			),
		] {
			assert_eq!(
				transaction_control(sql),
				Some(TxnOp::Begin {
					read_only,
					isolation
				}),
				"{sql}"
			);
		}
		// A mode that is not honoured is not intercepted, so the dialect parser
		// reports it rather than the connection ignoring it.
		for sql in [
			"BEGIN ISOLATION LEVEL READ COMMITTED",
			"BEGIN ISOLATION LEVEL READ UNCOMMITTED READ ONLY",
			"BEGIN DEFERRABLE",
			"BEGIN ISOLATION LEVEL SERIALIZABLE DEFERRABLE",
			"BEGIN ISOLATION LEVEL",
			"BEGIN ISOLATION LEVEL REPEATABLE",
			"BEGIN READ",
		] {
			assert_eq!(transaction_control(sql), None, "{sql}");
		}
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
	fn test_is_blank_pg_sql() {
		assert!(is_blank_pg_sql(""));
		assert!(is_blank_pg_sql("-- a\n/* b /* nested */ c */"));
		// Each character PostgreSQL's lexer skips separates comments.
		for space in [' ', '\t', '\n', '\r', '\x0b', '\x0c'] {
			assert!(is_blank_pg_sql(&format!("/* a */{space}/* b */{space}")), "{space:?}");
		}
		assert!(!is_blank_pg_sql("/* a */ SELECT 1"));
		assert!(!is_blank_pg_sql("/* a */\x0b;"));
		assert!(!is_blank_pg_sql("/* unterminated"));
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
		assert!(!set.is_local);
		let set = parse_set("SET extra_float_digits TO 3").unwrap();
		assert_eq!(set.name, "extra_float_digits");
		assert_eq!(set.value, "3");
		assert!(!set.is_local);
		// SET TIME ZONE models the timezone GUC.
		let set = parse_set("SET TIME ZONE 'UTC'").unwrap();
		assert_eq!(set.name, "timezone");
		assert_eq!(set.value, "'UTC'");
		assert!(!set.is_local);
		let set = parse_set("SET LOCAL timezone = 'Asia/Tokyo'").unwrap();
		assert_eq!(set.name, "timezone");
		assert_eq!(set.value, "'Asia/Tokyo'");
		// SET ROLE models the role GUC (P-02).
		let set = parse_set("SET ROLE admin").unwrap();
		assert_eq!(set.name, "role");
		assert_eq!(set.value, "admin");
		// Unmodelled SET syntax parses with an empty name (rejected by handle_set).
		let set = parse_set("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'").unwrap();
		assert_eq!(set.name, "");
		assert_eq!(set.value, "TRANSACTION SNAPSHOT '00000003-0000001B-1'");
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
		assert_eq!(parse_database_param("root", None, None).unwrap(), (None, None));
		assert_eq!(parse_database_param("root", Some("root"), None).unwrap(), (None, None));
		assert_eq!(parse_database_param("root", Some(""), None).unwrap(), (None, None));
		assert_eq!(parse_database_param("root", Some("ns"), None).unwrap(), (s("ns"), None));
		assert_eq!(parse_database_param("root", Some("ns/"), None).unwrap(), (s("ns"), None));
		assert_eq!(parse_database_param("root", Some("ns/db"), None).unwrap(), (s("ns"), s("db")));
		assert_eq!(parse_database_param("root", Some("/db"), None).unwrap(), (None, s("db")));
		assert_eq!(
			parse_database_param("root", Some("root/db"), None).unwrap(),
			(s("root"), s("db"))
		);
		// `options=-c namespace=<ns>` names the namespace, and `database` is the database alone.
		let ns = Some("orm");
		assert_eq!(
			parse_database_param("root", Some("prisma"), ns).unwrap(),
			(s("orm"), s("prisma"))
		);
		assert_eq!(parse_database_param("root", None, ns).unwrap(), (s("orm"), None));
		assert_eq!(parse_database_param("root", Some("root"), ns).unwrap(), (s("orm"), None));
		assert_eq!(parse_database_param("root", Some(""), ns).unwrap(), (s("orm"), None));
		assert_eq!(parse_database_param("root", Some("db"), Some("")).unwrap(), (s("db"), None));
		assert!(parse_database_param("root", Some("other/db"), ns).is_err());
	}

	#[test]
	fn reads_settings_from_options() {
		assert_eq!(option_setting("-c namespace=orm -c dialect=pgsql", "namespace"), Some("orm"));
		assert_eq!(option_setting("-cnamespace=orm", "namespace"), Some("orm"));
		assert_eq!(option_setting("dialect=pgsql namespace=orm", "namespace"), Some("orm"));
		assert_eq!(option_setting("-c dialect=pgsql", "namespace"), None);
		assert_eq!(option_setting("-c namespaces=orm", "namespace"), None);
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

	#[test]
	fn subject_names_record_matches_whole_identities_only() {
		let malice = Value::RecordId(RecordId::new("user", "malice"));
		for claimed in ["user:malice", "malice"] {
			assert!(subject_names_record(&malice, claimed), "{claimed}");
		}
		for claimed in [
			"alice",
			"user:alice",
			"ser:malice",
			"post:malice",
			"user:malice garbage",
			"user:malice:x",
			"user:malice;x",
			"user:malice/*x*/",
			" malice",
			"MALICE",
			"",
		] {
			assert!(!subject_names_record(&malice, claimed), "{claimed:?}");
		}

		let eleven = Value::RecordId(RecordId::new("user", RecordIdKey::Number(11)));
		for claimed in ["user:11", "11"] {
			assert!(subject_names_record(&eleven, claimed), "{claimed}");
		}
		for claimed in ["1", "user:1", "111", "user:11 "] {
			assert!(!subject_names_record(&eleven, claimed), "{claimed:?}");
		}

		// Only a record id has a key to name.
		assert!(!subject_names_record(&Value::String("alice".into()), "alice"));
	}

	#[test]
	fn pg_ident_quotes_names_that_are_not_plain() {
		assert_eq!(pg_ident("users"), "users");
		assert_eq!(pg_ident("a b"), "\"a b\"");
		assert_eq!(pg_ident("a\"b"), "\"a\"\"b\"");
		assert_eq!(pg_ident("x; DROP TABLE y"), "\"x; DROP TABLE y\"");
		assert_eq!(pg_ident("*"), "\"*\"");
	}

	#[test]
	fn pg_string_literal_doubles_embedded_quotes() {
		assert_eq!(pg_string_literal("o'brien"), "'o''brien'");
		assert_eq!(pg_string_literal("x' OR 'a'='a"), "'x'' OR ''a''=''a'");
		assert_eq!(pg_string_literal("a\\b"), "'a\\b'");
	}

	#[test]
	fn copy_column_lookup_query_keeps_the_table_name_inside_its_literal() {
		assert_eq!(
			copy_column_lookup_query("people"),
			"SELECT column_name FROM information_schema.columns WHERE table_name = 'people' ORDER BY ordinal_position;"
		);
		assert_eq!(
			copy_column_lookup_query("x' OR 'a'='a"),
			"SELECT column_name FROM information_schema.columns WHERE table_name = 'x'' OR ''a''=''a' ORDER BY ordinal_position;"
		);
	}

	#[test]
	fn surql_object_key_quotes_names_that_are_not_plain() {
		assert_eq!(surql_object_key("col_1"), "col_1");
		assert_eq!(surql_object_key("_x"), "_x");
		assert_eq!(surql_object_key("a b"), "\"a b\"");
		assert_eq!(surql_object_key("1abc"), "\"1abc\"");
		assert_eq!(surql_object_key(""), "\"\"");
		assert_eq!(surql_object_key("a\"b\\c"), "\"a\\\"b\\\\c\"");
		assert_eq!(surql_object_key("x: 1}]; DELETE v; {y"), "\"x: 1}]; DELETE v; {y\"");
	}
}
