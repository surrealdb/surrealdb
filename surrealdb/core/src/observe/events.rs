//! Structured observability events dispatched by the core to observers.
//!
//! Every event is split into a `Safe` half (bounded, no customer data) and a
//! `Ctx` half (may contain namespace/database/user identifiers or SQL text).
//! Consumers writing to unauthenticated sinks MUST only read the `Safe` half.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use uuid::Uuid;

use crate::expr::{Expr, TopLevelExpr};
use crate::iam::Level;

/// Outcome of a query, statement, or RPC call.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Outcome {
	Success,
	Error,
	Cancelled,
}

impl Outcome {
	/// Stable lower-case label for this outcome.
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Success => "success",
			Self::Error => "error",
			Self::Cancelled => "cancelled",
		}
	}
}

/// Map a `Result` to its observable outcome.
///
/// Only `Success` and `Error` are produced here; `Cancelled` is signalled
/// explicitly by callers that know the operation was aborted (for example a
/// transaction cancel) and cannot be inferred from a `Result` alone.
impl<T, E> From<&Result<T, E>> for Outcome {
	fn from(result: &Result<T, E>) -> Self {
		match result {
			Ok(_) => Self::Success,
			Err(_) => Self::Error,
		}
	}
}

/// Bounded classification for a single SurrealQL statement.
///
/// All variants are fixed at compile time so using this as a metric attribute
/// yields bounded cardinality. Any expression that does not map to a known
/// variant collapses to [`StatementType::Other`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum StatementType {
	Select,
	Create,
	Update,
	Upsert,
	Delete,
	Relate,
	Insert,
	Define,
	Remove,
	Rebuild,
	Alter,
	Info,
	Live,
	Kill,
	Let,
	Return,
	Foreach,
	IfElse,
	Sleep,
	Explain,
	Begin,
	Commit,
	Cancel,
	Access,
	Use,
	Option,
	Show,
	Break,
	Continue,
	Throw,
	Block,
	Other,
}

impl StatementType {
	/// Stable snake-case label safe for use as a metric attribute value.
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Select => "select",
			Self::Create => "create",
			Self::Update => "update",
			Self::Upsert => "upsert",
			Self::Delete => "delete",
			Self::Relate => "relate",
			Self::Insert => "insert",
			Self::Define => "define",
			Self::Remove => "remove",
			Self::Rebuild => "rebuild",
			Self::Alter => "alter",
			Self::Info => "info",
			Self::Live => "live",
			Self::Kill => "kill",
			Self::Let => "let",
			Self::Return => "return",
			Self::Foreach => "foreach",
			Self::IfElse => "ifelse",
			Self::Sleep => "sleep",
			Self::Explain => "explain",
			Self::Begin => "begin",
			Self::Commit => "commit",
			Self::Cancel => "cancel",
			Self::Access => "access",
			Self::Use => "use",
			Self::Option => "option",
			Self::Show => "show",
			Self::Break => "break",
			Self::Continue => "continue",
			Self::Throw => "throw",
			Self::Block => "block",
			Self::Other => "other",
		}
	}

	/// Classify a [`TopLevelExpr`] into its bounded statement category.
	pub(crate) fn from_top_level(expr: &TopLevelExpr) -> Self {
		match expr {
			TopLevelExpr::Begin => Self::Begin,
			TopLevelExpr::Cancel => Self::Cancel,
			TopLevelExpr::Commit => Self::Commit,
			TopLevelExpr::Access(_) => Self::Access,
			TopLevelExpr::Kill(_) => Self::Kill,
			TopLevelExpr::Live(_) => Self::Live,
			TopLevelExpr::Option(_) => Self::Option,
			TopLevelExpr::Use(_) => Self::Use,
			TopLevelExpr::Show(_) => Self::Show,
			TopLevelExpr::Expr(expr) => Self::from_expr(expr),
		}
	}

	/// Classify a bare [`Expr`] into its bounded statement category.
	pub(crate) fn from_expr(expr: &Expr) -> Self {
		match expr {
			Expr::Select(_) => Self::Select,
			Expr::Create(_) => Self::Create,
			Expr::Update(_) => Self::Update,
			Expr::Upsert(_) => Self::Upsert,
			Expr::Delete(_) => Self::Delete,
			Expr::Relate(_) => Self::Relate,
			Expr::Insert(_) => Self::Insert,
			Expr::Define(_) => Self::Define,
			Expr::Remove(_) => Self::Remove,
			Expr::Rebuild(_) => Self::Rebuild,
			Expr::Alter(_) => Self::Alter,
			Expr::Info(_) => Self::Info,
			Expr::Foreach(_) => Self::Foreach,
			Expr::IfElse(_) => Self::IfElse,
			Expr::Sleep(_) => Self::Sleep,
			Expr::Explain {
				..
			} => Self::Explain,
			Expr::Let(_) => Self::Let,
			Expr::Return(_) => Self::Return,
			Expr::Break => Self::Break,
			Expr::Continue => Self::Continue,
			Expr::Throw(_) => Self::Throw,
			Expr::Block(_) => Self::Block,
			// Anything that isn't a recognised statement-shaped
			// expression collapses to `Other`. Enumerated rather than
			// using a wildcard so a new `Expr` variant forces a
			// classification decision at compile time.
			Expr::Literal(_)
			| Expr::Param(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Prefix {
				..
			}
			| Expr::Postfix {
				..
			}
			| Expr::Binary {
				..
			}
			| Expr::FunctionCall(_)
			| Expr::Closure(_) => Self::Other,
		}
	}

	pub(crate) fn is_dml(self) -> bool {
		matches!(
			self,
			Self::Create | Self::Update | Self::Upsert | Self::Delete | Self::Relate | Self::Insert
		)
	}
}

/// Scope of an authentication credential, bounded to the [`Level`] discriminator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AuthScope {
	None,
	Root,
	Namespace,
	Database,
	Record,
}

impl AuthScope {
	/// Stable lower-case label for this scope.
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Root => "root",
			Self::Namespace => "namespace",
			Self::Database => "database",
			Self::Record => "record",
		}
	}
}

impl From<&Level> for AuthScope {
	fn from(level: &Level) -> Self {
		match level {
			Level::No => Self::None,
			Level::Root => Self::Root,
			Level::Namespace(_) => Self::Namespace,
			Level::Database(_, _) => Self::Database,
			Level::Record(_, _, _) => Self::Record,
		}
	}
}

/// Authentication-related action.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AuthAction {
	Signin,
	Signup,
	Authenticate,
	Refresh,
	Invalidate,
	Revoke,
}

impl AuthAction {
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Signin => "signin",
			Self::Signup => "signup",
			Self::Authenticate => "authenticate",
			Self::Refresh => "refresh",
			Self::Invalidate => "invalidate",
			Self::Revoke => "revoke",
		}
	}
}

/// Lifecycle action of a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SessionAction {
	Connect,
	Disconnect,
}

impl SessionAction {
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Connect => "connect",
			Self::Disconnect => "disconnect",
		}
	}
}

/// Wire protocol backing a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SessionProtocol {
	WebSocket,
	Http,
}

impl SessionProtocol {
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::WebSocket => "websocket",
			Self::Http => "http",
		}
	}
}

/// Direction of bytes counted on a [`NetworkBytesEvent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NetworkDirection {
	Received,
	Sent,
}

impl NetworkDirection {
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Received => "received",
			Self::Sent => "sent",
		}
	}
}

/// Bounded classification of an HTTP request method.
///
/// Used as a metric attribute and audit field; all variants are fixed at
/// compile time so cardinality is closed. Anything outside the standard
/// set collapses to [`HttpMethod::Other`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum HttpMethod {
	Get,
	Post,
	Put,
	Patch,
	Delete,
	Head,
	Options,
	Connect,
	Trace,
	Other,
}

impl HttpMethod {
	/// Stable lower-case label safe for use as a metric attribute value.
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Get => "get",
			Self::Post => "post",
			Self::Put => "put",
			Self::Patch => "patch",
			Self::Delete => "delete",
			Self::Head => "head",
			Self::Options => "options",
			Self::Connect => "connect",
			Self::Trace => "trace",
			Self::Other => "other",
		}
	}

	/// Classify a method name from an `http::Method`-like source.
	///
	/// Accepts the upper-case canonical form (`GET`, `POST`, …) as well as
	/// any other casing; non-standard names collapse to
	/// [`HttpMethod::Other`].
	pub fn from_method_str(name: &str) -> Self {
		match name.as_bytes() {
			b"GET" => Self::Get,
			b"POST" => Self::Post,
			b"PUT" => Self::Put,
			b"PATCH" => Self::Patch,
			b"DELETE" => Self::Delete,
			b"HEAD" => Self::Head,
			b"OPTIONS" => Self::Options,
			b"CONNECT" => Self::Connect,
			b"TRACE" => Self::Trace,
			_ => Self::Other,
		}
	}
}

/// Bounded classification of the HTTP wire-protocol version.
///
/// Used as a metric attribute. Unknown versions collapse to
/// [`HttpVersion::Other`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum HttpVersion {
	Http10,
	Http11,
	Http2,
	Http3,
	Other,
}

impl HttpVersion {
	/// Stable label safe for use as a metric attribute value.
	pub const fn as_label(self) -> &'static str {
		match self {
			Self::Http10 => "1_0",
			Self::Http11 => "1_1",
			Self::Http2 => "2",
			Self::Http3 => "3",
			Self::Other => "other",
		}
	}

	/// Classify a version string of the form `HTTP/1.0`, `HTTP/1.1`,
	/// `HTTP/2.0`, etc. Unknown forms collapse to [`HttpVersion::Other`].
	pub fn from_version_str(version: &str) -> Self {
		// Tolerate either the wire form (`HTTP/1.1`) or the bare numeric
		// suffix (`1.1`). Anything else collapses to `Other`.
		let stripped = version.strip_prefix("HTTP/").unwrap_or(version);
		match stripped {
			"1.0" | "10" => Self::Http10,
			"1.1" | "11" => Self::Http11,
			"2.0" | "2" => Self::Http2,
			"3.0" | "3" => Self::Http3,
			_ => Self::Other,
		}
	}
}

// --- StatementEvent ---

/// Safe portion of a [`StatementEvent`]. Attributes: bounded, no customer data.
#[derive(Clone, Copy, Debug)]
pub struct StatementEventSafe {
	pub kind: StatementType,
	pub outcome: Outcome,
	pub duration: Duration,
	pub read_only: bool,
	/// Number of rows returned (SELECT) or affected (CREATE / UPDATE /
	/// UPSERT / DELETE / RELATE / INSERT). Non-DML statements emit `0`.
	///
	/// For DML statements the count reflects records actually mutated by
	/// the iterator -- not the length of the post-RETURN value. This
	/// matters because `RETURN NONE` causes the iterator to drop every
	/// per-document value (it would otherwise yield an empty array even
	/// when N records were modified), and `RETURN BEFORE` on a fresh
	/// `CREATE` would otherwise collapse to `Value::None`.
	///
	/// Bounded scalar, non-identifying -- safe to surface as a counter
	/// increment on the dimensional metrics observer.
	pub result_rows: u64,
	/// Bounded error classification when `outcome` is [`Outcome::Error`].
	/// `None` for success / cancelled. Drawn from a fixed enum at the
	/// recording site so it stays safe for label-cardinality purposes.
	pub error_class: Option<&'static str>,
}

/// Pre-resolved tenant identity attached to events that originate inside the
/// executor or transaction layer.
///
/// Built once per request from the active [`crate::dbs::Session`] and stashed
/// on the [`crate::ctx::Context`] (and on each [`crate::kvs::Transaction`]
/// derived from that context) so emit sites can populate the `*Ctx` half of
/// each event without re-parsing the session value tree on every call.
///
/// `user` follows the same record-access collapsing rule as
/// [`NetworkBytesEventCtx::from_session`]: anonymous sessions map to `None`,
/// record-access principals to a fixed `<record>` sentinel, and everything
/// else to the actor id.
#[derive(Clone, Debug, Default)]
pub struct TenantIdentity {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

impl TenantIdentity {
	/// Build a [`TenantIdentity`] from an authenticated [`crate::dbs::Session`].
	pub fn from_session(sess: &crate::dbs::Session) -> Self {
		let user = if sess.au.is_anon() {
			None
		} else if sess.au.is_record() {
			Some("<record>".to_owned())
		} else {
			Some(sess.au.id().to_owned())
		};
		let client_ip = sess.ip.as_deref().and_then(|raw| raw.parse::<IpAddr>().ok());
		Self {
			namespace: sess.ns.clone(),
			database: sess.db.clone(),
			user,
			session_id: sess.id,
			client_ip,
		}
	}

	/// Project into a [`StatementEventCtx`] preserving the (optional) SQL.
	pub fn to_statement_ctx(&self, sql: Option<String>) -> StatementEventCtx {
		StatementEventCtx {
			sql,
			namespace: self.namespace.clone(),
			database: self.database.clone(),
			user: self.user.clone(),
			session_id: self.session_id,
			client_ip: self.client_ip,
		}
	}

	/// Project into a [`QueryEventCtx`].
	pub fn to_query_ctx(&self) -> QueryEventCtx {
		QueryEventCtx {
			namespace: self.namespace.clone(),
			database: self.database.clone(),
			user: self.user.clone(),
			session_id: self.session_id,
			client_ip: self.client_ip,
		}
	}

	/// Project into a [`TransactionEventCtx`].
	pub fn to_transaction_ctx(&self) -> TransactionEventCtx {
		TransactionEventCtx {
			namespace: self.namespace.clone(),
			database: self.database.clone(),
			user: self.user.clone(),
			session_id: self.session_id,
			client_ip: self.client_ip,
		}
	}

	/// Project into a [`RpcEventCtx`].
	pub fn to_rpc_ctx(&self) -> RpcEventCtx {
		RpcEventCtx {
			namespace: self.namespace.clone(),
			database: self.database.clone(),
			user: self.user.clone(),
			session_id: self.session_id,
			client_ip: self.client_ip,
		}
	}

	/// Project into an [`AuthEventCtx`].
	pub fn to_auth_ctx(&self) -> AuthEventCtx {
		AuthEventCtx {
			namespace: self.namespace.clone(),
			database: self.database.clone(),
			user: self.user.clone(),
			session_id: self.session_id,
			client_ip: self.client_ip,
		}
	}
}

/// Contextual portion of a [`StatementEvent`]. MAY contain customer data.
#[derive(Clone, Debug, Default)]
pub struct StatementEventCtx {
	/// Full statement SQL text. Only populated when the active observer opts in
	/// via [`crate::observe::ExecutionObserver::needs_statement_text`]. Must
	/// never be written to unauthenticated sinks.
	pub sql: Option<String>,
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

/// Emitted once per top-level statement completion.
#[derive(Clone, Debug)]
pub struct StatementEvent {
	pub safe: StatementEventSafe,
	pub ctx: StatementEventCtx,
}

// --- QueryEvent ---

/// Per-query counters summarising statement outcomes within a query batch.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryCounters {
	pub total: u32,
	pub ok: u32,
	pub err: u32,
}

/// Safe portion of a [`QueryEvent`].
#[derive(Clone, Copy, Debug)]
pub struct QueryEventSafe {
	pub outcome: Outcome,
	pub duration: Duration,
	pub counters: QueryCounters,
	/// Bounded error classification when `outcome` is [`Outcome::Error`].
	pub error_class: Option<&'static str>,
}

/// Contextual portion of a [`QueryEvent`].
#[derive(Clone, Debug, Default)]
pub struct QueryEventCtx {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

/// Emitted once per executor query batch.
#[derive(Clone, Debug)]
pub struct QueryEvent {
	pub safe: QueryEventSafe,
	pub ctx: QueryEventCtx,
}

// --- TransactionEvent ---

/// A point-in-time snapshot of the counters held by a `Transaction`.
///
/// `key_bytes_*` / `value_bytes_*` are the raw atomics. `total_bytes_*` and
/// `ops_total` are derived in [`TransactionMetrics::snapshot`] from the
/// matching atomic fields, so consumers can read either form without paying
/// for an extra atomic on the hot path.
#[derive(Clone, Copy, Debug, Default)]
pub struct TransactionMetricsSnapshot {
	pub keys_read: u64,
	pub keys_written: u64,
	pub key_bytes_read: u64,
	pub value_bytes_read: u64,
	pub key_bytes_written: u64,
	pub value_bytes_written: u64,
	/// `key_bytes_read + value_bytes_read`, derived in [`TransactionMetrics::snapshot`].
	pub total_bytes_read: u64,
	/// `key_bytes_written + value_bytes_written`, derived in [`TransactionMetrics::snapshot`].
	pub total_bytes_written: u64,
	pub ops_get: u32,
	pub ops_scan: u32,
	pub ops_put: u32,
	pub ops_set: u32,
	pub ops_del: u32,
	/// `ops_get + ops_scan + ops_put + ops_set + ops_del`, derived in
	/// [`TransactionMetrics::snapshot`] (saturating sum).
	pub ops_total: u32,
}

/// Atomic counters for a single transaction.
///
/// Held on the `Transaction` and updated from its KV methods. Operations use
/// [`Ordering::Relaxed`] because the counters never gate other state and are
/// only read in aggregate by [`TransactionMetrics::snapshot`] when the
/// transaction finishes.
#[derive(Debug, Default)]
pub struct TransactionMetrics {
	keys_read: AtomicU64,
	keys_written: AtomicU64,
	key_bytes_read: AtomicU64,
	value_bytes_read: AtomicU64,
	key_bytes_written: AtomicU64,
	value_bytes_written: AtomicU64,
	ops_get: AtomicU32,
	ops_scan: AtomicU32,
	ops_put: AtomicU32,
	ops_set: AtomicU32,
	ops_del: AtomicU32,
}

impl TransactionMetrics {
	/// Create a new empty counter set.
	pub fn new() -> Self {
		Self::default()
	}

	/// Record a point get: one `get`/`exists`/`getm` op returning
	/// `keys_found` key(s) whose encoded keys/values sum to `key_bytes` /
	/// `value_bytes` respectively.
	pub fn record_get(&self, keys_found: u64, key_bytes: u64, value_bytes: u64) {
		self.ops_get.fetch_add(1, Ordering::Relaxed);
		if keys_found > 0 {
			self.keys_read.fetch_add(keys_found, Ordering::Relaxed);
		}
		if key_bytes > 0 {
			self.key_bytes_read.fetch_add(key_bytes, Ordering::Relaxed);
		}
		if value_bytes > 0 {
			self.value_bytes_read.fetch_add(value_bytes, Ordering::Relaxed);
		}
	}

	/// Record a scan/range read: one scan op returning `keys_found` keys
	/// whose encoded keys/values sum to `key_bytes` / `value_bytes`
	/// respectively (`value_bytes` is 0 for keys-only scans).
	pub fn record_scan(&self, keys_found: u64, key_bytes: u64, value_bytes: u64) {
		self.ops_scan.fetch_add(1, Ordering::Relaxed);
		if keys_found > 0 {
			self.keys_read.fetch_add(keys_found, Ordering::Relaxed);
		}
		if key_bytes > 0 {
			self.key_bytes_read.fetch_add(key_bytes, Ordering::Relaxed);
		}
		if value_bytes > 0 {
			self.value_bytes_read.fetch_add(value_bytes, Ordering::Relaxed);
		}
	}

	/// Record a `set` (upsert) of one key with `key_bytes` / `value_bytes`
	/// payload bytes.
	pub fn record_set(&self, key_bytes: u64, value_bytes: u64) {
		self.ops_set.fetch_add(1, Ordering::Relaxed);
		self.keys_written.fetch_add(1, Ordering::Relaxed);
		if key_bytes > 0 {
			self.key_bytes_written.fetch_add(key_bytes, Ordering::Relaxed);
		}
		if value_bytes > 0 {
			self.value_bytes_written.fetch_add(value_bytes, Ordering::Relaxed);
		}
	}

	/// Record a `put` (insert-if-missing) of one key with `key_bytes` /
	/// `value_bytes` payload bytes. Also covers `putc` / `replace`.
	pub fn record_put(&self, key_bytes: u64, value_bytes: u64) {
		self.ops_put.fetch_add(1, Ordering::Relaxed);
		self.keys_written.fetch_add(1, Ordering::Relaxed);
		if key_bytes > 0 {
			self.key_bytes_written.fetch_add(key_bytes, Ordering::Relaxed);
		}
		if value_bytes > 0 {
			self.value_bytes_written.fetch_add(value_bytes, Ordering::Relaxed);
		}
	}

	/// Record one delete op affecting `keys_deleted` keys (0 when the delete
	/// is over a range whose size is not reported back to the caller). The
	/// `key_bytes` argument is summed into `key_bytes_written` because the
	/// tombstone occupies write capacity, even though no value is stored.
	pub fn record_del(&self, keys_deleted: u64, key_bytes: u64) {
		self.ops_del.fetch_add(1, Ordering::Relaxed);
		if keys_deleted > 0 {
			self.keys_written.fetch_add(keys_deleted, Ordering::Relaxed);
		}
		if key_bytes > 0 {
			self.key_bytes_written.fetch_add(key_bytes, Ordering::Relaxed);
		}
	}

	/// Freeze the current counter values into a `Copy` snapshot suitable for
	/// attaching to a [`TransactionEvent`]. Derived totals (`total_bytes_*`,
	/// `ops_total`) are computed here so consumers do not pay extra atomic
	/// updates on the hot path.
	pub fn snapshot(&self) -> TransactionMetricsSnapshot {
		let key_bytes_read = self.key_bytes_read.load(Ordering::Relaxed);
		let value_bytes_read = self.value_bytes_read.load(Ordering::Relaxed);
		let key_bytes_written = self.key_bytes_written.load(Ordering::Relaxed);
		let value_bytes_written = self.value_bytes_written.load(Ordering::Relaxed);
		let ops_get = self.ops_get.load(Ordering::Relaxed);
		let ops_scan = self.ops_scan.load(Ordering::Relaxed);
		let ops_put = self.ops_put.load(Ordering::Relaxed);
		let ops_set = self.ops_set.load(Ordering::Relaxed);
		let ops_del = self.ops_del.load(Ordering::Relaxed);
		TransactionMetricsSnapshot {
			keys_read: self.keys_read.load(Ordering::Relaxed),
			keys_written: self.keys_written.load(Ordering::Relaxed),
			key_bytes_read,
			value_bytes_read,
			key_bytes_written,
			value_bytes_written,
			total_bytes_read: key_bytes_read.saturating_add(value_bytes_read),
			total_bytes_written: key_bytes_written.saturating_add(value_bytes_written),
			ops_get,
			ops_scan,
			ops_put,
			ops_set,
			ops_del,
			ops_total: ops_get
				.saturating_add(ops_scan)
				.saturating_add(ops_put)
				.saturating_add(ops_set)
				.saturating_add(ops_del),
		}
	}
}

/// Safe portion of a [`TransactionEvent`].
#[derive(Clone, Copy, Debug)]
pub struct TransactionEventSafe {
	pub outcome: Outcome,
	pub write: bool,
	pub duration: Duration,
	pub metrics: TransactionMetricsSnapshot,
	/// Bounded error classification when `outcome` is [`Outcome::Error`].
	pub error_class: Option<&'static str>,
}

/// Contextual portion of a [`TransactionEvent`]. Carries optional
/// tenant-identifying context for enterprise audit destinations.
#[derive(Clone, Debug, Default)]
pub struct TransactionEventCtx {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

/// Emitted once per transaction completion (commit or cancel).
#[derive(Clone, Debug)]
pub struct TransactionEvent {
	pub safe: TransactionEventSafe,
	pub ctx: TransactionEventCtx,
}

// --- RpcEvent ---

/// Safe portion of an [`RpcEvent`].
///
/// `method` holds the already-bounded [`crate::rpc::Method`] and is safe to
/// emit as an attribute because all variants are known at compile time.
#[derive(Clone, Copy, Debug)]
pub struct RpcEventSafe {
	pub method: crate::rpc::Method,
	pub outcome: Outcome,
	pub duration: Duration,
	/// Bounded error classification when `outcome` is [`Outcome::Error`].
	pub error_class: Option<&'static str>,
}

/// Contextual portion of an [`RpcEvent`].
#[derive(Clone, Debug, Default)]
pub struct RpcEventCtx {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

/// Emitted once per RPC method invocation.
#[derive(Clone, Debug)]
pub struct RpcEvent {
	pub safe: RpcEventSafe,
	pub ctx: RpcEventCtx,
}

// --- AuthEvent ---

/// Safe portion of an [`AuthEvent`].
#[derive(Clone, Copy, Debug)]
pub struct AuthEventSafe {
	pub action: AuthAction,
	pub scope: AuthScope,
	pub outcome: Outcome,
	/// Bounded error classification when `outcome` is [`Outcome::Error`].
	pub error_class: Option<&'static str>,
}

/// Contextual portion of an [`AuthEvent`].
#[derive(Clone, Debug, Default)]
pub struct AuthEventCtx {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

/// Emitted once per authentication attempt (both successful and failed).
#[derive(Clone, Debug)]
pub struct AuthEvent {
	pub safe: AuthEventSafe,
	pub ctx: AuthEventCtx,
}

// --- SessionEvent ---

/// Safe portion of a [`SessionEvent`].
#[derive(Clone, Copy, Debug)]
pub struct SessionEventSafe {
	pub action: SessionAction,
	pub protocol: SessionProtocol,
	/// Populated on `Disconnect` with the total session lifetime.
	pub duration: Option<Duration>,
}

/// Contextual portion of a [`SessionEvent`].
#[derive(Clone, Debug, Default)]
pub struct SessionEventCtx {
	pub session_id: Option<Uuid>,
	pub service_name: Option<String>,
	pub client_ip: Option<IpAddr>,
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
}

/// Emitted on session connect and disconnect.
#[derive(Clone, Debug)]
pub struct SessionEvent {
	pub safe: SessionEventSafe,
	pub ctx: SessionEventCtx,
}

// --- NetworkBytesEvent ---

/// Safe portion of a [`NetworkBytesEvent`]. `direction` and `protocol` are
/// bounded enums; `bytes` is a non-identifying scalar. Suitable for
/// unauthenticated, unlabelled aggregate counters.
#[derive(Clone, Copy, Debug)]
pub struct NetworkBytesEventSafe {
	pub direction: NetworkDirection,
	pub protocol: SessionProtocol,
	pub bytes: u64,
}

/// Contextual portion of a [`NetworkBytesEvent`]. May contain
/// tenant-identifying fields. MUST NOT be emitted to unauthenticated sinks.
#[derive(Clone, Debug, Default)]
pub struct NetworkBytesEventCtx {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
}

impl NetworkBytesEventCtx {
	/// Build a ctx from an authenticated [`Session`].
	///
	/// Anonymous sessions leave `user` as `None`. Record-access principals
	/// collapse to a fixed `<record>` sentinel so dimensional metric
	/// cardinality stays bounded by the number of namespaces/databases
	/// rather than by the number of end-customer record ids that have
	/// signed in.
	pub fn from_session(sess: &crate::dbs::Session) -> Self {
		let user = if sess.au.is_anon() {
			None
		} else if sess.au.is_record() {
			Some("<record>".to_owned())
		} else {
			Some(sess.au.id().to_owned())
		};
		Self {
			namespace: sess.ns.clone(),
			database: sess.db.clone(),
			user,
		}
	}
}

/// Emitted whenever the server observes inbound or outbound bytes on a
/// client-facing protocol (HTTP, WebSocket).
///
/// The community observer reads only `safe` and increments a single
/// unlabelled counter per direction. Enterprise dimensional observers read
/// `ctx` and increment a tenant-attributed counter so cross-tenant byte
/// totals can be attributed for chargeback / showback.
#[derive(Clone, Debug)]
pub struct NetworkBytesEvent {
	pub safe: NetworkBytesEventSafe,
	pub ctx: NetworkBytesEventCtx,
}

// --- HttpRequestEvent ---

/// Safe portion of an [`HttpRequestStartEvent`] / [`HttpRequestEvent`].
///
/// Contains only bounded, non-identifying fields suitable for use on the
/// unauthenticated `/metrics` endpoint:
///
/// - `method` is the bounded [`HttpMethod`] enum.
/// - `route` is the matched Axum route template (e.g. `/sql`, `/key/:tb/:id`) or `None` for
///   unmatched paths. Sourced from the statically-declared router so the value set is closed.
/// - `version` is the bounded [`HttpVersion`] enum.
#[derive(Clone, Copy, Debug)]
pub struct HttpRequestStartEventSafe {
	pub method: HttpMethod,
	pub route: Option<&'static str>,
	pub version: HttpVersion,
}

/// Contextual portion of an [`HttpRequestStartEvent`] / [`HttpRequestEvent`].
///
/// May contain tenant-identifying fields. MUST NOT be emitted to
/// unauthenticated sinks; community observers read only the `safe` half.
#[derive(Clone, Debug, Default)]
pub struct HttpRequestEventCtx {
	pub namespace: Option<String>,
	pub database: Option<String>,
	pub user: Option<String>,
	pub session_id: Option<Uuid>,
	pub client_ip: Option<IpAddr>,
}

impl HttpRequestEventCtx {
	/// Build a ctx from an authenticated [`crate::dbs::Session`].
	///
	/// Mirrors [`NetworkBytesEventCtx::from_session`] for the namespace /
	/// database / user fields (record-access principals collapse to the
	/// `<record>` sentinel) and additionally carries the session id and
	/// client IP from the session. The session's `ip` is stored as a
	/// string and may not parse cleanly (proxy headers can include
	/// trailing port specifiers); a parse failure leaves `client_ip` as
	/// `None` rather than corrupting the field.
	pub fn from_session(sess: &crate::dbs::Session) -> Self {
		let user = if sess.au.is_anon() {
			None
		} else if sess.au.is_record() {
			Some("<record>".to_owned())
		} else {
			Some(sess.au.id().to_owned())
		};
		let client_ip = sess.ip.as_deref().and_then(|raw| raw.parse::<IpAddr>().ok());
		Self {
			namespace: sess.ns.clone(),
			database: sess.db.clone(),
			user,
			session_id: sess.id,
			client_ip,
		}
	}

	/// Project this ctx into a [`NetworkBytesEventCtx`] so the same
	/// authenticated session attribution can be reused for byte
	/// counters without re-deriving.
	pub fn to_network_bytes(&self) -> NetworkBytesEventCtx {
		NetworkBytesEventCtx {
			namespace: self.namespace.clone(),
			database: self.database.clone(),
			user: self.user.clone(),
		}
	}
}

/// Emitted at the start of an HTTP request, before the inner service runs.
///
/// Pairs with a matching [`HttpRequestEvent`] dispatched on completion.
/// Active-request gauges live here so they can be incremented up-front and
/// decremented on completion regardless of how the inner stack returns.
#[derive(Clone, Debug)]
pub struct HttpRequestStartEvent {
	pub safe: HttpRequestStartEventSafe,
	pub ctx: HttpRequestEventCtx,
}

/// Safe portion of an [`HttpRequestEvent`]. Bounded, no customer data.
///
/// Status codes are kept as `u16` rather than a bounded enum: HTTP defines
/// 5 hundred-blocks of values and a `u16` already restricts cardinality
/// adequately while keeping the surface honest about what was returned.
/// Operators that don't want per-status-code series should aggregate at the
/// scrape backend.
#[derive(Clone, Copy, Debug)]
pub struct HttpRequestEventSafe {
	pub method: HttpMethod,
	pub route: Option<&'static str>,
	pub status_code: Option<u16>,
	pub version: HttpVersion,
	pub outcome: Outcome,
	pub duration: Duration,
	pub request_size: Option<u64>,
	pub response_size: Option<u64>,
	/// Bounded error classification when `outcome` is [`Outcome::Error`].
	pub error_class: Option<&'static str>,
}

/// Emitted once per HTTP request completion, immediately after the inner
/// stack returns. Carries the full request lifecycle data: method / route /
/// status / version / latency / wire sizes plus the resolved tenant ctx.
#[derive(Clone, Debug)]
pub struct HttpRequestEvent {
	pub safe: HttpRequestEventSafe,
	pub ctx: HttpRequestEventCtx,
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::thread;

	use super::*;

	#[test]
	fn transaction_metrics_record_accumulates() {
		let m = TransactionMetrics::new();
		m.record_get(1, 16, 128);
		m.record_get(0, 0, 0);
		m.record_scan(4, 32, 512);
		m.record_set(8, 64);
		m.record_put(4, 32);
		m.record_del(3, 12);

		let s = m.snapshot();
		assert_eq!(s.ops_get, 2);
		assert_eq!(s.ops_scan, 1);
		assert_eq!(s.ops_set, 1);
		assert_eq!(s.ops_put, 1);
		assert_eq!(s.ops_del, 1);
		assert_eq!(s.keys_read, 5);
		assert_eq!(s.key_bytes_read, 48); // 16 + 32
		assert_eq!(s.value_bytes_read, 640); // 128 + 512
		assert_eq!(s.keys_written, 5); // 1 set + 1 put + 3 deleted
		assert_eq!(s.key_bytes_written, 24); // 8 + 4 + 12
		assert_eq!(s.value_bytes_written, 96); // 64 + 32
	}

	#[test]
	fn transaction_metrics_record_ignores_zero_byte_payloads() {
		let m = TransactionMetrics::new();
		m.record_get(0, 0, 0); // missing point read
		m.record_scan(0, 0, 0); // empty scan
		m.record_set(0, 0); // empty key + value write
		m.record_put(0, 0); // empty key + value write
		m.record_del(0, 0); // range delete with unknown size

		let s = m.snapshot();
		assert_eq!(s.ops_get, 1);
		assert_eq!(s.ops_scan, 1);
		assert_eq!(s.ops_set, 1);
		assert_eq!(s.ops_put, 1);
		assert_eq!(s.ops_del, 1);
		assert_eq!(s.keys_read, 0);
		assert_eq!(s.key_bytes_read, 0);
		assert_eq!(s.value_bytes_read, 0);
		// set / put always count the key, even with an empty value.
		assert_eq!(s.keys_written, 2);
		assert_eq!(s.key_bytes_written, 0);
		assert_eq!(s.value_bytes_written, 0);
	}

	#[test]
	fn transaction_metrics_snapshot_derives_totals() {
		// `total_bytes_*` and `ops_total` are derived in `snapshot()` rather
		// than tracked as separate atomics, to keep the hot-path atomic count
		// per call unchanged when key/value byte tracking was split.
		let m = TransactionMetrics::new();
		m.record_get(1, 4, 16);
		m.record_scan(2, 8, 64);
		m.record_set(4, 100);
		m.record_put(2, 50);
		m.record_del(1, 6);

		let s = m.snapshot();
		assert_eq!(s.total_bytes_read, s.key_bytes_read + s.value_bytes_read);
		assert_eq!(s.total_bytes_read, 12 + 80);
		assert_eq!(s.total_bytes_written, s.key_bytes_written + s.value_bytes_written);
		assert_eq!(s.total_bytes_written, (4 + 2 + 6) + (100 + 50));
		assert_eq!(s.ops_total, s.ops_get + s.ops_scan + s.ops_put + s.ops_set + s.ops_del);
		assert_eq!(s.ops_total, 5);
	}

	#[test]
	fn transaction_metrics_are_concurrent_safe() {
		// Counters are `Relaxed` atomics: they MUST accumulate losslessly across
		// threads or we silently under-count in the transaction event.
		let m = std::sync::Arc::new(TransactionMetrics::new());
		let mut handles = Vec::new();
		for _ in 0..8 {
			let m = Arc::clone(&m);
			handles.push(thread::spawn(move || {
				for _ in 0..1_000 {
					m.record_get(1, 1, 1);
					m.record_set(1, 1);
				}
			}));
		}
		for h in handles {
			h.join().unwrap();
		}
		let s = m.snapshot();
		assert_eq!(s.ops_get, 8_000);
		assert_eq!(s.ops_set, 8_000);
		assert_eq!(s.keys_read, 8_000);
		assert_eq!(s.key_bytes_read, 8_000);
		assert_eq!(s.value_bytes_read, 8_000);
		assert_eq!(s.keys_written, 8_000);
		assert_eq!(s.key_bytes_written, 8_000);
		assert_eq!(s.value_bytes_written, 8_000);
	}

	#[test]
	fn transaction_metrics_snapshot_is_non_destructive() {
		let m = TransactionMetrics::new();
		m.record_get(2, 4, 8);
		let a = m.snapshot();
		let b = m.snapshot();
		assert_eq!(a.ops_get, b.ops_get);
		assert_eq!(a.keys_read, b.keys_read);
		assert_eq!(a.key_bytes_read, b.key_bytes_read);
		assert_eq!(a.value_bytes_read, b.value_bytes_read);
		assert_eq!(a.total_bytes_read, b.total_bytes_read);
	}

	#[test]
	fn bounded_enums_have_stable_labels() {
		// The `as_label` helpers are metric-attribute values. Their output
		// must match `[a-z_]+` because we rely on it being Prometheus-safe.
		// Every variant of every bounded enum that lands on a metric label
		// is exercised here so adding a new variant without updating its
		// `as_label` formatting is caught at test time rather than as a
		// label-cardinality footgun in production.
		let mut labels: Vec<&str> = vec![
			Outcome::Success.as_label(),
			Outcome::Error.as_label(),
			Outcome::Cancelled.as_label(),
			SessionProtocol::Http.as_label(),
			SessionProtocol::WebSocket.as_label(),
		];
		labels.extend([
			StatementType::Select.as_label(),
			StatementType::Create.as_label(),
			StatementType::Update.as_label(),
			StatementType::Upsert.as_label(),
			StatementType::Delete.as_label(),
			StatementType::Relate.as_label(),
			StatementType::Insert.as_label(),
			StatementType::Define.as_label(),
			StatementType::Remove.as_label(),
			StatementType::Rebuild.as_label(),
			StatementType::Alter.as_label(),
			StatementType::Info.as_label(),
			StatementType::Live.as_label(),
			StatementType::Kill.as_label(),
			StatementType::Let.as_label(),
			StatementType::Return.as_label(),
			StatementType::Foreach.as_label(),
			StatementType::IfElse.as_label(),
			StatementType::Sleep.as_label(),
			StatementType::Explain.as_label(),
			StatementType::Begin.as_label(),
			StatementType::Commit.as_label(),
			StatementType::Cancel.as_label(),
			StatementType::Access.as_label(),
			StatementType::Use.as_label(),
			StatementType::Option.as_label(),
			StatementType::Show.as_label(),
			StatementType::Break.as_label(),
			StatementType::Continue.as_label(),
			StatementType::Throw.as_label(),
			StatementType::Block.as_label(),
			StatementType::Other.as_label(),
		]);
		labels.extend([NetworkDirection::Sent.as_label(), NetworkDirection::Received.as_label()]);
		labels.extend([
			AuthScope::None.as_label(),
			AuthScope::Root.as_label(),
			AuthScope::Namespace.as_label(),
			AuthScope::Database.as_label(),
			AuthScope::Record.as_label(),
		]);
		labels.extend([
			AuthAction::Signin.as_label(),
			AuthAction::Signup.as_label(),
			AuthAction::Authenticate.as_label(),
			AuthAction::Refresh.as_label(),
			AuthAction::Invalidate.as_label(),
			AuthAction::Revoke.as_label(),
		]);
		labels.extend([SessionAction::Connect.as_label(), SessionAction::Disconnect.as_label()]);
		labels.extend([
			HttpMethod::Get.as_label(),
			HttpMethod::Post.as_label(),
			HttpMethod::Put.as_label(),
			HttpMethod::Patch.as_label(),
			HttpMethod::Delete.as_label(),
			HttpMethod::Head.as_label(),
			HttpMethod::Options.as_label(),
			HttpMethod::Connect.as_label(),
			HttpMethod::Trace.as_label(),
			HttpMethod::Other.as_label(),
		]);
		for l in labels {
			assert!(!l.is_empty());
			assert!(
				l.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
				"`{l}` is not a safe label value",
			);
		}
		// HttpVersion labels are slightly different: they use digits and
		// underscores (e.g. "1_1"). Validate them with a relaxed predicate
		// so they don't fail the lower-case-only check above but still
		// match Prometheus label-value conventions.
		for l in [
			HttpVersion::Http10.as_label(),
			HttpVersion::Http11.as_label(),
			HttpVersion::Http2.as_label(),
			HttpVersion::Http3.as_label(),
			HttpVersion::Other.as_label(),
		] {
			assert!(!l.is_empty());
			assert!(
				l.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
				"`{l}` is not a safe label value",
			);
		}
	}

	#[test]
	fn http_method_from_str_classifies_known_methods() {
		assert_eq!(HttpMethod::from_method_str("GET"), HttpMethod::Get);
		assert_eq!(HttpMethod::from_method_str("POST"), HttpMethod::Post);
		assert_eq!(HttpMethod::from_method_str("PUT"), HttpMethod::Put);
		assert_eq!(HttpMethod::from_method_str("PATCH"), HttpMethod::Patch);
		assert_eq!(HttpMethod::from_method_str("DELETE"), HttpMethod::Delete);
		assert_eq!(HttpMethod::from_method_str("HEAD"), HttpMethod::Head);
		assert_eq!(HttpMethod::from_method_str("OPTIONS"), HttpMethod::Options);
		assert_eq!(HttpMethod::from_method_str("CONNECT"), HttpMethod::Connect);
		assert_eq!(HttpMethod::from_method_str("TRACE"), HttpMethod::Trace);
		// Anything non-standard (or wrong casing) collapses to Other.
		assert_eq!(HttpMethod::from_method_str("BREW"), HttpMethod::Other);
		assert_eq!(HttpMethod::from_method_str("get"), HttpMethod::Other);
	}

	#[test]
	fn http_version_from_str_classifies_wire_form() {
		assert_eq!(HttpVersion::from_version_str("HTTP/1.0"), HttpVersion::Http10);
		assert_eq!(HttpVersion::from_version_str("HTTP/1.1"), HttpVersion::Http11);
		assert_eq!(HttpVersion::from_version_str("HTTP/2.0"), HttpVersion::Http2);
		assert_eq!(HttpVersion::from_version_str("HTTP/3.0"), HttpVersion::Http3);
		// Bare-numeric form is also accepted because the hyper Debug impl
		// renders it that way for some HTTP/2 / HTTP/3 connections.
		assert_eq!(HttpVersion::from_version_str("2"), HttpVersion::Http2);
		assert_eq!(HttpVersion::from_version_str("HTTP/0.9"), HttpVersion::Other);
	}

	#[test]
	fn outcome_from_result_maps_success_and_error() {
		let ok: Result<u32, &str> = Ok(7);
		let err: Result<u32, &str> = Err("boom");
		assert_eq!(Outcome::from(&ok), Outcome::Success);
		assert_eq!(Outcome::from(&err), Outcome::Error);
	}

	mod network_ctx_from_session {
		use std::sync::Arc;

		use super::super::NetworkBytesEventCtx;
		use crate::dbs::Session;
		use crate::iam::{Auth, Role};

		#[test]
		fn root_user_populates_user() {
			let sess = Session {
				au: Arc::new(Auth::for_root(Role::Owner)),
				..Session::default()
			}
			.with_ns("acme")
			.with_db("prod");
			let ctx = NetworkBytesEventCtx::from_session(&sess);
			assert_eq!(ctx.namespace.as_deref(), Some("acme"));
			assert_eq!(ctx.database.as_deref(), Some("prod"));
			// `Auth::for_root` sets the actor id to `system_auth`.
			assert_eq!(ctx.user.as_deref(), Some("system_auth"));
		}

		#[test]
		fn record_principal_collapses_to_sentinel() {
			// Use a record-shaped principal id; the helper MUST NOT
			// surface it verbatim because record ids are unbounded.
			let sess = Session {
				au: Arc::new(Auth::for_record("user:abc123".to_owned(), "acme", "prod", "web")),
				..Session::default()
			};
			let ctx = NetworkBytesEventCtx::from_session(&sess);
			assert_eq!(ctx.user.as_deref(), Some("<record>"));
			assert!(
				!ctx.user.as_ref().unwrap().contains("abc123"),
				"record id leaked into ctx.user: {:?}",
				ctx.user
			);
		}

		#[test]
		fn anonymous_session_leaves_user_none() {
			let sess = Session::default();
			let ctx = NetworkBytesEventCtx::from_session(&sess);
			assert!(ctx.user.is_none());
		}

		#[test]
		fn ns_db_carry_through_verbatim() {
			let sess = Session::default().with_ns("a-ns").with_db("a-db");
			let ctx = NetworkBytesEventCtx::from_session(&sess);
			assert_eq!(ctx.namespace.as_deref(), Some("a-ns"));
			assert_eq!(ctx.database.as_deref(), Some("a-db"));
		}
	}

	mod tenant_identity_projections {
		//! Coverage for the `TenantIdentity::to_{rpc,auth}_ctx`
		//! projections used by the RPC dispatch site.
		//!
		//! These projections preserve the
		//! [`super::super::TenantIdentity::from_session`] collapsing
		//! rule (anonymous -> `None`, record-access -> `<record>`
		//! sentinel, else -> actor id) so per-tenant dimensional
		//! dashboards and audit destinations never receive raw
		//! record-access principal ids and never see synthetic
		//! empty-string users for unauthenticated traffic.

		use std::sync::Arc;

		use super::super::TenantIdentity;
		use crate::dbs::Session;
		use crate::iam::{Auth, Role};

		#[test]
		fn rpc_ctx_collapses_record_principal() {
			let sess = Session {
				au: Arc::new(Auth::for_record("user:abc123".to_owned(), "acme", "prod", "web")),
				..Session::default()
			};
			let identity = TenantIdentity::from_session(&sess);
			let rpc = identity.to_rpc_ctx();
			assert_eq!(rpc.user.as_deref(), Some("<record>"));
			assert!(
				!rpc.user.as_ref().unwrap().contains("abc123"),
				"record id leaked into rpc ctx.user: {:?}",
				rpc.user
			);
		}

		#[test]
		fn auth_ctx_collapses_record_principal() {
			let sess = Session {
				au: Arc::new(Auth::for_record("user:abc123".to_owned(), "acme", "prod", "web")),
				..Session::default()
			};
			let identity = TenantIdentity::from_session(&sess);
			let auth = identity.to_auth_ctx();
			assert_eq!(auth.user.as_deref(), Some("<record>"));
			assert!(
				!auth.user.as_ref().unwrap().contains("abc123"),
				"record id leaked into auth ctx.user: {:?}",
				auth.user
			);
		}

		#[test]
		fn projections_drop_user_for_anonymous() {
			let sess = Session::default();
			let identity = TenantIdentity::from_session(&sess);
			let rpc = identity.to_rpc_ctx();
			let auth = identity.to_auth_ctx();
			assert!(rpc.user.is_none(), "anonymous session must not surface a user label");
			assert!(auth.user.is_none(), "anonymous session must not surface a user label");
		}

		#[test]
		fn projections_carry_root_user_id() {
			let sess = Session {
				au: Arc::new(Auth::for_root(Role::Owner)),
				..Session::default()
			}
			.with_ns("acme")
			.with_db("prod");
			let identity = TenantIdentity::from_session(&sess);
			let rpc = identity.to_rpc_ctx();
			let auth = identity.to_auth_ctx();
			assert_eq!(rpc.namespace.as_deref(), Some("acme"));
			assert_eq!(rpc.database.as_deref(), Some("prod"));
			assert_eq!(rpc.user.as_deref(), Some("system_auth"));
			assert_eq!(auth.user.as_deref(), Some("system_auth"));
		}
	}
}
