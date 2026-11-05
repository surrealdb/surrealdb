use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_channel::Sender;
use surrealdb_expr::parser::{Query as Ast, TopLevelExpr};
use surrealdb_rpc::args::extract_args;
use surrealdb_rpc::error::{
	bad_lq_config, invalid_params, method_not_allowed, method_not_found, session_exists,
	session_expired, session_not_found,
};
use surrealdb_rpc::{DbResult, Method, Token, check_protected_param};
use surrealdb_types::{HashMap, object};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::catalog::providers::{CatalogProvider, NamespaceProvider, RootProvider};
use crate::ctx::CancelHandle;
use crate::dbs::capabilities::{ExperimentalTarget, MethodTarget};
use crate::dbs::{
	AuthPrincipalSnapshot, QueryResult, QueryStreamItem, QueryStreamJob, QueryType, Session,
};
use crate::exec::Error as ExecError;
use crate::expr::statements::live::LiveFields;
use crate::expr::statements::{
	CreateStatement, DeleteStatement, InsertStatement, KillStatement, LiveStatement,
	RelateStatement, SelectStatement, UpdateStatement, UpsertStatement,
};
use crate::expr::{
	Data as SqlData, Expr, ExtPrefix, Fields, Function, FunctionCall, Literal, Model, Output,
};
use crate::kvs::{Datastore, Dialect, QueryRequest, QuerySource, ReadOnlyOptions, TransactionType};
use crate::observe::{
	AuthAction, AuthEvent, AuthEventSafe, AuthScope, Outcome, RpcEvent, RpcEventSafe,
	TenantIdentity,
};
use crate::rpc::error::{query_timeout_error, types_error_from_anyhow};
use crate::rpc::sql_dialect;
use crate::types::{
	PublicArray, PublicRecordIdKey, PublicUuid, PublicValue, PublicVariables, SurrealValue,
};

/// utility function converting a `Value::String` into a `Expr::Table`
fn value_to_table(value: PublicValue) -> Expr {
	if let PublicValue::String(s) = value {
		Expr::Table(surrealdb_strand::TableName::new(s))
	} else {
		Expr::from_surface_value(value)
	}
}

/// returns if the expression returns a singular value when selected.
///
/// As this rpc is some what convuluted the singular conditions is not the same
/// for all cases.
fn singular(value: &PublicValue) -> bool {
	if matches!(value, PublicValue::Object(_)) {
		true
	} else if let PublicValue::RecordId(t) = value {
		!matches!(t.key, PublicRecordIdKey::Range(_))
	} else {
		false
	}
}

/// Map an RPC [`Method`] to an [`AuthAction`] when it represents an auth
/// lifecycle operation. Returns `None` for non-auth methods so the caller can
/// skip emitting an [`AuthEvent`] without branching on every variant.
const fn method_to_auth_action(method: Method) -> Option<AuthAction> {
	match method {
		Method::Signup => Some(AuthAction::Signup),
		Method::Signin => Some(AuthAction::Signin),
		Method::Authenticate => Some(AuthAction::Authenticate),
		Method::Refresh => Some(AuthAction::Refresh),
		Method::Invalidate => Some(AuthAction::Invalidate),
		Method::Revoke => Some(AuthAction::Revoke),
		Method::Unknown
		| Method::Ping
		| Method::Info
		| Method::Use
		| Method::Reset
		| Method::Kill
		| Method::Live
		| Method::Set
		| Method::Unset
		| Method::Select
		| Method::Insert
		| Method::Create
		| Method::Upsert
		| Method::Update
		| Method::Merge
		| Method::Patch
		| Method::Delete
		| Method::Version
		| Method::Query
		| Method::Gql
		| Method::Graphql
		| Method::Pgsql
		| Method::Mysql
		| Method::Relate
		| Method::Run
		| Method::InsertRelation
		| Method::Attach
		| Method::Sessions
		| Method::Detach
		| Method::Begin
		| Method::Commit
		| Method::Cancel
		| Method::QueryStream
		| Method::QueryCancel
		| _ => None,
	}
}

/// Deletes the live queries an execution registered after its caller had
/// already been answered.
///
/// An execution that outlives its wall-clock guard carries on detached, so that
/// it can finalise its transaction rather than be dropped holding it. Its
/// caller has been given a timeout by then, and no transport will ever see the
/// results, so a `LIVE SELECT` it commits afterwards has nobody to register it:
/// notifications for it would be produced and silently discarded for the
/// lifetime of the datastore, and no cleanup path could find it. Deleting the
/// rows is what keeps it from becoming that orphan.
pub(crate) async fn settle_detached_live_queries(
	kvs: &Datastore,
	results: Result<Vec<QueryResult>, surrealdb_types::Error>,
) {
	let Ok(results) = results else {
		return;
	};
	let orphans: Vec<Uuid> = results
		.iter()
		.filter(|result| matches!(result.query_type, QueryType::Live))
		.filter_map(|result| match &result.result {
			Ok(PublicValue::Uuid(id)) => Some(id.into_inner()),
			_ => None,
		})
		.collect();
	if !orphans.is_empty()
		&& let Err(err) = kvs.delete_queries(orphans).await
	{
		error!(
			target: "surrealdb::core::rpc",
			"Error cleaning up the live queries of a detached streaming query: {err}"
		);
	}
}

/// How long a streaming execution is given to stop itself after its wall-clock
/// timeout has tripped its cancel handle, before it is abandoned.
///
/// A cooperative stop needs the execution to reach a yield point. One parked
/// sending into a full `items` channel reaches none until its consumer drains,
/// so this bounds the wait rather than trusting the consumer to.
const QUERY_STREAM_STOP_GRACE: Duration = Duration::from_secs(5);

/// The variable the `select` method reads a single record through, the name
/// the SDK's own `select` binds it under.
const SELECT_RECORD_PARAM: &str = "_record_id";

/// The session a streaming execution's `LIVE SELECT`s may be registered
/// against, held so that registering them cannot race the teardown that would
/// have removed them.
///
/// A buffered query registers its live queries while still holding the session
/// read guard it ran under, so an auth-lifecycle operation — `invalidate`,
/// `reset`, a `signin` that changes principal — either runs first and is
/// followed by a registration under the *new* principal, or runs second and its
/// `cleanup_lqs` finds the registration and removes it. A streaming query
/// cannot hold that guard: a stream lives as long as its consumer reads, and
/// holding it would block every `use` / `set` / `signin` on the session behind a
/// slow client. So it drops the guard and takes this at the end instead.
///
/// Two things have to be true, and only both together close the window:
///
/// - The session must still be acting as the principal the execution ran as. Checking that the
///   session merely still *exists* is not enough — `invalidate` clears the authentication and keeps
///   the session, so its `cleanup_lqs` would sweep an empty map and the registration would land
///   afterwards, delivering change data under an authorization that had been torn down. That is the
///   shape GHSA-2xrp-m9c6-75rj describes.
/// - The guard must be *held* across the registration. Releasing it between the check and the
///   insert reopens the same window from the other side: a teardown could pass through the gap,
///   sweep nothing, and leave the registration behind it.
///
/// `None` means the live queries have no owner and must be deleted rather than
/// registered: the session has gone, or it is no longer the principal that
/// created them. A caller that already told its consumer about an id it must now
/// discard has to say so — the id will never deliver.
///
/// The returned guard is why [`RpcProtocol::handle_live`] must not acquire the
/// session lock.
pub async fn live_query_owner(
	sessions: &HashMap<Uuid, Arc<RwLock<Session>>>,
	session_id: Uuid,
	principal: &AuthPrincipalSnapshot,
) -> Option<tokio::sync::OwnedRwLockReadGuard<Session>> {
	let session = sessions.get(&session_id)?.read_owned().await;
	(!principal.differs_from(&session)).then_some(session)
}

/// Whether the `--query-timeout` wall-clock guard around
/// [`RpcProtocol::dispatch`] may cut `method` short.
///
/// Exempt are the methods whose commit the client must learn of, because a
/// future dropped mid-flight may have committed it. Transaction control
/// (`begin` / `commit` / `cancel`, see `Method::is_transaction_control`) is
/// one. So are `attach` and `detach`: a create committed but not reported
/// leaves the client an id that every retry finds taken, and a delete committed
/// but not reported leaves it a session that every retry finds gone. None of
/// them runs user-defined SurrealQL, so exempting them opens no unbounded work.
/// A `pgsql` or `mysql` call that holds a transaction-control entry is exempt
/// for the same reason as `commit`.
pub(crate) fn bounded_by_query_timeout(method: Method, params: &PublicArray) -> bool {
	let dialect = if method == Method::Pgsql {
		Some(Dialect::PgSql)
	} else if method == Method::Mysql {
		Some(Dialect::MySql)
	} else {
		None
	};
	if let Some(dialect) = dialect
		&& sql_dialect::has_transaction_control(params, dialect)
	{
		return false;
	}
	!method.is_transaction_control() && !matches!(method, Method::Attach | Method::Detach)
}

#[expect(async_fn_in_trait)]
pub trait RpcProtocol {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore;
	/// The datastore for this RPC interface as a shared handle.
	///
	/// Needed by machinery that captures the datastore beyond the borrow of a
	/// single call -- notably GraphQL schema generation, whose resolvers hold
	/// the datastore in the `async_graphql` request context.
	fn kvs_arc(&self) -> Arc<Datastore>;
	/// The version information for this RPC context
	fn version_data(&self) -> DbResult;

	/// Optional connection-level cancellation handle plumbed into the
	/// executor's [`crate::ctx::Context`]. When the handle is tripped the
	/// executor's `done`-walks short-circuit with `Reason::Canceled` at
	/// the next yield point (statement boundary, iterator hot loop check,
	/// HNSW/DiskANN search) and the transaction is finalised on the
	/// executor's normal error path. Bare-await sites (e.g. `SLEEP`)
	/// `select!` against the handle's awaitable view, so they are
	/// interrupted instead of running to completion.
	///
	/// The WebSocket implementation returns its connection cancel handle
	/// so a client disconnect cancels in-flight queries cleanly. Stateless
	/// implementations (HTTP RPC) return `None` and inherit the
	/// pre-cancellation behaviour.
	fn cancel_handle(&self) -> Option<CancelHandle> {
		None
	}

	/// The encoding this transport writes responses in, when it can splice
	/// rows encoded by the executor into them: `query` then answers with
	/// [`DbResult::Encoded`], whose rows were never built as public values.
	/// `None` answers with values, which is what a caller that reads results
	/// (the embedded SDK) needs.
	///
	/// By default, the format a stateless transport named for the request
	/// being dispatched ([`with_response_format`]); a transport with one
	/// format per connection returns it.
	fn row_format(&self) -> Option<surrealdb_rpc::RowFormat> {
		RESPONSE_FORMAT.try_with(|format| *format).ok().flatten()
	}

	// ------------------------------
	// Sessions
	// ------------------------------

	/// A pointer to all active sessions
	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>>;

	/// Registers a new session with the given ID
	async fn attach(&self, session_id: Uuid) -> Result<DbResult, surrealdb_types::Error> {
		if self.session_map().contains_key(&session_id) {
			return Err(session_exists(session_id));
		}
		let mut session = Session::default().with_rt(Self::LQ_SUPPORT);
		session.id = Some(session_id);
		self.session_map().insert(session_id, Arc::new(RwLock::new(session)));
		Ok(DbResult::Other(PublicValue::None))
	}

	/// Detaches a session from the given ID
	async fn detach(&self, session_id: Uuid) -> Result<DbResult, surrealdb_types::Error> {
		self.del_session(&session_id).await?;
		Ok(DbResult::Other(PublicValue::None))
	}

	/// The current session for this RPC context.
	///
	/// A session absent from the in-memory [`session_map`](Self::session_map)
	/// is rehydrated from durable storage via
	/// [`load_session`](Self::load_session) and reinstalled, so a client can
	/// resume an attached session after the node or isolate that held it is
	/// gone — the case that matters for edge deployments, where an idle
	/// Durable Object or Worker is evicted between requests. This branch is
	/// const-gated to nothing unless
	/// [`PERSIST_SESSIONS`](Self::PERSIST_SESSIONS) is set, so in-memory-only
	/// implementations keep the plain map lookup.
	async fn get_session(&self, id: &Uuid) -> Result<Arc<RwLock<Session>>, surrealdb_types::Error> {
		if let Some(session) = self.session_map().get(id) {
			return Ok(session);
		}
		if Self::PERSIST_SESSIONS
			&& let Some(restored) = self.load_session(id).await
		{
			self.set_session(*id, Arc::new(RwLock::new(restored)));
			if let Some(session) = self.session_map().get(id) {
				return Ok(session);
			}
		}
		Err(session_not_found(*id))
	}

	/// Stores a session for the given ID
	fn set_session(&self, id: Uuid, session: Arc<RwLock<Session>>) {
		self.session_map().insert(id, session);
	}

	/// Deletes a session.
	///
	/// The durable copy is removed **first**, and a failure there aborts the
	/// teardown with the local session left intact, so a caller (e.g.
	/// [`detach`](Self::detach) / logout) never reports success while a
	/// durable copy survives that could be rehydrated on this or another node.
	/// The durable removal is const-gated to nothing for in-memory-only
	/// implementations, whose teardown cannot fail.
	async fn del_session(&self, id: &Uuid) -> Result<(), surrealdb_types::Error> {
		// Remove the durable copy first, propagating any failure before we
		// touch local state — otherwise a `detach`/logout could return success
		// with the session still resurrectable.
		if Self::PERSIST_SESSIONS {
			self.forget_session(id).await?;
		}
		// Drop the in-memory session
		self.session_map().remove(id);
		// Cancel any live queries owned by this session
		self.cleanup_lqs(id).await;
		// Cancel any open transactions owned by this session
		self.cleanup_txns(id).await;
		Ok(())
	}

	/// Lists all sessions
	async fn sessions(&self) -> Result<DbResult, surrealdb_types::Error> {
		let array = self
			.session_map()
			.to_vec()
			.into_iter()
			.map(|(key, _)| PublicValue::Uuid(PublicUuid::from(key)))
			.collect();
		Ok(DbResult::Other(PublicValue::Array(array)))
	}

	// ------------------------------
	// Durable sessions
	// ------------------------------

	/// Whether this implementation persists attached sessions to durable
	/// storage so they outlive the process (or serverless isolate) that
	/// holds `session_map`. Off by default: sessions are in-memory only, and
	/// the three hooks below are never called, so
	/// [`get_session`](Self::get_session), [`execute`](Self::execute), and
	/// [`del_session`](Self::del_session) behave exactly as before.
	///
	/// When on, the session lifecycle mirrors to durable storage:
	/// [`get_session`](Self::get_session) rehydrates a session missing from
	/// `session_map` via [`load_session`](Self::load_session), so a client
	/// transparently resumes an attached session after the node or isolate
	/// that attached it is gone — the case that matters for edge deployments,
	/// where an idle Durable Object or Worker is evicted between requests.
	/// [`execute`](Self::execute) writes the session back via
	/// [`persist_session`](Self::persist_session) when a method changed it,
	/// and [`del_session`](Self::del_session) removes the durable copy via
	/// [`forget_session`](Self::forget_session).
	///
	/// This seam is about session *durability* only; it performs no
	/// request-level authorization. As in the in-memory model, a session id
	/// grants access to that session, so an implementation reachable by
	/// untrusted callers is responsible for its own per-request check (e.g.
	/// binding an authenticated session to the principal that authenticated
	/// it) — persistence neither adds nor removes that obligation.
	///
	/// The persist happens after the handler releases the session write lock,
	/// so an implementation that enables this **must serialize dispatch per
	/// session** (one in-flight RPC per session id); otherwise two concurrent
	/// mutations of the same session can persist out of order and drop the
	/// losing one (e.g. an `invalidate` racing a `signin`). A per-connection
	/// runtime — one Durable Object or Worker per session — satisfies this by
	/// construction.
	///
	/// Every durability branch is gated on this const, so an implementation
	/// whose const is `false` pays nothing for it.
	const PERSIST_SESSIONS: bool = false;

	/// Load a session from durable storage. Called by
	/// [`get_session`](Self::get_session) on a `session_map` miss to
	/// rehydrate an evicted session. Returns `None` (the default) for
	/// in-memory-only implementations, and is only ever called when
	/// [`PERSIST_SESSIONS`](Self::PERSIST_SESSIONS) is set.
	async fn load_session(&self, _id: &Uuid) -> Option<Session> {
		None
	}

	/// Persist a session that a method changed, so the change outlives
	/// eviction of `session_map`. The default is a no-op, and it is only ever
	/// called when [`PERSIST_SESSIONS`](Self::PERSIST_SESSIONS) is set.
	async fn persist_session(&self, _id: &Uuid, _session: &Session) {}

	/// Remove a session's durable copy, called from
	/// [`del_session`](Self::del_session) so teardown (`detach`, logout) is
	/// symmetric with persistence — a deleted session must not resurrect on
	/// a later [`load_session`](Self::load_session). An `Err` aborts the
	/// teardown so the caller does not report success while the durable copy
	/// survives; unlike [`persist_session`](Self::persist_session) (which is
	/// best-effort — a lost persist merely fails to save state), a lost
	/// forget is fail-open and must be surfaced. The default is a no-op that
	/// succeeds, and it is only ever called when
	/// [`PERSIST_SESSIONS`](Self::PERSIST_SESSIONS) is set.
	async fn forget_session(&self, _id: &Uuid) -> Result<(), surrealdb_types::Error> {
		Ok(())
	}

	// ------------------------------
	// Transactions
	// ------------------------------

	/// Retrieves a transaction by ID
	async fn get_tx(
		&self,
		_id: Uuid,
	) -> Result<Arc<crate::kvs::Transaction>, surrealdb_types::Error> {
		Err(method_not_allowed(Method::Unknown.to_string()))
	}

	/// Stores a transaction
	async fn set_tx(
		&self,
		_id: Uuid,
		_tx: Arc<crate::kvs::Transaction>,
	) -> Result<(), surrealdb_types::Error> {
		Err(method_not_found(Method::Unknown.to_string()))
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled by default
	const LQ_SUPPORT: bool = false;

	/// Handles the execution of a LIVE statement.
	///
	/// `namespace` and `database` are snapshotted from the registering
	/// session by the caller (`run_query`) using the read guard it
	/// already holds, and threaded down here so the implementation does
	/// NOT re-lock the same `RwLock<Session>` -- that would be a
	/// recursive read on a write-preferring lock and can deadlock against
	/// any concurrent session-mutating RPC on the same WebSocket
	/// (signin / signup / authenticate / set / unset / yuse / refresh /
	/// invalidate / revoke / reset).
	///
	/// That rule is load-bearing beyond the deadlock: a streaming caller
	/// registers with the read guard still held, which is what stops a
	/// registration racing the teardown that would have removed it. See
	/// [`live_query_owner`].
	///
	/// There is no counterpart hook for ending a registration. A statement that
	/// ends a subscription does not report the id it ended -- `KILL` resolves to
	/// `NONE`, and removing a table, database or namespace, or revoking a
	/// principal, ends subscriptions without naming them at all. The only signal
	/// carrying the id is the `Action::Killed` notification, so an implementation
	/// must drop the registration when it dispatches one.
	fn handle_live(
		&self,
		_lqid: &Uuid,
		_session_id: Uuid,
		_namespace: Option<String>,
		_database: Option<String>,
	) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_live function must be implemented if LQ_SUPPORT = true") }
	}

	/// Handles the cleanup of live queries
	fn cleanup_lqs(&self, session_id: &Uuid) -> impl std::future::Future<Output = ()> + Send;

	/// Handles the cleanup of all live queries
	fn cleanup_all_lqs(&self) -> impl std::future::Future<Output = ()> + Send;

	/// Handles the cleanup (cancellation) of any open transactions belonging to
	/// a specific session, run when the session is detached or reset. Default
	/// no-op: only transports that hold client-managed transactions (WebSocket)
	/// override this.
	fn cleanup_txns(&self, _session_id: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async {}
	}

	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation:
	/// [`dispatch`](Self::dispatch) runs it, then
	/// [`record_completion`](Self::record_completion) reports its outcome.
	///
	/// `session` is the resolved session ID (always valid). `client_session`
	/// is the raw value from the client request (`None` when the client did
	/// not specify one). Session-management methods (`attach`, `detach`) use
	/// `client_session` to reject calls with no explicit session ID.
	///
	/// A transport whose response can still fail after the method ran calls
	/// the two halves itself, recording the outcome once the response is
	/// settled, so observers see the outcome the client receives.
	async fn execute(
		&self,
		txn: Option<Uuid>,
		session: Uuid,
		client_session: Option<Uuid>,
		method: Method,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let start = web_time::Instant::now();
		let result = self.dispatch(txn, session, client_session, method, params).await;
		self.record_completion(session, method, start, &result).await;
		result
	}

	/// Runs a method on this RPC implementation, without reporting its
	/// outcome (see [`execute`](Self::execute)). Arguments are as for
	/// [`execute`](Self::execute).
	#[cfg_attr(feature = "observability", tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.execute",
		skip_all,
		fields(rpc.method = method.to_str(), rpc.session = %session)
	))]
	async fn dispatch(
		&self,
		txn: Option<Uuid>,
		session: Uuid,
		client_session: Option<Uuid>,
		method: Method,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let bounded = bounded_by_query_timeout(method, &params);
		let dispatch = async {
			// Check if capabilities allow executing the requested RPC method
			if !self.kvs().allows_rpc_method(&MethodTarget {
				method,
			}) {
				warn!("Capabilities denied RPC method call attempt, target: '{method}'");
				return Err(method_not_allowed(method.to_string()));
			}
			// Snapshot the session before dispatch so a change can be detected
			// and persisted afterwards (below). The outer `Some` marks "this
			// request is tracked for persistence"; the inner `Option` is the
			// session's pre-dispatch value, or `None` if it did not exist yet
			// (an `attach` about to create it). Only client-named sessions are
			// durable — a per-request ephemeral (no `client_session`) is never
			// persisted — and `get_session` rehydrates an evicted session on
			// the way, so this also covers the "evicted, first request" case.
			// Const-gated to nothing for in-memory-only implementations, which
			// never clone.
			let tracked: Option<Option<Session>> =
				if Self::PERSIST_SESSIONS && client_session.is_some() {
					Some(match self.get_session(&session).await {
						Ok(lock) => Some(lock.read().await.clone()),
						Err(_) => None,
					})
				} else {
					None
				};
			// Execute the desired method. The session methods are boxed: each holds a larger
			// state than a query (a signin or an authenticate several times one), and inline they
			// would size the future of every request, which a transport copies per message.
			let dispatched = match method {
				Method::Ping => Ok(DbResult::Other(PublicValue::None)),
				Method::Info => self.info(txn, session).await,
				Method::Use => Box::pin(self.yuse(session, params)).await,
				Method::Signup => Box::pin(self.signup(session, params)).await,
				Method::Signin => Box::pin(self.signin(session, params)).await,
				Method::Authenticate => Box::pin(self.authenticate(session, params)).await,
				Method::Refresh => Box::pin(self.refresh(session, params)).await,
				Method::Invalidate => self.invalidate(session).await,
				Method::Revoke => Box::pin(self.revoke(params)).await,
				Method::Reset => self.reset(session).await,
				Method::Kill => self.kill(txn, session, params).await,
				Method::Live => self.live(txn, session, params).await,
				Method::Set => self.set(session, params).await,
				Method::Unset => self.unset(session, params).await,
				Method::Query => self.query(txn, session, params).await,
				Method::Gql => self.gql(txn, session, params).await,
				Method::Graphql => self.graphql(txn, session, params).await,
				Method::Pgsql => self.pgsql(txn, session, params).await,
				Method::Mysql => self.mysql(txn, session, params).await,
				Method::Version => self.version(txn, params).await,
				Method::Begin => self.begin(txn, session, params).await,
				Method::Commit => self.commit(txn, session, params).await,
				Method::Cancel => self.cancel(txn, session, params).await,
				Method::Sessions => self.sessions().await,
				Method::Attach => match client_session {
					Some(id) => self.attach(id).await,
					None => Err(invalid_params("Expected a session ID")),
				},
				Method::Detach => match client_session {
					Some(id) => self.detach(id).await,
					None => Err(invalid_params("Expected a session ID")),
				},
				// Deprecated methods
				Method::Select => self.select(txn, session, params).await,
				Method::Insert => self.insert(txn, session, params).await,
				Method::Create => self.create(txn, session, params).await,
				Method::Upsert => self.upsert(txn, session, params).await,
				Method::Update => self.update(txn, session, params).await,
				Method::Merge => self.merge(txn, session, params).await,
				Method::Patch => self.patch(txn, session, params).await,
				Method::Delete => self.delete(txn, session, params).await,
				Method::Relate => self.relate(txn, session, params).await,
				Method::Run => self.run(txn, session, params).await,
				Method::InsertRelation => self.insert_relation(txn, session, params).await,
				// A streaming query is answered with a sequence of frames, so
				// it cannot come through this single-response dispatch: a
				// transport that supports it serves the method before ever
				// reaching `execute`, and every other transport reports it
				// absent.
				Method::QueryStream => Err(method_not_found(method.to_string())),
				Method::QueryCancel => self.query_cancel(params).await,
				Method::Unknown => Err(method_not_found(method.to_string())),
				_ => Err(method_not_found(method.to_string())),
			};
			// On success, persist the session if dispatch changed it (or
			// created it — `attach`). Comparing against the pre-dispatch
			// snapshot means there is no list of "mutating methods" to keep
			// in sync with the handlers: whatever alters the session is
			// written and a read-only method writes nothing. A session the
			// handler removed (`detach`) is gone from the map, so the lookup
			// misses and deletion rides `del_session` instead. The snapshot
			// is taken outside the handler's write lock, so correctness
			// relies on the per-session dispatch serialization
			// `PERSIST_SESSIONS` requires.
			if dispatched.is_ok()
				&& let Some(before) = tracked
				&& let Ok(lock) = self.get_session(&session).await
			{
				let after = lock.read().await;
				if before.as_ref() != Some(&*after) {
					self.persist_session(&session, &after).await;
				}
			}
			dispatched
		};
		// Apply an MCP-style wall-clock guard around the dispatch, reusing the
		// global `--query-timeout` value as its duration (default off, so no
		// behaviour change unless an operator sets it). The deadline plumbed
		// into the executor via `Datastore::setup_ctx` only fires at yield
		// points and does not bound the GraphQL engine's own work; this
		// wall-clock backstop bounds the whole method call. The methods whose
		// commit must be reported are exempt (see `bounded_by_query_timeout`);
		// the statements executed inside an explicit transaction are still
		// guarded individually.
		match self.kvs().query_timeout() {
			Some(timeout) if bounded => match common::time::timeout(timeout, dispatch).await {
				Ok(inner) => inner,
				Err(_elapsed) => {
					warn!(
						target: "surrealdb::core::rpc",
						timeout = ?timeout,
						method = method.to_str(),
						"RPC method exceeded the configured query timeout"
					);
					// A call that ran in an envelope transaction did not touch the session's.
					if method == Method::Pgsql
						&& txn.is_none() && let Ok(lock) = self.get_session(&session).await
					{
						sql_dialect::fail_open_transaction(&lock).await;
					}
					Err(query_timeout_error(timeout))
				}
			},
			_ => dispatch.await,
		}
	}

	/// Fires an [`crate::observe::RpcEvent`] for `method`, run on `session`
	/// from `start`, whatever its `result`. The safe half of the event carries
	/// only the bounded [`Method`] and the outcome label; the context half
	/// carries `session`'s namespace/database/user as it stands when this
	/// runs (ignored by the community metrics observer but consumed by
	/// enterprise audit sinks). Auth-related methods additionally fan out an
	/// [`AuthEvent`] so auth attempt counters can be broken out by
	/// action/scope/outcome.
	async fn record_completion(
		&self,
		session: Uuid,
		method: Method,
		start: web_time::Instant,
		result: &Result<DbResult, surrealdb_types::Error>,
	) {
		let observer = self.kvs().observer();
		// Nothing is observing, so none of the event context below is ever read.
		// Building it costs a session lookup, a read-lock acquisition and several
		// `String` clones on every single RPC, which is the default configuration
		// for an embedded datastore and for a server with metrics disabled.
		if observer.is_noop() {
			return;
		}
		let outcome = Outcome::from(result);
		// Resolve session context for the observer event. An unknown
		// session ID yields an empty `TenantIdentity` rather than an
		// error, keeping the metrics path infallible. We capture the
		// scope inside the same lock acquisition to avoid a second
		// lookup for auth events. Routing through
		// `TenantIdentity::from(&Session)` applies the documented
		// record/anonymous collapsing rule (anon -> `None`,
		// record-access -> `<record>` sentinel) so per-tenant /
		// dimensional dashboards stay bounded and audit destinations
		// never receive raw record-access principal ids.
		let (identity, scope) = match self.get_session(&session).await {
			Ok(session_lock) => {
				let s = session_lock.read().await;
				let scope = AuthScope::from(s.au.level());
				(TenantIdentity::from(&*s), scope)
			}
			Err(_) => (TenantIdentity::default(), AuthScope::None),
		};
		// Classify error-outcome events using the structured
		// [`surrealdb_types::ErrorDetails`] taxonomy so the
		// `error_class` attribute on `surrealdb.rpc.*` and
		// `surrealdb.auth.*` carries a real label
		// (`auth` / `permission` / `parse` / `client` / `txn_conflict` /
		// `ctx_cancelled` / `timeout` / `internal`) rather than the
		// `-` sentinel.
		let error_class =
			result.as_ref().err().map(crate::observe::error_class::classify_types_error);
		observer.on_rpc_complete(&RpcEvent {
			safe: RpcEventSafe {
				method,
				outcome,
				duration: start.elapsed(),
				error_class,
			},
			ctx: identity.to_rpc_ctx(),
		});
		if let Some(action) = method_to_auth_action(method) {
			observer.on_auth_event(&AuthEvent {
				safe: AuthEventSafe {
					action,
					scope,
					outcome,
					error_class,
				},
				ctx: identity.to_auth_ctx(),
			});
		}
	}

	// ------------------------------
	// Methods for authentication
	// ------------------------------

	/// Handles the USE RPC method for switching namespace and database context.
	///
	/// This method supports three usage patterns:
	/// 1. **Explicit selection**: `USE ns "namespace" db "database"` - directly sets ns/db
	/// 2. **Partial selection**: `USE ns "namespace"` - sets ns while preserving or clearing db
	/// 3. **Default selection**: `USE` (empty call) - applies defaults from config or token
	///
	/// When called with no arguments (pattern 3), the behavior depends on session state:
	/// - If the session already has ns/db from token authentication (JWT claims), those are
	///   preserved
	/// - Otherwise, defaults from the database configuration are applied if available
	///
	/// Returns an object with the resulting `namespace` and `database` values, allowing
	/// clients (especially HTTP) to sync their local state with the server session.
	async fn yuse(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;

		// Check permissions with read lock
		{
			let session = session_lock.read().await;
			// Check if the user is allowed to query
			if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
				return Err(method_not_allowed(Method::Use.to_string()));
			}
		}

		// For both ns+db, string = change, null = unset, none = do nothing
		// We need to be able to adjust either ns or db without affecting the other
		// To be able to select a namespace, and then list resources in that namespace,
		// as an example
		let (ns, db) = extract_args::<(PublicValue, PublicValue)>(params.into_vec())
			.ok_or(invalid_params("Expected (ns, db)".to_string()))?;
		// Get a write lock on the session to modify it
		let mut session = session_lock.write().await;
		// SECURITY (GHSA-2v9j): tenant-boundary gate, mirroring the SurrealQL
		// `USE` handler (`dbs::executor`) and the embedded `process_use`. The RPC
		// `use` method dispatches here, so an authenticated Database-, Namespace-
		// or Record-level principal must be verified as authorized for the target
		// namespace/database before the session is pivoted; otherwise it could
		// switch into a foreign tenant and reach any table there with satisfiable
		// PERMISSIONS. Resolve the target the same way the mutation below does (a
		// string changes it, `None` leaves it, `Null` unsets it) and check the
		// authenticated Auth level, which is the source of truth. Root and
		// anonymous (`Level::No`, e.g. `use` before `signin`) principals are
		// unrestricted by `can_access_ns_db`, preserving the pre-signin pattern.
		{
			let resolved_ns = if let PublicValue::String(ns) = &ns {
				Some(ns.as_str())
			} else if let PublicValue::Null = &ns {
				None
			} else {
				session.ns.as_deref()
			};
			let resolved_db = if let PublicValue::String(db) = &db {
				Some(db.as_str())
			} else if let PublicValue::Null = &db {
				None
			} else {
				session.db.as_deref()
			};
			if let Some(ns_name) = resolved_ns {
				let db_name = resolved_db.unwrap_or_default();
				if !session.au.can_access_ns_db(ns_name, db_name) {
					let err = if session.au.level().ns() == Some(ns_name) {
						ExecError::DbNotAllowed {
							db: db_name.to_owned(),
						}
					} else {
						ExecError::NsNotAllowed {
							ns: ns_name.to_owned(),
						}
					};
					return Err(surrealdb_types::Error::not_allowed(err.to_string(), None));
				}
			}
		}
		// Empty USE call: apply defaults only if session doesn't already have ns/db
		if ns.is_none() && db.is_none() {
			// Skip applying defaults if ns is already set (e.g., from token authentication)
			if session.ns.is_none() {
				// Fetch defaults from database configuration
				let kvs = self.kvs();
				let tx = kvs
					.transaction(TransactionType::Write)
					.await
					.map_err(types_error_from_anyhow)?;
				let (ns, db) = if let Some(x) = match tx.get_default_config().await {
					Err(e) => {
						let _ = tx.cancel().await;
						return Err(types_error_from_anyhow(e));
					}
					Ok(v) => v,
				} {
					(x.namespace.clone(), x.database.clone())
				} else {
					(None, None)
				};

				// SECURITY (GHSA-2v9j): gate the resolved default the same way an
				// explicit `use` is gated, so an empty `use` cannot pivot a scoped
				// principal onto a foreign default tenant. Root and anonymous
				// principals are unrestricted by `can_access_ns_db`.
				if let Some(ns_name) = &ns {
					let db_name = db.as_deref().unwrap_or_default();
					if !session.au.can_access_ns_db(ns_name, db_name) {
						let _ = tx.cancel().await;
						let err = if session.au.level().ns() == Some(ns_name.as_str()) {
							ExecError::DbNotAllowed {
								db: db_name.to_owned(),
							}
						} else {
							ExecError::NsNotAllowed {
								ns: ns_name.to_owned(),
							}
						};
						return Err(surrealdb_types::Error::not_allowed(err.to_string(), None));
					}
				}

				if let Some(ns) = ns {
					match tx.get_or_add_ns(None, &ns).await {
						Err(e) => {
							let _ = tx.cancel().await;
							return Err(types_error_from_anyhow(e));
						}
						Ok(v) => v,
					};

					if let Some(db) = db {
						match tx.ensure_ns_db(None, &ns, &db).await {
							Err(e) => {
								let _ = tx.cancel().await;
								return Err(types_error_from_anyhow(e));
							}
							Ok(v) => v,
						};
						session.db = Some(db);
					}

					session.ns = Some(ns);
				}

				if let Err(e) = tx.commit().await {
					let _ = tx.cancel().await;
					return Err(types_error_from_anyhow(e));
				}
			}
		} else {
			// SECURITY: SDKs commonly call `use` before `signin`, so we
			// set the session context even when the caller cannot
			// authorize the implicit `DEFINE NAMESPACE` /
			// `DEFINE DATABASE`-equivalent creation. The auto-creation
			// itself is gated on that authorization; downstream
			// operations against a non-existent namespace surface a
			// clean `NsNotFound` rather than a silently auto-created
			// resource that the session was never allowed to create.
			// See `SECURITY_GUIDE.md` section 3.
			//
			// Update the selected namespace
			if let PublicValue::None = ns {
			} else if let PublicValue::Null = ns {
				session.ns = None;
			} else if let PublicValue::String(ns) = ns {
				let kvs = self.kvs();
				let tx = kvs
					.transaction(TransactionType::Write)
					.await
					.map_err(types_error_from_anyhow)?;
				// Cancel before returning: this check runs with `tx` already
				// open, so propagating its error directly would drop a
				// writeable transaction without committing or cancelling it,
				// tripping `Transactor::drop`'s "a transaction was dropped
				// without being committed or cancelled" error and leaving the
				// transaction to be reaped instead of rolled back promptly.
				let create =
					match kvs.should_materialize_ns_on_use(&tx, session.au.as_ref(), &ns).await {
						Ok(create) => create,
						Err(e) => {
							let _ = tx.cancel().await;
							return Err(types_error_from_anyhow(e));
						}
					};
				if create {
					run!(tx, tx.get_or_add_ns(None, &ns).await).map_err(types_error_from_anyhow)?;
				} else {
					let _ = tx.cancel().await;
				}
				session.ns = Some(ns);
			} else {
				return Err(invalid_params(format!("Expected ns to be string, got {ns:?}")));
			}
			// Update the selected database
			if let PublicValue::None = db {
			} else if let PublicValue::Null = db {
				session.db = None;
			} else if let PublicValue::String(db) = db {
				// SECURITY: a `use` call that sets `db` without a namespace
				// previously hit `.expect("namespace should be set")`, and the
				// crate's `panic = 'abort'` setting would have aborted the
				// whole server process — a one-RPC remote DoS. Return a clean
				// error instead.
				let Some(ns) = session.ns.clone() else {
					return Err(invalid_params(
						"Cannot set database without first selecting a namespace".to_string(),
					));
				};
				let kvs = self.kvs();
				let tx = kvs
					.transaction(TransactionType::Write)
					.await
					.map_err(types_error_from_anyhow)?;
				// Cancel before returning, as in the namespace branch above: a
				// bare `?` here would drop `tx` while it is still open. A
				// single `use(ns, db)` call passes through both branches, so
				// the unguarded form could leak two transactions per call.
				let create = match kvs
					.should_materialize_db_on_use(&tx, session.au.as_ref(), &ns, &db)
					.await
				{
					Ok(create) => create,
					Err(e) => {
						let _ = tx.cancel().await;
						return Err(types_error_from_anyhow(e));
					}
				};
				if create {
					run!(tx, tx.ensure_ns_db(None, &ns, &db).await)
						.map_err(types_error_from_anyhow)?;
				} else {
					let _ = tx.cancel().await;
				}
				session.db = Some(db);
			} else {
				return Err(invalid_params(format!("Expected db to be string, got {db:?}")));
			}
		}
		// Clear any residual database
		if session.ns.is_none() && session.db.is_some() {
			session.db = None;
		}
		// Log the session ns/db values for debugging
		trace!(
			"USE response: session_id={:?}, ns={:?}, db={:?}",
			session_id, session.ns, session.db
		);
		// Build the return value
		let value = PublicValue::from_t(object! {
			namespace: session.ns.clone(),
			database: session.db.clone(),
		});
		// Return the namespace and database
		Ok(DbResult::Other(value))
	}

	#[cfg_attr(feature = "observability", tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.signup",
		skip_all,
		fields(rpc.session = %session_id)
	))]
	async fn signup(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Process the method arguments
		let Some(PublicValue::Object(params)) = extract_args(params.into_vec()) else {
			return Err(invalid_params("Expected (params:object)".to_string()));
		};
		// Get a write lock on the session
		let session_lock = self.get_session(&session_id).await?;
		let mut session = session_lock.write().await;
		let snapshot = AuthPrincipalSnapshot::capture(&session);
		// Attempt signup, mutating the session
		let out: Result<PublicValue> =
			crate::iam::signup::signup(self.kvs(), &mut session, params.into())
				.await
				.map(SurrealValue::into_value);
		let principal_changed = snapshot.differs_from(&session);
		drop(session);
		if principal_changed {
			self.cleanup_lqs(&session_id).await;
		}
		// Return the signup result
		out.map(DbResult::Other).map_err(types_error_from_anyhow)
	}

	#[cfg_attr(feature = "observability", tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.signin",
		skip_all,
		fields(rpc.session = %session_id)
	))]
	async fn signin(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Process the method arguments
		let Some(PublicValue::Object(params)) = extract_args(params.into_vec()) else {
			return Err(invalid_params("Expected (params:object)".to_string()));
		};
		// Get a write lock on the session
		let session_lock = self.get_session(&session_id).await?;
		let mut session = session_lock.write().await;
		let snapshot = AuthPrincipalSnapshot::capture(&session);
		// Attempt signin, mutating the session
		let out: Result<PublicValue> =
			crate::iam::signin::signin(self.kvs(), &mut session, params.into())
				.await
				.map(SurrealValue::into_value);
		let principal_changed = snapshot.differs_from(&session);
		drop(session);
		if principal_changed {
			self.cleanup_lqs(&session_id).await;
		}
		// Return the signin result
		out.map(DbResult::Other).map_err(types_error_from_anyhow)
	}

	#[cfg_attr(feature = "observability", tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.authenticate",
		skip_all,
		fields(rpc.session = %session_id)
	))]
	async fn authenticate(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Process the method arguments
		let Some(PublicValue::String(token)) = extract_args(params.into_vec()) else {
			return Err(invalid_params("Expected (token:string)".to_string()));
		};
		// Get a write lock on the session
		let session_lock = self.get_session(&session_id).await?;
		let mut session = session_lock.write().await;
		let snapshot = AuthPrincipalSnapshot::capture(&session);
		// Log before authentication
		trace!(
			"Authenticate RPC: session_id={:?}, before: ns={:?}, db={:?}",
			session_id, session.ns, session.db
		);
		// Attempt authentication, mutating the session
		let out: Result<PublicValue> =
			crate::iam::verify::token(self.kvs(), &mut session, token.as_str())
				.await
				.map(|_| PublicValue::None);
		// Log after authentication
		trace!(
			"Authenticate RPC: session_id={:?}, after: ns={:?}, db={:?}",
			session_id, session.ns, session.db
		);
		let principal_changed = snapshot.differs_from(&session);
		drop(session);
		if principal_changed {
			self.cleanup_lqs(&session_id).await;
		}
		// Return nothing on success
		out.map(DbResult::Other).map_err(types_error_from_anyhow)
	}

	/// Refreshes an access token using a refresh token.
	///
	/// This RPC method implements the token refresh flow, allowing clients to
	/// obtain a new access token without re-authenticating. The method:
	///
	/// 1. Validates the provided token contains both access and refresh components
	/// 2. Uses the refresh token to authenticate and create new tokens
	/// 3. Revokes the old refresh token (single-use security model)
	/// 4. Updates the session with the new authentication state
	/// 5. Returns the new token pair to the client
	///
	/// # Arguments
	///
	/// * `session_id` - Optional session identifier for stateful connections
	/// * `params` - Array containing the token with both access and refresh components
	///
	/// # Returns
	///
	/// A new token containing fresh access and refresh tokens.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The token parameter is missing or invalid
	/// - The token doesn't contain a refresh component
	/// - The refresh token is invalid, expired, or already revoked
	async fn refresh(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		// Process the method arguments
		let unexpected = || invalid_params("Expected (token:Token)".to_string());
		let Some(value) = extract_args(params.into_vec()) else {
			return Err(unexpected());
		};
		let Ok(token) = Token::from_value(value) else {
			return Err(unexpected());
		};
		// Get a write lock on the session
		let session_lock = self.get_session(&session_id).await?;
		let mut session = session_lock.write().await;
		let snapshot = AuthPrincipalSnapshot::capture(&session);
		// Attempt token refresh, which will:
		// - Validate the refresh token
		// - Revoke the old refresh token
		// - Create new access and refresh tokens
		// - Update the session with the new authentication state
		let out: Result<PublicValue> = crate::iam::token::refresh(token, self.kvs(), &mut session)
			.await
			.map(Token::into_value);
		let principal_changed = snapshot.differs_from(&session);
		drop(session);
		if principal_changed {
			self.cleanup_lqs(&session_id).await;
		}
		// Return the new token pair
		out.map(DbResult::Other).map_err(types_error_from_anyhow)
	}

	async fn invalidate(&self, session_id: Uuid) -> Result<DbResult, surrealdb_types::Error> {
		// Get a write lock on the session
		let session_lock = self.get_session(&session_id).await?;
		let mut session = session_lock.write().await;
		// Clear the current session
		crate::iam::clear::clear(&mut session).map_err(types_error_from_anyhow)?;
		// Cleanup live queries so that the now-invalidated session no longer receives
		// notifications.
		self.cleanup_lqs(&session_id).await;
		// Return nothing on success
		Ok(DbResult::Other(PublicValue::None))
	}

	/// Revokes a refresh token, preventing it from being used to obtain new access tokens.
	///
	/// This RPC method explicitly invalidates a refresh token without affecting the
	/// current session. This is useful for:
	///
	/// - Logout operations where you want to prevent future token refreshes
	/// - Security events requiring immediate token invalidation
	/// - Explicit token lifecycle management
	///
	/// Unlike `invalidate()`, which clears the entire session, `revoke()` only
	/// invalidates the specific refresh token, allowing other sessions using
	/// different tokens to remain active.
	///
	/// # Arguments
	///
	/// * `params` - Array containing the token with the refresh token to revoke
	///
	/// # Returns
	///
	/// Returns nothing on success.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The token parameter is missing or invalid
	/// - The token doesn't contain a refresh component
	/// - The token doesn't contain valid namespace/database/access information
	async fn revoke(&self, params: PublicArray) -> Result<DbResult, surrealdb_types::Error> {
		// Process the method arguments
		let unexpected = || invalid_params("Expected (token:Token)".to_string());
		let Some(value) = extract_args(params.into_vec()) else {
			return Err(unexpected());
		};
		let Ok(token) = Token::from_value(value) else {
			return Err(unexpected());
		};
		// Revoke the refresh token by removing the grant record from the database.
		// This prevents the refresh token from being used to obtain new access tokens.
		crate::iam::token::revoke_refresh_token(token, self.kvs())
			.await
			.map_err(types_error_from_anyhow)?;
		// Return nothing on success
		Ok(DbResult::Other(PublicValue::None))
	}

	async fn reset(&self, session_id: Uuid) -> Result<DbResult, surrealdb_types::Error> {
		// Get a write lock on the session
		let session_lock = self.get_session(&session_id).await?;
		let mut session = session_lock.write().await;
		// Reset the current session
		crate::iam::reset::reset(&mut session);
		// Cleanup live queries
		self.cleanup_lqs(&session_id).await;
		// Cancel any open transactions owned by this session
		self.cleanup_txns(&session_id).await;
		// Return nothing on success
		Ok(DbResult::Other(PublicValue::None))
	}

	// ------------------------------
	// Methods for identification
	// ------------------------------

	async fn info(
		&self,
		_txn: Option<Uuid>,
		session_id: Uuid,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		let vars = Some(session.variables.clone());
		let mut res = self.kvs().execute("SELECT * FROM $auth", &session, vars).await?;

		let result = res.remove(0).result?;

		let first = result.first().unwrap_or_default();
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for setting variables
	// ------------------------------

	async fn set(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;

		// Check permissions with read lock
		{
			let session = session_lock.read().await;
			// Check if the user is allowed to query
			if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
				return Err(method_not_allowed(Method::Set.to_string()));
			}
		}

		// Process the method arguments
		let Some((PublicValue::String(key), val)) =
			extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
		else {
			return Err(invalid_params("Expected (key:string, value:Value)".to_string()));
		};

		// Get a write lock on the session
		let mut session = session_lock.write().await;

		if session.expired() {
			return Err(session_expired());
		}

		check_protected_param(&key)?;
		sql_dialect::check_not_transaction_state(&key)?;
		match val {
			None | Some(PublicValue::None) => session.variables.remove(key.as_str()),
			Some(val) => session.variables.insert(key, val),
		}

		// Return nothing
		Ok(DbResult::Other(PublicValue::Null))
	}

	async fn unset(
		&self,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;

		// Check permissions with read lock
		{
			let session = session_lock.read().await;
			// Check if the user is allowed to query
			if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
				return Err(method_not_allowed(Method::Unset.to_string()));
			}
		}

		// Process the method arguments
		let Some(PublicValue::String(key)) = extract_args(params.into_vec()) else {
			return Err(invalid_params("Expected (key)".to_string()));
		};

		check_protected_param(&key)?;
		sql_dialect::check_not_transaction_state(&key)?;

		// Get a write lock on the session
		let mut session = session_lock.write().await;
		session.variables.remove(key.as_str());

		Ok(DbResult::Other(PublicValue::Null))
	}

	// ------------------------------
	// Methods for live queries
	// ------------------------------

	async fn kill(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Kill.to_string()));
		}
		// Process the method arguments
		let (id,) = extract_args::<(PublicValue,)>(params.into_vec())
			.ok_or(invalid_params("Expected (id)".to_string()))?;

		// Build the runtime statement.
		let ast = Ast {
			expressions: vec![TopLevelExpr::Kill(KillStatement {
				id: Expr::from_surface_value(id),
			})],
		};
		// Specify the query parameters
		let vars = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), vars)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		Ok(DbResult::Other(res.remove(0).result?))
	}

	async fn live(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Live.to_string()));
		}
		// Process the method arguments
		let (what, diff) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params("Expected (what, diff)".to_string()))?;

		// If value is a strand, handle it as if it was a table.
		let what = if let PublicValue::String(x) = what {
			Expr::Table(surrealdb_strand::TableName::new(x))
		} else {
			Expr::from_surface_value(what)
		};

		let fields = if diff.unwrap_or_default().is_true() {
			LiveFields::Diff
		} else {
			LiveFields::Select(Fields::all())
		};

		// Build the runtime statement.
		let sql = LiveStatement {
			id: Uuid::nil(),
			node: Uuid::nil(),
			fields,
			what,
			cond: None,
			fetch: None,
		};
		let ast = Ast {
			expressions: vec![TopLevelExpr::Live(Box::new(sql))],
		};
		// Specify the query parameters
		let vars = Some(session.variables.clone());

		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), vars)
			.await
			.map_err(types_error_from_anyhow)?;

		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for selecting
	// ------------------------------

	async fn select(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Select.to_string()));
		}
		// Process the method arguments
		let (what,) = extract_args::<(PublicValue,)>(params.into_vec())
			.ok_or(invalid_params("Expected (what:Value)".to_string()))?;

		// If the what is a single record with a non range value, make it return only a
		// single result.
		let only = if let PublicValue::RecordId(ref x) = what {
			!x.key.is_range()
		} else {
			false
		};

		// Specify the query parameters
		let mut vars = session.variables.clone();

		// A single record is read through a variable, so the statement is one
		// text whichever record it names, kept compiled for later requests. If
		// value is a string, handle it as if it was a table.
		let what = if only && let PublicValue::RecordId(id) = what {
			vars.insert(SELECT_RECORD_PARAM, PublicValue::RecordId(id));
			Expr::Param(crate::expr::Param::from(SELECT_RECORD_PARAM.to_owned()))
		} else if let PublicValue::String(x) = what {
			Expr::Table(surrealdb_strand::TableName::new(x))
		} else {
			Expr::from_surface_value(what)
		};

		// Build the runtime statement.
		let sql = SelectStatement {
			only,
			fields: Fields::all(),
			what: vec![what],
			with: None,
			cond: None,
			omit: vec![],
			split: None,
			group: None,
			order: None,
			limit: None,
			start: None,
			fetch: None,
			version: Expr::Literal(Literal::None),
			timeout: Expr::Literal(Literal::None),
			explain: None,
			tempfiles: false,
			for_update: false,
		};
		let ast = Ast::single_expr(Expr::Select(Box::new(sql)));

		// Execute the query on the database
		let mut res =
			run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), Some(vars))
				.await
				.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for inserting
	// ------------------------------

	async fn insert(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Insert.to_string()));
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, PublicValue)>(params.into_vec())
			.ok_or(invalid_params("Expected (what:Value, data:Value)".to_string()))?;

		let into = if matches!(what, PublicValue::Null | PublicValue::None) {
			None
		} else if let PublicValue::Table(x) = what {
			Some(Expr::Table(surrealdb_strand::TableName::new(x.into_string())))
		} else if let PublicValue::String(x) = what {
			Some(Expr::Table(surrealdb_strand::TableName::new(x)))
		} else {
			Some(Expr::from_surface_value(what))
		};

		// Build the runtime statement.
		let sql = InsertStatement {
			into,
			data: SqlData::SingleExpression(Expr::from_surface_value(data)),
			output: Some(Output::After),
			ignore: false,
			update: None,
			timeout: Expr::Literal(Literal::None),
			relation: false,
		};
		let ast = Ast::single_expr(Expr::Insert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	async fn insert_relation(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::InsertRelation.to_string()));
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, PublicValue)>(params.to_vec())
			.ok_or(invalid_params("Expected (what, data)".to_string()))?;

		let table_name = if matches!(what, PublicValue::Null | PublicValue::None) {
			None
		} else if let PublicValue::Table(x) = what {
			Some(Expr::Table(surrealdb_strand::TableName::new(x.into_string())))
		} else if let PublicValue::String(x) = what {
			Some(Expr::Table(surrealdb_strand::TableName::new(x)))
		} else {
			Some(Expr::from_surface_value(what))
		};

		let data = SqlData::SingleExpression(Expr::from_surface_value(data));

		// Build the runtime statement.
		let sql = InsertStatement {
			relation: true,
			into: table_name,
			data,
			output: Some(Output::After),
			ignore: false,
			update: None,
			timeout: Expr::Literal(Literal::None),
		};
		let ast = Ast::single_expr(Expr::Insert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for creating
	// ------------------------------

	async fn create(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Create.to_string()));
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params("Expected (what:Value, data:Value)".to_string()))?;

		let only = if matches!(what, PublicValue::String(_)) {
			true
		} else if let PublicValue::RecordId(ref x) = what {
			!matches!(x.key, PublicRecordIdKey::Range(_))
		} else {
			false
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_surface_value(x)));

		// Build the runtime statement.
		let sql = CreateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::None),
		};
		let ast = Ast::single_expr(Expr::Create(Box::new(sql)));
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), None)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for upserting
	// ------------------------------

	async fn upsert(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Upsert.to_string()));
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params("Expected (what:Value, data:Value)".to_string()))?;

		let only = if let PublicValue::RecordId(ref x) = what {
			!matches!(x.key, PublicRecordIdKey::Range(_))
		} else {
			false
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_surface_value(x)));

		// Build the runtime statement.
		let sql = UpsertStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			with: None,
			cond: None,
			timeout: Expr::Literal(Literal::None),
			explain: None,
		};
		let ast = Ast::single_expr(Expr::Upsert(Box::new(sql)));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for updating
	// ------------------------------

	async fn update(
		&self,
		_txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Update.to_string()));
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params("Expected (what, data)".to_string()))?;

		let only = if let PublicValue::RecordId(ref x) = what {
			!matches!(x.key, PublicRecordIdKey::Range(_))
		} else {
			false
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_surface_value(x)));
		// Build the runtime statement.
		let sql = UpdateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			with: None,
			cond: None,
			timeout: Expr::Literal(Literal::None),
			explain: None,
		};
		let ast = Ast::single_expr(Expr::Update(Box::new(sql)));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(ast, &session, var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for merging
	// ------------------------------

	async fn merge(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Merge.to_string()));
		}
		// Process the method arguments
		let (what, data) = extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params("Expected (what:Value, data:Value)".to_string()))?;

		let only = if let PublicValue::RecordId(ref x) = what {
			!matches!(x.key, PublicRecordIdKey::Range(_))
		} else {
			false
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::MergeExpression(Expr::from_surface_value(x)));
		// Build the runtime statement.
		let sql = UpdateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			..Default::default()
		};
		let ast = Ast::single_expr(Expr::Update(Box::new(sql)));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for patching
	// ------------------------------

	async fn patch(
		&self,
		_txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Patch.to_string()));
		}
		// Process the method arguments
		let (what, data, diff) =
			extract_args::<(PublicValue, Option<PublicValue>, Option<PublicValue>)>(
				params.into_vec(),
			)
			.ok_or(invalid_params("Expected (what:Value, data:Value, diff:Value)".to_string()))?;

		// Process the method arguments
		let only = if let PublicValue::RecordId(ref x) = what {
			!matches!(x.key, PublicRecordIdKey::Range(_))
		} else {
			false
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::PatchExpression(Expr::from_surface_value(x)));

		let diff = matches!(diff, Some(PublicValue::Bool(true)));

		// Build the runtime statement.
		let expr = Expr::Update(Box::new(UpdateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: if diff {
				Some(Output::Diff)
			} else {
				Some(Output::After)
			},
			with: None,
			cond: None,
			timeout: Expr::Literal(Literal::None),
			explain: None,
		}));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(Ast::single_expr(expr), &session, var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for relating
	// ------------------------------

	async fn relate(
		&self,
		_txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Relate.to_string()));
		}
		// Process the method arguments
		let (from, kind, with, data) =
			extract_args::<(PublicValue, PublicValue, PublicValue, Option<PublicValue>)>(
				params.to_vec(),
			)
			.ok_or(invalid_params(
				"Expected (from:Value, kind:Value, with:Value, data:Value)".to_string(),
			))?;

		// Returns if selecting on this value returns a single result.
		let only = singular(&from) && singular(&with);

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_surface_value(x)));

		// Build the runtime statement.
		let expr = Expr::Relate(Box::new(RelateStatement {
			only,
			or_update: false,
			from: Expr::from_surface_value(from),
			through: value_to_table(kind),
			to: Expr::from_surface_value(with),
			data,
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::None),
		}));
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = self.kvs().process(Ast::single_expr(expr), &session, var).await?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for deleting
	// ------------------------------

	async fn delete(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Delete.to_string()));
		}
		// Process the method arguments
		let (what,) = extract_args::<(PublicValue,)>(params.into_vec())
			.ok_or(invalid_params("Expected (what:Value)".to_string()))?;
		// Build the runtime statement.
		let sql = Expr::Delete(Box::new(DeleteStatement {
			only: singular(&what),
			what: vec![value_to_table(what)],
			output: Some(Output::Before),
			with: None,
			cond: None,
			timeout: Expr::Literal(Literal::None),
			explain: None,
		}));
		let ast = Ast::single_expr(sql);
		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, &session, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for getting info
	// ------------------------------

	async fn version(
		&self,
		_txn: Option<Uuid>,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		match params.len() {
			0 => Ok(self.version_data()),
			_ => Err(invalid_params("Expected 0 arguments".to_string())),
		}
	}

	// ------------------------------
	// Methods for querying
	// ------------------------------

	async fn query(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Query.to_string()));
		}
		// Process the method arguments
		let (query, vars, read_only, trace) = extract_query_params(params, &session)?;

		// A traced answer carries a trace beside each result, which neither the
		// results' own type nor their encoded form has room for.
		if !trace.is_empty() {
			return Ok(DbResult::Other(
				run_query_traced(self, txn, session_id, &session, &query, vars, read_only, trace)
					.await
					.map_err(types_error_from_anyhow)?,
			));
		}

		if let Some(format) = self.row_format() {
			return Ok(DbResult::Encoded(
				run_query_encoded(
					self,
					txn,
					session_id,
					&session,
					QueryForm::Text(&query, Dialect::SurrealQl),
					vars,
					format,
					read_only,
				)
				.await
				.map_err(types_error_from_anyhow)?,
			));
		}

		Ok(DbResult::Query(
			run_query_as(
				self,
				txn,
				session_id,
				&session,
				QueryForm::Text(&query, Dialect::SurrealQl),
				vars,
				read_only,
			)
			.await
			.map_err(types_error_from_anyhow)?,
		))
	}

	/// Prepare SurrealQL to stream its results, rather than answering with all
	/// of them at once.
	///
	/// Returns the parsed statement count together with the execution, because
	/// a transport has to announce that count before the first result exists.
	/// Drive [`QueryStreamJob::run`] while draining `items`.
	///
	/// This applies the same gates as [`Self::execute`] does for
	/// [`Method::Query`] — the capability check and the wall-clock guard —
	/// since a streaming call reaches the datastore without passing through it.
	/// The wall-clock guard bounds the whole execution rather than only its
	/// setup, so a client that reads slowly cannot hold a read snapshot open
	/// past the configured timeout.
	/// `cancel` is the caller's own handle on the execution, tripped when the
	/// consumer goes away. It is cooperative on purpose: the execution stops at
	/// its next yield point and finalises its transaction, where dropping the
	/// future instead would drop that transaction unfinished. A transport with
	/// a connection-level handle of its own is used as a fallback.
	///
	/// The principal returned alongside the job is the one the execution runs
	/// as. A caller registering the `LIVE SELECT`s the execution creates must
	/// pass it to [`live_query_owner`], because by then the session may be
	/// acting as someone else.
	///
	/// Rows arrive as [`QueryStreamItem::Frame`]s encoded in
	/// [`Self::row_format`] when the transport has one, and as values
	/// otherwise.
	async fn query_stream(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
		cancel: Option<CancelHandle>,
		items: Sender<QueryStreamItem>,
	) -> Result<(QueryStreamJob, AuthPrincipalSnapshot), surrealdb_types::Error> {
		// The capability gate lives in `execute`, which this does not go
		// through: a denied `query` must be denied here too.
		if !self.kvs().allows_rpc_method(&MethodTarget {
			method: Method::Query,
		}) {
			warn!("Capabilities denied RPC method call attempt, target: 'query'");
			return Err(method_not_allowed(Method::Query.to_string()));
		}

		// One acquisition, and everything the execution needs taken from it:
		// reading twice would let a concurrent `set` or `use` land in between,
		// so the variables and the session could come from states that never
		// existed together.
		//
		// The guard is then dropped rather than held across the execution, as
		// the buffered path holds it: a stream lives for as long as its
		// consumer reads, and holding the guard that long would block every
		// `use` / `set` / `signin` on the session behind a slow client. The
		// execution runs against the session as it was when the query started,
		// which is what the buffered path effectively gives it too — there, a
		// concurrent mutation simply waits instead.
		let (query, vars, read_only, trace, session, principal) = {
			let session_lock = self.get_session(&session_id).await?;
			let session = session_lock.read().await;
			// Check if the user is allowed to query
			if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
				return Err(method_not_allowed(Method::Query.to_string()));
			}
			if !Self::LQ_SUPPORT && session.rt {
				return Err(bad_lq_config());
			}
			let (query, vars, read_only, trace) = extract_query_params(params, &session)?;
			// Captured under the same guard as everything else the execution
			// needs, so it names the principal the query actually runs as rather
			// than whoever the session belongs to by the time it finishes.
			let principal = AuthPrincipalSnapshot::capture(&session);
			(query, vars, read_only, trace, session.clone(), principal)
		};

		// Kept so the wall-clock guard below can stop the execution
		// cooperatively rather than by dropping it. Deliberately the caller's
		// own handle and never the connection-level fallback below: tripping
		// that would cancel every other query on the same connection because
		// this one timed out.
		let guard_cancel = cancel.clone();
		let cancel = cancel.or_else(|| self.cancel_handle());
		let kvs = self.kvs_arc();
		let transaction = match txn {
			Some(txn_id) => Some(self.get_tx(txn_id).await?),
			None => None,
		};
		let job = kvs.run_streaming(
			QueryRequest::new(query.as_str(), &session)
				.with_variables(vars)
				.with_optional_transaction(transaction)
				.with_optional_cancel(cancel)
				.with_row_format(self.row_format())
				.with_trace(trace)
				.with_read_only(read_only),
			items,
		)?;

		// Bound the execution the way `execute` bounds a buffered one. The
		// guard wraps the run future, so it covers producing every row rather
		// than only reaching the first.
		//
		// It stops the execution by tripping its cancel handle and then
		// awaiting it, never by dropping it: the execution owns an open
		// transaction, and dropping the future would leave that transaction
		// neither committed nor cancelled. The executor short-circuits at its
		// next yield and finalises the transaction on its normal error path,
		// which is the same route a client-driven cancellation takes.
		//
		// The wait for it to stop is itself bounded. A cooperative stop needs
		// the execution to reach a yield point, and one parked sending into a
		// full `items` channel reaches none until its consumer drains -- so a
		// consumer that has stopped draining would otherwise turn a bounded
		// timeout into an indefinite wait. Past that grace the caller is
		// answered with its timeout and the execution carries on detached,
		// finalising its own transaction; its results settle themselves, since
		// nothing is left to hand them to.
		let timeout = self.kvs().query_timeout();
		// The observer event `execute` emits on completion. A streaming query
		// does not pass through `execute`, so it reports itself: the identity
		// comes from the session already cloned above rather than a second
		// lookup, and the duration covers producing every row, which is the
		// span a caller comparing it against a buffered query means.
		//
		// There is no auth event to match it: `method_to_auth_action` maps
		// `Query` to `None`, so `execute` emits none either.
		//
		// `None` when nothing is observing, which also skips building the
		// identity — the same short-circuit `execute` applies.
		let observer = Arc::clone(self.kvs().observer());
		let identity = (!observer.is_noop()).then(|| TenantIdentity::from(&session));
		let started = web_time::Instant::now();
		let QueryStreamJob {
			statement_count,
			mut run,
		} = job;
		let job = QueryStreamJob {
			statement_count,
			run: Box::pin(async move {
				let result = match timeout {
					Some(timeout) => {
						let deadline = common::time::sleep(timeout);
						tokio::pin!(deadline);
						let mut elapsed = false;
						loop {
							tokio::select! {
								// The execution is polled first so that it wins
								// a tie: a run that completes in the same poll
								// as the deadline has produced a real answer,
								// and an unbiased choice would discard it and
								// report a timeout roughly half the time.
								biased;
								result = &mut run => {
									// The deadline may have fired first, in
									// which case the execution has just
									// finished stopping and the timeout is the
									// caller's answer.
									break if elapsed {
										warn!(
											target: "surrealdb::core::rpc",
											timeout = ?timeout,
											"Streaming query exceeded the configured query timeout"
										);
										Err(query_timeout_error(timeout))
									} else {
										result
									};
								}
							_ = &mut deadline => {
									if elapsed {
										// The grace elapsed: the execution did
										// not stop cooperatively. It is handed
										// on rather than dropped -- dropping it
										// would leave its transaction neither
										// committed nor cancelled -- so the
										// caller gets its timeout now while the
										// execution finalises itself.
										error!(
											target: "surrealdb::core::rpc",
											timeout = ?timeout,
											"Streaming query did not stop within the grace period \
											 after its timeout; detaching it to finalise on its own"
										);
										#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
										{
											let kvs = Arc::clone(&kvs);
											common::rt::spawn_detached(async move {
												settle_detached_live_queries(&kvs, run.await).await;
											});
											break Err(query_timeout_error(timeout));
										}
										// No task to hand it to here, and
										// dropping it is the one thing that
										// must not happen, so keep polling it:
										// the tripped handle and the
										// execution's own context deadline are
										// what end it.
										#[cfg(all(target_family = "wasm", target_os = "unknown"))]
										{
											settle_detached_live_queries(&kvs, run.await).await;
											break Err(query_timeout_error(timeout));
										}
									}
									elapsed = true;
									// Cooperative: the execution keeps being
									// polled until it returns, so it finalises
									// its transaction itself. Its consumer is
									// what drains `items`, and it is driving
									// this future, so closing the channel here
									// is neither needed nor ours to do.
									if let Some(cancel) = &guard_cancel {
										cancel.trip();
									}
									// Re-arm as the abandonment deadline.
									deadline.as_mut().reset(
										common::time::Instant::now() + QUERY_STREAM_STOP_GRACE,
									);
								}
							}
						}
					}
					None => run.await,
				};
				if let Some(identity) = &identity {
					observer.on_rpc_complete(&RpcEvent {
						safe: RpcEventSafe {
							method: Method::Query,
							outcome: Outcome::from(&result),
							duration: started.elapsed(),
							error_class: result
								.as_ref()
								.err()
								.map(crate::observe::error_class::classify_types_error),
						},
						ctx: identity.to_rpc_ctx(),
					});
				}
				result
			}),
		};
		Ok((job, principal))
	}

	/// Cancels an in-flight streaming query by its originating request id.
	///
	/// The registry of in-flight streams is transport state — a stream is
	/// reachable only from the connection that started it — so the base
	/// implementation reports the method as absent, exactly as a server
	/// without streaming would. A transport that serves `query_stream`
	/// overrides this with a lookup into its own registry. `execute` applies
	/// the capability gate for [`Method::QueryCancel`] before dispatching
	/// here, as it does for every method.
	async fn query_cancel(&self, params: PublicArray) -> Result<DbResult, surrealdb_types::Error> {
		let _ = params;
		Err(method_not_found(Method::QueryCancel.to_string()))
	}

	async fn gql(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		#[cfg(not(feature = "gql"))]
		{
			let _ = (txn, session_id, params);
			Err(method_not_found(Method::Gql.to_string()))
		}
		#[cfg(feature = "gql")]
		{
			sql_dialect_query(self, txn, session_id, params, Method::Gql, Dialect::Gql).await
		}
	}

	/// Run PostgreSQL text against the namespace and database selected on the
	/// session, as `query` runs SurrealQL: the same variables, transactions,
	/// live-query registration and per-subject query gate. The `postgres`
	/// route capability is enforced by the datastore for the dialect itself,
	/// so no other method can reach the PostgreSQL front end around it.
	async fn pgsql(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		sql_dialect_query(self, txn, session_id, params, Method::Pgsql, Dialect::PgSql).await
	}

	/// Run MySQL text against the namespace and database selected on the
	/// session. See [`Self::pgsql`]; the capability is `mysql`.
	async fn mysql(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		sql_dialect_query(self, txn, session_id, params, Method::Mysql, Dialect::MySql).await
	}

	/// Execute a GraphQL request against the namespace and database selected on
	/// the session, returning the spec response envelope (`{ data, errors }`)
	/// as a value. GraphQL manages its own per-resolver transactions, so this
	/// method does not participate in the RPC explicit-transaction system.
	async fn graphql(
		&self,
		txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		#[cfg(not(all(feature = "graphql", not(target_family = "wasm"))))]
		{
			let _ = (txn, session_id, params);
			Err(method_not_found(Method::Graphql.to_string()))
		}
		#[cfg(all(feature = "graphql", not(target_family = "wasm")))]
		{
			// GraphQL manages its own per-resolver transactions and cannot join a
			// client's explicit `begin`/`commit` block, so reject an explicit
			// transaction id rather than silently running outside it.
			if txn.is_some() {
				return Err(invalid_params(
					"GraphQL does not support explicit transactions; run it outside begin/commit"
						.to_string(),
				));
			}
			// Resolve the session and check the user is allowed to query. There
			// is no HTTP route in play here, so -- as agreed -- access is gated
			// purely on the query capability for the subject, exactly like the
			// `query` and `gql` RPC methods.
			let session_lock = self.get_session(&session_id).await?;
			let session = session_lock.read().await;
			if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
				return Err(method_not_allowed(Method::Graphql.to_string()));
			}
			// Process the method arguments: (query, variables?, operationName?)
			let (query, variables, operation) =
				extract_args::<(PublicValue, Option<PublicValue>, Option<PublicValue>)>(
					params.into_vec(),
				)
				.ok_or(invalid_params(
					"Expected (query:string, variables:object, operation:string)".to_string(),
				))?;

			let PublicValue::String(query) = query else {
				return Err(invalid_params("Expected query to be a string".to_string()));
			};

			// GraphQL variables are plain JSON; convert from the public value.
			let variables = match variables {
				Some(v @ PublicValue::Object(_)) => v.into_json_value(),
				None | Some(PublicValue::None | PublicValue::Null) => serde_json::Value::Null,
				Some(unexpected) => {
					return Err(invalid_params(format!(
						"Expected variables to be an object, got {unexpected:?}"
					)));
				}
			};

			let operation = match operation {
				Some(PublicValue::String(s)) => Some(s),
				None | Some(PublicValue::None | PublicValue::Null) => None,
				Some(unexpected) => {
					return Err(invalid_params(format!(
						"Expected operation to be a string, got {unexpected:?}"
					)));
				}
			};

			// The datastore owns and caches the generated schema; the shared
			// `execute_request` helper builds the request exactly as the HTTP
			// `/graphql` transport does. Schema/config failures map to errors;
			// GraphQL execution errors ride back in the response envelope.
			let ds = self.kvs_arc();
			let json = crate::graphql::execute_request(&ds, &session, query, variables, operation)
				.await
				.map_err(|e| surrealdb_types::Error::query(e.to_string(), None))?;

			Ok(DbResult::Other(crate::rpc::format::json::json_to_value(json)))
		}
	}

	// ------------------------------
	// Methods for running functions
	// ------------------------------

	async fn run(
		&self,
		_txn: Option<Uuid>,
		session_id: Uuid,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let session_lock = self.get_session(&session_id).await?;
		let session = session_lock.read().await;
		// Check if the user is allowed to query
		if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(Method::Run.to_string()));
		}
		// Process the method arguments
		let (name, version, args) = extract_args::<(
			PublicValue,
			Option<PublicValue>,
			Option<PublicValue>,
		)>(params.into_vec())
		.ok_or(invalid_params("Expected (name:string, version:string, args:array)".to_string()))?;
		// Parse the function name argument
		let PublicValue::String(name) = name else {
			return Err(invalid_params(format!("Expected name to be string, got {name:?}")));
		};
		// Parse any function version argument
		let version = match version {
			Some(PublicValue::String(v)) => Some(v),
			None | Some(PublicValue::None | PublicValue::Null) => None,
			unexpected => {
				return Err(invalid_params(format!(
					"Expected version to be string, got {unexpected:?}"
				)));
			}
		};
		// Parse the function arguments if specified
		let args = match args {
			Some(PublicValue::Array(args)) => {
				args.into_iter().map(Expr::from_public_value).collect::<Vec<Expr>>()
			}
			None | Some(PublicValue::None | PublicValue::Null) => vec![],
			unexpected => {
				return Err(invalid_params(format!(
					"Expected args to be array, got {unexpected:?}"
				)));
			}
		};

		let segments = name.split("::").collect::<Vec<&str>>();
		let name = match segments.first() {
			Some(&"fn") => Function::Custom(segments[1..].join("::")),
			Some(&prefix @ ("ext" | "mod")) => {
				if !self
					.kvs()
					.get_capabilities()
					.allows_experimental(&ExperimentalTarget::Surrealism)
				{
					return Err(invalid_params(
						"Experimental capability `surrealism` is not enabled".to_string(),
					));
				}

				let Some(name) = segments.get(1).map(|x| (*x).to_string()) else {
					return Err(invalid_params("Expected an extension name".to_string()));
				};

				let sub = if segments.len() > 2 {
					Some(segments[2..].join("::"))
				} else {
					None
				};

				let prefix = if prefix == "mod" {
					ExtPrefix::Mod
				} else {
					ExtPrefix::Ext
				};
				Function::Module(name, sub, prefix)
			}
			Some(&"silo") => {
				if !self
					.kvs()
					.get_capabilities()
					.allows_experimental(&ExperimentalTarget::Surrealism)
				{
					return Err(invalid_params(
						"Experimental capability `surrealism` is not enabled".to_string(),
					));
				}

				let Some(org) = segments.get(1).map(|x| (*x).to_string()) else {
					return Err(invalid_params("Expected silo organisation name".to_string()));
				};

				let Some(pkg) = segments.get(2).map(|x| (*x).to_string()) else {
					return Err(invalid_params("Expected silo package name".to_string()));
				};

				let Some(version) = version else {
					return Err(invalid_params("Expected silo version".to_string()));
				};
				let mut split = version.split('.');
				let major = split.next().and_then(|s| s.parse::<u32>().ok()).ok_or_else(|| {
					invalid_params("Expected major version (u32) in version string".to_string())
				})?;
				let minor = split.next().and_then(|s| s.parse::<u32>().ok()).ok_or_else(|| {
					invalid_params("Expected minor version (u32) in version string".to_string())
				})?;
				let patch = split.next().and_then(|s| s.parse::<u32>().ok()).ok_or_else(|| {
					invalid_params("Expected patch version (u32) in version string".to_string())
				})?;

				let sub = if segments.len() > 3 {
					Some(segments[3..].join("::"))
				} else {
					None
				};

				Function::Silo {
					org,
					pkg,
					major,
					minor,
					patch,
					sub,
				}
			}
			Some(&"ml") => {
				let name = segments[1..].join("::");
				Function::Model(Model {
					name: name.into(),
					version: version
						.ok_or(invalid_params(
							"Expected version to be set for model function".to_string(),
						))?
						.into(),
				})
			}
			_ => Function::Normal(name),
		};

		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: name,
			arguments: args,
		}));
		let ast = Ast::single_expr(expr);

		// Specify the query parameters
		let var = Some(session.variables.clone());
		// Execute the function on the database
		let mut res = run_query(self, None, session_id, &session, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction.
	///
	/// `params` is empty or holds one options object, as for a query call's
	/// options: `{ read_only: { max_staleness, strict } }` opens a read-only
	/// transaction over a snapshot no older than the bound. See
	/// [`extract_begin_options`].
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		Err(method_not_allowed(Method::Begin.to_string()))
	}

	/// Commit a transaction
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		Err(method_not_allowed(Method::Commit.to_string()))
	}

	/// Cancel a transaction
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		Err(method_not_allowed(Method::Cancel.to_string()))
	}
}

/// The body of the `gql`, `pgsql` and `mysql` methods: read
/// `(statements, vars?, options?)`, layer the call's variables over the
/// session's, and run the text in `dialect`.
///
/// `statements` is one string or a batch: an array of strings and
/// `{ text, vars? }` objects, run in order with each entry's variables over the
/// call's. `gql` answers with the results of all entries in one array. `pgsql`
/// and `mysql` answer with the object of [`sql_dialect::response`]: a
/// transaction-control statement or MySQL `USE` that is an entry of its own acts
/// on the RPC session, a statement that fails is an `ERR` entry that does not
/// stop the entries after it, and `options` selects array rows or a describe.
async fn sql_dialect_query<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Uuid,
	params: PublicArray,
	method: Method,
	dialect: Dialect,
) -> Result<DbResult, surrealdb_types::Error>
where
	T: RpcProtocol + ?Sized,
{
	let session_lock = this.get_session(&session_id).await?;
	{
		let session = session_lock.read().await;
		if !this.kvs().allows_query_by_subject(session.au.as_ref()) {
			return Err(method_not_allowed(method.to_string()));
		}
	}
	let (statements, vars, options) =
		extract_args::<(PublicValue, Option<PublicValue>, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params(
				"Expected (statements:string|array, vars:object, options:object)".to_string(),
			))?;
	let options = sql_dialect::parse_options(options)?;
	if options.atomic && options.describe {
		return Err(invalid_params(
			"The options `atomic` and `describe` cannot be combined".to_string(),
		));
	}
	if dialect == Dialect::Gql
		&& (options.describe || options.row_array || options.atomic || options.value_text)
	{
		return Err(invalid_params("The options are not supported for gql".to_string()));
	}
	let call_vars = match vars {
		Some(PublicValue::Object(v)) => Some(v),
		None | Some(PublicValue::None | PublicValue::Null) => None,
		unexpected => {
			return Err(invalid_params(format!("Expected vars to be object, got {unexpected:?}")));
		}
	};
	let batch = if let PublicValue::Array(batch) = statements {
		batch.into_vec()
	} else {
		vec![statements]
	};
	let mut entries: Vec<(String, Option<surrealdb_types::Object>)> = Vec::new();
	for entry in batch {
		if let PublicValue::String(text) = entry {
			entries.push((text, None));
		} else if let PublicValue::Object(mut object) = entry {
			let Some(PublicValue::String(text)) = object.remove("text") else {
				return Err(invalid_params(
					"Expected a batch entry to have a `text` string".to_string(),
				));
			};
			let vars = match object.remove("vars") {
				Some(PublicValue::Object(v)) => Some(v),
				None | Some(PublicValue::None | PublicValue::Null) => None,
				unexpected => {
					return Err(invalid_params(format!(
						"Expected entry vars to be object, got {unexpected:?}"
					)));
				}
			};
			entries.push((text, vars));
		} else {
			return Err(invalid_params(
				"Expected statements to be a string or an array of strings and objects".to_string(),
			));
		}
	}
	if dialect == Dialect::Gql {
		let session = session_lock.read().await;
		let mut results = Vec::new();
		for (text, entry_vars) in entries {
			let vars = Some(merge_vars(&session.variables, call_vars.as_ref(), entry_vars));
			results.extend(
				run_query(this, txn, session_id, &session, QueryForm::Text(&text, dialect), vars)
					.await
					.map_err(types_error_from_anyhow)?,
			);
		}
		return Ok(DbResult::Query(results));
	}
	// Control statements and `describe` never reach `QuerySource`, so the dialect's
	// capability is enforced here for every call.
	this.kvs().check_sql_dialect(dialect)?;
	// The session preflight a query runs (expiry, anonymous access) applies to control
	// statements and `describe` too, which never reach the executor.
	this.kvs().check_session(&*session_lock.read().await)?;
	// An envelope transaction that does not exist fails the call before any entry can act on the
	// session.
	if let Some(id) = txn {
		this.get_tx(id).await?;
	}
	sql_dialect::live_transaction(this, &session_lock).await;
	let started = web_time::Instant::now();
	if options.atomic {
		if txn.is_some() {
			return Err(invalid_params(
				"An atomic call opens its own transaction and cannot run inside an envelope transaction"
					.to_string(),
			));
		}
		let ctx = SqlPart {
			session_id,
			method,
			dialect,
			options,
			started,
		};
		return run_sql_atomic(this, &session_lock, &ctx, entries, call_vars.as_ref()).await;
	}
	let mut statements = Vec::new();
	let mut described_params = None;
	for (text, entry_vars) in entries {
		if options.describe {
			let (open, _) = sql_dialect::live_transaction(this, &session_lock).await;
			let transaction = match txn.or(open) {
				Some(id) => Some(this.get_tx(id).await?),
				None => None,
			};
			let session = session_lock.read().await;
			check_dialect_subject(this, &session, method)?;
			let described =
				sql_dialect::describe(this.kvs(), &session, transaction.as_ref(), dialect, &text);
			// A describe stops when the call is cancelled, like a statement does.
			let outcome = match CALL_CANCEL.try_with(Clone::clone) {
				Ok(cancel) => tokio::select! {
					biased;
					() = cancel.cancelled() => None,
					outcome = described => Some(outcome),
				},
				Err(_) => Some(described.await),
			};
			let Some(outcome) = outcome else {
				statements.push(sql_dialect::cancelled_entry(dialect));
				continue;
			};
			let (entry, params) = outcome?;
			statements.push(entry);
			described_params = Some(params);
			continue;
		}
		let (open_txn, failed) = sql_dialect::live_transaction(this, &session_lock).await;
		let control = sql_dialect::control(&text, dialect);
		if control.is_some() {
			check_dialect_subject(this, &*session_lock.read().await, method)?;
		}
		let entry = match control {
			Some(sql_dialect::Control::Begin(_))
				if open_txn.is_some() && failed && dialect == Dialect::PgSql =>
			{
				sql_dialect::aborted_entry()
			}
			Some(sql_dialect::Control::Begin(asked)) if open_txn.is_some() => {
				if asked == sql_dialect::TxnModes::default() {
					sql_dialect::tagged_entry("BEGIN")
				} else {
					if txn.is_none()
						&& dialect == Dialect::PgSql
						&& let Some(open) = open_txn
					{
						sql_dialect::mark_failed(&session_lock, open).await;
					}
					sql_dialect::error_entry(
						"a transaction is already in progress, so the isolation level and access mode it was opened with cannot be changed",
						"25001",
						(dialect == Dialect::MySql).then_some(1568),
					)
				}
			}
			Some(sql_dialect::Control::Begin(asked)) => {
				let modes = asked.resolve(&*session_lock.read().await);
				match this.begin(None, session_id, modes.begin_params()).await.map(begun_id) {
					Ok(Some(id)) => {
						let mut session = session_lock.write().await;
						// The subject is checked on the guard that publishes the transaction, so a
						// principal changed meanwhile cannot keep one it was not permitted.
						if let Err(err) = check_dialect_subject(this, &session, method) {
							drop(session);
							let arg: PublicArray = vec![PublicValue::Uuid(id)].into();
							let _ = this.cancel(None, session_id, arg).await;
							return Err(err);
						}
						if session.variables.get(sql_dialect::TXN_VAR).is_some() {
							// A concurrent call opened the session's transaction first.
							drop(session);
							let arg: PublicArray = vec![PublicValue::Uuid(id)].into();
							let _ = this.cancel(None, session_id, arg).await;
						} else {
							session
								.variables
								.insert(sql_dialect::TXN_VAR.to_owned(), PublicValue::Uuid(id));
							session.variables.remove(sql_dialect::TXN_FAILED_VAR);
							modes.record(&mut session);
						}
						sql_dialect::tagged_entry("BEGIN")
					}
					Ok(None) => sql_dialect::error_entry(
						"the transaction could not be opened",
						"XX000",
						None,
					),
					Err(err) => sql_dialect::engine_error_entry(&err, dialect),
				}
			}
			Some(control @ (sql_dialect::Control::Commit | sql_dialect::Control::Rollback)) => {
				let tag = if matches!(control, sql_dialect::Control::Commit) {
					"COMMIT"
				} else {
					"ROLLBACK"
				};
				let Some(id) = open_txn else {
					statements.push(sql_dialect::tagged_entry(tag));
					continue;
				};
				let arg: PublicArray = vec![PublicValue::Uuid(PublicUuid::from(id))].into();
				let outcome = if matches!(control, sql_dialect::Control::Commit) && !failed {
					this.commit(None, session_id, arg).await
				} else {
					this.cancel(None, session_id, arg).await
				};
				let mut session = session_lock.write().await;
				// Only the transaction that was finalised is cleared; a concurrent call may
				// have opened another while this one awaited storage.
				let finalised = matches!(
					session.variables.get(sql_dialect::TXN_VAR),
					Some(PublicValue::Uuid(current)) if current.into_inner() == id
				);
				if finalised {
					session.variables.remove(sql_dialect::TXN_VAR);
					session.variables.remove(sql_dialect::TXN_FAILED_VAR);
				}
				drop(session);
				match outcome {
					Ok(_) => sql_dialect::tagged_entry(tag),
					Err(err) => sql_dialect::engine_error_entry(&err, dialect),
				}
			}
			Some(
				control @ (sql_dialect::Control::Set(..)
				| sql_dialect::Control::Show(_)
				| sql_dialect::Control::Reset(_)),
			) => {
				if failed {
					sql_dialect::aborted_entry()
				} else {
					let entry = sql_dialect::apply_setting(&session_lock, &control, |session| {
						check_dialect_subject(this, session, method)
					})
					.await?;
					if sql_dialect::is_error_entry(&entry)
						&& txn.is_none() && dialect == Dialect::PgSql
						&& let Some(open) = open_txn
					{
						sql_dialect::mark_failed(&session_lock, open).await;
					}
					entry
				}
			}
			Some(sql_dialect::Control::SetTransaction {
				modes,
				session: for_session,
			}) => {
				if failed {
					sql_dialect::aborted_entry()
				} else if txn.is_some() && !for_session {
					sql_dialect::error_entry(
						"the characteristics of a transaction named by the call cannot be set with SET TRANSACTION",
						"25001",
						(dialect == Dialect::MySql).then_some(1568),
					)
				} else {
					match set_transaction(
						this,
						&session_lock,
						session_id,
						open_txn.filter(|_| txn.is_none()),
						modes,
						for_session,
						dialect,
					)
					.await
					{
						Ok(entry) => {
							if sql_dialect::is_error_entry(&entry)
								&& txn.is_none() && dialect == Dialect::PgSql
								&& let Some(open) = open_txn
							{
								sql_dialect::mark_failed(&session_lock, open).await;
							}
							entry
						}
						Err(err) => return Err(err),
					}
				}
			}
			Some(sql_dialect::Control::Use(database)) => {
				let args: PublicArray =
					vec![PublicValue::None, PublicValue::String(database)].into();
				match this.yuse(session_id, args).await {
					Ok(_) => sql_dialect::tagged_entry("USE"),
					Err(err) => sql_dialect::engine_error_entry(&err, dialect),
				}
			}
			None if failed && dialect == Dialect::PgSql && txn.is_none() => {
				sql_dialect::aborted_entry()
			}
			None => {
				// Each statement of an entry runs on its own so that it answers with one entry
				// however many engine statements it lowers to. The first failure stops the
				// statements after it, as a failed PostgreSQL transaction or a failed
				// multi-statement text does.
				// An envelope transaction overrides the session's, and its failures are not the
				// session transaction's.
				let in_pg_txn = txn.is_none() && open_txn.is_some() && dialect == Dialect::PgSql;
				let parts = sql_dialect::split_statements(&text, dialect)
					.unwrap_or_else(|| vec![text.clone()]);
				let mut aborted = false;
				for part in parts {
					if aborted {
						statements.push(if in_pg_txn {
							sql_dialect::aborted_entry()
						} else {
							sql_dialect::skipped_entry(dialect)
						});
						continue;
					}
					let transaction = match txn.or(open_txn) {
						Some(id) => match this.get_tx(id).await {
							Ok(transaction) => Some(transaction),
							Err(err) => {
								aborted = true;
								statements.push(sql_dialect::engine_error_entry(&err, dialect));
								if let Some(open) = open_txn.filter(|_| in_pg_txn) {
									sql_dialect::mark_failed(&session_lock, open).await;
								}
								continue;
							}
						},
						None => None,
					};
					if txn.is_none() && open_txn.is_some() {
						sql_dialect::mark_used(&session_lock).await;
					}
					let outcome = run_sql_part(
						this,
						&session_lock,
						&SqlPart {
							session_id,
							method,
							dialect,
							options,
							started,
						},
						transaction,
						&part,
						call_vars.as_ref(),
						entry_vars.clone(),
					)
					.await;
					let (entry, errored) = match (outcome, open_txn.filter(|_| in_pg_txn)) {
						(Ok(done), _) => done,
						(Err(err), open) => {
							// A statement that hit the call's deadline, or was refused, leaves the
							// PostgreSQL transaction failed.
							if let Some(open) = open {
								sql_dialect::mark_failed(&session_lock, open).await;
							}
							return Err(err);
						}
					};
					aborted = errored;
					statements.push(entry);
					if aborted && let Some(open) = open_txn.filter(|_| in_pg_txn) {
						sql_dialect::mark_failed(&session_lock, open).await;
					}
				}
				continue;
			}
		};
		statements.push(entry);
	}
	let session = session_lock.read().await;
	Ok(DbResult::Other(sql_dialect::response(statements, described_params, &session)))
}

/// Run every statement of `entries` in one transaction of its own, committing only if all succeed.
/// After the first failure the statements that follow are not run and the transaction is
/// cancelled, so the response reports `committed: false`. A session that holds a transaction
/// refuses the call, and control statements are not allowed inside it.
async fn run_sql_atomic<T>(
	this: &T,
	session_lock: &RwLock<Session>,
	ctx: &SqlPart,
	entries: Vec<(String, Option<surrealdb_types::Object>)>,
	call_vars: Option<&surrealdb_types::Object>,
) -> Result<DbResult, surrealdb_types::Error>
where
	T: RpcProtocol + ?Sized,
{
	let (open, _) = sql_dialect::live_transaction(this, session_lock).await;
	if open.is_some() {
		return Err(invalid_params(
			"A transaction is already active on the session; an atomic call opens its own"
				.to_string(),
		));
	}
	let read_only = ctx.options.read_only.then_some(ReadOnlyOptions {
		max_staleness: std::time::Duration::ZERO,
		strict: false,
	});
	let (transaction, _) = this
		.kvs()
		.begin_transaction(ctx.options.isolation, read_only)
		.await
		.map_err(types_error_from_anyhow)?;
	let transaction = Arc::new(transaction);
	let mut statements = Vec::new();
	let mut failed = false;
	for (text, entry_vars) in entries {
		let parts =
			sql_dialect::split_statements(&text, ctx.dialect).unwrap_or_else(|| vec![text.clone()]);
		for part in parts {
			if failed {
				statements.push(sql_dialect::aborted_entry());
				continue;
			}
			if sql_dialect::is_session_statement(&part, ctx.dialect) {
				failed = true;
				statements.push(sql_dialect::error_entry(
					"session and transaction statements are not allowed in an atomic call",
					"25001",
					(ctx.dialect == Dialect::MySql).then_some(1400),
				));
				continue;
			}
			match run_sql_part(
				this,
				session_lock,
				ctx,
				Some(Arc::clone(&transaction)),
				&part,
				call_vars,
				entry_vars.clone(),
			)
			.await
			{
				Ok((entry, errored)) => {
					statements.push(entry);
					failed = errored;
				}
				Err(err) => {
					let _ = transaction.cancel().await;
					return Err(err);
				}
			}
		}
	}
	if failed {
		let _ = transaction.cancel().await;
		return Ok(DbResult::Other(sql_dialect::response_atomic(statements, false)));
	}
	transaction.commit().await.map_err(types_error_from_anyhow)?;
	Ok(DbResult::Other(sql_dialect::response_atomic(statements, true)))
}

/// The variables a statement runs with: the session's, then the call's, then the entry's.
fn merge_vars(
	base: &PublicVariables,
	call: Option<&surrealdb_types::Object>,
	entry: Option<surrealdb_types::Object>,
) -> PublicVariables {
	let mut merged = base.clone();
	if let Some(vars) = call {
		merged.extend(vars.clone().into());
	}
	if let Some(vars) = entry {
		merged.extend(vars.into());
	}
	merged
}

/// What every statement of one `pgsql` or `mysql` call shares.
struct SqlPart {
	session_id: Uuid,
	method: Method,
	dialect: Dialect,
	options: sql_dialect::Options,
	/// When the call started, from which the whole call's query-timeout deadline is measured.
	started: web_time::Instant,
}

/// Await `work` within what is left of the query timeout of a call that started at `started`; the
/// call fails with the timeout error once it is spent. Without a configured timeout it is awaited
/// as it is.
async fn within_deadline<T, F>(
	this: &T,
	started: web_time::Instant,
	work: F,
) -> Result<F::Output, surrealdb_types::Error>
where
	T: RpcProtocol + ?Sized,
	F: std::future::Future,
{
	let Some(limit) = this.kvs().query_timeout() else {
		return Ok(work.await);
	};
	let remaining = limit.saturating_sub(started.elapsed());
	common::time::timeout(remaining, work).await.map_err(|_| query_timeout_error(limit))
}

/// Run one SQL statement `part` in `transaction` (or its own transaction) and shape its one
/// entry. The flag is whether it failed. An `Err` is a failure of the call itself: a refused
/// subject, a denied dialect or a spent deadline.
async fn run_sql_part<T>(
	this: &T,
	session_lock: &RwLock<Session>,
	ctx: &SqlPart,
	transaction: Option<Arc<crate::kvs::Transaction>>,
	part: &str,
	call_vars: Option<&surrealdb_types::Object>,
	entry_vars: Option<surrealdb_types::Object>,
) -> Result<(PublicValue, bool), surrealdb_types::Error>
where
	T: RpcProtocol + ?Sized,
{
	let analysis = {
		let session = session_lock.read().await;
		check_dialect_subject(this, &session, ctx.method)?;
		within_deadline(
			this,
			ctx.started,
			sql_dialect::analyse(this.kvs(), &session, transaction.as_ref(), ctx.dialect, part),
		)
		.await?
	};
	let outcome = {
		let session = session_lock.read().await;
		check_dialect_subject(this, &session, ctx.method)?;
		#[cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(unused_mut))]
		let mut merged = merge_vars(&session.variables, call_vars, entry_vars);
		// Parameters of these methods carry no declared type, so text the lowering finds written
		// to or compared with a typed column is read as that column's type.
		#[cfg(any(feature = "postgres", feature = "mysql"))]
		merged.insert(surrealdb_sqlplan::UNTYPED_PARAMS_VAR.to_owned(), PublicValue::Bool(true));
		let vars = Some(merged);
		let run = run_query_in(
			this,
			transaction,
			ctx.session_id,
			&session,
			QueryForm::Text(part, ctx.dialect),
			vars,
			None,
		);
		// A call with a transaction-control entry is exempt from the method-level guard, so its
		// other statements share one deadline measured here.
		match within_deadline(this, ctx.started, run).await {
			Ok(outcome) => outcome,
			Err(err) => return Err(err),
		}
	};
	match outcome {
		Ok(results) => {
			let (entry, errored) =
				sql_dialect::statement_entry(results, ctx.dialect, ctx.options, analysis.as_ref());
			let entry = if errored {
				sql_dialect::char_position(entry, part)
			} else {
				entry
			};
			Ok((entry, errored))
		}
		Err(err) => {
			let err = types_error_from_anyhow(err);
			if err.is_not_allowed() {
				return Err(err);
			}
			let entry = sql_dialect::engine_error_entry(&err, ctx.dialect);
			Ok((sql_dialect::char_position(entry, part), true))
		}
	}
}

/// The subject gate and session preflight, applied to the session guard a statement is about to
/// run with, so a principal change between the call's first check and this one cannot slip a
/// subject the gate would refuse past it.
fn check_dialect_subject<T>(
	this: &T,
	session: &Session,
	method: Method,
) -> Result<(), surrealdb_types::Error>
where
	T: RpcProtocol + ?Sized,
{
	if !this.kvs().allows_query_by_subject(session.au.as_ref()) {
		return Err(method_not_allowed(method.to_string()));
	}
	this.kvs().check_session(session)
}

enum QueryForm<'a> {
	/// Query text in the given dialect. The `gql`, `pgsql` and `mysql`
	/// handlers are the only callers that name one; every other one submits
	/// SurrealQL.
	Text(&'a str, Dialect),
	Parsed(Ast),
}

/// Read a `query` call's arguments: the SurrealQL text, and the variables to
/// run it with.
///
/// The call's own variables are layered over the session's rather than
/// replacing them, so a query sees both, and a missing or null `vars` leaves
/// the session's in place.
fn extract_query_params(
	params: PublicArray,
	session: &Session,
) -> Result<
	(String, Option<PublicVariables>, Option<ReadOnlyOptions>, surrealdb_rpc::TraceKinds),
	surrealdb_types::Error,
> {
	let (query, vars, options) =
		extract_args::<(PublicValue, Option<PublicValue>, Option<PublicValue>)>(params.into_vec())
			.ok_or(invalid_params(
				"Expected (query:string, vars:object, options:object)".to_string(),
			))?;
	let ParsedOptions {
		read_only,
		trace,
		..
	} = parse_options(options, OptionsFor::Query)?;

	let PublicValue::String(query) = query else {
		return Err(invalid_params("Expected query to be string".to_string()));
	};

	// Specify the query variables
	let vars = match vars {
		Some(PublicValue::Object(v)) => {
			let mut merged = session.variables.clone();
			merged.extend(v.into());
			Some(merged)
		}
		None | Some(PublicValue::None | PublicValue::Null) => Some(session.variables.clone()),
		unexpected => {
			return Err(invalid_params(format!("Expected vars to be object, got {unexpected:?}")));
		}
	};

	Ok((query, vars, read_only, trace))
}

/// The options of a `begin` call: nothing, or one options object holding
/// `isolation` and `read_only`. See [`parse_options`].
pub fn extract_begin_options(
	params: PublicArray,
) -> Result<crate::kvs::BeginRequest, surrealdb_types::Error> {
	let mut params = params.into_vec().into_iter();
	let options = params.next();
	if params.next().is_some() {
		return Err(invalid_params("Expected (options:object)".to_string()));
	}
	let parsed = parse_options(options, OptionsFor::Begin)?;
	Ok(crate::kvs::BeginRequest {
		isolation: parsed.isolation,
		read_only: parsed.read_only,
	})
}

/// The `read_only` entry of a query call's options object. See
/// [`parse_options`]; a query call takes no isolation level.
#[cfg(test)]
fn extract_read_only(
	options: Option<PublicValue>,
) -> Result<Option<ReadOnlyOptions>, surrealdb_types::Error> {
	Ok(parse_options(options, OptionsFor::Query)?.read_only)
}

/// The call an options object belongs to, which decides the entries it takes.
#[derive(Clone, Copy)]
enum OptionsFor {
	/// A `begin` call: `isolation` and `read_only`.
	Begin,
	/// A `query` or `query_stream` call: `read_only` and `trace`.
	Query,
}

/// The entries of a `query` or `begin` call's options object.
#[derive(Default)]
struct ParsedOptions {
	isolation: Option<crate::kvs::Isolation>,
	read_only: Option<ReadOnlyOptions>,
	trace: surrealdb_rpc::TraceKinds,
}

/// The options object of a `query` or `begin` call.
///
/// - `read_only: { max_staleness: <duration>, strict: <bool> }` asks for the statements to run as
///   one read-only block, over a snapshot no older than the bound (default `0s`, the current
///   state), exactly as if they were wrapped in `BEGIN READ ONLY MAX STALENESS <duration> [STRICT];
///   …; COMMIT;`.
/// - `read_only: true` is `read_only: {}`, and `false` asks for nothing.
/// - `isolation: "snapshot" | "serializable"` selects the isolation level of the transaction a
///   `begin` opens; the SQL names `repeatable read`, `read committed` and `read uncommitted` mean
///   `snapshot`, in any case. A query call does not take it: a lone statement is always `SNAPSHOT`.
/// - `trace: ["relations", "references", "links", "variables", "subscriptions"]` asks a `query` or
///   `query_stream` call to report those kinds (see [`surrealdb_rpc::trace`]): beside the rows of a
///   stream, and beside each result of a buffered answer.
///
/// A missing or null options object asks for nothing. Unknown entries are
/// refused rather than ignored, so a misspelt option is never silently dropped.
fn parse_options(
	options: Option<PublicValue>,
	call: OptionsFor,
) -> Result<ParsedOptions, surrealdb_types::Error> {
	let allow_isolation = matches!(call, OptionsFor::Begin);
	let object = match options {
		None | Some(PublicValue::None | PublicValue::Null) => return Ok(ParsedOptions::default()),
		Some(PublicValue::Object(object)) => object,
		Some(other) => {
			return Err(invalid_params(format!("Expected options to be object, got {other:?}")));
		}
	};
	let mut isolation = None;
	let mut read_only = None;
	let mut trace = surrealdb_rpc::TraceKinds::NONE;
	for (key, value) in object.iter() {
		match (key.as_str(), value) {
			("trace", value) if !allow_isolation => trace = parse_trace_kinds(value)?,
			("isolation", PublicValue::None | PublicValue::Null) if allow_isolation => {}
			("isolation", PublicValue::String(level)) if allow_isolation => {
				isolation = Some(sql_dialect::isolation_named(level).ok_or_else(|| {
					invalid_params(format!(
						"Expected isolation to be `snapshot`, `serializable` or a SQL isolation level, got `{level}`"
					))
				})?);
			}
			("isolation", other) if allow_isolation => {
				return Err(invalid_params(format!(
					"Expected isolation to be a string, got {other:?}"
				)));
			}
			("read_only", PublicValue::None | PublicValue::Null | PublicValue::Bool(false)) => {}
			("read_only", PublicValue::Bool(true)) => {
				read_only = Some(ReadOnlyOptions {
					max_staleness: std::time::Duration::ZERO,
					strict: false,
				});
			}
			("read_only", PublicValue::Object(entry)) => {
				let mut max_staleness = None;
				let mut strict = false;
				for (key, value) in entry.iter() {
					match (key.as_str(), value) {
						("max_staleness", PublicValue::Duration(bound)) => {
							max_staleness = Some(std::time::Duration::from(*bound));
						}
						("max_staleness", other) => {
							return Err(invalid_params(format!(
								"Expected max_staleness to be a duration, got {other:?}"
							)));
						}
						("strict", PublicValue::Bool(flag)) => strict = *flag,
						("strict", other) => {
							return Err(invalid_params(format!(
								"Expected strict to be a bool, got {other:?}"
							)));
						}
						(other, _) => {
							return Err(invalid_params(format!(
								"Unknown read_only option `{other}`"
							)));
						}
					}
				}
				read_only = Some(ReadOnlyOptions {
					max_staleness: max_staleness.unwrap_or(std::time::Duration::ZERO),
					strict,
				});
			}
			("read_only", other) => {
				return Err(invalid_params(format!(
					"Expected read_only to be object, got {other:?}"
				)));
			}
			(other, _) => {
				return Err(invalid_params(format!("Unknown query option `{other}`")));
			}
		}
	}
	Ok(ParsedOptions {
		isolation,
		read_only,
		trace,
	})
}

/// The `trace` entry of a `query_stream` call's options: a list of kind names.
/// An unknown name is refused, so a client asking a server for a kind it does
/// not support learns so rather than reading an empty trace.
fn parse_trace_kinds(
	value: &PublicValue,
) -> Result<surrealdb_rpc::TraceKinds, surrealdb_types::Error> {
	let PublicValue::Array(names) = value else {
		return Err(invalid_params(format!("Expected trace to be an array, got {value:?}")));
	};
	let mut kinds = surrealdb_rpc::TraceKinds::NONE;
	for name in names.iter() {
		let PublicValue::String(name) = name else {
			return Err(invalid_params(format!("Expected a trace kind name, got {name:?}")));
		};
		let Some(kind) = surrealdb_rpc::TraceKinds::from_name(name) else {
			return Err(invalid_params(format!("Unknown trace kind `{name}`")));
		};
		kinds = kinds.union(kind);
	}
	Ok(kinds)
}

/// The id of the transaction a `begin` call opened, which the method answers with bare when it
/// took no options and as an object holding `id` when it did.
fn begun_id(result: DbResult) -> Option<PublicUuid> {
	let DbResult::Other(value) = result else {
		return None;
	};
	if let PublicValue::Uuid(id) = value {
		return Some(id);
	}
	if let PublicValue::Object(object) = &value
		&& let Some(PublicValue::Uuid(id)) = object.get("id")
	{
		return Some(*id);
	}
	None
}

/// Carry out `SET TRANSACTION` for the session transaction `open`, or for the next or every later
/// one when none is open. An open transaction that has run no statement is replaced by one opened
/// with the new modes; once it has run one, only a request for the modes it already has succeeds.
async fn set_transaction<T>(
	this: &T,
	session_lock: &tokio::sync::RwLock<Session>,
	session_id: Uuid,
	open: Option<Uuid>,
	modes: sql_dialect::TxnModes,
	for_session: bool,
	dialect: Dialect,
) -> Result<PublicValue, surrealdb_types::Error>
where
	T: RpcProtocol + ?Sized,
{
	let tag = sql_dialect::tagged_entry("SET");
	let Some(open) = open.filter(|_| !for_session) else {
		modes.store(&mut *session_lock.write().await, for_session);
		return Ok(tag);
	};
	let (wanted, same, fresh) = {
		let session = session_lock.read().await;
		let wanted = modes.over_open(&session);
		(
			wanted,
			wanted == sql_dialect::TxnModes::of_open(&session),
			sql_dialect::txn_is_fresh(&session),
		)
	};
	if same {
		return Ok(tag);
	}
	if !fresh {
		let (message, sqlstate, errno) = if dialect == Dialect::MySql {
			(
				"Transaction characteristics can't be changed while a transaction is in progress",
				"25001",
				Some(1568),
			)
		} else {
			("SET TRANSACTION ISOLATION LEVEL must be called before any query", "25001", None)
		};
		return Ok(sql_dialect::error_entry(message, sqlstate, errno));
	}
	let replacement = this.begin(None, session_id, wanted.begin_params()).await.map(begun_id);
	let id = match replacement {
		Ok(Some(id)) => id,
		Ok(None) => {
			return Ok(sql_dialect::error_entry(
				"the transaction could not be reopened",
				"XX000",
				None,
			));
		}
		Err(err) => return Ok(sql_dialect::engine_error_entry(&err, dialect)),
	};
	// The session lock was released while the replacement opened, so a concurrent call may have
	// run a statement in, ended or replaced the transaction. The replacement is published only if
	// the session still holds that same unused transaction.
	let mut session = session_lock.write().await;
	let unchanged = matches!(
		session.variables.get(sql_dialect::TXN_VAR),
		Some(PublicValue::Uuid(current)) if current.into_inner() == open
	) && sql_dialect::txn_is_fresh(&session);
	if !unchanged {
		drop(session);
		let abandoned: PublicArray = vec![PublicValue::Uuid(id)].into();
		// The replacement was never published, so a failure to cancel it is not observable.
		let _ = this.cancel(None, session_id, abandoned).await;
		return Ok(sql_dialect::error_entry(
			"the transaction changed while its characteristics were being set",
			"25001",
			(dialect == Dialect::MySql).then_some(1568),
		));
	}
	session.variables.insert(sql_dialect::TXN_VAR.to_owned(), PublicValue::Uuid(id));
	wanted.record(&mut session);
	drop(session);
	let old: PublicArray = vec![PublicValue::Uuid(PublicUuid::from(open))].into();
	// The replaced transaction held no statement and is abandoned either way, so a failure to
	// cancel it changes nothing the client can observe.
	let _ = this.cancel(None, session_id, old).await;
	Ok(tag)
}

/// Runs a query against a session the caller already holds a read guard on.
///
/// The session is passed in rather than looked up again on purpose. Every caller
/// reaches here holding a read guard on `RwLock<Session>`, and that lock is
/// task-fair: a writer queued between two acquisitions blocks the second read,
/// while the caller's outer guard blocks the writer. Re-locking here would
/// therefore deadlock against any concurrent `set` / `use` / `signin` /
/// `authenticate` on the same session, all of which a transport may apply
/// concurrently with this call. The same reasoning already keeps `handle_live`
/// off the lock — see the snapshot below.
async fn run_query<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Uuid,
	session: &Session,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
) -> Result<Vec<QueryResult>>
where
	T: RpcProtocol + ?Sized,
{
	run_query_as(this, txn, session_id, session, query, vars, None).await
}

/// [`run_query`] for a query that may run as one read-only block. See
/// [`ReadOnlyOptions`].
async fn run_query_as<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Uuid,
	session: &Session,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
	read_only: Option<ReadOnlyOptions>,
) -> Result<Vec<QueryResult>>
where
	T: RpcProtocol + ?Sized,
{
	if !T::LQ_SUPPORT && session.rt {
		return Err(bad_lq_config().into());
	}

	// A transaction UUID names a client-owned transaction to run inside — fail
	// if it is not one this connection holds. Without one the executor creates
	// and finalises a transaction per statement.
	let transaction = match txn {
		Some(txn_id) => Some(this.get_tx(txn_id).await.map_err(anyhow::Error::from)?),
		None => None,
	};
	run_query_in(this, transaction, session_id, session, query, vars, read_only).await
}

/// [`run_query_as`] for a request that asks for a trace: the statements run as a
/// streaming execution does, and its items are gathered back into the results
/// a buffered execution answers ([`surrealdb_rpc::Collected`]). The answer is
/// the results' wire form, each result carrying its statement's trace under
/// `trace` where it has one. The live queries the results start are registered
/// as [`run_query_in`] registers them.
#[expect(clippy::too_many_arguments, reason = "the call's session, query and options")]
async fn run_query_traced<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Uuid,
	session: &Session,
	text: &str,
	vars: Option<PublicVariables>,
	read_only: Option<ReadOnlyOptions>,
	trace: surrealdb_rpc::TraceKinds,
) -> Result<PublicValue>
where
	T: RpcProtocol + ?Sized,
{
	if !T::LQ_SUPPORT && session.rt {
		return Err(bad_lq_config().into());
	}
	let transaction = match txn {
		Some(txn_id) => Some(this.get_tx(txn_id).await.map_err(anyhow::Error::from)?),
		None => None,
	};
	let cancel = CALL_CANCEL.try_with(Clone::clone).ok().or_else(|| this.cancel_handle());
	let results = this
		.kvs_arc()
		.run_collected(
			QueryRequest::new(QuerySource::in_dialect(text, Dialect::SurrealQl), session)
				.with_variables(vars)
				.with_optional_transaction(transaction)
				.with_optional_cancel(cancel)
				.with_read_only(read_only)
				.with_trace(trace),
		)
		.await
		.map_err(anyhow::Error::from)?;
	let live_namespace = session.ns.clone();
	let live_database = session.db.clone();
	for (response, _) in &results {
		if response.query_type == QueryType::Live
			&& let Ok(PublicValue::Uuid(lqid)) = &response.result
		{
			this.handle_live(lqid, session_id, live_namespace.clone(), live_database.clone()).await;
		}
	}
	let answers = results
		.into_iter()
		.map(|(result, trace)| {
			let mut answer = result.into_value();
			if let (PublicValue::Object(answer), Some(trace)) = (&mut answer, trace) {
				answer.insert("trace", trace.into_value());
			}
			answer
		})
		.collect::<Vec<_>>();
	Ok(PublicValue::Array(answers.into()))
}

/// [`run_query_as`] with the transaction already in hand, for a caller that opened one itself.
async fn run_query_in<T>(
	this: &T,
	transaction: Option<Arc<crate::kvs::Transaction>>,
	session_id: Uuid,
	session: &Session,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
	read_only: Option<ReadOnlyOptions>,
) -> Result<Vec<QueryResult>>
where
	T: RpcProtocol + ?Sized,
{
	if !T::LQ_SUPPORT && session.rt {
		return Err(bad_lq_config().into());
	}
	let cancel = CALL_CANCEL.try_with(Clone::clone).ok().or_else(|| this.cancel_handle());
	let source = match query {
		QueryForm::Text(text, dialect) => QuerySource::in_dialect(text, dialect),
		QueryForm::Parsed(ast) => QuerySource::Ast(ast),
	};
	let res = this
		.kvs()
		.run(
			QueryRequest::new(source, session)
				.with_variables(vars)
				.with_optional_transaction(transaction)
				.with_optional_cancel(cancel)
				.with_read_only(read_only),
		)
		.await?;

	// Post-process hooks for web layer.
	//
	// `handle_live` needs the registering session's namespace / database
	// for the `surrealdb.live_query.active` gauge labelling. We snapshot
	// those off the session the caller lent us rather than letting
	// `handle_live` re-lock the same `RwLock<Session>`: that would be a
	// recursive read on a write-preferring lock, and any concurrent
	// session-mutating RPC on the same WebSocket would queue a writer
	// between the two reads and deadlock both futures.
	//
	// Registration is the only hook here. A statement that ends a subscription
	// does not report the id it ended, so the transport drops the registration
	// on that id's `Action::Killed` notification instead.
	let live_namespace = session.ns.clone();
	let live_database = session.db.clone();
	for response in &res {
		if response.query_type == QueryType::Live
			&& let Ok(PublicValue::Uuid(lqid)) = &response.result
		{
			this.handle_live(lqid, session_id, live_namespace.clone(), live_database.clone()).await;
		}
	}
	// Return the result to the client
	Ok(res)
}

tokio::task_local! {
	/// The cancel handle of the `pgsql` or `mysql` call a transport is dispatching, which the
	/// call's statements are cancelled through in place of the connection's.
	static CALL_CANCEL: CancelHandle;
}

/// Run `dispatch`, one `pgsql` or `mysql` call, so that tripping `handle` cancels the statement
/// it is running. A transport trips it to cancel that one request.
pub async fn with_call_cancel<F: std::future::Future>(
	handle: CancelHandle,
	dispatch: F,
) -> F::Output {
	CALL_CANCEL.scope(handle, dispatch).await
}

tokio::task_local! {
	/// The response format of the request a stateless transport is
	/// dispatching, which [`RpcProtocol::row_format`] answers by default.
	static RESPONSE_FORMAT: Option<surrealdb_rpc::RowFormat>;
}

/// Run `dispatch`, one request's dispatch on a transport shared by requests
/// that answer in different formats, with `format` as the format its response
/// is written in.
pub async fn with_response_format<F: std::future::Future>(
	format: Option<surrealdb_rpc::RowFormat>,
	dispatch: F,
) -> F::Output {
	RESPONSE_FORMAT.scope(format, dispatch).await
}

/// [`run_query`] on the streaming executor, with rows encoded in `format`:
/// the same results, the same live-query registration, with no public value
/// built for any row. See
/// [`collect_encoded`](crate::rpc::format::encoded::collect_encoded) for how
/// the execution is stopped if this future is dropped.
#[expect(clippy::too_many_arguments, reason = "each argument is a distinct input of the call")]
async fn run_query_encoded<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Uuid,
	session: &Session,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
	format: surrealdb_rpc::RowFormat,
	read_only: Option<ReadOnlyOptions>,
) -> Result<Vec<surrealdb_rpc::EncodedQueryResult>>
where
	T: RpcProtocol + ?Sized,
{
	if !T::LQ_SUPPORT && session.rt {
		return Err(bad_lq_config().into());
	}
	let source = match query {
		QueryForm::Text(text, dialect) => QuerySource::in_dialect(text, dialect),
		QueryForm::Parsed(ast) => QuerySource::Ast(ast),
	};
	let transaction = match txn {
		Some(txn_id) => Some(this.get_tx(txn_id).await.map_err(anyhow::Error::from)?),
		None => None,
	};
	let request = QueryRequest::new(source, session)
		.with_variables(vars)
		.with_optional_transaction(transaction)
		.with_read_only(read_only);
	let kvs = this.kvs_arc();
	let (res, encoded) =
		crate::rpc::format::encoded::collect_encoded(&kvs, request, this.cancel_handle(), format)
			.await?;
	// Registration of each `LIVE SELECT`, as `run_query` does it. A live
	// statement's value is its id, held on the result whatever streamed.
	let live_namespace = session.ns.clone();
	let live_database = session.db.clone();
	for response in &res {
		if response.query_type == QueryType::Live
			&& let Ok(PublicValue::Uuid(lqid)) = &response.result
		{
			this.handle_live(lqid, session_id, live_namespace.clone(), live_database.clone()).await;
		}
	}
	Ok(encoded)
}

#[cfg(test)]
mod tests {
	/// A streaming execution's live queries belong to the principal that created
	/// them, so the owner is only granted while the session is still acting as
	/// that principal — not merely while a session of that id exists.
	///
	/// `invalidate` keeps the session and clears its authentication, which is the
	/// case that made "does the session exist" the wrong question: it answers yes
	/// for a session whose `cleanup_lqs` has already swept the map.
	mod live_query_ownership {
		use std::sync::Arc;

		use surrealdb_iam::{Auth, Level};
		use surrealdb_types::HashMap;
		use tokio::sync::RwLock;
		use uuid::Uuid;

		use crate::dbs::{AuthPrincipalSnapshot, Session};
		use crate::rpc::live_query_owner;

		fn registered(session: Session) -> (HashMap<Uuid, Arc<RwLock<Session>>>, Uuid) {
			let id = Uuid::now_v7();
			let sessions = HashMap::new();
			sessions.insert(id, Arc::new(RwLock::new(session)));
			(sessions, id)
		}

		#[tokio::test]
		async fn an_unchanged_principal_owns_them() {
			let session = Session {
				au: Arc::new(Auth::for_root(surrealdb_iam::Role::Owner)),
				..Default::default()
			};
			let principal = AuthPrincipalSnapshot::capture(&session);
			let (sessions, id) = registered(session);

			assert!(
				live_query_owner(&sessions, id, &principal).await.is_some(),
				"the session is still who it was",
			);
		}

		#[tokio::test]
		async fn a_cleared_principal_does_not() {
			let session = Session {
				au: Arc::new(Auth::for_root(surrealdb_iam::Role::Owner)),
				..Default::default()
			};
			let principal = AuthPrincipalSnapshot::capture(&session);
			let (sessions, id) = registered(session);

			// What `invalidate` does: the session stays, its authentication goes.
			let entry = sessions.get(&id).expect("the session is registered");
			crate::iam::clear::clear(&mut *entry.write().await).expect("clear");

			assert!(
				live_query_owner(&sessions, id, &principal).await.is_none(),
				"an invalidated session does not own what it created before",
			);
		}

		#[tokio::test]
		async fn a_different_level_does_not() {
			let session = Session {
				au: Arc::new(Auth::for_root(surrealdb_iam::Role::Owner)),
				..Default::default()
			};
			let principal = AuthPrincipalSnapshot::capture(&session);
			let (sessions, id) = registered(session);

			let entry = sessions.get(&id).expect("the session is registered");
			entry.write().await.au = Arc::new(Auth::for_ns(surrealdb_iam::Role::Owner, "ns"));

			assert!(
				live_query_owner(&sessions, id, &principal).await.is_none(),
				"the same actor at a different level is a different principal",
			);
			// Guard against the assertion passing for the wrong reason.
			assert!(matches!(entry.read().await.au.level(), Level::Namespace(_)));
		}

		#[tokio::test]
		async fn a_session_that_has_gone_does_not() {
			let session = Session::default();
			let principal = AuthPrincipalSnapshot::capture(&session);
			let (sessions, _) = registered(session);

			assert!(
				live_query_owner(&sessions, Uuid::now_v7(), &principal).await.is_none(),
				"a session that was never registered owns nothing",
			);
		}
	}

	use std::collections::HashMap as StdHashMap;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::{Arc, Mutex};

	use surrealdb_types::HashMap;
	use tokio::sync::RwLock;
	use uuid::Uuid;

	use super::{DbResult, Method, RpcProtocol, bounded_by_query_timeout};
	use crate::dbs::Session;
	use crate::kvs::Datastore;
	use crate::types::{PublicArray, PublicValue};

	/// A minimal persisting `RpcProtocol`: sessions live in `sessions` (the
	/// in-memory map the trait requires) and are mirrored into `durable`
	/// (a stand-in for durable storage, holding serialized sessions).
	struct DurableRpc {
		ds: Arc<Datastore>,
		sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
		durable: Mutex<StdHashMap<Uuid, Vec<u8>>>,
		/// When set, [`forget_session`] fails, simulating a durable-storage
		/// error during teardown.
		fail_forget: AtomicBool,
	}

	impl DurableRpc {
		fn new(ds: Arc<Datastore>) -> Self {
			Self {
				ds,
				sessions: HashMap::new(),
				durable: Mutex::new(StdHashMap::new()),
				fail_forget: AtomicBool::new(false),
			}
		}
	}

	impl RpcProtocol for DurableRpc {
		const PERSIST_SESSIONS: bool = true;

		fn kvs(&self) -> &Datastore {
			&self.ds
		}
		fn kvs_arc(&self) -> Arc<Datastore> {
			Arc::clone(&self.ds)
		}
		fn version_data(&self) -> DbResult {
			DbResult::Other(PublicValue::String("surrealdb-test".to_owned()))
		}
		fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
			&self.sessions
		}
		fn cleanup_lqs(&self, _id: &Uuid) -> impl std::future::Future<Output = ()> + Send {
			std::future::ready(())
		}
		fn cleanup_all_lqs(&self) -> impl std::future::Future<Output = ()> + Send {
			std::future::ready(())
		}

		async fn load_session(&self, id: &Uuid) -> Option<Session> {
			let bytes = self.durable.lock().unwrap().get(id).cloned()?;
			serde_json::from_slice(&bytes).ok()
		}
		async fn persist_session(&self, id: &Uuid, session: &Session) {
			let bytes = serde_json::to_vec(session).unwrap();
			self.durable.lock().unwrap().insert(*id, bytes);
		}
		async fn forget_session(&self, id: &Uuid) -> Result<(), surrealdb_types::Error> {
			if self.fail_forget.load(Ordering::SeqCst) {
				return Err(surrealdb_types::Error::internal("durable delete failed".to_owned()));
			}
			self.durable.lock().unwrap().remove(id);
			Ok(())
		}
	}

	fn use_params(ns: &str, db: &str) -> PublicArray {
		vec![PublicValue::String(ns.to_owned()), PublicValue::String(db.to_owned())].into()
	}

	/// A buffered `query` that asks for a trace answers the results a plain one
	/// does, each carrying its statement's trace under `trace`.
	#[tokio::test]
	async fn a_buffered_query_answers_its_trace_beside_each_result() {
		use surrealdb_types::{Array, Object, RecordId, SurrealValue};

		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let text = |sql: &str| -> PublicArray { vec![PublicValue::String(sql.to_owned())].into() };
		let sid = Uuid::from_u128(61);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let setup = text("CREATE person:a, person:b; RELATE person:a->knows:ab->person:b;");
		rpc.execute(None, sid, Some(sid), Method::Query, setup).await.unwrap();

		let sql = "LET $x = 1; SELECT ->knows->person AS f FROM person:a;";
		let plain = rpc.execute(None, sid, Some(sid), Method::Query, text(sql)).await.unwrap();
		let mut options = Object::new();
		options.insert(
			"trace",
			PublicValue::Array(Array::from(vec![
				PublicValue::String("relations".to_owned()),
				PublicValue::String("variables".to_owned()),
			])),
		);
		let params: PublicArray = vec![
			PublicValue::String(sql.to_owned()),
			PublicValue::Object(Object::new()),
			PublicValue::Object(options),
		]
		.into();
		let traced = rpc.execute(None, sid, Some(sid), Method::Query, params).await.unwrap();

		let DbResult::Query(plain) = plain else {
			panic!("a plain query answers its results");
		};
		let DbResult::Other(PublicValue::Array(answers)) = traced else {
			panic!("a traced query answers its results as values");
		};
		assert_eq!(answers.len(), plain.len());
		let mut traces = Vec::new();
		for (answer, result) in answers.into_vec().into_iter().zip(plain) {
			let PublicValue::Object(mut answer) = answer else {
				panic!("each result is an object");
			};
			traces.push(answer.remove("trace"));
			let PublicValue::Object(expected) = result.into_value() else {
				panic!("each result is an object");
			};
			assert_eq!(answer.get("status"), expected.get("status"));
			assert_eq!(answer.get("result"), expected.get("result"));
		}
		let trace = |at: usize| {
			surrealdb_rpc::Trace::from_value(traces[at].clone().expect("a trace")).unwrap()
		};
		assert_eq!(trace(0).variables, vec![("x".to_owned(), PublicValue::from_t(1i64))]);
		let relations = trace(1).relations;
		assert_eq!(relations.len(), 1);
		assert_eq!(relations[0].edge, RecordId::new("knows", "ab"));
		assert_eq!(relations[0].out, Some(RecordId::new("person", "b")));
	}

	#[test]
	fn the_query_timeout_spares_methods_whose_commit_must_be_reported() {
		let exempt =
			[Method::Begin, Method::Commit, Method::Cancel, Method::Attach, Method::Detach];
		for method in exempt {
			assert!(
				!bounded_by_query_timeout(method, &PublicArray::new()),
				"{method} must not be cut short"
			);
		}
		let commit: PublicArray = vec![PublicValue::String("COMMIT".into())].into();
		for method in [Method::Pgsql, Method::Mysql] {
			assert!(!bounded_by_query_timeout(method, &commit), "{method} COMMIT must not be cut");
			assert!(
				bounded_by_query_timeout(method, &PublicArray::new()),
				"{method} queries are bounded"
			);
		}
		for method in [Method::Query, Method::Signin, Method::Signup, Method::Refresh, Method::Use]
		{
			assert!(
				bounded_by_query_timeout(method, &PublicArray::new()),
				"{method} must be bounded"
			);
		}
	}

	/// The `pgsql` and `mysql` methods under test: the method, the dialect's
	/// capability name, and a parameterised lookup in that dialect's syntax.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[allow(unused_mut, clippy::vec_init_then_push)]
	fn sql_dialect_methods() -> Vec<(Method, &'static str, &'static str)> {
		let mut methods = Vec::new();
		#[cfg(feature = "postgres")]
		methods.push((Method::Pgsql, "postgres", "SELECT name FROM person WHERE name = $1"));
		#[cfg(feature = "mysql")]
		methods.push((Method::Mysql, "mysql", "SELECT name FROM person WHERE name = ?"));
		methods
	}

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn text_params(text: &str, vars: Option<PublicValue>) -> PublicArray {
		let mut params = vec![PublicValue::String(text.to_owned())];
		params.extend(vars);
		params.into()
	}

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn number_value(n: i64) -> PublicValue {
		PublicValue::Number(surrealdb_types::Number::Int(n))
	}

	/// The statement entries of a dialect method's response object.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn statements_of(
		res: &Result<DbResult, surrealdb_types::Error>,
	) -> Vec<surrealdb_types::Object> {
		let Ok(DbResult::Other(PublicValue::Object(response))) = res else {
			return Vec::new();
		};
		let Some(PublicValue::Array(statements)) = response.get("statements") else {
			return Vec::new();
		};
		statements
			.clone()
			.into_vec()
			.into_iter()
			.filter_map(|entry| {
				if let PublicValue::Object(object) = entry {
					Some(object)
				} else {
					None
				}
			})
			.collect()
	}

	/// The rows of one statement entry.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn rows_of(entry: &surrealdb_types::Object) -> Vec<PublicValue> {
		match entry.get("rows") {
			Some(PublicValue::Array(rows)) => rows.clone().into_vec(),
			_ => Vec::new(),
		}
	}

	/// Whether a dialect method's response holds at least one row.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn has_rows(res: &Result<DbResult, surrealdb_types::Error>) -> bool {
		statements_of(res).iter().any(|entry| !rows_of(entry).is_empty())
	}

	/// Each dialect method runs text in its dialect against the session's
	/// namespace and database, with the call's variables, and what one session
	/// reads is confined to the database it selected.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_run_text_per_session() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let (a, b) = (Uuid::from_u128(51), Uuid::from_u128(52));
		for (sid, db) in [(a, "a"), (b, "b")] {
			rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
			rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", db)).await.unwrap();
		}
		let create = text_params("CREATE person:tobie SET name = 'Tobie'", None);
		rpc.execute(None, a, Some(a), Method::Query, create).await.unwrap();
		rpc.execute(
			None,
			b,
			Some(b),
			Method::Query,
			text_params("DEFINE TABLE person SCHEMALESS", None),
		)
		.await
		.unwrap();
		for (method, _, lookup) in sql_dialect_methods() {
			let vars = PublicValue::Object(surrealdb_types::object! { _1: "Tobie" });
			let found = rpc
				.execute(None, a, Some(a), method, text_params(lookup, Some(vars.clone())))
				.await;
			assert!(found.is_ok(), "{method} failed: {found:?}");
			let statements = statements_of(&found);
			assert_eq!(
				rows_of(&statements[0]),
				vec![PublicValue::Object(surrealdb_types::object! { name: "Tobie" })],
				"{method} found no row"
			);
			let other =
				rpc.execute(None, b, Some(b), method, text_params(lookup, Some(vars))).await;
			assert!(!has_rows(&other), "{method} leaked a row across sessions: {other:?}");
		}
	}

	/// Malformed text and malformed arguments are refused with an error rather
	/// than run.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_refuse_bad_input() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(53);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		for (method, _, _) in sql_dialect_methods() {
			let res =
				rpc.execute(None, sid, Some(sid), method, text_params("SELEC ((", None)).await;
			assert_eq!(
				statements_of(&res)[0].get("status"),
				Some(&PublicValue::String("ERR".into())),
				"{method}: unparsable text is an error entry"
			);
			rpc.execute(None, sid, Some(sid), method, vec![PublicValue::None].into())
				.await
				.expect_err("non-string text is an error");
			rpc.execute(None, sid, Some(sid), method, PublicArray::new())
				.await
				.expect_err("missing text is an error");
			let bad_vars = Some(PublicValue::String("x".to_owned()));
			rpc.execute(None, sid, Some(sid), method, text_params("SELECT 1", bad_vars))
				.await
				.expect_err("non-object vars are an error");
		}
	}

	/// A call may carry `options` and a batch of statements: defaults are
	/// accepted, an option asking for behaviour the body lacks is refused, and
	/// a batch returns the results of its entries in order.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_take_options_and_batches() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(57);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let with_options = |text: &str, options: surrealdb_types::Object| -> PublicArray {
			vec![PublicValue::String(text.into()), PublicValue::None, PublicValue::Object(options)]
				.into()
		};
		for (method, _, _) in sql_dialect_methods() {
			let res = rpc
				.execute(
					None,
					sid,
					Some(sid),
					method,
					with_options("SELECT 1 AS a", surrealdb_types::object! { rowMode: "array" }),
				)
				.await;
			let statements = statements_of(&res);
			assert_eq!(
				rows_of(&statements[0]),
				vec![PublicValue::Array(surrealdb_types::array![1])],
				"{method} array rows"
			);
			for refused in [
				surrealdb_types::object! { valueMode: "binary" },
				surrealdb_types::object! { rowMode: "tuples" },
				surrealdb_types::object! { nonsense: true },
			] {
				rpc.execute(None, sid, Some(sid), method, with_options("SELECT 1", refused))
					.await
					.expect_err("an unsupported option is refused");
			}
			let batch = PublicValue::Array(surrealdb_types::array![
				"SELEC ((",
				"SELECT 1",
				surrealdb_types::object! { text: "SELECT 2" }
			]);
			let res = rpc.execute(None, sid, Some(sid), method, vec![batch].into()).await;
			let statements = statements_of(&res);
			assert_eq!(statements.len(), 3, "{method}");
			assert_eq!(
				statements[0].get("status"),
				Some(&PublicValue::String("ERR".into())),
				"{method} parse error is an entry"
			);
			assert!(
				matches!(
					statements[0].get("error"),
					Some(PublicValue::Object(error)) if matches!(error.get("sqlstate"), Some(PublicValue::String(_)))
				),
				"{method} error carries a sqlstate"
			);
			assert!(!rows_of(&statements[1]).is_empty(), "{method} runs after a failed entry");
			assert!(!rows_of(&statements[2]).is_empty(), "{method}");
		}
	}

	/// Each SQL statement of a text answers with one entry, however many engine
	/// statements it lowers to, and a failure stops the statements after it.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_answer_one_entry_per_statement() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(62);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		for (method, _, _) in sql_dialect_methods() {
			let res = rpc
				.execute(None, sid, Some(sid), method, text_params("SELECT 1; SELECT 2", None))
				.await;
			assert_eq!(statements_of(&res).len(), 2, "{method} one entry per statement");
			let ddl = if method == Method::Pgsql {
				"CREATE TABLE widget_pg (a int, b text, c int)"
			} else {
				"CREATE TABLE widget_my (a INT, b TEXT, c INT)"
			};
			let res = rpc.execute(None, sid, Some(sid), method, text_params(ddl, None)).await;
			let statements = statements_of(&res);
			assert_eq!(statements.len(), 1, "{method} DDL answers with one entry: {res:?}");
			assert_eq!(
				statements[0].get("status"),
				Some(&PublicValue::String("OK".into())),
				"{method}: {res:?}"
			);
			let text = "SELECT * FROM missing_table; SELECT 1";
			let res = rpc.execute(None, sid, Some(sid), method, text_params(text, None)).await;
			let statements = statements_of(&res);
			if statements.len() == 2 {
				assert_eq!(statements[0].get("status"), Some(&PublicValue::String("ERR".into())));
				assert_eq!(
					statements[1].get("status"),
					Some(&PublicValue::String("ERR".into())),
					"{method} a failure stops the statements after it"
				);
			}
		}
	}

	/// A result carries its columns in projection order with their wire types, and
	/// a command tag with the affected-row count.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_results_are_typed_and_tagged() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(63);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let seed =
			text_params("DEFINE TABLE person SCHEMALESS; CREATE person:a SET name = 'a'", None);
		rpc.execute(None, sid, Some(sid), Method::Query, seed).await.unwrap();
		let array = PublicValue::Object(surrealdb_types::object! { rowMode: "array" });
		for (method, _, _) in sql_dialect_methods() {
			let params: PublicArray = vec![
				PublicValue::String("SELECT 1 AS b, 2 AS a".into()),
				PublicValue::None,
				array.clone(),
			]
			.into();
			let res = rpc.execute(None, sid, Some(sid), method, params).await;
			let statements = statements_of(&res);
			let Some(PublicValue::Array(columns)) = statements[0].get("columns") else {
				panic!("{method} carries no columns: {res:?}");
			};
			let names: Vec<_> = columns
				.iter()
				.filter_map(|column| {
					if let PublicValue::Object(column) = column
						&& let Some(PublicValue::String(name)) = column.get("name")
					{
						Some(name.clone())
					} else {
						None
					}
				})
				.collect();
			assert_eq!(names, ["b", "a"], "{method} projection order: {res:?}");
			assert!(
				columns
					.iter()
					.all(|column| matches!(column, PublicValue::Object(c) if matches!(c.get("type"), Some(PublicValue::Object(_))))),
				"{method} every column is typed: {res:?}"
			);
			assert_eq!(
				rows_of(&statements[0]),
				vec![PublicValue::Array(surrealdb_types::array![1, 2])],
				"{method}"
			);
			let Some(PublicValue::Object(command)) = statements[0].get("command") else {
				panic!("{method} carries no command: {res:?}");
			};
			let tag = if method == Method::Pgsql {
				"SELECT 1"
			} else {
				"SELECT"
			};
			assert_eq!(command.get("tag"), Some(&PublicValue::String(tag.into())), "{method}");
			let insert = "INSERT INTO person (name) VALUES ('z')";
			let res = rpc.execute(None, sid, Some(sid), method, text_params(insert, None)).await;
			let statements = statements_of(&res);
			let Some(PublicValue::Object(command)) = statements[0].get("command") else {
				panic!("{method} insert carries no command: {res:?}");
			};
			let tag = if method == Method::Pgsql {
				"INSERT 0 1"
			} else {
				"INSERT"
			};
			assert_eq!(
				command.get("tag"),
				Some(&PublicValue::String(tag.into())),
				"{method}: {res:?}"
			);
			assert_eq!(command.get("rows_affected"), Some(&number_value(1)), "{method}: {res:?}");
			assert!(rows_of(&statements[0]).is_empty(), "{method} a write answers with a count");
		}
	}

	/// `valueMode: "text"` answers every value as its text-protocol form.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_text_mode_answers_text_values() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(65);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let options =
			PublicValue::Object(surrealdb_types::object! { valueMode: "text", rowMode: "array" });
		for (method, _, _) in sql_dialect_methods() {
			let params: PublicArray = vec![
				PublicValue::String("SELECT true AS t, 12 AS n, 'x' AS s, NULL AS z".into()),
				PublicValue::None,
				options.clone(),
			]
			.into();
			let res = rpc.execute(None, sid, Some(sid), method, params).await;
			let truth = if method == Method::Pgsql {
				"t"
			} else {
				"1"
			};
			assert_eq!(
				rows_of(&statements_of(&res)[0]),
				vec![PublicValue::Array(surrealdb_types::array![
					truth,
					"12",
					"x",
					PublicValue::Null
				])],
				"{method}: {res:?}"
			);
		}
	}

	/// PostgreSQL `SET`, `SHOW` and `RESET` act on the session's settings.
	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn pgsql_settings_are_set_shown_and_reset() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(66);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let run = |text: &'static str| {
			let rpc = &rpc;
			async move {
				let res =
					rpc.execute(None, sid, Some(sid), Method::Pgsql, text_params(text, None)).await;
				statements_of(&res)
			}
		};
		let shown = |statements: &[surrealdb_types::Object]| rows_of(&statements[0]);
		assert_eq!(
			shown(&run("SHOW timezone").await),
			vec![PublicValue::Object(surrealdb_types::object! { timezone: "UTC" })]
		);
		let set = run("SET timezone = 'Europe/Paris'").await;
		assert_eq!(set[0].get("status"), Some(&PublicValue::String("OK".into())));
		assert_eq!(
			shown(&run("SHOW timezone").await),
			vec![PublicValue::Object(surrealdb_types::object! { timezone: "Europe/Paris" })]
		);
		run("RESET timezone").await;
		assert_eq!(
			shown(&run("SHOW timezone").await),
			vec![PublicValue::Object(surrealdb_types::object! { timezone: "UTC" })]
		);
		let unknown = run("SHOW no_such_setting").await;
		assert_eq!(unknown[0].get("status"), Some(&PublicValue::String("ERR".into())));
		// A setting named like a transaction variable cannot touch the transaction state.
		run("SET txn = 'x'").await;
		let lock = rpc.get_session(&sid).await.unwrap();
		assert!(lock.read().await.variables.get("sql_txn").is_none());
	}

	/// A describe stops when its call is cancelled.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_describe_can_be_cancelled() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(70);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let options = PublicValue::Object(surrealdb_types::object! { describe: true });
		for (method, _, _) in sql_dialect_methods() {
			let handle = crate::ctx::CancelHandle::new();
			handle.trip();
			let params: PublicArray =
				vec![PublicValue::String("SELECT 1".into()), PublicValue::None, options.clone()]
					.into();
			let res =
				super::with_call_cancel(handle, rpc.execute(None, sid, Some(sid), method, params))
					.await;
			assert_eq!(
				statements_of(&res)[0].get("status"),
				Some(&PublicValue::String("ERR".into())),
				"{method} a cancelled describe is an error: {res:?}"
			);
		}
	}

	/// An atomic call refuses what it cannot honour: an envelope transaction, a session
	/// statement inside it, and (for gql) the options it does not support.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_atomic_calls_refuse_conflicts() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(68);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let atomic = PublicValue::Object(surrealdb_types::object! { atomic: true });
		for (method, _, _) in sql_dialect_methods() {
			let params = |text: &str| -> PublicArray {
				vec![PublicValue::String(text.into()), PublicValue::None, atomic.clone()].into()
			};
			rpc.execute(Some(Uuid::from_u128(1)), sid, Some(sid), method, params("SELECT 1"))
				.await
				.expect_err("an envelope transaction conflicts with atomic");
			let both =
				PublicValue::Object(surrealdb_types::object! { atomic: true, describe: true });
			let combined: PublicArray =
				vec![PublicValue::String("SELECT 1".into()), PublicValue::None, both].into();
			rpc.execute(None, sid, Some(sid), method, combined)
				.await
				.expect_err("atomic and describe cannot be combined");
			// A bad envelope transaction fails the call before an entry acts on the session.
			if method == Method::Mysql {
				let batch = PublicValue::Array(surrealdb_types::array!["USE other", "SELECT 1"]);
				rpc.execute(Some(Uuid::from_u128(2)), sid, Some(sid), method, vec![batch].into())
					.await
					.expect_err("a missing envelope transaction is a call error");
				let lock = rpc.get_session(&sid).await.unwrap();
				assert_eq!(lock.read().await.db.as_deref(), Some("app"));
			}
			let set = if method == Method::Pgsql {
				"SET timezone = 'UTC'"
			} else {
				"SET autocommit = 1"
			};
			let res = rpc.execute(None, sid, Some(sid), method, params(set)).await;
			let statements = statements_of(&res);
			assert_eq!(
				statements[0].get("status"),
				Some(&PublicValue::String("ERR".into())),
				"{method} SET in an atomic call: {res:?}"
			);
		}
		#[cfg(feature = "gql")]
		for options in [
			surrealdb_types::object! { atomic: true },
			surrealdb_types::object! { valueMode: "text" },
		] {
			let params: PublicArray = vec![
				PublicValue::String("{ __typename }".into()),
				PublicValue::None,
				PublicValue::Object(options),
			]
			.into();
			rpc.execute(None, sid, Some(sid), Method::Gql, params)
				.await
				.expect_err("gql refuses the SQL options");
		}
	}

	/// A call whose cancel handle has been tripped answers with the dialect's cancellation
	/// error, and a call that is not cancelled is unaffected.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_calls_can_be_cancelled() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(67);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		for (method, _, _) in sql_dialect_methods() {
			let handle = crate::ctx::CancelHandle::new();
			handle.trip();
			let res = super::with_call_cancel(
				handle,
				rpc.execute(None, sid, Some(sid), method, text_params("SELECT 1", None)),
			)
			.await;
			let statements = statements_of(&res);
			assert_eq!(
				statements[0].get("status"),
				Some(&PublicValue::String("ERR".into())),
				"{method} a cancelled call is an error: {res:?}"
			);
			let expected = if method == Method::Pgsql {
				"57014"
			} else {
				"70100"
			};
			assert!(
				matches!(
					statements[0].get("error"),
					Some(PublicValue::Object(error)) if error.get("sqlstate") == Some(&PublicValue::String(expected.into()))
				),
				"{method} cancellation sqlstate: {res:?}"
			);
			let res =
				rpc.execute(None, sid, Some(sid), method, text_params("SELECT 1", None)).await;
			assert_eq!(
				statements_of(&res)[0].get("status"),
				Some(&PublicValue::String("OK".into())),
				"{method}"
			);
		}
	}

	/// An atomic call commits all its statements or none of them.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_atomic_calls_commit_all_or_nothing() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(64);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let seed = text_params("DEFINE TABLE person SCHEMALESS", None);
		rpc.execute(None, sid, Some(sid), Method::Query, seed).await.unwrap();
		let atomic = PublicValue::Object(surrealdb_types::object! { atomic: true });
		let committed = |res: &Result<DbResult, surrealdb_types::Error>| {
			let Ok(DbResult::Other(PublicValue::Object(response))) = res else {
				panic!("not a response: {res:?}");
			};
			let Some(PublicValue::Object(transaction)) = response.get("transaction") else {
				panic!("no transaction: {res:?}");
			};
			transaction.get("committed").cloned()
		};
		for (method, _, _) in sql_dialect_methods() {
			let batch = |texts: &[&str]| -> PublicArray {
				let entries: Vec<PublicValue> =
					texts.iter().map(|text| PublicValue::String((*text).to_owned())).collect();
				vec![PublicValue::Array(entries.into()), PublicValue::None, atomic.clone()].into()
			};
			let name = if method == Method::Pgsql {
				"pg"
			} else {
				"my"
			};
			let failing = format!("INSERT INTO person (name) VALUES ('{name}-lost')");
			let res = rpc
				.execute(None, sid, Some(sid), method, batch(&[failing.as_str(), "SELEC (("]))
				.await;
			assert_eq!(committed(&res), Some(PublicValue::Bool(false)), "{method}: {res:?}");
			let lookup = format!("SELECT name FROM person WHERE name = '{name}-lost'");
			let res = rpc.execute(None, sid, Some(sid), method, text_params(&lookup, None)).await;
			assert!(!has_rows(&res), "{method} a failed atomic call left a row: {res:?}");
			let kept = format!("INSERT INTO person (name) VALUES ('{name}-kept')");
			let res = rpc.execute(None, sid, Some(sid), method, batch(&[kept.as_str()])).await;
			assert_eq!(committed(&res), Some(PublicValue::Bool(true)), "{method}: {res:?}");
			let lookup = format!("SELECT name FROM person WHERE name = '{name}-kept'");
			let res = rpc.execute(None, sid, Some(sid), method, text_params(&lookup, None)).await;
			assert!(has_rows(&res), "{method} a committed atomic call left no row: {res:?}");
		}
	}

	/// `describe` answers with the columns and parameters of a select without
	/// running it, and only past the same gates as a run.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_describe_without_running() {
		use crate::dbs::Capabilities;
		use crate::dbs::capabilities::{RouteTarget, Targets};
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(58);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let ddl =
			text_params("DEFINE TABLE person SCHEMALESS; CREATE person:tobie SET name = 'T'", None);
		rpc.execute(None, sid, Some(sid), Method::Query, ddl).await.unwrap();
		let options = PublicValue::Object(surrealdb_types::object! { describe: true });
		for (method, _, lookup) in sql_dialect_methods() {
			let params: PublicArray =
				vec![PublicValue::String(lookup.into()), PublicValue::None, options.clone()].into();
			let res = rpc.execute(None, sid, Some(sid), method, params).await;
			let Ok(DbResult::Other(PublicValue::Object(response))) = &res else {
				panic!("{method} describe failed: {res:?}");
			};
			let Some(PublicValue::Array(params)) = response.get("params") else {
				panic!("{method} describe carries no params");
			};
			assert_eq!(params.len(), 1, "{method} parameter count");
			let statements = statements_of(&res);
			assert_eq!(statements[0].get("status"), Some(&PublicValue::String("OK".into())));
			let Some(PublicValue::Array(columns)) = statements[0].get("columns") else {
				panic!("{method} describe carries no columns");
			};
			let Some(PublicValue::Object(column)) = columns.first() else {
				panic!("{method} describes no column");
			};
			assert_eq!(column.get("name"), Some(&PublicValue::String("name".into())));
			assert!(rows_of(&statements[0]).is_empty(), "{method} describe ran the statement");
		}
		let denied = Targets::Some([RouteTarget::Postgres, RouteTarget::MySql].into());
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().without_http_routes(denied))
			.build_with_path("memory")
			.await
			.unwrap();
		let rpc = DurableRpc::new(ds);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		for (method, _, lookup) in sql_dialect_methods() {
			let params: PublicArray =
				vec![PublicValue::String(lookup.into()), PublicValue::None, options.clone()].into();
			rpc.execute(None, sid, Some(sid), method, params)
				.await
				.expect_err("a denied dialect cannot be described");
		}
	}

	/// `describe` reports one parameter per placeholder of a write as well as of a select.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_describe_counts_write_parameters() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(69);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		let seed = text_params("DEFINE TABLE person SCHEMALESS", None);
		rpc.execute(None, sid, Some(sid), Method::Query, seed).await.unwrap();
		let options = PublicValue::Object(surrealdb_types::object! { describe: true });
		for (method, _, _) in sql_dialect_methods() {
			let text = if method == Method::Pgsql {
				"INSERT INTO person (name, age) VALUES ($1, $2)"
			} else {
				"INSERT INTO person (name, age) VALUES (?, ?)"
			};
			let params: PublicArray =
				vec![PublicValue::String(text.into()), PublicValue::None, options.clone()].into();
			let res = rpc.execute(None, sid, Some(sid), method, params).await;
			let Ok(DbResult::Other(PublicValue::Object(response))) = &res else {
				panic!("{method}: {res:?}");
			};
			let Some(PublicValue::Array(params)) = response.get("params") else {
				panic!("{method} describes no params: {res:?}");
			};
			assert_eq!(params.len(), 2, "{method}: {res:?}");
		}
	}

	/// A transaction-control statement that cannot act on the session is an
	/// `ERR` entry and leaves the session idle; the response always reports
	/// the transaction status.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_report_transaction_status() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(59);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		// A transaction id the session names but nothing holds is cleared, not reported.
		{
			let lock = rpc.get_session(&sid).await.unwrap();
			lock.write().await.variables.insert(
				"sql_txn".to_owned(),
				PublicValue::Uuid(surrealdb_types::Uuid::from(Uuid::from_u128(60))),
			);
		}
		for (method, _, _) in sql_dialect_methods() {
			let res =
				rpc.execute(None, sid, Some(sid), method, text_params("SELECT 1", None)).await;
			let Ok(DbResult::Other(PublicValue::Object(response))) = &res else {
				panic!("{method}: {res:?}");
			};
			let Some(PublicValue::Object(transaction)) = response.get("transaction") else {
				panic!("{method} reports no transaction");
			};
			assert_eq!(transaction.get("status"), Some(&PublicValue::String("idle".into())));
			let lock = rpc.get_session(&sid).await.unwrap();
			assert!(lock.read().await.variables.get("sql_txn").is_none());
		}
		for name in ["sql_txn", "sql_txn_failed"] {
			let params: PublicArray =
				vec![PublicValue::String(name.into()), PublicValue::Bool(true)].into();
			rpc.execute(None, sid, Some(sid), Method::Set, params)
				.await
				.expect_err("a client cannot write the transaction state");
		}
		for (method, _, _) in sql_dialect_methods() {
			let res = rpc.execute(None, sid, Some(sid), method, text_params("BEGIN", None)).await;
			let statements = statements_of(&res);
			assert_eq!(statements[0].get("status"), Some(&PublicValue::String("ERR".into())));
			let Ok(DbResult::Other(PublicValue::Object(response))) = &res else {
				panic!("{method}: {res:?}");
			};
			let Some(PublicValue::Object(transaction)) = response.get("transaction") else {
				panic!("{method} reports no transaction");
			};
			assert_eq!(transaction.get("status"), Some(&PublicValue::String("idle".into())));
		}
	}

	/// The session preflight a query runs applies to control statements and
	/// `describe` as well: an anonymous session is refused when auth is on.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_run_the_session_preflight() {
		let ds = Datastore::builder().with_auth(true).build_with_path("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(61);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::default())));
		let describe = PublicValue::Object(surrealdb_types::object! { describe: true });
		for (method, _, _) in sql_dialect_methods() {
			for text in ["BEGIN", "SELECT 1", ""] {
				rpc.execute(None, sid, Some(sid), method, text_params(text, None))
					.await
					.expect_err("an anonymous session is refused");
			}
			let params: PublicArray =
				vec![PublicValue::String("SELECT 1".into()), PublicValue::None, describe.clone()]
					.into();
			rpc.execute(None, sid, Some(sid), method, params)
				.await
				.expect_err("an anonymous session cannot describe");
		}
	}

	/// A dialect whose capability is denied is refused by name on its own
	/// method; SurrealQL is unaffected, and a denied RPC method name refuses
	/// the method whatever the capability.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_capability_denial_has_no_bypass() {
		use crate::dbs::Capabilities;
		use crate::dbs::capabilities::{MethodTarget, RouteTarget, Targets};
		let denied_routes = Targets::Some([RouteTarget::Postgres, RouteTarget::MySql].into());
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().without_http_routes(denied_routes))
			.build_with_path("memory")
			.await
			.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(54);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		for (method, capability, _) in sql_dialect_methods() {
			for text in ["BEGIN", "COMMIT", "USE other"] {
				rpc.execute(None, sid, Some(sid), method, text_params(text, None))
					.await
					.expect_err("a denied dialect is refused for control statements");
			}
			let err = rpc
				.execute(None, sid, Some(sid), method, text_params("SELECT 1", None))
				.await
				.expect_err("a denied dialect is refused");
			assert!(
				err.to_string().contains(capability),
				"the refusal names `{capability}`: {err}"
			);
		}
		let surql = text_params("RETURN 1", None);
		rpc.execute(None, sid, Some(sid), Method::Query, surql).await.unwrap();
		// A denied method name refuses the method even with its capability held.
		let denied_methods = Targets::Some(
			[Method::Pgsql, Method::Mysql]
				.into_iter()
				.map(|method| MethodTarget {
					method,
				})
				.collect(),
		);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().without_rpc_methods(denied_methods))
			.build_with_path("memory")
			.await
			.unwrap();
		let rpc = DurableRpc::new(ds);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		for (method, _, _) in sql_dialect_methods() {
			let err = rpc
				.execute(None, sid, Some(sid), method, text_params("SELECT 1", None))
				.await
				.expect_err("a denied method name is refused");
			assert!(err.is_not_allowed(), "{method}: {err}");
		}
	}

	/// The dialect methods give a record user no more than `query` does: a
	/// subject the arbitrary-query gate refuses is refused here too, and one
	/// it admits is bound by the permissions on the tables it reads.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn sql_dialect_methods_follow_subject_gate_and_permissions() {
		use crate::dbs::Capabilities;
		use crate::dbs::capabilities::{ArbitraryQueryTarget, Targets};
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(Arc::clone(&ds));
		let owner = Uuid::from_u128(55);
		rpc.set_session(owner, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, owner, Some(owner), Method::Use, use_params("app", "app")).await.unwrap();
		let ddl = "DEFINE TABLE person SCHEMALESS PERMISSIONS NONE; \
		           CREATE person:tobie SET name = 'Tobie'";
		rpc.execute(None, owner, Some(owner), Method::Query, text_params(ddl, None)).await.unwrap();
		let record = Uuid::from_u128(56);
		let session = Session::for_record("app", "app", "acc", PublicValue::String("u".into()));
		rpc.set_session(record, Arc::new(RwLock::new(session)));
		for (method, _, lookup) in sql_dialect_methods() {
			let params = text_params(
				lookup,
				Some(PublicValue::Object(surrealdb_types::object! { _1: "Tobie" })),
			);
			let res = rpc.execute(None, record, Some(record), method, params).await;
			assert!(!has_rows(&res), "{method} returned a row the record user cannot read");
		}
		let ds = Datastore::builder()
			.with_capabilities(
				Capabilities::all()
					.without_arbitrary_query(Targets::Some([ArbitraryQueryTarget::Record].into())),
			)
			.build_with_path("memory")
			.await
			.unwrap();
		let rpc = DurableRpc::new(ds);
		let session = Session::for_record("app", "app", "acc", PublicValue::String("u".into()));
		rpc.set_session(record, Arc::new(RwLock::new(session)));
		for (method, _, _) in sql_dialect_methods() {
			rpc.execute(None, record, Some(record), method, text_params("SELECT 1", None))
				.await
				.expect_err("a record subject denied arbitrary queries is refused");
		}
	}

	/// Regression (GHSA-2v9j): the RPC `use` handler (`yuse`) enforces the same
	/// tenant boundary as the SurrealQL `USE` statement and `process_use` - a
	/// Database-level principal cannot pivot into a foreign tenant over RPC.
	#[tokio::test]
	async fn use_rpc_denies_cross_tenant_pivot_for_database_user() {
		use surrealdb_iam::{Level, Role};

		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(41);

		// A Database Editor confined to ns_a/db_a, already on its own tenant.
		let sess =
			Session::for_level(Level::Database("ns_a".to_owned(), "db_a".to_owned()), Role::Editor)
				.with_ns("ns_a")
				.with_db("db_a");
		rpc.set_session(sid, Arc::new(RwLock::new(sess)));

		let err = rpc
			.execute(None, sid, Some(sid), Method::Use, use_params("ns_b", "db_b"))
			.await
			.expect_err("cross-tenant RPC use must be denied");
		assert!(
			err.to_string().contains("ns_b"),
			"expected a tenant-boundary denial naming the foreign namespace, got: {err}"
		);

		// The session must not have been pivoted into tenant B.
		let lock = rpc.get_session(&sid).await.unwrap();
		let session = lock.read().await;
		assert_eq!(session.ns.as_deref(), Some("ns_a"));
		assert_eq!(session.db.as_deref(), Some("db_a"));
	}

	/// Sanity: the RPC `use` gate still lets a principal re-select its own tenant.
	#[tokio::test]
	async fn use_rpc_allows_same_tenant_for_database_user() {
		use surrealdb_iam::{Level, Role};

		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(42);

		let sess =
			Session::for_level(Level::Database("ns_a".to_owned(), "db_a".to_owned()), Role::Editor)
				.with_ns("ns_a")
				.with_db("db_a");
		rpc.set_session(sid, Arc::new(RwLock::new(sess)));

		rpc.execute(None, sid, Some(sid), Method::Use, use_params("ns_a", "db_a"))
			.await
			.expect("own-tenant RPC use must be allowed");
	}

	/// A root owner's session on `test`/`test`, over a database that holds
	/// `reading`, a table whose `id` names the components `city` and `time`.
	async fn keyed_rpc() -> (DurableRpc, Uuid) {
		let db = crate::exec::operators::test_util::TestDb::new(
			"DEFINE TABLE reading SCHEMALESS;
			DEFINE FIELD id ON reading TYPE [city: string, time: datetime];",
		)
		.await;
		let rpc = DurableRpc::new(Arc::clone(db.datastore()));
		let sid = Uuid::from_u128(51);
		let session = Session::owner().with_ns("test").with_db("test");
		rpc.set_session(sid, Arc::new(RwLock::new(session)));
		(rpc, sid)
	}

	/// The value `method` answers with for `params`, or its failure.
	async fn answer(
		rpc: &DurableRpc,
		sid: Uuid,
		method: Method,
		params: Vec<PublicValue>,
	) -> Result<PublicValue, surrealdb_types::Error> {
		let res = rpc.execute(None, sid, Some(sid), method, params.into()).await?;
		let DbResult::Other(value) = res else {
			panic!("`{method}` answered {res:?}");
		};
		Ok(value)
	}

	/// `select` of one record reads it through a variable, so the statement is
	/// kept compiled for later requests: a second record of the table is a hit.
	/// What it answers, a missing record, a table that does not exist and a
	/// record a permission hides are answered as the statement with the record
	/// written in place answers them.
	#[tokio::test]
	async fn select_rpc_of_one_record_is_kept_compiled_and_answers_as_written() {
		use surrealdb_types::ToSql;
		let db = crate::exec::operators::test_util::TestDb::new_with_auth(
			"DEFINE ACCESS user ON DATABASE TYPE RECORD;
			DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
			CREATE doc:1 SET owner = user:alice, v = 1;
			CREATE doc:2 SET owner = user:bob, v = 2;",
		)
		.await;
		let ds = Arc::clone(db.datastore());
		let rpc = DurableRpc::new(Arc::clone(&ds));
		let owner = Uuid::from_u128(61);
		let alice = Uuid::from_u128(62);
		rpc.set_session(
			owner,
			Arc::new(RwLock::new(Session::owner().with_ns("test").with_db("test"))),
		);
		let user = PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		rpc.set_session(
			alice,
			Arc::new(RwLock::new(Session::for_record("test", "test", "user", user))),
		);
		let written = |sid: Uuid, id: &str| {
			let ds = Arc::clone(&ds);
			let rpc = &rpc;
			let id = id.to_owned();
			async move {
				let session = rpc.get_session(&sid).await.unwrap();
				let session = session.read().await.clone();
				let mut out = ds
					.execute(&format!("SELECT * FROM ONLY {id}"), &session, None)
					.await
					.expect("the request runs");
				match out.remove(0).result {
					Ok(value) => value.to_sql(),
					Err(e) => format!("error: {}", e.message()),
				}
			}
		};
		let selected = |sid: Uuid, id: &str| {
			let rpc = &rpc;
			let id = crate::syn::value(id).unwrap();
			async move {
				match answer(rpc, sid, Method::Select, vec![id]).await {
					Ok(value) => value.to_sql(),
					Err(e) => format!("error: {}", e.message()),
				}
			}
		};
		// The statements written with the record in place are kept apart from the RPC's.
		let (one, two) = (written(owner, "doc:1").await, written(owner, "doc:2").await);
		let base = ds.statement_cache_stats();
		assert_eq!(selected(owner, "doc:1").await, one);
		assert_eq!(selected(owner, "doc:2").await, two);
		let kept = ds.statement_cache_stats();
		assert_eq!((kept.hits - base.hits, kept.misses - base.misses), (1, 1));
		for (sid, id) in [
			(owner, "doc:9"),
			(owner, "nosuch:1"),
			(alice, "doc:1"),
			(alice, "doc:2"),
			(alice, "nosuch:1"),
		] {
			assert_eq!(selected(sid, id).await, written(sid, id).await, "{id}");
		}
	}

	/// The London reading of the first of January, as a record id and as the
	/// record `temp` leaves it.
	fn london(temp: i64) -> (PublicValue, PublicValue) {
		let id = crate::syn::value("reading:['London', d'2024-01-01T00:00:00Z']").unwrap();
		let record = crate::syn::value(&format!(
			"{{ city: 'London', id: reading:['London', d'2024-01-01T00:00:00Z'], temp: {temp}, time: d'2024-01-01T00:00:00Z' }}"
		))
		.unwrap();
		(id, record)
	}

	/// The components of a record id are derived fields, so `create` takes the
	/// key from the ones its content carries and refuses a content that
	/// leaves one out: no key is drawn at random for it.
	#[tokio::test]
	async fn create_rpc_derives_the_key_from_the_components_of_its_content() {
		let (rpc, sid) = keyed_rpc().await;
		let (_, record) = london(1);
		let created = answer(
			&rpc,
			sid,
			Method::Create,
			vec![
				PublicValue::String("reading".to_owned()),
				crate::syn::value("{ city: 'London', time: d'2024-01-01T00:00:00Z', temp: 1 }")
					.unwrap(),
			],
		)
		.await
		.expect("the content names every component");
		assert_eq!(created, record);

		let refused = answer(
			&rpc,
			sid,
			Method::Create,
			vec![
				PublicValue::String("reading".to_owned()),
				crate::syn::value("{ city: 'London', temp: 2 }").unwrap(),
			],
		)
		.await
		.expect_err("the content leaves the time out");
		assert_eq!(
			refused.to_string(),
			"Cannot create a record in `reading` without the record id component `time`: set `time`, give the record id, or define a DEFAULT on the `id` field"
		);
	}

	/// `merge` may restate a component of the record id and may not change
	/// it; the refusal names the value written, and the record is as it was.
	#[tokio::test]
	async fn merge_rpc_cannot_change_a_component_of_the_record_id() {
		let (rpc, sid) = keyed_rpc().await;
		let (id, _) = london(1);
		answer(
			&rpc,
			sid,
			Method::Create,
			vec![
				PublicValue::String("reading".to_owned()),
				crate::syn::value("{ city: 'London', time: d'2024-01-01T00:00:00Z', temp: 1 }")
					.unwrap(),
			],
		)
		.await
		.expect("the create runs");

		let (_, restated) = london(2);
		let merged = answer(
			&rpc,
			sid,
			Method::Merge,
			vec![id.clone(), crate::syn::value("{ city: 'London', temp: 2 }").unwrap()],
		)
		.await
		.expect("restating a component is allowed");
		assert_eq!(merged, restated);

		let refused = answer(
			&rpc,
			sid,
			Method::Merge,
			vec![id.clone(), crate::syn::value("{ city: 'Paris' }").unwrap()],
		)
		.await
		.expect_err("a component cannot be changed");
		assert_eq!(
			refused.to_string(),
			"Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"
		);

		let read = answer(&rpc, sid, Method::Select, vec![id]).await.expect("the select runs");
		assert_eq!(read, restated);
	}

	/// `patch` may restate a component of the record id and may not change it,
	/// whichever operation does: the refusal names the value written, and the
	/// record is as it was.
	#[tokio::test]
	async fn patch_rpc_cannot_change_a_component_of_the_record_id() {
		let (rpc, sid) = keyed_rpc().await;
		let (id, _) = london(1);
		answer(
			&rpc,
			sid,
			Method::Create,
			vec![
				PublicValue::String("reading".to_owned()),
				crate::syn::value("{ city: 'London', time: d'2024-01-01T00:00:00Z', temp: 1 }")
					.unwrap(),
			],
		)
		.await
		.expect("the create runs");

		let (_, patched) = london(3);
		let applied = answer(
			&rpc,
			sid,
			Method::Patch,
			vec![
				id.clone(),
				crate::syn::value(
					"[{ op: 'replace', path: '/city', value: 'London' }, { op: 'replace', path: '/temp', value: 3 }]",
				)
				.unwrap(),
			],
		)
		.await
		.expect("restating a component is allowed");
		assert_eq!(applied, patched);

		let refused = answer(
			&rpc,
			sid,
			Method::Patch,
			vec![
				id.clone(),
				crate::syn::value("[{ op: 'replace', path: '/city', value: 'Paris' }]").unwrap(),
			],
		)
		.await
		.expect_err("a component cannot be changed");
		assert_eq!(
			refused.to_string(),
			"Found 'Paris' for the `city` field, but it is part of the record id of `reading` and does not match it"
		);

		let read = answer(&rpc, sid, Method::Select, vec![id]).await.expect("the select runs");
		assert_eq!(read, patched);
	}

	/// Counts `on_rpc_complete` calls, reporting itself as a no-op shell or not.
	///
	/// A real observer never lies about [`ExecutionObserver::is_noop`] — only
	/// `NoopObserver` overrides it. This one does so it can observe the
	/// short-circuit from the outside: with the flag set, a counted event means
	/// the gate did not fire.
	struct RpcCounter {
		noop: bool,
		count: std::sync::atomic::AtomicUsize,
	}

	impl crate::observe::ExecutionObserver for RpcCounter {
		fn on_rpc_complete(&self, _event: &crate::observe::RpcEvent) {
			self.count.fetch_add(1, Ordering::SeqCst);
		}
		fn is_noop(&self) -> bool {
			self.noop
		}
	}

	async fn count_rpc_events(noop: bool) -> usize {
		let observer = Arc::new(RpcCounter {
			noop,
			count: std::sync::atomic::AtomicUsize::new(0),
		});
		let ds = Datastore::builder()
			.with_observer(Arc::clone(&observer) as Arc<dyn crate::observe::ExecutionObserver>)
			.build_with_path("memory")
			.await
			.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(31);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		observer.count.load(Ordering::SeqCst)
	}

	/// The gate must not silence a live observer.
	#[tokio::test]
	async fn an_observed_rpc_still_reports_its_event() {
		assert_eq!(count_rpc_events(false).await, 1, "a live observer received no RPC event");
	}

	/// And with nothing observing, the whole event path — a session lookup, a
	/// read-lock acquisition and the identity clones — is skipped.
	#[tokio::test]
	async fn an_unobserved_rpc_builds_no_event() {
		assert_eq!(count_rpc_events(true).await, 0, "the no-op short-circuit did not fire");
	}

	/// The `set`/`let` and `unset` methods refuse a protected name, and leave the session's
	/// variables as they were. A name the system binds in a frame is not refused: the frame's
	/// own binding shadows it, so a client may hold a variable under that name.
	#[tokio::test]
	async fn set_and_unset_refuse_a_protected_name() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(41);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		let key = |name: &str| -> PublicArray { vec![PublicValue::String(name.to_owned())].into() };
		let keyed = |name: &str| -> PublicArray {
			vec![
				PublicValue::String(name.to_owned()),
				PublicValue::Number(surrealdb_types::Number::Int(1)),
			]
			.into()
		};
		for name in ["auth", "access", "token", "session"] {
			for method in [Method::Set, Method::Unset] {
				let params = if matches!(method, Method::Set) {
					keyed(name)
				} else {
					key(name)
				};
				let err = rpc
					.execute(None, sid, Some(sid), method, params)
					.await
					.expect_err("a refused name is not set or unset");
				assert!(err.to_string().contains(name), "the refusal of `{name}` names it: {err}");
			}
			// Setting a name to NONE removes it, which is an unset.
			let params: PublicArray =
				vec![PublicValue::String(name.to_owned()), PublicValue::None].into();
			rpc.execute(None, sid, Some(sid), Method::Set, params)
				.await
				.expect_err("a refused name is not removed by a set to NONE");
		}
		// An ordinary name is set and unset, as is each name the system binds in a frame.
		for name in
			["limit", "this", "self", "parent", "value", "input", "before", "after", "event"]
		{
			rpc.execute(None, sid, Some(sid), Method::Set, keyed(name)).await.unwrap();
			rpc.execute(None, sid, Some(sid), Method::Unset, key(name)).await.unwrap();
		}
	}

	/// A query and a session mutation on the same session must both complete.
	///
	/// `Session` sits behind a task-fair `RwLock`, so a queued writer blocks
	/// later readers. Any method that acquires a read guard and then reaches a
	/// second acquisition of the same lock therefore deadlocks against a
	/// concurrent `set` / `use` / `signin`: the writer waits on the outer guard
	/// while the inner read waits behind the writer.
	///
	/// A transport applies one connection's RPCs concurrently, so this is
	/// reachable from a single client. The two tasks race, so a run that happens
	/// not to interleave passes vacuously — but once the re-lock is gone no
	/// interleaving can hang, so this cannot fail spuriously.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_query_and_a_concurrent_session_mutation_both_complete() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = Arc::new(DurableRpc::new(ds));
		let sid = Uuid::from_u128(21);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();

		const ROUNDS: usize = 200;
		let queries = {
			let rpc = Arc::clone(&rpc);
			tokio::spawn(async move {
				for i in 0..ROUNDS {
					let params: PublicArray =
						vec![PublicValue::String(format!("RETURN {i};"))].into();
					rpc.execute(None, sid, Some(sid), Method::Query, params).await.unwrap();
				}
			})
		};
		let mutations = {
			let rpc = Arc::clone(&rpc);
			tokio::spawn(async move {
				for i in 0..ROUNDS {
					let params: PublicArray = vec![
						PublicValue::String("k".to_owned()),
						PublicValue::Number(surrealdb_types::Number::Int(i as i64)),
					]
					.into();
					rpc.execute(None, sid, Some(sid), Method::Set, params).await.unwrap();
				}
			})
		};

		tokio::time::timeout(std::time::Duration::from_secs(30), async {
			queries.await.unwrap();
			mutations.await.unwrap();
		})
		.await
		.expect("a query deadlocked against a concurrent session mutation");
	}

	#[tokio::test]
	async fn mutations_persist_and_an_evicted_session_is_rehydrated() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(7);

		// An attached, authenticated session (as `signin` would leave it).
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));

		// A session-mutating method (USE) must write the session through to
		// durable storage.
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		assert!(rpc.durable.lock().unwrap().contains_key(&sid), "USE did not persist the session");

		// Simulate isolate eviction: the in-memory session map is gone.
		rpc.session_map().remove(&sid);
		assert!(!rpc.session_map().contains_key(&sid));

		// `get_session` rehydrates the evicted session from durable storage
		// and reinstalls it, keeping its auth — so every caller (the gate, the
		// handlers, the observer) resumes it transparently.
		let restored = rpc.get_session(&sid).await.expect("session was not rehydrated");
		assert!(restored.read().await.au.is_root(), "auth lost on rehydrate");
		assert!(rpc.session_map().contains_key(&sid), "rehydrated session was not reinstalled");

		// And a full request against the evicted id succeeds end to end.
		rpc.session_map().remove(&sid);
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
	}

	#[tokio::test]
	async fn attach_persists_so_a_bare_session_survives_eviction() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(13);

		// `attach` creates the session; the change-detection treats a
		// create as a change, so it is persisted (nothing pre-existed).
		rpc.execute(None, sid, Some(sid), Method::Attach, PublicArray::new()).await.unwrap();
		assert!(
			rpc.durable.lock().unwrap().contains_key(&sid),
			"attach did not persist the session"
		);

		// Evicted before any further call, the bare session still resumes.
		rpc.session_map().remove(&sid);
		assert!(rpc.get_session(&sid).await.is_ok(), "bare attached session lost on eviction");
	}

	#[tokio::test]
	async fn read_only_methods_do_not_persist() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(9);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));

		// `version` touches no session state, so it must not persist.
		rpc.execute(None, sid, Some(sid), Method::Version, PublicArray::new()).await.unwrap();
		assert!(rpc.durable.lock().unwrap().is_empty(), "a read-only method persisted a session");
	}

	#[tokio::test]
	async fn detach_deletes_the_durable_copy() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(11);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		assert!(rpc.durable.lock().unwrap().contains_key(&sid));

		// Detach must remove the durable copy, so the session cannot be
		// rehydrated (resurrected) afterwards.
		rpc.execute(None, sid, Some(sid), Method::Detach, PublicArray::new()).await.unwrap();
		assert!(
			rpc.durable.lock().unwrap().is_empty(),
			"detach left a resurrectable durable session"
		);

		// A later request for the same id finds nothing to rehydrate.
		let err = rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await;
		assert!(err.is_err(), "a detached session was still usable");
	}

	#[tokio::test]
	async fn detach_fails_and_keeps_the_session_when_the_durable_delete_fails() {
		let ds = Datastore::new("memory").await.unwrap();
		let rpc = DurableRpc::new(ds);
		let sid = Uuid::from_u128(15);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));
		rpc.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		assert!(rpc.durable.lock().unwrap().contains_key(&sid));

		// A failing durable delete must abort the detach with an error, and
		// leave the session fully intact (both the durable copy and the local
		// entry) so a client cannot believe a still-resurrectable session was
		// torn down.
		rpc.fail_forget.store(true, Ordering::SeqCst);
		let err = rpc.execute(None, sid, Some(sid), Method::Detach, PublicArray::new()).await;
		assert!(err.is_err(), "detach must fail when the durable delete fails");
		assert!(
			rpc.durable.lock().unwrap().contains_key(&sid),
			"durable copy must survive a failed detach"
		);
		assert!(rpc.session_map().contains_key(&sid), "local session must survive a failed detach");

		// Once the durable store recovers, the detach succeeds and tears down.
		rpc.fail_forget.store(false, Ordering::SeqCst);
		rpc.execute(None, sid, Some(sid), Method::Detach, PublicArray::new()).await.unwrap();
		assert!(rpc.durable.lock().unwrap().is_empty());
		assert!(!rpc.session_map().contains_key(&sid));
	}
}

#[cfg(test)]
mod read_only_option_tests {
	use std::time::Duration;

	use surrealdb_types::{Array, Object, Value};

	use super::*;

	fn options(entry: Object) -> Value {
		let mut options = Object::new();
		options.insert("read_only", Value::Object(entry));
		Value::Object(options)
	}

	fn entry(bound: Option<Value>, strict: Option<Value>) -> Object {
		let mut entry = Object::new();
		if let Some(bound) = bound {
			entry.insert("max_staleness", bound);
		}
		if let Some(strict) = strict {
			entry.insert("strict", strict);
		}
		entry
	}

	#[test]
	fn a_read_only_option_asks_for_a_bounded_read_block() {
		let bound = Value::Duration(surrealdb_types::Duration::from_secs(5));
		let read_only =
			extract_read_only(Some(options(entry(Some(bound.clone()), Some(Value::Bool(true))))))
				.unwrap()
				.unwrap();
		assert_eq!(read_only.max_staleness, Duration::from_secs(5));
		assert!(read_only.strict);

		let read_only =
			extract_read_only(Some(options(entry(Some(bound), None)))).unwrap().unwrap();
		assert!(!read_only.strict);
	}

	#[test]
	fn a_read_only_option_without_a_bound_reads_the_current_state() {
		let read_only = extract_read_only(Some(options(entry(None, None)))).unwrap().unwrap();
		assert_eq!(read_only.max_staleness, Duration::ZERO);
		assert!(!read_only.strict);
	}

	#[test]
	fn no_options_ask_for_nothing() {
		assert!(extract_read_only(None).unwrap().is_none());
		assert!(extract_read_only(Some(Value::None)).unwrap().is_none());
		assert!(extract_read_only(Some(Value::Object(Object::new()))).unwrap().is_none());
	}

	#[test]
	fn a_malformed_read_only_option_is_refused() {
		let bound = Value::Duration(surrealdb_types::Duration::from_secs(5));
		let mut misspelt = Object::new();
		misspelt.insert("readonly", Value::Object(entry(Some(bound.clone()), None)));
		for options in [
			Some(Value::Bool(true)),
			Some(options(entry(Some(Value::Bool(true)), None))),
			Some({
				let mut unknown = entry(Some(bound), None);
				unknown.insert("stict", true);
				options(unknown)
			}),
			Some(Value::Object(misspelt)),
		] {
			assert!(extract_read_only(options.clone()).is_err(), "{options:?}");
		}
	}

	#[test]
	fn begin_takes_read_only_as_a_flag_and_isolation_by_its_sql_name() {
		let options = |entries: Vec<(&str, Value)>| {
			let mut object = Object::new();
			for (key, value) in entries {
				object.insert(key, value);
			}
			Array::from(vec![Value::Object(object)])
		};
		let request =
			extract_begin_options(options(vec![("read_only", Value::Bool(true))])).unwrap();
		assert_eq!(
			request.read_only,
			Some(ReadOnlyOptions {
				max_staleness: std::time::Duration::ZERO,
				strict: false
			})
		);
		let request =
			extract_begin_options(options(vec![("read_only", Value::Bool(false))])).unwrap();
		assert!(request.read_only.is_none());
		let request = extract_begin_options(options(vec![(
			"isolation",
			Value::String("Repeatable Read".to_string()),
		)]))
		.unwrap();
		assert_eq!(request.isolation, Some(crate::kvs::Isolation::Snapshot));
	}

	#[test]
	fn begin_takes_an_isolation_level_and_a_query_does_not() {
		let isolation = |level: Value| {
			let mut options = Object::new();
			options.insert("isolation", level);
			Value::Object(options)
		};
		let request = extract_begin_options(Array::from(vec![isolation(Value::String(
			"Serializable".to_string(),
		))]))
		.unwrap();
		assert_eq!(request.isolation, Some(crate::kvs::Isolation::Serializable));
		assert!(request.read_only.is_none());
		let request = extract_begin_options(Array::from(vec![isolation(Value::String(
			"snapshot".to_string(),
		))]))
		.unwrap();
		assert_eq!(request.isolation, Some(crate::kvs::Isolation::Snapshot));

		for level in [Value::String("repeatable".to_string()), Value::Bool(true)] {
			assert!(extract_begin_options(Array::from(vec![isolation(level)])).is_err());
		}
		// A lone statement is always `SNAPSHOT`, so a query call refuses the option.
		assert!(
			extract_read_only(Some(isolation(Value::String("serializable".to_string())))).is_err()
		);
	}

	#[test]
	fn begin_takes_at_most_one_options_object() {
		assert_eq!(
			extract_begin_options(Array::new()).unwrap(),
			crate::kvs::BeginRequest::default()
		);
		assert!(
			extract_begin_options(Array::from(vec![Value::Object(Object::new()), Value::None]))
				.is_err()
		);
	}
}

#[cfg(test)]
mod trace_option_tests {
	use surrealdb_rpc::TraceKinds;
	use surrealdb_types::{Array, Object, Value};

	use super::*;

	fn trace(names: &[&str]) -> Option<Value> {
		let mut options = Object::new();
		let names = names.iter().map(|name| Value::String((*name).to_owned())).collect::<Vec<_>>();
		options.insert("trace", Value::Array(Array::from(names)));
		Some(Value::Object(options))
	}

	fn query() -> OptionsFor {
		OptionsFor::Query
	}

	#[test]
	fn a_query_takes_the_kinds_it_names() {
		let parsed = parse_options(trace(&["relations", "variables"]), query()).unwrap();
		assert!(parsed.trace.contains(TraceKinds::RELATIONS));
		assert!(parsed.trace.contains(TraceKinds::VARIABLES));
		assert!(!parsed.trace.contains(TraceKinds::LINKS));
		assert!(parse_options(trace(&[]), query()).unwrap().trace.is_empty());
	}

	/// A kind this server does not know is refused, so a client never mistakes
	/// a kind it is not given for an empty one.
	#[test]
	fn an_unknown_kind_is_refused() {
		assert!(parse_options(trace(&["records"]), query()).is_err());
		let mut options = Object::new();
		options.insert("trace", Value::String("relations".to_owned()));
		assert!(parse_options(Some(Value::Object(options)), query()).is_err());
	}

	/// A `begin` runs nothing, so it has nothing to trace.
	#[test]
	fn a_begin_takes_no_trace() {
		assert!(parse_options(trace(&["relations"]), OptionsFor::Begin).is_err());
	}
}
