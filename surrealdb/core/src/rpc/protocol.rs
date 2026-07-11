use std::sync::Arc;

use anyhow::Result;
use surrealdb_types::{HashMap, object};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::catalog::providers::{CatalogProvider, NamespaceProvider, RootProvider};
use crate::ctx::CancelHandle;
use crate::dbs::capabilities::{ExperimentalTarget, MethodTarget};
use crate::dbs::{QueryResult, QueryType, Session};
use crate::iam::token::Token;
use crate::kvs::{Datastore, LockType, TransactionType};
use crate::observe::{
	AuthAction, AuthEvent, AuthEventSafe, AuthScope, Outcome, RpcEvent, RpcEventSafe,
	TenantIdentity,
};
use crate::rpc::args::extract_args;
use crate::rpc::{
	DbResult, Method, bad_lq_config, invalid_params, method_not_allowed, method_not_found,
	session_exists, session_expired, session_not_found, types_error_from_anyhow,
};
use crate::sql::statements::live::LiveFields;
use crate::sql::{
	Ast, CreateStatement, Data as SqlData, DeleteStatement, Expr, Fields, Function, FunctionCall,
	InsertStatement, KillStatement, Literal, LiveStatement, Model, Output, RelateStatement,
	SelectStatement, TopLevelExpr, UpdateStatement, UpsertStatement,
};
use crate::types::{
	PublicArray, PublicRecordIdKey, PublicUuid, PublicValue, PublicVariables, SurrealValue,
};

/// utility function converting a `Value::String` into a `Expr::Table`
fn value_to_table(value: PublicValue) -> Expr {
	match value {
		PublicValue::String(s) => Expr::Table(crate::val::TableName::new(s)),
		x => Expr::from_public_value(x),
	}
}

/// returns if the expression returns a singular value when selected.
///
/// As this rpc is some what convuluted the singular conditions is not the same
/// for all cases.
fn singular(value: &PublicValue) -> bool {
	match value {
		PublicValue::Object(_) => true,
		PublicValue::RecordId(t) => !matches!(t.key, PublicRecordIdKey::Range(_)),
		_ => false,
	}
}

// SECURITY: LIVE queries capture the session's auth principal at registration
// time (see `surrealdb/core/src/dbs/session.rs`). When an auth-lifecycle RPC
// changes the principal on the same WebSocket, those captured snapshots would
// continue to dispatch notifications under the prior context, bypassing the
// access controls that should now apply (GHSA-2xrp-m9c6-75rj). Callers
// snapshot the principal before the operation and tear LIVE subscriptions
// down when it changes. Token refresh against the same identity leaves the
// principal unchanged and preserves the subscriptions.
struct AuthPrincipalSnapshot {
	id: String,
	level: crate::iam::Level,
}

impl AuthPrincipalSnapshot {
	fn capture(session: &Session) -> Self {
		Self {
			id: session.au.id().to_string(),
			level: session.au.level().clone(),
		}
	}

	fn differs_from(&self, session: &Session) -> bool {
		session.au.id() != self.id || session.au.level() != &self.level
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
		_ => None,
	}
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
		self.del_session(&session_id).await;
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

	/// Deletes a session
	async fn del_session(&self, id: &Uuid) {
		self.session_map().remove(id);
		// Remove the durable copy too, so `detach`/logout is not silently
		// undone by a later rehydrate. Const-gated to nothing for
		// in-memory-only implementations.
		if Self::PERSIST_SESSIONS {
			self.forget_session(id).await;
		}
		self.cleanup_lqs(id).await;
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
	/// a later [`load_session`](Self::load_session). The default is a no-op,
	/// and it is only ever called when
	/// [`PERSIST_SESSIONS`](Self::PERSIST_SESSIONS) is set.
	async fn forget_session(&self, _id: &Uuid) {}

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
	fn handle_live(
		&self,
		_lqid: &Uuid,
		_session_id: Uuid,
		_namespace: Option<String>,
		_database: Option<String>,
	) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_live function must be implemented if LQ_SUPPORT = true") }
	}
	/// Handles the execution of a KILL statement
	fn handle_kill(&self, _lqid: &Uuid) -> impl std::future::Future<Output = ()> + Send {
		async { unimplemented!("handle_kill function must be implemented if LQ_SUPPORT = true") }
	}

	/// Handles the cleanup of live queries
	fn cleanup_lqs(&self, session_id: &Uuid) -> impl std::future::Future<Output = ()> + Send;

	/// Handles the cleanup of all live queries
	fn cleanup_all_lqs(&self) -> impl std::future::Future<Output = ()> + Send;

	// ------------------------------
	// Method execution
	// ------------------------------

	/// Executes a method on this RPC implementation.
	///
	/// `session` is the resolved session ID (always valid). `client_session`
	/// is the raw value from the client request (`None` when the client did
	/// not specify one). Session-management methods (`attach`, `detach`) use
	/// `client_session` to reject calls with no explicit session ID.
	///
	/// Wraps the dispatch in a timer and fires an
	/// [`crate::observe::RpcEvent`] on completion regardless of outcome. The
	/// safe half of the event carries only the bounded [`Method`] and the
	/// outcome label; the context half carries the current session's
	/// namespace/database/user (ignored by the community metrics observer
	/// but consumed by enterprise audit sinks). Auth-related methods
	/// additionally fan out an [`AuthEvent`] so auth attempt counters can
	/// be broken out by action/scope/outcome.
	#[tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.execute",
		skip_all,
		fields(rpc.method = method.to_str(), rpc.session = %session)
	)]
	async fn execute(
		&self,
		txn: Option<Uuid>,
		session: Uuid,
		client_session: Option<Uuid>,
		method: Method,
		params: PublicArray,
	) -> Result<DbResult, surrealdb_types::Error> {
		let start = web_time::Instant::now();
		let result: Result<DbResult, surrealdb_types::Error> = async {
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
			// Execute the desired method
			let dispatched = match method {
				Method::Ping => Ok(DbResult::Other(PublicValue::None)),
				Method::Info => self.info(txn, session).await,
				Method::Use => self.yuse(session, params).await,
				Method::Signup => self.signup(session, params).await,
				Method::Signin => self.signin(session, params).await,
				Method::Authenticate => self.authenticate(session, params).await,
				Method::Refresh => self.refresh(session, params).await,
				Method::Invalidate => self.invalidate(session).await,
				Method::Revoke => self.revoke(params).await,
				Method::Reset => self.reset(session).await,
				Method::Kill => self.kill(txn, session, params).await,
				Method::Live => self.live(txn, session, params).await,
				Method::Set => self.set(session, params).await,
				Method::Unset => self.unset(session, params).await,
				Method::Query => self.query(txn, session, params).await,
				Method::Gql => self.gql(txn, session, params).await,
				Method::Graphql => self.graphql(txn, session, params).await,
				Method::Version => self.version(txn, params).await,
				Method::Begin => self.begin(txn, session).await,
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
		}
		.await;
		let outcome = Outcome::from(&result);
		// Resolve session context for the observer event. An unknown
		// session ID yields an empty `TenantIdentity` rather than an
		// error, keeping the metrics path infallible. We capture the
		// scope inside the same lock acquisition to avoid a second
		// lookup for auth events. Routing through
		// `TenantIdentity::from_session` applies the documented
		// record/anonymous collapsing rule (anon -> `None`,
		// record-access -> `<record>` sentinel) so per-tenant /
		// dimensional dashboards stay bounded and audit destinations
		// never receive raw record-access principal ids.
		let (identity, scope) = match self.get_session(&session).await {
			Ok(session_lock) => {
				let s = session_lock.read().await;
				let scope = AuthScope::from(s.au.level());
				(TenantIdentity::from_session(&s), scope)
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
		let observer = self.kvs().observer();
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
		result
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
		// Empty USE call: apply defaults only if session doesn't already have ns/db
		if ns.is_none() && db.is_none() {
			// Skip applying defaults if ns is already set (e.g., from token authentication)
			if session.ns.is_none() {
				// Fetch defaults from database configuration
				let kvs = self.kvs();
				let tx = kvs
					.transaction(TransactionType::Write, LockType::Optimistic)
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
			match ns {
				PublicValue::None => (),
				PublicValue::Null => session.ns = None,
				PublicValue::String(ns) => {
					let kvs = self.kvs();
					let tx = kvs
						.transaction(TransactionType::Write, LockType::Optimistic)
						.await
						.map_err(types_error_from_anyhow)?;
					let create = kvs
						.should_materialize_ns_on_use(&tx, session.au.as_ref(), &ns)
						.await
						.map_err(types_error_from_anyhow)?;
					if create {
						run!(tx, tx.get_or_add_ns(None, &ns).await)
							.map_err(types_error_from_anyhow)?;
					} else {
						let _ = tx.cancel().await;
					}
					session.ns = Some(ns)
				}
				unexpected => {
					return Err(invalid_params(format!(
						"Expected ns to be string, got {unexpected:?}"
					)));
				}
			}
			// Update the selected database
			match db {
				PublicValue::None => (),
				PublicValue::Null => session.db = None,
				PublicValue::String(db) => {
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
						.transaction(TransactionType::Write, LockType::Optimistic)
						.await
						.map_err(types_error_from_anyhow)?;
					let create = kvs
						.should_materialize_db_on_use(&tx, session.au.as_ref(), &ns, &db)
						.await
						.map_err(types_error_from_anyhow)?;
					if create {
						run!(tx, tx.ensure_ns_db(None, &ns, &db).await)
							.map_err(types_error_from_anyhow)?;
					} else {
						let _ = tx.cancel().await;
					}
					session.db = Some(db)
				}
				unexpected => {
					return Err(invalid_params(format!(
						"Expected db to be string, got {unexpected:?}"
					)));
				}
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

	#[tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.signup",
		skip_all,
		fields(rpc.session = %session_id)
	)]
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

	#[tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.signin",
		skip_all,
		fields(rpc.session = %session_id)
	)]
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

	#[tracing::instrument(
		level = "debug",
		target = "surrealdb::core::rpc",
		name = "rpc.authenticate",
		skip_all,
		fields(rpc.session = %session_id)
	)]
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
		let out: Result<PublicValue> =
			token.refresh(self.kvs(), &mut session).await.map(Token::into_value);
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
		token.revoke_refresh_token(self.kvs()).await.map_err(types_error_from_anyhow)?;
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

		match val {
			None | Some(PublicValue::None) => session.variables.remove(key.as_str()),
			Some(val) => {
				crate::rpc::check_protected_param(&key)?;
				session.variables.insert(key, val)
			}
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

		// Specify the SQL query string
		let ast = Ast {
			expressions: vec![TopLevelExpr::Kill(KillStatement {
				id: Expr::from_public_value(id),
			})],
		};
		// Specify the query parameters
		let vars = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), vars)
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
		let what = match what {
			PublicValue::String(x) => Expr::Table(crate::val::TableName::new(x)),
			x => Expr::from_public_value(x),
		};

		let fields = if diff.unwrap_or_default().is_true() {
			LiveFields::Diff
		} else {
			LiveFields::Select(Fields::all())
		};

		// Specify the SQL query string
		let sql = LiveStatement {
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

		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), vars)
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
		let only = match what {
			PublicValue::RecordId(ref x) => !x.key.is_range(),
			_ => false,
		};

		// If value is a string, handle it as if it was a table.
		let what = match what {
			PublicValue::String(x) => Expr::Table(crate::val::TableName::new(x)),
			x => Expr::from_public_value(x),
		};

		// Specify the SQL query string
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
		};
		let ast = Ast::single_expr(Expr::Select(Box::new(sql)));

		// Specify the query parameters
		let vars = Some(session.variables.clone());
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), vars)
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

		let into = match what {
			PublicValue::Null | PublicValue::None => None,
			PublicValue::Table(x) => Some(Expr::Table(crate::val::TableName::new(x.into_string()))),
			PublicValue::String(x) => Some(Expr::Table(crate::val::TableName::new(x))),
			x => Some(Expr::from_public_value(x)),
		};

		// Specify the SQL query string
		let sql = InsertStatement {
			into,
			data: SqlData::SingleExpression(Expr::from_public_value(data)),
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var)
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

		let table_name = match what {
			PublicValue::Null | PublicValue::None => None,
			PublicValue::Table(x) => Some(Expr::Table(crate::val::TableName::new(x.into_string()))),
			PublicValue::String(x) => Some(Expr::Table(crate::val::TableName::new(x))),
			x => Some(Expr::from_public_value(x)),
		};

		let data = SqlData::SingleExpression(Expr::from_public_value(data));

		// Specify the SQL query string
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var)
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

		let only = match what {
			PublicValue::String(_) => true,
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));

		// Specify the SQL query string
		let sql = CreateStatement {
			only,
			what: vec![value_to_table(what)],
			data,
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::None),
		};
		let ast = Ast::single_expr(Expr::Create(Box::new(sql)));
		// Execute the query on the database
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), None)
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

		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));

		// Specify the SQL query string
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var)
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

		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));
		// Specify the SQL query string
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

		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::MergeExpression(Expr::from_public_value(x)));
		// Specify the SQL query string
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var)
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
		let only = match what {
			PublicValue::RecordId(ref x) => !matches!(x.key, PublicRecordIdKey::Range(_)),
			_ => false,
		};

		let data = data
			.and_then(|x| {
				if x.is_nullish() {
					None
				} else {
					Some(x)
				}
			})
			.map(|x| SqlData::PatchExpression(Expr::from_public_value(x)));

		let diff = matches!(diff, Some(PublicValue::Bool(true)));

		// Specify the SQL query string
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
			.map(|x| SqlData::ContentExpression(Expr::from_public_value(x)));

		// Specify the SQL query string
		let expr = Expr::Relate(Box::new(RelateStatement {
			only,
			or_update: false,
			from: Expr::from_public_value(from),
			through: value_to_table(kind),
			to: Expr::from_public_value(with),
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
		// Specify the SQL query string
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
		let mut res = run_query(self, txn, session_id, QueryForm::Parsed(ast), var)
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
		let (query, vars) =
			extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
				.ok_or(invalid_params("Expected (query:string, vars:object)".to_string()))?;

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
				return Err(invalid_params(format!(
					"Expected vars to be object, got {unexpected:?}"
				)));
			}
		};

		Ok(DbResult::Query(
			run_query(self, txn, session_id, QueryForm::Text(&query), vars)
				.await
				.map_err(types_error_from_anyhow)?,
		))
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
			let session_lock = self.get_session(&session_id).await?;
			let session = session_lock.read().await;
			// Check if the user is allowed to query
			if !self.kvs().allows_query_by_subject(session.au.as_ref()) {
				return Err(method_not_allowed(Method::Gql.to_string()));
			}
			// Process the method arguments
			let (query, vars) =
				extract_args::<(PublicValue, Option<PublicValue>)>(params.into_vec())
					.ok_or(invalid_params("Expected (query:string, vars:object)".to_string()))?;

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
				None | Some(PublicValue::None | PublicValue::Null) => {
					Some(session.variables.clone())
				}
				unexpected => {
					return Err(invalid_params(format!(
						"Expected vars to be object, got {unexpected:?}"
					)));
				}
			};

			// Parse and lower the GQL query into a prepared plan
			let plan = self.kvs().parse_gql(&query)?;

			Ok(DbResult::Query(
				run_query(self, txn, session_id, QueryForm::Plan(plan), vars)
					.await
					.map_err(types_error_from_anyhow)?,
			))
		}
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
		let name = match name {
			PublicValue::String(v) => v,
			unexpected => {
				return Err(invalid_params(format!(
					"Expected name to be string, got {unexpected:?}"
				)));
			}
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
			Some(&"mod") => {
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
					return Err(invalid_params("Expected module name".to_string()));
				};

				let sub = if segments.len() > 2 {
					Some(segments[2..].join("::"))
				} else {
					None
				};

				Function::Module(name, sub)
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
		let mut res = run_query(self, None, session_id, QueryForm::Parsed(ast), var)
			.await
			.map_err(types_error_from_anyhow)?;
		// Extract the first query result
		let first = res.remove(0).result?;
		Ok(DbResult::Other(first))
	}

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
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

enum QueryForm<'a> {
	Text(&'a str),
	Parsed(Ast),
	/// A pre-lowered GQL query. Only constructed by the `gql` handler, which
	/// is itself gated behind the `gql` feature.
	#[cfg(feature = "gql")]
	Plan(crate::gql::PreparedGqlQuery),
}

async fn run_query<T>(
	this: &T,
	txn: Option<Uuid>,
	session_id: Uuid,
	query: QueryForm<'_>,
	vars: Option<PublicVariables>,
) -> Result<Vec<QueryResult>>
where
	T: RpcProtocol + ?Sized,
{
	let session_lock = this.get_session(&session_id).await.map_err(anyhow::Error::from)?;
	let session = session_lock.read().await;
	if !T::LQ_SUPPORT && session.rt {
		return Err(bad_lq_config().into());
	}

	// If a transaction UUID is provided, retrieve it and execute with it
	let cancel = this.cancel_handle();
	let res = if let Some(txn_id) = txn {
		// Retrieve the transaction - fail if not found
		let tx = this.get_tx(txn_id).await.map_err(anyhow::Error::from)?;
		// Execute with the existing transaction by passing it through context
		match (query, cancel) {
			(QueryForm::Text(query), Some(cancel)) => {
				this.kvs()
					.execute_with_transaction_and_cancel(query, &session, vars, tx, cancel)
					.await?
			}
			(QueryForm::Text(query), None) => {
				this.kvs().execute_with_transaction(query, &session, vars, tx).await?
			}
			(QueryForm::Parsed(ast), Some(cancel)) => {
				this.kvs()
					.process_with_transaction_and_cancel(ast, &session, vars, tx, cancel)
					.await?
			}
			(QueryForm::Parsed(ast), None) => {
				this.kvs().process_with_transaction(ast, &session, vars, tx).await?
			}
			#[cfg(feature = "gql")]
			(QueryForm::Plan(plan), Some(cancel)) => {
				this.kvs()
					.process_gql_with_transaction_and_cancel(plan, &session, vars, tx, cancel)
					.await?
			}
			#[cfg(feature = "gql")]
			(QueryForm::Plan(plan), None) => {
				this.kvs().process_gql_with_transaction(plan, &session, vars, tx).await?
			}
		}
	} else {
		// No transaction - execute normally
		match (query, cancel) {
			(QueryForm::Text(query), Some(cancel)) => {
				this.kvs().execute_with_cancel(query, &session, vars, cancel).await?
			}
			(QueryForm::Text(query), None) => this.kvs().execute(query, &session, vars).await?,
			(QueryForm::Parsed(ast), Some(cancel)) => {
				this.kvs().process_with_cancel(ast, &session, vars, cancel).await?
			}
			(QueryForm::Parsed(ast), None) => this.kvs().process(ast, &session, vars).await?,
			#[cfg(feature = "gql")]
			(QueryForm::Plan(plan), Some(cancel)) => {
				this.kvs().process_gql_with_cancel(plan, &session, vars, cancel).await?
			}
			#[cfg(feature = "gql")]
			(QueryForm::Plan(plan), None) => this.kvs().process_gql(plan, &session, vars).await?,
		}
	};

	// Post-process hooks for web layer.
	//
	// `handle_live` needs the registering session's namespace / database
	// for the `surrealdb.live_query.active` gauge labelling. We snapshot
	// those off the read guard we already hold here rather than letting
	// `handle_live` re-lock the same `RwLock<Session>`: that would be a
	// recursive read on a write-preferring lock, and any concurrent
	// session-mutating RPC on the same WebSocket would queue a writer
	// between the two reads and deadlock both futures.
	let live_namespace = session.ns.clone();
	let live_database = session.db.clone();
	for response in &res {
		match &response.query_type {
			QueryType::Live => {
				if let Ok(PublicValue::Uuid(lqid)) = &response.result {
					this.handle_live(
						lqid,
						session_id,
						live_namespace.clone(),
						live_database.clone(),
					)
					.await;
				}
			}
			QueryType::Kill => {
				if let Ok(PublicValue::Uuid(lqid)) = &response.result {
					this.handle_kill(lqid).await;
				}
			}
			_ => {}
		}
	}
	// Return the result to the client
	Ok(res)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap as StdHashMap;
	use std::sync::{Arc, Mutex};

	use surrealdb_types::HashMap;
	use tokio::sync::RwLock;
	use uuid::Uuid;

	use super::{DbResult, Method, RpcProtocol};
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
		async fn forget_session(&self, id: &Uuid) {
			self.durable.lock().unwrap().remove(id);
		}
	}

	fn use_params(ns: &str, db: &str) -> PublicArray {
		vec![PublicValue::String(ns.to_owned()), PublicValue::String(db.to_owned())].into()
	}

	#[tokio::test]
	async fn mutations_persist_and_an_evicted_session_is_rehydrated() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let rpc = DurableRpc {
			ds,
			sessions: HashMap::new(),
			durable: Mutex::new(StdHashMap::new()),
		};
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
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let rpc = DurableRpc {
			ds,
			sessions: HashMap::new(),
			durable: Mutex::new(StdHashMap::new()),
		};
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
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let rpc = DurableRpc {
			ds,
			sessions: HashMap::new(),
			durable: Mutex::new(StdHashMap::new()),
		};
		let sid = Uuid::from_u128(9);
		rpc.set_session(sid, Arc::new(RwLock::new(Session::owner())));

		// `version` touches no session state, so it must not persist.
		rpc.execute(None, sid, Some(sid), Method::Version, PublicArray::new()).await.unwrap();
		assert!(rpc.durable.lock().unwrap().is_empty(), "a read-only method persisted a session");
	}

	#[tokio::test]
	async fn detach_deletes_the_durable_copy() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let rpc = DurableRpc {
			ds,
			sessions: HashMap::new(),
			durable: Mutex::new(StdHashMap::new()),
		};
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
}
