use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::iam::{Auth, Level};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{
	DbResult, Method, RpcProtocol, method_not_allowed, method_not_found, session_exists,
	session_not_found,
};
use surrealdb_types::{Array, Error as TypesError, HashMap, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::cnf::{HTTP_MAX_ATTACHED_SESSIONS, PKG_NAME, PKG_VERSION};

/// HTTP RPC handler with per-request session isolation.
///
/// Sessions are inserted under unique per-request keys by `post_handler`
/// and removed after execution completes. No default session is stored.
///
/// # Security
///
/// Unlike WebSocket, the HTTP transport shares a single `Http` instance
/// across every `POST /rpc` request (there is no per-connection scope).
/// To prevent session hijack via UUID enumeration or guessing, this implementation:
///
/// - Rejects the `sessions` method outright - no enumeration is offered.
/// - Requires the caller's request-level auth principal to match the attached session's current
///   `Session.au` principal on every request that targets a client-supplied session id. See
///   [`Http::verify_caller_for_session`].
///
/// Ephemeral (per-request) session ids remain hidden and untargetable by
/// clients; they are never returned from any public method and cannot be
/// referenced from another request.
pub struct Http {
	kvs: Arc<Datastore>,
	sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
	/// Set of session IDs that were created implicitly by the transport for
	/// per-request isolation. These must be hidden from `sessions()` so
	/// clients cannot enumerate or target internal request UUIDs.
	ephemeral_sessions: HashMap<Uuid, ()>,
}

impl Http {
	pub fn new(kvs: Arc<Datastore>) -> Self {
		Self {
			kvs,
			sessions: HashMap::new(),
			ephemeral_sessions: HashMap::new(),
		}
	}

	/// Register a session created implicitly by the transport for a single
	/// request. Tracked so it can be filtered from `sessions()` results.
	///
	/// Insert into `ephemeral_sessions` BEFORE `sessions` so any concurrent
	/// `sessions()` call that snapshots the session map will also observe
	/// the ephemeral marker and filter the UUID out. Reversing this order
	/// opens a window where the internal per-request UUID can be returned
	/// to the client.
	///
	/// Crate-private: only the HTTP transport may mint ephemeral sessions.
	/// Exposing this publicly would let embedder crates inject hidden
	/// sessions into the shared session map.
	pub(crate) fn register_ephemeral_session(&self, id: Uuid, session: Arc<RwLock<Session>>) {
		self.ephemeral_sessions.insert(id, ());
		self.sessions.insert(id, session);
	}

	/// Remove a previously registered ephemeral session.
	///
	/// Guards against destroying a named (attached) session by verifying the
	/// UUID was in fact registered as ephemeral before touching `sessions`.
	/// A non-ephemeral UUID is a safe no-op. This upholds the contract that
	/// named sessions are only torn down via the trait-level `detach` /
	/// `del_session` path (which also performs live-query cleanup).
	///
	/// Remove from `sessions` only AFTER confirming the UUID was ephemeral,
	/// then clear the ephemeral marker so the UUID disappears from future
	/// `sessions()` snapshots before the marker is cleared.
	///
	/// Crate-private: only the HTTP transport may recycle ephemeral sessions.
	pub(crate) fn remove_ephemeral_session(&self, id: &Uuid) {
		if self.ephemeral_sessions.contains_key(id) {
			self.sessions.remove(id);
			self.ephemeral_sessions.remove(id);
		}
	}

	/// Count of currently attached (non-ephemeral) sessions.
	///
	/// Used to enforce [`HTTP_MAX_ATTACHED_SESSIONS`]. The two maps are
	/// independent and concurrent, so the computed size can briefly
	/// overshoot by the number of in-flight ephemerals; this is bounded by
	/// the request concurrency and therefore safe as a loose cap.
	fn attached_session_count(&self) -> usize {
		self.sessions.len().saturating_sub(self.ephemeral_sessions.len())
	}

	/// Verify that the caller of an HTTP `/rpc` request is authorised to use
	/// a client-supplied session id.
	///
	/// Rules:
	///
	/// - Ephemeral ids (from any concurrent request) are not user-targetable and return
	///   `session_not_found` to avoid confirming their existence.
	/// - If the stored session has not yet been authenticated (`Session.au` at `Level::No`), any
	///   caller may reach into it. This preserves the standard `attach` followed by
	///   `signin`/`authenticate` flow: `attach` creates an anonymous session and a subsequent
	///   authentication call promotes it to the caller's principal. An unauthenticated session
	///   carries no elevated privileges, so this is safe given that session UUIDs are 128-bit
	///   random and cannot be enumerated — see the [`RpcProtocol::sessions`] override.
	/// - Once the stored session is bound to a principal, the caller's request-level principal
	///   fingerprint (`actor id` + `Level`) must exactly match the session's current `Session.au`
	///   principal. Roles are intentionally excluded from the comparison so that legitimate re-auth
	///   with a different role set for the same identity still matches.
	///
	/// All negative outcomes return `session_not_found` so the response
	/// cannot be used as an oracle to distinguish "session does not exist"
	/// from "session exists but you are not its owner".
	pub(crate) async fn verify_caller_for_session(
		&self,
		session_id: &Uuid,
		caller_au: &Auth,
	) -> Result<(), TypesError> {
		// Reject any attempt to target an ephemeral (per-request) session.
		// Ephemeral ids are generated server-side and must never be usable
		// across requests.
		if self.ephemeral_sessions.contains_key(session_id) {
			return Err(session_not_found(*session_id));
		}
		// Fetch the stored session (returns session_not_found if absent).
		let session_lock = self.get_session(session_id)?;
		// Read the principal fingerprint under a short read lock.
		let session_guard = session_lock.read().await;
		let session_au = session_guard.au.as_ref();
		if caller_may_use_session(session_au, caller_au) {
			Ok(())
		} else {
			Err(session_not_found(*session_id))
		}
	}
}

/// Determine whether a caller presenting `caller_au` may operate on a
/// session whose currently-bound principal is `session_au`.
///
/// Semantics (see [`Http::verify_caller_for_session`] for rationale):
///
/// - An unauthenticated session (`Session.au` at [`Level::No`]) is open to any caller; an
///   authentication command issued by the caller will then bind the session to the caller's
///   principal for all subsequent requests.
/// - An authenticated session is only reachable by callers whose request-level [`Auth`] matches on
///   actor id and [`Level`]. Roles are intentionally excluded so role grants/revocations for the
///   same identity do not lock the legitimate owner out.
fn caller_may_use_session(session_au: &Auth, caller_au: &Auth) -> bool {
	match session_au.level() {
		Level::No => true,
		_ => session_au.id() == caller_au.id() && session_au.level() == caller_au.level(),
	}
}

impl RpcProtocol for Http {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	/// The version information for this RPC context
	fn version_data(&self) -> DbResult {
		let value = Value::String(format!("{PKG_NAME}-{}", *PKG_VERSION));
		DbResult::Other(value)
	}

	/// A pointer to all active sessions
	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	/// Session enumeration is not available on the HTTP transport.
	///
	/// The HTTP `sessions()` method returned every attached session UUID to any
	/// anonymous caller, enabling trivial discovery of privileged sessions
	/// for hijack. HTTP has no legitimate per-caller use for listing other
	/// clients' sessions, so the method is refused outright.
	async fn sessions(&self) -> Result<DbResult, TypesError> {
		Err(method_not_allowed(Method::Sessions.to_string()))
	}

	/// Registers a new session with the given ID, subject to the
	/// [`HTTP_MAX_ATTACHED_SESSIONS`] cap.
	///
	/// The cap prevents a single anonymous caller from exhausting server
	/// memory by attaching unbounded sessions against the shared HTTP
	/// session map. The cap is deliberately loose (see
	/// [`Http::attached_session_count`]); brief overshoot by the number of
	/// in-flight ephemerals is acceptable and bounded.
	async fn attach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		if self.session_map().contains_key(&session_id) {
			return Err(session_exists(session_id));
		}
		if self.attached_session_count() >= *HTTP_MAX_ATTACHED_SESSIONS {
			return Err(method_not_allowed(Method::Attach.to_string()));
		}
		let mut session = Session::default().with_rt(Self::LQ_SUPPORT);
		session.id = Some(session_id);
		self.session_map().insert(session_id, Arc::new(RwLock::new(session)));
		Ok(DbResult::Other(Value::None))
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled on HTTP
	const LQ_SUPPORT: bool = false;

	/// Handles the cleanup of live queries
	async fn cleanup_lqs(&self, _session_id: &Uuid) {
		// Do nothing as HTTP is stateless
	}

	/// Handles the cleanup of live queries
	async fn cleanup_all_lqs(&self) {
		// Do nothing as HTTP is stateless
	}

	// ------------------------------
	// Overrides
	// ------------------------------

	/// Transactions are not supported on HTTP RPC context
	async fn begin(&self, _txn: Option<Uuid>, _session_id: Uuid) -> Result<DbResult, TypesError> {
		Err(method_not_found(Method::Begin.to_string()))
	}

	/// Transactions are not supported on HTTP RPC context
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
		Err(method_not_found(Method::Commit.to_string()))
	}

	/// Transactions are not supported on HTTP RPC context
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
		Err(method_not_found(Method::Cancel.to_string()))
	}
}
