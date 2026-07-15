use std::collections::HashMap as StdHashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::iam::{Auth, Level};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{
	DbResult, Method, RpcProtocol, method_not_allowed, method_not_found, session_exists,
	session_not_found, types_error_from_anyhow,
};
use surrealdb_types::{Array, Error as TypesError, HashMap, Value};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, RwLock};
use uuid::Uuid;
use web_time::{SystemTime, UNIX_EPOCH};

use crate::cnf::{HTTP_MAX_ATTACHED_SESSIONS, PKG_NAME, PKG_VERSION};

/// Milliseconds since the UNIX epoch, the clock the durable session store
/// stamps expiries with.
///
/// Matches the datastore's `SystemClock::now()`: a backwards system clock is a
/// fault and panics rather than silently yielding `0` — a `0` would make every
/// cached deadline look un-expired (`deadline <= 0` is false), stopping TTL
/// enforcement and cross-node revalidation, and would diverge from the
/// `expires_at` the datastore stamps with the same (panicking) clock.
fn now_ms() -> u64 {
	match SystemTime::now().duration_since(UNIX_EPOCH) {
		Ok(duration) => duration.as_millis() as u64,
		Err(error) => panic!("Clock may have gone backwards: {:?}", error.duration()),
	}
}

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
	/// `Some(ttl)` when client-attached sessions are mirrored to the
	/// datastore (`--durable-sessions`), so they survive server restarts and
	/// are shared across cluster nodes. `None` keeps sessions in-memory only
	/// and every durability path compiles down to an early return.
	///
	/// The durable copy is the authoritative cross-node state. Dispatch is
	/// serialized per session id on this node (see [`SessionLocks`]), and before
	/// every request [`revalidate_cached_session`](Http::revalidate_cached_session)
	/// reconciles this node's cached copy against the durable one, so a mutation,
	/// invalidation, or detach performed on another node takes effect here on
	/// the next request rather than being served from a stale cache.
	///
	/// The feature targets deployments that route a given session to one node at
	/// a time (sticky routing / one runtime per session, the model the
	/// `RpcProtocol` durable-session seam is designed for). Durable writes are
	/// compare-guarded so they never resurrect or clobber a session across nodes;
	/// but *concurrently mutating the same session id from two nodes* is
	/// best-effort — under that race a losing mutation may be dropped rather than
	/// merged, so it is not a supported way to share a live session.
	durable_session_ttl: Option<Duration>,
	/// Per-session dispatch serialization, required by
	/// [`RpcProtocol::PERSIST_SESSIONS`]: the post-dispatch persist happens
	/// after the handler releases the session write lock, so two concurrent
	/// requests for the same session id could otherwise persist out of order.
	/// Only consulted when durable sessions are enabled.
	session_locks: SessionLocks,
	/// The expiry (ms since the UNIX epoch) last written to the datastore per
	/// session — the write-throttle for [`Http::touch_durable_session`].
	/// Entries are removed in [`RpcProtocol::forget_session`].
	durable_deadlines: HashMap<Uuid, u64>,
}

impl Http {
	pub fn new(kvs: Arc<Datastore>) -> Self {
		Self::new_with_durability(kvs, None)
	}

	/// Create an HTTP RPC handler, optionally persisting client-attached
	/// sessions to the datastore with the given idle TTL.
	pub fn new_with_durability(kvs: Arc<Datastore>, durable_session_ttl: Option<Duration>) -> Self {
		Self {
			kvs,
			sessions: HashMap::new(),
			ephemeral_sessions: HashMap::new(),
			durable_session_ttl,
			session_locks: SessionLocks::default(),
			durable_deadlines: HashMap::new(),
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

	/// Drop cached sessions whose durable copy is known to have expired (by
	/// this node's last-recorded deadline), reclaiming space against the
	/// [`HTTP_MAX_ATTACHED_SESSIONS`] cap.
	///
	/// An abandoned attached session is otherwise only removed when its id is
	/// requested again (`revalidate_cached_session`) or detached, so without
	/// this a long-lived node could fill the cap with entries the durable
	/// TTL/GC has already expired and then refuse new sessions. Only entries
	/// whose local deadline has passed are dropped; a session refreshed on
	/// another node (making this node's deadline stale) is at worst
	/// re-rehydrated on its next request. Called only when the cap is hit, so
	/// the scan cost is off the common path.
	fn prune_expired_cached_sessions(&self) {
		let now = now_ms();
		for (id, deadline) in self.durable_deadlines.to_vec() {
			if deadline <= now {
				self.session_map().remove(&id);
				self.durable_deadlines.remove(&id);
			}
		}
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
		let session_lock = self.get_session(session_id).await?;
		// Read the principal fingerprint under a short read lock.
		let session_guard = session_lock.read().await;
		let session_au = session_guard.au.as_ref();
		if caller_may_use_session(session_au, caller_au) {
			Ok(())
		} else {
			Err(session_not_found(*session_id))
		}
	}

	/// The per-session dispatch locks, taken by the HTTP transport around
	/// requests that target a client-named session while durable sessions are
	/// enabled.
	pub(crate) fn session_locks(&self) -> &SessionLocks {
		&self.session_locks
	}

	/// Reconcile a *cached* durable session against the authoritative datastore
	/// copy **before** the ownership gate and dispatch run.
	///
	/// `get_session` serves a cached session without consulting storage, so
	/// without this the first request routed to a node after another node
	/// detached the session or changed its auth (`invalidate`, `USE`, ...)
	/// would execute against the stale cached state and only reconcile
	/// afterwards. This reloads the stored session (the cross-node source of
	/// truth) and:
	///
	/// - if it is gone or expired, drops the stale local copy, so the gate then returns
	///   `session_not_found`;
	/// - otherwise overwrites the cached [`Session`] with the stored value, so the gate and
	///   dispatch see the current auth/`USE` state.
	///
	/// Only cached sessions need this — a cache miss rehydrates fresh through
	/// `get_session`/[`load_session`](RpcProtocol::load_session), which enforces
	/// the attached-session cap. It is a pure read (the TTL is slid only after
	/// authorization, in [`touch_durable_session`](Http::touch_durable_session)),
	/// so it is safe before the caller is authorized. The caller must hold the
	/// session's dispatch lock.
	pub(crate) async fn revalidate_cached_session(&self, id: &Uuid) {
		if self.durable_session_ttl.is_none() {
			return;
		}
		if self.ephemeral_sessions.contains_key(id) {
			return;
		}
		// Only a session already cached here can be stale.
		let Some(lock) = self.sessions.get(id) else {
			return;
		};
		match self.kvs.load_rpc_session(*id).await {
			Ok(Some((stored, expires_at))) => {
				// Adopt the authoritative stored value. Held under the dispatch
				// lock, so no concurrent request on this node races the write.
				*lock.write().await = stored;
				self.durable_deadlines.insert(*id, expires_at);
			}
			Ok(None) => {
				// Revoked (detached/invalidated) on another node, or the idle
				// TTL elapsed — drop the stale local copy so the gate refuses
				// the request instead of serving it from cache.
				self.session_map().remove(id);
				self.durable_deadlines.remove(id);
			}
			Err(err) => {
				// Best effort: on a storage error keep serving the cached copy
				// rather than locking the session out.
				warn!("Failed to revalidate durable RPC session {id}: {err}");
			}
		}
	}

	/// Slide the durable copy's idle TTL after a successful request, when under
	/// half the TTL remains (at most ~2 writes per TTL window per active
	/// session).
	///
	/// This writes the node's **current** cached session (never a reload), so
	/// it can neither clobber a just-applied local mutation nor resurrect a
	/// session deleted elsewhere: it is update-if-present, and a `false` return
	/// means the entry was revoked on another node, so the stale local copy is
	/// dropped. Cross-node *value* reconciliation happens before dispatch in
	/// [`revalidate_cached_session`](Http::revalidate_cached_session); this only
	/// extends the expiry. The caller must hold the session's dispatch lock.
	pub(crate) async fn touch_durable_session(&self, id: &Uuid) {
		let Some(ttl) = self.durable_session_ttl else {
			return;
		};
		if self.ephemeral_sessions.contains_key(id) {
			return;
		}
		// Refresh only when under half the TTL; a missing deadline means this
		// node has not written the session yet (e.g. it rehydrated it), so
		// refresh conservatively.
		let refresh = match self.durable_deadlines.get(id) {
			Some(deadline) => deadline.saturating_sub(now_ms()) < ttl.as_millis() as u64 / 2,
			None => true,
		};
		if !refresh {
			return;
		}
		// Write the current in-memory session with a fresh expiry. A session no
		// longer cached (e.g. just detached) is left alone — never resurrected.
		let Some(lock) = self.sessions.get(id) else {
			return;
		};
		let session = lock.read().await.clone();
		match self.kvs.update_rpc_session(*id, &session, ttl).await {
			Ok(true) => {
				self.durable_deadlines.insert(*id, now_ms() + ttl.as_millis() as u64);
			}
			Ok(false) => {
				self.session_map().remove(id);
				self.durable_deadlines.remove(id);
			}
			Err(err) => {
				error!("Failed to refresh durable RPC session {id}: {err}");
			}
		}
	}
}

/// Per-session-id dispatch serialization for the HTTP transport.
///
/// [`RpcProtocol::PERSIST_SESSIONS`] requires one in-flight RPC per session
/// id: the post-dispatch persist runs after the handler releases the session
/// write lock, so two concurrent mutations of the same session could persist
/// out of order and drop the losing one (e.g. an `invalidate` racing a
/// `signin`). HTTP requests run concurrently against the shared [`Http`]
/// instance, so the transport takes this keyed lock around the ownership gate
/// and dispatch whenever a request targets a client-named session.
///
/// Entries are removed once no request holds or awaits an id's slot, so ids
/// probed by unauthenticated attackers do not accumulate — including when a
/// waiting request is canceled before it acquires the lock (see
/// [`acquire`](Self::acquire)). A keyed map is used rather than a fixed shard
/// pool because a shard collision would serialize an unrelated session behind
/// a potentially long-running query.
type SessionLockMap = StdMutex<StdHashMap<Uuid, Arc<AsyncMutex<()>>>>;

#[derive(Default)]
pub(crate) struct SessionLocks {
	locks: SessionLockMap,
}

impl SessionLocks {
	/// Wait until this session id has no other in-flight request, then hold
	/// its slot until the returned guard drops.
	///
	/// The slot's `Arc` is moved into the lock future, so if this future is
	/// canceled while waiting (the client disconnected, the request timed
	/// out), the moved `Arc` drops before `pending`'s [`Drop`] runs — leaving
	/// only the map's reference — and `pending` prunes the now-idle entry.
	/// Without that a canceled waiter, which never builds a
	/// [`SessionDispatchGuard`], could orphan its map entry forever.
	pub(crate) async fn acquire(&self, id: Uuid) -> SessionDispatchGuard<'_> {
		let mutex = {
			let mut locks = self.locks.lock().expect("session lock map poisoned");
			Arc::clone(locks.entry(id).or_default())
		};
		// Prunes the slot if the acquisition below is abandoned mid-wait.
		let mut pending = PendingAcquire {
			locks: &self.locks,
			id,
			armed: true,
		};
		// Await outside the map lock so a held session does not block
		// acquisition for other session ids. `mutex` is moved in, so on the
		// cancel path the lock future drops its reference before `pending`
		// runs, leaving only the map's — which `pending` then prunes.
		let permit = mutex.lock_owned().await;
		pending.armed = false;
		SessionDispatchGuard {
			locks: &self.locks,
			id,
			permit: Some(permit),
		}
	}

	#[cfg(test)]
	fn len(&self) -> usize {
		self.locks.lock().expect("session lock map poisoned").len()
	}
}

/// Remove `id`'s slot if it has gone idle — i.e. only the map itself still
/// references it (`strong_count == 1`), meaning no request holds or awaits it.
/// The map lock serializes this against a concurrent `acquire` cloning the
/// entry, so a slot in use is never removed.
fn prune_idle_lock(locks: &SessionLockMap, id: &Uuid) {
	let Ok(mut map) = locks.lock() else {
		return;
	};
	if let Some(mutex) = map.get(id)
		&& Arc::strong_count(mutex) == 1
	{
		map.remove(id);
	}
}

/// Cleanup for the window between reserving a slot and acquiring its lock. If
/// the acquiring future is canceled while waiting, this prunes the idle slot;
/// on success it is disarmed and the [`SessionDispatchGuard`] takes over.
struct PendingAcquire<'a> {
	locks: &'a SessionLockMap,
	id: Uuid,
	armed: bool,
}

impl Drop for PendingAcquire<'_> {
	fn drop(&mut self) {
		if self.armed {
			prune_idle_lock(self.locks, &self.id);
		}
	}
}

/// Holder of a session's dispatch slot. Dropping it releases the slot and
/// removes the map entry when no other request holds or awaits it.
pub(crate) struct SessionDispatchGuard<'a> {
	locks: &'a SessionLockMap,
	id: Uuid,
	permit: Option<OwnedMutexGuard<()>>,
}

impl Drop for SessionDispatchGuard<'_> {
	fn drop(&mut self) {
		// Release the lock (and this guard's `Arc` reference) before deciding
		// whether the slot is idle, so the check is uniform with the cancel
		// path: `strong_count == 1` means only the map holds the entry.
		self.permit.take();
		prune_idle_lock(self.locks, &self.id);
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

	/// The datastore for this RPC interface as a shared handle.
	fn kvs_arc(&self) -> Arc<Datastore> {
		Arc::clone(&self.kvs)
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
	///
	/// When durable sessions are enabled, `attach` is the **only** place a
	/// durable entry is created. Every later persist is update-only (see
	/// [`persist_session`](Http::persist_session)), so a session detached on
	/// another node is never resurrected by a stale cached copy writing itself
	/// back. A `session_id` that already has a durable copy is rehydrated by
	/// the pre-dispatch `get_session` snapshot and so fails the map check
	/// below with `session_exists`, never reaching the create.
	async fn attach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		if self.session_map().contains_key(&session_id) {
			return Err(session_exists(session_id));
		}
		if self.attached_session_count() >= *HTTP_MAX_ATTACHED_SESSIONS {
			// Reclaim durably-expired cached sessions before refusing.
			self.prune_expired_cached_sessions();
			if self.attached_session_count() >= *HTTP_MAX_ATTACHED_SESSIONS {
				return Err(method_not_allowed(Method::Attach.to_string()));
			}
		}
		let mut session = Session::default().with_rt(Self::LQ_SUPPORT);
		session.id = Some(session_id);
		// Create the durable copy first, only if absent: a concurrent attach
		// for the same id on another node must not be silently overwritten
		// (it reports `session_exists`). A failure fails the attach rather
		// than leaving a live in-memory session with no durable backing.
		if let Some(ttl) = self.durable_session_ttl {
			let created = self
				.kvs
				.create_rpc_session(session_id, &session, ttl)
				.await
				.map_err(types_error_from_anyhow)?;
			if !created {
				return Err(session_exists(session_id));
			}
			self.durable_deadlines.insert(session_id, now_ms() + ttl.as_millis() as u64);
		}
		self.session_map().insert(session_id, Arc::new(RwLock::new(session)));
		Ok(DbResult::Other(Value::None))
	}

	// ------------------------------
	// Durable sessions
	// ------------------------------

	/// The HTTP transport is durability-capable; whether sessions are
	/// actually persisted is decided at runtime by
	/// [`persist_sessions_enabled`](RpcProtocol::persist_sessions_enabled),
	/// i.e. by the `--durable-sessions` flag.
	const PERSIST_SESSIONS: bool = true;

	fn persist_sessions_enabled(&self) -> bool {
		self.durable_session_ttl.is_some()
	}

	/// Rehydrate a client-attached session from the datastore.
	///
	/// Expired entries are dropped by the store on load. The load is a pure
	/// read — it does **not** refresh the durable TTL — because rehydration
	/// runs inside [`get_session`], which the ownership gate calls *before* it
	/// has verified the caller. Refreshing here would let any request that
	/// merely knows the session id (including one about to be rejected) keep
	/// the session alive, and a load-then-write could resurrect a copy that
	/// another node concurrently detached. The TTL is refreshed instead by
	/// [`touch_durable_session`](Http::touch_durable_session), after a request
	/// is authorized and dispatched.
	///
	/// Rehydration re-enters the session into the shared map, so it enforces
	/// the same [`HTTP_MAX_ATTACHED_SESSIONS`] cap as [`attach`](Http::attach)
	/// — without this, sessions attached through other cluster nodes could
	/// balloon this node's memory past the cap.
	///
	/// A rehydrated session still passes through
	/// [`Http::verify_caller_for_session`] before any non-attach method runs;
	/// the ownership gate calls `get_session`, which is what triggers this
	/// rehydration in the first place.
	async fn load_session(&self, id: &Uuid) -> Option<Session> {
		// Only ever called under persistence, but stay defensive. The TTL
		// value itself is unused: the load path never refreshes the expiry.
		self.durable_session_ttl?;
		// Ephemeral per-request ids are never persisted, so never loaded.
		if self.ephemeral_sessions.contains_key(id) {
			return None;
		}
		if self.attached_session_count() >= *HTTP_MAX_ATTACHED_SESSIONS {
			// Reclaim entries the durable TTL has already expired before
			// refusing, so stale abandoned sessions don't wedge the cap.
			self.prune_expired_cached_sessions();
			if self.attached_session_count() >= *HTTP_MAX_ATTACHED_SESSIONS {
				warn!(
					"Refusing to rehydrate RPC session {id}: the attached session limit is reached"
				);
				return None;
			}
		}
		match self.kvs.load_rpc_session(*id).await {
			Ok(Some((session, expires_at))) => {
				// Cache the stored expiry (in memory only — no durable write,
				// so this stays authorization-neutral) so
				// [`touch_durable_session`](Http::touch_durable_session) knows
				// when to slide the TTL.
				self.durable_deadlines.insert(*id, expires_at);
				Some(session)
			}
			Ok(None) => None,
			Err(err) => {
				warn!("Failed to load durable RPC session {id}: {err}");
				None
			}
		}
	}

	/// Mirror a changed session to the datastore, refreshing its expiry.
	///
	/// This is **update-only** ([`update_rpc_session`](Datastore::update_rpc_session)):
	/// it never recreates a durable entry, because the entry is created once
	/// by [`attach`](Http::attach) and a persist can run on a node whose cached
	/// copy is stale — a session detached/invalidated on another node must not
	/// be resurrected here. When the durable entry is gone, the session has
	/// been revoked elsewhere, so the stale local copy is dropped too (see also
	/// [`touch_durable_session`](Http::touch_durable_session), which reconciles
	/// against the authoritative copy after every request).
	///
	/// Failures (as opposed to a missing entry) are logged and swallowed: the
	/// in-memory session stays authoritative for this node and the durable
	/// copy is merely stale until the next successful persist.
	async fn persist_session(&self, id: &Uuid, session: &Session) {
		let Some(ttl) = self.durable_session_ttl else {
			return;
		};
		// Ephemeral per-request sessions are never persisted. `execute` only
		// tracks client-named sessions, so this is defence in depth.
		if self.ephemeral_sessions.contains_key(id) {
			return;
		}
		match self.kvs.update_rpc_session(*id, session, ttl).await {
			Ok(true) => {
				self.durable_deadlines.insert(*id, now_ms() + ttl.as_millis() as u64);
			}
			Ok(false) => {
				// The durable entry is gone: the session was detached or
				// invalidated on another node. Drop the stale local copy
				// instead of resurrecting it, so subsequent requests re-check
				// (and are refused) rather than being served the revoked
				// session.
				self.session_map().remove(id);
				self.durable_deadlines.remove(id);
			}
			Err(err) => {
				error!("Failed to persist durable RPC session {id}: {err}");
			}
		}
	}

	/// Remove a session's durable copy on teardown (`detach`).
	///
	/// A failure here is security-relevant — leaving the durable copy would
	/// let a session the client deliberately tore down be rehydrated until its
	/// TTL elapses — so the error is propagated (not swallowed). `del_session`
	/// removes the durable copy before local state, so a failure aborts the
	/// detach with the session still fully intact and the client can retry.
	async fn forget_session(&self, id: &Uuid) -> Result<(), TypesError> {
		if self.durable_session_ttl.is_none() {
			return Ok(());
		}
		self.kvs.delete_rpc_session(*id).await.map_err(types_error_from_anyhow)?;
		self.durable_deadlines.remove(id);
		Ok(())
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

#[cfg(test)]
mod tests {
	use surrealdb_core::dbs::Capabilities;
	use surrealdb_core::iam::Role;

	use super::*;

	const TTL: Duration = Duration::from_secs(3600);

	async fn mem_ds() -> Arc<Datastore> {
		Arc::new(
			Datastore::builder()
				.with_capabilities(Capabilities::all())
				.build_with_path("memory")
				.await
				.unwrap(),
		)
	}

	/// A durability-enabled HTTP handler over the shared datastore —
	/// constructing a second one models another cluster node, or the same
	/// node after a restart.
	fn http(ds: &Arc<Datastore>) -> Http {
		Http::new_with_durability(Arc::clone(ds), Some(TTL))
	}

	fn use_params(ns: &str, db: &str) -> Array {
		vec![Value::String(ns.to_owned()), Value::String(db.to_owned())].into()
	}

	/// Attach `sid` on `rpc`, bind it to a root-Owner principal (as `signin`
	/// would), and select `ns`/`db` so the bound session is persisted.
	async fn attach_owner_session(rpc: &Http, sid: Uuid, ns: &str, db: &str) {
		rpc.execute(None, sid, Some(sid), Method::Attach, Array::new()).await.unwrap();
		{
			let lock = rpc.get_session(&sid).await.unwrap();
			lock.write().await.au = Arc::new(Auth::for_root(Role::Owner));
		}
		// A mutating method after the auth change persists the whole session.
		rpc.execute(None, sid, Some(sid), Method::Use, use_params(ns, db)).await.unwrap();
	}

	#[tokio::test]
	async fn attached_session_survives_a_new_instance() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let node_a = http(&ds);
		attach_owner_session(&node_a, sid, "app", "app").await;

		// A second instance over the same datastore — another cluster node,
		// or the same node after a restart — rehydrates the session with its
		// auth and selected namespace/database intact.
		let node_b = http(&ds);
		{
			let lock = node_b.get_session(&sid).await.expect("session was not rehydrated");
			let session = lock.read().await;
			assert!(session.au.is_root(), "auth lost on rehydrate");
			assert_eq!(session.ns.as_deref(), Some("app"));
			assert_eq!(session.db.as_deref(), Some("app"));
		}

		// And a full request against the rehydrated session succeeds.
		node_b
			.execute(None, sid, Some(sid), Method::Use, use_params("app", "other"))
			.await
			.unwrap();
		let lock = node_b.get_session(&sid).await.unwrap();
		assert_eq!(lock.read().await.db.as_deref(), Some("other"));
	}

	#[tokio::test]
	async fn caller_gate_applies_to_rehydrated_sessions() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		attach_owner_session(&http(&ds), sid, "app", "app").await;

		// On a fresh instance, the ownership gate rehydrates the session and
		// still binds it to the principal that authenticated it: an anonymous
		// caller is refused (indistinguishably from a missing session), the
		// owning principal passes.
		let node_b = http(&ds);
		assert!(node_b.verify_caller_for_session(&sid, &Auth::default()).await.is_err());
		assert!(node_b.verify_caller_for_session(&sid, &Auth::for_root(Role::Owner)).await.is_ok());
	}

	#[tokio::test]
	async fn detach_prevents_rehydration_everywhere() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let node_a = http(&ds);
		attach_owner_session(&node_a, sid, "app", "app").await;
		assert!(ds.load_rpc_session(sid).await.unwrap().is_some());

		// Detach removes the durable copy, so no later instance can
		// resurrect the session.
		node_a.execute(None, sid, Some(sid), Method::Detach, Array::new()).await.unwrap();
		assert!(ds.load_rpc_session(sid).await.unwrap().is_none());
		let node_b = http(&ds);
		assert!(node_b.get_session(&sid).await.is_err());
	}

	#[tokio::test]
	async fn disabled_instances_never_touch_the_datastore() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let off = Http::new(Arc::clone(&ds));
		assert!(!off.persist_sessions_enabled());

		// With durability off, attach and mutations stay in-memory only.
		off.execute(None, sid, Some(sid), Method::Attach, Array::new()).await.unwrap();
		off.execute(None, sid, Some(sid), Method::Use, use_params("app", "app")).await.unwrap();
		assert!(ds.load_rpc_session(sid).await.unwrap().is_none());

		// A durability-enabled instance finds nothing to rehydrate.
		assert!(http(&ds).get_session(&sid).await.is_err());
	}

	#[tokio::test]
	async fn ephemeral_sessions_are_never_persisted() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let rid = Uuid::new_v4();
		rpc.register_ephemeral_session(rid, Arc::new(RwLock::new(Session::owner())));

		// A per-request execution (no client-named session) persists nothing.
		rpc.execute(None, rid, None, Method::Use, use_params("app", "app")).await.unwrap();
		assert!(ds.load_rpc_session(rid).await.unwrap().is_none());

		// Defence in depth: the hooks themselves refuse ephemeral ids.
		rpc.persist_session(&rid, &Session::owner()).await;
		assert!(ds.load_rpc_session(rid).await.unwrap().is_none());
		ds.persist_rpc_session(rid, &Session::owner(), TTL).await.unwrap();
		assert!(rpc.load_session(&rid).await.is_none());
	}

	#[tokio::test]
	async fn rehydration_respects_the_attached_session_cap() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		// Fill the shared map up to the attached-session cap.
		for _ in 0..*HTTP_MAX_ATTACHED_SESSIONS {
			rpc.set_session(Uuid::new_v4(), Arc::new(RwLock::new(Session::default())));
		}
		// A durable session that another node attached must not be
		// rehydrated past the cap — but its durable copy must survive the
		// refusal, so a node with capacity can still load it.
		let sid = Uuid::new_v4();
		ds.persist_rpc_session(sid, &Session::owner(), TTL).await.unwrap();
		assert!(rpc.load_session(&sid).await.is_none());
		assert!(ds.load_rpc_session(sid).await.unwrap().is_some());
	}

	#[tokio::test]
	async fn prune_reclaims_durably_expired_cached_sessions() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let live = Uuid::new_v4();
		let expired = Uuid::new_v4();

		// Two cached sessions: one whose recorded deadline is in the future,
		// one whose deadline has passed (its durable copy has expired).
		rpc.set_session(live, Arc::new(RwLock::new(Session::owner())));
		rpc.durable_deadlines.insert(live, now_ms() + 60_000);
		rpc.set_session(expired, Arc::new(RwLock::new(Session::owner())));
		rpc.durable_deadlines.insert(expired, now_ms().saturating_sub(1));

		rpc.prune_expired_cached_sessions();

		assert!(rpc.session_map().contains_key(&live), "a live cached session is kept");
		assert!(
			!rpc.session_map().contains_key(&expired),
			"a durably-expired cached session is pruned to reclaim cap space"
		);
		assert!(rpc.durable_deadlines.get(&expired).is_none(), "its deadline is dropped too");
	}

	#[tokio::test]
	async fn rehydration_does_not_refresh_the_durable_ttl() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		attach_owner_session(&http(&ds), sid, "app", "app").await;
		let (_, before) =
			ds.load_rpc_session(sid).await.unwrap().expect("precondition: session persisted");

		// The ownership gate calls `get_session` -> `load_session` before it
		// has verified the caller. That rehydration must be a pure read: it
		// must not rewrite the durable expiry, so an unauthorized probe cannot
		// extend the session's life (the durable TTL is refreshed only by
		// `touch_durable_session`, after dispatch). It does cache the *stored*
		// expiry in memory, which is how the cached copy inherits the idle TTL.
		let node_b = http(&ds);
		assert!(node_b.load_session(&sid).await.is_some(), "session should rehydrate");
		let (_, after) = ds.load_rpc_session(sid).await.unwrap().expect("session still present");
		assert_eq!(after, before, "rehydration must not rewrite the durable expiry");
		assert_eq!(
			node_b.durable_deadlines.get(&sid),
			Some(before),
			"rehydration should cache the stored expiry unchanged"
		);
	}

	#[tokio::test]
	async fn cached_session_is_evicted_when_the_durable_copy_expires() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();

		// A cached session whose durable copy is still live is reconciled, not
		// evicted.
		attach_owner_session(&rpc, sid, "app", "app").await;
		assert!(rpc.session_map().contains_key(&sid));
		rpc.revalidate_cached_session(&sid).await;
		assert!(rpc.session_map().contains_key(&sid), "a live cached session must not be evicted");

		// Expire the durable copy (as the idle TTL elapsing would). The
		// pre-dispatch revalidation now finds it gone and drops the stale
		// cached copy, so the gate refuses the next request.
		ds.persist_rpc_session(sid, &Session::owner(), Duration::from_millis(1)).await.unwrap();
		tokio::time::sleep(Duration::from_millis(5)).await;
		rpc.revalidate_cached_session(&sid).await;
		assert!(
			!rpc.session_map().contains_key(&sid),
			"a cached session whose durable copy expired must be evicted"
		);
		assert!(rpc.durable_deadlines.get(&sid).is_none(), "its deadline must be dropped too");
	}

	#[tokio::test]
	async fn touch_refreshes_only_when_the_ttl_runs_low() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		rpc.execute(None, sid, Some(sid), Method::Attach, Array::new()).await.unwrap();
		let fresh = rpc.durable_deadlines.get(&sid).expect("attach should record a deadline");

		// A freshly persisted session has (almost) the full TTL left, so a
		// touch is a no-op.
		rpc.touch_durable_session(&sid).await;
		assert_eq!(rpc.durable_deadlines.get(&sid), Some(fresh));

		// Age the recorded deadline under half the TTL: the next touch
		// re-persists with a fresh expiry.
		let aged = now_ms() + TTL.as_millis() as u64 / 4;
		rpc.durable_deadlines.insert(sid, aged);
		rpc.touch_durable_session(&sid).await;
		let refreshed =
			rpc.durable_deadlines.get(&sid).expect("touch should keep the deadline entry");
		assert!(refreshed > aged, "touch did not refresh the durable deadline");
	}

	#[tokio::test]
	async fn read_only_request_observes_a_remote_detach() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		attach_owner_session(&rpc, sid, "app", "app").await;
		assert!(rpc.session_map().contains_key(&sid));

		// Another node detaches the session (durable copy deleted) while this
		// node holds a cached copy with a fresh deadline.
		ds.delete_rpc_session(sid).await.unwrap();

		// The pre-dispatch revalidation must notice the durable entry is gone
		// and drop the stale cached copy — so the request is refused before it
		// runs (not served from cache), for read-only requests too, which never
		// trigger `persist_session`.
		rpc.revalidate_cached_session(&sid).await;
		assert!(
			!rpc.session_map().contains_key(&sid),
			"a remotely-detached session must be evicted before dispatch"
		);
	}

	#[tokio::test]
	async fn revalidation_adopts_a_remote_mutation_that_keeps_the_entry() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		// Node A attaches and signs in; node B caches the authenticated copy.
		attach_owner_session(&http(&ds), sid, "app", "app").await;
		let node_b = http(&ds);
		{
			let lock = node_b.get_session(&sid).await.expect("B rehydrates");
			assert!(lock.read().await.au.is_root(), "B caches the authenticated session");
		}

		// Another node mutates the session in place, keeping the durable entry
		// (as `invalidate` does — it clears auth but does not detach).
		let cleared = Session {
			id: Some(sid),
			..Default::default()
		};
		ds.persist_rpc_session(sid, &cleared, TTL).await.unwrap();

		// B's pre-dispatch revalidation must adopt the stored (cleared) value
		// rather than keep serving — and later write back — its stale
		// authenticated copy.
		node_b.revalidate_cached_session(&sid).await;
		let lock = node_b.get_session(&sid).await.expect("still cached");
		assert!(!lock.read().await.au.is_root(), "B must adopt the auth cleared on another node");
	}

	#[tokio::test]
	async fn create_rpc_session_is_conditional() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		assert!(
			ds.create_rpc_session(sid, &Session::owner(), TTL).await.unwrap(),
			"the first create must succeed"
		);
		assert!(
			!ds.create_rpc_session(sid, &Session::owner(), TTL).await.unwrap(),
			"a create for an existing id must be refused, not overwrite"
		);
	}

	#[tokio::test]
	async fn attach_reports_session_exists_for_a_durable_copy_created_elsewhere() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		// Another node has already created the durable session.
		ds.create_rpc_session(sid, &Session::owner(), TTL).await.unwrap();

		// A fresh node attaches the same id (not in its local map). The durable
		// create is conditional, so attach reports `session_exists` rather than
		// clobbering the session created elsewhere.
		let rpc = http(&ds);
		assert!(
			rpc.attach(sid).await.is_err(),
			"attach must not overwrite a durable session created on another node"
		);
	}

	#[tokio::test]
	async fn attach_creates_durable_but_refresh_never_resurrects() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();

		// `attach` is the sole creator of the durable copy.
		rpc.execute(None, sid, Some(sid), Method::Attach, Array::new()).await.unwrap();
		assert!(
			ds.load_rpc_session(sid).await.unwrap().is_some(),
			"attach must create the durable copy"
		);

		// Simulate the session being detached/invalidated on another node.
		ds.delete_rpc_session(sid).await.unwrap();

		// A refresh (a mutation persist, or a TTL touch) from this node's
		// still-cached copy must NOT recreate the durable session, and must
		// drop the now-revoked local copy so it is no longer served.
		rpc.persist_session(&sid, &Session::owner()).await;
		assert!(
			ds.load_rpc_session(sid).await.unwrap().is_none(),
			"a refresh must not resurrect a session deleted on another node"
		);
		assert!(
			!rpc.session_map().contains_key(&sid),
			"the revoked session must be dropped from the local cache"
		);
	}

	#[tokio::test]
	async fn session_lock_slot_is_freed_after_a_canceled_waiter() {
		let locks = SessionLocks::default();
		let id = Uuid::new_v4();
		let held = locks.acquire(id).await;
		assert_eq!(locks.len(), 1);

		// A second acquire blocks while `held` owns the slot, then its future
		// is canceled (here by a timeout) before it ever acquires the lock.
		let waiter = tokio::time::timeout(Duration::from_millis(20), locks.acquire(id)).await;
		assert!(waiter.is_err(), "the waiter should time out while the slot is held");
		assert_eq!(locks.len(), 1, "the holder still owns the slot");

		// Releasing the holder with no live waiters prunes the slot.
		drop(held);
		assert_eq!(locks.len(), 0, "a released slot with no waiters must be pruned");
	}

	#[tokio::test]
	async fn session_lock_slot_is_freed_when_a_registered_waiter_is_dropped() {
		let locks = SessionLocks::default();
		let id = Uuid::new_v4();
		let held = locks.acquire(id).await;

		// Poll a second acquire once so it registers as a waiter on the slot,
		// while `held` still owns the lock.
		let mut waiter = Box::pin(locks.acquire(id));
		tokio::select! {
			biased;
			_ = &mut waiter => panic!("the waiter must not acquire while the slot is held"),
			_ = std::future::ready(()) => {}
		}
		assert_eq!(locks.len(), 1);

		// The holder releases while the waiter is still registered (so the
		// holder's Drop leaves the slot for the waiter), then the waiter is
		// canceled before acquiring. Neither builds a guard for the final
		// state, so the slot must still be pruned rather than orphaned.
		drop(held);
		drop(waiter);
		assert_eq!(locks.len(), 0, "a canceled registered waiter must not orphan its slot");
	}
}
