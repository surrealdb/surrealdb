use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use surrealdb_core::dbs::{Session, StoredRpcSession};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{RpcProtocol, query_timeout_error, types_error_from_anyhow};
use surrealdb_iam::{Auth, Level};
use surrealdb_rpc::error::{
	method_not_allowed, method_not_found, session_exists, session_not_found,
};
use surrealdb_rpc::{DbResult, Method};
use surrealdb_types::{Array, Error as TypesError, HashMap, Value};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use uuid::Uuid;
use web_time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cnf::{HTTP_MAX_ATTACHED_SESSIONS, PKG_NAME, PKG_VERSION};

/// How many conditional writes a request spends storing its changes to an
/// attached session, or sliding the session's TTL, before giving up against
/// concurrent writers.
///
/// Writers on one node take turns (see [`SESSION_WRITE_STRIPES`]), so a
/// write is lost only to another node, or to the first write of a request
/// that loaded the session before this node's last write to it.
const SESSION_WRITE_ATTEMPTS: usize = 5;

/// How many locks this node's writes to attached sessions are spread over,
/// by session id. Writes to one session always take the same lock, so they
/// take turns; writes to different sessions rarely share one.
const SESSION_WRITE_STRIPES: usize = 256;

/// The least time between two checks of this node's attach slots against
/// storage. A check reads every slot's stored session, so it runs only when the
/// cap is reached, one at a time, and no more often than this, so an anonymous
/// caller hammering a full node cannot multiply it.
const ATTACHED_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How many attach slots one storage read checks.
const ATTACHED_CHECK_BATCH: usize = 1000;

/// Milliseconds since the UNIX epoch, the clock the durable session store
/// stamps expiries with.
///
/// Matches the datastore's `SystemClock::now()`: a backwards system clock is a
/// fault and panics rather than silently yielding `0`, which would make every
/// stored expiry look distant and stop the TTL from ever sliding.
fn now_ms() -> u64 {
	match SystemTime::now().duration_since(UNIX_EPOCH) {
		Ok(duration) => duration.as_millis() as u64,
		Err(error) => panic!("Clock may have gone backwards: {:?}", error.duration()),
	}
}

/// The HTTP `/rpc` transport.
///
/// HTTP has no connection to hang session state on, so every request runs on a
/// session private to that request, registered under an id generated for the
/// request, never returned to any client, and dropped when the request ends:
///
/// - A request that names no session runs on the session its own headers and credentials produced.
/// - A request that names an attached session runs on a copy of that session as stored, loaded when
///   the request starts (see [`Http::handle`]).
///
/// Attached sessions live in the datastore, not in this process. Every node
/// serving `/rpc` against one datastore therefore serves every attached
/// session: a load balancer may route each request to any node, and a restart
/// loses nothing. A request that changes its session writes the change back
/// conditionally on the stored copy still being the one it loaded, merging
/// with any change committed meanwhile (see [`merge_session_changes`]), so
/// concurrent requests on one session need no coordination between them,
/// whether they run on one node or several.
///
/// # Security
///
/// - Session enumeration (`sessions`) is refused.
/// - A request that names an attached session must present request-level credentials whose
///   principal matches the one the stored session is bound to (see [`caller_may_use_session`]); a
///   mismatch is reported exactly like a missing session.
/// - Request-private session ids are never stored, so a client that names one finds no session.
/// - Each node bounds how many sessions callers can attach through it (see
///   [`HTTP_MAX_ATTACHED_SESSIONS`]).
///
/// # Expiry
///
/// An attached session expires between one and one and a half session TTLs
/// after the last request that named it. Every write sets the stored expiry
/// one and a half TTLs ahead, and a request that finds less than one TTL left
/// extends it (see [`refresh`](Self::refresh)), so a session in steady use is
/// written for its expiry at most about once per half TTL.
pub struct Http {
	kvs: Arc<Datastore>,
	/// The request-private sessions of the requests in flight.
	sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
	/// The least time an attached session lives after the last request that
	/// named it.
	session_ttl: Duration,
	/// Serializes this node's writes to one attached session, striped by id
	/// (see [`SESSION_WRITE_STRIPES`]).
	session_writes: Box<[AsyncMutex<()>]>,
	/// The slots counted against [`HTTP_MAX_ATTACHED_SESSIONS`]. Never locked
	/// across an `await`.
	attach_slots: Mutex<AttachSlots>,
	/// When the attach slots were last checked against storage, locked while
	/// a check runs.
	attached_checked: AsyncMutex<Option<Instant>>,
}

/// This node's attach slots: one per session attached through it that may
/// still be live, and one per attach in flight.
#[derive(Default)]
struct AttachSlots {
	/// The sessions attached through this node that may still be live, each
	/// with the number of the attach that took its slot.
	attached: std::collections::HashMap<Uuid, u64>,
	/// The attaches in flight, each holding a slot until it settles (see
	/// [`AttachSlot`]).
	reserved: usize,
	/// The number given to the next attach that takes a slot.
	next: u64,
}

impl AttachSlots {
	/// The slots taken, settled or not.
	fn used(&self) -> usize {
		self.attached.len() + self.reserved
	}

	/// Free `id`'s slot if the attach numbered `holder` still holds it, so a
	/// slot that a later attach of the same id has taken meanwhile is kept.
	fn release(&mut self, id: Uuid, holder: u64) {
		if self.attached.get(&id) == Some(&holder) {
			self.attached.remove(&id);
		}
	}
}

/// A slot reserved by an attach in flight. Dropping it settles the
/// reservation: the slot passes to the session unless
/// [`release`](Self::release) reports that the attach created nothing. An
/// attach abandoned mid-create, whose write may have committed, therefore
/// keeps its slot until [`Http::reclaim_attach_slots`] finds the session
/// absent.
struct AttachSlot<'a> {
	http: &'a Http,
	id: Uuid,
	created: bool,
}

impl AttachSlot<'_> {
	/// Free the slot: the attach created no session.
	fn release(mut self) {
		self.created = false;
	}
}

impl Drop for AttachSlot<'_> {
	fn drop(&mut self) {
		let mut slots = self.http.slots();
		slots.reserved -= 1;
		if self.created {
			let holder = slots.next;
			slots.next += 1;
			slots.attached.insert(self.id, holder);
		}
	}
}

impl Http {
	/// Create the HTTP transport, storing attached sessions in `kvs` with an
	/// idle TTL of `session_ttl` (see [Expiry](Self#expiry)).
	///
	/// # Panics
	///
	/// If `session_ttl` is under one millisecond. Expiries are stored in
	/// milliseconds, so such a TTL would expire every session at birth.
	pub fn new(kvs: Arc<Datastore>, session_ttl: Duration) -> Self {
		assert!(
			session_ttl >= Duration::from_millis(1),
			"the durable session TTL must be at least one millisecond, not {session_ttl:?}"
		);
		Self {
			kvs,
			sessions: HashMap::new(),
			session_ttl,
			session_writes: (0..SESSION_WRITE_STRIPES).map(|_| AsyncMutex::new(())).collect(),
			attach_slots: Mutex::default(),
			attached_checked: AsyncMutex::new(None),
		}
	}

	/// This node's attach slots. Every critical section is free of `await`
	/// and of panics, so a poisoned lock still holds consistent slots.
	fn slots(&self) -> MutexGuard<'_, AttachSlots> {
		self.attach_slots.lock().unwrap_or_else(PoisonError::into_inner)
	}

	/// The number of the attach holding `id`'s slot here, if any. Read it
	/// before the storage read that shows the session gone, and pass it to
	/// [`release_attach_slot`](Self::release_attach_slot).
	fn attach_slot_holder(&self, id: Uuid) -> Option<u64> {
		self.slots().attached.get(&id).copied()
	}

	/// Free `id`'s slot, found gone in storage after
	/// [`attach_slot_holder`](Self::attach_slot_holder) returned `holder`,
	/// unless the id has been attached here again meanwhile.
	fn release_attach_slot(&self, id: Uuid, holder: Option<u64>) {
		if let Some(holder) = holder {
			self.slots().release(id, holder);
		}
	}

	/// How far ahead each write sets an attached session's stored expiry.
	fn session_lifetime(&self) -> Duration {
		self.session_ttl + self.session_ttl / 2
	}

	/// The lock this node's writes to attached session `id` take turns on.
	fn session_write_lock(&self, id: Uuid) -> &AsyncMutex<()> {
		&self.session_writes[(id.as_u128() % SESSION_WRITE_STRIPES as u128) as usize]
	}

	/// Run one `/rpc` request.
	///
	/// `request` is the session the request's own headers and credentials
	/// produced, and `client_session` the attached session it names, if any.
	///
	/// A request naming an attached session loads that session's stored copy
	/// first, so it runs on every change any node has made to it, and a session
	/// detached or expired anywhere is refused here. It then passes the
	/// ownership gate: the request's principal must match the principal the
	/// stored session is bound to. `attach` is exempt, having no session yet;
	/// every other method, `detach` included, is gated. After a successful
	/// method, a change to the session is stored (see
	/// [`store_changes`](Self::store_changes)), and a session left unchanged has
	/// its TTL slid when it runs low (see [`refresh`](Self::refresh)). A failed
	/// method stores nothing, whatever it did to its copy of the session.
	pub(crate) async fn handle(
		&self,
		request: Session,
		txn: Option<Uuid>,
		client_session: Option<Uuid>,
		method: Method,
		params: Array,
	) -> Result<DbResult, TypesError> {
		let Some(id) = client_session else {
			return self.execute_private(request, txn, None, method, params).await;
		};
		if method == Method::Attach {
			return self.execute_private(request, txn, Some(id), method, params).await;
		}
		let Some((session, stored)) =
			self.kvs.load_rpc_session(id).await.map_err(types_error_from_anyhow)?
		else {
			return Err(session_not_found(id));
		};
		if !caller_may_use_session(&session.au, &request.au) {
			return Err(session_not_found(id));
		}
		let before = session.clone();
		let rid = Uuid::new_v4();
		let private = self.register_private_session(rid, session);
		let start = Instant::now();
		let result = match RpcProtocol::dispatch(self, txn, rid, Some(id), method, params).await {
			Ok(result) => self
				.settle(id, &request.au, stored, before, method, &private, start)
				.await
				.map(|()| result),
			Err(err) => Err(err),
		};
		// Recorded once the response is settled, so a request whose changes
		// could not be stored is observed as the failure the client receives.
		RpcProtocol::record_completion(self, rid, method, start, &result).await;
		result
	}

	/// Store what a successful `method` left in the request-private session
	/// `private`, run on attached session `id` as loaded (`before`, stored as
	/// `stored`): its changes when it changed the session (see
	/// [`store_changes`](Self::store_changes)), or a TTL slide when it did not
	/// (see [`refresh`](Self::refresh)). A `detach` removed the session, which
	/// leaves nothing to store.
	///
	/// The query timeout (`--query-timeout`) bounds the whole method call, so
	/// these writes get what remains of it since `start`, the method having
	/// spent the rest. A store cut short fails the request with the timeout,
	/// whether or not its write landed, as for any method the timeout cuts
	/// short. A slide cut short is dropped, like a slide that failed.
	/// Exempt are transaction-control methods, as in [`RpcProtocol::dispatch`],
	/// and the methods that have already issued credentials the client cannot
	/// ask for again: `signup` (which has created its user), `signin`, and
	/// `refresh` (which has spent its single-use refresh token). The timeout
	/// would withhold them only because the session write after them was slow.
	///
	/// When the changes cannot be stored, the request fails and `private` is
	/// put back as it was loaded, so the completion recorded for the request
	/// never carries a session context that was not stored.
	///
	/// The exception is a `refresh` that only renewed the session's
	/// authentication (the same identity with a new token and expiry, and
	/// possibly a recomputed `CONTEXT` payload; see [`renews_authentication`])
	/// and was refused because the session was removed or bound to another
	/// identity meanwhile. It succeeds without being stored: it is as if it ran
	/// before that change, which would have discarded the renewal anyway, and
	/// the token pair it returns is the client's only way to stay signed in.
	/// No stored copy is one the renewed token passes the ownership gate for
	/// while holding the authentication the renewal replaced.
	///
	/// A `refresh` whose store failed fails like any change: the stored copy
	/// still holds the authentication the renewal replaced, whose `CONTEXT` the
	/// renewal may have narrowed, under the identity the renewed token passes
	/// the gate as, so no token pair is handed out for it. The client keeps the
	/// session and its previous token, and signs in again once that token
	/// expires.
	///
	/// Any other method that renews, such as an `authenticate` with another
	/// token for the same identity, spent nothing the client cannot send again,
	/// so a refusal fails it as it fails any change.
	#[allow(
		clippy::too_many_arguments,
		reason = "one request's settled state, passed through once"
	)]
	async fn settle(
		&self,
		id: Uuid,
		caller: &Auth,
		stored: StoredRpcSession,
		before: Session,
		method: Method,
		private: &PrivateSession<'_>,
		start: Instant,
	) -> Result<(), TypesError> {
		if method == Method::Detach {
			return Ok(());
		}
		let Some(after) = private.session().await else {
			return Err(TypesError::internal(format!(
				"The request-private copy of session {id} was removed while the request ran"
			)));
		};
		let budget = self
			.kvs
			.query_timeout()
			.filter(|_| {
				!method.is_transaction_control()
					&& !matches!(method, Method::Signup | Method::Signin | Method::Refresh)
			})
			.map(|timeout| (timeout, timeout.saturating_sub(start.elapsed())));
		if after == before {
			let slide = self.refresh(id, stored);
			match budget {
				Some((_, remaining)) => {
					if tokio::time::timeout(remaining, slide).await.is_err() {
						warn!("Timed out sliding the expiry of durable RPC session {id}");
					}
				}
				None => slide.await,
			}
			return Ok(());
		}
		let renewal = method == Method::Refresh && renews_authentication(&before, &after);
		let store = self.store_changes(id, caller, stored, before.clone(), after);
		let written = match budget {
			Some((timeout, remaining)) => tokio::time::timeout(remaining, store)
				.await
				.unwrap_or_else(|_| Err(NotStored::Failed(query_timeout_error(timeout)))),
			None => store.await,
		};
		let err = match written {
			Ok(()) => return Ok(()),
			Err(NotStored::Refused) if renewal => {
				warn!(
					"The renewed authentication of durable RPC session {id} was not stored: the session was removed or re-authenticated meanwhile"
				);
				return Ok(());
			}
			Err(NotStored::Refused) => session_changed(id),
			Err(NotStored::Failed(err)) => err,
		};
		private.reset(before).await;
		Err(err)
	}

	/// Run `method` on `session` as a request-private session.
	async fn execute_private(
		&self,
		session: Session,
		txn: Option<Uuid>,
		client_session: Option<Uuid>,
		method: Method,
		params: Array,
	) -> Result<DbResult, TypesError> {
		let rid = Uuid::new_v4();
		let _private = self.register_private_session(rid, session);
		RpcProtocol::execute(self, txn, rid, client_session, method, params).await
	}

	/// Register `session` as the request-private session `id`, removed when
	/// the returned guard drops — also when a client disconnects and the
	/// request is dropped mid-flight.
	///
	/// Crate-private: only the HTTP transport may mint request-private
	/// sessions.
	pub(crate) fn register_private_session(
		&self,
		id: Uuid,
		session: Session,
	) -> PrivateSession<'_> {
		self.sessions.insert(id, Arc::new(RwLock::new(session)));
		PrivateSession {
			http: self,
			id,
		}
	}

	/// Write the changes a request made to attached session `id` — `after`,
	/// from `before`, which it loaded as `stored` — back to storage. `caller` is
	/// the principal the request's own credentials produced.
	///
	/// The write is conditional on the stored copy still being `stored`. When
	/// another request, on this node or another, changed the session
	/// meanwhile, this request's changes are merged onto the newer copy (see
	/// [`merge_session_changes`]) and the write retried, so concurrent changes
	/// to different parts of a session both survive and no change is
	/// overwritten by the value a request started from. This node's writes to
	/// one session take turns, so only another node's writes cost retries.
	///
	/// The newer copy must still be one this request could have run on, or its
	/// changes are refused rather than merged ([`NotStored::Refused`]):
	///
	/// - The session is still stored: one detached or expired meanwhile is never recreated.
	/// - The caller still passes the ownership gate against it.
	/// - Unless this request itself changed the session's identity, the session still has the
	///   identity this request ran under (see [`same_identity`]). A `use` or `set` is authorized
	///   against that identity, so one approved under an identity another request has since
	///   replaced is not applied under the new one. A renewal stored meanwhile keeps the identity,
	///   so it does not refuse them.
	///
	/// A write that errs, or keeps losing to concurrent writers through every
	/// attempt, is [`NotStored::Failed`]: the stored copy is then still one this
	/// request could have run on.
	///
	/// Either outcome fails the request (see [`settle`](Self::settle) for the
	/// one exception): its change did not take, and reporting success would
	/// tell a client that an `invalidate` logged it out, or a `use` switched
	/// its database, when the session is as it was.
	async fn store_changes(
		&self,
		id: Uuid,
		caller: &Auth,
		mut stored: StoredRpcSession,
		mut before: Session,
		mut after: Session,
	) -> Result<(), NotStored> {
		let _turn = self.session_write_lock(id).lock().await;
		for _ in 0..SESSION_WRITE_ATTEMPTS {
			let written = self
				.kvs
				.replace_rpc_session(id, &stored, &after, self.session_lifetime())
				.await
				.map_err(|err| NotStored::Failed(types_error_from_anyhow(err)))?;
			if written.is_some() {
				return Ok(());
			}
			let holder = self.attach_slot_holder(id);
			let Some((current, current_stored)) = self
				.kvs
				.load_rpc_session(id)
				.await
				.map_err(|err| NotStored::Failed(types_error_from_anyhow(err)))?
			else {
				self.release_attach_slot(id, holder);
				return Err(NotStored::Refused);
			};
			if !caller_may_use_session(&current.au, caller)
				|| (!same_identity(&before, &current) && same_identity(&before, &after))
			{
				return Err(NotStored::Refused);
			}
			after = merge_session_changes(&before, after, current.clone());
			before = current;
			stored = current_stored;
		}
		Err(NotStored::Failed(TypesError::internal(format!(
			"Session {id} was changed concurrently too often for this request's changes to be stored"
		))))
	}

	/// Slide attached session `id`'s expiry after a request that left the
	/// session unchanged, when less than one TTL of it remains (see
	/// [Expiry](Self#expiry)).
	///
	/// It extends the stored contents rather than writing this request's copy
	/// of them, so it never carries the request's view over a change made
	/// meanwhile. A failure is logged and leaves the session expiring as
	/// before: the request itself succeeded, and the next one tries again.
	async fn refresh(&self, id: Uuid, mut stored: StoredRpcSession) {
		let threshold = self.session_ttl.as_millis() as u64;
		if stored.expires_at().saturating_sub(now_ms()) >= threshold {
			return;
		}
		let _turn = self.session_write_lock(id).lock().await;
		for _ in 0..SESSION_WRITE_ATTEMPTS {
			match self.kvs.refresh_rpc_session(id, &stored, self.session_lifetime()).await {
				Ok(Some(_)) => return,
				Ok(None) => {}
				Err(err) => {
					warn!("Failed to refresh durable RPC session {id}: {err}");
					return;
				}
			}
			// Another request changed the session; extend what it stored, if
			// that still needs it.
			let holder = self.attach_slot_holder(id);
			match self.kvs.load_rpc_session(id).await {
				Ok(Some((_, current))) => stored = current,
				Ok(None) => {
					self.release_attach_slot(id, holder);
					return;
				}
				Err(err) => {
					warn!("Failed to refresh durable RPC session {id}: {err}");
					return;
				}
			}
			if stored.expires_at().saturating_sub(now_ms()) >= threshold {
				return;
			}
		}
	}

	/// Reserve one of this node's attach slots for attaching `id`, reclaiming
	/// the slots of sessions that ended elsewhere when the cap is reached.
	/// `None` when the node is full.
	///
	/// The check and the reservation are one step, and the slot is held from
	/// before the session is created until the attach settles, so concurrent
	/// attaches never hold more slots than the cap.
	async fn reserve_attach_slot(&self, id: Uuid) -> Option<AttachSlot<'_>> {
		if let Some(slot) = self.try_reserve_attach_slot(id) {
			return Some(slot);
		}
		self.reclaim_attach_slots().await;
		self.try_reserve_attach_slot(id)
	}

	/// Reserve a free attach slot for attaching `id`, if there is one.
	fn try_reserve_attach_slot(&self, id: Uuid) -> Option<AttachSlot<'_>> {
		let mut slots = self.slots();
		if slots.used() >= *HTTP_MAX_ATTACHED_SESSIONS {
			return None;
		}
		slots.reserved += 1;
		Some(AttachSlot {
			http: self,
			id,
			created: true,
		})
	}

	/// Drop the attach slots whose sessions are no longer live in storage:
	/// detached on another node, or expired.
	///
	/// A session attached here and detached on another node leaves its slot
	/// here, since nothing tells this node; without this, a client attaching
	/// through one node and detaching through others would exhaust that node's
	/// cap with sessions that no longer exist. See [`ATTACHED_CHECK_INTERVAL`]
	/// for how often it runs.
	async fn reclaim_attach_slots(&self) {
		let Ok(mut checked) = self.attached_checked.try_lock() else {
			return;
		};
		if checked.is_some_and(|at| at.elapsed() < ATTACHED_CHECK_INTERVAL) {
			return;
		}
		*checked = Some(Instant::now());
		// Each slot is freed only if the attach that held it when it was read
		// here still holds it, so a session attached here again while the
		// check runs keeps its slot.
		let held: Vec<(Uuid, u64)> =
			self.slots().attached.iter().map(|(&id, &holder)| (id, holder)).collect();
		for batch in held.chunks(ATTACHED_CHECK_BATCH) {
			let ids: Vec<Uuid> = batch.iter().map(|&(id, _)| id).collect();
			match self.kvs.live_rpc_sessions(&ids).await {
				Ok(live) => {
					let mut slots = self.slots();
					for (&(id, holder), live) in batch.iter().zip(live) {
						if !live {
							slots.release(id, holder);
						}
					}
				}
				Err(err) => {
					warn!("Failed to check attached RPC sessions against storage: {err}");
					return;
				}
			}
		}
	}
}

/// A request-private session, removed from [`Http`] when this drops.
pub(crate) struct PrivateSession<'a> {
	http: &'a Http,
	id: Uuid,
}

impl PrivateSession<'_> {
	/// The session as the request left it, or `None` if a method removed it.
	///
	/// Read from the session map rather than a handle kept at registration, so
	/// a method that replaced the session object is read correctly.
	async fn session(&self) -> Option<Session> {
		let lock = self.http.sessions.get(&self.id)?;
		let session = lock.read().await.clone();
		Some(session)
	}

	/// Replace the session with `session`, unless a method removed it.
	async fn reset(&self, session: Session) {
		if let Some(lock) = self.http.sessions.get(&self.id) {
			*lock.write().await = session;
		}
	}
}

impl Drop for PrivateSession<'_> {
	fn drop(&mut self) {
		self.http.sessions.remove(&self.id);
	}
}

/// Why a request's changes to an attached session were not stored (see
/// [`Http::store_changes`]).
#[derive(Debug)]
enum NotStored {
	/// The session changed under the request so that its changes no longer
	/// apply: it was removed, or is now bound to an identity the request did
	/// not run under. Reported as [`session_changed`].
	Refused,
	/// The write failed, was cut short, or lost to concurrent writers through
	/// every attempt. The stored copy is still one the request could have run
	/// on.
	Failed(TypesError),
}

/// The error for a request whose changes to attached session `id` cannot be
/// stored because the session changed under it while it ran: detached,
/// expired, or bound to another identity (see [`Http::store_changes`]).
///
/// Distinct from `session_not_found`: the method itself ran, so a client must
/// not take the request for one that never reached the session and resend it.
fn session_changed(id: Uuid) -> TypesError {
	TypesError::internal(format!(
		"Session {id} was detached, expired or re-authenticated while this request ran, so the request's changes to it were not stored"
	))
}

/// Determine whether a caller presenting `caller_au` may operate on a
/// session whose currently-bound principal is `session_au`.
///
/// - An unauthenticated session (`Session.au` at [`Level::No`]) is open to any caller; an
///   authentication command issued by the caller then binds the session to the caller's principal
///   for all subsequent requests. This preserves the standard `attach` followed by
///   `signin`/`authenticate` flow, and an unauthenticated session carries no privileges, while
///   session ids are 128-bit random and cannot be enumerated.
/// - An authenticated session is only reachable by callers whose request-level [`Auth`] matches on
///   actor id and [`Level`]. Roles are intentionally excluded so role grants/revocations for the
///   same identity do not lock the legitimate owner out.
fn caller_may_use_session(session_au: &Auth, caller_au: &Auth) -> bool {
	if *session_au.level() == Level::No {
		true
	} else {
		session_au.id() == caller_au.id() && session_au.level() == caller_au.level()
	}
}

/// Whether sessions `a` and `b` act as the same identity: the same actor id
/// and level.
///
/// This is all a `use` is authorized against (the query capability and the
/// tenant boundary read the level), so a change that keeps the identity leaves
/// a selection approved under it valid. That holds even when the change
/// recomputed the `CONTEXT` payload, as a `refresh` may.
fn same_identity(a: &Session, b: &Session) -> bool {
	a.au.id() == b.au.id() && a.au.level() == b.au.level()
}

/// Whether a request that left its session as `after`, from `before`, only
/// renewed the session's authentication: the same identity (see
/// [`same_identity`]), with nothing outside the authentication group (`au`,
/// `ac`, `tk`, `rd`, `data`, `exp`) changed. A `refresh` leaves its session so,
/// and so may an `authenticate` or a sign-in as the same identity. The
/// `CONTEXT` payload (`data`) may change, since a refresh recomputes it.
fn renews_authentication(before: &Session, after: &Session) -> bool {
	// Destructured in full, so a field added to `Session` must be placed
	// inside or outside the authentication group before this compiles.
	let Session {
		au: _,
		ac: _,
		tk: _,
		rd: _,
		data: _,
		exp: _,
		rt,
		ip,
		or,
		id,
		ns,
		db,
		variables,
		redact_volatile_explain_attrs,
	} = after;
	after != before
		&& same_identity(before, after)
		&& *rt == before.rt
		&& *ip == before.ip
		&& *or == before.or
		&& *id == before.id
		&& *ns == before.ns
		&& *db == before.db
		&& *variables == before.variables
		&& *redact_volatile_explain_attrs == before.redact_volatile_explain_attrs
}

/// Apply the changes a request made to a session — `ours`, changed from
/// `before` — on top of `theirs`, the stored copy another request committed
/// while it ran.
///
/// A field the request left as it was keeps the stored value, so a change made
/// elsewhere survives; a field the request changed takes the request's value.
/// Fields that only change together move together: the authentication state
/// (`au`, `ac`, `tk`, `rd`, `data`, `exp`) is set as one by signin, signup,
/// authenticate, invalidate and reset, and the selected namespace and database
/// by `use`, so a principal or a selection is never assembled from parts of
/// two. Variables merge per name.
///
/// A request that changed the identity (see [`same_identity`]) also keeps its
/// own selection: a `use` stored meanwhile was authorized against the identity
/// this request replaced, and must not be paired with the new one. A renewal
/// keeps the identity, so a `use` stored meanwhile keeps its selection even
/// when the renewal recomputed `CONTEXT`. The opposite case — the stored copy's
/// identity changed and this request's did not — is refused by
/// [`Http::store_changes`] before any merge.
///
/// So two requests setting different variables both land. When both changed
/// the same thing, the request storing later wins, as it would had the two run
/// one after the other.
fn merge_session_changes(before: &Session, ours: Session, theirs: Session) -> Session {
	let identity_changed = !same_identity(before, &ours);
	// Destructured in full, so a field added to `Session` needs a merge rule
	// here before this compiles.
	let Session {
		au,
		rt,
		ip,
		or,
		id,
		ns,
		db,
		ac,
		tk,
		rd,
		data,
		exp,
		variables,
		redact_volatile_explain_attrs,
	} = ours;
	let Session {
		au: their_au,
		rt: their_rt,
		ip: their_ip,
		or: their_or,
		id: their_id,
		ns: their_ns,
		db: their_db,
		ac: their_ac,
		tk: their_tk,
		rd: their_rd,
		data: their_data,
		exp: their_exp,
		variables: their_variables,
		redact_volatile_explain_attrs: their_redact_volatile_explain_attrs,
	} = theirs;
	let auth_changed = au != before.au
		|| ac != before.ac
		|| tk != before.tk
		|| rd != before.rd
		|| data != before.data
		|| exp != before.exp;
	let (au, ac, tk, rd, data, exp) = if auth_changed {
		(au, ac, tk, rd, data, exp)
	} else {
		(their_au, their_ac, their_tk, their_rd, their_data, their_exp)
	};
	let (ns, db) = if identity_changed || ns != before.ns || db != before.db {
		(ns, db)
	} else {
		(their_ns, their_db)
	};
	let mut merged_variables = their_variables;
	for (name, value) in variables.iter() {
		if before.variables.get(name) != Some(value) {
			merged_variables.insert(name.clone(), value.clone());
		}
	}
	for (name, _) in before.variables.iter() {
		if variables.get(name).is_none() {
			merged_variables.remove(name);
		}
	}
	Session {
		au,
		rt: if rt != before.rt {
			rt
		} else {
			their_rt
		},
		ip: if ip != before.ip {
			ip
		} else {
			their_ip
		},
		or: if or != before.or {
			or
		} else {
			their_or
		},
		id: if id != before.id {
			id
		} else {
			their_id
		},
		ns,
		db,
		ac,
		tk,
		rd,
		data,
		exp,
		variables: merged_variables,
		redact_volatile_explain_attrs: if redact_volatile_explain_attrs
			!= before.redact_volatile_explain_attrs
		{
			redact_volatile_explain_attrs
		} else {
			their_redact_volatile_explain_attrs
		},
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

	/// The request-private sessions of the requests in flight. Attached
	/// sessions are not here: they are stored in the datastore, and each
	/// request runs on its own copy.
	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	/// Session enumeration is not available on the HTTP transport.
	///
	/// HTTP has no legitimate per-caller use for listing other clients'
	/// sessions, and a listing would hand any anonymous caller the ids of
	/// privileged sessions to hijack, so the method is refused outright.
	async fn sessions(&self) -> Result<DbResult, TypesError> {
		Err(method_not_allowed(Method::Sessions.to_string()))
	}

	/// Create attached session `session_id` in storage.
	///
	/// The create is conditional, so it reports `session_exists` for an id any
	/// node has already attached rather than overwriting that session, and is
	/// refused once this node's attach cap is reached (see
	/// [`HTTP_MAX_ATTACHED_SESSIONS`]). Nothing is kept in memory: every later
	/// request loads the session from storage.
	async fn attach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		let Some(slot) = self.reserve_attach_slot(session_id).await else {
			return Err(method_not_allowed(Method::Attach.to_string()));
		};
		let mut session = Session::default().with_rt(Self::LQ_SUPPORT);
		session.id = Some(session_id);
		match self.kvs.create_rpc_session(session_id, &session, self.session_lifetime()).await {
			Ok(Some(_)) => Ok(DbResult::Other(Value::None)),
			Ok(None) => {
				slot.release();
				Err(session_exists(session_id))
			}
			Err(err) => {
				slot.release();
				Err(types_error_from_anyhow(err))
			}
		}
	}

	/// Remove attached session `session_id` from storage, so no node serves it
	/// again.
	///
	/// A failure fails the detach, leaving the session intact for the client
	/// to retry: reporting success while the stored copy survives would leave
	/// a session the client deliberately tore down usable until its TTL
	/// elapses.
	async fn detach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		let holder = self.attach_slot_holder(session_id);
		self.kvs.delete_rpc_session(session_id).await.map_err(types_error_from_anyhow)?;
		self.release_attach_slot(session_id, holder);
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
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
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
	use surrealdb_iam::{Actor, Role};
	use surrealdb_rpc::capabilities::Capabilities;

	use super::*;

	const TTL: Duration = Duration::from_secs(3600);

	async fn mem_ds() -> Arc<Datastore> {
		Datastore::builder()
			.with_capabilities(Capabilities::all())
			.build_with_path("memory")
			.await
			.unwrap()
	}

	/// An HTTP transport over `ds`. Constructing a second one over the same
	/// datastore models another cluster node, or the same node after a
	/// restart.
	fn http(ds: &Arc<Datastore>) -> Http {
		Http::new(Arc::clone(ds), TTL)
	}

	fn params(values: &[&str]) -> Array {
		values.iter().map(|v| Value::String((*v).to_owned())).collect::<Vec<_>>().into()
	}

	/// A request on attached session `sid` from a caller authenticated as the
	/// root owner.
	async fn owner_call(
		rpc: &Http,
		sid: Uuid,
		method: Method,
		args: &[&str],
	) -> Result<DbResult, TypesError> {
		rpc.handle(Session::owner(), None, Some(sid), method, params(args)).await
	}

	/// A request on attached session `sid` from an anonymous caller.
	async fn anonymous_call(
		rpc: &Http,
		sid: Uuid,
		method: Method,
		args: &[&str],
	) -> Result<DbResult, TypesError> {
		rpc.handle(Session::default(), None, Some(sid), method, params(args)).await
	}

	/// Store attached session `sid`, bound to the root owner as a `signin`
	/// would leave it, on namespace and database `app`, expiring as a write
	/// by [`http`] would set it.
	async fn store_owner_session(ds: &Datastore, sid: Uuid) {
		let mut session = Session::owner().with_ns("app").with_db("app");
		session.id = Some(sid);
		ds.persist_rpc_session(sid, &session, TTL + TTL / 2).await.unwrap();
	}

	/// Store attached session `sid` as `attach` leaves it: anonymous.
	async fn store_anonymous_session(ds: &Datastore, sid: Uuid, ns: &str, db: &str) {
		let mut session = Session::default().with_ns(ns).with_db(db);
		session.id = Some(sid);
		ds.persist_rpc_session(sid, &session, TTL + TTL / 2).await.unwrap();
	}

	/// A principal for a database user of `ns`/`db`.
	fn database_user(ns: &str, db: &str) -> Arc<Auth> {
		Arc::new(Auth::new(Actor::new(
			"alice".to_owned(),
			vec![Role::Viewer],
			Level::Database(ns.to_owned(), db.to_owned()),
		)))
	}

	/// A principal for a namespace user of `ns`, which may select any database
	/// in it.
	fn namespace_user(ns: &str) -> Arc<Auth> {
		Arc::new(Auth::new(Actor::new(
			"bob".to_owned(),
			vec![Role::Viewer],
			Level::Namespace(ns.to_owned()),
		)))
	}

	/// `session` as a `refresh` leaves it: the same identity with a new token
	/// and a `CONTEXT` payload recomputed as `context`.
	fn renewed(session: &Session, context: &str) -> Session {
		let mut renewed = session.clone();
		renewed.tk = Some(Value::String(format!("token for {context}")));
		renewed.data =
			Some(surrealdb_core::dbs::SessionData::new(Value::String(context.to_owned())));
		renewed
	}

	async fn stored(ds: &Datastore, sid: Uuid) -> Option<(Session, StoredRpcSession)> {
		ds.load_rpc_session(sid).await.unwrap()
	}

	#[tokio::test]
	async fn an_attached_session_is_served_by_every_instance() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let (node_a, node_b) = (http(&ds), http(&ds));

		// Attached through one node, changed through another, read back
		// through the first: each request runs on the stored session.
		anonymous_call(&node_a, sid, Method::Attach, &[]).await.unwrap();
		store_owner_session(&ds, sid).await;
		owner_call(&node_b, sid, Method::Use, &["app", "other"]).await.unwrap();
		owner_call(&node_a, sid, Method::Set, &["greeting", "hello"]).await.unwrap();

		let (session, _) = stored(&ds, sid).await.expect("the session is stored");
		assert!(session.au.is_root(), "the bound principal is kept");
		assert_eq!(session.db.as_deref(), Some("other"), "node B's `use` is kept");
		assert_eq!(
			session.variables.get("greeting"),
			Some(&Value::String("hello".to_owned())),
			"node A ran on node B's change and stored its own"
		);

		// A third instance — a restart — serves it too.
		owner_call(&http(&ds), sid, Method::Query, &["RETURN 1"]).await.unwrap();
	}

	#[tokio::test]
	async fn nothing_is_kept_in_memory_between_requests() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		store_owner_session(&ds, sid).await;
		owner_call(&rpc, sid, Method::Use, &["app", "app"]).await.unwrap();
		owner_call(&rpc, sid, Method::Query, &["RETURN 1"]).await.unwrap();
		rpc.handle(Session::owner(), None, None, Method::Query, params(&["RETURN 1"]))
			.await
			.unwrap();
		// A failing request cleans up as well.
		assert!(owner_call(&rpc, sid, Method::Query, &[]).await.is_err());
		assert!(rpc.session_map().is_empty(), "request-private sessions must not outlive requests");
	}

	#[tokio::test]
	async fn a_dropped_request_removes_its_private_session() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		store_owner_session(&ds, sid).await;
		// The client disconnects mid-query: the request future is dropped
		// while its private session is registered.
		let slow = owner_call(&rpc, sid, Method::Query, &["SLEEP 10s"]);
		assert!(tokio::time::timeout(Duration::from_millis(50), slow).await.is_err());
		let slow = rpc.handle(Session::owner(), None, None, Method::Query, params(&["SLEEP 10s"]));
		assert!(tokio::time::timeout(Duration::from_millis(50), slow).await.is_err());
		assert!(rpc.session_map().is_empty(), "a dropped request must not leak its session");
	}

	#[tokio::test]
	async fn concurrent_requests_on_one_session_are_not_serialized() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		store_owner_session(&ds, sid).await;
		// One request holds the session for a long query; another on the same
		// session — the shared-client pattern — must not wait behind it.
		let slow = owner_call(&rpc, sid, Method::Query, &["SLEEP 5s"]);
		let fast = async {
			tokio::time::sleep(Duration::from_millis(50)).await;
			tokio::time::timeout(
				Duration::from_secs(2),
				owner_call(&rpc, sid, Method::Query, &["RETURN 1"]),
			)
			.await
		};
		tokio::select! {
			_ = slow => panic!("the slow query finished first"),
			fast = fast => {
				fast.expect("a request must not wait behind another on its session").unwrap();
			}
		}
	}

	#[tokio::test]
	async fn attach_and_detach_need_a_session_id() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		assert!(
			rpc.handle(Session::owner(), None, None, Method::Attach, Array::new()).await.is_err()
		);
		assert!(
			rpc.handle(Session::owner(), None, None, Method::Detach, Array::new()).await.is_err()
		);
		assert_eq!(rpc.slots().used(), 0);
	}

	#[tokio::test]
	async fn the_ownership_gate_applies_to_stored_sessions() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);

		// An anonymous caller is refused, indistinguishably from a missing
		// session, and changes nothing; the owning principal passes.
		let refused = anonymous_call(&rpc, sid, Method::Use, &["app", "other"]).await;
		assert!(refused.is_err());
		assert!(anonymous_call(&rpc, sid, Method::Detach, &[]).await.is_err());
		let (session, _) = stored(&ds, sid).await.expect("a refused caller must not detach");
		assert_eq!(session.db.as_deref(), Some("app"));
		owner_call(&rpc, sid, Method::Use, &["app", "other"]).await.unwrap();

		// A different principal at the same level is refused too.
		let mut other = Session::owner();
		other.au = Arc::new(Auth::new(Actor::new(
			"someone-else".to_owned(),
			vec![Role::Owner],
			Level::Root,
		)));
		let refused =
			rpc.handle(other, None, Some(sid), Method::Query, params(&["RETURN 1"])).await;
		assert!(refused.is_err());
	}

	#[tokio::test]
	async fn an_unknown_or_expired_session_is_not_found() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		assert!(owner_call(&rpc, Uuid::new_v4(), Method::Query, &["RETURN 1"]).await.is_err());

		let sid = Uuid::new_v4();
		let mut session = Session::owner();
		session.id = Some(sid);
		ds.persist_rpc_session(sid, &session, Duration::from_millis(1)).await.unwrap();
		tokio::time::sleep(Duration::from_millis(5)).await;
		assert!(owner_call(&rpc, sid, Method::Query, &["RETURN 1"]).await.is_err());
		assert!(stored(&ds, sid).await.is_none(), "the expired copy is dropped");
	}

	#[tokio::test]
	async fn detach_removes_the_session_everywhere() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let (node_a, node_b) = (http(&ds), http(&ds));
		anonymous_call(&node_a, sid, Method::Attach, &[]).await.unwrap();
		store_owner_session(&ds, sid).await;
		owner_call(&node_a, sid, Method::Query, &["RETURN 1"]).await.unwrap();

		owner_call(&node_b, sid, Method::Detach, &[]).await.unwrap();
		assert!(stored(&ds, sid).await.is_none());
		assert!(owner_call(&node_a, sid, Method::Query, &["RETURN 1"]).await.is_err());
		// The id is free to attach again.
		anonymous_call(&node_a, sid, Method::Attach, &[]).await.unwrap();
	}

	#[tokio::test]
	async fn attach_never_overwrites_an_existing_session() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		// Attached through another node: this one reports it exists rather
		// than replacing the authenticated session with an anonymous one.
		assert!(anonymous_call(&http(&ds), sid, Method::Attach, &[]).await.is_err());
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert!(session.au.is_root());
	}

	#[tokio::test]
	async fn a_full_node_reclaims_slots_of_sessions_that_ended_elsewhere() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		fill_slots_with_ended_sessions(&rpc, *HTTP_MAX_ATTACHED_SESSIONS);

		// Within the check interval a full node refuses without re-reading
		// storage.
		*rpc.attached_checked.lock().await = Some(Instant::now());
		assert!(anonymous_call(&rpc, Uuid::new_v4(), Method::Attach, &[]).await.is_err());
		assert_eq!(rpc.slots().used(), *HTTP_MAX_ATTACHED_SESSIONS);

		// Once it may check again, it finds those sessions gone and attaches.
		*rpc.attached_checked.lock().await = None;
		let sid = Uuid::new_v4();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		assert_eq!(rpc.slots().used(), 1, "only the new session holds a slot");
		assert!(rpc.slots().attached.contains_key(&sid));
	}

	#[tokio::test]
	async fn a_live_session_keeps_its_slot() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let (live, gone) = (Uuid::new_v4(), Uuid::new_v4());
		anonymous_call(&rpc, live, Method::Attach, &[]).await.unwrap();
		anonymous_call(&rpc, gone, Method::Attach, &[]).await.unwrap();
		ds.delete_rpc_session(gone).await.unwrap();
		rpc.reclaim_attach_slots().await;
		assert!(rpc.slots().attached.contains_key(&live));
		assert!(!rpc.slots().attached.contains_key(&gone));
	}

	/// Take `count` of `rpc`'s attach slots with sessions that are no longer
	/// stored, as when each was detached through another node.
	fn fill_slots_with_ended_sessions(rpc: &Http, count: usize) {
		let mut slots = rpc.slots();
		for _ in 0..count {
			let holder = slots.next;
			slots.next += 1;
			slots.attached.insert(Uuid::new_v4(), holder);
		}
	}

	#[tokio::test]
	async fn an_attach_in_flight_holds_its_slot() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		fill_slots_with_ended_sessions(&rpc, *HTTP_MAX_ATTACHED_SESSIONS - 1);
		*rpc.attached_checked.lock().await = Some(Instant::now());

		// An attach whose create is still in flight holds the last slot, so a
		// concurrent attach is refused rather than overshooting the cap.
		let in_flight = rpc.try_reserve_attach_slot(Uuid::new_v4()).expect("one slot is free");
		assert!(anonymous_call(&rpc, Uuid::new_v4(), Method::Attach, &[]).await.is_err());
		assert_eq!(rpc.slots().used(), *HTTP_MAX_ATTACHED_SESSIONS);

		// An attach that created nothing frees its slot.
		in_flight.release();
		let sid = Uuid::new_v4();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		assert_eq!(rpc.slots().used(), *HTTP_MAX_ATTACHED_SESSIONS);
		assert!(rpc.slots().attached.contains_key(&sid));
	}

	#[tokio::test]
	async fn an_attach_that_fails_frees_its_slot() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		assert!(anonymous_call(&rpc, sid, Method::Attach, &[]).await.is_err());
		assert_eq!(rpc.slots().used(), 0);
	}

	#[tokio::test]
	async fn an_abandoned_attach_keeps_its_slot() {
		// Its create may have committed, so the slot stays taken until a check
		// against storage finds the session absent.
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		drop(rpc.try_reserve_attach_slot(sid).unwrap());
		let slots = rpc.slots();
		assert_eq!(slots.reserved, 0);
		assert!(slots.attached.contains_key(&sid));
	}

	#[tokio::test]
	async fn a_slot_taken_again_meanwhile_is_kept() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		let holder = rpc.attach_slot_holder(sid);

		// Detached through another node, then attached here again before this
		// node frees the slot it saw the session gone from.
		ds.delete_rpc_session(sid).await.unwrap();
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		rpc.release_attach_slot(sid, holder);
		assert!(rpc.slots().attached.contains_key(&sid));
		assert_eq!(rpc.slots().used(), 1);
	}

	#[tokio::test]
	async fn concurrent_changes_on_two_nodes_both_survive() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);

		// This request loaded the session and set `a`; meanwhile another
		// request set `b` and switched database.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let mut theirs = before.clone().with_db("theirs");
		theirs.variables.insert("b", Value::String("b".to_owned()));
		ds.replace_rpc_session(sid, &loaded, &theirs, TTL).await.unwrap().unwrap();
		let mut ours = before.clone();
		ours.variables.insert("a", Value::String("a".to_owned()));

		rpc.store_changes(sid, &Session::owner().au, loaded, before, ours).await.unwrap();
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert_eq!(session.variables.get("a"), Some(&Value::String("a".to_owned())));
		assert_eq!(session.variables.get("b"), Some(&Value::String("b".to_owned())));
		assert_eq!(
			session.db.as_deref(),
			Some("theirs"),
			"a change this request did not make is kept"
		);
	}

	#[tokio::test]
	async fn a_request_never_undoes_a_concurrent_invalidate() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);

		// This request loaded the authenticated session and set a variable;
		// meanwhile the client logged out through another node.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		owner_call(&http(&ds), sid, Method::Invalidate, &[]).await.unwrap();
		let mut ours = before.clone();
		ours.variables.insert("a", Value::String("a".to_owned()));

		// The variable was set under the principal the logout removed, so the
		// request fails rather than apply it to the logged-out session.
		let refused = rpc.store_changes(sid, &Session::owner().au, loaded, before, ours).await;
		assert!(matches!(refused, Err(NotStored::Refused)), "{refused:?}");
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert!(!session.au.is_root(), "the logout must survive a stale request's write");
		assert_eq!(session.variables.get("a"), None);
	}

	/// Records the method and outcome of every RPC completion it observes.
	#[derive(Default)]
	struct Completions(std::sync::Mutex<Vec<(Method, surrealdb_observe::Outcome)>>);

	impl surrealdb_observe::ExecutionObserver for Completions {
		fn on_rpc_complete(&self, event: &surrealdb_observe::RpcEvent) {
			self.0.lock().unwrap().push((event.safe.method, event.safe.outcome));
		}
	}

	#[tokio::test]
	async fn a_change_that_could_not_be_stored_is_observed_as_failed() {
		let observer = Arc::new(Completions::default());
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_observer(Arc::clone(&observer) as Arc<dyn surrealdb_observe::ExecutionObserver>)
			.build_with_path("memory")
			.await
			.unwrap();
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);

		// The request sets a variable and waits for its turn to store it;
		// meanwhile the client logs out through another node, so the change is
		// refused after the method itself succeeded.
		let turn = rpc.session_write_lock(sid).lock().await;
		let request =
			rpc.handle(Session::owner(), None, Some(sid), Method::Set, params(&["a", "1"]));
		tokio::pin!(request);
		let waiting = tokio::time::timeout(Duration::from_millis(200), &mut request).await;
		assert!(waiting.is_err(), "the request waits for its turn to store");
		owner_call(&http(&ds), sid, Method::Invalidate, &[]).await.unwrap();
		drop(turn);
		assert!(request.await.is_err());

		let completions = observer.0.lock().unwrap().clone();
		assert_eq!(
			completions,
			[
				(Method::Invalidate, surrealdb_observe::Outcome::Success),
				(Method::Set, surrealdb_observe::Outcome::Error)
			],
			"the request is observed as the failure the client received"
		);
	}

	#[tokio::test]
	#[should_panic(expected = "the durable session TTL must be at least one millisecond")]
	async fn a_session_ttl_under_one_millisecond_is_refused() {
		Http::new(mem_ds().await, Duration::from_micros(999));
	}

	#[tokio::test]
	async fn the_query_timeout_bounds_storing_a_change() {
		let timeout = Duration::from_millis(200);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_query_timeout(Some(timeout))
			.build_with_path("memory")
			.await
			.unwrap();
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);

		// The store waits for a turn this test never gives up, so only the
		// query timeout ends the request.
		let _turn = rpc.session_write_lock(sid).lock().await;
		let request =
			rpc.handle(Session::owner(), None, Some(sid), Method::Set, params(&["a", "1"]));
		let answer = tokio::time::timeout(Duration::from_secs(10), request)
			.await
			.expect("the query timeout must end the request");
		let Err(err) = answer else {
			panic!("a store cut short must fail the request");
		};
		assert_eq!(err.to_string(), query_timeout_error(timeout).to_string());
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert_eq!(session.variables.get("a"), None);
	}

	#[tokio::test]
	async fn a_change_made_under_a_replaced_principal_is_refused() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_anonymous_session(&ds, sid, "ns1", "db1").await;
		let rpc = http(&ds);

		// This request loaded the anonymous session and ran `use ns2 db2`;
		// meanwhile the session was signed in as a database user of ns1/db1.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let mut theirs = before.clone();
		theirs.au = database_user("ns1", "db1");
		ds.replace_rpc_session(sid, &loaded, &theirs, TTL).await.unwrap().unwrap();
		let ours = before.clone().with_ns("ns2").with_db("db2");

		// Refused for the anonymous caller, which no longer passes the gate,
		// and for the new principal too: the `use` was approved for the
		// anonymous session, not for it.
		for caller in [Session::default().au, database_user("ns1", "db1")] {
			let refused =
				rpc.store_changes(sid, &caller, loaded.clone(), before.clone(), ours.clone()).await;
			assert!(matches!(refused, Err(NotStored::Refused)), "{refused:?}");
			let (session, _) = stored(&ds, sid).await.unwrap();
			assert_eq!(session.au, database_user("ns1", "db1"));
			assert_eq!(
				(session.ns.as_deref(), session.db.as_deref()),
				(Some("ns1"), Some("db1")),
				"a principal is never paired with a selection approved for another"
			);
		}
	}

	#[tokio::test]
	async fn a_renewal_and_a_use_meanwhile_both_survive() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let mut session = Session::default().with_ns("ns1").with_db("db1");
		session.id = Some(sid);
		session.au = namespace_user("ns1");
		ds.persist_rpc_session(sid, &session, TTL + TTL / 2).await.unwrap();
		let rpc = http(&ds);

		// This request renewed the session, recomputing CONTEXT; meanwhile a
		// `use` under the same identity switched its database.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let theirs = before.clone().with_db("db2");
		ds.replace_rpc_session(sid, &loaded, &theirs, TTL).await.unwrap().unwrap();
		let ours = renewed(&before, "first");
		rpc.store_changes(sid, &before.au, loaded, before.clone(), ours.clone()).await.unwrap();
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert_eq!(
			session.db.as_deref(),
			Some("db2"),
			"a renewal keeps a selection made meanwhile"
		);
		assert_eq!((session.tk, session.data), (ours.tk, ours.data));

		// This request ran a `use`; meanwhile a renewal recomputed CONTEXT.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let theirs = renewed(&before, "second");
		ds.replace_rpc_session(sid, &loaded, &theirs, TTL).await.unwrap().unwrap();
		let ours = before.clone().with_db("db3");
		let caller = Arc::clone(&before.au);
		rpc.store_changes(sid, &caller, loaded, before, ours).await.unwrap();
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert_eq!(session.db.as_deref(), Some("db3"), "a renewal does not refuse a `use`");
		assert_eq!((session.tk, session.data), (theirs.tk, theirs.data));
	}

	#[test]
	fn only_a_new_token_for_the_same_principal_is_a_renewal() {
		let mut before = Session::default().with_ns("ns1").with_db("db1");
		before.au = database_user("ns1", "db1");
		let mut renewed = before.clone();
		renewed.tk = Some(Value::String("renewed".to_owned()));
		assert!(renews_authentication(&before, &renewed));
		assert!(!renews_authentication(&before, &before), "an unchanged session renews nothing");
		let mut recomputed = renewed.clone();
		recomputed.data =
			Some(surrealdb_core::dbs::SessionData::new(Value::String("context".to_owned())));
		assert!(renews_authentication(&before, &recomputed), "a refresh recomputes CONTEXT");
		let mut signed_in = renewed.clone();
		signed_in.au = database_user("ns1", "db2");
		assert!(!renews_authentication(&before, &signed_in), "another principal is a sign-in");
		let moved = renewed.with_db("db2");
		assert!(!renews_authentication(&before, &moved), "a selection is more than a renewal");
	}

	/// Settle `ours`, a change `method` made to session `sid` as loaded
	/// (`before`, stored as `loaded`), as `rpc` does once the method has run.
	async fn settle_change(
		rpc: &Http,
		sid: Uuid,
		method: Method,
		loaded: &StoredRpcSession,
		before: &Session,
		ours: Session,
	) -> Result<(), TypesError> {
		let caller = Arc::clone(&before.au);
		let private = rpc.register_private_session(Uuid::new_v4(), ours);
		rpc.settle(sid, &caller, loaded.clone(), before.clone(), method, &private, Instant::now())
			.await
	}

	#[tokio::test]
	async fn a_refresh_refused_meanwhile_still_succeeds() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let mut session = Session::default().with_ns("ns1").with_db("db1");
		session.id = Some(sid);
		session.au = database_user("ns1", "db1");
		ds.persist_rpc_session(sid, &session, TTL + TTL / 2).await.unwrap();
		let rpc = http(&ds);

		// This request ran a `refresh`, spending its refresh token for the new
		// token pair it returns. A change meanwhile left no stored copy holding
		// the authentication it replaced, so it succeeds without being stored.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let ours = renewed(&before, "narrowed");

		// Signed out meanwhile: the sign-out stays.
		let mut theirs = before.clone();
		theirs.au = Session::default().au;
		ds.replace_rpc_session(sid, &loaded, &theirs, TTL).await.unwrap().unwrap();
		settle_change(&rpc, sid, Method::Refresh, &loaded, &before, ours.clone()).await.unwrap();
		assert_eq!(stored(&ds, sid).await.unwrap().0.au, Session::default().au);

		// Detached meanwhile: nothing is recreated, while a `use` is still
		// refused.
		ds.delete_rpc_session(sid).await.unwrap();
		settle_change(&rpc, sid, Method::Refresh, &loaded, &before, ours).await.unwrap();
		let moved = before.clone().with_db("db2");
		assert!(settle_change(&rpc, sid, Method::Use, &loaded, &before, moved).await.is_err());
		assert!(stored(&ds, sid).await.is_none());
	}

	#[tokio::test]
	async fn an_authenticate_that_renews_fails_like_any_change() {
		let timeout = Duration::from_millis(200);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_query_timeout(Some(timeout))
			.build_with_path("memory")
			.await
			.unwrap();
		let sid = Uuid::new_v4();
		let mut session = Session::default().with_ns("ns1").with_db("db1");
		session.id = Some(sid);
		session.au = database_user("ns1", "db1");
		ds.persist_rpc_session(sid, &session, TTL + TTL / 2).await.unwrap();
		let rpc = http(&ds);

		// An `authenticate` with another token for the same identity leaves the
		// session as a `refresh` would, but spent nothing the client cannot send
		// again: its store cut short by the query timeout fails it, and the
		// session is kept.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let ours = renewed(&before, "authenticated");
		{
			let _turn = rpc.session_write_lock(sid).lock().await;
			let failed =
				settle_change(&rpc, sid, Method::Authenticate, &loaded, &before, ours.clone())
					.await;
			let Err(err) = failed else {
				panic!("a store cut short must fail an `authenticate`");
			};
			assert_eq!(err.to_string(), query_timeout_error(timeout).to_string());
		}
		assert_eq!(stored(&ds, sid).await.unwrap().0, before, "the session is kept as it was");

		// Detached meanwhile: refused, and nothing is recreated.
		ds.delete_rpc_session(sid).await.unwrap();
		let refused = settle_change(&rpc, sid, Method::Authenticate, &loaded, &before, ours).await;
		assert!(refused.is_err(), "a refusal must fail an `authenticate`");
		assert!(stored(&ds, sid).await.is_none());
	}

	#[tokio::test]
	async fn the_query_timeout_never_withholds_issued_credentials() {
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_query_timeout(Some(Duration::from_millis(200)))
			.build_with_path("memory")
			.await
			.unwrap();
		let sid = Uuid::new_v4();
		store_anonymous_session(&ds, sid, "ns1", "db1").await;
		let rpc = http(&ds);

		// This request signed in, issuing credentials; its store waits past
		// the query timeout, and still lands once it gets its turn.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let mut signed_in = before.clone();
		signed_in.au = database_user("ns1", "db1");
		let turn = rpc.session_write_lock(sid).lock().await;
		let settle = settle_change(&rpc, sid, Method::Signin, &loaded, &before, signed_in);
		tokio::pin!(settle);
		let waiting = tokio::time::timeout(Duration::from_millis(400), &mut settle).await;
		assert!(waiting.is_err(), "the store waits past the query timeout");
		drop(turn);
		settle.await.unwrap();
		assert_eq!(stored(&ds, sid).await.unwrap().0.au, database_user("ns1", "db1"));

		// This request then ran a `refresh`, spending its refresh token for the
		// token pair it returns; its store lands the same way, so the session
		// is never left holding the authentication the renewal replaced.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let ours = renewed(&before, "narrowed");
		let turn = rpc.session_write_lock(sid).lock().await;
		let settle = settle_change(&rpc, sid, Method::Refresh, &loaded, &before, ours.clone());
		tokio::pin!(settle);
		let waiting = tokio::time::timeout(Duration::from_millis(400), &mut settle).await;
		assert!(waiting.is_err(), "the store waits past the query timeout");
		drop(turn);
		settle.await.unwrap();
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert_eq!((session.tk, session.data), (ours.tk, ours.data));
	}

	#[tokio::test]
	async fn a_sign_in_keeps_its_own_selection() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_anonymous_session(&ds, sid, "ns1", "db1").await;
		let rpc = http(&ds);

		// This request signed in as a database user of ns1/db1; meanwhile an
		// anonymous `use ns2 db2` was stored.
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		let theirs = before.clone().with_ns("ns2").with_db("db2");
		ds.replace_rpc_session(sid, &loaded, &theirs, TTL).await.unwrap().unwrap();
		let mut ours = before.clone();
		ours.au = database_user("ns1", "db1");

		rpc.store_changes(sid, &Session::default().au, loaded, before, ours).await.unwrap();
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert_eq!(session.au, database_user("ns1", "db1"));
		assert_eq!((session.ns.as_deref(), session.db.as_deref()), (Some("ns1"), Some("db1")));
	}

	#[tokio::test]
	async fn concurrent_changes_on_one_node_all_land() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);
		// The shared-client pattern: many concurrent `set`s on one session.
		let names: Vec<String> = (0..32).map(|i| format!("v{i}")).collect();
		let sets = names.iter().map(|name| {
			rpc.handle(
				Session::owner(),
				None,
				Some(sid),
				Method::Set,
				params(&[name.as_str(), "1"]),
			)
		});
		for result in futures::future::join_all(sets).await {
			result.unwrap();
		}
		let (session, _) = stored(&ds, sid).await.unwrap();
		for name in &names {
			assert!(session.variables.get(name).is_some(), "${name} was lost");
		}
	}

	#[tokio::test]
	async fn a_change_to_a_session_removed_meanwhile_is_refused() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);
		let (before, loaded) = stored(&ds, sid).await.unwrap();
		ds.delete_rpc_session(sid).await.unwrap();
		let ours = before.clone().with_db("other");
		let refused = rpc.store_changes(sid, &Session::owner().au, loaded, before, ours).await;
		assert!(matches!(refused, Err(NotStored::Refused)), "{refused:?}");
		assert!(stored(&ds, sid).await.is_none(), "a removed session must not be recreated");
	}

	#[tokio::test]
	async fn every_write_sets_the_expiry_beyond_one_ttl() {
		let ds = mem_ds().await;
		let rpc = http(&ds);
		let sid = Uuid::new_v4();
		let floor = now_ms() + TTL.as_millis() as u64;
		anonymous_call(&rpc, sid, Method::Attach, &[]).await.unwrap();
		let (_, attached) = stored(&ds, sid).await.unwrap();
		assert!(attached.expires_at() > floor);
		anonymous_call(&rpc, sid, Method::Set, &["a", "1"]).await.unwrap();
		let (_, changed) = stored(&ds, sid).await.unwrap();
		assert!(changed.expires_at() > floor);
	}

	#[tokio::test]
	async fn refresh_slides_the_ttl_only_when_it_runs_low() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		store_owner_session(&ds, sid).await;
		let rpc = http(&ds);

		// More than a TTL left: no write.
		let (_, fresh) = stored(&ds, sid).await.unwrap();
		rpc.refresh(sid, fresh.clone()).await;
		let (_, after) = stored(&ds, sid).await.unwrap();
		assert_eq!(after.expires_at(), fresh.expires_at());

		// Under a TTL left: the expiry is pushed out beyond a full TTL.
		let mut session = Session::owner().with_ns("app").with_db("app");
		session.id = Some(sid);
		ds.persist_rpc_session(sid, &session, TTL / 2).await.unwrap();
		let (_, low) = stored(&ds, sid).await.unwrap();
		let floor = now_ms() + TTL.as_millis() as u64;
		rpc.refresh(sid, low.clone()).await;
		let (_, after) = stored(&ds, sid).await.unwrap();
		assert!(after.expires_at() > floor, "{} <= {floor}", after.expires_at());
	}

	#[tokio::test]
	async fn refresh_never_writes_back_a_stale_copy() {
		let ds = mem_ds().await;
		let sid = Uuid::new_v4();
		let mut session = Session::owner().with_ns("app").with_db("app");
		session.id = Some(sid);
		ds.persist_rpc_session(sid, &session, TTL / 2).await.unwrap();
		let rpc = http(&ds);

		// This request loaded the session with its TTL running low; another
		// request then logged the client out.
		let (_, loaded) = stored(&ds, sid).await.unwrap();
		owner_call(&http(&ds), sid, Method::Invalidate, &[]).await.unwrap();

		rpc.refresh(sid, loaded).await;
		let (session, _) = stored(&ds, sid).await.unwrap();
		assert!(!session.au.is_root(), "a refresh must not restore the pre-logout session");
	}

	fn variables(pairs: &[(&str, &str)]) -> surrealdb_types::Variables {
		let mut variables = surrealdb_types::Variables::default();
		for (name, value) in pairs {
			variables.insert(*name, Value::String((*value).to_owned()));
		}
		variables
	}

	#[test]
	fn merge_moves_authentication_as_one() {
		let before = Session::owner().with_ns("app").with_db("app");
		// Ours signed in as someone else; theirs changed only the token claims.
		let mut ours = before.clone();
		ours.au = Arc::new(Auth::for_root(Role::Viewer));
		ours.exp = Some(1);
		let mut theirs = before.clone();
		theirs.tk = Some(Value::String("their-token".to_owned()));
		let merged = merge_session_changes(&before, ours.clone(), theirs);
		assert_eq!(merged.au, ours.au);
		assert_eq!(merged.exp, Some(1));
		assert_eq!(merged.tk, None, "a principal is never assembled from two sign-ins");

		// Ours left authentication alone; theirs cleared it.
		let mut ours = before.clone();
		ours.variables = variables(&[("a", "1")]);
		let theirs = Session {
			ns: before.ns.clone(),
			db: before.db.clone(),
			..Session::default()
		};
		let merged = merge_session_changes(&before, ours, theirs);
		assert!(!merged.au.is_root());
	}

	#[test]
	fn merge_moves_the_selection_as_one() {
		let before = Session::owner().with_ns("app").with_db("app");
		let ours = before.clone().with_db("ours");
		let theirs = before.clone().with_ns("theirs").with_db("theirs");
		let merged = merge_session_changes(&before, ours, theirs);
		assert_eq!((merged.ns.as_deref(), merged.db.as_deref()), (Some("app"), Some("ours")));

		let ours = before.clone();
		let theirs = before.clone().with_ns("theirs").with_db("theirs");
		let merged = merge_session_changes(&before, ours, theirs);
		assert_eq!((merged.ns.as_deref(), merged.db.as_deref()), (Some("theirs"), Some("theirs")));
	}

	#[test]
	fn merge_keeps_a_sign_ins_selection() {
		// Ours signed in, which left the selection as it was; theirs switched
		// it under the principal ours replaced.
		let before = Session::default().with_ns("ns1").with_db("db1");
		let mut ours = before.clone();
		ours.au = database_user("ns1", "db1");
		let theirs = before.clone().with_ns("ns2").with_db("db2");
		let merged = merge_session_changes(&before, ours, theirs);
		assert_eq!((merged.ns.as_deref(), merged.db.as_deref()), (Some("ns1"), Some("db1")));
	}

	#[test]
	fn merge_keeps_a_selection_made_during_a_renewal() {
		// Ours renewed the authentication, recomputing CONTEXT, which keeps the
		// identity; theirs switched the database under that identity.
		let mut before = Session::default().with_ns("ns1").with_db("db1");
		before.au = namespace_user("ns1");
		let ours = renewed(&before, "recomputed");
		let theirs = before.clone().with_db("db2");
		let merged = merge_session_changes(&before, ours.clone(), theirs);
		assert_eq!((merged.ns.as_deref(), merged.db.as_deref()), (Some("ns1"), Some("db2")));
		assert_eq!((merged.tk, merged.data), (ours.tk, ours.data));
	}

	#[test]
	fn merge_combines_variables_per_name() {
		let mut before = Session::owner();
		before.variables = variables(&[("kept", "0"), ("ours_unset", "0"), ("theirs_unset", "0")]);
		let mut ours = before.clone();
		ours.variables = variables(&[("kept", "0"), ("theirs_unset", "0"), ("ours_new", "1")]);
		let mut theirs = before.clone();
		theirs.variables = variables(&[("kept", "0"), ("ours_unset", "0"), ("theirs_new", "2")]);
		let merged = merge_session_changes(&before, ours, theirs);
		assert_eq!(
			merged.variables,
			variables(&[("kept", "0"), ("ours_new", "1"), ("theirs_new", "2")])
		);
	}
}
