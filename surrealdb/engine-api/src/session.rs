//! The registry an engine keeps its sessions in.
//!
//! Session lifecycle travels on a [`SessionId`] channel of its own, separate
//! from the requests that run under those sessions, and the SDK guarantees only
//! that it announces a session before handing out the handle that uses it. A
//! request can therefore reach an engine before the event registering its
//! session has been applied.
//!
//! That makes a lookup which finds nothing ambiguous: the session may be one
//! whose registration has not been applied *yet*, which is worth waiting for, or
//! one that has *ended*, which is not. Waiting on the second never returns, so
//! the registry remembers which sessions it has seen end, and answers for them
//! rather than parking a request on a registration that has already come and
//! gone.
//!
//! The payload is whatever the engine establishes for a session: the embedded
//! engine publishes the state it keeps locally, and the gRPC engine publishes
//! the identity the server minted for it.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use uuid::Uuid;

use crate::SessionError;

/// How a session turned out: the payload the engine established for it, or why
/// it never was.
///
/// The failure is the engine's own error type where it has one, so that what
/// stopped a session being established survives to the request that goes on to
/// need it -- a connection failure has to still read as one, or a caller cannot
/// tell that reconnecting is what to do. The registry's own failures convert
/// into it.
pub type Established<T, E = SessionError> = Result<T, E>;

/// How many ended sessions are remembered, so that a request naming one is told
/// it has gone instead of waiting for it.
///
/// A request can only outlive its session by having been made from something
/// that held a handle and then released it, so the window this has to cover is
/// one request, not one session's lifetime. The bound is what keeps a
/// connection that clones a handle per request from accumulating one record per
/// clone for ever.
const REMEMBERED_ENDED_SESSIONS: usize = 1024;

/// One session's payload, and the signal that it has been established.
pub struct SessionEntry<T, E = SessionError> {
	ready: Notify,
	outcome: Mutex<Option<Established<T, E>>>,
}

impl<T, E> Default for SessionEntry<T, E> {
	fn default() -> Self {
		Self {
			ready: Notify::default(),
			outcome: Mutex::new(None),
		}
	}
}

impl<T: Clone, E: Clone> SessionEntry<T, E> {
	/// Waits until this session has been established, one way or the other.
	///
	/// Follows [`Notify`]'s race-free pattern: the notified future is created
	/// *before* the readiness check, so a `notify_waiters()` landing between the
	/// check and the await is not missed. The loop covers a spurious wake-up,
	/// which would otherwise return before the session existed.
	pub async fn wait_ready(&self) -> Established<T, E> {
		loop {
			let notified = self.ready.notified();
			if let Some(outcome) = self.outcome() {
				return outcome;
			}
			notified.await;
		}
	}

	/// How establishing this session turned out, or `None` if it has not been
	/// attempted yet. Unlike [`wait_ready`](Self::wait_ready) this does not wait
	/// for the answer.
	pub fn outcome(&self) -> Option<Established<T, E>> {
		self.guard().clone()
	}

	/// Publishes the outcome of establishing this session.
	///
	/// A failure is published rather than swallowed so that waiters fail with
	/// the reason the session was never established, instead of hanging.
	pub fn publish(&self, outcome: Established<T, E>) {
		*self.guard() = Some(outcome);
		self.ready.notify_waiters();
	}

	fn guard(&self) -> MutexGuard<'_, Option<Established<T, E>>> {
		self.outcome.lock().expect("session registry poisoned")
	}
}

/// The sessions an engine is serving, keyed by the id the SDK gave them.
pub struct SessionRegistry<T, E = SessionError> {
	entries: Mutex<Entries<T, E>>,
	/// Whether any further lifecycle event can still arrive.
	///
	/// Once the SDK has dropped its last handle no event ever will, so a session
	/// that is not registered by then never will be, and a request for one must
	/// fail rather than wait for a registration that cannot come.
	closed: AtomicBool,
}

impl<T, E> Default for SessionRegistry<T, E> {
	fn default() -> Self {
		Self {
			entries: Mutex::new(Entries::default()),
			closed: AtomicBool::new(false),
		}
	}
}

struct Entries<T, E> {
	live: HashMap<Uuid, Arc<SessionEntry<T, E>>>,
	ended: VecDeque<Uuid>,
}

impl<T, E> Default for Entries<T, E> {
	fn default() -> Self {
		Self {
			live: HashMap::new(),
			ended: VecDeque::new(),
		}
	}
}

impl<T, E> Entries<T, E> {
	fn end(&mut self, id: Uuid) -> Option<Arc<SessionEntry<T, E>>> {
		self.ended.push_back(id);
		if self.ended.len() > REMEMBERED_ENDED_SESSIONS {
			self.ended.pop_front();
		}
		self.live.remove(&id)
	}

	fn has_ended(&self, id: Uuid) -> bool {
		self.ended.contains(&id)
	}
}

impl<T: Clone, E: Clone + From<SessionError>> SessionRegistry<T, E> {
	fn map(&self) -> MutexGuard<'_, Entries<T, E>> {
		self.entries.lock().expect("session registry poisoned")
	}

	/// The payload a request runs against, waiting for the session to be
	/// registered if its lifecycle event has not been applied yet.
	pub async fn resolve(&self, id: Uuid) -> Established<T, E> {
		let entry = {
			let mut entries = self.map();
			// Deciding this here, under the same lock the lifecycle events are
			// applied behind, is what keeps a request from waiting on a session
			// that has already ended.
			if !entries.live.contains_key(&id)
				&& (entries.has_ended(id) || self.closed.load(Ordering::Acquire))
			{
				return Err(SessionError::NotFound(id).into());
			}
			Arc::clone(entries.live.entry(id).or_default())
		};
		entry.wait_ready().await
	}

	/// The entry a lifecycle event publishes into, creating it if a request is
	/// not already waiting on one.
	pub fn entry(&self, id: Uuid) -> Arc<SessionEntry<T, E>> {
		Arc::clone(self.map().live.entry(id).or_default())
	}

	/// How a session stands right now, without waiting for one or registering
	/// one.
	///
	/// This is what work that cannot wait resolves against — delivering a
	/// notification, say, which has nowhere to go if its session is gone.
	pub fn established(&self, id: Uuid) -> Option<Established<T, E>> {
		self.map().live.get(&id).cloned()?.outcome()
	}

	/// Ends a session, failing anything waiting on it.
	///
	/// The failure is published before the session is forgotten, so a request
	/// that outlived the handle it was made from fails rather than waiting for a
	/// session that has just gone away.
	pub fn end(&self, id: Uuid) -> Option<Established<T, E>> {
		let entry = self.map().end(id)?;
		let outcome = entry.outcome();
		entry.publish(Err(SessionError::NotFound(id).into()));
		outcome
	}

	/// Fails every session that has not been established, and every one asked
	/// for from now on, because no further lifecycle event can arrive.
	pub fn close(&self) {
		self.closed.store(true, Ordering::Release);
		for (id, entry) in self.map().live.iter() {
			if entry.outcome().is_none() {
				entry.publish(Err(SessionError::NotFound(*id).into()));
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn registry() -> Arc<SessionRegistry<u8, SessionError>> {
		Arc::new(SessionRegistry::default())
	}

	#[tokio::test]
	async fn a_request_waits_for_a_session_that_is_still_being_registered() {
		let sessions = registry();
		let id = Uuid::new_v4();

		let waiter = {
			let sessions = Arc::clone(&sessions);
			tokio::spawn(async move { sessions.resolve(id).await })
		};
		// Let the waiter reach the wait before the session exists.
		tokio::task::yield_now().await;
		sessions.entry(id).publish(Ok(7));

		assert_eq!(
			waiter.await.unwrap().expect("registered"),
			7,
			"a queued registration resolves the request"
		);
	}

	/// Waiting only makes sense while a registration can still arrive. A session
	/// that has ended is not going to be registered again, so a request naming
	/// one has to be told rather than left waiting.
	#[tokio::test]
	async fn a_request_for_an_ended_session_fails() {
		let sessions = registry();
		let id = Uuid::new_v4();

		sessions.entry(id).publish(Ok(7));
		assert_eq!(sessions.resolve(id).await.expect("registered"), 7);

		assert_eq!(
			sessions.end(id).expect("a live session").expect("established"),
			7,
			"ending hands back what was established"
		);
		assert!(matches!(sessions.resolve(id).await, Err(SessionError::NotFound(_))));
	}

	/// A request already parked when the session ends is failed too, rather than
	/// left waiting on an entry nothing will publish into again.
	#[tokio::test]
	async fn ending_a_session_fails_the_request_already_waiting_on_it() {
		let sessions = registry();
		let id = Uuid::new_v4();

		let waiter = {
			let sessions = Arc::clone(&sessions);
			tokio::spawn(async move { sessions.resolve(id).await })
		};
		tokio::task::yield_now().await;
		sessions.end(id);

		assert!(matches!(waiter.await.unwrap(), Err(SessionError::NotFound(_))));
	}

	/// A session that could not be established fails the request that needed it
	/// with the reason it could not, not with a summary of one: a caller
	/// deciding whether to reconnect has to be able to tell a connection failure
	/// from anything else.
	#[tokio::test]
	async fn the_reason_a_session_failed_reaches_the_request() {
		let sessions: Arc<SessionRegistry<u8, surrealdb_types::Error>> =
			Arc::new(SessionRegistry::default());
		let id = Uuid::new_v4();

		sessions.entry(id).publish(Err(surrealdb_types::Error::connection(
			"the server went away".to_string(),
			surrealdb_types::ConnectionError::ConnectionFailed,
		)));

		let error = sessions.resolve(id).await.expect_err("the session was never established");
		assert!(error.is_connection(), "the failure must still read as a connection failure");
	}

	/// Once the SDK has dropped its last handle no lifecycle event can arrive,
	/// so anything still waiting is waiting for good.
	#[tokio::test]
	async fn closing_fails_the_waiting_and_everything_after() {
		let sessions = registry();
		let waiting = Uuid::new_v4();

		let waiter = {
			let sessions = Arc::clone(&sessions);
			tokio::spawn(async move { sessions.resolve(waiting).await })
		};
		tokio::task::yield_now().await;
		sessions.close();

		assert!(matches!(waiter.await.unwrap(), Err(SessionError::NotFound(_))));
		assert!(matches!(sessions.resolve(Uuid::new_v4()).await, Err(SessionError::NotFound(_))));
	}
}
