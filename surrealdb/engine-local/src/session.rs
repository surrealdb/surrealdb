//! Per-session state, and the task that keeps it in step with the SDK.

use std::sync::Arc;

use async_channel::{Receiver, Sender};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_datastore::Transaction;
use surrealdb_engine_api::{SessionError, SessionId, session_error_to_error};
use surrealdb_types::{Error, HashMap, Notification, Variables};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::engine::kill_live_query;
use crate::spawn;

/// Everything one SDK session owns on the engine side.
pub(crate) struct SessionState {
	pub(crate) session: RwLock<Session>,
	pub(crate) vars: RwLock<Variables>,
	pub(crate) transactions: HashMap<Uuid, Arc<Transaction>>,
	/// Transactions whose query was abandoned part-way through.
	///
	/// Held rather than dropped. Some backends -- TiKV among them -- panic when
	/// a live transaction is dropped, so an abandoned one has to be cancelled by
	/// something that can await, which `Drop` cannot. The `commit` or `rollback`
	/// that follows does it.
	pub(crate) abandoned: HashMap<Uuid, Arc<Transaction>>,
	/// This session's live queries, and where each one's notifications go.
	///
	/// A live query is recorded as soon as the statement registering it
	/// succeeds, and gains its subscriber on the `subscribe_live` that follows.
	/// It has to be recorded at the earlier of the two: the datastore has it
	/// from that moment, so a teardown driven off this map would otherwise miss
	/// one caught between the two calls.
	pub(crate) live_queries: HashMap<Uuid, Option<Sender<Result<Notification, Error>>>>,
}

impl SessionState {
	pub(crate) fn new(id: Uuid) -> Self {
		let mut session = Session::default().with_rt(true);
		session.id = Some(id);
		Self {
			session: RwLock::new(session),
			vars: RwLock::new(Variables::default()),
			transactions: HashMap::new(),
			abandoned: HashMap::new(),
			live_queries: HashMap::new(),
		}
	}

	/// A copy of this session, as a freshly cloned SDK handle starts out.
	///
	/// Authentication and variables carry over; transactions and live queries
	/// do not, as both are owned by the handle that opened them.
	async fn cloned(&self, id: Uuid) -> Self {
		let mut session = self.session.read().await.clone();
		session.id = Some(id);
		Self {
			session: RwLock::new(session),
			vars: RwLock::new(self.vars.read().await.clone()),
			transactions: HashMap::new(),
			abandoned: HashMap::new(),
			live_queries: HashMap::new(),
		}
	}
}

/// The sessions this engine is serving, and the state each one owns.
pub(crate) type SessionRegistry = surrealdb_engine_api::SessionRegistry<Arc<SessionState>>;

/// The state a request runs against, waiting for the session to be registered
/// if its lifecycle event has not been applied yet.
pub(crate) async fn resolve(
	sessions: &SessionRegistry,
	id: Uuid,
) -> Result<Arc<SessionState>, Error> {
	sessions.resolve(id).await.map_err(session_error_to_error)
}

trait ApplySession {
	async fn apply(&self, event: SessionId);
}

impl ApplySession for SessionRegistry {
	async fn apply(&self, event: SessionId) {
		match event {
			SessionId::Initial(id) => self.entry(id).publish(Ok(Arc::new(SessionState::new(id)))),
			SessionId::Clone {
				old,
				new,
			} => {
				let outcome = match self.established(old) {
					Some(Ok(state)) => Ok(Arc::new(state.cloned(new).await)),
					Some(Err(error)) => Err(error),
					None => Err(SessionError::NotFound(old)),
				};
				self.entry(new).publish(outcome);
			}
			SessionId::Drop(id) => {
				self.end(id);
			}
		}
	}
}

/// Keeps `sessions` in step with the SDK's, then shuts the datastore down.
///
/// The session channel closing is the engine's stop signal: its senders live in
/// the `Surreal` handles, so it closes exactly when the last one is dropped.
/// Nothing can reach the engine after that -- a request needs a handle to be
/// made from -- so the datastore is shut down and the notification pump is
/// stopped by closing its channel.
pub(crate) async fn run(
	kvs: Arc<Datastore>,
	sessions: Arc<SessionRegistry>,
	session_rx: Receiver<SessionId>,
	notifications: Option<Receiver<Notification>>,
) {
	while let Ok(event) = session_rx.recv().await {
		sessions.apply(event).await;
	}
	// No further session can be established, so anything still waiting for one
	// is waiting for good.
	sessions.close();
	// The notification sender lives in the datastore, which this task holds,
	// so the pump would otherwise wait on a channel that can never close.
	if let Some(notifications) = notifications {
		notifications.close();
	}
	// Stops the datastore's maintenance tasks as well as the storage engine.
	kvs.shutdown().await.ok();
}

/// Delivers live-query notifications to the sessions that subscribed to them.
pub(crate) async fn pump(
	kvs: Arc<Datastore>,
	sessions: Arc<SessionRegistry>,
	notifications: Receiver<Notification>,
) {
	while let Ok(notification) = notifications.recv().await {
		deliver(&kvs, &sessions, notification).await;
	}
}

async fn deliver(kvs: &Arc<Datastore>, sessions: &SessionRegistry, notification: Notification) {
	let Some(session_id) = notification.session.map(|x| x.into_inner()) else {
		return;
	};
	let live_query_id = notification.id.into_inner();

	let state = match sessions.established(session_id) {
		Some(Ok(state)) => state,
		Some(Err(error)) => {
			warn!(
				"Failed to find session '{session_id:?}' for live query '{live_query_id}'; {error:?}"
			);
			return;
		}
		None => {
			let error = session_error_to_error(SessionError::NotFound(session_id));
			warn!(
				"Failed to find session '{session_id:?}' for live query '{live_query_id}'; {error}"
			);
			return;
		}
	};

	let sender = match state.live_queries.get(&live_query_id) {
		Some(Some(sender)) => sender,
		// Registered, but its subscriber has not arrived yet. The notification
		// has nowhere to go, and the subscriber will see everything from the
		// point it does arrive.
		Some(None) => return,
		None => {
			warn!("Failed to find live query '{live_query_id}' for session '{session_id:?}'");
			return;
		}
	};

	let kvs = Arc::clone(kvs);
	// Delivery is spawned so one blocked subscriber cannot hold up the rest.
	//
	// The session is read inside the task, and only where it is needed: an auth
	// operation holds this session's write lock for as long as it runs -- across
	// a remote JWKS fetch, for a JWT access method -- and reading it out here
	// would stall delivery to every *other* session behind that one.
	spawn(async move {
		if sender.send(Ok(notification)).await.is_err() {
			state.live_queries.remove(&live_query_id);
			let vars = state.vars.read().await.clone();
			let session = state.session.read().await.clone();
			if let Err(error) = kill_live_query(&kvs, live_query_id, &session, vars).await {
				warn!("Failed to kill live query '{live_query_id}'; {error}");
			}
		}
	});
}

#[cfg(test)]
mod tests {
	use surrealdb_types::Value;

	use super::*;

	fn registry() -> Arc<SessionRegistry> {
		Arc::new(SessionRegistry::default())
	}

	#[test_log::test(tokio::test)]
	async fn clone_carries_the_original_session_forward() {
		let sessions = registry();
		let old = Uuid::new_v4();
		let new = Uuid::new_v4();

		sessions.apply(SessionId::Initial(old)).await;
		sessions
			.resolve(old)
			.await
			.unwrap()
			.vars
			.write()
			.await
			.insert("a".to_string(), Value::Bool(true));
		sessions
			.apply(SessionId::Clone {
				old,
				new,
			})
			.await;

		let cloned = sessions.resolve(new).await.expect("the clone is registered");
		assert_eq!(cloned.session.read().await.id, Some(new));
		assert_eq!(cloned.vars.read().await.get("a"), Some(&Value::Bool(true)));
	}

	/// Cloning a session that was never registered has to *fail*, not hang:
	/// nothing later will establish it, so a waiter would wait for ever.
	#[test_log::test(tokio::test)]
	async fn clone_of_an_unknown_session_resolves_to_not_found() {
		let sessions = registry();
		let new = Uuid::new_v4();

		sessions
			.apply(SessionId::Clone {
				old: Uuid::new_v4(),
				new,
			})
			.await;

		assert!(sessions.resolve(new).await.is_err());
	}

	/// A request arrives for a session whose registration has not been applied
	/// yet, and must wait for it rather than report the session missing.
	///
	/// This is the cross-channel race the readiness signal exists for: the
	/// registration and the request travel separately, and only the order in
	/// which the SDK sends them is guaranteed.
	#[test_log::test(tokio::test)]
	async fn a_request_waits_for_a_session_that_is_still_being_registered() {
		let sessions = registry();
		let id = Uuid::new_v4();

		let waiter = {
			let sessions = Arc::clone(&sessions);
			tokio::spawn(async move { sessions.resolve(id).await.is_ok() })
		};

		// Let the waiter reach the wait before the session exists.
		tokio::task::yield_now().await;
		sessions.apply(SessionId::Initial(id)).await;

		assert!(waiter.await.unwrap(), "a queued registration must resolve the request");
	}

	/// Waiting only makes sense while a registration can still arrive. Once the
	/// SDK has dropped its last handle none can, so a request for a session that
	/// is not registered by then has to fail rather than wait for ever.
	#[cfg(feature = "kv-mem")]
	#[test_log::test(tokio::test)]
	async fn a_request_fails_once_no_further_session_can_arrive() {
		let kvs = Datastore::new("memory").await.unwrap();
		let sessions = registry();
		let (session_tx, session_rx) = async_channel::unbounded();
		let id = Uuid::new_v4();

		let waiter = {
			let sessions = Arc::clone(&sessions);
			tokio::spawn(async move { sessions.resolve(id).await })
		};
		// Park the waiter on a session nothing is going to register.
		tokio::task::yield_now().await;

		// The SDK's last handle going away is what closes this channel.
		drop(session_tx);
		run(kvs, Arc::clone(&sessions), session_rx, None).await;

		assert!(
			waiter.await.unwrap().is_err(),
			"a parked request must be failed, not left waiting"
		);
		assert!(
			sessions.resolve(Uuid::new_v4()).await.is_err(),
			"a request arriving after the last handle went away must fail immediately"
		);
	}

	/// A request for a session that has been dropped fails, rather than waiting
	/// for a registration that has already come and gone.
	#[test_log::test(tokio::test)]
	async fn a_request_for_a_dropped_session_fails() {
		let sessions = registry();
		let id = Uuid::new_v4();

		sessions.apply(SessionId::Initial(id)).await;
		let parked = {
			let sessions = Arc::clone(&sessions);
			tokio::spawn(async move { sessions.resolve(id).await })
		};
		assert!(parked.await.unwrap().is_ok(), "the session is registered");

		sessions.apply(SessionId::Drop(id)).await;
		assert!(sessions.resolve(id).await.is_err(), "a dropped session must not be waited for");
	}
}
