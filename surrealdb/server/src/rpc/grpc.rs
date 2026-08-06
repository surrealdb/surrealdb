//! The gRPC RPC transport.
//!
//! [`Grpc`] is the third [`RpcProtocol`] implementation, alongside
//! [`Http`](crate::rpc::http::Http) and
//! [`Websocket`](crate::rpc::websocket::Websocket). Every method reaches the
//! database through [`RpcProtocol::execute`], so capability gating, the
//! wall-clock query guard, and the RPC/auth observer events apply here exactly
//! as they do on the other two transports.
//!
//! # Session model
//!
//! gRPC has no connection-scoped state to hang a session on: the SDK's channel
//! reconnects transparently, and requests for one session may arrive on
//! different connections over its lifetime. This transport therefore follows
//! the HTTP model rather than the WebSocket one -- sessions live in one
//! process-wide map, and a request that names a session must prove it may use
//! it.
//!
//! # Security
//!
//! A request that names no session runs on an ephemeral session created for it
//! and dropped afterwards, so it can carry no state between requests and is
//! untargetable by any other caller.
//!
//! A session id is a **capability**: holding one is what authorises using the
//! session behind it. Two rules make that safe to rely on:
//!
//! - Session ids are minted by the server, never chosen by the client. `AttachSession` creates a
//!   session only when the request names none, and answers with a fresh v4 UUID. A caller therefore
//!   cannot squat an id, pre-create one another client will later be handed, or probe for one that
//!   exists -- naming an id that does not exist is refused rather than creating it.
//! - An id is 122 bits of randomness and is never logged or reported to anyone but the client that
//!   was issued it, so learning someone else's amounts to capturing their traffic.
//!
//! This is the same model as a session cookie, and it is what the protocol
//! describes: a client re-attaches to its session after a reconnect, which
//! rules out scoping sessions to a connection the way the WebSocket transport
//! does (a gRPC channel reconnects underneath the client without telling it).
//!
//! On top of that, a request that *does* carry transport-level credentials
//! must present ones matching the session it names -- see
//! [`Grpc::verify_caller_for_session`]. The SDK authenticates by RPC rather
//! than by header, so this does not fire for it; it exists so that a caller
//! which authenticates per request cannot reach a session belonging to a
//! different principal.

use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use surrealdb_core::channel::{Receiver, bounded};
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::capabilities::{MethodTarget, RouteTarget};
use surrealdb_core::dbs::{
	QUERY_STREAM_BUFFER, QueryResult, QueryStreamItem, QueryStreamJob, QueryType, Session,
};
use surrealdb_core::iam::check::check_ns_db;
use surrealdb_core::iam::{Auth, Token};
use surrealdb_core::kvs::{Datastore, Transaction, TransactionType, export};
use surrealdb_core::rpc::{
	DbResult, Method, RpcProtocol, invalid_params, method_not_allowed, session_exists,
	session_not_found, types_error_from_anyhow,
};
use surrealdb_protocol::method_names;
use surrealdb_protocol::proto::rpc::v1 as rpc;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SurrealDbService;
use surrealdb_protocol::proto::v1 as proto;
use surrealdb_types::{Array, Error as TypesError, HashMap, Notification, SurrealValue, Value};
use tokio::sync::{RwLock, mpsc};
use tonic::{Request, Response, Status};
use uuid::Uuid;
use web_time::Instant;

use crate::cnf::{
	GRPC_MAX_ATTACHED_SESSIONS, GRPC_NOTIFICATION_BUFFER, HTTP_MAX_IMPORT_BODY_SIZE,
	HTTP_MAX_RPC_BODY_SIZE, MAX_TRANSACTIONS_PER_SESSION, PKG_NAME, PKG_VERSION,
};
use crate::rpc::RpcState;

/// The size of the chunks a streaming export is broken into.
///
/// Matches the chunk size the SDK sends imports in, so both directions frame
/// their bytes the same way.
const EXPORT_CHUNK_SIZE: usize = surrealdb_protocol::DEFAULT_FILE_CHUNK_SIZE;

/// The most records one query batch frame carries, whatever a client asks for.
///
/// A statement's results are split across frames so that no single message
/// grows with the result set: a gRPC client decodes 4 MiB per message by
/// default, so without this an otherwise valid large `SELECT` is refused
/// outright rather than returning its rows.
///
/// The bound is a record count, not a byte budget, because measuring the
/// encoded size would mean depending on `prost` here. At 256 a record has
/// 16 KiB before a frame approaches that default, which covers ordinary
/// records but is not a guarantee -- a table of documents averaging more than
/// that still needs the client to raise its limit, which it does from the
/// `max_message_bytes` this server reports. Bounding the frame in bytes is the
/// only way to make it unconditional.
const QUERY_BATCH_RECORDS: usize = 256;

/// A registered LIVE query, and the subscription streaming it (once one
/// attaches).
struct LiveQuery {
	/// The session that registered it. Only that session may subscribe.
	session_id: Uuid,
	/// The namespace at registration time, for the active-LQ gauge.
	namespace: Option<String>,
	/// The database at registration time, for the active-LQ gauge.
	database: Option<String>,
	/// The subscription currently streaming this live query's notifications.
	/// At most one at a time -- `GetCapabilities` reports
	/// `multiple_subscribers: false` accordingly.
	subscriber: Option<Subscription>,
}

/// The subscription streaming a live query.
struct Subscription {
	/// Identifies this subscription among the ones a live query has had. A
	/// stream's teardown runs when the client drops it, which may be after it
	/// has already subscribed again, so the teardown compares this before
	/// detaching -- otherwise it would detach its own replacement.
	id: Uuid,
	/// Where this subscription's frames are delivered.
	frames: mpsc::Sender<Result<rpc::SubscribeResponse, Status>>,
}

/// The gRPC RPC handler.
///
/// One instance per server, shared by every gRPC request. See the module docs
/// for how a request's session is resolved and gated.
pub struct Grpc {
	kvs: Arc<Datastore>,
	/// Sessions attached by `AttachSession`, plus the ephemeral ones minted
	/// for requests that name no session.
	sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
	/// The subset of `sessions` created by the transport for a single
	/// request. Tracked so they can never be targeted across requests.
	ephemeral_sessions: HashMap<Uuid, ()>,
	/// Open client-driven transactions, each tagged with the session that
	/// began it so a detach can cancel them.
	transactions: DashMap<Uuid, (Uuid, Arc<Transaction>)>,
	/// How many transactions each session currently holds open, enforcing
	/// [`MAX_TRANSACTIONS_PER_SESSION`].
	transaction_counts: DashMap<Uuid, usize>,
	/// Registered LIVE queries, keyed by live query id.
	live_queries: DashMap<Uuid, LiveQuery>,
	metrics_observer: Option<Arc<crate::observe::metrics::MetricsObserver>>,
}

impl Grpc {
	pub fn new(
		kvs: Arc<Datastore>,
		metrics_observer: Option<Arc<crate::observe::metrics::MetricsObserver>>,
	) -> Self {
		Self {
			kvs,
			sessions: HashMap::new(),
			ephemeral_sessions: HashMap::new(),
			transactions: DashMap::new(),
			transaction_counts: DashMap::new(),
			live_queries: DashMap::new(),
			metrics_observer,
		}
	}

	/// Registers a session created by the transport for a single request.
	///
	/// The ephemeral marker is inserted *before* the session itself so a
	/// concurrent lookup can never observe the session without also observing
	/// that it is untargetable.
	fn register_ephemeral_session(&self, id: Uuid, session: Arc<RwLock<Session>>) {
		self.ephemeral_sessions.insert(id, ());
		self.sessions.insert(id, session);
	}

	/// How many sessions clients hold open, which is what
	/// [`GRPC_MAX_ATTACHED_SESSIONS`] caps.
	fn attached_session_count(&self) -> usize {
		self.sessions.len().saturating_sub(self.ephemeral_sessions.len())
	}

	/// Drops a request's ephemeral session and anything it left behind.
	async fn remove_ephemeral_session(&self, id: &Uuid) {
		self.sessions.remove(id);
		self.ephemeral_sessions.remove(id);
		self.cleanup_lqs(id).await;
		self.cleanup_txns(id).await;
	}

	/// Whether a caller presenting `caller` may operate on `session_id`.
	///
	/// Holding the session id is the primary authorisation (see the module
	/// docs), so an *unauthenticated* caller -- which is every request from an
	/// SDK, since it authenticates by RPC rather than by header -- passes on
	/// the strength of the id alone.
	///
	/// A session that is not yet bound to a principal is open to whoever holds
	/// its id, exactly as the HTTP transport has it: a session starts
	/// unauthenticated and the caller that attached it must be able to sign in
	/// on it. Once bound, a caller that authenticated at the transport level is
	/// held to the stronger rule the HTTP transport applies: its principal must
	/// match the session's. A caller who presents credentials has told us who
	/// it is, and letting it act on a session belonging to somebody else would
	/// be a privilege escalation whichever of the two identities is the more
	/// privileged.
	///
	/// Ephemeral sessions are refused outright: their ids are minted per
	/// request and must never be reachable from another one.
	async fn verify_caller_for_session(
		&self,
		session_id: &Uuid,
		caller: &Auth,
	) -> Result<(), TypesError> {
		if self.ephemeral_sessions.contains_key(session_id) {
			return Err(session_not_found(*session_id));
		}
		let session_lock = self.get_session(session_id).await?;
		let session = session_lock.read().await;
		// An anonymous caller is the ordinary case, and the id is what
		// authorises it. A session bound to nobody yet has no principal to
		// compare against, so the id is all there is either way.
		if matches!(caller.level(), surrealdb_core::iam::Level::No)
			|| matches!(session.au.level(), surrealdb_core::iam::Level::No)
		{
			return Ok(());
		}
		// Roles are deliberately not compared, so a role grant or revocation
		// for the same identity does not lock the legitimate owner out.
		if session.au.id() == caller.id() && session.au.level() == caller.level() {
			Ok(())
		} else {
			Err(session_not_found(*session_id))
		}
	}

	/// Reserves a transaction slot for a session, or reports that the session
	/// already holds the maximum.
	///
	/// Reserving up front (rather than counting the map afterwards) is what
	/// stops two concurrent `begin`s from both observing a below-limit count.
	fn reserve_transaction_slot(&self, session_id: Uuid) -> bool {
		let mut count = self.transaction_counts.entry(session_id).or_insert(0);
		if *count >= *MAX_TRANSACTIONS_PER_SESSION {
			return false;
		}
		*count += 1;
		true
	}

	/// Whether `session_id` is the session that opened `txn`.
	///
	/// Also answers `false` for a transaction that no longer exists, so a
	/// caller cannot tell "not yours" from "not there".
	fn transaction_belongs_to(&self, txn: &Uuid, session_id: Uuid) -> bool {
		self.transactions.get(txn).is_some_and(|entry| entry.value().0 == session_id)
	}

	/// Returns a slot reserved by [`reserve_transaction_slot`](Self::reserve_transaction_slot).
	fn release_transaction_slot(&self, session_id: &Uuid) {
		if let Some(mut count) = self.transaction_counts.get_mut(session_id)
			&& *count > 0
		{
			*count -= 1;
		}
		// Drop the guard before pruning: `remove_if` takes the same lock.
		self.transaction_counts.remove_if(session_id, |_, count| *count == 0);
	}

	/// Delivers a notification to the subscription streaming its live query,
	/// reporting whether this transport owns the live query at all.
	///
	/// Delivery never waits. A subscriber that stops reading fills its queue
	/// and is ended, which is what keeps the memory a subscription can hold to
	/// [`GRPC_NOTIFICATION_BUFFER`]: waiting instead would leave one pending
	/// send per notification, and the dispatcher that calls this keeps
	/// receiving regardless, so nothing would bound how many accumulate.
	/// Ending the subscription rather than dropping the notification is what
	/// tells the subscriber it fell behind -- this transport retains nothing,
	/// so a dropped notification could not be recovered.
	pub(crate) async fn dispatch_notification(&self, notification: &Notification) -> bool {
		let id = notification.id.into_inner();
		// Copy the sender and labels out, and drop the map guard, before
		// awaiting: holding a `DashMap` guard across an await blocks every
		// other user of that shard, including the `handle_kill` that would
		// unblock us.
		let Some((subscriber, namespace, database)) = self.live_queries.get(&id).map(|lq| {
			(
				lq.subscriber.as_ref().map(|s| s.frames.clone()),
				lq.namespace.clone(),
				lq.database.clone(),
			)
		}) else {
			// The live query belongs to another transport.
			return false;
		};
		let Some(subscriber) = subscriber else {
			// The live query has ended even though nothing was streaming it, so
			// the registration goes with it: leaving it would hold the
			// single-subscriber slot for an id that can never produce another
			// notification, and leave the active-LQ gauge counting it forever.
			if notification.action == surrealdb_types::Action::Killed {
				self.forget_live_query(&id);
			}
			// Nothing has subscribed yet. Notifications produced before a
			// subscription attaches are not retained -- this transport reports
			// `LIVE_QUERY_DELIVERY_AT_MOST_ONCE`.
			return true;
		};
		let frame = match notification.action {
			surrealdb_types::Action::Killed => {
				// The live query is gone, so the subscription ends with it.
				rpc::subscribe_response::Frame::End(rpc::SubscribeEnd {
					reason: rpc::SubscribeEndReason::Killed as i32,
					cursor: None,
				})
			}
			// The live query's own WHERE clause or projection is raising an
			// error. A subscriber has to be told -- that is what this action
			// exists for -- and the protocol's only channel for it is the
			// terminal error frame. The registration is left in place so the
			// client can still `KILL` the broken query.
			surrealdb_types::Action::Error => {
				rpc::subscribe_response::Frame::Error(proto::SurrealError::new(
					proto::ErrorKind::Query,
					notification.result.clone().into_string().unwrap_or_else(|_| {
						"The live query raised an evaluation error".to_string()
					}),
				))
			}
			action => match to_proto_notification(notification, action) {
				Ok(notification) => {
					if let Some(observer) = self.metrics_observer.as_ref() {
						observer.record_live_query_notification(
							namespace.as_deref(),
							database.as_deref(),
						);
					}
					rpc::subscribe_response::Frame::Notification(notification)
				}
				// A change this transport cannot render is the subscriber's to
				// know about: silently skipping it would leave a gap in a
				// stream that reports no retention and no resumption.
				Err(error) => rpc::subscribe_response::Frame::Error(to_proto_error(&error)),
			},
		};
		let mut terminal = !matches!(frame, rpc::subscribe_response::Frame::Notification(_));
		// The last slot is held back for a terminal frame, so a subscriber that
		// has fallen behind can always be told why its stream is ending.
		if !terminal && subscriber.capacity() <= 1 {
			warn!("Ending gRPC subscription to live query {id}: the subscriber is not keeping up");
			subscriber
				.try_send(Err(Status::resource_exhausted(
					"Notifications were produced faster than this subscription read them",
				)))
				.ok();
			terminal = true;
		} else {
			// A closed channel means the client dropped its stream; the
			// subscription guard clears the registration, so nothing to do here.
			subscriber
				.try_send(Ok(rpc::SubscribeResponse {
					frame: Some(frame),
				}))
				.ok();
		}
		if terminal {
			// Dropping the stored sender (and this local clone) closes the
			// channel, which is what ends the client's stream.
			drop(subscriber);
			if notification.action == surrealdb_types::Action::Killed {
				self.forget_live_query(&id);
			} else if let Some(mut entry) = self.live_queries.get_mut(&id) {
				entry.subscriber = None;
			}
		}
		true
	}

	/// Ends a subscription, if one is attached, with the given reason.
	///
	/// The reason is delivered best-effort: every caller drops the
	/// registration, and with it the sender, immediately afterwards, so the
	/// stream ends either way. Waiting for capacity instead would let one
	/// subscriber that has stopped reading block a `KILL`, a detach, or the
	/// shutdown sweep that runs before every other transport is drained.
	fn end_subscription(&self, live_query_id: &Uuid, reason: rpc::SubscribeEndReason) {
		let subscriber = self
			.live_queries
			.get(live_query_id)
			.and_then(|lq| lq.subscriber.as_ref().map(|s| s.frames.clone()));
		if let Some(subscriber) = subscriber {
			let frame = rpc::SubscribeResponse {
				frame: Some(rpc::subscribe_response::Frame::End(rpc::SubscribeEnd {
					reason: reason as i32,
					cursor: None,
				})),
			};
			subscriber.try_send(Ok(frame)).ok();
		}
	}

	/// Ends a live query this transport registered: the same teardown a
	/// subscription's guard performs when it owns the query it is streaming.
	async fn discard_live_query(&self, live_query_id: &Uuid) {
		self.end_subscription(live_query_id, rpc::SubscribeEndReason::Killed);
		self.forget_live_query(live_query_id);
		if let Err(err) = self.kvs.delete_queries(vec![*live_query_id]).await {
			error!("Error discarding live query {live_query_id}: {err}");
		}
	}

	/// Drops a live query's registration and balances the active-LQ gauge.
	fn forget_live_query(&self, live_query_id: &Uuid) -> Option<LiveQuery> {
		let (_, entry) = self.live_queries.remove(live_query_id)?;
		if let Some(observer) = self.metrics_observer.as_ref() {
			observer.adjust_live_query_active(
				-1,
				entry.namespace.as_deref(),
				entry.database.as_deref(),
			);
		}
		Some(entry)
	}

	/// Cancels every transaction any session left open.
	///
	/// A WebSocket's transactions are reclaimed when its socket closes, but a
	/// gRPC session outlives any one connection, so shutdown is the only point
	/// at which every open transaction is known to be finished with.
	pub(crate) async fn cleanup_all_txns(&self) {
		self.cleanup_txns_filtered(None).await;
	}

	/// Cancels every transaction a session left open.
	async fn cleanup_txns_filtered(&self, session_filter: Option<&Uuid>) {
		let doomed: Vec<Uuid> = self
			.transactions
			.iter()
			.filter(|entry| match session_filter {
				Some(session_id) => &entry.value().0 == session_id,
				None => true,
			})
			.map(|entry| *entry.key())
			.collect();
		for id in doomed {
			if let Some((_, (session_id, tx))) = self.transactions.remove(&id) {
				self.release_transaction_slot(&session_id);
				if let Err(err) = tx.cancel().await {
					warn!("Error cancelling gRPC transaction {id}: {err}");
				}
			}
		}
	}
}

impl RpcProtocol for Grpc {
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	fn kvs_arc(&self) -> Arc<Datastore> {
		Arc::clone(&self.kvs)
	}

	fn version_data(&self) -> DbResult {
		DbResult::Other(Value::String(format!("{PKG_NAME}-{}", *PKG_VERSION)))
	}

	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	/// Session enumeration is not offered.
	///
	/// As on HTTP, this transport's sessions are reachable by id from any
	/// connection, so listing them would hand every caller a set of ids to try
	/// the ownership gate against. There is no legitimate per-caller use for
	/// listing other clients' sessions.
	async fn sessions(&self) -> Result<DbResult, TypesError> {
		Err(method_not_allowed(Method::Sessions.to_string()))
	}

	/// Registers a session, subject to [`GRPC_MAX_ATTACHED_SESSIONS`].
	async fn attach(&self, session_id: Uuid) -> Result<DbResult, TypesError> {
		if self.sessions.contains_key(&session_id) {
			return Err(session_exists(session_id));
		}
		// Ephemeral sessions are transient and belong to a request that is
		// still running, so they do not count against the cap on how many
		// sessions clients may hold open.
		if self.attached_session_count() >= *GRPC_MAX_ATTACHED_SESSIONS {
			return Err(method_not_allowed(Method::Attach.to_string()));
		}
		let mut session = Session::default().with_rt(Self::LQ_SUPPORT);
		session.id = Some(session_id);
		self.sessions.insert(session_id, Arc::new(RwLock::new(session)));
		Ok(DbResult::Other(Value::None))
	}

	async fn get_tx(&self, id: Uuid) -> Result<Arc<Transaction>, TypesError> {
		self.transactions
			.get(&id)
			.map(|entry| Arc::clone(&entry.value().1))
			.ok_or_else(|| invalid_params("Transaction not found"))
	}

	// `set_tx` is deliberately left at its default. Nothing calls it -- a
	// transaction only enters the map through `begin`, which is also what
	// tags it with the session that must clean it up -- so an implementation
	// here could only produce an untracked transaction no cleanup path finds.

	const LQ_SUPPORT: bool = true;

	async fn handle_live(
		&self,
		lqid: &Uuid,
		session_id: Uuid,
		namespace: Option<String>,
		database: Option<String>,
	) {
		self.live_queries.insert(
			*lqid,
			LiveQuery {
				session_id,
				namespace: namespace.clone(),
				database: database.clone(),
				subscriber: None,
			},
		);
		if let Some(observer) = self.metrics_observer.as_ref() {
			observer.adjust_live_query_active(1, namespace.as_deref(), database.as_deref());
		}
		// The session id is a capability and stays out of the log; the live
		// query id identifies the registration well enough to trace it.
		trace!("Registered live query {lqid} on the gRPC transport");
	}

	async fn handle_kill(&self, lqid: &Uuid) {
		// End the stream before dropping the registration so the subscriber
		// learns *why* its stream ended rather than just seeing it close.
		self.end_subscription(lqid, rpc::SubscribeEndReason::Killed);
		if self.forget_live_query(lqid).is_some() {
			trace!("Unregistered live query {lqid} on the gRPC transport");
		}
	}

	async fn cleanup_lqs(&self, session_id: &Uuid) {
		let doomed: Vec<Uuid> = self
			.live_queries
			.iter()
			.filter(|entry| &entry.value().session_id == session_id)
			.map(|entry| *entry.key())
			.collect();
		for id in doomed {
			self.end_subscription(&id, rpc::SubscribeEndReason::SessionClosed);
			self.forget_live_query(&id);
			if let Err(err) = self.kvs.delete_queries(vec![id]).await {
				error!("Error cleaning up live query {id} on the gRPC transport: {err}");
			}
		}
	}

	async fn cleanup_all_lqs(&self) {
		let doomed: Vec<Uuid> = self.live_queries.iter().map(|entry| *entry.key()).collect();
		for id in doomed {
			self.end_subscription(&id, rpc::SubscribeEndReason::ServerShutdown);
			self.forget_live_query(&id);
			if let Err(err) = self.kvs.delete_queries(vec![id]).await {
				error!("Error cleaning up live query {id} on shutdown: {err}");
			}
		}
	}

	async fn cleanup_txns(&self, session_id: &Uuid) {
		self.cleanup_txns_filtered(Some(session_id)).await;
	}

	async fn begin(&self, _txn: Option<Uuid>, session_id: Uuid) -> Result<DbResult, TypesError> {
		// Reject a `begin` for a session that was never attached, so a client
		// cannot mint an endless stream of fabricated session ids -- each with
		// its own transaction budget and its own counter entry.
		self.get_session(&session_id).await?;
		// A transaction outlives the request that opened it, so it cannot run
		// on an ephemeral session: releasing that session at the end of this
		// request cancels the transaction, and the caller would be holding an
		// id for something that no longer exists.
		if self.ephemeral_sessions.contains_key(&session_id) {
			return Err(invalid_params("Opening a transaction requires an attached session"));
		}
		if !self.reserve_transaction_slot(session_id) {
			return Err(surrealdb_core::rpc::too_many_transactions());
		}
		let tx = match self.kvs.transaction(TransactionType::Write).await {
			Ok(tx) => tx,
			Err(err) => {
				self.release_transaction_slot(&session_id);
				return Err(types_error_from_anyhow(err));
			}
		};
		let id = Uuid::now_v7();
		self.transactions.insert(id, (session_id, Arc::new(tx)));
		// Close the begin/detach race: `del_session` removes the session from
		// the map *before* draining its transactions, so a detach that ran
		// during the await above would have drained the map before this
		// transaction was published. Re-checking after the insert means one
		// side always observes the other.
		if !self.sessions.contains_key(&session_id) {
			self.cleanup_txns_filtered(Some(&session_id)).await;
			return Err(session_not_found(session_id));
		}
		Ok(DbResult::Other(Value::Uuid(surrealdb_types::Uuid::from(id))))
	}

	async fn commit(
		&self,
		txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
		let txn = txn.ok_or_else(|| invalid_params("Expected a transaction id"))?;
		let Some((_, (session_id, tx))) = self.transactions.remove(&txn) else {
			return Err(invalid_params("Transaction not found"));
		};
		self.release_transaction_slot(&session_id);
		tx.commit().await.map_err(types_error_from_anyhow)?;
		Ok(DbResult::Other(Value::None))
	}

	async fn cancel(
		&self,
		txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
		let txn = txn.ok_or_else(|| invalid_params("Expected a transaction id"))?;
		let Some((_, (session_id, tx))) = self.transactions.remove(&txn) else {
			return Err(invalid_params("Transaction not found"));
		};
		self.release_transaction_slot(&session_id);
		tx.cancel().await.map_err(types_error_from_anyhow)?;
		Ok(DbResult::Other(Value::None))
	}
}

/// The session a request runs on.
#[derive(Clone, Copy)]
struct ResolvedSession {
	/// The session the method executes against.
	id: Uuid,
	/// The id the client asked for, which session-management methods
	/// (`attach`, `detach`) require and the ownership gate keys on. `None`
	/// when the request named no session and `id` is ephemeral.
	client: Option<Uuid>,
}

/// One gRPC request's view of the server.
///
/// Built per request by the mounting handler so it can carry that request's
/// authenticated [`Session`] -- the principal the ownership gate compares, and
/// the starting state of an ephemeral session.
pub struct GrpcService {
	state: Arc<RpcState>,
	/// The caller's request-level session, as resolved from this request's
	/// headers by the auth middleware.
	caller: Session,
}

impl GrpcService {
	pub fn new(state: Arc<RpcState>, caller: Session) -> Self {
		Self {
			state,
			caller,
		}
	}

	fn rpc(&self) -> &Grpc {
		&self.state.grpc
	}

	fn kvs(&self) -> &Datastore {
		&self.rpc().kvs
	}

	/// Resolves the session a request runs on, applying the ownership gate.
	///
	/// A request naming no session gets an ephemeral one seeded with the
	/// caller's own authentication, which the caller must drop again with
	/// [`release`](Self::release). An ephemeral session is not marked as
	/// supporting realtime, so a `LIVE SELECT` on one is refused rather than
	/// registering a live query that is torn down the moment the request that
	/// created it returns.
	///
	/// `gate` is false only for `AttachSession`, which names the session it is
	/// about to create: there is no session to check ownership of yet, and the
	/// handler has already established that the id was minted here.
	async fn resolve(
		&self,
		context: Option<&rpc::RequestContext>,
		gate: bool,
	) -> Result<ResolvedSession, Status> {
		match context.and_then(|context| context.session.as_ref()) {
			Some(session) => {
				let id = to_uuid(session)?;
				if gate {
					self.rpc()
						.verify_caller_for_session(&id, self.caller.au.as_ref())
						.await
						.map_err(|err| to_status(&err))?;
				}
				Ok(ResolvedSession {
					id,
					client: Some(id),
				})
			}
			None => {
				let id = Uuid::new_v4();
				let mut session = self.caller.clone();
				session.id = Some(id);
				self.rpc().register_ephemeral_session(id, Arc::new(RwLock::new(session)));
				Ok(ResolvedSession {
					id,
					client: None,
				})
			}
		}
	}

	/// Drops a request's ephemeral session, if it minted one.
	async fn release(&self, session: &ResolvedSession) {
		if session.client.is_none() {
			self.rpc().remove_ephemeral_session(&session.id).await;
		}
	}

	/// Runs a method for a request, resolving its session, applying the
	/// request's own timeout ceiling, and dropping any ephemeral session
	/// afterwards.
	async fn execute(
		&self,
		context: Option<&rpc::RequestContext>,
		method: Method,
		params: Array,
	) -> Result<DbResult, Status> {
		// Parse the transaction id before resolving a session: `resolve`
		// registers an ephemeral session, and an early return between the two
		// would strand it in the session map with nothing left to remove it.
		let txn = match context.and_then(|context| context.transaction.as_ref()) {
			Some(txn) => Some(to_uuid(txn)?),
			None => None,
		};
		let session = self.resolve(context, method != Method::Attach).await?;
		// A transaction may only be used by the session that opened it. The
		// map is process-wide, so without this the id alone would be enough
		// for any caller to read, write, commit or cancel inside somebody
		// else's transaction. An unowned id is refused the same way a missing
		// one is, so the answer is not an oracle for which of the two it was.
		if let Some(txn) = txn
			&& !self.rpc().transaction_belongs_to(&txn, session.id)
		{
			self.release(&session).await;
			return Err(to_status(&invalid_params("Transaction not found")));
		}
		let dispatch =
			RpcProtocol::execute(self.rpc(), txn, session.id, session.client, method, params);
		// `RequestContext.timeout` is a ceiling the caller asks for; the
		// server's own `--query-timeout` (applied inside `execute`) still
		// bounds the call independently, so the effective limit is whichever
		// expires first.
		//
		// It applies to `Query` alone, because expiring it drops the dispatch
		// future -- and the RPC and auth observer events fire at the *end* of
		// `RpcProtocol::execute`, so a dropped call is never recorded. Query is
		// the only method here whose wall-clock a caller cannot predict; for
		// the rest, honouring the ceiling would buy a caller nothing it could
		// not get by waiting, while letting it drive `Signin` under a deadline
		// shorter than the password hash and leave no failed-signin audit
		// record behind. Transaction control is doubly exempt: dropping a
		// commit future mid-flight is unsafe.
		let deadline = context
			.and_then(|context| context.timeout.as_ref())
			.and_then(|timeout| Duration::try_from(*timeout).ok())
			.filter(|_| method == Method::Query);
		let result = match deadline {
			Some(deadline) => match tokio::time::timeout(deadline, dispatch).await {
				Ok(result) => result,
				Err(_) => Err(surrealdb_core::rpc::query_timeout_error(deadline)),
			},
			None => dispatch.await,
		};
		self.release(&session).await;
		result.map_err(|err| to_status(&err))
	}

	/// Clones the [`Session`] a request runs against, for the paths that hand
	/// the datastore a session directly (import and export) rather than going
	/// through an RPC method.
	async fn session_for(
		&self,
		context: Option<&rpc::RequestContext>,
		route: RouteTarget,
	) -> Result<Session, Status> {
		if !self.kvs().allows_http_route(&route) {
			warn!("Capabilities denied gRPC route request attempt, target: '{route}'");
			return Err(Status::permission_denied(format!("Route {route} is not allowed")));
		}
		let resolved = self.resolve(context, true).await?;
		let session = match self.rpc().get_session(&resolved.id).await {
			Ok(lock) => lock.read().await.clone(),
			Err(err) => {
				self.release(&resolved).await;
				return Err(to_status(&err));
			}
		};
		self.release(&resolved).await;
		Ok(session)
	}
}

/// The stream type every server-streaming RPC on this service returns.
type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl SurrealDbService for GrpcService {
	async fn get_capabilities(
		&self,
		request: Request<rpc::GetCapabilitiesRequest>,
	) -> Result<Response<rpc::GetCapabilitiesResponse>, Status> {
		let request = request.into_inner();
		if let Some(client) = request.client.as_ref() {
			debug!(
				"gRPC client connected: {} {} on {}",
				client.name, client.version, client.platform
			);
		}
		Ok(Response::new(rpc::GetCapabilitiesResponse {
			capabilities: Some(self.server_capabilities()),
		}))
	}

	async fn health(
		&self,
		_request: Request<rpc::HealthRequest>,
	) -> Result<Response<rpc::HealthResponse>, Status> {
		if !self.kvs().allows_http_route(&RouteTarget::Health) {
			return Err(Status::permission_denied("Route health is not allowed"));
		}
		self.kvs().health_check().await.map_err(|err| {
			error!("Health check failed: {err}");
			Status::unavailable("Health check failed")
		})?;
		Ok(Response::new(rpc::HealthResponse {}))
	}

	async fn attach_session(
		&self,
		request: Request<rpc::AttachSessionRequest>,
	) -> Result<Response<rpc::AttachSessionResponse>, Status> {
		let context = request.into_inner().context;
		// Re-attaching to an existing session is what lets a client tell "the
		// server still has my session" from "the server lost it" after a
		// reconnect -- which decides whether its cached namespace, variables
		// and authentication are still in effect.
		if let Some(session) = context.as_ref().and_then(|context| context.session.as_ref()) {
			let id = to_uuid(session)?;
			// A named session is only ever adopted, never created. Creating
			// one here would let a caller choose its own id, and an id a
			// caller can choose is one it can squat, or pre-create so that it
			// already owns the session another client is later handed.
			self.rpc()
				.verify_caller_for_session(&id, self.caller.au.as_ref())
				.await
				.map_err(|err| to_status(&err))?;
			return Ok(Response::new(rpc::AttachSessionResponse {
				session: Some(proto::Uuid::from_uuid(id)),
				created: false,
			}));
		}
		// Creating a session mints its id here, so it is unguessable and
		// unique by construction.
		let id = Uuid::new_v4();
		let context = rpc::RequestContext {
			session: Some(proto::Uuid::from_uuid(id)),
			transaction: None,
			timeout: context.and_then(|context| context.timeout),
		};
		self.execute(Some(&context), Method::Attach, Array::new()).await?;
		Ok(Response::new(rpc::AttachSessionResponse {
			session: Some(proto::Uuid::from_uuid(id)),
			created: true,
		}))
	}

	async fn detach_session(
		&self,
		request: Request<rpc::DetachSessionRequest>,
	) -> Result<Response<rpc::DetachSessionResponse>, Status> {
		let context = request.into_inner().context;
		self.execute(context.as_ref(), Method::Detach, Array::new()).await?;
		Ok(Response::new(rpc::DetachSessionResponse {}))
	}

	async fn reset_session(
		&self,
		request: Request<rpc::ResetSessionRequest>,
	) -> Result<Response<rpc::ResetSessionResponse>, Status> {
		let context = request.into_inner().context;
		self.execute(context.as_ref(), Method::Reset, Array::new()).await?;
		Ok(Response::new(rpc::ResetSessionResponse {}))
	}

	async fn r#use(
		&self,
		request: Request<rpc::UseRequest>,
	) -> Result<Response<rpc::UseResponse>, Status> {
		let request = request.into_inner();
		let params =
			Array::from(vec![from_nullable(request.namespace), from_nullable(request.database)]);
		let result = self.execute(request.context.as_ref(), Method::Use, params).await?;
		// `yuse` answers with the resulting selection so a client can sync its
		// own view; the wire spells "nothing selected" as an empty string.
		let selection = match result {
			DbResult::Other(Value::Object(object)) => object,
			_ => {
				return Err(Status::internal("Use did not report the resulting selection"));
			}
		};
		let field = |key: &str| match selection.get(key) {
			Some(Value::String(value)) => value.clone(),
			_ => String::new(),
		};
		Ok(Response::new(rpc::UseResponse {
			namespace: field("namespace"),
			database: field("database"),
		}))
	}

	async fn set_variable(
		&self,
		request: Request<rpc::SetVariableRequest>,
	) -> Result<Response<rpc::SetVariableResponse>, Status> {
		let request = request.into_inner();
		let value = match request.value {
			Some(value) => from_proto_value(value)?,
			None => Value::None,
		};
		let params = Array::from(vec![Value::String(request.name), value]);
		self.execute(request.context.as_ref(), Method::Set, params).await?;
		Ok(Response::new(rpc::SetVariableResponse {}))
	}

	async fn unset_variable(
		&self,
		request: Request<rpc::UnsetVariableRequest>,
	) -> Result<Response<rpc::UnsetVariableResponse>, Status> {
		let request = request.into_inner();
		let params = Array::from(vec![Value::String(request.name)]);
		self.execute(request.context.as_ref(), Method::Unset, params).await?;
		Ok(Response::new(rpc::UnsetVariableResponse {}))
	}

	async fn signup(
		&self,
		request: Request<rpc::SignupRequest>,
	) -> Result<Response<rpc::SignupResponse>, Status> {
		let request = request.into_inner();
		let credentials = request
			.credentials
			.ok_or_else(|| Status::invalid_argument("Expected signup credentials"))?;
		let params = Array::from(vec![Value::Object(record_credentials(credentials)?)]);
		let result = self.execute(request.context.as_ref(), Method::Signup, params).await?;
		Ok(Response::new(rpc::SignupResponse {
			tokens: Some(to_tokens(result)?),
		}))
	}

	async fn signin(
		&self,
		request: Request<rpc::SigninRequest>,
	) -> Result<Response<rpc::SigninResponse>, Status> {
		let request = request.into_inner();
		let access = request
			.access_method
			.ok_or_else(|| Status::invalid_argument("Expected an access method"))?;
		let params = Array::from(vec![Value::Object(access_credentials(access)?)]);
		let result = self.execute(request.context.as_ref(), Method::Signin, params).await?;
		Ok(Response::new(rpc::SigninResponse {
			tokens: Some(to_tokens(result)?),
		}))
	}

	async fn authenticate(
		&self,
		request: Request<rpc::AuthenticateRequest>,
	) -> Result<Response<rpc::AuthenticateResponse>, Status> {
		let request = request.into_inner();
		let params = Array::from(vec![Value::String(request.token)]);
		let result = self.execute(request.context.as_ref(), Method::Authenticate, params).await?;
		// The session may end up authenticated with a token other than the one
		// presented, so the answer reports what is actually in effect rather
		// than leaving the client to assume its own.
		Ok(Response::new(rpc::AuthenticateResponse {
			expires_at: None,
			tokens: to_tokens(result).ok(),
		}))
	}

	/// Exchanges a refresh token for a fresh pair.
	///
	/// Both halves are needed: `iam::token::refresh` decodes the expired access
	/// token's claims to recover the namespace, database and access method the
	/// new pair is minted for.
	async fn refresh_tokens(
		&self,
		request: Request<rpc::RefreshTokensRequest>,
	) -> Result<Response<rpc::RefreshTokensResponse>, Status> {
		let request = request.into_inner();
		let params = Array::from(vec![token_value(request.access, request.refresh)]);
		let result = self.execute(request.context.as_ref(), Method::Refresh, params).await?;
		Ok(Response::new(rpc::RefreshTokensResponse {
			tokens: Some(to_tokens(result)?),
		}))
	}

	async fn revoke_tokens(
		&self,
		request: Request<rpc::RevokeTokensRequest>,
	) -> Result<Response<rpc::RevokeTokensResponse>, Status> {
		let request = request.into_inner();
		let params = Array::from(vec![token_value(request.access, request.refresh)]);
		self.execute(request.context.as_ref(), Method::Revoke, params).await?;
		Ok(Response::new(rpc::RevokeTokensResponse {}))
	}

	async fn invalidate(
		&self,
		request: Request<rpc::InvalidateRequest>,
	) -> Result<Response<rpc::InvalidateResponse>, Status> {
		let context = request.into_inner().context;
		self.execute(context.as_ref(), Method::Invalidate, Array::new()).await?;
		Ok(Response::new(rpc::InvalidateResponse {}))
	}

	async fn begin_transaction(
		&self,
		request: Request<rpc::BeginTransactionRequest>,
	) -> Result<Response<rpc::BeginTransactionResponse>, Status> {
		let context = request.into_inner().context;
		let result = self.execute(context.as_ref(), Method::Begin, Array::new()).await?;
		let DbResult::Other(Value::Uuid(id)) = result else {
			return Err(Status::internal("Begin did not return a transaction id"));
		};
		Ok(Response::new(rpc::BeginTransactionResponse {
			transaction: Some(proto::Uuid::from_uuid(id.into_inner())),
		}))
	}

	async fn commit_transaction(
		&self,
		request: Request<rpc::CommitTransactionRequest>,
	) -> Result<Response<rpc::CommitTransactionResponse>, Status> {
		let context = request.into_inner().context;
		self.execute(context.as_ref(), Method::Commit, Array::new()).await?;
		Ok(Response::new(rpc::CommitTransactionResponse {}))
	}

	async fn cancel_transaction(
		&self,
		request: Request<rpc::CancelTransactionRequest>,
	) -> Result<Response<rpc::CancelTransactionResponse>, Status> {
		let context = request.into_inner().context;
		self.execute(context.as_ref(), Method::Cancel, Array::new()).await?;
		Ok(Response::new(rpc::CancelTransactionResponse {}))
	}

	type QueryStream = ResponseStream<rpc::QueryResponse>;

	async fn query(
		&self,
		request: Request<rpc::QueryRequest>,
	) -> Result<Response<Self::QueryStream>, Status> {
		let request = request.into_inner();
		// The columnar encoding is defined but not served. Silently answering
		// with row-oriented values would be within the contract, but a client
		// that asked *only* for Arrow has told us it cannot read anything else.
		let arrow_only = !request.accepted_encodings.is_empty()
			&& request
				.accepted_encodings
				.iter()
				.all(|encoding| *encoding == rpc::ResultEncoding::Arrow as i32);
		if arrow_only {
			return Err(Status::unimplemented(
				"This server does not serve columnar (Arrow) query results",
			));
		}
		let batch_records = batch_records(request.max_batch_records);
		self.stream_query(request.context.as_ref(), request.query, request.variables, batch_records)
			.await
	}

	/// Calls a function, or a machine learning model when a version is given.
	///
	/// Reaching `Method::Run` rather than compiling the call to SurrealQL is
	/// what keeps the name a value: it is classified into a function, a model
	/// or a package by the same code every other transport uses, and an
	/// operator's `--deny-rpc run` applies here as it does there.
	async fn run(
		&self,
		request: Request<rpc::RunRequest>,
	) -> Result<Response<rpc::RunResponse>, Status> {
		let request = request.into_inner();
		let args = request
			.args
			.into_iter()
			.map(from_proto_value)
			.collect::<Result<Vec<Value>, Status>>()?;
		// The wire spells "no version" as an empty string; `run` wants it
		// absent, since an empty version names no model.
		let version = if request.version.is_empty() {
			Value::None
		} else {
			Value::String(request.version)
		};
		let params = Array::from(vec![
			Value::String(request.name),
			version,
			Value::Array(Array::from(args)),
		]);
		let result = self.execute(request.context.as_ref(), Method::Run, params).await?;
		let DbResult::Other(value) = result else {
			return Err(Status::internal("Run did not return a value"));
		};
		Ok(Response::new(rpc::RunResponse {
			result: Some(to_proto_value(value)?),
		}))
	}

	/// Ends a live query.
	///
	/// As with `Run`, going through `Method::Kill` is what makes the operation
	/// nameable: the capability gate sees a kill rather than the query it would
	/// otherwise have been compiled into.
	async fn kill(
		&self,
		request: Request<rpc::KillRequest>,
	) -> Result<Response<rpc::KillResponse>, Status> {
		let request = request.into_inner();
		let live_query_id = request
			.live_query_id
			.ok_or_else(|| Status::invalid_argument("Expected a live query id"))?;
		let params = Array::from(vec![Value::Uuid(to_uuid(&live_query_id)?.into())]);
		self.execute(request.context.as_ref(), Method::Kill, params).await?;
		Ok(Response::new(rpc::KillResponse {}))
	}

	type SubscribeStream = ResponseStream<rpc::SubscribeResponse>;

	async fn subscribe(
		&self,
		request: Request<rpc::SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		let request = request.into_inner();
		// A cursor this server cannot honour must be refused, not ignored:
		// resuming from "now" instead would silently skip every change in the
		// gap, which is indistinguishable from there having been none.
		if request.resume_from.is_some() {
			return Err(Status::unimplemented(
				"This server does not retain live query history to resume from",
			));
		}
		let session = self.resolve(request.context.as_ref(), true).await?;
		// A subscription outlives the request that created it, so it cannot
		// run on an ephemeral session -- there would be nothing left to own
		// the live query once this request returned.
		let Some(session_id) = session.client else {
			self.release(&session).await;
			return Err(Status::failed_precondition("Subscribing requires an attached session"));
		};
		let (live_query_id, owned) = match request.subscribe_to {
			Some(rpc::subscribe_request::SubscribeTo::LiveQueryId(id)) => (to_uuid(&id)?, false),
			Some(rpc::subscribe_request::SubscribeTo::Query(registration)) => {
				let id = self.register_live_query(request.context.as_ref(), registration).await?;
				(id, true)
			}
			None => {
				return Err(Status::invalid_argument("Expected a live query id or a query"));
			}
		};
		self.attach_subscription(session_id, live_query_id, owned)
	}

	async fn import_surql(
		&self,
		request: Request<tonic::Streaming<rpc::ImportSurqlRequest>>,
	) -> Result<Response<rpc::ImportSurqlResponse>, Status> {
		self.run_import(request.into_inner()).await
	}

	async fn import_ml_model(
		&self,
		request: Request<tonic::Streaming<rpc::ImportMlModelRequest>>,
	) -> Result<Response<rpc::ImportMlModelResponse>, Status> {
		self.run_ml_import(request.into_inner()).await
	}

	type ExportSurqlStream = ResponseStream<rpc::ExportSurqlResponse>;

	async fn export_surql(
		&self,
		request: Request<rpc::ExportSurqlRequest>,
	) -> Result<Response<Self::ExportSurqlStream>, Status> {
		let request = request.into_inner();
		let session = self.session_for(request.context.as_ref(), RouteTarget::Export).await?;
		let config = match request.config {
			Some(config) => from_proto_export_config(config),
			None => export::Config::default(),
		};
		let export = self.start_export(session, config).await?;
		Ok(Response::new(Box::pin(frame_byte_stream(export, |frame| rpc::ExportSurqlResponse {
			frame: Some(match frame {
				ByteFrame::Chunk(chunk) => rpc::export_surql_response::Frame::Chunk(chunk),
				ByteFrame::Trailer(trailer) => rpc::export_surql_response::Frame::Trailer(trailer),
				ByteFrame::Error(error) => rpc::export_surql_response::Frame::Error(error),
			}),
		}))))
	}

	type ExportDirectoryStream = ResponseStream<rpc::ExportDirectoryResponse>;

	/// Directory-format export is not served.
	///
	/// The format is a multi-file layout with its own versioning, compression
	/// and parallelism, and SurrealDB produces only the single SurrealQL
	/// stream that `ExportSurql` serves. `GetCapabilities` does not advertise
	/// `EXPORT_DIRECTORY`.
	async fn export_directory(
		&self,
		_request: Request<rpc::ExportDirectoryRequest>,
	) -> Result<Response<Self::ExportDirectoryStream>, Status> {
		Err(Status::unimplemented(
			"This server does not produce directory-format exports; use ExportSurql",
		))
	}

	type ExportMlModelStream = ResponseStream<rpc::ExportMlModelResponse>;

	async fn export_ml_model(
		&self,
		request: Request<rpc::ExportMlModelRequest>,
	) -> Result<Response<Self::ExportMlModelStream>, Status> {
		let request = request.into_inner();
		let session = self.session_for(request.context.as_ref(), RouteTarget::Ml).await?;
		let export = self.start_ml_export(session, request.name, request.version).await?;
		Ok(Response::new(Box::pin(frame_byte_stream(export, |frame| rpc::ExportMlModelResponse {
			frame: Some(match frame {
				ByteFrame::Chunk(chunk) => rpc::export_ml_model_response::Frame::Chunk(chunk),
				ByteFrame::Trailer(trailer) => {
					rpc::export_ml_model_response::Frame::Trailer(trailer)
				}
				ByteFrame::Error(error) => rpc::export_ml_model_response::Frame::Error(error),
			}),
		}))))
	}
}

/// Route capability targets, paired with the RPC that reaches them, so a
/// denied route is reported by `GetCapabilities` as a denied method.
const ROUTE_METHODS: &[(RouteTarget, &str)] = &[
	(RouteTarget::Health, method_names::HEALTH),
	(RouteTarget::Export, method_names::EXPORT_SURQL),
	(RouteTarget::Import, method_names::IMPORT_SURQL),
	(RouteTarget::Ml, method_names::EXPORT_ML_MODEL),
	(RouteTarget::Ml, method_names::IMPORT_ML_MODEL),
];

/// RPC methods, paired with the RPC that reaches them, for the same reason.
const RPC_METHODS: &[(Method, &str)] = &[
	(Method::Use, method_names::USE),
	(Method::Set, method_names::SET_VARIABLE),
	(Method::Unset, method_names::UNSET_VARIABLE),
	(Method::Signup, method_names::SIGNUP),
	(Method::Signin, method_names::SIGNIN),
	(Method::Authenticate, method_names::AUTHENTICATE),
	(Method::Refresh, method_names::REFRESH_TOKENS),
	(Method::Revoke, method_names::REVOKE_TOKENS),
	(Method::Invalidate, method_names::INVALIDATE),
	(Method::Begin, method_names::BEGIN_TRANSACTION),
	(Method::Commit, method_names::COMMIT_TRANSACTION),
	(Method::Cancel, method_names::CANCEL_TRANSACTION),
	(Method::Query, method_names::QUERY),
	(Method::Run, method_names::RUN),
	(Method::Kill, method_names::KILL),
	(Method::Attach, method_names::ATTACH_SESSION),
	(Method::Detach, method_names::DETACH_SESSION),
	(Method::Reset, method_names::RESET_SESSION),
];

impl GrpcService {
	/// Reports what this build serves, so a client can gate on it rather than
	/// discovering the answer from a failed call.
	fn server_capabilities(&self) -> rpc::ServerCapabilities {
		let mut capabilities = vec![
			"SESSIONS".to_string(),
			"TRANSACTIONS".to_string(),
			"LIVE_QUERIES".to_string(),
			"REFRESH_TOKENS".to_string(),
		];
		if cfg!(feature = "ml") {
			capabilities.push("ML_MODELS".to_string());
		}
		let mut denied = Vec::new();
		for (route, method) in ROUTE_METHODS {
			if !self.kvs().allows_http_route(route) {
				denied.push((*method).to_string());
			}
		}
		for (rpc_method, method) in RPC_METHODS {
			if !self.kvs().allows_rpc_method(&MethodTarget {
				method: *rpc_method,
			}) {
				denied.push((*method).to_string());
			}
		}
		// `Version` names no RPC: denying it withholds the reported build
		// version and says so here, which is the only way a client can tell
		// "denied" from "not reported".
		let version_denied = !self.kvs().allows_rpc_method(&MethodTarget {
			method: Method::Version,
		});
		if version_denied {
			denied.push(method_names::VERSION_PSEUDO_METHOD.to_string());
		}
		rpc::ServerCapabilities {
			server_version: if version_denied {
				String::new()
			} else {
				format!("{PKG_NAME}-{}", *PKG_VERSION)
			},
			low_api_version: None,
			high_api_version: None,
			capabilities,
			denied_methods: denied,
			limits: Some(rpc::Limits {
				max_message_bytes: *HTTP_MAX_RPC_BODY_SIZE as u64,
				max_chunk_bytes: EXPORT_CHUNK_SIZE as u64,
				max_query_duration: self
					.kvs()
					.query_timeout()
					.and_then(|timeout| proto::Duration::try_from(timeout).ok()),
				max_batch_records: QUERY_BATCH_RECORDS as u32,
			}),
			live_queries: Some(rpc::LiveQueryCapabilities {
				// Notifications are delivered straight to an attached
				// subscription and never retained, so one produced while
				// nothing is subscribed is lost rather than redelivered.
				delivery: rpc::LiveQueryDelivery::AtMostOnce as i32,
				resumable: false,
				multiple_subscribers: false,
				retention: None,
			}),
			// The codecs the gRPC route enables `accept_compressed` for, most
			// preferred first, so a client can compress an upload without first
			// spending a rejected call to discover whether it may. `identity`
			// is listed last because it is always accepted; naming it is what
			// separates this answer from the silence of a server too old to
			// have the field.
			accepted_message_encodings: crate::ntw::grpc::accepted_message_encodings(),
		}
	}

	/// Runs a query and hands back one result per statement.
	async fn run_query(
		&self,
		context: Option<&rpc::RequestContext>,
		query: String,
		variables: Option<proto::Variables>,
	) -> Result<Vec<QueryResult>, Status> {
		let variables = match variables {
			Some(variables) => from_proto_variables(variables)?,
			None => Value::None,
		};
		let params = Array::from(vec![Value::String(query), variables]);
		match self.execute(context, Method::Query, params).await? {
			DbResult::Query(results) => Ok(results),
			_ => Err(Status::internal("Query did not return statement results")),
		}
	}

	/// Runs a query, framing each statement's results as they are produced.
	///
	/// The stream is built before the query has run: `Begin` goes out first, as
	/// the protocol requires, carrying the statement count parsing already
	/// established. Everything after it is produced lazily, so the first rows
	/// reach the client while the rest of the scan is still happening.
	async fn stream_query(
		&self,
		context: Option<&rpc::RequestContext>,
		query: String,
		variables: Option<proto::Variables>,
		batch_records: usize,
	) -> Result<Response<ResponseStream<rpc::QueryResponse>>, Status> {
		// Parse the transaction id before resolving a session, for the reason
		// `execute` gives: an early return between the two would strand an
		// ephemeral session in the map with nothing left to remove it.
		let txn = match context.and_then(|context| context.transaction.as_ref()) {
			Some(txn) => Some(to_uuid(txn)?),
			None => None,
		};
		let session = self.resolve(context, true).await?;
		if let Some(txn) = txn
			&& !self.rpc().transaction_belongs_to(&txn, session.id)
		{
			self.release(&session).await;
			return Err(to_status(&invalid_params("Transaction not found")));
		}

		let variables = match variables {
			Some(variables) => match from_proto_variables(variables) {
				Ok(variables) => variables,
				Err(status) => {
					self.release(&session).await;
					return Err(status);
				}
			},
			None => Value::None,
		};
		let params = Array::from(vec![Value::String(query), variables]);

		let (items_tx, items_rx) = bounded(QUERY_STREAM_BUFFER);
		// The handle this request cancels with. The frames own it, so it trips
		// when the client stops reading -- see `ReleaseOnDrop`.
		let cancel = CancelHandle::new();
		// A client's own deadline, which `GrpcService::execute` applies for
		// buffered queries and this path would otherwise drop on the floor.
		let requested_timeout = context
			.and_then(|context| context.timeout)
			.and_then(|timeout| Duration::try_from(timeout).ok());
		let job = match RpcProtocol::query_stream(
			self.rpc(),
			txn,
			session.id,
			params,
			Some(cancel.clone()),
			items_tx,
		)
		.await
		{
			Ok(job) => job,
			Err(error) => {
				self.release(&session).await;
				return Err(to_status(&error));
			}
		};

		// A failure before execution began -- a parse error, a denied
		// capability, an unknown transaction -- has already been returned
		// above, which is what the protocol asks for: those end the RPC with a
		// transport error and no frames at all.
		let begin = rpc::QueryResponse {
			frame: Some(rpc::query_response::Frame::Begin(rpc::QueryBegin {
				query_id: Some(proto::Uuid::from_uuid(Uuid::new_v4())),
				statement_count: job.statement_count as u32,
			})),
		};

		// The execution is polled only when the client reads, so a deadline
		// awaited alongside it would never fire for a client that stops
		// reading -- and that client would pin an open read snapshot for as
		// long as it held the stream. This watchdog is driven by the runtime
		// instead, so it fires regardless.
		//
		// Tripping the handle alone is not enough: the execution parks on a
		// full `items` channel rather than at a yield point, so the receiver is
		// closed too, which fails that send and lets the executor finalise its
		// transaction on the way out.
		if let Some(deadline) = shortest(requested_timeout, self.rpc().kvs().query_timeout()) {
			let cancel = cancel.clone();
			let items = items_rx.clone();
			tokio::spawn(async move {
				tokio::time::sleep(deadline).await;
				cancel.trip();
				items.close();
			});
		}

		let state = QueryFraming::new(
			Arc::clone(&self.state),
			session,
			job,
			items_rx,
			batch_records,
			cancel,
		);
		let frames = futures::stream::unfold(Some(state), |state| async move {
			let mut state = state?;
			state.next().await.map(|frame| (Ok(frame), Some(state)))
		});
		Ok(Response::new(Box::pin(futures::stream::once(async move { Ok(begin) }).chain(frames))))
	}

	/// Registers a live query for a subscription that will own it.
	///
	/// The query runs before it can be checked, and running it is what
	/// registers any `LIVE SELECT` it carries. A registration is only kept
	/// alive by the subscription that owns it, so every one this call does not
	/// hand back is discarded here -- otherwise a query the check rejects
	/// leaves a live query behind with no subscriber and nothing to remove it
	/// before the session ends.
	async fn register_live_query(
		&self,
		context: Option<&rpc::RequestContext>,
		registration: rpc::LiveQueryRegistration,
	) -> Result<Uuid, Status> {
		let results = self.run_query(context, registration.query, registration.variables).await?;
		let registered: Vec<Uuid> = results
			.iter()
			.filter(|result| result.query_type == QueryType::Live)
			.filter_map(|result| match &result.result {
				Ok(Value::Uuid(id)) => Some(id.into_inner()),
				_ => None,
			})
			.collect();
		let outcome = single_live_query(results);
		let kept = outcome.as_ref().ok().copied();
		for id in registered {
			if Some(id) != kept {
				self.rpc().discard_live_query(&id).await;
			}
		}
		outcome
	}

	/// Attaches a subscription to a live query, returning its stream.
	fn attach_subscription(
		&self,
		session_id: Uuid,
		live_query_id: Uuid,
		owned: bool,
	) -> Result<Response<ResponseStream<rpc::SubscribeResponse>>, Status> {
		// One slot beyond the notification buffer, held back so a terminal frame
		// always fits -- a subscriber that stops reading is told why its stream
		// ended rather than seeing it close.
		let (sender, receiver) = mpsc::channel(*GRPC_NOTIFICATION_BUFFER + 1);
		let subscription_id = Uuid::new_v4();
		{
			let Some(mut entry) = self.rpc().live_queries.get_mut(&live_query_id) else {
				return Err(Status::not_found(format!("Live query {live_query_id} not found")));
			};
			// A live query is only visible to the session that registered it:
			// without this check any caller could name another session's live
			// query id and receive its changes.
			if entry.session_id != session_id {
				return Err(Status::not_found(format!("Live query {live_query_id} not found")));
			}
			if entry.subscriber.is_some() {
				return Err(Status::already_exists(format!(
					"Live query {live_query_id} already has a subscriber"
				)));
			}
			entry.subscriber = Some(Subscription {
				id: subscription_id,
				frames: sender,
			});
		}
		let begin = rpc::SubscribeResponse {
			frame: Some(rpc::subscribe_response::Frame::Begin(rpc::SubscribeBegin {
				subscription_id: Some(proto::Uuid::from_uuid(subscription_id)),
				live_query_id: Some(proto::Uuid::from_uuid(live_query_id)),
				cursor: None,
			})),
		};
		let guard = SubscriptionGuard {
			state: Arc::clone(&self.state),
			live_query_id,
			subscription_id,
			owned,
		};
		// The guard travels in the stream's state so that dropping the stream
		// -- which is how a client unsubscribes -- runs the teardown.
		let notifications =
			futures::stream::unfold((receiver, guard), |(mut receiver, guard)| async move {
				receiver.recv().await.map(|item| (item, (receiver, guard)))
			});
		let stream = futures::stream::once(async move { Ok(begin) }).chain(notifications);
		Ok(Response::new(Box::pin(stream)))
	}

	/// Applies a streamed SurrealQL import.
	async fn run_import(
		&self,
		mut frames: tonic::Streaming<rpc::ImportSurqlRequest>,
	) -> Result<Response<rpc::ImportSurqlResponse>, Status> {
		use futures::StreamExt;

		// The context rides on the opening frame, so the session cannot be
		// resolved until it arrives.
		let Some(first) = frames.next().await.transpose()? else {
			return Err(Status::invalid_argument("Import stream carried no frames"));
		};
		let Some(rpc::import_surql_request::Frame::Begin(begin)) = first.frame else {
			return Err(Status::invalid_argument("Import stream must open with a begin frame"));
		};
		let session = self.session_for(begin.context.as_ref(), RouteTarget::Import).await?;
		self.kvs()
			.check(
				&session,
				surrealdb_core::iam::Action::Edit,
				surrealdb_core::iam::ResourceKind::Any.on_level(session.au.level().to_owned()),
			)
			.map_err(|err| Status::permission_denied(err.to_string()))?;

		// Frame the request stream as the byte stream the importer consumes,
		// verifying the trailer as the bytes go past so a truncated or
		// corrupted transfer is reported rather than silently half-applied.
		//
		// The transfer is bounded as it goes. `max_decoding_message_size` caps
		// one chunk, not a stream of them, so without this the HTTP route's
		// import limit would be an operator control that `grpc://` ignores.
		let limit = *HTTP_MAX_IMPORT_BODY_SIZE as u64;
		let (bytes_tx, bytes_rx) = surrealdb::channel::bounded::<anyhow::Result<bytes::Bytes>>(1);
		let trailer = tokio::spawn(async move {
			let mut hasher = blake3::Hasher::new();
			let mut streamed: u64 = 0;
			while let Some(frame) = frames.next().await {
				let frame = match frame {
					Ok(frame) => frame,
					Err(status) => {
						bytes_tx.send(Err(anyhow::anyhow!("{}", status.message()))).await.ok();
						return Err(status);
					}
				};
				match frame.frame {
					Some(rpc::import_surql_request::Frame::Chunk(chunk)) => {
						streamed += chunk.data.len() as u64;
						if streamed > limit {
							let refused = Status::resource_exhausted(format!(
								"Import exceeds the {limit} byte limit"
							));
							bytes_tx.send(Err(anyhow::anyhow!("{}", refused.message()))).await.ok();
							return Err(refused);
						}
						hasher.update(&chunk.data);
						if bytes_tx.send(Ok(chunk.data)).await.is_err() {
							// The importer stopped reading, which means it
							// failed; its own error is the one to report.
							return Ok(());
						}
					}
					Some(rpc::import_surql_request::Frame::Trailer(trailer)) => {
						drop(bytes_tx);
						return verify_trailer(&trailer, streamed, hasher.finalize());
					}
					Some(rpc::import_surql_request::Frame::Begin(_)) => {
						return Err(Status::invalid_argument(
							"Import stream carried a second begin frame",
						));
					}
					None => {}
				}
			}
			// A byte stream is complete only once its trailer arrives.
			Err(Status::data_loss(
				"Import stream ended without a trailer; the import may be partially applied",
			))
		});

		let result = self.kvs().import_stream(&session, bytes_rx).await;
		// Take the framing verdict first: an import that failed *because* the
		// transfer was truncated should report the truncation, not the parse
		// error the truncation caused.
		match trailer.await {
			Ok(Ok(())) => {}
			Ok(Err(status)) => return Err(status),
			Err(err) => return Err(Status::internal(format!("Import framing task failed: {err}"))),
		}
		result.map_err(|err| Status::invalid_argument(err.to_string()))?;
		Ok(Response::new(rpc::ImportSurqlResponse {}))
	}

	/// Applies a streamed SurrealML model.
	///
	/// Unlike the SurrealQL import this cannot be applied as it arrives: the
	/// model's own header carries the name and version it is stored under, and
	/// that is only readable once the whole file is in hand. The transfer is
	/// bounded as it goes for the same reason the SurrealQL one is.
	#[cfg(feature = "ml")]
	async fn run_ml_import(
		&self,
		mut frames: tonic::Streaming<rpc::ImportMlModelRequest>,
	) -> Result<Response<rpc::ImportMlModelResponse>, Status> {
		use futures::StreamExt;

		let Some(first) = frames.next().await.transpose()? else {
			return Err(Status::invalid_argument("Import stream carried no frames"));
		};
		let Some(rpc::import_ml_model_request::Frame::Begin(begin)) = first.frame else {
			return Err(Status::invalid_argument("Import stream must open with a begin frame"));
		};
		let session = self.session_for(begin.context.as_ref(), RouteTarget::Ml).await?;
		let (namespace, database) =
			check_ns_db(&session).map_err(|err| Status::failed_precondition(err.to_string()))?;
		self.kvs()
			.check(
				&session,
				surrealdb_core::iam::Action::Edit,
				surrealdb_core::iam::ResourceKind::Model.on_db(&namespace, &database),
			)
			.map_err(|err| Status::permission_denied(err.to_string()))?;

		let limit = *HTTP_MAX_IMPORT_BODY_SIZE;
		let mut hasher = blake3::Hasher::new();
		let mut model = Vec::new();
		let mut trailer = None;
		while let Some(frame) = frames.next().await {
			match frame?.frame {
				Some(rpc::import_ml_model_request::Frame::Chunk(chunk)) => {
					if model.len() + chunk.data.len() > limit {
						return Err(Status::resource_exhausted(format!(
							"Import exceeds the {limit} byte limit"
						)));
					}
					hasher.update(&chunk.data);
					model.extend_from_slice(&chunk.data);
				}
				Some(rpc::import_ml_model_request::Frame::Trailer(sent)) => {
					trailer = Some(sent);
					break;
				}
				Some(rpc::import_ml_model_request::Frame::Begin(_)) => {
					return Err(Status::invalid_argument(
						"Import stream carried a second begin frame",
					));
				}
				None => {}
			}
		}
		// A byte stream is complete only once its trailer arrives.
		let trailer = trailer.ok_or_else(|| {
			Status::data_loss("Import stream ended without a trailer; the model was not stored")
		})?;
		verify_trailer(&trailer, model.len() as u64, hasher.finalize())?;

		let file = surrealdb_core::ml::storage::surml_file::SurMlFile::from_bytes(model)
			.map_err(|err| Status::invalid_argument(format!("Invalid SurrealML file: {err}")))?;
		let (name, version) = (file.header.name.to_string(), file.header.version.to_string());
		// The name and version the model is stored under come from its header,
		// so a file that carries neither has nowhere to go.
		if name.is_empty() || version.is_empty() {
			return Err(Status::invalid_argument("Model name and version must be set"));
		}
		// `Begin` names the model too; refusing a disagreement beats storing it
		// under a name the caller did not ask for.
		if !begin.name.is_empty() && begin.name != name {
			return Err(Status::invalid_argument(format!(
				"The request names model {} but the file carries {name}",
				begin.name
			)));
		}
		if !begin.version.is_empty() && begin.version != version {
			return Err(Status::invalid_argument(format!(
				"The request names version {} but the file carries {version}",
				begin.version
			)));
		}
		let description = file.header.description.to_string();
		self.kvs()
			.put_ml_model(&session, &name, &version, &description, file.to_bytes())
			.await
			.map_err(|err| Status::internal(err.to_string()))?;
		Ok(Response::new(rpc::ImportMlModelResponse {}))
	}

	/// SurrealML support is a build-time feature; without it there is nowhere
	/// to store a model. `GetCapabilities` omits `ML_MODELS` accordingly.
	#[cfg(not(feature = "ml"))]
	async fn run_ml_import(
		&self,
		_frames: tonic::Streaming<rpc::ImportMlModelRequest>,
	) -> Result<Response<rpc::ImportMlModelResponse>, Status> {
		Err(Status::unimplemented("This server was built without SurrealML support"))
	}

	/// Starts a SurrealQL export, handing back the channel its chunks arrive on
	/// and the handle that reports whether it finished.
	async fn start_export(
		&self,
		session: Session,
		config: export::Config,
	) -> Result<Export, Status> {
		let (namespace, database) =
			check_ns_db(&session).map_err(|err| Status::failed_precondition(err.to_string()))?;
		self.kvs()
			.check(
				&session,
				surrealdb_core::iam::Action::View,
				surrealdb_core::iam::ResourceKind::Any.on_db(&namespace, &database),
			)
			.map_err(|err| Status::permission_denied(err.to_string()))?;
		let (sender, chunks) = surrealdb::channel::bounded(1);
		let task = self
			.kvs()
			.export_with_config(&session, sender, config)
			.await
			.map_err(|err| Status::internal(err.to_string()))?;
		Ok(Export {
			chunks,
			outcome: tokio::spawn(task),
		})
	}

	/// Starts a SurrealML model export, handing back the channel its chunks
	/// arrive on and the handle that reports whether it finished.
	#[cfg(feature = "ml")]
	async fn start_ml_export(
		&self,
		session: Session,
		name: String,
		version: String,
	) -> Result<Export, Status> {
		let (namespace, database) =
			check_ns_db(&session).map_err(|err| Status::failed_precondition(err.to_string()))?;
		self.kvs()
			.check(
				&session,
				surrealdb_core::iam::Action::View,
				surrealdb_core::iam::ResourceKind::Model.on_db(&namespace, &database),
			)
			.map_err(|err| Status::permission_denied(err.to_string()))?;
		let info = self
			.kvs()
			.get_db_model(&namespace, &database, &name, &version)
			.await
			.map_err(|err| Status::internal(err.to_string()))?
			.ok_or_else(|| Status::not_found(format!("Model {name} {version} not found")))?;
		let path = format!("ml/{namespace}/{database}/{name}-{version}-{}.surml", info.hash);
		let mut data = surrealdb_core::obs::stream(path)
			.await
			.map_err(|err| Status::internal(format!("Failed to read model file: {err}")))?;
		let (sender, chunks) = surrealdb::channel::bounded(1);
		let outcome = tokio::spawn(async move {
			while let Some(chunk) = data.next().await {
				// A read failure has to reach the stream: stopping quietly
				// would frame a partial model as a complete one.
				let chunk = chunk.map_err(|err| anyhow::anyhow!("{err}"))?;
				if sender.send(chunk.to_vec()).await.is_err() {
					break;
				}
			}
			Ok(())
		});
		Ok(Export {
			chunks,
			outcome,
		})
	}

	/// SurrealML support is a build-time feature; without it there are no
	/// models to export. `GetCapabilities` omits `ML_MODELS` accordingly.
	#[cfg(not(feature = "ml"))]
	async fn start_ml_export(
		&self,
		_session: Session,
		_name: String,
		_version: String,
	) -> Result<Export, Status> {
		Err(Status::unimplemented("This server was built without SurrealML support"))
	}
}

/// An export in flight: the chunks it is producing, and the handle reporting
/// whether it ran to completion.
///
/// Both are needed to frame the stream correctly. The chunk channel closing
/// means the export stopped, but not *why*: a failure part-way through closes
/// it exactly as success does. Reading the outcome is what tells a trailer
/// (this is all of it) from an error frame (this is not).
struct Export {
	chunks: surrealdb::channel::Receiver<Vec<u8>>,
	outcome: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// A guard that tears a subscription down when its stream is dropped.
///
/// A client unsubscribes by dropping the stream, so this is the only signal
/// that a subscription has ended.
struct SubscriptionGuard {
	state: Arc<RpcState>,
	live_query_id: Uuid,
	/// Which subscription this guard ends. A stream is dropped whenever the
	/// client gets round to it, which may be after it has subscribed again, so
	/// the detach below only fires if this is still the subscription attached.
	subscription_id: Uuid,
	/// Whether the live query was created by this subscription, and so should
	/// be killed along with it.
	owned: bool,
}

impl Drop for SubscriptionGuard {
	fn drop(&mut self) {
		let state = Arc::clone(&self.state);
		let live_query_id = self.live_query_id;
		if !self.owned {
			// The live query outlives its subscribers; just detach.
			if let Some(mut entry) = state.grpc.live_queries.get_mut(&live_query_id)
				&& entry.subscriber.as_ref().is_some_and(|s| s.id == self.subscription_id)
			{
				entry.subscriber = None;
			}
			return;
		}
		// A live query registered by `Subscribe` is bound to that stream, so
		// ending the stream kills it. Teardown is asynchronous, so it runs on
		// its own task.
		tokio::spawn(async move {
			state.grpc.forget_live_query(&live_query_id);
			if let Err(err) = state.grpc.kvs.delete_queries(vec![live_query_id]).await {
				error!("Error killing subscription-owned live query {live_query_id}: {err}");
			}
		});
	}
}

/// The frames a streamed byte transfer is made of.
enum ByteFrame {
	Chunk(rpc::DataChunk),
	Trailer(rpc::DataTrailer),
	Error(proto::SurrealError),
}

/// The sooner of two optional deadlines.
fn shortest(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
	match (a, b) {
		(Some(a), Some(b)) => Some(a.min(b)),
		(deadline, None) | (None, deadline) => deadline,
	}
}

/// A streaming execution in flight, as [`QueryStreamJob::run`] hands it over.
type QueryStreamRun = Pin<Box<dyn Future<Output = Result<Vec<QueryResult>, TypesError>> + Send>>;

/// The first batch a statement sends, in records.
///
/// The ramp exists for time-to-first-row: a client waiting on a large `SELECT`
/// sees something after this many records rather than after a full batch. Each
/// subsequent batch doubles up to the negotiated maximum, so the small frames
/// are confined to the start and a long result still costs one frame per
/// [`batch_records`] rows.
const QUERY_FIRST_BATCH_RECORDS: usize = 16;

/// Releases a streaming query's resources when the stream carrying its results
/// is dropped.
///
/// A dropped stream means the client stopped reading, and that is the only
/// signal for it: the framing state is simply never polled again, so nothing
/// after the last `next()` runs. Everything the request owns therefore has to
/// be released from here rather than at the end of the stream.
///
/// Tripping the cancel handle is what stops the query. Waiting for the next
/// send to fail would not be enough: a query inside a phase that emits nothing
/// for a while -- a sort, an aggregate -- would run to completion first.
/// Cancelling this way rather than dropping the execution future is deliberate:
/// the future owns an open transaction, and dropping it would leave that
/// transaction neither committed nor cancelled.
struct ReleaseOnDrop {
	cancel: CancelHandle,
	/// The ephemeral session to remove, for a request that named none. A
	/// client-named session outlives the request and is left alone.
	ephemeral: Option<(Arc<RpcState>, Uuid)>,
}

impl ReleaseOnDrop {
	fn new(cancel: CancelHandle, state: Arc<RpcState>, session: &ResolvedSession) -> Self {
		Self {
			cancel,
			ephemeral: session.client.is_none().then_some((state, session.id)),
		}
	}

	/// Give up ownership of the ephemeral session, for the path that has
	/// already released it.
	fn released(&mut self) {
		self.ephemeral = None;
	}
}

impl Drop for ReleaseOnDrop {
	fn drop(&mut self) {
		self.cancel.trip();
		// Removing a session awaits, which a `Drop` cannot, so it goes to a
		// task. Nothing else holds a handle on this session -- the request that
		// made it is over -- so there is no ordering to preserve.
		if let Some((state, id)) = self.ephemeral.take() {
			tokio::spawn(async move {
				state.grpc.remove_ephemeral_session(&id).await;
			});
		}
	}
}

/// Turns a streaming execution's items into query response frames.
///
/// Holds one statement's rows back only until they fill a batch, so the wire
/// sees them long before the statement -- or the query -- has finished.
struct QueryFraming {
	state: Arc<RpcState>,
	session: ResolvedSession,
	/// The execution, until it has been driven to completion.
	run: Option<QueryStreamRun>,
	items: Receiver<QueryStreamItem>,
	/// The framing decisions, which do not depend on how the items arrive.
	frames: QueryFrames,
	started: Instant,
	/// Set once the terminating frame has been queued, which stops the stream.
	done: bool,
	/// Cancels the query and releases the request's session when this struct is
	/// dropped, i.e. when the client stops reading.
	release: ReleaseOnDrop,
}

/// Turns a streaming execution's items into query response frames.
///
/// Separate from [`QueryFraming`] because none of these decisions depend on how
/// the items arrive: a statement's rows are framed the same whether they were
/// produced one batch at a time or all at once.
struct QueryFrames {
	/// Rows waiting to be framed, and how many batches each statement has sent.
	pending: StdHashMap<u32, PendingStatement>,
	/// Frames produced but not yet yielded, in order.
	queued: VecDeque<rpc::QueryResponse>,
	/// The largest batch this client accepted.
	batch_records: usize,
	/// Statements already terminated, so a second attempt is ignored.
	///
	/// Two things can end a statement: its own `Finished` item, and a value
	/// this server could not encode. Both have to terminate it -- an encode
	/// failure cannot be deferred to the executor, which does not know about
	/// it -- so whichever happens first wins and the other is dropped. Sending
	/// both would put a frame after a terminal one, which the protocol forbids,
	/// and count the statement twice in `End.result_count`.
	terminated: StdHashSet<u32>,
}

/// One statement's rows on their way to the wire.
#[derive(Default)]
struct PendingStatement {
	values: Vec<proto::Value>,
	/// How many batches have gone out, which is the frame's `batch_index`.
	batches: u64,
	/// Records to accumulate before flushing, doubling per batch up to the
	/// client's maximum.
	target: usize,
	/// Records sent so far, which the terminal batch reports as the statement's
	/// `records_returned`.
	sent: i64,
	/// Set when the statement's value is a single value rather than a list.
	///
	/// This decides the terminal batch's `kind`, and it cannot be inferred from
	/// the count: a `SELECT` returning one row is still a one-element array,
	/// where `SELECT ONLY` returning one row is that row.
	single: bool,
}

impl PendingStatement {
	/// A statement with nothing sent yet, whose first batch is the small one
	/// the ramp starts from.
	fn new(batch_records: usize) -> Self {
		Self {
			target: QUERY_FIRST_BATCH_RECORDS.min(batch_records).max(1),
			..Default::default()
		}
	}
}

impl QueryFraming {
	fn new(
		state: Arc<RpcState>,
		session: ResolvedSession,
		job: QueryStreamJob,
		items: Receiver<QueryStreamItem>,
		batch_records: usize,
		cancel: CancelHandle,
	) -> Self {
		let release = ReleaseOnDrop::new(cancel, Arc::clone(&state), &session);
		Self {
			state,
			session,
			run: Some(job.run),
			items,
			frames: QueryFrames::new(batch_records),
			started: Instant::now(),
			done: false,
			release,
		}
	}

	/// The next frame, or `None` once the stream is complete.
	async fn next(&mut self) -> Option<rpc::QueryResponse> {
		loop {
			if let Some(frame) = self.frames.pop() {
				return Some(frame);
			}
			if self.done {
				// The session outlives the request that made it only when the
				// client named one; an ephemeral session is this stream's, and
				// nothing else will remove it. Doing it here rather than
				// leaving it to the drop guard keeps it awaited, so a caller
				// that reads the stream to its end sees the session gone.
				if self.session.client.is_none() {
					let state = Arc::clone(&self.state);
					let id = self.session.id;
					self.release.released();
					state.grpc.remove_ephemeral_session(&id).await;
				}
				return None;
			}
			// Drive the execution and the drain together: the channel is
			// bounded, so awaiting either alone would deadlock against the
			// other.
			let run = self.run.as_mut().expect("the execution is driven until it completes");
			tokio::select! {
				item = self.items.recv() => match item {
					Ok(item) => self.frames.absorb(item),
					// The execution dropped its sender, so the only thing left
					// is its own outcome.
					Err(_) => {
						let outcome = self.run.take().expect("still running").await;
						self.finish(outcome).await;
					}
				},
				outcome = run => {
					// The execution finished before its channel drained. Take
					// what is still buffered before terminating.
					self.run = None;
					while let Ok(item) = self.items.try_recv() {
						self.frames.absorb(item);
					}
					self.finish(outcome).await;
				}
			}
		}
	}

	/// Queue the frame that ends the stream.
	async fn finish(&mut self, outcome: Result<Vec<QueryResult>, TypesError>) {
		self.done = true;
		match outcome {
			Ok(results) => {
				let state = Arc::clone(&self.state);
				let session_id = self.session.id;
				register_live_queries(&state, session_id, &results).await;
				self.frames.end(self.started.elapsed());
			}
			// A failure that belongs to no single statement ends the stream
			// instead of completing it.
			Err(error) => self.frames.error(&error),
		}
	}
}

impl QueryFrames {
	fn new(batch_records: usize) -> Self {
		Self {
			pending: StdHashMap::new(),
			queued: VecDeque::new(),
			batch_records,
			terminated: StdHashSet::new(),
		}
	}

	/// The next frame produced so far, in order.
	fn pop(&mut self) -> Option<rpc::QueryResponse> {
		self.queued.pop_front()
	}

	/// Queue the frame that completes the stream successfully.
	fn end(&mut self, elapsed: Duration) {
		self.queued.push_back(rpc::QueryResponse {
			frame: Some(rpc::query_response::Frame::End(rpc::QueryEnd {
				result_count: self.terminated.len() as u32,
				execution_duration: proto::Duration::try_from(elapsed).ok(),
			})),
		});
	}

	/// Queue the frame that ends the stream without completing it.
	fn error(&mut self, error: &TypesError) {
		self.queued.push_back(rpc::QueryResponse {
			frame: Some(rpc::query_response::Frame::Error(to_proto_error(error))),
		});
	}

	/// Fold one item into the frames it produces.
	fn absorb(&mut self, item: QueryStreamItem) {
		// A statement this server already terminated -- because a value of its
		// own could not be encoded -- takes nothing further. Its rows would
		// otherwise be framed after its terminal batch, which the protocol
		// forbids and a client reading rows as they arrive would act on.
		if self.terminated.contains(&(item.index() as u32)) {
			return;
		}
		match item {
			QueryStreamItem::Rows {
				index,
				values,
			} => {
				let index = index as u32;
				let batch_records = self.batch_records;
				let entry = self
					.pending
					.entry(index)
					.or_insert_with(|| PendingStatement::new(batch_records));
				match try_values(values.into_iter()) {
					Ok(mut encoded) => entry.values.append(&mut encoded),
					// A value the wire cannot carry fails only the statement
					// that produced it, exactly as its own error would.
					Err(error) => {
						self.pending.remove(&index);
						self.queue_terminal(
							index,
							rpc::QueryStatementKind::Other,
							Some(to_proto_error(&error)),
							0,
						);
						return;
					}
				}
				while self.pending.get(&index).is_some_and(|p| p.values.len() >= p.target) {
					self.flush(index);
				}
			}
			QueryStreamItem::Value {
				index,
				value,
			} => {
				let index = index as u32;
				let batch_records = self.batch_records;
				match proto::Value::try_from(value).map_err(types_error_from_anyhow) {
					Ok(value) => {
						// A single value is not a list, so it is not batched:
						// the terminal frame carries it whole, marked `SINGLE`
						// so the client does not rebuild an array around it.
						let entry = self
							.pending
							.entry(index)
							.or_insert_with(|| PendingStatement::new(batch_records));
						entry.values.push(value);
						entry.single = true;
					}
					Err(error) => {
						self.pending.remove(&index);
						self.queue_terminal(
							index,
							rpc::QueryStatementKind::Other,
							Some(to_proto_error(&error)),
							0,
						);
					}
				}
			}
			QueryStreamItem::Finished {
				index,
				time,
				query_type,
				error,
			} => {
				let index = index as u32;
				let kind = match query_type {
					QueryType::Live => rpc::QueryStatementKind::Live,
					QueryType::Kill => rpc::QueryStatementKind::Kill,
					_ => rpc::QueryStatementKind::Other,
				};
				self.queue_terminal(
					index,
					kind,
					error.map(|e| to_proto_error(&e)),
					time.as_nanos(),
				);
			}
		}
	}

	/// Emit a full batch for `index`, leaving the statement open.
	fn flush(&mut self, index: u32) {
		let Some(entry) = self.pending.get_mut(&index) else {
			return;
		};
		let take = entry.target.min(entry.values.len());
		let values: Vec<proto::Value> = entry.values.drain(..take).collect();
		let batch_index = entry.batches;
		entry.batches += 1;
		entry.sent += values.len() as i64;
		// Ramp toward the client's maximum, so the small frames that make the
		// first row arrive early do not become a per-frame cost on a long result.
		entry.target = (entry.target * 2).min(self.batch_records).max(1);
		self.queued.push_back(rpc::QueryResponse {
			frame: Some(rpc::query_response::Frame::Batch(rpc::QueryBatchFrame {
				query_index: index,
				batch_index,
				// A statement's kind is only known once it finishes, and only
				// the terminal frame needs it: `LIVE` and `KILL` produce a
				// single value rather than rows, so nothing that reaches a
				// non-terminal batch is anything but an ordinary statement.
				statement_kind: rpc::QueryStatementKind::Other as i32,
				kind: rpc::QueryResponseKind::Batched as i32,
				stats: None,
				error: None,
				payload: Some(rpc::query_batch_frame::Payload::Values(rpc::ValueBatch {
					values,
				})),
			})),
		});
	}

	/// Emit the frame that completes a statement, carrying whatever is left.
	fn queue_terminal(
		&mut self,
		index: u32,
		kind: rpc::QueryStatementKind,
		error: Option<proto::SurrealError>,
		elapsed_nanos: u128,
	) {
		if !self.terminated.insert(index) {
			// Already terminated; see `terminated`.
			return;
		}
		let entry = self.pending.remove(&index).unwrap_or_default();
		// A statement whose rows were split reports the count it actually sent;
		// one that failed reports nothing, since its rows are retracted.
		let (payload, records) = if error.is_some() {
			(None, 0)
		} else {
			let records = entry.sent + entry.values.len() as i64;
			(
				Some(rpc::query_batch_frame::Payload::Values(rpc::ValueBatch {
					values: entry.values,
				})),
				records,
			)
		};
		let single = error.is_none() && entry.single;
		let stats = rpc::QueryStats {
			records_returned: records,
			bytes_returned: -1,
			records_scanned: -1,
			bytes_scanned: -1,
			execution_duration: u64::try_from(elapsed_nanos)
				.ok()
				.and_then(|nanos| proto::Duration::try_from(Duration::from_nanos(nanos)).ok()),
		};
		self.queued.push_back(rpc::QueryResponse {
			frame: Some(rpc::query_response::Frame::Batch(rpc::QueryBatchFrame {
				query_index: index,
				batch_index: entry.batches,
				kind: if single {
					rpc::QueryResponseKind::Single as i32
				} else {
					rpc::QueryResponseKind::BatchedFinal as i32
				},
				statement_kind: kind as i32,
				stats: Some(stats),
				error,
				payload,
			})),
		});
	}
}

/// Track the live queries an execution registered, and forget the ones it
/// killed.
///
/// A `LIVE SELECT` produces its id as an ordinary result, but the id is only
/// useful once the transport knows which session to deliver that query's
/// notifications to. The buffered path does this over the finished results; a
/// streaming one waits for the same point, since a registration inside a
/// transaction block is not real until the block commits.
async fn register_live_queries(state: &RpcState, session_id: Uuid, results: &[QueryResult]) {
	if !results.iter().any(|r| matches!(r.query_type, QueryType::Live | QueryType::Kill)) {
		return;
	}
	// Read the session's namespace and database once, and drop the guard before
	// calling the hooks: `handle_live` takes the same lock, and a concurrent
	// session-mutating request queued between two reads on a write-preferring
	// lock would deadlock both.
	let (namespace, database) = match state.grpc.get_session(&session_id).await {
		Ok(lock) => {
			let session = lock.read().await;
			(session.ns.clone(), session.db.clone())
		}
		Err(_) => (None, None),
	};
	for result in results {
		let Ok(Value::Uuid(id)) = &result.result else {
			continue;
		};
		match result.query_type {
			QueryType::Live => {
				state.grpc.handle_live(id, session_id, namespace.clone(), database.clone()).await;
			}
			QueryType::Kill => state.grpc.handle_kill(id).await,
			QueryType::Other => {}
		}
	}
}

/// Frames an export as a byte stream: chunks, then a trailer stating the
/// totals so the receiver can tell a complete transfer from a truncated one --
/// or, if the export failed part-way, an error frame in the trailer's place so
/// a partial transfer is never mistaken for a whole one.
fn frame_byte_stream<T, F>(export: Export, wrap: F) -> impl Stream<Item = Result<T, Status>> + Send
where
	F: Fn(ByteFrame) -> T + Send + 'static,
	T: Send + 'static,
{
	/// The state the stream carries between frames. A struct rather than a
	/// stage enum because the hasher is large, and an enum would size every
	/// variant to hold it.
	struct Framing {
		/// `None` once the terminating frame has been emitted, which is what
		/// stops the stream.
		export: Option<Export>,
		hasher: blake3::Hasher,
		streamed: u64,
	}

	futures::stream::unfold(
		(
			Framing {
				export: Some(export),
				hasher: blake3::Hasher::new(),
				streamed: 0,
			},
			wrap,
		),
		|(mut framing, wrap)| async move {
			let export = framing.export.take()?;
			match export.chunks.recv().await {
				Ok(chunk) => {
					framing.hasher.update(&chunk);
					framing.streamed += chunk.len() as u64;
					let frame = wrap(ByteFrame::Chunk(rpc::DataChunk {
						data: bytes::Bytes::from(chunk),
					}));
					framing.export = Some(export);
					Some((Ok(frame), (framing, wrap)))
				}
				// The channel closed, which happens both when the export
				// finished and when it gave up part-way. The task's own result
				// is what distinguishes them.
				Err(_) => {
					let frame = match export.outcome.await {
						Ok(Ok(())) => ByteFrame::Trailer(rpc::DataTrailer {
							bytes: framing.streamed,
							blake3: framing.hasher.finalize().to_hex().to_string(),
						}),
						Ok(Err(err)) => {
							error!("gRPC export failed: {err}");
							ByteFrame::Error(proto::SurrealError::new(
								proto::ErrorKind::Internal,
								"The export failed part-way through",
							))
						}
						Err(err) => {
							error!("gRPC export task panicked: {err}");
							ByteFrame::Error(proto::SurrealError::new(
								proto::ErrorKind::Internal,
								"The export failed part-way through",
							))
						}
					};
					// `framing.export` stays `None`, ending the stream after
					// this terminating frame.
					Some((Ok(wrap(frame)), (framing, wrap)))
				}
			}
		},
	)
}

/// The live query id a subscription's own query registered, or why the query
/// was not one a subscription can be built on.
fn single_live_query(mut results: Vec<QueryResult>) -> Result<Uuid, Status> {
	if results.len() != 1 {
		return Err(Status::invalid_argument("Expected exactly one LIVE SELECT statement"));
	}
	let result = results.remove(0);
	if result.query_type != QueryType::Live {
		return Err(Status::invalid_argument("Expected a LIVE SELECT statement"));
	}
	match result.result.map_err(|err| to_status(&err))? {
		Value::Uuid(id) => Ok(id.into_inner()),
		_ => Err(Status::internal("LIVE SELECT did not return a live query id")),
	}
}

/// Checks a byte stream's trailer against what actually arrived.
fn verify_trailer(
	trailer: &rpc::DataTrailer,
	streamed: u64,
	digest: blake3::Hash,
) -> Result<(), Status> {
	if trailer.bytes != streamed {
		return Err(Status::data_loss(format!(
			"Import trailer declared {} bytes but {streamed} arrived",
			trailer.bytes
		)));
	}
	// The checksum is optional; an empty one means the sender did not compute
	// it, which is different from computing a wrong one.
	if !trailer.blake3.is_empty() && !trailer.blake3.eq_ignore_ascii_case(&digest.to_hex()) {
		return Err(Status::data_loss("Import trailer checksum did not match the streamed bytes"));
	}
	Ok(())
}

/// Encodes values for the wire, reporting the first one it cannot carry.
fn try_values(values: impl Iterator<Item = Value>) -> Result<Vec<proto::Value>, TypesError> {
	values.map(|value| proto::Value::try_from(value).map_err(types_error_from_anyhow)).collect()
}

/// How many records to put in one batch, given what the client asked for.
///
/// A request above the server's own bound is clamped rather than refused, which
/// is what `QueryRequest.max_batch_records` requires: a client that asks for
/// more than it may have still gets its results. Zero means the client
/// expressed no preference, so the server chooses. The bound this clamps to is
/// the one reported as `Limits.max_batch_records`, so a client can discover it
/// rather than infer it from the batches it receives.
fn batch_records(requested: u32) -> usize {
	match requested as usize {
		0 => QUERY_BATCH_RECORDS,
		n => n.min(QUERY_BATCH_RECORDS),
	}
}

/// Renders a data-change notification in the shape a subscription streams.
///
/// The lifecycle actions (`Killed`, `Error`) have no `Notification` to render
/// into and are handled by the caller, which is why the action is passed
/// separately rather than read back off the notification.
fn to_proto_notification(
	notification: &Notification,
	action: surrealdb_types::Action,
) -> Result<rpc::Notification, TypesError> {
	let action = match action {
		surrealdb_types::Action::Create => rpc::Action::Created,
		surrealdb_types::Action::Update => rpc::Action::Updated,
		surrealdb_types::Action::Delete => rpc::Action::Deleted,
		// Unreachable: the caller matches these out first.
		surrealdb_types::Action::Killed | surrealdb_types::Action::Error => {
			rpc::Action::Unspecified
		}
	};
	let record_id = match &notification.record {
		Value::RecordId(record) => {
			Some(proto::RecordId::try_from(record.clone()).map_err(types_error_from_anyhow)?)
		}
		_ => None,
	};
	Ok(rpc::Notification {
		live_query_id: Some(proto::Uuid::from_uuid(notification.id.into_inner())),
		action: action as i32,
		record_id,
		value: Some(
			proto::Value::try_from(notification.result.clone()).map_err(types_error_from_anyhow)?,
		),
		cursor: None,
	})
}

/// Renders the tokens an auth method answered with.
///
/// The expiry fields are left unset: the methods answer with the tokens alone,
/// and a client that needs the expiry reads it from the access token's own
/// claims. Reporting a guessed expiry would be worse than reporting none.
fn to_tokens(result: DbResult) -> Result<rpc::Tokens, Status> {
	let DbResult::Other(value) = result else {
		return Err(Status::internal("Authentication did not return a token"));
	};
	// An access method that issues no token at all (record access without a
	// configured JWT) answers with nothing rather than an empty token.
	if matches!(value, Value::None | Value::Null) {
		return Ok(rpc::Tokens::default());
	}
	let (access, refresh) = match Token::from_value(value) {
		Ok(Token::Access(access)) => (access, String::new()),
		Ok(Token::WithRefresh {
			access,
			refresh,
		}) => (access, refresh),
		Err(err) => {
			return Err(Status::internal(format!("Authentication returned no token: {err}")));
		}
	};
	Ok(rpc::Tokens {
		access,
		refresh,
		expires_at: None,
		refresh_expires_at: None,
	})
}

/// Renders an access/refresh pair the way the `refresh` and `revoke` methods
/// expect to receive it.
fn token_value(access: String, refresh: String) -> Value {
	let token = if refresh.is_empty() {
		Token::Access(access)
	} else {
		Token::WithRefresh {
			access,
			refresh,
		}
	};
	token.into_value()
}

/// Flattens an access method back into the credentials object the `signin`
/// method parses, inverting what the SDK's engine builds.
fn access_credentials(access: rpc::AccessMethod) -> Result<surrealdb_types::Object, Status> {
	let method =
		access.method.ok_or_else(|| Status::invalid_argument("Expected an access method"))?;
	let mut object = surrealdb_types::Object::new();
	let mut set = |key: &str, value: String| {
		if !value.is_empty() {
			object.insert(key.to_string(), Value::String(value));
		}
	};
	match method {
		rpc::access_method::Method::User(user) => {
			set("ns", user.namespace);
			set("db", user.database);
			set("user", user.username);
			set("pass", user.password);
			set("ac", user.access);
		}
		rpc::access_method::Method::Bearer(bearer) => {
			set("ns", bearer.namespace);
			set("db", bearer.database);
			set("ac", bearer.access);
			set("key", bearer.key);
		}
		rpc::access_method::Method::Record(record) => {
			return record_credentials(record);
		}
	}
	Ok(object)
}

/// Flattens record credentials into the credentials object, with the access
/// method's own variables alongside the scope fields.
fn record_credentials(record: rpc::RecordCredentials) -> Result<surrealdb_types::Object, Status> {
	let mut object = match record.variables {
		Some(variables) => match from_proto_variables(variables)? {
			Value::Object(object) => object,
			_ => surrealdb_types::Object::new(),
		},
		None => surrealdb_types::Object::new(),
	};
	let mut set = |key: &str, value: String| {
		if !value.is_empty() {
			object.insert(key.to_string(), Value::String(value));
		}
	};
	set("ns", record.namespace);
	set("db", record.database);
	set("ac", record.access);
	Ok(object)
}

/// Maps the wire's export configuration onto the datastore's.
fn from_proto_export_config(config: rpc::ExportConfig) -> export::Config {
	use export::TableConfig;

	let tables = match config.tables.and_then(|tables| tables.selection) {
		Some(rpc::export_config::tables::Selection::All(_)) | None => TableConfig::All,
		Some(rpc::export_config::tables::Selection::None(_)) => TableConfig::None,
		Some(rpc::export_config::tables::Selection::Selected(selected)) => {
			TableConfig::Some(selected.tables)
		}
		Some(rpc::export_config::tables::Selection::Excluded(excluded)) => {
			TableConfig::Exclude(export::ExcludedTables {
				exclude: excluded.tables,
			})
		}
	};
	export::Config {
		users: config.users,
		accesses: config.accesses,
		params: config.params,
		functions: config.functions,
		analyzers: config.analyzers,
		tables,
		versions: config.versions,
		records: config.records,
		sequences: config.sequences,
		apis: config.apis,
		buckets: config.buckets,
		modules: config.modules,
		configs: config.configs,
	}
}

/// Maps a three-state namespace/database selection onto the value the `use`
/// method reads: absent leaves the selection alone, null clears it.
fn from_nullable(value: Option<rpc::NullableString>) -> Value {
	match value.and_then(|value| value.value) {
		Some(rpc::nullable_string::Value::Some(value)) => Value::String(value),
		Some(rpc::nullable_string::Value::Null(_)) => Value::Null,
		None => Value::None,
	}
}

/// Encodes a value for the wire, reporting one it cannot carry.
fn to_proto_value(value: Value) -> Result<proto::Value, Status> {
	proto::Value::try_from(value).map_err(|err| Status::internal(err.to_string()))
}

fn from_proto_value(value: proto::Value) -> Result<Value, Status> {
	Value::try_from(value).map_err(|err| Status::invalid_argument(err.to_string()))
}

fn from_proto_variables(variables: proto::Variables) -> Result<Value, Status> {
	let object = proto::Object {
		items: variables.variables,
	};
	from_proto_value(proto::Value {
		value: Some(proto::value::Value::Object(object)),
	})
}

fn to_uuid(uuid: &proto::Uuid) -> Result<Uuid, Status> {
	uuid.to_uuid().map_err(|err| Status::invalid_argument(err.to_string()))
}

/// Renders an error in the structured shape the protocol carries inside its
/// streams, preserving the kind, message and cause chain.
fn to_proto_error(error: &TypesError) -> proto::SurrealError {
	let kind = if error.is_validation() {
		proto::ErrorKind::Validation
	} else if error.is_configuration() {
		proto::ErrorKind::Configuration
	} else if error.is_query() {
		proto::ErrorKind::Query
	} else if error.is_serialization() {
		proto::ErrorKind::Serialization
	} else if error.is_not_allowed() {
		proto::ErrorKind::NotAllowed
	} else if error.is_not_found() {
		proto::ErrorKind::NotFound
	} else if error.is_already_exists() {
		proto::ErrorKind::AlreadyExists
	} else if error.is_connection() {
		proto::ErrorKind::Connection
	} else if error.is_thrown() {
		proto::ErrorKind::Thrown
	} else if error.is_context() {
		proto::ErrorKind::Context
	} else {
		proto::ErrorKind::Internal
	};
	let mut out = proto::SurrealError::new(kind, error.message());
	if let Some(cause) = error.cause() {
		out = out.with_cause(to_proto_error(cause));
	}
	out
}

/// Maps an error onto the gRPC status that terminates a unary call.
///
/// A status carries a code and a message but no structured error, so the
/// finer-grained detail an [`Error`](TypesError) holds survives only where the
/// protocol has a frame to put it in -- a query batch, or a stream's error
/// frame. The codes chosen here are the ones the SDK's engine maps back onto
/// the matching error kind.
fn to_status(error: &TypesError) -> Status {
	let message = error.message().to_string();
	if error.is_validation() {
		Status::invalid_argument(message)
	} else if error.is_configuration() {
		Status::unimplemented(message)
	} else if error.is_query() || error.is_thrown() {
		Status::aborted(message)
	} else if error.is_not_allowed() {
		Status::permission_denied(message)
	} else if error.is_not_found() {
		Status::not_found(message)
	} else if error.is_already_exists() {
		Status::already_exists(message)
	} else if error.is_connection() {
		Status::unavailable(message)
	} else {
		Status::internal(message)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_core::dbs::Capabilities;
	use surrealdb_types::{Object, SerializationError};

	use super::*;

	async fn service() -> GrpcService {
		service_for(Session::default()).await
	}

	/// A service whose requests arrive as `caller` -- the principal the auth
	/// middleware resolved from the request's own headers.
	async fn service_for(caller: Session) -> GrpcService {
		let datastore = Arc::new(
			Datastore::builder()
				.with_capabilities(Capabilities::all())
				.build_with_path("memory")
				.await
				.expect("datastore"),
		);
		GrpcService::new(Arc::new(RpcState::new(datastore)), caller)
	}

	fn register_live_query(service: &GrpcService, session_id: Uuid) -> Uuid {
		let id = Uuid::new_v4();
		service.rpc().live_queries.insert(
			id,
			LiveQuery {
				session_id,
				namespace: None,
				database: None,
				subscriber: None,
			},
		);
		id
	}

	/// A session starts life bound to nobody, so whoever holds its id may drive
	/// it -- including the caller that attached it while presenting
	/// credentials of its own. Comparing principals before the session has one
	/// would lock that caller out of the session it just created, with no way
	/// to sign in on it.
	#[tokio::test]
	async fn a_credentialed_caller_may_use_the_session_it_attached() {
		let service = service_for(Session::owner()).await;
		let session_id = Uuid::new_v4();
		service.rpc().attach(session_id).await.expect("attach");

		service
			.rpc()
			.verify_caller_for_session(&session_id, service.caller.au.as_ref())
			.await
			.expect("the caller that attached the session may use it");
	}

	/// Once a session is bound to a principal, a caller that authenticated at
	/// the transport level must be that principal. Anything else is a
	/// privilege change in one direction or the other.
	#[tokio::test]
	async fn a_credentialed_caller_may_not_use_another_principals_session() {
		let service = service_for(Session::owner()).await;
		let session_id = Uuid::new_v4();
		service.rpc().attach(session_id).await.expect("attach");
		// Bind the session to a different principal than the caller's.
		{
			let session = service.rpc().get_session(&session_id).await.expect("session");
			let mut session = session.write().await;
			session.au = Arc::new(Auth::for_ns(surrealdb_core::iam::Role::Owner, "test-ns"));
		}

		let refused = service
			.rpc()
			.verify_caller_for_session(&session_id, service.caller.au.as_ref())
			.await
			.expect_err("a different principal must be refused");
		assert!(refused.is_not_found(), "expected a not-found error, got {refused:?}");
	}

	/// A transaction belongs to the session that opened it. The map is
	/// process-wide, so without this check the id alone would let any caller
	/// read, write, commit or cancel inside somebody else's transaction.
	#[tokio::test]
	async fn a_transaction_is_only_usable_by_the_session_that_opened_it() {
		let service = service().await;
		let owner = Uuid::new_v4();
		service.rpc().attach(owner).await.expect("attach");
		let DbResult::Other(Value::Uuid(txn)) =
			service.rpc().begin(None, owner).await.expect("begin")
		else {
			panic!("begin should answer with a transaction id");
		};
		let txn = txn.into_inner();

		assert!(service.rpc().transaction_belongs_to(&txn, owner));
		assert!(!service.rpc().transaction_belongs_to(&txn, Uuid::new_v4()));
		assert!(!service.rpc().transaction_belongs_to(&Uuid::new_v4(), owner));
	}

	/// A transaction outlives the request that opened it, so it cannot run on
	/// the ephemeral session a request without one is given: releasing that
	/// session cancels the transaction the caller was just handed.
	#[tokio::test]
	async fn a_transaction_needs_an_attached_session() {
		let service = service().await;
		let ephemeral = Uuid::new_v4();
		service
			.rpc()
			.register_ephemeral_session(ephemeral, Arc::new(RwLock::new(Session::default())));

		let refused = service
			.rpc()
			.begin(None, ephemeral)
			.await
			.expect_err("an ephemeral session must not open a transaction");
		assert!(refused.is_validation(), "expected a validation error, got {refused:?}");
	}

	/// A subscriber that stops reading must be ended once its buffer fills.
	///
	/// Waiting for it instead would bound nothing: the notification dispatcher
	/// keeps receiving while a send is pending, so pending sends would
	/// accumulate without limit, each holding a whole notification.
	#[tokio::test]
	async fn a_subscriber_that_stops_reading_is_ended() {
		let service = service().await;
		let owner = Uuid::new_v4();
		let live_query_id = register_live_query(&service, owner);
		let subscription =
			service.attach_subscription(owner, live_query_id, false).expect("subscribe");
		// Held but never polled, so nothing drains the queue.
		let _stream = subscription.into_inner();

		let notification = Notification::new(
			live_query_id.into(),
			None,
			surrealdb_types::Action::Create,
			Value::None,
			Value::None,
		);
		// The buffer's worth of notifications fit; the one after it does not.
		for _ in 0..*GRPC_NOTIFICATION_BUFFER {
			assert!(service.rpc().dispatch_notification(&notification).await);
			assert!(
				service
					.rpc()
					.live_queries
					.get(&live_query_id)
					.is_some_and(|lq| lq.subscriber.is_some()),
				"the subscription must survive while its buffer has room"
			);
		}

		assert!(service.rpc().dispatch_notification(&notification).await);
		assert!(
			service
				.rpc()
				.live_queries
				.get(&live_query_id)
				.is_some_and(|lq| lq.subscriber.is_none()),
			"a full buffer must end the subscription rather than queue behind it"
		);
	}

	/// A gRPC session outlives any one connection, so nothing reclaims the
	/// transactions an abandoned one left open until the server stops.
	#[tokio::test]
	async fn shutdown_cancels_the_transactions_clients_left_open() {
		let service = service().await;
		let session_id = Uuid::new_v4();
		service.rpc().attach(session_id).await.expect("attach");
		service.rpc().begin(None, session_id).await.expect("begin");
		assert_eq!(service.rpc().transactions.len(), 1);

		service.rpc().cleanup_all_txns().await;
		assert!(service.rpc().transactions.is_empty(), "shutdown must cancel every transaction");
	}

	/// A live query is only reachable by the session that registered it.
	/// Without this, holding a live query id -- which is an ordinary query
	/// result, not a secret -- would be enough to receive another session's
	/// changes.
	///
	/// The refusal is `NotFound` rather than `PermissionDenied` so it does not
	/// confirm to a stranger that the id exists.
	#[tokio::test]
	async fn a_live_query_is_only_reachable_by_its_own_session() {
		let service = service().await;
		let owner = Uuid::new_v4();
		let live_query_id = register_live_query(&service, owner);

		let refused = service
			.attach_subscription(Uuid::new_v4(), live_query_id, false)
			.err()
			.expect("another session must be refused");
		assert_eq!(refused.code(), tonic::Code::NotFound);

		let Ok(_subscription) = service.attach_subscription(owner, live_query_id, false) else {
			panic!("the owner may subscribe");
		};
	}

	/// One subscriber at a time, matching what `GetCapabilities` reports. A
	/// second attach must be refused rather than quietly displacing the first,
	/// which would leave that subscriber with a stream that never ends and
	/// never delivers.
	#[tokio::test]
	async fn a_second_subscriber_is_refused() {
		let service = service().await;
		let owner = Uuid::new_v4();
		let live_query_id = register_live_query(&service, owner);

		let Ok(_subscription) = service.attach_subscription(owner, live_query_id, false) else {
			panic!("the first subscriber should be accepted");
		};
		let refused = service
			.attach_subscription(owner, live_query_id, false)
			.err()
			.expect("the second subscriber must be refused");
		assert_eq!(refused.code(), tonic::Code::AlreadyExists);
	}

	/// Dropping the stream is how a client unsubscribes, so it has to release
	/// the slot -- otherwise a client that reconnected could never re-attach
	/// to its own live query.
	#[tokio::test]
	async fn dropping_a_subscription_releases_it() {
		let service = service().await;
		let owner = Uuid::new_v4();
		let live_query_id = register_live_query(&service, owner);

		let Ok(subscription) = service.attach_subscription(owner, live_query_id, false) else {
			panic!("the first subscriber should be accepted");
		};
		drop(subscription);
		let Ok(_resubscribed) = service.attach_subscription(owner, live_query_id, false) else {
			panic!("re-subscribing after the first stream was dropped");
		};
	}

	/// Subscribing to a live query that does not exist is refused, so a client
	/// cannot register a subscription against an id nothing will ever produce.
	#[tokio::test]
	async fn an_unknown_live_query_is_refused() {
		let service = service().await;
		let refused = service
			.attach_subscription(Uuid::new_v4(), Uuid::new_v4(), false)
			.err()
			.expect("an unknown live query must be refused");
		assert_eq!(refused.code(), tonic::Code::NotFound);
	}

	fn frames(results: Vec<QueryResult>) -> Vec<rpc::QueryResponse> {
		frames_with(results, QUERY_BATCH_RECORDS)
	}

	/// Frame finished results the way the streaming path frames them, so these
	/// tests exercise the framing the server actually serves.
	///
	/// Feeding whole results in is the same thing a statement that produced its
	/// rows all at once does, and `Begin` is prepended here because the handler
	/// sends it before the execution starts rather than as part of the framing.
	fn frames_with(results: Vec<QueryResult>, batch_records: usize) -> Vec<rpc::QueryResponse> {
		let mut framing = QueryFrames::new(batch_records);
		let statement_count = results.len() as u32;
		let mut frames = vec![rpc::QueryResponse {
			frame: Some(rpc::query_response::Frame::Begin(rpc::QueryBegin {
				query_id: Some(proto::Uuid::from_uuid(Uuid::new_v4())),
				statement_count,
			})),
		}];
		for (index, result) in results.into_iter().enumerate() {
			for item in surrealdb_core::dbs::items_for_result(index, result) {
				framing.absorb(item);
			}
		}
		framing.end(Duration::from_millis(3));
		while let Some(frame) = framing.pop() {
			frames.push(frame);
		}
		frames
	}

	fn batch(response: &rpc::QueryResponse) -> &rpc::QueryBatchFrame {
		match response.frame.as_ref() {
			Some(rpc::query_response::Frame::Batch(batch)) => batch,
			_ => panic!("expected a batch frame"),
		}
	}

	fn result(value: Value) -> QueryResult {
		QueryResult {
			time: Duration::from_millis(1),
			result: Ok(value),
			query_type: QueryType::Other,
		}
	}

	/// Nothing follows a statement's terminal batch, including rows the
	/// executor had already produced for it.
	///
	/// A value this server cannot encode terminates its statement, but the
	/// executor knows nothing of that and keeps sending the batches it is
	/// partway through. Framing those would put rows after the terminal batch,
	/// which the protocol forbids and a client acting on rows as they arrive
	/// would treat as real results.
	#[test]
	fn a_terminated_statement_takes_no_further_rows() {
		let mut frames = QueryFrames::new(QUERY_BATCH_RECORDS);
		frames.queue_terminal(
			0,
			rpc::QueryStatementKind::Other,
			Some(to_proto_error(&TypesError::internal("unencodable".to_string()))),
			0,
		);
		// The batches the executor was already partway through sending.
		for _ in 0..4 {
			frames.absorb(QueryStreamItem::Rows {
				index: 0,
				values: vec![Value::Bool(true); QUERY_BATCH_RECORDS],
			});
		}
		frames.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: None,
		});
		frames.end(Duration::ZERO);

		let mut produced = Vec::new();
		while let Some(frame) = frames.pop() {
			produced.push(frame);
		}
		assert_eq!(produced.len(), 2, "one terminal batch and one end frame, nothing after");
		assert!(
			matches!(produced[0].frame, Some(rpc::query_response::Frame::Batch(_))),
			"the terminal batch"
		);
		assert!(matches!(produced[1].frame, Some(rpc::query_response::Frame::End(_))));
	}

	/// The deadline a streaming query runs under is the sooner of the client's
	/// own and the server's, and either alone still applies.
	///
	/// This is what bounds a client that stops reading: its execution is polled
	/// only when it reads, so the deadline has to come from a timer the runtime
	/// drives, and that timer needs a duration whichever side supplied one.
	#[test]
	fn a_streaming_query_takes_the_sooner_deadline() {
		let short = Duration::from_secs(1);
		let long = Duration::from_secs(60);
		assert_eq!(shortest(Some(short), Some(long)), Some(short));
		assert_eq!(shortest(Some(long), Some(short)), Some(short));
		assert_eq!(shortest(Some(short), None), Some(short), "the client's alone still applies");
		assert_eq!(shortest(None, Some(long)), Some(long), "the server's alone still applies");
		assert_eq!(shortest(None, None), None, "neither configured means no deadline");
	}

	/// A statement is terminated once, whichever ends it first.
	///
	/// A value this server cannot encode terminates its statement immediately,
	/// and the executor -- which knows nothing of the encoding -- goes on to
	/// send that statement's own `Finished`. Acting on both would put a frame
	/// after a terminal one and count the statement twice in `result_count`,
	/// which a client cross-checking that count reads as a lost frame.
	#[test]
	fn a_statement_is_terminated_only_once() {
		let mut frames = QueryFrames::new(QUERY_BATCH_RECORDS);
		frames.queue_terminal(0, rpc::QueryStatementKind::Other, None, 0);
		frames.absorb(QueryStreamItem::Finished {
			index: 0,
			time: Duration::ZERO,
			query_type: QueryType::Other,
			error: None,
		});
		frames.end(Duration::ZERO);

		let mut produced = Vec::new();
		while let Some(frame) = frames.pop() {
			produced.push(frame);
		}
		assert_eq!(produced.len(), 2, "one terminal batch and one end frame");
		assert!(matches!(produced[0].frame, Some(rpc::query_response::Frame::Batch(_))));
		match produced[1].frame.as_ref() {
			Some(rpc::query_response::Frame::End(end)) => {
				assert_eq!(end.result_count, 1, "the statement is counted once");
			}
			other => panic!("expected an end frame, got {other:?}"),
		}
	}

	/// A query stream always opens with `Begin` and closes with `End`. This
	/// server executes to completion first, so every statement it parsed also
	/// produced a result: `Begin.statement_count` and `End.result_count` agree,
	/// and `End` reports how long the whole query took.
	#[test]
	fn query_frames_are_bracketed_by_begin_and_end() {
		let frames = frames(vec![result(Value::None), result(Value::None)]);
		assert_eq!(frames.len(), 4);
		match frames[0].frame.as_ref() {
			Some(rpc::query_response::Frame::Begin(begin)) => {
				assert_eq!(begin.statement_count, 2);
			}
			_ => panic!("expected a begin frame"),
		}
		match frames[3].frame.as_ref() {
			Some(rpc::query_response::Frame::End(end)) => {
				assert_eq!(end.result_count, 2, "every parsed statement produced a result");
				assert!(
					end.execution_duration.is_some(),
					"the whole-query duration is what a client cannot sum from the per-statement ones"
				);
			}
			_ => panic!("expected an end frame"),
		}
	}

	/// A client's batch size is honoured, and a request above the server's own
	/// bound is clamped rather than refused -- asking for more than you may have
	/// still returns results.
	#[test]
	fn a_client_batch_size_is_honoured_and_clamped() {
		assert_eq!(batch_records(0), QUERY_BATCH_RECORDS, "zero means the server chooses");
		assert_eq!(batch_records(1), 1, "a client may ask for one record per batch");
		assert_eq!(batch_records(u32::MAX), QUERY_BATCH_RECORDS, "clamped, not refused");

		let rows = Array::from(
			(0..3).map(|i| Value::Number(surrealdb_types::Number::Int(i))).collect::<Vec<_>>(),
		);
		let frames = frames_with(vec![result(Value::Array(rows))], batch_records(1));
		let sizes = batch_sizes(&frames);
		assert!(
			sizes.iter().all(|n| *n <= 1),
			"a client asking for one record per batch gets no more than one, {sizes:?}"
		);
		assert_eq!(sizes.iter().sum::<usize>(), 3, "every record is still sent once");
	}

	/// How many records each of a statement's batches carried.
	fn batch_sizes(frames: &[rpc::QueryResponse]) -> Vec<usize> {
		frames[1..frames.len() - 1]
			.iter()
			.map(|response| match batch(response).payload.as_ref() {
				Some(rpc::query_batch_frame::Payload::Values(values)) => values.values.len(),
				None => 0,
				_ => panic!("expected a value batch"),
			})
			.collect()
	}

	/// A result larger than one batch is split, so no single message grows with
	/// the result set -- a client decoding with gRPC's 4 MiB default would
	/// otherwise refuse a large `SELECT` outright.
	///
	/// The batches ramp rather than being uniform: the first is small so the
	/// client sees rows early, and each one doubles up to the cap so a long
	/// result does not pay a per-frame cost for that.
	#[test]
	fn a_large_result_is_split_across_batches() {
		let records = QUERY_BATCH_RECORDS * 2 + 1;
		let rows = Array::from(
			(0..records)
				.map(|i| Value::Number(surrealdb_types::Number::Int(i as i64)))
				.collect::<Vec<_>>(),
		);
		let frames = frames(vec![result(Value::Array(rows))]);
		let batches: Vec<&rpc::QueryBatchFrame> =
			frames[1..frames.len() - 1].iter().map(batch).collect();

		assert!(batches.len() > 1, "a result this size must span several batches");
		assert_eq!(
			batches.iter().map(|b| b.batch_index).collect::<Vec<_>>(),
			(0..batches.len() as u64).collect::<Vec<_>>(),
			"batches must be indexed in order so the client can demultiplex them"
		);
		let kinds: Vec<i32> = batches.iter().map(|b| b.kind).collect();
		let (last, rest) = kinds.split_last().expect("at least one batch");
		assert_eq!(*last, rpc::QueryResponseKind::BatchedFinal as i32);
		assert!(
			rest.iter().all(|k| *k == rpc::QueryResponseKind::Batched as i32),
			"only the last batch completes the statement"
		);

		let sizes = batch_sizes(&frames);
		assert_eq!(sizes.iter().sum::<usize>(), records, "every record must be sent once");
		assert_eq!(sizes[0], QUERY_FIRST_BATCH_RECORDS, "the first batch is small, for latency");
		assert!(
			sizes.windows(2).all(|w| w[1] >= w[0] || w[1] == sizes[sizes.len() - 1]),
			"batches grow toward the cap, {sizes:?}"
		);
		assert!(sizes.iter().all(|n| *n <= QUERY_BATCH_RECORDS), "no batch exceeds the cap");

		// The stats describe the statement, so they ride on the batch that
		// completes it rather than being repeated or split.
		assert!(rest.iter().enumerate().all(|(i, _)| batches[i].stats.is_none()));
		assert_eq!(
			batches
				.last()
				.expect("at least one batch")
				.stats
				.as_ref()
				.expect("the final batch carries the stats")
				.records_returned,
			records as i64
		);
	}

	/// An empty list still owes the client one final batch, so it learns the
	/// statement's kind and stats.
	#[test]
	fn an_empty_result_still_sends_one_final_batch() {
		let frames = frames(vec![result(Value::Array(Array::new()))]);
		assert_eq!(frames.len(), 3);
		let batch = batch(&frames[1]);
		assert_eq!(batch.kind, rpc::QueryResponseKind::BatchedFinal as i32);
		assert_eq!(batch.stats.as_ref().expect("stats").records_returned, 0);
	}

	/// A list result is a batch of its elements; the SDK rebuilds the array
	/// from them. A scalar is a `SINGLE`, which the SDK unwraps.
	#[test]
	fn list_and_scalar_results_use_distinct_kinds() {
		let list = frames(vec![result(Value::Array(Array::from(vec![
			Value::Bool(true),
			Value::Bool(false),
		])))]);
		let list = batch(&list[1]);
		assert_eq!(list.kind, rpc::QueryResponseKind::BatchedFinal as i32);
		assert_eq!(list.stats.as_ref().expect("stats").records_returned, 2);

		let scalar = frames(vec![result(Value::Bool(true))]);
		let scalar = batch(&scalar[1]);
		assert_eq!(scalar.kind, rpc::QueryResponseKind::Single as i32);
		assert_eq!(scalar.stats.as_ref().expect("stats").records_returned, 1);
	}

	/// A statement's own failure rides in its batch rather than terminating
	/// the stream, so the statements around it still report their results.
	#[test]
	fn a_failed_statement_does_not_end_the_stream() {
		let frames = frames(vec![
			QueryResult {
				time: Duration::ZERO,
				result: Err(TypesError::query("boom".to_string(), None)),
				query_type: QueryType::Other,
			},
			result(Value::Bool(true)),
		]);
		let failed = batch(&frames[1]);
		assert_eq!(failed.error.as_ref().expect("error").kind, proto::ErrorKind::Query as i32);
		assert!(failed.payload.is_none());
		assert!(batch(&frames[2]).error.is_none());
		assert!(matches!(frames[3].frame, Some(rpc::query_response::Frame::End(_))));
	}

	/// The statement kind lets a client tell a LIVE SELECT's returned id from
	/// an ordinary result.
	#[test]
	fn live_and_kill_statements_are_labelled() {
		let live = frames(vec![QueryResult {
			time: Duration::ZERO,
			result: Ok(Value::None),
			query_type: QueryType::Live,
		}]);
		assert_eq!(batch(&live[1]).statement_kind, rpc::QueryStatementKind::Live as i32);

		let kill = frames(vec![QueryResult {
			time: Duration::ZERO,
			result: Ok(Value::None),
			query_type: QueryType::Kill,
		}]);
		assert_eq!(batch(&kill[1]).statement_kind, rpc::QueryStatementKind::Kill as i32);
	}

	/// `use` distinguishes "leave alone" from "clear", which a plain string
	/// cannot express.
	#[test]
	fn nullable_strings_are_three_state() {
		assert_eq!(from_nullable(None), Value::None);
		assert_eq!(
			from_nullable(Some(rpc::NullableString {
				value: Some(rpc::nullable_string::Value::Null(proto::NullValue {})),
			})),
			Value::Null
		);
		assert_eq!(
			from_nullable(Some(rpc::NullableString {
				value: Some(rpc::nullable_string::Value::Some("test".to_string())),
			})),
			Value::String("test".to_string())
		);
	}

	/// Credentials round-trip to the shape the `signin` method parses: the
	/// scope fields are named, and empty ones are omitted rather than sent as
	/// empty strings (which would select a namespace called "").
	#[test]
	fn user_credentials_flatten_to_the_signin_object() {
		let object = access_credentials(rpc::AccessMethod {
			method: Some(rpc::access_method::Method::User(rpc::UserCredentials {
				namespace: String::new(),
				database: String::new(),
				username: "root".to_string(),
				password: "root".to_string(),
				access: String::new(),
			})),
		})
		.expect("credentials");
		assert_eq!(object.get("user"), Some(&Value::String("root".to_string())));
		assert_eq!(object.get("pass"), Some(&Value::String("root".to_string())));
		assert!(object.get("ns").is_none());
		assert!(object.get("db").is_none());
	}

	/// Record credentials carry the access method's own variables through to
	/// the SIGNIN/SIGNUP clause alongside the scope fields.
	#[test]
	fn record_credentials_carry_their_variables() {
		let variables: proto::Object = std::collections::BTreeMap::from([(
			"email".to_string(),
			proto::Value::try_from(Value::String("a@b.c".to_string())).expect("encodable"),
		)])
		.into();
		let object = record_credentials(rpc::RecordCredentials {
			namespace: "test".to_string(),
			database: "test".to_string(),
			access: "user".to_string(),
			variables: Some(proto::Variables {
				variables: variables.items,
			}),
		})
		.expect("credentials");
		assert_eq!(object.get("ns"), Some(&Value::String("test".to_string())));
		assert_eq!(object.get("ac"), Some(&Value::String("user".to_string())));
		assert_eq!(object.get("email"), Some(&Value::String("a@b.c".to_string())));
	}

	/// An export config with no table selection exports every table, matching
	/// the datastore's own default.
	#[test]
	fn export_table_selection_maps_each_arm() {
		let config = |tables: Option<rpc::export_config::Tables>| {
			from_proto_export_config(rpc::ExportConfig {
				tables,
				..Default::default()
			})
			.tables
		};
		assert!(matches!(config(None), export::TableConfig::All));
		assert!(matches!(
			config(Some(rpc::export_config::Tables::from(false))),
			export::TableConfig::None
		));
		match config(Some(rpc::export_config::Tables {
			selection: Some(rpc::export_config::tables::Selection::Selected(
				rpc::export_config::SelectedTables {
					tables: vec!["person".to_string()],
				},
			)),
		})) {
			export::TableConfig::Some(tables) => assert_eq!(tables, vec!["person".to_string()]),
			other => panic!("expected a table selection, got {other:?}"),
		}
	}

	/// Every error kind maps to the status code the SDK's engine maps back
	/// onto that same kind, so a failure keeps its classification across the
	/// wire even though a status carries no structured error.
	#[test]
	fn error_kinds_map_to_round_tripping_status_codes() {
		use tonic::Code;
		let cases = [
			(TypesError::validation("v".to_string(), None), Code::InvalidArgument),
			(TypesError::configuration("c".to_string(), None), Code::Unimplemented),
			(TypesError::query("q".to_string(), None), Code::Aborted),
			(TypesError::not_allowed("n".to_string(), None), Code::PermissionDenied),
			(TypesError::not_found("f".to_string(), None), Code::NotFound),
			(TypesError::already_exists("a".to_string(), None), Code::AlreadyExists),
			(TypesError::connection("x".to_string(), None), Code::Unavailable),
			(TypesError::internal("i".to_string()), Code::Internal),
		];
		for (error, code) in cases {
			assert_eq!(to_status(&error).code(), code, "{}", error.message());
		}
	}

	/// The structured form keeps the cause chain that the status form has
	/// nowhere to put.
	#[test]
	fn proto_errors_keep_their_cause_chain() {
		let error = TypesError::query("outer".to_string(), None).with_cause(
			TypesError::serialization("inner".to_string(), SerializationError::Deserialization),
		);
		let proto = to_proto_error(&error);
		assert_eq!(proto.kind, proto::ErrorKind::Query as i32);
		let cause = proto.cause.expect("cause");
		assert_eq!(cause.kind, proto::ErrorKind::Serialization as i32);
		assert_eq!(cause.message, "inner");
	}

	/// The tokens an auth method answered with are read from whichever shape
	/// it used: a bare token, or an object carrying a refresh token too.
	#[test]
	fn tokens_are_read_from_either_answer_shape() {
		let bare = to_tokens(DbResult::Other(Value::String("access".to_string()))).expect("tokens");
		assert_eq!(bare.access, "access");
		assert!(bare.refresh.is_empty());

		let mut object = Object::new();
		object.insert("access".to_string(), Value::String("access".to_string()));
		object.insert("refresh".to_string(), Value::String("refresh".to_string()));
		let pair = to_tokens(DbResult::Other(Value::Object(object))).expect("tokens");
		assert_eq!(pair.access, "access");
		assert_eq!(pair.refresh, "refresh");
	}

	/// A trailer is what marks a byte stream complete, so a mismatched length
	/// or checksum has to fail the transfer rather than be ignored.
	#[test]
	fn trailers_are_verified_against_what_arrived() {
		let digest = blake3::hash(b"hello");
		let trailer = rpc::DataTrailer {
			bytes: 5,
			blake3: digest.to_hex().to_string(),
		};
		assert!(verify_trailer(&trailer, 5, digest).is_ok());
		assert!(verify_trailer(&trailer, 4, digest).is_err());

		// An absent checksum means the sender did not compute one.
		let unchecked = rpc::DataTrailer {
			bytes: 5,
			blake3: String::new(),
		};
		assert!(verify_trailer(&unchecked, 5, digest).is_ok());

		let wrong = rpc::DataTrailer {
			bytes: 5,
			blake3: blake3::hash(b"other").to_hex().to_string(),
		};
		assert!(verify_trailer(&wrong, 5, digest).is_err());
	}
}
