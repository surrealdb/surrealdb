pub mod format;
pub mod grpc;
pub mod http;
pub mod response;
pub mod streaming;
pub mod websocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use surrealdb_core::channel::Receiver;
#[cfg(feature = "graphql")]
use surrealdb_core::graphql::NotificationRouter;
use surrealdb_core::rpc::RpcProtocol;
use surrealdb_rpc::{DbResponse, DbResult};
use surrealdb_types::{Action, Notification};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(feature = "graphql")]
use crate::cnf::GRAPHQL_SUBSCRIPTION_CHANNEL_CAPACITY;
use crate::rpc::websocket::Websocket;

static CONN_CLOSED_ERR: &str = "Connection closed normally";
/// A type alias for an RPC Connection
type WebSocket = Arc<Websocket>;
/// Mapping of WebSocket ID to WebSocket
type WebSockets = RwLock<HashMap<Uuid, WebSocket>>;
/// Recorded state for a registered LIVE query. Stored on the global RPC
/// state so the live-query active gauge can be balanced (the cleanup paths
/// drop entries one-by-one with the originating tenant ctx) and so the
/// notification dispatch can label its delivery counter.
#[derive(Clone, Debug)]
pub struct LiveQueryEntry {
	pub websocket_id: Uuid,
	pub session_id: Uuid,
	/// Namespace at the time the LIVE statement was registered. `None`
	/// when the registering session had no NS selected.
	pub namespace: Option<String>,
	/// Database at the time the LIVE statement was registered. `None`
	/// when the registering session had no DB selected.
	pub database: Option<String>,
}

/// Mapping of LIVE Query ID to its registered entry.
type LiveQueries = RwLock<HashMap<Uuid, LiveQueryEntry>>;

pub struct RpcState {
	/// Stores the currently connected WebSockets
	pub web_sockets: WebSockets,
	/// Stores the currently initiated LIVE queries
	pub live_queries: LiveQueries,
	/// HTTP RPC handler with persistent sessions
	pub http: Arc<crate::rpc::http::Http>,
	/// gRPC RPC handler, holding the sessions, transactions and live query
	/// subscriptions of the gRPC transport
	pub grpc: Arc<crate::rpc::grpc::Grpc>,
	/// Prometheus observer for per-protocol network byte counters. `None`
	/// when `SURREAL_METRICS_ENABLED=false` so the byte counter path is
	/// entirely inert for unconfigured deployments.
	pub metrics_observer: Option<Arc<crate::observe::metrics::MetricsObserver>>,
	#[cfg(feature = "graphql")]
	pub(crate) notification_router: Arc<NotificationRouter>,
}

impl RpcState {
	pub fn new(datastore: Arc<surrealdb_core::kvs::Datastore>) -> Self {
		Self::new_with_metrics(datastore, None)
	}

	pub fn new_with_metrics(
		datastore: Arc<surrealdb_core::kvs::Datastore>,
		metrics_observer: Option<Arc<crate::observe::metrics::MetricsObserver>>,
	) -> Self {
		Self::new_with_options(datastore, metrics_observer, None)
	}

	pub fn new_with_options(
		datastore: Arc<surrealdb_core::kvs::Datastore>,
		metrics_observer: Option<Arc<crate::observe::metrics::MetricsObserver>>,
		durable_session_ttl: Option<std::time::Duration>,
	) -> Self {
		Self {
			web_sockets: RwLock::new(HashMap::new()),
			live_queries: RwLock::new(HashMap::new()),
			http: Arc::new(crate::rpc::http::Http::new_with_durability(
				Arc::clone(&datastore),
				durable_session_ttl,
			)),
			grpc: Arc::new(crate::rpc::grpc::Grpc::new(datastore, metrics_observer.clone())),
			metrics_observer,
			#[cfg(feature = "graphql")]
			notification_router: Arc::new(NotificationRouter::new(
				*GRAPHQL_SUBSCRIPTION_CHANNEL_CAPACITY,
			)),
		}
	}
}

/// Dispatch one live-query notification into the RPC state.
///
/// The helper is intentionally independent of the datastore notification channel so embedded
/// products can inject an already-authenticated notification received over an internal relay.
/// Unknown live-query ids and disconnected WebSocket sessions are no-ops.
///
/// An [`Action::Killed`] notification also ends the registration it names: this is the only
/// signal that carries the id of a subscription the datastore has ended, so nothing else can
/// drop the entry.
pub async fn dispatch_live_notification(notification: Notification, state: Arc<RpcState>) {
	#[cfg(feature = "graphql")]
	if state.notification_router.has_subscribers() {
		state.notification_router.dispatch(&notification);
	}
	// A live query belongs to exactly one transport: whichever one's
	// `handle_live` registered it. Ask gRPC first, and stop if it owns this
	// one, so the WebSocket lookup below is only reached for live queries
	// registered over a WebSocket.
	if state.grpc.dispatch_notification(&notification).await {
		return;
	}
	// Copy the lookup result out and drop the `live_queries` read guard BEFORE acquiring
	// `web_sockets`. Keeping those locks independent prevents cleanup paths from being blocked
	// by a client send on the hot notification path.
	//
	// A killed subscription is deleted from storage before this notification is sent, so this
	// is the last one its id will ever carry and the registration ends with it. Leaving it
	// would hold the entry -- and keep the active-LQ gauge counting it -- until the connection
	// closes. `KILL` is not the only source: removing a table, database, or namespace, and
	// revoking a principal, all end subscriptions this way. The entry is taken out of the map
	// before the frame is sent, so a second notification for the same id cannot decrement the
	// gauge twice, and the labels come from the entry itself so the gauge stays balanced
	// against the registration that incremented it.
	let live_query = if notification.action == Action::Killed {
		let entry = state.live_queries.write().await.remove(&notification.id);
		if let Some(entry) = entry.as_ref()
			&& let Some(obs) = state.metrics_observer.as_ref()
		{
			obs.adjust_live_query_active(-1, entry.namespace.as_deref(), entry.database.as_deref());
		}
		entry
	} else {
		state.live_queries.read().await.get(&notification.id).cloned()
	};
	if let Some(entry) = live_query
		&& let Some(rpc) = state.web_sockets.read().await.get(&entry.websocket_id).cloned()
	{
		// Count the notification once we know it will actually be delivered to a client. Drops
		// (unknown LQ id or disconnected WS) are deliberately not counted.
		if let Some(obs) = state.metrics_observer.as_ref() {
			obs.record_live_query_notification(
				entry.namespace.as_deref(),
				entry.database.as_deref(),
			);
		}
		// Hide the connection's implicit session UUID from the client: when a LIVE query was
		// registered without an explicit session_id it resolves to `rpc.id`, which is an
		// internal connection identifier the client never supplied.
		let wire_session_id = (entry.session_id != rpc.id).then_some(entry.session_id);
		let message = DbResponse::success(None, wire_session_id, DbResult::Live(notification));
		let format = rpc.format;
		let sender = rpc.channel.clone();
		crate::rpc::response::send(message, format, sender).await;
	}
}

/// Performs notification delivery to the WebSockets.
///
/// This function listens on the datastore's notification channel and forwards
/// LIVE query notifications to the appropriate WebSocket connections. It runs
/// in a loop until the provided [`CancellationToken`] is cancelled.
///
/// # Parameters
/// - `ds`:        The [`Datastore`] whose notification channel to listen on
/// - `state`:     The [`RpcState`] containing WebSocket and LIVE query mappings
/// - `canceller`: A [`CancellationToken`] that stops the loop when cancelled
///
/// # Usage
///
/// This is called automatically by
/// [`SurrealRouter::spawn_notifications`](crate::ntw::SurrealRouter::spawn_notifications).
/// If you need lower-level control you can call it directly inside your own `tokio::spawn`.
pub async fn notifications(
	channel: Receiver<Notification>,
	state: Arc<RpcState>,
	canceller: CancellationToken,
) {
	// Store messages being delivered
	let mut futures = FuturesUnordered::new();
	// Loop continuously
	loop {
		tokio::select! {
			//
			biased;
			// Check if this has shutdown
			_ = canceller.cancelled() => break,
			// Process any buffered messages
			Some(_) = futures.next() => continue,
			// Receive a notification on the channel
			Ok(notification) = channel.recv() => {
				futures.push(dispatch_live_notification(notification, Arc::clone(&state)));
			},
		}
	}
}

/// Closes all WebSocket connections, waiting for graceful shutdown.
///
/// Signals each connected WebSocket to shut down and then waits until all
/// connections have been drained from the [`RpcState`].
pub async fn graceful_shutdown(state: Arc<RpcState>) {
	// End gRPC subscriptions with a reason, so a subscriber learns the server
	// is going away and that re-subscribing later is reasonable, rather than
	// seeing its stream close without explanation.
	state.grpc.cleanup_all_lqs().await;
	// Cancel the transactions gRPC clients left open. A WebSocket's are
	// cancelled when its socket closes, but a gRPC session outlives any one
	// connection, so shutdown is the only point at which they are all known to
	// be finished with.
	state.grpc.cleanup_all_txns().await;
	// Close WebSocket connections, ensuring queued messages are processed
	for (_, rpc) in state.web_sockets.read().await.iter() {
		rpc.shutdown.cancel();
	}
	// Wait for all existing WebSocket connections to finish sending
	while !state.web_sockets.read().await.is_empty() {
		tokio::time::sleep(Duration::from_millis(250)).await;
	}
}

/// Forces a fast shutdown of all WebSocket connections.
///
/// Unlike [`graceful_shutdown`], this immediately drains the WebSocket map
/// without waiting for in-flight messages to be delivered.
pub fn shutdown(state: &Arc<RpcState>) {
	// Close all WebSocket connections immediately
	if let Ok(mut writer) = state.web_sockets.try_write() {
		writer.drain();
	}
}
