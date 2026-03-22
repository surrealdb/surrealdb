pub mod format;
pub mod http;
pub mod response;
pub mod websocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use opentelemetry::Context as TelemetryContext;
#[cfg(feature = "graphql")]
use surrealdb_core::gql::NotificationRouter;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{DbResponse, DbResult};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(feature = "graphql")]
use crate::cnf::GQL_SUBSCRIPTION_CHANNEL_CAPACITY;
use crate::rpc::websocket::Websocket;
use crate::telemetry::metrics::ws::NotificationContext;

static CONN_CLOSED_ERR: &str = "Connection closed normally";
/// A type alias for an RPC Connection
type WebSocket = Arc<Websocket>;
/// Mapping of WebSocket ID to WebSocket
type WebSockets = RwLock<HashMap<Uuid, WebSocket>>;
/// Mapping of LIVE Query ID to WebSocket ID + Session ID
type LiveQueries = RwLock<HashMap<Uuid, (Uuid, Option<Uuid>)>>;

pub struct RpcState {
	/// Stores the currently connected WebSockets
	pub web_sockets: WebSockets,
	/// Stores the currently initiated LIVE queries
	pub live_queries: LiveQueries,
	/// HTTP RPC handler with persistent sessions
	pub http: Arc<crate::rpc::http::Http>,
	#[cfg(feature = "graphql")]
	pub(crate) notification_router: Arc<NotificationRouter>,
}

impl RpcState {
	pub fn new(
		datastore: Arc<surrealdb_core::kvs::Datastore>,
		session: surrealdb_core::dbs::Session,
	) -> Self {
		Self {
			web_sockets: RwLock::new(HashMap::new()),
			live_queries: RwLock::new(HashMap::new()),
			http: Arc::new(crate::rpc::http::Http::new(datastore, session)),
			#[cfg(feature = "graphql")]
			notification_router: Arc::new(NotificationRouter::new(
				*GQL_SUBSCRIPTION_CHANNEL_CAPACITY,
			)),
		}
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
pub async fn notifications(ds: Arc<Datastore>, state: Arc<RpcState>, canceller: CancellationToken) {
	// Store messages being delivered
	let mut futures = FuturesUnordered::new();
	// Listen to the notifications channel
	if let Some(channel) = ds.notifications() {
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
					#[cfg(feature = "graphql")]
					if state.notification_router.has_subscribers() {
						state.notification_router.dispatch(&notification);
					}
					// Get the id for this notification
					let id = notification.id.as_ref();
					// Get the WebSocket for this notification
					let websocket = {
						state.live_queries.read().await.get(id).copied()
					};
					// Ensure the specified WebSocket exists
					if let Some((id, session_id)) = websocket.as_ref() {
						// Get the WebSocket for this notification
						let websocket = {
							state.web_sockets.read().await.get(id).cloned()
						};
						// Ensure the specified WebSocket exists
						if let Some(rpc) = websocket {
							// Serialize the message to send
							let message = DbResponse::success(None, session_id.map(Into::into), DbResult::Live(notification));
							// Add telemetry metrics
							let cx = TelemetryContext::new();
							let not_ctx = NotificationContext::default()
								  .with_live_id(id.to_string());
							let cx = Arc::new(cx.with_value(not_ctx));
							// Get the WebSocket output format
							let format = rpc.format;
							// Get the WebSocket sending channel
							let sender = rpc.channel.clone();
							// Send the notification to the client
							// let future = message.send(cx, format, sender);
							let future = crate::rpc::response::send(message, cx, format, sender);
							// Pus the future to the pipeline
							futures.push(future);
						}
					}
				},
			}
		}
	}
}

/// Closes all WebSocket connections, waiting for graceful shutdown.
///
/// Signals each connected WebSocket to shut down and then waits until all
/// connections have been drained from the [`RpcState`].
pub async fn graceful_shutdown(state: Arc<RpcState>) {
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
pub fn shutdown(state: Arc<RpcState>) {
	// Close all WebSocket connections immediately
	if let Ok(mut writer) = state.web_sockets.try_write() {
		writer.drain();
	}
}
