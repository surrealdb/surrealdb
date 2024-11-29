pub mod connection;
pub mod failure;
pub mod format;
pub mod post_context;
pub mod response;

use crate::rpc::connection::Connection;
use crate::rpc::response::success;
use crate::telemetry::metrics::ws::NotificationContext;
use opentelemetry::Context as TelemetryContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use surrealdb::kvs::Datastore;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static CONN_CLOSED_ERR: &str = "Connection closed normally";
/// A type alias for an RPC Connection
type WebSocket = Arc<RwLock<Connection>>;
/// Mapping of WebSocket ID to WebSocket
type WebSockets = RwLock<HashMap<Uuid, WebSocket>>;
/// Mapping of LIVE Query ID to WebSocket ID
type LiveQueries = RwLock<HashMap<Uuid, Uuid>>;

pub struct RpcState {
	/// Stores the currently connected WebSockets
	pub web_sockets: WebSockets,
	/// Stores the currently initiated LIVE queries
	pub live_queries: LiveQueries,
}

impl RpcState {
	pub fn new() -> Self {
		RpcState {
			web_sockets: WebSockets::default(),
			live_queries: LiveQueries::default(),
		}
	}
}

/// Performs notification delivery to the WebSockets
pub(crate) async fn notifications(
	ds: Arc<Datastore>,
	state: Arc<RpcState>,
	canceller: CancellationToken,
) {
	// Listen to the notifications channel
	if let Some(channel) = ds.notifications() {
		// Loop continuously
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Yield back to other tasks
				_ = tokio::task::yield_now() => (),
				// Receive a notification on the channel
				Ok(notification) = channel.recv() => {
					// Find which WebSocket the notification belongs to
					let found_ws = {
						// We remove the lock asap
						state.live_queries.read().await.get(&notification.id).cloned()
					};
					if let Some(id) = found_ws {
						// Check to see if the WebSocket exists
						let maybe_ws = {
							// We remove the lock ASAP
							// WS is an Arc anyway
							state.web_sockets.read().await.get(&id).cloned()
						};
						if let Some(rpc) = maybe_ws {
							// Serialize the message to send
							let message = success(None, notification);
							// Add metrics
							let cx = TelemetryContext::new();
							let not_ctx = NotificationContext::default()
								  .with_live_id(id.to_string());
							let cx = Arc::new(cx.with_value(not_ctx));
							// Get the WebSocket output format
							let format = rpc.read().await.format;
							// get the WebSocket sending channel
							let sender = rpc.read().await.channel.0.clone();
							// Send the notification to the client
							message.send(cx, format, &sender).await
						}
					}
				},
			}
		}
	}
}

/// Closes all WebSocket connections, waiting for graceful shutdown
pub(crate) async fn graceful_shutdown(state: Arc<RpcState>) {
	// Close WebSocket connections, ensuring queued messages are processed
	for (_, rpc) in state.web_sockets.read().await.iter() {
		rpc.read().await.shutdown.cancel();
	}
	// Wait for all existing WebSocket connections to finish sending
	while state.web_sockets.read().await.len() > 0 {
		tokio::time::sleep(Duration::from_millis(250)).await;
	}
}

/// Forces a fast shutdown of all WebSocket connections
pub(crate) fn shutdown(state: Arc<RpcState>) {
	// Close all WebSocket connections immediately
	if let Ok(mut writer) = state.web_sockets.try_write() {
		writer.drain();
	}
}
