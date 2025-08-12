pub mod failure;
pub mod format;
pub mod http;
pub mod response;
pub mod websocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use opentelemetry::Context as TelemetryContext;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::core::kvs::Datastore;
use crate::core::rpc::Data;
use crate::rpc::websocket::Websocket;
use crate::telemetry::metrics::ws::NotificationContext;

static CONN_CLOSED_ERR: &str = "Connection closed normally";
/// A type alias for an RPC Connection
type WebSocket = Arc<Websocket>;
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
					// Get the id for this notification
					let id = notification.id.as_ref();
					// Get the WebSocket for this notification
					let websocket = {
						state.live_queries.read().await.get(id).copied()
					};
					// Ensure the specified WebSocket exists
					if let Some(id) = websocket.as_ref() {
						// Get the WebSocket for this notification
						let websocket = {
							state.web_sockets.read().await.get(id).cloned()
						};
						// Ensure the specified WebSocket exists
						if let Some(rpc) = websocket {
							// Serialize the message to send
							let message = response::success(None, Data::Live(notification));
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
							let future = message.send(cx, format, sender);
							// Pus the future to the pipeline
							futures.push(future);
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
		rpc.shutdown.cancel();
	}
	// Wait for all existing WebSocket connections to finish sending
	while !state.web_sockets.read().await.is_empty() {
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
