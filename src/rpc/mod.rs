pub mod args;
pub mod connection;
pub mod failure;
pub mod format;
pub mod post_context;
pub mod request;
pub mod response;

use crate::dbs::DB;
use crate::rpc::connection::Connection;
use crate::rpc::response::success;
use crate::telemetry::metrics::ws::NotificationContext;
use once_cell::sync::Lazy;
use opentelemetry::Context as TelemetryContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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

/// Stores the currently connected WebSockets
pub(crate) static WEBSOCKETS: Lazy<WebSockets> = Lazy::new(WebSockets::default);
/// Stores the currently initiated LIVE queries
pub(crate) static LIVE_QUERIES: Lazy<LiveQueries> = Lazy::new(LiveQueries::default);

/// Performs notification delivery to the WebSockets
pub(crate) async fn notifications(canceller: CancellationToken) {
	// Listen to the notifications channel
	if let Some(channel) = DB.get().unwrap().notifications() {
		// Loop continuously
		loop {
			tokio::select! {
				//
				biased;
				// Check if this has shutdown
				_ = canceller.cancelled() => break,
				// Receive a notification on the channel
				Ok(notification) = channel.recv() => {
					// Find which WebSocket the notification belongs to
					if let Some(id) = LIVE_QUERIES.read().await.get(&notification.id) {
						// Check to see if the WebSocket exists
						if let Some(rpc) = WEBSOCKETS.read().await.get(id) {
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
							let sender = rpc.read().await.channels.0.clone();
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
pub(crate) async fn graceful_shutdown() {
	// Close WebSocket connections, ensuring queued messages are processed
	for (_, rpc) in WEBSOCKETS.read().await.iter() {
		rpc.read().await.canceller.cancel();
	}
	// Wait for all existing WebSocket connections to finish sending
	while WEBSOCKETS.read().await.len() > 0 {
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

/// Forces a fast shutdown of all WebSocket connections
pub(crate) fn shutdown() {
	// Close all WebSocket connections immediately
	if let Ok(mut writer) = WEBSOCKETS.try_write() {
		writer.drain();
	}
}
