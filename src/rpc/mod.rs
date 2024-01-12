pub mod args;
pub mod connection;
pub mod failure;
pub mod format;
pub mod request;
pub mod response;

use axum::extract::ws::Message;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::time::Duration;
use surrealdb::channel::Sender;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

static CONN_CLOSED_ERR: &str = "Connection closed normally";

pub struct WebSocketRef(Sender<Message>, CancellationToken);
// Mapping of WebSocketID to WebSocket
type WebSockets = RwLock<HashMap<Uuid, WebSocketRef>>;
// Mapping of LiveQueryID to WebSocketID
type LiveQueries = RwLock<HashMap<Uuid, Uuid>>;

pub(crate) static WEBSOCKETS: Lazy<WebSockets> = Lazy::new(WebSockets::default);
pub(crate) static LIVE_QUERIES: Lazy<LiveQueries> = Lazy::new(LiveQueries::default);

pub(crate) async fn graceful_shutdown() {
	// Close all WebSocket connections. Queued messages will still be processed.
	for (_, WebSocketRef(_, cancel_token)) in WEBSOCKETS.read().await.iter() {
		cancel_token.cancel();
	}

	// Wait for all existing WebSocket connections to gracefully close
	while WEBSOCKETS.read().await.len() > 0 {
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

pub(crate) fn shutdown() {
	// Close all WebSocket connections immediately
	if let Ok(mut writer) = WEBSOCKETS.try_write() {
		writer.drain();
	}
}
