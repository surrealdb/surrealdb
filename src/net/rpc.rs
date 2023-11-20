use crate::cnf;
use crate::rpc::connection::Connection;
use axum::routing::get;
use axum::Extension;
use axum::Router;
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
use tower_http::request_id::RequestId;
use uuid::Uuid;

use axum::{
	extract::ws::{WebSocket, WebSocketUpgrade},
	response::IntoResponse,
};

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new().route("/rpc", get(handler))
}

async fn handler(
	ws: WebSocketUpgrade,
	Extension(sess): Extension<Session>,
	Extension(req_id): Extension<RequestId>,
) -> impl IntoResponse {
	ws
		// Set the maximum frame size
		.max_frame_size(*cnf::WEBSOCKET_MAX_FRAME_SIZE)
		// Set the maximum message size
		.max_message_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
		// Set the potential WebSocket protocol formats
		.protocols(["surrealql-binary", "json", "cbor", "messagepack"])
		// Handle the WebSocket upgrade and process messages
		.on_upgrade(move |socket| handle_socket(socket, sess, req_id))
}

async fn handle_socket(ws: WebSocket, sess: Session, req_id: RequestId) {
	// Create a new connection instance
	let rpc = Connection::new(sess);
	// Update the WebSocket ID with the Request ID
	if let Ok(Ok(req_id)) = req_id.header_value().to_str().map(Uuid::parse_str) {
		// If the ID couldn't be updated, ignore the error and keep the default ID
		let _ = rpc.write().await.update_ws_id(req_id).await;
	}
	// Serve the socket connection requests
	Connection::serve(rpc, ws).await;
}
