use crate::cnf;
use crate::rpc::connection::Connection;
use axum::routing::get;
use axum::{
	extract::ws::{WebSocket, WebSocketUpgrade},
	response::IntoResponse,
	Extension, Router,
};
use http_body::Body as HttpBody;
use surrealdb::dbs::Session;
use tower_http::request_id::RequestId;
use uuid::Uuid;

const PROTOCOLS: [&str; 4] = [
	// For internal serialisation
	"surrealql-binary",
	// For basic JSON serialisation
	"json",
	// For basic CBOR serialisation
	"cbor",
	// For basic MessagePack serialisation
	"messagepack",
];

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
		// Set the potential WebSocket protocols
		.protocols(PROTOCOLS)
		// Set the maximum WebSocket frame size
		.max_frame_size(*cnf::WEBSOCKET_MAX_FRAME_SIZE)
		// Set the maximum WebSocket message size
		.max_message_size(*cnf::WEBSOCKET_MAX_MESSAGE_SIZE)
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
