use axum::extract::ws::Message;
use surrealdb_core::rpc::DbResponse;
use surrealdb_core::rpc::format::Format;
use tokio::sync::mpsc::Sender;
use tracing::Span;

use crate::rpc::format::WsFormat;

/// Send the response to the WebSocket channel.
///
/// Per-RPC duration, outcome, and method labels are recorded centrally
/// in [`surrealdb_core::rpc::protocol`] via [`RpcEvent`], so this
/// function only handles serialisation and dispatch -- it no longer
/// emits any telemetry of its own. Network byte counters for the
/// outbound frame are recorded by the WebSocket write loop in
/// [`crate::rpc::websocket`].
pub async fn send(response: DbResponse, fmt: Format, chn: Sender<Message>) {
	// Get the request id
	let id = response.id.clone();
	let session_id = response.session_id;
	// Create a new tracing span
	let span = Span::current();
	// Log the rpc response call
	debug!("Process RPC response");
	// Record tracing details for errors
	if let Err(err) = &response.result {
		span.record("otel.status_code", "ERROR");
		span.record("rpc.error_kind", format!("{:?}", err.kind_str()));
		span.record("rpc.error_message", err.message());
	}
	// Process the response for the format
	let (_len, msg) = match fmt.res_ws(response) {
		Ok((l, m)) => (l, m),
		Err(err) => fmt
			.res_ws(DbResponse::failure(id, session_id, err))
			.expect("Serialising internal error should always succeed"),
	};
	// Send the message to the write channel
	let _ = chn.send(msg).await;
}
