use axum::extract::ws::Message;
use surrealdb_core::rpc::format::Format;
use surrealdb_rpc::DbResponse;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tracing::Span;

use crate::rpc::format::WsFormat;

/// Serialise a response for the WebSocket wire.
///
/// A response that cannot be encoded is replaced by an encoding of that
/// failure, carrying the same request and session ids, so a caller always has
/// a message to deliver.
///
/// Per-RPC duration, outcome, and method labels are recorded centrally in
/// [`surrealdb_core::rpc::protocol`] via [`RpcEvent`], so this only handles
/// serialisation -- it emits no telemetry of its own. Network byte counters
/// for the outbound frame are recorded by the WebSocket write loop in
/// [`crate::rpc::websocket`].
fn encode(response: DbResponse, fmt: Format) -> Message {
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
	match fmt.res_ws(response) {
		Ok((_len, msg)) => msg,
		Err(err) => {
			let (_len, msg) = fmt
				.res_ws(DbResponse::failure(id, session_id, err))
				.expect("Serialising internal error should always succeed");
			msg
		}
	}
}

/// Send the response to the WebSocket channel, waiting for capacity.
///
/// Used by the request/response paths, whose sends are paced by the client's
/// own request rate: a connection can only have as many of these in flight as
/// it has requests outstanding, so waiting bounds itself. Anything the server
/// produces unprompted must use [`try_send`] instead.
pub async fn send(response: DbResponse, fmt: Format, chn: Sender<Message>) {
	// Send the message to the write channel
	let _ = chn.send(encode(response, fmt)).await;
}

/// Queue the response on the WebSocket channel without waiting for capacity.
///
/// `Err(Full)` means the client has stopped reading -- the write loop drains
/// the queue as fast as the socket accepts it, so nothing the caller can do
/// would make room -- and `Err(Closed)` that the connection has already gone
/// away. The two call for different answers, so they are handed back
/// unmerged.
pub fn try_send(
	response: DbResponse,
	fmt: Format,
	chn: &Sender<Message>,
) -> Result<(), TrySendError<Message>> {
	chn.try_send(encode(response, fmt))
}
