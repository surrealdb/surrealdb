use std::sync::Arc;

use axum::extract::ws::Message;
use opentelemetry::Context as TelemetryContext;
use surrealdb_core::rpc::DbResponse;
use surrealdb_core::rpc::format::Format;
use tokio::sync::mpsc::Sender;
use tracing::Span;

use crate::rpc::format::WsFormat;
use crate::telemetry::metrics::ws::record_rpc;

/// Send the response to the WebSocket channel
pub async fn send(
	response: DbResponse,
	cx: Arc<TelemetryContext>,
	fmt: Format,
	chn: Sender<Message>,
) {
	// Get the request id
	let id = response.id.clone();
	let session_id = response.session_id;
	// Create a new tracing span
	let span = Span::current();
	// Log the rpc response call
	debug!("Process RPC response");
	// Store whether this was an error
	let is_error = response.result.is_err();
	// Record tracing details for errors
	if let Err(err) = &response.result {
		span.record("otel.status_code", "ERROR");
		span.record("rpc.error_kind", format!("{:?}", err.kind_str()));
		span.record("rpc.error_message", err.message());
	}
	// Process the response for the format
	let (len, msg) = match fmt.res_ws(response) {
		Ok((l, m)) => (l, m),
		Err(err) => fmt
			.res_ws(DbResponse::failure(id, session_id, err))
			.expect("Serialising internal error should always succeed"),
	};
	// Send the message to the write channel
	if chn.send(msg).await.is_ok() {
		record_rpc(cx.as_ref(), len, is_error);
	};
}
