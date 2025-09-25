use std::sync::Arc;

use axum::extract::ws::Message;
use opentelemetry::Context as TelemetryContext;
use revision::revisioned;
use serde::Serialize;
use surrealdb_core::rpc::DbResponse;
use tokio::sync::mpsc::Sender;
use tracing::Span;

use crate::core::rpc::DbResult;
use crate::core::rpc::format::Format;
use crate::rpc::failure::Failure;
use crate::rpc::format::WsFormat;
use crate::telemetry::metrics::ws::record_rpc;
use crate::types::Value;

/// Send the response to the WebSocket channel
pub async fn send(
	response: DbResponse,
	cx: Arc<TelemetryContext>,
	fmt: Format,
	chn: Sender<Message>,
) {
	// Get the request id
	let id = response.id.clone();
	// Create a new tracing span
	let span = Span::current();
	// Log the rpc response call
	debug!("Process RPC response");
	// Store whether this was an error
	let is_error = response.result.is_err();
	// Record tracing details for errors
	if let Err(err) = &response.result {
		span.record("otel.status_code", "ERROR");
		span.record("rpc.error_code", err.code);
		span.record("rpc.error_message", err.message.as_ref());
	}
	// Process the response for the format
	let (len, msg) = match fmt.res_ws(response) {
		Ok((l, m)) => (l, m),
		Err(err) => {
			fmt.res_ws(failure(id, err)).expect("Serialising internal error should always succeed")
		}
	};
	// Send the message to the write channel
	if chn.send(msg).await.is_ok() {
		record_rpc(cx.as_ref(), len, is_error);
	};
}

// impl From<DbResponse> for Value {
// 	fn from(value: DbResponse) -> Self {
// 		value.into_value()
// 	}
// }

// /// Create a JSON RPC result response
// pub fn success<T: Into<DbResult>>(id: Option<Value>, data: T) -> DbResponse {
// 	DbResponse {
// 		id,
// 		result: Ok(data.into()),
// 	}
// }

// /// Create a JSON RPC failure response
// pub fn failure(id: Option<Value>, err: Failure) -> DbResponse {
// 	DbResponse {
// 		id,
// 		result: Err(err),
// 	}
// }

// pub trait IntoRpcResponse {
// 	fn into_response(self, id: Option<Value>) -> DbResponse;
// }

// impl<T, E> IntoRpcResponse for Result<T, E>
// where
// 	T: Into<DbResult>,
// 	E: Into<Failure>,
// {
// 	fn into_response(self, id: Option<Value>) -> DbResponse {
// 		match self {
// 			Ok(v) => success(id, v.into()),
// 			Err(err) => failure(id, err.into()),
// 		}
// 	}
// }
