use std::sync::Arc;

use axum::extract::ws::Message;
use opentelemetry::Context as TelemetryContext;
use revision::revisioned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use tracing::Span;

use crate::core::rpc::Data;
use crate::core::rpc::format::Format;
use crate::core::val::Value;
use crate::rpc::failure::Failure;
use crate::rpc::format::WsFormat;
use crate::telemetry::metrics::ws::record_rpc;

#[revisioned(revision = 1)]
#[derive(Debug, Serialize)]
pub struct Response {
	id: Option<Value>,
	result: Result<Data, Failure>,
}

impl Response {
	#[inline]
	pub fn into_value(self) -> Value {
		let mut value = match self.result {
			Ok(val) => map! { "result" => val.into_value() },
			Err(err) => map! {
				"error" => err.into_value(),
			},
		};
		if let Some(id) = self.id {
			value.insert("id", id);
		}
		value.into()
	}

	/// Send the response to the WebSocket channel
	pub async fn send(self, cx: Arc<TelemetryContext>, fmt: Format, chn: Sender<Message>) {
		// Get the request id
		let id = self.id.clone();
		// Create a new tracing span
		let span = Span::current();
		// Log the rpc response call
		debug!("Process RPC response");
		// Store whether this was an error
		let is_error = self.result.is_err();
		// Record tracing details for errors
		if let Err(err) = &self.result {
			span.record("otel.status_code", "ERROR");
			span.record("rpc.error_code", err.code);
			span.record("rpc.error_message", err.message.as_ref());
		}
		// Process the response for the format
		let (len, msg) = match fmt.res_ws(self) {
			Ok((l, m)) => (l, m),
			Err(err) => fmt
				.res_ws(failure(id, err))
				.expect("Serialising internal error should always succeed"),
		};
		// Send the message to the write channel
		if chn.send(msg).await.is_ok() {
			record_rpc(cx.as_ref(), len, is_error);
		};
	}
}

impl From<Response> for Value {
	fn from(value: Response) -> Self {
		value.into_value()
	}
}

/// Create a JSON RPC result response
pub fn success<T: Into<Data>>(id: Option<Value>, data: T) -> Response {
	Response {
		id,
		result: Ok(data.into()),
	}
}

/// Create a JSON RPC failure response
pub fn failure(id: Option<Value>, err: Failure) -> Response {
	Response {
		id,
		result: Err(err),
	}
}

pub trait IntoRpcResponse {
	fn into_response(self, id: Option<Value>) -> Response;
}

impl<T, E> IntoRpcResponse for Result<T, E>
where
	T: Into<Data>,
	E: Into<Failure>,
{
	fn into_response(self, id: Option<Value>) -> Response {
		match self {
			Ok(v) => success(id, v.into()),
			Err(err) => failure(id, err.into()),
		}
	}
}
