use crate::rpc::failure::Failure;
use crate::rpc::format::Format;
use crate::telemetry::metrics::ws::record_rpc;
use axum::extract::ws::Message;
use opentelemetry::Context as TelemetryContext;
use revision::revisioned;
use serde::Serialize;
use serde_json::Value as Json;
use std::sync::Arc;
use surrealdb::channel::Sender;
use surrealdb::rpc::Data;
use surrealdb::sql::Value;
use tracing::Span;

#[derive(Debug, Serialize)]
#[revisioned(revision = 1)]
pub struct Response {
	id: Option<Value>,
	result: Result<Data, Failure>,
}

impl Response {
	/// Convert and simplify the value into JSON
	#[inline]
	pub fn into_json(self) -> Json {
		Json::from(self.into_value())
	}

	#[inline]
	pub fn into_value(self) -> Value {
		let mut value = match self.result {
			Ok(val) => map! {
				"result" => Value::from(val),
			},
			Err(err) => map! {
				"error" => Value::from(err),
			},
		};
		if let Some(id) = self.id {
			value.insert("id", id);
		}
		value.into()
	}

	/// Send the response to the WebSocket channel
	pub async fn send(self, cx: Arc<TelemetryContext>, fmt: Format, chn: &Sender<Message>) {
		// Create a new tracing span
		let span = Span::current();
		// Log the rpc response call
		debug!("Process RPC response");

		let is_error = self.result.is_err();
		if let Err(err) = &self.result {
			span.record("otel.status_code", "Error");
			span.record(
				"otel.status_message",
				format!("code: {}, message: {}", err.code, err.message),
			);
			span.record("rpc.error_code", err.code);
			span.record("rpc.error_message", err.message.as_ref());
		}
		// Process the response for the format
		let (len, msg) = fmt.res_ws(self).unwrap();
		// Send the message to the write channel
		if chn.send(msg).await.is_ok() {
			record_rpc(cx.as_ref(), len, is_error);
		};
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
