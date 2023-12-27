use crate::err;
use crate::rpc::failure::Failure;
use crate::rpc::format::Format;
use crate::telemetry::metrics::ws::record_rpc;
use axum::extract::ws::Message;
use opentelemetry::Context as TelemetryContext;
use serde::Serialize;
use serde_json::{json, Value as Json};
use surrealdb::channel::Sender;
use surrealdb::dbs;
use surrealdb::dbs::Notification;
use surrealdb::sql;
use surrealdb::sql::Value;
use tracing::Span;

/// The data returned by the database
// The variants here should be in exactly the same order as `surrealdb::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
#[derive(Debug, Serialize)]
pub enum Data {
	/// Generally methods return a `sql::Value`
	Other(Value),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<dbs::Response>),
	/// Live queries return a notification
	Live(Notification),
	// Add new variants here
}

impl From<Value> for Data {
	fn from(v: Value) -> Self {
		Data::Other(v)
	}
}

impl From<String> for Data {
	fn from(v: String) -> Self {
		Data::Other(Value::from(v))
	}
}

impl From<Vec<dbs::Response>> for Data {
	fn from(v: Vec<dbs::Response>) -> Self {
		Data::Query(v)
	}
}

impl From<Notification> for Data {
	fn from(n: Notification) -> Self {
		Data::Live(n)
	}
}

#[derive(Debug, Serialize)]
pub struct Response {
	id: Option<Value>,
	result: Result<Data, Failure>,
}

impl Response {
	/// Convert and simplify the value into JSON
	#[inline]
	pub fn simplify(self) -> Json {
		let mut value = match self.result {
			Ok(data) => {
				let value = match data {
					Data::Query(vec) => {
						let responses: Vec<_> =
							vec.into_iter().map(dbs::ApiResponse::from).collect();
						sql::to_value(responses).unwrap()
					}
					Data::Live(notification) => sql::to_value(notification).unwrap(),
					Data::Other(value) => value,
				};
				json!({
					"result": Json::from(value),
				})
			}
			Err(failure) => json!({
				"error": failure,
			}),
		};
		if let Some(id) = self.id {
			value["id"] = id.into();
		}
		value
	}

	/// Send the response to the WebSocket channel
	pub async fn send(self, fmt: Format, chn: &Sender<Message>) {
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
		let (len, msg) = fmt.res(self).unwrap();
		// Send the message to the write channel
		if chn.send(msg).await.is_ok() {
			record_rpc(&TelemetryContext::current(), len, is_error);
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

impl From<err::Error> for Failure {
	fn from(err: err::Error) -> Self {
		Failure::custom(err.to_string())
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
