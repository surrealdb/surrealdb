use crate::err;
use crate::telemetry::metrics::ws::record_rpc;
use axum::extract::ws::Message;
use opentelemetry::Context as TelemetryContext;
use serde::Serialize;
use serde_json::{json, Value as Json};
use std::borrow::Cow;
use surrealdb::channel::Sender;
use surrealdb::dbs;
use surrealdb::dbs::Notification;
use surrealdb::sql;
use surrealdb::sql::Value;
use tracing::Span;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OutputFormat {
	Json, // JSON
	Cbor, // CBOR
	Pack, // MessagePack
	Full, // Full type serialization
}

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
	fn simplify(self) -> Json {
		let mut value = match self.result {
			Ok(data) => {
				let value = match data {
					Data::Query(vec) => sql::to_value(vec).unwrap(),
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
	pub async fn send(self, out: OutputFormat, chn: &Sender<Message>) {
		let span = Span::current();

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

		let (res_size, message) = match out {
			OutputFormat::Json => {
				let res = serde_json::to_string(&self.simplify()).unwrap();
				(res.len(), Message::Text(res))
			}
			OutputFormat::Cbor => {
				let res = serde_cbor::to_vec(&self.simplify()).unwrap();
				(res.len(), Message::Binary(res))
			}
			OutputFormat::Pack => {
				let res = serde_pack::to_vec(&self.simplify()).unwrap();
				(res.len(), Message::Binary(res))
			}
			OutputFormat::Full => {
				let res = surrealdb::sql::serde::serialize(&self).unwrap();
				(res.len(), Message::Binary(res))
			}
		};

		if chn.send(message).await.is_ok() {
			record_rpc(&TelemetryContext::current(), res_size, is_error);
		};
	}
}

#[derive(Clone, Debug, Serialize)]
pub struct Failure {
	code: i64,
	message: Cow<'static, str>,
}

#[allow(dead_code)]
impl Failure {
	pub const PARSE_ERROR: Failure = Failure {
		code: -32700,
		message: Cow::Borrowed("Parse error"),
	};

	pub const INVALID_REQUEST: Failure = Failure {
		code: -32600,
		message: Cow::Borrowed("Invalid Request"),
	};

	pub const METHOD_NOT_FOUND: Failure = Failure {
		code: -32601,
		message: Cow::Borrowed("Method not found"),
	};

	pub const INVALID_PARAMS: Failure = Failure {
		code: -32602,
		message: Cow::Borrowed("Invalid params"),
	};

	pub const INTERNAL_ERROR: Failure = Failure {
		code: -32603,
		message: Cow::Borrowed("Internal error"),
	};

	pub fn custom<S>(message: S) -> Failure
	where
		Cow<'static, str>: From<S>,
	{
		Failure {
			code: -32000,
			message: message.into(),
		}
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
