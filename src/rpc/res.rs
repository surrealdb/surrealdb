use serde::Serialize;
use std::borrow::Cow;
use surrealdb::channel::Sender;
use surrealdb::sql::serde::serialize_internal;
use surrealdb::sql::Value;
use warp::ws::Message;

#[derive(Clone)]
pub enum Output {
	Json, // JSON
	Cbor, // CBOR
	Pack, // MsgPack
	Full, // Full type serialization
}

#[derive(Serialize)]
pub struct Response<T> {
	#[serde(skip_serializing_if = "Option::is_none")]
	id: Option<Value>,
	#[serde(flatten)]
	content: Content<T>,
}

#[derive(Serialize)]
enum Content<T> {
	#[serde(rename = "result")]
	Success(T),
	#[serde(rename = "error")]
	Failure(Failure),
}

impl<T: Serialize> Response<T> {
	/// Send the response to the channel
	pub async fn send(self, out: Output, chn: Sender<Message>) {
		match out {
			Output::Json => {
				let res = serde_json::to_string(&self).unwrap();
				let res = Message::text(res);
				let _ = chn.send(res).await;
			}
			Output::Cbor => {
				let res = serde_cbor::to_vec(&self).unwrap();
				let res = Message::binary(res);
				let _ = chn.send(res).await;
			}
			Output::Pack => {
				let res = serde_pack::to_vec(&self).unwrap();
				let res = Message::binary(res);
				let _ = chn.send(res).await;
			}
			Output::Full => {
				let res = serialize_internal(|| serde_pack::to_vec(&self).unwrap());
				let res = Message::binary(res);
				let _ = chn.send(res).await;
			}
		}
	}
}

#[derive(Clone, Debug, Serialize)]
pub struct Failure {
	code: i64,
	message: Cow<'static, str>,
}

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
pub fn success<S: Serialize>(id: Option<Value>, val: S) -> Response<S> {
	Response {
		id,
		content: Content::Success(val),
	}
}

/// Create a JSON RPC failure response
pub fn failure(id: Option<Value>, err: Failure) -> Response<Value> {
	Response {
		id,
		content: Content::Failure(err),
	}
}
