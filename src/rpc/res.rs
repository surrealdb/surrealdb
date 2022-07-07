use serde::Serialize;
use std::borrow::Cow;
use surrealdb::channel::Sender;
use surrealdb::sql::Value;
use warp::ws::Message;

#[derive(Serialize)]
enum Content {
	#[serde(rename = "result")]
	Success(Value),
	#[serde(rename = "error")]
	Failure(Failure),
}

#[derive(Serialize)]
pub struct Response {
	id: Option<String>,
	#[serde(flatten)]
	content: Content,
}

impl Response {
	// Send the response to the channel
	pub async fn send(self, chn: Sender<Message>) {
		let res = serde_json::to_string(&self).unwrap();
		let res = Message::text(res);
		let _ = chn.send(res).await;
	}
	// Create a JSON RPC result response
	pub fn success(id: Option<String>, val: Value) -> Response {
		Response {
			id,
			content: Content::Success(val),
		}
	}
	// Create a JSON RPC failure response
	pub fn failure(id: Option<String>, err: Failure) -> Response {
		Response {
			id,
			content: Content::Failure(err),
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
