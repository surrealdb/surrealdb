#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

use crate::method::Method;
use crate::param::DbResponse;
use crate::param::Param;
use crate::protocol::Status;
use crate::ErrorKind;
use crate::Result;
use crate::Route;
use serde::Deserialize;
use std::mem;
use std::time::Duration;
use surrealdb::sql::Array;
use surrealdb::sql::Value;

type WsRoute = Route<(i64, Method, Param), Result<DbResponse>>;

const PATH: &str = "rpc";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const PING_METHOD: &str = "ping";

/// A WebSocket client for communicating with the server via WebSockets
#[derive(Debug, Clone)]
pub struct Client {
	id: i64,
	method: Method,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Failure {
	pub(crate) code: i64,
	pub(crate) message: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum QueryMethodResponse {
	Value(Value),
	String(String),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum SuccessValue {
	Query(Vec<(String, Status, QueryMethodResponse)>),
	Other(Value),
}

#[derive(Debug, Deserialize)]
pub(crate) enum Content {
	#[serde(rename = "result")]
	Success(SuccessValue),
	#[serde(rename = "error")]
	Failure(Failure),
}

impl DbResponse {
	fn from((method, content): (Method, Content)) -> Result<Self> {
		match content {
			Content::Success(SuccessValue::Query(results)) => Ok(DbResponse::Query(
				results
					.into_iter()
					.map(|(_duration, status, result)| match status {
						Status::Ok => match result {
							QueryMethodResponse::Value(value) => match value {
								Value::Array(Array(values)) => Ok(values),
								Value::None | Value::Null => Ok(vec![]),
								value => Ok(vec![value]),
							},
							QueryMethodResponse::String(string) => Ok(vec![string.into()]),
						},
						Status::Err => match result {
							QueryMethodResponse::Value(message) => {
								Err(ErrorKind::Query.with_message(message.to_string()))
							}
							QueryMethodResponse::String(message) => {
								Err(ErrorKind::Query.with_message(message))
							}
						},
					})
					.collect(),
			)),
			Content::Success(SuccessValue::Other(mut value)) => {
				if let Method::Create | Method::Delete = method {
					if let Value::Array(Array(array)) = &mut value {
						match &mut array[..] {
							[] => {
								value = Value::None;
							}
							[v] => {
								value = mem::take(v);
							}
							_ => {}
						}
					}
				}
				Ok(DbResponse::Other(value))
			}
			Content::Failure(failure) => Err(failure.into()),
		}
	}
}

#[derive(Debug, Deserialize)]
pub struct Response {
	#[serde(skip_serializing_if = "Option::is_none")]
	id: Option<Value>,
	#[serde(flatten)]
	pub(crate) content: Content,
}
