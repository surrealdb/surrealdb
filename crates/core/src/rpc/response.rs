use std::borrow::Cow;
use std::fmt::Display;
use std::time::Duration;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::{Kind, Object, SurrealValue, Value};

use crate::dbs::executor::convert_value_to_public_value;
use crate::types::PublicNotification;
use crate::{dbs, map};

/// Query statistics
// #[revisioned(revision = 1)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct DbResultStats {
	/// The time taken to execute the query.
	///
	/// Note: This comes from the `time` field of the [`crate::dbs::Response`] struct.
	pub execution_time: Option<Duration>,
}

impl DbResultStats {
	pub fn with_execution_time(mut self, execution_time: Duration) -> Self {
		self.execution_time = Some(execution_time);
		self
	}
}

/// The data returned by the database
// The variants here should be in exactly the same order as `crate::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
// #[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
pub enum DbResult {
	/// Generally methods return a `expr::Value`
	Other(Value),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<dbs::QueryResult>),
	/// Live queries return a notification
	Live(PublicNotification),
	// Add new variants here
}

impl SurrealValue for DbResult {
	fn kind_of() -> Kind {
		Kind::Any
	}

	fn is_value(value: &Value) -> bool {
		true
	}

	fn into_value(self) -> Value {
		match self {
			DbResult::Query(v) => {
				let converted: Vec<Value> = v.into_iter().map(|x| x.into_value()).collect();
				Value::Array(surrealdb_types::Array::from_values(converted))
			}
			DbResult::Live(v) => Value::from(surrealdb_types::Object::from_map(map! {
				"id".to_owned() => Value::Uuid(surrealdb_types::Uuid(v.id.0)),
				"action".to_owned() => Value::String(v.action.to_string()),
				"record".to_owned() => v.record,
				"result".to_owned() => v.result,

			})),
			DbResult::Other(v) => v,
		}
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		todo!("STU")
	}
}

// #[revisioned(revision = 1)]
#[derive(Clone, Debug, SurrealValue, Serialize, Deserialize)]
pub struct DbResultError {
	pub(crate) code: i64,
	pub(crate) message: Cow<'static, str>,
}

impl DbResultError {
	pub const PARSE_ERROR: DbResultError = DbResultError {
		code: -32700,
		message: Cow::Borrowed("Parse error"),
	};

	pub const INVALID_REQUEST: DbResultError = DbResultError {
		code: -32600,
		message: Cow::Borrowed("Invalid Request"),
	};

	pub const METHOD_NOT_FOUND: DbResultError = DbResultError {
		code: -32601,
		message: Cow::Borrowed("Method not found"),
	};

	pub const INVALID_PARAMS: DbResultError = DbResultError {
		code: -32602,
		message: Cow::Borrowed("Invalid params"),
	};

	/*
	pub const INTERNAL_ERROR: Failure = Failure {
		code: -32603,
		message: Cow::Borrowed("Internal error"),
	};
	*/

	pub fn custom(message: impl Into<Cow<'static, str>>) -> DbResultError {
		DbResultError {
			code: -32000,
			message: message.into(),
		}
	}
}

impl Display for DbResultError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.message)
	}
}

impl std::error::Error for DbResultError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		None
	}
}

// #[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DbResponse {
	pub id: Option<Value>,
	pub result: Result<DbResult, DbResultError>,
}

impl DbResponse {
	pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
		// crate::rpc::format::revision::decode(bytes)
		todo!("STU")
	}
}

impl SurrealValue for DbResponse {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(_))
	}

	fn into_value(self) -> Value {
		let mut value = match self.result {
			Ok(result) => map! { "result".to_string() => result.into_value() },
			Err(err) => map! {
				"error".to_string() => err.into_value(),
			},
		};
		if let Some(id) = self.id {
			value.insert("id".to_string(), id);
		}
		Value::Object(Object::from(value))
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		todo!("STU")
	}
}

impl From<DbResponse> for Value {
	fn from(value: DbResponse) -> Self {
		value.into_value()
	}
}

/// Create a JSON RPC result response
pub fn success<T: Into<DbResult>>(id: Option<Value>, data: T) -> DbResponse {
	DbResponse {
		id,
		result: Ok(data.into()),
	}
}

/// Create a JSON RPC failure response
pub fn failure(id: Option<Value>, err: DbResultError) -> DbResponse {
	DbResponse {
		id,
		result: Err(err),
	}
}

pub trait IntoRpcResponse {
	fn into_response(self, id: Option<Value>) -> DbResponse;
}

impl<T, E> IntoRpcResponse for Result<T, E>
where
	T: Into<DbResult>,
	E: Into<DbResultError>,
{
	fn into_response(self, id: Option<Value>) -> DbResponse {
		match self {
			Ok(v) => success(id, v.into()),
			Err(err) => failure(id, err.into()),
		}
	}
}
