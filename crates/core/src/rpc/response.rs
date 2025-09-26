use std::fmt::Display;
use std::time::Duration;

use anyhow::Context;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::object;

use crate::dbs::QueryResult;
use crate::rpc::RpcError;
use crate::types::{
	PublicArray, PublicKind, PublicNotification, PublicObject, PublicValue, SurrealValue,
};
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
#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
pub enum DbResult {
	/// Generally methods return a `expr::Value`
	Other(PublicValue),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<dbs::QueryResult>),
	/// Live queries return a notification
	Live(PublicNotification),
	// Add new variants here
}

impl SurrealValue for DbResult {
	fn kind_of() -> PublicKind {
		PublicKind::Any
	}

	fn is_value(_value: &PublicValue) -> bool {
		true
	}

	fn into_value(self) -> PublicValue {
		match self {
			DbResult::Query(v) => {
				let converted: Vec<PublicValue> = v.into_iter().map(|x| x.into_value()).collect();
				PublicValue::Array(PublicArray::from_values(converted))
			}
			DbResult::Live(v) => PublicValue::Object(object! {
				id: PublicValue::Uuid(v.id),
				action: v.action.into_value(),
				record: v.record,
				result: v.result,
			}),
			DbResult::Other(v) => v,
		}
	}

	fn from_value(value: PublicValue) -> anyhow::Result<Self> {
		match value {
			PublicValue::Array(arr) => {
				let results = arr
					.inner()
					.iter()
					.cloned()
					.map(QueryResult::from_value)
					.collect::<anyhow::Result<Vec<_>>>()?;
				Ok(DbResult::Query(results))
			}
			PublicValue::Object(obj) => {
				// Check if this is a Live result
				if obj.get("id").is_some() && obj.get("action").is_some() {
					let mut obj = obj.inner().clone();
					let id = obj.remove("id").context("Missing id")?;
					let action = obj.remove("action").context("Missing action")?;
					let record = obj.remove("record").unwrap_or(PublicValue::None);
					let result = obj.remove("result").unwrap_or(PublicValue::None);

					let PublicValue::Uuid(uuid) = id else {
						anyhow::bail!("Expected UUID for id field");
					};
					let PublicValue::String(action_str) = action else {
						anyhow::bail!("Expected string for action field");
					};

					// Parse action string to PublicAction
					let action = match action_str.as_str() {
						"CREATE" => crate::types::PublicAction::Create,
						"UPDATE" => crate::types::PublicAction::Update,
						"DELETE" => crate::types::PublicAction::Delete,
						_ => anyhow::bail!("Invalid action: {}", action_str),
					};

					Ok(DbResult::Live(PublicNotification::new(uuid, action, record, result)))
				} else {
					Ok(DbResult::Other(PublicValue::Object(obj)))
				}
			}
			other => Ok(DbResult::Other(other)),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue, Serialize, Deserialize)]
pub struct DbResultError {
	pub code: i64,
	pub message: String,
}

impl DbResultError {
	pub fn parse_error() -> DbResultError {
		DbResultError {
			code: -32700,
			message: "Parse error".to_string(),
		}
	}

	pub fn invalid_request() -> DbResultError {
		DbResultError {
			code: -32600,
			message: "Invalid Request".to_string(),
		}
	}

	pub fn method_not_found() -> DbResultError {
		DbResultError {
			code: -32601,
			message: "Method not found".to_string(),
		}
	}

	pub fn invalid_params() -> DbResultError {
		DbResultError {
			code: -32602,
			message: "Invalid params".to_string(),
		}
	}

	/*
	pub fn internal_error() -> DbResultError {
		DbResultError {
			code: -32603,
			message: "Internal error".to_string(),
		}
	}
	*/

	pub fn custom(message: impl Into<String>) -> DbResultError {
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

impl From<RpcError> for DbResultError {
	fn from(error: RpcError) -> Self {
		todo!("STU")
	}
}

pub type DbResponseResult = Result<DbResult, DbResultError>;

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DbResponse {
	pub id: Option<PublicValue>,
	pub result: DbResponseResult,
}

impl DbResponse {
	pub fn new(id: Option<PublicValue>, result: DbResponseResult) -> Self {
		Self {
			id,
			result,
		}
	}

	pub fn failure(id: Option<PublicValue>, error: DbResultError) -> Self {
		Self {
			id,
			result: Err(error),
		}
	}

	pub fn success(id: Option<PublicValue>, result: DbResult) -> Self {
		Self {
			id,
			result: Ok(result),
		}
	}

	pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
		// Decode using revision format
		crate::rpc::format::revision::decode(bytes)
	}
}

impl SurrealValue for DbResponse {
	fn kind_of() -> PublicKind {
		PublicKind::Object
	}

	fn is_value(value: &PublicValue) -> bool {
		matches!(value, PublicValue::Object(_))
	}

	fn into_value(self) -> PublicValue {
		let mut value = match self.result {
			Ok(result) => map! { "result".to_string() => result.into_value() },
			Err(err) => map! {
				"error".to_string() => err.into_value(),
			},
		};
		if let Some(id) = self.id {
			value.insert("id".to_string(), id);
		}
		PublicValue::Object(PublicObject::from(value))
	}

	fn from_value(value: PublicValue) -> anyhow::Result<Self> {
		let PublicValue::Object(mut obj) = value else {
			anyhow::bail!("Expected object for DbResponse");
		};

		let id = obj.remove("id");

		let result = if let Some(result) = obj.remove("result") {
			Ok(DbResult::from_value(result)?)
		} else if let Some(error) = obj.remove("error") {
			Err(DbResultError::from_value(error)?)
		} else {
			anyhow::bail!("DbResponse must have either 'result' or 'error' field");
		};

		Ok(DbResponse {
			id,
			result,
		})
	}
}
