use std::fmt::Display;
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use surrealdb_types::{ToSql, kind, object};
use thiserror::Error;
use uuid::Uuid;

use crate::dbs;
use crate::dbs::{QueryResult, QueryType};
use crate::rpc::RpcError;
use crate::rpc::request::SESSION_ID;
use crate::types::{
	PublicArray, PublicKind, PublicNotification, PublicObject, PublicValue, SurrealValue,
};

/// Query statistics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct DbResultStats {
	/// The time taken to execute the query.
	///
	/// Note: This comes from the `time` field of the [`crate::dbs::QueryResult`] struct.
	pub execution_time: Option<Duration>,
	pub query_type: Option<QueryType>,
}

impl DbResultStats {
	pub fn with_execution_time(mut self, execution_time: Duration) -> Self {
		self.execution_time = Some(execution_time);
		self
	}

	pub fn with_query_type(mut self, query_type: QueryType) -> Self {
		self.query_type = Some(query_type);
		self
	}
}

/// The data returned by the database
// The variants here should be in exactly the same order as `crate::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
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
		kind!(array | {
			id: uuid,
			session: uuid | none,
			action: string,
			record: any,
			result: any,
		} | any)
	}

	fn is_value(_value: &PublicValue) -> bool {
		true
	}

	fn into_value(self) -> PublicValue {
		match self {
			DbResult::Query(v) => {
				let converted: Vec<PublicValue> = v.into_iter().map(|x| x.into_value()).collect();
				PublicValue::Array(PublicArray::from(converted))
			}
			DbResult::Live(v) => PublicValue::Object(object! {
				id: PublicValue::Uuid(v.id),
				session: v.session.map(PublicValue::Uuid),
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
					.into_inner()
					.into_iter()
					.map(QueryResult::from_value)
					.collect::<anyhow::Result<Vec<_>>>()?;
				Ok(DbResult::Query(results))
			}
			PublicValue::Object(obj) => {
				// Check if this is a Live result
				if obj.get("id").is_some() && obj.get("action").is_some() {
					let mut obj = obj.into_inner();
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

					let session = match obj.remove(SESSION_ID) {
						Some(session) => SurrealValue::from_value(session)?,
						None => None,
					};

					// Parse action string to PublicAction
					let action = match action_str.as_str() {
						"CREATE" => crate::types::PublicAction::Create,
						"UPDATE" => crate::types::PublicAction::Update,
						"DELETE" => crate::types::PublicAction::Delete,
						_ => anyhow::bail!("Invalid action: {}", action_str),
					};

					Ok(DbResult::Live(PublicNotification::new(
						uuid, session, action, record, result,
					)))
				} else {
					Ok(DbResult::Other(PublicValue::Object(obj)))
				}
			}
			other => Ok(DbResult::Other(other)),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum DbResultError {
	ParseError(String),
	InvalidRequest(String),
	MethodNotFound(String),
	MethodNotAllowed(String),
	InvalidParams(String),
	LiveQueryNotSupported,
	BadLiveQueryConfig(String),
	BadGraphQLConfig(String),
	InternalError(String),
	Thrown(String),
	SerializationError(String),
	DeserializationError(String),
	ClientSideError(String),
	InvalidAuth(String),
	QueryNotExecuted(String),
	QueryTimedout(String),
	QueryCancelled,
}

impl DbResultError {
	const PARSE_ERROR: i64 = -32700;
	const INVALID_REQUEST: i64 = -32600;
	const METHOD_NOT_FOUND: i64 = -32601;
	const METHOD_NOT_ALLOWED: i64 = -32602;
	const INVALID_PARAMS: i64 = -32603;
	const LIVE_QUERY_NOT_SUPPORTED: i64 = -32604;
	const BAD_LIVE_QUERY_CONFIG: i64 = -32605;
	const BAD_GRAPHQL_CONFIG: i64 = -32606;
	const INTERNAL_ERROR: i64 = -32000;
	const CLIENT_SIDE_ERROR: i64 = -32001;
	const INVALID_AUTH: i64 = -32002;
	const QUERY_NOT_EXECUTED: i64 = -32003;
	const QUERY_TIMEDOUT: i64 = -32004;
	const QUERY_CANCELLED: i64 = -32005;
	const THROWN: i64 = -32006;
	const SERIALIZATION_ERROR: i64 = -32007;
	const DESERIALIZATION_ERROR: i64 = -32008;

	pub fn code(&self) -> i64 {
		match self {
			Self::ParseError(_) => Self::PARSE_ERROR,
			Self::InvalidRequest(_) => Self::INVALID_REQUEST,
			Self::MethodNotFound(_) => Self::METHOD_NOT_FOUND,
			Self::MethodNotAllowed(_) => Self::METHOD_NOT_ALLOWED,
			Self::InvalidParams(_) => Self::INVALID_PARAMS,
			Self::LiveQueryNotSupported => Self::LIVE_QUERY_NOT_SUPPORTED,
			Self::BadLiveQueryConfig(_) => Self::BAD_LIVE_QUERY_CONFIG,
			Self::BadGraphQLConfig(_) => Self::BAD_GRAPHQL_CONFIG,
			Self::InternalError(_) => Self::INTERNAL_ERROR,
			Self::Thrown(_) => Self::THROWN,
			Self::SerializationError(_) => Self::SERIALIZATION_ERROR,
			Self::DeserializationError(_) => Self::DESERIALIZATION_ERROR,
			Self::ClientSideError(_) => Self::CLIENT_SIDE_ERROR,
			Self::InvalidAuth(_) => Self::INVALID_AUTH,
			Self::QueryNotExecuted(_) => Self::QUERY_NOT_EXECUTED,
			Self::QueryTimedout(_) => Self::QUERY_TIMEDOUT,
			Self::QueryCancelled => Self::QUERY_CANCELLED,
		}
	}

	pub fn message(&self) -> String {
		match self {
			Self::ParseError(msg) => msg.clone(),
			Self::InvalidRequest(msg) => msg.clone(),
			Self::MethodNotFound(msg) => msg.clone(),
			Self::MethodNotAllowed(msg) => msg.clone(),
			Self::InvalidParams(msg) => msg.clone(),
			Self::LiveQueryNotSupported => "Live query not supported".to_string(),
			Self::BadLiveQueryConfig(msg) => msg.clone(),
			Self::BadGraphQLConfig(msg) => msg.clone(),
			Self::InternalError(msg) => msg.clone(),
			Self::Thrown(msg) => msg.clone(),
			Self::SerializationError(msg) => msg.clone(),
			Self::DeserializationError(msg) => msg.clone(),
			Self::ClientSideError(msg) => msg.clone(),
			Self::InvalidAuth(msg) => msg.clone(),
			Self::QueryNotExecuted(msg) => msg.clone(),
			Self::QueryTimedout(timeout) => format!("Query timed out: {timeout}"),
			Self::QueryCancelled => {
				"The query was not executed due to a cancelled transaction".to_string()
			}
		}
	}

	pub fn from_code(code: i64, message: String) -> DbResultError {
		match code {
			Self::PARSE_ERROR => Self::ParseError(message),
			Self::INVALID_REQUEST => Self::InvalidRequest(message),
			Self::METHOD_NOT_FOUND => Self::MethodNotFound(message),
			Self::METHOD_NOT_ALLOWED => Self::MethodNotAllowed(message),
			Self::INVALID_PARAMS => Self::InvalidParams(message),
			Self::LIVE_QUERY_NOT_SUPPORTED => Self::LiveQueryNotSupported,
			Self::BAD_LIVE_QUERY_CONFIG => Self::BadLiveQueryConfig(message),
			Self::BAD_GRAPHQL_CONFIG => Self::BadGraphQLConfig(message),
			Self::INTERNAL_ERROR => Self::InternalError(message),
			Self::THROWN => Self::Thrown(message),
			Self::SERIALIZATION_ERROR => Self::SerializationError(message),
			Self::DESERIALIZATION_ERROR => Self::DeserializationError(message),
			Self::CLIENT_SIDE_ERROR => Self::ClientSideError(message),
			Self::INVALID_AUTH => Self::InvalidAuth(message),
			Self::QUERY_NOT_EXECUTED => Self::QueryNotExecuted(message),
			Self::QUERY_TIMEDOUT => Self::QueryTimedout(message),
			Self::QUERY_CANCELLED => Self::QueryCancelled,
			// For any unknown code, map to InternalError
			_ => Self::InternalError(format!("Unknown error code {code}: {message}")),
		}
	}
}

impl SurrealValue for DbResultError {
	fn kind_of() -> PublicKind {
		kind!({
		  code: int,
		  message: string
		})
	}

	fn into_value(self) -> PublicValue {
		PublicValue::Object(object! {
			"code": self.code(),
			"message": self.message(),
		})
	}

	fn from_value(value: PublicValue) -> anyhow::Result<Self> {
		match value {
			PublicValue::Object(mut obj) => {
				let code = obj.remove("code").context("Missing code")?;
				let message = obj.remove("message").context("Missing message")?;
				Ok(DbResultError::from_code(code.into_int()?, message.into_string()?))
			}
			PublicValue::String(s) => Ok(DbResultError::Thrown(s)),
			other => anyhow::bail!("Expected object for DbResultError, got {}", other.to_sql()),
		}
	}
}

impl Display for DbResultError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.message())
	}
}

impl From<RpcError> for DbResultError {
	fn from(error: RpcError) -> Self {
		match error {
			RpcError::ParseError => DbResultError::ParseError("Parse error".to_string()),
			RpcError::InvalidRequest => {
				DbResultError::InvalidRequest("Invalid request".to_string())
			}
			RpcError::MethodNotFound => {
				DbResultError::MethodNotFound("Method not found".to_string())
			}
			RpcError::MethodNotAllowed => {
				DbResultError::MethodNotAllowed("Method not allowed".to_string())
			}
			RpcError::InvalidParams(message) => DbResultError::InvalidParams(message),
			RpcError::InternalError(error) => DbResultError::InternalError(error.to_string()),
			RpcError::LqNotSuported => DbResultError::LiveQueryNotSupported,
			RpcError::BadLQConfig => {
				DbResultError::BadLiveQueryConfig("Bad live query config".to_string())
			}
			RpcError::BadGQLConfig => {
				DbResultError::BadGraphQLConfig("Bad GraphQL config".to_string())
			}
			RpcError::Thrown(message) => DbResultError::Thrown(message),
			RpcError::Serialize(message) => DbResultError::SerializationError(message),
			RpcError::Deserialize(message) => DbResultError::DeserializationError(message),
			RpcError::SessionNotFound(id) => DbResultError::InternalError(match id {
				Some(id) => format!("Session not found: {id:?}"),
				None => "Default session not found".to_string(),
			}),
			RpcError::SessionExists(id) => {
				DbResultError::InternalError(format!("Session already exists: {id}"))
			}
		}
	}
}

#[derive(Debug)]
pub struct DbResponse {
	pub id: Option<PublicValue>,
	pub session_id: Option<Uuid>,
	pub result: Result<DbResult, DbResultError>,
}

impl DbResponse {
	pub fn new(
		id: Option<PublicValue>,
		session_id: Option<Uuid>,
		result: Result<DbResult, DbResultError>,
	) -> Self {
		Self {
			id,
			session_id,
			result,
		}
	}

	pub fn failure(
		id: Option<PublicValue>,
		session_id: Option<Uuid>,
		error: DbResultError,
	) -> Self {
		Self {
			id,
			session_id,
			result: Err(error),
		}
	}

	pub fn success(id: Option<PublicValue>, session_id: Option<Uuid>, result: DbResult) -> Self {
		Self {
			id,
			session_id,
			result: Ok(result),
		}
	}

	pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
		let value = crate::rpc::format::flatbuffers::decode(bytes)?;
		Self::from_value(value)
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
		if let Some(session_id) = self.session_id {
			value.insert(SESSION_ID.to_string(), PublicValue::Uuid(session_id.into()));
		}
		PublicValue::Object(PublicObject::from(value))
	}

	fn from_value(value: PublicValue) -> anyhow::Result<Self> {
		let PublicValue::Object(mut obj) = value else {
			anyhow::bail!("Expected object for DbResponse");
		};

		let session_id = SurrealValue::from_value(obj.remove(SESSION_ID).unwrap_or_default())?;

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
			session_id,
			result,
		})
	}
}
