use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use surrealdb_types::{Error as TypesError, kind, object};
use uuid::Uuid;

use crate::dbs;
use crate::dbs::{QueryResult, QueryType};
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

#[derive(Debug)]
pub struct DbResponse {
	pub id: Option<PublicValue>,
	pub session_id: Option<Uuid>,
	/// Success payload or wire-friendly error (kind, message, details, cause).
	pub result: Result<DbResult, TypesError>,
}

impl DbResponse {
	pub fn new(
		id: Option<PublicValue>,
		session_id: Option<Uuid>,
		result: Result<DbResult, TypesError>,
	) -> Self {
		Self {
			id,
			session_id,
			result,
		}
	}

	/// Build a failure response; `error` is converted into the wire error type.
	pub fn failure(
		id: Option<PublicValue>,
		session_id: Option<Uuid>,
		error: impl Into<TypesError>,
	) -> Self {
		Self {
			id,
			session_id,
			result: Err(error.into()),
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
				"error".to_string() => SurrealValue::into_value(err),
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
			Err(TypesError::from_value(error)?)
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
