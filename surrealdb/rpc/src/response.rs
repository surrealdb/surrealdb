use std::time::Duration;

use serde::{Deserialize, Serialize};
use surrealdb_types::{
	Action as PublicAction, Array as PublicArray, Error as TypesError, Kind as PublicKind,
	Notification as PublicNotification, Object as PublicObject, SurrealValue, Value as PublicValue,
	kind, object,
};
use uuid::Uuid;

use crate::query::{QueryResult, QueryType};
use crate::request::SESSION_ID;

/// Query statistics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct DbResultStats {
	/// The time taken to execute the query.
	///
	/// Note: This comes from the `time` field of the [`crate::query::QueryResult`] struct.
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
// This enum is the single wire type shared by the server and the Rust SDK's
// remote engines; both sides match on it directly, so a new variant reaches
// them together.
#[derive(Debug, Serialize, Deserialize)]
pub enum DbResult {
	/// Generally methods return a `expr::Value`
	Other(PublicValue),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<QueryResult>),
	/// A query's responses with their rows already encoded for the wire, for
	/// a transport that asked for them in its format. Its value is that of
	/// [`Self::Query`] over the decoded rows. Never deserialized: it is
	/// produced only where the rows are encoded.
	#[serde(skip_deserializing)]
	Encoded(Vec<crate::EncodedQueryResult>),
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
			DbResult::Encoded(v) => {
				let converted: Vec<PublicValue> =
					v.into_iter().map(|x| x.into_query_result().into_value()).collect();
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

	fn from_value(value: PublicValue) -> Result<Self, TypesError> {
		match value {
			PublicValue::Array(arr) => {
				let results = arr
					.into_inner()
					.into_iter()
					.map(QueryResult::from_value)
					.collect::<Result<Vec<_>, TypesError>>()?;
				Ok(DbResult::Query(results))
			}
			PublicValue::Object(obj) => {
				// Check if this is a Live result
				if obj.get("id").is_some() && obj.get("action").is_some() {
					let mut obj = obj.into_inner();
					let id = obj
						.remove("id")
						.ok_or_else(|| TypesError::internal("Missing id".to_string()))?;
					let action = obj
						.remove("action")
						.ok_or_else(|| TypesError::internal("Missing action".to_string()))?;
					let record = obj.remove("record").unwrap_or(PublicValue::None);
					let result = obj.remove("result").unwrap_or(PublicValue::None);

					let PublicValue::Uuid(uuid) = id else {
						return Err(TypesError::internal("Expected UUID for id field".to_string()));
					};

					let session = match obj.remove(SESSION_ID) {
						Some(session) => SurrealValue::from_value(session)?,
						None => None,
					};

					// `Action` owns its wire form, so every variant `into_value` writes
					// decodes here.
					let action = PublicAction::from_value(action)?;

					Ok(DbResult::Live(PublicNotification::new(
						uuid, session, action, record, result,
					)))
				} else {
					Ok(DbResult::Other(PublicValue::Object(obj)))
				}
			}
			other @ PublicValue::None
			| other @ PublicValue::Null
			| other @ PublicValue::Bool(_)
			| other @ PublicValue::Number(_)
			| other @ PublicValue::String(_)
			| other @ PublicValue::Bytes(_)
			| other @ PublicValue::Duration(_)
			| other @ PublicValue::Datetime(_)
			| other @ PublicValue::Date(_)
			| other @ PublicValue::Time(_)
			| other @ PublicValue::Timestamp(_)
			| other @ PublicValue::Interval(_)
			| other @ PublicValue::Uuid(_)
			| other @ PublicValue::Geometry(_)
			| other @ PublicValue::Table(_)
			| other @ PublicValue::RecordId(_)
			| other @ PublicValue::File(_)
			| other @ PublicValue::Range(_)
			| other @ PublicValue::Regex(_)
			| other @ PublicValue::Set(_) => Ok(DbResult::Other(other)),
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
}

impl SurrealValue for DbResponse {
	fn kind_of() -> PublicKind {
		PublicKind::Object
	}

	fn is_value(value: &PublicValue) -> bool {
		matches!(value, PublicValue::Object(_))
	}

	fn into_value(self) -> PublicValue {
		let mut value: std::collections::BTreeMap<&str, PublicValue> =
			std::collections::BTreeMap::new();
		match self.result {
			Ok(result) => {
				value.insert("result", result.into_value());
			}
			Err(err) => {
				value.insert("error", SurrealValue::into_value(err));
			}
		}
		if let Some(id) = self.id {
			value.insert("id", id);
		}
		if let Some(session_id) = self.session_id {
			value.insert(SESSION_ID, PublicValue::Uuid(session_id.into()));
		}
		PublicValue::Object(PublicObject::from(value))
	}

	fn from_value(value: PublicValue) -> Result<Self, TypesError> {
		let PublicValue::Object(mut obj) = value else {
			return Err(TypesError::internal("Expected object for DbResponse".to_string()));
		};

		let session_id = SurrealValue::from_value(obj.remove(SESSION_ID).unwrap_or_default())?;

		let id = obj.remove("id");

		let result = if let Some(result) = obj.remove("result") {
			Ok(DbResult::from_value(result)?)
		} else if let Some(error) = obj.remove("error") {
			Err(TypesError::from_value(error)?)
		} else {
			return Err(TypesError::internal(
				"DbResponse must have either 'result' or 'error' field".to_string(),
			));
		};

		Ok(DbResponse {
			id,
			session_id,
			result,
		})
	}
}

/// Decode a binary (flatbuffers) RPC response payload into a [`DbResponse`].
pub fn db_response_from_bytes(bytes: &[u8]) -> Result<DbResponse, TypesError> {
	let value: PublicValue =
		crate::wire::decode(bytes).map_err(|e| TypesError::internal(e.to_string()))?;
	DbResponse::from_value(value)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn notification(action: PublicAction) -> PublicNotification {
		PublicNotification::new(
			Uuid::from_u128(1).into(),
			Some(Uuid::from_u128(2).into()),
			action,
			PublicValue::None,
			PublicValue::None,
		)
	}

	/// Every action the encoder writes for a live notification decodes back to
	/// itself, so a `KILLED` or `ERROR` frame is not lost on a client.
	#[test]
	fn live_result_round_trips_every_action() {
		for action in [
			PublicAction::Create,
			PublicAction::Update,
			PublicAction::Delete,
			PublicAction::Killed,
			PublicAction::Error,
		] {
			let value = DbResult::Live(notification(action)).into_value();
			match DbResult::from_value(value) {
				Ok(DbResult::Live(decoded)) => assert_eq!(decoded.action, action),
				other => panic!("{action} did not decode as a live result: {other:?}"),
			}
		}
	}

	#[test]
	fn live_result_rejects_an_unknown_action() {
		let value = PublicValue::Object(object! {
			id: PublicValue::Uuid(Uuid::from_u128(1).into()),
			action: PublicValue::String("REVOKED".to_string()),
			record: PublicValue::None,
			result: PublicValue::None,
		});
		assert!(DbResult::from_value(value).is_err());
	}
}
