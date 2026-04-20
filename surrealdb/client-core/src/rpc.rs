use std::time::Duration;

use serde::{Deserialize, Serialize};
use surrealdb_types::{
	Action, Array, Error as TypesError, Kind, Notification, Object, SurrealValue, Value, decode,
	kind, object,
};
use uuid::Uuid;

use crate::response::{QueryResult, QueryType};

const SESSION_ID: &str = "session";

/// Query statistics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct DbResultStats {
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

#[derive(Debug, Serialize, Deserialize)]
pub enum DbResult {
	Other(Value),
	Query(Vec<QueryResult>),
	Live(Notification),
}

impl SurrealValue for DbResult {
	fn kind_of() -> Kind {
		kind!(array | {
			id: uuid,
			session: uuid | none,
			action: string,
			record: any,
			result: any,
		} | any)
	}

	fn is_value(_value: &Value) -> bool {
		true
	}

	fn into_value(self) -> Value {
		match self {
			DbResult::Query(v) => {
				let converted: Vec<Value> = v.into_iter().map(|x| x.into_value()).collect();
				Value::Array(Array::from(converted))
			}
			DbResult::Live(v) => Value::Object(object! {
				id: Value::Uuid(v.id),
				session: v.session.map(Value::Uuid),
				action: v.action.into_value(),
				record: v.record,
				result: v.result,
			}),
			DbResult::Other(v) => v,
		}
	}

	fn from_value(value: Value) -> Result<Self, TypesError> {
		match value {
			Value::Array(arr) => {
				let results = arr
					.into_inner()
					.into_iter()
					.map(QueryResult::from_value)
					.collect::<Result<Vec<_>, TypesError>>()?;
				Ok(DbResult::Query(results))
			}
			Value::Object(obj) => {
				if obj.get("id").is_some() && obj.get("action").is_some() {
					let mut obj = obj.into_inner();
					let id = obj
						.remove("id")
						.ok_or_else(|| TypesError::internal("Missing id".to_string()))?;
					let action = obj
						.remove("action")
						.ok_or_else(|| TypesError::internal("Missing action".to_string()))?;
					let record = obj.remove("record").unwrap_or(Value::None);
					let result = obj.remove("result").unwrap_or(Value::None);

					let Value::Uuid(uuid) = id else {
						return Err(TypesError::internal("Expected UUID for id field".to_string()));
					};
					let Value::String(action_str) = action else {
						return Err(TypesError::internal(
							"Expected string for action field".to_string(),
						));
					};

					let session = match obj.remove(SESSION_ID) {
						Some(session) => SurrealValue::from_value(session)?,
						None => None,
					};

					let action = match action_str.as_str() {
						"CREATE" => Action::Create,
						"UPDATE" => Action::Update,
						"DELETE" => Action::Delete,
						_ => {
							return Err(TypesError::internal(format!(
								"Invalid action: {}",
								action_str
							)));
						}
					};

					Ok(DbResult::Live(Notification::new(uuid, session, action, record, result)))
				} else {
					Ok(DbResult::Other(Value::Object(obj)))
				}
			}
			other => Ok(DbResult::Other(other)),
		}
	}
}

#[derive(Debug)]
pub struct DbResponse {
	pub id: Option<Value>,
	pub session_id: Option<Uuid>,
	pub result: Result<DbResult, TypesError>,
}

impl DbResponse {
	pub fn new(
		id: Option<Value>,
		session_id: Option<Uuid>,
		result: Result<DbResult, TypesError>,
	) -> Self {
		Self {
			id,
			session_id,
			result,
		}
	}

	pub fn failure(
		id: Option<Value>,
		session_id: Option<Uuid>,
		error: impl Into<TypesError>,
	) -> Self {
		Self {
			id,
			session_id,
			result: Err(error.into()),
		}
	}

	pub fn success(id: Option<Value>, session_id: Option<Uuid>, result: DbResult) -> Self {
		Self {
			id,
			session_id,
			result: Ok(result),
		}
	}

	pub fn from_bytes(bytes: &[u8]) -> Result<Self, TypesError> {
		let value = decode(bytes).map_err(|e| TypesError::internal(e.to_string()))?;
		Self::from_value(value)
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
		let mut value = Object::new();
		match self.result {
			Ok(result) => value.insert("result".to_string(), result.into_value()),
			Err(err) => value.insert("error".to_string(), SurrealValue::into_value(err)),
		};
		if let Some(id) = self.id {
			value.insert("id".to_string(), id);
		}
		if let Some(session_id) = self.session_id {
			value.insert(SESSION_ID.to_string(), Value::Uuid(session_id.into()));
		}
		Value::Object(value)
	}

	fn from_value(value: Value) -> Result<Self, TypesError> {
		let Value::Object(mut obj) = value else {
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
