use std::fmt;
use std::time::Duration;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::{
	Error as TypesError, ErrorDetails, Kind, SurrealValue, Value, kind, object,
};
use web_time::Instant;

use crate::expr::TopLevelExpr;

#[revisioned(revision = 1)]
#[derive(
	Debug,
	Copy,
	Clone,
	Default,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Serialize,
	Deserialize,
	SurrealValue,
)]
#[surreal(untagged, lowercase)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
	// Any kind of query
	#[default]
	#[surreal(value = none)]
	Other,
	// Indicates that the response live query id must be tracked
	Live,
	// Indicates that the live query should be removed from tracking
	Kill,
}

impl fmt::Display for QueryType {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			QueryType::Other => "other".fmt(f),
			QueryType::Live => "live".fmt(f),
			QueryType::Kill => "kill".fmt(f),
		}
	}
}

impl QueryType {
	/// Returns the query type for the given toplevel expression.
	pub(crate) fn for_toplevel_expr(expr: &TopLevelExpr) -> Self {
		match expr {
			TopLevelExpr::Live(_) => QueryType::Live,
			TopLevelExpr::Kill(_) => QueryType::Kill,
			_ => QueryType::Other,
		}
	}
}

/// The return value when running a query set on the database.
#[derive(Debug, Clone)]
pub struct QueryResult {
	pub time: Duration,
	pub result: Result<Value, TypesError>,
	// Record the query type in case processing the response is necessary (such as tracking live
	// queries).
	pub query_type: QueryType,
}

impl QueryResult {
	/// Retrieve the response as a normal result
	pub fn output(self) -> Result<Value, TypesError> {
		self.result
	}
}

/// Serialise this error into the query-result wire shape: `result` (message string), optional
/// `kind` and `details`. Does not include `code`. Used for query result responses
/// for backwards compatibility (old clients expect `result` to be the message string).
fn into_query_result_value(error: &TypesError) -> Value {
	let mut details = error.details().clone().into_value();

	if let Value::Object(ref mut obj) = details {
		obj.insert("result", error.message().to_string());
		details
	} else {
		Value::Object(object! {
			result: "Failed to serialise error",
			kind: "Internal",
		})
	}
}

/// Deserialise an error from the query-result wire shape. Requires `result` (message string).
/// The remaining fields (`kind`, optional `details`) are the flattened `ErrorDetails`.
fn from_query_result_value(value: Value) -> Result<TypesError, TypesError> {
	let Value::Object(mut map) = value else {
		return Err(TypesError::internal("Expected object for query result error".to_string()));
	};
	let message = map
		.remove("result")
		.ok_or_else(|| {
			TypesError::internal("Missing result (message) for query result error".to_string())
		})?
		.into_string()
		.map_err(|e| TypesError::internal(e.to_string()))?;
	let details = ErrorDetails::from_value(Value::Object(map)).unwrap_or(ErrorDetails::Internal);
	Ok(TypesError::from_details(message, details))
}

impl SurrealValue for QueryResult {
	fn kind_of() -> Kind {
		kind!(
			{
				status: "OK",
				time: string,
				result: any,
				query_type: (QueryType::kind_of()),
			} | {
				status: "ERR",
				time: string,
				result: string,
				kind: string,
				details: any,
				query_type: (QueryType::kind_of()),
			}
		)
	}

	fn is_value(value: &Value) -> bool {
		value.is_object_and(|map| {
			map.get("status").is_some_and(Status::is_value)
				&& map.get("time").is_some_and(Value::is_string)
				&& map.get("result").is_some()
				&& map.get("type").is_some_and(QueryType::is_value)
		})
	}

	fn into_value(self) -> Value {
		let mut map = object! {
			status: Status::from(&self.result).into_value(),
			time: format!("{:?}", self.time).into_value(),
			type: self.query_type.into_value(),
		};
		match self.result {
			Ok(v) => {
				map.insert("result", v);
			}
			Err(e) => {
				let err_val = into_query_result_value(&e);
				if let Value::Object(err_obj) = err_val {
					for (k, v) in err_obj.into_inner() {
						map.insert(k, v);
					}
				}
			}
		}
		Value::Object(map)
	}

	fn from_value(value: Value) -> Result<Self, TypesError> {
		// Assert required fields
		let Value::Object(mut map) = value else {
			return Err(TypesError::internal("Expected object for QueryResult".to_string()));
		};
		let Some(status) = map.remove("status") else {
			return Err(TypesError::internal("Expected status for QueryResult".to_string()));
		};
		let Some(time) = map.remove("time") else {
			return Err(TypesError::internal("Expected time for QueryResult".to_string()));
		};
		let Some(result) = map.remove("result") else {
			return Err(TypesError::internal("Expected result for QueryResult".to_string()));
		};

		// Grab status, query type and time
		let status = Status::from_value(status)?;
		let query_type =
			map.remove("type").map(QueryType::from_value).transpose()?.unwrap_or_default();

		let time = humantime::parse_duration(
			&time.into_string().map_err(|e| TypesError::internal(e.to_string()))?,
		)
		.map_err(|e| TypesError::internal(e.to_string()))?;

		// Grab result based on status

		let result = match status {
			Status::Ok => Ok(Value::from_value(result)?),
			Status::Err => {
				map.insert("result".to_string(), result);
				Err(from_query_result_value(Value::Object(map))?)
			}
		};

		Ok(QueryResult {
			time,
			result,
			query_type,
		})
	}
}

pub struct QueryResultBuilder {
	start_time: Instant,
	result: Result<Value, TypesError>,
	query_type: QueryType,
}

impl QueryResultBuilder {
	pub fn started_now() -> Self {
		Self {
			start_time: Instant::now(),
			result: Ok(Value::None),
			query_type: QueryType::Other,
		}
	}

	pub fn instant_none() -> QueryResult {
		QueryResult {
			time: Duration::ZERO,
			result: Ok(Value::None),
			query_type: QueryType::Other,
		}
	}

	pub fn with_result(mut self, result: Result<Value, TypesError>) -> Self {
		self.result = result;
		self
	}

	pub fn with_query_type(mut self, query_type: QueryType) -> Self {
		self.query_type = query_type;
		self
	}

	pub fn finish(self) -> QueryResult {
		QueryResult {
			time: self.start_time.elapsed(),
			result: self.result,
			query_type: self.query_type,
		}
	}

	pub fn finish_with_result(self, result: Result<Value, TypesError>) -> QueryResult {
		QueryResult {
			time: self.start_time.elapsed(),
			result,
			query_type: self.query_type,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
#[serde(rename_all = "UPPERCASE")]
#[surreal(untagged, uppercase)]
pub enum Status {
	Ok,
	Err,
}

impl Status {
	pub fn is_ok(&self) -> bool {
		matches!(self, Status::Ok)
	}

	pub fn is_err(&self) -> bool {
		matches!(self, Status::Err)
	}
}

impl<'a, T, E> From<&'a Result<T, E>> for Status {
	fn from(result: &'a Result<T, E>) -> Self {
		match result {
			Ok(_) => Status::Ok,
			Err(_) => Status::Err,
		}
	}
}

impl Serialize for QueryResult {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.clone().into_value().serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for QueryResult {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		// Deserialize as a Value first, then convert
		let value = Value::deserialize(deserializer)?;
		QueryResult::from_value(value).map_err(serde::de::Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{AuthError, NotAllowedError, NotFoundError, ValidationError};

	use super::*;

	fn error_query_result(error: TypesError) -> QueryResult {
		QueryResult {
			time: Duration::from_millis(42),
			result: Err(error),
			query_type: QueryType::Other,
		}
	}

	/// Verify that the `kind` field in a serialized error QueryResult is NOT
	/// duplicated inside `details`. This was a bug where `into_query_result_value`
	/// manually added `kind` and then also included it inside `details` via
	/// `ErrorDetails::into_value()`.
	#[test]
	fn query_result_error_no_kind_duplication() {
		let err = TypesError::not_allowed("Token expired".into(), AuthError::TokenExpired);
		let qr = error_query_result(err);
		let val = qr.into_value();
		let Value::Object(ref obj) = val else {
			panic!("Expected object");
		};

		assert_eq!(obj.get("status"), Some(&Value::String("ERR".into())));
		assert_eq!(obj.get("result"), Some(&Value::String("Token expired".into())));
		assert_eq!(obj.get("kind"), Some(&Value::String("NotAllowed".into())));

		// `details` must contain the inner NotAllowedError, NOT a duplicate of ErrorDetails
		let Some(Value::Object(details)) = obj.get("details") else {
			panic!("Expected details object");
		};
		assert_eq!(
			details.get("kind"),
			Some(&Value::String("Auth".into())),
			"details.kind should be the inner variant, not a duplicate of the top-level kind"
		);
	}

	#[test]
	fn query_result_error_round_trip_with_details() {
		let err = TypesError::not_allowed("Token expired".into(), AuthError::TokenExpired);
		let qr = error_query_result(err);
		let val = qr.into_value();
		let parsed = QueryResult::from_value(val).expect("round-trip should succeed");

		let err = parsed.result.unwrap_err();
		assert!(err.is_not_allowed());
		assert_eq!(err.message(), "Token expired");
		assert!(matches!(
			err.not_allowed_details(),
			Some(NotAllowedError::Auth(AuthError::TokenExpired))
		));
	}

	#[test]
	fn query_result_error_round_trip_nested_struct_details() {
		let err = TypesError::not_found(
			"Table not found".into(),
			NotFoundError::Table {
				name: "users".into(),
			},
		);
		let qr = error_query_result(err);
		let val = qr.into_value();
		let parsed = QueryResult::from_value(val).expect("round-trip should succeed");

		let err = parsed.result.unwrap_err();
		assert!(err.is_not_found());
		assert!(matches!(
			err.not_found_details(),
			Some(NotFoundError::Table { name }) if name == "users"
		));
	}

	#[test]
	fn query_result_error_round_trip_no_inner_details() {
		let err = TypesError::internal("Something went wrong".into());
		let qr = error_query_result(err);
		let val = qr.clone().into_value();

		let Value::Object(ref obj) = val else {
			panic!("Expected object");
		};
		assert_eq!(obj.get("kind"), Some(&Value::String("Internal".into())));
		assert!(!obj.contains_key("details"), "Internal errors should have no details");

		let parsed = QueryResult::from_value(val).expect("round-trip should succeed");
		let err = parsed.result.unwrap_err();
		assert!(err.is_internal());
		assert_eq!(err.message(), "Something went wrong");
	}

	#[test]
	fn query_result_error_round_trip_validation_parse() {
		let err = TypesError::validation("Parse error".into(), ValidationError::Parse);
		let qr = error_query_result(err);
		let val = qr.into_value();
		let parsed = QueryResult::from_value(val).expect("round-trip should succeed");

		let err = parsed.result.unwrap_err();
		assert!(err.is_validation());
		assert_eq!(err.validation_details(), Some(&ValidationError::Parse));
	}

	#[test]
	fn query_result_ok_round_trip() {
		let qr = QueryResult {
			time: Duration::from_millis(10),
			result: Ok(Value::String("hello".into())),
			query_type: QueryType::Other,
		};
		let val = qr.into_value();
		let parsed = QueryResult::from_value(val).expect("round-trip should succeed");

		let v = parsed.result.unwrap();
		assert_eq!(v, Value::String("hello".into()));
	}
}
