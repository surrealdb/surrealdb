use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use revision::{Revisioned, revisioned};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use surrealdb_types::{Kind, Object, SurrealValue, Value};

use crate::expr::TopLevelExpr;
use crate::rpc::DbResultError;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Response";

#[revisioned(revision = 1)]
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryType {
	// Any kind of query
	#[default]
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
	pub fn for_toplevel_expr(expr: &TopLevelExpr) -> Self {
		match expr {
			TopLevelExpr::Live(_) => QueryType::Live,
			TopLevelExpr::Kill(_) => QueryType::Kill,
			_ => QueryType::Other,
		}
	}
}

/// The return value when running a query set on the database.
#[revisioned(revision = 1)]
#[derive(Debug)]
pub struct QueryResult {
	pub time: Duration,
	pub result: Result<Value, DbResultError>,
	// Record the query type in case processing the response is necessary (such as tracking live
	// queries).
	pub query_type: QueryType,
}

impl QueryResult {
	/// Retrieve the response as a normal result
	pub fn output(self) -> Result<Value> {
		self.result.map_err(|err| anyhow::anyhow!(err.to_string()))
	}
}

impl SurrealValue for QueryResult {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(_))
	}

	/// Convert's the response into a value as it is send across the net.
	fn into_value(self) -> Value {
		let mut res = Object::new();
		res.insert("time".to_owned(), Value::String(format!("{:?}", self.time)));

		if !matches!(self.query_type, QueryType::Other) {
			res.insert("type".to_owned(), Value::String(self.query_type.to_string()));
		}

		match self.result {
			Ok(v) => {
				res.insert("status".to_owned(), Value::String("OK".to_string()));
				res.insert("result".to_owned(), v);
			}
			Err(e) => {
				res.insert("status".to_owned(), Value::String("ERR".to_string()));
				res.insert("result".to_owned(), Value::String(e.to_string()));
			}
		}

		Value::Object(res)
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		let Value::Object(mut obj) = value else {
			anyhow::bail!("Expected object for QueryResult");
		};

		let time_str = obj
			.remove("time")
			.and_then(|v| v.into_string().ok())
			.unwrap_or_else(|| "0ns".to_string());
		// Parse duration string (e.g. "1.234ms" -> Duration)
		let time = surrealdb_types::Duration::from_str(&time_str)
			.map(|d| std::time::Duration::from(d))
			.unwrap_or_default();

		let query_type = obj
			.remove("type")
			.and_then(|v| v.into_string().ok())
			.and_then(|s| match s.as_str() {
				"live" => Some(QueryType::Live),
				"kill" => Some(QueryType::Kill),
				_ => Some(QueryType::Other),
			})
			.unwrap_or(QueryType::Other);

		let status = obj
			.remove("status")
			.and_then(|v| v.into_string().ok())
			.unwrap_or_else(|| "OK".to_string());

		let result = if status == "OK" {
			Ok(obj.remove("result").unwrap_or(Value::None))
		} else {
			let error_msg = obj
				.remove("result")
				.and_then(|v| v.into_string().ok())
				.unwrap_or_else(|| "Unknown error".to_string());
			Err(DbResultError::custom(error_msg))
		};

		Ok(QueryResult {
			time,
			result,
			query_type,
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
	Ok,
	Err,
}

impl Serialize for QueryResult {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let includes_type = !matches!(self.query_type, QueryType::Other);
		let mut val = serializer.serialize_struct(
			TOKEN,
			if includes_type {
				3
			} else {
				4
			},
		)?;

		val.serialize_field("time", &format!("{:?}", self.time))?;
		if includes_type {
			val.serialize_field("type", &self.query_type)?;
		}

		match &self.result {
			Ok(v) => {
				val.serialize_field("status", &Status::Ok)?;
				val.serialize_field("result", v)?;
			}
			Err(e) => {
				val.serialize_field("status", &Status::Err)?;
				val.serialize_field("result", &Value::from(e.to_string()))?;
			}
		}
		val.end()
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
