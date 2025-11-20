use std::fmt;
use std::time::{Duration, Instant};

use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::{Kind, SurrealValue, Value, kind, object};
use surrealdb_protocol::proto::rpc::v1::QueryResponse;

use crate::expr::TopLevelExpr;
use crate::rpc::DbResultError;

#[revisioned(revision = 1)]
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, SurrealValue)]
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
		Value::Object(object! {
			status: Status::from(&self.result).into_value(),
			time: format!("{:?}", self.time).into_value(),
			result: match self.result {
				Ok(v) => v.into_value(),
				Err(e) => Value::from_string(e.to_string()),
			},
			type: self.query_type.into_value(),
		})
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		// Assert required fields
		let Value::Object(mut map) = value else {
			anyhow::bail!("Expected object for QueryResult");
		};
		let Some(status) = map.remove("status") else {
			anyhow::bail!("Expected status for QueryResult");
		};
		let Some(time) = map.remove("time") else {
			anyhow::bail!("Expected time for QueryResult");
		};
		let Some(result) = map.remove("result") else {
			anyhow::bail!("Expected result for QueryResult");
		};

		// Grab status, query type and time
		let status = Status::from_value(status)?;
		let query_type =
			map.remove("type").map(QueryType::from_value).transpose()?.unwrap_or_default();

		let time = humantime::parse_duration(&time.into_string()?)?;

		// Grab result based on status

		let result = match status {
			Status::Ok => Ok(Value::from_value(result)?),
			Status::Err => Err(DbResultError::from_value(result)?),
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
	result: Result<Value, DbResultError>,
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

	pub fn with_result(mut self, result: Result<Value, DbResultError>) -> Self {
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

	pub fn finish_with_result(self, result: Result<Value, DbResultError>) -> QueryResult {
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
